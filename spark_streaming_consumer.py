from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, DoubleType
from pyspark.sql.functions import from_json, col, concat_ws, to_timestamp, date_format, sum, count, window
from pyspark.sql.functions import length, when, regexp_replace, trim, lit, udf, current_date
import requests
import json
from datetime import datetime, timedelta
import threading
import time
from exchange_rate import current_exchange_rate, fetch_exchange_rate, update_exchange_rate_periodically


# Khởi tạo Spark Session  
spark = SparkSession.builder \
    .appName("KafkaCreditCardStreaming") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.5") \
    .getOrCreate()

spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")
spark.sparkContext.setLogLevel("WARN")

# Lấy tỷ giá lần đầu
fetch_exchange_rate()

# Chạy thread cập nhật tỷ giá định kỳ
exchange_rate_thread = threading.Thread(target=update_exchange_rate_periodically, daemon=True)
exchange_rate_thread.start()

# Tạo function để broadcast tỷ giá
def create_exchange_rate_broadcast():
    return spark.sparkContext.broadcast(current_exchange_rate)

# Cấu trúc schema cho dữ liệu Kafka
schema = StructType() \
    .add("User", StringType()) \
    .add("Card", StringType()) \
    .add("Year", StringType()) \
    .add("Month", StringType()) \
    .add("Day", StringType()) \
    .add("Time", StringType()) \
    .add("Amount", StringType()) \
    .add("Use Chip", StringType()) \
    .add("Merchant Name", StringType()) \
    .add("Merchant City", StringType()) \
    .add("Merchant State", StringType()) \
    .add("Zip", StringType()) \
    .add("MCC", StringType()) \
    .add("Errors?", StringType()) \
    .add("Is Fraud?", StringType())

# Đọc dữ liệu từ Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "credit-card-transactions") \
    .option("startingOffsets", "earliest") \
    .load()

# Chuyển value từ bytes → JSON
json_df = df.selectExpr("CAST(value AS STRING) as json_data") \
    .select(from_json(col("json_data"), schema).alias("data")) \
    .select("data.*")

# Lọc ra giao dịch hợp lệ (không fraud, không lỗi)
invalid_df1 = json_df.filter(col("Errors?").isNotNull())
invalid_df2 = invalid_df1.filter(col("Is Fraud?") == "No")

# Định nghĩa hàm xử lý cho mỗi batch
def process_batch(batch_df, batch_id):

    try:
        if batch_df.head(1):
            # Lấy tỷ giá hiện tại cho batch này
            current_rate = current_exchange_rate
            print(f"Batch {batch_id}: Sử dụng tỷ giá {current_rate}")
            
            # Xử lý batch với tỷ giá hiện tại
            processed_batch = batch_df \
                .withColumn("Time_fixed", when(length(col("Time")) <= 5, concat_ws(":", col("Time"), lit("00"))).otherwise(col("Time"))) \
                .withColumn("datetime_str", concat_ws(" ", concat_ws("/", col("Day"), col("Month"), col("Year")), col("Time_fixed"))) \
                .withColumn("timestamp", to_timestamp(col("datetime_str"), "dd/MM/yyyy HH:mm:ss")) \
                .withColumn("date", date_format(col("timestamp"), "dd/MM/yyyy")) \
                .withColumn("time", date_format(col("timestamp"), "HH:mm:ss")) \
                .withColumn("Amount_Cleaned", regexp_replace(trim(col("Amount")), "[$,]", "")) \
                .withColumn("Current_Exchange_Rate", lit(current_rate)) \
                .withColumn("Amount_VND", col("Amount_Cleaned").cast("double") * col("Current_Exchange_Rate"))
            
            # Ghi batch vào HDFS
            processed_batch.select(
                "Card", "date", "time", "Merchant Name", "Merchant City", "Amount_VND"
            ).write \
                .mode("append") \
                .option("header", "true") \
                .format("csv") \
                .save("hdfs://localhost:9000/processed_data/transactions")
            
            # Tính toán và ghi thống kê
            # agg_batch = processed_batch \
            #     .withWatermark("timestamp", "1 day") \
            #     .groupBy(window(col("timestamp"), "1 day"), col("Merchant Name")) \
            #     .agg(
            #         count("*").alias("num_transactions"),
            #         sum("Amount_VND").alias("total_amount_vnd")
            #     )
            
            agg_batch = processed_batch \
                .withWatermark("timestamp", "1 day") \
                .groupBy(window(col("timestamp"), "1 day"), col("Merchant Name")) \
                .agg(
                    count("*").alias("num_transactions"),
                    sum("Amount_VND").alias("total_amount_vnd")
                ) \
                .withColumn(
                    "window_str",
                    concat_ws(" - ",
                        date_format(col("window.start"), "yyyy-MM-dd"),
                        date_format(col("window.end"), "yyyy-MM-dd")
                    )
                ) \
                .drop("window")
            
            agg_batch.write \
                .mode("append") \
                .option("header", "true") \
                .format("csv") \
                .save("hdfs://localhost:9000/processed_data/statistics")
            
    except Exception:
        import traceback
        traceback.print_exc()
        raise

# Áp dụng function xử lý batch
query = invalid_df2.writeStream \
    .foreachBatch(process_batch) \
    .option("checkpointLocation", "hdfs://localhost:9000/checkpoints/main") \
    .start()

# Chờ kết thúc
query.awaitTermination()