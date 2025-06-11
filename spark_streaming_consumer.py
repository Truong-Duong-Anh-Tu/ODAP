from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType
from pyspark.sql.functions import from_json, col, concat_ws, to_timestamp, date_format, sum, count, window
from pyspark.sql.functions import from_json, col, concat_ws, to_timestamp, date_format, sum, count, window, length, when, regexp_replace, trim
from pyspark.sql.functions import col, length, when, concat_ws, lit
# Tỷ giá VND (có thể cập nhật động trong tương lai)
EXCHANGE_RATE = 24000



spark = SparkSession.builder \
    .appName("KafkaCreditCardStreaming") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.4") \
    .getOrCreate()
    
spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")    
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

# Khởi tạo Spark Session
spark = SparkSession.builder \
    .appName("KafkaCreditCardStreaming") \
    .master("local[*]") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

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

processed_df = invalid_df2 \
    .withColumn("Time_fixed",when(length(col("Time")) <= 5, concat_ws(":", col("Time"), lit("00"))).otherwise(col("Time"))) \
    .withColumn("datetime_str", concat_ws(" ", concat_ws("/", col("Day"), col("Month"), col("Year")), col("Time_fixed"))) \
    .withColumn("timestamp", to_timestamp(col("datetime_str"), "dd/MM/yyyy HH:mm:ss")) \
    .withColumn("date", date_format(col("timestamp"), "dd/MM/yyyy")) \
    .withColumn("time", date_format(col("timestamp"), "HH:mm:ss")) \
    .withColumn("Amount_Cleaned", regexp_replace(trim(col("Amount")), "[$,]", "")) \
    .withColumn("Amount_VND", col("Amount_Cleaned").cast("double") * EXCHANGE_RATE)

# 7. Ghi dữ liệu đã xử lý xuống HDFS
write_query = processed_df.select(
    "Card", "date", "time", "Merchant Name", "Merchant City", "Amount_VND"
).writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "hdfs://localhost:9000/processed_data/transactions") \
    .option("checkpointLocation", "hdfs://localhost:9000/checkpoints/transactions") \
    .start()

# 8. Tính toán thống kê mỗi ngày theo Merchant Name
agg_df = processed_df \
    .withWatermark("timestamp", "1 day") \
    .groupBy(window(col("timestamp"), "1 day"), col("Merchant Name")) \
    .agg(
        count("*").alias("num_transactions"),
        sum("Amount_VND").alias("total_amount_vnd")
    )

# 9. Ghi dữ liệu thống kê xuống HDFS
agg_query = agg_df.writeStream \
    .outputMode("append") \
    .format("csv") \
    .option("path", "hdfs://localhost:9000/processed_data/statistics") \
    .option("checkpointLocation", "hdfs://localhost:9000/checkpoints/statistics") \
    .start()

# 10. Chờ kết thúc
write_query.awaitTermination()
agg_query.awaitTermination()
