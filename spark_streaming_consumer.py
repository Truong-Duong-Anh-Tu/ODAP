from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType

spark = SparkSession.builder \
    .appName("KafkaCreditCardStreaming") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.4") \
    .getOrCreate()
    
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

query = invalid_df2.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
