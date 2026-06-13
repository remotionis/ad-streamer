from pyspark.sql import SparkSession
from pyspark.sql.funcitons import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, TimestampType


KAFKA_BROKER = "redpanda:9092"
TOPIC_NAME = "ad-stream-topic"
GCS_BUCKET = ""

BRONZE_PATH = f"{GCS_BUCKET}/bronze/"
CHECKPOINT_PATH = f"{GCS_BUCKET}/checkpoints/bronze/"


spark = SparkSession.builder \
	.appName("Ads_bronze") \
	.config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
	.config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
	.getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# 스키마 설정
schema = StructType([
	StructField("event_id", StringType(), True),
	StructField("user_id", StringType(), True),
	StructField("ad_id", StringType(), True),
	StructField("event_type", StringType(), True),
	StructField("site_url", StringType(), True),
	StructField("device_os", StringType(), True),
	StructField("ip_address", StringType(), True),
	StructField("timestamp", TimestampType(), True)
])


# 버퍼로부터 데이터 리딩
df_kafka = spark.readStream \
	.format("kafka") \
	.option("kafka.bootstrap.servers", KAFKA_BROKER) \
	.option("subscribe", TOPIC_NAME) \
	.option("startingOffsets", "earliest") \
	.option("maxOffsetPerTrigger", 50000) \
	.load()


# json으로 파싱
df_parsed = df_kafka.selectExpr("CAST(value AS STRING) as json_str") \
	.select(from_json(col("json_str"), schema).alias("data")) \
	.select("data.*")


# GCS 적재 - 포맷: Delta
query = df_parsed.writeStream \
	.format("delta") \
	.outputMode("append") \
	.option("checkpointLocation", CHECKPOINT_PATH) \
	.trigger(processingTime="10 seconds") \
	.start(BRONZE_PATH)


query.awaitTermination()
