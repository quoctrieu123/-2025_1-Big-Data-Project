from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import from_json, col
from influxdb_client import InfluxDBClient, Point, WritePrecision
import os
from utils import load_environment_variables
from influxdb_client.client.write_api import SYNCHRONOUS
from dotenv import load_dotenv
# ======== 1. Cấu hình môi trường ========
load_dotenv()
env_vars = load_environment_variables()

KAFKA_BROKERS = "kafka1:19092,kafka2:19093,kafka3:19094"
KAFKA_TOPIC = env_vars.get("WEATHER_KAFKA_TOPIC", "weather-data")

INFLUX_URL = "http://influxdb:8086"
INFLUX_TOKEN = env_vars.get("INFLUXDB_TOKEN")
INFLUX_ORG = env_vars.get("INFLUXDB_ORG", "primary")
INFLUX_BUCKET = env_vars.get("INFLUXDB_BUCKET", "primary")
INFLUX_MEASUREMENT = env_vars.get("INFLUXDB_MEASUREMENT", "weather-data")

spark = (
    SparkSession.builder
    .appName("KafkaToInfluxDB")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ======== 3. Định nghĩa schema JSON ========
schema = StructType([
    StructField("datetime", TimestampType(), True),
    StructField("temperature_2m", DoubleType(), True),
    StructField("relative_humidity_2m", DoubleType(), True),
    StructField("rain", DoubleType(), True),
    StructField("wind_speed_10m", DoubleType(), True),
    StructField("wind_direction_10m", DoubleType(), True),
    StructField("visibility", DoubleType(), True),
    StructField("precipitation_probability", DoubleType(), True),
    StructField("apparent_temperature", DoubleType(), True),
    StructField("cloud_cover", DoubleType(), True),
    StructField("cloud_cover_low", DoubleType(), True),
    StructField("cloud_cover_mid", DoubleType(), True),
    StructField("cloud_cover_high", DoubleType(), True),
])

# ======== 4. Đọc stream từ Kafka ========
print(f"✅ Connecting to Kafka brokers: {KAFKA_BROKERS}")
print(f"✅ Subscribing topic: {KAFKA_TOPIC}")

df_kafka = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

# Kafka value là binary => cần chuyển sang string
df_parsed = (
    df_kafka.selectExpr("CAST(value AS STRING) as json_str")
    .select(from_json(col("json_str"), schema).alias("data"))
    .select("data.*")
)

# ======== 5. Hàm ghi vào InfluxDB ========
def write_to_influxdb(batch_df, batch_id):
    records = batch_df.collect()
    if not records:
        return

    with InfluxDBClient(url=INFLUX_URL, token=INFLUX_TOKEN, org=INFLUX_ORG) as client:
        write_api = client.write_api(write_options=SYNCHRONOUS)
        points = []
        for row in records:
            point = (
                Point(INFLUX_MEASUREMENT)
                .time(row["datetime"], WritePrecision.S)
                .field("temperature_2m", row["temperature_2m"])
                .field("relative_humidity_2m", row["relative_humidity_2m"])
                .field("rain", row["rain"])
                .field("wind_speed_10m", row["wind_speed_10m"])
                .field("wind_direction_10m", row["wind_direction_10m"])
                .field("visibility", row["visibility"])
                .field("precipitation_probability", row["precipitation_probability"])
                .field("apparent_temperature", row["apparent_temperature"])
                .field("cloud_cover", row["cloud_cover"])
                .field("cloud_cover_low", row["cloud_cover_low"])
                .field("cloud_cover_mid", row["cloud_cover_mid"])
                .field("cloud_cover_high", row["cloud_cover_high"])
            )
            points.append(point)

        write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=points)

# ======== 6. Ghi stream vào InfluxDB ========
query = (
    df_parsed.writeStream
    .foreachBatch(write_to_influxdb)
    .outputMode("update")
    .start()
)

query.awaitTermination()
