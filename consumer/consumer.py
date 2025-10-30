from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import from_json, col
from influxdb_client import InfluxDBClient, Point, WritePrecision
import os
from influxdb_client.client.write_api import SYNCHRONOUS
from dotenv import load_dotenv

load_dotenv(dotenv_path= os.path.join(os.path.dirname(__file__), '..', '.env'))

KAFKA_BROKERS = os.environ.get("KAFKA_INTERNAL_SERVERS")
KAFKA_TOPIC = os.environ.get("WEATHER_KAFKA_TOPIC")

INFLUX_URL = os.environ.get("INFLUXDB_SERVER")
INFLUX_TOKEN = os.environ.get("INFLUXDB_TOKEN")
INFLUX_ORG = os.environ.get("INFLUXDB_ORG", "primary")
INFLUX_BUCKET = os.environ.get("INFLUXDB_BUCKET", "primary")
INFLUX_MEASUREMENT = os.environ.get("INFLUXDB_MEASUREMENT", "weather-data")
spark = (
    SparkSession.builder
    .appName("KafkaToInfluxDB")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("city_name", StringType(), True),
    StructField("datetime", TimestampType(), True),
    StructField("temperature", DoubleType(), True),
    StructField("humidity", DoubleType(), True),
    StructField("wind_speed", DoubleType(), True),
    StructField("cloud_cover", DoubleType(), True),
    StructField("visibility", DoubleType(), True),
    StructField("air_pressure", DoubleType(), True),
    StructField("air_pressure_sea_level", DoubleType(), True),
    StructField("air_pressure_ground_level", DoubleType(), True),
    StructField("apparent_temperature", DoubleType(), True),
    StructField("wind_direction", DoubleType(), True),
    StructField("rain", DoubleType(), True),
    StructField("country", StringType(), True),
])

print(f"Connecting to Kafka brokers: {KAFKA_BROKERS}")
print(f"Subscribing topic: {KAFKA_TOPIC}")

df_kafka = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKERS)
    .option("subscribe", KAFKA_TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

df_parsed = (
    df_kafka.selectExpr("CAST(value AS STRING) as json_str")
    .select(from_json(col("json_str"), schema).alias("data"))
    .select("data.*")
)

def write_to_influxdb(batch_df, batch_id):
    print(f"--- Processing Batch ID: {batch_id} ---")
    records = batch_df.collect()

    if not records:
        print(f"Batch {batch_id}: No records to write.")
        return

    print(f"Batch {batch_id}: Collected {len(records)} records. Preparing to write to InfluxDB...")

    try:
        with InfluxDBClient(url=INFLUX_URL, token= INFLUX_TOKEN, org=INFLUX_ORG) as client:
            write_api = client.write_api(write_options=SYNCHRONOUS)
            points = []
            for row in records:
                if row["datetime"] is None:
                    print(f"Batch {batch_id}: Skipping record with null datetime")
                    continue
                    
                point = (
                    Point(INFLUX_MEASUREMENT)
                    .time(row["datetime"], WritePrecision.S)
                    .tag("city_name", row["city_name"])
                    .tag("country", row["country"])
                    .field("temperature", row["temperature"])
                    .field("humidity", row["humidity"])
                    .field("wind_speed", row["wind_speed"])
                    .field("cloud_cover", row["cloud_cover"])
                    .field("visibility", row["visibility"])
                    .field("air_pressure", row["air_pressure"])
                    .field("air_pressure_sea_level", row["air_pressure_sea_level"])
                    .field("air_pressure_ground_level", row["air_pressure_ground_level"])
                    .field("apparent_temperature", row["apparent_temperature"])
                    .field("wind_direction", row["wind_direction"])
                    .field("rain", row["rain"])
                )
                points.append(point)
            
            if points:
                write_api.write(bucket=INFLUX_BUCKET, org=INFLUX_ORG, record=points)
                print(f"Batch {batch_id}: Successfully wrote {len(points)} points to InfluxDB.")

    except Exception as e:
        print(f"ERROR in Batch {batch_id}: Failed to write to InfluxDB.")
        print(f"Error details: {e}")
df_parsed.printSchema()
query_console = (
    df_parsed.writeStream
    .foreachBatch(write_to_influxdb)
    .outputMode("update")
    .start()
)
query_console.awaitTermination()

