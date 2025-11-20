import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from dotenv import load_dotenv

# Load environment
path = os.path.join(os.path.dirname(__file__), "..", ".env")
load_dotenv(dotenv_path=path)

HDFS_PATH = os.environ.get("HDFS_OUTPUT_PATH")
MONGODB_URI = os.environ.get("MONGODB_URI")
print(f"HDFS_PATH: {HDFS_PATH}")
print(f"MONGODB_URI: {MONGODB_URI}")

if not HDFS_PATH or not MONGODB_URI:
    raise EnvironmentError("Cannot read HDFS PATH and MONGODB URI.")

try:
    spark = (
        SparkSession.builder
        .appName("WeatherBatchAnalysis")
        .config("spark.mongodb.connection.uri", MONGODB_URI)
        .config("spark.mongodb.write.connection.uri", MONGODB_URI)
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    print("Created Spark Session successfully")
except Exception as e:
    print(f"Error when creating Spark Session: {e}")
    exit(1)
print("Reading data from HDFS parquet...")

try:
    df = spark.read.parquet(HDFS_PATH)
except Exception as e:
    print(f"Error when reading from HDFS: {e}")
    exit(1)

print("Start Batch Analysis ...")
df = df.dropDuplicates().orderBy("city_name", "datetime")

city_stats = df.groupBy("city_name", "country").agg(
    F.mean("temperature").alias("avg_temp"),
    F.max("temperature").alias("max_temp"),
    F.min("temperature").alias("min_temp"),
    F.mean("humidity").alias("avg_humidity"),
    F.sum("rain").alias("total_rain")
)

city_stats.show()

df = df.withColumn("date", F.to_date("datetime"))
daily_stats = df.groupBy("city_name", "country", "date").agg(
    F.max("temperature").alias("daily_high_temp"),
    F.min("temperature").alias("daily_low_temp"),
    F.sum("rain").alias("daily_total_rain")
).orderBy("date", "city_name")

daily_stats.show()

print("Writing batch views to MongoDB....")
try:
    (
        city_stats.write
        .format("mongodb")
        .mode("overwrite")
        .option("spark.mongodb.connection.uri", MONGODB_URI)
        .option("spark.mongodb.database", "weather_db")
        .option("spark.mongodb.collection", "city_summary")
        .save()
    )
    (
        daily_stats.write
        .format("mongodb")
        .mode("overwrite")
        .option("spark.mongodb.connection.uri", MONGODB_URI)
        .option("spark.mongodb.database", "weather_db")
        .option("spark.mongodb.collection", "daily_summary")
        .save()
    )
    print("Write to MongoDB successfully")
except Exception as e:
    print(f"Error when writing to MongoDB: {e}")
    exit(1)

spark.stop()
print("Finishing Batch Processing!")
