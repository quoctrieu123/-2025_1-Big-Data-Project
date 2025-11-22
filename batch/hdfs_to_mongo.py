import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col

# Load env file (shared with other services)
from dotenv import load_dotenv
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '..', '.env'))

HDFS_PATH = os.getenv("HDFS_OUTPUT_PATH")
MONGO_URI = os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGO_DB_NAME", "weather")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "weather_batch")

if not HDFS_PATH:
    raise RuntimeError("HDFS_OUTPUT_PATH is not set")
if not MONGO_URI:
    raise RuntimeError("MONGO_URI is not set")

spark = (
    SparkSession.builder
    .appName("HDFSToMongoBatch")
    .config("spark.mongodb.write.connection.uri", MONGO_URI)
    .config("spark.mongodb.write.database", MONGO_DB)
    .config("spark.mongodb.write.collection", MONGO_COLLECTION)
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print(f"Reading parquet from HDFS path: {HDFS_PATH}")
try:
    df: DataFrame = spark.read.parquet(HDFS_PATH)
except Exception as e:
    print(f"Failed to read parquet from {HDFS_PATH}: {e}")
    spark.stop()
    raise

if df.rdd.isEmpty():
    print("No data found in HDFS path; exiting.")
    spark.stop()
    raise SystemExit(0)

# Optional basic cleansing / deduping by (city_name, datetime)
if "city_name" in df.columns and "datetime" in df.columns:
    before = df.count()
    df = df.dropna(subset=["city_name", "datetime"]).dropDuplicates(["city_name", "datetime"])
    after = df.count()
    print(f"Deduped records: before={before}, after={after}")

print(f"Writing to MongoDB: db={MONGO_DB}, collection={MONGO_COLLECTION}")
try:
    df.write.format("mongodb").mode("append").save()
    print("Write to MongoDB completed.")
except Exception as e:
    print(f"Failed writing to MongoDB: {e}")
    raise
finally:
    spark.stop()
