import os
from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator

def main():
    spark = SparkSession.builder \
        .appName("WeatherPredictionModel") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Đọc data từ HDFS
    HDFS_PATH = os.environ.get("HDFS_OUTPUT_PATH", "hdfs://hdfs-namenode.bigdata.svc.cluster.local:8020/weather-data")
    print(f"Reading data from: {HDFS_PATH}")

    try:
        df = spark.read.parquet(HDFS_PATH)
        # Cache data to avoid re-reading small files from HDFS multiple times
        df.cache()
    except Exception as e:
        print(f"Error reading data from HDFS: {e}")
        print("Please ensure the streaming job has written some data to HDFS first.")
        spark.stop()
        return

    
    if df.count() == 0:
        print("Data is empty. Skipping training.")
        spark.stop()
        return

    # Cấu trúc data
    print("Data schema:")
    df.printSchema()

    # Loại bỏ duplicate
    print("Removing duplicate records...")
    df = df.dropDuplicates()

    #Chọn cột làm feature
    excluded_cols = ['temperature', 'city_name', 'country', 'datetime', 'timestamp', 'rank']
    
    numeric_types = ['int', 'double', 'float', 'long', 'bigint', 'smallint', 'tinyint']
    df = df.drop("apparent_temperature")
    feature_cols = [c for c, t in df.dtypes if t in numeric_types and c not in excluded_cols]
    print(f"Selected feature columns: {feature_cols}")
    
    if not feature_cols:
        print("No feature columns found! Check schema.")
        spark.stop()
        return

    # Chia theo time base
    print("Splitting data based on time (80% train, 20% test)...")
    windowSpec = Window.orderBy("datetime")
    df_with_rank = df.withColumn("rank", row_number().over(windowSpec)) # tạo cột rank là số thứ tự theo datetime
    
    total_count = df_with_rank.count()
    train_count = int(total_count * 0.8)
    
    print(f"Total records: {total_count}")
    print(f"Splitting: First {train_count} records for Training, remaining {total_count - train_count} for Testing.")
    
    train_raw = df_with_rank.filter(col("rank") <= train_count).drop("rank") # lấy phần train
    test_raw = df_with_rank.filter(col("rank") > train_count).drop("rank") # lấy phần test

    
    assembler = VectorAssembler(
        inputCols=feature_cols,
        outputCol="features_unscaled"
    )
    
    #normalize data bằng StandardScaler
    scaler = StandardScaler(inputCol="features_unscaled", outputCol="features", withStd=True, withMean=True)
    
    # lấy vector features
    train_vec = assembler.transform(train_raw)
    test_vec = assembler.transform(test_raw)
    
    # normalize features
    print("Fitting StandardScaler on training data...")
    scaler_model = scaler.fit(train_vec)
    
    # normalize train và test data
    train_data = scaler_model.transform(train_vec).select("features", "temperature")
    test_data = scaler_model.transform(test_vec).select("features", "temperature")

    print(f"Training dataset count: {train_data.count()}")
    print(f"Test dataset count: {test_data.count()}")

    #train model
    lr = LinearRegression(featuresCol="features", labelCol="temperature")
    print("Training Linear Regression model...")
    lr_model = lr.fit(train_data)

    #hệ số model
    print(f"Coefficients: {lr_model.coefficients}")
    print(f"Intercept: {lr_model.intercept}")

    # đánh giá model
    predictions = lr_model.transform(test_data)
    
    evaluator = RegressionEvaluator(
        labelCol="temperature", predictionCol="prediction", metricName="rmse"
    )
    rmse = evaluator.evaluate(predictions)
    print(f"Root Mean Squared Error (RMSE) on test data: {rmse}")

    r2_evaluator = RegressionEvaluator(
        labelCol="temperature", predictionCol="prediction", metricName="r2"
    )
    r2 = r2_evaluator.evaluate(predictions)
    print(f"R2 on test data: {r2}")

    # in một vài dự đoán trong test set
    print("Sample predictions:")
    predictions.select("temperature", "prediction", "features").show(5)

    #lưu model lên HDFS
    MODEL_PATH = "hdfs://hdfs-namenode.bigdata.svc.cluster.local:8020/models/weather_prediction_v1"
    try:
        lr_model.write().overwrite().save(MODEL_PATH)
        print(f"Model successfully saved to {MODEL_PATH}")
    except Exception as e:
        print(f"Error saving model: {e}")

    spark.stop()

if __name__ == "__main__":
    main()
