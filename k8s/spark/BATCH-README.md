# Spark Batch Processing on Kubernetes

This folder contains manifests for the Spark batch job that reads weather data from HDFS and writes aggregated results to MongoDB. The job runs continuously with 5-minute intervals.

## Architecture

```
HDFS (weather-data/) → Spark Batch Job → MongoDB (weather.weather_batch)
```

The batch job:
1. Reads Parquet files from HDFS at `hdfs://hdfs-namenode.bigdata.svc.cluster.local:8020/weather-data`
2. Performs data cleaning (deduplication by city_name + datetime)
3. Writes results to MongoDB collection `weather.weather_batch`
4. Runs every 5 minutes automatically

## Prerequisites

- HDFS NameNode and DataNode running
- MongoDB running
- Spark image built with batch scripts: `spark-consumer:latest`
- Streaming consumer should be running to populate HDFS with data

## Deployment

### 1. Build/Update Spark Image (if needed)

```powershell
# Set Minikube Docker environment
minikube docker-env | Invoke-Expression

# Build image with batch scripts
docker build -t spark-consumer:latest -f spark.Dockerfile .
```

### 2. Apply ConfigMap and Deployment

```powershell
kubectl apply -f k8s/spark/batch-configmap.yaml
kubectl apply -f k8s/spark/batch-deployment.yaml
```

### 3. Verify Deployment

```powershell
kubectl -n bigdata get pods -l app=spark-batch
kubectl -n bigdata logs -f deploy/spark-batch
```

Expected logs:
```
Waiting for HDFS and MongoDB...
Starting batch processing loop...
=== Starting batch job at ... ===
Reading parquet from HDFS path: hdfs://hdfs-namenode.bigdata.svc.cluster.local:8020/weather-data
Deduped records: before=..., after=...
Writing to MongoDB: db=weather, collection=weather_batch
Write to MongoDB completed.
Sleeping for 5 minutes before next run...
```

## Configuration

All configuration is in `batch-configmap.yaml`:

| Variable | Default | Description |
|----------|---------|-------------|
| `HDFS_OUTPUT_PATH` | `hdfs://hdfs-namenode.bigdata.svc.cluster.local:8020/weather-data` | HDFS directory containing Parquet files |
| `MONGO_URI` | `mongodb://mongodb.bigdata.svc.cluster.local:27017/weather` | MongoDB connection string |
| `MONGO_DB_NAME` | `weather` | Target database |
| `MONGO_COLLECTION` | `weather_batch` | Target collection |

## Batch Job Behavior

- **Initial Wait**: 60 seconds for HDFS/MongoDB readiness
- **Run Interval**: Every 5 minutes
- **Error Handling**: If HDFS is empty or job fails, it logs error and retries in next cycle
- **Restart Policy**: Always (K8s restarts container if it crashes)

## Checking MongoDB Data

```powershell
# Access MongoDB pod
kubectl -n bigdata exec -it mongodb-0 -- mongosh

# In mongosh:
use weather
db.weather_batch.countDocuments()
db.weather_batch.find().limit(5)
```

## Troubleshooting

### Batch job shows "Unable to infer schema"
HDFS is empty. Wait for streaming consumer to write data first:
```powershell
kubectl -n bigdata logs -f deploy/spark-streaming-consumer
```

### Check HDFS contents
```powershell
kubectl -n bigdata exec hdfs-namenode-0 -- hdfs dfs -ls /weather-data
kubectl -n bigdata exec hdfs-namenode-0 -- hdfs dfs -du -h /weather-data
```

### MongoDB connection issues
Verify MongoDB is running:
```powershell
kubectl -n bigdata get pods -l app=mongodb
kubectl -n bigdata logs mongodb-0
```

## Cleanup

```powershell
kubectl -n bigdata delete deploy/spark-batch
kubectl -n bigdata delete cm spark-batch-config
```

## Next Steps

- Add CronJob instead of continuous loop for scheduled batch processing
- Implement incremental processing (read only new files)
- Add data quality checks and monitoring
- Export batch results to external analytics systems
