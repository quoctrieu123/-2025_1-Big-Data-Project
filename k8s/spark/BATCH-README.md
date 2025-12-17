Spark Batch Processing on Kubernetes
Deployment


Build image Docker nếu chưa build
```powershell
# Set Minikube Docker environment
minikube docker-env | Invoke-Expression

# Build image with batch scripts
docker build -t spark-consumer:latest -f spark.Dockerfile .
```


Tạo configmap và deployment cho batch job
```powershell
kubectl apply -f k8s/spark/batch-configmap.yaml
kubectl apply -f k8s/spark/batch-deployment.yaml
```

Check batch job
```powershell
kubectl -n bigdata get pods -l app=spark-batch
kubectl -n bigdata logs -f deploy/spark-batch
```



Xem data trong mongodb
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
