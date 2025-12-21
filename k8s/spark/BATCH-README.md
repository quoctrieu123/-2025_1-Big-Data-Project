Spark Batch Processing on Kubernetes

Build image Docker nếu chưa build
```powershell
# Build image cho spark
minikube docker-env | Invoke-Expression


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
kubectl -n bigdata logs deploy/spark-batch | Select-String -Pattern "INFO|WARNING|WARN" -NotMatch
```



Xem data trong mongodb
```powershell
kubectl -n bigdata exec -it mongodb-0 -- mongosh

use weather
db.weather_batch.countDocuments()
db.weather_batch.find().limit(5)
```


## Xóa data trong spark-batch

```powershell
kubectl -n bigdata delete deploy/spark-batch
kubectl -n bigdata delete cm spark-batch-config
```
