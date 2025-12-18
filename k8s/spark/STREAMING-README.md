# Spark Streaming Consumer

Cần build docker image từ spark.Dockerfile nếu chưa build

```powershell
docker build -t spark-consumer:latest -f spark.Dockerfile .
```


Tạo configmap và secret cho spark streaming consumer
```powershell
# IMPORTANT: edit k8s/spark/consumer-secret.yaml to replace the placeholder token
kubectl apply -f k8s/spark/consumer-configmap.yaml
kubectl apply -f k8s/spark/consumer-secret.yaml
```

Deploy streaming pods
```powershell
kubectl apply -f k8s/spark/consumer-deployment.yaml
kubectl -n bigdata get pods -l app=spark-streaming-consumer
```

Check logs nếu cần
```powershell
f
```

Xóa đi nếu cần
```powershell
kubectl -n bigdata delete deploy/spark-streaming-consumer
kubectl -n bigdata delete cm spark-consumer-config
kubectl -n bigdata delete secret spark-consumer-secrets
```
