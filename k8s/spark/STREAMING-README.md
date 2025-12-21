# Instruction for Spark Streaming Deployment

Build docker image (for first run only):
```powershell
docker build -t spark-consumer:latest -f spark.Dockerfile .
```


Create ConfigMap and ConfigMap for Spark Streaming Deployment:
```powershell
kubectl apply -f k8s/spark/consumer-configmap.yaml
kubectl apply -f k8s/spark/consumer-secret.yaml
```

Create Spark Streaming Deployment:
```powershell
kubectl apply -f k8s/spark/consumer-deployment.yaml
kubectl -n bigdata get pods -l app=spark-streaming-consumer
```

Check logs (if needed):
```powershell
kubeclt -n bigdata logs -l app=spark-streaming-consumer
```

Delete Spark Streaming Component:
```powershell
kubectl -n bigdata delete deploy/spark-streaming-consumer
kubectl -n bigdata delete cm spark-consumer-config
kubectl -n bigdata delete secret spark-consumer-secrets
```
