# Instruction for Spark Batch Deployment

Build docker image (for first run only):
```powershell
minikube docker-env | Invoke-Expression
docker build -t spark-consumer:latest -f spark.Dockerfile .
```

Create ConfigMap and create Spark Batch Deployment:
```powershell
kubectl apply -f k8s/spark/batch-configmap.yaml
kubectl apply -f k8s/spark/batch-deployment.yaml
```

Check logs (if needed):
```powershell
kubectl -n bigdata get pods -l app=spark-batch
kubectl -n bigdata logs deploy/spark-batch | Select-String -Pattern "INFO|WARNING|WARN" -NotMatch
```


Delete Spark Batch Component:
```powershell
kubectl -n bigdata delete deploy/spark-batch
kubectl -n bigdata delete cm spark-batch-config
```
