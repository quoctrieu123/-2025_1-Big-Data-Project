# Instruction for InfluxDb StatefulSet
Create StatefulSet for InfluxDb:
```powershell
kubectl apply -f k8s/influxdb/secret.yaml
```

Create InfluxDB StatefulSet and check logs (if needed):
```powershell
kubectl apply -f k8s/influxdb/statefulset.yaml
kubectl -n bigdata get pods -l app=influxdb
```
Expose port to access InfluxDb UI from localhost:
```powershell
kubectl -n bigdata port-forward svc/influxdb 8086:8086
```


Delete PVC if needed:
```powershell
kubectl -n bigdata delete statefulset influxdb
kubectl -n bigdata delete pvc data-influxdb-0
```

