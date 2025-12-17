Tạo secret cho influxdb
```powershell
kubectl apply -f k8s/influxdb/secret.yaml
```

Deploy statefulset và check
```powershell
kubectl apply -f k8s/influxdb/statefulset.yaml
kubectl -n bigdata get pods -l app=influxdb
```
Port forward ra ngoài
```powershell
kubectl -n bigdata port-forward svc/influxdb 8086:8086
```


Xóa data trong pvc influxdb
```powershell
kubectl -n bigdata delete statefulset influxdb
kubectl -n bigdata delete pvc data-influxdb-0
```

