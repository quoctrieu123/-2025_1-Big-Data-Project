Deploy mongo


Chạy statefulsetset và check
```bash
kubectl apply -f k8s/mongodb/statefulset.yaml
kubectl -n bigdata get pods -l app=mongodb
```



Mở qua localhost
```bash
kubectl -n bigdata port-forward svc/mongodb 27017:27017
```

Kết nối mongo shell
```bash
mongosh mongodb://localhost:27017
```

Xóa data trong pvc mongodb
```bash
kubectl -n bigdata delete statefulset mongodb
kubectl -n bigdata delete pvc mongodb-data-mongodb-0
```




Check MongoDB log
```bash
kubectl -n bigdata logs -f mongodb-0
```

Check database
```bash
kubectl -n bigdata exec -it mongodb-0 -- mongosh --eval "db.stats()"
```
