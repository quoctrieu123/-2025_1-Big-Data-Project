# Instruction for MongoDb Statefulset

Create MongoDB statefulset
```bash
kubectl apply -f k8s/mongodb/statefulset.yaml
kubectl -n bigdata get pods -l app=mongodb
```
Expose port to access MongoDb UI from MongoDb Compass:
```bash
kubectl -n bigdata port-forward svc/mongodb 27017:27017
```


Delete PVC (if needed):
```bash
kubectl -n bigdata delete statefulset mongodb
kubectl -n bigdata delete pvc mongodb-data-mongodb-0
```

Check MongoDb log (if needed):
```bash
kubectl -n bigdata logs -f mongodb-0
```

Check MongoDb database (if needed):
```bash
kubectl -n bigdata exec -it mongodb-0 -- mongosh --eval "db.stats()"
```
