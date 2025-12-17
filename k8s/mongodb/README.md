# MongoDB on Kubernetes

Deploy MongoDB 7.0 as a StatefulSet with persistent storage for batch processing results.

## 1. Prerequisites
- Namespace `bigdata` already created (`kubectl apply -f k8s/namespace.yaml`)
- Default StorageClass available (Docker Desktop / Minikube provides this)

## 2. Deploy MongoDB
```bash
kubectl apply -f k8s/mongodb/statefulset.yaml
kubectl -n bigdata get pods -l app=mongodb
```

Wait for the pod to reach `Running` status.

## 3. Access MongoDB
MongoDB is exposed as a ClusterIP service for internal access:
- **Internal DNS**: `mongodb.bigdata.svc.cluster.local:27017`
- **From Spark jobs**: Use connection string `mongodb://mongodb.bigdata.svc.cluster.local:27017`

For external access (debugging):
```bash
kubectl -n bigdata port-forward svc/mongodb 27017:27017
```

Then connect using:
```bash
mongosh mongodb://localhost:27017
```

## 4. Configuration
MongoDB runs without authentication by default (suitable for development). For production:
1. Add authentication by setting environment variables in the StatefulSet
2. Create a Secret with credentials
3. Update Spark batch job to use authenticated connection string

## 5. Persistence
Data is stored in a 5Gi PVC. To wipe data:
```bash
kubectl -n bigdata delete statefulset mongodb
kubectl -n bigdata delete pvc mongodb-data-mongodb-0
```

## 6. Integration with Spark
The Spark batch job (`write-to-mongodb.py`) connects to MongoDB using:
- URI: `mongodb://mongodb.bigdata.svc.cluster.local:27017`
- Database: Configured in the batch script
- Collection: Weather data aggregates

Update the batch job ConfigMap/environment if needed.

## 7. Monitoring
Check MongoDB logs:
```bash
kubectl -n bigdata logs -f mongodb-0
```

Check database status:
```bash
kubectl -n bigdata exec -it mongodb-0 -- mongosh --eval "db.stats()"
```
