# InfluxDB on Kubernetes (Docker Desktop)

deploy InfluxDB 2.x as a single-node StatefulSet with persistent storage.

## 1. Prepare secrets
Edit `k8s/influxdb/secret.yaml` to set `DOCKER_INFLUXDB_INIT_ADMIN_TOKEN` (and optionally username/password/org/bucket). Then apply:
```powershell
kubectl apply -f k8s/influxdb/secret.yaml
```

## 2. Deploy InfluxDB
```powershell
kubectl apply -f k8s/influxdb/statefulset.yaml
kubectl -n bigdata get pods -l app=influxdb
```
Wait for the pod to reach `Running`.

## 3. Access the UI / API
- Port-forward:
  ```powershell
  kubectl -n bigdata port-forward svc/influxdb 8086:8086
  ```
  Then open http://localhost:8086 and log in with the credentials from the secret.
- Or expose via NodePort/Ingress if needed (not included by default).

## 4. Buckets & tokens
The setup mode creates the org/bucket/token defined in the secret. If you change them later, remember to update:
- Spark consumer ConfigMap / Secret
- Grafana datasource configuration

## 5. Persistence
The StatefulSet requests a 5 Gi PVC. On Docker Desktop this binds to the default storage class. To wipe data completely:
```powershell
kubectl -n bigdata delete statefulset influxdb
kubectl -n bigdata delete pvc data-influxdb-0
```

## 6. Next steps
Deploy Grafana (`k8s/grafana/`) and point the datasource to `http://influxdb.bigdata.svc.cluster.local:8086`.
