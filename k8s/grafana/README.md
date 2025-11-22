# Grafana on Kubernetes (Docker Desktop)

Provisioned Grafana (OSS) with InfluxDB datasource auto-configured via ConfigMap.

## 1. Apply secrets & config
- Update `k8s/influxdb/secret.yaml` first so the token is correct (Grafana reads it indirectly).
- Apply Grafana-specific manifests:
```powershell
kubectl apply -f k8s/grafana/secret.yaml
kubectl apply -f k8s/grafana/configmap-datasource.yaml
```

## 2. Deploy Grafana
```powershell
kubectl apply -f k8s/grafana/deployment.yaml
kubectl -n bigdata get pods -l app=grafana
```

## 3. Access the UI
The Service is exposed via NodePort 31300:
```powershell
kubectl -n bigdata get svc grafana
# then open http://localhost:31300
```
Default admin credentials are stored in `grafana-secrets` (admin/admin123 by default—change before production). The initial datasource should already point to InfluxDB using the token from `influxdb-secrets`.

## 4. Persisted storage
The PVC `grafana-pvc` stores dashboards and settings. Delete it only if you want a clean state:
```powershell
kubectl -n bigdata delete pvc grafana-pvc
```

## 5. Troubleshooting
| Symptom | Fix |
|---------|-----|
| Grafana pod CrashLoop `datasource.yml: permission denied` | Ensure ConfigMap mount path remains `/etc/grafana/provisioning/datasources`. |
| Datasource error `invalid token` | Verify `DOCKER_INFLUXDB_INIT_ADMIN_TOKEN` in `influxdb-secrets` matches the token Grafana reads. Re-apply both secrets if changed. |
| Can’t reach UI | Confirm NodePort 31300 not used elsewhere, or port-forward: `kubectl -n bigdata port-forward svc/grafana 3000:3000`. |
