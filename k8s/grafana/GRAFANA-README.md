Tạo secret và config cho grafana

```powershell
kubectl apply -f k8s/grafana/secret.yaml
kubectl apply -f k8s/grafana/configmap-datasource.yaml
```

Deploy pod grafana
```powershell
kubectl apply -f k8s/grafana/deployment.yaml
kubectl -n bigdata get pods -l app=grafana
```

Mở cổng ra ngoài
The Service is exposed via NodePort 31300:
```powershell
kubectl -n bigdata get svc grafana
```

Xóa PVC của grafana
```powershell
kubectl -n bigdata delete pvc grafana-pvc
```

