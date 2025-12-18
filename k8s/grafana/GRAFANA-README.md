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
kubectl -n bigdata port-forward svc/grafana  31300:3000    
```

Xóa PVC của grafana
```powershell
kubectl -n bigdata delete pvc grafana-pvc
```

