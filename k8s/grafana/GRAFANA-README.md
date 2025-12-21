# Instruction for Grafna Deployment

Create Secret và ConfigMap cho grafana:
```powershell
kubectl apply -f k8s/grafana/secret.yaml
kubectl apply -f k8s/grafana/configmap-datasource.yaml
```

Create Grafana Deployment and check logs (if needed):
```powershell
kubectl apply -f k8s/grafana/deployment.yaml
kubectl -n bigdata get pods -l app=grafana
```

Expose port to access from localhost:
```powershell
kubectl -n bigdata port-forward svc/grafana  31300:3000    
```

Delete PVC if needed
```powershell
kubectl -n bigdata delete pvc grafana-pvc
```

