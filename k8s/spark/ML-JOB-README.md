# Instruction for ML-Job job:
Delete job (if needed):
```bash
kubectl -n bigdata delete job spark-ml-training
```

Run job:
```bash
kubectl apply -f k8s/spark/ml-job.yaml
```

Check logs (if needed):
```bash
kubectl -n bigdata logs job/spark-ml-training | Select-String -Pattern "WARN WindowExec:|WARN InstanceBuilder" -NotMatch
```