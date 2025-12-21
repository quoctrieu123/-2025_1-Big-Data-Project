Xóa job:
kubectl -n bigdata delete job spark-ml-training

Chạy job:
kubectl apply -f k8s/spark/ml-job.yaml

Xem log:
kubectl -n bigdata logs job/spark-ml-training | Select-String -Pattern "WARN WindowExec:|WARN InstanceBuilder" -NotMatch