Tạo HDFS và YARN trên Kubernetes


Deploy HDFS statefulset và yarn deployment 

Tạo configmap:
```bash
kubectl apply -f k8s/hdfs/configmap.yaml
```

Deploy YARN:
```bash
kubectl apply -f k8s/hdfs/yarn-deployments.yaml
kubectl -n bigdata get pods -l app=yarn-resourcemanager
kubectl -n bigdata get pods -l app=yarn-nodemanager
```

Deploy Service của yarn
```bash
kubectl apply -f k8s/hdfs/services.yaml
```

Deploy HDFS:
```bash
kubectl apply -f k8s/hdfs/hdfs-statefulsets.yaml
kubectl -n bigdata get pods -l app=hdfs-namenode
kubectl -n bigdata get pods -l app=hdfs-datanode
```


Port forward để xem
```bash
kubectl -n bigdata port-forward svc/hdfs-namenode 9870:9870
```
Open http://localhost:9870 to see HDFS status and DataNode connections.


Test thao tác các file trong hdfs
```bash
# Create a test directory
kubectl -n bigdata exec -it hdfs-namenode-0 -- hdfs dfs -mkdir -p /test

# List directories
kubectl -n bigdata exec -it hdfs-namenode-0 -- hdfs dfs -ls /

# Upload a file
kubectl -n bigdata exec -it hdfs-namenode-0 -- hdfs dfs -put /opt/hadoop/README.txt /test/

# Check cluster status
kubectl -n bigdata exec -it hdfs-namenode-0 -- hdfs dfsadmin -report
```


Check YARN ResourceManager UI:
```bash
kubectl -n bigdata port-forward svc/yarn-resourcemanager 8088:8088
```

Xóa hết dữ liệu trong pvc
```bash
kubectl -n bigdata delete statefulset hdfs-namenode hdfs-datanode
kubectl -n bigdata delete pvc namenode-data-hdfs-namenode-0
kubectl -n bigdata delete pvc datanode-data-hdfs-datanode-0
```



Xem logs:
```bash

kubectl -n bigdata logs -f hdfs-namenode-0


kubectl -n bigdata logs -f hdfs-datanode-0


kubectl -n bigdata logs -f deployment/yarn-resourcemanager


kubectl -n bigdata logs -f deployment/yarn-nodemanager
```


Xóa các deployment hdfs/yarn
```bash
kubectl -n bigdata delete -f k8s/hdfs/
```

