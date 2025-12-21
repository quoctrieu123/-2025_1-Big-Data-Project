# Instruction for HDFS Statefulset and YARN Deployment

Create ConfigMap for YARN and HDFS:
```bash
kubectl apply -f k8s/hdfs/configmap.yaml
kubectl apply -f k8s/hdfs/hadoop-config-files.yaml
```

Create YARN Deployments and check logs (if needed):
```bash
kubectl apply -f k8s/hdfs/yarn-deployments.yaml
kubectl -n bigdata get pods -l app=yarn-resourcemanager
kubectl -n bigdata get pods -l app=yarn-nodemanager
```

Create YARN service:
```bash
kubectl apply -f k8s/hdfs/services.yaml
```

Create HDFS StatefulSets and check logs (if needed):
```bash
kubectl apply -f k8s/hdfs/hdfs-statefulsets.yaml
kubectl -n bigdata get pods -l app=hdfs-namenode
kubectl -n bigdata get pods -l app=hdfs-datanode
```


Expose port to access Namenode UI from localhost:
```bash
kubectl -n bigdata port-forward svc/hdfs-namenode 9870:9870
```



Some test command in HDFS:
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


Expose port to access YARN UI from localhost:
```bash
kubectl -n bigdata port-forward svc/yarn-resourcemanager 8088:8088
```

Delete PVC if needed:
```bash
kubectl -n bigdata delete statefulset hdfs-namenode hdfs-datanode
kubectl -n bigdata delete pvc namenode-data-hdfs-namenode-0
kubectl -n bigdata delete pvc datanode-data-hdfs-datanode-0
```



Check HDFS and YARN logs (if needed):
```bash
kubectl -n bigdata logs -f hdfs-namenode-0
kubectl -n bigdata logs -f hdfs-datanode-0
kubectl -n bigdata logs -f deployment/yarn-resourcemanager
kubectl -n bigdata logs -f deployment/yarn-nodemanager
```


Delete all components in HDFS and YARN:
```bash
kubectl -n bigdata delete -f k8s/hdfs/
```

