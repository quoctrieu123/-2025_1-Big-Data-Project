# HDFS & YARN on Kubernetes

Deploy Hadoop HDFS and YARN cluster on Kubernetes for distributed storage and batch processing.

## Architecture
- **HDFS NameNode**: Manages file system metadata (StatefulSet with persistent storage)
- **HDFS DataNode**: Stores actual data blocks (StatefulSet with persistent storage)
- **YARN ResourceManager**: Manages cluster resources and job scheduling
- **YARN NodeManager**: Executes containers on worker nodes

## 1. Prerequisites
- Namespace `bigdata` already created
- Default StorageClass available
- Sufficient resources (recommended: 4 CPU cores, 8GB RAM for full stack)

## 2. Deploy HDFS & YARN

### Step 1: Apply ConfigMap
```bash
kubectl apply -f k8s/hdfs/configmap.yaml
```

### Step 2: Deploy YARN components (must be first)
```bash
kubectl apply -f k8s/hdfs/yarn-deployments.yaml
kubectl -n bigdata get pods -l app=yarn-resourcemanager
kubectl -n bigdata get pods -l app=yarn-nodemanager
```

Wait for YARN pods to be Running.

### Step 3: Deploy Services
```bash
kubectl apply -f k8s/hdfs/services.yaml
```

### Step 4: Deploy HDFS
```bash
kubectl apply -f k8s/hdfs/hdfs-statefulsets.yaml
kubectl -n bigdata get pods -l app=hdfs-namenode
kubectl -n bigdata get pods -l app=hdfs-datanode
```

Wait for all pods to reach `Running` status. NameNode may take 1-2 minutes to format on first startup.

## 3. Verify HDFS Cluster

Check NameNode web UI (requires port-forward):
```bash
kubectl -n bigdata port-forward svc/hdfs-namenode 9870:9870
```
Open http://localhost:9870 to see HDFS status and DataNode connections.

Test HDFS from command line:
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

## 4. Verify YARN Cluster

Check YARN ResourceManager UI:
```bash
kubectl -n bigdata port-forward svc/yarn-resourcemanager 8088:8088
```
Open http://localhost:8088 to see YARN cluster metrics and applications.

Check NodeManager status:
```bash
kubectl -n bigdata exec -it yarn-resourcemanager-<pod-id> -- yarn node -list
```

## 5. Integration with Spark

Spark jobs can now write to HDFS using:
- **URI**: `hdfs://hdfs-namenode:8020/path/to/data`
- **From Spark streaming consumer**: Already configured in ConfigMap

Update Spark consumer ConfigMap if needed:
```yaml
HDFS_OUTPUT_PATH: "hdfs://hdfs-namenode:8020/weather-data"
```

## 6. Scaling

Scale DataNodes for more storage:
```bash
kubectl -n bigdata scale statefulset hdfs-datanode --replicas=2
```

Scale NodeManagers for more compute:
```bash
kubectl -n bigdata scale deployment yarn-nodemanager --replicas=2
```

## 7. Persistence

Data is stored in PVCs:
- NameNode metadata: `namenode-data-hdfs-namenode-0` (5Gi)
- DataNode storage: `datanode-data-hdfs-datanode-0` (10Gi per replica)

To wipe all HDFS data:
```bash
kubectl -n bigdata delete statefulset hdfs-namenode hdfs-datanode
kubectl -n bigdata delete pvc namenode-data-hdfs-namenode-0
kubectl -n bigdata delete pvc datanode-data-hdfs-datanode-0
```

## 8. Troubleshooting

| Issue | Solution |
|-------|----------|
| NameNode stuck in SafeMode | Wait 30s or force leave: `kubectl -n bigdata exec hdfs-namenode-0 -- hdfs dfsadmin -safemode leave` |
| DataNode not connecting | Check logs: `kubectl -n bigdata logs hdfs-datanode-0`. Ensure NameNode is fully started first. |
| YARN jobs failing | Check ResourceManager logs: `kubectl -n bigdata logs -l app=yarn-resourcemanager` |
| Permission denied in HDFS | HDFS permissions are disabled in config. Check `dfs.permissions=false` in ConfigMap. |

## 9. Monitoring

View logs:
```bash
# NameNode logs
kubectl -n bigdata logs -f hdfs-namenode-0

# DataNode logs
kubectl -n bigdata logs -f hdfs-datanode-0

# ResourceManager logs
kubectl -n bigdata logs -f deployment/yarn-resourcemanager

# NodeManager logs
kubectl -n bigdata logs -f deployment/yarn-nodemanager
```

## 10. Cleanup

Remove all HDFS/YARN components:
```bash
kubectl -n bigdata delete -f k8s/hdfs/
```

This will remove deployments, services, and StatefulSets but **keep PVCs** for data safety. Delete PVCs manually if needed.
