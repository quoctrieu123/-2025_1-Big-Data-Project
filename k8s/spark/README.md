# Spark Streaming Consumer

## 1. Build/publish the Spark image
Cần build docker image từ spark.Dockerfile nếu chưa build

```powershell
# from the repo root
docker build -t spark-consumer:latest -f spark.Dockerfile .
```

## 2. Khởi tạo namespace nếu chưa có
```powershell
kubectl apply -f k8s/namespace.yaml
```

## 3. Create ConfigMap and Secret
Edit the files first if you need different endpoints (e.g., InfluxDB running outside the cluster).

```powershell
# IMPORTANT: edit k8s/spark/consumer-secret.yaml to replace the placeholder token
kubectl apply -f k8s/spark/consumer-configmap.yaml
kubectl apply -f k8s/spark/consumer-secret.yaml
```

## 4. Deploy the streaming job
```powershell
kubectl apply -f k8s/spark/consumer-deployment.yaml
kubectl -n bigdata get pods -l app=spark-streaming-consumer
```

## 5. Checking logs
```powershell
kubectl -n bigdata logs -f deploy/spark-streaming-consumer
```
Expect to see messages like `Connecting to Kafka brokers` followed by batch write logs for InfluxDB and optionally HDFS. K8s will restart the pod if the Spark process exits.

## 6. Configuration knobs
| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_INTERNAL_SERVERS` | `kafka-*.kafka-headless...` | Kafka bootstrap servers inside the cluster |
| `WEATHER_KAFKA_TOPIC` | `weather-data` | Topic to subscribe |
| `INFLUXDB_SERVER` | `http://influxdb.bigdata.svc.cluster.local:8086` | Change if InfluxDB runs elsewhere (e.g., `http://host.docker.internal:8086`) |
| `INFLUXDB_TOKEN` | from Secret | Must match your InfluxDB API token |
| `HDFS_OUTPUT_PATH` | `hdfs://hdfs-namenode.bigdata.svc.cluster.local:8020/weather-data` | HDFS path for writing Parquet files |

## 7. Cleanup
```powershell
kubectl -n bigdata delete deploy/spark-streaming-consumer
kubectl -n bigdata delete cm spark-consumer-config
kubectl -n bigdata delete secret spark-consumer-secrets
```

## 8. Next steps
- Switch `--master` to `k8s://...` and add executor pods once you deploy Spark Operator or the official scripts.
- Add HorizontalPodAutoscaler around Kafdrop/Kafka for bursty workloads.
- Deploy InfluxDB/Grafana inside K8s and update the ConfigMap accordingly.
