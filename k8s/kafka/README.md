# Kafka on Kubernetes (Docker Desktop)
Bước deploy kafka cluster trên k8s

## 1. Yêu cầu
- Docker Desktop with Kubernetes enabled.
- Cài helm 3 giúp tải và release chart.
- Chạy `kubectl apply -f k8s/namespace.yaml` để tạo namespace `bigdata`.


## 2. Tải chart và release Kafka
```bash
helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo update
helm upgrade --install kafka bitnami/kafka --namespace bigdata --create-namespace=false -f k8s/kafka/values.yaml
```


## 3. Kiểm tra trạng thái release
```bash
kubectl -n bigdata get pods -l app.kubernetes.io/name=kafka
kubectl -n bigdata get service kafka kafka-broker-headless
```
Wait until the broker reports `Running`.

## 4. Enable External Access (Minikube/Docker Desktop)
Port forward kafkabroker ra ngoài vì Minikube/Docker Desktop không hỗ trợ NodePort trực tiếp.

```bash
# Port-forward the Kafka broker service to localhost
kubectl -n bigdata port-forward svc/kafka-broker-0-external 30092:9094
kubectl -n bigdata port-forward svc/kafka-broker-1-external 30093:9094
kubectl -n bigdata port-forward svc/kafka-broker-2-external 30094:9094
```

## 5. Tạo Topic 

```bash
# xóa topic cũ
kubectl -n bigdata exec kafka-broker-0 -- kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic weather-data

# Chia parition và replica node 
kubectl -n bigdata exec kafka-broker-0 -- kafka-topics.sh --bootstrap-server localhost:9092 --create --topic weather-data --replica-assignment 101:102:100,102:100:101,100:101:102

```

## 6. Chạy producer
```bash
pip install -r requirements.txt  
python producer/producer.py
```


## 7. Kafdrop Deployment
Do kafdrop không có trong bitnami chart => cần clone thủ công

```bash
# clone kafdrop chart vào thư mục packes/kafdrop
git clone https://github.com/obsidiandynamics/kafdrop.git packages/kafdrop

# deploy chart kafdrop
helm upgrade --install kafdrop packages/kafdrop/chart -n bigdata -f k8s/kafka/kafdrop-values.yaml
```

### Bật Kafdrop
```bash
# Port-forward Kafdrop service to localhost
kubectl -n bigdata port-forward svc/kafdrop 30900:9000
```


## Cách scale hệ thống (tăng kafka broker)
- Scale brokers: `kubectl -n bigdata scale statefulset kafka-broker --replicas=2` (update nodePorts accordingly).
- Xóa: `helm -n bigdata uninstall kafka` 

## 9. Một số lỗi có thể gặp
| Symptom | Fix |
|---------|-----|
| Producer stuck on `Connection refused` | Ensure port-forward is running: `kubectl -n bigdata port-forward svc/kafka-broker-0-external 30092:9094`. Check pods are Running with `kubectl -n bigdata get pods`. |
| Cannot access Kafdrop on localhost:30900 | Ensure port-forward is running: `kubectl -n bigdata port-forward svc/kafdrop 30900:9000`. Keep the terminal open. |
| Broker CrashLoop due to storage | Docker Desktop sometimes needs more resources; increase disk size or disable persistence in values file. |
| Topic not created | `auto.create.topics.enable` is true; otherwise run `kubectl -n bigdata exec -it kafka-0 -- kafka-topics.sh --create ...`. |
| Kafdrop cannot reach brokers | Confirm `KAFKA_BROKERCONNECT` in `kafdrop-values.yaml` matches the headless service hostnames. If Kafdrop shows "Unable to retrieve brokers", check that Kafka pods are Running. |
| Pods stuck in `Init:ImagePullBackOff` | Ensure you’re on chart ≥32 with `global.imageRegistry=public.ecr.aws` (Docker Hub tags now require a Bitnami subscription). |
| Helm errors about “unrecognized containers” | Keep `global.security.allowInsecureImages=true` when overriding the registry to Bitnami’s ECR mirror. |
