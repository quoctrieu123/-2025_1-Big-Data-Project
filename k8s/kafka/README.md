Tạo namesapce
minikube start --memory 12288 --cpus 4 --driver=docker
minikube addons enable metrics-server
- Chạy `kubectl apply -f k8s/namespace.yaml` để tạo namespace `bigdata`.


Tải helm chart kafka nếu chưa có
```bash
helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo update
helm upgrade --install kafka bitnami/kafka --namespace bigdata --create-namespace=false -f k8s/kafka/values.yaml
```


Check pod và service
```bash
kubectl -n bigdata get pods -l app.kubernetes.io/name=kafka
kubectl -n bigdata get service kafka kafka-broker-headless
```


Port forward  ra ngoài để producer.py kết nối
```bash
kubectl -n bigdata port-forward svc/kafka-broker-0-external 30092:9094
kubectl -n bigdata port-forward svc/kafka-broker-1-external 30093:9094
kubectl -n bigdata port-forward svc/kafka-broker-2-external 30094:9094
```

Tạo và phân partition, xóa topic cũ nếu đã tồn tại
```bash
kubectl -n bigdata exec kafka-broker-0 -- kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic weather-data

kubectl -n bigdata exec kafka-broker-0 -- kafka-topics.sh --bootstrap-server localhost:9092 --create --topic weather-data --replica-assignment 101:102:100,102:100:101,100:101:102

```

```bash
python producer/producer.py
```


Do kafdrop không có trong bitnami chart => cần clone thủ công

```bash
# clone kafdrop chart vào thư mục packes/kafdrop
git clone https://github.com/obsidiandynamics/kafdrop.git packages/kafdrop
helm upgrade --install kafdrop packages/kafdrop/chart -n bigdata -f k8s/kafka/kafdrop-values.yaml
```

Truy cập localhost qua port forward
```bash
kubectl -n bigdata port-forward svc/kafdrop 30900:9000
```


