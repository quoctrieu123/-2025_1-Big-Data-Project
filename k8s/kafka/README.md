# Instruction for Kafka Statefulsets:

Step to run the pipeline from previous initialization:
```powershell
minikube start --memory 12288 --cpus 4 --driver=docker
minikube status
kubectl -n bigdata delete pods -l app.kubernetes.io/instance=kafka
kubectl -n bigdata get pods
kubectl -n bigdata port-forward svc/kafka-broker-0-external 30092:9094
kubectl -n bigdata port-forward svc/kafka-broker-1-external 30093:9094
kubectl -n bigdata port-forward svc/kafka-broker-2-external 30094:9094
python producer/producer.py
kubectl -n bigdata port-forward svc/kafdrop 30900:9000
```
Create namespace "Bigdata" (for first run only):
```powershell
kubectl apply -f k8s/namespace.yaml` 
```

Download Kafka Helm Chart and create Kafka components:
```bash
helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo update
helm upgrade --install kafka bitnami/kafka --namespace bigdata --create-namespace=false -f k8s/kafka/values.yaml
```


Check components' status (if needed):
```bash
kubectl -n bigdata get pods -l app.kubernetes.io/name=kafka
kubectl -n bigdata get service kafka kafka-broker-headless
```


Expose three brokers' ports for `producer.py` connection:
```bash
kubectl -n bigdata port-forward svc/kafka-broker-0-external 30092:9094
kubectl -n bigdata port-forward svc/kafka-broker-1-external 30093:9094
kubectl -n bigdata port-forward svc/kafka-broker-2-external 30094:9094
```

Create topic, partitions, assignments (for first run only):
```bash
kubectl -n bigdata exec kafka-broker-0 -- kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic weather-data

kubectl -n bigdata exec kafka-broker-0 -- kafka-topics.sh --bootstrap-server localhost:9092 --create --topic weather-data --replica-assignment 101:102:100,102:100:101,100:101:102
```

Run `producer.py` to fetch data into pipeline:
```bash
python producer/producer.py
```


Clone Kafdrop Chart (for first run only):
```bash
git clone https://github.com/obsidiandynamics/kafdrop.git packages/kafdrop

helm upgrade --install kafdrop packages/kafdrop/chart -n bigdata -f k8s/kafka/kafdrop-values.yaml
```

Expose port to access Kafdrop UI from localhost:
```bash
kubectl -n bigdata port-forward svc/kafdrop 30900:9000
```
Delete all kafka components:
```bash
kubectl -n bigdata delete pods -l app.kubernetes.io/instance=kafka
```