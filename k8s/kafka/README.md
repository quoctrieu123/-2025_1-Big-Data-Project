# Kafka on Kubernetes (Docker Desktop)

These steps deploy a 1-broker Kafka cluster (KRaft mode with dedicated controller) on the existing `bigdata` namespace using the Bitnami Helm chart. External access is exposed via NodePort 30092 so the local `producer.py` can publish events.

## 1. Prerequisites
- Docker Desktop with Kubernetes enabled.
- Namespace `bigdata` already created (`kubectl apply -f k8s/namespace.yaml`).
- Helm v3 installed.

## 2. Install / Upgrade Kafka
```bash
helm repo add bitnami https://charts.bitnami.com/bitnami
helm repo update
helm upgrade --install kafka bitnami/kafka \
  --namespace bigdata \
  --create-namespace=false \
  -f k8s/kafka/values.yaml
```

> Chart 32.x (KRaft) is used; controller external access is disabled while broker NodePort remains available for local access.
>
> Since Aug 2025 Bitnami only mirrors the newer tags on Docker Hub for paying customers, so the `values.yaml` sets `global.imageRegistry=public.ecr.aws` to pull `public.ecr.aws/bitnami/*` images instead and enables `global.security.allowInsecureImages=true` to bypass the chart’s verification warning.
>
> All listeners are configured as `PLAINTEXT`, so no SASL credentials are required when connecting from `producer.py` or other clients.

## 3. Check cluster status
```bash
kubectl -n bigdata get pods -l app.kubernetes.io/name=kafka
kubectl -n bigdata get svc kafka,kafka-broker-headless
```
Wait until the broker reports `Running`.

## 4. Enable External Access (Minikube/Docker Desktop)
For Minikube or Docker Desktop Kubernetes, NodePort services are not automatically accessible on localhost. You need to set up port forwarding:

```bash
# Port-forward the Kafka broker service to localhost
kubectl -n bigdata port-forward svc/kafka-broker-0-external 30092:9094
```

Keep this terminal running while using the producer. Alternatively, run it in the background or use a separate terminal.

## 5. Update `.env` for producer
Set `KAFKA_EXTERNAL_SERVERS="127.0.0.1:30092"`. The Python producer will read this list and connect through the port-forward.

## 6. Run the producer
Use the existing virtualenv or pip env:
```bash
pip install -r requirements.txt  # if not already installed
python producer/producer.py
```
Expected log: each city thread starts and records are published. Verify topic via Kafdrop or kafka-console-consumer (see section 7).

## 7. Kafdrop Deployment
Since the official Helm repository is no longer hosted, we install from the source:

```bash
# Clone the repository (if not already done)
git clone https://github.com/obsidiandynamics/kafdrop.git packages/kafdrop

# Install using the local chart
helm upgrade --install kafdrop packages/kafdrop/chart \
  -n bigdata \
  -f k8s/kafka/kafdrop-values.yaml
```

### Access Kafdrop UI (Minikube/Docker Desktop)
Like Kafka, Kafdrop's NodePort requires port-forwarding on Minikube/Docker Desktop:

```bash
# Port-forward Kafdrop service to localhost
kubectl -n bigdata port-forward svc/kafdrop 30900:9000
```

Keep this terminal running, then open your browser to:
**http://localhost:30900**

You should see the Kafdrop UI with the `weather-data` topic and messages from the producer. The broker connect string in Kafdrop is configured as `kafka-broker-0.kafka-broker-headless.bigdata.svc.cluster.local:9092` (internal cluster DNS).

## 8. Scaling & cleanup
- Scale brokers: `kubectl -n bigdata scale statefulset kafka-broker --replicas=2` (update nodePorts accordingly).
- Uninstall: `helm -n bigdata uninstall kafka` (PVCs remain; delete manually if needed).

## 9. Troubleshooting
| Symptom | Fix |
|---------|-----|
| Producer stuck on `Connection refused` | Ensure port-forward is running: `kubectl -n bigdata port-forward svc/kafka-broker-0-external 30092:9094`. Check pods are Running with `kubectl -n bigdata get pods`. |
| Cannot access Kafdrop on localhost:30900 | Ensure port-forward is running: `kubectl -n bigdata port-forward svc/kafdrop 30900:9000`. Keep the terminal open. |
| Broker CrashLoop due to storage | Docker Desktop sometimes needs more resources; increase disk size or disable persistence in values file. |
| Topic not created | `auto.create.topics.enable` is true; otherwise run `kubectl -n bigdata exec -it kafka-0 -- kafka-topics.sh --create ...`. |
| Kafdrop cannot reach brokers | Confirm `KAFKA_BROKERCONNECT` in `kafdrop-values.yaml` matches the headless service hostnames. If Kafdrop shows "Unable to retrieve brokers", check that Kafka pods are Running. |
| Pods stuck in `Init:ImagePullBackOff` | Ensure you’re on chart ≥32 with `global.imageRegistry=public.ecr.aws` (Docker Hub tags now require a Bitnami subscription). |
| Helm errors about “unrecognized containers” | Keep `global.security.allowInsecureImages=true` when overriding the registry to Bitnami’s ECR mirror. |

Next steps: deploy Kafka consumers (Spark streaming) via Spark-on-Kubernetes or connect existing services through ClusterIP within the namespace.
