This is the repository for the project under the course "Big Data" in the semester 2025.1.

## Docker Compose stack (legacy)
- Khởi chạy: `docker compose --env-file .env up --build --remove-orphans -d`
- Xem log consumer: `docker logs -f spark-submit-streaming`

## Kubernetes migration (Docker Desktop)
Các manifest/values nằm trong thư mục `k8s/`.

### Kafka trên Kubernetes
1. Bật Kubernetes trong Docker Desktop và đảm bảo đã tạo namespace `bigdata`.
2. Cài Helm chart Bitnami với file cấu hình `k8s/kafka/values.yaml`:
	```bash
	helm repo add bitnami https://charts.bitnami.com/bitnami
	helm repo update
	helm upgrade --install kafka bitnami/kafka -n bigdata -f k8s/kafka/values.yaml
	```
3. Cập nhật `.env` (đã cấu hình sẵn) để `producer.py` dùng các NodePort `localhost:30092,30093,30094`.
4. Chạy producer trên máy host:
	```bash
	pip install -r requirements.txt
	python producer/producer.py
	```
5. Xem thêm chi tiết tại `k8s/kafka/README.md` (kiểm tra pod, port-forward, troubleshooting).

### Các dịch vụ khác
- `k8s/namespace.yaml`: namespace `bigdata`.
- `k8s/influxdb/`: StatefulSet + Secret để chạy InfluxDB 2.x.
	1. Sửa token trong `k8s/influxdb/secret.yaml`.
	2. `kubectl apply -f k8s/influxdb/secret.yaml && kubectl apply -f k8s/influxdb/statefulset.yaml`.
	3. Port-forward UI: `kubectl -n bigdata port-forward svc/influxdb 8086:8086`.
- `k8s/grafana/`: Deployment + datasource ConfigMap trỏ tới InfluxDB.
	1. `kubectl apply -f k8s/grafana/secret.yaml` (đổi admin pass nếu cần).
	2. `kubectl apply -f k8s/grafana/configmap-datasource.yaml`.
	3. `kubectl apply -f k8s/grafana/deployment.yaml` và truy cập http://localhost:31300.
- `k8s/spark/`: Spark streaming consumer chạy `consumer.py` bên trong K8s.