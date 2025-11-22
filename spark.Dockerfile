# Sử dụng image 3.5.1
FROM apache/spark:3.5.1

# Chuyển sang user root để cài đặt
USER root

# === PHẦN 1: Cài đặt các gói C cơ bản ===
# Cần thiết cho các thư viện Python *khác* có thể cần build
RUN apt-get update && \
    apt-get install -y python3-dev gcc build-essential \
    && apt-get clean

# === PHẦN 2: CÀI ĐẶT PYTHON (ĐÃ SỬA LỖI) ===

# BƯỚC QUAN TRỌNG: Nâng cấp pip, setuptools, và wheel
# Giúp pip tải "wheel" (binary) của confluent-kafka
# thay vì cố gắng build nó (tránh mọi lỗi về libssl và apt).
RUN pip install --upgrade pip setuptools wheel

# Bây giờ, cài đặt requirements.txt như bình thường
WORKDIR /app

COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy streaming/batch source code so the image is self-contained for Kubernetes
COPY consumer /app/consumer
COPY batch /app/batch

ENV PYTHONPATH=/app