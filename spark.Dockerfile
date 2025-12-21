FROM apache/spark:3.5.1
USER root
RUN apt-get update && \
    apt-get install -y python3-dev gcc build-essential \
    && apt-get clean
RUN pip install --upgrade pip setuptools wheel
WORKDIR /app

COPY requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt


COPY consumer /app/consumer

ENV PYTHONPATH=/app
