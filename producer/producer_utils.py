from dotenv import load_dotenv
from datetime import datetime
import socket
import json
import time as t
from confluent_kafka import Producer
import openmeteo_requests
import pandas as pd
import requests_cache
from retry_requests import retry
from consumer.utils import load_environment_variables

# Load biến môi trường
load_dotenv()
env_vars = load_environment_variables()

# Cấu hình Kafka
KAFKA_BROKERS = "localhost:9092,localhost:9093,localhost:9094"
conf = {
    'bootstrap.servers': KAFKA_BROKERS,
    'client.id': socket.gethostname(),
    'enable.idempotence': True,
}
producer = Producer(conf)


def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")


def send_to_kafka(producer, topic, key, message):
    producer.produce(
        topic,
        key=key,
        value=json.dumps(message, default=str).encode("utf-8"),
        callback=delivery_report
    )
    producer.flush()


def retrieve_weather_data(producer, kafka_topic, latitude, longitude):
    cache_session = requests_cache.CachedSession('.cache', expire_after=3600) #cache dữ liệu trong 1h
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2) #tự động thử lại khi có lỗi kết nối
    openmeteo = openmeteo_requests.Client(session=retry_session) #tạo client openmeteo với session đã cấu hình

    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": [
            "temperature_2m", "relative_humidity_2m", "rain",
            "wind_speed_10m", "wind_direction_10m", "visibility",
            "precipitation_probability", "apparent_temperature",
            "cloud_cover", "cloud_cover_low", "cloud_cover_mid", "cloud_cover_high"
        ],
        "timezone": "Asia/Bangkok"
    }

    while True:
        try:
            responses = openmeteo.weather_api(url, params=params)
            response = responses[0]
            hourly = response.Hourly()

            hourly_data = {
                "date": pd.date_range(
                    start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
                    end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
                    freq=pd.Timedelta(seconds=hourly.Interval()),
                    inclusive="left"
                ),
                "temperature_2m": hourly.Variables(0).ValuesAsNumpy(),
                "relative_humidity_2m": hourly.Variables(1).ValuesAsNumpy(),
                "rain": hourly.Variables(2).ValuesAsNumpy(),
                "wind_speed_10m": hourly.Variables(3).ValuesAsNumpy(),
                "wind_direction_10m": hourly.Variables(4).ValuesAsNumpy(),
                "visibility": hourly.Variables(5).ValuesAsNumpy(),
                "precipitation_probability": hourly.Variables(6).ValuesAsNumpy(),
                "apparent_temperature": hourly.Variables(7).ValuesAsNumpy(),
                "cloud_cover": hourly.Variables(8).ValuesAsNumpy(),
                "cloud_cover_low": hourly.Variables(9).ValuesAsNumpy(),
                "cloud_cover_mid": hourly.Variables(10).ValuesAsNumpy(),
                "cloud_cover_high": hourly.Variables(11).ValuesAsNumpy()
            }

            df = pd.DataFrame(data=hourly_data)

            for _, row in df.iterrows():
                record = {
                    "datetime": row["date"].isoformat(),
                    "temperature_2m": row["temperature_2m"],
                    "relative_humidity_2m": row["relative_humidity_2m"],
                    "rain": row["rain"],
                    "wind_speed_10m": row["wind_speed_10m"],
                    "wind_direction_10m": row["wind_direction_10m"],
                    "visibility": row["visibility"],
                    "precipitation_probability": row["precipitation_probability"],
                    "apparent_temperature": row["apparent_temperature"],
                    "cloud_cover": row["cloud_cover"],
                    "cloud_cover_low": row["cloud_cover_low"],
                    "cloud_cover_mid": row["cloud_cover_mid"],
                    "cloud_cover_high": row["cloud_cover_high"]
                }
                print(record)
                send_to_kafka(producer, kafka_topic, str(row["date"]), record)

            print(f"Sent {len(df)} records to topic '{kafka_topic}'")
        except Exception as e:
            print(f"Error while retrieving weather data: {e}")

        t.sleep(10)  # cập nhật dữ liệu mỗi 10s
