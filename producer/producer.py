import socket
import os
from dotenv import load_dotenv
from confluent_kafka import Producer
from producer_utils import retrieve_weather_data

load_dotenv()

KAFKA_BROKERS = os.environ.get("KAFKA_EXTERNAL_SERVERS")
conf = {
    'bootstrap.servers': KAFKA_BROKERS,
    'client.id': socket.gethostname(),
    'enable.idempotence': True,
}
producer = Producer(conf)

if __name__ == "__main__":
    # Lấy dữ liệu từ file .env
    latitude = float(os.environ.get("LATITUDE"))
    longitude = float(os.environ.get("LONGITUDE"))
    kafka_topic = os.environ.get("WEATHER_KAFKA_TOPIC", "weather-data")

    retrieve_weather_data(producer, kafka_topic, latitude, longitude)
