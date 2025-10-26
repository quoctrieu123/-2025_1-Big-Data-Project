import sys, socket
from pathlib import Path

# Đảm bảo có thể import từ thư mục cha
path_to_utils = Path(__file__).parent.parent
sys.path.insert(0, str(path_to_utils))

from dotenv import load_dotenv
from script.utils import load_environment_variables
from confluent_kafka import Producer
from producer_utils import retrieve_weather_data

load_dotenv()
env_vars = load_environment_variables()

KAFKA_BROKERS = "localhost:9092,localhost:9093,localhost:9094"
conf = {
    'bootstrap.servers': KAFKA_BROKERS,
    'client.id': socket.gethostname(),
    'enable.idempotence': True,
}
producer = Producer(conf)

if __name__ == "__main__":
    # Lấy dữ liệu từ file .env
    latitude = float(env_vars.get("LATITUDE", 21.033333))
    longitude = float(env_vars.get("LONGITUDE", 105.849998))
    kafka_topic = env_vars.get("WEATHER_KAFKA_TOPIC", "weather-data")

    retrieve_weather_data(producer, kafka_topic, latitude, longitude)
