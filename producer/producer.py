import sys, socket
from pathlib import Path
import threading
import os
# Đảm bảo có thể import từ thư mục cha
path_to_utils = Path(__file__).parent.parent
sys.path.insert(0, str(path_to_utils))

from dotenv import load_dotenv
from confluent_kafka import Producer
from producer_utils_v2 import retrieve_weather_data

load_dotenv()

KAFKA_BROKERS = os.environ.get("KAFKA_EXTERNAL_SERVERS")
conf = {
    'bootstrap.servers': KAFKA_BROKERS,
    'client.id': socket.gethostname(),
    'enable.idempotence': True,
}
producer = Producer(conf)

if __name__ == "__main__":
    kafka_topic = os.environ.get("WEATHER_KAFKA_TOPIC", "weather-data")
    cities = [
        {"name": "Hanoi", "lat": 21.033333, "lon": 105.849998},
        {"name": "Bangkok", "lat": 13.44, "lon": 100.30},
        {"name": "Phnom Penh", "lat": 11.562108, "lon": 104.888535},
        {"name": "Da Nang", "lat": 16.047199, "lon": 108.219955},
        {"name": "Ho Chi Minh City", "lat": 10.7683, "lon": 106.6758}
    ]
    
    threads = []
    for city in cities:
        print(f"Starting producer thread for {city['name']}...")

        thread = threading.Thread(target=retrieve_weather_data, 
                                  args=(producer, kafka_topic, city['lat'], city['lon'], city['name']),
                                  daemon=True)
        threads.append(thread)
        thread.start()

    print(f"All {len(threads)} producer threads are running.")
    try:
        while True:
            threading.Event().wait(1)
    except KeyboardInterrupt:
        print("\nShutting down producers...")