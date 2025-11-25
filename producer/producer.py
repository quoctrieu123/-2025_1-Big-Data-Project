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
        {"name": "Ho Chi Minh City", "lat": 10.7683, "lon": 106.6758},
        
        {"name": "London", "lat": 51.5074, "lon": -0.1278},
        {"name": "Paris", "lat": 48.8566, "lon": 2.3522},
        {"name": "Berlin", "lat": 52.5200, "lon": 13.4050},
        {"name": "Madrid", "lat": 40.4168, "lon": -3.7038},
        {"name": "Rome", "lat": 41.9028, "lon": 12.4964},

        {"name": "New York", "lat": 40.7128, "lon": -74.0060},
        {"name": "Los Angeles", "lat": 34.0522, "lon": -118.2437},
        {"name": "Toronto", "lat": 43.651070, "lon": -79.347015},
        {"name": "Mexico City", "lat": 19.4326, "lon": -99.1332},
        {"name": "Buenos Aires", "lat": -34.6037, "lon": -58.3816},

        {"name": "Cairo", "lat": 30.0444, "lon": 31.2357},
        {"name": "Nairobi", "lat": -1.2921, "lon": 36.8219},
        {"name": "Lagos", "lat": 6.5244, "lon": 3.3792},
        {"name": "Johannesburg", "lat": -26.2041, "lon": 28.0473},
        {"name": "Casablanca", "lat": 33.5731, "lon": -7.5898}
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