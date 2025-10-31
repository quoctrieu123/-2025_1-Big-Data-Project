from dotenv import load_dotenv
from datetime import datetime, timezone, timedelta
import socket
import json
import time as t
from confluent_kafka import Producer
import requests  
import os
from pathlib import Path
import sys
path = Path(__file__).parent.parent
sys.path.insert(0, str(path))
load_dotenv()

# Lấy API key từ biến môi trường
# OPENWEATHER_API_KEY = env_vars.get('OPENWEATHER_API_KEY')
# if not OPENWEATHER_API_KEY:
#     raise ValueError("OPENWEATHER_API_KEY not found in environment variables. Please add it to your .env file.")

OPENWEATHER_API_KEY = ""

KAFKA_BROKERS = os.environ.get("KAFKA_EXTERNAL_SERVERS")
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


def retrieve_weather_data(producer, kafka_topic, latitude, longitude, city_name):
    while True:
        try:
            url = f"https://api.openweathermap.org/data/2.5/weather?lat={latitude}&lon={longitude}&appid={OPENWEATHER_API_KEY}&units=metric"

            response = requests.get(url)
            response.raise_for_status() 
            weather_data = response.json()

            dt_timestamp = weather_data.get('dt')
            tz_offset_seconds = weather_data.get('timezone')

            if dt_timestamp is not None and tz_offset_seconds is not None:
                tz_info = timezone(timedelta(seconds=tz_offset_seconds))
                dt_object = datetime.fromtimestamp(dt_timestamp, tz=tz_info)
                iso_datetime = dt_object.isoformat()
            else:
                iso_datetime = datetime.now(timezone.utc).isoformat() 

            main_data = weather_data.get('main', {})
            wind_data = weather_data.get('wind', {})
            clouds_data = weather_data.get('clouds', {})
            sys_data = weather_data.get('sys', {})

            record = {
                "city_name": city_name,
                "datetime": iso_datetime,
                "temperature": main_data.get('temp'), 
                "humidity": main_data.get('humidity'),
                "wind_speed": wind_data.get('speed'),
                "cloud_cover": clouds_data.get('all'),
                "visibility": weather_data.get('visibility'),
                
                "air_pressure": main_data.get('pressure'),
                "air_pressure_sea_level": main_data.get('sea_level'),
                "air_pressure_ground_level": main_data.get('grnd_level'),

                "apparent_temperature": main_data.get('feels_like'),
                "wind_direction": wind_data.get('deg'),

                "rain": weather_data.get('rain', {}).get('1h', 0.0),
                'country': sys_data.get('country'),
            }
            message_key = str(dt_timestamp or datetime.now().timestamp())

            print(f"Sending transformed weather data: {record}")
            send_to_kafka(
                producer,
                kafka_topic,
                message_key,
                record 
            )

        except Exception as e:
            print(f"Error in thread for {city_name}: {e}")

        t.sleep(10)