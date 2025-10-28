#file dùng để check dữ liệu trong kafka topic
import sys
from confluent_kafka import Consumer, KafkaException
from consumer.utils import load_environment_variables
from dotenv import load_dotenv
load_dotenv()
env_vars = load_environment_variables()
KAFKA_BROKERS = "localhost:9092,localhost:9093,localhost:9094"
KAFKA_TOPIC = env_vars.get("WEATHER_KAFKA_TOPIC", "weather-data")
conf = {
    'bootstrap.servers': KAFKA_BROKERS,
    'group.id': 'weather-data-consumer-group-1',
    'auto.offset.reset': 'earliest',
}
consumer = Consumer(conf)
def consume_messages(consumer, topic):
    try:
        consumer.subscribe([topic])
        print(f"Subscribed to topic: {topic}")
        while True:
            msg = consumer.poll(1.0)  # timeout of 1 second
            if msg is None:
                continue
            if msg.error():
                raise KafkaException(msg.error())
            print(f"Received message: {msg.value().decode('utf-8')}")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

if __name__ == "__main__":
    consume_messages(consumer, KAFKA_TOPIC)