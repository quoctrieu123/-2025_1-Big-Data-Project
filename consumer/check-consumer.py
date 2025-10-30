from confluent_kafka import Consumer, KafkaException
from dotenv import load_dotenv
import os
load_dotenv()
KAFKA_BROKERS = os.environ.get("KAFKA_EXTERNAL_SERVERS")
KAFKA_TOPIC = os.environ.get("WEATHER_KAFKA_TOPIC", "weather-data")
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
            msg = consumer.poll(1.0) 
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