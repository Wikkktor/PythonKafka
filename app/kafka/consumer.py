from confluent_kafka import Consumer, KafkaError
import json

from app.config import KAFKA_BROKER_URL, KAFKA_GROUP_ID, KAFKA_TOPIC


def start_consumer():
    consumer_config: dict[str, str] = {
        "bootstrap.servers": KAFKA_BROKER_URL,
        "group.id": KAFKA_GROUP_ID,
        "auto.offset.reset": "earliest",
    }

    consumer: Consumer = Consumer(consumer_config)
    consumer.subscribe([KAFKA_TOPIC])
    print("[Kafka Consumer] Listening for messages...")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print(f"[Kafka Consumer] Error: {msg.error()}")
            else:
                data = json.loads(msg.value().decode("utf-8"))
                print(f"[Kafka Consumer] Received message: {data}")
    except KeyboardInterrupt:
        print("\n[Kafka Consumer] Shutting down...")
    finally:
        consumer.close()


if __name__ == "__main__":
    start_consumer()
