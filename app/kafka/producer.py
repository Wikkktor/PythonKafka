from confluent_kafka import Producer
import json

from app.config import KAFKA_BROKER_URL


producer_config: dict[str, str] = {"bootstrap.servers": KAFKA_BROKER_URL}

producer: Producer = Producer(producer_config)


def delivery_report(err, msg) -> None:
    if err:
        print(f"[Kafka Producer] Delivery failed: {err}")
    else:
        print(
            f"[Kafka Producer] Message delivered to {msg.topic()} [{msg.partition()}]"
        )


def send_message(topic: str, value: dict) -> None:
    serialized = json.dumps(value).encode("utf-8")
    producer.produce(topic, value=serialized, callback=delivery_report)
    producer.flush()
