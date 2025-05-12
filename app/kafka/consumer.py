import json
import threading
from confluent_kafka import Consumer, KafkaError
from app.config import KAFKA_BROKER_URL, KAFKA_GROUP_ID, KAFKA_TOPIC
from app.logger import logger


class KafkaConsumerService:
    def __init__(self):
        self.running: bool = False
        self.thread: threading.Thread | None = None

    def _consume(self):
        consumer_config: dict[str] = {
            "bootstrap.servers": KAFKA_BROKER_URL,
            "group.id": KAFKA_GROUP_ID,
            "auto.offset.reset": "earliest",
        }

        consumer: Consumer = Consumer(consumer_config)
        consumer.subscribe([KAFKA_TOPIC])
        logger.info("[Kafka Consumer] Started in background")

        try:
            while self.running:
                msg = consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        logger.error(f"[Kafka Error] {msg.error()}")
                else:
                    data = json.loads(msg.value().decode("utf-8"))
                    logger.info(f"[Kafka] Received: {data}")
        except Exception as e:
            logger.error(f"[Kafka Consumer Error] {e}")
        finally:
            consumer.close()
            logger.info("[Kafka Consumer] Stopped")

    def start(self):
        self.running = True
        self.thread = threading.Thread(target=self._consume)
        self.thread.start()

    def stop(self):
        self.running = False
        if self.thread:
            self.thread.join()
