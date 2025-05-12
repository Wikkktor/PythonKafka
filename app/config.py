import os

KAFKA_BROKER_URL: str = os.getenv("KAFKA_BROKER_URL", "localhost:9092")
KAFKA_TOPIC: str = os.getenv("KAFKA_TOPIC", "LearningKafka")
KAFKA_GROUP_ID: str = os.getenv("KAFKA_GROUP_ID", "LearningKafka-group")
