from fastapi import FastAPI
from contextlib import asynccontextmanager
from pydantic import BaseModel

from app.kafka.producer import send_message
from app.kafka.consumer import KafkaConsumerService
from app.config import KAFKA_TOPIC


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    kafka_consumer.start()
    yield
    # Shutdown
    kafka_consumer.stop()


app: FastAPI = FastAPI(
    title="Kafka-FastAPI Integration", version="1.0", lifespan=lifespan
)
kafka_consumer: KafkaConsumerService = KafkaConsumerService()


class MessageModel(BaseModel):
    key: str
    value: dict


@app.post("/publish/", response_model=dict, status_code=201)
def publish_message(payload: MessageModel):
    send_message(KAFKA_TOPIC, {"key": payload.key, "value": payload.value})
    return {"status": "Message sent to Kafka"}
