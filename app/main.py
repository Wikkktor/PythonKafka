from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from app.kafka.producer import send_message
from app.config import KAFKA_TOPIC

app: FastAPI = FastAPI(title="Kafka-FastAPI Integration", version="1.0")


class MessageModel(BaseModel):
    key: str
    value: dict


@app.post("/publish/", response_model=dict, status_code=201)
def publish_message(payload: MessageModel):
    try:
        send_message(KAFKA_TOPIC, {"key": payload.key, "value": payload.value})
        return {"status": "Message sent to Kafka"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Kafka error: {str(e)}")
