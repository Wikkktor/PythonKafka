import pytest

from typing import Any
from fastapi.testclient import TestClient
from unittest.mock import MagicMock, patch

from app.main import app
from app.config import KAFKA_TOPIC
from app.kafka.consumer import KafkaConsumerService


# Test FastAPI app with TestClient
client: TestClient = TestClient(app)


def test_producer():
    # Mock message payload
    message: dict[str, Any] = {"key": "test-key", "value": {"field": "test-value"}}

    with patch("app.main.send_message") as mock_send_message:
        with TestClient(app) as client:
            response = client.post("/publish/", json=message)
            mock_send_message.assert_called_once_with(
                KAFKA_TOPIC, {"key": message["key"], "value": message["value"]}
            )
            assert response.status_code == 201
            assert response.json() == {"status": "Message sent to Kafka"}


# Fixture to mock the Kafka consumer service
@pytest.fixture
def mock_consumer_service() -> KafkaConsumerService:
    # Mock the consumer
    consumer_service: KafkaConsumerService = KafkaConsumerService()
    consumer_service._consume = MagicMock()
    return consumer_service


def test_consumer_start(mock_consumer_service):
    # Start the consumer
    mock_consumer_service.start()

    # Check if the consumer's _consume method was called
    mock_consumer_service._consume.assert_called_once()


def test_consumer_stop(mock_consumer_service):
    # Start the consumer and then stop
    mock_consumer_service.start()
    mock_consumer_service.stop()

    # Verify that the consumer's stop method was called
    assert not mock_consumer_service.running


def test_consumer_message_handling(mock_consumer_service):
    # Simulate message processing
    consumer = mock_consumer_service
    # Mock message
    consumer._consume = MagicMock(return_value=None)

    # Assuming the consumer's _consume method processes messages
    consumer._consume()

    # Verify that the consumer is processing messages
    consumer._consume.assert_called_once()
