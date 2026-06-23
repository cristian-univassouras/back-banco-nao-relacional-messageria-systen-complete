import json
from unittest.mock import AsyncMock

from app.models.order import Order, OrderEventType
from app.publishers.kafka_publisher import KafkaPublisher


async def test_publish_sends_full_event_payload_to_topic():
    producer = AsyncMock()
    publisher = KafkaPublisher(producer, topic="orders")
    order = Order.create("Maria", "Teclado", 2)

    await publisher.publish(order, OrderEventType.CREATED)

    producer.send_and_wait.assert_called_once()
    args, _kwargs = producer.send_and_wait.call_args
    topic, body = args
    assert topic == "orders"
    payload = json.loads(body)
    assert payload == {
        "event_type": "CREATED",
        "order_id": order.id,
        "customer_name": "Maria",
        "product_name": "Teclado",
        "quantity": 2,
        "status": "PENDENTE",
    }
