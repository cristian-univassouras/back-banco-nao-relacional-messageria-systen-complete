import json
from unittest.mock import MagicMock

from app.models.order import Order, OrderEventType
from app.publishers.kafka_publisher import KafkaPublisher


def test_publish_sends_full_event_payload_to_topic():
    producer = MagicMock()
    publisher = KafkaPublisher(producer, topic="orders")
    order = Order.create("Maria", "Teclado", 2)

    publisher.publish(order, OrderEventType.CREATED)

    producer.send.assert_called_once()
    args, _kwargs = producer.send.call_args
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
    producer.flush.assert_called_once()
