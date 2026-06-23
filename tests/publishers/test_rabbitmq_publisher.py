import json
from unittest.mock import MagicMock

from app.models.order import Order, OrderEventType
from app.publishers.rabbitmq_publisher import RabbitMQPublisher


def test_publish_declares_queue_on_init():
    channel = MagicMock()

    RabbitMQPublisher(channel, queue_name="orders")

    channel.queue_declare.assert_called_once_with(queue="orders", durable=True)


def test_publish_sends_json_payload_with_order_identification():
    channel = MagicMock()
    publisher = RabbitMQPublisher(channel, queue_name="orders")
    order = Order.create("Maria", "Teclado", 2)

    publisher.publish(order, OrderEventType.CREATED)

    channel.basic_publish.assert_called_once()
    _, kwargs = channel.basic_publish.call_args
    assert kwargs["exchange"] == ""
    assert kwargs["routing_key"] == "orders"
    payload = json.loads(kwargs["body"])
    assert payload == {
        "event_type": "CREATED",
        "order_id": order.id,
        "status": "PENDENTE",
    }
