import json
from unittest.mock import AsyncMock, MagicMock

from app.models.order import Order, OrderEventType
from app.publishers.rabbitmq_publisher import RabbitMQPublisher


async def test_create_declares_queue():
    channel = MagicMock()
    channel.declare_queue = AsyncMock()

    await RabbitMQPublisher.create(channel, queue_name="orders")

    channel.declare_queue.assert_called_once_with("orders", durable=True)


async def test_publish_sends_json_payload_with_order_identification():
    channel = MagicMock()
    channel.declare_queue = AsyncMock()
    channel.default_exchange = MagicMock()
    channel.default_exchange.publish = AsyncMock()
    publisher = await RabbitMQPublisher.create(channel, queue_name="orders")
    order = Order.create("Maria", "Teclado", 2)

    await publisher.publish(order, OrderEventType.CREATED)

    channel.default_exchange.publish.assert_called_once()
    args, kwargs = channel.default_exchange.publish.call_args
    message = args[0]
    assert kwargs["routing_key"] == "orders"
    payload = json.loads(message.body)
    assert payload == {
        "event_type": "CREATED",
        "order_id": order.id,
        "status": "PENDENTE",
    }
