import json

import aio_pika

from app.models.order import Order, OrderEventType
from app.publishers.event_publisher import EventPublisher


class RabbitMQPublisher(EventPublisher):
    def __init__(self, channel, queue_name: str = "orders") -> None:
        self._channel = channel
        self._queue_name = queue_name

    @classmethod
    async def create(cls, channel, queue_name: str = "orders") -> "RabbitMQPublisher":
        await channel.declare_queue(queue_name, durable=True)
        return cls(channel, queue_name)

    async def publish(self, order: Order, event_type: OrderEventType) -> None:
        message = {
            "event_type": event_type.value,
            "order_id": order.id,
            "status": order.status.value,
        }
        await self._channel.default_exchange.publish(
            aio_pika.Message(body=json.dumps(message).encode("utf-8")),
            routing_key=self._queue_name,
        )
