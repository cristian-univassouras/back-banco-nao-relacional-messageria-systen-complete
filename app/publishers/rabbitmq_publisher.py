import json

from app.models.order import Order, OrderEventType
from app.publishers.event_publisher import EventPublisher


class RabbitMQPublisher(EventPublisher):
    def __init__(self, channel, queue_name: str = "orders") -> None:
        self._channel = channel
        self._queue_name = queue_name
        self._channel.queue_declare(queue=self._queue_name, durable=True)

    def publish(self, order: Order, event_type: OrderEventType) -> None:
        message = {
            "event_type": event_type.value,
            "order_id": order.id,
            "status": order.status.value,
        }
        self._channel.basic_publish(
            exchange="",
            routing_key=self._queue_name,
            body=json.dumps(message),
        )
