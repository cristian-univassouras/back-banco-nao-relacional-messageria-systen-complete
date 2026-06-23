import json

from app.models.order import Order, OrderEventType
from app.publishers.event_publisher import EventPublisher


class KafkaPublisher(EventPublisher):
    def __init__(self, producer, topic: str = "orders") -> None:
        self._producer = producer
        self._topic = topic

    def publish(self, order: Order, event_type: OrderEventType) -> None:
        message = {
            "event_type": event_type.value,
            "order_id": order.id,
            "customer_name": order.customer_name,
            "product_name": order.product_name,
            "quantity": order.quantity,
            "status": order.status.value,
        }
        self._producer.send(self._topic, json.dumps(message).encode("utf-8"))
        self._producer.flush()
