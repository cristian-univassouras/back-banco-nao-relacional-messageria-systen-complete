from __future__ import annotations

from app.models.order import Order, OrderEventType
from app.publishers.event_publisher import EventPublisher


class FakeEventPublisher(EventPublisher):
    def __init__(self) -> None:
        self.published: list[tuple[Order, OrderEventType]] = []

    async def publish(self, order: Order, event_type: OrderEventType) -> None:
        self.published.append((order, event_type))
