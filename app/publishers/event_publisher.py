from abc import ABC, abstractmethod

from app.models.order import Order, OrderEventType


class EventPublisher(ABC):
    @abstractmethod
    async def publish(self, order: Order, event_type: OrderEventType) -> None:
        raise NotImplementedError
