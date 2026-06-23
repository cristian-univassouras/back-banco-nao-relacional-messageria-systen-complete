from __future__ import annotations

from app.models.order import Order, OrderEventType, OrderNotFound, OrderStatus
from app.publishers.event_publisher import EventPublisher
from app.repositories.order_repository import OrderRepository


class OrderService:
    def __init__(self, repository: OrderRepository, publishers: list[EventPublisher]) -> None:
        self._repository = repository
        self._publishers = publishers

    def _publish(self, order: Order, event_type: OrderEventType) -> None:
        for publisher in self._publishers:
            publisher.publish(order, event_type)

    def create_order(self, customer_name: str, product_name: str, quantity: int) -> Order:
        order = Order.create(customer_name, product_name, quantity)
        self._repository.save(order)
        self._publish(order, OrderEventType.CREATED)
        return order

    def list_orders(self) -> list[Order]:
        return self._repository.list_all()

    def get_order(self, order_id: str) -> Order:
        order = self._repository.get_by_id(order_id)
        if order is None:
            raise OrderNotFound(order_id)
        return order

    def update_order_status(self, order_id: str, status: OrderStatus) -> Order:
        order = self._repository.update_status(order_id, status)
        self._publish(order, OrderEventType.UPDATED)
        return order

    def delete_order(self, order_id: str) -> None:
        order = self.get_order(order_id)
        self._repository.delete(order_id)
        self._publish(order, OrderEventType.DELETED)
