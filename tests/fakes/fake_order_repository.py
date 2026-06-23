from __future__ import annotations

from app.models.order import Order, OrderNotFound, OrderStatus
from app.repositories.order_repository import OrderRepository


class FakeOrderRepository(OrderRepository):
    def __init__(self) -> None:
        self._orders: dict[str, Order] = {}

    def save(self, order: Order) -> None:
        self._orders[order.id] = order

    def list_all(self) -> list[Order]:
        return list(self._orders.values())

    def get_by_id(self, order_id: str) -> Order | None:
        return self._orders.get(order_id)

    def update_status(self, order_id: str, status: OrderStatus) -> Order:
        order = self._orders.get(order_id)
        if order is None:
            raise OrderNotFound(order_id)
        order.status = status
        return order

    def delete(self, order_id: str) -> None:
        if order_id not in self._orders:
            raise OrderNotFound(order_id)
        del self._orders[order_id]
