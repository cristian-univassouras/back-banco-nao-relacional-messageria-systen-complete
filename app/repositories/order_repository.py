from __future__ import annotations

from abc import ABC, abstractmethod

from app.models.order import Order, OrderStatus


class OrderRepository(ABC):
    @abstractmethod
    async def save(self, order: Order) -> None:
        raise NotImplementedError

    @abstractmethod
    async def list_all(self) -> list[Order]:
        raise NotImplementedError

    @abstractmethod
    async def get_by_id(self, order_id: str) -> Order | None:
        raise NotImplementedError

    @abstractmethod
    async def update_status(self, order_id: str, status: OrderStatus) -> Order:
        raise NotImplementedError

    @abstractmethod
    async def delete(self, order_id: str) -> None:
        raise NotImplementedError
