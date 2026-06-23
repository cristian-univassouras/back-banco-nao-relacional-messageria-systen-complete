from __future__ import annotations

import uuid
from dataclasses import dataclass
from enum import Enum


class OrderStatus(str, Enum):
    PENDENTE = "PENDENTE"
    ENVIADO = "ENVIADO"
    ENTREGUE = "ENTREGUE"
    CANCELADO = "CANCELADO"


class OrderEventType(str, Enum):
    CREATED = "CREATED"
    UPDATED = "UPDATED"
    DELETED = "DELETED"


class OrderNotFound(Exception):
    def __init__(self, order_id: str) -> None:
        self.order_id = order_id
        super().__init__(f"Order {order_id} not found")


@dataclass
class Order:
    id: str
    customer_name: str
    product_name: str
    quantity: int
    status: OrderStatus

    @classmethod
    def create(cls, customer_name: str, product_name: str, quantity: int) -> "Order":
        return cls(
            id=str(uuid.uuid4()),
            customer_name=customer_name,
            product_name=product_name,
            quantity=quantity,
            status=OrderStatus.PENDENTE,
        )
