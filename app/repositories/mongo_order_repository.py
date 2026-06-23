from __future__ import annotations

from pymongo import ReturnDocument
from pymongo.collection import Collection

from app.models.order import Order, OrderNotFound, OrderStatus
from app.repositories.order_repository import OrderRepository


class MongoOrderRepository(OrderRepository):
    def __init__(self, collection: Collection) -> None:
        self._collection = collection

    @staticmethod
    def _to_document(order: Order) -> dict:
        return {
            "_id": order.id,
            "customer_name": order.customer_name,
            "product_name": order.product_name,
            "quantity": order.quantity,
            "status": order.status.value,
        }

    @staticmethod
    def _to_order(document: dict) -> Order:
        return Order(
            id=document["_id"],
            customer_name=document["customer_name"],
            product_name=document["product_name"],
            quantity=document["quantity"],
            status=OrderStatus(document["status"]),
        )

    def save(self, order: Order) -> None:
        self._collection.insert_one(self._to_document(order))

    def list_all(self) -> list[Order]:
        return [self._to_order(doc) for doc in self._collection.find()]

    def get_by_id(self, order_id: str) -> Order | None:
        document = self._collection.find_one({"_id": order_id})
        return self._to_order(document) if document else None

    def update_status(self, order_id: str, status: OrderStatus) -> Order:
        document = self._collection.find_one_and_update(
            {"_id": order_id},
            {"$set": {"status": status.value}},
            return_document=ReturnDocument.AFTER,
        )
        if document is None:
            raise OrderNotFound(order_id)
        return self._to_order(document)

    def delete(self, order_id: str) -> None:
        result = self._collection.delete_one({"_id": order_id})
        if result.deleted_count == 0:
            raise OrderNotFound(order_id)
