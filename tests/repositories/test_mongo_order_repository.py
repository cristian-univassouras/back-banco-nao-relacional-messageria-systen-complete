import mongomock
import pytest

from app.models.order import Order, OrderNotFound, OrderStatus
from app.repositories.mongo_order_repository import MongoOrderRepository


@pytest.fixture
def repo():
    client = mongomock.MongoClient()
    collection = client["test_db"]["orders"]
    return MongoOrderRepository(collection)


def test_save_and_get_by_id(repo):
    order = Order.create("Maria", "Teclado", 2)
    repo.save(order)

    found = repo.get_by_id(order.id)

    assert found == order


def test_get_by_id_returns_none_when_missing(repo):
    assert repo.get_by_id("missing") is None


def test_list_all_returns_saved_orders(repo):
    order1 = Order.create("Maria", "Teclado", 2)
    order2 = Order.create("Joao", "Mouse", 1)
    repo.save(order1)
    repo.save(order2)

    orders = repo.list_all()

    assert {o.id for o in orders} == {order1.id, order2.id}


def test_update_status_persists_new_status(repo):
    order = Order.create("Maria", "Teclado", 2)
    repo.save(order)

    updated = repo.update_status(order.id, OrderStatus.ENVIADO)

    assert updated.status == OrderStatus.ENVIADO
    assert repo.get_by_id(order.id).status == OrderStatus.ENVIADO


def test_update_status_raises_when_missing(repo):
    with pytest.raises(OrderNotFound):
        repo.update_status("missing", OrderStatus.ENVIADO)


def test_delete_removes_order(repo):
    order = Order.create("Maria", "Teclado", 2)
    repo.save(order)

    repo.delete(order.id)

    assert repo.get_by_id(order.id) is None


def test_delete_raises_when_missing(repo):
    with pytest.raises(OrderNotFound):
        repo.delete("missing")
