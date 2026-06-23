import pytest

from app.models.order import Order, OrderNotFound, OrderStatus
from tests.fakes.fake_order_repository import FakeOrderRepository


def test_save_and_list_all():
    repo = FakeOrderRepository()
    order = Order.create("Maria", "Teclado", 2)

    repo.save(order)

    assert repo.list_all() == [order]


def test_get_by_id_returns_none_when_missing():
    repo = FakeOrderRepository()

    assert repo.get_by_id("missing") is None


def test_update_status_changes_status():
    repo = FakeOrderRepository()
    order = Order.create("Maria", "Teclado", 2)
    repo.save(order)

    updated = repo.update_status(order.id, OrderStatus.ENVIADO)

    assert updated.status == OrderStatus.ENVIADO
    assert repo.get_by_id(order.id).status == OrderStatus.ENVIADO


def test_update_status_raises_when_missing():
    repo = FakeOrderRepository()

    with pytest.raises(OrderNotFound):
        repo.update_status("missing", OrderStatus.ENVIADO)


def test_delete_removes_order():
    repo = FakeOrderRepository()
    order = Order.create("Maria", "Teclado", 2)
    repo.save(order)

    repo.delete(order.id)

    assert repo.get_by_id(order.id) is None


def test_delete_raises_when_missing():
    repo = FakeOrderRepository()

    with pytest.raises(OrderNotFound):
        repo.delete("missing")
