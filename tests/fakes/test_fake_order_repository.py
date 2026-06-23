import pytest

from app.models.order import Order, OrderNotFound, OrderStatus
from tests.fakes.fake_order_repository import FakeOrderRepository


async def test_save_and_list_all():
    repo = FakeOrderRepository()
    order = Order.create("Maria", "Teclado", 2)

    await repo.save(order)

    assert await repo.list_all() == [order]


async def test_get_by_id_returns_none_when_missing():
    repo = FakeOrderRepository()

    assert await repo.get_by_id("missing") is None


async def test_update_status_changes_status():
    repo = FakeOrderRepository()
    order = Order.create("Maria", "Teclado", 2)
    await repo.save(order)

    updated = await repo.update_status(order.id, OrderStatus.ENVIADO)

    assert updated.status == OrderStatus.ENVIADO
    found = await repo.get_by_id(order.id)
    assert found.status == OrderStatus.ENVIADO


async def test_update_status_raises_when_missing():
    repo = FakeOrderRepository()

    with pytest.raises(OrderNotFound):
        await repo.update_status("missing", OrderStatus.ENVIADO)


async def test_delete_removes_order():
    repo = FakeOrderRepository()
    order = Order.create("Maria", "Teclado", 2)
    await repo.save(order)

    await repo.delete(order.id)

    assert await repo.get_by_id(order.id) is None


async def test_delete_raises_when_missing():
    repo = FakeOrderRepository()

    with pytest.raises(OrderNotFound):
        await repo.delete("missing")
