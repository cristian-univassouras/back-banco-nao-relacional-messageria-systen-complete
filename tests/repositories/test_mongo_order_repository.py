import pytest
from mongomock_motor import AsyncMongoMockClient

from app.models.order import Order, OrderNotFound, OrderStatus
from app.repositories.mongo_order_repository import MongoOrderRepository


@pytest.fixture
def repo():
    client = AsyncMongoMockClient()
    collection = client["test_db"]["orders"]
    return MongoOrderRepository(collection)


async def test_save_and_get_by_id(repo):
    order = Order.create("Maria", "Teclado", 2)
    await repo.save(order)

    found = await repo.get_by_id(order.id)

    assert found == order


async def test_get_by_id_returns_none_when_missing(repo):
    assert await repo.get_by_id("missing") is None


async def test_list_all_returns_saved_orders(repo):
    order1 = Order.create("Maria", "Teclado", 2)
    order2 = Order.create("Joao", "Mouse", 1)
    await repo.save(order1)
    await repo.save(order2)

    orders = await repo.list_all()

    assert {o.id for o in orders} == {order1.id, order2.id}


async def test_update_status_persists_new_status(repo):
    order = Order.create("Maria", "Teclado", 2)
    await repo.save(order)

    updated = await repo.update_status(order.id, OrderStatus.ENVIADO)

    assert updated.status == OrderStatus.ENVIADO
    found = await repo.get_by_id(order.id)
    assert found.status == OrderStatus.ENVIADO


async def test_update_status_raises_when_missing(repo):
    with pytest.raises(OrderNotFound):
        await repo.update_status("missing", OrderStatus.ENVIADO)


async def test_delete_removes_order(repo):
    order = Order.create("Maria", "Teclado", 2)
    await repo.save(order)

    await repo.delete(order.id)

    assert await repo.get_by_id(order.id) is None


async def test_delete_raises_when_missing(repo):
    with pytest.raises(OrderNotFound):
        await repo.delete("missing")
