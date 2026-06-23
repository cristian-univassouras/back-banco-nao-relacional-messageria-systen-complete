import pytest

from app.models.order import OrderEventType, OrderNotFound, OrderStatus
from app.services.order_service import OrderService
from tests.fakes.fake_event_publisher import FakeEventPublisher
from tests.fakes.fake_order_repository import FakeOrderRepository


def make_service():
    repo = FakeOrderRepository()
    publishers = [FakeEventPublisher(), FakeEventPublisher()]
    service = OrderService(repo, publishers)
    return service, repo, publishers


async def test_create_order_persists_and_publishes_to_all_publishers():
    service, repo, publishers = make_service()

    order = await service.create_order("Maria", "Teclado", 2)

    assert order.status == OrderStatus.PENDENTE
    found = await repo.get_by_id(order.id)
    assert found == order
    for publisher in publishers:
        assert publisher.published == [(order, OrderEventType.CREATED)]


async def test_list_orders_returns_all_persisted_orders():
    service, _repo, _publishers = make_service()
    order1 = await service.create_order("Maria", "Teclado", 2)
    order2 = await service.create_order("Joao", "Mouse", 1)

    orders = await service.list_orders()

    assert orders == [order1, order2]


async def test_get_order_returns_existing_order():
    service, _repo, _publishers = make_service()
    created = await service.create_order("Maria", "Teclado", 2)

    found = await service.get_order(created.id)

    assert found == created


async def test_get_order_raises_when_missing():
    service, _repo, _publishers = make_service()

    with pytest.raises(OrderNotFound):
        await service.get_order("missing")


async def test_update_order_status_changes_status_and_publishes():
    service, _repo, publishers = make_service()
    created = await service.create_order("Maria", "Teclado", 2)

    updated = await service.update_order_status(created.id, OrderStatus.ENVIADO)

    assert updated.status == OrderStatus.ENVIADO
    for publisher in publishers:
        assert publisher.published[-1] == (updated, OrderEventType.UPDATED)


async def test_update_order_status_raises_when_missing():
    service, _repo, _publishers = make_service()

    with pytest.raises(OrderNotFound):
        await service.update_order_status("missing", OrderStatus.ENVIADO)


async def test_delete_order_removes_order_and_publishes():
    service, repo, publishers = make_service()
    created = await service.create_order("Maria", "Teclado", 2)

    await service.delete_order(created.id)

    assert await repo.get_by_id(created.id) is None
    for publisher in publishers:
        assert publisher.published[-1] == (created, OrderEventType.DELETED)


async def test_delete_order_raises_when_missing():
    service, _repo, _publishers = make_service()

    with pytest.raises(OrderNotFound):
        await service.delete_order("missing")
