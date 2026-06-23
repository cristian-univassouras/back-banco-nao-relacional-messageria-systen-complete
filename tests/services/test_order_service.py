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


def test_create_order_persists_and_publishes_to_all_publishers():
    service, repo, publishers = make_service()

    order = service.create_order("Maria", "Teclado", 2)

    assert order.status == OrderStatus.PENDENTE
    assert repo.get_by_id(order.id) == order
    for publisher in publishers:
        assert publisher.published == [(order, OrderEventType.CREATED)]


def test_list_orders_returns_all_persisted_orders():
    service, _repo, _publishers = make_service()
    order1 = service.create_order("Maria", "Teclado", 2)
    order2 = service.create_order("Joao", "Mouse", 1)

    orders = service.list_orders()

    assert orders == [order1, order2]


def test_get_order_returns_existing_order():
    service, _repo, _publishers = make_service()
    created = service.create_order("Maria", "Teclado", 2)

    found = service.get_order(created.id)

    assert found == created


def test_get_order_raises_when_missing():
    service, _repo, _publishers = make_service()

    with pytest.raises(OrderNotFound):
        service.get_order("missing")


def test_update_order_status_changes_status_and_publishes():
    service, _repo, publishers = make_service()
    created = service.create_order("Maria", "Teclado", 2)

    updated = service.update_order_status(created.id, OrderStatus.ENVIADO)

    assert updated.status == OrderStatus.ENVIADO
    for publisher in publishers:
        assert publisher.published[-1] == (updated, OrderEventType.UPDATED)


def test_update_order_status_raises_when_missing():
    service, _repo, _publishers = make_service()

    with pytest.raises(OrderNotFound):
        service.update_order_status("missing", OrderStatus.ENVIADO)


def test_delete_order_removes_order_and_publishes():
    service, repo, publishers = make_service()
    created = service.create_order("Maria", "Teclado", 2)

    service.delete_order(created.id)

    assert repo.get_by_id(created.id) is None
    for publisher in publishers:
        assert publisher.published[-1] == (created, OrderEventType.DELETED)


def test_delete_order_raises_when_missing():
    service, _repo, _publishers = make_service()

    with pytest.raises(OrderNotFound):
        service.delete_order("missing")
