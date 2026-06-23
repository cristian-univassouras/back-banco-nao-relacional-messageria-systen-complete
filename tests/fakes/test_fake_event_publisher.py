from app.models.order import Order, OrderEventType
from tests.fakes.fake_event_publisher import FakeEventPublisher


async def test_publish_records_order_and_event_type():
    publisher = FakeEventPublisher()
    order = Order.create("Maria", "Teclado", 2)

    await publisher.publish(order, OrderEventType.CREATED)

    assert publisher.published == [(order, OrderEventType.CREATED)]
