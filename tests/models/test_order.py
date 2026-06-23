from app.models.order import Order, OrderStatus


def test_create_sets_initial_status_pendente():
    order = Order.create(customer_name="Maria", product_name="Teclado", quantity=2)

    assert order.status == OrderStatus.PENDENTE


def test_create_generates_unique_ids():
    order1 = Order.create(customer_name="Maria", product_name="Teclado", quantity=2)
    order2 = Order.create(customer_name="Joao", product_name="Mouse", quantity=1)

    assert order1.id != order2.id
    assert order1.id
    assert order2.id


def test_create_stores_provided_fields():
    order = Order.create(customer_name="Maria", product_name="Teclado", quantity=2)

    assert order.customer_name == "Maria"
    assert order.product_name == "Teclado"
    assert order.quantity == 2
