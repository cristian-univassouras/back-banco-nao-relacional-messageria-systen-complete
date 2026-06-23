import pytest
from pydantic import ValidationError

from app.schemas.order import OrderCreate, OrderResponse, OrderStatusUpdate
from app.models.order import OrderStatus


def test_order_create_accepts_valid_payload():
    dto = OrderCreate(customer_name="Maria", product_name="Teclado", quantity=2)

    assert dto.quantity == 2


def test_order_create_rejects_non_positive_quantity():
    with pytest.raises(ValidationError):
        OrderCreate(customer_name="Maria", product_name="Teclado", quantity=0)


def test_order_status_update_accepts_valid_status():
    dto = OrderStatusUpdate(status="ENVIADO")

    assert dto.status == OrderStatus.ENVIADO


def test_order_response_round_trips_fields():
    dto = OrderResponse(
        id="abc-123",
        customer_name="Maria",
        product_name="Teclado",
        quantity=2,
        status=OrderStatus.PENDENTE,
    )

    assert dto.model_dump()["status"] == "PENDENTE"
