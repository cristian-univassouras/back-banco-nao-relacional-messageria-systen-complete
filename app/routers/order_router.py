from fastapi import APIRouter, Depends, HTTPException

from app.dependencies import get_order_service
from app.models.order import Order, OrderNotFound
from app.schemas.order import OrderCreate, OrderResponse, OrderStatusUpdate
from app.services.order_service import OrderService

router = APIRouter(prefix="/orders", tags=["orders"])


def _to_response(order: Order) -> OrderResponse:
    return OrderResponse(
        id=order.id,
        customer_name=order.customer_name,
        product_name=order.product_name,
        quantity=order.quantity,
        status=order.status,
    )


@router.post("", response_model=OrderResponse, status_code=201)
def create_order(
    payload: OrderCreate, service: OrderService = Depends(get_order_service)
) -> OrderResponse:
    order = service.create_order(payload.customer_name, payload.product_name, payload.quantity)
    return _to_response(order)


@router.get("", response_model=list[OrderResponse])
def list_orders(service: OrderService = Depends(get_order_service)) -> list[OrderResponse]:
    return [_to_response(order) for order in service.list_orders()]


@router.get("/{order_id}", response_model=OrderResponse)
def get_order(order_id: str, service: OrderService = Depends(get_order_service)) -> OrderResponse:
    try:
        order = service.get_order(order_id)
    except OrderNotFound:
        raise HTTPException(status_code=404, detail="Order not found")
    return _to_response(order)


@router.patch("/{order_id}/status", response_model=OrderResponse)
def update_order_status(
    order_id: str,
    payload: OrderStatusUpdate,
    service: OrderService = Depends(get_order_service),
) -> OrderResponse:
    try:
        order = service.update_order_status(order_id, payload.status)
    except OrderNotFound:
        raise HTTPException(status_code=404, detail="Order not found")
    return _to_response(order)


@router.delete("/{order_id}", status_code=204)
def delete_order(order_id: str, service: OrderService = Depends(get_order_service)) -> None:
    try:
        service.delete_order(order_id)
    except OrderNotFound:
        raise HTTPException(status_code=404, detail="Order not found")
