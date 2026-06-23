from pydantic import BaseModel, Field

from app.models.order import OrderStatus


class OrderCreate(BaseModel):
    customer_name: str = Field(min_length=1)
    product_name: str = Field(min_length=1)
    quantity: int = Field(gt=0)


class OrderStatusUpdate(BaseModel):
    status: OrderStatus


class OrderResponse(BaseModel):
    id: str
    customer_name: str
    product_name: str
    quantity: int
    status: OrderStatus
