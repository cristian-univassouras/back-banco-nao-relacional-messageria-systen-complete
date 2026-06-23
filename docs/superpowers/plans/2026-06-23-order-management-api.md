# Order Management API Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a FastAPI CRUD for orders, persisting to MongoDB and publishing CREATED/UPDATED/DELETED events to RabbitMQ and Kafka, fully runnable via `docker-compose up`.

**Architecture:** Layered (Router → Service → Repository/Publisher), with `OrderRepository` and `EventPublisher` as abstract interfaces (Dependency Inversion), `MongoOrderRepository`/`RabbitMQPublisher`/`KafkaPublisher` as concrete implementations, and `Order.create(...)` as a domain factory method that guarantees a unique id and initial status `PENDENTE`. See `docs/superpowers/specs/2026-06-23-order-management-api-design.md` for the full design rationale.

**Tech Stack:** Python 3.11+, FastAPI, Pydantic v2 + pydantic-settings, pymongo (sync MongoDB driver), pika (sync RabbitMQ client), kafka-python (pure-Python Kafka client, no C build dependency — easier on Windows than confluent-kafka), pytest + httpx (FastAPI `TestClient`), mongomock (unit-tests `MongoOrderRepository` without a real MongoDB).

## Global Constraints

- Every `Order` must have: unique id, customer name, product name, quantity, status. Initial status is always `PENDENTE` (from `CONTEXTO.md`).
- Creating an order must: persist to MongoDB, publish to a RabbitMQ queue, and publish to a Kafka topic (from `CONTEXTO.md`).
- The app must run via a single `docker-compose up` command, with FastAPI, MongoDB, RabbitMQ, Kafka, and Zookeeper as services (from `CONTEXTO.md`).
- Tests use Pytest and must cover at minimum order creation and order listing (from `CONTEXTO.md`); this plan extends coverage to get/update/delete too.
- `OrderService` must depend only on the `OrderRepository` and `EventPublisher` interfaces, never on `pymongo`/`pika`/`kafka-python` directly (from the design spec).
- Update is restricted to `status` only; full field edits are out of scope (from the design spec).

---

### Task 1: Project scaffolding + domain model (`Order`)

**Files:**
- Create: `requirements.txt`
- Create: `pyproject.toml`
- Create: `app/__init__.py`
- Create: `app/models/__init__.py`
- Create: `app/models/order.py`
- Create: `tests/__init__.py`
- Test: `tests/models/test_order.py`

**Interfaces:**
- Produces: `OrderStatus` (Enum: `PENDENTE`, `ENVIADO`, `ENTREGUE`, `CANCELADO`), `OrderEventType` (Enum: `CREATED`, `UPDATED`, `DELETED`), `OrderNotFound(Exception)`, `Order` dataclass with fields `id: str`, `customer_name: str`, `product_name: str`, `quantity: int`, `status: OrderStatus`, and classmethod `Order.create(customer_name: str, product_name: str, quantity: int) -> Order`.

- [ ] **Step 1: Create project structure and dependency files**

`requirements.txt`:
```
fastapi
uvicorn[standard]
pydantic
pydantic-settings
pymongo
pika
kafka-python
pytest
httpx
mongomock
```

`pyproject.toml`:
```toml
[tool.pytest.ini_options]
pythonpath = ["."]
testpaths = ["tests"]
```

Create empty files `app/__init__.py`, `app/models/__init__.py`, `tests/__init__.py`.

Install dependencies:
```bash
pip install -r requirements.txt
```

- [ ] **Step 2: Write the failing test for `Order.create`**

`tests/models/test_order.py`:
```python
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
```

Create empty `tests/models/__init__.py` so pytest can collect the package.

- [ ] **Step 3: Run tests to verify they fail**

Run: `pytest tests/models/test_order.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'app.models.order'`

- [ ] **Step 4: Implement `Order`**

`app/models/order.py`:
```python
from __future__ import annotations

import uuid
from dataclasses import dataclass
from enum import Enum


class OrderStatus(str, Enum):
    PENDENTE = "PENDENTE"
    ENVIADO = "ENVIADO"
    ENTREGUE = "ENTREGUE"
    CANCELADO = "CANCELADO"


class OrderEventType(str, Enum):
    CREATED = "CREATED"
    UPDATED = "UPDATED"
    DELETED = "DELETED"


class OrderNotFound(Exception):
    def __init__(self, order_id: str) -> None:
        self.order_id = order_id
        super().__init__(f"Order {order_id} not found")


@dataclass
class Order:
    id: str
    customer_name: str
    product_name: str
    quantity: int
    status: OrderStatus

    @classmethod
    def create(cls, customer_name: str, product_name: str, quantity: int) -> "Order":
        return cls(
            id=str(uuid.uuid4()),
            customer_name=customer_name,
            product_name=product_name,
            quantity=quantity,
            status=OrderStatus.PENDENTE,
        )
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `pytest tests/models/test_order.py -v`
Expected: 3 passed

- [ ] **Step 6: Commit**

```bash
git add requirements.txt pyproject.toml app/__init__.py app/models tests/__init__.py tests/models
git commit -m "feat: add Order domain model with create factory"
```

---

### Task 2: API schemas (DTOs)

**Files:**
- Create: `app/schemas/__init__.py`
- Create: `app/schemas/order.py`
- Test: `tests/schemas/test_order_schemas.py`

**Interfaces:**
- Consumes: `OrderStatus` from `app.models.order` (Task 1).
- Produces: `OrderCreate(customer_name: str, product_name: str, quantity: int)`, `OrderStatusUpdate(status: OrderStatus)`, `OrderResponse(id: str, customer_name: str, product_name: str, quantity: int, status: OrderStatus)` — all Pydantic `BaseModel`.

- [ ] **Step 1: Write the failing test**

Create empty `tests/schemas/__init__.py`.

`tests/schemas/test_order_schemas.py`:
```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/schemas/test_order_schemas.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'app.schemas.order'`

- [ ] **Step 3: Implement the schemas**

`app/schemas/order.py`:
```python
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
```

Create empty `app/schemas/__init__.py`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/schemas/test_order_schemas.py -v`
Expected: 4 passed

- [ ] **Step 5: Commit**

```bash
git add app/schemas tests/schemas
git commit -m "feat: add Order API schemas (DTOs)"
```

---

### Task 3: `OrderRepository` interface + `FakeOrderRepository`

**Files:**
- Create: `app/repositories/__init__.py`
- Create: `app/repositories/order_repository.py`
- Create: `tests/fakes/__init__.py`
- Create: `tests/fakes/fake_order_repository.py`
- Test: `tests/fakes/test_fake_order_repository.py`

**Interfaces:**
- Consumes: `Order`, `OrderStatus`, `OrderNotFound` from `app.models.order` (Task 1).
- Produces: `OrderRepository` (ABC) with abstract methods `save(order: Order) -> None`, `list_all() -> list[Order]`, `get_by_id(order_id: str) -> Order | None`, `update_status(order_id: str, status: OrderStatus) -> Order`, `delete(order_id: str) -> None`. `FakeOrderRepository` implements all of them in-memory, raising `OrderNotFound` from `update_status`/`delete` when the id is missing.

- [ ] **Step 1: Write the failing test**

Create empty `tests/fakes/__init__.py`.

`tests/fakes/test_fake_order_repository.py`:
```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/fakes/test_fake_order_repository.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'app.repositories.order_repository'`

- [ ] **Step 3: Implement the interface and the fake**

`app/repositories/order_repository.py`:
```python
from __future__ import annotations

from abc import ABC, abstractmethod

from app.models.order import Order, OrderStatus


class OrderRepository(ABC):
    @abstractmethod
    def save(self, order: Order) -> None:
        raise NotImplementedError

    @abstractmethod
    def list_all(self) -> list[Order]:
        raise NotImplementedError

    @abstractmethod
    def get_by_id(self, order_id: str) -> Order | None:
        raise NotImplementedError

    @abstractmethod
    def update_status(self, order_id: str, status: OrderStatus) -> Order:
        raise NotImplementedError

    @abstractmethod
    def delete(self, order_id: str) -> None:
        raise NotImplementedError
```

Create empty `app/repositories/__init__.py`.

`tests/fakes/fake_order_repository.py`:
```python
from __future__ import annotations

from app.models.order import Order, OrderNotFound, OrderStatus
from app.repositories.order_repository import OrderRepository


class FakeOrderRepository(OrderRepository):
    def __init__(self) -> None:
        self._orders: dict[str, Order] = {}

    def save(self, order: Order) -> None:
        self._orders[order.id] = order

    def list_all(self) -> list[Order]:
        return list(self._orders.values())

    def get_by_id(self, order_id: str) -> Order | None:
        return self._orders.get(order_id)

    def update_status(self, order_id: str, status: OrderStatus) -> Order:
        order = self._orders.get(order_id)
        if order is None:
            raise OrderNotFound(order_id)
        order.status = status
        return order

    def delete(self, order_id: str) -> None:
        if order_id not in self._orders:
            raise OrderNotFound(order_id)
        del self._orders[order_id]
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/fakes/test_fake_order_repository.py -v`
Expected: 6 passed

- [ ] **Step 5: Commit**

```bash
git add app/repositories tests/fakes
git commit -m "feat: add OrderRepository interface and in-memory fake"
```

---

### Task 4: `EventPublisher` interface + `FakeEventPublisher`

**Files:**
- Create: `app/publishers/__init__.py`
- Create: `app/publishers/event_publisher.py`
- Create: `tests/fakes/fake_event_publisher.py`
- Test: `tests/fakes/test_fake_event_publisher.py`

**Interfaces:**
- Consumes: `Order`, `OrderEventType` from `app.models.order` (Task 1).
- Produces: `EventPublisher` (ABC) with abstract method `publish(order: Order, event_type: OrderEventType) -> None`. `FakeEventPublisher` records every call in `self.published: list[tuple[Order, OrderEventType]]`.

- [ ] **Step 1: Write the failing test**

`tests/fakes/test_fake_event_publisher.py`:
```python
from app.models.order import Order, OrderEventType
from tests.fakes.fake_event_publisher import FakeEventPublisher


def test_publish_records_order_and_event_type():
    publisher = FakeEventPublisher()
    order = Order.create("Maria", "Teclado", 2)

    publisher.publish(order, OrderEventType.CREATED)

    assert publisher.published == [(order, OrderEventType.CREATED)]
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/fakes/test_fake_event_publisher.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'app.publishers.event_publisher'`

- [ ] **Step 3: Implement the interface and the fake**

`app/publishers/event_publisher.py`:
```python
from abc import ABC, abstractmethod

from app.models.order import Order, OrderEventType


class EventPublisher(ABC):
    @abstractmethod
    def publish(self, order: Order, event_type: OrderEventType) -> None:
        raise NotImplementedError
```

Create empty `app/publishers/__init__.py`.

`tests/fakes/fake_event_publisher.py`:
```python
from __future__ import annotations

from app.models.order import Order, OrderEventType
from app.publishers.event_publisher import EventPublisher


class FakeEventPublisher(EventPublisher):
    def __init__(self) -> None:
        self.published: list[tuple[Order, OrderEventType]] = []

    def publish(self, order: Order, event_type: OrderEventType) -> None:
        self.published.append((order, event_type))
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/fakes/test_fake_event_publisher.py -v`
Expected: 1 passed

- [ ] **Step 5: Commit**

```bash
git add app/publishers tests/fakes/fake_event_publisher.py tests/fakes/test_fake_event_publisher.py
git commit -m "feat: add EventPublisher interface and in-memory fake"
```

---

### Task 5: `OrderService` (CRUD orchestration)

**Files:**
- Create: `app/services/__init__.py`
- Create: `app/services/order_service.py`
- Test: `tests/services/test_order_service.py`

**Interfaces:**
- Consumes: `OrderRepository` (Task 3), `EventPublisher` (Task 4), `Order`, `OrderStatus`, `OrderEventType`, `OrderNotFound` (Task 1), `FakeOrderRepository` (Task 3), `FakeEventPublisher` (Task 4).
- Produces: `OrderService(repository: OrderRepository, publishers: list[EventPublisher])` with methods `create_order(customer_name: str, product_name: str, quantity: int) -> Order`, `list_orders() -> list[Order]`, `get_order(order_id: str) -> Order`, `update_order_status(order_id: str, status: OrderStatus) -> Order`, `delete_order(order_id: str) -> None`.

- [ ] **Step 1: Write the failing tests**

Create empty `tests/services/__init__.py`.

`tests/services/test_order_service.py`:
```python
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
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/services/test_order_service.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'app.services.order_service'`

- [ ] **Step 3: Implement `OrderService`**

`app/services/order_service.py`:
```python
from __future__ import annotations

from app.models.order import Order, OrderEventType, OrderNotFound, OrderStatus
from app.publishers.event_publisher import EventPublisher
from app.repositories.order_repository import OrderRepository


class OrderService:
    def __init__(self, repository: OrderRepository, publishers: list[EventPublisher]) -> None:
        self._repository = repository
        self._publishers = publishers

    def _publish(self, order: Order, event_type: OrderEventType) -> None:
        for publisher in self._publishers:
            publisher.publish(order, event_type)

    def create_order(self, customer_name: str, product_name: str, quantity: int) -> Order:
        order = Order.create(customer_name, product_name, quantity)
        self._repository.save(order)
        self._publish(order, OrderEventType.CREATED)
        return order

    def list_orders(self) -> list[Order]:
        return self._repository.list_all()

    def get_order(self, order_id: str) -> Order:
        order = self._repository.get_by_id(order_id)
        if order is None:
            raise OrderNotFound(order_id)
        return order

    def update_order_status(self, order_id: str, status: OrderStatus) -> Order:
        order = self._repository.update_status(order_id, status)
        self._publish(order, OrderEventType.UPDATED)
        return order

    def delete_order(self, order_id: str) -> None:
        order = self.get_order(order_id)
        self._repository.delete(order_id)
        self._publish(order, OrderEventType.DELETED)
```

Create empty `app/services/__init__.py`.

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/services/test_order_service.py -v`
Expected: 8 passed

- [ ] **Step 5: Commit**

```bash
git add app/services tests/services
git commit -m "feat: add OrderService with full CRUD orchestration"
```

---

### Task 6: `MongoOrderRepository`

**Files:**
- Create: `app/repositories/mongo_order_repository.py`
- Test: `tests/repositories/test_mongo_order_repository.py`

**Interfaces:**
- Consumes: `OrderRepository` (Task 3), `Order`, `OrderStatus`, `OrderNotFound` (Task 1). Uses `pymongo.collection.Collection` and `pymongo.ReturnDocument`.
- Produces: `MongoOrderRepository(collection: Collection)` implementing all `OrderRepository` methods against a MongoDB collection, storing `Order.id` as the document `_id`.

- [ ] **Step 1: Write the failing tests using `mongomock`**

Create empty `tests/repositories/__init__.py`.

`tests/repositories/test_mongo_order_repository.py`:
```python
import mongomock
import pytest

from app.models.order import Order, OrderNotFound, OrderStatus
from app.repositories.mongo_order_repository import MongoOrderRepository


@pytest.fixture
def repo():
    client = mongomock.MongoClient()
    collection = client["test_db"]["orders"]
    return MongoOrderRepository(collection)


def test_save_and_get_by_id(repo):
    order = Order.create("Maria", "Teclado", 2)
    repo.save(order)

    found = repo.get_by_id(order.id)

    assert found == order


def test_get_by_id_returns_none_when_missing(repo):
    assert repo.get_by_id("missing") is None


def test_list_all_returns_saved_orders(repo):
    order1 = Order.create("Maria", "Teclado", 2)
    order2 = Order.create("Joao", "Mouse", 1)
    repo.save(order1)
    repo.save(order2)

    orders = repo.list_all()

    assert {o.id for o in orders} == {order1.id, order2.id}


def test_update_status_persists_new_status(repo):
    order = Order.create("Maria", "Teclado", 2)
    repo.save(order)

    updated = repo.update_status(order.id, OrderStatus.ENVIADO)

    assert updated.status == OrderStatus.ENVIADO
    assert repo.get_by_id(order.id).status == OrderStatus.ENVIADO


def test_update_status_raises_when_missing(repo):
    with pytest.raises(OrderNotFound):
        repo.update_status("missing", OrderStatus.ENVIADO)


def test_delete_removes_order(repo):
    order = Order.create("Maria", "Teclado", 2)
    repo.save(order)

    repo.delete(order.id)

    assert repo.get_by_id(order.id) is None


def test_delete_raises_when_missing(repo):
    with pytest.raises(OrderNotFound):
        repo.delete("missing")
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/repositories/test_mongo_order_repository.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'app.repositories.mongo_order_repository'`

- [ ] **Step 3: Implement `MongoOrderRepository`**

`app/repositories/mongo_order_repository.py`:
```python
from __future__ import annotations

from pymongo import ReturnDocument
from pymongo.collection import Collection

from app.models.order import Order, OrderNotFound, OrderStatus
from app.repositories.order_repository import OrderRepository


class MongoOrderRepository(OrderRepository):
    def __init__(self, collection: Collection) -> None:
        self._collection = collection

    @staticmethod
    def _to_document(order: Order) -> dict:
        return {
            "_id": order.id,
            "customer_name": order.customer_name,
            "product_name": order.product_name,
            "quantity": order.quantity,
            "status": order.status.value,
        }

    @staticmethod
    def _to_order(document: dict) -> Order:
        return Order(
            id=document["_id"],
            customer_name=document["customer_name"],
            product_name=document["product_name"],
            quantity=document["quantity"],
            status=OrderStatus(document["status"]),
        )

    def save(self, order: Order) -> None:
        self._collection.insert_one(self._to_document(order))

    def list_all(self) -> list[Order]:
        return [self._to_order(doc) for doc in self._collection.find()]

    def get_by_id(self, order_id: str) -> Order | None:
        document = self._collection.find_one({"_id": order_id})
        return self._to_order(document) if document else None

    def update_status(self, order_id: str, status: OrderStatus) -> Order:
        document = self._collection.find_one_and_update(
            {"_id": order_id},
            {"$set": {"status": status.value}},
            return_document=ReturnDocument.AFTER,
        )
        if document is None:
            raise OrderNotFound(order_id)
        return self._to_order(document)

    def delete(self, order_id: str) -> None:
        result = self._collection.delete_one({"_id": order_id})
        if result.deleted_count == 0:
            raise OrderNotFound(order_id)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/repositories/test_mongo_order_repository.py -v`
Expected: 7 passed

- [ ] **Step 5: Commit**

```bash
git add app/repositories/mongo_order_repository.py tests/repositories
git commit -m "feat: add MongoOrderRepository implementation"
```

---

### Task 7: `RabbitMQPublisher`

**Files:**
- Create: `app/publishers/rabbitmq_publisher.py`
- Test: `tests/publishers/test_rabbitmq_publisher.py`

**Interfaces:**
- Consumes: `EventPublisher` (Task 4), `Order`, `OrderEventType` (Task 1).
- Produces: `RabbitMQPublisher(channel, queue_name: str = "orders")` implementing `publish(order, event_type)` by declaring the queue and calling `channel.basic_publish` with a JSON payload containing `event_type`, `order_id`, `status`.

- [ ] **Step 1: Write the failing test using a mock channel**

Create empty `tests/publishers/__init__.py`.

`tests/publishers/test_rabbitmq_publisher.py`:
```python
import json
from unittest.mock import MagicMock

from app.models.order import Order, OrderEventType
from app.publishers.rabbitmq_publisher import RabbitMQPublisher


def test_publish_declares_queue_on_init():
    channel = MagicMock()

    RabbitMQPublisher(channel, queue_name="orders")

    channel.queue_declare.assert_called_once_with(queue="orders", durable=True)


def test_publish_sends_json_payload_with_order_identification():
    channel = MagicMock()
    publisher = RabbitMQPublisher(channel, queue_name="orders")
    order = Order.create("Maria", "Teclado", 2)

    publisher.publish(order, OrderEventType.CREATED)

    channel.basic_publish.assert_called_once()
    _, kwargs = channel.basic_publish.call_args
    assert kwargs["exchange"] == ""
    assert kwargs["routing_key"] == "orders"
    payload = json.loads(kwargs["body"])
    assert payload == {
        "event_type": "CREATED",
        "order_id": order.id,
        "status": "PENDENTE",
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/publishers/test_rabbitmq_publisher.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'app.publishers.rabbitmq_publisher'`

- [ ] **Step 3: Implement `RabbitMQPublisher`**

`app/publishers/rabbitmq_publisher.py`:
```python
import json

from app.models.order import Order, OrderEventType
from app.publishers.event_publisher import EventPublisher


class RabbitMQPublisher(EventPublisher):
    def __init__(self, channel, queue_name: str = "orders") -> None:
        self._channel = channel
        self._queue_name = queue_name
        self._channel.queue_declare(queue=self._queue_name, durable=True)

    def publish(self, order: Order, event_type: OrderEventType) -> None:
        message = {
            "event_type": event_type.value,
            "order_id": order.id,
            "status": order.status.value,
        }
        self._channel.basic_publish(
            exchange="",
            routing_key=self._queue_name,
            body=json.dumps(message),
        )
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/publishers/test_rabbitmq_publisher.py -v`
Expected: 2 passed

- [ ] **Step 5: Commit**

```bash
git add app/publishers/rabbitmq_publisher.py tests/publishers
git commit -m "feat: add RabbitMQPublisher implementation"
```

---

### Task 8: `KafkaPublisher`

**Files:**
- Create: `app/publishers/kafka_publisher.py`
- Test: `tests/publishers/test_kafka_publisher.py`

**Interfaces:**
- Consumes: `EventPublisher` (Task 4), `Order`, `OrderEventType` (Task 1).
- Produces: `KafkaPublisher(producer, topic: str = "orders")` implementing `publish(order, event_type)` by calling `producer.send(topic, json_bytes)` and `producer.flush()`, with a full payload (`event_type`, `order_id`, `customer_name`, `product_name`, `quantity`, `status`).

- [ ] **Step 1: Write the failing test using a mock producer**

`tests/publishers/test_kafka_publisher.py`:
```python
import json
from unittest.mock import MagicMock

from app.models.order import Order, OrderEventType
from app.publishers.kafka_publisher import KafkaPublisher


def test_publish_sends_full_event_payload_to_topic():
    producer = MagicMock()
    publisher = KafkaPublisher(producer, topic="orders")
    order = Order.create("Maria", "Teclado", 2)

    publisher.publish(order, OrderEventType.CREATED)

    producer.send.assert_called_once()
    args, _kwargs = producer.send.call_args
    topic, body = args
    assert topic == "orders"
    payload = json.loads(body)
    assert payload == {
        "event_type": "CREATED",
        "order_id": order.id,
        "customer_name": "Maria",
        "product_name": "Teclado",
        "quantity": 2,
        "status": "PENDENTE",
    }
    producer.flush.assert_called_once()
```

- [ ] **Step 2: Run test to verify it fails**

Run: `pytest tests/publishers/test_kafka_publisher.py -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'app.publishers.kafka_publisher'`

- [ ] **Step 3: Implement `KafkaPublisher`**

`app/publishers/kafka_publisher.py`:
```python
import json

from app.models.order import Order, OrderEventType
from app.publishers.event_publisher import EventPublisher


class KafkaPublisher(EventPublisher):
    def __init__(self, producer, topic: str = "orders") -> None:
        self._producer = producer
        self._topic = topic

    def publish(self, order: Order, event_type: OrderEventType) -> None:
        message = {
            "event_type": event_type.value,
            "order_id": order.id,
            "customer_name": order.customer_name,
            "product_name": order.product_name,
            "quantity": order.quantity,
            "status": order.status.value,
        }
        self._producer.send(self._topic, json.dumps(message).encode("utf-8"))
        self._producer.flush()
```

- [ ] **Step 4: Run test to verify it passes**

Run: `pytest tests/publishers/test_kafka_publisher.py -v`
Expected: 1 passed

- [ ] **Step 5: Commit**

```bash
git add app/publishers/kafka_publisher.py tests/publishers/test_kafka_publisher.py
git commit -m "feat: add KafkaPublisher implementation"
```

---

### Task 9: Wiring (`config.py`, `dependencies.py`, `main.py`) + Router + API tests

**Files:**
- Create: `app/config.py`
- Create: `app/dependencies.py`
- Create: `app/main.py`
- Create: `app/routers/__init__.py`
- Create: `app/routers/order_router.py`
- Create: `tests/conftest.py`
- Test: `tests/api/test_order_create.py`
- Test: `tests/api/test_order_list.py`
- Test: `tests/api/test_order_get.py`
- Test: `tests/api/test_order_update.py`
- Test: `tests/api/test_order_delete.py`

**Interfaces:**
- Consumes: `OrderService` (Task 5), `OrderRepository`/`MongoOrderRepository` (Task 3, 6), `EventPublisher`/`RabbitMQPublisher`/`KafkaPublisher` (Task 4, 7, 8), `OrderCreate`/`OrderStatusUpdate`/`OrderResponse` (Task 2), `FakeOrderRepository`/`FakeEventPublisher` (Task 3, 4).
- Produces: FastAPI `app` (in `app.main`) exposing `POST /orders`, `GET /orders`, `GET /orders/{order_id}`, `PATCH /orders/{order_id}/status`, `DELETE /orders/{order_id}`. `get_order_service` dependency function in `app.dependencies`, overridable in tests.

- [ ] **Step 1: Implement `config.py`**

`app/config.py`:
```python
from functools import lru_cache

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    mongo_uri: str = "mongodb://localhost:27017"
    mongo_db_name: str = "orders_db"
    rabbitmq_url: str = "amqp://guest:guest@localhost:5672/"
    kafka_bootstrap_servers: str = "localhost:9092"

    class Config:
        env_file = ".env"


@lru_cache
def get_settings() -> Settings:
    return Settings()
```

- [ ] **Step 2: Implement `dependencies.py`**

`app/dependencies.py`:
```python
from fastapi import Request

from app.publishers.event_publisher import EventPublisher
from app.publishers.kafka_publisher import KafkaPublisher
from app.publishers.rabbitmq_publisher import RabbitMQPublisher
from app.repositories.mongo_order_repository import MongoOrderRepository
from app.repositories.order_repository import OrderRepository
from app.services.order_service import OrderService


def get_order_repository(request: Request) -> OrderRepository:
    settings = request.app.state.settings
    collection = request.app.state.mongo_client[settings.mongo_db_name]["orders"]
    return MongoOrderRepository(collection)


def get_event_publishers(request: Request) -> list[EventPublisher]:
    channel = request.app.state.rabbitmq_connection.channel()
    rabbitmq_publisher = RabbitMQPublisher(channel)
    kafka_publisher = KafkaPublisher(request.app.state.kafka_producer)
    return [rabbitmq_publisher, kafka_publisher]


def get_order_service(request: Request) -> OrderService:
    return OrderService(
        get_order_repository(request),
        get_event_publishers(request),
    )
```

- [ ] **Step 3: Implement `order_router.py`**

Create empty `app/routers/__init__.py`.

`app/routers/order_router.py`:
```python
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
```

- [ ] **Step 4: Implement `main.py`**

`app/main.py`:
```python
from contextlib import asynccontextmanager

import pika
from fastapi import FastAPI
from kafka import KafkaProducer
from pymongo import MongoClient

from app.config import get_settings
from app.routers.order_router import router as order_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    app.state.settings = settings
    app.state.mongo_client = MongoClient(settings.mongo_uri)
    app.state.rabbitmq_connection = pika.BlockingConnection(
        pika.URLParameters(settings.rabbitmq_url)
    )
    app.state.kafka_producer = KafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers
    )
    yield
    app.state.mongo_client.close()
    app.state.rabbitmq_connection.close()
    app.state.kafka_producer.close()


app = FastAPI(title="Order Management API", lifespan=lifespan)
app.include_router(order_router)
```

- [ ] **Step 5: Write `tests/conftest.py` with the API test fixture**

`tests/conftest.py`:
```python
import pytest
from fastapi.testclient import TestClient

from app.dependencies import get_order_service
from app.main import app
from app.services.order_service import OrderService
from tests.fakes.fake_event_publisher import FakeEventPublisher
from tests.fakes.fake_order_repository import FakeOrderRepository


@pytest.fixture
def api():
    repo = FakeOrderRepository()
    publishers = [FakeEventPublisher(), FakeEventPublisher()]

    app.dependency_overrides[get_order_service] = lambda: OrderService(repo, publishers)
    client = TestClient(app)

    yield client, repo, publishers

    app.dependency_overrides.clear()
```

Note: the fixture does NOT use `with TestClient(app) as client`, so the `lifespan` in `main.py` never runs — no real MongoDB/RabbitMQ/Kafka connection is attempted during tests, since `get_order_service` is fully overridden with fakes.

- [ ] **Step 6: Write the failing API tests**

Create empty `tests/api/__init__.py`.

`tests/api/test_order_create.py`:
```python
from app.models.order import OrderEventType


def test_create_order_returns_201_with_pendente_status(api):
    client, _repo, _publishers = api

    response = client.post(
        "/orders",
        json={"customer_name": "Maria", "product_name": "Teclado", "quantity": 2},
    )

    assert response.status_code == 201
    body = response.json()
    assert body["status"] == "PENDENTE"
    assert body["id"]


def test_create_order_persists_and_publishes_created_event(api):
    client, repo, publishers = api

    response = client.post(
        "/orders",
        json={"customer_name": "Maria", "product_name": "Teclado", "quantity": 2},
    )
    order_id = response.json()["id"]

    assert repo.get_by_id(order_id) is not None
    for publisher in publishers:
        assert publisher.published[-1][1] == OrderEventType.CREATED


def test_create_order_rejects_invalid_quantity(api):
    client, _repo, _publishers = api

    response = client.post(
        "/orders",
        json={"customer_name": "Maria", "product_name": "Teclado", "quantity": 0},
    )

    assert response.status_code == 422
```

`tests/api/test_order_list.py`:
```python
def test_list_orders_returns_all_created_orders(api):
    client, _repo, _publishers = api
    client.post(
        "/orders",
        json={"customer_name": "Maria", "product_name": "Teclado", "quantity": 2},
    )
    client.post(
        "/orders",
        json={"customer_name": "Joao", "product_name": "Mouse", "quantity": 1},
    )

    response = client.get("/orders")

    assert response.status_code == 200
    body = response.json()
    assert len(body) == 2
    assert {o["product_name"] for o in body} == {"Teclado", "Mouse"}


def test_list_orders_returns_empty_list_when_no_orders(api):
    client, _repo, _publishers = api

    response = client.get("/orders")

    assert response.status_code == 200
    assert response.json() == []
```

`tests/api/test_order_get.py`:
```python
def test_get_order_returns_existing_order(api):
    client, _repo, _publishers = api
    created = client.post(
        "/orders",
        json={"customer_name": "Maria", "product_name": "Teclado", "quantity": 2},
    ).json()

    response = client.get(f"/orders/{created['id']}")

    assert response.status_code == 200
    assert response.json() == created


def test_get_order_returns_404_when_missing(api):
    client, _repo, _publishers = api

    response = client.get("/orders/does-not-exist")

    assert response.status_code == 404
```

`tests/api/test_order_update.py`:
```python
from app.models.order import OrderEventType


def test_update_order_status_changes_status(api):
    client, _repo, _publishers = api
    created = client.post(
        "/orders",
        json={"customer_name": "Maria", "product_name": "Teclado", "quantity": 2},
    ).json()

    response = client.patch(f"/orders/{created['id']}/status", json={"status": "ENVIADO"})

    assert response.status_code == 200
    assert response.json()["status"] == "ENVIADO"


def test_update_order_status_publishes_updated_event(api):
    client, _repo, publishers = api
    created = client.post(
        "/orders",
        json={"customer_name": "Maria", "product_name": "Teclado", "quantity": 2},
    ).json()

    client.patch(f"/orders/{created['id']}/status", json={"status": "ENVIADO"})

    for publisher in publishers:
        assert publisher.published[-1][1] == OrderEventType.UPDATED


def test_update_order_status_returns_404_when_missing(api):
    client, _repo, _publishers = api

    response = client.patch("/orders/does-not-exist/status", json={"status": "ENVIADO"})

    assert response.status_code == 404
```

`tests/api/test_order_delete.py`:
```python
from app.models.order import OrderEventType


def test_delete_order_removes_it(api):
    client, repo, _publishers = api
    created = client.post(
        "/orders",
        json={"customer_name": "Maria", "product_name": "Teclado", "quantity": 2},
    ).json()

    response = client.delete(f"/orders/{created['id']}")

    assert response.status_code == 204
    assert repo.get_by_id(created["id"]) is None


def test_delete_order_publishes_deleted_event(api):
    client, _repo, publishers = api
    created = client.post(
        "/orders",
        json={"customer_name": "Maria", "product_name": "Teclado", "quantity": 2},
    ).json()

    client.delete(f"/orders/{created['id']}")

    for publisher in publishers:
        assert publisher.published[-1][1] == OrderEventType.DELETED


def test_delete_order_returns_404_when_missing(api):
    client, _repo, _publishers = api

    response = client.delete("/orders/does-not-exist")

    assert response.status_code == 404
```

- [ ] **Step 7: Run tests to verify they fail**

Run: `pytest tests/api -v`
Expected: FAIL with `ModuleNotFoundError: No module named 'app.dependencies'` (or similar, before implementation exists)

- [ ] **Step 8: Run tests to verify they pass**

Run: `pytest tests/api -v`
Expected: 13 passed

- [ ] **Step 9: Run the full test suite**

Run: `pytest -v`
Expected: all tests from Tasks 1–9 pass (45 passed)

- [ ] **Step 10: Commit**

```bash
git add app/config.py app/dependencies.py app/main.py app/routers tests/conftest.py tests/api
git commit -m "feat: wire FastAPI app with router, dependencies, and API tests"
```

---

### Task 10: Containerization (`Dockerfile` + `docker-compose.yml`)

**Files:**
- Create: `Dockerfile`
- Create: `docker-compose.yml`
- Create: `.dockerignore`

**Interfaces:**
- Consumes: `app/` package and `requirements.txt` (Tasks 1–9). Reads `MONGO_URI`, `MONGO_DB_NAME`, `RABBITMQ_URL`, `KAFKA_BOOTSTRAP_SERVERS` env vars matching `app/config.py:Settings` field names.

- [ ] **Step 1: Write `Dockerfile`**

`Dockerfile`:
```dockerfile
FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app ./app

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

- [ ] **Step 2: Write `.dockerignore`**

`.dockerignore`:
```
__pycache__
*.pyc
.pytest_cache
tests
docs
.git
```

- [ ] **Step 3: Write `docker-compose.yml`**

`docker-compose.yml`:
```yaml
version: "3.9"

services:
  api:
    build: .
    ports:
      - "8000:8000"
    environment:
      MONGO_URI: mongodb://mongo:27017
      MONGO_DB_NAME: orders_db
      RABBITMQ_URL: amqp://guest:guest@rabbitmq:5672/
      KAFKA_BOOTSTRAP_SERVERS: kafka:9092
    depends_on:
      - mongo
      - rabbitmq
      - kafka

  mongo:
    image: mongo:7
    ports:
      - "27017:27017"

  rabbitmq:
    image: rabbitmq:3-management
    ports:
      - "5672:5672"
      - "15672:15672"

  zookeeper:
    image: confluentinc/cp-zookeeper:7.6.0
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181

  kafka:
    image: confluentinc/cp-kafka:7.6.0
    depends_on:
      - zookeeper
    ports:
      - "9092:9092"
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
```

- [ ] **Step 4: Validate the compose file syntax**

Run: `docker-compose config`
Expected: prints the fully resolved compose configuration with no errors (does not require the daemon to start containers, only a valid Docker installation)

- [ ] **Step 5: Bring the stack up and smoke-test it**

Run: `docker-compose up --build -d`
Wait for containers to report healthy/running, then:
Run: `curl -X POST http://localhost:8000/orders -H "Content-Type: application/json" -d "{\"customer_name\":\"Maria\",\"product_name\":\"Teclado\",\"quantity\":2}"`
Expected: HTTP 201 with a JSON body containing `"status":"PENDENTE"`

Run: `curl http://localhost:8000/orders`
Expected: HTTP 200 with a JSON array containing the order created above

Run: `docker-compose down`

- [ ] **Step 6: Commit**

```bash
git add Dockerfile docker-compose.yml .dockerignore
git commit -m "feat: add Dockerfile and docker-compose stack"
```

---

### Task 11: Final verification

**Files:**
- Create: `README.md`

**Interfaces:**
- Consumes: nothing new — this task only verifies prior work and documents how to run it.

- [ ] **Step 1: Run the full automated test suite**

Run: `pytest -v`
Expected: all tests pass (45 passed), 0 failed

- [ ] **Step 2: Write `README.md`**

`README.md`:
```markdown
# Order Management API

API FastAPI para gerenciamento de pedidos, com persistencia em MongoDB e
publicacao de eventos em RabbitMQ e Kafka.

## Como executar

```bash
docker-compose up --build
```

A API fica disponivel em `http://localhost:8000`. Documentacao interativa em
`http://localhost:8000/docs`.

## Endpoints

- `POST /orders` - cria um pedido (status inicial: PENDENTE)
- `GET /orders` - lista todos os pedidos
- `GET /orders/{id}` - busca um pedido por id
- `PATCH /orders/{id}/status` - atualiza o status de um pedido
- `DELETE /orders/{id}` - remove um pedido

## Testes

```bash
pip install -r requirements.txt
pytest -v
```

Os testes usam fakes em memoria para o repositorio e os publishers, entao
nao dependem de MongoDB/RabbitMQ/Kafka rodando.
```

- [ ] **Step 3: Commit**

```bash
git add README.md
git commit -m "docs: add README with run and test instructions"
```
