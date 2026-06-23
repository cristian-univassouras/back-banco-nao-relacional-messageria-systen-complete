# Async I/O Migration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Migrate the Order Management API's I/O from blocking clients (`pymongo`, `pika`, `kafka-python`) to non-blocking async clients (`motor`, `aio-pika`, `aiokafka`), so the FastAPI request thread never blocks waiting on MongoDB/RabbitMQ/Kafka.

**Architecture:** Unchanged layering (Router → Service → Repository/Publisher). Every method on `OrderRepository`, `EventPublisher`, `OrderService`, and every router endpoint becomes `async def`. `MongoOrderRepository`/`RabbitMQPublisher`/`KafkaPublisher` swap their underlying client library for an async equivalent. API-level tests (`tests/api/*`) and `tests/conftest.py` need **no changes** — FastAPI's `TestClient` already drives async endpoints transparently.

**Tech Stack:** `motor` (MongoDB), `aio-pika` (RabbitMQ), `aiokafka` (Kafka), `mongomock-motor` (testing `MongoOrderRepository` without a real MongoDB), `pytest-asyncio` with `asyncio_mode = "auto"` (running `async def test_...` functions without per-test decorators).

## Global Constraints

- Every I/O call to MongoDB, RabbitMQ, or Kafka must go through an `await` on a non-blocking client (`motor`/`aio-pika`/`aiokafka`) — no blocking calls left in `app/`.
- `OrderRepository` and `EventPublisher` interface methods are `async def`; `OrderService` methods are `async def`; all 5 router endpoints are `async def`.
- `OrderService` depends only on the `OrderRepository`/`EventPublisher` interfaces, never on `motor`/`aio-pika`/`aiokafka` directly (unchanged from the original design).
- The "communication between systems" requirement (RabbitMQ/Kafka pub/sub) is already satisfied and does not change — this migration only changes how the Python process waits for network I/O.
- `tests/api/*` and `tests/conftest.py` stay synchronous `def test_...` (no change) — `TestClient` already supports async endpoints.
- Final test count must stay 45 (same tests, converted to `async def` where they exercise async code; no tests added or removed).
- `docker-compose.yml` does not change — only `requirements.txt` and the application code change.

---

### Task 1: Update dependencies and Pytest async config

**Files:**
- Modify: `requirements.txt`
- Modify: `pyproject.toml`

**Interfaces:**
- Produces: `pytest-asyncio` active with `asyncio_mode = "auto"`, so any `async def test_...` function in later tasks runs without a decorator. `motor`, `aio-pika`, `aiokafka`, `mongomock-motor` available for import.

- [ ] **Step 1: Update `requirements.txt`**

Replace the file contents with:

```
fastapi
uvicorn[standard]
pydantic
pydantic-settings
motor
aio-pika
aiokafka
pytest
httpx
mongomock-motor
pytest-asyncio
```

- [ ] **Step 2: Install the updated dependencies**

```bash
pip install -r requirements.txt
```

- [ ] **Step 3: Add `asyncio_mode` to `pyproject.toml`**

Replace the file contents with:

```toml
[tool.pytest.ini_options]
pythonpath = ["."]
testpaths = ["tests"]
asyncio_mode = "auto"
```

- [ ] **Step 4: Run the full suite to confirm no regression from the dependency/config change alone**

Run: `pytest -v`
Expected: 45 passed (no async code exists yet — this just confirms `pytest-asyncio` installation and config didn't break anything)

- [ ] **Step 5: Commit**

```bash
git add requirements.txt pyproject.toml
git commit -m "build: switch to async MongoDB/RabbitMQ/Kafka clients and pytest-asyncio"
```

---

### Task 2: `OrderRepository` interface + `FakeOrderRepository` → async

**Files:**
- Modify: `app/repositories/order_repository.py`
- Modify: `tests/fakes/fake_order_repository.py`
- Modify: `tests/fakes/test_fake_order_repository.py`

**Interfaces:**
- Consumes: `Order`, `OrderStatus`, `OrderNotFound` from `app.models.order` (unchanged).
- Produces: `OrderRepository` with `async def save(order) -> None`, `async def list_all() -> list[Order]`, `async def get_by_id(order_id) -> Order | None`, `async def update_status(order_id, status) -> Order`, `async def delete(order_id) -> None`. `FakeOrderRepository` implements all of them as coroutines (logic unchanged from the sync version).

- [ ] **Step 1: Update the test file to call the repository with `await`**

`tests/fakes/test_fake_order_repository.py`:
```python
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
```

- [ ] **Step 2: Run the test file to verify it fails**

Run: `pytest tests/fakes/test_fake_order_repository.py -v`
Expected: FAIL — `TypeError: object NoneType can't be used in 'await' expression` (or similar), because `FakeOrderRepository`'s methods are still synchronous and return plain values, not coroutines

- [ ] **Step 3: Update `OrderRepository` to declare async abstract methods**

`app/repositories/order_repository.py`:
```python
from __future__ import annotations

from abc import ABC, abstractmethod

from app.models.order import Order, OrderStatus


class OrderRepository(ABC):
    @abstractmethod
    async def save(self, order: Order) -> None:
        raise NotImplementedError

    @abstractmethod
    async def list_all(self) -> list[Order]:
        raise NotImplementedError

    @abstractmethod
    async def get_by_id(self, order_id: str) -> Order | None:
        raise NotImplementedError

    @abstractmethod
    async def update_status(self, order_id: str, status: OrderStatus) -> Order:
        raise NotImplementedError

    @abstractmethod
    async def delete(self, order_id: str) -> None:
        raise NotImplementedError
```

- [ ] **Step 4: Update `FakeOrderRepository` to implement the async interface**

`tests/fakes/fake_order_repository.py`:
```python
from __future__ import annotations

from app.models.order import Order, OrderNotFound, OrderStatus
from app.repositories.order_repository import OrderRepository


class FakeOrderRepository(OrderRepository):
    def __init__(self) -> None:
        self._orders: dict[str, Order] = {}

    async def save(self, order: Order) -> None:
        self._orders[order.id] = order

    async def list_all(self) -> list[Order]:
        return list(self._orders.values())

    async def get_by_id(self, order_id: str) -> Order | None:
        return self._orders.get(order_id)

    async def update_status(self, order_id: str, status: OrderStatus) -> Order:
        order = self._orders.get(order_id)
        if order is None:
            raise OrderNotFound(order_id)
        order.status = status
        return order

    async def delete(self, order_id: str) -> None:
        if order_id not in self._orders:
            raise OrderNotFound(order_id)
        del self._orders[order_id]
```

- [ ] **Step 5: Run the test file to verify it passes**

Run: `pytest tests/fakes/test_fake_order_repository.py -v`
Expected: 6 passed

- [ ] **Step 6: Commit**

```bash
git add app/repositories/order_repository.py tests/fakes/fake_order_repository.py tests/fakes/test_fake_order_repository.py
git commit -m "refactor: make OrderRepository and FakeOrderRepository async"
```

---

### Task 3: `EventPublisher` interface + `FakeEventPublisher` → async

**Files:**
- Modify: `app/publishers/event_publisher.py`
- Modify: `tests/fakes/fake_event_publisher.py`
- Modify: `tests/fakes/test_fake_event_publisher.py`

**Interfaces:**
- Consumes: `Order`, `OrderEventType` from `app.models.order` (unchanged).
- Produces: `EventPublisher` with `async def publish(order, event_type) -> None`. `FakeEventPublisher` implements it as a coroutine that appends to `self.published` (logic unchanged).

- [ ] **Step 1: Update the test file to call publish with `await`**

`tests/fakes/test_fake_event_publisher.py`:
```python
from app.models.order import Order, OrderEventType
from tests.fakes.fake_event_publisher import FakeEventPublisher


async def test_publish_records_order_and_event_type():
    publisher = FakeEventPublisher()
    order = Order.create("Maria", "Teclado", 2)

    await publisher.publish(order, OrderEventType.CREATED)

    assert publisher.published == [(order, OrderEventType.CREATED)]
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `pytest tests/fakes/test_fake_event_publisher.py -v`
Expected: FAIL — awaiting a non-coroutine return value

- [ ] **Step 3: Update `EventPublisher` to declare an async abstract method**

`app/publishers/event_publisher.py`:
```python
from abc import ABC, abstractmethod

from app.models.order import Order, OrderEventType


class EventPublisher(ABC):
    @abstractmethod
    async def publish(self, order: Order, event_type: OrderEventType) -> None:
        raise NotImplementedError
```

- [ ] **Step 4: Update `FakeEventPublisher`**

`tests/fakes/fake_event_publisher.py`:
```python
from __future__ import annotations

from app.models.order import Order, OrderEventType
from app.publishers.event_publisher import EventPublisher


class FakeEventPublisher(EventPublisher):
    def __init__(self) -> None:
        self.published: list[tuple[Order, OrderEventType]] = []

    async def publish(self, order: Order, event_type: OrderEventType) -> None:
        self.published.append((order, event_type))
```

- [ ] **Step 5: Run the test to verify it passes**

Run: `pytest tests/fakes/test_fake_event_publisher.py -v`
Expected: 1 passed

- [ ] **Step 6: Commit**

```bash
git add app/publishers/event_publisher.py tests/fakes/fake_event_publisher.py tests/fakes/test_fake_event_publisher.py
git commit -m "refactor: make EventPublisher and FakeEventPublisher async"
```

---

### Task 4: `OrderService` → async

**Files:**
- Modify: `app/services/order_service.py`
- Modify: `tests/services/test_order_service.py`

**Interfaces:**
- Consumes: `OrderRepository` (Task 2, async), `EventPublisher` (Task 3, async), `Order`, `OrderStatus`, `OrderEventType`, `OrderNotFound` from `app.models.order`, `FakeOrderRepository` (Task 2), `FakeEventPublisher` (Task 3).
- Produces: `OrderService(repository, publishers)` with `async def create_order(customer_name, product_name, quantity) -> Order`, `async def list_orders() -> list[Order]`, `async def get_order(order_id) -> Order`, `async def update_order_status(order_id, status) -> Order`, `async def delete_order(order_id) -> None`. Constructor signature is unchanged (still plain `__init__`, not async).

- [ ] **Step 1: Update the test file to call the service with `await`**

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
```

- [ ] **Step 2: Run the test file to verify it fails**

Run: `pytest tests/services/test_order_service.py -v`
Expected: FAIL — awaiting non-coroutine return values from the still-synchronous `OrderService`

- [ ] **Step 3: Update `OrderService` to be fully async**

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

    async def _publish(self, order: Order, event_type: OrderEventType) -> None:
        for publisher in self._publishers:
            await publisher.publish(order, event_type)

    async def create_order(self, customer_name: str, product_name: str, quantity: int) -> Order:
        order = Order.create(customer_name, product_name, quantity)
        await self._repository.save(order)
        await self._publish(order, OrderEventType.CREATED)
        return order

    async def list_orders(self) -> list[Order]:
        return await self._repository.list_all()

    async def get_order(self, order_id: str) -> Order:
        order = await self._repository.get_by_id(order_id)
        if order is None:
            raise OrderNotFound(order_id)
        return order

    async def update_order_status(self, order_id: str, status: OrderStatus) -> Order:
        order = await self._repository.update_status(order_id, status)
        await self._publish(order, OrderEventType.UPDATED)
        return order

    async def delete_order(self, order_id: str) -> None:
        order = await self.get_order(order_id)
        await self._repository.delete(order_id)
        await self._publish(order, OrderEventType.DELETED)
```

- [ ] **Step 4: Run the test file to verify it passes**

Run: `pytest tests/services/test_order_service.py -v`
Expected: 8 passed

- [ ] **Step 5: Commit**

```bash
git add app/services/order_service.py tests/services/test_order_service.py
git commit -m "refactor: make OrderService async"
```

---

### Task 5: `MongoOrderRepository` → async with `motor` + `mongomock-motor`

**Files:**
- Modify: `app/repositories/mongo_order_repository.py`
- Modify: `tests/repositories/test_mongo_order_repository.py`

**Interfaces:**
- Consumes: `OrderRepository` (Task 2, async), `Order`, `OrderStatus`, `OrderNotFound` from `app.models.order`. Uses `motor.motor_asyncio.AsyncIOMotorCollection` and `pymongo.ReturnDocument` (still part of `motor`'s dependency chain — `motor` depends on `pymongo` internally for shared types like `ReturnDocument`).
- Produces: `MongoOrderRepository(collection: AsyncIOMotorCollection)` implementing all 5 `OrderRepository` methods as coroutines.

- [ ] **Step 1: Update the test file to use `mongomock-motor` and `await`**

`tests/repositories/test_mongo_order_repository.py`:
```python
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
```

- [ ] **Step 2: Run the test file to verify it fails**

Run: `pytest tests/repositories/test_mongo_order_repository.py -v`
Expected: FAIL — `MongoOrderRepository`'s methods are still synchronous, awaiting their (non-coroutine) return values raises `TypeError`

- [ ] **Step 3: Update `MongoOrderRepository` to use `motor`**

`app/repositories/mongo_order_repository.py`:
```python
from __future__ import annotations

from motor.motor_asyncio import AsyncIOMotorCollection
from pymongo import ReturnDocument

from app.models.order import Order, OrderNotFound, OrderStatus
from app.repositories.order_repository import OrderRepository


class MongoOrderRepository(OrderRepository):
    def __init__(self, collection: AsyncIOMotorCollection) -> None:
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

    async def save(self, order: Order) -> None:
        await self._collection.insert_one(self._to_document(order))

    async def list_all(self) -> list[Order]:
        return [self._to_order(doc) async for doc in self._collection.find()]

    async def get_by_id(self, order_id: str) -> Order | None:
        document = await self._collection.find_one({"_id": order_id})
        return self._to_order(document) if document else None

    async def update_status(self, order_id: str, status: OrderStatus) -> Order:
        document = await self._collection.find_one_and_update(
            {"_id": order_id},
            {"$set": {"status": status.value}},
            return_document=ReturnDocument.AFTER,
        )
        if document is None:
            raise OrderNotFound(order_id)
        return self._to_order(document)

    async def delete(self, order_id: str) -> None:
        result = await self._collection.delete_one({"_id": order_id})
        if result.deleted_count == 0:
            raise OrderNotFound(order_id)
```

- [ ] **Step 4: Run the test file to verify it passes**

Run: `pytest tests/repositories/test_mongo_order_repository.py -v`
Expected: 7 passed

If `find_one_and_update` with `return_document=ReturnDocument.AFTER` does not behave as expected under `mongomock-motor`, report this as a concern in your report (status `DONE_WITH_CONCERNS`) rather than silently working around it — the controller needs to know if the testing library has a gap.

- [ ] **Step 5: Commit**

```bash
git add app/repositories/mongo_order_repository.py tests/repositories/test_mongo_order_repository.py
git commit -m "refactor: migrate MongoOrderRepository from pymongo to motor"
```

---

### Task 6: `RabbitMQPublisher` → async with `aio-pika`

**Files:**
- Modify: `app/publishers/rabbitmq_publisher.py`
- Modify: `tests/publishers/test_rabbitmq_publisher.py`

**Interfaces:**
- Consumes: `EventPublisher` (Task 3, async), `Order`, `OrderEventType` from `app.models.order`.
- Produces: `RabbitMQPublisher(channel, queue_name: str = "orders")` (plain, non-async constructor) plus an async factory `await RabbitMQPublisher.create(channel, queue_name: str = "orders") -> RabbitMQPublisher` that declares the queue and returns the instance. `publish(order, event_type)` is now `async def`.

`aio_pika` cannot declare a queue inside a synchronous `__init__` (declaring a queue is a network round-trip and must be awaited), so queue declaration moves to the `create` classmethod. Callers (Task 8) must use `await RabbitMQPublisher.create(...)` instead of `RabbitMQPublisher(...)`.

- [ ] **Step 1: Update the test file for the async factory and `await publish`**

`tests/publishers/test_rabbitmq_publisher.py`:
```python
import json
from unittest.mock import AsyncMock, MagicMock

from app.models.order import Order, OrderEventType
from app.publishers.rabbitmq_publisher import RabbitMQPublisher


async def test_create_declares_queue():
    channel = MagicMock()
    channel.declare_queue = AsyncMock()

    await RabbitMQPublisher.create(channel, queue_name="orders")

    channel.declare_queue.assert_called_once_with("orders", durable=True)


async def test_publish_sends_json_payload_with_order_identification():
    channel = MagicMock()
    channel.declare_queue = AsyncMock()
    channel.default_exchange = MagicMock()
    channel.default_exchange.publish = AsyncMock()
    publisher = await RabbitMQPublisher.create(channel, queue_name="orders")
    order = Order.create("Maria", "Teclado", 2)

    await publisher.publish(order, OrderEventType.CREATED)

    channel.default_exchange.publish.assert_called_once()
    args, kwargs = channel.default_exchange.publish.call_args
    message = args[0]
    assert kwargs["routing_key"] == "orders"
    payload = json.loads(message.body)
    assert payload == {
        "event_type": "CREATED",
        "order_id": order.id,
        "status": "PENDENTE",
    }
```

- [ ] **Step 2: Run the test file to verify it fails**

Run: `pytest tests/publishers/test_rabbitmq_publisher.py -v`
Expected: FAIL — `RabbitMQPublisher.create` does not exist yet (`AttributeError`)

- [ ] **Step 3: Update `RabbitMQPublisher` to use `aio-pika`**

`app/publishers/rabbitmq_publisher.py`:
```python
import json

import aio_pika

from app.models.order import Order, OrderEventType
from app.publishers.event_publisher import EventPublisher


class RabbitMQPublisher(EventPublisher):
    def __init__(self, channel, queue_name: str = "orders") -> None:
        self._channel = channel
        self._queue_name = queue_name

    @classmethod
    async def create(cls, channel, queue_name: str = "orders") -> "RabbitMQPublisher":
        await channel.declare_queue(queue_name, durable=True)
        return cls(channel, queue_name)

    async def publish(self, order: Order, event_type: OrderEventType) -> None:
        message = {
            "event_type": event_type.value,
            "order_id": order.id,
            "status": order.status.value,
        }
        await self._channel.default_exchange.publish(
            aio_pika.Message(body=json.dumps(message).encode("utf-8")),
            routing_key=self._queue_name,
        )
```

- [ ] **Step 4: Run the test file to verify it passes**

Run: `pytest tests/publishers/test_rabbitmq_publisher.py -v`
Expected: 2 passed

- [ ] **Step 5: Commit**

```bash
git add app/publishers/rabbitmq_publisher.py tests/publishers/test_rabbitmq_publisher.py
git commit -m "refactor: migrate RabbitMQPublisher from pika to aio-pika"
```

---

### Task 7: `KafkaPublisher` → async with `aiokafka`

**Files:**
- Modify: `app/publishers/kafka_publisher.py`
- Modify: `tests/publishers/test_kafka_publisher.py`

**Interfaces:**
- Consumes: `EventPublisher` (Task 3, async), `Order`, `OrderEventType` from `app.models.order`.
- Produces: `KafkaPublisher(producer, topic: str = "orders")` (constructor unchanged) with `async def publish(order, event_type) -> None`, implemented via `await producer.send_and_wait(topic, body)` (replaces the old `send()` + `flush()` pair — `send_and_wait` is `aiokafka`'s single-call equivalent that awaits delivery).

- [ ] **Step 1: Update the test file for `await publish` and `send_and_wait`**

`tests/publishers/test_kafka_publisher.py`:
```python
import json
from unittest.mock import AsyncMock

from app.models.order import Order, OrderEventType
from app.publishers.kafka_publisher import KafkaPublisher


async def test_publish_sends_full_event_payload_to_topic():
    producer = AsyncMock()
    publisher = KafkaPublisher(producer, topic="orders")
    order = Order.create("Maria", "Teclado", 2)

    await publisher.publish(order, OrderEventType.CREATED)

    producer.send_and_wait.assert_called_once()
    args, _kwargs = producer.send_and_wait.call_args
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
```

- [ ] **Step 2: Run the test file to verify it fails**

Run: `pytest tests/publishers/test_kafka_publisher.py -v`
Expected: FAIL — `KafkaPublisher.publish` is still synchronous and calls `producer.send`/`producer.flush`, not `producer.send_and_wait`

- [ ] **Step 3: Update `KafkaPublisher` to use `aiokafka`**

`app/publishers/kafka_publisher.py`:
```python
import json

from app.models.order import Order, OrderEventType
from app.publishers.event_publisher import EventPublisher


class KafkaPublisher(EventPublisher):
    def __init__(self, producer, topic: str = "orders") -> None:
        self._producer = producer
        self._topic = topic

    async def publish(self, order: Order, event_type: OrderEventType) -> None:
        message = {
            "event_type": event_type.value,
            "order_id": order.id,
            "customer_name": order.customer_name,
            "product_name": order.product_name,
            "quantity": order.quantity,
            "status": order.status.value,
        }
        await self._producer.send_and_wait(
            self._topic, json.dumps(message).encode("utf-8")
        )
```

- [ ] **Step 4: Run the test file to verify it passes**

Run: `pytest tests/publishers/test_kafka_publisher.py -v`
Expected: 1 passed

- [ ] **Step 5: Commit**

```bash
git add app/publishers/kafka_publisher.py tests/publishers/test_kafka_publisher.py
git commit -m "refactor: migrate KafkaPublisher from kafka-python to aiokafka"
```

---

### Task 8: Wiring (`dependencies.py`, `main.py`, `order_router.py`) → async

**Files:**
- Modify: `app/dependencies.py`
- Modify: `app/main.py`
- Modify: `app/routers/order_router.py`

**Interfaces:**
- Consumes: `OrderService` (Task 4, async), `MongoOrderRepository` (Task 5, async), `RabbitMQPublisher.create` (Task 6), `KafkaPublisher` (Task 7), `OrderCreate`/`OrderStatusUpdate`/`OrderResponse` (unchanged), `get_settings` (unchanged, in `app/config.py`).
- Produces: FastAPI `app` exposing the same 5 endpoints as before, now `async def`. `get_order_service` in `app.dependencies` is now `async def` — FastAPI resolves async dependencies natively, and `tests/conftest.py`'s override (`app.dependency_overrides[get_order_service] = lambda: OrderService(repo, publishers)`, a **synchronous** lambda) keeps working unmodified: FastAPI inspects the override callable itself, sees it is sync, and calls it directly without awaiting.

This task does **not** touch `tests/conftest.py` or any file in `tests/api/` — they already work unchanged against async endpoints.

- [ ] **Step 1: Update `app/dependencies.py`**

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


async def get_event_publishers(request: Request) -> list[EventPublisher]:
    channel = await request.app.state.rabbitmq_connection.channel()
    rabbitmq_publisher = await RabbitMQPublisher.create(channel)
    kafka_publisher = KafkaPublisher(request.app.state.kafka_producer)
    return [rabbitmq_publisher, kafka_publisher]


async def get_order_service(request: Request) -> OrderService:
    return OrderService(
        get_order_repository(request),
        await get_event_publishers(request),
    )
```

- [ ] **Step 2: Update `app/main.py`**

```python
from contextlib import asynccontextmanager

import aio_pika
from aiokafka import AIOKafkaProducer
from fastapi import FastAPI
from motor.motor_asyncio import AsyncIOMotorClient

from app.config import get_settings
from app.routers.order_router import router as order_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    app.state.settings = settings
    app.state.mongo_client = AsyncIOMotorClient(settings.mongo_uri)
    app.state.rabbitmq_connection = await aio_pika.connect_robust(settings.rabbitmq_url)
    app.state.kafka_producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers
    )
    await app.state.kafka_producer.start()
    yield
    app.state.mongo_client.close()
    await app.state.rabbitmq_connection.close()
    await app.state.kafka_producer.stop()


app = FastAPI(title="Order Management API", lifespan=lifespan)
app.include_router(order_router)
```

- [ ] **Step 3: Update `app/routers/order_router.py`**

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
async def create_order(
    payload: OrderCreate, service: OrderService = Depends(get_order_service)
) -> OrderResponse:
    order = await service.create_order(
        payload.customer_name, payload.product_name, payload.quantity
    )
    return _to_response(order)


@router.get("", response_model=list[OrderResponse])
async def list_orders(
    service: OrderService = Depends(get_order_service),
) -> list[OrderResponse]:
    return [_to_response(order) for order in await service.list_orders()]


@router.get("/{order_id}", response_model=OrderResponse)
async def get_order(
    order_id: str, service: OrderService = Depends(get_order_service)
) -> OrderResponse:
    try:
        order = await service.get_order(order_id)
    except OrderNotFound:
        raise HTTPException(status_code=404, detail="Order not found")
    return _to_response(order)


@router.patch("/{order_id}/status", response_model=OrderResponse)
async def update_order_status(
    order_id: str,
    payload: OrderStatusUpdate,
    service: OrderService = Depends(get_order_service),
) -> OrderResponse:
    try:
        order = await service.update_order_status(order_id, payload.status)
    except OrderNotFound:
        raise HTTPException(status_code=404, detail="Order not found")
    return _to_response(order)


@router.delete("/{order_id}", status_code=204)
async def delete_order(
    order_id: str, service: OrderService = Depends(get_order_service)
) -> None:
    try:
        await service.delete_order(order_id)
    except OrderNotFound:
        raise HTTPException(status_code=404, detail="Order not found")
```

- [ ] **Step 4: Run the full suite to verify everything passes together**

Run: `pytest -v`
Expected: 45 passed (`tests/api/*` and `tests/conftest.py` untouched, still synchronous, still passing against the now-async endpoints)

- [ ] **Step 5: Commit**

```bash
git add app/dependencies.py app/main.py app/routers/order_router.py
git commit -m "refactor: wire async dependencies, lifespan, and router endpoints"
```

---

### Task 9: Final verification (Docker smoke test + README + commit)

**Files:**
- Modify: `README.md`

**Interfaces:**
- Consumes: nothing new — this task verifies the full async stack end-to-end and documents the change.

- [ ] **Step 1: Run the full pytest suite one more time**

Run: `pytest -v`
Expected: 45 passed, 0 failed, output pristine (no warnings)

- [ ] **Step 2: Rebuild and smoke-test the Docker Compose stack**

`docker-compose.yml` itself does not change, but the Docker image must be rebuilt since `requirements.txt` changed.

Run: `docker-compose up --build -d`
Wait for containers to become healthy (RabbitMQ/Kafka may take a minute; the `api` service already has `restart: on-failure` from the original setup to absorb the startup race), then:

Run: `curl -X POST http://localhost:8000/orders -H "Content-Type: application/json" -d "{\"customer_name\":\"Maria\",\"product_name\":\"Teclado\",\"quantity\":2}"`
Expected: HTTP 201 with a JSON body containing `"status":"PENDENTE"`

Run: `curl http://localhost:8000/orders`
Expected: HTTP 200 with a JSON array containing the order created above

Run: `docker-compose down`

- [ ] **Step 3: Update `README.md`'s Stack section**

In the `## Stack` section, replace:

```markdown
- **Persistência:** MongoDB (`pymongo`)
- **Mensageria:** RabbitMQ (`pika`) e Kafka (`kafka-python`)
```

with:

```markdown
- **Persistência:** MongoDB (`motor`, assíncrono)
- **Mensageria:** RabbitMQ (`aio-pika`) e Kafka (`aiokafka`), ambos assíncronos
```

- [ ] **Step 4: Commit**

```bash
git add README.md
git commit -m "docs: update README to reflect async MongoDB/RabbitMQ/Kafka clients"
```
