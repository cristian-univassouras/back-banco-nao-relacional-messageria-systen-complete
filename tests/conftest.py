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
