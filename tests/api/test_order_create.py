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


async def test_create_order_persists_and_publishes_created_event(api):
    client, repo, publishers = api

    response = client.post(
        "/orders",
        json={"customer_name": "Maria", "product_name": "Teclado", "quantity": 2},
    )
    order_id = response.json()["id"]

    assert await repo.get_by_id(order_id) is not None
    for publisher in publishers:
        assert publisher.published[-1][1] == OrderEventType.CREATED


def test_create_order_rejects_invalid_quantity(api):
    client, _repo, _publishers = api

    response = client.post(
        "/orders",
        json={"customer_name": "Maria", "product_name": "Teclado", "quantity": 0},
    )

    assert response.status_code == 422
