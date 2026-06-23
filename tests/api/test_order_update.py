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
