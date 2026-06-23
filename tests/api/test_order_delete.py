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
