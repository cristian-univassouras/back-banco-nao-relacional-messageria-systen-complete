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
