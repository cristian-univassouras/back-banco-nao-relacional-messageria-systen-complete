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
