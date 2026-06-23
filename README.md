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
