<div align="center">

# Order Management API

API para gerenciamento de pedidos com persistência em MongoDB e publicação
de eventos em RabbitMQ e Kafka.

![Python](https://img.shields.io/badge/python-3.11%2B-blue)
![FastAPI](https://img.shields.io/badge/FastAPI-009688?logo=fastapi&logoColor=white)
![MongoDB](https://img.shields.io/badge/MongoDB-47A248?logo=mongodb&logoColor=white)
![RabbitMQ](https://img.shields.io/badge/RabbitMQ-FF6600?logo=rabbitmq&logoColor=white)
![Kafka](https://img.shields.io/badge/Apache%20Kafka-231F20?logo=apachekafka&logoColor=white)
![Tests](https://img.shields.io/badge/tests-45%20passing-brightgreen)

</div>

## Sobre o projeto

Trabalho acadêmico (Banco de Dados Não Relacional — Universidade Vassouras)
que simula a modernização de parte da arquitetura de pedidos de um
e-commerce: persistência em um banco NoSQL e comunicação assíncrona com
sistemas externos via mensageria. O enunciado completo está em
[`CONTEXTO.md`](CONTEXTO.md); o racional de arquitetura está em
[`docs/superpowers/specs/2026-06-23-order-management-api-design.md`](docs/superpowers/specs/2026-06-23-order-management-api-design.md).

Cada pedido tem: id único, nome do cliente, nome do produto, quantidade e
status (inicia sempre como `PENDENTE`). Ao criar, atualizar o status ou
remover um pedido, um evento correspondente é publicado tanto no RabbitMQ
quanto no Kafka.

## Arquitetura

Camadas com inversão de dependência: `Router → Service → Repository/Publisher`.
`OrderRepository` e `EventPublisher` são interfaces abstratas — a camada de
serviço nunca importa `pymongo`/`pika`/`kafka-python` diretamente, apenas as
implementações concretas (`MongoOrderRepository`, `RabbitMQPublisher`,
`KafkaPublisher`) as conhecem.

```
app/
├── models/        # entidade de dominio Order, OrderStatus, OrderEventType
├── schemas/       # DTOs Pydantic (OrderCreate, OrderResponse, ...)
├── repositories/  # OrderRepository (interface) + MongoOrderRepository
├── publishers/    # EventPublisher (interface) + RabbitMQ/Kafka
├── services/      # OrderService (regra de negocio)
├── routers/       # endpoints FastAPI
├── dependencies.py
├── config.py
└── main.py
```

## Como executar

Requer [Docker](https://www.docker.com/) e Docker Compose instalados.

```bash
docker-compose up --build
```

Um único comando sobe todos os serviços: FastAPI, MongoDB, RabbitMQ, Kafka e
Zookeeper. A API fica disponível em `http://localhost:8000`, com documentação
interativa em `http://localhost:8000/docs`.

> Se o seu Docker usa o plugin moderno (Compose v2), use `docker compose up --build` (com espaço) em vez de `docker-compose`.

## Endpoints

| Método | Rota                     | Descrição                          |
|--------|---------------------------|-------------------------------------|
| POST   | `/orders`                 | Cria um pedido (status inicial: `PENDENTE`) |
| GET    | `/orders`                 | Lista todos os pedidos              |
| GET    | `/orders/{id}`             | Busca um pedido por id              |
| PATCH  | `/orders/{id}/status`      | Atualiza o status de um pedido      |
| DELETE | `/orders/{id}`             | Remove um pedido                    |

## Testes

```bash
pip install -r requirements.txt
pytest -v
```

45 testes, todos usando fakes em memória para o repositório e os publishers
— não dependem de MongoDB, RabbitMQ ou Kafka rodando.

## Stack

- **API:** FastAPI + Pydantic v2
- **Persistência:** MongoDB (`motor`, assíncrono)
- **Mensageria:** RabbitMQ (`aio-pika`) e Kafka (`aiokafka`), ambos assíncronos
- **Testes:** Pytest, `pytest-asyncio`, `httpx` (TestClient), `mongomock-motor`
- **Conteinerização:** Docker + Docker Compose
