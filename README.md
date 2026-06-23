<div align="center">

# API de Gerenciamento de Pedidos

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

Prova acadêmica - P2 (Banco de Dados Não Relacional — Universidade Vassouras)
que simula a modernização de parte da arquitetura de pedidos de um
e-commerce: persistência em um banco NoSQL e comunicação assíncrona com
sistemas externos via mensageria.

Cada pedido tem: id único, nome do cliente, nome do produto, quantidade e
status (inicia sempre como `PENDENTE`). Ao criar, atualizar o status ou
remover um pedido, um evento correspondente é publicado tanto no RabbitMQ
quanto no Kafka.

## Arquitetura

Camadas com inversão de dependência: `Router → Service → Repository/Publisher`.
`OrderRepository` e `EventPublisher` são interfaces abstratas (com métodos
`async def`) — a camada de serviço nunca importa `motor`/`aio-pika`/`aiokafka`
diretamente, apenas as implementações concretas (`MongoOrderRepository`,
`RabbitMQPublisher`, `KafkaPublisher`) as conhecem. Todo o I/O de rede
(MongoDB, RabbitMQ, Kafka) é assíncrono e não-bloqueante de ponta a ponta.

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

Estrutura completa do projeto:

```
.
├── .dockerignore
├── .env.example
├── .gitignore
├── Dockerfile
├── README.md
├── docker-compose.yml
├── pyproject.toml
├── requirements.txt
├── app/
│   ├── __init__.py
│   ├── config.py
│   ├── dependencies.py
│   ├── main.py
│   ├── models/
│   │   ├── __init__.py
│   │   └── order.py
│   ├── publishers/
│   │   ├── __init__.py
│   │   ├── event_publisher.py
│   │   ├── kafka_publisher.py
│   │   └── rabbitmq_publisher.py
│   ├── repositories/
│   │   ├── __init__.py
│   │   ├── mongo_order_repository.py
│   │   └── order_repository.py
│   ├── routers/
│   │   ├── __init__.py
│   │   └── order_router.py
│   ├── schemas/
│   │   ├── __init__.py
│   │   └── order.py
│   └── services/
│       ├── __init__.py
│       └── order_service.py
└── tests/
    ├── __init__.py
    ├── conftest.py
    ├── api/
    │   ├── __init__.py
    │   ├── test_order_create.py
    │   ├── test_order_delete.py
    │   ├── test_order_get.py
    │   ├── test_order_list.py
    │   └── test_order_update.py
    ├── fakes/
    │   ├── __init__.py
    │   ├── fake_event_publisher.py
    │   ├── fake_order_repository.py
    │   ├── test_fake_event_publisher.py
    │   └── test_fake_order_repository.py
    ├── models/
    │   ├── __init__.py
    │   └── test_order.py
    ├── publishers/
    │   ├── __init__.py
    │   ├── test_kafka_publisher.py
    │   └── test_rabbitmq_publisher.py
    ├── repositories/
    │   ├── __init__.py
    │   └── test_mongo_order_repository.py
    ├── schemas/
    │   ├── __init__.py
    │   └── test_order_schemas.py
    └── services/
        ├── __init__.py
        └── test_order_service.py
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

45 testes (rodando via `pytest-asyncio`), todos usando fakes em memória para o
repositório e os publishers — não dependem de MongoDB, RabbitMQ ou Kafka
rodando.

## Variáveis de ambiente

Copie `.env.example` para `.env` se quiser rodar a API localmente (fora do
Docker), apontando para os serviços expostos pelo `docker-compose` ou
rodando localmente nas mesmas portas:

```bash
cp .env.example .env
```

Dentro do `docker-compose up`, as variáveis já são definidas direto no
`docker-compose.yml` — o `.env` não é necessário nesse fluxo.

## Tecnologias

- **API:** FastAPI + Pydantic v2
- **Persistência:** MongoDB (`motor`, assíncrono)
- **Mensageria:** RabbitMQ (`aio-pika`) e Kafka (`aiokafka`), ambos assíncronos
- **Testes:** Pytest, `pytest-asyncio`, `httpx` (TestClient), `mongomock-motor`
- **Conteinerização:** Docker + Docker Compose
