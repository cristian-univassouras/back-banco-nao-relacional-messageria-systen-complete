# Design: Migração para I/O Assíncrono (motor / aio-pika / aiokafka)

## Contexto

O projeto já implementa o requisito de **comunicação assíncrona entre sistemas** (RabbitMQ + Kafka, pub/sub desacoplado — ver `CONTEXTO.md` e `docs/superpowers/specs/2026-06-23-order-management-api-design.md`). Esta migração trata de um requisito diferente, mais estrito, presente em `CHECKLIST.md`: **I/O assíncrono dentro do código** — a thread da requisição HTTP não deve ficar bloqueada esperando o MongoDB/RabbitMQ/Kafka responderem.

Hoje a stack é 100% síncrona: `pymongo`, `pika`, `kafka-python`, endpoints `def` (não `async def`). Esta migração troca essas bibliotecas pelas equivalentes assíncronas, mantendo intacta a arquitetura em camadas (Router → Service → Repository/Publisher) e os contratos de interface já definidos (`OrderRepository`, `EventPublisher`).

## Decisões

- **MongoDB:** `pymongo` → `motor` (`AsyncIOMotorClient`/`AsyncIOMotorCollection`).
- **RabbitMQ:** `pika` → `aio-pika`.
- **Kafka:** `kafka-python` → `aiokafka`.
- **Teste do Mongo:** `mongomock` → `mongomock-motor` (mantém o princípio de não precisar de MongoDB real para os testes de `MongoOrderRepository`).
- **Runner async no Pytest:** `pytest-asyncio` com `asyncio_mode = "auto"` no `pyproject.toml` — testes `async def` rodam sem precisar de decorator manual.
- A comunicação assíncrona **entre sistemas** (RabbitMQ/Kafka pub/sub) não muda — esta migração afeta apenas o I/O **dentro** do processo Python.

## Mudanças por arquivo

| Arquivo | Mudança |
|---|---|
| `app/repositories/order_repository.py` | Todos os métodos abstratos viram `async def`. |
| `app/publishers/event_publisher.py` | `publish` vira `async def`. |
| `tests/fakes/fake_order_repository.py` | Métodos viram `async def` (lógica idêntica). |
| `tests/fakes/fake_event_publisher.py` | `publish` vira `async def`. |
| `app/repositories/mongo_order_repository.py` | Usa `AsyncIOMotorCollection`; todas as chamadas (`insert_one`, `find`, `find_one_and_update`, `delete_one`) ganham `await`. `list_all` itera o cursor assíncrono (`async for doc in self._collection.find(): ...`). |
| `app/publishers/rabbitmq_publisher.py` | Construtor síncrono não pode declarar fila com `await`; introduz `classmethod async create(channel, queue_name) -> RabbitMQPublisher` que faz `await channel.declare_queue(...)` e retorna a instância. `publish()` vira `async def`, usando `await channel.default_exchange.publish(aio_pika.Message(body), routing_key=queue_name)`. |
| `app/publishers/kafka_publisher.py` | Usa `AIOKafkaProducer`; `publish()` vira `await self._producer.send_and_wait(topic, body)` (substitui `send()` + `flush()`). |
| `app/services/order_service.py` | Os 5 métodos viram `async def`; `_publish` faz `for publisher in self._publishers: await publisher.publish(...)`. |
| `app/routers/order_router.py` | Os 5 endpoints viram `async def`, com `await service.xxx(...)`. |
| `app/dependencies.py` | `get_event_publishers` e `get_order_service` viram `async def` (`get_order_repository` permanece síncrono — criar `MongoOrderRepository` a partir de uma collection não faz I/O). FastAPI resolve `Depends` assíncronos nativamente. |
| `app/main.py` (`lifespan`) | `AsyncIOMotorClient` não precisa de `await` na criação (conexão é lazy). RabbitMQ: `await aio_pika.connect_robust(settings.rabbitmq_url)`. Kafka: `AIOKafkaProducer(...)` seguido de `await producer.start()`; no shutdown, `await producer.stop()` e `await rabbitmq_connection.close()`. |
| `requirements.txt` | Remove `pymongo`, `pika`, `kafka-python`, `mongomock`; adiciona `motor`, `aio-pika`, `aiokafka`, `mongomock-motor`, `pytest-asyncio`. |
| `pyproject.toml` | Adiciona `asyncio_mode = "auto"` em `[tool.pytest.ini_options]`. |

## Testes

- **Unitários** (`tests/models`, `tests/schemas`, `tests/fakes`, `tests/services`, `tests/repositories`, `tests/publishers`): viram `async def test_...`, executados automaticamente via `pytest-asyncio` (`asyncio_mode = "auto"`), sem decorators manuais.
- **`MongoOrderRepository`**: testado com `mongomock_motor.AsyncMongoMockClient`, mesmo papel do `mongomock` de hoje — sem precisar de MongoDB real.
- **`RabbitMQPublisher`/`KafkaPublisher`**: mocks (`unittest.mock.AsyncMock`) no lugar do canal/produtor, mesma estratégia de hoje, só que assíncrona.
- **API (`tests/api/*`)**: **sem mudança** — o `TestClient` do FastAPI já executa endpoints `async def` internamente; os testes continuam `def test_...` síncronos chamando `client.post(...)` etc. `tests/conftest.py`'s `api` fixture continua igual.
- Resultado esperado: mesma contagem de testes (45), mesma cobertura, todos passando.

## Docker / Compose

Nenhuma mudança nos serviços do `docker-compose.yml` (Mongo, RabbitMQ, Kafka, Zookeeper continuam iguais) — a migração é só no lado do cliente Python. `requirements.txt` atualizado já é suficiente; o `Dockerfile` não muda.

## Fora de escopo

- Resolver o item pendente do `CHECKLIST.md` sobre volume do MongoDB (tratado separadamente, fora desta migração).
- Mudar o padrão de mensageria (RabbitMQ/Kafka) em si — só a forma como o código espera pelas respostas de rede.
- Otimizações de pool/reuso de conexão além do que já existia na versão síncrona (ex.: continuar abrindo um canal RabbitMQ por requisição, agora de forma assíncrona).
