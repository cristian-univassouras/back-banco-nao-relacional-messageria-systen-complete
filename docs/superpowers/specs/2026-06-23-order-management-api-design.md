# Design: API de Gerenciamento de Pedidos (MongoDB + RabbitMQ + Kafka)

## Contexto

Ver `CONTEXTO.md` para o enunciado completo do trabalho. Resumo: API FastAPI para gerenciar pedidos, persistindo no MongoDB e publicando eventos no RabbitMQ e Kafka quando um pedido é criado. O enunciado exige no mínimo cadastro e listagem; este design expande para um CRUD completo (criar, listar, buscar por id, atualizar status, remover), publicando eventos em todas as mudanças de estado.

## Decisões de arquitetura

- **Arquitetura em camadas**: Router (FastAPI) → Service → Repository/Publisher.
- **Inversão de dependência**: Service depende de interfaces (`OrderRepository`, `EventPublisher`), não de implementações concretas (MongoDB, RabbitMQ, Kafka).
- **Strategy Pattern para mensageria**: `EventPublisher` é uma interface única; `RabbitMQPublisher` e `KafkaPublisher` são estratégias intercambiáveis injetadas como lista no Service.
- **Repository Pattern**: `OrderRepository` abstrai a persistência; `MongoOrderRepository` é a única implementação (MongoDB é fixo pelo requisito, mas a interface facilita testes).
- **Modelo de domínio separado de DTOs de API**: entidade `Order` (interna) é distinta de `OrderCreate`/`OrderResponse` (contratos HTTP).
- **Estrutura de pastas por camada** (não por feature), já que o projeto tem um único recurso (Pedido).

## Fluxo de dados

### Criação (`POST /orders`)
1. Router valida `OrderCreate` (customer_name, product_name, quantity).
2. Router chama `OrderService.create_order(dto)`.
3. Service cria `Order` via `Order.create(...)` — gera `id` (UUID4) e força `status = PENDENTE`.
4. Service chama `OrderRepository.save(order)` → persiste no MongoDB.
5. Service chama `publish(order, OrderEventType.CREATED)` em cada `EventPublisher` (RabbitMQ e Kafka).
6. Router retorna `OrderResponse`.

### Listagem (`GET /orders`)
1. Service chama `OrderRepository.list_all()`.
2. Router converte para `list[OrderResponse]`.

### Busca por id (`GET /orders/{id}`)
1. Service chama `OrderRepository.get_by_id(id)`.
2. Se não encontrado, lança `OrderNotFound` → Router responde 404.

### Atualização de status (`PATCH /orders/{id}/status`)
1. Router valida `OrderStatusUpdate` (novo `status`).
2. Service busca o pedido (`OrderNotFound` se ausente), chama `OrderRepository.update_status(id, status)`.
3. Service publica `OrderEventType.UPDATED` em cada `EventPublisher`.

### Remoção (`DELETE /orders/{id}`)
1. Service busca o pedido (`OrderNotFound` se ausente).
2. Service chama `OrderRepository.delete(id)`.
3. Service publica `OrderEventType.DELETED` em cada `EventPublisher` (com os dados do pedido removido).

## Componentes

| Componente | Local | Responsabilidade |
|---|---|---|
| `Order`, `OrderStatus`, `OrderEventType`, `OrderNotFound` | `app/models/order.py` | Entidade de domínio. `Order.create(...)` garante `id` único e `status = PENDENTE`. `OrderStatus`: `PENDENTE`, `ENVIADO`, `ENTREGUE`, `CANCELADO`. |
| `OrderCreate`, `OrderStatusUpdate`, `OrderResponse` | `app/schemas/order.py` | DTOs de entrada/saída da API (Pydantic). |
| `OrderRepository` (ABC) | `app/repositories/order_repository.py` | Interface: `save`, `list_all`, `get_by_id`, `update_status`, `delete`. |
| `MongoOrderRepository` | `app/repositories/mongo_order_repository.py` | Implementação com MongoDB, mapeando `Order` ↔ documento da coleção `orders`. |
| `EventPublisher` (ABC) | `app/publishers/event_publisher.py` | Interface: `publish(order, event_type)`. |
| `RabbitMQPublisher` | `app/publishers/rabbitmq_publisher.py` | Publica mensagem mínima (id, status, event_type) em fila RabbitMQ. |
| `KafkaPublisher` | `app/publishers/kafka_publisher.py` | Publica evento completo em tópico Kafka. |
| `OrderService` | `app/services/order_service.py` | Orquestra: `create_order`, `list_orders`, `get_order`, `update_order_status`, `delete_order`. Recebe `OrderRepository` e `list[EventPublisher]` via injeção. |
| `order_router` | `app/routers/order_router.py` | Endpoints HTTP, conversão DTO ↔ domínio, tradução de exceções de domínio em `HTTPException`. |
| `dependencies.py` | `app/dependencies.py` | Composition root: factories `get_order_repository`, `get_event_publishers`, `get_order_service` usadas com `Depends`. |
| `config.py` | `app/config.py` | `Settings` (Pydantic `BaseSettings`) lendo `MONGO_URI`, `RABBITMQ_URL`, `KAFKA_BOOTSTRAP_SERVERS` de variáveis de ambiente. |
| `main.py` | `app/main.py` | Cria a app FastAPI, inclui o router. |

## Estrutura de pastas

```
app/
├── main.py
├── config.py
├── dependencies.py
├── models/
│   └── order.py
├── schemas/
│   └── order.py
├── repositories/
│   ├── order_repository.py
│   └── mongo_order_repository.py
├── publishers/
│   ├── event_publisher.py
│   ├── rabbitmq_publisher.py
│   └── kafka_publisher.py
├── services/
│   └── order_service.py
└── routers/
    └── order_router.py

tests/
├── conftest.py
├── fakes/
│   ├── fake_order_repository.py
│   └── fake_event_publisher.py
├── test_order_create.py
├── test_order_list.py
├── test_order_get.py
├── test_order_update.py
└── test_order_delete.py

Dockerfile
docker-compose.yml
requirements.txt
CONTEXTO.md
```

## Tratamento de erros

- `OrderNotFound` (exceção de domínio) é lançada pelo `OrderService`/`OrderRepository` quando um id não existe.
- O Router captura `OrderNotFound` e responde `404 Not Found`.
- Erros de validação de entrada (ex.: `quantity <= 0`, `status` inválido) são responsabilidade do Pydantic nos schemas, resultando em `422` automaticamente pelo FastAPI.

## Estratégia de testes

Como `OrderRepository` e `EventPublisher` são interfaces, os testes substituem as implementações reais por fakes em memória via `app.dependency_overrides` do FastAPI — nenhum teste depende de MongoDB/RabbitMQ/Kafka rodando.

- `test_order_create.py`: `POST /orders` → 201, `status == "PENDENTE"`, `id` presente; `FakeOrderRepository` recebeu o pedido; ambos os `FakeEventPublisher` receberam `publish(order, CREATED)`.
- `test_order_list.py`: popula o fake repository, `GET /orders` retorna todos os pedidos.
- `test_order_get.py`: `GET /orders/{id}` retorna o pedido existente; `GET /orders/{id_inexistente}` retorna 404.
- `test_order_update.py`: `PATCH /orders/{id}/status` altera o status; fakes recebem evento `UPDATED`; id inexistente retorna 404.
- `test_order_delete.py`: `DELETE /orders/{id}` remove o pedido; fakes recebem evento `DELETED`; id inexistente retorna 404.

## Conteinerização

`docker-compose.yml` sobe, com um único comando (`docker-compose up`): FastAPI (build local via `Dockerfile`), MongoDB, RabbitMQ, Kafka e Zookeeper. A API lê endereços/portas desses serviços via variáveis de ambiente (`config.py`), com hostnames resolvidos pela rede interna do Compose (ex.: `mongo`, `rabbitmq`, `kafka`).

## Fora de escopo

- Autenticação/autorização.
- Validação de transição de status (ex.: impedir voltar de `ENTREGUE` para `PENDENTE`) — não exigida pelo enunciado.
- Atualização de campos além do status (nome do cliente, produto, quantidade) — decisão explícita de manter o update restrito ao ciclo de vida do pedido.
