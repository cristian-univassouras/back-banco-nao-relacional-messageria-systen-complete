Aqui está um checklist estruturado para garantir que todos os requisitos mínimos da arquitetura e da aplicação foram atendidos. Você pode usar este guia para validar o projeto antes da entrega.

---

## 📋 Checklist de Validação do Projeto

### 1. 🏗️ Arquitetura e Conteinerização (`docker-compose.yml` e Dockerfiles)

* [ ] **Dockerfile:** Criado para a aplicação FastAPI (configurando Python, instalação de dependências e inicialização do Uvicorn).
* [ ] **Zookeeper:** Serviço configurado e rodando (necessário para o Kafka na arquitetura tradicional).
* [ ] **Apache Kafka:** Serviço configurado, dependendo do Zookeeper (`depends_on`), com as portas e variáveis de ambiente expostas corretamente.
* [ ] **RabbitMQ:** Serviço configurado e com as portas de comunicação (e preferencialmente do painel de gerenciamento) expostas.
* [ ] **MongoDB:** Serviço configurado com mapeamento de volume para persistência de dados.
* [ ] **FastAPI:** Serviço configurado no compose, dependendo do MongoDB, RabbitMQ e Kafka para iniciar.
* [ ] **Comando Único:** Toda a infraestrutura e a aplicação sobem com sucesso executando apenas `docker-compose up --build`.

### 2. ⚡ Desenvolvimento da API (FastAPI)

* [ ] **Modelagem do Pedido:** Schema/Modelo (Pydantic/Beanie/ODMantic) contendo:
* Identificador único (gerado automaticamente)
* Nome do cliente
* Nome do produto
* Quantidade
* Status do pedido


* [ ] **Status Inicial:** Garantido via código ou banco de dados que o status padrão ao criar é rigidamente **`PENDENTE`**.
* [ ] **Endpoint de Cadastro:** `POST /pedidos` (ou similar) recebendo os dados do cliente, produto e quantidade.
* [ ] **Endpoint de Listagem:** `GET /pedidos` retornando um array com todos os pedidos do banco de dados.

### 3. 💾 Persistência de Dados (MongoDB)

* [ ] **Conexão:** A API conecta-se com sucesso ao MongoDB usando as credenciais e variáveis de ambiente configuradas no Docker.
* [ ] **Isolamento:** Os pedidos estão sendo salvos em uma coleção (collection) específica para o recurso de pedidos.
* [ ] **Persistência Real:** O identificador único gerado é salvo no documento do banco de dados.

### 4. 📣 Mensageria e Eventos (RabbitMQ & Kafka)

* [ ] **Produtor RabbitMQ:** Implementado no fluxo de criação. Publica uma mensagem contendo ao menos o ID do pedido (ou dados mínimos) em uma fila específica.
* [ ] **Produtor Kafka:** Implementado no fluxo de criação. Publica um evento estruturado informando a criação do pedido em um tópico específico.
* [ ] **Operação Assíncrona/Não-bloqueante:** O disparo das mensagens/eventos ocorre no mesmo fluxo do cadastro do pedido, sem travar a resposta HTTP do usuário.

### 5. 🧪 Testes Automatizados (Pytest)

* [ ] **Configuração do Pytest:** Arquivo `pytest.ini` ou estrutura de testes pronta e passível de ser executada de dentro do container ou localmente.
* [ ] **Teste de Cadastro:** Um caso de teste que faz um `POST` válido para a API e valida o status `201 Created` (ou `200`), confirmando se o ID foi gerado e o status inicial veio como `PENDENTE`.
* [ ] **Teste de Listagem:** Um caso de teste que faz um `GET` para a API e valida se o retorno é uma lista e se contém o status esperado (`200 OK`).

---

### 📂 Estrutura de Arquivos Mínima Esperada

Certifique-se de que seu repositório possui uma organização clara, parecida com esta:

```text
├── app/
│   ├── __init__.py
│   ├── main.py          # Inicialização do FastAPI e Rotas
│   ├── database.py      # Conexão com MongoDB (Motor/Pymongo)
│   ├── queue.py         # Produtores RabbitMQ e Kafka
│   ├── schemas.py       # Validação de dados (Pydantic)
│   └── models.py        # Modelos do Banco de Dados
├── tests/
│   ├── __init__.py
│   └── test_api.py      # Testes do POST e GET com Pytest
├── Dockerfile           # Dockerfile da aplicação FastAPI
├── docker-compose.yml   # Orquestração de todos os serviços
├── requirements.txt     # fastapi, uvicorn, pydantic, motor, aiokafka, pika, pytest, httpx, etc.
└── README.md            # Instruções de como rodar o comando e testar

```