# Contexto

Uma empresa de comércio eletrônico deseja modernizar parte de sua arquitetura utilizando banco de dados não relacional e comunicação assíncrona entre sistemas.

Você foi contratado para desenvolver uma API responsável pelo gerenciamento de pedidos. Além de armazenar os dados em um banco NoSQL, a aplicação deverá publicar eventos para sistemas externos utilizando RabbitMQ e Kafka.

## Requisitos da Aplicação

Desenvolva uma API utilizando FastAPI para gerenciamento de pedidos.

Cada pedido deve possuir as seguintes informações:

- Identificador único
- Nome do cliente
- Nome do produto
- Quantidade
- Status do pedido

O status inicial de todo pedido deve ser `PENDENTE`.

## Funcionalidades

### 1. Cadastro de Pedido

Implemente um endpoint que permita cadastrar um novo pedido.

Ao cadastrar um pedido, a aplicação deve:

- Gerar um identificador único para o pedido;
- Armazenar os dados no MongoDB;
- Publicar uma mensagem em uma fila RabbitMQ informando que um pedido foi criado;
- Publicar um evento em um tópico Kafka registrando a criação do pedido.

### 2. Consulta de Pedidos

Implemente um endpoint que permita listar todos os pedidos cadastrados no banco de dados.

## Persistência

Utilize o MongoDB como banco de dados principal da aplicação.

Os dados dos pedidos devem ser armazenados em uma coleção específica para esse recurso.

## Mensageria

### RabbitMQ

Ao criar um pedido, publique uma mensagem contendo informações mínimas que permitam identificar o pedido criado.

### Kafka

Ao criar um pedido, publique um evento informando a criação do pedido para consumo por outros sistemas.

## Testes

Utilize Pytest para criar testes automatizados da API.

Os testes devem validar, no mínimo:

- O cadastro de pedidos;
- A listagem de pedidos.

## Conteinerização

A aplicação deve ser executada utilizando Docker Compose.

O ambiente deve conter, obrigatoriamente:

- FastAPI
- MongoDB
- RabbitMQ
- Kafka
- Zookeeper

Todos os serviços devem iniciar por meio de um único comando.

## Estrutura de Entrega

O projeto deverá conter todos os arquivos necessários para execução da aplicação, incluindo:

- Código-fonte da API;
- Configuração do MongoDB;
- Configuração do RabbitMQ;
- Configuração do Kafka;
- Testes automatizados;
- Dockerfile;
- docker-compose.yml
