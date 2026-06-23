from fastapi import Request

from app.publishers.event_publisher import EventPublisher
from app.publishers.kafka_publisher import KafkaPublisher
from app.publishers.rabbitmq_publisher import RabbitMQPublisher
from app.repositories.mongo_order_repository import MongoOrderRepository
from app.repositories.order_repository import OrderRepository
from app.services.order_service import OrderService


def get_order_repository(request: Request) -> OrderRepository:
    settings = request.app.state.settings
    collection = request.app.state.mongo_client[settings.mongo_db_name]["orders"]
    return MongoOrderRepository(collection)


def get_event_publishers(request: Request) -> list[EventPublisher]:
    channel = request.app.state.rabbitmq_connection.channel()
    rabbitmq_publisher = RabbitMQPublisher(channel)
    kafka_publisher = KafkaPublisher(request.app.state.kafka_producer)
    return [rabbitmq_publisher, kafka_publisher]


def get_order_service(request: Request) -> OrderService:
    return OrderService(
        get_order_repository(request),
        get_event_publishers(request),
    )
