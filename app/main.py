from contextlib import asynccontextmanager

import pika
from fastapi import FastAPI
from kafka import KafkaProducer
from pymongo import MongoClient

from app.config import get_settings
from app.routers.order_router import router as order_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    app.state.settings = settings
    app.state.mongo_client = MongoClient(settings.mongo_uri)
    app.state.rabbitmq_connection = pika.BlockingConnection(
        pika.URLParameters(settings.rabbitmq_url)
    )
    app.state.kafka_producer = KafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers
    )
    yield
    app.state.mongo_client.close()
    app.state.rabbitmq_connection.close()
    app.state.kafka_producer.close()


app = FastAPI(title="Order Management API", lifespan=lifespan)
app.include_router(order_router)
