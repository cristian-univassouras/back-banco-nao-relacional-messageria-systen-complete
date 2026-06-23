from contextlib import asynccontextmanager

import aio_pika
from aiokafka import AIOKafkaProducer
from fastapi import FastAPI
from motor.motor_asyncio import AsyncIOMotorClient

from app.config import get_settings
from app.routers.order_router import router as order_router


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()
    app.state.settings = settings
    app.state.mongo_client = AsyncIOMotorClient(settings.mongo_uri)
    app.state.rabbitmq_connection = await aio_pika.connect_robust(settings.rabbitmq_url)
    app.state.kafka_producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers
    )
    await app.state.kafka_producer.start()
    yield
    app.state.mongo_client.close()
    await app.state.rabbitmq_connection.close()
    await app.state.kafka_producer.stop()


app = FastAPI(title="Order Management API", lifespan=lifespan)
app.include_router(order_router)
