from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    mongo_uri: str = "mongodb://localhost:27017"
    mongo_db_name: str = "orders_db"
    rabbitmq_url: str = "amqp://guest:guest@localhost:5672/"
    kafka_bootstrap_servers: str = "localhost:9092"

    model_config = SettingsConfigDict(env_file=".env")


@lru_cache
def get_settings() -> Settings:
    return Settings()
