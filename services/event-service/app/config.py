from __future__ import annotations

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    service_name: str = "event-service"
    database_url: str = "postgresql+asyncpg://eventticket:eventticket@postgres:5432/event_db"
    kafka_bootstrap_servers: str = "kafka:9092"
    enable_kafka: bool = True
