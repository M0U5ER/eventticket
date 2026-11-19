"""Configuration for booking-service."""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Service settings loaded from environment variables."""

    service_name: str = "booking-service"
    database_url: str = "postgresql+asyncpg://postgres:postgres@booking-db:5432/booking_db"
    kafka_bootstrap_servers: str = "kafka:9092"
    event_service_url: str = "http://event-service:8001"
    enable_kafka: bool = True

    model_config = SettingsConfigDict(env_prefix="BOOKING_", extra="ignore")
