"""Configuration for payment-service."""

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Service settings loaded from environment variables."""

    service_name: str = "payment-service"
    database_url: str = "postgresql+asyncpg://postgres:postgres@payment-db:5432/payment_db"
    kafka_bootstrap_servers: str = "kafka:9092"
    enable_kafka: bool = True

    model_config = SettingsConfigDict(env_prefix="PAYMENT_", extra="ignore")
