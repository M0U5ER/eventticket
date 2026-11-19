from __future__ import annotations

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    service_name: str = "api-gateway"
    event_service_url: str = "http://event-service:8001"
    booking_service_url: str = "http://booking-service:8002"
    payment_service_url: str = "http://payment-service:8003"
    ticket_service_url: str = "http://ticket-notification-service:8004"
    request_timeout_seconds: float = 10.0

    model_config = SettingsConfigDict(env_prefix="GATEWAY_", extra="ignore")


settings = Settings()
