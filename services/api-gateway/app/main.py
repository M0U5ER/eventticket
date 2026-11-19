from __future__ import annotations

from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI

from app.config import settings
from app.routes import router
from shared.logging import configure_logging

configure_logging(settings.service_name)


def create_app(http_client: httpx.AsyncClient | None = None) -> FastAPI:
    owns_client = http_client is None

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if owns_client:
            app.state.http_client = httpx.AsyncClient(
                timeout=settings.request_timeout_seconds
            )
        try:
            yield
        finally:
            if owns_client:
                await app.state.http_client.aclose()

    app = FastAPI(
        title="EventTicket API Gateway",
        version="1.0.0",
        description="Unified entry point for EventTicket microservices",
        lifespan=lifespan,
    )
    app.state.service_urls = {
        "event-service": settings.event_service_url,
        "booking-service": settings.booking_service_url,
        "payment-service": settings.payment_service_url,
        "ticket-notification-service": settings.ticket_service_url,
    }
    if http_client is not None:
        app.state.http_client = http_client

    app.include_router(router)
    return app


app = create_app()
