from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from app.config import settings
from app.consumer import handle_ticket_event
from app.routes import router
from shared.database import Base, create_session_factory
from shared.events import (
    TOPIC_BOOKING_CANCELLED,
    TOPIC_PAYMENT_FAILED,
    TOPIC_PAYMENT_SUCCEEDED,
)
from shared.kafka import KafkaEventConsumer, KafkaEventProducer
from shared.logging import configure_logging

logger = configure_logging(settings.service_name)


def create_app(
    *,
    database_url: str | None = None,
    kafka_bootstrap_servers: str | None = None,
    enable_kafka: bool | None = None,
    engine: AsyncEngine | None = None,
    session_factory: async_sessionmaker[AsyncSession] | None = None,
    producer: KafkaEventProducer | None = None,
) -> FastAPI:
    db_url = database_url or settings.database_url
    bootstrap = kafka_bootstrap_servers or settings.kafka_bootstrap_servers
    kafka_enabled = settings.enable_kafka if enable_kafka is None else enable_kafka

    own_engine = engine is None
    if engine is None or session_factory is None:
        engine, session_factory = create_session_factory(db_url)

    kafka_producer = producer or KafkaEventProducer(bootstrap_servers=bootstrap)
    consumer: KafkaEventConsumer | None = None

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        nonlocal consumer
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

        if kafka_enabled:
            await kafka_producer.start()
            consumer = KafkaEventConsumer(
                bootstrap_servers=bootstrap,
                topics=[
                    TOPIC_PAYMENT_SUCCEEDED,
                    TOPIC_PAYMENT_FAILED,
                    TOPIC_BOOKING_CANCELLED,
                ],
                group_id="ticket-notification-service-group",
                producer=kafka_producer,
                handler=lambda topic, env: handle_ticket_event(
                    topic, env, session_factory, kafka_producer
                ),
            )
            await consumer.start()

        try:
            yield
        finally:
            if consumer is not None:
                await consumer.stop()
            if kafka_enabled:
                await kafka_producer.stop()
            if own_engine:
                await engine.dispose()

    app = FastAPI(title="EventTicket Ticket & Notification Service", lifespan=lifespan)
    app.state.engine = engine
    app.state.session_factory = session_factory
    app.state.producer = kafka_producer
    app.include_router(router)
    return app


app = create_app()
