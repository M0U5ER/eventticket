"""FastAPI application entrypoint for payment-service."""

from __future__ import annotations

from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from app.config import Settings
from app.consumer import PaymentServiceConsumerHandler
from app.routes import router
from shared.database import Base, create_session_factory
from shared.events import TOPIC_BOOKING_CANCELLED, TOPIC_BOOKING_CREATED
from shared.kafka import KafkaEventConsumer, KafkaEventProducer
from shared.logging import configure_logging


def create_app(
    database_url: str | None = None,
    kafka_bootstrap_servers: str | None = None,
    enable_kafka: bool | None = None,
    engine: AsyncEngine | None = None,
    session_factory: async_sessionmaker[AsyncSession] | None = None,
    producer: Any | None = None,
) -> FastAPI:
    """Create and configure the payment-service FastAPI application."""
    settings = Settings()
    logger = configure_logging(settings.service_name)
    use_kafka = settings.enable_kafka if enable_kafka is None else enable_kafka
    bootstrap = kafka_bootstrap_servers or settings.kafka_bootstrap_servers

    if engine is None or session_factory is None:
        db_url = database_url or settings.database_url
        engine, session_factory = create_session_factory(db_url)

    kafka_producer = producer or KafkaEventProducer(bootstrap_servers=bootstrap)
    consumer: KafkaEventConsumer | None = None

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        nonlocal consumer
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

        app.state.session_factory = session_factory
        app.state.producer = kafka_producer

        if use_kafka:
            await kafka_producer.start()
            handler = PaymentServiceConsumerHandler(
                session_factory=session_factory,
                producer=kafka_producer,
            )
            consumer = KafkaEventConsumer(
                bootstrap_servers=bootstrap,
                topics=[TOPIC_BOOKING_CREATED, TOPIC_BOOKING_CANCELLED],
                group_id="payment-service-group",
                producer=kafka_producer,
                handler=handler.handle,
            )
            await consumer.start()
            logger.info("payment-service started with Kafka")
        else:
            logger.info("payment-service started without Kafka")

        try:
            yield
        finally:
            if consumer is not None:
                await consumer.stop()
            if use_kafka:
                await kafka_producer.stop()

    app = FastAPI(title="EventTicket Payment Service", version="1.0.0", lifespan=lifespan)
    app.include_router(router)
    return app


app = create_app()
