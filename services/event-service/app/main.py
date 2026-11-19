from __future__ import annotations

from contextlib import asynccontextmanager

from fastapi import FastAPI
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from app.config import Settings
from app.consumer import EventServiceConsumerHandler
from app.routes import router
from shared.database import Base, create_session_factory
from shared.events import (
    TOPIC_BOOKING_CANCELLED,
    TOPIC_PAYMENT_FAILED,
    TOPIC_PAYMENT_SUCCEEDED,
)
from shared.kafka import KafkaEventConsumer, KafkaEventProducer
from shared.logging import configure_logging


def create_app(
    database_url: str | None = None,
    kafka_bootstrap_servers: str | None = None,
    enable_kafka: bool | None = None,
    engine: AsyncEngine | None = None,
    session_factory: async_sessionmaker[AsyncSession] | None = None,
) -> FastAPI:
    settings = Settings()
    configure_logging(settings.service_name)
    resolved_db_url = database_url or settings.database_url
    resolved_kafka = kafka_bootstrap_servers or settings.kafka_bootstrap_servers
    resolved_enable_kafka = (
        settings.enable_kafka if enable_kafka is None else enable_kafka
    )

    if engine is None or session_factory is None:
        engine, session_factory = create_session_factory(resolved_db_url)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        app.state.engine = engine
        app.state.session_factory = session_factory

        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

        producer: KafkaEventProducer | None = None
        consumer: KafkaEventConsumer | None = None
        if resolved_enable_kafka:
            producer = KafkaEventProducer(resolved_kafka)
            await producer.start()
            handler = EventServiceConsumerHandler(session_factory)
            consumer = KafkaEventConsumer(
                bootstrap_servers=resolved_kafka,
                topics=[
                    TOPIC_PAYMENT_SUCCEEDED,
                    TOPIC_PAYMENT_FAILED,
                    TOPIC_BOOKING_CANCELLED,
                ],
                group_id="event-service",
                producer=producer,
                handler=handler.handle,
            )
            await consumer.start()

        try:
            yield
        finally:
            if consumer is not None:
                await consumer.stop()
            if producer is not None:
                await producer.stop()
            await engine.dispose()

    app = FastAPI(title="EventTicket - Event Service", lifespan=lifespan)
    app.include_router(router)
    return app


app = create_app()
