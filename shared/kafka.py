from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from typing import Any
from uuid import UUID, uuid4

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from pydantic import BaseModel

from shared.events import TOPIC_DLQ, DlqEnvelope, EventEnvelope

logger = logging.getLogger(__name__)


class KafkaEventProducer:
    """Async Kafka producer wrapping EventEnvelope serialization."""

    def __init__(
        self,
        bootstrap_servers: str,
        client: Any | None = None,
    ) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._client = client
        self._started = False

    async def start(self) -> None:
        if self._client is None:
            self._client = AIOKafkaProducer(
                bootstrap_servers=self._bootstrap_servers,
                acks="all",
                enable_idempotence=True,
            )
        if not self._started:
            await self._client.start()
            self._started = True

    async def stop(self) -> None:
        if self._client is not None and self._started:
            await self._client.stop()
            self._started = False

    async def publish(
        self,
        topic: str,
        event_type: str,
        payload: BaseModel,
        key: str,
        correlation_id: UUID | None = None,
    ) -> EventEnvelope:
        """Wrap payload in EventEnvelope and send it partitioned by key."""
        if self._client is None:
            raise RuntimeError("KafkaEventProducer is not started")

        resolved_correlation = correlation_id
        if resolved_correlation is None:
            raw_booking_id = getattr(payload, "booking_id", None)
            resolved_correlation = (
                raw_booking_id if isinstance(raw_booking_id, UUID) else uuid4()
            )

        envelope = EventEnvelope(
            event_id=uuid4(),
            event_type=event_type,
            occurred_at=datetime.now(timezone.utc),
            correlation_id=resolved_correlation,
            payload=payload.model_dump(mode="json"),
        )
        await self._client.send_and_wait(
            topic,
            value=envelope.model_dump_json().encode("utf-8"),
            key=key.encode("utf-8"),
        )
        return envelope


class KafkaEventConsumer:
    """Async Kafka consumer with retries and dead-letter queue fallback."""

    def __init__(
        self,
        bootstrap_servers: str,
        topics: list[str],
        group_id: str,
        producer: KafkaEventProducer,
        handler: Callable[[str, EventEnvelope], Awaitable[None]],
        max_retries: int = 3,
        retry_delay_seconds: float = 0.5,
    ) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._topics = topics
        self._group_id = group_id
        self._producer = producer
        self._handler = handler
        self._max_retries = max_retries
        self._retry_delay_seconds = retry_delay_seconds
        self._consumer: AIOKafkaConsumer | None = None
        self._task: asyncio.Task[None] | None = None
        self._running = False

    async def start(self) -> None:
        self._consumer = AIOKafkaConsumer(
            *self._topics,
            bootstrap_servers=self._bootstrap_servers,
            group_id=self._group_id,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
        )
        await self._consumer.start()
        self._running = True
        self._task = asyncio.create_task(self._consume_loop())

    async def stop(self) -> None:
        self._running = False
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
        if self._consumer is not None:
            await self._consumer.stop()
            self._consumer = None

    async def _consume_loop(self) -> None:
        assert self._consumer is not None
        while self._running:
            try:
                async for msg in self._consumer:
                    if not self._running:
                        break
                    key = msg.key.decode("utf-8", errors="replace") if msg.key else None
                    await self.process_raw_message(
                        topic=msg.topic,
                        key=key,
                        raw_value=msg.value,
                    )
                    await self._consumer.commit()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Kafka consume loop error; retrying in 1s")
                await asyncio.sleep(1.0)

    async def process_raw_message(
        self,
        topic: str,
        key: str | None,
        raw_value: bytes,
    ) -> bool:
        """Parse, retry handler up to max_retries, and send to DLQ on failure."""
        raw_text = raw_value.decode("utf-8", errors="replace")
        try:
            envelope = EventEnvelope.model_validate_json(raw_text)
        except Exception as exc:
            logger.warning("Malformed Kafka message on %s: %s", topic, exc)
            await self._send_to_dlq(
                source_topic=topic,
                key=key,
                error_message=f"Deserialization error: {exc}",
                raw_payload=raw_text,
            )
            return False

        last_exc: Exception | None = None
        for attempt in range(1, self._max_retries + 1):
            try:
                await self._handler(topic, envelope)
                return True
            except Exception as exc:
                last_exc = exc
                logger.warning(
                    "Handler failed on topic=%s event_id=%s (attempt %d/%d): %s",
                    topic,
                    envelope.event_id,
                    attempt,
                    self._max_retries,
                    exc,
                )
                if attempt < self._max_retries and self._retry_delay_seconds > 0:
                    await asyncio.sleep(self._retry_delay_seconds)

        await self._send_to_dlq(
            source_topic=topic,
            key=key or str(envelope.correlation_id),
            error_message=str(last_exc) if last_exc else "Unknown handler error",
            raw_payload=raw_text,
            correlation_id=envelope.correlation_id,
        )
        return False

    async def _send_to_dlq(
        self,
        source_topic: str,
        key: str | None,
        error_message: str,
        raw_payload: str,
        correlation_id: UUID | None = None,
    ) -> None:
        dlq_payload = DlqEnvelope(
            source_topic=source_topic,
            consumer_group=self._group_id,
            error_message=error_message,
            raw_payload=raw_payload,
            failed_at=datetime.now(timezone.utc),
        )
        await self._producer.publish(
            topic=TOPIC_DLQ,
            event_type=TOPIC_DLQ,
            payload=dlq_payload,
            key=key or str(uuid4()),
            correlation_id=correlation_id or uuid4(),
        )
