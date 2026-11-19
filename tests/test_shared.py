from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any
from uuid import uuid4

import pytest
from sqlalchemy.ext.asyncio import AsyncSession

from shared.database import Base, create_session_factory, try_register_event
from shared.events import (
    TOPIC_BOOKING_CREATED,
    TOPIC_DLQ,
    BookingCreatedPayload,
    DlqEnvelope,
    EventEnvelope,
)
from shared.kafka import KafkaEventConsumer, KafkaEventProducer


class FakeAIOKafkaProducer:
    """In-memory stub for AIOKafkaProducer."""

    def __init__(self) -> None:
        self.sent: list[tuple[str, bytes | None, bytes]] = []

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass

    async def send_and_wait(
        self,
        topic: str,
        value: bytes,
        key: bytes | None = None,
    ) -> None:
        self.sent.append((topic, key, value))


def test_event_envelope_roundtrip() -> None:
    booking_id = uuid4()
    payload = BookingCreatedPayload(
        booking_id=booking_id,
        event_id=uuid4(),
        seat_id=uuid4(),
        customer_name="Иван Иванов",
        customer_email="ivan@example.com",
        amount=Decimal("1500.00"),
    )
    envelope = EventEnvelope(
        event_id=uuid4(),
        event_type=TOPIC_BOOKING_CREATED,
        occurred_at=datetime.now(timezone.utc),
        correlation_id=booking_id,
        payload=payload.model_dump(mode="json"),
    )
    raw = envelope.model_dump_json()
    restored = EventEnvelope.model_validate_json(raw)
    assert restored.event_id == envelope.event_id
    assert restored.event_type == TOPIC_BOOKING_CREATED
    assert restored.correlation_id == booking_id
    restored_payload = BookingCreatedPayload.model_validate(restored.payload)
    assert restored_payload.booking_id == booking_id
    assert restored_payload.amount == Decimal("1500.00")


@pytest.mark.asyncio
async def test_kafka_producer_uses_booking_id_key() -> None:
    fake = FakeAIOKafkaProducer()
    producer = KafkaEventProducer(bootstrap_servers="localhost:9092", client=fake)
    booking_id = uuid4()
    payload = BookingCreatedPayload(
        booking_id=booking_id,
        event_id=uuid4(),
        seat_id=uuid4(),
        customer_name="Анна",
        customer_email="anna@example.com",
        amount=Decimal("2000.00"),
    )

    envelope = await producer.publish(
        topic=TOPIC_BOOKING_CREATED,
        event_type=TOPIC_BOOKING_CREATED,
        payload=payload,
        key=str(booking_id),
    )

    assert len(fake.sent) == 1
    topic, key_bytes, value_bytes = fake.sent[0]
    assert topic == TOPIC_BOOKING_CREATED
    assert key_bytes == str(booking_id).encode("utf-8")
    parsed = EventEnvelope.model_validate_json(value_bytes.decode("utf-8"))
    assert parsed.event_id == envelope.event_id


@pytest.mark.asyncio
async def test_consumer_retries_twice_and_succeeds_without_dlq() -> None:
    fake_prod = FakeAIOKafkaProducer()
    producer = KafkaEventProducer(bootstrap_servers="localhost:9092", client=fake_prod)
    attempts = 0

    async def flaky_handler(topic: str, env: EventEnvelope) -> None:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise RuntimeError("transient db error")

    consumer = KafkaEventConsumer(
        bootstrap_servers="localhost:9092",
        topics=[TOPIC_BOOKING_CREATED],
        group_id="test-group",
        producer=producer,
        handler=flaky_handler,
        max_retries=3,
        retry_delay_seconds=0.0,
    )

    envelope = EventEnvelope(
        event_id=uuid4(),
        event_type=TOPIC_BOOKING_CREATED,
        occurred_at=datetime.now(timezone.utc),
        correlation_id=uuid4(),
        payload={"booking_id": str(uuid4())},
    )
    ok = await consumer.process_raw_message(
        topic=TOPIC_BOOKING_CREATED,
        key="k1",
        raw_value=envelope.model_dump_json().encode("utf-8"),
    )

    assert ok is True
    assert attempts == 3
    assert len(fake_prod.sent) == 0


@pytest.mark.asyncio
async def test_consumer_sends_poisoned_message_to_dlq_after_3_retries() -> None:
    fake_prod = FakeAIOKafkaProducer()
    producer = KafkaEventProducer(bootstrap_servers="localhost:9092", client=fake_prod)
    attempts = 0

    async def failing_handler(topic: str, env: EventEnvelope) -> None:
        nonlocal attempts
        attempts += 1
        raise ValueError("permanent failure")

    consumer = KafkaEventConsumer(
        bootstrap_servers="localhost:9092",
        topics=[TOPIC_BOOKING_CREATED],
        group_id="booking-group",
        producer=producer,
        handler=failing_handler,
        max_retries=3,
        retry_delay_seconds=0.0,
    )

    # 1) Handler fails 3 times -> DLQ
    envelope = EventEnvelope(
        event_id=uuid4(),
        event_type=TOPIC_BOOKING_CREATED,
        occurred_at=datetime.now(timezone.utc),
        correlation_id=uuid4(),
        payload={"booking_id": str(uuid4())},
    )
    ok_handler = await consumer.process_raw_message(
        topic=TOPIC_BOOKING_CREATED,
        key="b1",
        raw_value=envelope.model_dump_json().encode("utf-8"),
    )
    assert ok_handler is False
    assert attempts == 3
    assert len(fake_prod.sent) == 1
    dlq_topic, dlq_key, dlq_val = fake_prod.sent[0]
    assert dlq_topic == TOPIC_DLQ
    assert dlq_key == b"b1"
    dlq_env = EventEnvelope.model_validate_json(dlq_val.decode("utf-8"))
    dlq_payload = DlqEnvelope.model_validate(dlq_env.payload)
    assert dlq_payload.source_topic == TOPIC_BOOKING_CREATED
    assert dlq_payload.consumer_group == "booking-group"
    assert "permanent failure" in dlq_payload.error_message

    # 2) Malformed JSON -> immediate DLQ, consumer stays alive
    ok_json = await consumer.process_raw_message(
        topic=TOPIC_BOOKING_CREATED,
        key="bad-key",
        raw_value=b"{not-valid-json",
    )
    assert ok_json is False
    assert len(fake_prod.sent) == 2
    assert fake_prod.sent[1][0] == TOPIC_DLQ


@pytest.mark.asyncio
async def test_try_register_event_idempotency() -> None:
    engine, session_factory = create_session_factory("sqlite+aiosqlite:///:memory:")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    event_id = uuid4()
    async with session_factory() as session:
        first = await try_register_event(session, event_id)
        await session.commit()
        second = await try_register_event(session, event_id)
        await session.rollback()

    await engine.dispose()
    assert first is True
    assert second is False
