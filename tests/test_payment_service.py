"""Tests for payment-service (Kafka payment processing, idempotency, refunds, REST API)."""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest
import pytest_asyncio
from pydantic import BaseModel

ROOT_DIR = Path(__file__).resolve().parents[1]
SERVICE_DIR = ROOT_DIR / "services" / "payment-service"
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

for mod_name in list(sys.modules):
    if mod_name == "app" or mod_name.startswith("app."):
        del sys.modules[mod_name]
if str(SERVICE_DIR) not in sys.path:
    sys.path.insert(0, str(SERVICE_DIR))

from app.consumer import PaymentServiceConsumerHandler  # noqa: E402
from app.main import create_app  # noqa: E402
from shared.database import create_session_factory  # noqa: E402
from shared.events import (  # noqa: E402
    TOPIC_BOOKING_CANCELLED,
    TOPIC_BOOKING_CREATED,
    TOPIC_PAYMENT_FAILED,
    TOPIC_PAYMENT_SUCCEEDED,
    BookingCancelledPayload,
    BookingCreatedPayload,
    EventEnvelope,
)


class FakeProducer:
    """In-memory Kafka producer stub capturing published events."""

    def __init__(self) -> None:
        self.published: list[dict[str, Any]] = []

    async def start(self) -> None:
        return None

    async def stop(self) -> None:
        return None

    async def publish(
        self,
        topic: str,
        event_type: str,
        payload: BaseModel,
        key: str,
        correlation_id: UUID | None = None,
    ) -> EventEnvelope:
        envelope = EventEnvelope(
            event_id=uuid4(),
            event_type=event_type,
            occurred_at=datetime.now(timezone.utc),
            correlation_id=correlation_id or uuid4(),
            payload=payload.model_dump(mode="json"),
        )
        self.published.append(
            {"topic": topic, "key": key, "envelope": envelope, "payload": payload}
        )
        return envelope


@pytest_asyncio.fixture
async def payment_env():
    engine, session_factory = create_session_factory("sqlite+aiosqlite:///:memory:")
    producer = FakeProducer()
    app = create_app(
        enable_kafka=False,
        engine=engine,
        session_factory=session_factory,
        producer=producer,
    )
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            yield app, client, session_factory, producer
    await engine.dispose()


@pytest.mark.asyncio
async def test_booking_created_succeeds_and_publishes_payment_succeeded(payment_env) -> None:
    _, client, session_factory, producer = payment_env
    handler = PaymentServiceConsumerHandler(session_factory=session_factory, producer=producer)
    booking_id = uuid4()

    envelope = EventEnvelope(
        event_id=uuid4(),
        event_type=TOPIC_BOOKING_CREATED,
        occurred_at=datetime.now(timezone.utc),
        correlation_id=booking_id,
        payload=BookingCreatedPayload(
            booking_id=booking_id,
            event_id=uuid4(),
            seat_id=uuid4(),
            customer_name="Иван Иванов",
            customer_email="ivan@example.com",
            amount=Decimal("2500.00"),
            simulate_payment_fail=False,
        ).model_dump(mode="json"),
    )
    await handler.handle(TOPIC_BOOKING_CREATED, envelope)

    assert len(producer.published) == 1
    assert producer.published[0]["topic"] == TOPIC_PAYMENT_SUCCEEDED
    assert producer.published[0]["key"] == str(booking_id)

    resp = await client.get(f"/api/payments/by-booking/{booking_id}")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "SUCCEEDED"
    assert data["provider_tx_id"] is not None


@pytest.mark.asyncio
async def test_booking_created_simulate_fail_publishes_payment_failed(payment_env) -> None:
    _, client, session_factory, producer = payment_env
    handler = PaymentServiceConsumerHandler(session_factory=session_factory, producer=producer)
    booking_id = uuid4()

    envelope = EventEnvelope(
        event_id=uuid4(),
        event_type=TOPIC_BOOKING_CREATED,
        occurred_at=datetime.now(timezone.utc),
        correlation_id=booking_id,
        payload=BookingCreatedPayload(
            booking_id=booking_id,
            event_id=uuid4(),
            seat_id=uuid4(),
            customer_name="Пётр Петров",
            customer_email="petr@example.com",
            amount=Decimal("1500.00"),
            simulate_payment_fail=True,
        ).model_dump(mode="json"),
    )
    await handler.handle(TOPIC_BOOKING_CREATED, envelope)

    assert len(producer.published) == 1
    assert producer.published[0]["topic"] == TOPIC_PAYMENT_FAILED
    assert producer.published[0]["key"] == str(booking_id)

    resp = await client.get(f"/api/payments/by-booking/{booking_id}")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "FAILED"
    assert data["failure_reason"] is not None


@pytest.mark.asyncio
async def test_payment_idempotency_on_duplicate_event_and_booking(payment_env) -> None:
    _, _, session_factory, producer = payment_env
    handler = PaymentServiceConsumerHandler(session_factory=session_factory, producer=producer)
    booking_id = uuid4()
    payload = BookingCreatedPayload(
        booking_id=booking_id,
        event_id=uuid4(),
        seat_id=uuid4(),
        customer_name="Анна Смирнова",
        customer_email="anna@example.com",
        amount=Decimal("3000.00"),
        simulate_payment_fail=False,
    ).model_dump(mode="json")

    first = EventEnvelope(
        event_id=uuid4(),
        event_type=TOPIC_BOOKING_CREATED,
        occurred_at=datetime.now(timezone.utc),
        correlation_id=booking_id,
        payload=payload,
    )
    await handler.handle(TOPIC_BOOKING_CREATED, first)
    # Duplicate event_id
    await handler.handle(TOPIC_BOOKING_CREATED, first)
    # Different event_id, same booking_id
    second = EventEnvelope(
        event_id=uuid4(),
        event_type=TOPIC_BOOKING_CREATED,
        occurred_at=datetime.now(timezone.utc),
        correlation_id=booking_id,
        payload=payload,
    )
    await handler.handle(TOPIC_BOOKING_CREATED, second)

    assert len(producer.published) == 1


@pytest.mark.asyncio
async def test_booking_cancelled_refunds_succeeded_payment_and_read_endpoints(payment_env) -> None:
    _, client, session_factory, producer = payment_env
    handler = PaymentServiceConsumerHandler(session_factory=session_factory, producer=producer)
    booking_id = uuid4()
    event_id = uuid4()
    seat_id = uuid4()

    created_env = EventEnvelope(
        event_id=uuid4(),
        event_type=TOPIC_BOOKING_CREATED,
        occurred_at=datetime.now(timezone.utc),
        correlation_id=booking_id,
        payload=BookingCreatedPayload(
            booking_id=booking_id,
            event_id=event_id,
            seat_id=seat_id,
            customer_name="Мария Кузнецова",
            customer_email="maria@example.com",
            amount=Decimal("2000.00"),
            simulate_payment_fail=False,
        ).model_dump(mode="json"),
    )
    await handler.handle(TOPIC_BOOKING_CREATED, created_env)

    cancelled_env = EventEnvelope(
        event_id=uuid4(),
        event_type=TOPIC_BOOKING_CANCELLED,
        occurred_at=datetime.now(timezone.utc),
        correlation_id=booking_id,
        payload=BookingCancelledPayload(
            booking_id=booking_id,
            event_id=event_id,
            seat_id=seat_id,
            customer_email="maria@example.com",
            reason="Customer requested refund",
        ).model_dump(mode="json"),
    )
    await handler.handle(TOPIC_BOOKING_CANCELLED, cancelled_env)

    health_resp = await client.get("/health")
    assert health_resp.status_code == 200
    assert health_resp.json()["service"] == "payment-service"

    list_resp = await client.get("/api/payments")
    assert list_resp.status_code == 200
    assert len(list_resp.json()) == 1
    assert list_resp.json()[0]["status"] == "REFUNDED"
