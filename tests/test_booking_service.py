"""Tests for booking-service (REST API, idempotency, cancel rules, Kafka consumer)."""

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
SERVICE_DIR = ROOT_DIR / "services" / "booking-service"
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

for mod_name in list(sys.modules):
    if mod_name == "app" or mod_name.startswith("app."):
        del sys.modules[mod_name]
if str(SERVICE_DIR) not in sys.path:
    sys.path.insert(0, str(SERVICE_DIR))

from app.consumer import BookingServiceConsumerHandler  # noqa: E402
from app.main import create_app  # noqa: E402
from shared.database import create_session_factory  # noqa: E402
from shared.events import (  # noqa: E402
    TOPIC_BOOKING_CANCELLED,
    TOPIC_BOOKING_CREATED,
    TOPIC_PAYMENT_FAILED,
    TOPIC_PAYMENT_SUCCEEDED,
    EventEnvelope,
    PaymentFailedPayload,
    PaymentSucceededPayload,
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


class FakeEventClient:
    """Stub for HTTP calls to event-service internal reserve endpoint."""

    def __init__(self) -> None:
        self.calls: list[tuple[UUID, UUID]] = []
        self.mode: str = "ok"  # "ok" | "conflict" | "not_found" | "unreachable"
        self.price: Decimal = Decimal("2500.00")

    async def reserve_seat(self, seat_id: UUID, booking_id: UUID, event_id: UUID) -> dict[str, Any]:
        self.calls.append((seat_id, booking_id))
        if self.mode == "unreachable":
            raise httpx.ConnectError("event-service unreachable")
        if self.mode == "conflict":
            request = httpx.Request("POST", f"http://event-service:8001/internal/seats/{seat_id}/reserve")
            response = httpx.Response(409, json={"detail": "Seat already reserved"}, request=request)
            raise httpx.HTTPStatusError("409 Conflict", request=request, response=response)
        if self.mode == "not_found":
            request = httpx.Request("POST", f"http://event-service:8001/internal/seats/{seat_id}/reserve")
            response = httpx.Response(404, json={"detail": "Seat not found"}, request=request)
            raise httpx.HTTPStatusError("404 Not Found", request=request, response=response)
        return {
            "seat_id": str(seat_id),
            "event_id": str(event_id),
            "price": str(self.price),
            "status": "RESERVED",
            "reserved_booking_id": str(booking_id),
        }


@pytest_asyncio.fixture
async def booking_env():
    engine, session_factory = create_session_factory("sqlite+aiosqlite:///:memory:")
    producer = FakeProducer()
    event_client = FakeEventClient()
    app = create_app(
        enable_kafka=False,
        engine=engine,
        session_factory=session_factory,
        producer=producer,
        event_client=event_client,
    )
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
            yield app, client, session_factory, producer, event_client
    await engine.dispose()


@pytest.mark.asyncio
async def test_create_booking_happy_path(booking_env) -> None:
    _, client, _, producer, event_client = booking_env
    event_id = uuid4()
    seat_id = uuid4()

    resp = await client.post(
        "/api/bookings",
        json={
            "event_id": str(event_id),
            "seat_id": str(seat_id),
            "customer_name": "Иван Иванов",
            "customer_email": "ivan@example.com",
            "simulate_payment_fail": False,
        },
    )
    assert resp.status_code == 201
    data = resp.json()
    assert data["status"] == "PENDING"
    assert Decimal(str(data["amount"])) == Decimal("2500.00")
    assert len(event_client.calls) == 1
    assert len(producer.published) == 1
    assert producer.published[0]["topic"] == TOPIC_BOOKING_CREATED
    assert producer.published[0]["key"] == data["id"]


@pytest.mark.asyncio
async def test_create_booking_seat_conflict_returns_409_without_event(booking_env) -> None:
    _, client, _, producer, event_client = booking_env
    event_client.mode = "conflict"

    resp = await client.post(
        "/api/bookings",
        json={
            "event_id": str(uuid4()),
            "seat_id": str(uuid4()),
            "customer_name": "Пётр Петров",
            "customer_email": "petr@example.com",
        },
    )
    assert resp.status_code == 409
    assert len(producer.published) == 0


@pytest.mark.asyncio
async def test_create_booking_idempotency_key_returns_existing(booking_env) -> None:
    _, client, _, producer, event_client = booking_env
    payload = {
        "event_id": str(uuid4()),
        "seat_id": str(uuid4()),
        "customer_name": "Анна Смирнова",
        "customer_email": "anna@example.com",
    }
    headers = {"Idempotency-Key": "idem-123"}

    first = await client.post("/api/bookings", json=payload, headers=headers)
    second = await client.post("/api/bookings", json=payload, headers=headers)

    assert first.status_code == 201
    assert second.status_code in (200, 201)
    assert first.json()["id"] == second.json()["id"]
    assert len(event_client.calls) == 1
    assert len(producer.published) == 1


@pytest.mark.asyncio
async def test_create_booking_event_service_unreachable_returns_503(booking_env) -> None:
    _, client, _, producer, event_client = booking_env
    event_client.mode = "unreachable"

    resp = await client.post(
        "/api/bookings",
        json={
            "event_id": str(uuid4()),
            "seat_id": str(uuid4()),
            "customer_name": "Олег Сидоров",
            "customer_email": "oleg@example.com",
        },
    )
    assert resp.status_code == 503
    assert len(producer.published) == 0


@pytest.mark.asyncio
async def test_cancel_booking_rules_and_consumer_transitions(booking_env) -> None:
    _, client, session_factory, producer, _ = booking_env
    event_id = uuid4()
    seat_id = uuid4()

    created = await client.post(
        "/api/bookings",
        json={
            "event_id": str(event_id),
            "seat_id": str(seat_id),
            "customer_name": "Мария Кузнецова",
            "customer_email": "maria@example.com",
        },
    )
    assert created.status_code == 201
    booking_id = UUID(created.json()["id"])

    # Cannot cancel PENDING booking -> 409 Conflict
    pending_cancel = await client.post(
        f"/api/bookings/{booking_id}/cancel",
        json={"reason": "Передумала"},
    )
    assert pending_cancel.status_code == 409

    # payment.succeeded -> CONFIRMED
    handler = BookingServiceConsumerHandler(session_factory=session_factory)
    succeeded_env = EventEnvelope(
        event_id=uuid4(),
        event_type="payment.succeeded",
        occurred_at=datetime.now(timezone.utc),
        correlation_id=booking_id,
        payload=PaymentSucceededPayload(
            booking_id=booking_id,
            payment_id=uuid4(),
            event_id=event_id,
            seat_id=seat_id,
            customer_name="Мария Кузнецова",
            customer_email="maria@example.com",
            amount=Decimal("2500.00"),
            provider_tx_id="tx-001",
        ).model_dump(mode="json"),
    )
    await handler.handle(TOPIC_PAYMENT_SUCCEEDED, succeeded_env)
    # Duplicate event_id ignored idempotently
    await handler.handle(TOPIC_PAYMENT_SUCCEEDED, succeeded_env)

    get_confirmed = await client.get(f"/api/bookings/{booking_id}")
    assert get_confirmed.status_code == 200
    assert get_confirmed.json()["status"] == "CONFIRMED"

    # Cancel CONFIRMED booking -> 200 + booking.cancelled published
    confirmed_cancel = await client.post(
        f"/api/bookings/{booking_id}/cancel",
        json={"reason": "Изменились планы"},
    )
    assert confirmed_cancel.status_code == 200
    assert confirmed_cancel.json()["status"] == "CANCELLED"
    cancelled_events = [p for p in producer.published if p["topic"] == TOPIC_BOOKING_CANCELLED]
    assert len(cancelled_events) == 1
    assert cancelled_events[0]["key"] == str(booking_id)

    # Cannot cancel already CANCELLED booking -> 409 Conflict
    repeat_cancel = await client.post(
        f"/api/bookings/{booking_id}/cancel",
        json={"reason": "Повтор"},
    )
    assert repeat_cancel.status_code == 409


@pytest.mark.asyncio
async def test_consumer_payment_failed_cancels_booking(booking_env) -> None:
    _, client, session_factory, _, _ = booking_env
    event_id = uuid4()
    seat_id = uuid4()

    created = await client.post(
        "/api/bookings",
        json={
            "event_id": str(event_id),
            "seat_id": str(seat_id),
            "customer_name": "Тест Тестов",
            "customer_email": "test@example.com",
            "simulate_payment_fail": True,
        },
    )
    booking_id = UUID(created.json()["id"])

    handler = BookingServiceConsumerHandler(session_factory=session_factory)
    failed_env = EventEnvelope(
        event_id=uuid4(),
        event_type="payment.failed",
        occurred_at=datetime.now(timezone.utc),
        correlation_id=booking_id,
        payload=PaymentFailedPayload(
            booking_id=booking_id,
            payment_id=uuid4(),
            event_id=event_id,
            seat_id=seat_id,
            customer_email="test@example.com",
            reason="Insufficient funds",
        ).model_dump(mode="json"),
    )
    await handler.handle(TOPIC_PAYMENT_FAILED, failed_env)

    get_resp = await client.get(f"/api/bookings/{booking_id}")
    assert get_resp.status_code == 200
    assert get_resp.json()["status"] == "CANCELLED"
    assert get_resp.json()["cancel_reason"] == "Insufficient funds"
