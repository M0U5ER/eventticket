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
SERVICE_DIR = ROOT_DIR / "services" / "ticket-notification-service"


def _import_service_app():
    if str(ROOT_DIR) not in sys.path:
        sys.path.insert(0, str(ROOT_DIR))
    service_str = str(SERVICE_DIR)
    if service_str in sys.path:
        sys.path.remove(service_str)
    sys.path.insert(0, service_str)

    loaded = sys.modules.get("app")
    loaded_file = getattr(loaded, "__file__", "") or ""
    if loaded is None or str(SERVICE_DIR) not in str(Path(loaded_file).resolve()):
        for key in list(sys.modules.keys()):
            if key == "app" or key.startswith("app."):
                del sys.modules[key]

    from app.consumer import handle_ticket_event
    from app.main import create_app

    return create_app, handle_ticket_event


from shared.database import Base, create_session_factory
from shared.events import (
    BookingCancelledPayload,
    EventEnvelope,
    PaymentFailedPayload,
    PaymentSucceededPayload,
    TOPIC_BOOKING_CANCELLED,
    TOPIC_PAYMENT_FAILED,
    TOPIC_PAYMENT_SUCCEEDED,
    TOPIC_TICKET_ISSUED,
)


class FakeProducer:
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
            {
                "topic": topic,
                "event_type": event_type,
                "key": key,
                "envelope": envelope,
            }
        )
        return envelope


@pytest_asyncio.fixture
async def ticket_env(tmp_path: Path):
    create_app, handle_ticket_event = _import_service_app()
    db_path = tmp_path / "ticket_test.db"
    engine, session_factory = create_session_factory(
        f"sqlite+aiosqlite:///{db_path.as_posix()}"
    )
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    fake_producer = FakeProducer()
    app = create_app(
        enable_kafka=False,
        engine=engine,
        session_factory=session_factory,
        producer=fake_producer,
    )
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield {
            "client": client,
            "session_factory": session_factory,
            "producer": fake_producer,
            "handle_event": handle_ticket_event,
        }

    await engine.dispose()


@pytest.mark.asyncio
async def test_health_endpoint(ticket_env) -> None:
    client: httpx.AsyncClient = ticket_env["client"]
    resp = await client.get("/health")
    assert resp.status_code == 200
    assert resp.json()["service"] == "ticket-notification-service"


@pytest.mark.asyncio
async def test_payment_succeeded_issues_ticket_and_notification(ticket_env) -> None:
    client: httpx.AsyncClient = ticket_env["client"]
    session_factory = ticket_env["session_factory"]
    producer: FakeProducer = ticket_env["producer"]
    handle_event = ticket_env["handle_event"]

    booking_id = uuid4()
    payment_id = uuid4()
    event_id = uuid4()
    seat_id = uuid4()
    correlation_id = uuid4()

    payload = PaymentSucceededPayload(
        booking_id=booking_id,
        payment_id=payment_id,
        event_id=event_id,
        seat_id=seat_id,
        customer_name="Иван Иванов",
        customer_email="ivan@example.com",
        amount=Decimal("2500.00"),
        provider_tx_id="tx-12345",
    )
    envelope = EventEnvelope(
        event_id=uuid4(),
        event_type=TOPIC_PAYMENT_SUCCEEDED,
        occurred_at=datetime.now(timezone.utc),
        correlation_id=correlation_id,
        payload=payload.model_dump(mode="json"),
    )

    await handle_event(TOPIC_PAYMENT_SUCCEEDED, envelope, session_factory, producer)

    resp = await client.get(f"/api/tickets/by-booking/{booking_id}")
    assert resp.status_code == 200
    ticket = resp.json()
    assert ticket["booking_id"] == str(booking_id)
    assert ticket["status"] == "ACTIVE"
    assert ticket["ticket_code"].startswith("ET-")
    assert ticket["customer_email"] == "ivan@example.com"

    list_resp = await client.get("/api/tickets")
    assert list_resp.status_code == 200
    assert len(list_resp.json()) == 1

    notif_resp = await client.get(f"/api/notifications?booking_id={booking_id}")
    assert notif_resp.status_code == 200
    notifs = notif_resp.json()
    assert len(notifs) == 1
    assert notifs[0]["event_type"] == TOPIC_PAYMENT_SUCCEEDED
    assert notifs[0]["channel"] == "EMAIL"
    assert notifs[0]["status"] == "SENT"

    assert len(producer.published) == 1
    pub = producer.published[0]
    assert pub["topic"] == TOPIC_TICKET_ISSUED
    assert pub["key"] == str(booking_id)
    assert pub["envelope"].payload["ticket_code"] == ticket["ticket_code"]


@pytest.mark.asyncio
async def test_missing_ticket_returns_404(ticket_env) -> None:
    client: httpx.AsyncClient = ticket_env["client"]
    resp = await client.get(f"/api/tickets/by-booking/{uuid4()}")
    assert resp.status_code == 404


@pytest.mark.asyncio
async def test_payment_failed_creates_notification_without_ticket(ticket_env) -> None:
    client: httpx.AsyncClient = ticket_env["client"]
    session_factory = ticket_env["session_factory"]
    producer: FakeProducer = ticket_env["producer"]
    handle_event = ticket_env["handle_event"]

    booking_id = uuid4()
    payload = PaymentFailedPayload(
        booking_id=booking_id,
        payment_id=uuid4(),
        event_id=uuid4(),
        seat_id=uuid4(),
        customer_email="fail@example.com",
        reason="Insufficient funds",
    )
    envelope = EventEnvelope(
        event_id=uuid4(),
        event_type=TOPIC_PAYMENT_FAILED,
        occurred_at=datetime.now(timezone.utc),
        correlation_id=uuid4(),
        payload=payload.model_dump(mode="json"),
    )

    await handle_event(TOPIC_PAYMENT_FAILED, envelope, session_factory, producer)

    ticket_resp = await client.get(f"/api/tickets/by-booking/{booking_id}")
    assert ticket_resp.status_code == 404

    notif_resp = await client.get(f"/api/notifications?booking_id={booking_id}")
    assert notif_resp.status_code == 200
    notifs = notif_resp.json()
    assert len(notifs) == 1
    assert notifs[0]["event_type"] == TOPIC_PAYMENT_FAILED
    assert "Insufficient funds" in notifs[0]["message"]
    assert producer.published == []


@pytest.mark.asyncio
async def test_booking_cancelled_updates_ticket_and_sends_notification(ticket_env) -> None:
    client: httpx.AsyncClient = ticket_env["client"]
    session_factory = ticket_env["session_factory"]
    producer: FakeProducer = ticket_env["producer"]
    handle_event = ticket_env["handle_event"]

    booking_id = uuid4()
    event_id = uuid4()
    seat_id = uuid4()

    succeeded = EventEnvelope(
        event_id=uuid4(),
        event_type=TOPIC_PAYMENT_SUCCEEDED,
        occurred_at=datetime.now(timezone.utc),
        correlation_id=uuid4(),
        payload=PaymentSucceededPayload(
            booking_id=booking_id,
            payment_id=uuid4(),
            event_id=event_id,
            seat_id=seat_id,
            customer_name="Анна Смирнова",
            customer_email="anna@example.com",
            amount=Decimal("1500.00"),
            provider_tx_id="tx-ok",
        ).model_dump(mode="json"),
    )
    await handle_event(TOPIC_PAYMENT_SUCCEEDED, succeeded, session_factory, producer)

    cancelled = EventEnvelope(
        event_id=uuid4(),
        event_type=TOPIC_BOOKING_CANCELLED,
        occurred_at=datetime.now(timezone.utc),
        correlation_id=uuid4(),
        payload=BookingCancelledPayload(
            booking_id=booking_id,
            event_id=event_id,
            seat_id=seat_id,
            customer_email="anna@example.com",
            reason="User requested cancellation",
        ).model_dump(mode="json"),
    )
    await handle_event(TOPIC_BOOKING_CANCELLED, cancelled, session_factory, producer)

    ticket_resp = await client.get(f"/api/tickets/by-booking/{booking_id}")
    assert ticket_resp.status_code == 200
    assert ticket_resp.json()["status"] == "CANCELLED"

    notif_resp = await client.get(f"/api/notifications?booking_id={booking_id}")
    assert notif_resp.status_code == 200
    event_types = {n["event_type"] for n in notif_resp.json()}
    assert event_types == {TOPIC_PAYMENT_SUCCEEDED, TOPIC_BOOKING_CANCELLED}


@pytest.mark.asyncio
async def test_idempotency_duplicate_event_and_booking(ticket_env) -> None:
    client: httpx.AsyncClient = ticket_env["client"]
    session_factory = ticket_env["session_factory"]
    producer: FakeProducer = ticket_env["producer"]
    handle_event = ticket_env["handle_event"]

    booking_id = uuid4()
    payload = PaymentSucceededPayload(
        booking_id=booking_id,
        payment_id=uuid4(),
        event_id=uuid4(),
        seat_id=uuid4(),
        customer_name="Пётр Петров",
        customer_email="petr@example.com",
        amount=Decimal("3000.00"),
        provider_tx_id="tx-dup",
    ).model_dump(mode="json")

    first_event_id = uuid4()
    envelope1 = EventEnvelope(
        event_id=first_event_id,
        event_type=TOPIC_PAYMENT_SUCCEEDED,
        occurred_at=datetime.now(timezone.utc),
        correlation_id=uuid4(),
        payload=payload,
    )
    # Exact duplicate event_id
    envelope_same_id = EventEnvelope(
        event_id=first_event_id,
        event_type=TOPIC_PAYMENT_SUCCEEDED,
        occurred_at=datetime.now(timezone.utc),
        correlation_id=uuid4(),
        payload=payload,
    )
    # Different event_id, same booking_id
    envelope_same_booking = EventEnvelope(
        event_id=uuid4(),
        event_type=TOPIC_PAYMENT_SUCCEEDED,
        occurred_at=datetime.now(timezone.utc),
        correlation_id=uuid4(),
        payload=payload,
    )

    await handle_event(TOPIC_PAYMENT_SUCCEEDED, envelope1, session_factory, producer)
    await handle_event(TOPIC_PAYMENT_SUCCEEDED, envelope_same_id, session_factory, producer)
    await handle_event(TOPIC_PAYMENT_SUCCEEDED, envelope_same_booking, session_factory, producer)

    tickets_resp = await client.get("/api/tickets")
    assert len(tickets_resp.json()) == 1
    assert len(producer.published) == 1
