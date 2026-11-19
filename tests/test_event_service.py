from __future__ import annotations

import sys
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from uuid import uuid4

import httpx
import pytest
import pytest_asyncio

SERVICE_DIR = Path(__file__).resolve().parents[1] / "services" / "event-service"


def _load_event_modules():
    existing = sys.modules.get("app")
    existing_file = getattr(existing, "__file__", "") or ""
    if existing is not None and str(SERVICE_DIR) not in existing_file:
        for key in list(sys.modules):
            if key == "app" or key.startswith("app."):
                del sys.modules[key]
    for p in list(sys.path):
        if "services" in p and p != str(SERVICE_DIR):
            sys.path.remove(p)
    if str(SERVICE_DIR) not in sys.path:
        sys.path.insert(0, str(SERVICE_DIR))

    from app.consumer import EventServiceConsumerHandler
    from app.main import create_app

    return create_app, EventServiceConsumerHandler


from shared.database import create_session_factory  # noqa: E402
from shared.events import (  # noqa: E402
    TOPIC_BOOKING_CANCELLED,
    TOPIC_PAYMENT_FAILED,
    TOPIC_PAYMENT_SUCCEEDED,
    BookingCancelledPayload,
    EventEnvelope,
    PaymentFailedPayload,
    PaymentSucceededPayload,
)


@pytest_asyncio.fixture
async def event_app(tmp_path: Path):
    create_app, _ = _load_event_modules()
    db_file = tmp_path / "event_test.db"
    engine, session_factory = create_session_factory(f"sqlite+aiosqlite:///{db_file}")
    app = create_app(
        database_url=f"sqlite+aiosqlite:///{db_file}",
        enable_kafka=False,
        engine=engine,
        session_factory=session_factory,
    )
    async with app.router.lifespan_context(app):
        yield app, session_factory
    await engine.dispose()


async def _seed_event_and_seat(client: httpx.AsyncClient) -> tuple[str, str]:
    venue_resp = await client.post(
        "/api/venues",
        json={"name": "БКЗ Томск", "address": "пл. Ленина, 12а", "capacity": 100},
    )
    assert venue_resp.status_code == 201
    venue_id = venue_resp.json()["id"]

    starts_at = (datetime.now(timezone.utc) + timedelta(days=7)).isoformat()
    event_resp = await client.post(
        "/api/events",
        json={
            "venue_id": venue_id,
            "title": "Концерт симфонического оркестра",
            "description": "Вечер классики",
            "starts_at": starts_at,
            "base_price": "1500.00",
            "rows": 2,
            "seats_per_row": 3,
        },
    )
    assert event_resp.status_code == 201
    event_id = event_resp.json()["id"]

    seats_resp = await client.get(f"/api/events/{event_id}/seats")
    assert seats_resp.status_code == 200
    seats = seats_resp.json()
    assert len(seats) == 6
    assert all(s["status"] == "AVAILABLE" for s in seats)
    return event_id, seats[0]["id"]


@pytest.mark.asyncio
async def test_atomic_seat_reserve_200_and_409_conflict(event_app) -> None:
    app, _ = event_app
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        _, seat_id = await _seed_event_and_seat(client)

        booking_1 = str(uuid4())
        r1 = await client.post(
            f"/internal/seats/{seat_id}/reserve",
            json={"booking_id": booking_1},
        )
        assert r1.status_code == 200
        assert r1.json()["status"] == "RESERVED"
        assert r1.json()["reserved_booking_id"] == booking_1

        # Second concurrent/duplicate reserve on same seat must return 409
        booking_2 = str(uuid4())
        r2 = await client.post(
            f"/internal/seats/{seat_id}/reserve",
            json={"booking_id": booking_2},
        )
        assert r2.status_code == 409

        # Unknown seat returns 404
        r3 = await client.post(
            f"/internal/seats/{uuid4()}/reserve",
            json={"booking_id": str(uuid4())},
        )
        assert r3.status_code == 404


@pytest.mark.asyncio
async def test_payment_succeeded_marks_seat_sold_and_is_idempotent(event_app) -> None:
    app, session_factory = event_app
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        event_id, seat_id = await _seed_event_and_seat(client)
        booking_id = uuid4()
        res = await client.post(
            f"/internal/seats/{seat_id}/reserve",
            json={"booking_id": str(booking_id)},
        )
        assert res.status_code == 200

        _, EventServiceConsumerHandler = _load_event_modules()
        handler = EventServiceConsumerHandler(session_factory)
        payload = PaymentSucceededPayload(
            booking_id=booking_id,
            payment_id=uuid4(),
            event_id=uuid4() if not event_id else event_id,
            seat_id=seat_id,
            customer_name="Иван",
            customer_email="ivan@example.com",
            amount=Decimal("1500.00"),
            provider_tx_id="tx-123",
        )
        env = EventEnvelope(
            event_id=uuid4(),
            event_type=TOPIC_PAYMENT_SUCCEEDED,
            occurred_at=datetime.now(timezone.utc),
            correlation_id=booking_id,
            payload=payload.model_dump(mode="json"),
        )
        await handler.handle(TOPIC_PAYMENT_SUCCEEDED, env)
        # Duplicate delivery of same event_id should be ignored safely
        await handler.handle(TOPIC_PAYMENT_SUCCEEDED, env)

        seats_resp = await client.get(f"/api/events/{event_id}/seats")
        seat = next(s for s in seats_resp.json() if s["id"] == seat_id)
        assert seat["status"] == "SOLD"


@pytest.mark.asyncio
async def test_late_payment_failed_does_not_release_seat_owned_by_new_booking(event_app) -> None:
    """Review Focus #1: late failure/cancel from old booking must not release new booking's seat."""
    app, session_factory = event_app
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        event_id, seat_id = await _seed_event_and_seat(client)
        old_booking = uuid4()
        new_booking = uuid4()

        # Reserve for old_booking, then release it via booking.cancelled
        assert (
            await client.post(
                f"/internal/seats/{seat_id}/reserve",
                json={"booking_id": str(old_booking)},
            )
        ).status_code == 200

        _, EventServiceConsumerHandler = _load_event_modules()
        handler = EventServiceConsumerHandler(session_factory)
        cancel_env = EventEnvelope(
            event_id=uuid4(),
            event_type=TOPIC_BOOKING_CANCELLED,
            occurred_at=datetime.now(timezone.utc),
            correlation_id=old_booking,
            payload=BookingCancelledPayload(
                booking_id=old_booking,
                event_id=event_id,
                seat_id=seat_id,
                customer_email="old@example.com",
                reason="user cancelled",
            ).model_dump(mode="json"),
        )
        await handler.handle(TOPIC_BOOKING_CANCELLED, cancel_env)

        # Now new_booking reserves the same seat
        r_new = await client.post(
            f"/internal/seats/{seat_id}/reserve",
            json={"booking_id": str(new_booking)},
        )
        assert r_new.status_code == 200

        # Late payment.failed arrives for old_booking -> must NOT release seat!
        late_fail_env = EventEnvelope(
            event_id=uuid4(),
            event_type=TOPIC_PAYMENT_FAILED,
            occurred_at=datetime.now(timezone.utc),
            correlation_id=old_booking,
            payload=PaymentFailedPayload(
                booking_id=old_booking,
                payment_id=uuid4(),
                event_id=event_id,
                seat_id=seat_id,
                customer_email="old@example.com",
                reason="late timeout",
            ).model_dump(mode="json"),
        )
        await handler.handle(TOPIC_PAYMENT_FAILED, late_fail_env)

        seats_resp = await client.get(f"/api/events/{event_id}/seats")
        seat = next(s for s in seats_resp.json() if s["id"] == seat_id)
        assert seat["status"] == "RESERVED"
        assert seat["reserved_booking_id"] == str(new_booking)
