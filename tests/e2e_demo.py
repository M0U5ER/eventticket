"""End-to-end verification script for the live EventTicket Docker Compose stack."""

from __future__ import annotations

import asyncio
import json
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable

import httpx
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer

GATEWAY_URL = "http://localhost:8000"
KAFKA_BOOTSTRAP = "localhost:29092"
LOG_PATH = Path("labs/lab5-microservices/evidence/e2e-log.txt")


def _log(lines: list[str], message: str) -> None:
    print(message)
    lines.append(message)


async def _wait_until(
    predicate: Callable[[], object],
    timeout: float = 20.0,
    interval: float = 0.5,
):
    deadline = time.monotonic() + timeout
    last_err: Exception | None = None
    while time.monotonic() < deadline:
        try:
            result = predicate()
            if asyncio.iscoroutine(result):
                result = await result
            if result:
                return result
        except Exception as exc:
            last_err = exc
        await asyncio.sleep(interval)
    raise TimeoutError(f"Condition not met within {timeout}s (last_err={last_err})")


async def run_e2e() -> int:
    log_lines: list[str] = []
    _log(log_lines, f"=== EventTicket E2E Demo ({datetime.now(timezone.utc).isoformat()}) ===")

    async with httpx.AsyncClient(base_url=GATEWAY_URL, timeout=15.0) as client:
        # 0. Health check
        health = (await client.get("/health")).json()
        assert health["status"] == "ok", f"Gateway unhealthy: {health}"
        _log(log_lines, f"[0] Gateway /health -> {json.dumps(health)}")

        # 1. Create venue + event (2 rows x 3 seats = 6 seats)
        venue_resp = await client.post(
            "/api/venues",
            json={"name": "ТПУ Главный зал", "address": "пр. Ленина, 30", "capacity": 100},
        )
        assert venue_resp.status_code == 201, venue_resp.text
        venue_id = venue_resp.json()["id"]

        starts_at = (datetime.now(timezone.utc) + timedelta(days=7)).isoformat()
        event_resp = await client.post(
            "/api/events",
            json={
                "venue_id": venue_id,
                "title": "Конференция ИШИТР 2026",
                "starts_at": starts_at,
                "base_price": "1500.00",
                "rows": 2,
                "seats_per_row": 3,
            },
        )
        assert event_resp.status_code == 201, event_resp.text
        event_id = event_resp.json()["id"]

        seats = (await client.get(f"/api/events/{event_id}/seats")).json()
        assert len(seats) == 6
        seat1, seat2 = seats[0], seats[1]
        _log(log_lines, f"[1] PASS: Venue {venue_id}, Event {event_id}, created {len(seats)} seats")

        # 2. Booking seat 1 (simulate_payment_fail=False) + Idempotency-Key check
        idem_key = f"demo-1-{int(time.time())}"
        b1_payload = {
            "event_id": event_id,
            "seat_id": seat1["id"],
            "customer_name": "Казанцев И.К.",
            "customer_email": "demo1@tpu.ru",
            "amount": "1500.00",
            "simulate_payment_fail": False,
        }
        b1_resp = await client.post("/api/bookings", json=b1_payload, headers={"Idempotency-Key": idem_key})
        assert b1_resp.status_code == 201, b1_resp.text
        b1_id = b1_resp.json()["id"]

        b1_dup = await client.post("/api/bookings", json=b1_payload, headers={"Idempotency-Key": idem_key})
        assert b1_dup.status_code in (200, 201) and b1_dup.json()["id"] == b1_id

        async def _b1_confirmed():
            r = await client.get(f"/api/bookings/{b1_id}")
            return r.json() if r.json()["status"] == "CONFIRMED" else None

        await _wait_until(_b1_confirmed)
        pay1 = (await client.get(f"/api/payments/by-booking/{b1_id}")).json()
        assert pay1["status"] == "SUCCEEDED"

        async def _ticket_active():
            r = await client.get(f"/api/tickets/by-booking/{b1_id}")
            return r.json() if r.status_code == 200 and r.json()["status"] == "ACTIVE" else None

        ticket1 = await _wait_until(_ticket_active)
        seats_after_b1 = (await client.get(f"/api/events/{event_id}/seats")).json()
        assert next(s for s in seats_after_b1 if s["id"] == seat1["id"])["status"] == "SOLD"
        _log(log_lines, f"[2] PASS: Booking {b1_id} CONFIRMED, Payment SUCCEEDED, Ticket {ticket1['ticket_code']} ACTIVE, idempotency verified")

        # 3. Second buyer tries to reserve seat 1 -> 409 Conflict
        b_conflict = await client.post(
            "/api/bookings",
            json={**b1_payload, "customer_email": "other@tpu.ru"},
            headers={"Idempotency-Key": f"demo-conflict-{int(time.time())}"},
        )
        assert b_conflict.status_code == 409, f"Expected 409, got {b_conflict.status_code}: {b_conflict.text}"
        _log(log_lines, f"[3] PASS: Concurrent booking of seat 1 rejected with 409 Conflict")

        # 4. Booking seat 2 (simulate_payment_fail=True) -> compensation via Kafka
        b2_resp = await client.post(
            "/api/bookings",
            json={
                "event_id": event_id,
                "seat_id": seat2["id"],
                "customer_name": "Тестовый Покупатель",
                "customer_email": "demo2@tpu.ru",
                "amount": "1500.00",
                "simulate_payment_fail": True,
            },
            headers={"Idempotency-Key": f"demo-fail-{int(time.time())}"},
        )
        assert b2_resp.status_code == 201
        b2_id = b2_resp.json()["id"]

        async def _b2_cancelled():
            r = await client.get(f"/api/bookings/{b2_id}")
            return r.json() if r.json()["status"] == "CANCELLED" else None

        await _wait_until(_b2_cancelled)
        seats_after_b2 = (await client.get(f"/api/events/{event_id}/seats")).json()
        assert next(s for s in seats_after_b2 if s["id"] == seat2["id"])["status"] == "AVAILABLE"
        notifs_b2 = (await client.get("/api/notifications", params={"email": "demo2@tpu.ru"})).json()
        assert any(n["event_type"] == "payment.failed" for n in notifs_b2)
        _log(log_lines, f"[4] PASS: Booking {b2_id} compensated to CANCELLED, seat 2 returned to AVAILABLE, payment.failed notification logged")

        # 5. Manual cancellation of confirmed booking 1
        cancel_resp = await client.post(
            f"/api/bookings/{b1_id}/cancel",
            json={"reason": "E2E demo manual cancellation"},
        )
        assert cancel_resp.status_code == 200, cancel_resp.text
        assert cancel_resp.json()["status"] == "CANCELLED"

        async def _b1_refunded_and_released():
            p = (await client.get(f"/api/payments/by-booking/{b1_id}")).json()
            t = (await client.get(f"/api/tickets/by-booking/{b1_id}")).json()
            s_list = (await client.get(f"/api/events/{event_id}/seats")).json()
            seat1_status = next(s for s in s_list if s["id"] == seat1["id"])["status"]
            return p["status"] == "REFUNDED" and t["status"] == "CANCELLED" and seat1_status == "AVAILABLE"

        await _wait_until(_b1_refunded_and_released)
        _log(log_lines, f"[5] PASS: Manual cancel of {b1_id} -> Payment REFUNDED, Ticket CANCELLED, Seat 1 AVAILABLE")

    # 6. Poison-pill message -> eventticket.dlq
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP)
    consumer = AIOKafkaConsumer(
        "eventticket.dlq",
        bootstrap_servers=KAFKA_BOOTSTRAP,
        auto_offset_reset="earliest",
        group_id=f"e2e-dlq-check-{int(time.time())}",
    )
    await producer.start()
    await consumer.start()
    try:
        await producer.send_and_wait("booking.created", value=b"{broken", key=b"e2e-poison")

        async def _dlq_has_poison():
            batch = await consumer.getmany(timeout_ms=1000)
            for _, records in batch.items():
                for rec in records:
                    if b"{broken" in rec.value:
                        return rec.value.decode("utf-8", errors="replace")
            return None

        dlq_msg = await _wait_until(_dlq_has_poison)
        _log(log_lines, f"[6] PASS: Poison pill routed to eventticket.dlq: {dlq_msg[:180]}")
    finally:
        await consumer.stop()
        await producer.stop()

    LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    LOG_PATH.write_text("\n".join(log_lines) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    sys.exit(asyncio.run(run_e2e()))
