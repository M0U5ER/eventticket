from __future__ import annotations

import sys
from pathlib import Path

import httpx
import pytest
import pytest_asyncio

SERVICE_DIR = Path(__file__).resolve().parents[1] / "services" / "api-gateway"


def _load_gateway_modules():
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

    from app.main import create_app

    return create_app


@pytest_asyncio.fixture
async def gateway_client():
    create_app = _load_gateway_modules()
    recorded_requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        recorded_requests.append(request)
        host = request.url.host
        path = request.url.path

        if path == "/health":
            if host == "payment-service-down":
                raise httpx.ConnectError("connection refused", request=request)
            return httpx.Response(200, json={"status": "ok", "service": host})

        if host == "event-service" and path == "/api/venues" and request.method == "POST":
            return httpx.Response(201, json={"id": "v1", "name": "Hall A"})

        if host == "event-service" and path == "/api/events/e1/seats" and request.method == "GET":
            return httpx.Response(200, json=[{"id": "s1", "status": "AVAILABLE"}])

        if host == "booking-service" and path == "/api/bookings" and request.method == "POST":
            idem = request.headers.get("Idempotency-Key")
            return httpx.Response(201, json={"id": "b1", "idempotency_key": idem})

        if host == "booking-service" and path == "/api/bookings/b1/cancel" and request.method == "POST":
            return httpx.Response(409, json={"detail": "Cannot cancel PENDING booking"})

        if host == "payment-service" and path == "/api/payments/by-booking/b1":
            return httpx.Response(200, json={"booking_id": "b1", "status": "SUCCEEDED"})

        if host == "ticket-notification-service" and path == "/api/tickets/by-booking/b1":
            return httpx.Response(200, json={"booking_id": "b1", "ticket_code": "ET-1234"})

        if host == "ticket-notification-service" and path == "/api/notifications":
            return httpx.Response(
                200,
                json=[{"recipient_email": request.url.params.get("email"), "event_type": "payment.succeeded"}],
            )

        if host == "down-service":
            raise httpx.ConnectError("connection refused", request=request)

        if host == "slow-service":
            raise httpx.ReadTimeout("timed out", request=request)

        return httpx.Response(404, json={"detail": "Not found"})

    mock_http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    app = create_app(http_client=mock_http)
    app.state.service_urls = {
        "event-service": "http://event-service:8001",
        "booking-service": "http://booking-service:8002",
        "payment-service": "http://payment-service:8003",
        "ticket-notification-service": "http://ticket-notification-service:8004",
    }

    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        yield client, app, recorded_requests

    await mock_http.aclose()


@pytest.mark.asyncio
async def test_aggregated_health_ok_and_degraded(gateway_client):
    client, app, _ = gateway_client

    resp = await client.get("/health")
    assert resp.status_code == 200
    body = resp.json()
    assert body["status"] == "ok"
    assert body["services"] == {
        "event-service": "ok",
        "booking-service": "ok",
        "payment-service": "ok",
        "ticket-notification-service": "ok",
    }

    app.state.service_urls["payment-service"] = "http://payment-service-down:8003"
    resp_deg = await client.get("/health")
    assert resp_deg.status_code == 200
    deg = resp_deg.json()
    assert deg["status"] == "degraded"
    assert deg["services"]["payment-service"] == "unreachable"
    assert deg["services"]["event-service"] == "ok"


@pytest.mark.asyncio
async def test_proxy_routes_forward_method_headers_and_query(gateway_client):
    client, _, recorded = gateway_client

    r1 = await client.post("/api/venues", json={"name": "Hall A", "city": "Tomsk"})
    assert r1.status_code == 201
    assert r1.json()["name"] == "Hall A"

    r2 = await client.get("/api/events/e1/seats")
    assert r2.status_code == 200
    assert r2.json()[0]["status"] == "AVAILABLE"

    r3 = await client.post(
        "/api/bookings",
        json={"event_id": "e1", "seat_id": "s1"},
        headers={"Idempotency-Key": "demo-1"},
    )
    assert r3.status_code == 201
    assert r3.json()["idempotency_key"] == "demo-1"

    r4 = await client.post("/api/bookings/b1/cancel")
    assert r4.status_code == 409

    r5 = await client.get("/api/payments/by-booking/b1")
    assert r5.status_code == 200
    assert r5.json()["status"] == "SUCCEEDED"

    r6 = await client.get("/api/tickets/by-booking/b1")
    assert r6.status_code == 200
    assert r6.json()["ticket_code"] == "ET-1234"

    r7 = await client.get("/api/notifications", params={"email": "user@example.com"})
    assert r7.status_code == 200
    assert r7.json()[0]["recipient_email"] == "user@example.com"


@pytest.mark.asyncio
async def test_proxy_returns_502_and_504_on_downstream_errors(gateway_client):
    client, app, _ = gateway_client

    app.state.service_urls["event-service"] = "http://down-service:8001"
    r_down = await client.get("/api/events/e1/seats")
    assert r_down.status_code == 502

    app.state.service_urls["booking-service"] = "http://slow-service:8002"
    r_slow = await client.get("/api/bookings")
    assert r_slow.status_code == 504
