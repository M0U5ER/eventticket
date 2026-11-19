from __future__ import annotations

import asyncio

import httpx
from fastapi import APIRouter, HTTPException, Request, Response, status

router = APIRouter()

_HOP_BY_HOP_HEADERS = {"host", "content-length", "connection", "accept-encoding"}
_PROXY_METHODS = ["GET", "POST", "PUT", "PATCH", "DELETE"]

_ROUTE_MAPPINGS: list[tuple[str, str, list[str]]] = [
    ("/api/venues", "event-service", ["GET", "POST"]),
    ("/api/venues/{path:path}", "event-service", _PROXY_METHODS),
    ("/api/events", "event-service", ["GET", "POST"]),
    ("/api/events/{path:path}", "event-service", _PROXY_METHODS),
    ("/api/bookings", "booking-service", ["GET", "POST"]),
    ("/api/bookings/{path:path}", "booking-service", _PROXY_METHODS),
    ("/api/payments", "payment-service", ["GET"]),
    ("/api/payments/{path:path}", "payment-service", ["GET"]),
    ("/api/tickets", "ticket-notification-service", ["GET"]),
    ("/api/tickets/{path:path}", "ticket-notification-service", ["GET"]),
    ("/api/notifications", "ticket-notification-service", ["GET"]),
    ("/api/notifications/{path:path}", "ticket-notification-service", ["GET"]),
]


async def _check_downstream_health(
    http_client: httpx.AsyncClient,
    service_name: str,
    base_url: str,
) -> tuple[str, str]:
    try:
        resp = await http_client.get(f"{base_url}/health", timeout=3.0)
        if resp.status_code == 200:
            return service_name, "ok"
        return service_name, f"unhealthy ({resp.status_code})"
    except httpx.HTTPError:
        return service_name, "unreachable"


@router.get("/health", tags=["health"])
async def health(request: Request) -> dict:
    service_urls: dict[str, str] = request.app.state.service_urls
    http_client: httpx.AsyncClient = request.app.state.http_client

    results = await asyncio.gather(
        *(
            _check_downstream_health(http_client, name, url)
            for name, url in service_urls.items()
        )
    )
    statuses = dict(results)
    overall = "ok" if all(v == "ok" for v in statuses.values()) else "degraded"
    return {"status": overall, "service": "api-gateway", "services": statuses}


async def _proxy_request(request: Request, service_key: str) -> Response:
    service_urls: dict[str, str] = request.app.state.service_urls
    http_client: httpx.AsyncClient = request.app.state.http_client
    target_base = service_urls[service_key].rstrip("/")
    target_url = f"{target_base}{request.url.path}"

    forwarded_headers = {
        k: v for k, v in request.headers.items() if k.lower() not in _HOP_BY_HOP_HEADERS
    }
    body = await request.body()

    try:
        upstream = await http_client.request(
            method=request.method,
            url=target_url,
            params=request.query_params,
            headers=forwarded_headers,
            content=body,
        )
    except httpx.TimeoutException as exc:
        raise HTTPException(
            status_code=status.HTTP_504_GATEWAY_TIMEOUT,
            detail=f"Timeout calling {service_key}",
        ) from exc
    except httpx.RequestError as exc:
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail=f"Upstream {service_key} unreachable",
        ) from exc

    media_type = upstream.headers.get("content-type")
    return Response(
        content=upstream.content,
        status_code=upstream.status_code,
        media_type=media_type,
    )


def _register_proxy_routes() -> None:
    for path_pattern, service_key, methods in _ROUTE_MAPPINGS:
        async def _handler(
            request: Request,
            _svc: str = service_key,
        ) -> Response:
            return await _proxy_request(request, _svc)

        router.add_api_route(
            path_pattern,
            _handler,
            methods=methods,
            tags=[service_key],
            name=f"proxy_{service_key}_{path_pattern.strip('/').replace('/', '_')}",
        )


_register_proxy_routes()
