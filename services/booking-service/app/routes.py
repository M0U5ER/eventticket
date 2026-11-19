"""HTTP client and REST routes for booking-service."""

from __future__ import annotations

from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

import httpx
from fastapi import APIRouter, Depends, Header, HTTPException, Request, status
from fastapi.responses import JSONResponse
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Booking
from app.schemas import BookingCancelRequest, BookingCreate, BookingResponse
from shared.events import (
    TOPIC_BOOKING_CANCELLED,
    TOPIC_BOOKING_CREATED,
    BookingCancelledPayload,
    BookingCreatedPayload,
)

router = APIRouter()


class EventServiceClient:
    """Async HTTP client for synchronous seat reservation in event-service."""

    def __init__(self, base_url: str, timeout: float = 5.0) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout

    async def reserve_seat(
        self, seat_id: UUID, booking_id: UUID, event_id: UUID
    ) -> dict[str, Any]:
        url = f"{self._base_url}/internal/seats/{seat_id}/reserve"
        async with httpx.AsyncClient(timeout=self._timeout) as client:
            response = await client.post(url, json={"booking_id": str(booking_id)})
            response.raise_for_status()
            return response.json()


async def get_session(request: Request):
    """Yield an AsyncSession from app.state.session_factory."""
    async with request.app.state.session_factory() as session:
        yield session


@router.get("/health")
async def health() -> dict[str, str]:
    """Health check endpoint."""
    return {"status": "ok", "service": "booking-service"}


@router.post("/api/bookings", response_model=BookingResponse, status_code=status.HTTP_201_CREATED)
async def create_booking(
    payload: BookingCreate,
    request: Request,
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
    session: AsyncSession = Depends(get_session),
) -> BookingResponse | JSONResponse:
    """Reserve a seat synchronously in event-service, persist PENDING booking, publish booking.created."""
    if idempotency_key:
        existing_res = await session.execute(
            select(Booking).where(Booking.idempotency_key == idempotency_key)
        )
        existing = existing_res.scalar_one_or_none()
        if existing is not None:
            return JSONResponse(
                status_code=status.HTTP_200_OK,
                content=BookingResponse.model_validate(existing).model_dump(mode="json"),
            )

    booking_id = uuid4()
    event_client = request.app.state.event_client
    try:
        seat_data = await event_client.reserve_seat(
            seat_id=payload.seat_id,
            booking_id=booking_id,
            event_id=payload.event_id,
        )
    except httpx.HTTPStatusError as exc:
        detail = "Seat reservation failed"
        try:
            body = exc.response.json()
            detail = body.get("detail", detail)
        except Exception:
            pass
        raise HTTPException(status_code=exc.response.status_code, detail=detail) from exc
    except httpx.RequestError as exc:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="event-service is unavailable",
        ) from exc

    amount = Decimal(str(seat_data["price"]))
    booking = Booking(
        id=booking_id,
        event_id=payload.event_id,
        seat_id=payload.seat_id,
        customer_name=payload.customer_name,
        customer_email=str(payload.customer_email),
        amount=amount,
        status="PENDING",
        idempotency_key=idempotency_key,
    )
    session.add(booking)
    await session.commit()
    await session.refresh(booking)

    event_payload = BookingCreatedPayload(
        booking_id=booking.id,
        event_id=booking.event_id,
        seat_id=booking.seat_id,
        customer_name=booking.customer_name,
        customer_email=booking.customer_email,
        amount=booking.amount,
        simulate_payment_fail=payload.simulate_payment_fail,
    )
    await request.app.state.producer.publish(
        topic=TOPIC_BOOKING_CREATED,
        event_type=TOPIC_BOOKING_CREATED,
        payload=event_payload,
        key=str(booking.id),
        correlation_id=booking.id,
    )
    return BookingResponse.model_validate(booking)


@router.get("/api/bookings", response_model=list[BookingResponse])
async def list_bookings(session: AsyncSession = Depends(get_session)) -> list[BookingResponse]:
    """List all bookings ordered by creation time descending."""
    result = await session.execute(select(Booking).order_by(Booking.created_at.desc()))
    return [BookingResponse.model_validate(b) for b in result.scalars().all()]


@router.get("/api/bookings/{booking_id}", response_model=BookingResponse)
async def get_booking(
    booking_id: UUID, session: AsyncSession = Depends(get_session)
) -> BookingResponse:
    """Get a booking by ID."""
    booking = await session.get(Booking, booking_id)
    if booking is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Booking not found")
    return BookingResponse.model_validate(booking)


@router.post("/api/bookings/{booking_id}/cancel", response_model=BookingResponse)
async def cancel_booking(
    booking_id: UUID,
    payload: BookingCancelRequest,
    request: Request,
    session: AsyncSession = Depends(get_session),
) -> BookingResponse:
    """Manually cancel a booking. Allowed ONLY in CONFIRMED status (409 Conflict otherwise)."""
    booking = await session.get(Booking, booking_id)
    if booking is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Booking not found")
    if booking.status != "CONFIRMED":
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Cannot cancel booking in status {booking.status}; only CONFIRMED bookings can be cancelled",
        )

    booking.status = "CANCELLED"
    booking.cancel_reason = payload.reason
    await session.commit()
    await session.refresh(booking)

    cancelled_payload = BookingCancelledPayload(
        booking_id=booking.id,
        event_id=booking.event_id,
        seat_id=booking.seat_id,
        customer_email=booking.customer_email,
        reason=payload.reason,
    )
    await request.app.state.producer.publish(
        topic=TOPIC_BOOKING_CANCELLED,
        event_type=TOPIC_BOOKING_CANCELLED,
        payload=cancelled_payload,
        key=str(booking.id),
        correlation_id=booking.id,
    )
    return BookingResponse.model_validate(booking)
