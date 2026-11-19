"""Pydantic schemas for booking-service REST API."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, EmailStr, Field


class BookingCreate(BaseModel):
    """Request body for creating a booking."""

    event_id: UUID
    seat_id: UUID
    customer_name: str = Field(..., min_length=1, max_length=200)
    customer_email: EmailStr
    simulate_payment_fail: bool = False


class BookingCancelRequest(BaseModel):
    """Request body for manual cancellation of a CONFIRMED booking."""

    reason: str = Field(default="Cancelled by customer", min_length=1, max_length=500)


class BookingResponse(BaseModel):
    """Serialized booking representation."""

    model_config = ConfigDict(from_attributes=True)

    id: UUID
    event_id: UUID
    seat_id: UUID
    customer_name: str
    customer_email: str
    amount: Decimal
    status: str
    cancel_reason: str | None
    idempotency_key: str | None
    created_at: datetime
    updated_at: datetime
