"""Pydantic schemas for payment-service REST API."""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from uuid import UUID

from pydantic import BaseModel, ConfigDict


class PaymentResponse(BaseModel):
    """Serialized payment representation."""

    model_config = ConfigDict(from_attributes=True)

    id: UUID
    booking_id: UUID
    customer_email: str
    amount: Decimal
    status: str
    failure_reason: str | None
    provider_tx_id: str | None
    created_at: datetime
    updated_at: datetime
