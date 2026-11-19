from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from pydantic import BaseModel, EmailStr

TOPIC_BOOKING_CREATED = "booking.created"
TOPIC_PAYMENT_SUCCEEDED = "payment.succeeded"
TOPIC_PAYMENT_FAILED = "payment.failed"
TOPIC_BOOKING_CANCELLED = "booking.cancelled"
TOPIC_TICKET_ISSUED = "ticket.issued"
TOPIC_DLQ = "eventticket.dlq"


class EventEnvelope(BaseModel):
    """Unified Kafka event envelope for EventTicket choreography."""

    event_id: UUID
    event_type: str
    occurred_at: datetime
    correlation_id: UUID
    payload: dict[str, Any]


class BookingCreatedPayload(BaseModel):
    booking_id: UUID
    event_id: UUID
    seat_id: UUID
    customer_name: str
    customer_email: EmailStr
    amount: Decimal
    simulate_payment_fail: bool = False


class PaymentSucceededPayload(BaseModel):
    booking_id: UUID
    payment_id: UUID
    event_id: UUID
    seat_id: UUID
    customer_name: str
    customer_email: EmailStr
    amount: Decimal
    provider_tx_id: str


class PaymentFailedPayload(BaseModel):
    booking_id: UUID
    payment_id: UUID
    event_id: UUID
    seat_id: UUID
    customer_email: EmailStr
    reason: str


class BookingCancelledPayload(BaseModel):
    booking_id: UUID
    event_id: UUID
    seat_id: UUID
    customer_email: EmailStr
    reason: str


class TicketIssuedPayload(BaseModel):
    ticket_id: UUID
    booking_id: UUID
    ticket_code: str
    customer_email: EmailStr


class DlqEnvelope(BaseModel):
    """Dead-letter queue payload emitted after retries are exhausted or on parse failure."""

    source_topic: str
    consumer_group: str
    error_message: str
    raw_payload: str
    failed_at: datetime
