from __future__ import annotations

from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict


class TicketResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    booking_id: UUID
    event_id: UUID
    seat_id: UUID
    customer_name: str
    customer_email: str
    ticket_code: str
    status: str
    issued_at: datetime


class NotificationResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    booking_id: UUID
    recipient_email: str
    channel: str
    event_type: str
    subject: str
    message: str
    status: str
    created_at: datetime
