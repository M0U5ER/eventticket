from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class VenueCreate(BaseModel):
    name: str = Field(..., min_length=1, max_length=200)
    address: str = Field(..., min_length=1, max_length=300)
    capacity: int = Field(..., gt=0)


class VenueResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    name: str
    address: str
    capacity: int
    created_at: datetime


class EventCreate(BaseModel):
    venue_id: UUID
    title: str = Field(..., min_length=1, max_length=200)
    description: str = ""
    starts_at: datetime
    base_price: Decimal = Field(..., gt=0)
    rows: int = Field(default=1, ge=1, le=50)
    seats_per_row: int = Field(default=10, ge=1, le=50)


class EventResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    venue_id: UUID
    title: str
    description: str
    starts_at: datetime
    base_price: Decimal
    status: str
    created_at: datetime


class SeatResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    id: UUID
    event_id: UUID
    row_num: int
    seat_num: int
    price: Decimal
    status: str
    reserved_booking_id: UUID | None
    version: int


class SeatReserveRequest(BaseModel):
    booking_id: UUID


class SeatReserveResponse(BaseModel):
    seat_id: UUID
    event_id: UUID
    price: Decimal
    status: str
    reserved_booking_id: UUID
