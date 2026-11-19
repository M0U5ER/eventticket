from __future__ import annotations

from collections.abc import AsyncGenerator
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request, status
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Event, Seat, Venue
from app.schemas import (
    EventCreate,
    EventResponse,
    SeatReserveRequest,
    SeatReserveResponse,
    SeatResponse,
    VenueCreate,
    VenueResponse,
)

router = APIRouter()


async def get_session(request: Request) -> AsyncGenerator[AsyncSession, None]:
    session_factory = request.app.state.session_factory
    async with session_factory() as session:
        yield session


@router.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok", "service": "event-service"}


@router.post(
    "/api/venues",
    response_model=VenueResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_venue(
    body: VenueCreate,
    session: AsyncSession = Depends(get_session),
) -> Venue:
    venue = Venue(name=body.name, address=body.address, capacity=body.capacity)
    session.add(venue)
    await session.commit()
    await session.refresh(venue)
    return venue


@router.get("/api/venues", response_model=list[VenueResponse])
async def list_venues(session: AsyncSession = Depends(get_session)) -> list[Venue]:
    result = await session.execute(select(Venue).order_by(Venue.created_at.desc()))
    return list(result.scalars().all())


@router.post(
    "/api/events",
    response_model=EventResponse,
    status_code=status.HTTP_201_CREATED,
)
async def create_event(
    body: EventCreate,
    session: AsyncSession = Depends(get_session),
) -> Event:
    venue = await session.get(Venue, body.venue_id)
    if venue is None:
        raise HTTPException(status_code=404, detail="Venue not found")

    event = Event(
        venue_id=body.venue_id,
        title=body.title,
        description=body.description,
        starts_at=body.starts_at,
        base_price=body.base_price,
    )
    session.add(event)
    await session.flush()

    seats = [
        Seat(
            event_id=event.id,
            row_num=row,
            seat_num=seat_num,
            price=body.base_price,
            status="AVAILABLE",
        )
        for row in range(1, body.rows + 1)
        for seat_num in range(1, body.seats_per_row + 1)
    ]
    session.add_all(seats)
    await session.commit()
    await session.refresh(event)
    return event


@router.get("/api/events", response_model=list[EventResponse])
async def list_events(session: AsyncSession = Depends(get_session)) -> list[Event]:
    result = await session.execute(select(Event).order_by(Event.starts_at.asc()))
    return list(result.scalars().all())


@router.get("/api/events/{event_id}", response_model=EventResponse)
async def get_event(
    event_id: UUID,
    session: AsyncSession = Depends(get_session),
) -> Event:
    event = await session.get(Event, event_id)
    if event is None:
        raise HTTPException(status_code=404, detail="Event not found")
    return event


@router.get("/api/events/{event_id}/seats", response_model=list[SeatResponse])
async def list_event_seats(
    event_id: UUID,
    session: AsyncSession = Depends(get_session),
) -> list[Seat]:
    event = await session.get(Event, event_id)
    if event is None:
        raise HTTPException(status_code=404, detail="Event not found")
    result = await session.execute(
        select(Seat)
        .where(Seat.event_id == event_id)
        .order_by(Seat.row_num.asc(), Seat.seat_num.asc())
    )
    return list(result.scalars().all())


@router.post(
    "/internal/seats/{seat_id}/reserve",
    response_model=SeatReserveResponse,
)
async def reserve_seat(
    seat_id: UUID,
    body: SeatReserveRequest,
    session: AsyncSession = Depends(get_session),
) -> SeatReserveResponse:
    stmt = (
        update(Seat)
        .where(Seat.id == seat_id, Seat.status == "AVAILABLE")
        .values(
            status="RESERVED",
            reserved_booking_id=body.booking_id,
            version=Seat.version + 1,
        )
    )
    result = await session.execute(stmt)
    if result.rowcount == 0:
        existing = await session.get(Seat, seat_id)
        if existing is None:
            raise HTTPException(status_code=404, detail="Seat not found")
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail=f"Seat is already {existing.status}",
        )
    await session.commit()
    seat = await session.get(Seat, seat_id)
    assert seat is not None
    return SeatReserveResponse(
        seat_id=seat.id,
        event_id=seat.event_id,
        price=seat.price,
        status=seat.status,
        reserved_booking_id=body.booking_id,
    )
