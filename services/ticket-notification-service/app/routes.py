from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, HTTPException, Query, Request, status
from sqlalchemy import select

from app.models import Notification, Ticket
from app.schemas import NotificationResponse, TicketResponse

router = APIRouter()


@router.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok", "service": "ticket-notification-service"}


@router.get("/api/tickets", response_model=list[TicketResponse])
async def list_tickets(request: Request) -> list[TicketResponse]:
    session_factory = request.app.state.session_factory
    async with session_factory() as session:
        result = await session.execute(
            select(Ticket).order_by(Ticket.issued_at.desc())
        )
        tickets = result.scalars().all()
        return [TicketResponse.model_validate(t) for t in tickets]


@router.get("/api/tickets/by-booking/{booking_id}", response_model=TicketResponse)
async def get_ticket_by_booking(booking_id: UUID, request: Request) -> TicketResponse:
    session_factory = request.app.state.session_factory
    async with session_factory() as session:
        result = await session.execute(
            select(Ticket).where(Ticket.booking_id == booking_id)
        )
        ticket = result.scalar_one_or_none()
        if ticket is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Ticket for booking {booking_id} not found",
            )
        return TicketResponse.model_validate(ticket)


@router.get("/api/notifications", response_model=list[NotificationResponse])
async def list_notifications(
    request: Request,
    booking_id: UUID | None = Query(default=None),
) -> list[NotificationResponse]:
    session_factory = request.app.state.session_factory
    async with session_factory() as session:
        stmt = select(Notification)
        if booking_id is not None:
            stmt = stmt.where(Notification.booking_id == booking_id)
        stmt = stmt.order_by(Notification.created_at.desc())
        result = await session.execute(stmt)
        notifications = result.scalars().all()
        return [NotificationResponse.model_validate(n) for n in notifications]
