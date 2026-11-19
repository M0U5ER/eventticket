"""REST read routes for payment-service."""

from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request, status
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models import Payment
from app.schemas import PaymentResponse

router = APIRouter()


async def get_session(request: Request):
    """Yield an AsyncSession from app.state.session_factory."""
    async with request.app.state.session_factory() as session:
        yield session


@router.get("/health")
async def health() -> dict[str, str]:
    """Health check endpoint."""
    return {"status": "ok", "service": "payment-service"}


@router.get("/api/payments", response_model=list[PaymentResponse])
async def list_payments(session: AsyncSession = Depends(get_session)) -> list[PaymentResponse]:
    """List all payments ordered by creation time descending."""
    result = await session.execute(select(Payment).order_by(Payment.created_at.desc()))
    return [PaymentResponse.model_validate(p) for p in result.scalars().all()]


@router.get("/api/payments/by-booking/{booking_id}", response_model=PaymentResponse)
async def get_payment_by_booking(
    booking_id: UUID, session: AsyncSession = Depends(get_session)
) -> PaymentResponse:
    """Get a payment by its booking_id."""
    result = await session.execute(select(Payment).where(Payment.booking_id == booking_id))
    payment = result.scalar_one_or_none()
    if payment is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Payment not found")
    return PaymentResponse.model_validate(payment)
