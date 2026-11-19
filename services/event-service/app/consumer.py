from __future__ import annotations

import logging

from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models import Seat
from shared.database import try_register_event
from shared.events import (
    TOPIC_BOOKING_CANCELLED,
    TOPIC_PAYMENT_FAILED,
    TOPIC_PAYMENT_SUCCEEDED,
    BookingCancelledPayload,
    EventEnvelope,
    PaymentFailedPayload,
    PaymentSucceededPayload,
)

logger = logging.getLogger(__name__)


class EventServiceConsumerHandler:
    """Handles payment and cancellation events to update seat ownership."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def handle(self, topic: str, envelope: EventEnvelope) -> None:
        async with self._session_factory() as session:
            if not await try_register_event(session, envelope.event_id):
                logger.info("Duplicate event %s ignored", envelope.event_id)
                await session.rollback()
                return

            if topic == TOPIC_PAYMENT_SUCCEEDED:
                payload = PaymentSucceededPayload.model_validate(envelope.payload)
                await session.execute(
                    update(Seat)
                    .where(
                        Seat.id == payload.seat_id,
                        Seat.reserved_booking_id == payload.booking_id,
                    )
                    .values(status="SOLD", version=Seat.version + 1)
                )
            elif topic == TOPIC_PAYMENT_FAILED:
                payload = PaymentFailedPayload.model_validate(envelope.payload)
                await self._release_seat(session, payload.seat_id, payload.booking_id)
            elif topic == TOPIC_BOOKING_CANCELLED:
                payload = BookingCancelledPayload.model_validate(envelope.payload)
                await self._release_seat(session, payload.seat_id, payload.booking_id)

            await session.commit()

    @staticmethod
    async def _release_seat(
        session: AsyncSession,
        seat_id,
        booking_id,
    ) -> None:
        # Only release if this exact booking currently holds the seat.
        await session.execute(
            update(Seat)
            .where(
                Seat.id == seat_id,
                Seat.reserved_booking_id == booking_id,
            )
            .values(
                status="AVAILABLE",
                reserved_booking_id=None,
                version=Seat.version + 1,
            )
        )
