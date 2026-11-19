"""Kafka consumer handler for booking-service."""

from __future__ import annotations

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models import Booking
from shared.database import try_register_event
from shared.events import (
    TOPIC_PAYMENT_FAILED,
    TOPIC_PAYMENT_SUCCEEDED,
    EventEnvelope,
    PaymentFailedPayload,
    PaymentSucceededPayload,
)


class BookingServiceConsumerHandler:
    """Processes payment.succeeded and payment.failed events idempotently."""

    def __init__(self, session_factory: async_sessionmaker[AsyncSession]) -> None:
        self._session_factory = session_factory

    async def handle(self, topic: str, envelope: EventEnvelope) -> None:
        async with self._session_factory() as session:
            is_new = await try_register_event(session, envelope.event_id)
            if not is_new:
                return

            if topic == TOPIC_PAYMENT_SUCCEEDED:
                payload = PaymentSucceededPayload.model_validate(envelope.payload)
                booking = await session.get(Booking, payload.booking_id)
                if booking is not None and booking.status == "PENDING":
                    booking.status = "CONFIRMED"
            elif topic == TOPIC_PAYMENT_FAILED:
                payload = PaymentFailedPayload.model_validate(envelope.payload)
                booking = await session.get(Booking, payload.booking_id)
                if booking is not None and booking.status == "PENDING":
                    booking.status = "CANCELLED"
                    booking.cancel_reason = payload.reason

            await session.commit()
