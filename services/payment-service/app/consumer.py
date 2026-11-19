"""Kafka consumer handler for payment-service."""

from __future__ import annotations

from typing import Any
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models import Payment
from shared.database import try_register_event
from shared.events import (
    TOPIC_BOOKING_CANCELLED,
    TOPIC_BOOKING_CREATED,
    TOPIC_PAYMENT_FAILED,
    TOPIC_PAYMENT_SUCCEEDED,
    BookingCancelledPayload,
    BookingCreatedPayload,
    EventEnvelope,
    PaymentFailedPayload,
    PaymentSucceededPayload,
)


class PaymentServiceConsumerHandler:
    """Processes booking.created and booking.cancelled events idempotently."""

    def __init__(
        self, session_factory: async_sessionmaker[AsyncSession], producer: Any
    ) -> None:
        self._session_factory = session_factory
        self._producer = producer

    async def handle(self, topic: str, envelope: EventEnvelope) -> None:
        async with self._session_factory() as session:
            is_new = await try_register_event(session, envelope.event_id)
            if not is_new:
                return

            if topic == TOPIC_BOOKING_CREATED:
                payload = BookingCreatedPayload.model_validate(envelope.payload)
                existing_res = await session.execute(
                    select(Payment).where(Payment.booking_id == payload.booking_id)
                )
                if existing_res.scalar_one_or_none() is not None:
                    await session.commit()
                    return

                if payload.simulate_payment_fail:
                    reason = "Simulated payment failure"
                    payment = Payment(
                        booking_id=payload.booking_id,
                        customer_email=str(payload.customer_email),
                        amount=payload.amount,
                        status="FAILED",
                        failure_reason=reason,
                    )
                    session.add(payment)
                    await session.commit()
                    await session.refresh(payment)

                    await self._producer.publish(
                        topic=TOPIC_PAYMENT_FAILED,
                        event_type=TOPIC_PAYMENT_FAILED,
                        payload=PaymentFailedPayload(
                            booking_id=payload.booking_id,
                            payment_id=payment.id,
                            event_id=payload.event_id,
                            seat_id=payload.seat_id,
                            customer_email=payload.customer_email,
                            reason=reason,
                        ),
                        key=str(payload.booking_id),
                        correlation_id=envelope.correlation_id,
                    )
                else:
                    tx_id = f"tx-{uuid4().hex[:12]}"
                    payment = Payment(
                        booking_id=payload.booking_id,
                        customer_email=str(payload.customer_email),
                        amount=payload.amount,
                        status="SUCCEEDED",
                        provider_tx_id=tx_id,
                    )
                    session.add(payment)
                    await session.commit()
                    await session.refresh(payment)

                    await self._producer.publish(
                        topic=TOPIC_PAYMENT_SUCCEEDED,
                        event_type=TOPIC_PAYMENT_SUCCEEDED,
                        payload=PaymentSucceededPayload(
                            booking_id=payload.booking_id,
                            payment_id=payment.id,
                            event_id=payload.event_id,
                            seat_id=payload.seat_id,
                            customer_name=payload.customer_name,
                            customer_email=payload.customer_email,
                            amount=payload.amount,
                            provider_tx_id=tx_id,
                        ),
                        key=str(payload.booking_id),
                        correlation_id=envelope.correlation_id,
                    )
            elif topic == TOPIC_BOOKING_CANCELLED:
                payload = BookingCancelledPayload.model_validate(envelope.payload)
                result = await session.execute(
                    select(Payment).where(Payment.booking_id == payload.booking_id)
                )
                payment = result.scalar_one_or_none()
                if payment is not None and payment.status == "SUCCEEDED":
                    payment.status = "REFUNDED"
                await session.commit()
            else:
                await session.commit()
