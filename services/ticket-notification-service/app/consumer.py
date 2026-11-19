from __future__ import annotations

import logging
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.models import Notification, Ticket
from shared.database import try_register_event
from shared.events import (
    BookingCancelledPayload,
    EventEnvelope,
    PaymentFailedPayload,
    PaymentSucceededPayload,
    TOPIC_BOOKING_CANCELLED,
    TOPIC_PAYMENT_FAILED,
    TOPIC_PAYMENT_SUCCEEDED,
    TOPIC_TICKET_ISSUED,
    TicketIssuedPayload,
)
from shared.kafka import KafkaEventProducer

logger = logging.getLogger("ticket-notification-service.consumer")


async def _handle_payment_succeeded(
    session: AsyncSession,
    envelope: EventEnvelope,
    producer: KafkaEventProducer,
) -> TicketIssuedPayload | None:
    payload = PaymentSucceededPayload.model_validate(envelope.payload)

    existing_result = await session.execute(
        select(Ticket).where(Ticket.booking_id == payload.booking_id)
    )
    if existing_result.scalar_one_or_none() is not None:
        logger.info(
            "Ticket already exists for booking_id=%s; skipping duplicate issuance",
            payload.booking_id,
        )
        return None

    ticket = Ticket(
        id=uuid4(),
        booking_id=payload.booking_id,
        event_id=payload.event_id,
        seat_id=payload.seat_id,
        customer_name=payload.customer_name,
        customer_email=str(payload.customer_email),
        ticket_code=f"ET-{uuid4().hex[:8].upper()}",
        status="ACTIVE",
    )
    notification = Notification(
        id=uuid4(),
        booking_id=payload.booking_id,
        recipient_email=str(payload.customer_email),
        channel="EMAIL",
        event_type=TOPIC_PAYMENT_SUCCEEDED,
        subject=f"Билет {ticket.ticket_code} оформлен",
        message=(
            f"Здравствуйте, {payload.customer_name}! "
            f"Оплата {payload.amount} подтверждена. Ваш билет: {ticket.ticket_code}."
        ),
        status="SENT",
    )
    session.add(ticket)
    session.add(notification)

    return TicketIssuedPayload(
        ticket_id=ticket.id,
        booking_id=payload.booking_id,
        ticket_code=ticket.ticket_code,
        customer_email=payload.customer_email,
    )


async def _handle_payment_failed(
    session: AsyncSession,
    envelope: EventEnvelope,
) -> None:
    payload = PaymentFailedPayload.model_validate(envelope.payload)
    notification = Notification(
        id=uuid4(),
        booking_id=payload.booking_id,
        recipient_email=str(payload.customer_email),
        channel="EMAIL",
        event_type=TOPIC_PAYMENT_FAILED,
        subject="Ошибка оплаты бронирования",
        message=(
            f"Не удалось оплатить бронирование {payload.booking_id}. "
            f"Причина: {payload.reason}."
        ),
        status="SENT",
    )
    session.add(notification)


async def _handle_booking_cancelled(
    session: AsyncSession,
    envelope: EventEnvelope,
) -> None:
    payload = BookingCancelledPayload.model_validate(envelope.payload)
    ticket_result = await session.execute(
        select(Ticket).where(Ticket.booking_id == payload.booking_id)
    )
    ticket = ticket_result.scalar_one_or_none()
    if ticket is not None:
        ticket.status = "CANCELLED"

    notification = Notification(
        id=uuid4(),
        booking_id=payload.booking_id,
        recipient_email=str(payload.customer_email),
        channel="EMAIL",
        event_type=TOPIC_BOOKING_CANCELLED,
        subject="Бронирование отменено",
        message=(
            f"Бронирование {payload.booking_id} отменено. "
            f"Причина: {payload.reason}."
        ),
        status="SENT",
    )
    session.add(notification)


async def handle_ticket_event(
    topic: str,
    envelope: EventEnvelope,
    session_factory: async_sessionmaker[AsyncSession],
    producer: KafkaEventProducer,
) -> None:
    ticket_issued_payload: TicketIssuedPayload | None = None

    async with session_factory() as session:
        async with session.begin():
            registered = await try_register_event(session, envelope.event_id)
            if not registered:
                logger.info(
                    "Duplicate event_id=%s on topic=%s; skipping",
                    envelope.event_id,
                    topic,
                )
                return

            if topic == TOPIC_PAYMENT_SUCCEEDED:
                ticket_issued_payload = await _handle_payment_succeeded(
                    session, envelope, producer
                )
            elif topic == TOPIC_PAYMENT_FAILED:
                await _handle_payment_failed(session, envelope)
            elif topic == TOPIC_BOOKING_CANCELLED:
                await _handle_booking_cancelled(session, envelope)
            else:
                logger.warning("Unhandled topic %s in ticket-notification-service", topic)

    if ticket_issued_payload is not None:
        await producer.publish(
            topic=TOPIC_TICKET_ISSUED,
            event_type=TOPIC_TICKET_ISSUED,
            payload=ticket_issued_payload,
            key=str(ticket_issued_payload.booking_id),
            correlation_id=envelope.correlation_id,
        )
