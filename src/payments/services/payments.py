import logging
from uuid import UUID, uuid4

from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from payments.db.models import Payment
from payments.domain.exceptions import PaymentNotFoundError
from payments.messaging.events import PAYMENT_CREATED, PaymentCreatedEvent
from payments.messaging.topology import NEW_PAYMENTS_ROUTING_KEY
from payments.repositories.outbox import OutboxRepository
from payments.repositories.payments import PaymentRepository
from payments.schemas import PaymentCreate

logger = logging.getLogger(__name__)


class PaymentService:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session
        self._payments = PaymentRepository(session)
        self._outbox = OutboxRepository(session)

    async def create(self, data: PaymentCreate, idempotency_key: str) -> Payment:
        """Create a payment, or return the one already created with this key.

        The payment and its outbox event are committed in one transaction, so
        an event exists if and only if the payment does.
        """
        existing = await self._payments.get_by_idempotency_key(idempotency_key)
        if existing is not None:
            logger.info("Idempotent replay for key %r -> payment %s", idempotency_key, existing.id)
            return existing

        payment = Payment(
            id=uuid4(),
            amount=data.amount,
            currency=data.currency,
            description=data.description,
            meta=data.metadata,
            idempotency_key=idempotency_key,
            webhook_url=str(data.webhook_url),
        )
        self._payments.add(payment)
        self._outbox.add(
            event_type=PAYMENT_CREATED,
            routing_key=NEW_PAYMENTS_ROUTING_KEY,
            payload=PaymentCreatedEvent(payment_id=payment.id).model_dump(mode="json"),
        )

        try:
            await self._session.commit()
        except IntegrityError:
            # A concurrent request with the same key won the race on the UNIQUE constraint.
            await self._session.rollback()
            existing = await self._payments.get_by_idempotency_key(idempotency_key)
            if existing is None:
                raise
            return existing

        logger.info("Payment %s created", payment.id)
        return payment

    async def get(self, payment_id: UUID) -> Payment:
        payment = await self._payments.get(payment_id)
        if payment is None:
            raise PaymentNotFoundError(payment_id)
        return payment
