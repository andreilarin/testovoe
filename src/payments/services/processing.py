import logging
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payments.db.models import utcnow
from payments.domain.enums import PaymentStatus
from payments.domain.exceptions import PaymentNotFoundError
from payments.infra.gateway import PaymentGateway
from payments.infra.webhooks import WebhookNotifier
from payments.repositories.payments import PaymentRepository
from payments.schemas import WebhookPayload

logger = logging.getLogger(__name__)


class PaymentProcessor:
    """Charges a pending payment and notifies the client.

    Safe to run any number of times for the same payment (messages are
    delivered at-least-once):
      * the row is locked with SELECT ... FOR UPDATE, so concurrent duplicates
        are serialized and the gateway is charged at most once;
      * a payment that is already final skips the gateway, so a retry caused
        by a failed webhook only re-sends the webhook.
    """

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        gateway: PaymentGateway,
        notifier: WebhookNotifier,
    ) -> None:
        self._session_factory = session_factory
        self._gateway = gateway
        self._notifier = notifier

    async def process(self, payment_id: UUID) -> None:
        async with self._session_factory() as session, session.begin():
            payment = await PaymentRepository(session).get(payment_id, for_update=True)
            if payment is None:
                raise PaymentNotFoundError(payment_id)

            if payment.status is PaymentStatus.PENDING:
                result = await self._gateway.charge(payment.id, payment.amount, payment.currency)
                payment.status = PaymentStatus.SUCCEEDED if result.success else PaymentStatus.FAILED
                payment.processed_at = utcnow()
                logger.info("Payment %s -> %s", payment.id, payment.status)
            else:
                logger.info("Payment %s already %s, re-sending webhook", payment.id, payment.status)

            webhook_url = payment.webhook_url
            payload = WebhookPayload.from_model(payment)

        # Outside the transaction: the status is committed regardless of webhook outcome.
        await self._notifier.send(webhook_url, payload)
