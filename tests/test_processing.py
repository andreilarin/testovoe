from decimal import Decimal
from uuid import UUID, uuid4

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payments.db.models import Payment
from payments.domain.enums import Currency, PaymentStatus
from payments.domain.exceptions import PaymentNotFoundError
from payments.infra.gateway import ChargeResult
from payments.infra.webhooks import WebhookDeliveryError
from payments.schemas import WebhookPayload
from payments.services.processing import PaymentProcessor


class FakeGateway:
    def __init__(self, success: bool = True) -> None:
        self.success = success
        self.calls: list[UUID] = []

    async def charge(self, payment_id: UUID, amount: Decimal, currency: Currency) -> ChargeResult:
        self.calls.append(payment_id)
        return ChargeResult(success=self.success)


class FakeNotifier:
    def __init__(self, fail: bool = False) -> None:
        self.fail = fail
        self.sent: list[tuple[str, WebhookPayload]] = []

    async def send(self, url: str, payload: WebhookPayload) -> None:
        if self.fail:
            raise WebhookDeliveryError("boom")
        self.sent.append((url, payload))


@pytest.fixture
async def payment(session_factory: async_sessionmaker[AsyncSession]) -> Payment:
    payment = Payment(
        id=uuid4(),
        amount=Decimal("99.90"),
        currency=Currency.EUR,
        meta={"order": 1},
        idempotency_key="key",
        webhook_url="http://example.com/hook",
    )
    async with session_factory() as session, session.begin():
        session.add(payment)
    return payment


async def _reload(session_factory: async_sessionmaker[AsyncSession], payment_id: UUID) -> Payment:
    async with session_factory() as session:
        return await session.get_one(Payment, payment_id)


async def test_successful_charge_marks_succeeded_and_notifies(
    session_factory: async_sessionmaker[AsyncSession], payment: Payment
) -> None:
    gateway, notifier = FakeGateway(success=True), FakeNotifier()

    await PaymentProcessor(session_factory, gateway, notifier).process(payment.id)

    stored = await _reload(session_factory, payment.id)
    assert stored.status is PaymentStatus.SUCCEEDED
    assert stored.processed_at is not None
    [(url, payload)] = notifier.sent
    assert url == "http://example.com/hook"
    assert payload.event == "payment.succeeded"
    assert payload.payment_id == payment.id
    assert payload.metadata == {"order": 1}


async def test_declined_charge_marks_failed_and_notifies(
    session_factory: async_sessionmaker[AsyncSession], payment: Payment
) -> None:
    notifier = FakeNotifier()

    await PaymentProcessor(session_factory, FakeGateway(success=False), notifier).process(
        payment.id
    )

    assert (await _reload(session_factory, payment.id)).status is PaymentStatus.FAILED
    assert notifier.sent[0][1].event == "payment.failed"


async def test_redelivery_does_not_charge_twice(
    session_factory: async_sessionmaker[AsyncSession], payment: Payment
) -> None:
    gateway, notifier = FakeGateway(), FakeNotifier()
    processor = PaymentProcessor(session_factory, gateway, notifier)

    await processor.process(payment.id)
    await processor.process(payment.id)

    assert gateway.calls == [payment.id]
    assert len(notifier.sent) == 2  # webhook is re-sent, charge is not


async def test_webhook_failure_raises_but_keeps_status(
    session_factory: async_sessionmaker[AsyncSession], payment: Payment
) -> None:
    processor = PaymentProcessor(session_factory, FakeGateway(), FakeNotifier(fail=True))

    with pytest.raises(WebhookDeliveryError):
        await processor.process(payment.id)

    assert (await _reload(session_factory, payment.id)).status is PaymentStatus.SUCCEEDED


async def test_unknown_payment_raises_not_found(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    processor = PaymentProcessor(session_factory, FakeGateway(), FakeNotifier())

    with pytest.raises(PaymentNotFoundError):
        await processor.process(uuid4())
