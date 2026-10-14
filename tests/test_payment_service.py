from decimal import Decimal

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payments.db.models import OutboxEvent, Payment
from payments.domain.enums import Currency
from payments.schemas import PaymentCreate
from payments.services.payments import PaymentService

DATA = PaymentCreate(
    amount=Decimal("10.00"),
    currency=Currency.USD,
    webhook_url="http://example.com/hook",
)


async def test_concurrent_duplicate_resolves_to_existing_payment(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    async with session_factory() as session:
        winner = await PaymentService(session).create(DATA, "race-key")

    # Simulate losing the race: the pre-check does not see the winner's row yet,
    # so the insert hits the UNIQUE constraint.
    async with session_factory() as session:
        service = PaymentService(session)
        real_lookup = service._payments.get_by_idempotency_key
        calls = 0

        async def stale_then_real(key: str) -> Payment | None:
            nonlocal calls
            calls += 1
            return None if calls == 1 else await real_lookup(key)

        service._payments.get_by_idempotency_key = stale_then_real  # type: ignore[method-assign]
        loser = await service.create(DATA, "race-key")

    assert loser.id == winner.id
    async with session_factory() as session:
        assert await session.scalar(select(func.count()).select_from(Payment)) == 1
        assert await session.scalar(select(func.count()).select_from(OutboxEvent)) == 1
