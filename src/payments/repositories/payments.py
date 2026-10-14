from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from payments.db.models import Payment


class PaymentRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    def add(self, payment: Payment) -> None:
        self._session.add(payment)

    async def get(self, payment_id: UUID, *, for_update: bool = False) -> Payment | None:
        stmt = select(Payment).where(Payment.id == payment_id)
        if for_update:
            stmt = stmt.with_for_update()
        return await self._session.scalar(stmt)

    async def get_by_idempotency_key(self, idempotency_key: str) -> Payment | None:
        stmt = select(Payment).where(Payment.idempotency_key == idempotency_key)
        return await self._session.scalar(stmt)
