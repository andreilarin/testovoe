from collections.abc import Sequence
from typing import Any

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from payments.db.models import OutboxEvent


class OutboxRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    def add(self, *, event_type: str, routing_key: str, payload: dict[str, Any]) -> OutboxEvent:
        event = OutboxEvent(event_type=event_type, routing_key=routing_key, payload=payload)
        self._session.add(event)
        return event

    async def lock_unpublished(self, limit: int) -> Sequence[OutboxEvent]:
        """Lock a batch of unpublished events.

        SKIP LOCKED lets several relay replicas work in parallel
        without picking up the same rows.
        """
        stmt = (
            select(OutboxEvent)
            .where(OutboxEvent.published_at.is_(None))
            .order_by(OutboxEvent.id)
            .limit(limit)
            .with_for_update(skip_locked=True)
        )
        return (await self._session.scalars(stmt)).all()
