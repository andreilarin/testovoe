import asyncio
import logging
from typing import Protocol

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payments.db.models import OutboxEvent, utcnow
from payments.repositories.outbox import OutboxRepository

logger = logging.getLogger(__name__)

_MAX_ERROR_LENGTH = 1000


class EventPublisher(Protocol):
    async def publish(self, event: OutboxEvent) -> None: ...


class OutboxRelay:
    """Publishes committed outbox events to the broker (at-least-once).

    An event is marked published only after the broker confirmed it.
    If the process dies between the confirm and the commit, the event is
    published again on the next run; consumers are idempotent.
    """

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        publisher: EventPublisher,
        *,
        batch_size: int,
        poll_interval: float,
    ) -> None:
        self._session_factory = session_factory
        self._publisher = publisher
        self._batch_size = batch_size
        self._poll_interval = poll_interval

    async def publish_batch(self) -> int:
        """Publish one batch. Returns the number of events published."""
        published = 0
        async with self._session_factory() as session, session.begin():
            for event in await OutboxRepository(session).lock_unpublished(self._batch_size):
                event.attempts += 1
                try:
                    await self._publisher.publish(event)
                except Exception as exc:
                    event.last_error = repr(exc)[:_MAX_ERROR_LENGTH]
                    logger.exception("Failed to publish outbox event %s", event.id)
                    # The broker is most likely unavailable: stop and keep order.
                    break
                event.published_at = utcnow()
                event.last_error = None
                published += 1
        if published:
            logger.info("Published %d outbox event(s)", published)
        return published

    async def run(self, stop: asyncio.Event) -> None:
        logger.info("Outbox relay started")
        while not stop.is_set():
            try:
                published = await self.publish_batch()
            except Exception:
                logger.exception("Outbox relay iteration failed")
                published = 0
            # A full batch means there is probably more work: don't sleep.
            if published < self._batch_size:
                try:
                    await asyncio.wait_for(stop.wait(), timeout=self._poll_interval)
                except TimeoutError:
                    pass
        logger.info("Outbox relay stopped")
