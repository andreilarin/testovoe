from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payments.db.models import OutboxEvent
from payments.repositories.outbox import OutboxRepository
from payments.services.outbox_relay import OutboxRelay


class FakePublisher:
    def __init__(self, fail: bool = False) -> None:
        self.fail = fail
        self.published: list[int] = []

    async def publish(self, event: OutboxEvent) -> None:
        if self.fail:
            raise ConnectionError("broker down")
        self.published.append(event.id)


async def _add_events(session_factory: async_sessionmaker[AsyncSession], count: int) -> None:
    async with session_factory() as session, session.begin():
        repo = OutboxRepository(session)
        for i in range(count):
            repo.add(event_type="payment.created", routing_key="payments.new", payload={"n": i})


async def _events(session_factory: async_sessionmaker[AsyncSession]) -> list[OutboxEvent]:
    async with session_factory() as session:
        return list(await session.scalars(select(OutboxEvent).order_by(OutboxEvent.id)))


def _relay(session_factory, publisher, batch_size: int = 10) -> OutboxRelay:
    return OutboxRelay(session_factory, publisher, batch_size=batch_size, poll_interval=0.01)


async def test_publishes_pending_events_in_order_and_marks_them(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    await _add_events(session_factory, 3)
    publisher = FakePublisher()
    relay = _relay(session_factory, publisher)

    assert await relay.publish_batch() == 3
    assert await relay.publish_batch() == 0  # nothing is published twice

    events = await _events(session_factory)
    assert publisher.published == [e.id for e in events]
    assert all(e.published_at is not None for e in events)


async def test_respects_batch_size(session_factory: async_sessionmaker[AsyncSession]) -> None:
    await _add_events(session_factory, 5)
    relay = _relay(session_factory, FakePublisher(), batch_size=2)

    assert [await relay.publish_batch() for _ in range(4)] == [2, 2, 1, 0]


async def test_failed_publish_keeps_event_for_next_run(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    await _add_events(session_factory, 2)
    publisher = FakePublisher(fail=True)
    relay = _relay(session_factory, publisher)

    assert await relay.publish_batch() == 0

    first, second = await _events(session_factory)
    assert first.published_at is None and first.attempts == 1
    assert "broker down" in first.last_error
    assert second.attempts == 0  # relay stops at the first failure

    publisher.fail = False
    assert await relay.publish_batch() == 2
