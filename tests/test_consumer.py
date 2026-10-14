"""Handler wiring, run against FastStream's in-memory TestRabbitBroker.

TestRabbitBroker does not emulate TTL/DLX, so the RetryDispatcher is a fake:
the routing itself is covered in test_retry.py.
"""

from collections.abc import AsyncIterator
from typing import Any
from uuid import UUID, uuid4

import pytest
from faststream.rabbit import TestRabbitBroker

from payments.domain.exceptions import PaymentNotFoundError
from payments.messaging.topology import NEW_PAYMENTS_ROUTING_KEY, PAYMENTS_EXCHANGE
from payments.workers.consumer import broker

PAYMENT_ID = uuid4()


class FakeProcessor:
    def __init__(self, error: Exception | None = None) -> None:
        self.error = error
        self.processed: list[UUID] = []

    async def process(self, payment_id: UUID) -> None:
        self.processed.append(payment_id)
        if self.error:
            raise self.error


class FakeRetryDispatcher:
    def __init__(self) -> None:
        self.retried: list[Exception] = []
        self.dead_lettered: list[Exception] = []

    async def retry_or_dead_letter(self, message: Any, error: Exception) -> None:
        self.retried.append(error)

    async def dead_letter(self, message: Any, error: Exception) -> None:
        self.dead_lettered.append(error)


@pytest.fixture
async def test_broker() -> AsyncIterator[TestRabbitBroker]:
    async with TestRabbitBroker(broker) as br:
        yield br


async def _run(test_broker, processor: FakeProcessor) -> FakeRetryDispatcher:
    dispatcher = FakeRetryDispatcher()
    broker.context.set_global("processor", processor)
    broker.context.set_global("retry_dispatcher", dispatcher)
    await test_broker.publish(
        {"payment_id": str(PAYMENT_ID)},
        exchange=PAYMENTS_EXCHANGE,
        routing_key=NEW_PAYMENTS_ROUTING_KEY,
    )
    return dispatcher


async def test_processes_payment_created_event(test_broker: TestRabbitBroker) -> None:
    processor = FakeProcessor()

    dispatcher = await _run(test_broker, processor)

    assert processor.processed == [PAYMENT_ID]
    assert dispatcher.retried == dispatcher.dead_lettered == []


async def test_transient_error_is_retried(test_broker: TestRabbitBroker) -> None:
    error = RuntimeError("webhook down")

    dispatcher = await _run(test_broker, FakeProcessor(error))

    assert dispatcher.retried == [error]


async def test_missing_payment_goes_straight_to_dlq(test_broker: TestRabbitBroker) -> None:
    error = PaymentNotFoundError(PAYMENT_ID)

    dispatcher = await _run(test_broker, FakeProcessor(error))

    assert dispatcher.dead_lettered == [error]
    assert dispatcher.retried == []
