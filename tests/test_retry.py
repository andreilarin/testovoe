from types import SimpleNamespace
from typing import Any

import pytest

from payments.messaging.retry import ERROR_HEADER, RETRY_COUNT_HEADER, RetryDispatcher
from payments.messaging.topology import DEAD_LETTER_EXCHANGE, RETRY_EXCHANGE

DELAYS_MS = [2000, 4000, 8000]


class FakePublisher:
    def __init__(self) -> None:
        self.calls: list[dict[str, Any]] = []

    async def publish(self, message: Any = None, **kwargs: Any) -> None:
        self.calls.append({"body": message, **kwargs})


def _message(retries_done: int | None = None) -> Any:
    headers: dict[str, Any] = {"x-death": [{"count": 1}], "x-event-type": "payment.created"}
    if retries_done is not None:
        headers[RETRY_COUNT_HEADER] = retries_done
    return SimpleNamespace(
        body=b'{"payment_id": "x"}',
        headers=headers,
        content_type="application/json",
        message_id="outbox-1",
        correlation_id="corr-1",
    )


@pytest.mark.parametrize(("retries_done", "expected_delay"), [(None, 2000), (1, 4000), (2, 8000)])
async def test_schedules_retry_with_exponential_backoff(
    retries_done: int | None, expected_delay: int
) -> None:
    publisher = FakePublisher()

    await RetryDispatcher(publisher, DELAYS_MS).retry_or_dead_letter(
        _message(retries_done), RuntimeError("webhook down")
    )

    [call] = publisher.calls
    assert call["exchange"] is RETRY_EXCHANGE
    assert call["routing_key"] == f"payments.retry.{expected_delay}ms"
    assert call["headers"][RETRY_COUNT_HEADER] == (retries_done or 0) + 1
    assert "webhook down" in call["headers"][ERROR_HEADER]
    assert call["body"] == b'{"payment_id": "x"}'
    assert call["message_id"] == "outbox-1"
    assert call["persist"] is True


async def test_dead_letters_after_max_retries() -> None:
    publisher = FakePublisher()

    await RetryDispatcher(publisher, DELAYS_MS).retry_or_dead_letter(
        _message(retries_done=3), RuntimeError("still down")
    )

    [call] = publisher.calls
    assert call["exchange"] is DEAD_LETTER_EXCHANGE
    assert call["routing_key"] == "payments.dlq"
    assert call["headers"][RETRY_COUNT_HEADER] == 3
    assert "still down" in call["headers"][ERROR_HEADER]


async def test_keeps_custom_headers_and_drops_x_death() -> None:
    publisher = FakePublisher()

    await RetryDispatcher(publisher, DELAYS_MS).retry_or_dead_letter(_message(), RuntimeError())

    headers = publisher.calls[0]["headers"]
    assert "x-death" not in headers
    assert headers["x-event-type"] == "payment.created"
