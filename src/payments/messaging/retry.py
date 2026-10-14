import logging
from typing import Any, Protocol

from faststream.rabbit import RabbitExchange
from faststream.rabbit.message import RabbitMessage

from payments.messaging.topology import (
    DEAD_LETTER_EXCHANGE,
    DEAD_LETTER_ROUTING_KEY,
    RETRY_EXCHANGE,
    retry_routing_key,
)

logger = logging.getLogger(__name__)

RETRY_COUNT_HEADER = "x-retry-count"
ERROR_HEADER = "x-last-error"
_MAX_ERROR_LENGTH = 500


class Publisher(Protocol):
    async def publish(
        self,
        message: Any = None,
        *,
        exchange: RabbitExchange | None = None,
        routing_key: str = "",
        persist: bool = False,
        headers: dict[str, Any] | None = None,
        content_type: str | None = None,
        message_id: str | None = None,
        correlation_id: str | None = None,
    ) -> Any: ...


class RetryDispatcher:
    """Moves a failed message to the next backoff queue, or to the DLQ.

    The retry copy is published before the original is acked (FastStream acks
    after the handler returns), so a crash in between can only produce
    a duplicate, never a loss. The consumer is idempotent, so duplicates are safe.
    """

    def __init__(self, publisher: Publisher, retry_delays_ms: list[int]) -> None:
        self._publisher = publisher
        self._delays_ms = retry_delays_ms

    @property
    def max_retries(self) -> int:
        return len(self._delays_ms)

    async def retry_or_dead_letter(self, message: RabbitMessage, error: Exception) -> None:
        retries_done = int(message.headers.get(RETRY_COUNT_HEADER, 0))
        if retries_done >= self.max_retries:
            await self.dead_letter(message, error)
            return

        delay_ms = self._delays_ms[retries_done]
        logger.warning(
            "Message %s failed (retry %d/%d in %d ms): %r",
            message.message_id,
            retries_done + 1,
            self.max_retries,
            delay_ms,
            error,
        )
        await self._republish(
            message,
            exchange=RETRY_EXCHANGE,
            routing_key=retry_routing_key(delay_ms),
            retries_done=retries_done + 1,
            error=error,
        )

    async def dead_letter(self, message: RabbitMessage, error: Exception) -> None:
        retries_done = int(message.headers.get(RETRY_COUNT_HEADER, 0))
        logger.error(
            "Message %s moved to DLQ after %d retries: %r",
            message.message_id,
            retries_done,
            error,
        )
        await self._republish(
            message,
            exchange=DEAD_LETTER_EXCHANGE,
            routing_key=DEAD_LETTER_ROUTING_KEY,
            retries_done=retries_done,
            error=error,
        )

    async def _republish(
        self,
        message: RabbitMessage,
        *,
        exchange: RabbitExchange,
        routing_key: str,
        retries_done: int,
        error: Exception,
    ) -> None:
        # x-death is maintained by RabbitMQ itself; carrying a stale copy only confuses.
        headers = {k: v for k, v in message.headers.items() if k != "x-death"}
        headers[RETRY_COUNT_HEADER] = retries_done
        headers[ERROR_HEADER] = repr(error)[:_MAX_ERROR_LENGTH]

        await self._publisher.publish(
            message.body,
            exchange=exchange,
            routing_key=routing_key,
            persist=True,
            headers=headers,
            content_type=message.content_type,
            message_id=message.message_id,
            correlation_id=message.correlation_id,
        )
