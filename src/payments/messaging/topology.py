"""RabbitMQ topology.

    payments (direct) --payments.new--> [payments.new] --> consumer
                                           |  reject / nack(requeue=False)
                                           v
    payments.dlx (direct) --payments.dlq--> [payments.dlq]

    payments.retry (direct) --payments.retry.<ms>--> [payments.retry.<ms>]
        x-message-ttl=<ms>; on expiry dead-letters back to payments/payments.new

One retry queue per backoff step: with a single queue and per-message TTL,
a long-delay message at the head would hold back short-delay ones behind it.
The delay is part of the queue name, so changing the backoff config creates
new queues instead of failing with PRECONDITION_FAILED on redeclaration.
"""

from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange, RabbitQueue

PAYMENTS_EXCHANGE = RabbitExchange("payments", type=ExchangeType.DIRECT, durable=True)
RETRY_EXCHANGE = RabbitExchange("payments.retry", type=ExchangeType.DIRECT, durable=True)
DEAD_LETTER_EXCHANGE = RabbitExchange("payments.dlx", type=ExchangeType.DIRECT, durable=True)

NEW_PAYMENTS_ROUTING_KEY = "payments.new"
DEAD_LETTER_ROUTING_KEY = "payments.dlq"

NEW_PAYMENTS_QUEUE = RabbitQueue(
    "payments.new",
    durable=True,
    routing_key=NEW_PAYMENTS_ROUTING_KEY,
    arguments={
        "x-dead-letter-exchange": DEAD_LETTER_EXCHANGE.name,
        "x-dead-letter-routing-key": DEAD_LETTER_ROUTING_KEY,
    },
)

DEAD_LETTER_QUEUE = RabbitQueue(
    "payments.dlq",
    durable=True,
    routing_key=DEAD_LETTER_ROUTING_KEY,
)


def retry_routing_key(delay_ms: int) -> str:
    return f"payments.retry.{delay_ms}ms"


def retry_queue(delay_ms: int) -> RabbitQueue:
    return RabbitQueue(
        retry_routing_key(delay_ms),
        durable=True,
        routing_key=retry_routing_key(delay_ms),
        arguments={
            "x-message-ttl": delay_ms,
            "x-dead-letter-exchange": PAYMENTS_EXCHANGE.name,
            "x-dead-letter-routing-key": NEW_PAYMENTS_ROUTING_KEY,
        },
    )


async def declare_topology(broker: RabbitBroker, retry_delays_ms: list[int]) -> None:
    """Declare all exchanges, queues and bindings. Idempotent."""
    payments_exchange = await broker.declare_exchange(PAYMENTS_EXCHANGE)
    retry_exchange = await broker.declare_exchange(RETRY_EXCHANGE)
    dead_letter_exchange = await broker.declare_exchange(DEAD_LETTER_EXCHANGE)

    new_queue = await broker.declare_queue(NEW_PAYMENTS_QUEUE)
    await new_queue.bind(payments_exchange, routing_key=NEW_PAYMENTS_ROUTING_KEY)

    dlq = await broker.declare_queue(DEAD_LETTER_QUEUE)
    await dlq.bind(dead_letter_exchange, routing_key=DEAD_LETTER_ROUTING_KEY)

    for delay_ms in retry_delays_ms:
        queue = await broker.declare_queue(retry_queue(delay_ms))
        await queue.bind(retry_exchange, routing_key=retry_routing_key(delay_ms))
