"""Payment consumer: `faststream run payments.workers.consumer:app`."""

import logging
from typing import Annotated

import httpx
from faststream import AckPolicy, Context, ContextRepo, FastStream
from faststream.rabbit import RabbitBroker
from faststream.rabbit.annotations import RabbitMessage
from faststream.rabbit.schemas import Channel

from payments.core.config import get_settings
from payments.core.logging import configure_logging
from payments.db.session import create_engine, create_session_factory
from payments.domain.exceptions import PaymentNotFoundError
from payments.infra.gateway import EmulatedPaymentGateway
from payments.infra.webhooks import HttpWebhookNotifier
from payments.messaging.events import PaymentCreatedEvent
from payments.messaging.retry import RetryDispatcher
from payments.messaging.topology import NEW_PAYMENTS_QUEUE, PAYMENTS_EXCHANGE, declare_topology
from payments.services.processing import PaymentProcessor

logger = logging.getLogger(__name__)

settings = get_settings()
configure_logging(settings.log_level)

broker = RabbitBroker(
    settings.rabbitmq_url,
    default_channel=Channel(
        prefetch_count=settings.consumer_prefetch_count,
        publisher_confirms=True,
        on_return_raises=True,
    ),
)
app = FastStream(broker)


@app.on_startup
async def setup(context: ContextRepo) -> None:
    engine = create_engine(settings.database_url)
    http_client = httpx.AsyncClient(timeout=settings.webhook_timeout)
    gateway = EmulatedPaymentGateway(
        min_delay=settings.gateway_min_delay,
        max_delay=settings.gateway_max_delay,
        success_rate=settings.gateway_success_rate,
    )
    context.set_global("engine", engine)
    context.set_global("http_client", http_client)
    context.set_global(
        "processor",
        PaymentProcessor(create_session_factory(engine), gateway, HttpWebhookNotifier(http_client)),
    )
    context.set_global("retry_dispatcher", RetryDispatcher(broker, settings.retry_delays_ms))

    # Declare retry queues and DLQ before the subscriber starts consuming.
    await broker.connect()
    await declare_topology(broker, settings.retry_delays_ms)


@app.after_shutdown
async def teardown(context: ContextRepo) -> None:
    await context.get("http_client").aclose()
    await context.get("engine").dispose()


@broker.subscriber(
    NEW_PAYMENTS_QUEUE,
    PAYMENTS_EXCHANGE,
    # Anything that escapes the handler (undecodable body, broker failure while
    # scheduling a retry) is rejected and dead-lettered into payments.dlq.
    ack_policy=AckPolicy.REJECT_ON_ERROR,
)
async def handle_payment_created(
    event: PaymentCreatedEvent,
    message: RabbitMessage,
    processor: Annotated[PaymentProcessor, Context("processor")],
    retry_dispatcher: Annotated[RetryDispatcher, Context("retry_dispatcher")],
) -> None:
    try:
        await processor.process(event.payment_id)
    except PaymentNotFoundError as exc:
        # Retrying cannot help: the event was produced by a committed transaction.
        await retry_dispatcher.dead_letter(message, exc)
    except Exception as exc:
        await retry_dispatcher.retry_or_dead_letter(message, exc)
