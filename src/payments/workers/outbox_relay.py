"""Outbox relay process: `python -m payments.workers.outbox_relay`."""

import asyncio
import contextlib
import signal

from faststream.rabbit import RabbitBroker
from faststream.rabbit.schemas import Channel

from payments.core.config import get_settings
from payments.core.logging import configure_logging
from payments.db.session import create_engine, create_session_factory
from payments.messaging.publisher import RabbitEventPublisher
from payments.messaging.topology import declare_topology
from payments.services.outbox_relay import OutboxRelay


async def main() -> None:
    settings = get_settings()
    configure_logging(settings.log_level)

    engine = create_engine(settings.database_url)
    broker = RabbitBroker(
        settings.rabbitmq_url,
        default_channel=Channel(publisher_confirms=True, on_return_raises=True),
    )
    relay = OutboxRelay(
        create_session_factory(engine),
        RabbitEventPublisher(broker),
        batch_size=settings.outbox_batch_size,
        poll_interval=settings.outbox_poll_interval,
    )

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        with contextlib.suppress(NotImplementedError):  # not supported on Windows
            loop.add_signal_handler(sig, stop.set)

    try:
        await broker.start()
        await declare_topology(broker, settings.retry_delays_ms)
        await relay.run(stop)
    finally:
        await broker.stop()
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())
