from faststream.rabbit import RabbitBroker

from payments.db.models import OutboxEvent
from payments.messaging.topology import PAYMENTS_EXCHANGE


class RabbitEventPublisher:
    """Publishes outbox events; returns only after the broker confirmed the message.

    Requires a channel with `on_return_raises=True`, so an unroutable
    message raises instead of silently disappearing.
    """

    def __init__(self, broker: RabbitBroker) -> None:
        self._broker = broker

    async def publish(self, event: OutboxEvent) -> None:
        await self._broker.publish(
            event.payload,
            exchange=PAYMENTS_EXCHANGE,
            routing_key=event.routing_key,
            persist=True,
            mandatory=True,
            message_id=f"outbox-{event.id}",
            message_type=event.event_type,
            headers={"x-event-type": event.event_type, "x-outbox-id": event.id},
        )
