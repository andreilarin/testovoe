from uuid import UUID

from pydantic import BaseModel

PAYMENT_CREATED = "payment.created"


class PaymentCreatedEvent(BaseModel):
    """Message published to `payments.new`.

    Carries only the id: the consumer reads the current state from the DB,
    so a redelivered or duplicated message can never apply stale data.
    """

    payment_id: UUID
