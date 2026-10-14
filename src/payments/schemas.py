from datetime import datetime
from decimal import Decimal
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, HttpUrl

from payments.db.models import Payment
from payments.domain.enums import Currency, PaymentStatus


class PaymentCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    amount: Decimal = Field(gt=0, max_digits=18, decimal_places=2, examples=["1500.00"])
    currency: Currency
    description: str | None = Field(default=None, max_length=500)
    metadata: dict[str, Any] = Field(default_factory=dict)
    webhook_url: HttpUrl


class PaymentAccepted(BaseModel):
    payment_id: UUID
    status: PaymentStatus
    created_at: datetime

    @classmethod
    def from_model(cls, payment: Payment) -> "PaymentAccepted":
        return cls(payment_id=payment.id, status=payment.status, created_at=payment.created_at)


class PaymentRead(BaseModel):
    payment_id: UUID
    amount: Decimal
    currency: Currency
    description: str | None
    metadata: dict[str, Any]
    status: PaymentStatus
    idempotency_key: str
    webhook_url: str
    created_at: datetime
    processed_at: datetime | None

    @classmethod
    def from_model(cls, payment: Payment) -> "PaymentRead":
        return cls(
            payment_id=payment.id,
            amount=payment.amount,
            currency=payment.currency,
            description=payment.description,
            metadata=payment.meta,
            status=payment.status,
            idempotency_key=payment.idempotency_key,
            webhook_url=payment.webhook_url,
            created_at=payment.created_at,
            processed_at=payment.processed_at,
        )


class WebhookPayload(BaseModel):
    """Body POSTed to the client's webhook_url once a payment is processed."""

    event: str
    payment_id: UUID
    status: PaymentStatus
    amount: Decimal
    currency: Currency
    metadata: dict[str, Any]
    processed_at: datetime | None

    @classmethod
    def from_model(cls, payment: Payment) -> "WebhookPayload":
        return cls(
            event=f"payment.{payment.status}",
            payment_id=payment.id,
            status=payment.status,
            amount=payment.amount,
            currency=payment.currency,
            metadata=payment.meta,
            processed_at=payment.processed_at,
        )
