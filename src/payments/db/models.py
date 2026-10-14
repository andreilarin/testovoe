import uuid
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

from sqlalchemy import BigInteger, DateTime, Enum, Index, Integer, Numeric, String, Text, func, text
from sqlalchemy.orm import Mapped, mapped_column

from payments.db.base import Base
from payments.domain.enums import Currency, PaymentStatus


def utcnow() -> datetime:
    return datetime.now(UTC)


def _str_enum(enum_cls: type, name: str) -> Enum:
    return Enum(
        enum_cls,
        name=name,
        native_enum=False,
        create_constraint=True,
        length=16,
        values_callable=lambda members: [member.value for member in members],
    )


class Payment(Base):
    __tablename__ = "payments"

    id: Mapped[uuid.UUID] = mapped_column(primary_key=True, default=uuid.uuid4)
    amount: Mapped[Decimal] = mapped_column(Numeric(18, 2))
    currency: Mapped[Currency] = mapped_column(_str_enum(Currency, "payment_currency"))
    description: Mapped[str | None] = mapped_column(String(500))
    # `metadata` is reserved by SQLAlchemy declarative, so the attribute is `meta`.
    meta: Mapped[dict[str, Any]] = mapped_column("metadata", default=dict)
    status: Mapped[PaymentStatus] = mapped_column(
        _str_enum(PaymentStatus, "payment_status"),
        default=PaymentStatus.PENDING,
    )
    idempotency_key: Mapped[str] = mapped_column(String(255), unique=True)
    webhook_url: Mapped[str] = mapped_column(String(2048))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, server_default=func.now()
    )
    processed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))


class OutboxEvent(Base):
    __tablename__ = "outbox"
    __table_args__ = (
        # Relay only ever scans unpublished events.
        Index(
            "ix_outbox_unpublished",
            "id",
            postgresql_where=text("published_at IS NULL"),
        ),
    )

    # SQLite autoincrements only INTEGER primary keys.
    id: Mapped[int] = mapped_column(
        BigInteger().with_variant(Integer, "sqlite"), primary_key=True, autoincrement=True
    )
    event_type: Mapped[str] = mapped_column(String(100))
    routing_key: Mapped[str] = mapped_column(String(255))
    payload: Mapped[dict[str, Any]]
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), default=utcnow, server_default=func.now()
    )
    published_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True))
    attempts: Mapped[int] = mapped_column(default=0, server_default="0")
    last_error: Mapped[str | None] = mapped_column(Text)
