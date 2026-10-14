import asyncio
import logging
import random
from dataclasses import dataclass
from decimal import Decimal
from typing import Protocol
from uuid import UUID

from payments.domain.enums import Currency

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class ChargeResult:
    success: bool
    reason: str | None = None


class PaymentGateway(Protocol):
    async def charge(
        self, payment_id: UUID, amount: Decimal, currency: Currency
    ) -> ChargeResult: ...


class EmulatedPaymentGateway:
    """Fake acquirer: waits 2-5 s and approves ~90% of charges.

    A decline is a normal business outcome (payment becomes `failed`),
    not an error, so it is never retried.
    """

    def __init__(
        self,
        *,
        min_delay: float,
        max_delay: float,
        success_rate: float,
        rng: random.Random | None = None,
    ) -> None:
        self._min_delay = min_delay
        self._max_delay = max_delay
        self._success_rate = success_rate
        self._rng = rng or random.Random()

    async def charge(self, payment_id: UUID, amount: Decimal, currency: Currency) -> ChargeResult:
        await asyncio.sleep(self._rng.uniform(self._min_delay, self._max_delay))
        if self._rng.random() < self._success_rate:
            logger.info("Gateway approved payment %s (%s %s)", payment_id, amount, currency)
            return ChargeResult(success=True)
        logger.info("Gateway declined payment %s (%s %s)", payment_id, amount, currency)
        return ChargeResult(success=False, reason="declined_by_issuer")
