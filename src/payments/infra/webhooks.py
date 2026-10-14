import logging
from typing import Protocol

import httpx

from payments.schemas import WebhookPayload

logger = logging.getLogger(__name__)


class WebhookDeliveryError(Exception):
    pass


class WebhookNotifier(Protocol):
    async def send(self, url: str, payload: WebhookPayload) -> None: ...


class HttpWebhookNotifier:
    def __init__(self, client: httpx.AsyncClient) -> None:
        self._client = client

    async def send(self, url: str, payload: WebhookPayload) -> None:
        try:
            response = await self._client.post(
                url,
                content=payload.model_dump_json(),
                headers={
                    "Content-Type": "application/json",
                    "X-Event-Type": payload.event,
                    # Lets the receiver deduplicate: delivery is at-least-once.
                    "X-Payment-Id": str(payload.payment_id),
                },
            )
        except httpx.HTTPError as exc:
            raise WebhookDeliveryError(f"Webhook {url} unreachable: {exc!r}") from exc

        if not response.is_success:
            raise WebhookDeliveryError(f"Webhook {url} responded with HTTP {response.status_code}")
        logger.info("Webhook delivered for payment %s to %s", payload.payment_id, url)
