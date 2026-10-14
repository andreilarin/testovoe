"""Demo webhook receiver (not part of the service).

POST /webhook                    accepts; fails randomly with WEBHOOK_RECEIVER_FAIL_RATE
POST /webhook/flaky?fail_times=N fails the first N deliveries of each payment, then accepts
POST /webhook/fail               always returns 500, to watch retries end up in the DLQ
GET  /webhook                    last received notifications
GET  /webhook/attempts/{id}      how many deliveries were attempted for a payment
"""

import logging
import os
import random
from collections import Counter, deque
from typing import Any

from fastapi import FastAPI, HTTPException, Request

logger = logging.getLogger("webhook_receiver")
logging.basicConfig(level="INFO", format="%(asctime)s %(levelname)s [%(name)s] %(message)s")

FAIL_RATE = float(os.getenv("WEBHOOK_RECEIVER_FAIL_RATE", "0"))

app = FastAPI(title="Webhook receiver (demo)")
received: deque[dict[str, Any]] = deque(maxlen=100)
attempts: Counter[str] = Counter()


async def _register_attempt(request: Request) -> tuple[dict[str, Any], int]:
    payload = await request.json()
    payment_id = str(payload.get("payment_id"))
    attempts[payment_id] += 1
    return payload, attempts[payment_id]


def _accept(payload: dict[str, Any], attempt: int) -> dict[str, str]:
    received.append(payload)
    logger.info(
        "Received %s for payment %s (attempt %d)",
        payload.get("event"),
        payload.get("payment_id"),
        attempt,
    )
    return {"status": "ok"}


@app.post("/webhook")
async def receive(request: Request) -> dict[str, str]:
    payload, attempt = await _register_attempt(request)
    if random.random() < FAIL_RATE:
        logger.warning("Simulated failure for payment %s", payload.get("payment_id"))
        raise HTTPException(status_code=503, detail="simulated failure")
    return _accept(payload, attempt)


@app.post("/webhook/flaky")
async def flaky(request: Request, fail_times: int = 2) -> dict[str, str]:
    payload, attempt = await _register_attempt(request)
    if attempt <= fail_times:
        logger.warning(
            "Flaky failure %d/%d for payment %s", attempt, fail_times, payload.get("payment_id")
        )
        raise HTTPException(status_code=503, detail="flaky failure")
    return _accept(payload, attempt)


@app.post("/webhook/fail")
async def always_fail(request: Request) -> None:
    payload, attempt = await _register_attempt(request)
    logger.warning(
        "Rejecting webhook for payment %s (attempt %d)", payload.get("payment_id"), attempt
    )
    raise HTTPException(status_code=500, detail="always fails")


@app.get("/webhook")
async def list_received() -> list[dict[str, Any]]:
    return list(received)


@app.get("/webhook/attempts/{payment_id}")
async def get_attempts(payment_id: str) -> dict[str, int]:
    return {"attempts": attempts[payment_id]}
