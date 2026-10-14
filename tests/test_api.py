import uuid

from httpx import AsyncClient
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from payments.db.models import OutboxEvent, Payment

PAYMENT_BODY = {
    "amount": "1500.50",
    "currency": "RUB",
    "description": "Order #42",
    "metadata": {"order_id": 42},
    "webhook_url": "http://example.com/webhook",
}


async def _count(session_factory: async_sessionmaker[AsyncSession], model: type) -> int:
    async with session_factory() as session:
        return await session.scalar(select(func.count()).select_from(model))


async def test_rejects_missing_api_key(client: AsyncClient) -> None:
    response = await client.get(f"/api/v1/payments/{uuid.uuid4()}", headers={"X-API-Key": ""})
    assert response.status_code == 401


async def test_rejects_wrong_api_key(client: AsyncClient) -> None:
    response = await client.post(
        "/api/v1/payments",
        json=PAYMENT_BODY,
        headers={"X-API-Key": "wrong", "Idempotency-Key": "k1"},
    )
    assert response.status_code == 401


async def test_create_payment_writes_payment_and_outbox_event(
    client: AsyncClient, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    response = await client.post(
        "/api/v1/payments", json=PAYMENT_BODY, headers={"Idempotency-Key": "k1"}
    )

    assert response.status_code == 202
    body = response.json()
    assert body["status"] == "pending"
    assert set(body) == {"payment_id", "status", "created_at"}

    async with session_factory() as session:
        event = await session.scalar(select(OutboxEvent))
    assert event is not None
    assert event.event_type == "payment.created"
    assert event.routing_key == "payments.new"
    assert event.payload == {"payment_id": body["payment_id"]}
    assert event.published_at is None


async def test_same_idempotency_key_returns_same_payment(
    client: AsyncClient, session_factory: async_sessionmaker[AsyncSession]
) -> None:
    headers = {"Idempotency-Key": "same-key"}
    first = await client.post("/api/v1/payments", json=PAYMENT_BODY, headers=headers)
    second = await client.post("/api/v1/payments", json=PAYMENT_BODY, headers=headers)

    assert first.status_code == second.status_code == 202
    assert first.json()["payment_id"] == second.json()["payment_id"]
    assert await _count(session_factory, Payment) == 1
    assert await _count(session_factory, OutboxEvent) == 1


async def test_idempotency_key_is_required(client: AsyncClient) -> None:
    response = await client.post("/api/v1/payments", json=PAYMENT_BODY)
    assert response.status_code == 422


async def test_rejects_unsupported_currency(client: AsyncClient) -> None:
    response = await client.post(
        "/api/v1/payments",
        json={**PAYMENT_BODY, "currency": "GBP"},
        headers={"Idempotency-Key": "k1"},
    )
    assert response.status_code == 422


async def test_rejects_non_positive_amount(client: AsyncClient) -> None:
    response = await client.post(
        "/api/v1/payments",
        json={**PAYMENT_BODY, "amount": "0"},
        headers={"Idempotency-Key": "k1"},
    )
    assert response.status_code == 422


async def test_get_payment_returns_details(client: AsyncClient) -> None:
    created = await client.post(
        "/api/v1/payments", json=PAYMENT_BODY, headers={"Idempotency-Key": "k1"}
    )
    payment_id = created.json()["payment_id"]

    response = await client.get(f"/api/v1/payments/{payment_id}")

    assert response.status_code == 200
    body = response.json()
    assert body["payment_id"] == payment_id
    assert body["amount"] == "1500.50"
    assert body["currency"] == "RUB"
    assert body["description"] == "Order #42"
    assert body["metadata"] == {"order_id": 42}
    assert body["status"] == "pending"
    assert body["idempotency_key"] == "k1"
    assert body["webhook_url"] == "http://example.com/webhook"
    assert body["processed_at"] is None


async def test_get_unknown_payment_returns_404(client: AsyncClient) -> None:
    response = await client.get(f"/api/v1/payments/{uuid.uuid4()}")
    assert response.status_code == 404
