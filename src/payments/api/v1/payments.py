from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, Header, HTTPException, status

from payments.api.deps import PaymentServiceDep, verify_api_key
from payments.domain.exceptions import PaymentNotFoundError
from payments.schemas import PaymentAccepted, PaymentCreate, PaymentRead

router = APIRouter(
    prefix="/payments",
    tags=["payments"],
    dependencies=[Depends(verify_api_key)],
    responses={status.HTTP_401_UNAUTHORIZED: {"description": "Invalid or missing API key"}},
)


@router.post(
    "",
    status_code=status.HTTP_202_ACCEPTED,
    response_model=PaymentAccepted,
    summary="Create a payment",
    description=(
        "Accepts a payment for asynchronous processing. Repeating the request with "
        "the same Idempotency-Key returns the originally created payment."
    ),
)
async def create_payment(
    body: PaymentCreate,
    service: PaymentServiceDep,
    idempotency_key: Annotated[str, Header(alias="Idempotency-Key", min_length=1, max_length=255)],
) -> PaymentAccepted:
    payment = await service.create(body, idempotency_key)
    return PaymentAccepted.from_model(payment)


@router.get(
    "/{payment_id}",
    response_model=PaymentRead,
    summary="Get a payment",
    responses={status.HTTP_404_NOT_FOUND: {"description": "Payment not found"}},
)
async def get_payment(payment_id: UUID, service: PaymentServiceDep) -> PaymentRead:
    try:
        payment = await service.get(payment_id)
    except PaymentNotFoundError as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    return PaymentRead.from_model(payment)
