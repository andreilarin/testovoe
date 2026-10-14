from uuid import UUID


class PaymentNotFoundError(Exception):
    def __init__(self, payment_id: UUID) -> None:
        super().__init__(f"Payment {payment_id} not found")
        self.payment_id = payment_id
