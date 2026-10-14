from functools import lru_cache

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql+asyncpg://payments:payments@localhost:5432/payments"
    rabbitmq_url: str = "amqp://guest:guest@localhost:5672/"
    api_key: SecretStr = SecretStr("dev-secret-key")
    log_level: str = "INFO"

    # Outbox relay
    outbox_poll_interval: float = Field(default=1.0, gt=0)
    outbox_batch_size: int = Field(default=100, gt=0)

    # Consumer
    consumer_prefetch_count: int = Field(default=1, gt=0)
    retry_max_attempts: int = Field(default=3, ge=0)
    retry_base_delay: float = Field(default=2.0, gt=0)

    # Payment gateway emulation
    gateway_min_delay: float = Field(default=2.0, ge=0)
    gateway_max_delay: float = Field(default=5.0, ge=0)
    gateway_success_rate: float = Field(default=0.9, ge=0, le=1)

    webhook_timeout: float = Field(default=10.0, gt=0)

    @property
    def retry_delays_ms(self) -> list[int]:
        """Exponential backoff delays: base, base*2, base*4, ..."""
        return [
            int(self.retry_base_delay * 1000 * 2**attempt)
            for attempt in range(self.retry_max_attempts)
        ]


@lru_cache
def get_settings() -> Settings:
    return Settings()
