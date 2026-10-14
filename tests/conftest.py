from collections.abc import AsyncIterator

import pytest
from httpx import ASGITransport, AsyncClient
from pydantic import SecretStr
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import StaticPool

from payments.api.app import create_app
from payments.core.config import Settings
from payments.db import models  # noqa: F401
from payments.db.base import Base
from payments.db.session import create_session_factory

API_KEY = "test-api-key"


@pytest.fixture
async def engine() -> AsyncIterator[AsyncEngine]:
    # In-memory SQLite keeps the suite infrastructure-free. Postgres-only bits
    # (JSONB, FOR UPDATE SKIP LOCKED) degrade gracefully on SQLite.
    engine = create_async_engine("sqlite+aiosqlite://", poolclass=StaticPool)
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield engine
    await engine.dispose()


@pytest.fixture
def session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return create_session_factory(engine)


@pytest.fixture
def settings() -> Settings:
    return Settings(_env_file=None, api_key=SecretStr(API_KEY))


@pytest.fixture
async def client(
    settings: Settings, session_factory: async_sessionmaker[AsyncSession]
) -> AsyncIterator[AsyncClient]:
    app = create_app(settings)
    # ASGITransport does not run the lifespan, so wire the test DB in directly.
    app.state.session_factory = session_factory
    async with AsyncClient(
        transport=ASGITransport(app=app),
        base_url="http://test",
        headers={"X-API-Key": API_KEY},
    ) as client:
        yield client
