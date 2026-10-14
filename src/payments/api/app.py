from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from fastapi import FastAPI

from payments.api.v1.payments import router as payments_router
from payments.core.config import Settings, get_settings
from payments.core.logging import configure_logging
from payments.db.session import create_engine, create_session_factory


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or get_settings()
    configure_logging(settings.log_level)

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        engine = create_engine(settings.database_url)
        app.state.session_factory = create_session_factory(engine)
        yield
        await engine.dispose()

    app = FastAPI(title="Payments service", version="1.0.0", lifespan=lifespan)
    app.state.settings = settings
    app.include_router(payments_router, prefix="/api/v1")

    @app.get("/health", tags=["ops"], include_in_schema=False)
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    return app
