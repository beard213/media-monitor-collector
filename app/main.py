from fastapi import FastAPI

from app.api.collections import router as collections_router
from app.adapters.registry import platform_statuses
from app.core.config import get_settings

settings = get_settings()

app = FastAPI(
    title=settings.app_name,
    version="0.1.0",
    description="Unified foreign platform collection service architecture.",
)


@app.get("/health")
def health() -> dict:
    return {
        "ok": True,
        "app": settings.app_name,
        "environment": settings.environment,
    }


@app.get("/platforms")
def platforms() -> dict:
    return {"items": platform_statuses()}


app.include_router(collections_router)
