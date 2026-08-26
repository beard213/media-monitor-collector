from __future__ import annotations

from fastapi import APIRouter

from app.ingestion.agent import AgentClient

router = APIRouter(prefix="/agent", tags=["agent"])


@router.get("/health")
def agent_health() -> dict:
    return AgentClient.from_settings().healthcheck()
