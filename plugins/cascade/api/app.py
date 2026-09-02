from __future__ import annotations

from fastapi import FastAPI
from fastapi.responses import HTMLResponse

from cascade.api.routes import accounts, campaigns, exceptions, scenario

cascade_app = FastAPI(title="Cascade API")
cascade_app.include_router(campaigns.router)
cascade_app.include_router(accounts.router)
cascade_app.include_router(exceptions.router)
cascade_app.include_router(scenario.router)
cascade_app.include_router(scenario.orchestration_router)


@cascade_app.on_event("startup")
def initialize_cascade_schema() -> None:
    """Initialize the separate Cascade database after plugin imports settle."""

    from cascade.store import ensure_schema

    ensure_schema()


@cascade_app.get("/app", response_class=HTMLResponse)
def standalone_app() -> str:
    return """<!doctype html><html><head><title>Cascade</title></head><body><div id='cascade-root'></div><script type='module' src='/cascade/static/cascade.umd.cjs'></script></body></html>"""
