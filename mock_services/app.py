from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

FIXTURE_DIR = Path(__file__).parent / "fixtures"


def _read(name: str) -> Any:
    with (FIXTURE_DIR / name).open(encoding="utf-8") as handle:
        return json.load(handle)


app = FastAPI(title="Cascade Mock Systems")
_usage = {"day0": _read("day0_usage.json"), "day7": _read("day7_usage.json")}
_crm = _read("crm.json")
_contracts = _read("contracts.json")
_change = _read("api_v1_sunset.json")
_snapshot = "day0"
_lock = asyncio.Lock()


class AdvanceRequest(BaseModel):
    snapshot: str


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/usage/accounts")
async def usage_accounts(snapshot: str | None = Query(default=None)) -> list[dict[str, Any]]:
    async with _lock:
        requested = snapshot or _snapshot
        if requested not in _usage:
            raise HTTPException(status_code=400, detail=f"Unknown snapshot: {requested}")
        return _usage[requested]


@app.get("/usage/accounts/{account_id}")
async def usage_account(account_id: str, snapshot: str | None = Query(default=None)) -> dict[str, Any]:
    rows = await usage_accounts(snapshot)
    for row in rows:
        if row["account_id"] == account_id:
            return row
    raise HTTPException(status_code=404, detail=f"Unknown account: {account_id}")


@app.get("/crm/accounts")
async def crm_accounts() -> list[dict[str, Any]]:
    return _crm


@app.get("/crm/accounts/{account_id}")
async def crm_account(account_id: str) -> dict[str, Any]:
    for row in _crm:
        if row["account_id"] == account_id:
            return row
    raise HTTPException(status_code=404, detail=f"Unknown account: {account_id}")


@app.get("/contracts/accounts")
async def contract_accounts() -> list[dict[str, Any]]:
    return _contracts


@app.get("/contracts/accounts/{account_id}")
async def contract_account(account_id: str) -> dict[str, Any]:
    for row in _contracts:
        if row["account_id"] == account_id:
            return row
    raise HTTPException(status_code=404, detail=f"Unknown account: {account_id}")


@app.get("/migration/change")
async def migration_change() -> dict[str, Any]:
    return _change


@app.get("/scenario")
async def scenario() -> dict[str, str]:
    async with _lock:
        return {"snapshot": _snapshot}


@app.post("/scenario/advance")
async def advance_scenario(request: AdvanceRequest) -> dict[str, str]:
    global _snapshot
    if request.snapshot not in _usage:
        raise HTTPException(status_code=400, detail=f"Unknown snapshot: {request.snapshot}")
    async with _lock:
        _snapshot = request.snapshot
        return {"snapshot": _snapshot}
