from __future__ import annotations

import os
from typing import Any

import httpx


class MockSystemsClient:
    """Small HTTP-only boundary around the mocked vendor systems."""

    def __init__(self, base_url: str | None = None, timeout: float = 30.0):
        self.base_url = (base_url or os.getenv("CASCADE_MOCK_URL", "http://localhost:8001")).rstrip("/")
        self.timeout = timeout

    def _get(self, path: str, **params: Any) -> Any:
        response = httpx.get(f"{self.base_url}{path}", params=params or None, timeout=self.timeout)
        response.raise_for_status()
        return response.json()

    def usage_accounts(self, snapshot: str | None = None) -> list[dict[str, Any]]:
        return self._get("/usage/accounts", **({"snapshot": snapshot} if snapshot else {}))

    def usage_account(self, account_id: str, snapshot: str | None = None) -> dict[str, Any]:
        return self._get(f"/usage/accounts/{account_id}", **({"snapshot": snapshot} if snapshot else {}))

    def crm_accounts(self) -> list[dict[str, Any]]:
        return self._get("/crm/accounts")

    def crm_account(self, account_id: str) -> dict[str, Any]:
        return self._get(f"/crm/accounts/{account_id}")

    def contract_accounts(self) -> list[dict[str, Any]]:
        return self._get("/contracts/accounts")

    def contract_account(self, account_id: str) -> dict[str, Any]:
        return self._get(f"/contracts/accounts/{account_id}")

    def migration_change(self) -> dict[str, Any]:
        return self._get("/migration/change")

    def scenario(self) -> dict[str, str]:
        return self._get("/scenario")

    def advance(self, snapshot: str) -> dict[str, str]:
        response = httpx.post(f"{self.base_url}/scenario/advance", json={"snapshot": snapshot}, timeout=self.timeout)
        response.raise_for_status()
        return response.json()
