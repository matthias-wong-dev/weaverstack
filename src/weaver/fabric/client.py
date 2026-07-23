"""Fabric REST transport.

Thin on purpose: a token, a base URL, and enough error translation that a
failure says what failed rather than surfacing a bare HTTP status.
"""

from __future__ import annotations

import json
from typing import Any

from ..errors import WeaverError
from .auth import FABRIC_SCOPE, get_token

#: Generic technical defaults, not environment-specific.
FABRIC_API = "https://api.fabric.microsoft.com/v1"
ONELAKE_DFS = "https://onelake.dfs.fabric.microsoft.com"
DEFAULT_TIMEOUT = 60.0


class FabricError(WeaverError):
    """Raised when a Fabric API call fails."""


class FabricClient:
    """Authenticated access to the Fabric REST API."""

    def __init__(
        self,
        *,
        api_base_url: str = FABRIC_API,
        token: str | None = None,
        timeout: float = DEFAULT_TIMEOUT,
    ) -> None:
        self.api_base_url = api_base_url.rstrip("/")
        self.timeout = timeout
        self._token = token

    @property
    def token(self) -> str:
        if self._token is None:
            self._token = get_token(FABRIC_SCOPE)
        return self._token

    def request(
        self,
        method: str,
        path: str,
        *,
        payload: Any = None,
        expected: tuple[int, ...] = (200, 201, 202),
    ):
        import requests

        url = path if path.startswith("http") else f"{self.api_base_url}/{path.lstrip('/')}"
        response = requests.request(
            method,
            url,
            headers={
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json",
            },
            data=json.dumps(payload) if payload is not None else None,
            timeout=self.timeout,
        )
        if response.status_code not in expected:
            raise FabricError(
                f"{method} {url} returned {response.status_code}: "
                f"{response.text.strip()[:400] or 'no body'}"
            )
        return response

    def get_json(self, path: str) -> dict:
        response = self.request("GET", path, expected=(200,))
        return response.json() if response.content else {}

    def paged(self, path: str, *, key: str = "value") -> list[dict]:
        """Every item across a paged listing."""

        items: list[dict] = []
        next_path: str | None = path
        while next_path:
            payload = self.get_json(next_path)
            items.extend(payload.get(key, []))
            next_path = payload.get("continuationUri")
        return items
