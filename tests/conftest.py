from __future__ import annotations

from collections.abc import Callable
from typing import Any

import httpx
import pytest

from nifi_mcp.client import NiFiClient
from nifi_mcp.config import Settings


def settings(**overrides: Any) -> Settings:
    values = {
        "api_url": "https://nifi.example.test/nifi-api",
        "readonly": False,
        "auth": "bearer",
        "bearer_token": "test-token",
        "tls_verify": True,
        "client_id": "nifi-mcp-test",
    }
    values.update(overrides)
    return Settings(**values)


class Router:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []
        self._handlers: dict[tuple[str, str], Callable[[httpx.Request], httpx.Response]] = {}

    def add(self, method: str, path: str, handler: Callable[[httpx.Request], httpx.Response]) -> None:
        self._handlers[(method.upper(), path)] = handler

    def json(
        self,
        method: str,
        path: str,
        payload: Any,
        status: int = 200,
    ) -> None:
        def _handler(_request: httpx.Request, body: Any = payload, code: int = status) -> httpx.Response:
            return httpx.Response(code, json=body)

        self.add(method, path, _handler)

    def handle(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if path.startswith("/nifi-api"):
            path = path[len("/nifi-api") :] or "/"
        key = (request.method.upper(), path)
        self.calls.append(key)
        handler = self._handlers.get(key)
        if handler is None:
            return httpx.Response(404, json={"message": f"unhandled {key}"})
        return handler(request)


@pytest.fixture
def router() -> Router:
    return Router()


@pytest.fixture
def client(router: Router) -> NiFiClient:
    transport = httpx.MockTransport(router.handle)
    return NiFiClient(settings(), transport=transport)
