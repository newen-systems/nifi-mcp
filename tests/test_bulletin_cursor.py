"""nifi_get_bulletins pages by bulletin id, which is what NiFi's cursor is.

FlowResource.getBulletinBoard documents "after" as "Includes bulletins with an id after this value"
(BulletinQuery.setAfter). A millisecond time sent there would hide every bulletin, so the old after_ms
argument is refused with a message that names after_id, and no request is sent.
"""

from __future__ import annotations

import json
from typing import Any

import httpx
import pytest
from conftest import settings

from nifi_mcp.client import NiFiClient
from nifi_mcp.server import configure, mcp


class Board(httpx.AsyncBaseTransport):
    def __init__(self) -> None:
        self.queries: list[str] = []

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.queries.append(request.url.query.decode())
        bulletins = [{"id": 42, "bulletin": {"id": 42, "level": "ERROR", "message": "boom"}}]
        return httpx.Response(200, json={"bulletinBoard": {"bulletins": bulletins}})


async def _call(params: dict[str, Any]) -> tuple[dict[str, Any], Board]:
    transport = Board()
    client = NiFiClient(settings(), transport=transport)
    configure(client, settings())
    try:
        out = await mcp.call_tool("nifi_get_bulletins", {"params": params})
    finally:
        await client.aclose()
    blocks = out[0] if isinstance(out, tuple) else out
    return json.loads("".join(getattr(block, "text", "") for block in blocks)), transport


@pytest.mark.asyncio
async def test_after_id_is_sent_as_the_bulletin_id_cursor() -> None:
    payload, transport = await _call({"after_id": 41})
    assert transport.queries == ["after=41"]
    assert payload["bulletins"][0]["id"] == 42


@pytest.mark.asyncio
async def test_no_cursor_sends_no_after() -> None:
    _payload, transport = await _call({})
    assert transport.queries == [""]


@pytest.mark.asyncio
@pytest.mark.parametrize("value", [1790000000000, 5])
async def test_after_ms_is_refused_and_names_after_id(value: int) -> None:
    payload, transport = await _call({"after_ms": value})
    assert payload["status"] == "error", payload
    assert "after_id" in payload["error"] and "bulletin id" in payload["error"], payload
    assert str(value) not in payload["error"], payload
    assert transport.queries == []


def test_the_schema_says_the_cursor_is_a_bulletin_id() -> None:
    tool = mcp._tool_manager.get_tool("nifi_get_bulletins")
    assert tool is not None
    schema = json.dumps(tool.parameters)
    assert "after_ms" not in schema
    assert "after_id" in schema and "bulletin id" in schema and "not a time" in schema
