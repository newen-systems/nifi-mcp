"""Read-only queue sampling, cluster routing and temporary listing cleanup."""
import json

import httpx
import pytest
from conftest import settings

from nifi_mcp.client import NiFiClient
from nifi_mcp.errors import NiFiError
from nifi_mcp.server import configure, mcp

CONNECTION = "11111111-1111-1111-1111-111111111111"


class QueueTransport(httpx.AsyncBaseTransport):
    def __init__(self) -> None:
        self.calls: list[httpx.Request] = []

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        self.calls.append(request)
        if request.method == "DELETE":
            return httpx.Response(200, json={})
        if "/flowfiles/" in request.url.path:
            return httpx.Response(200, json={"flowFile": {"attributes": {"filename": "sample.json"}}})
        return httpx.Response(202 if request.method == "POST" else 200, json={
            "listingRequest": {"id": "listing", "finished": request.method == "GET",
                               "flowFileSummaries": [{"uuid": "file-a", "clusterNodeId": "node-a"},
                                                     {"uuid": "file-b", "clusterNodeId": "node-b"}]}
        })


@pytest.mark.asyncio
@pytest.mark.parametrize("include_attributes", [False, True])
async def test_bounded_queue_listing_is_readonly_and_routes_details(include_attributes: bool) -> None:
    transport = QueueTransport()
    config = settings(readonly=True)
    client = NiFiClient(config, transport=transport)
    configure(client, config)
    try:
        result = await mcp.call_tool("nifi_list_queue", {"params": {
            "connection_id": CONNECTION, "limit": 1, "include_attributes": include_attributes,
        }})
        blocks = result[0] if isinstance(result, tuple) else result
        payload = json.loads("".join(getattr(block, "text", "") for block in blocks))
    finally:
        await client.aclose()
    listing = payload["listingRequest"]
    assert listing["returnedCount"] == 1
    assert listing["sampleTruncated"] is True
    assert len(listing["flowFileSummaries"]) == 1
    detail_requests = [r for r in transport.calls if "/flowfiles/" in r.url.path]
    assert len(detail_requests) == int(include_attributes)
    if include_attributes:
        assert detail_requests[0].url.params["clusterNodeId"] == "node-a"
        assert listing["flowFileSummaries"][0]["details"]["flowFile"]["attributes"]["filename"] == "sample.json"
    assert any(r.method == "DELETE" and r.url.path.endswith("/listing-requests/listing") for r in transport.calls)
    assert not any("drop-requests" in r.url.path for r in transport.calls)


@pytest.mark.asyncio
async def test_listing_timeout_still_removes_temporary_request() -> None:
    class PendingQueue(QueueTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            response = await super().handle_async_request(request)
            if request.method == "GET":
                return httpx.Response(200, json={"listingRequest": {"id": "listing", "finished": False}})
            return response

    transport = PendingQueue()
    client = NiFiClient(settings(readonly=True), transport=transport)
    try:
        with pytest.raises(NiFiError, match="still running"):
            await client.list_queue(CONNECTION, timeout_seconds=0)
    finally:
        await client.aclose()
    assert transport.calls[-1].method == "DELETE"
    assert transport.calls[-1].url.path.endswith("/listing-requests/listing")
