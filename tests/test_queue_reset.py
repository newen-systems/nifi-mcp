"""Selected queue resets verify remaining data without weakening mutation gates."""

import json

import httpx
import pytest
from conftest import settings
from pydantic import ValidationError

from nifi_mcp.client import NiFiClient
from nifi_mcp.server import EmptyQueueIn, QueueIn, configure, mcp

CONNECTION = "00000000-0000-0000-0000-000000000111"


def wire(router, *, remaining=0, readback_status=200, drop_status=202, readonly=False):
    reads = 0

    def connection(request):
        nonlocal reads
        reads += 1
        if reads > 1 and readback_status != 200:
            return httpx.Response(readback_status, text="password=never-echo-this")
        return httpx.Response(
            200,
            json={
                "component": {"id": CONNECTION},
                "status": {"aggregateSnapshot": {"flowFilesQueued": 5 if reads == 1 else remaining}},
            },
        )

    router.add("GET", f"/connections/{CONNECTION}", connection)
    router.json(
        "POST",
        f"/flowfile-queues/{CONNECTION}/drop-requests",
        {"dropRequest": {"id": "drop", "finished": False}},
        status=drop_status,
    )
    router.json(
        "GET",
        f"/flowfile-queues/{CONNECTION}/drop-requests/drop",
        {"dropRequest": {"id": "drop", "finished": True, "droppedCount": 5}},
    )
    router.json("DELETE", f"/flowfile-queues/{CONNECTION}/drop-requests/drop", {})
    cfg = settings(readonly=readonly)
    configure(NiFiClient(cfg, transport=httpx.MockTransport(router.handle)), cfg)


async def call(**params):
    result = await mcp.call_tool("nifi_empty_queue", {"params": {"connection_id": CONNECTION, **params}})
    blocks = result[0] if isinstance(result, tuple) else result
    return json.loads("".join(getattr(block, "text", "") for block in blocks))


async def test_authorized_selected_reset_reports_verified_empty_and_only_drops_selected_queue(router):
    wire(router)
    result = await call(timeout_seconds=1)
    assert result["outcome"] == "applied"
    assert result["connection_id"] == CONNECTION
    assert result["remaining_count"] == 0
    assert result["empty"] is True
    assert [item for item in router.calls if item[0] != "GET"] == [
        ("POST", f"/flowfile-queues/{CONNECTION}/drop-requests"),
        ("DELETE", f"/flowfile-queues/{CONNECTION}/drop-requests/drop"),
    ]
    assert all(CONNECTION in path for _, path in router.calls)


async def test_refilled_queue_reports_remaining_and_never_stops_or_redrops(router):
    wire(router, remaining=2)
    result = await call()
    assert result["outcome"] == "applied"
    assert result["remaining_count"] == 2
    assert result["empty"] is False
    assert "wait for active threads" in result["warnings"][0]
    assert len([method for method, _ in router.calls if method == "POST"]) == 1
    assert not any(method == "PUT" for method, _ in router.calls)


@pytest.mark.parametrize("status,remaining", [(403, 0), (404, 0), (200, None), (200, "not-a-count")])
async def test_verification_failure_keeps_drop_applied_but_empty_unknown(router, status, remaining):
    wire(router, remaining=remaining, readback_status=status)
    result = await call()
    assert result["outcome"] == "applied"
    assert result["empty"] is None
    assert result["remaining_count"] is None
    assert "unknown" in result["warnings"][0]
    assert "never-echo-this" not in json.dumps(result)


async def test_drop_permission_denied_is_not_bypassed(router):
    wire(router, drop_status=403)
    result = await call()
    assert result["outcome"] == "not_applied"
    assert result["status"] == "error"
    assert router.calls == [
        ("GET", f"/connections/{CONNECTION}"),
        ("POST", f"/flowfile-queues/{CONNECTION}/drop-requests"),
    ]


async def test_readonly_queue_reset_sends_nothing(router):
    wire(router, readonly=True)
    result = await call()
    assert result["outcome"] == "not_applied"
    assert result["type"] == "NiFiReadOnlyError"
    assert router.calls == []


@pytest.mark.parametrize("timeout", [0, -1, 121, float("inf"), float("nan")])
def test_reset_timeout_is_bounded(timeout):
    with pytest.raises(ValidationError):
        EmptyQueueIn(connection_id=CONNECTION, timeout_seconds=timeout)


def test_listing_schema_remains_unchanged():
    with pytest.raises(ValidationError):
        QueueIn(connection_id=CONNECTION, timeout_seconds=1)
    assert EmptyQueueIn(connection_id=CONNECTION, timeout_seconds=120).timeout_seconds == 120


async def test_queue_drop_annotation_forbids_idempotency_assumption():
    tool = next(tool for tool in await mcp.list_tools() if tool.name == "nifi_empty_queue")
    assert tool.annotations.idempotentHint is False
    assert tool.annotations.destructiveHint is True


async def test_timed_out_drop_is_unknown_without_cancelling_or_repeating(router):
    wire(router)
    router.json(
        "GET", f"/flowfile-queues/{CONNECTION}/drop-requests/drop", {"dropRequest": {"id": "drop", "finished": False}}
    )
    result = await call(timeout_seconds=0.01)
    assert result["outcome"] == "unknown"
    assert len([method for method, _ in router.calls if method == "POST"]) == 1
    assert not any(method == "DELETE" for method, _ in router.calls)


async def test_multi_cluster_reset_still_requires_explicit_selection(router):
    wire(router)
    cfg = settings(cluster_name="nifi-dev", clusters={"other": {"api_url": "https://other.example.test"}})
    configure(NiFiClient(cfg, transport=httpx.MockTransport(router.handle)), cfg)
    refused = await call()
    assert refused["outcome"] == "not_applied"
    assert router.calls == []
    result = await call(cluster="nifi-dev")
    assert result["outcome"] == "applied"
    assert result["connection_id"] == CONNECTION


async def test_protected_cluster_reset_requires_matching_confirmation(router):
    wire(router)
    cfg = settings(cluster_name="nifi-prod", environment="prod")
    configure(NiFiClient(cfg, transport=httpx.MockTransport(router.handle)), cfg)
    refused = await call(cluster="nifi-prod")
    assert refused["outcome"] == "not_applied"
    assert router.calls == []
    result = await call(cluster="nifi-prod", confirm_cluster="nifi-prod")
    assert result["outcome"] == "applied"
    assert result["cluster"] == "nifi-prod"
    assert result["empty"] is True
