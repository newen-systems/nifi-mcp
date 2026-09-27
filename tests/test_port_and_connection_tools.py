import json
from typing import Any

import httpx
import pytest
from conftest import Router, settings

from nifi_mcp.client import NiFiClient
from nifi_mcp.server import configure, mcp

PORT_ID = "00000000-0000-0000-0000-0000000000e1"


def _wire(router: Router) -> None:
    configure(NiFiClient(settings(), transport=httpx.MockTransport(router.handle)), settings())


async def _call(tool: str, params: dict[str, Any]) -> dict[str, Any]:
    out = await mcp.call_tool(tool, {"params": params})
    blocks = out[0] if isinstance(out, tuple) else out
    return json.loads("".join(getattr(block, "text", "") for block in blocks))


@pytest.mark.asyncio
@pytest.mark.parametrize(("kind", "segment"), [("input_port", "input-ports"), ("output_port", "output-ports")])
async def test_delete_component_deletes_a_port_at_its_current_revision(router: Router, kind: str, segment: str) -> None:
    # The builder's rollback hint lists ports by id; the delete tool must take them.
    deleted: dict[str, Any] = {}

    def delete(request: httpx.Request) -> httpx.Response:
        deleted.update(request.url.params)
        return httpx.Response(200, json={"id": PORT_ID})

    router.json("GET", f"/{segment}/{PORT_ID}", {"id": PORT_ID, "revision": {"version": 4}})
    router.add("DELETE", f"/{segment}/{PORT_ID}", delete)
    _wire(router)
    payload = await _call("nifi_delete_component", {"kind": kind, "component_id": PORT_ID})
    assert payload["status"] == "ok", payload
    assert (payload["kind"], payload["deleted"]) == (kind, PORT_ID)
    assert deleted["version"] == "4"
    assert deleted["clientId"] == settings().client_id


PG = "00000000-0000-0000-0000-0000000000a1"
SRC = "00000000-0000-0000-0000-0000000000a2"
DST = "00000000-0000-0000-0000-0000000000a3"
CONN = "00000000-0000-0000-0000-0000000000a4"
_QUEUE = {
    "back_pressure_object_threshold": 5,
    "back_pressure_data_size_threshold": "10 MB",
    "flow_file_expiration": "1 min",
}
_QUEUE_DTO = {"backPressureObjectThreshold": 5, "backPressureDataSizeThreshold": "10 MB", "flowFileExpiration": "1 min"}


def _echo(sent: list[dict[str, Any]], status: int) -> Any:
    def handler(request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        sent.append(body)
        component = {"id": CONN, **body["component"]}
        return httpx.Response(status, json={"id": CONN, "revision": {"version": 2}, "component": component})

    return handler


_CREATE = {
    "parent_id": PG,
    "source_id": SRC,
    "source_group_id": PG,
    "destination_id": DST,
    "destination_group_id": PG,
    "relationships": "success",
}


@pytest.mark.asyncio
async def test_create_connection_sends_queue_backpressure_and_expiration(router: Router) -> None:
    # ConnectionDTO backPressureObjectThreshold, backPressureDataSizeThreshold, flowFileExpiration.
    sent: list[dict[str, Any]] = []
    router.add("POST", f"/process-groups/{PG}/connections", _echo(sent, 201))
    _wire(router)
    payload = await _call("nifi_create_connection", {**_CREATE, **_QUEUE})
    assert payload["status"] == "ok", payload
    assert sent[0]["component"].items() >= _QUEUE_DTO.items()
    assert payload["connection"].items() >= _QUEUE.items()


@pytest.mark.asyncio
async def test_create_connection_without_queue_settings_leaves_nifi_defaults(router: Router) -> None:
    sent: list[dict[str, Any]] = []
    router.add("POST", f"/process-groups/{PG}/connections", _echo(sent, 201))
    _wire(router)
    await _call("nifi_create_connection", _CREATE)
    assert not set(_QUEUE_DTO) & set(sent[0]["component"])


@pytest.mark.asyncio
async def test_update_connection_changes_only_the_queue_settings_given(router: Router) -> None:
    sent: list[dict[str, Any]] = []
    router.json("GET", f"/connections/{CONN}", {"id": CONN, "revision": {"version": 7}, "component": {"id": CONN}})
    router.add("PUT", f"/connections/{CONN}", _echo(sent, 200))
    _wire(router)
    payload = await _call("nifi_update_connection", {"connection_id": CONN, "flow_file_expiration": "30 sec"})
    assert payload["status"] == "ok", payload
    assert sent[0]["revision"]["version"] == 7
    # No selectedRelationships, source or destination: StandardConnectionDAO leaves them alone.
    assert sent[0]["component"] == {"id": CONN, "flowFileExpiration": "30 sec"}


@pytest.mark.asyncio
async def test_update_connection_with_nothing_to_change_is_refused(router: Router) -> None:
    _wire(router)
    payload = await _call("nifi_update_connection", {"connection_id": CONN})
    assert payload["status"] == "error"
    assert router.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("tool", "params"),
    [
        ("nifi_create_connection", {**_CREATE, "back_pressure_objct_threshold": 5}),
        ("nifi_update_connection", {"connection_id": CONN, "flowfile_expiration": "1 min"}),
        ("nifi_create_processor", {"parent_id": PG, "processor_type": "x.P", "name": "P", "schedule": "10 sec"}),
        ("nifi_create_parameter_context", {"name": "c", "parameters": [{"name": "a", "sensitve": True}]}),
        ("nifi_delete_component", {"kind": "processor", "component_id": SRC, "force": True}),
    ],
)
async def test_unknown_tool_arguments_are_an_error_not_a_silent_no_op(
    router: Router, tool: str, params: dict[str, Any]
) -> None:
    # A misspelt setting was dropped and the tool reported success.
    _wire(router)
    payload = await _call(tool, params)
    assert payload["status"] == "error", payload
    assert payload["type"] == "ValidationError"
    assert "Extra inputs are not permitted" in payload["error"]
    assert router.calls == []


@pytest.mark.asyncio
async def test_unknown_top_level_tool_argument_is_an_error(router: Router) -> None:
    _wire(router)
    out = await mcp.call_tool("nifi_get_flow", {"params": {}, "verbose": True})
    blocks = out[0] if isinstance(out, tuple) else out
    payload = json.loads("".join(getattr(block, "text", "") for block in blocks))
    assert payload["status"] == "error"
    assert "verbose" in payload["error"]
    assert router.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("size", ["canary-1234", "10 canary-1234", "MB", "1.5.5 MB"])
async def test_a_data_size_that_is_not_one_is_refused_before_any_request(router: Router, size: str) -> None:
    # NiFi echoed "Invalid data size: <value>" after it had already set the other queue fields.
    _wire(router)
    for tool, params in (
        ("nifi_update_connection", {"connection_id": CONN, "back_pressure_data_size_threshold": size}),
        ("nifi_create_connection", {**_CREATE, "back_pressure_data_size_threshold": size}),
    ):
        payload = await _call(tool, params)
        assert payload["status"] == "error", payload
        assert "back_pressure_data_size_threshold" in payload["error"]
        assert "canary-1234" not in json.dumps(payload)
    assert router.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("size", ["10 MB", "1 GB", "0.5 kb", "10MB", "2 TB", "100 B"])
async def test_a_nifi_data_size_is_sent_as_given(router: Router, size: str) -> None:
    sent: list[dict[str, Any]] = []
    router.json("GET", f"/connections/{CONN}", {"id": CONN, "revision": {"version": 1}, "component": {"id": CONN}})
    router.add("PUT", f"/connections/{CONN}", _echo(sent, 200))
    _wire(router)
    payload = await _call("nifi_update_connection", {"connection_id": CONN, "back_pressure_data_size_threshold": size})
    assert payload["status"] == "ok", payload
    assert sent[0]["component"]["backPressureDataSizeThreshold"] == size


@pytest.mark.asyncio
async def test_the_client_refuses_a_bad_data_size_without_echoing_it() -> None:
    sent: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        sent.append(request)
        body = json.loads(request.content)
        return httpx.Response(400, text=f"Invalid data size: {body['component']['backPressureDataSizeThreshold']}")

    client = NiFiClient(settings(), transport=httpx.MockTransport(handler))
    with pytest.raises(Exception) as excinfo:
        await client.update_connection(CONN, version=3, back_pressure_data_size_threshold="canary-1234")
    assert "canary-1234" not in str(excinfo.value)
    assert "back_pressure_data_size_threshold" in str(excinfo.value)
    assert sent == []


@pytest.mark.asyncio
async def test_a_refused_data_size_after_other_queue_fields_is_not_a_clean_failure(router: Router) -> None:
    # StandardConnectionDAO.configureConnection sets flowFileExpiration and
    # backPressureObjectThreshold before DataUnit.parseDataSize throws, and nothing restores them.
    router.json(
        "GET",
        f"/connections/{CONN}",
        {"id": CONN, "revision": {"version": 1}, "component": {"id": CONN, "parentGroupId": PG}},
    )
    router.add("PUT", f"/connections/{CONN}", lambda _r: httpx.Response(400, text="Invalid data size: 10 MB"))
    _wire(router)
    payload = await _call(
        "nifi_update_connection",
        {"connection_id": CONN, "flow_file_expiration": "1 min", "back_pressure_data_size_threshold": "10 MB"},
    )
    assert payload["status"] == "error", payload
    assert payload["type"] == "NiFiUncertainError"
    assert payload["outcome"] == "unknown"
    assert "flow_file_expiration" in payload["error"]
    assert "may already be applied" in payload["error"]
    assert f"Read back with nifi_get_flow on {PG}: " in payload["hint"]
    assert "10 MB" not in payload["error"]
