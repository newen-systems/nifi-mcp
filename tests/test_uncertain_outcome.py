"""A mutation with no definite answer says it may have been applied and which read to run first."""

import json
from typing import Any

import httpx
import pytest
from conftest import settings

from nifi_mcp.client import NiFiClient
from nifi_mcp.errors import NiFiError, NiFiTimeoutError, NiFiUncertainError
from nifi_mcp.server import configure, mcp

PG = "00000000-0000-0000-0000-0000000000aa"
SVC = "00000000-0000-0000-0000-0000000000cc"


class Mutations(httpx.AsyncBaseTransport):
    """GETs answer; mutations time out, or get `status` from a gateway, unless `ok` names their path."""

    def __init__(self, status: int | None = None, ok: tuple[str, ...] = ()) -> None:
        self.status = status
        self.ok = ok

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        if request.method == "GET":
            body = {"processGroupFlow": {"id": PG, "flow": {}}, "component": {"id": PG}, "revision": {"version": 1}}
            return httpx.Response(200, json=body)
        if any(path.endswith(suffix) for suffix in self.ok):
            entity = {"id": SVC, "revision": {"version": 1}, "component": {"id": SVC, "state": "DISABLED"}}
            return httpx.Response(201, json=entity)
        if self.status:
            return httpx.Response(self.status, text="<html>upstream timed out</html>")
        raise httpx.ReadTimeout("timed out", request=request)


async def _call(transport: httpx.AsyncBaseTransport, tool: str, params: dict[str, Any]) -> dict[str, Any]:
    client = NiFiClient(settings(), transport=transport)
    configure(client, settings())
    try:
        out = await mcp.call_tool(tool, {"params": params})
    finally:
        await client.aclose()
    blocks = out[0] if isinstance(out, tuple) else out
    return json.loads("".join(getattr(block, "text", "") for block in blocks))


_CREATES = [
    ("nifi_create_processor", {"parent_id": PG, "processor_type": "x.P", "name": "P"}, f"nifi_get_flow on {PG}"),
    ("nifi_create_process_group", {"parent_id": PG, "name": "g"}, f"nifi_get_flow on {PG}"),
    (
        "nifi_create_controller_service",
        {"parent_id": PG, "service_type": "x.S", "name": "S"},
        f"nifi_list_controller_services on {PG}",
    ),
    (
        "nifi_create_connection",
        {
            "parent_id": PG,
            "source_id": PG,
            "source_group_id": PG,
            "destination_id": PG,
            "destination_group_id": PG,
            "relationships": "success",
        },
        f"nifi_get_flow on {PG}",
    ),
    ("nifi_import_flow", {"parent_id": PG, "group_name": "g", "snapshot": {}}, f"nifi_get_flow on {PG}"),
    ("nifi_create_parameter_context", {"name": "ctx"}, "nifi_list_parameter_contexts"),
]


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [None, 502, 504])
@pytest.mark.parametrize(("tool", "params", "read"), _CREATES)
async def test_single_create_with_no_answer_says_it_may_have_landed(
    tool: str, params: dict[str, Any], read: str, status: int | None
) -> None:
    # A retry after a timeout or a gateway 502/504 can make a duplicate.
    payload = await _call(Mutations(status), tool, params)
    assert payload["status"] == "error"
    assert payload["type"] == ("NiFiTimeoutError" if status is None else "NiFiUncertainError")
    assert payload["outcome"] == "unknown"
    assert "may have been applied" in payload["error"], payload
    hint = payload["hint"]
    assert f"Read back with {read}: " in hint, hint
    assert "duplicate" in hint


@pytest.mark.asyncio
async def test_enable_with_no_answer_after_a_create_names_the_created_service() -> None:
    payload = await _call(
        Mutations(504, ok=("/controller-services",)),
        "nifi_create_controller_service",
        {"parent_id": PG, "service_type": "x.S", "name": "S"},
    )
    assert payload["type"] == "NiFiUncertainError"
    assert payload["outcome"] == "unknown"
    assert f"Created controller service {SVC}" in payload["error"]
    assert f"Read back with nifi_get_controller_service on {SVC}: " in payload["hint"]
    assert payload["applied_requests"] == [f"POST /process-groups/{PG}/controller-services"]


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [502, 504])
async def test_client_raises_uncertain_for_a_gateway_error_on_a_mutation(status: int) -> None:
    client = NiFiClient(settings(), transport=Mutations(status))
    try:
        with pytest.raises(NiFiUncertainError) as caught:
            await client.set_controller_service_state(SVC, "ENABLED", 1)
    finally:
        await client.aclose()
    assert caught.value.status_code == status
    assert not isinstance(caught.value, NiFiTimeoutError)
    assert caught.value.outcome == "unknown"
    assert f"Read back with nifi_get_controller_service on {SVC}: " in (caught.value.hint or "")


@pytest.mark.asyncio
async def test_a_connection_lost_after_sending_a_mutation_is_uncertain() -> None:
    class Drops(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            raise httpx.RemoteProtocolError("Server disconnected without sending a response.", request=request)

    client = NiFiClient(settings(), transport=Drops())
    try:
        with pytest.raises(NiFiUncertainError, match="may have been applied"):
            await client.create_processor(PG, "x.P", "P")
    finally:
        await client.aclose()


@pytest.mark.asyncio
async def test_a_refused_connection_is_a_plain_error() -> None:
    class Refuses(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            raise httpx.ConnectError("refused", request=request)

    client = NiFiClient(settings(), transport=Refuses())
    try:
        with pytest.raises(NiFiError) as caught:
            await client.create_processor(PG, "x.P", "P")
    finally:
        await client.aclose()
    assert not isinstance(caught.value, NiFiUncertainError)


@pytest.mark.asyncio
async def test_a_gateway_error_on_a_read_is_retried_and_stays_a_plain_error() -> None:
    hits: list[str] = []

    class Gateway(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            hits.append(request.method)
            return httpx.Response(504, text="upstream timed out")

    client = NiFiClient(settings(), transport=Gateway())
    try:
        with pytest.raises(NiFiError) as caught:
            await client.get_flow(PG)
    finally:
        await client.aclose()
    assert hits == ["GET", "GET"]
    assert not isinstance(caught.value, NiFiUncertainError)


class Raises(httpx.AsyncBaseTransport):
    """GETs answer; every mutation raises `exc` from the transport."""

    def __init__(self, exc: type[httpx.TransportError]) -> None:
        self.exc = exc
        self.mutations: list[str] = []

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        if request.method == "GET":
            body = {"processGroupFlow": {"id": PG, "flow": {}}, "component": {"id": PG}, "revision": {"version": 1}}
            return httpx.Response(200, json=body)
        self.mutations.append(f"{request.method} {request.url.path}")
        raise self.exc(self.exc.__name__, request=request)


_NEVER_SENT = [httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout, httpx.ProxyError, httpx.UnsupportedProtocol]


@pytest.mark.asyncio
@pytest.mark.parametrize("exc", _NEVER_SENT)
async def test_a_mutation_that_was_never_sent_is_a_plain_error(exc: type[httpx.TransportError]) -> None:
    # ConnectTimeout and PoolTimeout come before any byte is written, like ConnectError.
    client = NiFiClient(settings(), transport=Raises(exc))
    try:
        with pytest.raises(NiFiError) as caught:
            await client.create_processor(PG, "x.P", "P")
    finally:
        await client.aclose()
    assert not isinstance(caught.value, NiFiUncertainError), str(caught.value)
    assert "may have been applied" not in str(caught.value)
    assert "nothing was sent" in str(caught.value)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "exc", [httpx.ReadTimeout, httpx.WriteTimeout, httpx.RemoteProtocolError, httpx.ReadError, httpx.WriteError]
)
async def test_a_mutation_that_may_have_been_written_stays_uncertain(exc: type[httpx.TransportError]) -> None:
    client = NiFiClient(settings(), transport=Raises(exc))
    try:
        with pytest.raises(NiFiUncertainError, match="may have been applied"):
            await client.create_processor(PG, "x.P", "P")
    finally:
        await client.aclose()


@pytest.mark.asyncio
@pytest.mark.parametrize("exc", [httpx.ConnectTimeout, httpx.PoolTimeout])
async def test_a_read_that_could_not_connect_in_time_is_still_retried(exc: type[httpx.TransportError]) -> None:
    calls: list[str] = []

    class Slow(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            calls.append(request.method)
            raise exc("slow", request=request)

    client = NiFiClient(settings(), transport=Slow())
    try:
        with pytest.raises(NiFiTimeoutError):
            await client.get_flow(PG)
    finally:
        await client.aclose()
    assert calls == ["GET", "GET"]


@pytest.mark.asyncio
@pytest.mark.parametrize("exc", [httpx.ConnectTimeout, httpx.PoolTimeout])
async def test_a_spec_step_that_was_never_sent_is_not_in_created(exc: type[httpx.TransportError]) -> None:
    # SpecResult.mutate records only a step NiFi may have received as state unknown.
    transport = Raises(exc)
    payload = await _call(
        transport,
        "nifi_apply_flow_spec",
        {"spec": {"parent_process_group_id": PG, "process_group": {"name": "g"}}},
    )
    assert payload["status"] == "error", payload
    assert payload["cause"] == "nifi"
    assert payload["created"] == []
    assert "may have been applied" not in payload["error"]
    assert transport.mutations == [f"POST /nifi-api/process-groups/{PG}/process-groups"]


CONN = "00000000-0000-0000-0000-000000000001"
PORT = "00000000-0000-0000-0000-000000000002"


class HeldFlow(httpx.AsyncBaseTransport):
    """GET of a connection or port names PG as its parent, and PG's flow lists both; mutations time out."""

    def __init__(self) -> None:
        self.reads: list[str] = []

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        if request.method != "GET":
            raise httpx.ReadTimeout("timed out", request=request)
        self.reads.append(request.url.path)
        held = {"revision": {"version": 3}, "component": {"parentGroupId": PG}}
        if request.url.path.endswith(f"/flow/process-groups/{PG}"):
            flow = {
                "connections": [{"id": CONN, **held}],
                "inputPorts": [{"id": PORT, **held}],
                "outputPorts": [{"id": PORT, **held}],
            }
            return httpx.Response(200, json={"processGroupFlow": {"id": PG, "flow": flow}})
        return httpx.Response(200, json={"id": request.url.path.rsplit("/", 1)[-1], **held})


@pytest.mark.asyncio
@pytest.mark.parametrize("known_group", [True, False])
@pytest.mark.parametrize(
    ("call", "kind", "component"),
    [
        (lambda c, g: c.update_connection(CONN, version=3, name="q", parent_id=g), "connection", CONN),
        (lambda c, g: c.delete_connection(CONN, 3, parent_id=g), "connection", CONN),
        (lambda c, g: c.update_port(PORT, version=3, kind="INPUT_PORT", x=0, y=0, parent_id=g), "input port", PORT),
        (lambda c, g: c.delete_port(PORT, 3, "OUTPUT_PORT", parent_id=g), "output port", PORT),
    ],
)
async def test_an_uncertain_connection_or_port_change_is_read_back_from_its_group(
    call: Any, kind: str, component: str, known_group: bool
) -> None:
    # No get tool returns a connection or port; nifi_get_flow on its group does,
    # and the client runs that read before it writes the hint.
    transport = HeldFlow()
    client = NiFiClient(settings(), transport=transport)
    try:
        with pytest.raises(NiFiTimeoutError) as caught:
            await call(client, PG if known_group else None)
    finally:
        await client.aclose()
    assert caught.value.outcome == "unknown"
    hint = caught.value.hint or ""
    assert f"Read back with nifi_get_flow on {PG}: {kind} {component} " in hint, hint
    assert "revision 3" in hint
    assert transport.reads[-1] == f"/nifi-api/flow/process-groups/{PG}"


def _held(kind: str, cid: str) -> dict[str, Any]:
    return {"id": cid, "revision": {"version": 3}, "component": {"id": cid, "parentGroupId": PG}}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("tool", "params", "get_path", "component"),
    [
        ("nifi_update_connection", {"connection_id": CONN, "name": "q"}, f"/connections/{CONN}", CONN),
        ("nifi_delete_component", {"kind": "connection", "component_id": CONN}, f"/connections/{CONN}", CONN),
        ("nifi_delete_component", {"kind": "input_port", "component_id": PORT}, f"/input-ports/{PORT}", PORT),
        ("nifi_delete_component", {"kind": "output_port", "component_id": PORT}, f"/output-ports/{PORT}", PORT),
    ],
)
async def test_a_timed_out_connection_or_port_tool_names_its_parent_group(
    tool: str, params: dict[str, Any], get_path: str, component: str
) -> None:
    class Held(httpx.AsyncBaseTransport):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            if request.method == "GET":
                return httpx.Response(200, json=_held(tool, component))
            raise httpx.ReadTimeout("timed out", request=request)

    payload = await _call(Held(), tool, params)
    assert payload["type"] == "NiFiTimeoutError", payload
    assert payload["outcome"] == "unknown"
    assert f"Read back with nifi_get_flow on {PG}: " in payload["hint"]
    assert component in payload["hint"]


@pytest.mark.asyncio
async def test_a_timed_out_layout_move_names_the_group_being_laid_out() -> None:
    from nifi_mcp.flow_spec import _move_ports, _route_connections

    client = NiFiClient(settings(), transport=Raises(httpx.ReadTimeout))
    outline = {
        "input_ports": [{"id": PORT, "name": "in", "x": 0, "y": 0, "revision": 1}],
        "connections": [{"id": CONN, "source": {"id": PORT}, "destination": {"id": SVC}, "revision": 1}],
    }
    try:
        with pytest.raises(NiFiTimeoutError) as moved:
            await _move_ports(client, PG, outline, {PORT: (100.0, 100.0)})
        with pytest.raises(NiFiTimeoutError) as routed:
            await _route_connections(client, PG, outline, {PORT: (0.0, 0.0), SVC: (0.0, 300.0)})
    finally:
        await client.aclose()
    assert f"Read back with nifi_get_flow on {PG}: it does not list input port {PORT}" in (moved.value.hint or "")
    assert f"Read back with nifi_get_flow on {PG}: it does not list connection {CONN}" in (routed.value.hint or "")
