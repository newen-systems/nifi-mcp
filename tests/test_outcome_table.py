"""Every mutation ends in one Outcome (applied, not_applied or unknown), and unknown is
read back before its hint is written.

One table crosses every mutation tool, every nifi_apply_flow_spec step and every
nifi_layout_process_group move with every failure class. FakeNiFi answers every read and every
mutation; the one request a case targets fails with the class under test.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Any

import httpx
import pytest
from conftest import settings
from fake_nifi import (
    CHILD,
    CONN,
    CTX,
    NEWPG,
    NEWPORT,
    NEWPROC,
    NEWSVC,
    PG,
    PORT_IN,
    PORT_OUT,
    PROC,
    SVC,
    FakeNiFi,
)

from nifi_mcp.client import NiFiClient
from nifi_mcp.server import configure, mcp

_NEVER_SENT = ("ConnectError", "ConnectTimeout", "PoolTimeout")
_NO_ANSWER = ("ReadTimeout", "WriteTimeout", "RemoteProtocolError")
FAILURES: list[str] = [*_NEVER_SENT, *_NO_ANSWER, "400", "404", "409", "502", "504", "2xx"]


def _expected(failure: str) -> str:
    if failure == "2xx":
        return "applied"
    if failure in _NEVER_SENT or failure in {"400", "404", "409"}:
        return "not_applied"
    return "unknown"


@dataclass(frozen=True)
class Case:
    label: str
    tool: str
    params: dict[str, Any]
    target: tuple[str, str]
    read: str
    finding: str
    # For a nifi_apply_flow_spec step: the created[] kind the step records, and whether it is a create.
    spec_kind: str | None = None
    spec_create: bool = True


SPEC = {
    "parent_process_group_id": PG,
    "process_group": {"name": "g", "parameter_context_id": CTX},
    "objects": [
        {"type": "controller_service", "name": "S", "service_type": "x.S"},
        {"type": "processor", "name": "P", "processor_type": "x.P", "properties": {"svc": "@S"}},
        {"type": "output_port", "name": "out"},
        {"type": "connection", "source": "P", "target": "out", "relationships": ["success"]},
    ],
}
_SERVICE = {"parent_id": PG, "service_type": "x.S", "name": "S"}
_CONNECTION = {
    "parent_id": PG,
    "source_id": PROC,
    "source_group_id": PG,
    "destination_id": PORT_OUT,
    "destination_group_id": PG,
    "destination_type": "OUTPUT_PORT",
    "relationships": ["success"],
}
_FLOW_PG = f"nifi_get_flow on {PG}"

CASES = [
    Case("create group", "nifi_create_process_group", {"parent_id": PG, "name": "g"},
         ("POST", f"/process-groups/{PG}/process-groups"), _FLOW_PG, "it lists no process group"),
    Case("create processor", "nifi_create_processor", {"parent_id": PG, "processor_type": "x.P", "name": "P"},
         ("POST", f"/process-groups/{PG}/processors"), _FLOW_PG, "it lists no processor"),
    Case("update processor", "nifi_update_processor", {"processor_id": PROC, "name": "renamed"},
         ("PUT", f"/processors/{PROC}"), f"nifi_get_processor on {PROC}", "still at revision 1"),
    Case("schedule processor", "nifi_set_run_status", {"component_id": PROC, "state": "RUNNING"},
         ("PUT", f"/processors/{PROC}/run-status"), f"nifi_get_processor on {PROC}", "its state is STOPPED"),
    Case("create connection", "nifi_create_connection", _CONNECTION,
         ("POST", f"/process-groups/{PG}/connections"), _FLOW_PG,
         f"it lists 1 connection(s) from {PROC} to {PORT_OUT}"),
    Case("update connection", "nifi_update_connection", {"connection_id": CONN, "name": "q"},
         ("PUT", f"/connections/{CONN}"), _FLOW_PG, f"connection {CONN} is still at revision 1"),
    Case("create service", "nifi_create_controller_service", {**_SERVICE, "enable": False},
         ("POST", f"/process-groups/{PG}/controller-services"), f"nifi_list_controller_services on {PG}",
         "it lists no controller service"),
    Case("enable created service", "nifi_create_controller_service", _SERVICE,
         ("PUT", f"/controller-services/{NEWSVC}/run-status"), f"nifi_get_controller_service on {NEWSVC}",
         "its state is DISABLED"),
    Case("update service", "nifi_update_controller_service", {"service_id": SVC, "name": "renamed"},
         ("PUT", f"/controller-services/{SVC}"), f"nifi_get_controller_service on {SVC}", "still at revision 1"),
    Case("enable service", "nifi_set_controller_service_state", {"service_id": SVC, "state": "ENABLED"},
         ("PUT", f"/controller-services/{SVC}/run-status"), f"nifi_get_controller_service on {SVC}",
         "its state is DISABLED"),
    Case("schedule group", "nifi_schedule_process_group", {"process_group_id": PG, "state": "RUNNING"},
         ("PUT", f"/flow/process-groups/{PG}"), _FLOW_PG, f"processor states in {PG}: STOPPED 1"),
    Case("delete processor", "nifi_delete_component", {"kind": "processor", "component_id": PROC},
         ("DELETE", f"/processors/{PROC}"), f"nifi_get_processor on {PROC}", "still exists at revision 1"),
    Case("delete connection", "nifi_delete_component", {"kind": "connection", "component_id": CONN},
         ("DELETE", f"/connections/{CONN}"), _FLOW_PG, "still exists at revision 1"),
    Case("delete service", "nifi_delete_component", {"kind": "controller_service", "component_id": SVC},
         ("DELETE", f"/controller-services/{SVC}"), f"nifi_get_controller_service on {SVC}", "still exists"),
    Case("delete input port", "nifi_delete_component", {"kind": "input_port", "component_id": PORT_IN},
         ("DELETE", f"/input-ports/{PORT_IN}"), _FLOW_PG, "still exists at revision 1"),
    Case("delete output port", "nifi_delete_component", {"kind": "output_port", "component_id": PORT_OUT},
         ("DELETE", f"/output-ports/{PORT_OUT}"), _FLOW_PG, "still exists at revision 1"),
    Case("delete group", "nifi_delete_component", {"kind": "process_group", "component_id": CHILD},
         ("DELETE", f"/process-groups/{CHILD}"), f"nifi_get_flow on {CHILD}", "still exists"),
    Case("delete context", "nifi_delete_component", {"kind": "parameter_context", "component_id": CTX},
         ("DELETE", f"/parameter-contexts/{CTX}"), f"nifi_get_parameter_context on {CTX}", "still exists"),
    Case("bind context", "nifi_bind_parameter_context", {"process_group_id": CHILD, "parameter_context_id": CTX},
         ("PUT", f"/process-groups/{CHILD}"), f"nifi_get_flow on {CHILD}", f"its parameter context is {CTX}"),
    Case("empty queue", "nifi_empty_queue", {"connection_id": CONN},
         ("POST", f"/flowfile-queues/{CONN}/drop-requests"), _FLOW_PG, f"connection {CONN} has 4 FlowFiles queued"),
    Case("import flow", "nifi_import_flow", {"parent_id": PG, "group_name": "g", "snapshot": {}},
         ("POST", f"/process-groups/{PG}/process-groups/import"), _FLOW_PG, "it lists no process group"),
    Case("replace flow", "nifi_replace_flow", {"process_group_id": PG, "snapshot": {}},
         ("POST", f"/process-groups/{PG}/replace-requests"), _FLOW_PG, "still at revision 1"),
    Case("create context", "nifi_create_parameter_context", {"name": "ctx"},
         ("POST", "/parameter-contexts"), "nifi_list_parameter_contexts", "it lists no parameter context"),
    Case("update context", "nifi_update_parameter_context", {"parameter_context_id": CTX, "name": "renamed"},
         ("POST", f"/parameter-contexts/{CTX}/update-requests"), f"nifi_get_parameter_context on {CTX}",
         "still at revision 1"),
    Case("layout: processor move", "nifi_layout_process_group", {"process_group_id": PG},
         ("PUT", f"/processors/{PROC}"), f"nifi_get_processor on {PROC}", "still at revision 1"),
    Case("layout: port move", "nifi_layout_process_group", {"process_group_id": PG},
         ("PUT", f"/input-ports/{PORT_IN}"), _FLOW_PG, f"input port {PORT_IN} is still at revision 1"),
    Case("layout: group move", "nifi_layout_process_group", {"process_group_id": PG},
         ("PUT", f"/process-groups/{CHILD}"), f"nifi_get_flow on {CHILD}", "still at revision 1"),
    Case("layout: connection route", "nifi_layout_process_group", {"process_group_id": PG},
         ("PUT", f"/connections/{CONN}"), _FLOW_PG, f"connection {CONN} is still at revision 1"),
    Case("spec: group", "nifi_apply_flow_spec", {"spec": SPEC},
         ("POST", f"/process-groups/{PG}/process-groups"), _FLOW_PG, "it lists no process group",
         spec_kind="process_group"),
    Case("spec: service", "nifi_apply_flow_spec", {"spec": SPEC},
         ("POST", f"/process-groups/{NEWPG}/controller-services"), f"nifi_list_controller_services on {NEWPG}",
         "it lists no controller service", spec_kind="controller_service"),
    Case("spec: enable", "nifi_apply_flow_spec", {"spec": SPEC},
         ("PUT", f"/controller-services/{NEWSVC}/run-status"), f"nifi_get_controller_service on {NEWSVC}",
         "its state is DISABLED", spec_kind="controller_service", spec_create=False),
    Case("spec: processor", "nifi_apply_flow_spec", {"spec": SPEC},
         ("POST", f"/process-groups/{NEWPG}/processors"), f"nifi_get_flow on {NEWPG}", "it lists no processor",
         spec_kind="processor"),
    Case("spec: port", "nifi_apply_flow_spec", {"spec": SPEC},
         ("POST", f"/process-groups/{NEWPG}/output-ports"), f"nifi_get_flow on {NEWPG}", "it lists no output port",
         spec_kind="output_port"),
    Case("spec: connection", "nifi_apply_flow_spec", {"spec": SPEC},
         ("POST", f"/process-groups/{NEWPG}/connections"), f"nifi_get_flow on {NEWPG}",
         f"it lists no connection from {NEWPROC} to {NEWPORT}", spec_kind="connection"),
]

# A read tool call that shows the state, and the REST read it is.
_READ_PATHS = [
    (re.compile(r"^nifi_get_flow on (\S+)$"), "/flow/process-groups/{0}"),
    (re.compile(r"^nifi_list_controller_services on (\S+)$"), "/flow/process-groups/{0}/controller-services"),
    (re.compile(r"^nifi_get_processor on (\S+)$"), "/processors/{0}"),
    (re.compile(r"^nifi_get_controller_service on (\S+)$"), "/controller-services/{0}"),
    (re.compile(r"^nifi_get_parameter_context on (\S+)$"), "/parameter-contexts/{0}"),
    (re.compile(r"^nifi_list_parameter_contexts$"), "/flow/parameter-contexts"),
]


def _read_path(read: str) -> str:
    return next(template.format(*m.groups()) for pattern, template in _READ_PATHS if (m := pattern.match(read)))


async def _run(case: Case, failure: str) -> tuple[dict[str, Any], FakeNiFi]:
    transport = FakeNiFi(case.target, failure)
    client = NiFiClient(settings(), transport=transport)
    configure(client, settings())
    try:
        out = await mcp.call_tool(case.tool, {"params": case.params})
    finally:
        await client.aclose()
    blocks = out[0] if isinstance(out, tuple) else out
    return json.loads("".join(getattr(block, "text", "") for block in blocks)), transport


def _check_spec(case: Case, payload: dict[str, Any], outcome: str) -> None:
    created = payload["created"]
    mine = [item for item in created if item["kind"] == case.spec_kind]
    if outcome == "applied":
        assert created and all(item["outcome"] == "applied" for item in created), created
        return
    before = [item for item in created if item not in mine]
    assert all(item["outcome"] == "applied" and item["id"] for item in before), created
    if outcome == "unknown":
        # The step NiFi may have applied is listed, with an id only if an earlier step recorded one.
        assert mine and mine[-1]["outcome"] == "unknown", created
        assert (mine[-1]["id"] is None) is case.spec_create, created
        return
    # not_applied: a create that did not land is not listed; an enable that did not land leaves the
    # service as created, disabled.
    if case.spec_create:
        assert mine == [], created
    else:
        assert mine and mine[-1]["outcome"] == "applied" and mine[-1]["state"] == "DISABLED", created


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", FAILURES)
@pytest.mark.parametrize("case", CASES, ids=[case.label for case in CASES])
async def test_every_mutation_reports_one_outcome_for_every_failure_class(case: Case, failure: str) -> None:
    payload, transport = await _run(case, failure)
    outcome = _expected(failure)
    assert transport.calls.count(case.target) == 1, transport.calls
    assert payload["outcome"] == outcome, payload
    if outcome == "applied":
        assert payload["status"] == "ok", payload
        assert "hint" not in payload or "Read back" not in payload["hint"], payload
    else:
        assert payload["status"] == "error", payload
    if case.spec_kind:
        _check_spec(case, payload, outcome)
    if outcome == "applied":
        return
    hint = payload["hint"]
    if outcome == "unknown":
        # The read the hint names ran after the failed request and before the hint was written.
        sent_at = transport.calls.index(case.target)
        assert ("GET", _read_path(case.read)) in transport.calls[sent_at + 1 :], transport.calls
        assert f"Read back with {case.read}: " in hint, hint
        assert case.finding in hint, hint
        assert "may have been applied" in payload["error"], payload
    elif failure in _NEVER_SENT:
        assert "Nothing reached NiFi, so the request changed nothing" in hint, hint
        assert "Read back" not in hint
    else:
        assert "NiFi refused the request, so it changed nothing" in hint, hint
        assert "Read back" not in hint


def test_the_table_covers_every_mutation_tool() -> None:
    # A new mutation tool fails here until it has a row.
    mutating = {
        tool.name
        for tool in mcp._tool_manager.list_tools()
        if tool.annotations is not None and tool.annotations.readOnlyHint is False
    }
    assert mutating == {case.tool for case in CASES}


class FailingPoll(FakeNiFi):
    """Every mutation lands; the poll of the async request it started fails with `poll`: an HTTP 500,
    a lost connection, or a finished request that did not complete (a failureReason, or a failed or
    cancelled state, as NiFi words it for a drop request and for an update or replace request)."""

    def __init__(self, poll: str) -> None:
        super().__init__(("-", "-"), "2xx")
        self.poll = poll

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path.removeprefix("/nifi-api")
        if request.method == "GET" and re.search(r"-requests/\w+$", path):
            self.calls.append((request.method, path))
            if self.poll == "500":
                return httpx.Response(500, text="poll failed")
            if self.poll == "ConnectError":
                raise httpx.ConnectError("gone", request=request)
            ended: dict[str, Any] = {"requestId": "r", "id": "r", "complete": True, "finished": True}
            if self.poll == "failureReason":
                ended["failureReason"] = "Failed to drop FlowFiles due to java.io.IOException"
            else:
                ended["state"] = self.poll.removeprefix("state:")
            key = "dropRequest" if "drop-requests" in path else "request"
            return httpx.Response(200, json={key: ended})
        return await super().handle_async_request(request)


_ASYNC = [case for case in CASES if case.label in {"empty queue", "replace flow", "update context"}]
# DropFlowFileState.toString ("Failed", "Canceled by user"); the update and replace requests' states.
_ENDED = ["failureReason", "state:Failed", "state:Canceled by user", "state:FAILURE", "state:CANCELED"]
_POLLS = [(case, poll) for case in _ASYNC for poll in ("500", "ConnectError", *_ENDED)]


@pytest.mark.asyncio
@pytest.mark.parametrize(("case", "poll"), _POLLS, ids=[f"{case.label}-{poll}" for case, poll in _POLLS])
async def test_an_accepted_async_request_that_cannot_be_followed_is_unknown(case: Case, poll: str) -> None:
    # NiFi accepted the drop, parameter update or replace; if it cannot be followed to the
    # end, or ends with a failureReason or a failed or cancelled state part way, it may be applied in
    # part. A drop can fail after dropping some FlowFiles (SwappablePriorityQueue.dropFlowFiles).
    transport = FailingPoll(poll)
    client = NiFiClient(settings(), transport=transport)
    configure(client, settings())
    try:
        out = await mcp.call_tool(case.tool, {"params": case.params})
    finally:
        await client.aclose()
    blocks = out[0] if isinstance(out, tuple) else out
    payload = json.loads("".join(getattr(block, "text", "") for block in blocks))
    assert payload["status"] == "error", payload
    assert payload["outcome"] == "unknown", payload
    assert f"Read back with {case.read}: " in payload["hint"], payload
    assert case.finding in payload["hint"], payload
    assert payload["applied_requests"] == [f"{case.target[0]} {case.target[1]}"], payload


@pytest.mark.parametrize("state", ["Completed successfully", "COMPLETE", None])
def test_a_request_that_completed_is_not_a_failure(state: str | None) -> None:
    request = {"finished": True, "state": state, "failureReason": None}
    assert NiFiClient._async_failure({"dropRequest": request}, "dropRequest", "request") is None


class ReloginFails(FakeNiFi):
    """NIFI_AUTH=jwt: the first login works, the targeted request gets 401 (the token expired), and
    the re-login the client tries then fails."""

    def __init__(self, target: tuple[str, str], relogin: str) -> None:
        super().__init__(target, "401")
        self.relogin = relogin
        self.logins = 0

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path.removeprefix("/nifi-api")
        if path == "/access/token":
            self.calls.append((request.method, path))
            self.logins += 1
            if self.logins == 1:
                return httpx.Response(201, text="tok")
            if self.relogin == "ConnectError":
                raise httpx.ConnectError("idp down", request=request)
            return httpx.Response(int(self.relogin), text="idp down")
        return await super().handle_async_request(request)


def _jwt() -> Any:
    return settings(auth="jwt", bearer_token=None, username="u", password="p")


@pytest.mark.asyncio
@pytest.mark.parametrize("relogin", ["503", "400", "ConnectError"])
@pytest.mark.parametrize("case", CASES, ids=[case.label for case in CASES])
async def test_a_401_whose_relogin_fails_is_not_applied(case: Case, relogin: str) -> None:
    # The 401 is a refusal, logged not_applied before the re-login, so a failed
    # re-login never inherits the outcome of a request that landed earlier in the same call.
    transport = ReloginFails(case.target, relogin)
    client = NiFiClient(_jwt(), transport=transport)
    configure(client, settings())
    try:
        out = await mcp.call_tool(case.tool, {"params": case.params})
    finally:
        await client.aclose()
    blocks = out[0] if isinstance(out, tuple) else out
    payload = json.loads("".join(getattr(block, "text", "") for block in blocks))
    assert transport.calls.count(case.target) == 1, transport.calls
    assert transport.logins == 2, transport.calls
    assert payload["status"] == "error", payload
    assert payload["outcome"] == "not_applied", payload
    target = f"{case.target[0]} {case.target[1]}"
    assert target not in payload.get("applied_requests", []), payload
    assert "HTTP 401, so it changed nothing" in payload["hint"], payload
    if case.spec_kind:
        _check_spec(case, payload, "not_applied")


@pytest.mark.asyncio
async def test_a_created_service_whose_enable_gets_401_and_a_failed_relogin_is_not_applied() -> None:
    # The reported reproduction: the create landed, the enable did not.
    case = next(case for case in CASES if case.label == "enable created service")
    transport = ReloginFails(case.target, "503")
    client = NiFiClient(_jwt(), transport=transport)
    configure(client, settings())
    try:
        out = await mcp.call_tool(case.tool, {"params": case.params})
    finally:
        await client.aclose()
    blocks = out[0] if isinstance(out, tuple) else out
    payload = json.loads("".join(getattr(block, "text", "") for block in blocks))
    assert payload["applied_requests"] == [f"POST /process-groups/{PG}/controller-services"], payload
    assert payload["outcome"] == "not_applied", payload


@pytest.mark.asyncio
@pytest.mark.parametrize("case", CASES, ids=[case.label for case in CASES])
async def test_a_401_whose_relogin_works_is_decided_by_the_replay(case: Case) -> None:
    class ExpiresOnce(FakeNiFi):
        def __init__(self) -> None:
            super().__init__(case.target, "2xx")
            self.refused = False

        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            path = request.url.path.removeprefix("/nifi-api")
            if path == "/access/token":
                return httpx.Response(201, text="tok")
            if (request.method, path) == case.target and not self.refused:
                self.refused = True
                self.calls.append((request.method, path))
                return httpx.Response(401, text="token expired")
            return await super().handle_async_request(request)

    transport = ExpiresOnce()
    client = NiFiClient(_jwt(), transport=transport)
    configure(client, settings())
    try:
        out = await mcp.call_tool(case.tool, {"params": case.params})
    finally:
        await client.aclose()
    blocks = out[0] if isinstance(out, tuple) else out
    payload = json.loads("".join(getattr(block, "text", "") for block in blocks))
    assert transport.calls.count(case.target) == 2, transport.calls
    assert payload["status"] == "ok", payload
    assert payload["outcome"] == "applied", payload


@pytest.mark.asyncio
async def test_a_parameter_update_that_landed_but_cannot_be_read_is_applied() -> None:
    class ReadFailsAfter(FakeNiFi):
        async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
            path = request.url.path.removeprefix("/nifi-api")
            if request.method == "GET" and path == f"/parameter-contexts/{CTX}" and any(
                call[0] == "POST" for call in self.calls
            ):
                return httpx.Response(500, text="read failed")
            return await super().handle_async_request(request)

    client = NiFiClient(settings(), transport=ReadFailsAfter(("-", "-"), "2xx"))
    configure(client, settings())
    try:
        out = await mcp.call_tool(
            "nifi_update_parameter_context", {"params": {"parameter_context_id": CTX, "name": "renamed"}}
        )
    finally:
        await client.aclose()
    blocks = out[0] if isinstance(out, tuple) else out
    payload = json.loads("".join(getattr(block, "text", "") for block in blocks))
    assert payload["status"] == "error"
    assert payload["outcome"] == "applied", payload
    assert f"nifi_get_parameter_context on {CTX}" in payload["hint"]
