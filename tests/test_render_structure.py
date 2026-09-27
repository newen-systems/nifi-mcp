"""Both directions: a mask never touches what the model needs, and never misses a value.

The structural fields of a result (status, outcome, cause, type, kind, state, ref) and every NiFi id
are never masked, whatever the model submitted: every mutating tool is called with each of those
fields' words as its values, and every numeric argument is matched against an id holding the same
digits. A submitted value in prose or in an echoed field always is masked: every read and result path
that can carry a NiFi validation sentence or a property map is driven with a value NiFi reports
invalid and repeats in its explanation.
"""

from __future__ import annotations

import copy
import json
import re
from typing import Any

import httpx
import pytest
from conftest import settings
from fake_nifi import CONN, FakeNiFi
from test_error_rendering import CANARY, TOOLS

from nifi_mcp.client import NiFiClient
from nifi_mcp.compact import compact_bulletin, compact_processor
from nifi_mcp.redaction import REDACT_MARK, versioned_id
from nifi_mcp.render import STRUCTURAL_KEYS, Renderer
from nifi_mcp.server import _dump, configure, mcp


async def _call(tool: str, arguments: dict[str, Any], transport: httpx.AsyncBaseTransport) -> str:
    client = NiFiClient(settings(), transport=transport)
    configure(client, settings())
    try:
        out = await mcp.call_tool(tool, arguments)
    finally:
        await client.aclose()
    blocks = out[0] if isinstance(out, tuple) else out
    return "".join(getattr(block, "text", "") for block in blocks)


def _mutating(tool: str) -> bool:
    found = mcp._tool_manager.get_tool(tool)
    return bool(found and found.annotations and found.annotations.readOnlyHint is False)


def _replace(value: Any, old: str, new: str) -> Any:
    if isinstance(value, dict):
        return {key: _replace(item, old, new) for key, item in value.items()}
    if isinstance(value, list):
        return [_replace(item, old, new) for item in value]
    if isinstance(value, str):
        return value.replace(old, new)
    return value


def _structural(payload: Any) -> list[tuple[str, Any]]:
    """Every structural field anywhere in a payload."""
    found: list[tuple[str, Any]] = []
    if isinstance(payload, dict):
        for key, item in payload.items():
            if key in STRUCTURAL_KEYS and not isinstance(item, dict | list):
                found.append((key, item))
            found += _structural(item)
    elif isinstance(payload, list):
        for item in payload:
            found += _structural(item)
    return found


# The words the structural fields take: status, outcome, cause, the exception types, and a created[]
# item's kind, state and ref.
WORDS = [
    "error",
    "ok",
    "applied",
    "not_applied",
    "unknown",
    "nifi",
    "spec",
    "NiFiError",
    "NiFiUncertainError",
    "NiFiConflictError",
    "NiFiTimeoutError",
    "process_group",
    "controller_service",
    "objects[0]",
    "DISABLED",
]
MUTATING = [tool for tool in TOOLS if _mutating(tool)]
# (failure, outcome) for every mutation the tool sends: refused, or no definite answer.
FAILURES = [("409", "not_applied"), ("502", "unknown")]


@pytest.mark.asyncio
@pytest.mark.parametrize(("failure", "outcome"), FAILURES)
@pytest.mark.parametrize("word", WORDS)
@pytest.mark.parametrize("tool", MUTATING)
async def test_no_structural_field_is_masked_by_a_submitted_word(
    tool: str, word: str, failure: str, outcome: str
) -> None:
    # Every mutating tool, every value it takes set to the word.
    params = _replace(copy.deepcopy(TOOLS[tool]), CANARY, word)
    payload = json.loads(await _call(tool, {"params": params}, FakeNiFi("mutations", failure)))
    assert payload["status"] == "error", payload
    assert payload["outcome"] == outcome, payload
    if tool == "nifi_apply_flow_spec":
        assert payload["cause"] == "nifi", payload
    else:
        assert re.fullmatch(r"NiFi\w*Error", payload["type"]), payload
    fields = _structural(payload)
    assert all(REDACT_MARK not in str(value) for _key, value in fields), fields


# A NiFi id holding the digits of each numeric argument, as a lab NiFi's ids do (e208aeb8-01a0-1000-...).
def _id_with(number: int) -> str:
    head = f"a{number}".ljust(8, "b")[:8]
    return f"{head}-0000-4000-8000-000000000000"


X, Y, THRESHOLD, AFTER, LIMIT = 1272, 3456, 10000, 918273, 100
NUMERIC: dict[str, tuple[dict[str, Any], list[int]]] = {
    "nifi_create_process_group": ({"parent_id": "{id}", "name": "g", "x": X, "y": Y}, [X, Y]),
    "nifi_create_processor": (
        {"parent_id": "{id}", "processor_type": "x.P", "name": "P", "x": X, "y": Y}, [X, Y]
    ),
    "nifi_update_processor": ({"processor_id": "{id}", "x": X, "y": Y}, [X, Y]),
    "nifi_import_flow": ({"parent_id": "{id}", "group_name": "g", "snapshot": {}, "x": X, "y": Y}, [X, Y]),
    "nifi_create_connection": (
        {
            "parent_id": "{id}",
            "source_id": "{id}",
            "source_group_id": "{id}",
            "destination_id": "{id}",
            "destination_group_id": "{id}",
            "relationships": ["success"],
            "back_pressure_object_threshold": THRESHOLD,
            "back_pressure_data_size_threshold": f"{THRESHOLD} MB",
        },
        [THRESHOLD],
    ),
    "nifi_update_connection": (
        {
            "connection_id": "{id}",
            "back_pressure_object_threshold": THRESHOLD,
            "back_pressure_data_size_threshold": f"{THRESHOLD} MB",
        },
        [THRESHOLD],
    ),
    "nifi_apply_flow_spec": (
        {
            "parent_process_group_id": "{id}",
            "spec": {
                "layout": "manual",
                "process_group": {"name": "g", "x": X, "y": Y},
                "objects": [
                    {"type": "processor", "name": "P", "processor_type": "x.P", "x": X, "y": Y},
                    {"type": "output_port", "name": "out", "x": X, "y": Y + 400},
                    {
                        "type": "connection",
                        "source": "P",
                        "target": "out",
                        "relationships": ["success"],
                        "back_pressure_object_threshold": THRESHOLD,
                    },
                ],
            },
        },
        [X, Y, THRESHOLD],
    ),
}
_NUMERIC_CASES = [(tool, number) for tool, (_params, numbers) in NUMERIC.items() for number in numbers]


def _fill(value: Any, nid: str) -> Any:
    if isinstance(value, dict):
        return {key: _fill(item, nid) for key, item in value.items()}
    if isinstance(value, list):
        return [_fill(item, nid) for item in value]
    return nid if value == "{id}" else value


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["502", "ReadTimeout", "409"])
@pytest.mark.parametrize(("tool", "number"), _NUMERIC_CASES, ids=[f"{t}-{n}" for t, n in _NUMERIC_CASES])
async def test_a_submitted_number_never_alters_an_id(tool: str, number: int, failure: str) -> None:
    # The id is in the error, the hint's read target, process_group_id
    # and applied_requests; the number the tool was sent is also in it.
    nid = _id_with(number)
    params = _fill(NUMERIC[tool][0], nid)
    text = await _call(tool, {"params": params}, FakeNiFi("mutations", failure))
    payload = json.loads(text)
    assert payload["status"] == "error", payload
    assert nid in text, text
    assert f"a{REDACT_MARK}" not in text, text
    if tool == "nifi_apply_flow_spec":
        assert payload["process_group_id"] in {nid, None} or payload["outcome"] != "unknown", payload
    if payload["outcome"] == "unknown" and tool != "nifi_apply_flow_spec":
        assert nid in payload["hint"], payload["hint"]


@pytest.mark.asyncio
async def test_a_spec_coordinate_keeps_the_parent_id_in_the_result_and_hint() -> None:
    # The reported reproduction.
    group = "3f1272ab-0000-4000-8000-000000000000"
    spec = {
        "parent_process_group_id": group,
        "layout": "manual",
        "objects": [{"type": "processor", "name": "P", "processor_type": "x.P", "x": 1272, "y": 0}],
    }
    transport = FakeNiFi(("POST", f"/process-groups/{group}/processors"), "502")
    text = await _call("nifi_apply_flow_spec", {"params": {"spec": spec}}, transport)
    payload = json.loads(text)
    assert payload["outcome"] == "unknown", payload
    assert payload["process_group_id"] == group, payload
    assert group in payload["hint"], payload["hint"]


class _BulletinIdBody(httpx.AsyncBaseTransport):
    """The bulletin board read fails with a body naming a component whose id holds the cursor's digits."""

    def __init__(self, nid: str) -> None:
        self.nid = nid

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text=f"bulletin source {self.nid} failed after {request.url.query.decode()}")


@pytest.mark.asyncio
@pytest.mark.parametrize(("field", "number"), [("after_id", AFTER), ("limit", LIMIT)])
async def test_a_bulletin_cursor_or_limit_never_alters_an_id(field: str, number: int) -> None:
    nid = _id_with(number)
    text = await _call("nifi_get_bulletins", {"params": {field: number}}, _BulletinIdBody(nid))
    assert json.loads(text)["status"] == "error", text
    assert nid in text, text


def test_the_renderer_keeps_an_id_that_holds_a_submitted_number_or_value() -> None:
    nid = "0a10000b-0000-4000-8000-00000000ffff"
    renderer = Renderer({"threshold": 10000, "name": "0a10000b", "x": 1272.0})
    for render in (renderer.own, renderer.foreign):
        assert render(f"connection {nid} at 10000") == f"connection {nid} at {REDACT_MARK}"
    # A value holding an id is still masked whole.
    wide = Renderer({"name": f"secret {nid}"})
    assert wide.foreign(f"NiFi says secret {nid}") == f"NiFi says {REDACT_MARK}"


# ---- a value NiFi reports invalid and repeats: every read and result path ----

PROC_ID = "e208aeb8-01a0-1000-ffff-ffffcca6a21b"
SVC_ID = "e208aeb8-01a0-1000-ffff-ffffcca6a21c"
PG_ID = "e208aeb8-01a0-1000-ffff-ffffcca6a21d"
RPG_ID = "e208aeb8-01a0-1000-ffff-ffffcca6a21e"
CHILD_PROC_ID = "e208aeb8-01a0-1000-ffff-ffffcca6a21f"
# The identifier the snapshot gives the service: its versionedComponentId, not one derived from its id.
SVC_VERSIONED = "5ad7c0de-0000-3000-8000-00000000000c"
KEPT = "10 sec kept value"
SENTENCE = (
    f"'Record Reader' validated against '{CANARY}' is invalid because "
    f"Invalid Controller Service: {CANARY} is not a valid Controller Service Identifier"
)
# Every non-property field NiFi validates with a subject of its own (redaction._INVALID_FIELDS).
PERIOD = f"{CANARY}-period"
RUN_SCHEDULE = (
    f"'Run Schedule' validated against '{PERIOD}' is invalid because Scheduling Period is not a valid time duration"
)
NIC = f"{CANARY}-nic"
NETWORK = (
    f"'Network Interface Name' validated against '{NIC}' is invalid because "
    f"Could not obtain Network Interface with name {NIC}"
)


def _component(cid: str, kind: str) -> dict[str, Any]:
    properties = {"record-reader": CANARY, "Keep": KEPT}
    descriptors = {
        "record-reader": {"name": "record-reader", "displayName": "Record Reader"},
        "Keep": {"name": "Keep", "displayName": "Keep"},
    }
    component: dict[str, Any] = {
        "id": cid,
        "parentGroupId": PG_ID,
        "name": "Fetch",
        "type": f"x.{kind}",
        "state": "STOPPED" if kind == "Processor" else "DISABLED",
        "validationStatus": "INVALID",
        "validationErrors": [SENTENCE, RUN_SCHEDULE] if kind == "Processor" else [SENTENCE],
        "position": {"x": 0.0, "y": 0.0},
    }
    if kind == "Processor":
        component["config"] = {"properties": properties, "descriptors": descriptors, "schedulingPeriod": PERIOD}
    else:
        component.update(properties=properties, descriptors=descriptors, versionedComponentId=SVC_VERSIONED)
    return {"id": cid, "revision": {"version": 3}, "component": component}


def _remote(rid: str = RPG_ID) -> dict[str, Any]:
    component = {
        "id": rid, "parentGroupId": PG_ID, "name": "Remote", "targetUris": "https://nifi.example",
        "localNetworkInterface": NIC, "validationErrors": [NETWORK], "position": {"x": 0.0, "y": 600.0},
    }
    return {"id": rid, "revision": {"version": 1}, "component": component}


def _snapshot() -> dict[str, Any]:
    """A /download snapshot: the same components as NiFi exports them, without validation state, one
    of them in a child group, each under the identifier NiFi gives it (redaction.versioned_id)."""
    processor = {"identifier": versioned_id({"id": PROC_ID}), "name": "Fetch", "type": "x.Processor",
                 "schedulingPeriod": PERIOD, "properties": {"record-reader": CANARY, "Keep": KEPT}}
    service = {"identifier": SVC_VERSIONED, "name": "S", "type": "x.Service", "properties": {"record-reader": CANARY}}
    remote = {"identifier": versioned_id({"id": RPG_ID}), "name": "Remote", "localNetworkInterface": NIC}
    child_processor = {**processor, "identifier": versioned_id({"id": CHILD_PROC_ID})}
    child = {"identifier": "v4", "name": "child", "processors": [child_processor]}
    contents = {"identifier": "v0", "processors": [processor], "controllerServices": [service],
                "remoteProcessGroups": [remote], "processGroups": [child]}
    return {"flowContents": contents}


def _status(remote_ids: list[str]) -> dict[str, Any]:
    remotes = [{"id": rid, "remoteProcessGroupStatusSnapshot": {"id": rid}} for rid in remote_ids]
    child = {"id": "c", "processGroupStatusSnapshot": {"remoteProcessGroupStatusSnapshots": remotes}}
    return {"processGroupStatus": {"aggregateSnapshot": {"processGroupStatusSnapshots": [child]}}}


class InvalidNiFi(httpx.AsyncBaseTransport):
    """Every processor, controller service and remote process group is INVALID with the canary as a
    value NiFi repeats in its validation sentence; every bulletin repeats that sentence."""

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path.removeprefix("/nifi-api")
        processor, service = _component(PROC_ID, "Processor"), _component(SVC_ID, "Service")
        if path == f"/process-groups/{PG_ID}/download":
            return httpx.Response(200, json=_snapshot())
        if path == f"/process-groups/{PG_ID}/processors" and request.method == "GET":
            return httpx.Response(200, json={"processors": [processor, _component(CHILD_PROC_ID, "Processor")]})
        if path == f"/flow/process-groups/{PG_ID}/status":
            return httpx.Response(200, json=_status([RPG_ID]))
        if path == f"/remote-process-groups/{RPG_ID}":
            return httpx.Response(200, json=_remote())
        if path == "/flow/bulletin-board":
            item = {"id": 7, "bulletin": {"id": 7, "level": "ERROR", "sourceId": PROC_ID, "message": SENTENCE}}
            return httpx.Response(200, json={"bulletinBoard": {"bulletins": [item]}})
        if path.endswith("/controller-services") and request.method == "GET":
            return httpx.Response(200, json={"controllerServices": [service]})
        if path.startswith("/flow/process-groups/"):
            flow = {"processors": [processor], "controllerServices": [service], "remoteProcessGroups": [_remote()]}
            return httpx.Response(200, json={"processGroupFlow": {"id": PG_ID, "flow": flow}})
        if "/processors" in path:
            return httpx.Response(201 if request.method == "POST" else 200, json=processor)
        if "/controller-services" in path:
            return httpx.Response(201 if request.method == "POST" else 200, json=service)
        return httpx.Response(404, json={"message": "unhandled"})


READS = [
    ("nifi_get_processor", {"component_id": PROC_ID}),
    ("nifi_get_controller_service", {"service_id": SVC_ID}),
    ("nifi_get_flow", {"process_group_id": PG_ID}),
    ("nifi_get_health", {"process_group_id": PG_ID}),
    ("nifi_list_controller_services", {"process_group_id": PG_ID}),
    ("nifi_get_bulletins", {}),
    ("nifi_export_flow", {"process_group_id": PG_ID}),
    ("nifi_export_flow", {"process_group_id": PG_ID, "include_services": True}),
]
RESULTS = [
    ("nifi_create_processor", {"parent_id": PG_ID, "processor_type": "x.P", "name": "Fetch", "x": 0, "y": 0}),
    ("nifi_update_processor", {"processor_id": PROC_ID, "name": "Fetch"}),
    ("nifi_create_controller_service", {"parent_id": PG_ID, "service_type": "x.S", "name": "S", "enable": False}),
    ("nifi_update_controller_service", {"service_id": SVC_ID, "name": "S"}),
    ("nifi_delete_component", {"kind": "processor", "component_id": PROC_ID}),
    ("nifi_delete_component", {"kind": "controller_service", "component_id": SVC_ID}),
]
_NO_VERBOSE = {"nifi_delete_component", "nifi_export_flow"}
_PATHS = [
    (tool, {**params, **({"verbose": verbose} if tool not in _NO_VERBOSE else {})})
    for tool, params in READS + RESULTS
    for verbose in ((False,) if tool in _NO_VERBOSE else (False, True))
]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("tool", "params"), _PATHS, ids=[f"{t}-{'verbose' if p.get('verbose') else 'compact'}" for t, p in _PATHS]
)
async def test_no_read_or_result_repeats_a_value_nifi_reports_invalid(tool: str, params: dict[str, Any]) -> None:
    # The quoted copy, the copy in the explanation, and the property value;
    # The run schedule and network interface NiFi names with their own subject.
    text = await _call(tool, {"params": params}, InvalidNiFi())
    payload = json.loads(text)
    assert payload.get("status") != "error", payload
    assert CANARY not in text, text
    if tool in {"nifi_get_processor", "nifi_get_controller_service"} and params.get("verbose"):
        # The read stays useful: the reason, and every value NiFi accepts.
        assert KEPT in text and "is invalid because" in text, text
    if tool == "nifi_export_flow":
        # The export masks what the live group reports invalid, in
        # every group of the snapshot, and keeps every other value.
        assert KEPT in text and text.count(REDACT_MARK) >= 6, text


# A valid twin of every invalid component, holding the same values. A processor in the
# parent and one in the child group (a property and the run schedule), a controller service (a property)
# and a remote process group (the network interface).
TWIN_PROC_ID = "e208aeb8-01a0-1000-ffff-ffffcca6a220"
TWIN_CHILD_PROC_ID = "e208aeb8-01a0-1000-ffff-ffffcca6a221"
TWIN_SVC_ID = "e208aeb8-01a0-1000-ffff-ffffcca6a222"
TWIN_RPG_ID = "e208aeb8-01a0-1000-ffff-ffffcca6a223"


def _valid(entity: dict[str, Any], cid: str) -> dict[str, Any]:
    twin = copy.deepcopy(entity)
    twin["id"] = twin["component"]["id"] = cid
    twin["component"].update(name="Twin", validationStatus="VALID", validationErrors=None)
    twin["component"].pop("versionedComponentId", None)
    return twin


def _twin_snapshot() -> dict[str, Any]:
    snapshot = _snapshot()
    contents = snapshot["flowContents"]
    twin = {**contents["processors"][0], "identifier": versioned_id({"id": TWIN_PROC_ID}), "name": "Twin"}
    contents["processors"].append(twin)
    child = contents["processGroups"][0]
    child["processors"].append({**twin, "identifier": versioned_id({"id": TWIN_CHILD_PROC_ID})})
    contents["controllerServices"].append(
        {**contents["controllerServices"][0], "identifier": versioned_id({"id": TWIN_SVC_ID}), "name": "Twin"}
    )
    contents["remoteProcessGroups"].append(
        {**contents["remoteProcessGroups"][0], "identifier": versioned_id({"id": TWIN_RPG_ID}), "name": "Twin"}
    )
    return snapshot


class TwinNiFi(InvalidNiFi):
    """InvalidNiFi with a VALID twin of every component, holding the same values."""

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path.removeprefix("/nifi-api")
        if path == f"/process-groups/{PG_ID}/download":
            return httpx.Response(200, json=_twin_snapshot())
        if path == f"/process-groups/{PG_ID}/processors":
            processors = [_component(cid, "Processor") for cid in (PROC_ID, CHILD_PROC_ID)]
            twins = [_valid(processors[0], cid) for cid in (TWIN_PROC_ID, TWIN_CHILD_PROC_ID)]
            return httpx.Response(200, json={"processors": processors + twins})
        if path.endswith("/controller-services"):
            service = _component(SVC_ID, "Service")
            return httpx.Response(200, json={"controllerServices": [service, _valid(service, TWIN_SVC_ID)]})
        if path == f"/flow/process-groups/{PG_ID}/status":
            return httpx.Response(200, json=_status([RPG_ID, TWIN_RPG_ID]))
        if path == f"/remote-process-groups/{TWIN_RPG_ID}":
            return httpx.Response(200, json=_valid(_remote(), TWIN_RPG_ID))
        return await super().handle_async_request(request)


@pytest.mark.asyncio
@pytest.mark.parametrize("include_services", [False, True])
async def test_the_export_masks_a_value_only_in_the_component_it_is_invalid_on(include_services: bool) -> None:
    # Every sibling path of the export mask (processor property and run schedule, in the
    # parent and in a child group; controller service property, matched by versionedComponentId; remote
    # process group interface), each next to a valid twin that holds the same value.
    params = {"process_group_id": PG_ID, "include_services": include_services}
    text = await _call("nifi_export_flow", {"params": params}, TwinNiFi())
    contents = json.loads(text)["flowContents"]
    child = contents["processGroups"][0]
    groups = {
        "processor": contents["processors"],
        "child processor": child["processors"],
        "service": contents["controllerServices"],
        "remote": contents["remoteProcessGroups"],
    }
    fields = {
        "processor": ("record-reader", "schedulingPeriod"),
        "child processor": ("record-reader", "schedulingPeriod"),
        "service": ("record-reader",),
        "remote": ("localNetworkInterface",),
    }
    for kind, (invalid, twin) in groups.items():
        assert twin["name"] == "Twin", (kind, twin)
        for field in fields[kind]:
            got = [item["properties"][field] if field == "record-reader" else item[field] for item in (invalid, twin)]
            assert got[0] == REDACT_MARK, (kind, field, invalid)
            assert CANARY in got[1], (kind, field, twin)
    assert contents["processors"][1]["properties"]["Keep"] == KEPT


# Importing one flow twice gives each pair of copies the same versionedComponentId
# (StandardVersionedComponentSynchronizer sets it to the snapshot identifier), and /download gives both
# copies that identifier. Each copy is INVALID with a value of its own.
SHARED_VID = "5ad7c0de-0000-3000-8000-0000000000aa"
COPY_IDS = ("e208aeb8-01a0-1000-ffff-ffffcca6a301", "e208aeb8-01a0-1000-ffff-ffffcca6a302")
COPY_VALUES = {cid: f"{CANARY}-copy{i}" for i, cid in enumerate(COPY_IDS)}


def _copy(kind: str, cid: str) -> dict[str, Any]:
    value = COPY_VALUES[cid]
    if kind == "remote":
        entity = _remote(cid)
        entity["component"].update(
            localNetworkInterface=value,
            validationErrors=[f"'Network Interface Name' validated against '{value}' is invalid because bad"],
        )
    else:
        entity = _component(cid, "Processor" if kind == "processor" else "Service")
        component = entity["component"]
        holder = component["config"] if kind == "processor" else component
        holder["properties"]["record-reader"] = value
        component["validationErrors"] = [f"'Record Reader' validated against '{value}' is invalid because bad"]
    entity["component"]["versionedComponentId"] = SHARED_VID
    return entity


def _snap_copy(kind: str, cid: str) -> dict[str, Any]:
    if kind == "remote":
        return {"identifier": SHARED_VID, "name": "Remote", "localNetworkInterface": COPY_VALUES[cid]}
    return {"identifier": SHARED_VID, "name": "Fetch", "type": "x.P", "properties": {"record-reader": COPY_VALUES[cid]}}


class SharedIdNiFi(httpx.AsyncBaseTransport):
    """Two imported copies of one component of `kind`, each in a group of its own."""

    def __init__(self, kind: str) -> None:
        self.kind = kind

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path.removeprefix("/nifi-api")
        kind = self.kind
        live = {kind: [_copy(kind, cid) for cid in COPY_IDS]}
        if path == f"/process-groups/{PG_ID}/download":
            key = {"processor": "processors", "service": "controllerServices", "remote": "remoteProcessGroups"}[kind]
            groups = [{"identifier": f"g{i}", "name": f"copy{i}", key: [_snap_copy(kind, cid)]}
                      for i, cid in enumerate(COPY_IDS)]
            return httpx.Response(200, json={"flowContents": {"identifier": "root", "processGroups": groups}})
        if path == f"/process-groups/{PG_ID}/processors":
            return httpx.Response(200, json={"processors": live.get("processor", [])})
        if path.endswith("/controller-services"):
            return httpx.Response(200, json={"controllerServices": live.get("service", [])})
        if path == f"/flow/process-groups/{PG_ID}/status":
            return httpx.Response(200, json=_status(list(COPY_IDS) if kind == "remote" else []))
        if path.startswith("/remote-process-groups/"):
            return httpx.Response(200, json=_copy("remote", path.rsplit("/", 1)[1]))
        return httpx.Response(404, json={"message": "unhandled"})


@pytest.mark.asyncio
@pytest.mark.parametrize("include_services", [False, True])
@pytest.mark.parametrize("kind", ["processor", "service", "remote"])
async def test_each_copy_that_shares_a_snapshot_identifier_keeps_its_invalid_value_masked(
    kind: str, include_services: bool
) -> None:
    params = {"process_group_id": PG_ID, "include_services": include_services}
    text = await _call("nifi_export_flow", {"params": params}, SharedIdNiFi(kind))
    assert json.loads(text).get("status") != "error", text
    for value in COPY_VALUES.values():
        assert value not in text, text
    assert text.count(REDACT_MARK) == 2, text


def test_the_explanation_copy_is_masked_in_compact_views() -> None:
    # The reported reproduction.
    entity = {"id": CONN, "component": {"id": CONN, "name": "Fetch", "validationErrors": [SENTENCE]}}
    bulletin = {"bulletin": {"id": 1, "level": "ERROR", "message": SENTENCE}}
    rendered = _dump({"processor": compact_processor(entity), "bulletins": [compact_bulletin(bulletin)]})
    assert CANARY not in rendered, rendered
    assert "is not a valid Controller Service Identifier" in rendered, rendered


def test_every_copy_is_masked_when_a_body_lists_several_sentences() -> None:
    body = (
        "Unable to start: ['A' validated against 'first-value' is invalid because first-value is bad, "
        "'B' validated against 'second-value' is invalid because second-value is bad]"
    )
    rendered = Renderer().foreign(body)
    assert "first-value" not in rendered and "second-value" not in rendered, rendered


@pytest.mark.asyncio
async def test_a_delete_confirms_without_the_entity() -> None:
    params = {"kind": "processor", "component_id": PROC_ID}
    text = await _call("nifi_delete_component", {"params": params}, InvalidNiFi())
    assert json.loads(text) == {
        "status": "ok",
        "deleted": PROC_ID,
        "kind": "processor",
        "revision": 3,
        "outcome": "applied",
    }
