"""No error, refusal or hint a tool returns repeats a value the model submitted.

Every tool in the surface is called with a canary in every field it accepts (strings, numbers,
property maps, snapshots, spec objects), then driven down every error path: argument validation,
a spec refusal, a NiFi 4xx body and a 5xx body that repeat the whole request, a timeout, a request
never sent, a mutation with no definite answer (read back from a canvas whose components carry the
canary as their names) and a refused mutation. The canary must not appear in the tool output.

The fake NiFi's bodies are plain sentences that match none of the pattern masks in redaction.py, so
this test holds only through render.Renderer.
"""

from __future__ import annotations

import json
from typing import Any

import pytest
from conftest import settings
from fake_nifi import CHILD, CONN, CTX, NEWPG, NEWSVC, PG, PROC, SVC, FakeNiFi

from nifi_mcp.client import NiFiClient
from nifi_mcp.render import Renderer
from nifi_mcp.server import configure, mcp

CANARY = "zqx7Canary4815"
# Numbers a model can submit (a coordinate, a threshold, a timestamp).
FLOAT = 987654.25
INT = 918273
BUNDLE = {"group": CANARY, "artifact": CANARY, "version": CANARY}

SPEC: dict[str, Any] = {
    "process_group": {"name": CANARY, "comments": CANARY, "parameter_context_id": CTX, "x": FLOAT, "y": FLOAT},
    "objects": [
        {
            "type": "controller_service",
            "name": f"{CANARY}S",
            "service_type": CANARY,
            "properties": {"Password": CANARY, "Plain": CANARY, "Port": INT},
            "bundle": BUNDLE,
        },
        {
            "type": "processor",
            "name": f"{CANARY}P",
            "processor_type": CANARY,
            "properties": {"Service": f"@{CANARY}S", "Plain": CANARY, "Size": FLOAT},
            "auto_terminated": [CANARY],
            "scheduling_period": CANARY,
            "scheduling_strategy": "TIMER_DRIVEN",
            "comments": CANARY,
            "bundle": BUNDLE,
            "x": FLOAT,
            "y": FLOAT,
            "run_every": CANARY,
        },
        {"type": "output_port", "name": f"{CANARY}O", "x": FLOAT, "y": FLOAT},
        {
            "type": "connection",
            "name": f"{CANARY}C",
            "source": f"{CANARY}P",
            "target": f"{CANARY}O",
            "relationships": [CANARY],
            "back_pressure_object_threshold": INT,
            "back_pressure_data_size_threshold": f"{INT} MB",
            "flow_file_expiration": CANARY,
        },
    ],
}

# Valid arguments for every tool, with a canary in every field a model can fill freely.
TOOLS: dict[str, dict[str, Any] | None] = {
    "nifi_about": None,
    "nifi_current_user": None,
    "nifi_list_parameter_contexts": None,
    "nifi_get_flow": {"process_group_id": PG},
    "nifi_search": {"query": CANARY},
    "nifi_list_processor_types": {"type_filter": CANARY, "limit": 5},
    "nifi_list_controller_service_types": {"type_filter": CANARY, "limit": 5},
    "nifi_get_processor_definition": {"group": CANARY, "artifact": CANARY, "version": CANARY, "type_name": CANARY},
    "nifi_get_processor": {"component_id": PROC},
    "nifi_list_controller_services": {"process_group_id": PG},
    "nifi_get_health": {"process_group_id": PG},
    "nifi_get_bulletins": {"after_id": INT, "limit": 5},
    "nifi_list_queue": {"connection_id": CONN},
    "nifi_get_controller_service": {"service_id": SVC},
    "nifi_get_parameter_context": {"parameter_context_id": CTX},
    "nifi_export_flow": {"process_group_id": PG},
    "nifi_create_process_group": {"parent_id": PG, "name": CANARY, "x": FLOAT, "y": FLOAT, "comments": CANARY},
    "nifi_create_processor": {
        "parent_id": PG,
        "processor_type": CANARY,
        "name": CANARY,
        "x": FLOAT,
        "y": FLOAT,
        "properties": {"Password": CANARY, "Plain": CANARY},
        "auto_terminated": [CANARY],
        "bundle": BUNDLE,
        "scheduling_period": CANARY,
        "scheduling_strategy": "TIMER_DRIVEN",
        "comments": CANARY,
    },
    "nifi_update_processor": {
        "processor_id": PROC,
        "name": CANARY,
        "properties": {"Password": CANARY},
        "auto_terminated": [CANARY],
        "scheduling_period": CANARY,
        "comments": CANARY,
        "x": FLOAT,
        "y": FLOAT,
    },
    "nifi_set_run_status": {"component_id": PROC, "state": "RUNNING"},
    "nifi_create_connection": {
        "parent_id": PG,
        "source_id": PROC,
        "source_group_id": PG,
        "destination_id": PROC,
        "destination_group_id": PG,
        "relationships": [CANARY],
        "name": CANARY,
        "back_pressure_object_threshold": INT,
        "back_pressure_data_size_threshold": f"{INT} MB",
        "flow_file_expiration": CANARY,
    },
    "nifi_update_connection": {
        "connection_id": CONN,
        "name": CANARY,
        "back_pressure_object_threshold": INT,
        "back_pressure_data_size_threshold": f"{INT} MB",
        "flow_file_expiration": CANARY,
    },
    "nifi_create_controller_service": {
        "parent_id": PG,
        "service_type": CANARY,
        "name": CANARY,
        "properties": {"Password": CANARY, "Plain": CANARY},
        "bundle": BUNDLE,
    },
    "nifi_update_controller_service": {"service_id": SVC, "name": CANARY, "properties": {"Password": CANARY}},
    "nifi_set_controller_service_state": {"service_id": SVC, "state": "ENABLED"},
    "nifi_schedule_process_group": {"process_group_id": PG, "state": "RUNNING"},
    "nifi_delete_component": {"kind": "processor", "component_id": PROC},
    "nifi_import_flow": {
        "parent_id": PG,
        "group_name": CANARY,
        "snapshot": {"flowContents": {"name": CANARY, "comments": CANARY}},
        "x": FLOAT,
        "y": FLOAT,
    },
    "nifi_replace_flow": {"process_group_id": PG, "snapshot": {"flowContents": {"name": CANARY}}},
    "nifi_apply_flow_spec": {"spec": SPEC, "parent_process_group_id": PG},
    "nifi_layout_process_group": {"process_group_id": PG},
    "nifi_empty_queue": {"connection_id": CONN},
    "nifi_create_parameter_context": {
        "name": CANARY,
        "description": CANARY,
        "parameters": [{"name": CANARY, "value": CANARY, "description": CANARY}],
    },
    "nifi_update_parameter_context": {
        "parameter_context_id": CTX,
        "parameters": [{"name": CANARY, "value": CANARY, "description": CANARY}],
        "remove": [f"{CANARY}-old"],
        "name": CANARY,
        "description": CANARY,
    },
    "nifi_bind_parameter_context": {"process_group_id": CHILD, "parameter_context_id": CTX},
}


def _leaks(text: str) -> list[str]:
    lowered = text.lower()
    return [str(value) for value in (CANARY, FLOAT, INT) if str(value).lower() in lowered]


async def _call(tool: str, arguments: dict[str, Any], transport: FakeNiFi) -> str:
    client = NiFiClient(settings(), transport=transport)
    configure(client, settings())
    try:
        out = await mcp.call_tool(tool, arguments)
    finally:
        await client.aclose()
    blocks = out[0] if isinstance(out, tuple) else out
    return "".join(getattr(block, "text", "") for block in blocks)


def _fields(tool: str) -> list[str]:
    """Every field of the tool's params model (none for a tool without params)."""
    found = mcp._tool_manager.get_tool(tool)
    assert found is not None
    params = found.fn_metadata.arg_model.model_fields.get("params")
    return list(params.annotation.model_fields) if params and params.annotation else []


def _mutating(tool: str) -> bool:
    found = mcp._tool_manager.get_tool(tool)
    return bool(found and found.annotations and found.annotations.readOnlyHint is False)


# (target, failure) for the fake NiFi: every request fails, or only the mutations.
_NIFI_PATHS = {
    "nifi-4xx-body": ("all", "400"),
    "nifi-5xx-body": ("all", "500"),
    # The mutation's own body repeated back: the reads before it succeed.
    "nifi-4xx-body-on-mutation": ("mutations", "400"),
    "nifi-404-body-on-mutation": ("mutations", "404"),
    "nifi-5xx-body-on-mutation": ("mutations", "500"),
    "timeout": ("all", "ReadTimeout"),
    "never-sent": ("all", "ConnectError"),
    "uncertain": ("mutations", "502"),
    "uncertain-timeout": ("mutations", "WriteTimeout"),
    "refused": ("mutations", "409"),
}


def test_every_tool_is_covered() -> None:
    assert set(TOOLS) == {tool.name for tool in mcp._tool_manager.list_tools()}


# A read tool sends no mutation, so it runs only the paths where every request fails.
_TOOL_PATHS = [
    (tool, path) for tool in TOOLS for path, (target, _failure) in _NIFI_PATHS.items()
    if target == "all" or _mutating(tool)
]


@pytest.mark.asyncio
@pytest.mark.parametrize(("tool", "path"), _TOOL_PATHS, ids=[f"{tool}-{path}" for tool, path in _TOOL_PATHS])
async def test_no_nifi_error_path_repeats_a_submitted_value(tool: str, path: str) -> None:
    params = TOOLS[tool]
    target, failure = _NIFI_PATHS[path]
    # Reads answer with components named after the canary, so a read-back that repeated a matched
    # name would leak it.
    transport = FakeNiFi(target, failure, name=CANARY)
    text = await _call(tool, {} if params is None else {"params": params}, transport)
    payload = json.loads(text)
    assert payload["status"] == "error", payload
    assert _leaks(text) == [], text


@pytest.mark.asyncio
@pytest.mark.parametrize("tool", list(TOOLS))
async def test_no_argument_error_repeats_a_submitted_value(tool: str) -> None:
    # The canary in every field the tool's argument model has, whatever its type, plus one it lacks.
    arguments = {"params": {**dict.fromkeys(_fields(tool), CANARY), "extra": CANARY}}
    text = await _call(tool, arguments, FakeNiFi("all", "2xx"))
    assert json.loads(text)["status"] == "error", text
    assert _leaks(text) == [], text


_SPEC_STEPS = [
    ("POST", f"/process-groups/{PG}/process-groups"),
    ("POST", f"/process-groups/{NEWPG}/controller-services"),
    ("PUT", f"/controller-services/{NEWSVC}/run-status"),
    ("POST", f"/process-groups/{NEWPG}/processors"),
    ("POST", f"/process-groups/{NEWPG}/output-ports"),
    ("POST", f"/process-groups/{NEWPG}/connections"),
]


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["400", "409", "500", "502", "ReadTimeout", "ConnectError"])
@pytest.mark.parametrize("step", _SPEC_STEPS, ids=[f"{m} {p.split('/')[-1]}" for m, p in _SPEC_STEPS])
async def test_no_spec_step_failure_repeats_a_submitted_value(step: tuple[str, str], failure: str) -> None:
    # Each spec step failing after the ones before it landed: created[], hint and warnings included.
    transport = FakeNiFi(step, failure, name=CANARY)
    text = await _call("nifi_apply_flow_spec", {"params": TOOLS["nifi_apply_flow_spec"]}, transport)
    payload = json.loads(text)
    assert payload["status"] == "error", payload
    assert step in transport.calls
    assert _leaks(text) == [], text


def _with(path: list[Any], value: Any) -> dict[str, Any]:
    spec = json.loads(json.dumps(SPEC))
    holder: Any = spec
    for key in path[:-1]:
        holder = holder[key]
    holder[path[-1]] = value
    return spec


_REFUSALS = {
    "unknown top-level key": {**SPEC, CANARY: CANARY},
    "unknown process_group key": _with(["process_group", "parameter_context"], CANARY),
    "parent id is not an id": {**SPEC, "parent_process_group_id": CANARY},
    "context id is not an id": _with(["process_group", "parameter_context_id"], CANARY),
    "group x is not a number": _with(["process_group", "x"], CANARY),
    "processor y is a list": _with(["objects", 1, "y"], [CANARY]),
    "missing processor type": _with(["objects", 1, "processor_type"], None),
    "unknown connection source": _with(["objects", 3, "source"], f"{CANARY}-nope"),
    "unknown service reference": _with(["objects", 1, "properties", "Service"], f"@{CANARY}-nope"),
    "duplicate name": _with(["objects", 2, "name"], f"{CANARY}P"),
    "relationships hold a dict": _with(["objects", 3, "relationships"], {CANARY: CANARY}),
    "bad data size": _with(["objects", 3, "back_pressure_data_size_threshold"], CANARY),
    "negative threshold": _with(["objects", 3, "back_pressure_object_threshold"], -INT),
    "expiration is a list": _with(["objects", 3, "flow_file_expiration"], [CANARY]),
    "property is an object": _with(["objects", 1, "properties", "Plain"], {CANARY: CANARY}),
    "object is a string": _with(["objects", 0], CANARY),
    "objects is a string": {**SPEC, "objects": CANARY},
}


@pytest.mark.asyncio
@pytest.mark.parametrize("spec", list(_REFUSALS.values()), ids=list(_REFUSALS))
async def test_no_spec_refusal_repeats_a_submitted_value(spec: dict[str, Any]) -> None:
    transport = FakeNiFi("all", "2xx", name=CANARY)
    text = await _call("nifi_apply_flow_spec", {"params": {"spec": spec}}, transport)
    payload = json.loads(text)
    assert payload["status"] == "error", payload
    assert payload["cause"] == "spec", payload
    assert [call for call in transport.calls if call[0] != "GET"] == [], transport.calls
    assert _leaks(text) == [], text


def test_the_renderer_masks_a_value_however_it_is_quoted() -> None:
    value = 'se"cr<et>\\one\nline two'
    renderer = Renderer({"field": value})
    for quoted in (value, json.dumps(value), '"' + value.replace('"', '&quot;') + '"'):
        rendered = renderer.foreign(f"NiFi says {quoted} is wrong")
        assert "cr<et>" not in rendered and "line two" not in rendered, rendered


def test_the_renderer_keeps_its_own_words_and_ids() -> None:
    # A submitted value that is part of a tool name, or that is an id, does not wreck the hint.
    renderer = Renderer({"name": "flow", "id": PG, "state": "RUNNING"})
    text = f"Read back with nifi_get_flow on {PG}: processor states in {PG}: RUNNING 1"
    assert renderer.own(text) == text
