"""A setting NiFi refuses is named in the hint, and never gets the conflict recipe.

NiFi checks a processor's scheduling, penalty and yield durations, concurrent tasks and auto-terminated
relationships, and a connection's expiration and queue size, before it applies an update
(StandardProcessorDAO and StandardConnectionDAO validateProposedConfiguration). A single node answers
400 (ValidationExceptionMapper); a cluster answers 409, "Node ... is unable to fulfill this request due
to: ..." (ThreadPoolRequestReplicator). Either way NiFi changed nothing and there is nothing to stop,
refresh or empty: the hint names the field, never the value.
"""

from __future__ import annotations

import json
from typing import Any

import httpx
import pytest
from conftest import settings
from fake_nifi import CONN, NEWPG, PG, PORT_OUT, PROC, SVC, FakeNiFi

from nifi_mcp.client import NiFiClient
from nifi_mcp.server import configure, mcp

CANARY = "canary-r9-xyz"
NODE = "Node nifi-1.nifi.nifi:8443 is unable to fulfill this request due to: "
RECIPE = "Stop running processors"

# (NiFi sentence, the field the hint names). The processor fields NiFi validates this way; the tools
# expose scheduling_period, scheduling_strategy and auto_terminated, and a snapshot or a future tool
# can set the rest.
PROCESSOR_SENTENCES = [
    ("Scheduling period is not a valid time duration (ie 30 sec, 5 min)", "scheduling_period"),
    (f"Scheduling Period '{CANARY}' is not a valid cron expression: bad", "scheduling_period"),
    ("Scheduling strategy: Value must be one of [TIMER_DRIVEN, CRON_DRIVEN]", "scheduling_strategy"),
    ("Penalty duration is not a valid time duration (ie 30 sec, 5 min)", "penalty_duration"),
    ("Yield duration is not a valid time duration (ie 30 sec, 5 min)", "yield_duration"),
    ("Concurrent tasks must be greater than 0.", "concurrent_tasks"),
    (
        f"Cannot automatically terminate '{CANARY}' relationship because a Connection already exists",
        "auto_terminated",
    ),
    ("Bulletin level: Value must be one of [TRACE, DEBUG, INFO, WARN, ERROR, NONE]", "bulletin_level"),
    ("Execution node: Value must be one of [ALL, PRIMARY]", "execution_node"),
]
# AbstractComponentNode.verifyCanUpdateProperties, which the processor and controller service DAOs
# record in validateProposedConfiguration before they change anything.
PROPERTY_SENTENCES = [
    (
        "The property 'Password' is a sensitive property so it can reference a Parameter only if there is no "
        "other context around the value. For instance, the value '#{abc}' is allowed but 'password#{abc}' "
        "is not allowed.",
        "property 'Password'",
    ),
    (
        "The property 'Password' cannot reference more than one Parameter because it is a sensitive property.",
        "property 'Password'",
    ),
]
CONNECTION_SENTENCES = [
    ("Flow file expiration is not a valid time duration (ie 30 sec, 5 min)", "flow_file_expiration"),
    ("Max queue size must be a non-negative integer", "back_pressure_object_threshold"),
    ("The label index must be positive.", "label_index"),
    ("When the destination is a remote input port its group id is required.", "destination_group_id"),
    ("Unable to find the specified remote process group.", "source_group_id or destination_group_id"),
    ("Unable to find the specified destination.", "destination_id"),
]

_UPDATE_PROCESSOR = ("nifi_update_processor", {"processor_id": PROC, "scheduling_period": CANARY},
                     ("PUT", f"/processors/{PROC}"))
_CREATE_PROCESSOR = (
    "nifi_create_processor",
    {"parent_id": PG, "processor_type": "x.P", "name": "P", "scheduling_period": CANARY, "x": 0, "y": 0},
    ("POST", f"/process-groups/{PG}/processors"),
)
_UPDATE_CONNECTION = ("nifi_update_connection", {"connection_id": CONN, "flow_file_expiration": CANARY},
                      ("PUT", f"/connections/{CONN}"))

_CREATE_CONNECTION = (
    "nifi_create_connection",
    {
        "parent_id": PG, "source_id": PROC, "source_group_id": PG, "destination_id": PORT_OUT,
        "destination_group_id": PG, "destination_type": "OUTPUT_PORT", "relationships": ["success"],
        "flow_file_expiration": CANARY,
    },
    ("POST", f"/process-groups/{PG}/connections"),
)
_UPDATE_SERVICE = ("nifi_update_controller_service", {"service_id": SVC, "properties": {"Password": CANARY}},
                   ("PUT", f"/controller-services/{SVC}"))
_CREATE_SERVICE = (
    "nifi_create_controller_service",
    {"parent_id": PG, "service_type": "x.S", "name": "S", "properties": {"Password": CANARY}, "enable": False},
    ("POST", f"/process-groups/{PG}/controller-services"),
)
_PROCESSOR_TOOLS = (
    ({**_UPDATE_PROCESSOR[1], "properties": {"Password": CANARY}}, _UPDATE_PROCESSOR),
    ({**_CREATE_PROCESSOR[1], "properties": {"Password": CANARY}}, _CREATE_PROCESSOR),
)

# Every refused sentence over every tool that can receive it, as a cluster 409 and a single-node 400.
CASES = [
    (tool, with_properties if (sentence, field) in PROPERTY_SENTENCES else params, target, sentence, field, status)
    for with_properties, (tool, params, target) in _PROCESSOR_TOOLS
    for sentence, field in PROCESSOR_SENTENCES + PROPERTY_SENTENCES
    for status in (409, 400)
] + [
    (tool, params, target, sentence, field, status)
    for tool, params, target in (_UPDATE_SERVICE, _CREATE_SERVICE)
    for sentence, field in PROPERTY_SENTENCES
    for status in (409, 400)
] + [
    (tool, params, target, sentence, field, status)
    for tool, params, target in (_UPDATE_CONNECTION, _CREATE_CONNECTION)
    for sentence, field in CONNECTION_SENTENCES
    for status in (409, 400)
]


class Refuses(FakeNiFi):
    """Answers every request; the target is refused with NiFi's sentence as a plain-text body."""

    def __init__(self, target: tuple[str, str], status: int, text: str) -> None:
        super().__init__(target, "2xx")
        self.refusal = (target, status, text)

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        target, status, text = self.refusal
        path = request.url.path.removeprefix("/nifi-api")
        if (request.method, path) == target:
            self.calls.append((request.method, path))
            return httpx.Response(status, text=text)
        return await super().handle_async_request(request)


async def _call(tool: str, params: dict[str, Any], transport: FakeNiFi) -> tuple[str, dict[str, Any]]:
    client = NiFiClient(settings(), transport=transport)
    configure(client, settings())
    try:
        out = await mcp.call_tool(tool, {"params": params})
    finally:
        await client.aclose()
    blocks = out[0] if isinstance(out, tuple) else out
    text = "".join(getattr(block, "text", "") for block in blocks)
    return text, json.loads(text)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("tool", "params", "target", "sentence", "field", "status"),
    CASES,
    ids=[f"{c[0]}-{c[4]}-{c[5]}-{i}" for i, c in enumerate(CASES)],
)
async def test_a_refused_setting_is_named_without_the_conflict_recipe(
    tool: str, params: dict[str, Any], target: tuple[str, str], sentence: str, field: str, status: int
) -> None:
    body = f"{NODE}{sentence}" if status == 409 else sentence
    text, payload = await _call(tool, params, Refuses(target, status, body))
    assert payload["status"] == "error", payload
    assert payload["outcome"] == "not_applied", payload
    assert payload["type"] == "NiFiError", payload
    assert RECIPE not in text, text
    assert field in payload["hint"], payload["hint"]
    assert "changed nothing" in payload["hint"], payload["hint"]
    assert CANARY not in text, text


_SPEC = {
    "parent_process_group_id": PG,
    "process_group": {"name": "g"},
    "objects": [
        {"type": "controller_service", "name": "S", "service_type": "x.S", "properties": {"Password": CANARY}},
        {
            "type": "processor", "name": "P", "processor_type": "x.P", "scheduling_period": CANARY,
            "properties": {"Password": CANARY},
        },
        {"type": "output_port", "name": "O"},
        {"type": "connection", "source": "P", "target": "O", "relationships": ["success"],
         "flow_file_expiration": CANARY},
    ],
}
SPEC_CASES = [
    (target, sentence, field, status)
    for target, sentences in (
        (("POST", f"/process-groups/{NEWPG}/processors"), PROCESSOR_SENTENCES + PROPERTY_SENTENCES),
        (("POST", f"/process-groups/{NEWPG}/controller-services"), PROPERTY_SENTENCES),
        (("POST", f"/process-groups/{NEWPG}/connections"), CONNECTION_SENTENCES),
    )
    for sentence, field in sentences
    for status in (409, 400)
]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("target", "sentence", "field", "status"),
    SPEC_CASES,
    ids=[f"{c[0][1].rsplit('/', 1)[-1]}-{c[2]}-{c[3]}-{i}" for i, c in enumerate(SPEC_CASES)],
)
async def test_a_refused_setting_in_a_spec_is_named_in_the_hint(
    target: tuple[str, str], sentence: str, field: str, status: int
) -> None:
    body = f"{NODE}{sentence}" if status == 409 else sentence
    text, payload = await _call("nifi_apply_flow_spec", {"spec": _SPEC}, Refuses(target, status, body))
    assert payload["outcome"] == "not_applied", payload
    assert field in payload["hint"], payload
    assert "nothing needs stopping" in payload["hint"], payload
    assert RECIPE not in text and CANARY not in text, text


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "body",
    [
        f"[1, {PROC}] is not the most up-to-date revision. This component appears to have been modified",
        f"{NODE}{PROC} is currently running",
    ],
)
async def test_a_real_conflict_keeps_the_recipe(body: str) -> None:
    transport = Refuses(("PUT", f"/processors/{PROC}"), 409, body)
    text, payload = await _call("nifi_update_processor", {"processor_id": PROC, "name": "renamed"}, transport)
    assert payload["type"] == "NiFiConflictError", payload
    assert RECIPE in text, text
    assert payload["hint"].startswith("NiFi refused the request, so it changed nothing"), payload


def _guides() -> dict[str, str]:
    from nifi_mcp.server import INSTRUCTIONS, nifi_debug_flow

    return {
        "INSTRUCTIONS": INSTRUCTIONS,
        "nifi_debug_flow": nifi_debug_flow(),
    }


@pytest.mark.parametrize("guide", ["INSTRUCTIONS", "nifi_debug_flow"])
def test_every_guide_reserves_the_conflict_recipe_for_a_conflict(guide: str) -> None:
    # Every place that tells the model what to do on HTTP 409 separates a refused value
    # (the hint names a field) from a revision, running-component or queue conflict.
    text = _guides()[guide]
    assert "409" in text, guide
    assert "hint names a field" in text, guide
    assert "NiFiConflictError" in text, guide
    assert "Stop the processor, empty the queue, retry" not in text, guide
