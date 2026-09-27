import json
from typing import Any

import httpx
import pytest

from nifi_mcp.errors import NiFiError
from nifi_mcp.flow_spec import apply_flow_spec, relayout_process_group, resolve_service_refs
from nifi_mcp.layout import boxes_overlap


class FakeClient:
    def __init__(self) -> None:
        self.calls: list[str] = []
        self.positions: dict[str, tuple[Any, Any]] = {}
        self.pg_kwargs: list[dict[str, Any]] = []
        self.parent_context: str | None = None
        self.service_props: dict[str, Any] = {}
        self.processor_kwargs: dict[str, dict[str, Any]] = {}
        self.port_positions: dict[str, tuple[Any, Any]] = {}
        self.connection_kwargs: list[dict[str, Any]] = []
        self._ids = 0

    def _id(self, prefix: str) -> str:
        self._ids += 1
        return f"{prefix}-{self._ids}"

    async def create_process_group(self, parent_id: str, name: str, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(f"pg:{parent_id}:{name}")
        self.pg_kwargs.append(kwargs)
        return {"id": self._id("pg"), "revision": {"version": 0}, "component": {"name": name}}

    async def create_controller_service(
        self, parent_id: str, service_type: str, name: str, **kwargs: Any
    ) -> dict[str, Any]:
        self.calls.append(f"svc:{name}")
        self.service_props[name] = kwargs.get("properties")
        return {"id": self._id("svc"), "revision": {"version": 0}, "component": {"name": name}}

    async def set_controller_service_state(self, service_id: str, state: str, version: int) -> dict[str, Any]:
        self.calls.append(f"enable:{service_id}:{state}")
        return {"id": service_id, "revision": {"version": version + 1}, "component": {"state": state}}

    async def create_processor(self, parent_id: str, processor_type: str, name: str, **kwargs: Any) -> dict[str, Any]:
        self.positions[name] = (kwargs.get("x"), kwargs.get("y"))
        self.processor_kwargs[name] = kwargs
        self.calls.append(f"proc:{name}:{kwargs.get('properties')}")
        return {"id": self._id("proc"), "revision": {"version": 0}, "component": {"name": name}}

    async def create_port(self, parent_id: str, kind: str, name: str, **kwargs: Any) -> dict[str, Any]:
        self.port_positions[name] = (kwargs.get("x"), kwargs.get("y"))
        self.calls.append(f"port:{kind}:{name}")
        return {"id": self._id("port"), "component": {"name": name}}

    async def create_connection(self, parent_id: str, **kwargs: Any) -> dict[str, Any]:
        self.connection_kwargs.append(kwargs)
        self.calls.append(f"conn:{kwargs['source_id']}->{kwargs['destination_id']}:bends={kwargs.get('bends')}")
        return {"id": self._id("conn")}

    async def get_process_group(self, process_group_id: str) -> dict[str, Any]:
        context = self.parent_context if process_group_id == "root" else None
        component = {"id": process_group_id, "parameterContext": {"id": context} if context else None}
        return {"id": process_group_id, "revision": {"version": 1}, "component": component}

    async def get_processor(self, processor_id: str) -> dict[str, Any]:
        return {
            "id": processor_id,
            "revision": {"version": 1},
            "component": {"id": processor_id, "position": {"x": 0, "y": 0}},
        }

    async def update_processor(self, processor_id: str, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(f"move:{processor_id}:{kwargs.get('x')}:{kwargs.get('y')}")
        return {"id": processor_id, "revision": {"version": 2}}

    async def get_connection(self, connection_id: str) -> dict[str, Any]:
        return {"id": connection_id, "revision": {"version": 1}, "component": {"id": connection_id}}

    async def update_connection(self, connection_id: str, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(f"route:{connection_id}:bends={kwargs.get('bends')}")
        return {"id": connection_id}

    async def update_port(self, port_id: str, **kwargs: Any) -> dict[str, Any]:
        self.calls.append(f"portmove:{port_id}:{kwargs.get('x')}:{kwargs.get('y')}")
        return {"id": port_id}

    async def get_flow(self, process_group_id: str) -> dict[str, Any]:
        return {
            "processGroupFlow": {
                "id": process_group_id,
                "breadcrumb": {"breadcrumb": {"id": process_group_id, "name": "spec"}},
                "flow": {
                    "processors": [
                        {
                            "id": "g",
                            "revision": {"version": 3},
                            "component": {
                                "id": "g",
                                "name": "Generate",
                                "position": {"x": 0, "y": 0},
                            },
                        },
                        {
                            "id": "l",
                            "revision": {"version": 4},
                            "component": {
                                "id": "l",
                                "name": "Log",
                                "position": {"x": 580, "y": 0},
                            },
                        },
                    ],
                    "connections": [
                        {
                            "id": "c1",
                            "revision": {"version": 2},
                            "component": {
                                "id": "c1",
                                "source": {"id": "g"},
                                "destination": {"id": "l"},
                                "selectedRelationships": ["success"],
                            },
                        }
                    ],
                    "processGroups": [],
                },
            }
        }


def test_resolve_service_refs() -> None:
    resolved = resolve_service_refs(
        {"HTTP Context Map": "@Ctx", "Port": "8080"},
        {"Ctx": "svc-1"},
    )
    assert resolved["HTTP Context Map"] == "svc-1"
    assert resolved["Port"] == "8080"


def test_resolve_service_refs_unknown() -> None:
    from nifi_mcp.flow_spec import SpecError

    with pytest.raises(SpecError, match="references a controller service"):
        resolve_service_refs({"HTTP Context Map": "@Missing"}, {})


@pytest.mark.asyncio
async def test_apply_flow_spec_wires_by_name() -> None:
    client = FakeClient()
    spec = {
        "process_group": {"name": "http-log"},
        "objects": [
            {
                "type": "controller_service",
                "service_type": "org.apache.nifi.http.StandardHttpContextMap",
                "name": "Ctx",
            },
            {
                "type": "processor",
                "processor_type": "org.apache.nifi.processors.standard.HandleHttpRequest",
                "name": "Listen",
                "properties": {"HTTP Context Map": "@Ctx", "Listening Port": "18080"},
                "auto_terminated": [],
            },
            {
                "type": "processor",
                "processor_type": "org.apache.nifi.processors.standard.LogAttribute",
                "name": "Log",
                "auto_terminated": ["success"],
            },
            {
                "type": "connection",
                "source": "Listen",
                "target": "Log",
                "relationships": ["success"],
            },
        ],
    }
    result = await apply_flow_spec(client, spec, parent_process_group_id="root")  # type: ignore[arg-type]
    assert result["status"] == "ok"
    assert any(item["kind"] == "process_group" for item in result["created"])
    assert "enable:" in ",".join(client.calls)
    assert any(call.startswith("conn:") for call in client.calls)
    listen = next(call for call in client.calls if call.startswith("proc:Listen"))
    service_id = next(item["id"] for item in result["created"] if item["kind"] == "controller_service")
    assert service_id in listen
    from nifi_mcp.layout import ROW_PITCH

    assert client.positions["Listen"] == (0.0, 0.0)
    assert client.positions["Log"] == (0.0, ROW_PITCH)


@pytest.mark.asyncio
async def test_relayout_stacks_top_to_bottom() -> None:
    from nifi_mcp.layout import ROW_PITCH

    client = FakeClient()
    result = await relayout_process_group(client, "pg-1")  # type: ignore[arg-type]
    assert result["status"] == "ok"
    assert result["direction"] == "TB"
    assert result["row_pitch"] == ROW_PITCH
    assert {"id": "l", "name": "Log", "x": 0.0, "y": ROW_PITCH} in result["moved"]
    assert any(call.startswith("move:l:0.0:") for call in client.calls)
    assert any(call.startswith("route:c1:") for call in client.calls)


@pytest.mark.asyncio
async def test_spec_binds_parameter_context_on_create() -> None:
    client = FakeClient()
    ctx = "00000000-0000-0000-0000-00000000c7c1"
    spec = {"process_group": {"name": "review", "parameter_context_id": ctx}, "objects": []}
    result = await apply_flow_spec(client, spec, parent_process_group_id="root")  # type: ignore[arg-type]
    assert result["status"] == "ok"
    assert client.pg_kwargs[0]["parameter_context_id"] == ctx


_PARENT = "00000000-0000-0000-0000-0000000000f0"


class EmptyCanvasClient(FakeClient):
    """Parent canvas that reports the groups this client has created so far."""

    def __init__(self) -> None:
        super().__init__()
        self.groups: list[dict[str, Any]] = []

    async def create_process_group(self, parent_id: str, name: str, **kwargs: Any) -> dict[str, Any]:
        entity = await super().create_process_group(parent_id, name, **kwargs)
        if parent_id == _PARENT:
            self.groups.append(
                {"id": entity["id"], "component": {"name": name, "position": {"x": kwargs["x"], "y": kwargs["y"]}}}
            )
        return entity

    async def get_flow(self, process_group_id: str) -> dict[str, Any]:
        flow = {"processGroups": self.groups} if process_group_id == _PARENT else {}
        return {"processGroupFlow": {"id": process_group_id, "flow": flow}}


@pytest.mark.asyncio
async def test_two_specs_under_one_parent_do_not_share_a_cell() -> None:
    from nifi_mcp.layout import PG_HEIGHT, PG_WIDTH, boxes_overlap

    client = EmptyCanvasClient()
    for name in ("ingest", "transform"):
        spec = {"process_group": {"name": name}, "objects": []}
        out = await apply_flow_spec(client, spec, parent_process_group_id=_PARENT)  # type: ignore[arg-type]
        assert out["status"] == "ok"
    first, second = (kw for kw in client.pg_kwargs)
    a = (first["x"], first["y"])
    b = (second["x"], second["y"])
    assert not boxes_overlap(a, b, width=PG_WIDTH, height=PG_HEIGHT), (a, b)


@pytest.mark.asyncio
async def test_spec_group_avoids_processors_on_parent() -> None:
    from nifi_mcp.layout import CARD_HEIGHT, CARD_WIDTH, boxes_overlap

    client = FakeClient()  # parent canvas holds processors at (0,0) and (580,0)
    out = await apply_flow_spec(client, {"process_group": {"name": "g"}, "objects": []})  # type: ignore[arg-type]
    assert out["status"] == "ok"
    slot = (client.pg_kwargs[0]["x"], client.pg_kwargs[0]["y"])
    for card in ((0.0, 0.0), (580.0, 0.0)):
        assert not boxes_overlap(slot, card, width=CARD_WIDTH, height=CARD_HEIGHT), slot


@pytest.mark.asyncio
async def test_spec_group_keeps_explicit_position() -> None:
    client = FakeClient()
    spec = {"process_group": {"name": "g", "x": 840, "y": 216}, "objects": []}
    await apply_flow_spec(client, spec)  # type: ignore[arg-type]
    assert (client.pg_kwargs[0]["x"], client.pg_kwargs[0]["y"]) == (840.0, 216.0)


@pytest.mark.asyncio
async def test_spec_port_connection_needs_no_relationships() -> None:
    client = FakeClient()
    spec = {
        "process_group": {"name": "transform"},
        "objects": [
            {"type": "input_port", "name": "in"},
            {"type": "processor", "processor_type": "x.ConvertRecord", "name": "Convert", "auto_terminated": []},
            {"type": "connection", "source": "in", "target": "Convert", "relationships": []},
        ],
    }
    result = await apply_flow_spec(client, spec)  # type: ignore[arg-type]
    assert result["status"] == "ok", result
    conn = next(call for call in client.calls if call.startswith("conn:"))
    assert conn.startswith("conn:port-")


@pytest.mark.asyncio
async def test_spec_processor_connection_without_relationships_fails_before_creating() -> None:
    client = FakeClient()
    spec = {
        "process_group": {"name": "g"},
        "objects": [
            {"type": "processor", "processor_type": "x.A", "name": "A"},
            {"type": "processor", "processor_type": "x.B", "name": "B"},
            {"type": "connection", "source": "A", "target": "B"},
        ],
    }
    result = await apply_flow_spec(client, spec)  # type: ignore[arg-type]
    assert result["status"] == "error"
    assert "objects[2] (connection)" in result["error"]
    assert result["created"] == []
    assert client.calls == []


@pytest.mark.asyncio
async def test_service_property_service_ref_is_resolved() -> None:
    client = FakeClient()
    spec = {
        "process_group": {"name": "transform"},
        "objects": [
            {"type": "controller_service", "service_type": "x.AvroSchemaRegistry", "name": "Reg"},
            {
                "type": "controller_service",
                "service_type": "org.apache.nifi.json.JsonTreeReader",
                "name": "Reader",
                "properties": {"schema-registry": "@Reg"},
            },
        ],
    }
    result = await apply_flow_spec(client, spec, parent_process_group_id="root")  # type: ignore[arg-type]
    assert result["status"] == "ok"
    assert client.service_props["Reader"]["schema-registry"] == result["name_map"]["Reg"]
    enables = [call for call in client.calls if call.startswith("enable:")]
    assert enables == [f"enable:{result['name_map']['Reg']}:ENABLED", f"enable:{result['name_map']['Reader']}:ENABLED"]


@pytest.mark.asyncio
async def test_service_ref_to_a_later_service_is_an_error() -> None:
    client = FakeClient()
    spec = {
        "objects": [
            {"type": "controller_service", "service_type": "x.R", "name": "Reader", "properties": {"r": "@Reg"}},
            {"type": "controller_service", "service_type": "x.S", "name": "Reg"},
        ],
    }
    result = await apply_flow_spec(client, spec)  # type: ignore[arg-type]
    assert result["status"] == "error"
    assert "objects[0]: property 'r' references a controller service" in result["error"]
    assert "Reg" not in result["error"]


class RunningClient(FakeClient):
    """Processors report RUNNING; the move fails like a NiFi 409 would."""

    def __init__(self) -> None:
        super().__init__()
        self.state = {"g": "RUNNING", "l": "RUNNING"}
        self.run_status_calls: list[tuple[str, str]] = []

    async def get_processor(self, processor_id: str) -> dict[str, Any]:
        entity = await super().get_processor(processor_id)
        entity["component"]["state"] = self.state.get(processor_id, "STOPPED")
        return entity

    async def set_processor_run_status(self, processor_id: str, state: str, version: int) -> dict[str, Any]:
        self.run_status_calls.append((processor_id, state))
        self.state[processor_id] = state
        return {"id": processor_id, "revision": {"version": version + 1}}

    async def update_processor(self, processor_id: str, **kwargs: Any) -> dict[str, Any]:
        raise NiFiError("move failed")


@pytest.mark.asyncio
async def test_failed_move_leaves_running_processor_running() -> None:
    client = RunningClient()
    with pytest.raises(NiFiError, match="move failed"):
        await relayout_process_group(client, "pg-1")  # type: ignore[arg-type]
    assert client.state["l"] == "RUNNING"
    assert client.run_status_calls == []


@pytest.mark.asyncio
async def test_relayout_moves_running_processor_without_stopping_it() -> None:
    class MovingClient(RunningClient):
        async def update_processor(self, processor_id: str, **kwargs: Any) -> dict[str, Any]:
            return await FakeClient.update_processor(self, processor_id, **kwargs)

    client = MovingClient()
    result = await relayout_process_group(client, "pg-1")  # type: ignore[arg-type]
    assert result["status"] == "ok"
    assert any(call.startswith("move:l:") for call in client.calls)
    assert client.run_status_calls == []


@pytest.mark.asyncio
async def test_spec_scheduling_period_reaches_processor() -> None:
    client = FakeClient()
    spec = {
        "process_group": {"name": "ingest"},
        "objects": [
            {
                "type": "processor",
                "processor_type": "org.apache.nifi.processors.standard.GenerateFlowFile",
                "name": "Gen",
                "scheduling_period": "10 sec",
                "scheduling_strategy": "TIMER_DRIVEN",
                "auto_terminated": [],
            }
        ],
    }
    result = await apply_flow_spec(client, spec, parent_process_group_id="root")  # type: ignore[arg-type]
    assert result["status"] == "ok"
    assert client.processor_kwargs["Gen"]["scheduling_period"] == "10 sec"
    assert client.processor_kwargs["Gen"]["scheduling_strategy"] == "TIMER_DRIVEN"
    assert "warnings" not in result


@pytest.mark.asyncio
async def test_spec_unknown_processor_keys_are_reported() -> None:
    client = FakeClient()
    spec = {
        "objects": [
            {"type": "processor", "processor_type": "x.Gen", "name": "Gen", "run_every": "10 sec"},
            {"type": "funnel", "name": "F"},
        ]
    }
    result = await apply_flow_spec(client, spec)  # type: ignore[arg-type]
    assert result["status"] == "ok"
    assert any("run_every" in warning for warning in result["warnings"])
    assert any(warning.startswith("objects[1]: its type is not one") for warning in result["warnings"])


@pytest.mark.asyncio
async def test_spec_group_inherits_parent_parameter_context() -> None:
    client = FakeClient()
    client.parent_context = "ctx-parent"
    await apply_flow_spec(client, {"process_group": {"name": "ingest"}, "objects": []})  # type: ignore[arg-type]
    await apply_flow_spec(
        client,  # type: ignore[arg-type]
        {"process_group": {"name": "own", "inherit_parameter_context": False}, "objects": []},
    )
    assert client.pg_kwargs[0]["parameter_context_id"] == "ctx-parent"
    assert client.pg_kwargs[1]["parameter_context_id"] is None


@pytest.mark.asyncio
async def test_spec_into_existing_group_avoids_its_cards() -> None:
    from nifi_mcp.layout import CARD_HEIGHT, CARD_WIDTH, boxes_overlap

    client = FakeClient()  # every group already holds processors at (0,0) and (580,0)
    spec = {
        "objects": [
            {"type": "processor", "processor_type": "x.LogAttribute", "name": "Log", "auto_terminated": ["success"]},
            {"type": "output_port", "name": "out"},
        ]
    }
    out = await apply_flow_spec(client, spec, parent_process_group_id=_PARENT)  # type: ignore[arg-type]
    assert out["status"] == "ok", out
    placed = [client.positions["Log"], client.port_positions["out"]]
    for new in placed:
        for card in ((0.0, 0.0), (580.0, 0.0)):
            assert not boxes_overlap(new, card, width=CARD_WIDTH, height=CARD_HEIGHT), (new, card)
    assert not boxes_overlap(placed[0], placed[1], width=CARD_WIDTH, height=CARD_HEIGHT), placed


@pytest.mark.asyncio
async def test_spec_relationships_string_is_not_split_into_characters() -> None:
    client = FakeClient()
    sent: dict[str, Any] = {}

    async def create_connection(parent_id: str, **kwargs: Any) -> dict[str, Any]:
        sent.update(kwargs)
        return {"id": "c-1"}

    client.create_connection = create_connection  # type: ignore[method-assign]
    spec = {
        "process_group": {"name": "g"},
        "objects": [
            {"type": "processor", "processor_type": "x.A", "name": "A"},
            {"type": "processor", "processor_type": "x.B", "name": "B", "auto_terminated": ["success"]},
            {"type": "connection", "source": "A", "target": "B", "relationships": "success"},
        ],
    }
    out = await apply_flow_spec(client, spec)  # type: ignore[arg-type]
    assert out["status"] == "ok", out
    assert sent["relationships"] == ["success"]


EXISTING = "11111111-1111-1111-1111-111111111111"


class RejectingClient(FakeClient):
    """Accepts every create except the processor named Second."""

    async def create_processor(self, parent_id: str, processor_type: str, name: str, **kwargs: Any) -> dict[str, Any]:
        if name == "Second":
            raise NiFiError("second processor rejected")
        return await super().create_processor(parent_id, processor_type, name, **kwargs)


_TWO_PROCESSORS = [
    {"type": "processor", "processor_type": "x.A", "name": "First"},
    {"type": "processor", "processor_type": "x.B", "name": "Second"},
]


@pytest.mark.asyncio
async def test_failure_in_existing_group_never_says_delete_the_group() -> None:
    result = await apply_flow_spec(
        RejectingClient(),  # type: ignore[arg-type]
        {"objects": _TWO_PROCESSORS},
        parent_process_group_id=EXISTING,
    )
    assert result["status"] == "error"
    assert result["cause"] == "nifi"
    assert result["process_group_id"] == EXISTING
    assert [item["ref"] for item in result["created"]] == ["objects[0]"]
    assert "Delete the process group" not in result["hint"]
    assert "do not delete it" in result["hint"]
    assert result["created"][0]["id"] in result["hint"]


@pytest.mark.asyncio
async def test_failure_after_creating_the_group_says_delete_that_group() -> None:
    result = await apply_flow_spec(
        RejectingClient(),  # type: ignore[arg-type]
        {"process_group": {"name": "g"}, "objects": _TWO_PROCESSORS},
    )
    group_id = result["created"][0]["id"]
    assert result["created"][0]["kind"] == "process_group"
    assert result["process_group_id"] == group_id
    assert f"This call created process group {group_id}" in result["hint"]


async def test_failure_after_enabling_services_says_disable_them_before_deleting_the_group() -> None:
    # NiFi refuses to delete a group holding an enabled service
    # (StandardControllerServiceNode.verifyCanDelete), so the hint must disable them first.
    client = RejectingClient()
    result = await apply_flow_spec(
        client,  # type: ignore[arg-type]
        {
            "process_group": {"name": "g"},
            "objects": [{"type": "controller_service", "service_type": "x.S", "name": "Reader"}, *_TWO_PROCESSORS],
        },
    )
    assert result["status"] == "error"
    group_id = result["created"][0]["id"]
    service_id = next(item["id"] for item in result["created"] if item["kind"] == "controller_service")
    assert f"enable:{service_id}:ENABLED" in client.calls
    hint = result["hint"]
    assert f"This call created process group {group_id}" in hint
    assert service_id in hint
    assert hint.index("Disable") < hint.index("delete the group")
    assert "stopped" in hint


@pytest.mark.asyncio
async def test_failure_after_creating_a_group_without_services_just_deletes_it() -> None:
    result = await apply_flow_spec(
        RejectingClient(),  # type: ignore[arg-type]
        {"process_group": {"name": "g"}, "objects": _TWO_PROCESSORS},
    )
    assert "disable" not in result["hint"].lower()


@pytest.mark.asyncio
async def test_malformed_properties_on_a_later_object_are_refused_before_the_first_create() -> None:
    # Preflight reads every item's properties, so the first service is not created.
    client = FakeClient()
    spec = {
        "objects": [
            {"type": "controller_service", "service_type": "x.S", "name": "Ok", "properties": {"a": "1"}},
            {"type": "controller_service", "service_type": "x.S", "name": "Bad", "properties": ["not-a-dict"]},
        ]
    }
    result = await apply_flow_spec(client, spec, parent_process_group_id=EXISTING)  # type: ignore[arg-type]
    assert client.calls == []
    assert result["status"] == "error"
    assert result["cause"] == "spec"
    assert result["error"].startswith("Malformed spec (AttributeError)")
    assert result["created"] == []
    assert result["hint"].startswith("Nothing was created")


def test_json_scalars_reach_nifi_as_nifi_strings() -> None:
    # NiFi matches allowable values exactly ("true", not "True"); null unsets a property.
    resolved = resolve_service_refs(
        {"Unique FlowFiles": True, "Ignore": False, "Custom Text": None, "Batch Size": 1, "Rate": 0.5}, {}
    )
    assert resolved == {
        "Unique FlowFiles": "true",
        "Ignore": "false",
        "Custom Text": None,
        "Batch Size": "1",
        "Rate": "0.5",
    }


def test_nested_property_value_is_refused_by_name() -> None:
    with pytest.raises(TypeError, match="Headers"):
        resolve_service_refs({"Headers": {"a": "b"}}, {})


async def test_boolean_and_null_properties_are_posted_in_nifi_shape() -> None:
    client = FakeClient()
    spec = {
        "objects": [
            {
                "type": "processor",
                "processor_type": "x.Gen",
                "name": "Gen",
                "properties": {"Unique FlowFiles": True, "Custom Text": None},
            }
        ]
    }
    out = await apply_flow_spec(client, spec, parent_process_group_id="root")
    assert out["status"] == "ok", out
    assert client.processor_kwargs["Gen"]["properties"] == {"Unique FlowFiles": "true", "Custom Text": None}


@pytest.mark.parametrize("key", ["auto_terminated", "autoTerminatedRelationships"])
async def test_auto_terminated_string_is_wrapped_in_a_list(key: str) -> None:
    # NiFi maps autoTerminatedRelationships into a Set<String> and 400s on a bare string.
    client = FakeClient()
    spec = {"objects": [{"type": "processor", "processor_type": "x.Log", "name": "Log", key: "success"}]}
    out = await apply_flow_spec(client, spec, parent_process_group_id="root")
    assert out["status"] == "ok", out
    assert client.processor_kwargs["Log"]["auto_terminated"] == ["success"]


async def test_kind_objects_get_non_overlapping_positions() -> None:
    # "kind" is an alias of "type"; its cards must go through the same layout.
    client = FakeClient()
    spec = {
        "objects": [
            {"kind": "output_port", "name": "out"},
            {"kind": "processor", "processor_type": "x.Gen", "name": "Gen"},
            {"kind": "processor", "processor_type": "x.Upd", "name": "Upd"},
            {"kind": "connection", "source": "Gen", "target": "Upd", "relationships": ["success"]},
            {"kind": "connection", "source": "Upd", "target": "out", "relationships": ["success"]},
        ]
    }
    out = await apply_flow_spec(client, spec, parent_process_group_id="root")
    assert out["status"] == "ok", out
    points = {**client.positions, **client.port_positions}
    names = list(points)
    for i, a in enumerate(names):
        for b in names[i + 1 :]:
            assert not boxes_overlap(points[a], points[b]), f"{a} {points[a]} overlaps {b} {points[b]}"


async def test_mixed_type_and_kind_objects_do_not_overlap() -> None:
    client = FakeClient()
    spec = {
        "objects": [
            {"type": "processor", "processor_type": "x.Gen", "name": "Gen"},
            {"kind": "processor", "processor_type": "x.Upd", "name": "Upd"},
            {"type": "connection", "source": "Gen", "target": "Upd", "relationships": ["success"]},
        ]
    }
    out = await apply_flow_spec(client, spec, parent_process_group_id="root")
    assert out["status"] == "ok", out
    assert not boxes_overlap(client.positions["Gen"], client.positions["Upd"]), client.positions


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "second",
    [
        {"type": "processor", "processor_type": "x.Log", "name": "Log", "auto_terminated": ["success"]},
        {"type": "output_port", "name": "Log"},
        {"type": "controller_service", "service_type": "x.Svc", "name": "Log"},
    ],
)
async def test_duplicate_names_are_refused_before_anything_is_created(second: dict[str, Any]) -> None:
    # Connections, @service references, layout and name_map are keyed by name.
    client = FakeClient()
    spec = {
        "process_group": {"name": "g"},
        "objects": [
            {"type": "processor", "processor_type": "x.Log", "name": "Log", "auto_terminated": ["success"]},
            second,
        ],
    }
    result = await apply_flow_spec(client, spec)
    assert result["status"] == "error"
    assert result["cause"] == "spec"
    assert "objects[0] (processor) and objects[1]" in result["error"]
    assert "Log" not in result["error"]
    assert result["created"] == []
    assert client.calls == []


@pytest.mark.asyncio
async def test_duplicate_of_the_group_name_is_refused() -> None:
    client = FakeClient()
    spec = {
        "process_group": {"name": "Log"},
        "objects": [{"type": "processor", "processor_type": "x.Log", "name": "Log"}],
    }
    result = await apply_flow_spec(client, spec)
    assert result["cause"] == "spec"
    assert client.calls == []


@pytest.mark.asyncio
async def test_manual_layout_defaults_do_not_collide() -> None:
    client = FakeClient()
    spec = {
        "layout": "manual",
        "objects": [
            {"type": "output_port", "name": "out"},
            {"type": "processor", "processor_type": "x.Gen", "name": "Gen"},
            {"type": "input_port", "name": "in"},
            {"type": "processor", "processor_type": "x.Log", "name": "Log"},
        ],
    }
    out = await apply_flow_spec(client, spec, parent_process_group_id="root")
    assert out["status"] == "ok", out
    cards = list(client.positions.values()) + list(client.port_positions.values())
    for i, a in enumerate(cards):
        for b in cards[i + 1 :]:
            assert not boxes_overlap(a, b), (client.positions, client.port_positions)


_ALIASED_LIST_KEYS = [
    ("objects", "processor", "auto_terminated", "auto_terminated"),
    ("objects", "processor", "autoTerminatedRelationships", "auto_terminated"),
    ("objects", "connection", "relationships", "relationships"),
    ("objects", "connection", "selectedRelationships", "relationships"),
    ("connections", "connection", "relationships", "relationships"),
    ("connections", "connection", "selectedRelationships", "relationships"),
]


@pytest.mark.parametrize(("where", "kind", "key", "canonical"), _ALIASED_LIST_KEYS)
def test_every_list_key_is_a_list_after_parse(where: str, kind: str, key: str, canonical: str) -> None:
    # One normalisation at parse time, so no builder ever sees a bare string or an alias.
    from nifi_mcp.flow_spec import normalise_spec

    item = {"type": kind, "name": "X", key: "success"}
    objects, connections = normalise_spec({where: [item]})
    (parsed,) = objects or connections
    assert parsed[canonical] == ["success"]
    assert key == canonical or key not in parsed
    assert item[key] == "success", "the caller's spec is not mutated"


@pytest.mark.parametrize(
    ("kind", "canonical", "alias"),
    [
        ("processor", "auto_terminated", "autoTerminatedRelationships"),
        ("connection", "relationships", "selectedRelationships"),
    ],
)
def test_a_list_key_and_its_alias_are_merged_not_dropped(kind: str, canonical: str, alias: str) -> None:
    # Both keys set keeps every name from both, in order, once.
    from nifi_mcp.flow_spec import normalise_spec

    item = {"type": kind, "name": "P", canonical: ["success", "retry"], alias: ["failure", "success"]}
    (parsed,), _ = normalise_spec({"objects": [item]})
    assert parsed[canonical] == ["success", "retry", "failure"]
    assert alias not in parsed


@pytest.mark.parametrize("bad", [7, {"success": True}, ["success", 3]])
async def test_non_name_relationship_values_are_refused_before_anything_is_created(bad: Any) -> None:
    client = FakeClient()
    spec = {
        "process_group": {"name": "g"},
        "objects": [{"type": "processor", "processor_type": "x.Log", "name": "Log", "auto_terminated": bad}],
    }
    result = await apply_flow_spec(client, spec)  # type: ignore[arg-type]
    assert result["status"] == "error"
    assert result["cause"] == "spec"
    assert "objects[0] (processor): auto_terminated" in result["error"]
    assert client.calls == []


@pytest.mark.parametrize("spec", [{"objects": "Log"}, {"objects": ["Log"]}, {"connections": {"source": "a"}}])
async def test_objects_of_the_wrong_shape_are_refused_before_anything_is_created(spec: dict[str, Any]) -> None:
    client = FakeClient()
    result = await apply_flow_spec(client, {"process_group": {"name": "g"}, **spec})  # type: ignore[arg-type]
    assert result["cause"] == "spec", result
    assert client.calls == []


async def test_selected_relationships_string_reaches_nifi_as_a_list() -> None:
    client = FakeClient()
    sent: dict[str, Any] = {}

    async def create_connection(parent_id: str, **kwargs: Any) -> dict[str, Any]:
        sent.update(kwargs)
        return {"id": "c-1"}

    client.create_connection = create_connection  # type: ignore[method-assign]
    spec = {
        "process_group": {"name": "g"},
        "objects": [
            {"type": "processor", "processor_type": "x.A", "name": "A"},
            {"type": "processor", "processor_type": "x.B", "name": "B", "autoTerminatedRelationships": "success"},
        ],
        "connections": [{"source": "A", "target": "B", "selectedRelationships": "success"}],
    }
    out = await apply_flow_spec(client, spec)  # type: ignore[arg-type]
    assert out["status"] == "ok", out
    assert sent["relationships"] == ["success"]
    assert client.processor_kwargs["B"]["auto_terminated"] == ["success"]
    assert "warnings" not in out


class StageFailingClient(FakeClient):
    """Raises NiFiError from one stage of a spec build: the method named by fail_at."""

    def __init__(self, fail_at: str) -> None:
        super().__init__()
        self.fail_at = fail_at
        self.group_ids: list[str] = []

    def _maybe_fail(self, stage: str) -> None:
        if stage == self.fail_at:
            raise NiFiError(f"{stage} rejected")

    async def create_process_group(self, parent_id: str, name: str, **kwargs: Any) -> dict[str, Any]:
        self._maybe_fail("create_process_group")
        entity = await super().create_process_group(parent_id, name, **kwargs)
        self.group_ids.append(entity["id"])
        return entity

    async def create_controller_service(self, parent_id: str, service_type: str, name: str, **kw: Any) -> dict:
        self._maybe_fail("create_controller_service")
        return await super().create_controller_service(parent_id, service_type, name, **kw)

    async def set_controller_service_state(self, service_id: str, state: str, version: int) -> dict[str, Any]:
        self._maybe_fail("set_controller_service_state")
        return await super().set_controller_service_state(service_id, state, version)

    async def create_processor(self, parent_id: str, processor_type: str, name: str, **kw: Any) -> dict:
        self._maybe_fail("create_processor")
        return await super().create_processor(parent_id, processor_type, name, **kw)

    async def create_port(self, parent_id: str, kind: str, name: str, **kwargs: Any) -> dict[str, Any]:
        self._maybe_fail("create_port")
        return await super().create_port(parent_id, kind, name, **kwargs)

    async def create_connection(self, parent_id: str, **kwargs: Any) -> dict[str, Any]:
        self._maybe_fail("create_connection")
        return await super().create_connection(parent_id, **kwargs)

    async def get_flow(self, process_group_id: str) -> dict[str, Any]:
        # The summary read: the first read of the target group after something was created in it.
        if self._ids and (process_group_id in self.group_ids or process_group_id == EXISTING):
            self._maybe_fail("get_flow")
        return await super().get_flow(process_group_id)


_STAGED_SPEC = [
    {"type": "controller_service", "service_type": "x.Reader", "name": "Reader"},
    {"type": "processor", "processor_type": "x.Convert", "name": "Convert", "properties": {"Reader": "@Reader"}},
    {"type": "output_port", "name": "out"},
    {"type": "connection", "source": "Convert", "target": "out", "relationships": "success"},
]

# stage -> kinds created before it fails, in order
_STAGES = [
    ("create_process_group", []),
    ("create_controller_service", ["process_group"]),
    ("set_controller_service_state", ["process_group", "controller_service"]),
    ("create_processor", ["process_group", "controller_service"]),
    ("create_port", ["process_group", "controller_service", "processor"]),
    ("create_connection", ["process_group", "controller_service", "processor", "output_port"]),
    ("get_flow", ["process_group", "controller_service", "processor", "output_port", "connection"]),
]


@pytest.mark.parametrize(("stage", "kinds"), _STAGES)
@pytest.mark.parametrize("new_group", [True, False], ids=["new-group", "existing-group"])
async def test_every_failure_stage_reports_created_and_a_matching_hint(
    stage: str, kinds: list[str], new_group: bool
) -> None:
    # One result object, built from the first create, is returned on every exit path.
    if not new_group:
        if stage == "create_process_group":
            return
        kinds = kinds[1:]
    client = StageFailingClient(stage)
    spec: dict[str, Any] = {"objects": _STAGED_SPEC}
    if new_group:
        spec["process_group"] = {"name": "g"}
    result = await apply_flow_spec(client, spec, parent_process_group_id=None if new_group else EXISTING)
    assert [item["kind"] for item in result["created"]] == kinds, result
    if result["status"] == "ok":
        names = {item["name"]: item["id"] for item in result["created"] if "->" not in item["name"]}
        assert result["name_map"] == names
    else:
        # An error result names what it created by place in the spec, never by name.
        assert "name_map" not in result
        assert all("name" not in item and item["ref"] for item in result["created"]), result
    hint = result["hint"]
    if stage == "get_flow":
        assert result["status"] == "ok"
        assert "flow" not in result
        assert f"nifi_get_flow on {result['process_group_id']}" in hint
        return
    assert result["status"] == "error"
    assert result["cause"] == "nifi"
    assert f"{stage} rejected" in result["error"]
    if not kinds:
        assert hint.startswith("Nothing was created")
        assert result["process_group_id"] == ("root" if new_group else EXISTING)
        return
    enabled = [item["id"] for item in result["created"] if item.get("state") == "ENABLED"]
    assert bool(enabled) == (stage not in {"create_controller_service", "set_controller_service_state"})
    if new_group:
        group_id = result["created"][0]["id"]
        assert result["process_group_id"] == group_id
        assert f"This call created process group {group_id}" in hint
        assert ("Disable" in hint) == bool(enabled)
        assert all(service_id in hint for service_id in enabled)
    else:
        assert result["process_group_id"] == EXISTING
        assert "do not delete it" in hint
        assert all(item["id"] in hint for item in result["created"])


async def test_success_has_no_hint_and_the_full_created_list() -> None:
    client = StageFailingClient("none")
    result = await apply_flow_spec(client, {"process_group": {"name": "g"}, "objects": _STAGED_SPEC})
    assert result["status"] == "ok"
    assert "hint" not in result
    assert [item["kind"] for item in result["created"]] == _STAGES[-1][1]
    assert "flow" in result


@pytest.mark.parametrize(
    "group", [{"name": "g", "x": "canary-1234"}, {"name": "g", "position": {"y": ["canary-1234"]}}]
)
async def test_a_bad_group_coordinate_is_refused_without_echoing_it(group: dict[str, Any]) -> None:
    # float() on process_group.x put the submitted string in the error.
    client = FakeClient()
    result = await apply_flow_spec(client, {"process_group": group, "objects": []})  # type: ignore[arg-type]
    assert result["status"] == "error"
    assert result["cause"] == "spec"
    assert "canary-1234" not in json.dumps(result), result
    assert "process_group: " in result["error"]
    assert client.calls == []


class RecordingClient:
    """Reads return empty canvases; the only mutation it allows is recorded."""

    def __init__(self) -> None:
        self.calls: list[str] = []

    async def get_flow(self, process_group_id: str) -> dict[str, Any]:
        return {"processGroupFlow": {"id": process_group_id, "flow": {}}}

    async def get_process_group(self, process_group_id: str) -> dict[str, Any]:
        return {"id": process_group_id, "revision": {"version": 0}, "component": {"id": process_group_id}}

    async def create_process_group(self, parent_id: str, name: str, **kwargs: Any) -> dict[str, Any]:
        self.calls.append("create_process_group")
        return {"id": "pg-1", "revision": {"version": 0}, "component": {"id": "pg-1", "name": name}}


_P = {"type": "processor", "processor_type": "x.P", "name": "P"}
_CTX = "00000000-0000-0000-0000-0000000000c7"
_Q = {"type": "processor", "processor_type": "x.Q", "name": "Q"}


@pytest.mark.parametrize(
    ("objects", "error"),
    [
        ([{"type": "controller_service", "name": "Reader"}], "objects[0] (controller_service) is missing service_type"),
        ([{"type": "controller_service", "service_type": "x.R"}], "objects[0] (controller_service) is missing name"),
        ([{"type": "processor", "name": "P"}], "objects[0] (processor) is missing processor_type"),
        ([_P, {"type": "output_port"}], "objects[1] (output_port) is missing name"),
        (
            [_P, {"type": "connection", "source": "Nope", "target": "P", "relationships": "success"}],
            "objects[1] (connection): its source is not the name",
        ),
        ([_P, _Q, {"type": "connection", "source": "P", "target": "Q"}], "at least one relationship"),
        ([{**_P, "properties": {"Reader": "@Missing"}}], "objects[0]: property 'Reader' references a controller"),
        (
            [
                {"type": "controller_service", "service_type": "x.W", "name": "W", "properties": {"r": "@R"}},
                {"type": "controller_service", "service_type": "x.R", "name": "R"},
            ],
            "objects[0]: property 'r' references a controller service",
        ),
    ],
)
async def test_spec_errors_leave_nothing_behind_and_say_cause_spec(objects: list[dict[str, Any]], error: str) -> None:
    # Preflight runs before _place_cards and _create_group; cause nifi is for NiFi only.
    client = RecordingClient()
    result = await apply_flow_spec(client, {"process_group": {"name": "g"}, "objects": objects})  # type: ignore[arg-type]
    assert result["status"] == "error"
    assert (client.calls, result["cause"]) == ([], "spec"), result
    assert error in result["error"], result
    assert result["hint"].startswith("Nothing was created")


_TIMEOUT_PG = "00000000-0000-0000-0000-0000000000aa"


class TimesOut(httpx.AsyncBaseTransport):
    """Reads and the creates named in `answers` succeed; every other request times out, or gets
    `gateway` (a 502 or 504 from a proxy) when one is set."""

    def __init__(self, *answers: str, gateway: int | None = None) -> None:
        self.answers = answers
        self.gateway = gateway
        self.sent: list[str] = []

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path
        self.sent.append(f"{request.method} {path}")
        if request.method == "GET":
            flow = {"processGroupFlow": {"id": "root", "flow": {}}}
            return httpx.Response(200, json={**flow, "component": {"id": "root"}, "revision": {"version": 1}})
        for suffix in self.answers:
            if request.method == "POST" and path.endswith(suffix):
                cid = _TIMEOUT_PG if suffix == "/process-groups" else f"{suffix.strip('/')}-1"
                component = {"id": cid, "name": "n", "state": "DISABLED"}
                return httpx.Response(201, json={"id": cid, "revision": {"version": 1}, "component": component})
        if self.gateway:
            return httpx.Response(self.gateway, text="upstream timed out")
        raise httpx.ReadTimeout("timed out", request=request)


async def _apply_with_timeouts(transport: TimesOut, spec: dict[str, Any], parent: str | None = None) -> dict[str, Any]:
    from conftest import settings

    from nifi_mcp.client import NiFiClient

    client = NiFiClient(settings(), transport=transport)
    try:
        return await apply_flow_spec(client, spec, parent_process_group_id=parent)
    finally:
        await client.aclose()


_READER = {"type": "controller_service", "service_type": "x.Reader", "name": "Reader"}


async def test_enable_timeout_is_unknown_and_the_hint_disables_before_deleting() -> None:
    # The enable PUT may have landed; NiFi will not delete a group holding an enabled service.
    transport = TimesOut("/process-groups", "/controller-services")
    result = await _apply_with_timeouts(transport, {"process_group": {"name": "g"}, "objects": [_READER]})
    assert result["cause"] == "nifi"
    service = result["created"][1]
    assert (service["kind"], service["state"]) == ("controller_service", "unknown")
    hint = result["hint"]
    assert "may have applied" in hint
    assert "Disable" in hint and service["id"] in hint
    assert hint.index("Disable") < hint.index("delete the group")


async def test_group_create_timeout_never_says_nothing_was_created() -> None:
    transport = TimesOut()
    result = await _apply_with_timeouts(transport, {"process_group": {"name": "g"}, "objects": [_READER]})
    assert "POST /nifi-api/process-groups/root/process-groups" in transport.sent
    assert result["created"] == [
        {"kind": "process_group", "id": None, "ref": "process_group", "state": "unknown", "outcome": "unknown"}
    ]
    hint = result["hint"]
    assert "Nothing was created" not in hint
    assert "nifi_get_flow on root" in hint
    assert "do not apply the same spec again until you have checked" in hint


async def test_create_timeout_in_an_existing_group_never_names_a_component_to_delete() -> None:
    # A component called Reader may have been there before; only an id ties one to this call.
    transport = TimesOut()
    result = await _apply_with_timeouts(transport, {"objects": [_READER]}, parent=_TIMEOUT_PG)
    assert result["created"] == [
        {"kind": "controller_service", "id": None, "ref": "objects[0]", "state": "unknown", "outcome": "unknown"}
    ]
    hint = result["hint"]
    assert "Nothing was created" not in hint
    assert "do not delete it" in hint
    assert "'Reader' if it exists" not in hint
    assert "delete only" not in hint
    assert "was not there before this call" in hint


async def test_group_create_timeout_never_says_delete_a_group_by_name() -> None:
    # A group called ingest may already exist in root, with a whole flow inside it.
    result = await _apply_with_timeouts(TimesOut(), {"process_group": {"name": "ingest"}, "objects": []})
    assert result["created"][0]["id"] is None
    hint = result["hint"]
    assert "delete" not in hint.lower(), hint
    assert "nifi_get_flow on root" in hint
    assert "was not there before this call" in hint


async def test_known_ids_are_still_listed_next_to_an_unknown_create_in_an_existing_group() -> None:
    transport = TimesOut("/controller-services")
    spec = {"objects": [_READER, {**_READER, "name": "Writer"}]}
    result = await _apply_with_timeouts(transport, spec, parent=_TIMEOUT_PG)
    created = result["created"]
    assert [item["ref"] for item in created] == ["objects[0]", "objects[1]"]
    hint = result["hint"]
    assert f"controller_service {created[0]['id']}" in hint
    assert "'Writer'" not in hint.split("delete only", 1)[-1]


@pytest.mark.parametrize("status", [502, 504])
async def test_gateway_error_on_enable_is_unknown_like_a_timeout(status: int) -> None:
    # A proxy 502/504 says nothing about whether NiFi applied the enable.
    transport = TimesOut("/process-groups", "/controller-services", gateway=status)
    result = await _apply_with_timeouts(transport, {"process_group": {"name": "g"}, "objects": [_READER]})
    service = result["created"][1]
    assert service["state"] == "unknown", service
    hint = result["hint"]
    assert "Disable" in hint and service["id"] in hint
    assert hint.index("Disable") < hint.index("delete the group")
    assert "may have been applied" in result["error"]


@pytest.mark.parametrize("status", [502, 504])
async def test_gateway_error_on_group_create_is_unknown_like_a_timeout(status: int) -> None:
    transport = TimesOut(gateway=status)
    result = await _apply_with_timeouts(transport, {"process_group": {"name": "g"}, "objects": []})
    assert result["created"] == [
        {"kind": "process_group", "id": None, "ref": "process_group", "state": "unknown", "outcome": "unknown"}
    ]
    hint = result["hint"]
    assert "Nothing was created" not in hint
    assert "do not apply the same spec again until you have checked" in hint


_BAD_IDS = ["canary-1234", "../../controller", "root/../x", ""]


@pytest.mark.parametrize("bad", [bad for bad in _BAD_IDS if bad])
async def test_spec_parent_id_that_is_not_a_nifi_id_is_refused_before_any_request(bad: str) -> None:
    # The spec field went on the wire and came back in the error and process_group_id.
    transport = TimesOut()
    spec = {"parent_process_group_id": bad, "process_group": {"name": "g"}, "objects": []}
    result = await _apply_with_timeouts(transport, spec)
    assert transport.sent == []
    assert result["cause"] == "spec"
    assert "parent_process_group_id" in result["error"]
    assert bad not in result["error"]
    assert result["process_group_id"] is None
    assert bad not in json.dumps(result)
    assert result["hint"].startswith("Nothing was created")


@pytest.mark.parametrize("bad", [bad for bad in _BAD_IDS if bad])
async def test_spec_parameter_context_id_that_is_not_a_nifi_id_is_refused_before_any_request(bad: str) -> None:
    transport = TimesOut()
    spec = {"process_group": {"name": "g", "parameter_context_id": bad}, "objects": []}
    result = await _apply_with_timeouts(transport, spec)
    assert transport.sent == []
    assert result["cause"] == "spec"
    assert "process_group.parameter_context_id" in result["error"]
    assert bad not in json.dumps(result)


async def test_spec_parent_id_accepts_root_and_a_uuid() -> None:
    for good in ("root", _TIMEOUT_PG):
        transport = TimesOut("/process-groups")
        spec = {"parent_process_group_id": good, "process_group": {"name": "g"}}
        result = await _apply_with_timeouts(transport, spec)
        assert result["status"] == "ok", result
        assert f"POST /nifi-api/process-groups/{good}/process-groups" in transport.sent


async def test_spec_connection_carries_queue_settings_to_nifi() -> None:
    # A spec can bound a queue and expire FlowFiles without a second tool call.
    client = FakeClient()
    spec = {
        "process_group": {"name": "g"},
        "objects": [
            {"type": "processor", "processor_type": "x.Gen", "name": "Gen"},
            {"type": "processor", "processor_type": "x.Log", "name": "Log", "auto_terminated": "success"},
            {
                "type": "connection",
                "source": "Gen",
                "target": "Log",
                "relationships": "success",
                "back_pressure_object_threshold": "5",
                "back_pressure_data_size_threshold": "10 MB",
                "flow_file_expiration": "1 min",
            },
        ],
    }
    result = await apply_flow_spec(client, spec)  # type: ignore[arg-type]
    assert result["status"] == "ok", result
    assert "warnings" not in result or not any("ignored unknown keys" in w for w in result["warnings"])
    kwargs = client.connection_kwargs[0]
    assert kwargs["back_pressure_object_threshold"] == 5
    assert kwargs["back_pressure_data_size_threshold"] == "10 MB"
    assert kwargs["flow_file_expiration"] == "1 min"


@pytest.mark.parametrize(
    ("key", "value", "message"),
    [
        ("back_pressure_object_threshold", "canary-1234", "must be a whole number"),
        ("back_pressure_object_threshold", -1, "must not be negative"),
        ("back_pressure_object_threshold", True, "must be a whole number"),
        ("back_pressure_data_size_threshold", 10, "must be a string"),
        # DataUnit.parseDataSize echoes a bad size in its 400 body, after NiFi applied the rest.
        ("back_pressure_data_size_threshold", "canary-1234", "must be a data size"),
        ("back_pressure_data_size_threshold", "10 XB canary-1234", "must be a data size"),
        ("flow_file_expiration", {"raw": "canary-1234"}, "must be a string"),
    ],
)
async def test_spec_queue_setting_of_the_wrong_type_is_refused_before_any_create(
    key: str, value: Any, message: str
) -> None:
    client = RecordingClient()
    objects = [_P, _Q, {"type": "connection", "source": "P", "target": "Q", "relationships": "success", key: value}]
    result = await apply_flow_spec(client, {"process_group": {"name": "g"}, "objects": objects})  # type: ignore[arg-type]
    assert (client.calls, result["cause"]) == ([], "spec"), result
    assert key in result["error"] and message in result["error"]
    assert "canary-1234" not in json.dumps(result)


@pytest.mark.parametrize(
    ("spec", "where", "keys"),
    [
        (
            {"process_group": {"name": "g", "parameter_context": _CTX, "comment": "typo"}},
            "process_group",
            "comment, parameter_context",
        ),
        ({"process_group": {"name": "g"}, "layot": "manual"}, "spec", "layot"),
        ({"create_process_group": {"name": "g", "parameterContextId": _CTX}}, "process_group", "parameterContextId"),
    ],
)
async def test_an_unknown_process_group_or_top_level_key_is_refused_before_any_create(
    spec: dict[str, Any], where: str, keys: str
) -> None:
    # A misspelt parameter_context_id built the group unbound and reported status ok.
    client = RecordingClient()
    result = await apply_flow_spec(client, {**spec, "objects": [_P]})  # type: ignore[arg-type]
    assert (client.calls, result["cause"], result["created"]) == ([], "spec", []), result
    assert f"{where} has unknown keys {keys}" in result["error"]
    assert _CTX not in json.dumps(result)


async def test_every_documented_process_group_and_top_level_key_is_accepted() -> None:
    client = FakeClient()
    spec = {
        "parent_process_group_id": "root",
        "layout": "manual",
        "process_group": {
            "name": "g",
            "comments": "c",
            "parameter_context_id": _CTX,
            "inherit_parameter_context": False,
            "position": {"x": 0, "y": 0},
        },
        "objects": [_P],
        "connections": [],
    }
    result = await apply_flow_spec(client, spec)  # type: ignore[arg-type]
    assert result["status"] == "ok", result


class BackEdgeClient(FakeClient):
    """A -> B -> C with a retry line C -> A back up the axis, through B."""

    async def get_flow(self, process_group_id: str) -> dict[str, Any]:
        def proc(cid: str) -> dict[str, Any]:
            component = {"id": cid, "name": cid, "position": {"x": 0, "y": 0}}
            return {"id": cid, "revision": {"version": 1}, "component": component}

        def conn(cid: str, src: str, dst: str) -> dict[str, Any]:
            return {
                "id": cid,
                "revision": {"version": 1},
                "component": {"id": cid, "source": {"id": src}, "destination": {"id": dst}},
            }

        flow = {
            "processors": [proc("A"), proc("B"), proc("C")],
            "connections": [conn("ab", "A", "B"), conn("bc", "B", "C"), conn("ca", "C", "A")],
        }
        return {"processGroupFlow": {"id": process_group_id, "flow": flow}}


@pytest.mark.asyncio
async def test_relayout_warns_when_a_connection_still_crosses_a_card() -> None:
    result = await relayout_process_group(BackEdgeClient(), "pg-1")  # type: ignore[arg-type]
    assert result["status"] == "ok"
    assert len(result["warnings"]) == 1
    assert "C->A" in result["warnings"][0]
    assert "B at (0.0, 240.0)" in result["warnings"][0]


@pytest.mark.asyncio
async def test_relayout_has_no_warnings_when_the_check_passes() -> None:
    result = await relayout_process_group(FakeClient(), "pg-1")  # type: ignore[arg-type]
    assert "warnings" not in result
