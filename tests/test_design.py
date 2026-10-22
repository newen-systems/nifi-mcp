"""Design review is read-only, conservative and shared by pre/post checks."""

import json

import pytest
from conftest import settings

from nifi_mcp.design import DESIGN_METHOD, review_graph, review_spec
from nifi_mcp.flow_spec import normalise_spec, place_card_why
from nifi_mcp.server import INSTRUCTIONS, ReviewDesignIn, configure, nifi_flow_builder, nifi_review_flow_design
from nifi_mcp.validation import validate_after_action


def test_spec_aliases_use_structural_refs_and_never_echo_property_values():
    spec = {
        "objects": [
            {"kind": "processor", "name": "secret-name", "class": "org.apache.nifi.UpdateAttribute",
             "properties": {"token": "secret-value"}},
            {"type": "processor", "name": "b", "processor_type": "org.apache.nifi.UpdateAttribute"},
        ],
        "connections": [{"from": "secret-name", "to": "b", "relationships": "success"}],
    }
    result = review_spec(*normalise_spec(spec))
    assert result["findings"][0]["components"] == ["objects[0]", "objects[1]"]
    assert "secret" not in json.dumps(result)
    assert result["status"] == "review_required"


def test_cycles_duplicates_and_alternative_relationships_are_not_claimed_as_bottlenecks():
    outline = {
        "processors": [{"id": "a", "type": "UpdateAttribute"}, {"id": "b", "type": "InvokeHTTP"}],
        "connections": [
            {"source": {"id": a}, "destination": {"id": b}}
            for a, b in [("a", "a"), ("a", "b"), ("a", "b"), ("a", "port"), ("b", "a")]
        ],
    }
    result = review_graph(outline)
    assert len(result["findings"]) == 1
    assert result["findings"][0]["fan_out"] == 2
    assert "alternatives" in result["findings"][0]["message"]
    assert review_graph({})["status"] == "review_required"


async def test_review_reads_canvas_and_reports_missing_contract_fields_without_writes(client, router):
    router.json("GET", "/flow/process-groups/root", {"processGroupFlow": {"id": "root", "flow": {}}})
    router.json("GET", "/process-groups/root", {"component": {"id": "root", "comments": ""}})
    configure(client, settings())
    result = json.loads(await nifi_review_flow_design(ReviewDesignIn(
        required_fields=["digest", "registry", "credential_ref"], supplied_fields=["registry"],
        runtime_fields=["credential_ref"], spec={"objects": []},
    )))
    assert result["input_contract"]["missing_fields"] == ["digest"]
    assert result["input_contract"]["status"] == "incomplete"
    assert result["proposed_design_review"]["processor_count"] == 0
    assert router.calls == [("GET", "/flow/process-groups/root"), ("GET", "/process-groups/root")]


@pytest.mark.parametrize("parent", ["root", "00000000-0000-0000-0000-000000000001"])
async def test_new_root_group_fills_grid_gap_despite_arbitrary_coordinates(client, router, parent):
    router.json("GET", f"/flow/process-groups/{parent}", {"processGroupFlow": {
        "id": parent, "flow": {"processGroups": [
            {"id": str(i), "component": {"position": {"x": 800, "y": y}}}
            for i, y in enumerate([400, 688, 1264])
        ]},
    }})
    point, note, reason = await place_card_why(client, parent, "process_group", 9000, 9000)
    assert point == (800, 976)
    assert "ignored" in note
    assert reason
    assert len(router.calls) == 1


async def test_non_root_explicit_position_is_preserved(client, router):
    router.json("GET", "/flow/process-groups/group", {"processGroupFlow": {
        "id": "group", "parentGroupId": "parent", "flow": {},
    }})
    point, note, _ = await place_card_why(client, "group", "process_group", 800, 800)
    assert point == (800, 800)
    assert note is None


async def test_post_validation_includes_design_candidates_without_invalidating_nifi(client, router):
    router.json("GET", "/flow/process-groups/root", {"processGroupFlow": {
        "id": "root", "flow": {
            "processors": [{"id": str(i), "component": {
                "type": "org.apache.nifi.UpdateAttribute", "position": {"x": 0, "y": i * 240},
            }} for i in range(2)],
            "connections": [{"component": {"source": {"id": "0"}, "destination": {"id": "1"}}}],
        },
    }})
    router.json("GET", "/flow/process-groups/root/controller-services", {"controllerServices": []})
    router.json("GET", "/process-groups/root", {"component": {"id": "root", "comments": ""}})
    result = await validate_after_action(client, {"root": True}, auto_layout=False)
    assert result["ok"] is True
    assert result["design_review"]["root"]["findings"][0]["kind"] == "consolidation_candidate"
    assert all(method == "GET" for method, _ in router.calls)


def test_methodology_reaches_instructions_and_builder_prompt():
    assert DESIGN_METHOD in INSTRUCTIONS
    assert DESIGN_METHOD in nifi_flow_builder()


async def test_design_review_reports_saved_connection_overlap_before_any_edits(client, router):
    router.json("GET", "/flow/process-groups/root", {"processGroupFlow": {
        "id": "root", "flow": {
            "processors": [{"id": str(i), "component": {"position": {"x": 0, "y": i * 240}}} for i in range(2)],
            "connections": [{"id": str(i), "component": {
                "source": {"id": source}, "destination": {"id": target}, "bends": [],
            }} for i, (source, target) in enumerate([("0", "1"), ("1", "0")])],
        },
    }})
    router.json("GET", "/process-groups/root", {"component": {"id": "root", "comments": ""}})
    configure(client, settings())
    result = json.loads(await nifi_review_flow_design(ReviewDesignIn()))
    overlap = next(i for i in result["layout_issues"] if i["kind"] == "overlapping_connections")
    assert overlap["connection_ids"] == ["0", "1"]
    assert router.calls == [("GET", "/flow/process-groups/root"), ("GET", "/process-groups/root")]


def test_instructions_teach_context_sensitive_relationship_names():
    assert "Name relationships and queues for what they represent" in INSTRUCTIONS
    assert "No fixed list of names" in INSTRUCTIONS


async def test_processor_definition_returns_every_relationship_description(client, router):
    from nifi_mcp.server import ProcessorDefinitionIn, nifi_get_processor_definition

    long = "Requests that have not been verified yet. " * 20
    router.json("GET", "/flow/processor-definition/g/a/1/T", {"supportedRelationships": [
        {"name": "unverified", "description": long, "autoTerminated": False},
        {"name": "complete", "description": "Done."},
    ]})
    router.json("GET", "/process-groups/root", {"component": {"id": "root", "comments": ""}})
    configure(client, settings())
    data = json.loads(await nifi_get_processor_definition(ProcessorDefinitionIn(
        group="g", artifact="a", version="1", type_name="T")))
    assert {r["name"]: r["description"] for r in data["supportedRelationships"]} == {
        "unverified": long, "complete": "Done."}


def test_instructions_reserve_nifi_built_in_names():
    for word in ("retries_exceeded", "absolute.path", "FlowFileTransform", "mime.type"):
        assert word in INSTRUCTIONS


def test_instructions_end_parking_queues_at_log_message():
    assert "LogMessage" in INSTRUCTIONS
    assert "never UpdateAttribute" in INSTRUCTIONS
    assert "LogMessage" in DESIGN_METHOD


def _transfer_loop(retry_limit, waiting_limit):
    """The live topology: RouteTransfer -> RetryTransfer 'retryable, waiting' and back on 'retry'."""
    return {
        "processors": [
            {"id": "route", "name": "RouteTransfer", "type": "x.RouteOnAttribute"},
            {"id": "retry", "name": "RetryTransfer", "type": "x.RetryFlowFile"},
            {"id": "log", "name": "LogTransfer", "type": "x.LogMessage"},
        ],
        "connections": [
            {"id": "c1", "name": "retryable, waiting", "source": {"id": "route"}, "destination": {"id": "retry"},
             "back_pressure_object_threshold": waiting_limit},
            {"id": "c2", "name": "retry", "source": {"id": "retry"}, "destination": {"id": "route"},
             "back_pressure_object_threshold": retry_limit},
            {"id": "c3", "name": "failed", "source": {"id": "retry"}, "destination": {"id": "log"},
             "back_pressure_object_threshold": 20},
        ],
    }


def _deadlocks(result):
    return [f for f in result["findings"] if f["kind"] == "backpressure_loop_deadlock"]


def test_live_retry_loop_with_low_thresholds_is_flagged_with_names_and_fix():
    (finding,) = _deadlocks(review_graph(_transfer_loop(20, 20)))
    assert sorted(finding["components"]) == ["c1", "c2", "retry", "route"]
    for text in ("RouteTransfer", "RetryTransfer", "'retryable, waiting'", "'retry'", "10000", "data size threshold"):
        assert text in finding["message"]
    assert "LogTransfer" not in finding["message"]


@pytest.mark.parametrize("retry_limit,waiting_limit", [(10000, 20), (20, 10000), (None, 20), (0, 20), (20, 25000)])
def test_loop_with_one_connection_at_default_or_above_is_not_flagged(retry_limit, waiting_limit):
    assert _deadlocks(review_graph(_transfer_loop(retry_limit, waiting_limit))) == []


def test_low_threshold_outside_a_loop_is_not_flagged():
    outline = _transfer_loop(20, 20)
    outline["connections"] = [c for c in outline["connections"] if c["id"] != "c2"]
    assert _deadlocks(review_graph(outline)) == []


def test_loop_through_a_port_and_a_self_loop_are_flagged_other_groups_children_are_not():
    def low(cid, source, dest):
        return {"id": cid, "name": cid, "source": {"id": source}, "destination": {"id": dest},
                "back_pressure_object_threshold": 5}

    outline = {
        "processors": [{"id": "a", "name": "A"}, {"id": "b", "name": "B"}],
        "output_ports": [{"id": "p", "name": "Out"}],
        "connections": [
            low("1", "a", "p"), low("2", "p", "a"), low("3", "b", "b"),
            low("4", "b", "child-port"), low("5", "child-port", "b"),
        ],
    }
    found = _deadlocks(review_graph(outline))
    assert [f["components"] for f in found] == [["a", "p", "1", "2"], ["b", "3"]]


def test_spec_with_low_threshold_on_every_loop_edge_reports_it_before_any_write():
    spec = {"objects": [
        {"type": "processor", "name": "Route", "processor_type": "x.A"},
        {"type": "processor", "name": "Retry", "processor_type": "x.B"},
        {"type": "connection", "source": "Route", "target": "Retry", "back_pressure_object_threshold": 20},
        {"type": "connection", "source": "Retry", "target": "Route", "back_pressure_object_threshold": "20"},
    ]}
    assert len(_deadlocks(review_spec(*normalise_spec(spec)))) == 1


def test_spec_loop_with_an_unset_edge_keeps_nifi_default_and_is_not_flagged():
    objects = [
        {"type": "processor", "name": "Route", "processor_type": "x.A"},
        {"type": "processor", "name": "Retry", "processor_type": "x.B"},
        {"type": "connection", "source": "Route", "target": "Retry", "back_pressure_object_threshold": 20},
        {"type": "connection", "source": "Retry", "target": "Route"},
    ]
    assert _deadlocks(review_spec(*normalise_spec({"objects": objects}))) == []


def test_instructions_tell_loops_to_keep_the_default_object_threshold():
    assert "keeps the default Back Pressure Object Threshold (10000)" in INSTRUCTIONS
