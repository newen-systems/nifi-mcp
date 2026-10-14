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
    configure(client, settings())
    result = json.loads(await nifi_review_flow_design(ReviewDesignIn(
        required_fields=["digest", "registry", "credential_ref"], supplied_fields=["registry"],
        runtime_fields=["credential_ref"], spec={"objects": []},
    )))
    assert result["input_contract"]["missing_fields"] == ["digest"]
    assert result["input_contract"]["status"] == "incomplete"
    assert result["proposed_design_review"]["processor_count"] == 0
    assert router.calls == [("GET", "/flow/process-groups/root")]


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
    result = await validate_after_action(client, {"root": True}, auto_layout=False)
    assert result["ok"] is True
    assert result["design_review"]["root"]["findings"][0]["kind"] == "consolidation_candidate"
    assert all(method == "GET" for method, _ in router.calls)


def test_methodology_reaches_instructions_and_builder_prompt():
    assert DESIGN_METHOD in INSTRUCTIONS
    assert DESIGN_METHOD in nifi_flow_builder()
