"""Persistent layout intent must survive compact reads, later repair, and process restarts."""
from copy import deepcopy
from unittest.mock import AsyncMock

import pytest
from pydantic import ValidationError

from nifi_mcp.flow_spec import (
    _mapped_layout_intent,
    _move_remote_groups,
    load_layout_intent,
    normalise_spec,
    persist_layout_intent,
    relayout_process_group,
)
from nifi_mcp.ordering import read_intent, write_intent
from nifi_mcp.server import LayoutGroupIn
from nifi_mcp.validation import _back_edge_issues


@pytest.mark.asyncio
async def test_intent_uses_full_owning_comments_and_fresh_revision():
    prose = "Operator documentation. " * 50
    client = AsyncMock()
    client.get_process_group.return_value = {
        "component": {"id": "actual-root", "comments": prose}, "revision": {"version": 19},
    }
    intent = {"stages": [["low"], ["high"]]}
    await persist_layout_intent(client, "root", intent)
    call = client.update_process_group.call_args
    assert call.args == ("actual-root",)
    assert call.kwargs["version"] == 19
    assert call.kwargs["comments"].startswith(prose.rstrip())
    assert read_intent(call.kwargs["comments"]) == intent
    client.get_process_group.return_value["component"]["comments"] = call.kwargs["comments"]
    outline = await load_layout_intent(client, "root", {"id": "actual-root"})
    assert outline["layout_intent"] == intent
    client.update_process_group.reset_mock()
    await persist_layout_intent(client, "root", intent)
    client.update_process_group.assert_not_called()


@pytest.mark.asyncio
async def test_malformed_saved_intent_is_not_silently_discarded():
    client = AsyncMock()
    client.get_process_group.return_value = {"component": {"comments": "nifi-layout: broken JSON"}}
    with pytest.raises(ValueError, match="Invalid nifi-layout marker in comments on canvas root"):
        await load_layout_intent(client, "root", {})
    client.update_process_group.assert_not_called()


def test_layout_intent_schema_rejects_typos_before_mutation():
    with pytest.raises(ValidationError):
        LayoutGroupIn(layout_intent={"dependancies": []})
    with pytest.raises(ValidationError):
        normalise_spec({"layout_intent": {"dependancies": []}})


def test_build_maps_every_intent_reference_to_created_id():
    intent = {
        "dependencies": [{"source": "A", "target": "B"}],
        "stages": [["A"], ["B"]], "feedback": [{"source": "B", "target": "A"}],
        "areas": [{"id": "flow", "members": ["A", "B"], "independent": True}],
        "roles": {"A": "low", "B": "high"}, "domain": "diode-low-to-high",
    }
    before = deepcopy(intent)
    mapped = _mapped_layout_intent(intent, {"A": {"id": "id-a"}, "B": {"id": "id-b"}})
    assert mapped["stages"] == [["id-a"], ["id-b"]]
    assert mapped["dependencies"] == [{"source": "id-a", "target": "id-b"}]
    assert mapped["feedback"] == [{"source": "id-b", "target": "id-a"}]
    assert mapped["areas"][0]["members"] == ["id-a", "id-b"]
    assert mapped["roles"] == {"id-a": "low", "id-b": "high"}
    assert intent == before


@pytest.mark.asyncio
async def test_remote_group_move_is_position_only_with_fresh_revision():
    from unittest.mock import Mock

    client = AsyncMock()
    client.get_json.return_value = {"revision": {"version": 42}, "component": {"transmitting": True}}
    client._revision = Mock(side_effect=lambda v: {"version": v, "clientId": "test"})
    client._ack = Mock(return_value=False)
    outline = {"remote_process_groups": [{"id": "remote", "position": {"x": 0, "y": 0}}]}
    moved = await _move_remote_groups(client, outline, {"remote": (16, 480)})
    assert moved[0]["kind"] == "remote_process_group"
    body = client.json.call_args.kwargs["json_body"]
    assert body["revision"]["version"] == 42
    assert body["component"] == {"id": "remote", "position": {"x": 16, "y": 480}}
    client.json.reset_mock()
    await _move_remote_groups(client, outline, {"remote": (0, 0)})
    client.json.assert_not_called()


def test_order_checks_same_row_and_clearance_with_logical_dependency():
    for y in (0, 128, 232):
        cards = {i: {"id": i, "position": {"x": x, "y": top}} for i, x, top in (("a", 0, 0), ("b", 512, y))}
        outline = {"id": "root", "processors": list(cards.values()),
                   "layout_intent": {"dependencies": [{"source": "a", "target": "b"}]}}
        assert _back_edge_issues(outline, cards)
    cards["b"]["position"]["y"] = 240
    assert _back_edge_issues(outline, cards) == []


@pytest.mark.asyncio
async def test_relayout_checks_saved_state_not_requested_positions(monkeypatch):
    import nifi_mcp.flow_spec as module

    intent = {"dependencies": [{"source": "a", "target": "b"}]}
    outline = {"id": "root", "layout_intent": intent, "processors": [
        {"id": "a", "position": {"x": 0, "y": 0}},
        {"id": "b", "position": {"x": 512, "y": 0}},
    ]}
    hierarchy = AsyncMock(return_value=({"root": outline}, {}, {"root": "root"}))
    monkeypatch.setattr(module, "read_layout_hierarchy", hierarchy)
    reads = AsyncMock(return_value=outline)
    monkeypatch.setattr(module, "load_layout_intent", reads)
    monkeypatch.setattr(module, "positions_from_flow", lambda _: {"a": (0, 0), "b": (0, 240)})
    monkeypatch.setattr(module, "_move_processors", AsyncMock(return_value=[]))
    client = AsyncMock()
    client.get_flow.return_value = {"processGroupFlow": {"id": "root", "flow": {}}}
    result = await relayout_process_group(client, "root")
    assert reads.await_count == 2
    assert any(issue["kind"] == "back_edge" for issue in result["layout_issues"])


def test_marker_replacement_preserves_prose():
    comments = 'First paragraph.\nnifi-layout: {"stages":[["old"]]}\nLast paragraph.'
    changed = write_intent(comments, {"stages": [["new"]]})
    assert "First paragraph.\nLast paragraph." in changed
    assert changed.count("nifi-layout: ") == 1
    assert read_intent(changed) == {"stages": [["new"]]}


def test_cycle_validation_exempts_only_identified_feedback():
    cards = {i: {"id": i, "position": {"x": 0, "y": y}} for i, y in (("a", 480), ("b", 240), ("c", 0))}
    flow = {"id": "root", "processors": list(cards.values()), "connections": [
        {"source": {"id": s}, "destination": {"id": d}} for s, d in (("a", "b"), ("b", "c"), ("c", "a"))
    ], "layout_intent": {"feedback": [{"source": "c", "target": "a"}]}}
    issues = _back_edge_issues(flow, cards)
    assert {i["component_id"] for i in issues if i["kind"] == "back_edge"} == {"b", "c"}


def test_explicit_conflict_is_reported_separately_from_uncertainty():
    cards = {i: {"id": i, "position": {"x": 0, "y": y}} for i, y in (("a", 0), ("b", 240))}
    flow = {"id": "root", "processors": list(cards.values()),
            "connections": [{"source": {"id": "a"}, "destination": {"id": "b"}}],
            "layout_intent": {"dependencies": [{"source": "b", "target": "a"}]}}
    assert any(i["kind"] == "layout_conflict" for i in _back_edge_issues(flow, cards))


@pytest.mark.asyncio
async def test_hierarchy_reads_each_canvas_once_and_keeps_raw_for_validation():
    from nifi_mcp.flow_spec import read_layout_hierarchy

    client = AsyncMock()
    flows = {
        "parent": {"processGroupFlow": {"id": "parent", "flow": {
            "processGroups": [{"id": "child", "component": {"id": "child"}}],
        }}},
        "child": {"processGroupFlow": {"id": "child", "parentGroupId": "parent", "flow": {
            "processors": [{"id": "p", "component": {"id": "p", "validationStatus": "INVALID"}}],
        }}},
    }
    client.get_flow.side_effect = lambda gid: deepcopy(flows["parent" if gid == "root" else gid])
    outlines, raw, aliases = await read_layout_hierarchy(client, ["root", "child"])
    assert client.get_flow.await_count == 2
    assert aliases["root"] == "parent"
    assert aliases["child"] == "child"
    assert set(outlines) == {"parent", "child"}
    assert "port_context" in outlines["child"]
    assert raw["child"]["processGroupFlow"]["flow"]["processors"][0]["component"]["validationStatus"] == "INVALID"


@pytest.mark.asyncio
async def test_unreadable_requested_canvas_is_not_a_successful_empty_layout():
    from nifi_mcp.flow_spec import read_layout_hierarchy

    client = AsyncMock()
    client.get_flow.side_effect = RuntimeError("denied")
    with pytest.raises(ValueError, match="Cannot read requested layout canvas"):
        await read_layout_hierarchy(client, ["root"])


@pytest.mark.asyncio
async def test_flow_spec_persists_mapped_intent_after_build_and_relayouts(monkeypatch):
    from test_flow_spec import FakeClient

    import nifi_mcp.flow_spec as module

    persist = AsyncMock()
    layout = AsyncMock(return_value={"layout_issues": [], "warnings": []})
    monkeypatch.setattr(module, "persist_layout_intent", persist)
    monkeypatch.setattr(module, "relayout_process_group", layout)
    client = FakeClient()
    result = await module.apply_flow_spec(client, {
        "process_group": {"name": "Pipeline"},
        "objects": [
            {"type": "processor", "name": name, "processor_type": "example.Processor"}
            for name in ("Receive", "Deliver")
        ],
        "layout_intent": {"stages": [["Receive"], ["Deliver"]]},
    })
    assert result["status"] == "ok", result
    names = result["name_map"]
    assert persist.call_args.args[1] == result["process_group_id"]
    assert persist.call_args.args[2]["stages"] == [[names["Receive"]], [names["Deliver"]]]
    layout.assert_awaited_once_with(client, result["process_group_id"], skip_unchanged=True)
    assert client.connection_kwargs == []  # A logical handoff never becomes an executable connection.


@pytest.mark.asyncio
async def test_multiple_requested_roots_share_one_global_read_budget():
    from nifi_mcp.flow_spec import read_layout_hierarchy

    client = AsyncMock()
    client.get_flow.side_effect = lambda gid: {"processGroupFlow": {"id": gid, "flow": {}}}
    with pytest.raises(ValueError, match="Cannot read requested layout canvas"):
        await read_layout_hierarchy(client, ["one", "two", "three"], max_groups=2)
    assert client.get_flow.await_count == 2


@pytest.mark.asyncio
async def test_topology_change_discards_stale_feedback_context(monkeypatch):
    import nifi_mcp.flow_spec as module

    cards = [{"id": name, "position": {"x": 0, "y": y}} for name, y in (("a", 0), ("b", 240))]
    connections = [{"id": src + dst, "source": {"id": src}, "destination": {"id": dst}}
                   for src, dst in (("a", "b"), ("b", "a"))]
    original = {"id": "root", "processors": cards, "connections": connections,
                "port_context": {"forward": [["a", "b"]], "feedback": [["b", "a"]]}}
    saved = {"id": "root", "processors": deepcopy(cards), "connections": [connections[1]]}
    client = AsyncMock()
    client.get_flow.return_value = {"processGroupFlow": {"id": "root", "flow": {}}}
    monkeypatch.setattr(module, "load_layout_intent", AsyncMock(side_effect=[original, saved]))
    for name in ("_move_processors", "_move_ports", "_move_funnels", "_move_process_groups", "_move_remote_groups"):
        monkeypatch.setattr(module, name, AsyncMock(return_value=[]))
    monkeypatch.setattr(module, "_route_connections", AsyncMock(return_value=([], [])))
    result = await module.relayout_process_group(client, "root", _outlines={"root": original})
    assert any(i["kind"] == "layout_conflict" and "topology changed" in i["message"] for i in result["layout_issues"])
    assert any(i["kind"] == "back_edge" for i in result["layout_issues"])
    assert "port_context" not in saved
