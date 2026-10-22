"""Proper line crossings are visible advisories; collisions still invalidate saved geometry."""
from copy import deepcopy
from unittest.mock import AsyncMock

import pytest

from nifi_mcp.validation import MAX_ISSUES, _summary, connection_layout_issues, layout_issues


def _crossing_canvas():
    return {
        "id": "root", "processors": [
            {"id": cid, "position": {"x": x, "y": y}}
            for cid, x, y in (("a", 0, 0), ("b", 1000, 0), ("c", 0, 1000), ("d", 1000, 1000))
        ],
        "connections": [
            {"id": "ad", "source": {"id": "a"}, "destination": {"id": "d"},
             "bends": [{"x": 176, "y": 304}, {"x": 1176, "y": 704}], "label_index": 0},
            {"id": "bc", "source": {"id": "b"}, "destination": {"id": "c"},
             "bends": [{"x": 1176, "y": 304}, {"x": 176, "y": 704}], "label_index": 0},
        ],
    }


def _summarize(flow, issues=None):
    return _summary({"issues": layout_issues(flow) if issues is None else issues,
                     "groups": {flow["id"]: flow}, "cut": False}, [], [])


def test_saved_proper_crossing_is_reported_without_invalidating_layout():
    flow = _crossing_canvas()
    findings = connection_layout_issues(flow)
    assert findings
    assert all(i["kind"] == "connection_crossing" for i in findings)
    assert all(i["severity"] == "advisory" for i in findings)
    summary = _summarize(flow)
    assert summary["ok"] is True
    assert summary["issues"] == []
    assert summary["layout_advisories"] == findings


def test_crossing_advisory_does_not_hide_a_card_collision():
    flow = _crossing_canvas()
    flow["labels"] = [{"id": "note", "position": {"x": 560, "y": 440}, "width": 240, "height": 128}]
    summary = _summarize(flow)
    assert summary["ok"] is False
    assert summary["layout_advisories"]
    assert any(i["kind"] == "connection_geometry" and "crosses note" in i["message"] for i in summary["issues"])


@pytest.mark.parametrize("message", [
    "connection a->b crosses the label of connection c->d",
    "connection a->b runs along connection c->d from (0, 0) to (10, 10)",
    "label a->b at (0, 0) overlaps label c->d at (0, 0)",
])
def test_other_geometry_is_never_downgraded(monkeypatch, message):
    import nifi_mcp.validation as module

    monkeypatch.setattr(module, "layout_problems", lambda boxes, pairs, routes: [message] if pairs else [])
    summary = _summarize(_crossing_canvas())
    assert summary["ok"] is False
    assert summary["issues"][0]["kind"] == "connection_geometry"
    assert "layout_advisories" not in summary


def test_advisory_limit_does_not_consume_error_budget():
    advisory = {"kind": "connection_crossing", "severity": "advisory", "message": "crossing"}
    error = {"kind": "connection_geometry", "message": "collision"}
    result = _summarize(_crossing_canvas(), [advisory] * (MAX_ISSUES + 2) + [error])
    assert result["issues"] == [error]
    assert result["ok"] is False
    assert len(result["layout_advisories"]) == MAX_ISSUES
    assert result["layout_advisories_not_shown"] == 2


@pytest.mark.asyncio
async def test_relayout_and_recursive_children_expose_advisories_separately(monkeypatch):
    import nifi_mcp.flow_spec as module

    root = {"id": "root", "process_groups": [{"id": "child", "position": {"x": 0, "y": 0}}]}
    child = {**_crossing_canvas(), "id": "child"}
    outlines = {"root": root, "child": child}

    async def intent(client, gid, outline):
        return deepcopy(outlines[gid])

    monkeypatch.setattr(module, "load_layout_intent", intent)
    monkeypatch.setattr(module, "positions_from_flow", lambda _: {})
    for name in ("_move_processors", "_move_ports", "_move_funnels", "_move_process_groups", "_move_remote_groups"):
        monkeypatch.setattr(module, name, AsyncMock(return_value=[]))
    monkeypatch.setattr(module, "_route_connections", AsyncMock(return_value=([], [])))
    client = AsyncMock()
    client.get_flow.return_value = {"processGroupFlow": {"flow": {}}}
    result = await module.relayout_process_group(client, "root", recursive=True, _outlines=outlines)
    assert result["layout_issues"] == result["layout_advisories"] == []
    assert result["child_groups"][0]["layout_issues"] == []
    assert result["child_groups"][0]["layout_advisories"][0]["kind"] == "connection_crossing"


@pytest.mark.asyncio
async def test_crossing_alone_does_not_generate_a_failed_layout_warning(monkeypatch):
    import nifi_mcp.flow_spec as module

    flow = _crossing_canvas()
    routes = [(c["bends"], c["label_index"]) for c in flow["connections"]]
    monkeypatch.setattr(module, "route_connections", lambda boxes, pairs: routes)
    _, warnings = await module._route_connections(AsyncMock(), "root", flow, {}, skip_unchanged=True)
    assert warnings == []
