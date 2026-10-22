"""Ports and funnels are first-class cards: placed on the axis, checked, repaired and named."""

from __future__ import annotations

from typing import Any

import pytest

from nifi_mcp.design import review_graph
from nifi_mcp.layout import (
    AXIS_CENTRE,
    REAL_CARD_SIZES,
    layout_problems,
    outline_edges,
    positions_from_flow,
    real_boxes,
)
from nifi_mcp.validation import layout_issues


def _card(cid: str, x: float = 0, y: float = 0) -> dict[str, Any]:
    return {"id": cid, "name": cid, "position": {"x": x, "y": y}}


def _conn(src: str, dst: str, stype: str, rels: list[str], name: str | None = None) -> dict[str, Any]:
    return {
        "id": f"{src}-{dst}",
        "name": name,
        "source": {"id": src, "name": src, "type": stype},
        "destination": {"id": dst, "type": "PROCESSOR"},
        "relationships": rels,
    }


def _centre(placed: dict[str, tuple[float, float]], cid: str, kind: str) -> float:
    return placed[cid][0] + REAL_CARD_SIZES[kind][0] / 2


def test_port_feeding_a_processor_sits_on_the_processor_axis() -> None:
    flow = {
        "input_ports": [_card("FromDiode")],
        "processors": [_card("Route", 400, 0), _card("Store", 400, 240)],
        "connections": [_conn("FromDiode", "Route", "INPUT_PORT", []), _conn("Route", "Store", "PROCESSOR", ["ok"])],
    }
    placed = positions_from_flow(flow)
    assert _centre(placed, "FromDiode", "input_port") == AXIS_CENTRE
    assert _centre(placed, "Route", "processor") == AXIS_CENTRE
    assert placed["FromDiode"][1] < placed["Route"][1] < placed["Store"][1]
    assert layout_problems(real_boxes(flow, placed), outline_edges(flow)) == []


def test_funnel_joining_two_branches_is_placed_back_on_the_fork_axis() -> None:
    flow = {
        "processors": [_card("Split"), _card("A"), _card("B")],
        "funnels": [_card("Join")],
        "connections": [
            _conn("Split", "A", "PROCESSOR", ["a"]),
            _conn("Split", "B", "PROCESSOR", ["b"]),
            _conn("A", "Join", "PROCESSOR", ["success"]),
            _conn("B", "Join", "PROCESSOR", ["success"]),
        ],
    }
    for conn in flow["connections"]:
        if conn["destination"]["id"] == "Join":
            conn["destination"]["type"] = "FUNNEL"
    placed = positions_from_flow(flow)
    assert _centre(placed, "Join", "funnel") == _centre(placed, "Split", "processor")
    assert placed["Join"][1] > max(placed["A"][1], placed["B"][1])
    assert layout_problems(real_boxes(flow, placed), outline_edges(flow)) == []


def test_validation_flags_an_off_axis_port_and_the_layout_repairs_it() -> None:
    flow = {
        "id": "g",
        "input_ports": [_card("FromDiode", 0, 240)],
        "processors": [_card("Route", 432, 0)],
        "connections": [_conn("FromDiode", "Route", "INPUT_PORT", [])],
    }
    issues = layout_issues(flow)
    assert any(i["kind"] == "input_port_not_top" and i.get("component_id") == "FromDiode" for i in issues)
    placed = positions_from_flow(flow)
    repaired = {**flow, "input_ports": [_card("FromDiode", *placed["FromDiode"])],
                "processors": [_card("Route", *placed["Route"])]}
    assert not [i for i in layout_issues(repaired) if i["kind"] in {"off_grid", "overlap", "input_port_not_top"}]


def test_validation_includes_funnel_overlap_and_off_grid() -> None:
    flow = {"id": "g", "processors": [_card("P", 0, 0)], "funnels": [_card("F", 12, 4)]}
    kinds = {(i["kind"], i.get("component_id")) for i in layout_issues(flow)}
    assert ("overlap", "F") in kinds or ("overlap", "P") in kinds
    assert ("off_grid", "F") in kinds


def test_unnamed_connections_pass_review() -> None:
    flow = {
        "connections": [
            _conn("Route", "Store", "PROCESSOR", ["failure", "hash"]),
            _conn("FromDiode", "Route", "INPUT_PORT", []),
            _conn("Fun", "Route", "FUNNEL", []),
            _conn("Route", "Other", "PROCESSOR", ["failure", "hash"], ""),
            _conn("Route", "Same", "PROCESSOR", ["failure", "hash"], "hash, failure"),
        ]
    }
    assert [f for f in review_graph(flow)["findings"] if f["kind"] == "connection_name"] == []


def test_design_review_flags_a_name_that_contradicts_the_relationships() -> None:
    flow = {
        "connections": [
            _conn("Route", "Store", "PROCESSOR", ["failure", "hash"], "RouteToStore"),
            _conn("FromDiode", "Route", "INPUT_PORT", [], "FromDiodeToRoute"),
            _conn("Fun", "Route", "FUNNEL", [], "anything"),
        ]
    }
    names = [f for f in review_graph(flow)["findings"] if f["kind"] == "connection_name"]
    assert sorted(f["components"][0] for f in names) == ["FromDiode-Route", "Route-Store"]


def test_output_port_fed_by_a_wide_fork_is_not_flagged_where_the_layout_puts_it() -> None:
    # Live ImageTransferLowSide: ToDiode hangs off a processor with five outgoing connections. The layout
    # puts it one row down with the other leaves; validation must not flag a spot repair keeps.
    targets = [_card(f"T{i}") for i in range(4)]
    flow: dict[str, Any] = {
        "id": "g",
        "processors": [_card("RouteTransfer"), *targets],
        "output_ports": [_card("ToDiode")],
        "connections": [_conn("RouteTransfer", t["id"], "PROCESSOR", [t["id"]]) for t in targets]
        + [{**_conn("RouteTransfer", "ToDiode", "PROCESSOR", ["ready"]), "destination": {"id": "ToDiode"}}],
    }
    placed = positions_from_flow(flow)
    for key in ("processors", "output_ports"):
        flow[key] = [_card(c["id"], *placed[c["id"]]) for c in flow[key]]
    assert not [i for i in layout_issues(flow) if i.get("component_id") == "ToDiode"]
    flow["output_ports"] = [_card("ToDiode", 312, 0)]
    issues = layout_issues(flow)
    assert any(i["kind"] == "output_port_not_bottom" and i.get("component_id") == "ToDiode" for i in issues)


def _group(cid: str, y: float) -> dict[str, Any]:
    return _card(cid, -16, y)


def _group_conn(cid: str, src: str, dst: str, dst_is_group: bool) -> dict[str, Any]:
    return {
        "id": cid,
        "source": {"id": f"{src}-out", "group_id": src, "type": "OUTPUT_PORT"},
        "destination": {"id": dst, "group_id": dst if dst_is_group else None, "type": "INPUT_PORT"},
    }


def _logs_canvas(port_y: float) -> dict[str, Any]:
    """Three child groups chained ingest -> route -> deliver, and route also feeding the group's own output port."""
    return {
        "id": "logs",
        "name": "logs",
        "process_groups": [_group("ingest", 240), _group("route", 528), _group("deliver", 816)],
        "output_ports": [_card("to-diode", 56, port_y)],
        "connections": [
            _group_conn("c1", "ingest", "route", True),
            _group_conn("c2", "route", "deliver", True),
            _group_conn("c3", "route", "to-diode", False),
        ],
    }


@pytest.mark.parametrize("port_y", [0, 240, 2000])
def test_a_port_fed_by_a_child_group_sits_below_that_group_wherever_it_was(port_y: float) -> None:
    flow = _logs_canvas(port_y)
    placed = positions_from_flow(flow)
    assert placed["ingest"][1] < placed["route"][1] < placed["to-diode"][1]
    assert placed["to-diode"][0] + REAL_CARD_SIZES["output_port"][0] / 2 == AXIS_CENTRE


def test_a_port_above_the_group_that_feeds_it_is_flagged_as_a_back_edge() -> None:
    flow = _logs_canvas(0)
    kinds = [i["kind"] for i in layout_issues(flow)]
    assert "back_edge" in kinds
    placed = positions_from_flow(flow)
    for key in ("process_groups", "output_ports"):
        for card in flow[key]:
            card["position"] = dict(zip("xy", placed[card["id"]], strict=True))
    assert "back_edge" not in [i["kind"] for i in layout_issues(flow)]


def test_a_retry_loop_is_not_a_back_edge() -> None:
    flow = {
        "id": "g",
        "processors": [_card("A", 0, 0), _card("B", 0, 240)],
        "connections": [_conn("A", "B", "PROCESSOR", ["ok"]), _conn("B", "A", "PROCESSOR", ["retry"])],
    }
    assert "back_edge" not in [i["kind"] for i in layout_issues(flow)]
