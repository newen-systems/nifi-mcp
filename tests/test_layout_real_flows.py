"""The nine production flows whose relayout failed the layout check at 9c45bf0, anonymised.

Names, ids and relationship names are replaced by generic ones in their original sorted order, so
sibling order and every tiebreak match the real flows. Positions are the real ones.
"""

import json
from pathlib import Path

import pytest

from nifi_mcp.layout import (
    CONNECTION_LABEL_WIDTH,
    LABEL_MARGIN,
    REAL_CARD_SIZES,
    SELF_LOOP_Y,
    layered_positions,
    layout_problems,
    outline_edges,
    positions_from_flow,
    real_boxes,
    relationship_order,
    route_connections,
)

FLOWS = json.loads((Path(__file__).parent / "fixtures" / "layout_real_flows.json").read_text())


def _relayout(flow: dict) -> tuple[dict, list, list]:
    boxes = real_boxes(flow, positions_from_flow(flow))
    listed = sorted(
        zip(flow["connections"], outline_edges(flow), strict=True), key=lambda item: relationship_order(item[0])
    )
    pairs = [pair for _conn, pair in listed]
    return boxes, pairs, route_connections(boxes, pairs)


@pytest.mark.parametrize("name", sorted(FLOWS))
def test_real_flow_relayout_passes_the_full_check(name: str) -> None:
    boxes, pairs, routes = _relayout(FLOWS[name])
    assert layout_problems(boxes, pairs, routes) == []


def test_fixture_covers_every_failure_class() -> None:
    edges = [pair for flow in FLOWS.values() for pair in outline_edges(flow)]
    assert any(src == dst for src, dst in edges), "self-loops"
    assert any(edges.count(pair) > 1 for pair in edges if pair[0] != pair[1]), "duplicate pairs"
    fan_in = max(
        sum(1 for _src, dst in outline_edges(flow) if dst == sink)
        for flow in FLOWS.values()
        for _s, sink in outline_edges(flow)
    )
    assert fan_in >= 8, "fan-in sinks"


def test_self_loop_sits_outside_its_card_with_its_label_on_the_outer_stretch() -> None:
    edges = [("A", "B"), ("B", "B"), ("B", "B")]
    placed = layered_positions(["A", "B"], edges)
    boxes = {key: (*point, *REAL_CARD_SIZES["processor"]) for key, point in placed.items()}
    first, second = route_connections(boxes, edges)[1:]
    x, y, width, height = boxes["B"]
    centre_y = y + height / 2
    reach = CONNECTION_LABEL_WIDTH / 2 + LABEL_MARGIN
    right = x + width + reach
    assert first == (
        [{"x": right, "y": centre_y + dy} for dy in (-SELF_LOOP_Y, 0.0, SELF_LOOP_Y)],
        1,
    )
    assert [b["x"] for b in second[0]] == [x - reach] * 3
    assert layout_problems(boxes, edges) == []


def test_a_self_loop_reserves_its_space_on_the_row() -> None:
    # A fork of leaves puts B and C side by side one row down; B's loop pushes C out past its label.
    edges = [("A", "B"), ("A", "C"), ("B", "B")]
    placed = layered_positions(["A", "B", "C"], edges)
    boxes = {key: (*point, *REAL_CARD_SIZES["processor"]) for key, point in placed.items()}
    assert placed["B"][1] == placed["C"][1]
    loop_label_right = route_connections(boxes, edges)[2][0][1]["x"] + CONNECTION_LABEL_WIDTH / 2
    assert placed["C"][0] >= loop_label_right + LABEL_MARGIN
    assert layout_problems(boxes, edges) == []
