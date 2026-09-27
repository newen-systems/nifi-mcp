import itertools
import math

import pytest

from nifi_mcp.layout import (
    AXIS_CENTRE,
    CANVAS_GRID,
    CARD_HEIGHT,
    CARD_WIDTH,
    COL_GAP,
    COL_PITCH,
    CONNECTION_LABEL_HEIGHT,
    CONNECTION_LABEL_WIDTH,
    LABEL_GAP,
    LABEL_MARGIN,
    PG_AXIS_X,
    PG_COL_PITCH,
    PG_GAP,
    PG_HEIGHT,
    PG_ROW_GAP,
    PG_ROW_PITCH,
    PG_WIDTH,
    REAL_CARD_SIZES,
    ROW_GAP,
    ROW_PITCH,
    SIDE_LABEL_MARGIN,
    _path,
    _path_hits_cards,
    assert_no_box_overlap,
    assign_canvas_positions,
    bends_for_connections,
    boxes_overlap,
    layered_positions,
    next_grid_point,
    positions_from_flow,
    processor_footprint,
    route_connections,
    row_pitch,
    sibling_pitch,
    side_pitch,
)


def _real(placed: dict[str, tuple[float, float]], kinds: dict[str, str] | None = None) -> dict:
    return {key: (*point, *REAL_CARD_SIZES[(kinds or {}).get(key, "processor")]) for key, point in placed.items()}


def _axis(placed: dict[str, tuple[float, float]], key: str, kind: str = "processor") -> float:
    return placed[key][0] + REAL_CARD_SIZES[kind][0] / 2


def test_280_pitch_overlaps_stats_cards() -> None:
    assert boxes_overlap((0.0, 0.0), (280.0, 0.0), width=CARD_WIDTH)


def test_row_pitch_is_card_plus_label_plus_margins_on_the_grid() -> None:
    for height, pitch in ((REAL_CARD_SIZES["processor"][1], ROW_PITCH), (PG_HEIGHT, PG_ROW_PITCH)):
        assert pitch == math.ceil((height + CONNECTION_LABEL_HEIGHT + 2 * LABEL_MARGIN) / CANVAS_GRID) * CANVAS_GRID
        assert pitch % CANVAS_GRID == 0
    assert (ROW_PITCH, PG_ROW_PITCH) == (240.0, 288.0)
    assert ROW_PITCH - CARD_HEIGHT == ROW_GAP
    assert COL_PITCH - CARD_WIDTH == COL_GAP


def test_fork_row_top_aligns_and_uses_its_tallest_card() -> None:
    # A fork of leaves (port and P both feed the join J) sits one row down; J follows the taller card.
    kinds = {"A": "processor", "port": "output_port", "P": "processor", "J": "processor"}
    edges = [("A", "port"), ("A", "P"), ("port", "J"), ("P", "J")]
    placed = layered_positions(["A", "port", "P", "J"], edges, kinds=kinds)
    assert placed["port"][1] == placed["P"][1] == 128.0 + LABEL_GAP
    assert placed["J"][1] == placed["P"][1] + 128.0 + LABEL_GAP
    assert _bent(placed, edges, kinds) == []


def test_pitches_equal_the_formulas() -> None:
    for height in (128.0, 48.0, 80.0, 176.0, 148.0):
        assert row_pitch(height) == height + LABEL_GAP
    assert row_pitch(128.0) == ROW_PITCH == 240.0
    assert row_pitch(48.0) == 160.0
    assert row_pitch(PG_HEIGHT) == PG_ROW_PITCH == 288.0
    for kind in ("processor", "input_port", "process_group", "funnel"):
        width = REAL_CARD_SIZES[kind][0]
        expected = math.ceil(max(width + 2 * LABEL_MARGIN, 2 * (CONNECTION_LABEL_WIDTH + LABEL_MARGIN)) / 8) * 8
        assert sibling_pitch(width) == expected == COL_PITCH == 512.0


def _bent(placed: dict, edges: list, kinds: dict | None = None) -> list:
    """Full card, label and line check, then the connections that needed the routed fallback."""
    boxes = _real(placed, kinds)
    routes = route_connections(boxes, edges)
    assert_no_box_overlap(boxes, edges, routes)
    return [edge for edge, (bends, _index) in zip(edges, routes, strict=True) if bends]


def test_chain_stays_on_one_axis() -> None:
    edges = [("A", "B"), ("B", "C")]
    placed = layered_positions(["A", "B", "C"], edges)
    assert placed == {"A": (0.0, 0.0), "B": (0.0, ROW_PITCH), "C": (0.0, 2 * ROW_PITCH)}
    assert _bent(placed, edges) == []


def test_port_processor_port_chain_has_equal_gaps() -> None:
    kinds = {"in": "input_port", "P": "processor", "out": "output_port"}
    edges = [("in", "P"), ("P", "out")]
    placed = layered_positions(["in", "P", "out"], edges, kinds=kinds)
    gap = math.ceil((CONNECTION_LABEL_HEIGHT + 2 * LABEL_MARGIN) / CANVAS_GRID) * CANVAS_GRID
    assert gap == LABEL_GAP == 112.0
    y_in = 0.0
    y_p = y_in + REAL_CARD_SIZES["input_port"][1] + gap
    y_out = y_p + REAL_CARD_SIZES["processor"][1] + gap
    assert [placed[k][1] for k in ("in", "P", "out")] == [y_in, y_p, y_out] == [0.0, 160.0, 400.0]
    assert {_axis(placed, k, kind) for k, kind in kinds.items()} == {AXIS_CENTRE}
    assert_no_box_overlap(_real(placed, kinds), edges)


def test_three_way_fork_spreads_one_sibling_pitch_apart() -> None:
    edges = [("A", "B"), ("A", "C"), ("A", "D")]
    placed = layered_positions(["A", "B", "C", "D"], edges)
    axes = [_axis(placed, k) for k in ("B", "C", "D")]
    assert axes == [AXIS_CENTRE - COL_PITCH, AXIS_CENTRE, AXIS_CENTRE + COL_PITCH]
    assert {placed[k][1] for k in ("B", "C", "D")} == {ROW_PITCH}
    assert _bent(placed, edges) == []


def test_fork_with_a_continuation_puts_the_others_on_the_parent_row() -> None:
    # B continues (it has a child), so A and C go perpendicular on A's row, right then left.
    edges = [("P", "A"), ("P", "B"), ("P", "C"), ("B", "D")]
    placed = layered_positions(["P", "A", "B", "C", "D"], edges)
    p = _axis(placed, "P")
    reach = side_pitch(352.0, 352.0)
    assert reach == math.ceil((176 + CONNECTION_LABEL_WIDTH + 2 * SIDE_LABEL_MARGIN + 176) / 8) * 8 == 672.0
    assert reach - 352.0 == CONNECTION_LABEL_WIDTH + 2 * 40.0
    assert (_axis(placed, "B"), placed["B"][1]) == (p, ROW_PITCH)
    assert (_axis(placed, "A"), placed["A"][1]) == (p - reach, 0.0)
    assert (_axis(placed, "C"), placed["C"][1]) == (p + reach, 0.0)
    assert _bent(placed, edges) == []


def test_nested_fork_continues_down_and_its_leaves_spread_below() -> None:
    edges = [("A", "B"), ("A", "C"), ("B", "D"), ("B", "E"), ("C", "F"), ("C", "G")]
    placed = layered_positions(list("ABCDEFG"), edges)
    a, b, c, d, e, f, g = (_axis(placed, k) for k in "ABCDEFG")
    assert b == a and placed["B"][1] == ROW_PITCH
    assert placed["C"][1] == 0.0
    assert (d, e) == (b - COL_PITCH / 2, b + COL_PITCH / 2)
    assert (f, g) == (c - COL_PITCH / 2, c + COL_PITCH / 2)
    assert placed["D"][1] == placed["E"][1] == 2 * ROW_PITCH
    assert placed["F"][1] == placed["G"][1] == ROW_PITCH
    # C's leaves share B's row, so C sits far enough out that F clears B's reserved column.
    assert f - b >= COL_PITCH
    assert c - a >= side_pitch(352.0, 352.0)
    assert _bent(placed, edges) == []


def test_join_comes_back_to_the_fork_axis() -> None:
    edges = [("A", "B"), ("A", "C"), ("B", "D"), ("C", "D"), ("D", "E")]
    placed = layered_positions(["A", "B", "C", "D", "E"], edges)
    assert _axis(placed, "D") == _axis(placed, "E") == _axis(placed, "A")
    assert placed["D"][1] == 2 * ROW_PITCH
    assert placed["E"][1] == 3 * ROW_PITCH
    assert _bent(placed, edges) == []


DEMO_FORK = [
    ("GenerateFlowFile", "UpdateAttribute"),
    ("UpdateAttribute", "RouteOnAttribute"),
    ("RouteOnAttribute", "RouteSecond"),
    ("RouteOnAttribute", "Tag"),
    ("RouteOnAttribute", "Audit"),
    ("RouteSecond", "MergeContent"),
    ("RouteSecond", "Reject"),
    ("Tag", "MergeContent"),
    ("Audit", "PutA"),
    ("Audit", "PutB"),
    ("MergeContent", "LogAttribute"),
]


def test_demo_fork_flow_fits() -> None:
    names = list(dict.fromkeys(name for edge in DEMO_FORK for name in edge))
    placed = layered_positions(names, DEMO_FORK)
    axis = {name: _axis(placed, name) for name in names}
    assert axis["GenerateFlowFile"] == axis["UpdateAttribute"] == axis["RouteOnAttribute"] == AXIS_CENTRE
    assert axis["MergeContent"] == axis["LogAttribute"] == AXIS_CENTRE
    assert sorted({y for _x, y in placed.values()}) == [n * ROW_PITCH for n in range(7)]
    by_row: dict[float, list[float]] = {}
    for name, (_x, y) in placed.items():
        by_row.setdefault(y, []).append(axis[name])
    for axes in by_row.values():
        axes.sort()
        assert all(right - left >= COL_PITCH for left, right in itertools.pairwise(axes))
    assert_no_box_overlap(_real(placed), DEMO_FORK)


# demo-fork-flow as built live (e30fecbf-...), edges in relationship order.
LIVE_FORK = [
    ("GenerateFlowFile", "set-level-region"),
    ("set-level-region", "route-level"),
    ("route-level", "log-level0"),
    ("route-level", "route-region"),
    ("route-level", "tag-level2"),
    ("route-region", "log-region0"),
    ("route-region", "log-region1"),
    ("tag-level2", "log-level2"),
    ("tag-level2", "MergeContent"),
    ("log-region1", "MergeContent"),
    ("MergeContent", "log-merged"),
]
# What the previous relayout left on that canvas: straight lines, region0 and region1 swapped.
LIVE_FORK_BEFORE = {
    "GenerateFlowFile": (0.0, 0.0),
    "set-level-region": (0.0, 240.0),
    "route-level": (0.0, 480.0),
    "log-level0": (-768.0, 720.0),
    "route-region": (0.0, 720.0),
    "tag-level2": (768.0, 720.0),
    "log-region1": (-256.0, 960.0),
    "log-region0": (256.0, 960.0),
    "log-level2": (768.0, 960.0),
    "MergeContent": (0.0, 1200.0),
    "log-merged": (0.0, 1440.0),
}


def test_live_fork_straight_join_crosses_a_card() -> None:
    boxes = _real(LIVE_FORK_BEFORE)
    straight = [([], None)] * len(LIVE_FORK)
    # Both its label and its line cross log-region0; the label is reported first.
    with pytest.raises(ValueError, match=r"log-region0 .* overlaps label tag-level2->MergeContent"):
        assert_no_box_overlap(boxes, LIVE_FORK, straight)


def test_a_line_through_a_card_fails_even_with_its_label_clear() -> None:
    boxes = _real({"A": (0.0, 0.0), "B": (0.0, 240.0), "D": (0.0, 960.0)})
    with pytest.raises(ValueError, match="connection A->D crosses B"):
        assert_no_box_overlap(boxes, [("A", "D")], [([], None)])
    assert_no_box_overlap(boxes, [("A", "D")])


def test_live_fork_matches_the_perpendicular_reference_layout() -> None:
    names = sorted({name for edge in LIVE_FORK for name in edge})
    placed = layered_positions(names, LIVE_FORK)
    axis = {name: _axis(placed, name) for name in names}
    y = {name: placed[name][1] for name in names}
    centre, reach = AXIS_CENTRE, side_pitch(352.0, 352.0)
    for name in ("GenerateFlowFile", "set-level-region", "route-level", "route-region", "MergeContent", "log-merged"):
        assert axis[name] == centre, name
    assert y["log-level0"] == y["tag-level2"] == y["route-level"] == 2 * ROW_PITCH
    assert (axis["log-level0"], axis["tag-level2"]) == (centre - reach, centre + reach)
    assert y["route-region"] == y["log-level2"] == 3 * ROW_PITCH
    assert axis["log-level2"] == axis["tag-level2"]
    assert (axis["log-region0"], axis["log-region1"]) == (centre - COL_PITCH / 2, centre + COL_PITCH / 2)
    assert y["log-region0"] == y["log-region1"] == 4 * ROW_PITCH
    assert (y["MergeContent"], y["log-merged"]) == (5 * ROW_PITCH, 6 * ROW_PITCH)


def test_live_fork_join_from_the_side_column_falls_back_to_a_route() -> None:
    names = sorted({name for edge in LIVE_FORK for name in edge})
    placed = layered_positions(names, LIVE_FORK)
    boxes = _real(placed)
    # Straight, the join edge clips log-level2 under its own source and crosses log-region1, the leaf
    # right of the axis in the row above the join.
    straight = _path(boxes, "tag-level2", "MergeContent", [])
    ends = ("tag-level2", "MergeContent")
    crossed = {key for key in boxes if _path_hits_cards(straight, {key: boxes[key]}, ends)}
    assert crossed == {"log-level2", "log-region1"}
    with pytest.raises(ValueError, match="tag-level2->MergeContent"):
        assert_no_box_overlap(boxes, LIVE_FORK, [([], None)] * len(LIVE_FORK))
    assert _bent(placed, LIVE_FORK) == [("tag-level2", "MergeContent")]
    join = LIVE_FORK.index(("tag-level2", "MergeContent"))
    bends, label_index = route_connections(boxes, LIVE_FORK)[join]
    tag, merge = boxes["tag-level2"], boxes["MergeContent"]
    lane = bends[1]["x"]
    # The lane clears every card it passes by a margin.
    top, bottom = tag[1] + tag[3] / 2, merge[1] + merge[3] / 2
    for x, y, width, height in boxes.values():
        if y < bottom and y + height > top:
            assert not x - LABEL_MARGIN < lane < x + width + LABEL_MARGIN
    points = [(b["x"], b["y"]) for b in bends]
    # Out of tag-level2's side facing the lane on its centre line, down the lane, into MergeContent's side.
    assert points[0] == (tag[0] + tag[2] + LABEL_MARGIN, tag[1] + tag[3] / 2)
    assert points[1] == (lane, tag[1] + tag[3] / 2)
    assert points[-2] == (lane, merge[1] + merge[3] / 2)
    assert points[-1] == (merge[0] + merge[2] + LABEL_MARGIN, merge[1] + merge[3] / 2)
    # The label sits on the lane, on a row gap's centre line.
    assert points[label_index][0] == lane
    assert points[label_index][1] in {box[1] - LABEL_GAP / 2 for box in boxes.values()}


def test_live_fork_old_route_ran_along_another_connection() -> None:
    # The route relayout drew before: straight down from tag-level2's centre, on top of tag-level2 to
    # log-level2, then along the gap above MergeContent.
    names = sorted({name for edge in LIVE_FORK for name in edge})
    boxes = _real(layered_positions(names, LIVE_FORK))
    tag, level2, merge = boxes["tag-level2"], boxes["log-level2"], boxes["MergeContent"]
    below_tag, above_merge = level2[1] - LABEL_GAP / 2, merge[1] - LABEL_GAP / 2
    outside = level2[0] + level2[2] + 2 * LABEL_MARGIN
    old = [
        {"x": tag[0] + tag[2] / 2, "y": below_tag},
        {"x": outside, "y": below_tag},
        {"x": outside, "y": above_merge},
        {"x": merge[0] + merge[2] / 2, "y": above_merge},
    ]
    routes = [(old, 2) if edge == ("tag-level2", "MergeContent") else ([], None) for edge in LIVE_FORK]
    with pytest.raises(ValueError, match="runs along connection"):
        assert_no_box_overlap(boxes, LIVE_FORK, routes)
    assert_no_box_overlap(boxes, LIVE_FORK)


def test_two_routed_lines_into_one_side_take_different_lines() -> None:
    names = list(dict.fromkeys(name for edge in DEMO_FORK for name in edge))
    placed = layered_positions(names, DEMO_FORK)
    boxes = _real(placed)
    routes = dict(zip(DEMO_FORK, route_connections(boxes, DEMO_FORK), strict=True))
    entries = {
        (routes[edge][0][-1]["x"], routes[edge][0][-1]["y"])
        for edge in (("RouteSecond", "MergeContent"), ("Tag", "MergeContent"))
    }
    assert len(entries) == 2
    assert_no_box_overlap(boxes, DEMO_FORK)


# demo-multifork-flow as built live (e336a045-...), edges in relationship order.
MULTIFORK = [
    ("GenerateFlowFile", "classify"),
    ("classify", "route-kind"),
    ("route-kind", "log-kind0"),
    ("route-kind", "route-prio"),
    ("route-kind", "spread"),
    ("route-kind", "tag-kind3"),
    ("route-prio", "tag-low"),
    ("route-prio", "route-zone"),
    ("route-zone", "log-z0"),
    ("route-zone", "log-z1"),
    ("route-zone", "throttle-z2"),
    ("throttle-z2", "log-z2"),
    ("tag-low", "log-low"),
    ("log-low", "merge-final"),
    ("spread", "worker-1"),
    ("spread", "worker-2"),
    ("spread", "worker-3"),
    ("worker-1", "merge-workers"),
    ("worker-2", "merge-workers"),
    ("worker-3", "merge-workers"),
    ("merge-workers", "log-workers"),
    ("log-workers", "merge-final"),
    ("tag-kind3", "audit-kind3"),
    ("tag-kind3", "merge-final"),
    ("merge-final", "log-final"),
]


def test_multifork_two_side_children_on_one_row_fail_the_check() -> None:
    # What 2697854 built: kind0 and kind3 both left of route-kind on its row, the outer line through kind0.
    boxes = _real({"route-kind": (0.0, 480.0), "log-kind0": (-672.0, 480.0), "tag-kind3": (-1696.0, 480.0)})
    edges = [("route-kind", "log-kind0"), ("route-kind", "tag-kind3")]
    with pytest.raises(ValueError, match=r"log-kind0 .* overlaps label route-kind->tag-kind3"):
        assert_no_box_overlap(boxes, edges, [([], None), ([], None)])


def test_multifork_keeps_one_side_child_per_side_and_relationship_order() -> None:
    names = sorted({name for edge in MULTIFORK for name in edge})
    placed = layered_positions(names, MULTIFORK)
    axis = {name: _axis(placed, name) for name in names}
    y = {name: placed[name][1] for name in names}
    # kind0 | kind1 (main) | kind2 | kind3, left to right.
    assert axis["log-kind0"] < axis["route-kind"] < axis["spread"] < axis["tag-kind3"]
    assert y["log-kind0"] == y["spread"] == y["route-kind"]
    assert y["route-prio"] == y["route-kind"] + ROW_PITCH
    # kind3 is the second child on the right: one row down, in its own column past spread's family.
    assert y["tag-kind3"] == y["route-kind"] + ROW_PITCH
    assert axis["tag-kind3"] - axis["worker-3"] >= COL_PITCH
    # route-zone's zone0 and zone1 both come before its main child: zone1 on its row, zone0 dropped.
    assert y["log-z1"] == y["route-zone"] and axis["log-z1"] < axis["route-zone"]
    assert y["log-z0"] == y["route-zone"] + ROW_PITCH and axis["log-z0"] < axis["log-z1"]
    assert set(_bent(placed, MULTIFORK)) == {("route-kind", "tag-kind3"), ("tag-kind3", "merge-final")}


def test_routes_do_not_depend_on_the_order_connections_are_listed() -> None:
    import random

    for shape in (MULTIFORK, LIVE_FORK, DEMO_FORK):
        names = sorted({name for edge in shape for name in edge})
        boxes = _real(layered_positions(names, shape))
        expected = dict(zip(shape, route_connections(boxes, shape), strict=True))
        for seed in range(20):
            shuffled = list(shape)
            random.Random(seed).shuffle(shuffled)  # noqa: S311 - seeded test data, not a secret
            assert dict(zip(shuffled, route_connections(boxes, shuffled), strict=True)) == expected, seed


def test_relayout_keeps_fork_siblings_in_relationship_order() -> None:
    ids = {name: f"id-{name}" for edge in LIVE_FORK for name in edge}
    rel = {
        ("route-level", "log-level0"): "level0",
        ("route-level", "route-region"): "level1",
        ("route-level", "tag-level2"): "level2",
        ("route-region", "log-region0"): "region0",
        ("route-region", "log-region1"): "region1",
    }
    flow = {
        "processors": [
            {"id": ids[name], "name": name, "position": {"x": 0, "y": 0}} for name in sorted(ids, reverse=True)
        ],
        "connections": [
            {
                "source": {"id": ids[src]},
                "destination": {"id": ids[dst], "name": dst},
                "relationships": [rel.get((src, dst), "success")],
            }
            for src, dst in reversed(LIVE_FORK)
        ],
    }
    placed = positions_from_flow(flow)
    assert placed[ids["log-region0"]][0] < placed[ids["log-region1"]][0]
    assert placed[ids["log-level0"]][0] < placed[ids["route-region"]][0] < placed[ids["tag-level2"]][0]


def test_cycle_is_broken_and_stays_vertical() -> None:
    placed = layered_positions(["A", "B"], [("A", "B"), ("B", "A")])
    assert placed == {"A": (0.0, 0.0), "B": (0.0, ROW_PITCH)}


def test_assign_mutates_spec() -> None:
    objects = [
        {"type": "processor", "name": "Generate", "x": 0, "y": 0},
        {"type": "processor", "name": "Log", "x": 280, "y": 0},
        {"type": "connection", "source": "Generate", "target": "Log", "relationships": ["success"]},
    ]
    assign_canvas_positions(objects)
    by_name = {item["name"]: item for item in objects if item.get("name") in {"Generate", "Log"}}
    assert by_name["Log"]["y"] == ROW_PITCH
    assert by_name["Log"]["x"] == by_name["Generate"]["x"] == 0.0


def test_positions_from_compact_flow() -> None:
    flow = {
        "processors": [
            {"id": "g", "name": "Generate", "position": {"x": 0, "y": 0}},
            {"id": "l", "name": "Log", "position": {"x": 280, "y": 0}},
        ],
        "connections": [
            {
                "source": {"id": "g"},
                "destination": {"id": "l"},
            }
        ],
    }
    placed = positions_from_flow(flow)
    assert placed["l"][1] >= placed["g"][1] + CARD_HEIGHT
    assert_no_box_overlap(_real(placed), [("g", "l")])


def test_straight_vertical_needs_no_bends() -> None:
    positions = {"g": (0.0, 0.0), "l": (0.0, ROW_PITCH)}
    bends = bends_for_connections(positions, [("g", "l")])
    assert bends == [[]]


def test_parallel_connections_get_distinct_bends() -> None:
    positions = {"g": (0.0, 0.0), "l": (0.0, ROW_PITCH)}
    bends = bends_for_connections(positions, [("g", "l"), ("g", "l")])
    assert len(bends) == 2
    assert bends[0] != bends[1]
    assert all(len(path) == 2 for path in bends)
    xs = {path[0]["x"] for path in bends}
    assert len(xs) == 2


def test_distinct_pairs_from_one_source_get_no_bends() -> None:
    positions = {"a": (0.0, 0.0), "b": (0.0, ROW_PITCH), "c": (COL_PITCH, ROW_PITCH)}
    assert bends_for_connections(positions, [("a", "b"), ("a", "c")]) == [[], []]


def test_self_loop_bends_off_the_card() -> None:
    positions = {"a": (0.0, 0.0)}
    bends = bends_for_connections(positions, [("a", "a")])
    assert len(bends[0]) == 2
    assert all(point["x"] > CARD_WIDTH for point in bends[0])


def test_skip_layer_stays_straight() -> None:
    positions = {
        "a": (0.0, 0.0),
        "b": (0.0, ROW_PITCH),
        "c": (0.0, ROW_PITCH * 2),
    }
    assert bends_for_connections(positions, [("a", "c")]) == [[]]


def test_side_sink_stays_straight() -> None:
    positions = {"a": (0.0, 0.0), "fail": (COL_PITCH, 0.0)}
    assert bends_for_connections(positions, [("a", "fail")]) == [[]]


def test_pg_rows_hold_a_connection_label() -> None:
    assert PG_COL_PITCH - PG_WIDTH == PG_GAP
    assert PG_ROW_PITCH - PG_HEIGHT == PG_ROW_GAP
    assert PG_ROW_GAP >= CONNECTION_LABEL_HEIGHT + 2 * LABEL_MARGIN


def _centre(placed: dict[str, tuple[float, float]], key: str, kind: str) -> float:
    return placed[key][0] + REAL_CARD_SIZES[kind][0] / 2


def test_stacked_processor_port_and_group_share_one_centre() -> None:
    # The ingest group: in -> Generate -> Update -> out, and a child group below.
    flow = {
        "input_ports": [{"id": "in", "name": "in", "position": {"x": 0, "y": 0}}],
        "processors": [
            {"id": "g", "name": "Generate", "position": {"x": 0, "y": 0}},
            {"id": "u", "name": "Update", "position": {"x": 0, "y": 0}},
        ],
        "output_ports": [{"id": "out", "name": "out", "position": {"x": 0, "y": 0}}],
        "process_groups": [{"id": "pg", "name": "child", "position": {"x": 0, "y": 0}}],
        "connections": [
            {"source": {"id": "in"}, "destination": {"id": "g"}},
            {"source": {"id": "g"}, "destination": {"id": "u"}},
            {"source": {"id": "u"}, "destination": {"id": "out"}},
        ],
    }
    placed = positions_from_flow(flow)
    kinds = {"in": "input_port", "g": "processor", "u": "processor", "out": "output_port", "pg": "process_group"}
    assert {_centre(placed, key, kind) for key, kind in kinds.items()} == {REAL_CARD_SIZES["processor"][0] / 2}
    assert [placed[key][1] for key in ("in", "g", "u", "out")] == [0.0, 160.0, 400.0, 640.0]
    boxes = {key: (*placed[key], *REAL_CARD_SIZES[kind]) for key, kind in kinds.items()}
    assert_no_box_overlap(boxes, [("in", "g"), ("g", "u"), ("u", "out")])


def test_spec_layout_centres_ports_on_the_axis() -> None:
    objects = [
        {"type": "processor", "name": "P"},
        {"type": "output_port", "name": "out"},
        {"type": "connection", "source": "P", "target": "out"},
    ]
    positions = assign_canvas_positions(objects)
    centre = positions["P"][0] + REAL_CARD_SIZES["processor"][0] / 2
    assert positions["out"][0] + REAL_CARD_SIZES["output_port"][0] / 2 == centre
    assert objects[1]["x"] == positions["out"][0]


def test_next_grid_point_stacks_groups_down() -> None:
    assert next_grid_point([]) == (0.0, 0.0)
    assert next_grid_point([(0.0, 0.0)]) == (0.0, PG_ROW_PITCH)
    assert next_grid_point([(0.0, 0.0), (0.0, PG_ROW_PITCH)]) == (0.0, PG_ROW_PITCH * 2)


def test_positions_from_flow_stacks_unconnected_groups_by_name() -> None:
    flow = {
        "process_groups": [
            {"id": "sys", "name": "log-syslog", "position": {"x": 0, "y": 0}},
            {"id": "gate", "name": "ingest-main", "position": {"x": 0, "y": 0}},
            {"id": "pfs", "name": "log-firewall-filterlog", "position": {"x": 420, "y": 0}},
        ],
        "processors": [],
        "connections": [],
    }
    placed = positions_from_flow(flow)
    assert placed["gate"] == (PG_AXIS_X, 0.0)
    assert placed["pfs"] == (PG_AXIS_X, PG_ROW_PITCH)
    assert placed["sys"] == (PG_AXIS_X, PG_ROW_PITCH * 2)


def _demo_flow(ingest: tuple[float, float], transform: tuple[float, float]) -> dict:
    # The live demo: transform is named first so only the port connection can put ingest on top.
    return {
        "process_groups": [
            {"id": "t", "name": "a-transform", "position": {"x": transform[0], "y": transform[1]}},
            {"id": "i", "name": "b-ingest", "position": {"x": ingest[0], "y": ingest[1]}},
        ],
        "connections": [
            {
                "id": "c",
                "source": {"id": "out", "type": "OUTPUT_PORT", "group_id": "i"},
                "destination": {"id": "in", "type": "INPUT_PORT", "group_id": "t"},
            }
        ],
    }


def _group_boxes(placed: dict[str, tuple[float, float]]) -> dict[str, tuple[float, float, float, float]]:
    return {key: (x, y, *REAL_CARD_SIZES["process_group"]) for key, (x, y) in placed.items()}


def test_side_by_side_groups_fail_the_label_check() -> None:
    # The layout the user rejected: ingest (0,0) and transform (424,0), label drawn across both cards.
    boxes = _group_boxes({"i": (0.0, 0.0), "t": (424.0, 0.0)})
    assert_no_box_overlap(boxes)
    with pytest.raises(ValueError, match="label i->t"):
        assert_no_box_overlap(boxes, [("i", "t")])


def test_demo_groups_stack_top_down_clear_of_the_label() -> None:
    placed = positions_from_flow(_demo_flow((0.0, 0.0), (424.0, 0.0)))
    assert placed["i"][0] == placed["t"][0] == PG_AXIS_X
    assert placed["t"][1] >= placed["i"][1] + PG_HEIGHT + CONNECTION_LABEL_HEIGHT
    assert placed["t"][1] == placed["i"][1] + PG_ROW_PITCH
    assert_no_box_overlap(_group_boxes(placed), [("i", "t")])
    # The label sits in the gap, clear of both cards, and the line runs down both centres.
    label_top = placed["i"][1] + PG_HEIGHT + (PG_ROW_GAP - CONNECTION_LABEL_HEIGHT) / 2
    assert label_top > placed["i"][1] + PG_HEIGHT
    assert label_top + CONNECTION_LABEL_HEIGHT < placed["t"][1]
    assert CONNECTION_LABEL_WIDTH < PG_WIDTH


def test_groups_stack_below_processors_on_their_axis() -> None:
    flow = {
        "processors": [{"id": "p", "name": "P", "position": {"x": 0, "y": 0}}],
        **_demo_flow((0.0, 0.0), (0.0, 0.0)),
    }
    placed = positions_from_flow(flow)
    assert placed["p"] == (0.0, 0.0)
    processor_centre = REAL_CARD_SIZES["processor"][0] / 2
    assert placed["i"][0] + PG_WIDTH / 2 == processor_centre
    assert placed["i"][1] >= ROW_PITCH
    assert placed["t"][1] == placed["i"][1] + PG_ROW_PITCH


def test_relayout_keeps_processor_and_child_group_apart() -> None:
    flow = {
        "processors": [{"id": "gen", "name": "Generate", "position": {"x": 900, "y": 900}}],
        "process_groups": [{"id": "child", "name": "ingest", "position": {"x": 0, "y": 0}}],
        "connections": [],
    }
    placed = positions_from_flow(flow)
    assert not boxes_overlap(placed["gen"], placed["child"], width=CARD_WIDTH, height=CARD_HEIGHT), placed


def test_relayout_mixed_canvas_has_no_overlap_of_any_kind() -> None:
    flow = {
        "processors": [
            {"id": f"p{i}", "name": f"P{i}", "position": {"x": 0, "y": 0}} for i in range(4)
        ],
        "input_ports": [{"id": "in", "name": "in", "position": {"x": 0, "y": 0}}],
        "process_groups": [
            {"id": f"g{i}", "name": f"g{i}", "position": {"x": 420 * i, "y": 0}} for i in range(5)
        ],
        "connections": [
            {"source": {"id": "in"}, "destination": {"id": "p0"}},
            {"source": {"id": "p0"}, "destination": {"id": "p1"}},
            {"source": {"id": "p0"}, "destination": {"id": "p2"}},
            {"source": {"id": "p1"}, "destination": {"id": "p3"}},
        ],
    }
    placed = positions_from_flow(flow)
    kinds = {key: "process_group" for key in placed if key.startswith("g")} | {"in": "input_port"}
    edges = [(c["source"]["id"], c["destination"]["id"]) for c in flow["connections"]]
    assert_no_box_overlap(_real(placed, kinds), edges)


def test_next_slot_clears_off_lattice_group_rounded_down() -> None:
    slot = next_grid_point([(300.0, 0.0)])
    assert not boxes_overlap(slot, (300.0, 0.0), width=PG_WIDTH, height=PG_HEIGHT), slot


def test_next_slot_clears_off_lattice_group_rounded_up() -> None:
    slot = next_grid_point([(200.0, 0.0)])
    assert not boxes_overlap(slot, (200.0, 0.0), width=PG_WIDTH, height=PG_HEIGHT), slot


def test_next_slot_clears_processor_cards() -> None:
    slot = next_grid_point([], obstacles=[processor_footprint((0.0, 0.0)), processor_footprint((492.0, 0.0))])
    for card in ((0.0, 0.0), (492.0, 0.0)):
        assert not boxes_overlap(slot, card, width=CARD_WIDTH, height=CARD_HEIGHT), slot


def test_origin_below_clears_every_obstacle() -> None:
    from nifi_mcp.layout import ROW_PITCH, group_footprint, origin_below, processor_footprint

    assert origin_below([]) == (0.0, 0.0)
    obstacles = [processor_footprint((0.0, 0.0)), group_footprint((492.0, 300.0))]
    x, y = origin_below(obstacles)
    assert x == 0.0
    assert y == 3 * ROW_PITCH  # group footprint ends at 588, the next processor row starts at 720
    assert all(y >= top + height for _left, top, _width, height in obstacles)


def test_positions_from_flow_snaps_groups_clear_of_funnels_and_remote_groups() -> None:
    flow = {
        "process_groups": [{"id": "g1", "name": "a", "position": {"x": 0, "y": 0}}],
        "funnels": [{"id": "f1", "position": {"x": 0, "y": 0}}],
        "remote_process_groups": [{"id": "r1", "position": {"x": PG_COL_PITCH, "y": 0}}],
    }
    x, y = positions_from_flow(flow)["g1"]
    for ox, oy, ow, oh in ((0.0, 0.0, 48.0, 48.0), (PG_COL_PITCH, 0.0, 384.0, 176.0)):
        assert not (x < ox + ow and ox < x + PG_WIDTH and y < oy + oh and oy < y + PG_HEIGHT)


def test_group_requested_within_nifi_card_width_of_another_is_shifted() -> None:
    # NiFi draws a group 384 wide (process-group-manager.service.ts), not 380.
    from nifi_mcp.layout import Canvas, rects_overlap

    nifi_group = (384.0, 176.0)
    for x in (381.0, 382.0, 383.0):
        canvas = Canvas()
        canvas.place("a", "process_group", (0.0, 0.0))
        (bx, by), note = canvas.place("b", "process_group", (x, 0.0))
        assert not rects_overlap((0.0, 0.0, *nifi_group), (bx, by, *nifi_group)), (bx, by)
        assert note, (x, bx, by)


def test_model_visible_layout_numbers_match_the_layout_constants() -> None:
    # The tool description said 420 and PROCESSOR_WRAP=3 after the code moved on.
    from pathlib import Path

    from nifi_mcp import server
    from nifi_mcp.flow_spec import next_process_group_slot
    from nifi_mcp.layout import PG_COL_PITCH, PG_ROW_PITCH

    root = Path(__file__).resolve().parent.parent
    col, row = int(PG_COL_PITCH), int(PG_ROW_PITCH)
    lattice = f"{col}x{row}"
    tool = server.nifi_layout_process_group.__doc__ or ""
    assert f"{row}px rows, {col}px columns" in tool
    assert "240px rows" in tool and f"{int(COL_PITCH)}px apart" in tool
    assert lattice in (next_process_group_slot.__doc__ or "")
    assert lattice in server.INSTRUCTIONS
    assert f"{lattice} lattice" in (root / "docs/designs/nifi-mcp-nl-flow-building.md").read_text()
    for text in (tool, server.INSTRUCTIONS):
        assert f"420x{row}" not in text and f"{row}px rows, 420px" not in text
