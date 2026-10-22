"""Regression for the four-source intake screenshot: queues stay below their sources."""
from copy import deepcopy
from itertools import pairwise

from nifi_mcp.layout import (
    _path,
    _paths_cross,
    _segments_cross,
    collision_problems,
    layout_problems,
    outline_edges,
    positions_from_flow,
    real_boxes,
    route_connections,
)


def intake():
    flow = {"id": "intake", "processors": [], "input_ports": [], "connections": [],
            "output_ports": [{"id": "out", "position": {"x": 824, "y": 400}}]}
    for i in range(4):
        flow["processors"].append({"id": f"p{i}", "position": {"x": 512 * i, "y": 160}})
        flow["input_ports"].append({"id": f"in{i}", "position": {"x": 56 + 512 * i, "y": 0}})
        for src, dst in ((f"in{i}", f"p{i}"), (f"p{i}", "out")):
            flow["connections"].append({"id": f"{src}-{dst}", "source": {"id": src}, "destination": {"id": dst}})
    return flow


def test_saved_leftward_detour_is_reported_even_when_other_clearances_pass():
    flow = intake()
    flow["connections"][3]["bends"] = [{"x": 176, "y": 344}]
    flow["connections"][7]["bends"] = [{"x": 1584, "y": 344}]
    boxes, pairs = real_boxes(flow, {}), outline_edges(flow)
    routes = [(c.get("bends", []), 0) for c in flow["connections"]]
    assert collision_problems(boxes, pairs, routes) == []
    assert any("p0->out" in p and "p1->out" in p and " crosses connection " in p
               for p in layout_problems(boxes, pairs, routes))


def test_parallel_fanin_reserves_queue_band_before_convergence():
    flow = intake()
    before = deepcopy(flow)
    placed = positions_from_flow(flow)
    boxes, pairs = real_boxes(flow, placed), outline_edges(flow)
    routes = route_connections(boxes, pairs)
    assert layout_problems(boxes, pairs, routes) == []
    assert flow == before
    for (src, dst), (bends, label) in zip(pairs, routes, strict=True):
        if dst == "out":
            sx, sy, sw, sh = boxes[src]
            assert bends[label]["x"] == sx + sw / 2
            assert sy + sh < bends[label]["y"] < boxes[dst][1]
            assert {b["x"] for b in bends} == {sx + sw / 2}
            path = _path(boxes, src, dst, bends)
            assert all(p[1] <= q[1] for p, q in pairwise(path))
    # Listing order cannot change bends or queue positions.
    permuted = list(reversed(pairs))
    assert list(reversed(route_connections(boxes, permuted))) == routes


def test_intersection_detector_accepts_shared_card_endpoint_and_rejects_proper_crossing():
    assert _segments_cross(((0, 0), (10, 10)), ((0, 10), (10, 0)))
    assert _segments_cross(((0, 0), (10, 0)), ((5, 0), (5, 10)))
    assert not _segments_cross(((0, 0), (10, 10)), ((10, 10), (20, 0)))
    assert not _segments_cross(((0, 0), (10, 0)), ((0, 5), (10, 5)))


def test_shared_bend_is_reported_but_shared_card_terminal_is_allowed():
    assert _paths_cross([(0, 0), (10, 10), (20, 0)], [(0, 20), (10, 10), (20, 20)])
    assert not _paths_cross([(0, 0), (10, 10)], [(20, 0), (10, 10)])
