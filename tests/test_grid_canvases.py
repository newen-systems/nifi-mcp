"""Unrelated groups pack for a monitor; workflow relationships remain inside blocks."""
from copy import deepcopy

from nifi_mcp.layout import (
    LABEL_GAP,
    collision_problems,
    outline_edges,
    positions_from_flow,
    real_boxes,
)
from nifi_mcp.ordering import precedence_graph


def research():
    return {"id": "research", "process_groups": [{"id": f"g{i:02}"} for i in range(10)],
            "output_ports": [{"id": "out"}], "connections": [{
                "id": "source-out", "source": {"id": "child-port", "group_id": "g03"},
                "destination": {"id": "out"}}]}


def test_research_packs_grid_with_source_output_cluster_on_bottom_boundary():
    flow = research()
    placed = positions_from_flow(flow)
    groups = [placed[c["id"]] for c in flow["process_groups"]]
    assert len({x for x, y in groups}) > 1
    assert len({y for x, y in groups}) > 1
    source, out = placed["g03"], placed["out"]
    assert source[0] + 192 == out[0] + 120
    assert out[1] == source[1] + 176 + LABEL_GAP
    assert source[1] == max(y for x, y in groups)
    boxes = real_boxes(flow, placed)
    assert collision_problems(boxes, outline_edges(flow)) == []
    width = max(x + w for x, y, w, h in boxes.values()) - min(x for x, y, w, h in boxes.values())
    height = max(y + h for x, y, w, h in boxes.values()) - min(y for x, y, w, h in boxes.values())
    assert 1.5 < width / height < 2.3


def test_logical_handoff_and_mixed_actual_chain_stay_in_indivisible_blocks():
    flow = research()
    flow["processors"] = [{"id": "middle"}]
    flow["connections"] += [
        {"id": "a", "source": {"id": "port", "group_id": "g00"}, "destination": {"id": "middle"}},
        {"id": "b", "source": {"id": "middle"}, "destination": {"id": "port", "group_id": "g01"}},
    ]
    flow["layout_intent"] = {"dependencies": [{"source": "g05", "target": "g06"}]}
    before = deepcopy(flow)
    placed = positions_from_flow(flow)
    boxes = real_boxes(flow, placed)
    for src, dst in precedence_graph(flow).forward:
        assert boxes[dst][1] >= boxes[src][1] + boxes[src][3] + LABEL_GAP
    assert boxes["g00"][0] + 192 == boxes["middle"][0] + 176 == boxes["g01"][0] + 192
    assert boxes["g05"][0] == boxes["g06"][0]
    assert flow == before
    assert collision_problems(boxes, outline_edges(flow)) == []


def test_grid_is_identical_after_saved_positions_names_and_listing_order_change():
    flow = research()
    expected = positions_from_flow(flow)
    for key in ("process_groups", "output_ports"):
        for i, card in enumerate(flow[key]):
            x, y = expected[card["id"]]
            card.update(name=f"renamed-{i}", position={"x": x, "y": y})
        flow[key].reverse()
    flow["connections"].reverse()
    assert positions_from_flow(flow) == expected


def test_disconnected_processor_flows_keep_parallel_top_down_chains_without_screen_packing(monkeypatch):
    import nifi_mcp.layout as layout

    def unexpected_packing(*args, **kwargs):
        raise AssertionError("ordinary flows must not use the viewport packer")

    monkeypatch.setattr(layout, "pack_blocks", unexpected_packing)
    flow = {"id": "actual-flow", "processors": [], "connections": []}
    for i in range(10):
        flow["processors"] += [{"id": f"start{i}"}, {"id": f"finish{i}"}]
        flow["connections"].append({"source": {"id": f"start{i}"}, "destination": {"id": f"finish{i}"}})
    flow["layout_intent"] = {"areas": [
        {"id": f"area{i}", "members": [f"start{i}", f"finish{i}"], "independent": True} for i in range(10)
    ]}
    placed = positions_from_flow(flow)
    assert len({placed[f"start{i}"][1] for i in range(10)}) == 1
    for i in range(10):
        assert placed[f"start{i}"][0] == placed[f"finish{i}"][0]
        assert placed[f"finish{i}"][1] >= placed[f"start{i}"][1] + 128 + LABEL_GAP


def test_mixed_flow_and_endpoint_canvases_do_not_use_navigation_grid(monkeypatch):
    import nifi_mcp.layout as layout

    def unexpected_packing(*args, **kwargs):
        raise AssertionError("mixed flow canvases must not use the viewport packer")

    monkeypatch.setattr(layout, "pack_blocks", unexpected_packing)
    for kind in ("processors", "funnels", "remote_process_groups"):
        flow = research()
        flow[kind] = [{"id": "local-stage"}]
        positions_from_flow(flow)


def test_connected_child_group_workflow_remains_one_vertical_chain(monkeypatch):
    import nifi_mcp.layout as layout

    def unexpected_packing(*args, **kwargs):
        raise AssertionError("a connected group workflow must not use the viewport packer")

    monkeypatch.setattr(layout, "pack_blocks", unexpected_packing)
    flow = {"id": "nested-flow", "process_groups": [{"id": f"g{i}"} for i in range(10)],
            "layout_intent": {"stages": [[f"g{i}"] for i in range(10)]}}
    placed = positions_from_flow(flow)
    assert len({x for x, y in placed.values()}) == 1
    for i in range(9):
        assert placed[f"g{i+1}"][1] >= placed[f"g{i}"][1] + 176 + LABEL_GAP


def test_navigation_grid_keeps_related_child_groups_clustered():
    flow = research()
    flow["layout_intent"] = {"dependencies": [{"source": "g05", "target": "g06"}]}
    placed = positions_from_flow(flow)
    assert placed["g05"][0] == placed["g06"][0]
    assert placed["g06"][1] >= placed["g05"][1] + 176 + LABEL_GAP
    assert len({x for x, y in placed.values()}) > 1
    assert collision_problems(real_boxes(flow, placed), outline_edges(flow)) == []
