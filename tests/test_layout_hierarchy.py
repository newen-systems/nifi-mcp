"""Structural hierarchy regressions: no domain/name heuristics or live writes."""
from copy import deepcopy

import pytest

from nifi_mcp.hierarchy import enrich_hierarchy, load_hierarchy


def card(cid):
    return {"id": cid, "name": "deliberately uninformative"}


def endpoint(cid, owner):
    return {"id": cid, "group_id": owner}


def connection(cid, src, dst):
    return {"id": cid, "source": endpoint(*src), "destination": endpoint(*dst)}


def fixture():
    return {
        "root": {
            "id": "root", "process_groups": [card("poc"), card("low"), card("high")],
            "connections": [
                connection("a", ("out", "poc"), ("in", "low")),
                connection("b", ("out", "low"), ("in", "high")),
                connection("c", ("out", "high"), ("in", "poc")),
            ],
        },
        "poc": {
            "id": "poc", "parent_group_id": "root", "processors": [card("start"), card("finish")],
            "input_ports": [card("in")], "output_ports": [card("out")],
            "connections": [
                connection("d", ("start", "poc"), ("out", "poc")),
                connection("e", ("in", "poc"), ("finish", "poc")),
            ],
        },
        **{gid: {
            "id": gid, "parent_group_id": "root", "processors": [card("work")],
            "input_ports": [card("in")], "output_ports": [card("out")],
            "connections": [connection("f", ("in", gid), ("work", gid)),
                            connection("g", ("work", gid), ("out", gid))],
        } for gid in ("low", "high")},
    }


def test_false_group_cycle_and_parent_context():
    result = enrich_hierarchy(fixture())
    root = result["root"]["port_context"]
    assert root["forward"] == [["low", "high"], ["poc", "low"]]
    assert root["feedback"] == [["high", "poc"]]
    assert root["cycle_feedback"] == []
    assert not root["uncertainties"]
    poc = result["poc"]["port_context"]
    assert poc["dependencies"] == [["start", "finish"]]
    assert poc["ranks"]["out"] < poc["ranks"]["in"]


def test_nested_actual_port_paths():
    outlines = fixture()
    outlines["low"]["processors"] = []
    outlines["low"]["process_groups"] = [card("nested")]
    outlines["low"]["connections"] = [
        connection("f", ("in", "low"), ("in", "nested")),
        connection("g", ("out", "nested"), ("out", "low")),
    ]
    outlines["nested"] = {
        "id": "nested", "parent_group_id": "low", "input_ports": [card("in")],
        "output_ports": [card("out")],
        "connections": [connection("n", ("in", "nested"), ("out", "nested"))],
    }
    result = enrich_hierarchy(outlines)
    assert result["poc"]["port_context"]["dependencies"] == [["start", "finish"]]
    # Removing the nested path must remove the inferred inter-chain handoff.
    outlines["nested"]["connections"] = []
    assert enrich_hierarchy(outlines)["poc"]["port_context"]["dependencies"] == []


def test_genuine_cycle_cuts_only_one_edge_and_reports_uncertainty():
    flow = {"processors": [card(c) for c in "abcd"], "connections": [
        connection(str(i), (src, "g"), (dst, "g"))
        for i, (src, dst) in enumerate([("a", "b"), ("b", "c"), ("c", "b"), ("c", "d")])
    ]}
    context = enrich_hierarchy({"g": flow})["g"]["port_context"]
    assert context["feedback"] == [["c", "b"]]
    assert context["cycle_feedback"] == [["c", "b"]]
    assert context["forward"] == [["a", "b"], ["b", "c"], ["c", "d"]]
    assert "real cycle" in context["uncertainties"][0]


def test_self_loop_is_explicit_feedback():
    flow = {"processors": [card("a")], "connections": [connection("s", ("a", "g"), ("a", "g"))]}
    ctx = enrich_hierarchy({"g": flow})["g"]["port_context"]
    assert ctx["feedback"] == ctx["cycle_feedback"] == [["a", "a"]]


def test_disconnected_flows_are_not_ordered_by_rank_or_name():
    flow = {"processors": [card(c) for c in "abcd"], "connections": [
        connection("a", ("a", "g"), ("b", "g")), connection("b", ("c", "g"), ("d", "g")),
    ], "layout_intent": {"areas": [{"id": "isolated", "members": ["c", "d"]}]}}
    result = enrich_hierarchy({"g": flow})["g"]
    assert result["port_context"]["dependencies"] == []
    assert result["layout_intent"] == flow["layout_intent"]


def test_remote_endpoint_is_unknown_not_a_switch():
    flow = {"processors": [card("a"), card("b")], "remote_process_groups": [card("remote")],
            "connections": [connection("a", ("a", "g"), ("in", "remote")),
                            connection("b", ("out", "remote"), ("b", "g"))]}
    ctx = enrich_hierarchy({"g": flow})["g"]["port_context"]
    assert not ctx["dependencies"]
    assert not ctx["forward"]
    assert any("remote endpoint" in n for n in ctx["uncertainties"])


def test_deterministic_pure_and_idempotent():
    outlines = fixture()
    original = deepcopy(outlines)
    result = enrich_hierarchy(outlines)
    assert outlines == original
    assert enrich_hierarchy(result) == result
    reversed_input = dict(reversed(list(outlines.items())))
    for flow in reversed_input.values():
        for value in flow.values():
            if isinstance(value, list):
                value.reverse()
    again = enrich_hierarchy(reversed_input)
    assert {g: f["port_context"] for g, f in again.items()} == {g: f["port_context"] for g, f in result.items()}


class Client:
    def __init__(self, outlines, denied=()):
        self.outlines, self.denied, self.calls = outlines, denied, []

    async def get_flow(self, gid):
        self.calls.append(gid)
        if gid in self.denied:
            raise PermissionError("private server detail must not be surfaced")
        return self.outlines[gid]


async def test_loader_ascends_then_reads_siblings():
    client = Client(fixture())
    result = await load_hierarchy(client, "poc")
    assert set(client.calls) == {"poc", "root", "low", "high"}
    assert len(client.calls) == 4
    assert result["poc"]["port_context"]["dependencies"] == [["start", "finish"]]


async def test_denial_does_not_invent_paths():
    result = await load_hierarchy(Client(fixture(), denied={"low"}), "poc")
    ctx = result["poc"]["port_context"]
    assert not ctx["dependencies"]
    assert any("PermissionError" in n for n in ctx["uncertainties"])
    assert "private server detail" not in str(result)


async def test_loader_is_bounded_and_reports_limit():
    client = Client(fixture())
    result = await load_hierarchy(client, "poc", max_groups=2)
    assert len(client.calls) == 2
    assert any("read limit" in n for n in result["poc"]["port_context"]["uncertainties"])


async def test_all_reads_denied_still_reports_unknown():
    result = await load_hierarchy(Client({}, denied={"root"}))
    assert result["root"]["port_context"]["uncertainties"]


async def test_raw_api_root_alias():
    client = Client({"root": {"processGroupFlow": {"id": "actual-root", "flow": {}}}})
    result = await load_hierarchy(client)
    assert set(result) == {"actual-root"}


async def test_cancellation_propagates():
    import asyncio

    class Cancelled:
        async def get_flow(self, gid):
            raise asyncio.CancelledError

    client = Cancelled()
    with pytest.raises(asyncio.CancelledError):
        await load_hierarchy(client)


def test_unrelated_internal_source_does_not_pull_group_stage_up():
    outlines = fixture()
    outlines["high"]["processors"].append(card("unrelated-source"))
    ctx = enrich_hierarchy(outlines)["root"]["port_context"]
    assert ctx["forward"] == [["low", "high"], ["poc", "low"]]
    assert ctx["feedback"] == [["high", "poc"]]


def test_partial_descendant_cannot_prove_false_cycle():
    outlines = fixture()
    outlines["poc"]["process_groups"] = [card("denied")]
    ctx = enrich_hierarchy(outlines)["root"]["port_context"]
    assert ctx["feedback"] == []
    assert ctx["forward"] == [["low", "high"]]
    assert any("denied" in note for note in ctx["uncertainties"])


def test_mixed_cards_use_parent_ports_not_group_internal_sources():
    outlines = fixture()
    root = outlines["root"]
    root["processors"] = [card("p1"), card("p2")]
    root["connections"] = [
        connection("a", ("out", "poc"), ("p1", "root")),
        connection("b", ("p1", "root"), ("p2", "root")),
        connection("c", ("p2", "root"), ("in", "high")),
    ]
    outlines["high"]["processors"].append(card("unrelated"))
    ctx = enrich_hierarchy(outlines)["root"]["port_context"]
    assert ctx["forward"] == [["p1", "p2"], ["p2", "high"], ["poc", "p1"]]
    assert not ctx["feedback"]


def test_fork_join_and_retry_keep_ordinary_dependencies():
    flow = {"processors": [card(c) for c in "abcde"], "connections": [
        connection(str(i), (src, "g"), (dst, "g"))
        for i, (src, dst) in enumerate([
            ("a", "b"), ("b", "c"), ("b", "d"), ("c", "e"), ("d", "e"), ("e", "b"),
        ])
    ]}
    ctx = enrich_hierarchy({"g": flow})["g"]["port_context"]
    assert ctx["feedback"] == [["e", "b"]]
    assert ctx["forward"] == [["a", "b"], ["b", "c"], ["b", "d"], ["c", "e"], ["d", "e"]]


def test_log_named_chain_has_no_special_keyword_order():
    flow = {"processors": [{"id": c, "name": name} for c, name in
                           [("z", "Collect"), ("y", "Parse"), ("x", "Deliver")]],
            "connections": [connection("1", ("z", "g"), ("y", "g")),
                            connection("2", ("y", "g"), ("x", "g"))]}
    ctx = enrich_hierarchy({"g": flow})["g"]["port_context"]
    assert ctx["forward"] == [["y", "x"], ["z", "y"]]
    assert not ctx["feedback"]


async def test_compact_intent_is_preserved_for_callers_metadata_loader():
    outlines = fixture()
    outlines["poc"]["layout_intent"] = {"stages": [["start"], ["finish"]]}
    result = await load_hierarchy(Client(outlines), "poc")
    assert result["poc"]["layout_intent"] == outlines["poc"]["layout_intent"]


def test_short_independent_input_does_not_reverse_long_group_dependency():
    outlines = fixture()
    outlines["low"]["input_ports"].append(card("other"))
    outlines["root"]["processors"] = [card("short-source")]
    outlines["root"]["connections"].append(connection("short", ("short-source", "root"), ("other", "low")))
    outlines["poc"]["processors"].extend([card("before1"), card("before2")])
    outlines["poc"]["connections"].extend([
        connection("long1", ("before1", "poc"), ("before2", "poc")),
        connection("long2", ("before2", "poc"), ("start", "poc")),
    ])
    ctx = enrich_hierarchy(outlines)["root"]["port_context"]
    assert ["poc", "low"] in ctx["forward"]
    assert ctx["feedback"] == [["high", "poc"]]


def test_multiple_port_paths_same_visible_pair_preserve_forward_and_return():
    outlines = {
        "root": {"process_groups": [card("a"), card("b")], "connections": [
            connection("1", ("out1", "a"), ("in1", "b")),
            connection("2", ("out", "b"), ("in", "a")),
            connection("3", ("out2", "a"), ("in2", "b")),
        ]},
        "a": {"input_ports": [card("in")], "output_ports": [card("out1"), card("out2")],
              "connections": [connection("4", ("in", "a"), ("out2", "a"))]},
        "b": {"input_ports": [card("in1"), card("in2")], "output_ports": [card("out")],
              "connections": [connection("5", ("in1", "b"), ("out", "b"))]},
    }
    ctx = enrich_hierarchy(outlines)["root"]["port_context"]
    assert ["a", "b"] in ctx["forward"]
    assert ["a", "b"] in ctx["feedback"]
    assert ["b", "a"] in ctx["feedback"]
    assert ctx["cycle_feedback"] == []


def test_cycle_entry_never_cuts_a_dependency_between_cyclic_regions():
    # Downstream has two completed inputs; upstream only one. Entry priority
    # must still respect SCC order instead of counting completed inputs alone.
    pairs = [("a", "b"), ("b", "c"), ("c", "b"), ("c", "z"),
             ("d", "z"), ("e", "z"), ("z", "y"), ("y", "z")]
    flow = {"processors": [card(c) for c in "abcdeyz"], "connections": [
        connection(str(i), (src, "g"), (dst, "g")) for i, (src, dst) in enumerate(pairs)
    ]}
    ctx = enrich_hierarchy({"g": flow})["g"]["port_context"]
    assert ["c", "z"] in ctx["forward"]
    assert ["c", "z"] not in ctx["feedback"]


def _captured_fixture():
    import json
    from pathlib import Path

    return json.loads((Path(__file__).parent / "fixtures" / "layout_port_hierarchy.json").read_text())


def test_sanitized_capture_contains_only_neutral_topology():
    import re

    fixture = _captured_fixture()
    assert len(fixture["outlines"]) == 16
    for gid, flow in fixture["outlines"].items():
        assert re.fullmatch(r"n\d{4}", gid)
        assert set(flow) <= {"id", "name", "parent_group_id", "processors", "process_groups", "input_ports",
                             "output_ports", "funnels", "remote_process_groups", "connections"}
        for key in ("processors", "process_groups", "input_ports", "output_ports", "funnels", "remote_process_groups"):
            for item in flow[key]:
                assert set(item) == {"id", "name"}
                assert item["id"] == item["name"]
                assert re.fullmatch(r"n\d{4}", item["id"])
        for conn in flow["connections"]:
            assert set(conn) == {"id", "source", "destination"}
            for end in (conn["source"], conn["destination"]):
                assert set(end) <= {"id", "group_id", "type"}
                assert re.fullmatch(r"n\d{4}", end["id"])
                assert end["group_id"] in fixture["outlines"]


def test_captured_hierarchy_placement_validation_and_repeatability():
    from nifi_mcp.layout import LABEL_GAP, OUTLINE_KINDS, positions_from_flow, real_boxes
    from nifi_mcp.ordering import precedence_graph
    from nifi_mcp.validation import _back_edge_issues, _port_row_issues

    fixture = _captured_fixture()
    roles = fixture["roles"]
    # Role metadata is deliberately never passed to enrichment or placement.
    outlines = enrich_hierarchy(fixture["outlines"])
    root_graph = precedence_graph(outlines[roles["root"]])
    assert (roles["poc"], roles["low"]) in root_graph.forward
    assert (roles["research"], roles["low"]) in root_graph.forward
    assert (roles["low"], roles["high"]) in root_graph.forward
    assert (roles["high"], roles["poc"]) in root_graph.feedback
    assert (roles["high"], roles["poc"]) not in root_graph.forward
    assert (roles["request-low"], roles["request-high"]) in precedence_graph(outlines[roles["poc"]]).forward
    first = {}
    for gid, flow in outlines.items():
        positions = positions_from_flow(flow)
        first[gid] = positions
        cards = {c["id"]: c for key in OUTLINE_KINDS for c in flow.get(key, [])}
        for cid, (px, py) in positions.items():
            cards[cid]["position"] = {"x": px, "y": py}
        boxes = real_boxes(flow, {})
        for src, dst in precedence_graph(flow).forward:
            assert boxes[dst][1] >= boxes[src][1] + boxes[src][3] + LABEL_GAP, (gid, src, dst)
        assert _back_edge_issues(flow, cards) == [], gid
        assert _port_row_issues(flow, cards) == [], gid
    root_pos = first[roles["root"]]
    assert root_pos[roles["poc"]][1] < root_pos[roles["low"]][1] < root_pos[roles["high"]][1]
    assert root_pos[roles["research"]][1] < root_pos[roles["low"]][1]
    assert first[roles["poc"]][roles["request-low"]][1] < first[roles["poc"]][roles["request-high"]][1]
    # Reload saved positions through exactly the same hierarchy/ordering path.
    again = enrich_hierarchy(outlines)
    for gid, flow in again.items():
        assert positions_from_flow(flow) == first[gid], gid
    # Validation must also reject a saved inversion of an inferred handoff.
    poc = deepcopy(outlines[roles["poc"]])
    cards = {c["id"]: c for key in OUTLINE_KINDS for c in poc.get(key, [])}
    cards[roles["request-high"]]["position"]["y"] = cards[roles["request-low"]]["position"]["y"]
    assert any(i["kind"] == "back_edge" for i in _back_edge_issues(poc, cards))


def test_integrated_ordering_preserves_pair_that_is_both_forward_and_return():
    from nifi_mcp.layout import positions_from_flow
    from nifi_mcp.ordering import precedence_graph

    flow = {"process_groups": [card("a"), card("b")], "connections": [
        connection("1", ("out1", "a"), ("in1", "b")),
        connection("2", ("out2", "a"), ("in2", "b")),
    ], "port_context": {"forward": [["a", "b"]], "feedback": [["a", "b"]],
                        "cycle_feedback": [], "dependencies": [], "ranks": {}, "uncertainties": []}}
    graph = precedence_graph(flow)
    assert ("a", "b") in graph.forward
    assert ("a", "b") in graph.feedback
    placed = positions_from_flow(flow)
    assert placed["a"][1] < placed["b"][1]


def test_cycle_feedback_is_subset_of_feedback_on_sanitized_capture():
    fixture = _captured_fixture()
    outlines = enrich_hierarchy(fixture["outlines"])
    root = outlines[fixture["roles"]["root"]]["port_context"]
    assert root["cycle_feedback"] == []
    assert root["feedback"] == [[fixture["roles"]["high"], fixture["roles"]["poc"]]]
    assert any(f["port_context"]["cycle_feedback"] for f in outlines.values())
    for flow in outlines.values():
        ctx = flow["port_context"]
        assert {tuple(p) for p in ctx["cycle_feedback"]} <= {tuple(p) for p in ctx["feedback"]}


def test_true_cycle_crossing_group_ports_is_marked_as_cycle_feedback():
    outlines = {
        "root": {"process_groups": [card("a"), card("b")], "connections": [
            connection("1", ("out", "a"), ("in", "b")),
            connection("2", ("out", "b"), ("in", "a")),
        ]},
        "a": {"processors": [card("entry")], "input_ports": [card("in")], "output_ports": [card("out")],
              "connections": [connection("3", ("in", "a"), ("out", "a")),
                              connection("4", ("entry", "a"), ("in", "a"))]},
        "b": {"input_ports": [card("in")], "output_ports": [card("out")],
              "connections": [connection("5", ("in", "b"), ("out", "b"))]},
    }
    ctx = enrich_hierarchy(outlines)["root"]["port_context"]
    assert ctx["forward"] == [["a", "b"]]
    assert ctx["feedback"] == ctx["cycle_feedback"] == [["b", "a"]]
