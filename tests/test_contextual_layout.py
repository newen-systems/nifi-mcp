"""One precedence model across every card type and every canvas."""
import copy

import pytest

from nifi_mcp.layout import LABEL_GAP, positions_from_flow, real_boxes
from nifi_mcp.ordering import precedence_graph, read_intent, write_intent


def canvas(kinds, edges=(), intent=None):
    flow = {"id": "root", "connections": [
        {"id": f"c{i}", "source": {"id": s}, "destination": {"id": d}}
        for i, (s, d) in enumerate(edges)
    ]}
    for kind, ids in kinds.items():
        flow[kind] = [{"id": cid, "name": cid} for cid in ids]
    if intent is not None:
        flow["layout_intent"] = intent
    return flow


def assert_forward(flow):
    graph = precedence_graph(flow)
    boxes = real_boxes(flow, positions_from_flow(flow))
    for src, dst in graph.forward:
        assert boxes[dst][1] >= boxes[src][1] + boxes[src][3] + LABEL_GAP, (src, dst, boxes)
    return boxes


def test_mixed_group_processor_processor_group_with_child_ports():
    flow = canvas({"process_groups": ["g1", "g2"], "processors": ["p1", "p2"]},
                  [("g1", "p1"), ("p1", "p2"), ("p2", "g2")])
    flow["connections"][0]["source"] = {"id": "child-output", "group_id": "g1", "type": "OUTPUT_PORT"}
    flow["connections"][-1]["destination"] = {"id": "child-input", "group_id": "g2", "type": "INPUT_PORT"}
    boxes = assert_forward(flow)
    assert len({x + w / 2 for x, y, w, h in boxes.values()}) == 1


def test_root_diode_order_uses_declared_domain_never_alphabetical_grid():
    flow = canvas({"process_groups": ["diode-high", "transfer", "diode-low"]}, intent={
        "domain": "diode-low-to-high", "roles": {"diode-high": "high", "transfer": "transfer", "diode-low": "low"},
    })
    for i, card in enumerate(flow["process_groups"]):
        card["position"] = {"x": i * 424, "y": 0}
    boxes = assert_forward(flow)
    assert boxes["diode-low"][1] < boxes["transfer"][1] < boxes["diode-high"][1]


def test_names_high_low_without_domain_do_not_infer_order():
    flow = canvas({"process_groups": ["high", "low"]})
    graph = precedence_graph(flow)
    assert graph.forward == []
    assert graph.uncertainties
    boxes = assert_forward(flow)
    # A neutral grid can use multiple rows without inventing a dependency.
    for card in flow["process_groups"]:
        card["name"] = "unrelated renamed area"
    assert assert_forward(flow) == boxes


def test_inputs_outputs_bound_mixed_remote_and_local_cards():
    flow = canvas({"input_ports": ["in"], "output_ports": ["out"], "processors": ["p"],
                   "remote_process_groups": ["r"], "process_groups": ["g"], "funnels": ["f"]},
                  [("in", "r"), ("r", "p"), ("p", "g"), ("g", "f"), ("f", "out")])
    boxes = assert_forward(flow)
    assert min(y for _, y, _, _ in boxes.values()) == boxes["in"][1]
    assert max(y for _, y, _, _ in boxes.values()) == boxes["out"][1]


def test_all_fork_children_below_parent_and_join_below_all_prerequisites():
    flow = canvas({"processors": ["fork", "main", "next", "side", "join"]},
                  [("fork", "main"), ("main", "next"), ("fork", "side"), ("next", "join"), ("side", "join")])
    boxes = assert_forward(flow)
    assert boxes["side"][1] == boxes["main"][1] > boxes["fork"][1]
    assert boxes["join"][0] == boxes["fork"][0]


def test_feedback_exempts_only_declared_return_edge():
    flow = canvas({"processors": ["a", "b", "c"]}, [("a", "b"), ("b", "c"), ("c", "a")],
                  {"feedback": [{"source": "c", "target": "a"}]})
    graph = precedence_graph(flow)
    assert graph.forward == [("a", "b"), ("b", "c")]
    assert graph.feedback == [("c", "a")]
    assert graph.conflicts == []
    assert_forward(flow)


def test_feedback_declaration_cannot_exempt_an_ordinary_forward_edge():
    flow = canvas({"processors": ["a", "b"]}, [("a", "b")],
                  {"feedback": [{"source": "a", "target": "b"}]})
    graph = precedence_graph(flow)
    assert graph.feedback == []
    assert graph.conflicts
    assert_forward(flow)


def test_logical_log_handoffs_do_not_create_connections():
    nodes = ["delivery", "enrichment", "parsing", "ingestion", "collection"]
    intent = {"domain": "log-delivery", "roles": dict(zip(nodes, nodes, strict=True))}
    flow = canvas({"process_groups": nodes}, intent=intent)
    before = copy.deepcopy(flow)
    boxes = assert_forward(flow)
    assert [boxes[n][1] for n in reversed(nodes)] == sorted(boxes[n][1] for n in nodes)
    assert flow == before
    assert flow["connections"] == []


def test_explicit_isolated_areas_remain_parallel_internally_top_down():
    flow = canvas({"processors": ["a", "b", "c", "d"]}, [("a", "b"), ("c", "d")], {
        "areas": [{"id": "one", "members": ["a", "b"]}, {"id": "two", "members": ["c", "d"]}],
        "domain": "input-output", "roles": {"a": "input", "b": "processing", "c": "processing", "d": "output"},
    })
    assert not precedence_graph(flow).conflicts
    boxes = assert_forward(flow)
    assert boxes["a"][1] == boxes["c"][1]
    assert boxes["a"][0] != boxes["c"][0]


def test_actual_connection_wins_conflicting_semantics_and_reports_conflict():
    flow = canvas({"processors": ["a", "b"]}, [("a", "b")],
                  {"domain": "input-output", "roles": {"a": "output", "b": "input"}})
    graph = precedence_graph(flow)
    assert graph.forward == [("a", "b")]
    assert graph.conflicts
    assert_forward(flow)


def test_ambiguous_names_require_unique_reference():
    flow = canvas({"processors": ["a", "b", "c"]}, intent={"stages": [["same"], ["c"]]})
    flow["processors"][0]["name"] = flow["processors"][1]["name"] = "same"
    graph = precedence_graph(flow)
    assert graph.forward == []
    assert any("ambiguous" in s for s in graph.conflicts)


def test_repeated_relayout_and_api_order_produce_identical_positions():
    flow = canvas({"processors": ["z", "a", "b", "c"], "process_groups": ["g"]},
                  [("z", "a"), ("a", "b"), ("z", "c"), ("b", "g"), ("c", "g")])
    positions = positions_from_flow(flow)
    for key in ("processors", "process_groups"):
        for item in flow[key]:
            item["position"] = dict(zip(("x", "y"), positions[item["id"]], strict=True))
        flow[key].reverse()
    flow["connections"].reverse()
    assert positions_from_flow(flow) == positions


def test_intent_round_trips_without_clipping_or_overwriting_prose():
    intent = {"stages": [["one"], ["two"]]}
    prose = "Existing comments " * 100
    text = write_intent(prose, intent)
    assert text.startswith(prose.rstrip())
    assert read_intent(text)["stages"] == intent["stages"]
    assert write_intent(text, intent) == text
    with pytest.raises(ValueError):
        read_intent(text + '\nnifi-layout: {}')


def test_four_intake_ports_align_over_parallel_processors_and_centre_output_join():
    flow = canvas({"input_ports": [f"in{i}" for i in range(4)],
                   "processors": [f"p{i}" for i in range(4)], "output_ports": ["out"]},
                  [(f"in{i}", f"p{i}") for i in range(4)] + [(f"p{i}", "out") for i in range(4)])
    boxes = assert_forward(flow)
    def centre(n):
        return boxes[n][0] + boxes[n][2] / 2
    for i in range(4):
        assert centre(f"in{i}") == centre(f"p{i}")
    assert centre("out") == sum(centre(f"p{i}") for i in range(4)) / 4


def test_colliding_boundary_ports_do_not_shift_an_unrelated_chain():
    from nifi_mcp.layout import _port_row

    flow = canvas({"input_ports": ["a", "b", "c"], "processors": ["p", "q"]})
    placed = {"a": (0, 0), "b": (0, 0), "c": (0, 0), "p": (0, 160), "q": (2048, 160)}
    ports = _port_row(flow, ["a", "b", "c"], 0, placed, {"a": ["p"], "b": ["p"], "c": ["q"]})
    assert ports["c"][0] + 120 == placed["q"][0] + 176
    assert (ports["a"][0] + ports["b"][0]) / 2 + 120 == placed["p"][0] + 176


def test_declared_genuine_feedback_overrides_inferred_cycle_break():
    from nifi_mcp.hierarchy import enrich_hierarchy

    flow = canvas({"processors": ["a", "b", "c"]}, [("a", "b"), ("b", "c"), ("c", "a")],
                  {"feedback": [{"source": "b", "target": "c"}]})
    enriched = enrich_hierarchy({"g": flow})["g"]
    graph = precedence_graph(enriched)
    assert not graph.conflicts
    assert graph.feedback == [("b", "c")]
    assert_forward(enriched)


def test_same_visible_pair_with_forward_and_return_connections_keeps_precedence():
    flow = canvas({"process_groups": ["a", "b"]}, [("a", "b")])
    flow["port_context"] = {"forward": [["a", "b"]], "feedback": [["a", "b"]]}
    graph = precedence_graph(flow)
    assert ("a", "b") in graph.forward
    assert_forward(flow)


def test_contextual_poc_body_chain_shares_axis_despite_later_ingress():
    from test_layout_hierarchy import fixture

    from nifi_mcp.hierarchy import enrich_hierarchy

    flow = enrich_hierarchy(fixture())["poc"]
    boxes = assert_forward(flow)
    assert boxes["start"][0] == boxes["finish"][0]
    assert boxes["start"][1] < boxes["finish"][1]


def test_fallback_never_exempts_forward_dependency_between_cycles():
    from nifi_mcp.ordering import reaches

    pairs = [("a", "b"), ("b", "c"), ("c", "b"), ("c", "z"),
             ("d", "z"), ("e", "z"), ("z", "y"), ("y", "z")]
    flow = canvas({"processors": list("abcdeyz")}, pairs)
    graph = precedence_graph(flow)
    assert ("c", "z") in graph.forward
    assert all(reaches(pairs, dst, src) for src, dst in graph.feedback)
    assert_forward(flow)
