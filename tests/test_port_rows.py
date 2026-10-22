"""Input ports sit on the top row above every other card, output ports on the bottom row below every other card."""

from __future__ import annotations

from itertools import pairwise
from typing import Any

import pytest

from nifi_mcp.layout import COL_PITCH, REAL_CARD_SIZES, positions_from_flow
from nifi_mcp.validation import layout_issues

PORT_KINDS = {"input_port_not_top", "output_port_not_bottom"}


def _card(cid: str, x: float = 0, y: float = 0, **extra: Any) -> dict[str, Any]:
    return {"id": cid, "name": cid, "position": {"x": x, "y": y}, **extra}


def _conn(src: str, dst: str, src_group: str = "", dst_group: str = "") -> dict[str, Any]:
    return {
        "id": f"{src}-{dst}",
        "source": {"id": src, "name": src, "type": "PROCESSOR", **({"group_id": src_group} if src_group else {})},
        "destination": {"id": dst, "type": "PROCESSOR", **({"group_id": dst_group} if dst_group else {})},
        "relationships": ["success"],
    }


def _apply(flow: dict[str, Any]) -> dict[str, Any]:
    """The flow with every card at the position the layout gives it."""
    placed = positions_from_flow(flow)
    out = dict(flow)
    for key in ("processors", "input_ports", "output_ports", "process_groups", "funnels", "remote_process_groups"):
        out[key] = [
            {**c, "position": dict(zip("xy", placed.get(c["id"], (0, 0)), strict=True))} for c in flow.get(key, [])
        ]
    return out


def _port_issues(flow: dict[str, Any]) -> list[dict[str, Any]]:
    return [i for i in layout_issues({"id": "g", **flow}) if i["kind"] in PORT_KINDS]


def _rows(flow: dict[str, Any]) -> tuple[list[float], list[float]]:
    placed = positions_from_flow(flow)
    return (
        [placed[c["id"]][1] for c in flow.get("input_ports", [])],
        [placed[c["id"]][1] for c in flow.get("output_ports", [])],
    )


def _assert_rule(flow: dict[str, Any]) -> dict[str, Any]:
    laid = _apply(flow)
    assert _port_issues(laid) == []
    placed = positions_from_flow(flow)
    for port in flow.get("input_ports", []):
        top = placed[port["id"]][1]
        for key in ("processors", "process_groups", "funnels", "output_ports"):
            assert all(placed[c["id"]][1] > top for c in flow.get(key, []))
    for port in flow.get("output_ports", []):
        top = placed[port["id"]][1]
        for key, kind in (("processors", "processor"), ("process_groups", "process_group"), ("funnels", "funnel")):
            assert all(top >= placed[c["id"]][1] + REAL_CARD_SIZES[kind][1] for c in flow.get(key, []))
    return placed


def test_ports_fed_only_by_child_groups() -> None:
    flow = {
        "input_ports": [_card("in")],
        "output_ports": [_card("out")],
        "process_groups": [_card("a"), _card("b")],
        "connections": [_conn("in", "a", dst_group="a"), _conn("a", "b", "a", "b"), _conn("b", "out", src_group="b")],
    }
    placed = _assert_rule(flow)
    assert placed["in"][1] < placed["a"][1] < placed["b"][1] < placed["out"][1]


def test_output_port_fed_by_a_processor_in_the_middle_of_a_fork_goes_to_the_bottom() -> None:
    flow = {
        "processors": [_card("route"), _card("x"), _card("y"), _card("z")],
        "output_ports": [_card("out")],
        "connections": [_conn("route", t) for t in ("x", "y", "z", "out")],
    }
    placed = _assert_rule(flow)
    assert placed["out"][1] > max(placed[k][1] for k in "xyz")


def test_several_ports_share_one_row_a_pitch_apart() -> None:
    flow = {
        "input_ports": [_card("in1"), _card("in2"), _card("in3")],
        "output_ports": [_card("out1"), _card("out2")],
        "processors": [_card("p"), _card("q")],
        "connections": [
            _conn("in1", "p"), _conn("in2", "p"), _conn("in3", "q"), _conn("p", "q"),
            _conn("p", "out1"), _conn("q", "out2"),
        ],
    }
    _assert_rule(flow)
    top, bottom = _rows(flow)
    assert len(set(top)) == 1
    assert len(set(bottom)) == 1
    placed = positions_from_flow(flow)
    xs = sorted(placed[k][0] for k in ("in1", "in2", "in3"))
    assert [b - a for a, b in pairwise(xs)] == [COL_PITCH, COL_PITCH]


def test_unconnected_ports_still_take_the_top_and_bottom_rows() -> None:
    flow = {
        "input_ports": [_card("lonely-in")],
        "output_ports": [_card("lonely-out")],
        "processors": [_card("a"), _card("b")],
        "connections": [_conn("a", "b")],
    }
    placed = _assert_rule(flow)
    assert placed["lonely-in"][1] < placed["a"][1]
    assert placed["lonely-out"][1] > placed["b"][1]


def test_a_group_of_only_ports_and_child_groups() -> None:
    flow = {
        "input_ports": [_card("in")],
        "output_ports": [_card("out")],
        "process_groups": [_card("g1"), _card("g2")],
    }
    _assert_rule(flow)


def test_only_ports() -> None:
    flow = {"input_ports": [_card("in")], "output_ports": [_card("out")]}
    placed = _assert_rule(flow)
    assert placed["in"][1] < placed["out"][1]


def test_ports_clear_fixed_labels_and_relaid_remote_groups() -> None:
    flow = {
        "input_ports": [_card("in")],
        "output_ports": [_card("out")],
        "processors": [_card("p", 0, 0)],
        "labels": [_card("note", 0, -600, width=300, height=100)],
        "remote_process_groups": [_card("remote", 600, 2000)],
        "connections": [_conn("in", "p"), _conn("p", "out")],
    }
    placed = positions_from_flow(flow)
    assert placed["in"][1] < -600
    assert placed["out"][1] >= placed["remote"][1] + REAL_CARD_SIZES["remote_process_group"][1]


@pytest.mark.parametrize(
    ("flow", "kind"),
    [
        ({"input_ports": [_card("in", 0, 240)], "processors": [_card("p", 0, 0)]}, "input_port_not_top"),
        ({"input_ports": [_card("in", 0, 0)], "processors": [_card("p", 0, 0)]}, "input_port_not_top"),
        ({"input_ports": [_card("in", 0, 0)], "process_groups": [_card("g", 0, 0)]}, "input_port_not_top"),
        ({"output_ports": [_card("out", 0, 0)], "processors": [_card("p", 0, 240)]}, "output_port_not_bottom"),
        ({"output_ports": [_card("out", 0, 0)], "process_groups": [_card("g", 0, -100)]}, "output_port_not_bottom"),
    ],
)
def test_validation_flags_a_port_off_its_row(flow: dict[str, Any], kind: str) -> None:
    assert kind in {i["kind"] for i in _port_issues(flow)}
    assert _port_issues(_apply(flow)) == []


def test_validation_is_quiet_for_ports_on_their_rows() -> None:
    flow = {
        "input_ports": [_card("in1", 0, -160), _card("in2", 512, -160)],
        "processors": [_card("p", 0, 0)],
        "output_ports": [_card("out", 0, 240)],
    }
    assert _port_issues(flow) == []


def _sink_flow() -> dict[str, Any]:
    chain = ["a", "b", "c", "d"]
    return {
        "processors": [_card(c) for c in [*chain, "sink"]],
        "output_ports": [_card("out")],
        "connections": [*(_conn(x, y) for x, y in pairwise(chain)), _conn("d", "out"), _conn("b", "sink")],
    }


def test_output_port_is_below_a_sink_on_a_failure_branch() -> None:
    placed = _assert_rule(_sink_flow())
    assert placed["out"][1] >= placed["sink"][1] + REAL_CARD_SIZES["processor"][1]
