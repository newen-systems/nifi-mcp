"""Whatever mix of specs, single creates, moves and relayouts a model sends, no card the
server placed overlaps another card on the resulting canvas. Seeded random, no extra dependency."""

from __future__ import annotations

import json
import random
from typing import Any

import pytest
from conftest import settings

from nifi_mcp.compact import compact_flow
from nifi_mcp.flow_spec import apply_flow_spec, relayout_process_group
from nifi_mcp.layout import OUTLINE_KINDS, rects_overlap
from nifi_mcp.server import (
    CreateProcessGroupIn,
    CreateProcessorIn,
    UpdateProcessorIn,
    configure,
    nifi_create_process_group,
    nifi_create_processor,
    nifi_update_processor,
)

ITERATIONS = 300
# Few distinct coordinates, so requested spots collide often.
COORDS = [-100.0, 0.0, 100.0, 216.0, 272.0, 382.0, 420.0, 492.0, 544.0, 984.0]
# Card sizes written out here, not imported from layout.py, so the check cannot share a wrong
# constant with the code it checks. Groups, remote groups, funnels and labels are NiFi 2.x's own
# (nifi-frontend *-manager.service.ts). Processors use the house overlap rule, 420x200
# (QUALITY_RULES.md), which covers NiFi's 352x128 processor. Ports use NiFi's larger, remote-access
# port card, 240x80 (port-manager.service.ts:57-60).
NIFI_CARDS: dict[str, tuple[float, float]] = {
    "processor": (420.0, 200.0),
    "input_port": (240.0, 80.0),
    "output_port": (240.0, 80.0),
    "process_group": (384.0, 176.0),
    "funnel": (48.0, 48.0),
    "remote_process_group": (384.0, 176.0),
    "label": (148.0, 148.0),
}
_FLOW_KEYS = {
    "processor": "processors",
    "input_port": "inputPorts",
    "output_port": "outputPorts",
    "process_group": "processGroups",
    "funnel": "funnels",
    "remote_process_group": "remoteProcessGroups",
    "label": "labels",
}


class SimNiFi:
    """Just enough of NiFi to keep every canvas in memory: creates, reads, moves."""

    def __init__(self) -> None:
        self._n = 0
        self.canvases: dict[str, dict[str, list[dict[str, Any]]]] = {}
        self.where: dict[str, tuple[str, str]] = {}  # id -> (group, flow key)

    def new_id(self) -> str:
        self._n += 1
        return f"00000000-0000-0000-0000-{self._n:012d}"

    def canvas(self, group_id: str) -> dict[str, list[dict[str, Any]]]:
        return self.canvases.setdefault(group_id, {key: [] for key in [*_FLOW_KEYS.values(), "connections"]})

    def add_card(self, group_id: str, kind: str, name: str, x: float, y: float, **extra: Any) -> str:
        cid = self.new_id()
        component = {"id": cid, "name": name, "parentGroupId": group_id, "position": {"x": x, "y": y}, **extra}
        self.canvas(group_id)[_FLOW_KEYS[kind]].append({"id": cid, "revision": {"version": 0}, "component": component})
        self.where[cid] = (group_id, _FLOW_KEYS[kind])
        if kind == "process_group":
            self.canvas(cid)
        return cid

    def entity(self, cid: str) -> dict[str, Any]:
        group_id, key = self.where[cid]
        return next(item for item in self.canvas(group_id)[key] if item["id"] == cid)

    def move(self, cid: str, x: float | None, y: float | None) -> dict[str, Any]:
        entity = self.entity(cid)
        pos = entity["component"]["position"]
        pos["x"] = pos["x"] if x is None else x
        pos["y"] = pos["y"] if y is None else y
        entity["revision"]["version"] += 1
        return entity

    async def authenticate(self) -> None:
        return None

    async def get_flow(self, group_id: str) -> dict[str, Any]:
        return {"processGroupFlow": {"id": group_id, "flow": json.loads(json.dumps(self.canvas(group_id)))}}

    async def get_process_group(self, group_id: str) -> dict[str, Any]:
        return {"id": group_id, "revision": {"version": 0}, "component": {"id": group_id}}

    async def create_process_group(self, parent_id: str, name: str, *, x: float, y: float, **_: Any) -> dict:
        return self.entity(self.add_card(parent_id, "process_group", name, x, y))

    async def create_processor(self, parent_id: str, _type: str, name: str, *, x: float, y: float, **_: Any) -> dict:
        return self.entity(self.add_card(parent_id, "processor", name, x, y))

    async def create_port(self, parent_id: str, kind: str, name: str, *, x: float, y: float) -> dict[str, Any]:
        return self.entity(self.add_card(parent_id, kind.lower(), name, x, y))

    async def create_controller_service(self, parent_id: str, _type: str, name: str, **_: Any) -> dict:
        return {"id": self.new_id(), "revision": {"version": 0}, "component": {"name": name, "state": "DISABLED"}}

    async def set_controller_service_state(self, service_id: str, state: str, version: int) -> dict[str, Any]:
        return {"id": service_id, "revision": {"version": version + 1}, "component": {"state": state}}

    async def create_connection(self, parent_id: str, **kw: Any) -> dict[str, Any]:
        cid = self.new_id()
        component = {"id": cid, "source": {"id": kw["source_id"]}, "destination": {"id": kw["destination_id"]}}
        self.canvas(parent_id)["connections"].append({"id": cid, "revision": {"version": 0}, "component": component})
        return {"id": cid}

    async def get_processor(self, cid: str) -> dict[str, Any]:
        return self.entity(cid)

    async def update_processor(self, cid: str, *, x: float | None = None, y: float | None = None, **_: Any) -> dict:
        return self.move(cid, x, y)

    async def update_port(self, cid: str, *, x: float, y: float, **_: Any) -> dict[str, Any]:
        return self.move(cid, x, y)

    async def update_process_group(self, cid: str, *, x: float, y: float, **_: Any) -> dict[str, Any]:
        return self.move(cid, x, y)

    async def get_connection(self, cid: str) -> dict[str, Any]:
        return {"id": cid, "revision": {"version": 0}}

    async def update_connection(self, cid: str, **_: Any) -> dict[str, Any]:
        return {"id": cid}


def _seed_canvas(sim: SimNiFi, rng: random.Random, group_id: str) -> set[str]:
    """Cards that were there before the model arrived. They may overlap each other."""
    before: set[str] = set()
    for _ in range(rng.randint(0, 5)):
        kind = rng.choice(list(_FLOW_KEYS))
        extra: dict[str, Any] = {}
        if kind == "label":
            extra = {"width": rng.choice([148.0, 600.0, 1500.0]), "height": rng.choice([60.0, 148.0, 400.0])}
        x, y = rng.choice(COORDS) * rng.choice([1, 2]), rng.choice(COORDS) * rng.choice([1, 2])
        before.add(sim.add_card(group_id, kind, f"old-{kind}", x, y, **extra))
    return before


def _point(rng: random.Random) -> dict[str, float]:
    return {"x": rng.choice(COORDS), "y": rng.choice(COORDS)} if rng.random() < 0.5 else {}


def _random_spec(rng: random.Random, *, new_group: bool) -> dict[str, Any]:
    objects: list[dict[str, Any]] = []
    cards: list[tuple[str, str]] = []
    for i in range(rng.randint(0, 7)):
        kind = rng.choice(["processor", "processor", "input_port", "output_port", "controller_service"])
        item: dict[str, Any] = {rng.choice(["type", "kind"]): kind, "name": f"{kind[:4]}{i}"}
        if kind == "processor":
            item["processor_type"] = "x.P"
            item["auto_terminated"] = "success"
        if kind == "controller_service":
            item["service_type"] = "x.S"
        else:
            placement = _point(rng)
            if placement and rng.random() < 0.5:
                item["position"] = placement
            else:
                item.update(placement)
            cards.append((item["name"], kind))
        objects.append(item)
    for _ in range(rng.randint(0, len(cards))):
        (src, src_kind), (dst, _dst_kind) = rng.choice(cards), rng.choice(cards)
        conn: dict[str, Any] = {"type": "connection", "source": src, "target": dst}
        if src_kind == "processor":
            conn["relationships"] = "success"
        objects.append(conn)
    spec: dict[str, Any] = {"objects": objects, "layout": rng.choice(["auto", "manual"])}
    if new_group:
        spec["process_group"] = {"name": "spec-group", **_point(rng)}
    return spec


async def _step(sim: SimNiFi, rng: random.Random, groups: list[str], mine: set[str]) -> None:
    group_id = rng.choice(groups)
    action = rng.choice(["spec", "spec-new-group", "processor", "group", "move", "relayout"])
    if action.startswith("spec"):
        spec = _random_spec(rng, new_group=action == "spec-new-group")
        result = await apply_flow_spec(sim, spec, parent_process_group_id=group_id)  # type: ignore[arg-type]
        assert result["status"] == "ok", result
        mine.update(item["id"] for item in result["created"])
        if action == "spec-new-group":
            groups.append(result["process_group_id"])
    elif action == "processor":
        args = {"parent_id": group_id, "processor_type": "x.P", "name": "single", **_point(rng)}
        out = json.loads(await nifi_create_processor(CreateProcessorIn(**args)))
        mine.add(out["processor"]["id"])
    elif action == "group":
        args = {"parent_id": group_id, "name": "single-group", **_point(rng)}
        out = json.loads(await nifi_create_process_group(CreateProcessGroupIn(**args)))
        mine.add(out["process_group"]["id"])
        groups.append(out["process_group"]["id"])
    elif action == "move":
        processors = [item["id"] for item in sim.canvas(group_id)["processors"] if item["id"] in mine]
        if processors:
            args = {"processor_id": rng.choice(processors), **({"x": 0.0, "y": 0.0} | _point(rng))}
            out = json.loads(await nifi_update_processor(UpdateProcessorIn(**args)))
            assert out["status"] == "ok", out
    else:
        await relayout_process_group(sim, group_id)  # type: ignore[arg-type]
        movable = ("processors", "inputPorts", "outputPorts", "processGroups")
        mine.update(item["id"] for key in movable for item in sim.canvas(group_id)[key])


def _overlaps(sim: SimNiFi, mine: set[str]) -> list[str]:
    found: list[str] = []
    for group_id in sim.canvases:
        outline = compact_flow({"processGroupFlow": {"id": group_id, "flow": sim.canvas(group_id)}})
        cards = []
        for key, kind in OUTLINE_KINDS.items():
            for item in outline.get(key) or []:
                width, height = NIFI_CARDS[kind]
                if kind == "label":
                    width, height = item.get("width") or width, item.get("height") or height
                pos = item["position"]
                cards.append((item["id"], kind, (pos["x"], pos["y"], width, height)))
        for i, (a, kind_a, box_a) in enumerate(cards):
            for b, kind_b, box_b in cards[i + 1 :]:
                if (a in mine or b in mine) and rects_overlap(box_a, box_b):
                    found.append(f"{group_id}: {kind_a} {a} {box_a} overlaps {kind_b} {b} {box_b}")
    return found


@pytest.mark.parametrize("seed", range(ITERATIONS))
async def test_no_placed_card_overlaps_another(seed: int) -> None:
    rng = random.Random(seed)  # noqa: S311 - seeded test data, not a secret
    sim = SimNiFi()
    configure(sim, settings())  # type: ignore[arg-type]
    parent = sim.new_id()
    sim.canvas(parent)
    _seed_canvas(sim, rng, parent)
    groups, mine = [parent], set()
    for _ in range(rng.randint(1, 5)):
        await _step(sim, rng, groups, mine)
    assert _overlaps(sim, mine) == []


async def test_explicit_spot_on_a_card_is_shifted_and_reported() -> None:
    sim = SimNiFi()
    configure(sim, settings())  # type: ignore[arg-type]
    parent = sim.new_id()
    funnel = sim.add_card(parent, "funnel", "f", 0.0, 0.0)
    out = json.loads(
        await nifi_create_processor(CreateProcessorIn(parent_id=parent, processor_type="x.P", name="P", x=0, y=0))
    )
    pos = out["processor"]["position"]
    assert (pos["x"], pos["y"]) == (0.0, 240.0)
    assert out["warnings"][0].startswith("The requested x/y for the new processor would overlap funnel")
    assert funnel in out["warnings"][0]
    assert "(0, 240)" in out["warnings"][0]


async def test_manual_spec_spot_on_an_existing_card_is_shifted_and_reported() -> None:
    sim = SimNiFi()
    parent = sim.new_id()
    sim.add_card(parent, "processor", "old", 492.0, 0.0)
    card = {"type": "processor", "processor_type": "x.P", "name": "P", "x": 500, "y": 50}
    spec = {"layout": "manual", "objects": [card]}
    result = await apply_flow_spec(sim, spec, parent_process_group_id=parent)  # type: ignore[arg-type]
    assert result["status"] == "ok", result
    assert _overlaps(sim, {item["id"] for item in result["created"]}) == []
    assert any("The requested x/y for processor objects[" in warning for warning in result["warnings"])
