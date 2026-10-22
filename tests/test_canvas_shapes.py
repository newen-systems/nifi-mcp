"""Funnels, remote process groups and labels are real cards: placement and relayout must clear them."""

import pytest

from nifi_mcp.flow_spec import (
    canvas_footprints,
    next_process_group_slot,
    next_processor_slot,
    relayout_process_group,
)
from nifi_mcp.layout import CARD_HEIGHT, CARD_WIDTH, PG_HEIGHT, PG_WIDTH, rects_overlap

FUNNEL = (48.0, 48.0)  # nifi-frontend funnel-manager.service.ts
RPG = (384.0, 176.0)  # nifi-frontend remote-process-group-manager.service.ts


class MixedCanvas:
    """One processor far away, plus a funnel and a remote process group both at (0,0)."""

    def __init__(self, extra: dict | None = None) -> None:
        self.moves: list[tuple[str, float, float]] = []
        self.extra = extra or {}
        self.positions = {}

    async def get_flow(self, process_group_id: str) -> dict:
        proc = {
            "id": "p1",
            "revision": {"version": 1},
            "component": {"id": "p1", "name": "Log", "position": {"x": 900, "y": 900}},
        }
        funnel = {"id": "f1", "component": {"id": "f1", "position": {"x": 0, "y": 0}}}
        rpg = {"id": "r1", "component": {"id": "r1", "position": {"x": 0, "y": 0}}}
        flow = {"processors": [proc], "funnels": [funnel], "remoteProcessGroups": [rpg], **self.extra}
        for items in flow.values():
            for item in items:
                if item["id"] in self.positions:
                    item["component"]["position"] = self.positions[item["id"]]
        return {"processGroupFlow": {"id": process_group_id, "flow": flow}}

    async def get_processor(self, pid: str) -> dict:
        return {"id": pid, "revision": {"version": 1}, "component": {"id": pid, "position": {"x": 900, "y": 900}}}

    async def get_funnel(self, fid: str) -> dict:
        return {"id": fid, "revision": {"version": 1}}

    async def update_funnel(self, fid: str, **kw) -> dict:
        self.moves.append((fid, kw["x"], kw["y"]))
        self.positions[fid] = {"x": kw["x"], "y": kw["y"]}
        return {"id": fid}

    async def update_processor(self, pid: str, **kw) -> dict:
        self.moves.append((pid, kw["x"], kw["y"]))
        self.positions[pid] = {"x": kw["x"], "y": kw["y"]}
        return {"id": pid}

    async def get_process_group(self, gid: str) -> dict:
        return {"id": gid, "revision": {"version": 1}, "component": {"id": gid, "comments": ""}}

    async def get_json(self, path: str) -> dict:
        return {"id": path.rsplit("/", 1)[-1], "revision": {"version": 1}}

    def _revision(self, version: int) -> dict:
        return {"version": version}

    def _ack(self) -> bool:
        return False

    async def json(self, method: str, path: str, *, json_body: dict) -> dict:
        cid = path.rsplit("/", 1)[-1]
        pos = json_body["component"]["position"]
        self.positions[cid] = pos
        self.moves.append((cid, pos["x"], pos["y"]))
        return {"id": cid}


def _clear_of_existing_cards(point: tuple[float, float], w: float, h: float) -> None:
    for name, (cw, ch) in (("funnel", FUNNEL), ("remote process group", RPG)):
        assert not rects_overlap((*point, w, h), (0.0, 0.0, cw, ch)), f"{point} lands on the {name} at (0,0)"


@pytest.mark.asyncio
async def test_next_processor_slot_clears_funnel_and_remote_group() -> None:
    slot = await next_processor_slot(MixedCanvas(), "g")
    _clear_of_existing_cards(slot, CARD_WIDTH, CARD_HEIGHT)


@pytest.mark.asyncio
async def test_next_group_slot_clears_funnel_and_remote_group() -> None:
    slot = await next_process_group_slot(MixedCanvas(), "g")
    _clear_of_existing_cards(slot, PG_WIDTH, PG_HEIGHT)


@pytest.mark.asyncio
async def test_relayout_does_not_move_processor_onto_funnel_or_remote_group() -> None:
    client = MixedCanvas()
    result = await relayout_process_group(client, "g")
    moves = {pid: (x, y) for pid, x, y in client.moves}
    x, y = moves["p1"]
    assert not rects_overlap((x, y, CARD_WIDTH, CARD_HEIGHT), (*moves.get("r1", (0, 0)), *RPG))
    assert result["layout_issues"] == []
    # The funnel is a placed card now: it joins the processor tree rather than staying put.
    assert "f1" in moves


@pytest.mark.asyncio
async def test_label_footprint_uses_its_own_size() -> None:
    label = {"id": "l1", "component": {"id": "l1", "position": {"x": 0, "y": 1000}, "width": 2000, "height": 300}}
    boxes = await canvas_footprints(MixedCanvas({"labels": [label]}), "g")
    slot = await next_processor_slot(MixedCanvas({"labels": [label]}), "g")
    assert any(box[:2] == (0.0, 1000.0) and box[2] >= 2000 and box[3] >= 300 for box in boxes)
    assert not rects_overlap((*slot, CARD_WIDTH, CARD_HEIGHT), (0.0, 1000.0, 2000.0, 300.0))
