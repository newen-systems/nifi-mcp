"""Post-action validation: what it finds, what it repairs, and that it never changes an outcome."""

from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any

import httpx
import pytest
from conftest import settings

from nifi_mcp.client import NiFiClient
from nifi_mcp.compact import compact_flow
from nifi_mcp.server import configure, mcp
from nifi_mcp.validation import layout_issues, validate_after_action

ROOT = "00000000-0000-0000-0000-000000000001"
SIDE = "00000000-0000-0000-0000-000000000002"
P1, P2, P3 = (f"00000000-0000-0000-0000-00000000010{n}" for n in (1, 2, 3))
SVC = "00000000-0000-0000-0000-000000000200"


def _proc(pid: str, group: str, x: float, y: float, **component: Any) -> dict[str, Any]:
    base = {
        "id": pid,
        "parentGroupId": group,
        "name": f"proc-{pid[-3:]}",
        "position": {"x": x, "y": y},
        "state": "STOPPED",
        "validationStatus": "VALID",
        "relationships": [],
    }
    return {"id": pid, "revision": {"version": 1}, "component": {**base, **component}}


def _group(gid: str, parent: str | None, x: float = 0, y: float = 0) -> dict[str, Any]:
    comp = {"id": gid, "parentGroupId": parent, "name": f"group-{gid[-1]}", "position": {"x": x, "y": y}}
    return {"id": gid, "revision": {"version": 1}, "component": comp}


def _conn(cid: str, group: str, source: str, destination: str, rels: list[str]) -> dict[str, Any]:
    comp = {
        "id": cid,
        "parentGroupId": group,
        "source": {"id": source, "groupId": group, "type": "PROCESSOR"},
        "destination": {"id": destination, "groupId": group, "type": "PROCESSOR"},
        "selectedRelationships": rels,
    }
    return {"id": cid, "revision": {"version": 1}, "component": comp}


class Canvas(httpx.AsyncBaseTransport):
    """A NiFi that keeps its flows: reads return them, a processor PUT moves the card. Counts writes."""

    def __init__(self, flows: dict[str, dict[str, list[dict[str, Any]]]], services: list[dict[str, Any]] | None = None):
        self.flows, self.services, self.writes, self.flow_reads = flows, services or [], [], 0
        self.fail_flow_reads_after: int | None = None
        self.root_id: str | None = None

    def _find(self, cid: str) -> dict[str, Any]:
        return next(e for flow in self.flows.values() for items in flow.values() for e in items if e["id"] == cid)

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path.removeprefix("/nifi-api")
        if request.method != "GET":
            return self._write(request, path)
        if match := re.fullmatch(r"/flow/process-groups/([^/]+)", path):
            self.flow_reads += 1
            if self.fail_flow_reads_after is not None and self.flow_reads > self.fail_flow_reads_after:
                return httpx.Response(400, json={"message": "no"})
            flow = self.flows[match[1]]
            body = {
                "processGroupFlow": {
                    "id": match[1],
                    "parentGroupId": None if match[1] == self.root_id else "up",
                    "breadcrumb": {"breadcrumb": {"id": match[1]}},
                    "flow": flow,
                }
            }
            return httpx.Response(200, json=body)
        if path.endswith("/controller-services"):
            return httpx.Response(200, json={"controllerServices": self.services})
        if match := re.fullmatch(r"/process-groups/([^/]+)", path):
            return httpx.Response(200, json=_group(match[1], None))
        return httpx.Response(200, json=self._find(path.rsplit("/", 1)[-1]))

    def _write(self, request: httpx.Request, path: str) -> httpx.Response:
        body = json.loads(request.content or b"{}")
        self.writes.append((request.method, path))
        if request.method == "PUT" and (match := re.fullmatch(r"/(processors|process-groups)/([^/]+)", path)):
            entity = self._find(match[2])
            entity["component"].update(body.get("component") or {})
            return httpx.Response(200, json=entity)
        if request.method == "POST" and path.endswith("/processors"):
            group = path.split("/")[2]
            entity = _proc("00000000-0000-0000-0000-0000000001ff", group, 0, 0)
            entity["component"]["position"] = body["component"]["position"]
            self.flows[group]["processors"].append(entity)
            return httpx.Response(201, json=entity)
        return httpx.Response(200, json={})


def _versioned(entity: dict[str, Any]) -> dict[str, Any]:
    entity["component"]["versionControlInformation"] = {"state": "UP_TO_DATE"}
    return entity


def _flow(processors=(), groups=(), connections=()) -> dict[str, list[dict[str, Any]]]:
    return {"processors": list(processors), "processGroups": list(groups), "connections": list(connections)}


def _client(transport: Canvas) -> NiFiClient:
    return NiFiClient(settings(), transport=transport)


def _kinds(block: dict[str, Any]) -> set[str]:
    return {i["kind"] for i in block["issues"]}


def test_overlapping_cards_are_reported_by_name_and_id() -> None:
    outline = compact_flow(
        {"processGroupFlow": {"id": ROOT, "flow": _flow([_proc(P1, ROOT, 0, 0), _proc(P2, ROOT, 100, 40)])}}
    )
    overlaps = [i for i in layout_issues(outline) if i["kind"] == "overlap"]
    assert len(overlaps) == 1
    assert overlaps[0]["component_id"] == P1
    assert P2 in overlaps[0]["message"]
    assert overlaps[0]["group_id"] == ROOT


def test_cards_clear_of_each_other_do_not_overlap() -> None:
    outline = compact_flow(
        {"processGroupFlow": {"id": ROOT, "flow": _flow([_proc(P1, ROOT, 0, 0), _proc(P2, ROOT, 0, 240)])}}
    )
    assert not [i for i in layout_issues(outline) if i["kind"] == "overlap"]


def test_child_groups_must_sit_on_a_regular_grid() -> None:
    def kinds(*points: tuple[float, float]) -> set[str]:
        groups = [_group(f"00000000-0000-0000-0000-00000000030{n}", ROOT, x, y) for n, (x, y) in enumerate(points)]
        return {
            i["kind"]
            for i in layout_issues(compact_flow({"processGroupFlow": {"id": ROOT, "flow": _flow(groups=groups)}}))
        }

    assert "lattice" not in kinds((-16, 0), (-16, 288), (-16, 576))
    assert "lattice" in kinds((0, 0), (500, 100), (130, 700))
    assert "lattice" not in kinds((0, 0), (424, 0))
    # A single row establishes a placement grid with the default row pitch.
    assert "lattice" not in kinds((416, 0), (0, 0), (832, 0))
    assert "lattice" not in kinds((0, 0), (0, 208), (0, 416), (0, 624))
    assert "lattice" not in kinds((0, 0), (416, 0), (0, 208), (416, 208))


def test_a_card_off_the_canvas_grid_is_reported_and_a_mirrored_tidy_flow_is_not() -> None:
    def off(*points: tuple[float, float]) -> list[str]:
        procs = [_proc(f"00000000-0000-0000-0000-00000000010{n}", ROOT, x, y) for n, (x, y) in enumerate(points)]
        outline = compact_flow({"processGroupFlow": {"id": ROOT, "flow": _flow(procs)}})
        return [i["component_id"][-1] for i in layout_issues(outline) if i["kind"] == "off_grid"]

    assert off((0, 0), (3, 240)) == ["1"]
    assert off((976, 0), (0, 240), (1648, 480)) == []


@pytest.mark.asyncio
async def test_invalid_processors_and_unterminated_relationships_are_reported() -> None:
    bad = _proc(P1, ROOT, 0, 0, validationStatus="INVALID", validationErrors=["'URL' is required"])
    bad["component"]["relationships"] = [
        {"name": "success", "autoTerminate": False},
        {"name": "failure", "autoTerminate": True},
        {"name": "retry", "autoTerminate": False},
    ]
    sink = _proc(P2, ROOT, 0, 240)
    wired = _conn("c1", ROOT, P1, P2, ["success"])
    transport = Canvas({ROOT: _flow([bad, sink], connections=[wired])})
    block = await validate_after_action(_client(transport), {ROOT: False}, auto_layout=False)
    found = {(i["kind"], i["component_id"]) for i in block["issues"]}
    assert ("invalid_processor", P1) in found
    assert next(i for i in block["issues"] if i["kind"] == "invalid_processor")["message"] == "'URL' is required"
    unterminated = [i for i in block["issues"] if i["kind"] == "unterminated_relationship"]
    assert [("retry" in i["message"]) for i in unterminated] == [True]
    assert block["ok"] is False
    assert block["checked_groups"] == 1
    assert transport.writes == []


@pytest.mark.asyncio
async def test_a_connection_to_a_missing_component_and_a_disabled_service_are_reported() -> None:
    orphan = _conn("c1", ROOT, P1, "00000000-0000-0000-0000-0000000009ff", ["success"])
    service = {
        "id": SVC,
        "component": {
            "id": SVC,
            "parentGroupId": ROOT,
            "name": "pool",
            "state": "DISABLED",
            "validationStatus": "VALID",
            "referencingComponents": [
                {"component": {"id": P1, "name": "proc-101", "referenceType": "PROCESSOR", "state": "STOPPED"}}
            ],
        },
    }
    transport = Canvas({ROOT: _flow([_proc(P1, ROOT, 0, 0)], connections=[orphan])}, [service])
    block = await validate_after_action(_client(transport), {ROOT: False}, auto_layout=False)
    assert {"dangling_connection", "disabled_service"} <= _kinds(block)


@pytest.mark.asyncio
async def test_the_walk_is_recursive_and_a_clean_tree_is_ok() -> None:
    transport = Canvas(
        {
            ROOT: _flow(groups=[_group(SIDE, ROOT, -16, 0)]),
            SIDE: _flow([_proc(P2, SIDE, 0, 0)]),
        }
    )
    block = await validate_after_action(_client(transport), {ROOT: True}, auto_layout=True)
    reviews = block.pop("design_review")
    assert set(reviews) == {ROOT, SIDE}
    assert all(review["status"] == "review_required" for review in reviews.values())
    assert block == {"ok": True, "checked_groups": 2, "issues": [], "layout_repaired": []}
    assert transport.flow_reads == 2


@pytest.mark.asyncio
async def test_layout_repair_is_limited_to_the_touched_group() -> None:
    transport = Canvas(
        {
            ROOT: _flow([_proc(P1, ROOT, 0, 0), _proc(P2, ROOT, 0, 0)], groups=[_group(SIDE, ROOT, -16, 0)]),
            SIDE: _flow([_proc(P3, SIDE, 0, 0), _proc("00000000-0000-0000-0000-0000000001aa", SIDE, 0, 0)]),
        }
    )
    block = await validate_after_action(_client(transport), {ROOT: True}, auto_layout=True)
    assert block["layout_repaired"] == [ROOT]
    assert all(i["group_id"] == SIDE for i in block["issues"]), block["issues"]
    assert {i["kind"] for i in block["issues"]} == {"overlap"}
    moved = {path.rsplit("/", 1)[-1] for _m, path in transport.writes}
    assert P2 in moved or P1 in moved
    assert moved <= {P1, P2, SIDE}, moved  # the child group's card moves; nothing inside it does


@pytest.mark.asyncio
async def test_auto_layout_off_reports_without_writing() -> None:
    transport = Canvas({ROOT: _flow([_proc(P1, ROOT, 0, 0), _proc(P2, ROOT, 0, 0)])})
    block = await validate_after_action(_client(transport), {ROOT: True}, auto_layout=False)
    assert block["layout_repaired"] == []
    assert "overlap" in _kinds(block)
    assert transport.writes == []


@pytest.mark.asyncio
async def test_a_group_the_action_only_read_is_never_repaired() -> None:
    transport = Canvas({ROOT: _flow([_proc(P1, ROOT, 0, 0), _proc(P2, ROOT, 0, 0)])})
    block = await validate_after_action(_client(transport), {ROOT: False}, auto_layout=True)
    assert block["layout_repaired"] == []
    assert transport.writes == []


@pytest.mark.asyncio
async def test_a_group_under_version_control_is_not_relaid_out() -> None:
    transport = Canvas({ROOT: _flow([_proc(P1, ROOT, 0, 0), _proc(P2, ROOT, 0, 0)])})
    original = transport.handle_async_request

    async def versioned(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith(f"/nifi-api/process-groups/{ROOT}"):
            return httpx.Response(200, json=_versioned(_group(ROOT, None)))
        return await original(request)

    transport.handle_async_request = versioned  # type: ignore[method-assign]
    block = await validate_after_action(_client(transport), {ROOT: True}, auto_layout=True)
    assert block["layout_not_repaired_versioned"] == [ROOT]
    assert block["layout_repaired"] == []
    assert transport.writes == []


@pytest.mark.asyncio
async def test_a_failed_validation_read_leaves_the_outcome_alone() -> None:
    transport = Canvas({ROOT: _flow([_proc(P1, ROOT, 0, 0)])})
    transport.fail_flow_reads_after = 1  # placement reads the canvas once; the validation read is the second
    configure(_client(transport), settings())
    out = await mcp.call_tool(
        "nifi_create_processor",
        {
            "params": {
                "parent_id": ROOT,
                "processor_type": "org.apache.nifi.processors.standard.LogMessage",
                "name": "log",
            }
        },
    )
    blocks = out[0] if isinstance(out, tuple) else out
    payload = json.loads("".join(getattr(block, "text", "") for block in blocks))
    assert payload["status"] == "ok"
    assert payload["outcome"] == "applied"
    assert payload["validation"]["ok"] is None
    assert payload["validation"]["error"]
    assert ("POST", f"/process-groups/{ROOT}/processors") in transport.writes


@pytest.mark.asyncio
async def test_a_mutation_tool_returns_the_validation_block() -> None:
    transport = Canvas({ROOT: _flow([_proc(P1, ROOT, 0, 0)])})
    configure(_client(transport), settings())
    out = await mcp.call_tool(
        "nifi_create_processor",
        {
            "params": {
                "parent_id": ROOT,
                "processor_type": "org.apache.nifi.processors.standard.LogMessage",
                "name": "log",
            }
        },
    )
    blocks = out[0] if isinstance(out, tuple) else out
    payload = json.loads("".join(getattr(block, "text", "") for block in blocks))
    assert payload["outcome"] == "applied"
    assert payload["validation"]["checked_groups"] == 1
    assert "layout_repaired" in payload["validation"]


@pytest.mark.asyncio
async def test_the_root_canvas_is_reported_and_never_repaired() -> None:
    transport = Canvas({ROOT: _flow([_proc(P1, ROOT, 0, 0), _proc(P2, ROOT, 0, 0)])})
    transport.root_id = ROOT
    block = await validate_after_action(_client(transport), {ROOT: True}, auto_layout=True)
    assert "overlap" in _kinds(block)
    assert block["layout_repaired"] == []
    assert block["layout_not_repaired_root"] == [ROOT]
    assert transport.writes == []


REAL = json.loads((Path(__file__).parent / "fixtures" / "validation_real_canvases.json").read_text())


@pytest.mark.parametrize("name", sorted(REAL))
def test_hand_tidy_production_cards_have_no_card_layout_issues(name: str) -> None:
    """The root's 3-column group grid, two mirrored-fork flows (46 cards once flagged off grid), the backbone
    groups after they were stacked one per row, and a backbone chain."""
    # These older snapshots store card positions/topology, not the live saved bends.
    assert not [i for i in layout_issues(REAL[name]) if i["kind"] in {"overlap", "lattice", "off_grid"}]


@pytest.mark.asyncio
@pytest.mark.parametrize("name", ["mirrored_fork_a", "mirrored_fork_b"])
async def test_hand_tidy_production_canvases_are_not_repaired(name: str) -> None:
    outline = REAL[name]
    entities = [_proc(p["id"], name, p["position"]["x"], p["position"]["y"]) for p in outline["processors"]]
    transport = Canvas({name: _flow(entities)})
    block = await validate_after_action(_client(transport), {name: True}, auto_layout=True)
    assert block["layout_repaired"] == []
    assert not [i for i in block["issues"] if i["kind"] in {"overlap", "lattice", "off_grid"}]
    assert transport.writes == []


@pytest.mark.parametrize("reverse", [False, True])
async def test_saved_unbent_connections_report_overlap_without_replacing_manual_routes(reverse: bool) -> None:
    # Actual reported geometry: RouteTransfer (0,720) and FetchBlob (-512,960), no saved bends.
    first = _conn("out", ROOT, P1, P2, ["http"])
    second = _conn("back", ROOT, P2 if reverse else P1, P1 if reverse else P2, ["Response"])
    for connection in (first, second):
        connection["component"].update(bends=[], labelIndex=1)
    transport = Canvas({ROOT: _flow(
        [_proc(P1, ROOT, 0, 720), _proc(P2, ROOT, -512, 960)], connections=[first, second],
    )})
    block = await validate_after_action(_client(transport), {ROOT: True}, auto_layout=True)
    assert block["ok"] is False
    issue = next(i for i in block["issues"] if i["kind"] == "overlapping_connections")
    assert issue["connection_ids"] == ["out", "back"]
    assert any(i["kind"] == "connection_geometry" for i in block["issues"])
    assert transport.writes == []


def test_saved_label_index_is_checked_instead_of_hypothetically_rerouting() -> None:
    # The label is saved at bend 1, inside P3; a fresh computed route would hide this defect.
    connection = _conn("route", ROOT, P1, P2, ["success"])
    connection["component"].update(
        bends=[{"x": 600, "y": 192}, {"x": 688, "y": 464}], labelIndex=1,
    )
    outline = compact_flow({"processGroupFlow": {"id": ROOT, "flow": _flow(
        [_proc(P1, ROOT, 0, 0), _proc(P2, ROOT, 0, 960), _proc(P3, ROOT, 512, 400)],
        connections=[connection],
    )}})
    assert outline["connections"][0]["label_index"] == 1
    issues = layout_issues(outline)
    assert any(i["kind"] == "connection_geometry" and "label" in i["message"] for i in issues)


@pytest.mark.parametrize("group_key", ["process_groups", "remote_process_groups"])
def test_overlap_uses_visible_group_cards_instead_of_hidden_port_ids(group_key: str) -> None:
    outline = {
        "id": ROOT, group_key: [{"id": SIDE, "position": {"x": 0, "y": 0}}],
        "processors": [{"id": P1, "position": {"x": 0, "y": 288}}],
        "connections": [
            {"id": "a", "source": {"id": "port-a", "group_id": SIDE},
             "destination": {"id": P1, "group_id": ROOT}},
            {"id": "b", "source": {"id": P1, "group_id": ROOT},
             "destination": {"id": "port-b", "group_id": SIDE}},
        ],
    }
    issue = next(i for i in layout_issues(outline) if i["kind"] == "overlapping_connections")
    assert issue["connection_ids"] == ["a", "b"]
