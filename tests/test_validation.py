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
CONN = "00000000-0000-0000-0000-000000000300"


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
    return {"id": cid, "revision": {"version": 1}, "component": comp,
            "status": {"aggregateSnapshot": {"flowFilesQueued": 0}}}


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
            actual = self.root_id if match[1] == "root" and self.root_id else match[1]
            flow = self.flows[actual]
            parent = next((gid for gid, canvas in self.flows.items()
                           if any(child["id"] == actual for child in canvas.get("processGroups", []))), None)
            body = {
                "processGroupFlow": {
                    "id": actual,
                    "parentGroupId": parent,
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
        if request.method == "DELETE":
            cid = path.rsplit("/", 1)[-1]
            for flow in self.flows.values():
                for items in flow.values():
                    items[:] = [item for item in items if item.get("id") != cid]
            return httpx.Response(200, json={})
        kinds = "processors|process-groups|connections|input-ports|output-ports|funnels|remote-process-groups"
        pattern = rf"/({kinds})/([^/]+)"
        if request.method == "PUT" and (match := re.fullmatch(pattern, path)):
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


def test_child_groups_do_not_require_a_regular_grid() -> None:
    def kinds(*points: tuple[float, float]) -> set[str]:
        groups = [_group(f"00000000-0000-0000-0000-00000000030{n}", ROOT, x, y) for n, (x, y) in enumerate(points)]
        return {
            i["kind"]
            for i in layout_issues(compact_flow({"processGroupFlow": {"id": ROOT, "flow": _flow(groups=groups)}}))
        }

    assert "lattice" not in kinds((-16, 0), (-16, 288), (-16, 576))
    assert "lattice" not in kinds((0, 0), (500, 100), (130, 700))
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
    assert block == {"ok": True, "checked_groups": 2, "issues": [], "layout_repaired": [ROOT, SIDE]}


@pytest.mark.asyncio
async def test_layout_repair_covers_the_touched_group_and_descendants() -> None:
    transport = Canvas(
        {
            ROOT: _flow([_proc(P1, ROOT, 0, 0), _proc(P2, ROOT, 0, 0)], groups=[_group(SIDE, ROOT, -16, 0)]),
            SIDE: _flow([_proc(P3, SIDE, 0, 0), _proc("00000000-0000-0000-0000-0000000001aa", SIDE, 0, 0)]),
        }
    )
    block = await validate_after_action(_client(transport), {ROOT: True}, auto_layout=True)
    assert block["layout_repaired"] == [ROOT, SIDE]
    assert block["issues"] == []
    moved = {path.rsplit("/", 1)[-1] for _m, path in transport.writes}
    assert P2 in moved or P1 in moved
    assert moved & {P3, "00000000-0000-0000-0000-0000000001aa"}


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
async def test_a_group_under_version_control_is_laid_out_and_flagged_for_a_registry_commit() -> None:
    transport = Canvas({ROOT: _flow([_proc(P1, ROOT, 0, 0), _proc(P2, ROOT, 0, 0)])})
    original = transport.handle_async_request

    async def versioned(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith(f"/nifi-api/process-groups/{ROOT}"):
            return httpx.Response(200, json=_versioned(_group(ROOT, None)))
        return await original(request)

    transport.handle_async_request = versioned  # type: ignore[method-assign]
    block = await validate_after_action(_client(transport), {ROOT: True}, auto_layout=True)
    assert block["layout_repaired"] == [ROOT]
    assert block["layout_needs_registry_commit"] == [ROOT]
    assert "nifi_commit_process_group" in block["next"]
    assert transport.writes


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
async def test_the_root_canvas_is_repaired_like_any_other_group() -> None:
    transport = Canvas({ROOT: _flow([_proc(P1, ROOT, 0, 0), _proc(P2, ROOT, 0, 0)])})
    transport.root_id = ROOT
    block = await validate_after_action(_client(transport), {ROOT: True}, auto_layout=True)
    assert block["layout_repaired"] == [ROOT]
    assert "overlap" not in _kinds(block)
    assert "layout_not_repaired_root" not in block
    assert transport.writes


REAL = json.loads((Path(__file__).parent / "fixtures" / "validation_real_canvases.json").read_text())


@pytest.mark.parametrize("name", sorted(REAL))
def test_hand_tidy_production_cards_have_no_card_layout_issues(name: str) -> None:
    """The root's 3-column group grid, two mirrored-fork flows (46 cards once flagged off grid), the backbone
    groups after they were stacked one per row, and a backbone chain."""
    # These older snapshots store card positions/topology, not the live saved bends.
    assert not [i for i in layout_issues(REAL[name]) if i["kind"] in {"overlap", "lattice", "off_grid"}]


@pytest.mark.asyncio
@pytest.mark.parametrize("name", ["mirrored_fork_a", "mirrored_fork_b"])
async def test_hand_tidy_production_canvases_end_clean_and_a_second_pass_writes_nothing(name: str) -> None:
    outline = REAL[name]
    entities = [_proc(p["id"], name, p["position"]["x"], p["position"]["y"]) for p in outline["processors"]]
    transport = Canvas({name: _flow(entities)})
    client = _client(transport)
    block = await validate_after_action(client, {name: True}, auto_layout=True)
    assert not [i for i in block["issues"] if i["kind"] in {"overlap", "lattice", "off_grid"}]
    transport.writes.clear()
    await validate_after_action(client, {name: True}, auto_layout=True)
    assert transport.writes == []


@pytest.mark.parametrize("reverse", [False, True])
async def test_saved_unbent_connections_are_re_routed_after_a_change(reverse: bool) -> None:
    # Actual reported geometry: RouteTransfer (0,720) and FetchBlob (-512,960), no saved bends.
    first = _conn("out", ROOT, P1, P2, ["http"])
    second = _conn("back", ROOT, P2 if reverse else P1, P1 if reverse else P2, ["Response"])
    for connection in (first, second):
        connection["component"].update(bends=[], labelIndex=1)
    transport = Canvas({ROOT: _flow(
        [_proc(P1, ROOT, 0, 720), _proc(P2, ROOT, -512, 960)], connections=[first, second],
    )})
    block = await validate_after_action(_client(transport), {ROOT: True}, auto_layout=True)
    assert block["layout_repaired"] == [ROOT]
    assert not [i for i in block["issues"] if i["kind"] in {"overlapping_connections", "connection_geometry"}]
    assert any(path.startswith("/connections/") for _method, path in transport.writes)


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


GAP = [("processor", P3), ("connection", CONN), ("controller_service", SVC)]


def _gap_canvas() -> Canvas:
    # P1 and P3 sit with a gap where a deleted card used to be: no overlap, lattice or grid issue.
    return Canvas({ROOT: _flow([_proc(P1, ROOT, 0, 0), _proc(P3, ROOT, 0, 960)])})


@pytest.mark.asyncio
async def test_a_group_no_check_flags_is_still_laid_out_again() -> None:
    gap = _gap_canvas()
    block = await validate_after_action(_client(gap), {ROOT: True}, auto_layout=True)
    assert block["layout_repaired"] == [ROOT]
    assert ("PUT", f"/processors/{P3}") in gap.writes


@pytest.mark.asyncio
async def test_the_layout_respects_auto_layout_and_the_report_only_touch() -> None:
    off = _gap_canvas()
    block = await validate_after_action(_client(off), {ROOT: True}, auto_layout=False)
    assert block["layout_repaired"] == []
    assert off.writes == []
    read_only = _gap_canvas()
    block = await validate_after_action(_client(read_only), {ROOT: False}, auto_layout=True)
    assert block["layout_repaired"] == []
    assert read_only.writes == []
    pinned = _gap_canvas()
    pinned_client = _client(pinned)
    orig = pinned_client.get_process_group

    async def versioned(group_id: str) -> dict[str, Any]:
        return _versioned(await orig(group_id))

    pinned_client.get_process_group = versioned  # type: ignore[method-assign]
    block = await validate_after_action(pinned_client, {ROOT: True}, auto_layout=True)
    assert block["layout_repaired"] == [ROOT]
    assert block["layout_needs_registry_commit"] == [ROOT]
    assert pinned.writes


@pytest.mark.asyncio
@pytest.mark.parametrize(("kind", "cid"), GAP)
async def test_every_removal_returns_validation_with_the_layout_re_run(kind: str, cid: str) -> None:
    conn = _conn(CONN, ROOT, P1, P3, ["success"])
    svc = {"id": SVC, "revision": {"version": 1},
           "component": {"id": SVC, "parentGroupId": ROOT, "name": "s", "state": "DISABLED"}}
    connections = [] if kind == "processor" else [conn]
    transport = Canvas({ROOT: _flow([_proc(P1, ROOT, 0, 0), _proc(P3, ROOT, 0, 960)], connections=connections)})
    transport.flows[ROOT]["controllerServices"] = [svc]
    configure(_client(transport), settings(mcp_auto_layout=True))
    out = await mcp.call_tool("nifi_delete_component", {"params": {"kind": kind, "component_id": cid}})
    blocks = out[0] if isinstance(out, tuple) else out
    payload = json.loads("".join(getattr(block, "text", "") for block in blocks))
    assert payload["validation"]["layout_repaired"] == [ROOT]


LAYOUT_KINDS = {"overlap", "lattice", "off_grid", "overlapping_connections", "connection_geometry"}
LIVE_ROOT = [(416, 0), (0, 416), (416, 416), (416, 208), (0, 704), (0, 992), (0, 0), (0, 208), (832, 0), (832, 208)]
SCATTERED_ROOT = [
    (3, 5), (410, 77), (-19, 391), (801, 120), (60, 700), (13, 1003), (700, 600), (1, 403), (333, 333), (9, 9),
]  # fmt: skip


def _root_canvas(points: list[tuple[float, float]], *, bends: bool) -> Canvas:
    """A root canvas: ten groups, no cards of its own, two port-to-port connections down one column
    (a logs group to a low side to a high side), anonymised."""
    ids = [f"00000000-0000-0000-0000-0000000004{n:02d}" for n in range(len(points))]
    groups = [_group(gid, ROOT, x, y) for gid, (x, y) in zip(ids, points, strict=True)]

    def port(group: str, kind: str) -> dict[str, Any]:
        pid = f"{group}-{kind}"
        pos = {"x": 0, "y": 0 if kind == "in" else 240}
        comp = {"id": pid, "parentGroupId": group, "name": kind, "position": pos}
        return {"id": pid, "revision": {"version": 1}, "component": comp}

    def link(cid: str, src: str, dst: str) -> dict[str, Any]:
        entity = _conn(cid, ROOT, f"{src}-out", f"{dst}-in", ["success"])
        comp = entity["component"]
        comp["source"] = {"id": f"{src}-out", "type": "OUTPUT_PORT", "groupId": src}
        comp["destination"] = {"id": f"{dst}-in", "type": "INPUT_PORT", "groupId": dst}
        comp["labelIndex"] = 1
        # Drawn straight through the first group's card, label on top of it.
        comp["bends"] = [{"x": points[1][0] + 100, "y": points[1][1] + 80}] if bends else []
        return entity

    flows = {ROOT: _flow(groups=groups, connections=[link("c-1", ids[1], ids[4]), link("c-2", ids[4], ids[5])])}
    for gid in ids:
        flows[gid] = {**_flow(), "inputPorts": [port(gid, "in")], "outputPorts": [port(gid, "out")]}
    canvas = Canvas(flows)
    canvas.root_id = ROOT
    return canvas


@pytest.mark.asyncio
@pytest.mark.parametrize("points", [LIVE_ROOT, SCATTERED_ROOT], ids=["live", "scattered"])
@pytest.mark.parametrize("bends", [False, True])
async def test_the_root_canvas_ends_with_no_layout_issue_after_any_change(
    points: list[tuple[float, float]], bends: bool
) -> None:
    transport = _root_canvas(points, bends=bends)
    client = _client(transport)
    block = await validate_after_action(client, {ROOT: True}, auto_layout=True)
    assert set(block["layout_repaired"]) == set(transport.flows)
    assert [i for i in block["issues"] if i["kind"] in LAYOUT_KINDS] == []
    assert "layout_not_repaired_root" not in block
    transport.writes.clear()
    again = await validate_after_action(client, {ROOT: True}, auto_layout=True)
    assert [i for i in again["issues"] if i["kind"] in LAYOUT_KINDS] == []
    assert transport.writes == []


@pytest.mark.asyncio
async def test_nifi_layout_process_group_on_root_leaves_no_lattice_issue() -> None:
    transport = _root_canvas(SCATTERED_ROOT, bends=True)
    configure(_client(transport), settings(mcp_auto_layout=True))
    out = await mcp.call_tool("nifi_layout_process_group", {"params": {}})
    blocks = out[0] if isinstance(out, tuple) else out
    payload = json.loads("".join(getattr(block, "text", "") for block in blocks))
    assert payload.get("status") == "ok", payload
    assert [i for i in payload["validation"]["issues"] if i["kind"] in LAYOUT_KINDS] == []


@pytest.mark.asyncio
async def test_descendant_repairs_share_the_scan_hierarchy(monkeypatch) -> None:
    from unittest.mock import AsyncMock

    import nifi_mcp.validation as module

    transport = Canvas({
        ROOT: _flow(groups=[_group(SIDE, ROOT)]),
        SIDE: _flow([_proc(P1, SIDE, 0, 0), _proc(P2, SIDE, 0, 0)]),
    })
    reads = AsyncMock(wraps=module.read_layout_hierarchy)
    monkeypatch.setattr(module, "read_layout_hierarchy", reads)
    block = await validate_after_action(_client(transport), {ROOT: True, SIDE: True}, auto_layout=True)
    assert block["layout_repaired"] == [ROOT, SIDE]
    assert reads.await_count == 2  # Initial scan and fresh validation; no full-tree read per repaired group.
    assert block["ok"] is True


def test_unreadable_endpoint_owner_is_not_reported_as_dangling() -> None:
    from nifi_mcp.validation import _connection_issues

    conn = _conn(CONN, ROOT, P1, P2, ["success"])
    conn["component"]["destination"]["groupId"] = SIDE
    flow = _flow(connections=[conn])
    assert _connection_issues({"id": ROOT}, {"flow": flow}, {P1}, {ROOT}) == []
    assert _connection_issues({"id": ROOT}, {"flow": flow}, {P1}, {ROOT, SIDE})[0]["kind"] == "dangling_connection"
