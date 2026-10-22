"""Post-action validation: after a tool changes NiFi, read the changed process group and every group
below it once, report what is wrong, and re-run the layout on a touched group whose layout is.

Layout problems are repaired (NIFI_MCP_AUTO_LAYOUT, default true), a versioned group included: its moved
cards are a local modification, listed in layout_needs_registry_commit. Validity problems are only reported.
The result never changes the tool's outcome: a failed read reports ok=None and the error.
"""

from __future__ import annotations

import itertools
from contextvars import ContextVar
from typing import Any

from nifi_mcp.client import NiFiClient
from nifi_mcp.design import review_graph
from nifi_mcp.flow_spec import load_layout_intent, read_layout_hierarchy, relayout_process_group
from nifi_mcp.layout import (
    CANVAS_GRID,
    LABEL_GAP,
    OUTLINE_KINDS,
    layout_problems,
    outline_edges,
    positions_from_flow,
    real_boxes,
    rects_overlap,
)
from nifi_mcp.ordering import precedence_graph

MAX_GROUPS = 200
MAX_ISSUES = 50

# group id -> may the layout be repaired there. Set per tool call; a tool calls touch().
TOUCHED: ContextVar[dict[str, bool] | None] = ContextVar("nifi_mcp_touched_groups", default=None)

Issue = dict[str, Any]


def touch(group_id: str | None, *, repair: bool = True) -> None:
    """Record a process group this call changed, so validation covers it and everything below it.

    With `repair` (the default) its layout is re-run after the change, flagged or not."""
    touched = TOUCHED.get()
    if touched is not None and group_id:
        touched[group_id] = touched.get(group_id, False) or repair


def _issue(group: dict[str, Any], kind: str, message: str, cid: str | None = None, name: str | None = None) -> Issue:
    found: Issue = {"group_id": group["id"], "group_name": group.get("name"), "kind": kind, "message": message}
    if cid:
        found["component_id"] = cid
        found["component_name"] = name
    return found


def _point(item: dict[str, Any]) -> tuple[float, float]:
    pos = item.get("position") or {}
    return float(pos.get("x") or 0), float(pos.get("y") or 0)


def _cards(outline: dict[str, Any], keys: tuple[str, ...]) -> dict[str, dict[str, Any]]:
    return {str(i["id"]): i for key in keys for i in outline.get(key) or [] if i.get("id")}


def layout_issues(outline: dict[str, Any]) -> list[Issue]:
    """Card layout and the actual saved connection paths/labels on NiFi's canvas."""
    found: list[Issue] = []
    cards = _cards(outline, tuple(OUTLINE_KINDS))
    for (left, a), (right, b) in itertools.combinations(real_boxes(outline, {}).items(), 2):
        if rects_overlap(a, b):
            other = cards[right].get("name") or right
            found.append(_issue(outline, "overlap", f"overlaps {other} ({right})", left, cards[left].get("name")))
    placed = ("processors", "input_ports", "output_ports", "funnels", "process_groups", "remote_process_groups")
    for cid, card in _cards(outline, placed).items():
        x, y = _point(card)
        if x % CANVAS_GRID or y % CANVAS_GRID:
            found.append(
                _issue(
                    outline,
                    "off_grid",
                    f"is at {(x, y)}, not on the {CANVAS_GRID:g}px canvas grid",
                    cid,
                    card.get("name"),
                )
            )
    found += _off_axis_issues(outline, cards) + _back_edge_issues(outline, cards) + _port_row_issues(outline, cards)
    return found + connection_layout_issues(outline)


def _back_edge_issues(outline: dict[str, Any], cards: dict[str, dict[str, Any]]) -> list[Issue]:
    """Check saved geometry against exactly the forward constraints used by placement."""
    graph = precedence_graph(outline)
    boxes = real_boxes(outline, {})
    found = [_issue(outline, "layout_conflict", message) for message in graph.conflicts]
    for src, dst in graph.forward:
        if src not in boxes or dst not in boxes:
            continue
        required = boxes[src][1] + boxes[src][3] + LABEL_GAP
        if boxes[dst][1] < required:
            message = (
                f"{_label(cards, dst)} must be below {_label(cards, src)} with at least "
                f"{LABEL_GAP:g}px clearance; saved y={boxes[dst][1]:g}, required y>={required:g}"
            )
            found.append(_issue(outline, "back_edge", message, dst, cards.get(dst, {}).get("name")))
    return found


def _port_row_issues(outline: dict[str, Any], cards: dict[str, dict[str, Any]]) -> list[Issue]:
    """Input ports belong on the top row and output ports on the bottom row. An input port needs every other
    card strictly below its top edge; an output port needs its top edge at or below every processor, group,
    remote group and funnel's bottom edge. Read from the saved positions, so it holds on any canvas."""
    boxes = real_boxes(outline, {})
    kinds = {str(i["id"]): kind for key, kind in OUTLINE_KINDS.items() for i in outline.get(key) or [] if i.get("id")}
    solid = {"processor", "process_group", "remote_process_group", "funnel"}
    found: list[Issue] = []
    for cid, kind in kinds.items():
        if cid not in boxes or kind not in {"input_port", "output_port"}:
            continue
        top = boxes[cid][1]
        if kind == "input_port":
            above = [o for o, box in boxes.items() if o != cid and kinds.get(o) != "input_port" and box[1] <= top]
            if above:
                message = f"is not on the top row: {_label(cards, above[0])} is above or level with it"
                found.append(_issue(outline, "input_port_not_top", message, cid, cards[cid].get("name")))
            continue
        under = [o for o, box in boxes.items() if kinds.get(o) in solid and top < box[1] + box[3]]
        if under:
            message = f"is not on the bottom row: it is above the bottom edge of {_label(cards, under[0])}"
            found.append(_issue(outline, "output_port_not_bottom", message, cid, cards[cid].get("name")))
    return found


def _label(cards: dict[str, dict[str, Any]], cid: str) -> str:
    return str((cards.get(cid) or {}).get("name") or cid)


def _off_axis_issues(outline: dict[str, Any], cards: dict[str, dict[str, Any]]) -> list[Issue]:
    """A port or funnel must share its neighbour's axis (above or below it) or its row (a fork's side
    child). One that does neither sits off the top-down lattice and its label crowds the neighbour."""
    boxes = real_boxes(outline, {})
    kinds = {str(i["id"]): kind for key, kind in OUTLINE_KINDS.items() for i in outline.get(key) or [] if i.get("id")}
    edges = outline_edges(outline)
    found: list[Issue] = []
    for cid in cards:
        if cid not in boxes or kinds.get(cid) != "funnel":
            continue
        x, y, width, _h = boxes[cid]
        neighbours = [b for pair in edges if cid in pair for b in pair if b != cid and b in boxes]
        if neighbours and not any(
            abs(boxes[n][0] + boxes[n][2] / 2 - (x + width / 2)) < 1 or abs(boxes[n][1] - y) < 1 for n in neighbours
        ):
            if _is_laid_out(outline, cid):
                continue
            message = "is off the top-down axis of the cards it connects to"
            found.append(_issue(outline, "off_grid", message, cid, cards[cid].get("name")))
    return found


def _is_laid_out(outline: dict[str, Any], cid: str) -> bool:
    """Whether the layout itself puts this card where it is, such as a leaf of a wide fork one row down.
    Repair would leave it there, so flagging it could never be cleared."""
    try:
        return positions_from_flow(outline).get(cid) == _point(_cards(outline, tuple(OUTLINE_KINDS))[cid])
    except ValueError:
        return False


def is_connection_crossing(problem: str) -> bool:
    """Proper line intersections are advisory; label crossings and shared stretches are not."""
    return problem.startswith("connection ") and " crosses connection " in problem


def split_layout_issues(issues: list[Issue]) -> tuple[list[Issue], list[Issue]]:
    return (
        [issue for issue in issues if issue.get("severity") != "advisory"],
        [issue for issue in issues if issue.get("severity") == "advisory"],
    )


def connection_layout_issues(outline: dict[str, Any]) -> list[Issue]:
    """Inspect saved routes. Never regenerate bends just to decide whether a canvas is valid."""
    connections = outline.get("connections") or []
    pairs = outline_edges(outline)
    straight: dict[tuple[str, ...], list[dict[str, Any]]] = {}
    for conn, pair in zip(connections, pairs, strict=True):
        if all(pair) and not conn.get("bends"):
            straight.setdefault(tuple(sorted(pair)), []).append(conn)
    found = [
        {
            **_issue(
                outline, "overlapping_connections",
                "Unbent connections share canvas endpoints in either direction; add bends to separate them.",
                group[0].get("id"), group[0].get("name"),
            ),
            "connection_ids": [c["id"] for c in group if c.get("id")],
        }
        for group in straight.values() if len(group) > 1
    ]
    boxes = real_boxes(outline, {})
    card_problems = set(layout_problems(boxes, [], []))
    routes = [(c.get("bends") or [], c.get("label_index")) for c in connections]
    for problem in layout_problems(boxes, pairs, routes):
        if problem in card_problems:
            continue
        if is_connection_crossing(problem):
            found.append({**_issue(outline, "connection_crossing", problem), "severity": "advisory"})
        else:
            found.append(_issue(outline, "connection_geometry", problem))
    return found


def _errors(comp: dict[str, Any]) -> str:
    return "; ".join((comp.get("validationErrors") or [])[:8]) or "no reason given"


def _used_relationships(flow: dict[str, Any]) -> dict[str, set[str]]:
    """Relationship names each source in the group sends somewhere, by source id."""
    used: dict[str, set[str]] = {}
    for conn in flow.get("connections") or []:
        comp = conn.get("component") or {}
        used.setdefault((comp.get("source") or {}).get("id"), set()).update(comp.get("selectedRelationships") or [])
    return used


def _unterminated(comp: dict[str, Any], used: set[str]) -> list[str]:
    return [
        str(rel.get("name"))
        for rel in comp.get("relationships") or []
        if not rel.get("autoTerminate") and rel.get("name") not in used
    ]


def _processor_issues(group: dict[str, Any], raw: dict[str, Any]) -> list[Issue]:
    flow = raw.get("flow") or {}
    used = _used_relationships(flow)
    found: list[Issue] = []
    for proc in flow.get("processors") or []:
        comp = proc.get("component") or {}
        cid, name = proc.get("id") or comp.get("id"), comp.get("name")
        if comp.get("validationStatus") == "INVALID":
            found.append(_issue(group, "invalid_processor", _errors(comp), cid, name))
        found += [
            _issue(
                group,
                "unterminated_relationship",
                f"relationship '{rel}' is neither connected nor auto-terminated",
                cid,
                name,
            )
            for rel in _unterminated(comp, used.get(cid, set()))
        ]
    return found


def _endpoint_missing(end, group, known, readable_groups) -> bool:
    owner = str(end.get("groupId") or group["id"])
    if readable_groups is not None and owner not in readable_groups:
        return False  # An unreadable owner establishes uncertainty, never absence.
    return bool(end.get("id") and end.get("type") in _LOCAL_TYPES and end["id"] not in known)


def _connection_issues(
    group: dict[str, Any], raw: dict[str, Any], known: set[str], readable_groups: set[str] | None = None
) -> list[Issue]:
    found: list[Issue] = []
    for conn in (raw.get("flow") or {}).get("connections") or []:
        comp = conn.get("component") or {}
        for side in ("source", "destination"):
            end = comp.get(side) or {}
            if _endpoint_missing(end, group, known, readable_groups):
                found.append(
                    _issue(
                        group,
                        "dangling_connection",
                        f"{side} {end.get('name') or end['id']} no longer exists",
                        conn.get("id"),
                        comp.get("name"),
                    )
                )
    return found


_LOCAL_TYPES = frozenset({"PROCESSOR", "INPUT_PORT", "OUTPUT_PORT", "FUNNEL"})


def _live_users(comp: dict[str, Any]) -> list[str]:
    """Processors that reference a service and are not disabled."""
    users = [r.get("component") or {} for r in comp.get("referencingComponents") or []]
    return [
        str(user.get("name") or user.get("id"))
        for user in users
        if user.get("referenceType") == "PROCESSOR" and user.get("state") != "DISABLED"
    ]


def _service_issues(services: list[dict[str, Any]], names: dict[str, str]) -> list[Issue]:
    found: list[Issue] = []
    for svc in services:
        comp = svc.get("component") or {}
        group = {"id": comp.get("parentGroupId"), "name": names.get(str(comp.get("parentGroupId")))}
        cid, name = svc.get("id") or comp.get("id"), comp.get("name")
        if comp.get("validationStatus") == "INVALID":
            found.append(_issue(group, "invalid_service", _errors(comp), cid, name))
        live = _live_users(comp) if comp.get("state") in {"DISABLED", "DISABLING"} else []
        if live:
            found.append(
                _issue(
                    group, "disabled_service", f"is disabled; enabled processors use it: {', '.join(live)}", cid, name
                )
            )
    return found


async def _scan(client: NiFiClient, roots: list[str]) -> dict[str, Any]:
    hierarchy, raw, alias = await read_layout_hierarchy(client, roots, max_groups=MAX_GROUPS)
    # Ancestors supply port reachability, but validity checks stay within the touched subtrees.
    selected: set[str] = set()
    pending = [alias[g] for g in roots]
    while pending:
        gid = pending.pop()
        if gid in selected or gid not in hierarchy or gid not in raw:
            continue
        selected.add(gid)
        pending.extend(str(c["id"]) for c in hierarchy[gid].get("process_groups", []) if c.get("id"))
    outlines = {
        gid: await load_layout_intent(client, gid, hierarchy[gid]) for gid in sorted(selected)
    }
    flows = {gid: raw[gid] for gid in outlines}
    cut = any("read limit" in note for flow in hierarchy.values() for note in flow.get("hierarchy_uncertainties", []))
    known = {
        str(item["id"])
        for outline in hierarchy.values()
        for key in ("processors", "input_ports", "output_ports", "funnels")
        for item in outline.get(key) or []
        if item.get("id")
    }
    issues: list[Issue] = []
    for gid, entity in flows.items():
        flow = entity.get("processGroupFlow") or {}
        outline = outlines[gid]
        issues += layout_issues(outline) + _processor_issues(outline, flow)
        issues += _connection_issues(outline, flow, known, set(raw))
    services = await client.get_json(
        f"/flow/process-groups/{roots[0]}/controller-services",
        params={"includeDescendantGroups": "true", "includeAncestorGroups": "false"},
    )
    names = {gid: str(o.get("name") or "") for gid, o in outlines.items()}
    in_scope = [
        s
        for s in services.get("controllerServices") or []
        if str((s.get("component") or {}).get("parentGroupId")) in flows
    ]
    issues += _service_issues(in_scope, names)
    return {"issues": issues, "groups": outlines, "hierarchy": hierarchy, "alias": alias, "cut": cut}


async def _repair(
    client: NiFiClient, group_id: str, outlines: dict[str, dict[str, Any]]
) -> tuple[bool, bool]:
    """Re-run the layout on one group. Returns (laid out, now differs from its registry version): a
    versioned group is laid out like any other, and a write there is a local modification to commit."""
    entity = await client.get_process_group(group_id)
    versioned = bool((entity.get("component") or {}).get("versionControlInformation"))
    result = await relayout_process_group(client, group_id, skip_unchanged=True, _outlines=outlines)
    return True, versioned and bool(result.get("moved") or result.get("connections_routed"))


def _summary(scan: dict[str, Any], repaired: list[str], modified: list[str]) -> dict[str, Any]:
    issues, advisories = split_layout_issues(scan["issues"])
    out: dict[str, Any] = {
        "ok": not issues,
        "checked_groups": len(scan["groups"]),
        "issues": issues[:MAX_ISSUES],
        "layout_repaired": repaired,
        "design_review": {gid: review_graph(outline) for gid, outline in scan["groups"].items()},
    }
    uncertainties = {
        gid: graph.uncertainties for gid, outline in scan["groups"].items()
        if (graph := precedence_graph(outline)).uncertainties
    }
    if uncertainties:
        out["layout_uncertainties"] = uncertainties
    if advisories:
        out["layout_advisories"] = advisories[:MAX_ISSUES]
        if len(advisories) > MAX_ISSUES:
            out["layout_advisories_not_shown"] = len(advisories) - MAX_ISSUES
    if len(issues) > MAX_ISSUES:
        out["issues_not_shown"] = len(issues) - MAX_ISSUES
    if modified:
        out["layout_needs_registry_commit"] = modified
        out["next"] = (
            "nifi_commit_process_group on each group in layout_needs_registry_commit, or nifi_config reverts the layout"
        )
    if scan["cut"]:
        out["truncated"] = f"stopped at {MAX_GROUPS} groups"
    return out


def _descendants(groups: dict[str, dict[str, Any]], roots: list[str]) -> list[str]:
    """Parent before children, once each, restricted to canvases successfully read in this scan."""
    pending, ordered, seen = list(roots), [], set()
    while pending:
        gid = pending.pop(0)
        if gid in seen or gid not in groups:
            continue
        seen.add(gid)
        ordered.append(gid)
        pending.extend(str(c["id"]) for c in groups[gid].get("process_groups", []) if c.get("id"))
    return ordered


async def validate_after_action(client: NiFiClient, touched: dict[str, bool], *, auto_layout: bool) -> dict[str, Any]:
    """The validation block for one tool result. Never raises: a failed read is {ok: None, error}."""
    if not touched:
        return {"ok": None, "skipped": "this change does not belong to a process group"}
    try:
        roots = list(touched)
        scan = await _scan(client, roots)
        repaired: list[str] = []
        modified: list[str] = []
        # Every group a change touched is laid out again, flagged or not: a removal or an edit leaves gaps
        # and bends that no check reports. Only writes that differ from what NiFi holds are sent.
        due = [scan["alias"][g] for g in roots if touched[g] and scan["alias"].get(g)]
        if auto_layout and due:
            due = _descendants(scan["groups"], due)
            results = [(gid, await _repair(client, gid, scan["hierarchy"])) for gid in due]
            repaired = [gid for gid, (done, _changed) in results if done]
            modified = [gid for gid, (_done, changed) in results if changed]
            scan = await _scan(client, roots)
        return _summary(scan, repaired, modified)
    except Exception as exc:  # noqa: BLE001 - validation must never change the tool's outcome
        return {"ok": None, "error": f"{type(exc).__name__}: {str(exc)[:300]}"}
