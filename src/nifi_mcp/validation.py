"""Post-action validation: after a tool changes NiFi, read the changed process group and every group
below it once, report what is wrong, and re-run the layout on a touched group whose layout is.

Layout problems are repaired (NIFI_MCP_AUTO_LAYOUT, default true). Validity problems are only reported.
The result never changes the tool's outcome: a failed read reports ok=None and the error.
"""

from __future__ import annotations

import asyncio
import itertools
from contextvars import ContextVar
from typing import Any

from nifi_mcp.client import NiFiClient
from nifi_mcp.compact import compact_flow
from nifi_mcp.design import review_graph
from nifi_mcp.flow_spec import relayout_process_group
from nifi_mcp.layout import (
    CANVAS_GRID,
    OUTLINE_KINDS,
    infer_lattice,
    layout_problems,
    outline_edges,
    real_boxes,
    rects_overlap,
)

MAX_GROUPS = 200
MAX_ISSUES = 50
LAYOUT_KINDS = frozenset({"overlap", "lattice", "off_grid"})

# group id -> may the layout be repaired there. Set per tool call; a tool calls touch().
TOUCHED: ContextVar[dict[str, bool] | None] = ContextVar("nifi_mcp_touched_groups", default=None)

Issue = dict[str, Any]


def touch(group_id: str | None, *, repair: bool = True) -> None:
    """Record a process group this call changed, so validation covers it and everything below it."""
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
    groups = outline.get("process_groups") or []
    # A regular grid of any shape is the operator's own and relayout keeps it; only an irregular one is flagged.
    if len(groups) > 1 and infer_lattice([_point(g) for g in groups]) is None:
        found.append(_issue(outline, "lattice", "child process groups are not on a regular grid"))
    for cid, card in _cards(outline, ("processors", "input_ports", "output_ports", "process_groups")).items():
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
    return found + connection_layout_issues(outline)


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
    found += [
        _issue(outline, "connection_geometry", problem)
        for problem in layout_problems(boxes, pairs, routes) if problem not in card_problems
    ]
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


def _connection_issues(group: dict[str, Any], raw: dict[str, Any], known: set[str]) -> list[Issue]:
    found: list[Issue] = []
    for conn in (raw.get("flow") or {}).get("connections") or []:
        comp = conn.get("component") or {}
        for side in ("source", "destination"):
            end = comp.get(side) or {}
            if end.get("id") and end.get("type") in _LOCAL_TYPES and end["id"] not in known:
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


async def _read_tree(client: NiFiClient, roots: list[str]) -> tuple[dict[str, dict[str, Any]], dict[str, str], bool]:
    """Raw flow of every group under `roots` (one GET per group, a level at a time), the real id of each
    requested root ("root" is an alias), and whether the walk was cut short."""
    flows: dict[str, dict[str, Any]] = {}
    alias: dict[str, str] = {}
    level = list(dict.fromkeys(roots))
    while level and len(flows) < MAX_GROUPS:
        level = level[: MAX_GROUPS - len(flows)]
        children: list[str] = []
        for gid, entity in zip(level, await asyncio.gather(*(client.get_flow(g) for g in level)), strict=True):
            real = str((entity.get("processGroupFlow") or {}).get("id") or gid)
            alias[gid], flows[real] = real, entity
            children += [
                str(c["id"])
                for c in ((entity.get("processGroupFlow") or {}).get("flow") or {}).get("processGroups") or []
            ]
        level = [c for c in dict.fromkeys(children) if c not in flows]
    return flows, alias, bool(level)


async def _scan(client: NiFiClient, roots: list[str]) -> dict[str, Any]:
    flows, alias, cut = await _read_tree(client, roots)
    outlines = {gid: compact_flow(entity) for gid, entity in flows.items()}
    known = {
        str(item["id"])
        for outline in outlines.values()
        for key in ("processors", "input_ports", "output_ports", "funnels")
        for item in outline.get(key) or []
        if item.get("id")
    }
    issues: list[Issue] = []
    for gid, entity in flows.items():
        raw = entity.get("processGroupFlow") or {}
        outline = outlines[gid]
        issues += layout_issues(outline) + _processor_issues(outline, raw) + _connection_issues(outline, raw, known)
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
    return {"issues": issues, "groups": outlines, "alias": alias, "cut": cut}


async def _repair(client: NiFiClient, group_id: str) -> bool:
    """Re-run the layout on one group; False when it is under version control (left to the operator)."""
    entity = await client.get_process_group(group_id)
    if (entity.get("component") or {}).get("versionControlInformation"):
        return False
    await relayout_process_group(client, group_id)
    return True


def _summary(scan: dict[str, Any], repaired: list[str], skipped: list[str], root: list[str]) -> dict[str, Any]:
    issues = scan["issues"]
    out: dict[str, Any] = {
        "ok": not issues,
        "checked_groups": len(scan["groups"]),
        "issues": issues[:MAX_ISSUES],
        "layout_repaired": repaired,
        "design_review": {gid: review_graph(outline) for gid, outline in scan["groups"].items()},
    }
    if len(issues) > MAX_ISSUES:
        out["issues_not_shown"] = len(issues) - MAX_ISSUES
    if skipped:
        out["layout_not_repaired_versioned"] = skipped
    if root:
        out["layout_not_repaired_root"] = root
    if scan["cut"]:
        out["truncated"] = f"stopped at {MAX_GROUPS} groups"
    return out


async def validate_after_action(client: NiFiClient, touched: dict[str, bool], *, auto_layout: bool) -> dict[str, Any]:
    """The validation block for one tool result. Never raises: a failed read is {ok: None, error}."""
    if not touched:
        return {"ok": None, "skipped": "this change does not belong to a process group"}
    try:
        roots = list(touched)
        scan = await _scan(client, roots)
        repaired: list[str] = []
        skipped: list[str] = []
        layout_groups = {i["group_id"] for i in scan["issues"] if i["kind"] in LAYOUT_KINDS}
        due = [scan["alias"][g] for g in roots if touched[g] and scan["alias"].get(g) in layout_groups]
        is_root = {g for g in due if scan["groups"][g].get("parent_group_id") is None}
        due = [g for g in due if g not in is_root]
        if auto_layout and due:
            results = [(gid, await _repair(client, gid)) for gid in dict.fromkeys(due)]
            repaired = [gid for gid, done in results if done]
            skipped = [gid for gid, done in results if not done]
            scan = await _scan(client, roots)
        return _summary(scan, repaired, skipped, sorted(is_root))
    except Exception as exc:  # noqa: BLE001 - validation must never change the tool's outcome
        return {"ok": None, "error": f"{type(exc).__name__}: {str(exc)[:300]}"}
