"""Read-only, port-expanded topology shared by layout and saved-position checks.

``enrich_hierarchy({group_id: compact_outline})`` returns deep copies with
``port_context = {forward, feedback, cycle_feedback, dependencies, ranks, uncertainties}``.
Pairs contain visible card IDs; ranks contain this canvas's port IDs. Feedback
includes a later visit to an earlier group card, even when the expanded graph is
acyclic. ``cycle_feedback`` is the subset whose expanded edges were cut to break
real cycles; callers can replace those heuristic cuts with valid explicit cycle
intent. Structural group returns remain in ``feedback`` without being added to
``cycle_feedback``. A visible pair may occur in both forward and feedback when
separate port paths require each classification; preserve its forward constraint.
Dependencies are layout constraints, never executable connections.

Only observed connections establish reachability. Child groups are expanded at
actual ports, never treated as all-inputs-to-all-outputs switches. Remote groups
remain opaque. Missing/denied reads cannot establish negative reachability.
"""
from __future__ import annotations

from collections import defaultdict
from copy import deepcopy
from typing import Any

from nifi_mcp.compact import compact_flow

_ATOMIC = ("processors", "input_ports", "output_ports", "funnels")
_CARDS = (*_ATOMIC, "process_groups", "remote_process_groups")
Node = tuple[str, str]


def _ids(flow: dict, keys: tuple[str, ...]) -> set[str]:
    return {str(c["id"]) for key in keys for c in flow.get(key, []) if c.get("id")}


def _endpoint(gid: str, end: dict, outlines: dict, notes: list[str]) -> Node | None:
    flow = outlines[gid]
    owner = str(end.get("group_id") or end.get("groupId") or gid)
    cid = str(end.get("id") or "")
    if owner in _ids(flow, ("remote_process_groups",)):
        notes.append(f"remote endpoint {owner}/{cid} has unknown internal reachability")
        return None
    if owner != gid and owner not in _ids(flow, ("process_groups",)):
        notes.append(f"endpoint {owner}/{cid} is not owned by this canvas or a child")
        return None
    if owner not in outlines or cid not in _ids(outlines[owner], _ATOMIC):
        notes.append(f"endpoint {owner}/{cid} is unreadable or absent; reachability unknown")
        return None
    if owner != gid and cid not in _ids(outlines[owner], ("input_ports", "output_ports")):
        notes.append(f"child endpoint {owner}/{cid} is not a port")
        return None
    return owner, cid


def _visible(gid: str, end: dict, flow: dict) -> str:
    owner = str(end.get("group_id") or end.get("groupId") or gid)
    return owner if owner in _ids(flow, ("process_groups", "remote_process_groups")) else str(end.get("id") or "")


def _order(nodes: set[Node], edges: set[tuple[Node, Node]]) -> tuple[dict, set, list[str]]:
    """Kahn order; cut only incoming edges to a stable entry when truly cyclic."""
    reachable = _reachable(edges)
    done: set[Node] = set()
    ranks: dict[Node, int] = {}
    feedback = set()
    notes = []
    incoming: dict[Node, set[Node]] = defaultdict(set)
    for src, dst in edges:
        incoming[dst].add(src)
    while len(done) < len(nodes):
        waiting = sorted(nodes - done)
        ready = [n for n in waiting if not (incoming[n] - done)]
        if ready:
            node = ready[0]
        else:
            # Enter only a source SCC of the remaining graph. A downstream SCC
            # may have more completed inputs, but cutting its upstream dependency
            # would incorrectly label an ordinary forward edge as feedback.
            entries = [n for n in waiting if all(
                src == n or src in reachable.get(n, set()) for src in incoming[n] - done
            )]
            node = min(entries, key=lambda n: (-len(incoming[n] & done), n))
            cuts = {(src, node) for src in incoming[node] - done}
            feedback.update(cuts)
            notes.append(f"real cycle entered at {node[0]}/{node[1]}; feedback inferred, confirm layout intent")
        predecessors = [src for src in incoming[node] if (src, node) not in feedback]
        ranks[node] = max((ranks[src] + 1 for src in predecessors), default=0)
        done.add(node)
    return ranks, feedback, notes


def _reachable(edges: set[tuple[Node, Node]]) -> dict[Node, set[Node]]:
    adjacent: dict[Node, set[Node]] = defaultdict(set)
    for src, dst in edges:
        adjacent[src].add(dst)
    result = {}
    for start in sorted(adjacent):
        todo, seen = list(adjacent[start]), set()
        while todo:
            node = todo.pop()
            if node not in seen:
                seen.add(node)
                todo.extend(adjacent[node] - seen)
        result[start] = seen
    return result


def _members(gid: str, outlines: dict, active: frozenset = frozenset()) -> set[Node]:
    if gid in active or gid not in outlines:
        return set()
    members = {(gid, cid) for cid in _ids(outlines[gid], _ATOMIC)}
    for child in _ids(outlines[gid], ("process_groups",)):
        members.update(_members(child, outlines, active | {gid}))
    return members


def _disconnected_dependencies(gid, flow, records, cards, reach, members) -> set:
    local = {(s, d) for s, d, _ in records}
    linked = _reachable({((gid, s), (gid, d)) for s, d in local | {(d, s) for s, d in local}})
    dependencies = set()
    eligible = cards - _ids(flow, ("input_ports", "output_ports", "remote_process_groups"))
    for src in sorted(eligible):
        for dst in sorted(eligible - {src}):
            if (gid, dst) in linked.get((gid, src), set()):
                continue
            if any(reach.get(n, set()) & members[dst] for n in members[src]):
                dependencies.add((src, dst))
    return dependencies


def _context(
    gid: str, outlines: dict, records: list, ranks: dict, feedback: set, reach: dict, incomplete: set[str],
) -> dict:
    flow = outlines[gid]
    cards = _ids(flow, _CARDS)
    members = {cid: {(gid, cid)} for cid in cards}
    for child in _ids(flow, ("process_groups",)):
        members[child] = _members(child, outlines)
    # Stage numbers from independent roots are not comparable: an unrelated
    # short path into a group must not turn a long upstream path into feedback.
    # A return is proven by an earlier outgoing port of the destination group
    # reaching this connection's source through the expanded forward graph.
    outgoing: dict[str, set[Node]] = defaultdict(set)
    groups = _ids(flow, ("process_groups",))
    for source, _, (src, _) in records:
        outgoing[source].add(src)
    forward, returns = set(), set()
    cycle_returns = {(source, target) for source, target, expanded in records if expanded in feedback}
    for source, target, expanded in records:
        pair = source, target
        if ({source, target} & groups & incomplete) and expanded not in feedback:
            # Missing internal paths might close a presently apparent false cycle.
            # Defer to ordinary actual-edge handling rather than assert a return.
            continue
        earlier_visit = target in groups and any(
            expanded[0] in reach.get(port, set()) for port in outgoing[target]
        )
        if expanded in feedback or source == target or earlier_visit:
            returns.add(pair)
        else:
            forward.add(pair)
    # Do not infer a handoff within a locally connected area. Context orders only
    # otherwise disconnected chains, using paths that leave and re-enter a canvas.
    dependencies = _disconnected_dependencies(gid, flow, records, cards, reach, members)
    ports = _ids(flow, ("input_ports", "output_ports"))
    return {
        "forward": [list(p) for p in sorted(forward)],
        "feedback": [list(p) for p in sorted(returns)],
        "cycle_feedback": [list(p) for p in sorted(cycle_returns)],
        "dependencies": [list(p) for p in sorted(dependencies)],
        "ranks": {p: ranks[(gid, p)] for p in sorted(ports) if (gid, p) in ranks},
        "uncertainties": [],
    }


def _canvas_connections(gid, flow, result, notes, edges, records) -> None:
    for conn in flow.get("connections", []):
        ends = [conn.get(k) or {} for k in ("source", "destination")]
        src, dst = [_endpoint(gid, end, result, notes) for end in ends]
        if src is not None and dst is not None:
            edges.add((src, dst))
            records[gid].append((*[_visible(gid, e, flow) for e in ends], (src, dst)))


def enrich_hierarchy(outlines: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    """Enrich compact outlines without mutating input or adding NiFi connections.

    Incomplete reads may be supplied as ``hierarchy_uncertainties: [str, ...]``.
    Unresolved actual pairs are omitted from both classifications: callers must
    retain their ordinary actual-edge handling for those pairs.
    """
    result = deepcopy(outlines)
    notes = []
    nodes = {(gid, cid) for gid, flow in result.items() for cid in _ids(flow, _ATOMIC)}
    edges: set[tuple[Node, Node]] = set()
    records: dict[str, list] = defaultdict(list)
    incomplete: set[str] = set()
    for gid, flow in sorted(result.items()):
        notes.extend(flow.get("hierarchy_uncertainties", []))
        before = len(notes)
        for child in sorted(_ids(flow, ("process_groups",)) - result.keys()):
            notes.append(f"child canvas {child} unreadable; port reachability unknown")
        _canvas_connections(gid, flow, result, notes, edges, records)
        if len(notes) > before:
            incomplete.add(gid)
    # Missing descendants affect their ancestors, not unrelated sibling groups.
    while True:
        parents = {gid for gid, flow in result.items()
                   if _ids(flow, ("process_groups",)) & incomplete}
        if parents <= incomplete:
            break
        incomplete.update(parents)
    ranks, feedback, cycles = _order(nodes, edges)
    reach = _reachable(edges - feedback)
    for gid, flow in result.items():
        flow["port_context"] = _context(gid, result, records[gid], ranks, feedback, reach, incomplete)
        flow["port_context"]["uncertainties"] = sorted(set(notes + cycles))
    return result


async def _read_canvas(client, gid, notes):
    try:
        raw = await client.get_flow(gid)
    except Exception as exc:  # noqa: BLE001 — denied/unavailable topology is explicitly unknown
        notes.append(f"canvas {gid} read failed ({type(exc).__name__}); reachability unknown")
        return None
    flow = compact_flow(raw) if "processGroupFlow" in raw or "flow" in raw else deepcopy(raw)
    return flow


def _queue_relatives(flow, pending, attempted, outlines) -> None:
    parent = flow.get("parent_group_id")
    if parent and parent not in attempted and parent not in outlines:
        pending.insert(0, str(parent))
    pending.extend(sorted(_ids(flow, ("process_groups",)) - attempted - outlines.keys()))


async def load_hierarchy(client: Any, group_id: str = "root", *, max_groups: int = 256) -> dict[str, dict[str, Any]]:
    """Read requested canvas, ancestors, and their descendants, with a read cap.

    At most ``max_groups`` get_flow attempts are made, including denied reads.
    Cancellation propagates. Failures are reported without server error details.
    The mapping uses actual group IDs (``root`` is an API alias only).
    """
    if max_groups < 1:
        raise ValueError("max_groups must be positive")
    outlines, attempted, notes = {}, set(), []
    pending = [group_id]
    while pending and len(attempted) < max_groups:
        gid = pending.pop(0)
        if gid in attempted or gid in outlines:
            continue
        attempted.add(gid)
        flow = await _read_canvas(client, gid, notes)
        if flow is None:
            continue
        actual = str(flow.get("id") or gid)
        outlines[actual] = flow
        _queue_relatives(flow, pending, attempted, outlines)
    if any(g not in outlines and g not in attempted for g in pending):
        notes.append(f"hierarchy read limit {max_groups} reached; remaining reachability unknown")
    if not outlines:
        outlines[group_id] = {"id": group_id}
    for flow in outlines.values():
        flow["hierarchy_uncertainties"] = sorted(set(notes))
    return enrich_hierarchy(outlines)
