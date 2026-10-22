"""Canvas precedence, independent of geometry and executable NiFi connections.

Actual edges are authoritative. Explicit layout intent supplies logical handoffs;
scoped domain roles are weaker evidence. Names resolve references, never infer roles.
"""
from __future__ import annotations

import json
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

MARKER = "nifi-layout: "
KINDS = {
    "processors": "processor", "process_groups": "process_group",
    "input_ports": "input_port", "output_ports": "output_port",
    "funnels": "funnel", "remote_process_groups": "remote_process_group",
}
# These sequences apply only inside an explicitly declared domain, using declared roles.
DOMAIN_ROLES = {
    "diode-low-to-high": ("low", "transfer", "high"),
    "log-delivery": ("collection", "ingestion", "parsing", "enrichment", "delivery"),
    "etl": ("extract", "transform", "load"),
    "receive-publish": ("receive", "validate", "route", "publish"),
    "read-write": ("read", "decode", "process", "encode", "write"),
    "request-response": ("arrival", "handling", "response"),
    "data-storage": ("raw", "normalisation", "aggregation", "storage"),
    "input-output": ("input", "processing", "output"),
    "source-destination": ("source", "transformation", "destination"),
    "incoming-outgoing": ("incoming", "handling", "outgoing"),
}


class Dependency(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    source: str = Field(min_length=1)
    target: str = Field(min_length=1)


class Area(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    id: str = Field(min_length=1)
    members: list[str]
    independent: bool = True


class LayoutIntent(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    dependencies: list[Dependency] = Field(default_factory=list)
    stages: list[list[str]] = Field(default_factory=list)
    feedback: list[Dependency] = Field(default_factory=list)
    areas: list[Area] = Field(default_factory=list)
    domain: str | None = None
    roles: dict[str, str] = Field(default_factory=dict)


def read_intent(comments: str | None) -> dict[str, Any]:
    lines = [line[len(MARKER):] for line in (comments or "").splitlines() if line.startswith(MARKER)]
    if len(lines) > 1:
        raise ValueError("multiple nifi-layout declarations; keep exactly one")
    if not lines:
        return {}
    return LayoutIntent.model_validate(json.loads(lines[0])).model_dump(exclude_defaults=True)


def write_intent(comments: str | None, intent: dict[str, Any] | LayoutIntent) -> str:
    parsed = LayoutIntent.model_validate(intent).model_dump(exclude_defaults=True)
    prose = "\n".join(line for line in (comments or "").splitlines() if not line.startswith(MARKER)).rstrip()
    declaration = MARKER + json.dumps(parsed, separators=(",", ":"), sort_keys=True)
    return f"{prose}\n{declaration}" if prose else declaration


def reaches(edges: list[tuple[str, str]], start: str, goal: str) -> bool:
    todo, seen = [start], set()
    adjacent: dict[str, list[str]] = defaultdict(list)
    for src, dst in edges:
        adjacent[src].append(dst)
    while todo:
        node = todo.pop()
        if node == goal:
            return True
        if node not in seen:
            seen.add(node)
            todo.extend(adjacent[node])
    return False


@dataclass
class Precedence:
    nodes: list[str]
    kinds: dict[str, str]
    forward: list[tuple[str, str]] = field(default_factory=list)
    feedback: list[tuple[str, str]] = field(default_factory=list)
    conflicts: list[str] = field(default_factory=list)
    uncertainties: list[str] = field(default_factory=list)
    areas: dict[str, str] = field(default_factory=dict)

    def add(self, pair: tuple[str, str], evidence: str) -> None:
        if pair in self.forward:
            return
        src, dst = pair
        if src == dst or reaches(self.forward, dst, src):
            self.conflicts.append(f"{evidence} {src} -> {dst} conflicts with established flow order")
        else:
            self.forward.append(pair)


def _resolve(ref: str, cards: dict[str, dict], graph: Precedence) -> str | None:
    if ref in cards:
        return ref
    matches = [cid for cid, card in cards.items() if card.get("name") == ref]
    if len(matches) == 1:
        return matches[0]
    graph.conflicts.append(f"layout reference {ref!r} is {'ambiguous' if matches else 'unresolved'}")
    return None


def _pairs(items: list[Dependency], cards: dict, graph: Precedence) -> list[tuple[str, str]]:
    pairs = []
    for item in items:
        src, dst = _resolve(item.source, cards, graph), _resolve(item.target, cards, graph)
        if src and dst:
            pairs.append((src, dst))
    return pairs


def _actual(flow: dict, cards: dict, graph: Precedence) -> list[tuple[str, str]]:
    groups = {cid for cid, kind in graph.kinds.items() if kind in {"process_group", "remote_process_group"}}

    def end(value: dict) -> str:
        group = str(value.get("group_id") or value.get("groupId") or "")
        return group if group in groups else str(value.get("id") or "")

    pairs = []
    for conn in flow.get("connections") or []:
        pair = end(conn.get("source") or {}), end(conn.get("destination") or {})
        if all(cid in cards for cid in pair):
            pairs.append(pair)
        else:
            graph.uncertainties.append(f"connection {conn.get('id', '')} has an endpoint absent from this canvas")
    return sorted(set(pairs))


def _next_actual_node(nodes, remaining, done) -> str:
    waiting = [n for n in nodes if n not in done]
    ready = [n for n in waiting if not any(d == n and s not in done and s != d for s, d in remaining)]
    # Only cut incoming edges that are themselves in a genuine cycle.
    # A downstream region with more completed inputs must not steal an
    # ordinary dependency from an unresolved upstream cyclic region.
    candidates = ready or [n for n in waiting if all(
        s in done or s == n or reaches(remaining, n, s) for s, d in remaining if d == n
    )]
    node = candidates[0] if ready else max(
        candidates, key=lambda n: sum(s in done and d == n for s, d in remaining)
    )
    return node


def _declared_cycle_feedback(pairs, declared, graph) -> None:
    for pair in declared:
        rest = [edge for edge in pairs if edge != pair]
        if pair not in pairs or not reaches(rest, pair[1], pair[0]):
            graph.conflicts.append(f"declared feedback {pair[0]} -> {pair[1]} is not an actual cycle edge")
        else:
            graph.feedback.append(pair)


def _actual_order(pairs: list[tuple[str, str]], declared: list[tuple[str, str]], graph: Precedence) -> None:
    _declared_cycle_feedback(pairs, declared, graph)
    remaining = [p for p in pairs if p not in graph.feedback]
    done: set[str] = set()
    while len(done) < len(graph.nodes):
        node = _next_actual_node(graph.nodes, remaining, done)
        for pair in [p for p in remaining if p[0] == node]:
            if pair[1] in done or pair[1] == node:
                graph.feedback.append(pair)
                if pair[0] != pair[1]:
                    graph.uncertainties.append(
                        f"cycle break {pair[0]} -> {pair[1]} inferred; declare feedback to confirm"
                    )
            else:
                graph.forward.append(pair)
        done.add(node)


def _area_members(area, cards, graph) -> None:
    for ref in area.members:
        cid = _resolve(ref, cards, graph)
        if cid and area.independent:
            if cid in graph.areas:
                graph.conflicts.append(f"component {cid} belongs to multiple independent areas")
            graph.areas[cid] = area.id


def _areas(intent: LayoutIntent, cards: dict, graph: Precedence) -> None:
    seen: set[str] = set()
    for area in intent.areas:
        if area.id in seen:
            graph.conflicts.append(f"duplicate layout area {area.id!r}")
        seen.add(area.id)
        _area_members(area, cards, graph)
    for src, dst in graph.forward:
        if graph.areas.get(src) != graph.areas.get(dst) and (src in graph.areas or dst in graph.areas):
            graph.conflicts.append(f"independent area conflicts with actual dependency {src} -> {dst}")


def _explicit(intent: LayoutIntent, cards: dict, graph: Precedence) -> None:
    pairs = _pairs(intent.dependencies, cards, graph)
    stages = [[cid for ref in stage if (cid := _resolve(ref, cards, graph))] for stage in intent.stages]
    # Every earlier stage precedes every later one, even across an empty stage.
    pairs += [(src, dst) for i, stage in enumerate(stages)
              for later in stages[i + 1:] for src in stage for dst in later]
    for src, dst in pairs:
        if graph.areas.get(src) != graph.areas.get(dst) and (src in graph.areas or dst in graph.areas):
            graph.conflicts.append(f"layout dependency {src} -> {dst} crosses an explicitly independent area")
        else:
            graph.add((src, dst), "layout dependency")


def _semantic(intent: LayoutIntent, cards: dict, graph: Precedence) -> None:
    if not intent.roles:
        return
    roles = DOMAIN_ROLES.get(intent.domain or "")
    if roles is None:
        graph.uncertainties.append("semantic roles require a supported explicit domain; no name-based order inferred")
        return
    ranked = {}
    for ref, role in intent.roles.items():
        cid = _resolve(ref, cards, graph)
        if role not in roles:
            graph.conflicts.append(f"role {role!r} is unknown in domain {intent.domain!r}")
        elif cid:
            ranked[cid] = roles.index(role)
    for src in sorted(ranked):
        for dst in sorted(ranked):
            if ranked[src] < ranked[dst] and graph.areas.get(src) == graph.areas.get(dst):
                graph.add((src, dst), "domain role")


def _declared_returns(
    actual: list[tuple[str, str]], declared: list[tuple[str, str]],
    returns: set[tuple[str, str]], pinned: set[tuple[str, str]],
    cycles: set[tuple[str, str]], graph: Precedence,
) -> list[tuple[str, str]]:
    valid = []
    for pair in declared:
        # A proven later group visit can be confirmed as feedback without treating
        # the collapsed group cycle as a real port-level cycle.
        if pair in returns:
            continue
        same_cycle = {edge for edge in cycles
                      if reaches(actual, pair[0], edge[0]) and reaches(actual, edge[0], pair[0])}
        if pair in pinned and not same_cycle:
            graph.conflicts.append(f"declared feedback {pair[0]} -> {pair[1]} conflicts with port-level progression")
        else:
            returns.difference_update(same_cycle)
            valid.append(pair)
    return valid


def _topology(actual: list[tuple[str, str]], intent: LayoutIntent, cards: dict, flow: dict, graph: Precedence) -> None:
    """Port-expanded classifications outrank collapsed card cycles.

    Unclassified connections retain ordinary graph treatment. Context is generated
    from NiFi reads by hierarchy.py, never parsed from user layout intent.
    """
    context = flow.get("port_context") or {}
    notes = context.get("uncertainties") or []
    if intent.feedback:
        notes = ["port topology contains cycles; declared feedback controls this canvas where valid"
                 if note.startswith("real cycle entered") else note for note in notes]
    graph.uncertainties.extend(dict.fromkeys(notes))
    returns = {tuple(p) for p in context.get("feedback", [])} & set(actual)
    pinned = {tuple(p) for p in context.get("forward", [])} & set(actual)
    cycles = {tuple(p) for p in context.get("cycle_feedback", [])} & set(actual)
    valid = _declared_returns(actual, _pairs(intent.feedback, cards, graph), returns, pinned, cycles, graph)
    # Some distinct port connections collapse onto the same visible pair: retain
    # forward precedence even when another connection between those cards returns.
    graph.feedback.extend(sorted(returns - pinned))
    _actual_order([p for p in actual if p not in returns or p in pinned], valid, graph)
    graph.feedback.extend(sorted(returns - set(graph.feedback)))
    for pair in context.get("dependencies", []):
        edge = tuple(pair)
        if len(edge) == 2 and all(n in cards for n in edge):
            graph.add(edge, "parent port handoff")


def precedence_graph(flow: dict[str, Any]) -> Precedence:
    cards = {str(c["id"]): c for key in KINDS for c in flow.get(key) or [] if c.get("id")}
    kinds = {str(c["id"]): kind for key, kind in KINDS.items() for c in flow.get(key) or [] if c.get("id")}
    graph = Precedence(sorted(cards), kinds)
    try:
        intent = LayoutIntent.model_validate(flow.get("layout_intent") or {})
    except ValueError:
        graph.conflicts.append("invalid layout intent; correct its schema before relying on semantic ordering")
        intent = LayoutIntent()
    actual = _actual(flow, cards, graph)
    _topology(actual, intent, cards, flow, graph)
    _areas(intent, cards, graph)
    _explicit(intent, cards, graph)
    _semantic(intent, cards, graph)
    connected = {cid for pair in graph.forward + graph.feedback for cid in pair}
    unknown = [cid for cid in graph.nodes if cid not in connected and cid not in graph.areas]
    if len(graph.nodes) > 1 and unknown:
        graph.uncertainties.append("order unknown for unconnected components: " + ", ".join(unknown))
    # Distinguish independent components from several real inputs joining one flow.
    components = [{n} for n in graph.nodes]
    for src, dst in graph.forward + graph.feedback:
        touching = [part for part in components if src in part or dst in part]
        merged = set().union(*touching)
        components = [part for part in components if part not in touching] + [merged]
    if len(components) > 1 and not unknown and not graph.areas:
        graph.uncertainties.append("inter-flow order is unknown without declared layout intent")
    graph.nodes.sort(key=lambda cid: (graph.areas.get(cid, ""), cid))
    return graph
