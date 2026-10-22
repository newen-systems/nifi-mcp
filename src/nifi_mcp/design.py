"""Contract-first flow review. Findings are candidates, never proof of a bottleneck."""

from __future__ import annotations

from typing import Any

from nifi_mcp.layout import spec_kind

DESIGN_METHOD = """
Before designing or extending a flow, call nifi_review_flow_design on the target canvas.
1. Define the ingress contract before choosing processors: required fields, what the producer
   already knows, and what genuinely must be discovered at runtime. For registry ingestion, include
   known image identity, digest, registry/repository, layer descriptors/URLs when available,
   correlation/idempotency key and operation intent at ingress. Validate once; reject or quarantine incomplete requests.
   Use credential references and scoped controller services; never embed bearer tokens in webhook
   payloads, URLs, attributes or logs. Runtime credential acquisition/refresh can be necessary.
2. Map data dependencies, including retries/cycles; do not assume the whole NiFi graph is a DAG.
   Identify shared prerequisites and high-fan-out blocking dependency candidates. Resolve shared
   work once before fan-out; run independent work with bounded concurrency after it is eligible.
   A fan-out alone is not a measured bottleneck; use queue growth, rates and task duration to decide.
3. Justify each processor by a distinct transformation, I/O, routing, security or failure boundary.
   Reuse existing stages; combine compatible attribute assignments/extractions in one processor.
   Prefer record/batch processing and parameterized operations over one processor per field or layer.
   Sending one bulk MCP spec does not itself make the resulting NiFi flow batch-efficient.
   Preserve streaming, ordering, backpressure, idempotency and independent retry/failure semantics;
   do not replace a sensible flow with a giant script or batch that amplifies failures or memory use.
4. Present the minimal stage graph, input contract, batch boundary, shared prerequisites, bounded
   parallelism, retries/dead-letter path (ending at a stopped LogMessage, not UpdateAttribute) and reasons
   for any remaining enrichment stages.
   If upstream facts are unavailable, state the gap and required producer change before building.
5. Review the proposed spec with nifi_review_flow_design before applying it. Read the target and
   parent canvas before edits/additions; omit arbitrary x/y and use existing grid gaps, especially
   for root process groups. Keep connected stages in flow order, not merely in empty space.
   Inspect saved connection routes and layout warnings too; preserve deliberate manual bends.
6. After creation, call nifi_review_flow_design again on the actual group and reconcile the live
   graph with the contract and proposed stages. Resolve or justify every design finding before
   starting it; NiFi VALID and validation.ok do not certify efficiency. Verify representative input,
   missing fields, retries and duplicate delivery; measure throughput/queue growth under load before
   claiming optimization. Never invent critical-path timing without measurements.
""".strip()

_COMBINABLE = {"UpdateAttribute", "EvaluateJsonPath", "EvaluateXPath", "EvaluateXQuery"}


def _same_name(actual: str, expected: str) -> bool:
    """Relationship order on a saved connection is NiFi's, so compare the names as a set."""
    return actual == expected or set(actual.split(", ")) == set(expected.split(", "))


def _name_findings(connections: list[dict[str, Any]]) -> list[dict[str, Any]]:
    found = []
    for conn in connections:
        src = conn.get("source") or {}
        name = conn.get("name")
        if not name:
            continue
        if src.get("type") == "PROCESSOR":
            expected = ", ".join(r for r in conn.get("relationships") or [] if r)
        elif src.get("type") in {"INPUT_PORT", "OUTPUT_PORT"}:
            expected = src.get("name") or ""
        else:
            expected = ""
        if expected and not _same_name(str(name), expected):
            found.append({
                "kind": "connection_name", "components": [conn.get("id")],
                "message": f"Leave the connection unnamed, NiFi shows '{expected}'; this name contradicts it.",
            })
    return found


DEFAULT_OBJECT_THRESHOLD = 10000


def _threshold(conn: dict[str, Any]) -> int | None:
    """The object threshold when it is a limit low enough to fill; 0 is NiFi's 'no limit', None is unset."""
    value = conn.get("back_pressure_object_threshold")
    if isinstance(value, bool) or not isinstance(value, int):
        return None
    return value if 0 < value < DEFAULT_OBJECT_THRESHOLD else None


def _reach(start: str, adjacency: dict[str, set[str]]) -> set[str]:
    seen, todo = {start}, [start]
    while todo:
        for nxt in adjacency.get(todo.pop(), ()):
            if nxt not in seen:
                seen.add(nxt)
                todo.append(nxt)
    return seen


def _deadlock_findings(nodes: dict[str, str], connections: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Loops inside one group where every connection holds fewer than NiFi's default 10000 objects.

    NiFi does not schedule a processor while any outgoing connection is full, so a loop whose queues
    can all fill stops for good. One finding per strongly connected set of low-limit connections."""
    low = []
    for conn in connections:
        source, dest = (conn.get("source") or {}).get("id"), (conn.get("destination") or {}).get("id")
        limit = _threshold(conn)
        if source in nodes and dest in nodes and limit is not None:
            low.append((source, dest, str(conn.get("name") or conn.get("id")), limit, conn.get("id")))
    forward: dict[str, set[str]] = {}
    backward: dict[str, set[str]] = {}
    for source, dest, *_ in low:
        forward.setdefault(source, set()).add(dest)
        backward.setdefault(dest, set()).add(source)
    found, done = [], set()
    for node in sorted(forward):
        if node in done:
            continue
        members = _reach(node, forward) & _reach(node, backward)
        done |= members
        edges = [(s, d, label, limit, cid) for s, d, label, limit, cid in low if s in members and d in members]
        if edges:
            found.append(_deadlock_finding(members, edges, nodes))
    return found


def _deadlock_finding(members: set[str], edges: list[tuple], nodes: dict[str, str]) -> dict[str, Any]:
    text = ", ".join(f"{nodes[s]} -> {nodes[d]} via '{label}' ({limit})" for s, d, label, limit, _ in edges)
    return {
        "kind": "backpressure_loop_deadlock",
        "components": sorted(members) + [e[4] for e in edges if e[4]],
        "message": f"Loop of {', '.join(sorted(nodes[m] for m in members))} can deadlock: {text}. NiFi does not "
        "schedule a processor while any of its outgoing connections is full, so once every connection in the "
        "loop is full each stage waits on the next and nothing moves. Set the loop connections' Back Pressure "
        f"Object Threshold to {DEFAULT_OBJECT_THRESHOLD} (NiFi's default) and keep the data size threshold.",
    }


def review_graph(outline: dict[str, Any]) -> dict[str, Any]:
    """Use topology and types only: no payloads, properties or credentials in the report."""
    processors = {p["id"]: p for p in outline.get("processors", [])}
    edges = {
        (c["source"]["id"], c["destination"]["id"])
        for c in outline.get("connections", [])
        if c.get("source", {}).get("id") and c.get("destination", {}).get("id")
    }
    nodes = {
        str(item["id"]): str(item.get("name") or item["id"])
        for key in ("processors", "input_ports", "output_ports", "funnels")
        for item in outline.get(key) or []
        if item.get("id")
    }
    findings = _deadlock_findings(nodes, outline.get("connections", []))
    for source, target in sorted(edges):
        left, right = processors.get(source, {}), processors.get(target, {})
        kind = str(left.get("type", "")).rsplit(".", 1)[-1]
        if source != target and kind in _COMBINABLE and kind == str(right.get("type", "")).rsplit(".", 1)[-1]:
            findings.append({
                "kind": "consolidation_candidate", "components": [source, target],
                "message": "Check whether these stages can share one extraction/assignment pass. Preserve "
                "attribute evaluation order, routing and failure boundaries; do not merge automatically.",
            })
    findings += _name_findings(outline.get("connections", []))
    outgoing: dict[str, set[str]] = {}
    for source, target in edges:
        if source != target:
            outgoing.setdefault(source, set()).add(target)
    for source, targets in sorted(outgoing.items()):
        if len(targets) > 1:
            findings.append({
                "kind": "shared_prerequisite_review", "components": [source], "fan_out": len(targets),
                "message": "Review this fan-out for shared blocking work and bounded downstream concurrency. "
                "Relationships may be alternatives, not parallel work; measure before calling it a bottleneck.",
            })
    return {
        "status": "review_required", "processor_count": len(processors), "connection_count": len(edges),
        "findings": findings[:50], "findings_not_shown": max(0, len(findings) - 50),
        "limitations": "Topology cannot establish input completeness, safe consolidation or throughput. "
        "An empty findings list is not design approval.",
    }


def review_spec(objects: list[dict[str, Any]], connections: list[dict[str, Any]]) -> dict[str, Any]:
    """Review normalized spec topology, identifying submitted objects by safe structural refs."""
    refs = {item.get("name"): item["_ref"] for item in objects}
    processors = [
        {"id": item["_ref"], "type": item.get("processor_type") or item.get("class") or ""}
        for item in objects if spec_kind(item) == "processor"
    ]
    links = [item for item in objects if spec_kind(item) == "connection"] + connections
    return review_graph({
        "processors": processors,
        "connections": [
            {"id": f"connections[{i}]", "name": f"connections[{i}]",
             "source": {"id": refs.get(item.get("source") or item.get("from"))},
             "destination": {"id": refs.get(item.get("target") or item.get("destination") or item.get("to"))},
             "back_pressure_object_threshold": item.get("back_pressure_object_threshold")}
            for i, item in enumerate(links)
        ],
    })
