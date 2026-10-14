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
   parallelism, retries/dead-letter path and reasons for any remaining enrichment stages.
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


def review_graph(outline: dict[str, Any]) -> dict[str, Any]:
    """Use topology and types only: no payloads, properties or credentials in the report."""
    processors = {p["id"]: p for p in outline.get("processors", [])}
    edges = {
        (c["source"]["id"], c["destination"]["id"])
        for c in outline.get("connections", [])
        if c.get("source", {}).get("id") and c.get("destination", {}).get("id")
    }
    findings = []
    for source, target in sorted(edges):
        left, right = processors.get(source, {}), processors.get(target, {})
        kind = str(left.get("type", "")).rsplit(".", 1)[-1]
        if source != target and kind in _COMBINABLE and kind == str(right.get("type", "")).rsplit(".", 1)[-1]:
            findings.append({
                "kind": "consolidation_candidate", "components": [source, target],
                "message": "Check whether these stages can share one extraction/assignment pass. Preserve "
                "attribute evaluation order, routing and failure boundaries; do not merge automatically.",
            })
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
            {"source": {"id": refs.get(item.get("source") or item.get("from"))},
             "destination": {"id": refs.get(item.get("target") or item.get("destination") or item.get("to"))}}
            for item in links
        ],
    })
