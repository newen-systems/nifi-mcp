# Contract-first, dependency-aware flow design

Before choosing processors, call `nifi_review_flow_design` on the target group. The tool reads the
canvas and returns the methodology, a declared-input check and topology review. For example:

```json
{
  "params": {
    "process_group_id": "root",
    "required_fields": ["image_digest", "registry", "repository", "correlation_id"],
    "supplied_fields": ["image_digest", "registry", "repository"],
    "runtime_fields": []
  }
}
```

This reports `correlation_id` as missing. These lists contain field names, never secret values.
They describe the producer contract; the server cannot verify the producer's actual payload.
Fields in `runtime_fields` require justification: why can the producer not supply them? Use scoped
credential references/controller services instead of passing bearer tokens through webhook payloads.

The agent must establish the input contract, explain the minimal stage graph, identify shared
prerequisites before fan-out, and choose batch boundaries and bounded concurrency. Prefer one
compatible extraction/assignment pass over a processor per field, and parameterized processing over
a processor per image layer. Preserve evaluation order, backpressure, ordering, idempotency and
independent failure/retry boundaries. A fan-out is a review candidate, not evidence of a bottleneck.

Call the review again with `spec` before applying the proposed flow, and on the actual group after
creation. `nifi_apply_flow_spec` automatically analyzes normalized topology before its first write;
its result contains `design_review`. Applied mutations also include live design reviews under
`validation.design_review`, using the existing recursive canvas read. Current heuristics flag
consecutive processors of the same extraction/attribute type and multi-target fan-outs.

The methodology is an agent obligation, not a stateful authorization gate. Findings are advisory;
the server does not block writes or merge processors automatically. `review_required` remains even
with no findings: graph structure cannot prove contract completeness or safe consolidation. NiFi
`validation.ok` continues to describe validity/layout, not efficiency. Resolve or justify design
findings before starting, exercise representative and malformed inputs/retries/duplicates, and
measure throughput and queue growth before claiming an optimization.

Placement reads the canvas. New root process groups fill a free cell in the existing grid even if
x/y are supplied; the result explains ignored coordinates. A single group anchors a new column.
Nested groups retain explicit positioning with collision checks. Existing root groups are not
rearranged. Agents must inspect target/parent canvases before other edits, including connection
routes and warnings; layout validity alone does not certify route geometry.

Restart MCP processes after installing the change so clients receive the updated tool metadata and
instructions. Container deployments require a rebuilt image and rollout.
