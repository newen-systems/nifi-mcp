---
title: Contextual top-down canvas layout
type: reference
status: accepted
tags: [nifi, mcp, layout]
updated: 2026-10-21
---

# Contextual top-down canvas layout

Within a flow cluster, vertical position expresses progression and horizontal
position expresses parallel work. Unrelated child-group clusters occupy a navigation grid only on parent group-collection canvases. This
applies to every canvas, including root, and to mixed chains of groups,
processors, ports, funnels and remote endpoints. Every forward destination clears
the source card plus the queue label and arrowhead gap. Every fork child starts
below its parent. Joins follow every prerequisite and return toward the fork axis.
Input ports occupy their owning canvas's top boundary, output ports its bottom.
A child's output port is a source on its parent's canvas, not a global output.

## One ordering model

`ordering.py` builds the precedence graph used by placement and saved-position
validation. `hierarchy.py` expands local group endpoints through their actual port
paths. An apparent group cycle need not be a cycle through the ports: a group can
send early in a workflow and receive its result later through an unrelated input.
That later visit uses a return lane, while the group's first stage stays upstream.
Paths that leave and re-enter a canvas order its otherwise disconnected chains.
Unreadable child canvases and opaque remote internals remain unknown.

Evidence priority is actual directed connections and port paths, explicit stages
and dependencies, then declared domain roles. Names resolve unique references and
never establish semantics by themselves. Known workflow dependencies determine order
within each cluster. Alphabetical lists and grids can organise unrelated work, but
neither establishes a dependency. Neutral placement uses component IDs for stable
tie-breaking and reports unresolved order. Explicit
independent areas suppress inferred cross-area dependencies; an actual cross-area
connection is reported as a conflict, not hidden.

Only identified feedback edges are exempt from forward ordering. Other edges in
the same cyclic region remain constrained. Cycles without an unambiguous entry
use a deterministic break and report that uncertainty. No layout operation creates,
deletes or rewires executable connections.

## Clusters and monitor layout

Group components related by actual connections or documented logical handoffs into
one flow cluster before packing the canvas. Keep every cluster internally top-down,
including mixed processor/group chains. A logical handoff can join a cluster even
when there is no visible NiFi connection. Preserve explicitly isolated areas as
separate blocks, subject to reporting any conflict with actual dependencies.

The 16:9 screen-fit preference applies only to parent navigation collections of
child process groups, like a menu of app icons. Pack unrelated child groups and
related child-group clusters into a stable grid; each cluster stays top-down.
Boundary ports may accompany these groups, such as `diode-test-source -> to-diode`.
Every packed component must contain a child group, and the canvas may contain no
processors, funnels or remote endpoints. A single connected group workflow also
keeps its top-down layout without packing.

Ordinary processor and mixed-component flow canvases have **no aspect-ratio target**,
even if some stages are disconnected or explicitly independent. They use the full
canvas in dependency-driven top-down order, with horizontal space for branches.
Opening a child group does not inherit its parent's navigation-grid objective.

For eligible navigation canvases, maximize display scale on a 16:9 screen using
`max(width / (16 / 9), height)`, including boundary-port height. Never compress
clearance or alter a related cluster's internal ordering. Stable component IDs
break geometric ties; navigation grid rows imply no executable dependencies.

## Declaring logical handoffs

`nifi_layout_process_group` accepts optional `layout_intent`, also accepted at the
flow spec's top level. It persists as a single `nifi-layout: {...}` line in the owning
group's comments, preserving surrounding prose. Automatic repair and validation read
the full comments, so intent survives server restarts and compact-output clipping.
Use IDs or unique component names on that canvas. Build-time names are resolved to
created IDs. This metadata is a layout constraint, not a NiFi connection.

```json
{
  "stages": [["Collect"], ["Ingest"], ["Parse", "Enrich"], ["Deliver"]],
  "dependencies": [{"source": "Parse", "target": "Enrich"}],
  "areas": [{"id": "independent-lab", "members": ["LabSource", "LabSink"], "independent": true}]
}
```

Every earlier stage precedes every later stage. Items within a stage can run in
parallel unless a dependency orders them. `feedback` uses the same source/target
shape but must identify a real return edge. `domain` plus `roles` offers explicit
context for documented roles, for example `diode-low-to-high` with roles `low`,
`transfer`, `high`, or `log-delivery` with `collection`, `ingestion`, `parsing`,
`enrichment`, `delivery`. Bare names such as "high" do not imply direction.
Unknown references, ambiguous names, conflicting dependencies and invalid domain
roles are reported. Stronger evidence retains precedence over weaker hints.

## Geometry and verification

Real card dimensions determine spacing: processors 352x128, groups 384x176,
ports 240x48 and funnels 48x48. The 240x79 queue label plus margins gives a 112px
minimum vertical gap, snapped to NiFi's 8px grid. Chains share a centred axis.
Parallel branches reserve their whole subtree, including self-loop labels.
Remote groups participate in placement; annotation labels remain fixed obstacles.

The router separates duplicate connections, self-loops and return lanes. Geometry
checks distinguish ordinary line-line crossings from hard collisions. Card/label
collisions, shared line stretches and lines through labels remain errors. Proper
line crossings are reported as `connection_crossing` advisories and minimized after
hard collisions are cleared. Some topologies require crossings: retain a structured,
readable arrangement rather than distorting the flow to promise zero crossings.
Connections may meet at their common NiFi endpoint; intersections elsewhere are
still reported even when the connections share a destination.

Plan fan-in routes and queue labels together. Preserve the branches' left-to-right
order as they approach the join, placing labels in clear space beneath their source
processors where practical. Reserve enough horizontal and vertical space for all
labels before choosing bends. An arbitrary left detour that crosses another incoming
branch is an avoidable crossing to remove through coordinated routing. Expand
clearance where useful, and report remaining crossings when a clearer arrangement
cannot be found. An advisory records the intersection; it does not prove that the
crossing is mathematically unavoidable.

If the initial tree has unresolved geometry,
a bounded search widens parallel columns while preserving progression and centred
chains. Remaining hard collisions are errors. Saved-state results separate
`layout_advisories` from error issues; a crossing alone does not make validation
`ok` false. No other geometry error is suppressed by this distinction.
Saved coordinates, bends and labelIndex are checked after writes. Repeating layout
on unchanged topology and intent produces the same positions and routes.

## Superseded alternatives

Perpendicular children on a fork's own row conceal downstream progression. Separate
processor and group placement breaks mixed chains. Applying alphabetical order or
preserving an existing grid across known dependencies can reverse the workflow;
using a grid for unrelated blocks is supported. Exempting all edges inside a cyclic
region conceals reversed forward edges. These shortcuts cannot replace dependency
analysis. A free-slot lattice remains provisional collision avoidance during creation;
automatic repair applies precedence within clusters and neutral packing between them.
