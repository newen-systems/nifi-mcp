# Mutation lifecycle preflight

`NiFiClient.json` runs read-only preflight before selected lifecycle-sensitive
PUT and DELETE requests. Existing client signatures are unchanged. Raw JSON
calls through this method receive the same checks, including destination
reroutes. Attempts to change a connection source in place or remove every
relationship are refused before writing. Geometry-only edits retain their existing
behavior.

| Operation | Prerequisites inspected |
| --- | --- |
| Processor configuration or name update | Stopped state and reported active threads |
| Processor deletion | Quiescent processor, no incoming connections from another component; outgoing connections independently deletable |
| Connection deletion | Empty queue, stopped source and destination; upstream producers behind funnels checked recursively |
| Connection name, queue settings, relationships, or destination update | Current destination stopped, except funnels and input ports |
| Port deletion | Quiescent port and connection dependencies on both its owning and parent canvases |
| Funnel deletion | Incoming connections removed; outgoing connections independently deletable; funnels themselves cannot be stopped |
| Process-group deletion | No external port connections; descendants quiescent; queues empty; owned services disabled; remote transmission stopped |
| Controller-service configuration or deletion | Service disabled |
| Controller-service disable | Referencing processors/reporting tasks stopped and dependent services inactive |
| Controller-service enable | Refuse while still disabling |

Deleting a processor, port or funnel can delete its empty outgoing connections
as part of NiFi's normal operation. Preflight checks those connections without
issuing separate deletions. A disabled service can be deleted even when inactive
components reference it; NiFi allows this, and the remaining references may then
be invalid. No automatic stop, restart, queue drop or dependency deletion is added.

The usual connection-removal sequence is: stop upstream production, allow the
destination to drain the queue, stop the destination, wait for active tasks to
finish, verify zero queued FlowFiles, then delete. For a user-authorized disposable test reset, use `nifi_empty_queue` on the
selected connection without asking again for each queue. Existing authorization
applies to those selected test queues; it does not authorize unrelated queues.

## Refusals, races and bounds

A local refusal uses `NiFiConflictError`, `outcome=not_applied`, a target path,
and an actionable hint through the existing Renderer. Blocked components and
service consumers are identified by validated NiFi UUIDs, which the Renderer
already preserves; arbitrary component identifiers and names are never quoted.
No synthetic HTTP status
is invented. A dependency GET failure becomes `NiFiError` with `not_applied`:
a read timeout does not mean an unsent mutation might have landed. The remote
error body is omitted. Property values and request bodies are not included in
preflight explanations. Earlier applied requests remain in the mutation ledger.

Reads are fresh for each mutation. After inspecting dependencies, preflight reads
the target again and compares its revision with the caller's revision; it never
silently replaces that revision. Processor/port state, service edit state, and
connection-delete queue size are checked again at this point. A version mismatch
requires the caller to reconsider the change. No mutation retries or waits were
added; existing transport uncertainty and authenticated 401 replay rules remain.

HTTP 409 advice identifies familiar revision, queue and lifecycle causes without
echoing remote text into the hint. Unclassified conflicts require diagnosis. Both
server rule-5 prompts use the same guidance: no blind retries, drops outside the
authorized scope, or stopping unrelated flows.

Inspection is deliberately bounded: 128 unique connection endpoints/funnels,
128 connections for component detachment, 32 canvases and 256 canvas entities for
group deletion, and 128 service references/services per inspected list. Cycles
have visited sets. Exceeding a bound refuses the write and suggests smaller areas.
Inspection does not cache between operations. Missing required lifecycle state,
queue size, or unreadable dependency components causes refusal.

Preflight is advisory relative to NiFi's authoritative write lock. Component
states, queues, descendants, cluster status and permissions can change after a
read. Status snapshots can lag; absent active-thread counts cannot prove that
there are no threads. NiFi still handles concurrent changes, extension validation,
parameter-context restrictions, versioned-flow restrictions and cluster consensus.
The helper does not implement every NiFi verifier. Direct calls to the lower-level
`request` transport bypass JSON preflight. Creation and most run-state operations
remain governed by NiFi. No live-cluster acceptance was performed for this change.

## Reset one disposable test queue

After authorization to reset disposable test data, call `nifi_empty_queue` with
these arguments. Replace the fixture UUID with the selected connection ID and
use a configured cluster name from `nifi_list_clusters`:

```json
{
  "params": {
    "cluster": "nifi-dev",
    "connection_id": "00000000-0000-0000-0000-000000000111",
    "timeout_seconds": 30
  }
}
```

Only that queue is dropped. No fresh confirmation is needed for each selected
queue within the authorized reset. Cluster selection, `NIFI_READONLY`, and NiFi
access policies remain enforced. A 403 is an actual permission refusal and is
not bypassed.

`timeout_seconds` defaults to 30 and accepts `0 < timeout_seconds <= 120`.
The listing tool's arguments are unchanged. Successful completion returns
`connection_id`, the existing drop `result`, `remaining_count`, and `empty`.
The last two fields come from a separate connection-status read. If that read
fails or lacks a count, both are null, a warning explains the uncertainty, and
the completed drop remains `outcome: applied`.

A producer can refill a queue immediately after a drop. A positive remaining
count yields `empty: false` and a warning to stop the producer and wait for its
active threads before another authorized reset. The tool stops nothing and
never submits a second drop. The empty result is a status snapshot, not a promise
that future data cannot arrive. The tool advertises `idempotentHint: false`:
a repeated drop can discard newly arrived data. A timed-out drop remains unknown
and is not cancelled or retried automatically.

## Source audit

The rules were checked against Apache NiFi `rel/nifi-2.11.0`, not inferred from
component names. In particular, the connection DAO's implementation checks the
current destination for name and queue-setting changes as well as reroutes,
despite a narrower comment in that source. It exempts funnels and input ports;
connection deletion only exempts destination funnels. Geometry fields bypass the
DAO's lifecycle-update branch.

- [StandardConnection.verifyCanDelete](https://github.com/apache/nifi/blob/rel/nifi-2.11.0/nifi-framework-bundle/nifi-framework/nifi-framework-components/src/main/java/org/apache/nifi/connectable/StandardConnection.java)
- [StandardConnectionDAO.verifyUpdate / updateConnection](https://github.com/apache/nifi/blob/rel/nifi-2.11.0/nifi-framework-bundle/nifi-framework/nifi-web/nifi-web-api/src/main/java/org/apache/nifi/web/dao/impl/StandardConnectionDAO.java)
- [StandardProcessorNode.verifyCanDelete / verifyCanUpdate](https://github.com/apache/nifi/blob/rel/nifi-2.11.0/nifi-framework-bundle/nifi-framework/nifi-framework-components/src/main/java/org/apache/nifi/controller/StandardProcessorNode.java)
- [AbstractPort.verifyCanDelete](https://github.com/apache/nifi/blob/rel/nifi-2.11.0/nifi-framework-bundle/nifi-framework/nifi-framework-core-api/src/main/java/org/apache/nifi/controller/AbstractPort.java)
- [StandardFunnel.verifyCanDelete](https://github.com/apache/nifi/blob/rel/nifi-2.11.0/nifi-framework-bundle/nifi-framework/nifi-framework-core-api/src/main/java/org/apache/nifi/controller/StandardFunnel.java)
- [StandardProcessGroup.verifyCanDelete](https://github.com/apache/nifi/blob/rel/nifi-2.11.0/nifi-framework-bundle/nifi-framework/nifi-framework-components/src/main/java/org/apache/nifi/groups/StandardProcessGroup.java)
- [StandardControllerServiceNode lifecycle verification](https://github.com/apache/nifi/blob/rel/nifi-2.11.0/nifi-framework-bundle/nifi-framework/nifi-framework-components/src/main/java/org/apache/nifi/controller/service/StandardControllerServiceNode.java)

Mutation fixtures must expose these read prerequisites so transport-outcome tests
reach their intended write. `tests/test_mutation_preflight.py` separately verifies
pre-write refusals, traversal bounds, revision races and unchanged write outcomes.
