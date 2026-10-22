"""Bounded, read-only lifecycle checks; NiFi remains authoritative under its write lock.

Never cache across operations, replace caller revisions, stop components, or drop queues.
See docs/mutation-preflight.md for scope, source references and residual races.
"""

from __future__ import annotations

import re
from typing import TYPE_CHECKING, Any

from nifi_mcp.errors import NiFiConflictError, Outcome

if TYPE_CHECKING:
    from nifi_mcp.client import NiFiClient

_ENDPOINTS = {"PROCESSOR": "processors", "INPUT_PORT": "input-ports", "OUTPUT_PORT": "output-ports"}
_CONNECTION_FIELDS = {
    "destination",
    "source",
    "selectedRelationships",
    "name",
    "position",
    "prioritizers",
    "backPressureDataSizeThreshold",
    "backPressureObjectThreshold",
    "flowFileExpiration",
}
_MAX_ENDPOINTS = 128


_SMALLER_CHILD_AREAS = "Delete smaller child areas first."


def refuse(path: str, reason: str, hint: str) -> None:
    raise NiFiConflictError(
        f"Mutation preflight refused: {reason}",
        path=path,
        outcome=Outcome.NOT_APPLIED,
        hint=f"No mutation was sent. {hint} Read the current revision before submitting again.",
    )


def snapshot(entity: dict[str, Any]) -> dict[str, Any]:
    return (entity.get("status") or {}).get("aggregateSnapshot") or {}


def subject(entity: dict[str, Any], noun: str = "component") -> str:
    """Only UUIDs are safe to quote as identifiers; names/properties are never advice."""
    identity = (entity.get("component") or {}).get("id")
    if isinstance(identity, str) and re.fullmatch(
        r"[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}", identity
    ):
        return f"{noun} {identity}"
    return noun


def check_stopped(path: str, entity: dict[str, Any]) -> None:
    component = entity.get("component") or {}
    state = component.get("state") or snapshot(entity).get("runStatus")
    threads = snapshot(entity).get("activeThreadCount", component.get("activeThreadCount", 0))
    if state in {"RUNNING", "RUN_ONCE", "STARTING", "STOPPING"} or threads:
        refuse(
            path,
            f"{subject(entity)} is running or still has active threads",
            f"Stop {subject(entity)}; poll its state and active thread count until quiescent.",
        )
    if state not in {"STOPPED", "DISABLED", "INVALID"}:
        refuse(
            path,
            f"{subject(entity)} lifecycle state is unavailable",
            "Read the affected component with an identity permitted to inspect its state.",
        )


class Preflight:
    def __init__(self, client: NiFiClient, path: str) -> None:
        self.client = client
        self.path = path
        self.seen: set[tuple[str, str]] = set()
        self.flow: dict[str, Any] | None = None

    async def endpoint(self, endpoint: dict[str, Any]) -> None:
        kind, identity = endpoint.get("type"), endpoint.get("id")
        key = (str(kind), str(identity))
        if key in self.seen:
            return
        self.seen.add(key)
        if len(self.seen) > _MAX_ENDPOINTS:
            refuse(
                self.path,
                "dependency inspection exceeded its bounded endpoint limit",
                "Quiesce and inspect the flow in smaller areas before changing this connection.",
            )
        if kind in _ENDPOINTS and identity:
            entity = await self.client.get_json(f"/{_ENDPOINTS[kind]}/{identity}")
            check_stopped(self.path, entity)
        elif kind in {"REMOTE_INPUT_PORT", "REMOTE_OUTPUT_PORT"}:
            # ConnectableDTO.running includes remote port transmission state.
            if endpoint.get("running") is not False:
                refuse(
                    self.path,
                    f"{subject({'component': endpoint}, 'remote endpoint')} is transmitting "
                    "or its state is unavailable",
                    "Stop transmission on the remote port and read the connection endpoints again.",
                )
        else:
            refuse(
                self.path,
                "endpoint lifecycle cannot be established",
                "Inspect the connection endpoint types and permissions before changing it.",
            )

    async def _funnel_canvas(self, endpoint) -> None:
        group = endpoint.get("groupId")
        if not group:
            refuse(self.path, "funnel owning canvas is unavailable", "Read the connection's source group.")
        entity = await self.client.get_flow(group)
        self.flow = (entity.get("processGroupFlow") or {}).get("flow")
        if self.flow is None:
            refuse(
                self.path, "funnel dependencies are unavailable", "Read the owning canvas with full permissions."
            )

    async def source(self, endpoint: dict[str, Any]) -> None:
        if endpoint.get("type") != "FUNNEL":
            await self.endpoint(endpoint)
            return
        key = ("FUNNEL", str(endpoint.get("id")))
        if key in self.seen:
            return
        self.seen.add(key)
        if len(self.seen) > _MAX_ENDPOINTS:
            refuse(
                self.path,
                "funnel dependency inspection exceeded its bounded limit",
                "Inspect and quiesce upstream producers in smaller areas.",
            )
        if self.flow is None:
            await self._funnel_canvas(endpoint)
        for item in self.flow.get("connections", []):
            conn = item.get("component")
            if conn is None:
                refuse(self.path, "a funnel dependency is not readable", "Obtain read access to the owning canvas.")
            if (conn.get("destination") or {}).get("id") == endpoint.get("id"):
                await self.source(conn.get("source") or {})

    def connection_change(self, entity: dict[str, Any], proposed: dict[str, Any]) -> None:
        current = self.readable(entity)
        source = proposed.get("source")
        if source and source.get("id") != (current.get("source") or {}).get("id"):
            refuse(
                self.path,
                "a connection source cannot be changed in place",
                "Plan a replacement connection and handle queued data before removing the old connection.",
            )
        if proposed.get("selectedRelationships") == []:
            refuse(
                self.path,
                "all relationships cannot be removed from a connection",
                "Select at least one supported relationship, or drain and delete the connection.",
            )

    async def connection(self, entity: dict[str, Any], delete: bool) -> None:
        component = entity.get("component") or {}
        destination = component.get("destination") or {}
        if delete:
            queued = snapshot(entity).get("flowFilesQueued")
            if queued != 0:
                refuse(
                    self.path,
                    f"{subject(entity, 'queue')} is nonempty or its size is unavailable",
                    "Stop upstream production, let the destination drain the queue, then stop the destination. "
                    "Confirm zero queued FlowFiles. For a user-authorized disposable test reset, use nifi_empty_queue "
                    "on this connection without repeated confirmation; otherwise drain it "
                    "or obtain scoped authorization.",
                )
            await self.source(component.get("source") or {})
        exemptions = {"FUNNEL"} if delete else {"FUNNEL", "INPUT_PORT"}
        if destination.get("type") not in exemptions:
            await self.endpoint(destination)

    async def canvas(self, group: str) -> dict[str, Any]:
        entity = await self.client.get_flow(group)
        canvas = (entity.get("processGroupFlow") or {}).get("flow")
        if canvas is None:
            refuse(self.path, "canvas dependencies are unavailable", "Obtain read access to the owning canvas.")
        return canvas

    def readable(self, entity: dict[str, Any]) -> dict[str, Any]:
        component = entity.get("component")
        if component is None:
            refuse(self.path, "a dependency is not readable", "Obtain read access to the affected components.")
        return component

    async def detached(self, entity: dict[str, Any], *, port: bool = False) -> None:
        component = self.readable(entity)
        group = component.get("parentGroupId")
        if not group:
            refuse(self.path, "owning canvas is unavailable", "Read the component and its owning canvas.")
        canvases = [await self.canvas(group)]
        if port:
            owner = await self.client.get_process_group(group)
            parent = self.readable(owner).get("parentGroupId")
            if parent:
                canvases.append(await self.canvas(parent))
        connections = [item for canvas in canvases for item in canvas.get("connections", [])]
        if len(connections) > _MAX_ENDPOINTS:
            refuse(self.path, "connection inspection exceeded its bounded limit", "Inspect a smaller flow area.")
        for item in connections:
            conn = self.readable(item)
            source, destination = conn.get("source") or {}, conn.get("destination") or {}
            identity = component.get("id")
            if destination.get("id") == identity and source.get("id") != identity:
                refuse(
                    self.path,
                    f"{subject(entity)} still has incoming connections",
                    "Drain and remove incoming connections before deletion; stop both endpoints first.",
                )
            if source.get("id") == identity:
                current = await self.client.get_connection(conn["id"])
                await self.connection(current, True)

    async def service(self, entity: dict[str, Any], *, disabling: bool = False) -> None:
        component = self.readable(entity)
        state = component.get("state")
        if not disabling:
            if state != "DISABLED":
                refuse(
                    self.path,
                    f"{subject(entity, 'controller service')} is enabled, transitioning, or its state is unavailable",
                    "Stop consumers, disable dependent services, then disable this service and poll for DISABLED.",
                )
            return
        if state == "DISABLED":
            return
        if state not in {"ENABLED", "ENABLING"}:
            refuse(
                self.path,
                "controller service is transitioning or its state is unavailable",
                "Read the service state again after its transition completes.",
            )
        refs = await self.client.get_json(f"/controller-services/{component['id']}/references")
        consumers = refs.get("controllerServiceReferencingComponents")
        if consumers is None:
            refuse(self.path, "service consumers are unavailable", "Obtain read access to service references.")
        if len(consumers) > _MAX_ENDPOINTS:
            refuse(
                self.path,
                "service consumer inspection exceeded its bounded limit",
                "Inspect consumers in smaller sets.",
            )
        for item in consumers:
            consumer = self.readable(item)
            state = consumer.get("state")
            if state not in {"STOPPED", "DISABLED", "INVALID"} or consumer.get("activeThreadCount", 0):
                refuse(
                    self.path,
                    f"{subject(entity, 'controller service')} has active or unreadable consumer {subject(item)}",
                    "Stop referencing processors and reporting tasks; disable dependent services first. "
                    "Wait for their active threads and transitions to finish.",
                )

    async def group(self, entity: dict[str, Any]) -> None:
        component = self.readable(entity)
        if component.get("parentGroupId") is None:
            refuse(self.path, "root process group cannot be deleted", "Delete a child group instead.")
        parent = await self.canvas(component["parentGroupId"])
        for item in parent.get("connections", []):
            conn = self.readable(item)
            if any((conn.get(side) or {}).get("groupId") == component["id"] for side in ("source", "destination")):
                refuse(
                    self.path,
                    f"process group still has external connections ({subject(item, 'connection')})",
                    "Drain and remove connections crossing the group boundary before deleting it.",
                )
        pending, visited, count = [component["id"]], set(), 0
        while pending:
            group = pending.pop()
            if group in visited:
                continue
            visited.add(group)
            if len(visited) > 32:
                refuse(
                    self.path, "group inspection exceeded its bounded depth/size", _SMALLER_CHILD_AREAS
                )
            canvas = await self.canvas(group)
            count += sum(len(items) for items in canvas.values() if isinstance(items, list))
            if count > 256:
                refuse(
                    self.path,
                    "group inspection exceeded its bounded component limit",
                    _SMALLER_CHILD_AREAS,
                )
            await self.group_contents(group, canvas)
            pending.extend(self.readable(item)["id"] for item in canvas.get("processGroups", []))

    async def group_contents(self, group: str, canvas: dict[str, Any]) -> None:
        for kind in ("processors", "inputPorts", "outputPorts"):
            for item in canvas.get(kind, []):
                check_stopped(self.path, item)
        for item in canvas.get("connections", []):
            if snapshot(item).get("flowFilesQueued") != 0:
                refuse(
                    self.path,
                    f"{subject(item, 'queue')} in the process group has data or unavailable state",
                    "Stop production, drain queues, then stop consumers; confirm every queue is empty.",
                )
        for item in canvas.get("remoteProcessGroups", []):
            if self.readable(item).get("transmitting") is not False:
                refuse(self.path, "remote transmission is active or unavailable", "Stop remote transmission first.")
        await self.group_services(group)

    async def group_services(self, group: str) -> None:
        services = await self.client.list_controller_services(group)
        if "controllerServices" not in services:
            refuse(self.path, "group services are unavailable", "Obtain read access to group services.")
        if len(services["controllerServices"]) > _MAX_ENDPOINTS:
            refuse(self.path, "group service inspection exceeded its limit", _SMALLER_CHILD_AREAS)
        for item in services["controllerServices"]:
            if self.readable(item).get("parentGroupId") == group:
                await self.service(item)


def operation(method: str, path: str, component: dict[str, Any]) -> str | None:
    parts = path.strip("/").split("/")
    if method == "PUT" and len(parts) == 3 and parts[0] == "controller-services" and parts[2] == "run-status":
        return "service_state"
    if len(parts) != 2 or method not in {"PUT", "DELETE"}:
        return None
    kind, delete = parts[0], method == "DELETE"
    if kind == "processors" and (delete or "name" in component or bool(component.get("config"))):
        return "processor"
    if kind == "connections" and (delete or bool(_CONNECTION_FIELDS & component.keys())):
        return "connection"
    if kind in {"input-ports", "output-ports", "funnels", "process-groups"} and delete:
        return kind
    if kind == "controller-services" and (delete or bool(component.keys() - {"id", "position"})):
        return "service"
    return None


def revision(path: str, entity: dict[str, Any], expected: Any) -> None:
    actual = (entity.get("revision") or {}).get("version")
    if actual is not None and expected is not None and actual != expected:
        refuse(path, "submitted revision is stale", "Read the component and reconsider the intended change.")


async def _service_state(checker, entity, body) -> None:
    if body.get("state") == "DISABLED":
        await checker.service(entity, disabling=True)
    elif (entity.get("component") or {}).get("state") == "DISABLING":
        refuse(checker.path, "controller service is still disabling", "Wait for DISABLED before enabling it.")


async def inspect(checker: Preflight, op: str, entity: dict[str, Any], delete: bool, body: Any) -> None:
    if op == "connection":
        if not delete:
            checker.connection_change(entity, (body or {}).get("component") or {})
        await checker.connection(entity, delete)
    elif op == "service":
        await checker.service(entity)
    elif op == "service_state":
        await _service_state(checker, entity, body)
    elif op == "process-groups":
        await checker.group(entity)
    else:
        if op != "funnels":
            check_stopped(checker.path, entity)
        if delete:
            await checker.detached(entity, port=op in {"input-ports", "output-ports"})


async def preflight(client: NiFiClient, method: str, path: str, body: Any, params: Any) -> None:
    """Inspect supported lifecycle-sensitive operations, including raw JSON reroutes."""
    component = (body or {}).get("component") or {}
    op = operation(method, path, component)
    if not op:
        return
    read_path = path.removesuffix("/run-status")
    entity = await client.get_json(read_path)
    delete = method == "DELETE"
    expected = (params or {}).get("version") if delete else ((body or {}).get("revision") or {}).get("version")
    revision(path, entity, expected)
    checker = Preflight(client, path)
    await inspect(checker, op, entity, delete, body)
    # A dependency walk can take time. Detect intervening edits without refreshing the
    # caller's version or replaying a mutation. NiFi still arbitrates races after this read.
    latest = await client.get_json(read_path)
    revision(path, latest, expected)
    if op in {"processor", "input-ports", "output-ports"}:
        check_stopped(path, latest)
    elif op == "service":
        await checker.service(latest)
    elif op == "connection" and delete and snapshot(latest).get("flowFilesQueued") != 0:
        refuse(path, "queue changed during preflight", "Quiesce upstream producers and inspect the queue again.")
