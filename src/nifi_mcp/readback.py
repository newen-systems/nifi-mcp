"""What a mutation with an unknown outcome did, read back from NiFi before its hint is written.

Each read is the one the hint names, so the model sees what the client saw and can run it again.
A finding holds ids, counts, revisions and component states only: a name the request sent is
compared, never repeated, because it may hold a value the model should not see echoed.
"""

from __future__ import annotations

import re
from collections import Counter
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from nifi_mcp.errors import NiFiError

Get = Callable[[str], Awaitable[Any]]


@dataclass(frozen=True)
class ReadBack:
    """The read that shows the state (as a tool call the model can make) and what it showed."""

    read: str
    finding: str


# POST /process-groups/{pg}/<kind>: the flow key that lists that kind, and its name.
_FLOW_KEYS = {
    "processors": ("processors", "processor"),
    "process-groups": ("processGroups", "process group"),
    "input-ports": ("inputPorts", "input port"),
    "output-ports": ("outputPorts", "output port"),
    "connections": ("connections", "connection"),
}
_CREATE = re.compile(
    r"^/process-groups/([^/]+)/(processors|process-groups|input-ports|output-ports|connections)(/import)?$"
)
_CREATE_SERVICE = re.compile(r"^/process-groups/([^/]+)/controller-services$")
_CREATE_CONTEXT = re.compile(r"^/parameter-contexts$")
_REPLACE = re.compile(r"^/process-groups/([^/]+)/replace-requests$")
_ENTITY = re.compile(
    r"^/(processors|controller-services|process-groups|parameter-contexts)/([^/]+)(?:/run-status|/update-requests)?$"
)
_ENTITY_READ = {
    "processors": ("nifi_get_processor on {0}", "processor"),
    "controller-services": ("nifi_get_controller_service on {0}", "controller service"),
    "process-groups": ("nifi_get_flow on {0}", "process group"),
    "parameter-contexts": ("nifi_get_parameter_context on {0}", "parameter context"),
}
# Components no get tool returns: nifi_get_flow on the group that holds them shows their state.
_HELD = re.compile(r"^/(connections|input-ports|output-ports)/([^/]+)$")
_QUEUE = re.compile(r"^/flowfile-queues/([^/]+)/(?:drop|listing)-requests$")
_SCHEDULE = re.compile(r"^/flow/process-groups/([^/]+)$")
_ENABLE_ALL = re.compile(r"^/flow/process-groups/([^/]+)/controller-services$")


def _flow_items(flow: dict[str, Any], key: str) -> list[dict[str, Any]]:
    return ((flow.get("processGroupFlow") or {}).get("flow") or {}).get(key) or []


def _component(entity: dict[str, Any]) -> dict[str, Any]:
    return entity.get("component") or {}


def _id(entity: dict[str, Any]) -> str:
    return str(entity.get("id") or _component(entity).get("id"))


def _revision(entity: dict[str, Any]) -> int:
    return int((entity.get("revision") or {}).get("version") or 0)


def _listed(kind: str, found: list[str], how: str) -> str:
    if not found:
        return f"it lists no {kind} {how}, so the create has not landed (or not yet)"
    return (
        f"it lists {len(found)} {kind}(s) {how}: {', '.join(found)}. One that was there before this call "
        "would be listed too"
    )


def _sent_version(body: dict[str, Any], params: dict[str, Any]) -> int | None:
    revision = body.get("revision") or body.get("processGroupRevision") or {}
    version = revision.get("version", params.get("version"))
    return None if version is None else int(version)


def _revision_finding(label: str, entity: dict[str, Any], method: str, sent: int | None) -> str:
    now = _revision(entity)
    if method == "DELETE":
        return f"{label} still exists at revision {now}, so the delete has not taken effect (or not yet)"
    if sent is None:
        return f"{label} is at revision {now}"
    if now > sent:
        return f"{label} is at revision {now}, past revision {sent} this request was sent with, so a change landed"
    return (
        f"{label} is still at revision {now}, the one this request was sent with, so no change has landed "
        "(or not yet)"
    )


def _gone(label: str, method: str) -> str:
    return f"{label} does not exist" + (", so the delete took effect" if method == "DELETE" else "")


def _states(items: list[dict[str, Any]]) -> str:
    counts = Counter(str(_component(item).get("state") or "UNKNOWN") for item in items)
    return ", ".join(f"{state} {count}" for state, count in sorted(counts.items())) or "none"


async def _created(get: Get, match: re.Match[str], body: dict[str, Any]) -> ReadBack:
    group, segment, imported = match[1], match[2], match[3]
    key, kind = _FLOW_KEYS[segment]
    read = f"nifi_get_flow on {group}"
    items = _flow_items(await get(f"/flow/process-groups/{group}"), key)
    component = _component(body)
    if segment == "connections":
        src, dst = (component.get("source") or {}).get("id"), (component.get("destination") or {}).get("id")
        found = [
            _id(item)
            for item in items
            if (_component(item).get("source") or {}).get("id") == src
            and (_component(item).get("destination") or {}).get("id") == dst
        ]
        return ReadBack(read, _listed(kind, found, f"from {src} to {dst}"))
    name = body.get("groupName") if imported else component.get("name")
    found = [_id(item) for item in items if _component(item).get("name") == name]
    return ReadBack(read, _listed(kind, found, "with the name this request sent"))


async def _created_service(get: Get, match: re.Match[str], body: dict[str, Any]) -> ReadBack:
    group = match[1]
    listing = await get(f"/flow/process-groups/{group}/controller-services")
    name = _component(body).get("name")
    found = [
        _id(item)
        for item in listing.get("controllerServices") or []
        if _component(item).get("name") == name and _component(item).get("parentGroupId") == group
    ]
    return ReadBack(
        f"nifi_list_controller_services on {group}",
        _listed("controller service", found, "with the name this request sent"),
    )


async def _created_context(get: Get, body: dict[str, Any]) -> ReadBack:
    listing = await get("/flow/parameter-contexts")
    name = _component(body).get("name")
    found = [_id(item) for item in listing.get("parameterContexts") or [] if _component(item).get("name") == name]
    return ReadBack(
        "nifi_list_parameter_contexts", _listed("parameter context", found, "with the name this request sent")
    )


async def _entity(get: Get, method: str, segment: str, cid: str, sent: int | None, body: dict[str, Any]) -> ReadBack:
    template, kind = _ENTITY_READ[segment]
    read, label = template.format(cid), f"{kind} {cid}"
    try:
        entity = await (_group_entity(get, cid) if segment == "process-groups" else get(f"/{segment}/{cid}"))
    except NiFiError as exc:
        if exc.status_code == 404:
            return ReadBack(read, _gone(label, method))
        raise
    finding = _revision_finding(label, entity, method, sent)
    state = _component(entity).get("state")
    if state and method != "DELETE":
        finding += f"; its state is {state}"
    if "parameterContext" in _component(body):
        bound = (_component(entity).get("parameterContext") or {}).get("id")
        finding += f"; its parameter context is {bound or 'none'}"
    return ReadBack(read, finding)


async def _group_entity(get: Get, group: str) -> dict[str, Any]:
    """A process group as nifi_get_flow reads it: ProcessGroupFlowEntity carries the group's revision,
    and ProcessGroupFlowDTO its parameter context."""
    flow = await get(f"/flow/process-groups/{group}")
    context = (flow.get("processGroupFlow") or {}).get("parameterContext")
    return {"revision": flow.get("revision"), "component": {"parameterContext": context}}


async def _held(
    get: Get, method: str, segment: str, cid: str, sent: int | None, group: str | None
) -> ReadBack:
    key, kind = _FLOW_KEYS[segment]
    label = f"{kind} {cid}"
    if group is None:
        try:
            group = str(_component(await get(f"/{segment}/{cid}")).get("parentGroupId") or "") or None
        except NiFiError as exc:
            if exc.status_code == 404:
                return ReadBack(f"nifi_get_flow on the process group that held {label}", _gone(label, method))
            raise
    if group is None:
        return ReadBack(f"nifi_get_flow on the process group that holds {label}", f"{label} names no parent group")
    read = f"nifi_get_flow on {group}"
    items = _flow_items(await get(f"/flow/process-groups/{group}"), key)
    item = next((item for item in items if _id(item) == cid), None)
    if item is None:
        took = ", so the delete took effect" if method == "DELETE" else ""
        return ReadBack(read, f"it does not list {label}{took}")
    return ReadBack(read, _revision_finding(label, item, method, sent))


async def _queue(get: Get, cid: str) -> ReadBack:
    group = str(_component(await get(f"/connections/{cid}")).get("parentGroupId") or "")
    items = _flow_items(await get(f"/flow/process-groups/{group}"), "connections")
    item = next((item for item in items if _id(item) == cid), {})
    queued = ((item.get("status") or {}).get("aggregateSnapshot") or {}).get("flowFilesQueued")
    count = "an unknown number of" if queued is None else str(int(queued))
    return ReadBack(f"nifi_get_flow on {group}", f"connection {cid} has {count} FlowFiles queued")


async def _schedule(get: Get, group: str) -> ReadBack:
    processors = _flow_items(await get(f"/flow/process-groups/{group}"), "processors")
    return ReadBack(f"nifi_get_flow on {group}", f"processor states in {group}: {_states(processors)}")


async def _enable_all(get: Get, group: str) -> ReadBack:
    listing = await get(f"/flow/process-groups/{group}/controller-services")
    own = [item for item in listing.get("controllerServices") or [] if _component(item).get("parentGroupId") == group]
    return ReadBack(
        f"nifi_list_controller_services on {group}", f"controller service states in {group}: {_states(own)}"
    )


def _plan(
    get: Get, method: str, path: str, body: dict[str, Any], params: dict[str, Any], group: str | None
) -> tuple[str, Awaitable[ReadBack]]:
    """The read for this request, as named when the read itself fails, and the coroutine that runs it."""
    sent = _sent_version(body, params)
    if match := _CREATE.match(path):
        return f"nifi_get_flow on {match[1]}", _created(get, match, body)
    if match := _CREATE_SERVICE.match(path):
        return f"nifi_list_controller_services on {match[1]}", _created_service(get, match, body)
    if _CREATE_CONTEXT.match(path):
        return "nifi_list_parameter_contexts", _created_context(get, body)
    if match := _REPLACE.match(path):
        return f"nifi_get_flow on {match[1]}", _entity(get, method, "process-groups", match[1], sent, body)
    if match := _ENTITY.match(path):
        return _ENTITY_READ[match[1]][0].format(match[2]), _entity(get, method, match[1], match[2], sent, body)
    if match := _HELD.match(path):
        read = f"nifi_get_flow on {group}" if group else "nifi_get_flow on the process group that holds it"
        return read, _held(get, method, match[1], match[2], sent, group)
    if match := _QUEUE.match(path):
        return "nifi_get_flow on the group that holds the connection", _queue(get, match[1])
    if match := _ENABLE_ALL.match(path):
        return f"nifi_list_controller_services on {match[1]}", _enable_all(get, match[1])
    if match := _SCHEDULE.match(path):
        return f"nifi_get_flow on {match[1]}", _schedule(get, match[1])
    raise LookupError(path)


async def read_back(
    get: Get,
    method: str,
    path: str,
    body: Any = None,
    params: dict[str, Any] | None = None,
    group: str | None = None,
) -> ReadBack:
    """Run the read that shows what `method path` did. Never raises: a read that fails is the finding."""
    try:
        read, pending = _plan(
            get, method.upper(), path, body if isinstance(body, dict) else {}, params or {}, group
        )
    except LookupError:
        return ReadBack(
            "nifi_get_flow on the process group that holds the component", "no read-back covers this request"
        )
    try:
        return await pending
    except (NiFiError, ValueError, TypeError, AttributeError) as exc:
        status = f"HTTP {exc.status_code}" if isinstance(exc, NiFiError) and exc.status_code else type(exc).__name__
        return ReadBack(read, f"the read-back failed as well ({status}), so the state is not verified")
