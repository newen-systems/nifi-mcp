"""LLM-sized views of NiFi entities. Full JSON is opt-in via verbose=true."""

from __future__ import annotations

from typing import Any

from nifi_mcp.redaction import redact

_EMPTY = (None, "", [], {})


def _clip(text: Any, limit: int = 160) -> str | None:
    if not text:
        return None
    value = str(text).strip()
    if not value:
        return None
    if len(value) <= limit:
        return value
    return value[: limit - 3] + "..."


def _compact(fields: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in fields.items() if value not in _EMPTY}


def _component(entity: dict[str, Any] | None) -> dict[str, Any]:
    if not entity:
        return {}
    return entity.get("component") or entity


def _revision(entity: dict[str, Any] | None) -> dict[str, Any]:
    if not entity:
        return {}
    return entity.get("revision") or {}


def compact_processor(entity: dict[str, Any]) -> dict[str, Any]:
    comp = _component(entity)
    status = entity.get("status") or {}
    snapshot = status.get("aggregateSnapshot") or status
    return _compact(
        {
            "id": entity.get("id") or comp.get("id"),
            "name": comp.get("name"),
            "type": (comp.get("type") or "").rsplit(".", 1)[-1],
            "full_type": comp.get("type"),
            "state": comp.get("state") or snapshot.get("runStatus"),
            "validation_status": comp.get("validationStatus"),
            "validation_errors": (comp.get("validationErrors") or [])[:8],
            "position": comp.get("position"),
            "revision": _revision(entity).get("version"),
            "queued": snapshot.get("queued") or snapshot.get("flowFilesQueued"),
            "bulletin_count": len(entity.get("bulletins") or []),
        }
    )


def _endpoint(end: dict[str, Any]) -> dict[str, Any]:
    """A connection end. group_id says which child group a port end sits in, for group layout."""
    return {"id": end.get("id"), "name": end.get("name"), "type": end.get("type"), "group_id": end.get("groupId")}


def compact_connection(entity: dict[str, Any]) -> dict[str, Any]:
    comp = _component(entity)
    status = entity.get("status") or {}
    snapshot = status.get("aggregateSnapshot") or status
    source = comp.get("source") or {}
    dest = comp.get("destination") or {}
    return _compact(
        {
            "id": entity.get("id") or comp.get("id"),
            "name": comp.get("name"),
            "source": _endpoint(source),
            "destination": _endpoint(dest),
            "relationships": comp.get("selectedRelationships") or [],
            "queued_count": snapshot.get("flowFilesQueued"),
            "queued_bytes": snapshot.get("bytesQueued"),
            "queued": snapshot.get("queued"),
            "revision": _revision(entity).get("version"),
            "bends": comp.get("bends") or [],
            "back_pressure_object_threshold": comp.get("backPressureObjectThreshold"),
            "back_pressure_data_size_threshold": comp.get("backPressureDataSizeThreshold"),
            "flow_file_expiration": comp.get("flowFileExpiration"),
        }
    )


def compact_process_group(entity: dict[str, Any]) -> dict[str, Any]:
    comp = _component(entity)
    return _compact(
        {
            "id": entity.get("id") or comp.get("id"),
            "name": comp.get("name"),
            "comments": _clip(comp.get("comments")),
            "position": comp.get("position"),
            "running": entity.get("runningCount"),
            "stopped": entity.get("stoppedCount"),
            "invalid": entity.get("invalidCount"),
            "disabled": entity.get("disabledCount"),
            "revision": _revision(entity).get("version"),
            "parameter_context": (comp.get("parameterContext") or {}).get("id"),
        }
    )


def compact_controller_service(entity: dict[str, Any]) -> dict[str, Any]:
    comp = _component(entity)
    return _compact(
        {
            "id": entity.get("id") or comp.get("id"),
            "name": comp.get("name"),
            "type": (comp.get("type") or "").rsplit(".", 1)[-1],
            "full_type": comp.get("type"),
            "state": comp.get("state"),
            "validation_status": comp.get("validationStatus"),
            "validation_errors": (comp.get("validationErrors") or [])[:8],
            "revision": _revision(entity).get("version"),
        }
    )


def compact_port(entity: dict[str, Any]) -> dict[str, Any]:
    comp = _component(entity)
    return _compact(
        {
            "id": entity.get("id") or comp.get("id"),
            "name": comp.get("name"),
            "state": comp.get("state"),
            "type": comp.get("type"),
            "position": comp.get("position"),
            "revision": _revision(entity).get("version"),
        }
    )


def compact_canvas_shape(entity: dict[str, Any]) -> dict[str, Any]:
    """Funnel, remote process group or label: only what placement needs to keep clear of it."""
    comp = _component(entity)
    return _compact(
        {
            "id": entity.get("id") or comp.get("id"),
            "name": comp.get("name"),
            "position": comp.get("position") or entity.get("position"),
            "width": comp.get("width"),
            "height": comp.get("height"),
        }
    )


def compact_parameter_context(entity: dict[str, Any]) -> dict[str, Any]:
    comp = _component(entity)
    parameters = []
    for item in comp.get("parameters") or []:
        param = item.get("parameter") or item
        parameters.append(
            _compact(
                {
                    "name": param.get("name"),
                    "value": param.get("value"),
                    "sensitive": bool(param.get("sensitive")),
                    "description": _clip(param.get("description")),
                }
            )
        )
    bound = [(ref.get("component") or ref).get("id") or ref.get("id") for ref in comp.get("boundProcessGroups") or []]
    return redact(
        _compact(
            {
                "id": entity.get("id") or comp.get("id"),
                "name": comp.get("name"),
                "description": _clip(comp.get("description")),
                "parameters": parameters,
                "bound_process_groups": [gid for gid in bound if gid],
                "revision": _revision(entity).get("version"),
            }
        )
    )


def compact_bulletin(item: dict[str, Any]) -> dict[str, Any]:
    bulletin = item.get("bulletin") or item
    return _compact(
        {
            "id": bulletin.get("id") or item.get("id"),
            "level": bulletin.get("level"),
            "category": bulletin.get("category"),
            "source_id": bulletin.get("sourceId"),
            "source_name": bulletin.get("sourceName"),
            "message": _clip(bulletin.get("message"), 240),
            "timestamp": bulletin.get("timestamp"),
            "group_id": bulletin.get("groupId"),
        }
    )


def compact_processor_type(item: dict[str, Any]) -> dict[str, Any]:
    bundle = item.get("bundle") or {}
    return _compact(
        {
            "type": item.get("type"),
            "description": _clip(item.get("description"), 160),
            "tags": item.get("tags") or [],
            "bundle": {
                "group": bundle.get("group"),
                "artifact": bundle.get("artifact"),
                "version": bundle.get("version"),
            },
            "restricted": bool(item.get("restricted")),
        }
    )


def compact_flow(flow_entity: dict[str, Any]) -> dict[str, Any]:
    """Collapse GET /flow/process-groups/{id} into a canvas outline."""
    pg_flow = flow_entity.get("processGroupFlow") or flow_entity
    flow = pg_flow.get("flow") or {}
    breadcrumb = ((pg_flow.get("breadcrumb") or {}).get("breadcrumb") or {})
    return {
        "id": pg_flow.get("id") or breadcrumb.get("id"),
        "name": breadcrumb.get("name"),
        "parent_group_id": pg_flow.get("parentGroupId"),
        "process_groups": [compact_process_group(item) for item in flow.get("processGroups") or []],
        "processors": [compact_processor(item) for item in flow.get("processors") or []],
        "connections": [compact_connection(item) for item in flow.get("connections") or []],
        "input_ports": [compact_port(item) for item in flow.get("inputPorts") or []],
        "output_ports": [compact_port(item) for item in flow.get("outputPorts") or []],
        "controller_services": [
            compact_controller_service(item) for item in flow.get("controllerServices") or []
        ],
        "remote_process_groups": [
            compact_canvas_shape(item) for item in flow.get("remoteProcessGroups") or []
        ],
        "funnels": [compact_canvas_shape(item) for item in flow.get("funnels") or []],
        "labels": [compact_canvas_shape(item) for item in flow.get("labels") or []],
    }


def maybe_compact(payload: Any, *, verbose: bool, compact_fn: Any | None = None) -> Any:
    redacted = redact(payload)
    if verbose:
        return redacted
    if compact_fn is None:
        return redacted
    if isinstance(payload, list):
        return [compact_fn(item) for item in payload]
    return compact_fn(payload)
