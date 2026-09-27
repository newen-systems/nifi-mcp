"""Declarative flow apply. Name-based wiring absorbed from ms82119/NiFiMCP."""

from __future__ import annotations

from collections.abc import Awaitable
from dataclasses import dataclass, field
from typing import Any

from nifi_mcp.client import DATA_SIZE_FIELD, NiFiClient, check_data_size, is_nifi_id, relationships_for_source
from nifi_mcp.compact import compact_flow
from nifi_mcp.errors import NiFiError, Outcome
from nifi_mcp.layout import (
    COL_PITCH,
    PG_COL_PITCH,
    PG_ROW_PITCH,
    REAL_CARD_SIZES,
    ROW_PITCH,
    Box,
    Canvas,
    assert_no_box_overlap,
    assign_canvas_positions,
    origin_below,
    outline_edges,
    positions_from_flow,
    real_boxes,
    relationship_order,
    route_connections,
    spec_kind,
)
from nifi_mcp.redaction import Vetted
from nifi_mcp.render import Renderer

SERVICE_PREFIX = "@"
KIND_PROCESSOR = "processor"
KIND_SERVICE = "controller_service"
KIND_CONNECTION = "connection"
KIND_INPUT_PORT = "input_port"
KIND_OUTPUT_PORT = "output_port"


_kind = spec_kind
# Where a normalised item sits in the spec ("objects[2]", "connections[0]"). Refusals, warnings and
# created[] name an item by it, never by its name: a name is a submitted value.
REF = "_ref"


class SpecError(ValueError):
    """A defect the builder found in the spec itself. NiFi was not asked, so cause is "spec"."""


def _name(item: dict[str, Any]) -> str:
    name = item.get("name")
    if not name:
        raise SpecError(f"{_ref(item)} is missing name")
    return str(name)


def _ref(item: dict[str, Any]) -> str:
    return str(item.get(REF) or "object")


def _label(item: dict[str, Any], where: str) -> str:
    """An item as refusals name it: its place in the spec and, for a kind this builder knows, the kind."""
    kind = spec_kind(item)
    return f"{where} ({kind})" if kind in _KNOWN_KEYS else where


def coordinate(label: str, value: Any) -> float:
    """A spec x or y as a float. The error names the field and the type, never the value."""
    if value is None or value == "":
        return 0.0
    if isinstance(value, bool) or not isinstance(value, int | float | str):
        raise TypeError(f"{label} must be a number, not {type(value).__name__}")
    try:
        return float(value)
    except ValueError:
        raise ValueError(f"{label} must be a number, not a non-numeric string") from None


def _named_points(objects: list[dict[str, Any]]) -> dict[str, tuple[float, float]]:
    points: dict[str, tuple[float, float]] = {}
    for item in objects:
        name = item.get("name")
        if not name or item.get("x") is None or item.get("y") is None:
            continue
        points[str(name)] = (float(item["x"]), float(item["y"]))
    return points


def _property_value(key: str, value: Any) -> str | None:
    """A JSON scalar as NiFi stores it. NiFi matches allowable values exactly ("true", never "True")
    and removes a property whose value is null (AbstractComponentNode.setProperties)."""
    if value is None:
        return None
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int | float):
        return str(value)
    raise TypeError(f"property {key!r} must be a string, number, boolean or null, not {type(value).__name__}")


# List-valued spec keys, each with its aliases, canonical name first. NiFi maps both into a
# Set<String> (ProcessorConfigDTO.autoTerminatedRelationships, ConnectionDTO.selectedRelationships).
_LIST_KEYS = (
    ("auto_terminated", "autoTerminatedRelationships"),
    ("relationships", "selectedRelationships"),
)


def name_list(label: str, value: Any) -> list[str]:
    """A relationship field as a list of names. A bare string is one name, never its characters."""
    if value is None or value == "":
        return []
    if isinstance(value, str):
        return [value]
    if isinstance(value, list | tuple) and all(isinstance(entry, str) for entry in value):
        return list(value)
    raise TypeError(f"{label} must be a relationship name or a list of names, not {_shape(value)}")


def _shape(value: Any) -> str:
    """What a rejected value is, without the value itself (it may hold a secret)."""
    if isinstance(value, list | tuple):
        kinds = sorted({type(entry).__name__ for entry in value if not isinstance(entry, str)})
        return f"a {type(value).__name__} holding {', '.join(kinds)}"
    return type(value).__name__


# Connection queue settings (ConnectionDTO backPressureObjectThreshold, backPressureDataSizeThreshold,
# flowFileExpiration). NiFi keeps its defaults for any not set.
_QUEUE_KEYS = ("back_pressure_object_threshold", "back_pressure_data_size_threshold", "flow_file_expiration")


def queue_setting(label: str, value: Any) -> int | str:
    """One connection queue setting in the type NiFi takes. The error names the field, never the value."""
    if label.endswith("back_pressure_object_threshold"):
        if isinstance(value, str) and value.strip().isdigit():
            value = int(value)
        if isinstance(value, bool) or not isinstance(value, int):
            raise TypeError(f"{label} must be a whole number of FlowFiles, not {_shape(value)}")
        if value < 0:
            raise ValueError(f"{label} must not be negative")
        return value
    if not isinstance(value, str) or not value.strip():
        raise TypeError(f"{label} must be a string such as '1 GB' or '1 min', not {_shape(value)}")
    if label.endswith(DATA_SIZE_FIELD):
        check_data_size(value, label)
    return value


def _normalise_queue(out: dict[str, Any], label: str) -> None:
    for key in _QUEUE_KEYS:
        if out.get(key) is not None:
            out[key] = queue_setting(f"{label}: {key}", out[key])


def _normalise_item(item: Any, where: str) -> dict[str, Any]:
    if not isinstance(item, dict):
        raise TypeError(f"{where} must be an object, not {type(item).__name__}")
    out = dict(item)
    out[REF] = where
    label = _label(item, where)
    for canonical, alias in _LIST_KEYS:
        present = [key for key in (canonical, alias) if key in out]
        if not present:
            continue
        # Both keys set: keep every name from both, in order, so neither list is lost.
        merged = [name for key in present for name in name_list(f"{label}: {key}", out.pop(key))]
        out[canonical] = list(dict.fromkeys(merged))
    if (spec_kind(item) or KIND_CONNECTION) == KIND_CONNECTION:
        _normalise_queue(out, label)
    for where_xy, holder in (("", out), ("position.", out.get("position"))):
        if not isinstance(holder, dict):
            continue
        if holder is not out:
            holder = out["position"] = dict(holder)
        for axis in ("x", "y"):
            if holder.get(axis) is not None:
                holder[axis] = coordinate(f"{label}: {where_xy}{axis}", holder[axis])
    return out


def _item_list(spec: dict[str, Any], *keys: str) -> list[Any]:
    value = next((spec[key] for key in keys if spec.get(key)), [])
    if not isinstance(value, list):
        raise TypeError(f"spec {keys[0]} must be a list of objects, not {type(value).__name__}")
    return value


# Keys the builder reads at the spec top level and in its process_group object. Anything else is
# refused, as the tool argument models refuse it (extra="forbid"): a misspelt parameter_context_id
# would build the group unbound and report success.
_SPEC_KEYS = frozenset(
    {
        "objects",
        "nifi_objects",
        "connections",
        "process_group",
        "create_process_group",
        "parent_process_group_id",
        "layout",
    }
)
_GROUP_KEYS = frozenset({"name", "comments", "parameter_context_id", "inherit_parameter_context", "x", "y", "position"})


def _refuse_unknown_keys(where: str, given: dict[str, Any], known: frozenset[str]) -> None:
    unknown = sorted(str(key) for key in set(given) - known)
    if unknown:
        raise SpecError(
            f"{where} has unknown keys {', '.join(unknown)}; it takes {', '.join(sorted(known))}. Nothing was created."
        )


def normalise_spec(spec: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Objects and extra connections, copied, with every list-valued key in its canonical list form
    and every x and y a float.

    Runs once, before preflight, so no builder sees an alias, a bare string where NiFi wants a list,
    or a coordinate that is not a number. An unknown key at the top level or in process_group is
    refused here, before any request.
    """
    _refuse_unknown_keys("spec", spec, _SPEC_KEYS)
    for key in ("process_group", "create_process_group"):
        if isinstance(spec.get(key), dict):
            _refuse_unknown_keys("process_group", spec[key], _GROUP_KEYS)
    objects = _item_list(spec, "objects", "nifi_objects")
    connections = _item_list(spec, "connections")
    return (
        [_normalise_item(item, f"objects[{i}]") for i, item in enumerate(objects)],
        [_normalise_item(item, f"connections[{i}]") for i, item in enumerate(connections)],
    )


def resolve_service_refs(
    properties: dict[str, Any], service_ids: dict[str, str], where: str = "object"
) -> dict[str, str | None]:
    resolved: dict[str, str | None] = {}
    for key, value in properties.items():
        if not isinstance(value, str):
            resolved[key] = _property_value(key, value)
            continue
        if value.startswith(SERVICE_PREFIX):
            ref = value[len(SERVICE_PREFIX) :]
            if ref not in service_ids:
                raise SpecError(
                    f"{where}: property {key!r} references a controller service (@name) that this spec does not "
                    "declare before it. Declare the service first; @ takes its exact name."
                )
            resolved[key] = service_ids[ref]
        else:
            resolved[key] = value
    return resolved


# What a spec with the wrong shape raises (a list where a dict belongs, a missing key, a bad
# number). NiFi and network failures arrive as NiFiError from the client.
_MALFORMED_SPEC = (KeyError, AttributeError, TypeError, ValueError)
_LIVE_SERVICE_STATES = {"ENABLED", "ENABLING"}
# State of a created[] step whose request got no definite answer (a timeout, a 5xx, a lost
# connection): NiFi may or may not have applied it.
UNKNOWN = str(Outcome.UNKNOWN)
APPLIED = str(Outcome.APPLIED)


@dataclass
class SpecResult:
    """The one result of apply_flow_spec, built up from the first component the call creates.

    Every exit path (a NiFi error, a malformed spec, a failed summary read, success) returns
    to_dict() of this same object, so created[] and the hint can never disagree with what exists.
    """

    # None only when the spec named a parent that is not a NiFi id: nothing was sent.
    process_group_id: str | None
    created_group: bool = False
    created: list[dict[str, Any]] = field(default_factory=list)
    names: dict[str, dict[str, str]] = field(default_factory=dict)
    service_ids: dict[str, str] = field(default_factory=dict)
    versions: dict[str, int] = field(default_factory=dict)
    warnings: list[str] = field(default_factory=list)
    flow: dict[str, Any] | None = None
    error: str | None = None
    cause: str | None = None
    hint: str | None = None
    # The failing request's Outcome; applied when the build finished.
    outcome: str = APPLIED
    # What the read-back of each unknown step showed, in order.
    read_backs: list[str] = field(default_factory=list)
    # Renders every error, hint and warning of an error result (the spec's values never appear).
    renderer: Renderer = field(default_factory=Renderer)

    def record(self, step: dict[str, Any], entity: dict[str, Any], nifi_type: str | None = None) -> dict[str, Any]:
        """A step (from _step) whose create NiFi applied: its id goes into created[] and name_map."""
        step["id"] = str(entity.get("id") or (entity.get("component") or {}).get("id"))
        step["outcome"] = APPLIED
        if not any(item is step for item in self.created):
            self.created.append(step)
        if nifi_type:
            self.names[step["name"]] = {"id": step["id"], "type": nifi_type, "group_id": self.process_group_id}
        return step

    def record_group(self, step: dict[str, Any], entity: dict[str, Any], parent_id: str) -> None:
        self.record(step, entity)
        self.created_group = True
        self.process_group_id = step["id"]
        self.names[step["name"]] = {"id": self.process_group_id, "type": "PROCESS_GROUP", "group_id": parent_id}

    def record_service(self, step: dict[str, Any], entity: dict[str, Any]) -> None:
        item = self.record(step, entity, "CONTROLLER_SERVICE")
        self.service_ids[item["name"]] = item["id"]
        self.set_service_state(item, entity)

    def set_service_state(self, item: dict[str, Any], entity: dict[str, Any]) -> None:
        item["state"] = (entity.get("component") or {}).get("state") or "DISABLED"
        item["outcome"] = APPLIED
        self.versions[item["id"]] = int((entity.get("revision") or {}).get("version") or 0)

    async def mutate[T](self, step: dict[str, Any], call: Awaitable[T]) -> T:
        """Send one create or state change for `step` (a created[] item, or the one it will be).

        The client never retries a mutation, so one with outcome unknown (a timeout, a 5xx, a lost
        connection) may or may not have been applied: the step goes into created[] with outcome and
        state unknown, and the hint carries what the client's read-back showed. A step that was
        not applied (never sent, or refused) is not added.
        """
        try:
            return await call
        except NiFiError as exc:
            if exc.outcome is Outcome.UNKNOWN:
                if not any(item is step for item in self.created):
                    self.created.append(step)
                step["state"] = step["outcome"] = UNKNOWN
                if exc.hint:
                    self.read_backs.append(exc.hint)
            raise

    def fail(self, exc: BaseException, cause: str, outcome: Outcome | None = None, hint: str | None = None) -> None:
        """outcome and hint: the failing request's. No outcome means no request failed (a spec defect,
        a read), so nothing the failing step asked for was applied. An unknown request's hint (its
        read-back) is already in read_backs."""
        error = self.renderer.error_text(exc)
        if cause == "spec" and not isinstance(exc, SpecError | NiFiError):
            error = f"Malformed spec ({type(exc).__name__}): {error}"
        self.error, self.cause = error, cause
        self.outcome = str(outcome or Outcome.NOT_APPLIED)
        rollback = _rollback_hint(self)
        self.hint = f"{hint} {rollback}" if hint and outcome is not Outcome.UNKNOWN else rollback

    def summary_unavailable(self, exc: NiFiError) -> None:
        # Everything was built; only the summary read failed. Keep the ids so the model can find it.
        self.warnings.append(
            f"Built, but the flow view could not be fetched ({self.renderer.error_text(exc)}); "
            f"call nifi_get_flow on {self.process_group_id} to inspect it."
        )
        self.hint = f"Everything in created[] exists. Call nifi_get_flow on {self.process_group_id} to inspect it."

    def to_dict(self) -> dict[str, Any]:
        """An ok result names what it built. An error result names each created[] item by its place
        in the spec (ref) and id, never by name, and passes through the renderer whole."""
        out: dict[str, Any] = {"status": "error" if self.error else "ok", "outcome": self.outcome}
        if self.error:
            out["error"] = self.error
            out["cause"] = self.cause
        out["process_group_id"] = self.process_group_id
        if self.error:
            out["created"] = [{key: value for key, value in item.items() if key != "name"} for item in self.created]
        else:
            out["created"] = self.created
            # Ids are never secrets; keys are the model's own names (a processor may be called FetchToken).
            out["name_map"] = Vetted({name: meta["id"] for name, meta in self.names.items()})
        if self.flow is not None:
            out["flow"] = self.flow
        if self.hint:
            out["hint"] = self.hint
        if self.warnings:
            out["warnings"] = self.warnings
        return self.renderer.payload(out) if self.error else out


def _rollback_hint(result: SpecResult) -> str:
    unknown = [item for item in result.created if item.get("state") == UNKNOWN]
    if not unknown:
        return _undo_hint(result)
    steps = "; ".join(
        f"enabling {item['kind']} {item['id']}" if item.get("id") else f"creating {item['kind']} {item['ref']}"
        for item in unknown
    )
    hint = (
        f"A request got no definite answer ({steps}), so NiFi may have applied it: created[] lists it "
        f"with outcome unknown. {' '.join(result.read_backs)} Check before retrying anything, and do not "
        "apply the same spec again until you have checked."
    )
    if any(not item.get("id") for item in unknown):
        # No id, so nothing ties a component to this call: an existing one may share the name.
        hint += (
            f" A create with no id in created[] may have added a component to {result.process_group_id}: "
            "if so, it is one that was not there before this call. Tell it apart from what was already "
            "there by comparing with what you read before this call, and leave alone anything you cannot "
            "tie to this call."
        )
    return f"{hint} {_undo_hint(result)}"


def _undo_hint(result: SpecResult) -> str:
    created = result.created
    target_pg = result.process_group_id
    # Only an id ties a component to this call; a same-named one may have been there before.
    known = [item for item in created if item.get("id")]
    if not created:
        return "Nothing was created. Fix the spec and apply it again."
    if not known:
        again = "Once you know what exists, fix the spec and apply it again."
        if created[0]["kind"] == "process_group":
            # Only the group create was sent: nothing else was asked of NiFi.
            return again
        return (
            f"The spec was applied into existing process group {target_pg}, which held other components "
            f"before this call: do not delete it. {again}"
        )
    if result.created_group:
        services = [item for item in created if item["kind"] == KIND_SERVICE and item.get("id")]
        enabled = [item["id"] for item in services if item.get("state") in _LIVE_SERVICE_STATES]
        maybe = [item["id"] for item in services if item.get("state") == UNKNOWN]
        if not enabled and not maybe:
            return (
                f"This call created process group {target_pg}. Delete that group to roll back everything "
                "it created, then fix the spec and apply it again."
            )
        # NiFi refuses to delete a group that holds an enabled service
        # (StandardProcessGroup.verifyCanDelete -> StandardControllerServiceNode.verifyCanDelete).
        how = "enabled, or may have enabled," if maybe else "enabled"
        return (
            f"This call created process group {target_pg} and {how} controller services in it: "
            f"{', '.join(enabled + maybe)}. Disable those services first (nifi_set_controller_service_state "
            "DISABLED; the processors this call created that reference them are stopped), then delete "
            "the group to roll back everything it created. Then fix the spec and apply it again."
        )
    ids = ", ".join(f"{item['kind']} {item['id']}" for item in known)
    return (
        f"The spec was applied into existing process group {target_pg}, which held other components "
        f"before this call: do not delete it. To roll back, delete only these created[] objects by id "
        f"with nifi_delete_component (connections first, disable services before deleting them): {ids}. "
        "Then fix the spec and apply it again."
    )


async def apply_flow_spec(
    client: NiFiClient,
    spec: dict[str, Any],
    *,
    parent_process_group_id: str | None = None,
    renderer: Renderer | None = None,
) -> dict[str, Any]:
    """Create a process group from a JSON spec of services, processors, and connections.

    renderer: the tool call's, built from everything it submitted; by default one built from the spec."""
    parent_id = str(parent_process_group_id or spec.get("parent_process_group_id") or "root")
    # A value that is not an id is never echoed: it may be a secret pasted into the wrong field.
    result = SpecResult(
        process_group_id=parent_id if is_nifi_id(parent_id) else None,
        renderer=renderer or Renderer(spec, parent_process_group_id),
    )
    try:
        _preflight_ids(spec, parent_id)
        await _build(client, spec, parent_id, result)
    except SpecError as exc:
        result.fail(exc, "spec")
        return result.to_dict()
    except NiFiError as exc:
        result.fail(exc, "nifi", exc.outcome, exc.hint)
        return result.to_dict()
    except _MALFORMED_SPEC as exc:
        result.fail(exc, "spec")
        return result.to_dict()
    try:
        result.flow = await client.get_flow(result.process_group_id)
    except NiFiError as exc:
        result.summary_unavailable(exc)
    return result.to_dict()


async def _build(client: NiFiClient, spec: dict[str, Any], parent_id: str, result: SpecResult) -> None:
    """Every create goes through result, so a failure at any stage leaves it describing what exists."""
    objects, extra_connections = normalise_spec(spec)
    create_pg = spec.get("process_group") or spec.get("create_process_group")
    _preflight_group(create_pg)
    _preflight_items(objects)
    _preflight_names(objects, create_pg)
    _preflight_service_refs(objects)
    _preflight_connections(objects, extra_connections)
    await _place_cards(
        client,
        objects,
        extra_connections,
        auto=str(spec.get("layout") or "auto").lower() != "manual",
        existing_group=None if create_pg else parent_id,
        result=result,
    )
    if create_pg:
        await _create_group(client, create_pg, parent_id, result)
    await _create_services(client, objects, result)
    await _enable_services(client, result)
    await _create_processors(client, objects, result)
    await _create_ports(client, objects, result)
    connections = [item for item in objects if _kind(item) == KIND_CONNECTION] + extra_connections
    await _create_connections(client, connections, objects, result)
    result.warnings.extend(_unknown_key_warnings(objects + extra_connections))


async def _create_group(client: NiFiClient, create_pg: dict[str, Any], parent_id: str, result: SpecResult) -> None:
    pg_name = create_pg["name"]
    x, y = await _group_position(client, create_pg, parent_id, result)
    context_id = create_pg.get("parameter_context_id")
    if context_id is None and create_pg.get("inherit_parameter_context", True):
        context_id = await parent_parameter_context(client, parent_id)
    step = _step("process_group", str(pg_name), "process_group")
    pg = await result.mutate(
        step,
        client.create_process_group(
            parent_id,
            pg_name,
            x=x,
            y=y,
            comments=create_pg.get("comments"),
            parameter_context_id=context_id,
        ),
    )
    result.record_group(step, pg, parent_id)


def _step(kind: str, name: str, ref: str) -> dict[str, Any]:
    """A created[] item before its request is sent: no id until NiFi answers with one."""
    return {"kind": kind, "id": None, "ref": ref, "name": name}


_COMMON_KEYS = {"type", "kind", "name", "x", "y", "position", REF}
_KNOWN_KEYS = {
    KIND_PROCESSOR: _COMMON_KEYS
    | {
        "processor_type",
        "class",
        "properties",
        "auto_terminated",
        "autoTerminatedRelationships",
        "bundle",
        "scheduling_period",
        "scheduling_strategy",
        "comments",
    },
    KIND_SERVICE: _COMMON_KEYS | {"service_type", "class", "properties", "bundle"},
    KIND_INPUT_PORT: _COMMON_KEYS,
    KIND_OUTPUT_PORT: _COMMON_KEYS,
    KIND_CONNECTION: _COMMON_KEYS
    | {"source", "from", "target", "destination", "to", "relationships", "selectedRelationships", *_QUEUE_KEYS},
}


def _unknown_key_warnings(items: list[dict[str, Any]]) -> list[str]:
    """Spec keys this builder does not apply, so a typo or unsupported setting is not silently lost."""
    warnings: list[str] = []
    for item in items:
        kind = _kind(item) or KIND_CONNECTION
        known = _KNOWN_KEYS.get(kind)
        if known is None:
            warnings.append(f"{_ref(item)}: its type is not one this builder knows, so it was ignored")
            continue
        unknown = sorted(str(key) for key in set(item) - known)
        if unknown:
            warnings.append(f"{_ref(item)} ({kind}): ignored unknown keys {', '.join(unknown)}")
    return warnings


def _requested_point(item: dict[str, Any]) -> tuple[float, float] | None:
    pos = item.get("position") or {}
    x = pos.get("x", item.get("x"))
    y = pos.get("y", item.get("y"))
    if x is None and y is None:
        return None
    return float(x or 0), float(y or 0)


async def _place_cards(
    client: NiFiClient,
    objects: list[dict[str, Any]],
    extra_connections: list[dict[str, Any]],
    *,
    auto: bool,
    existing_group: str | None,
    result: SpecResult,
) -> None:
    """Decide every processor and port position before anything is created, through one Canvas.

    Auto layout lays the spec out as one tree below every card already in the group. Manual
    layout keeps each x/y and gives objects without one the next free slot. Either way, a card
    that would overlap another is shifted clear and the move is reported in warnings.
    """
    cards = [item for item in objects if _kind(item) in _SPEC_SOURCE_TYPES]
    if not cards:
        return
    canvas = await read_canvas(client, existing_group) if existing_group else Canvas()
    if auto:
        assign_canvas_positions(objects, extra_connections, origin=origin_below(canvas.footprints()))
    # Cards with a spot first, so a defaulted card never takes a spot another card asked for.
    ordered = sorted(cards, key=lambda item: _requested_point(item) is None)
    for item in ordered:
        point, note = canvas.place(_ref(item), _kind(item), _requested_point(item))
        item["x"], item["y"] = point
        item["position"] = {"x": point[0], "y": point[1]}
        if note:
            result.warnings.append(note)


async def _group_position(
    client: NiFiClient, create_pg: dict[str, Any], parent_id: str, result: SpecResult
) -> tuple[float, float]:
    pos = create_pg.get("position") or {}
    point, note = await place_card(
        client,
        parent_id,
        "process_group",
        pos.get("x", create_pg.get("x")),
        pos.get("y", create_pg.get("y")),
    )
    if note:
        result.warnings.append(note)
    return point


async def _create_services(client: NiFiClient, objects: list[dict[str, Any]], result: SpecResult) -> None:
    for item in objects:
        if _kind(item) != KIND_SERVICE:
            continue
        step = _step(KIND_SERVICE, _name(item), _ref(item))
        # A service may reference services declared before it; _enable_services keeps spec order.
        properties = resolve_service_refs(item.get("properties") or {}, result.service_ids, _ref(item))
        entity = await result.mutate(
            step,
            client.create_controller_service(
                result.process_group_id,
                str(_type_of(item)),
                step["name"],
                properties=properties or None,
                bundle=item.get("bundle"),
            ),
        )
        result.record_service(step, entity)


async def _enable_services(client: NiFiClient, result: SpecResult) -> None:
    for item in result.created:
        if item["kind"] != KIND_SERVICE:
            continue
        enabled = await result.mutate(
            item, client.set_controller_service_state(item["id"], "ENABLED", result.versions[item["id"]])
        )
        result.set_service_state(item, enabled)


async def _create_processors(client: NiFiClient, objects: list[dict[str, Any]], result: SpecResult) -> None:
    for item in objects:
        if _kind(item) != KIND_PROCESSOR:
            continue
        step = _step(KIND_PROCESSOR, _name(item), _ref(item))
        properties = resolve_service_refs(item.get("properties") or {}, result.service_ids, _ref(item))
        entity = await result.mutate(
            step,
            client.create_processor(
                result.process_group_id,
                str(_type_of(item)),
                step["name"],
                x=item["x"],
                y=item["y"],
                properties=properties or None,
                auto_terminated=item.get("auto_terminated") or None,
                bundle=item.get("bundle"),
                scheduling_period=item.get("scheduling_period"),
                scheduling_strategy=item.get("scheduling_strategy"),
                comments=item.get("comments"),
            ),
        )
        result.record(step, entity, "PROCESSOR")


async def _create_ports(client: NiFiClient, objects: list[dict[str, Any]], result: SpecResult) -> None:
    for item in objects:
        kind = _kind(item)
        if kind not in {KIND_INPUT_PORT, KIND_OUTPUT_PORT}:
            continue
        step = _step(kind, _name(item), _ref(item))
        port_type = "INPUT_PORT" if kind == KIND_INPUT_PORT else "OUTPUT_PORT"
        entity = await result.mutate(
            step,
            client.create_port(result.process_group_id, port_type, step["name"], x=item["x"], y=item["y"]),
        )
        result.record(step, entity, port_type)


_SPEC_SOURCE_TYPES = {KIND_PROCESSOR: "PROCESSOR", KIND_INPUT_PORT: "INPUT_PORT", KIND_OUTPUT_PORT: "OUTPUT_PORT"}


_NAMED_KINDS = {*_SPEC_SOURCE_TYPES, KIND_SERVICE}
# The key that names the NiFi type of a processor or service; "class" is accepted for both.
_TYPE_KEYS = {KIND_PROCESSOR: "processor_type", KIND_SERVICE: "service_type"}


def _type_of(item: dict[str, Any]) -> Any:
    return item.get(_TYPE_KEYS[_kind(item)]) or item.get("class")


def _preflight_ids(spec: dict[str, Any], parent_id: str) -> None:
    """Every id the spec carries goes into a URL path or a request body, so each must be a NiFi
    UUID (or 'root'), checked as the tool arguments are, before any request. The error names the
    field, never the value."""
    fields = [("parent_process_group_id", parent_id)]
    create_pg = spec.get("process_group") or spec.get("create_process_group")
    if isinstance(create_pg, dict) and create_pg.get("parameter_context_id") not in (None, ""):
        fields.append(("process_group.parameter_context_id", create_pg["parameter_context_id"]))
    for label, value in fields:
        if not is_nifi_id(value):
            raise SpecError(f"{label} must be a NiFi component UUID or 'root'")


def _preflight_group(create_pg: Any) -> None:
    if create_pg is None:
        return
    if not isinstance(create_pg, dict):
        raise SpecError(f"process_group must be an object with a name, not {type(create_pg).__name__}")
    if not create_pg.get("name"):
        raise SpecError("process_group.name is required")


def _preflight_items(objects: list[dict[str, Any]]) -> None:
    """Every processor, port and service has a name, and every processor and service its type,
    before anything is placed or created, so a malformed item leaves nothing behind."""
    for index, item in enumerate(objects):
        kind = _kind(item)
        if kind not in _NAMED_KINDS:
            continue
        if not item.get("name"):
            raise SpecError(f"objects[{index}] ({kind}) is missing name")
        if kind in _TYPE_KEYS and not _type_of(item):
            raise SpecError(f"objects[{index}] ({kind}) is missing {_TYPE_KEYS[kind]}")


def _preflight_service_refs(objects: list[dict[str, Any]]) -> None:
    """Each @reference names a service declared earlier (services) or anywhere (processors),
    the order _create_services and _create_processors resolve them in."""
    services = [item for item in objects if _kind(item) == KIND_SERVICE]
    for index, item in enumerate(services):
        earlier = {str(other["name"]): "" for other in services[:index]}
        resolve_service_refs(item.get("properties") or {}, earlier, _ref(item))
    every = {str(item["name"]): "" for item in services}
    for item in objects:
        if _kind(item) == KIND_PROCESSOR:
            resolve_service_refs(item.get("properties") or {}, every, _ref(item))


def _preflight_names(objects: list[dict[str, Any]], create_pg: dict[str, Any] | None) -> None:
    """Refuse duplicate names before anything is created.

    NiFi allows them, but connections, @service references, layout and name_map are keyed by name,
    so a second 'Log' would be wired, placed and reported as if it were the first.
    """
    seen: dict[str, str] = {}
    if create_pg and create_pg.get("name"):
        seen[str(create_pg["name"])] = "process_group"
    for item in objects:
        kind = _kind(item)
        if kind not in _NAMED_KINDS or not item.get("name"):
            continue
        name = str(item["name"])
        if name in seen:
            raise SpecError(
                f"{seen[name]} and {_label(item, _ref(item))} have the same name. Connections, @service "
                "references and name_map are keyed by name, so every process group, controller service, "
                "processor and port in a spec needs its own name. Rename one."
            )
        seen[name] = _label(item, _ref(item))


def _preflight_connections(objects: list[dict[str, Any]], extra_connections: list[dict[str, Any]]) -> None:
    """Reject bad connections before anything is created, so a spec error leaves nothing behind."""
    names = {
        str(item.get("name")): {"id": "", "type": _SPEC_SOURCE_TYPES[_kind(item)], "group_id": ""}
        for item in objects
        if _kind(item) in _SPEC_SOURCE_TYPES
    }
    connections = [item for item in objects if _kind(item) == KIND_CONNECTION] + extra_connections
    for item in connections:
        _connection_endpoints(item, names)


def _connection_endpoints(
    item: dict[str, Any], names: dict[str, dict[str, str]]
) -> tuple[str, str, dict[str, str], dict[str, str], list[str]]:
    source_name = item.get("source") or item.get("from")
    dest_name = item.get("target") or item.get("destination") or item.get("to")
    relationships = item.get("relationships") or []
    where = _label(item, _ref(item))
    if not source_name or not dest_name:
        raise SpecError(f"{where} requires source and target names")
    if source_name not in names:
        raise SpecError(f"{where}: its source is not the name of a processor or port in this spec")
    if dest_name not in names:
        raise SpecError(f"{where}: its target is not the name of a processor or port in this spec")
    source = names[source_name]
    try:
        selected = relationships_for_source(source["type"], list(relationships))
    except NiFiError as exc:
        raise SpecError(f"{where}: {exc.args[0]}") from None
    return str(source_name), str(dest_name), source, names[dest_name], selected


async def _create_connections(
    client: NiFiClient,
    connections: list[dict[str, Any]],
    objects: list[dict[str, Any]],
    result: SpecResult,
) -> None:
    boxes = {
        name: (x, y, *REAL_CARD_SIZES.get(spec_kind(item), REAL_CARD_SIZES["processor"]))
        for item in objects
        for name, (x, y) in _named_points([item]).items()
    }
    prepared = [_connection_endpoints(item, result.names) for item in connections]
    pairs = [(src, dst) for src, dst, *_rest in prepared]
    routes = route_connections(boxes, pairs)
    for item, ends, (bends, label_index) in zip(connections, prepared, routes, strict=True):
        source_name, dest_name, source, dest, relationships = ends
        step = _step(KIND_CONNECTION, str(item.get("name") or f"{source_name}->{dest_name}"), _ref(item))
        entity = await result.mutate(
            step,
            client.create_connection(
                result.process_group_id,
                source_id=source["id"],
                source_group_id=source["group_id"],
                source_type=source["type"],
                destination_id=dest["id"],
                destination_group_id=dest["group_id"],
                destination_type=dest["type"],
                relationships=relationships,
                name=item.get("name"),
                bends=bends or None,
                label_index=label_index,
                **{key: item[key] for key in _QUEUE_KEYS if item.get(key) is not None},
            ),
        )
        result.record(step, entity)


def _unchanged_xy(item: dict[str, Any], x: float, y: float) -> bool:
    cur = item.get("position") or {}
    return float(cur.get("x") or 0) == x and float(cur.get("y") or 0) == y


def _entity_version(entity: dict[str, Any]) -> int:
    return int((entity.get("revision") or {}).get("version") or 0)


async def _move_processors(
    client: NiFiClient, outline: dict[str, Any], positions: dict[str, tuple[float, float]]
) -> list[dict[str, Any]]:
    moved: list[dict[str, Any]] = []
    for proc in outline.get("processors") or []:
        pid = str(proc.get("id") or "")
        if pid not in positions:
            continue
        x, y = positions[pid]
        if _unchanged_xy(proc, x, y):
            continue
        # Position-only PUT: StandardProcessorDAO.verifyUpdate does not count position as a
        # modification, so running processors move in place and are never stopped.
        current = await client.get_processor(pid)
        await client.update_processor(pid, version=_entity_version(current), x=x, y=y)
        moved.append({"id": pid, "name": proc.get("name"), "x": x, "y": y})
    return moved


async def _move_process_groups(
    client: NiFiClient, outline: dict[str, Any], positions: dict[str, tuple[float, float]]
) -> list[dict[str, Any]]:
    moved: list[dict[str, Any]] = []
    for group in outline.get("process_groups") or []:
        gid = str(group.get("id") or "")
        if gid not in positions:
            continue
        x, y = positions[gid]
        if _unchanged_xy(group, x, y):
            continue
        current = await client.get_process_group(gid)
        version = int((current.get("revision") or {}).get("version") or 0)
        await client.update_process_group(gid, version=version, x=x, y=y)
        moved.append({"id": gid, "name": group.get("name"), "x": x, "y": y, "kind": "process_group"})
    return moved


def _context_of(group: dict[str, Any]) -> str | None:
    return ((group.get("component") or {}).get("parameterContext") or {}).get("id")


async def parent_parameter_context(client: NiFiClient, parent_id: str) -> str | None:
    """The parent's parameter context. NiFi's REST create does not inherit it; the UI copies it."""
    return _context_of(await client.get_process_group(parent_id))


async def bind_parameter_context(
    client: NiFiClient, process_group_id: str, context_id: str | None, *, recursive: bool = True
) -> dict[str, Any]:
    """Bind a context to a group in one PUT. recursive also binds every descendant, whatever context it
    had, exactly like the UI's "Apply recursively"; a rejected request changes no group."""
    current = await client.get_process_group(process_group_id)
    return await client.set_process_group_parameter_context(
        process_group_id, context_id, version=_entity_version(current), recursive=recursive
    )


async def read_canvas(client: NiFiClient, process_group_id: str) -> Canvas:
    """Every card NiFi draws on a canvas: groups, processors, ports, funnels, remote groups, labels."""
    return Canvas.from_outline(compact_flow(await client.get_flow(process_group_id)))


async def canvas_footprints(client: NiFiClient, process_group_id: str) -> list[Box]:
    """Footprint of every card on a canvas, from the one occupancy model."""
    return (await read_canvas(client, process_group_id)).footprints()


async def next_process_group_slot(client: NiFiClient, parent_id: str) -> tuple[float, float]:
    """Free 424x288 cell whose footprint clears every card on the parent canvas."""
    return (await read_canvas(client, parent_id)).free_slot("process_group")


async def next_processor_slot(client: NiFiClient, parent_id: str) -> tuple[float, float]:
    """Free 512x240 processor cell whose footprint clears every card on the canvas."""
    return (await read_canvas(client, parent_id)).free_slot(KIND_PROCESSOR)


async def place_card(
    client: NiFiClient,
    parent_id: str,
    kind: str,
    x: float | None,
    y: float | None,
    *,
    moving: str | None = None,
) -> tuple[tuple[float, float], str | None]:
    """Where one new (or moved) card goes on a canvas, and a note if its requested spot was taken.

    Both x and y omitted: the next free slot. Otherwise the requested point (a missing coordinate
    is 0), shifted down clear of every other card. `moving` is the id of a card being moved.
    """
    canvas = await read_canvas(client, parent_id)
    if moving:
        canvas.remove(moving)
    point = None if x is None and y is None else (coordinate(f"{kind}: x", x), coordinate(f"{kind}: y", y))
    return canvas.place(moving or f"new {kind}", kind, point, label=None if moving else f"the new {kind}")


async def _move_ports(
    client: NiFiClient, group_id: str, outline: dict[str, Any], positions: dict[str, tuple[float, float]]
) -> list[dict[str, Any]]:
    moved: list[dict[str, Any]] = []
    for key, kind in (("input_ports", "INPUT_PORT"), ("output_ports", "OUTPUT_PORT")):
        for port in outline.get(key) or []:
            pid = str(port.get("id") or "")
            if pid not in positions:
                continue
            x, y = positions[pid]
            if _unchanged_xy(port, x, y):
                continue
            version = int(port.get("revision") or 0)
            await client.update_port(pid, version=version, kind=kind, x=x, y=y, parent_id=group_id)
            moved.append({"id": pid, "name": port.get("name"), "x": x, "y": y, "kind": kind})
    return moved


async def _route_connections(
    client: NiFiClient, group_id: str, outline: dict[str, Any], positions: dict[str, tuple[float, float]]
) -> tuple[list[dict[str, Any]], list[str]]:
    """Send each connection's bends. Returns what was sent and a warning for any connection whose line
    or label the router could not clear, named by id, so a failed check is never silent."""
    kept: list[dict[str, Any]] = []
    pairs: list[tuple[str, str]] = []
    listed = zip(outline.get("connections") or [], outline_edges(outline), strict=True)
    for conn, pair in sorted(listed, key=lambda item: relationship_order(item[0])):
        if not all(pair) or not conn.get("id"):
            continue
        pairs.append(pair)
        kept.append(conn)
    boxes = real_boxes(outline, positions)
    routes = route_connections(boxes, pairs)
    warnings: list[str] = []
    try:
        assert_no_box_overlap(boxes, pairs, routes)
    except ValueError as exc:
        warnings.append(f"Layout check failed after relayout: {exc}. Move or bend that connection by hand.")
    routed: list[dict[str, Any]] = []
    for conn, (bends, label_index) in zip(kept, routes, strict=True):
        version = int(conn.get("revision") or 0)
        if version == 0:
            current = await client.get_connection(str(conn["id"]))
            version = int((current.get("revision") or {}).get("version") or 0)
        await client.update_connection(
            str(conn["id"]),
            version=version,
            bends=bends,
            label_index=label_index,
            parent_id=group_id,
        )
        routed.append({"id": conn["id"], "bends": len(bends)})
    return routed, warnings


async def relayout_process_group(client: NiFiClient, process_group_id: str) -> dict[str, Any]:
    """Lay processors out top-down, forks sideways. Bend duplicates, and lines that would cross a card."""
    outline = compact_flow(await client.get_flow(process_group_id))
    positions = positions_from_flow(outline)
    moved = await _move_processors(client, outline, positions)
    moved.extend(await _move_ports(client, process_group_id, outline, positions))
    moved.extend(await _move_process_groups(client, outline, positions))
    routed, warnings = await _route_connections(client, process_group_id, outline, positions)
    result = {
        "status": "ok",
        "process_group_id": process_group_id,
        "moved": moved,
        "connections_routed": routed,
        "col_pitch": COL_PITCH,
        "row_pitch": ROW_PITCH,
        "pg_col_pitch": PG_COL_PITCH,
        "pg_row_pitch": PG_ROW_PITCH,
        "direction": "TB",
    }
    if warnings:
        result["warnings"] = warnings
    return result
