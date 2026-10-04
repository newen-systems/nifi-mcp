"""FastMCP server: natural-language NiFi 2.x flow design."""

from __future__ import annotations

import inspect
import json
import logging
import ssl
import sys
from collections.abc import Awaitable, Callable
from functools import wraps
from typing import Annotated, Any, Literal, get_type_hints
from urllib.parse import urlsplit

from mcp.server.fastmcp import FastMCP
from mcp.server.fastmcp.exceptions import ToolError
from pydantic import (
    AfterValidator,
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    ValidationError,
    model_validator,
)

from nifi_mcp import compact as C
from nifi_mcp.client import LEDGER, NiFiClient, check_data_size, is_nifi_id
from nifi_mcp.config import Settings
from nifi_mcp.errors import NiFiError, NiFiReadOnlyError, Outcome, safe_error_message
from nifi_mcp.flow_spec import (
    apply_flow_spec,
    bind_parameter_context,
    parent_parameter_context,
    place_card,
    relayout_process_group,
)
from nifi_mcp.redaction import KNOWN, mask_components, redact
from nifi_mcp.render import Renderer
from nifi_mcp.user_auth import CALLER_CLIENT, UserTokenVerifier, caller_client

logging.basicConfig(stream=sys.stderr, level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
LOG = logging.getLogger("nifi_mcp")

INSTRUCTIONS = """
You are driving Apache NiFi 2.x through this MCP server.

Rules:
1. Never build on the root canvas. Create a process group first
   (nifi_create_process_group or nifi_apply_flow_spec with process_group).
2. Discover types with nifi_list_processor_types / nifi_list_controller_service_types,
   then nifi_get_processor_definition before setting properties.
   Property keys are the NiFi display names unless the definition says otherwise.
3. Wire connections by selectedRelationships. Auto-terminate unused relationships
   or the processor stays INVALID.
4. Controller services must be ENABLED before processors that reference them will
   validate. In a flow spec, set a property to @ServiceName.
5. Mutations are revision-locked. Tools fetch the current revision; if you get
   a NiFiConflictError (HTTP 409), stop the component and retry. A 409 whose
   hint names a field instead (scheduling_period, property 'Password', ...) is
   a value NiFi refused, and it changed nothing: fix it, nothing needs stopping.
6. Default output is compact. Pass verbose=true only when you need full entities.
7. If your deployment reconciles versioned process groups from a registry,
   prototype on an unversioned group.
8. Writes are on unless NIFI_READONLY=true. Do not ask the user to paste tokens;
   they live in env. Prototype on an unversioned sandbox process group.
9. Layout is mermaid TB on a centered axis: nifi_apply_flow_spec stacks
   processors top-to-bottom (240px rows). At a fork the main branch continues
   down; the others sit on the fork's row, perpendicular, 672px out. A
   fork of leaves spreads one row down, 512px apart. A join returns to
   the fork's axis. Every card is centred on its axis. Bends only
   for exact 1:1 connection overlaps, and for a line or label that would
   cross a card, a second line between one pair, and a retry line back
   up: out of the source's side, along a free lane, into the target's
   side, never along another line. Self-loops sit outside the card. Child process groups stack top-down
   in flow order on a 424x288 lattice, one per row, with room for the
   connection label between them. A spec into an existing group starts below its cards;
   nifi_create_processor / nifi_create_process_group without x/y take the
   next free cell. After a messy canvas, call nifi_layout_process_group.
10. #{name} parameter references need a parameter context bound to the group:
   nifi_create_parameter_context, then nifi_bind_parameter_context (or pass
   parameter_context_id when creating the group). New child groups inherit the
   parent's context; binding applies to every descendant group in one request. Delete it with
   nifi_delete_component kind=parameter_context after the group is gone.
11. Every tool that changes NiFi returns outcome: applied, not_applied or unknown.
   unknown means NiFi may have applied it: the hint says which read shows the state and what
   that read showed. Run it again before retrying; a retried create can make a duplicate.
""".strip()



class _NiFiMCP(FastMCP):
    """FastMCP whose argument errors carry no submitted values.

    FastMCP validates tool arguments before json_tool runs, and Tool.run
    (mcp.server.fastmcp.tools.base) re-raises the pydantic ValidationError as
    ToolError(f"...: {e}"), whose text embeds input_value: a secret the model just sent.
    FastMCP._setup_handlers registers self.call_tool, so this covers stdio and in-process calls.
    """

    async def call_tool(self, name: str, arguments: dict[str, Any]) -> Any:
        tool = self._tool_manager.get_tool(name)
        mutating = bool(tool and tool.annotations and tool.annotations.readOnlyHint is False)
        # FastMCP's argument model ignores unknown top-level keys; refuse them like unknown fields.
        unknown = sorted(set(arguments or {}) - set(tool.fn_metadata.arg_model.model_fields)) if tool else []
        if tool and unknown:
            error = NiFiError(f"Unknown argument(s) {', '.join(unknown)}. Tool arguments go inside params.")
            return tool.fn_metadata.convert_result(_tool_error(error, Renderer(arguments), mutating=mutating))
        try:
            return await super().call_tool(name, arguments)
        except ToolError as exc:
            if tool is None or not isinstance(exc.__cause__, Exception):
                raise
            error = _tool_error(exc.__cause__, Renderer(arguments), mutating=mutating)
            return tool.fn_metadata.convert_result(error)


mcp = _NiFiMCP("nifi_mcp", instructions=INSTRUCTIONS)

_client: NiFiClient | None = None
_settings: Settings | None = None


def configure(client: NiFiClient, settings: Settings) -> None:
    """Tests inject a mock client here."""
    global _client, _settings
    _client = client
    _settings = settings


def get_settings() -> Settings:
    global _settings
    if _settings is None:
        _settings = Settings()  # type: ignore[call-arg]
    return _settings


def get_client() -> NiFiClient:
    global _client
    if get_settings().auth == "passthrough":
        client = CALLER_CLIENT.get()
        if client is None:
            raise NiFiError("A verified caller is required; shared credentials are disabled")
        return client
    if _client is None:
        _client = NiFiClient(get_settings())
    return _client


def _require_write() -> None:
    if get_settings().readonly:
        raise NiFiReadOnlyError(
            "Server is read-only (NIFI_READONLY=true). Set NIFI_READONLY=false to create or change flows."
        )


def _entity_id(entity: dict[str, Any]) -> str | None:
    return entity.get("id") or (entity.get("component") or {}).get("id")


def _version(entity: dict[str, Any]) -> int:
    return int((entity.get("revision") or {}).get("version") or 0)


def _parent_of(entity: dict[str, Any]) -> str | None:
    """The process group that holds a connection or port: the read that shows one after an uncertain change."""
    parent = (entity.get("component") or {}).get("parentGroupId")
    return str(parent) if parent else None


def _dump(payload: Any) -> str:
    """Every tool result passes through here, so redaction cannot be skipped by a verbose path."""
    # No list clipping here: compact views are already sized, verbose paths clip where they fetch.
    return json.dumps(redact(payload, max_items=sys.maxsize), indent=2, default=str)


NOTHING_SENT_HINT = "No request that changes NiFi was sent, so nothing changed."


def _outcome(exc: Exception, ledger: list[dict[str, str]]) -> str:
    """A failed mutation tool's outcome: the failing request's, else the last request it sent (a read
    that fails after a change landed leaves that change applied), else not_applied."""
    if isinstance(exc, NiFiError) and exc.outcome is not None:
        return str(exc.outcome)
    return ledger[-1]["outcome"] if ledger else str(Outcome.NOT_APPLIED)


def _tool_error(
    exc: Exception, renderer: Renderer, *, mutating: bool = False, ledger: list[dict[str, str]] | None = None
) -> str:
    """Every tool error, argument errors included, is rendered by the call's Renderer."""
    hint = exc.hint if isinstance(exc, NiFiError) else None
    if not mutating:
        return _dump(renderer.tool_error(exc, hint=hint))
    ledger = ledger or []
    # Earlier requests of this call that landed before it failed.
    applied = [f"{item['method']} {item['path']}" for item in ledger if item["outcome"] == Outcome.APPLIED]
    hint = hint or (None if ledger else NOTHING_SENT_HINT)
    return _dump(renderer.tool_error(exc, outcome=_outcome(exc, ledger), hint=hint, applied=applied))


type ToolResult = str | dict[str, Any]


def _tool[**P](fn: Callable[P, Awaitable[ToolResult]], *, mutating: bool) -> Callable[P, Awaitable[str]]:
    """MCP tools return JSON error objects instead of raising into the host.

    A mutating tool's result always carries outcome (applied, not_applied or unknown): an error takes
    it from the request that failed, a result from this call's ledger of requests."""

    @wraps(fn)
    async def wrapper(*args: P.args, **kwargs: P.kwargs) -> str:
        ledger: list[dict[str, str]] = []
        token = LEDGER.set(ledger)
        known = KNOWN.set(set())
        try:
            async with caller_client(get_settings()):
                result = await fn(*args, **kwargs)
        except Exception as exc:  # noqa: BLE001 - boundary: never raise to the MCP host
            return _tool_error(exc, Renderer(args, kwargs), mutating=mutating, ledger=ledger)
        finally:
            KNOWN.reset(known)
            LEDGER.reset(token)
        if isinstance(result, dict):
            if mutating:
                result.setdefault("outcome", str(Outcome.APPLIED))
            return _dump(result)
        return result

    # FastMCP reads the return type for the output schema: the host always gets the JSON text.
    hints = get_type_hints(fn)
    signature = inspect.signature(fn)
    wrapper.__signature__ = signature.replace(  # type: ignore[attr-defined]
        parameters=[p.replace(annotation=hints.get(p.name, p.annotation)) for p in signature.parameters.values()],
        return_annotation=str,
    )
    return wrapper


def json_tool[**P](fn: Callable[P, Awaitable[ToolResult]]) -> Callable[P, Awaitable[str]]:
    """A read tool."""
    return _tool(fn, mutating=False)


def mutation_tool[**P](fn: Callable[P, Awaitable[ToolResult]]) -> Callable[P, Awaitable[str]]:
    """A tool that changes NiFi: every result and error says whether the change was applied."""
    return _tool(fn, mutating=True)


def _ann(
    title: str,
    *,
    read_only: bool = True,
    destructive: bool = False,
    idempotent: bool = True,
) -> dict[str, Any]:
    return {
        "title": title,
        "readOnlyHint": read_only,
        "destructiveHint": destructive,
        "idempotentHint": idempotent,
        "openWorldHint": True,
    }


def _check_nifi_id(value: str) -> str:
    """Component ids go into URL paths, so only NiFi UUIDs (or 'root') are accepted."""
    if not is_nifi_id(value):
        raise ValueError("must be a NiFi component UUID or 'root'")
    return value


NiFiId = Annotated[str, AfterValidator(_check_nifi_id)]


def _one_name_is_a_list(value: Any) -> Any:
    """NiFi maps relationship fields into a Set<String>; a bare "success" means ["success"]."""
    return [value] if isinstance(value, str) else value


# A queue data size NiFi can parse; the error names the field only.
DataSize = Annotated[str, AfterValidator(check_data_size)]
RelationshipNames = Annotated[list[str], BeforeValidator(_one_name_is_a_list)]


class ToolIn(BaseModel):
    """Every tool argument model. An unknown field is an error, so a misspelt setting is never
    dropped while the tool reports success."""

    model_config = ConfigDict(extra="forbid")


class VerboseIn(ToolIn):
    verbose: bool = Field(default=False, description="Return full redacted JSON instead of the compact view")


class ProcessGroupIn(VerboseIn):
    process_group_id: NiFiId = Field(
        default="root",
        description="Process group id, or 'root' for the canvas root",
        min_length=1,
    )


class SearchIn(VerboseIn):
    query: str = Field(..., min_length=1, max_length=200, description="NiFi search string")


class ProcessorTypesIn(VerboseIn):
    type_filter: str | None = Field(
        default=None,
        description="Case-insensitive substring of the class name, e.g. 'GenerateFlowFile' or 'record'",
    )
    limit: int = Field(default=25, ge=1, le=100)


class ProcessorDefinitionIn(ToolIn):
    group: str = Field(..., description="Maven groupId, e.g. org.apache.nifi")
    artifact: str = Field(..., description="Maven artifactId, e.g. nifi-standard-nar")
    version: str
    type_name: str = Field(..., description="Fully qualified processor class")


class ComponentIdIn(VerboseIn):
    component_id: NiFiId = Field(..., min_length=1)


class CreateProcessGroupIn(VerboseIn):
    parent_id: NiFiId = Field(default="root")
    name: str = Field(..., min_length=1, max_length=200)
    x: float | None = None
    y: float | None = None
    comments: str | None = None
    parameter_context_id: NiFiId | None = Field(
        default=None, description="Bind this parameter context so #{name} references resolve"
    )
    inherit_parameter_context: bool = Field(
        default=True, description="With no parameter_context_id, bind the parent's context (as the NiFi UI does)"
    )


class ParameterIn(ToolIn):
    name: str = Field(..., min_length=1)
    value: str | None = None
    sensitive: bool | None = Field(
        default=None,
        description="Store as a sensitive parameter; NiFi masks it on read. Omit to keep the current flag "
        "(new parameters are not sensitive). NiFi cannot change an existing parameter's flag in place: "
        "remove the parameter in one update, then add it with the new flag and value in a second.",
    )
    description: str | None = None


class CreateParameterContextIn(VerboseIn):
    name: str = Field(..., min_length=1, max_length=200)
    description: str | None = None
    parameters: list[ParameterIn] = Field(default_factory=list)


class UpdateParameterContextIn(VerboseIn):
    parameter_context_id: NiFiId
    parameters: list[ParameterIn] = Field(
        default_factory=list, description="Parameters to add or change. Unlisted parameters are kept."
    )
    remove: list[str] = Field(default_factory=list, description="Parameter names to delete")
    name: str | None = None
    description: str | None = None


class ParameterContextIdIn(VerboseIn):
    parameter_context_id: NiFiId


class BindParameterContextIn(VerboseIn):
    process_group_id: NiFiId
    parameter_context_id: NiFiId | None = Field(
        ..., description="Parameter context id to bind, or null to unbind the group"
    )
    apply_recursively: bool = Field(
        default=True,
        description="Also bind every descendant group, replacing any context it had (the UI's Apply recursively)",
    )


class CreateProcessorIn(VerboseIn):
    parent_id: NiFiId
    processor_type: str = Field(
        ...,
        description="Fully qualified class, e.g. org.apache.nifi.processors.standard.LogAttribute",
    )
    name: str
    x: float | None = Field(default=None, description="Omit x and y to take the next free processor cell")
    y: float | None = None
    properties: dict[str, str] = Field(default_factory=dict)
    auto_terminated: RelationshipNames = Field(default_factory=list)
    bundle: dict[str, str] | None = None
    scheduling_period: str | None = Field(default=None, description="Run schedule, e.g. '10 sec' or a CRON")
    scheduling_strategy: Literal["TIMER_DRIVEN", "CRON_DRIVEN"] | None = None
    comments: str | None = None


class UpdateProcessorIn(VerboseIn):
    processor_id: NiFiId
    name: str | None = None
    properties: dict[str, str | None] | None = Field(
        default=None,
        description="Only the properties to change. Unlisted properties (and secrets) are kept; null removes one.",
    )
    auto_terminated: RelationshipNames | None = None
    scheduling_period: str | None = None
    scheduling_strategy: Literal["TIMER_DRIVEN", "CRON_DRIVEN"] | None = None
    comments: str | None = None
    x: float | None = None
    y: float | None = None


class LayoutGroupIn(ToolIn):
    process_group_id: NiFiId


class RunStatusIn(ToolIn):
    component_id: NiFiId
    state: Literal["RUNNING", "STOPPED", "DISABLED", "RUN_ONCE"]


class CreateConnectionIn(VerboseIn):
    parent_id: NiFiId
    source_id: NiFiId
    source_group_id: NiFiId
    source_type: Literal[
        "PROCESSOR",
        "FUNNEL",
        "INPUT_PORT",
        "OUTPUT_PORT",
        "REMOTE_INPUT_PORT",
        "REMOTE_OUTPUT_PORT",
    ] = "PROCESSOR"
    destination_id: NiFiId
    destination_group_id: NiFiId
    destination_type: Literal[
        "PROCESSOR",
        "FUNNEL",
        "INPUT_PORT",
        "OUTPUT_PORT",
        "REMOTE_INPUT_PORT",
        "REMOTE_OUTPUT_PORT",
    ] = "PROCESSOR"
    relationships: RelationshipNames = Field(
        default_factory=list,
        description="Processor relationships to route, e.g. ['success']. Required for PROCESSOR sources; "
        "leave empty for INPUT_PORT, OUTPUT_PORT and FUNNEL sources.",
    )
    name: str | None = None
    back_pressure_object_threshold: int | None = Field(
        default=None, ge=0, description="Queue backpressure: FlowFiles queued before the source pauses (NiFi: 10000)"
    )
    back_pressure_data_size_threshold: DataSize | None = Field(
        default=None, description="Queue backpressure by size: a number and B, KB, MB, GB or TB, e.g. '1 GB' "
        "(NiFi default) or '10 MB'"
    )
    flow_file_expiration: str | None = Field(
        default=None, min_length=1, description="Drop queued FlowFiles older than this, e.g. '1 min'; '0 sec' never"
    )


_QUEUE_FIELDS = ("back_pressure_object_threshold", "back_pressure_data_size_threshold", "flow_file_expiration")


class UpdateConnectionIn(VerboseIn):
    connection_id: NiFiId
    name: str | None = None
    back_pressure_object_threshold: int | None = Field(default=None, ge=0)
    back_pressure_data_size_threshold: DataSize | None = None
    flow_file_expiration: str | None = Field(default=None, min_length=1)


class CreateServiceIn(VerboseIn):
    parent_id: NiFiId
    service_type: str
    name: str
    properties: dict[str, str] = Field(default_factory=dict)
    bundle: dict[str, str] | None = None
    enable: bool = True


class ServiceIdIn(VerboseIn):
    service_id: NiFiId


class UpdateServiceIn(VerboseIn):
    service_id: NiFiId
    name: str | None = None
    properties: dict[str, str | None] | None = Field(
        default=None,
        description="Only the properties to change. Unlisted properties (and secrets) are kept; null removes one.",
    )


class ServiceStateIn(ToolIn):
    service_id: NiFiId
    state: Literal["ENABLED", "DISABLED"]


class ScheduleGroupIn(ToolIn):
    process_group_id: NiFiId
    state: Literal["RUNNING", "STOPPED"]


class DeleteComponentIn(ToolIn):
    kind: Literal[
        "processor",
        "connection",
        "controller_service",
        "input_port",
        "output_port",
        "process_group",
        "parameter_context",
    ]
    component_id: NiFiId


class ExportFlowIn(ToolIn):
    process_group_id: NiFiId
    include_services: bool = False


class ImportFlowIn(VerboseIn):
    parent_id: NiFiId
    group_name: str
    snapshot: dict[str, Any]
    x: float | None = Field(default=None, description="Omit x and y to take the next free process group cell")
    y: float | None = None


class ReplaceFlowIn(ToolIn):
    process_group_id: NiFiId
    snapshot: dict[str, Any]


class ApplySpecIn(ToolIn):
    spec: dict[str, Any] = Field(
        ...,
        description=(
            "Declarative flow: {process_group?: {name,x,y,parameter_context_id}, parent_process_group_id?, objects: ["
            "{type: controller_service|processor|connection|input_port|output_port, ...}]}. "
            "processor keys: name, processor_type, properties, auto_terminated, bundle, "
            "scheduling_period, scheduling_strategy, comments, x, y. connection keys: source, target, "
            "relationships, name, back_pressure_object_threshold, back_pressure_data_size_threshold, "
            "flow_file_expiration. Unknown keys come back in warnings."
        ),
    )
    parent_process_group_id: NiFiId | None = None


class QueueIn(ToolIn):
    connection_id: NiFiId


class BulletinsIn(VerboseIn):
    after_id: int | None = Field(
        default=None,
        ge=0,
        description="Only bulletins with an id greater than this: pass the largest bulletin id you have "
        "seen to get newer ones. A bulletin id, not a time (NiFi's 'after' cursor).",
    )
    limit: int = Field(default=30, ge=1, le=100)

    @model_validator(mode="before")
    @classmethod
    def _after_ms_is_gone(cls, data: Any) -> Any:
        # NiFi's cursor is a bulletin id (FlowResource.getBulletinBoard "after"); a millisecond time
        # sent there silently hides every bulletin.
        if isinstance(data, dict) and "after_ms" in data:
            raise ValueError("after_ms is not supported: NiFi filters by bulletin id. Pass after_id instead.")
        return data


@mcp.tool(
    name="nifi_about",
    annotations=_ann(title="NiFi version"),
)
@json_tool
async def nifi_about() -> str:
    """NiFi version, build, and whether this is 2.x. Call first on a new session."""
    client = get_client()
    await client.authenticate()
    about = await client.about()
    major, minor, patch = NiFiClient.version_tuple(about)
    return _dump(
        {
            "version": f"{major}.{minor}.{patch}",
            "is_nifi_2x": major >= 2,
            "readonly": get_settings().readonly,
            "api_url": get_settings().api_url,
            "about": redact(about.get("about") or about),
        }
    )


@mcp.tool(
    name="nifi_current_user",
    annotations=_ann(title="Current NiFi identity"),
)
@json_tool
async def nifi_current_user() -> str:
    """Who the configured credentials authenticate as, plus anonymous/permissions flags."""
    client = get_client()
    await client.authenticate()
    return _dump(redact(await client.current_user()))


@mcp.tool(
    name="nifi_get_flow",
    annotations=_ann(title="Process group canvas"),
)
@json_tool
async def nifi_get_flow(params: ProcessGroupIn) -> str:
    """Compact outline of a process group: child groups, processors, connections, ports."""
    client = get_client()
    await client.authenticate()
    flow = await client.get_flow(params.process_group_id)
    payload = flow if params.verbose else C.compact_flow(flow)
    return _dump(redact(payload) if params.verbose else payload)


@mcp.tool(
    name="nifi_search",
    annotations=_ann(title="Search the canvas"),
)
@json_tool
async def nifi_search(params: SearchIn) -> str:
    """Search processors, groups, and other components by name or id."""
    client = get_client()
    await client.authenticate()
    return _dump(redact(await client.search(params.query)))


@mcp.tool(
    name="nifi_list_processor_types",
    annotations=_ann(title="Discover processor types"),
)
@json_tool
async def nifi_list_processor_types(params: ProcessorTypesIn) -> str:
    """List installed processor types. Filter by class-name substring, then call nifi_get_processor_definition."""
    client = get_client()
    await client.authenticate()
    data = await client.list_processor_types()
    types = data.get("processorTypes") or []
    needle = (params.type_filter or "").lower()
    if needle:
        types = [item for item in types if needle in (item.get("type") or "").lower()]
    sliced = types[: params.limit]
    view = sliced if params.verbose else [C.compact_processor_type(item) for item in sliced]
    return _dump({"count": len(sliced), "total_matched": len(types), "processor_types": view})


@mcp.tool(
    name="nifi_get_processor_definition",
    annotations=_ann(title="Processor property schema"),
)
@json_tool
async def nifi_get_processor_definition(params: ProcessorDefinitionIn) -> str:
    """Property descriptors, relationships, and supported scheduling for one processor type."""
    client = get_client()
    await client.authenticate()
    definition = await client.get_processor_definition(
        params.group, params.artifact, params.version, params.type_name
    )
    return _dump(redact(definition))


@mcp.tool(
    name="nifi_get_processor",
    annotations=_ann(title="Processor details"),
)
@json_tool
async def nifi_get_processor(params: ComponentIdIn) -> str:
    """One processor: state, validation errors, properties (secrets redacted)."""
    client = get_client()
    await client.authenticate()
    entity = await client.get_processor(params.component_id)
    return _dump(entity if params.verbose else C.compact_processor(entity))


@mcp.tool(
    name="nifi_list_controller_service_types",
    annotations=_ann(title="Discover controller services"),
)
@json_tool
async def nifi_list_controller_service_types(params: ProcessorTypesIn) -> str:
    """List installed controller-service types. Filter by class-name substring."""
    client = get_client()
    await client.authenticate()
    data = await client.list_controller_service_types()
    types = data.get("controllerServiceTypes") or []
    needle = (params.type_filter or "").lower()
    if needle:
        types = [item for item in types if needle in (item.get("type") or "").lower()]
    sliced = types[: params.limit]
    view = sliced if params.verbose else [C.compact_processor_type(item) for item in sliced]
    return _dump({"count": len(sliced), "total_matched": len(types), "controller_service_types": view})


@mcp.tool(
    name="nifi_list_controller_services",
    annotations=_ann(title="Controller services in a group"),
)
@json_tool
async def nifi_list_controller_services(params: ProcessGroupIn) -> str:
    """Controller services visible to a process group (includes inherited)."""
    client = get_client()
    await client.authenticate()
    data = await client.list_controller_services(params.process_group_id)
    services = data.get("controllerServices") or []
    view = services if params.verbose else [C.compact_controller_service(item) for item in services]
    return _dump({"count": len(services), "controller_services": view})


@mcp.tool(
    name="nifi_get_health",
    annotations=_ann(title="Process group health"),
)
@json_tool
async def nifi_get_health(params: ProcessGroupIn) -> str:
    """Running/stopped/invalid counts, queued connections, and processors with validation errors."""
    client = get_client()
    await client.authenticate()
    flow = await client.get_flow(params.process_group_id)
    outline = C.compact_flow(flow)
    invalid = [proc for proc in outline["processors"] if proc.get("validation_status") == "INVALID"]
    queued = [
        conn
        for conn in outline["connections"]
        if (conn.get("queued_count") or 0) > 0
    ]
    return _dump(
        {
            "id": outline["id"],
            "name": outline["name"],
            "processors": len(outline["processors"]),
            "connections": len(outline["connections"]),
            "invalid_processors": invalid,
            "queued_connections": queued,
            "child_groups": outline["process_groups"],
        }
    )


@mcp.tool(
    name="nifi_get_bulletins",
    annotations=_ann(title="Bulletin board"),
)
@json_tool
async def nifi_get_bulletins(params: BulletinsIn) -> str:
    """Recent NiFi bulletins (errors/warnings). Use after a flow misbehaves.

    after_id is a bulletin id cursor, not a time: pass the largest id from the last call for newer ones.
    """
    client = get_client()
    await client.authenticate()
    data = await client.bulletins(params.after_id)
    board = data.get("bulletinBoard") or data
    items = board.get("bulletins") or []
    sliced = items[: params.limit]
    view = sliced if params.verbose else [C.compact_bulletin(item) for item in sliced]
    return _dump({"count": len(sliced), "bulletins": view})


@mcp.tool(
    name="nifi_list_queue",
    annotations=_ann(title="List connection queue"),
)
@json_tool
async def nifi_list_queue(params: QueueIn) -> str:
    """Sample FlowFiles sitting on a connection. Connection must not be empty-delete-blocked."""
    client = get_client()
    await client.authenticate()
    return _dump(redact(await client.list_queue(params.connection_id)))


def _with_note(payload: dict[str, Any], note: str | None) -> dict[str, Any]:
    if note:
        payload["warnings"] = [note]
    return payload


@mcp.tool(
    name="nifi_create_process_group",
    annotations=_ann(title="Create process group", read_only=False, idempotent=False),
)
@mutation_tool
async def nifi_create_process_group(params: CreateProcessGroupIn) -> dict[str, Any]:
    """Create an empty process group. Build every new flow inside one of these, not on root.

    Omit x/y to take the next free 424x288 cell below the other groups. An x/y that
    would overlap another card is shifted down clear of it, and the result says so in warnings.
    """
    _require_write()
    client = get_client()
    await client.authenticate()
    (x, y), note = await place_card(client, params.parent_id, "process_group", params.x, params.y)
    context_id = params.parameter_context_id
    if context_id is None and params.inherit_parameter_context:
        context_id = await parent_parameter_context(client, params.parent_id)
    entity = await client.create_process_group(
        params.parent_id,
        params.name,
        x=x,
        y=y,
        comments=params.comments,
        parameter_context_id=context_id,
    )
    view = entity if params.verbose else C.compact_process_group(entity)
    return _with_note({"status": "ok", "process_group": view}, note)


@mcp.tool(
    name="nifi_create_processor",
    annotations=_ann(title="Create processor", read_only=False, idempotent=False),
)
@mutation_tool
async def nifi_create_processor(params: CreateProcessorIn) -> dict[str, Any]:
    """Add a processor to a process group. Prefer nifi_apply_flow_spec for a whole graph.

    Omit x/y to take the first free 512x240 cell that clears every card already in the group. An
    x/y that would overlap another card is shifted down clear of it, and the result says so.
    """
    _require_write()
    client = get_client()
    await client.authenticate()
    (x, y), note = await place_card(client, params.parent_id, "processor", params.x, params.y)
    entity = await client.create_processor(
        params.parent_id,
        params.processor_type,
        params.name,
        x=x,
        y=y,
        properties=params.properties or None,
        auto_terminated=params.auto_terminated or None,
        bundle=params.bundle,
        scheduling_period=params.scheduling_period,
        scheduling_strategy=params.scheduling_strategy,
        comments=params.comments,
    )
    view = entity if params.verbose else C.compact_processor(entity)
    return _with_note({"status": "ok", "processor": view}, note)


@mcp.tool(
    name="nifi_update_processor",
    annotations=_ann(title="Update processor", read_only=False),
)
@mutation_tool
async def nifi_update_processor(params: UpdateProcessorIn) -> dict[str, Any]:
    """Change processor properties, name, schedule or position. Sends only the fields you set.

    Property, name and schedule changes need the processor STOPPED; a position-only move does not.
    A move that would overlap another card is shifted down clear of it, and the result says so.
    """
    _require_write()
    client = get_client()
    await client.authenticate()
    current = await client.get_processor(params.processor_id)
    x, y, note = params.x, params.y, None
    component = current.get("component") or {}
    if (x is not None or y is not None) and component.get("parentGroupId"):
        here = component.get("position") or {}
        (x, y), note = await place_card(
            client,
            str(component["parentGroupId"]),
            "processor",
            here.get("x") if x is None else x,
            here.get("y") if y is None else y,
            moving=params.processor_id,
        )
    entity = await client.update_processor(
        params.processor_id,
        version=_version(current),
        name=params.name,
        properties=params.properties,
        auto_terminated=params.auto_terminated,
        scheduling_period=params.scheduling_period,
        scheduling_strategy=params.scheduling_strategy,
        comments=params.comments,
        x=x,
        y=y,
    )
    view = entity if params.verbose else C.compact_processor(entity)
    return _with_note({"status": "ok", "processor": view}, note)


@mcp.tool(
    name="nifi_set_run_status",
    annotations=_ann(title="Start or stop a processor", read_only=False),
)
@mutation_tool
async def nifi_set_run_status(params: RunStatusIn) -> dict[str, Any]:
    """Set one processor to RUNNING, STOPPED, DISABLED, or RUN_ONCE."""
    _require_write()
    client = get_client()
    await client.authenticate()
    current = await client.get_processor(params.component_id)
    entity = await client.set_processor_run_status(params.component_id, params.state, _version(current))
    return {"status": "ok", "processor": C.compact_processor(entity)}


@mcp.tool(
    name="nifi_create_connection",
    annotations=_ann(title="Connect two components", read_only=False, idempotent=False),
)
@mutation_tool
async def nifi_create_connection(params: CreateConnectionIn) -> dict[str, Any]:
    """Connect a source to a destination. Processor sources need relationships; port sources take none.

    Wiring child groups: connect an OUTPUT_PORT (source_group_id = its group) to an INPUT_PORT
    (destination_group_id = its group) with parent_id = the group that contains both children.
    """
    _require_write()
    client = get_client()
    await client.authenticate()
    entity = await client.create_connection(
        params.parent_id,
        source_id=params.source_id,
        source_group_id=params.source_group_id,
        source_type=params.source_type,
        destination_id=params.destination_id,
        destination_group_id=params.destination_group_id,
        destination_type=params.destination_type,
        relationships=params.relationships,
        name=params.name,
        back_pressure_object_threshold=params.back_pressure_object_threshold,
        back_pressure_data_size_threshold=params.back_pressure_data_size_threshold,
        flow_file_expiration=params.flow_file_expiration,
    )
    view = entity if params.verbose else C.compact_connection(entity)
    return {"status": "ok", "connection": view}


@mcp.tool(
    name="nifi_update_connection",
    annotations=_ann(title="Change a connection's queue settings or name", read_only=False),
)
@mutation_tool
async def nifi_update_connection(params: UpdateConnectionIn) -> dict[str, Any]:
    """Change a connection's name, queue backpressure or FlowFile expiration. Sends only the fields you set.

    Works on a running flow: NiFi only checks component state when a connection's destination changes.
    """
    _require_write()
    settings = params.model_dump(include={"name", *_QUEUE_FIELDS}, exclude_none=True)
    if not settings:
        raise NiFiError(
            "Nothing to change: set name, back_pressure_object_threshold, back_pressure_data_size_threshold "
            "or flow_file_expiration."
        )
    client = get_client()
    await client.authenticate()
    current = await client.get_connection(params.connection_id)
    entity = await client.update_connection(
        params.connection_id, version=_version(current), parent_id=_parent_of(current), **settings
    )
    view = entity if params.verbose else C.compact_connection(entity)
    return {"status": "ok", "connection": view}


@mcp.tool(
    name="nifi_create_controller_service",
    annotations=_ann(title="Create controller service", read_only=False, idempotent=False),
)
@mutation_tool
async def nifi_create_controller_service(params: CreateServiceIn) -> dict[str, Any]:
    """Create a controller service in a process group and optionally enable it."""
    _require_write()
    client = get_client()
    await client.authenticate()
    entity = await client.create_controller_service(
        params.parent_id,
        params.service_type,
        params.name,
        properties=params.properties or None,
        bundle=params.bundle,
    )
    if params.enable:
        service_id = str(_entity_id(entity))
        try:
            entity = await client.set_controller_service_state(service_id, "ENABLED", _version(entity))
        except NiFiError as exc:
            # The create landed; say so, so the model does not create a second service.
            exc.args = (f"Created controller service {service_id}, but enabling it failed: {exc.args[0]}",)
            raise
    view = entity if params.verbose else C.compact_controller_service(entity)
    return {"status": "ok", "controller_service": view}


def _controller_service_view(entity: dict[str, Any], verbose: bool) -> Any:
    if verbose:
        return redact(entity)
    component = entity.get("component") or {}
    properties = component.get("properties") or {}
    view = redact(
        {
            "properties": properties,
            "descriptors": component.get("descriptors") or {},
            "validationErrors": component.get("validationErrors") or [],
        }
    )
    return {**C.compact_controller_service(entity), **({"properties": view["properties"]} if properties else {})}


@mcp.tool(
    name="nifi_get_controller_service",
    annotations=_ann(title="Controller service details"),
)
@json_tool
async def nifi_get_controller_service(params: ServiceIdIn) -> str:
    """One controller service: state, validation errors and properties (secrets redacted)."""
    client = get_client()
    await client.authenticate()
    entity = await client.get_controller_service(params.service_id)
    return _dump(_controller_service_view(entity, params.verbose))


@mcp.tool(
    name="nifi_update_controller_service",
    annotations=_ann(title="Update controller service", read_only=False),
)
@mutation_tool
async def nifi_update_controller_service(params: UpdateServiceIn) -> dict[str, Any]:
    """Change a controller service's properties or name in place. Sends only the fields you set.

    NiFi only updates a DISABLED service: disable it with nifi_set_controller_service_state (stop
    referencing processors first), update, then enable it again. Processor references stay valid.
    """
    _require_write()
    client = get_client()
    await client.authenticate()
    current = await client.get_controller_service(params.service_id)
    entity = await client.update_controller_service(
        params.service_id,
        version=_version(current),
        properties=params.properties,
        name=params.name,
    )
    return {"status": "ok", "controller_service": _controller_service_view(entity, params.verbose)}


@mcp.tool(
    name="nifi_set_controller_service_state",
    annotations=_ann(title="Enable or disable a service", read_only=False),
)
@mutation_tool
async def nifi_set_controller_service_state(params: ServiceStateIn) -> dict[str, Any]:
    """Enable or disable a controller service. Referencing processors must be stopped to disable."""
    _require_write()
    client = get_client()
    await client.authenticate()
    current = await client.get_controller_service(params.service_id)
    entity = await client.set_controller_service_state(params.service_id, params.state, _version(current))
    return {"status": "ok", "controller_service": C.compact_controller_service(entity)}


@mcp.tool(
    name="nifi_schedule_process_group",
    annotations=_ann(title="Start or stop a whole group", read_only=False),
)
@mutation_tool
async def nifi_schedule_process_group(params: ScheduleGroupIn) -> dict[str, Any]:
    """Bulk RUNNING or STOPPED for every authorized processor in a process group."""
    _require_write()
    client = get_client()
    await client.authenticate()
    result = await client.schedule_process_group(params.process_group_id, params.state)
    return {"status": "ok", "result": redact(result)}


@mcp.tool(
    name="nifi_delete_component",
    annotations=_ann(title="Delete a component", read_only=False, destructive=True),
)
@mutation_tool
async def nifi_delete_component(params: DeleteComponentIn) -> dict[str, Any]:
    """Delete a stopped processor, empty connection, disabled service, stopped input or output port
    with no connections, empty process group, or parameter context.

    A parameter context can only be deleted once no process group is bound to it. The result
    confirms the id, kind and the revision NiFi deleted; it does not repeat the deleted entity.
    """
    _require_write()
    client = get_client()
    await client.authenticate()
    kind = params.kind
    cid = params.component_id
    if kind == "processor":
        current = await client.get_processor(cid)
        result = await client.delete_processor(cid, _version(current))
    elif kind == "connection":
        current = await client.get_connection(cid)
        result = await client.delete_connection(cid, _version(current), parent_id=_parent_of(current))
    elif kind == "controller_service":
        current = await client.get_controller_service(cid)
        result = await client.delete_controller_service(cid, _version(current))
    elif kind in {"input_port", "output_port"}:
        port_kind = kind.upper()
        current = await client.get_port(cid, port_kind)
        result = await client.delete_port(cid, _version(current), port_kind, parent_id=_parent_of(current))
    elif kind == "parameter_context":
        current = await client.get_parameter_context(cid)
        result = await client.delete_parameter_context(cid, _version(current))
    else:
        current = await client.get_process_group(cid)
        result = await client.delete_process_group(cid, _version(current))
    # NiFi answers with the deleted entity; it is not returned, so a value it held (an invalid
    # property, a parameter) is not repeated. The revision is enough to confirm the delete.
    return {"status": "ok", "deleted": cid, "kind": kind, "revision": _version(result) if result else None}


@mcp.tool(
    name="nifi_export_flow",
    annotations=_ann(title="Download flow definition JSON"),
)
@json_tool
async def nifi_export_flow(params: ExportFlowIn) -> str:
    """Download a process group as a versioned flow snapshot (same JSON the UI exports).

    A property, run schedule or network interface NiFi reports invalid in the live group is masked in
    the component it is invalid on, as on every other read.
    """
    client = get_client()
    await client.authenticate()
    snapshot = await client.download_flow(params.process_group_id, include_services=params.include_services)
    invalid = await client.invalid_values(params.process_group_id, include_ancestors=params.include_services)
    return _dump(redact(mask_components(snapshot, invalid)))


@mcp.tool(
    name="nifi_import_flow",
    annotations=_ann(title="Upload flow definition", read_only=False, idempotent=False),
)
@mutation_tool
async def nifi_import_flow(params: ImportFlowIn) -> dict[str, Any]:
    """Import a versioned flow snapshot as a new child process group.

    Omit x/y to take the next free process group cell; an x/y on another card is shifted clear of it.
    """
    _require_write()
    client = get_client()
    await client.authenticate()
    (x, y), note = await place_card(client, params.parent_id, "process_group", params.x, params.y)
    entity = await client.import_flow(params.parent_id, params.snapshot, params.group_name, x=x, y=y)
    view = entity if params.verbose else C.compact_process_group(entity)
    return _with_note({"status": "ok", "process_group": view}, note)


@mcp.tool(
    name="nifi_replace_flow",
    annotations=_ann(title="Replace process group contents", read_only=False, destructive=True, idempotent=False),
)
@mutation_tool
async def nifi_replace_flow(params: ReplaceFlowIn) -> dict[str, Any]:
    """Overwrite an existing process group with a flow snapshot. Destructive. Prefer a sandbox PG."""
    _require_write()
    client = get_client()
    await client.authenticate()
    result = await client.replace_flow(params.process_group_id, params.snapshot)
    return {"status": "ok", "result": redact(result)}


@mcp.tool(
    name="nifi_apply_flow_spec",
    annotations=_ann(title="Build a flow from a JSON spec", read_only=False, idempotent=False),
)
@mutation_tool
async def nifi_apply_flow_spec(params: ApplySpecIn) -> dict[str, Any]:
    """Create a complete flow from a declarative spec: process group, services, processors, ports, connections.

    Property values starting with @ (processor or controller_service properties) are resolved to a
    controller service created earlier in the same spec. Services are enabled in spec order.
    JSON booleans and numbers become NiFi text (true -> "true", 10 -> "10"); null unsets a property.
    auto_terminated and relationships take a list or a single name.
    Connections refer to components by name. Prefer this over many create_* calls.

    Every result, ok or error, has outcome (applied, not_applied or unknown: the failing request's)
    and lists created[] (kind, id, ref, outcome; services carry their state). ref is the item's
    place in the spec (process_group, objects[2], connections[0]). An ok result adds each item's
    name and name_map; an error names items by ref only, and never repeats a value from the spec.
    An error, or a build whose flow view could not be read, adds a hint saying how to
    roll back or inspect exactly what this call created. cause "spec" means the spec was refused
    before NiFi was asked (a missing name or type, an unknown connection end or @service, or an
    unknown key at the top level or in process_group): nothing was created. Top-level keys:
    process_group, objects, connections, parent_process_group_id, layout. process_group keys: name,
    comments, parameter_context_id, inherit_parameter_context, x, y, position. A request with no
    definite answer (a timeout, a 5xx, a lost connection) is in created[] with outcome "unknown":
    NiFi may have applied it, and the hint says what the server's read-back found. Check again
    before retrying. Only created[] items with an id are ever named for deletion.
    """
    _require_write()
    client = get_client()
    await client.authenticate()
    result = await apply_flow_spec(
        client, params.spec, parent_process_group_id=params.parent_process_group_id, renderer=Renderer(params)
    )
    if result.get("flow") and not isinstance(result["flow"], str):
        result["flow"] = C.compact_flow(result["flow"])
    return redact(result)


@mcp.tool(
    name="nifi_layout_process_group",
    annotations=_ann(title="Auto-layout a process group", read_only=False),
)
@mutation_tool
async def nifi_layout_process_group(params: LayoutGroupIn) -> dict[str, Any]:
    """Lay processors out top-down, forks sideways, and stack child process groups top-down.

    Processors: 240px rows (tallest card + label + 32px). At a fork the main branch continues down and
    the others sit on the fork's row, 672px out; a fork of leaves spreads one row down, 512px apart.
    Joins return to the fork's axis; every card is centred on its axis by its own width.
    A self-loop sits outside its card's side with its label on the outer stretch. The second of two
    connections between one pair, a retry line back up, and any line or label that would cross a card
    or another label are routed: out of the source's side (else its top or bottom), along a free lane
    between card columns, into the target's side (else its top or bottom), never along another line.
    Child process groups: 288px rows, 424px columns, one per row in flow order, upstream above downstream.
    """
    _require_write()
    client = get_client()
    await client.authenticate()
    result = await relayout_process_group(client, params.process_group_id)
    return result


@mcp.tool(
    name="nifi_empty_queue",
    annotations=_ann(title="Drop a connection queue", read_only=False, destructive=True),
)
@mutation_tool
async def nifi_empty_queue(params: QueueIn) -> dict[str, Any]:
    """Drop every FlowFile on a connection. Data loss. Required before deleting a non-empty connection."""
    _require_write()
    client = get_client()
    await client.authenticate()
    return {"status": "ok", "result": redact(await client.empty_queue(params.connection_id))}


@mcp.tool(
    name="nifi_list_parameter_contexts",
    annotations=_ann(title="Parameter contexts"),
)
@json_tool
async def nifi_list_parameter_contexts() -> str:
    """List parameter contexts. Sensitive parameter values are redacted."""
    client = get_client()
    await client.authenticate()
    return _dump(redact(await client.list_parameter_contexts()))


def _parameter_context_view(entity: dict[str, Any], verbose: bool) -> Any:
    return redact(entity) if verbose else C.compact_parameter_context(entity)


def _as_dicts(parameters: list[ParameterIn]) -> list[dict[str, Any]]:
    return [item.model_dump() for item in parameters]


@mcp.tool(
    name="nifi_get_parameter_context",
    annotations=_ann(title="Parameter context details"),
)
@json_tool
async def nifi_get_parameter_context(params: ParameterContextIdIn) -> str:
    """One parameter context: parameters (sensitive values redacted) and bound process groups."""
    client = get_client()
    await client.authenticate()
    entity = await client.get_parameter_context(params.parameter_context_id)
    return _dump(_parameter_context_view(entity, params.verbose))


@mcp.tool(
    name="nifi_create_parameter_context",
    annotations=_ann(title="Create parameter context", read_only=False, idempotent=False),
)
@mutation_tool
async def nifi_create_parameter_context(params: CreateParameterContextIn) -> dict[str, Any]:
    """Create a parameter context with parameters. Bind it to a group with nifi_bind_parameter_context."""
    _require_write()
    client = get_client()
    await client.authenticate()
    entity = await client.create_parameter_context(
        params.name, parameters=_as_dicts(params.parameters), description=params.description
    )
    return {"status": "ok", "parameter_context": _parameter_context_view(entity, params.verbose)}


@mcp.tool(
    name="nifi_update_parameter_context",
    annotations=_ann(title="Update parameter context", read_only=False),
)
@mutation_tool
async def nifi_update_parameter_context(params: UpdateParameterContextIn) -> dict[str, Any]:
    """Add, change, or remove parameters. NiFi restarts referencing components itself.

    A name cannot be in both parameters and remove. To change whether an existing parameter is
    sensitive, remove it in one call and add it back with the new flag and value in the next.
    """
    _require_write()
    client = get_client()
    await client.authenticate()
    entity = await client.update_parameter_context(
        params.parameter_context_id,
        parameters=_as_dicts(params.parameters),
        remove=params.remove,
        name=params.name,
        description=params.description,
    )
    return {"status": "ok", "parameter_context": _parameter_context_view(entity, params.verbose)}


@mcp.tool(
    name="nifi_bind_parameter_context",
    annotations=_ann(title="Bind parameter context to a group", read_only=False),
)
@mutation_tool
async def nifi_bind_parameter_context(params: BindParameterContextIn) -> dict[str, Any]:
    """Bind a parameter context to a process group so #{name} references resolve. null unbinds.

    NiFi does not inherit contexts, so by default this is the UI's "Apply recursively": one request
    with processGroupUpdateStrategy=ALL_DESCENDANTS binds the group and every descendant, replacing
    any context a descendant had. NiFi checks every group before changing any, so an error means
    no group was rebound. apply_recursively=false binds only this group.
    """
    _require_write()
    client = get_client()
    await client.authenticate()
    entity = await bind_parameter_context(
        client, params.process_group_id, params.parameter_context_id, recursive=params.apply_recursively
    )
    view = redact(entity) if params.verbose else C.compact_process_group(entity)
    applied_to = "ALL_DESCENDANTS" if params.apply_recursively else "THIS_GROUP"
    return {"status": "ok", "process_group": view, "applied_to": applied_to}


@mcp.prompt()
def nifi_flow_builder() -> str:
    """How to go from a natural-language pipeline request to a running NiFi 2 flow."""
    return """
Build the flow in this order:
1. nifi_about: confirm 2.x and write access.
2. nifi_get_flow on root: pick a sandbox parent, never dump onto root.
3. nifi_list_processor_types / nifi_list_controller_service_types for the processors you need.
4. nifi_get_processor_definition for each type. Use the returned property names.
5. Prefer one nifi_apply_flow_spec call:
   - process_group: {name, parameter_context_id?}
   - objects: controller_service, processor, connection (source/target by name, relationships list)
     connection queue limits: back_pressure_object_threshold, back_pressure_data_size_threshold,
     flow_file_expiration (change them later with nifi_update_connection)
   - properties that need a service: "@ServiceName"
   - auto_terminated: relationships you will not connect
   - scheduling_period / scheduling_strategy on a processor, e.g. "10 sec"
   - omit x/y; auto-layout is top-to-bottom (240px rows) so arrows stay visible
   If properties use #{param}, first nifi_create_parameter_context and pass its id as
   process_group.parameter_context_id (or nifi_bind_parameter_context afterwards).
6. nifi_get_health on the new group. Fix INVALID processors with nifi_update_processor and
   INVALID controller services with nifi_get_controller_service / nifi_update_controller_service.
7. nifi_schedule_process_group state=RUNNING only after validation is clean.
8. If it fails: nifi_get_bulletins, nifi_list_queue, nifi_get_processor.
Do not use NiFi 1.x templates. They are gone in 2.x. Use flow JSON (export/import/replace) instead.
""".strip()


@mcp.prompt()
def nifi_debug_flow() -> str:
    """Debug a broken or stalled process group."""
    return """
1. nifi_get_health: INVALID processors and queued connections first.
2. nifi_get_bulletins: ERROR/WARNING messages.
3. For each INVALID processor: nifi_get_processor, then nifi_get_processor_definition.
4. Queues that never drain: nifi_list_queue, then inspect the downstream processor state.
5. NiFiConflictError (HTTP 409) on delete: empty the queue (nifi_empty_queue) and stop the processor first.
   A 409 whose hint names a field is a refused value: fix that value instead.
6. Do not start processors that still report validation errors; NiFi will refuse or no-op.
""".strip()


@mcp.prompt()
def nifi_best_practices() -> str:
    """NiFi 2 flow-building practices for this MCP."""
    return """
- One process group per pipeline. Name it after the data path, not "Flow Pipeline".
- Parameter contexts for anything that changes by environment.
  Do not hard-code passwords; NiFi stores them as sensitive properties.
- Controller services at the process-group level that uses them, not on root, unless several groups must share one.
- Auto-terminate unused relationships. A dangling "failure" relationship keeps the processor INVALID.
- Prefer record-oriented processors (JsonTreeReader / JsonRecordSetWriter) over splitting text by hand.
- Layout: nifi_apply_flow_spec auto-places processors top-to-bottom (240px rows, side branches 672px out).
  Never 280px horizontally (cards are 420 wide).
  Child process groups stack top-down on a 424x288 lattice (112px row gap for the connection label).
  If cards or arrows overlap, nifi_layout_process_group.
  spec layout=manual only if you set x/y.
- If your deployment reconciles versioned process groups from a registry, prototype on an unversioned group.
""".strip()


def configure_http(settings: Settings) -> None:
    """Configure the registered tools as an authenticated, stateless resource server."""
    from mcp.server.auth.settings import AuthSettings
    from mcp.server.transport_security import TransportSecuritySettings

    # Load and validate the dedicated NiFi credential once before accepting callers.
    settings.proxy_tls_context  # noqa: B018 - cached TLS context initialization
    mcp.settings.host = settings.host
    mcp.settings.port = settings.port
    mcp.settings.stateless_http = True
    public_url = urlsplit(settings.oauth_resource_url or "")
    mcp.settings.transport_security = TransportSecuritySettings(
        allowed_hosts=[public_url.netloc, "127.0.0.1:*", "localhost:*", "[::1]:*"],
        allowed_origins=[f"{public_url.scheme}://{public_url.netloc}"],
    )
    mcp.settings.auth = AuthSettings(
        issuer_url=settings.oauth_issuer_url,
        resource_server_url=settings.oauth_resource_url,
        required_scopes=settings.oauth_scopes,
        validate_token_resource=True,
    )
    mcp._token_verifier = UserTokenVerifier(settings)


def main() -> None:
    try:
        settings = get_settings()
    except ValidationError as exc:
        LOG.error("Invalid configuration: %s", safe_error_message(exc))
        raise SystemExit(2) from None
    if settings.transport == "streamable-http":
        try:
            configure_http(settings)
        except (OSError, ssl.SSLError, ValueError):
            LOG.error("Cannot configure HTTP authentication; check OAuth URLs, CA and proxy certificate/key")
            raise SystemExit(2) from None
    mcp.run(transport=settings.transport)


if __name__ == "__main__":
    main()
