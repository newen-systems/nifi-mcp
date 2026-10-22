"""FastMCP server: natural-language NiFi 2.x flow design."""

from __future__ import annotations

import inspect
import json
import logging
import ssl
import sys
from collections.abc import Awaitable, Callable
from contextvars import ContextVar
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
from nifi_mcp.design import DESIGN_METHOD, review_graph, review_spec
from nifi_mcp.errors import NiFiError, NiFiReadOnlyError, Outcome, safe_error_message
from nifi_mcp.flow_spec import (
    apply_flow_spec,
    bind_parameter_context,
    normalise_spec,
    parent_parameter_context,
    persist_layout_intent,
    place_card,
    place_card_why,
    read_layout_outline,
    relayout_process_group,
)
from nifi_mcp.ordering import LayoutIntent
from nifi_mcp.redaction import KNOWN, mask_components, redact
from nifi_mcp.registry import commit_process_group, version_state
from nifi_mcp.render import Renderer
from nifi_mcp.user_auth import CALLER_CLIENT, UserTokenVerifier, caller_client
from nifi_mcp.validation import TOUCHED, layout_issues, touch, validate_after_action

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
   A loop (retry, redelivery) keeps the default Back Pressure Object Threshold (10000) on every connection in it:
   NiFi does not schedule a processor whose outgoing connection is full, so small limits all round a loop deadlock it.
4. Controller services must be ENABLED before processors that reference them will
   validate. In a flow spec, set a property to @ServiceName.
5. Mutations are revision-locked and lifecycle-sensitive writes have bounded dependency preflight.
   A NiFiConflictError (HTTP 409 or local preflight) needs diagnosis: re-read the target and identify
   revision, state, queue or dependency blockers. Quiesce only the required components; wait for active
   tasks and service transitions to finish. For deletion, stop production, drain queues, then stop consumers.
   Never drop data outside the authorized scope, stop unrelated flow, or blindly retry. Within a user-authorized
   disposable test reset, empty the selected queues with nifi_empty_queue; do not ask again for each selected queue.
   Cluster-selection requirements, read-only mode and NiFi access policies still apply. An unknown outcome requires
   read-back before deciding whether another mutation is needed. A hint naming a refused field
   (scheduling_period, property 'Password', ...) means fix that value; nothing needs stopping.
6. Default output is compact. Pass verbose=true only when you need full entities.
7. External automation may revert VERSIONED process groups.
   Prototype on an unversioned sandbox PG.
8. Writes are on unless NIFI_READONLY=true. Do not ask the user to paste tokens;
   they live in env. Prototype on an unversioned sandbox process group.
9. Every canvas, root included, expresses flow order top-to-bottom like Mermaid TD.
   Within a related flow cluster, vertical position means progression and horizontal position means parallel work.
   Only parent navigation canvases of child process groups use a 16:9 grid for unrelated group clusters.
   Boundary ports may accompany those groups. Ordinary processor, mixed-component and connected flows
   stay top-down with no aspect-ratio target, even when they contain disconnected or independent stages.
   Keep each related group cluster internally top-down; navigation grid rows imply no execution order.
   One ordering graph covers processors, groups, ports, funnels and remote endpoints. Actual connections
   take precedence, then declared dependencies/stages, then documented domain roles. Names alone do not
   establish ambiguous order. Actual and logical dependencies define clusters before packing. Grids and
   alphabetical displays can organise unrelated work but never override known dependencies. Neutral block
   placement uses stable component IDs; grid rows and columns outside clusters imply no dependencies.
   Every forward destination belongs below its source with clearance for cards, queues and labels.
   Chains share a centred axis; fork branches begin below the fork, spread horizontally, and joins sit
   below all prerequisites towards the main axis. Only identified feedback edges may return upward.
   Input ports occupy their owning canvas's top boundary; output ports its bottom boundary.
   Child-group port direction is relative to that child: its output feeds later stages on the parent canvas.
   Trace actual child input-to-output port paths before treating a group-card loop as feedback. A group
   may send early and receive later through unrelated ports: route the later visit back separately.
   Parent port paths also order otherwise disconnected child chains. Unreadable paths remain unknown.
   nifi_layout_process_group accepts layout_intent with dependencies, stages, feedback, areas, domain and roles.
   Intent persists in the owning group's comments and is used by subsequent automatic repair.
   Explicit independent areas remain internally top-down. Unknown order uses neutral block packing and
   reports uncertainty. Inferred layout dependencies never create executable NiFi connections.
   Coordinate fan-in routes and labels, preserving branch order into the join. Prefer queue labels in
   clear space beneath their source processors; reserve enough room instead of detouring left across
   another incoming branch. Shared stretches, card/label collisions and lines through labels are errors.
   Minimize proper line-line crossings after clearing those errors; some crossings may remain in a neat,
   structured layout. Report them as connection_crossing advisories, not proof of unavoidability.
   Connections may meet at a common endpoint; intersections elsewhere are still reported.
   The server re-runs layout after changes (rule 12); report remaining conflicts and violations accurately.
10. Parameter ownership is root.service.key[.key], lowercase dot-separated segments: gitlab.pypi.username
    belongs in context gitlab. Group all gitlab.* parameters there, with service as the second segment.
    List/reuse root contexts; never create random aliases, flatten names or copy values across contexts.
    A flow needing multiple roots binds a composed context inheriting the root contexts via
    inherited_context_ids. Credentials are sensitive; references use the full #{gitlab.pypi.username}.
    Do not silently rename legacy parameters or references. #{name} references need a context bound to the group:
   nifi_create_parameter_context, then nifi_bind_parameter_context (or pass
   parameter_context_id when creating the group). New child groups inherit the
   parent's context; binding applies to every descendant group in one request. Delete it with
   nifi_delete_component kind=parameter_context after the group is gone.
11. Every tool that changes NiFi returns outcome: applied, not_applied or unknown.
   unknown means NiFi may have applied it: the hint says which read shows the state and what
   that read showed. Run it again before retrying; a retried create can make a duplicate.
12. After every applied change the server reads the changed process group and every group below
   it, and returns validation: {ok, checked_groups, issues, layout_repaired}. Each issue names its
   group, component, kind and message. The layout of every group the change touched is run again
   every time, whether or not anything is flagged: creates, updates, moves, connections and
   deletes (a removal leaves a gap and bends) all end tidy, and the root canvas is laid out like
   any other group. layout_repaired lists the groups laid out again. A versioned group is laid out
   too: layout_needs_registry_commit lists those now differing from their registry version, which
   need a new registry version (nifi_commit_process_group) or flow deployment can revert them. Ordering violations
   flag insufficient forward clearance; only identified feedback is exempt.
   NIFI_MCP_AUTO_LAYOUT=false turns the repair off.
   Never leave the canvas for later: if you change NiFi outside this server (raw REST, a script,
   the UI), call nifi_layout_process_group on every group you touched, root included (its default),
   as the last step; the server cannot see those changes. Validity issues (invalid_processor,
   invalid_service, unterminated_relationship, dangling_connection, disabled_service) are never
   fixed for you: fix each one before the next step. ok false means act on issues. ok null means
   the check could not run (see error); the change itself still stands, read the group yourself.
   Saved bends/labelIndex are checked too: overlapping_connections lists the exact unbent
   connection IDs (including opposite directions); connection_geometry reports shared line
   segments, label collisions and card crossings as errors. Proper line-line crossings are reported
   separately in layout_advisories as connection_crossing with severity advisory and do not alone make
   validation.ok false. A common endpoint is allowed; intersections elsewhere are still reported.
   Minimize crossings while keeping the layout structured, and report remaining hard geometry errors.
   validation.design_review contains advisory topology findings, separate from validity/layout ok.
   Resolve or justify these and complete the input-contract review before starting the flow.
   validation never changes outcome.
13. Use PascalCase for processor and process-group display names (PlanRegistryTransfer,
   SplitJobs, JobToAttributes); camelCase is also acceptable. Preserve explicit names requested
   by the user, such as ImageTransfer. Relationship names and property keys keep their NiFi-defined case.
14. Leave a connection unnamed: NiFi shows its selected relationships on the canvas (a port source
   shows the port). Give a name only when it says something the relationships do not, and then it
   must not contradict them. nifi_review_flow_design flags a set name that does.
15. Name relationships and queues for what they represent. First decide whether the identifier is
   an action, entity, state, outcome, condition or event, then use the form that fits: nouns for
   entities, verbs for actions, adjectives or state descriptors for conditions, outcome terms for
   results. Take the word from the domain and lifecycle stage. A state or outcome names the
   condition where it is evaluated, not the next operation or destination. Keep distinctions: an
   intermediate state is not a terminal outcome, a recoverable condition is not a permanent
   failure. Identifiers in equivalent roles share a grammatical form; different concepts do not
   share one word. No fixed list of names. NiFi's built-in names keep their meaning and are never
   reused for something else: relationships the framework or standard processors define (success,
   failure, original; InvokeHTTP's Response, Retry, No Retry, Failure, Original; RetryFlowFile's
   retry, retries_exceeded; a Python FlowFileTransform always gets failure and original) and core
   FlowFile attributes (uuid, filename, path, absolute.path, mime.type, priority). Never use them
   as a custom processor's own names. Renaming a live processor's relationships breaks its flows,
   so ask the owner first. A queue is left unnamed so NiFi shows its relationships
   (rule 14). Read each relationship's description in nifi_get_processor_definition to judge it.
16. A terminal or parking queue (hold, dead-letter, stale, a failure sink the operator inspects before
   discarding) ends at a LogMessage processor (org.apache.nifi.processors.standard.LogMessage),
   never UpdateAttribute. Leave it stopped or disabled so FlowFiles wait in its input queue; when
   enabled it logs each FlowFile and its success relationship is auto-terminated. Properties:
   Log Level, Log Prefix, Log Message.
17. Every tool takes params.cluster, an exact name from nifi_list_clusters. With more than one
   cluster every call names its cluster, reads included; there is no default. A change on a protected
   cluster also needs params.confirm_cluster set to the same name, or it is refused and nothing is
   sent. Every result names the cluster it acted on. Component ids belong to one cluster: pass the
   cluster you read them from.
""".strip() + "\n\nRequired design methodology:\n" + DESIGN_METHOD



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

_clients: dict[str, NiFiClient] = {}
_clusters: dict[str, Settings] | None = None
# The cluster the running tool call addresses; unset means the first.
CLUSTER: ContextVar[str | None] = ContextVar("nifi_cluster", default=None)


def configure(client: NiFiClient, settings: Settings) -> None:
    """Tests inject a mock client here: it serves the first cluster of settings."""
    global _clusters
    _clusters = settings.cluster_settings()
    _clients.clear()
    _clients[settings.cluster_name] = client


def clusters() -> dict[str, Settings]:
    """Every configured cluster by name, the default first."""
    global _clusters
    if _clusters is None:
        _clusters = Settings().cluster_settings()  # type: ignore[call-arg]
    return _clusters


def get_settings() -> Settings:
    """The settings of the cluster the running tool call addresses."""
    configured = clusters()
    return configured.get(CLUSTER.get() or "") or next(iter(configured.values()))


def get_client() -> NiFiClient:
    settings = get_settings()
    if settings.auth == "passthrough":
        client = CALLER_CLIENT.get()
        if client is None:
            raise NiFiError("A verified caller is required; shared credentials are disabled")
        return client
    if settings.cluster_name not in _clients:
        _clients[settings.cluster_name] = NiFiClient(settings)
    return _clients[settings.cluster_name]


def _enter_cluster(args: tuple[Any, ...], kwargs: dict[str, Any], *, mutating: bool) -> str:
    """Select the call's cluster and make it the one get_settings() and get_client() resolve to."""
    selected = _select_cluster(*_requested_cluster(args, kwargs), mutating=mutating)
    CLUSTER.set(selected)
    return selected


def _cluster_names() -> list[str]:
    """The configured cluster names, which the server publishes (nifi_list_clusters) and may repeat."""
    try:
        return list(clusters())
    except ValueError:
        return []


def _requested_cluster(args: tuple[Any, ...], kwargs: dict[str, Any]) -> tuple[str | None, str | None]:
    for value in (*args, *kwargs.values()):
        if isinstance(value, ClusterIn):
            return value.cluster, value.confirm_cluster
    return None, None


def _select_cluster(requested: str | None, confirm: str | None, *, mutating: bool) -> str:
    """The cluster a call addresses, by exact name. With several configured every call names one, and a
    change on a protected cluster names it twice."""
    configured = clusters()
    names = ", ".join(configured)
    if requested is None and len(configured) > 1:
        raise NiFiError(f"This server reaches clusters {names}. Name one in params.cluster; nothing was sent.")
    name = next(iter(configured)) if requested is None else requested
    if name not in configured:
        raise NiFiError(f"Unknown cluster. Valid names: {names} (exact match). Nothing was sent.")
    if confirm is not None and confirm != name:
        raise NiFiError(f"params.confirm_cluster does not match cluster {name}. Nothing was sent.")
    if mutating and configured[name].is_protected and confirm != name:
        raise NiFiError(
            f"Cluster {name} is protected: a change needs params.confirm_cluster=\"{name}\" as well as "
            "params.cluster. Nothing was sent."
        )
    return name


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
    """Every tool result passes through here, so redaction cannot be skipped by a verbose path.
    Inside a tool call it also names the cluster the call addressed."""
    cluster = CLUSTER.get()
    if cluster is not None and isinstance(payload, dict) and "cluster" not in payload:
        payload = {"cluster": cluster, **payload}
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


def _finish(result: ToolResult, cluster: str | None, *, mutating: bool) -> str:
    """A tool's JSON text: a dict result gains the cluster it acted on, and a mutation's its outcome."""
    if not isinstance(result, dict):
        return result
    if mutating:
        result.setdefault("outcome", str(Outcome.APPLIED))
    return _dump(result if cluster is None else {"cluster": cluster, **result})


def _tool[**P](fn: Callable[P, Awaitable[ToolResult]], *, mutating: bool) -> Callable[P, Awaitable[str]]:
    """MCP tools return JSON error objects instead of raising into the host.

    A mutating tool's result always carries outcome (applied, not_applied or unknown): an error takes
    it from the request that failed, a result from this call's ledger of requests."""

    hints = get_type_hints(fn)
    # nifi_list_clusters addresses no cluster; every other tool takes ClusterIn params.
    addresses_cluster = any(isinstance(hint, type) and issubclass(hint, ClusterIn) for hint in hints.values())

    @wraps(fn)
    async def wrapper(*args: P.args, **kwargs: P.kwargs) -> str:
        ledger: list[dict[str, str]] = []
        token = LEDGER.set(ledger)
        known = KNOWN.set(set())
        touched: dict[str, bool] = {}
        touch_token = TOUCHED.set(touched)
        cluster_token = CLUSTER.set(None)
        selected: str | None = None
        try:
            selected = _enter_cluster(args, kwargs, mutating=mutating) if addresses_cluster else None
            async with caller_client(get_settings()):
                result = await fn(*args, **kwargs)
                if mutating and isinstance(result, dict) and result.get("outcome", Outcome.APPLIED) == Outcome.APPLIED:
                    result["validation"] = await validate_after_action(
                        get_client(), touched, auto_layout=get_settings().mcp_auto_layout
                    )
        except Exception as exc:  # noqa: BLE001 - boundary: never raise to the MCP host
            return _tool_error(exc, Renderer(args, kwargs, public=_cluster_names()), mutating=mutating, ledger=ledger)
        finally:
            CLUSTER.reset(cluster_token)
            TOUCHED.reset(touch_token)
            KNOWN.reset(known)
            LEDGER.reset(token)
        return _finish(result, selected, mutating=mutating)

    # FastMCP reads the return type for the output schema: the host always gets the JSON text.
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


class ClusterIn(ToolIn):
    cluster: str | None = Field(
        default=None,
        description="Exact cluster name from nifi_list_clusters. Required when more than one is configured.",
    )
    confirm_cluster: str | None = Field(
        default=None,
        description="The same name again, required for a change on a protected cluster",
    )


# The params of a tool that takes nothing else.
NO_CLUSTER = ClusterIn()


class VerboseIn(ClusterIn):
    verbose: bool = Field(default=False, description="Return full redacted JSON instead of the compact view")


class ProcessGroupIn(VerboseIn):
    process_group_id: NiFiId = Field(
        default="root",
        description="Process group id, or 'root' for the canvas root",
        min_length=1,
    )


class ReviewDesignIn(ProcessGroupIn):
    spec: dict[str, Any] | None = Field(default=None, description="Optional proposed flow spec; never applied")
    required_fields: list[str] = Field(default_factory=list, description="Required ingress field names")
    supplied_fields: list[str] = Field(default_factory=list, description="Field names the producer supplies; no values")
    runtime_fields: list[str] = Field(default_factory=list, description="Fields requiring justified runtime discovery")


class SearchIn(VerboseIn):
    query: str = Field(..., min_length=1, max_length=200, description="NiFi search string")


class ProcessorTypesIn(VerboseIn):
    type_filter: str | None = Field(
        default=None,
        description="Case-insensitive substring of the class name, e.g. 'GenerateFlowFile' or 'record'",
    )
    limit: int = Field(default=25, ge=1, le=100)


class ProcessorDefinitionIn(ClusterIn):
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
    name: str = Field(..., min_length=1, description="Lowercase root.service.key[.key], e.g. gitlab.pypi.username")
    value: str | None = None
    sensitive: bool | None = Field(
        default=None,
        description="Store as a sensitive parameter; NiFi masks it on read. Omit to keep the current flag "
        "(new parameters are not sensitive). NiFi cannot change an existing parameter's flag in place: "
        "remove the parameter in one update, then add it with the new flag and value in a second.",
    )
    description: str | None = None


class CreateParameterContextIn(VerboseIn):
    inherited_context_ids: list[NiFiId] | None = Field(
        default=None, description="Root contexts to inherit for a composed flow context; never copy their values"
    )
    name: str = Field(..., min_length=1, max_length=200)
    description: str | None = None
    parameters: list[ParameterIn] = Field(default_factory=list)


class UpdateParameterContextIn(VerboseIn):
    inherited_context_ids: list[NiFiId] | None = Field(
        default=None, description="Replace inherited contexts; omit to preserve, [] to clear"
    )
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


class LayoutGroupIn(ClusterIn):
    process_group_id: NiFiId = "root"
    layout_intent: LayoutIntent | None = None


def _nonblank(value: str) -> str:
    if not value.strip():
        raise ValueError("must say what changed")
    return value.strip()


class CommitGroupIn(ClusterIn):
    process_group_id: NiFiId
    comment: Annotated[str, Field(min_length=1, max_length=500), AfterValidator(_nonblank)]


class RunStatusIn(ClusterIn):
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


class ServiceStateIn(ClusterIn):
    service_id: NiFiId
    state: Literal["ENABLED", "DISABLED"]


class ScheduleGroupIn(ClusterIn):
    process_group_id: NiFiId
    state: Literal["RUNNING", "STOPPED"]


class DeleteComponentIn(ClusterIn):
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


class ExportFlowIn(ClusterIn):
    process_group_id: NiFiId
    include_services: bool = False


class ImportFlowIn(VerboseIn):
    parent_id: NiFiId
    group_name: str
    snapshot: dict[str, Any]
    x: float | None = Field(default=None, description="Omit x and y to take the next free process group cell")
    y: float | None = None


class ReplaceFlowIn(ClusterIn):
    process_group_id: NiFiId
    snapshot: dict[str, Any]


class ApplySpecIn(ClusterIn):
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


class QueueIn(ClusterIn):
    connection_id: NiFiId


class ListQueueIn(QueueIn):
    limit: int = Field(default=100, ge=1, le=10000, description="Maximum FlowFile summaries returned")
    timeout_seconds: float = Field(default=15.0, gt=0, le=120)
    include_attributes: bool = Field(default=False, description="Read each returned FlowFile's metadata and attributes")


class EmptyQueueIn(QueueIn):
    timeout_seconds: float = Field(default=30.0, gt=0, le=120)


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
    name="nifi_list_clusters",
    annotations=_ann(title="Configured NiFi clusters"),
)
@json_tool
async def nifi_list_clusters() -> dict[str, Any]:
    """The NiFi clusters this server reaches. Pass a name as params.cluster to any other tool, and again
    as params.confirm_cluster for a change on a protected one."""
    configured = clusters()
    return {
        "clusters": [
            {
                "name": name,
                "environment": cfg.environment,
                "protected": cfg.is_protected,
                "ui_url": cfg.ui_url,
                "api_url": cfg.api_url,
                "kube_cluster": cfg.kube_cluster,
                "namespace": cfg.namespace,
                "readonly": cfg.readonly,
            }
            for name, cfg in configured.items()
        ],
        "cluster_required": len(configured) > 1,
    }


@mcp.tool(
    name="nifi_about",
    annotations=_ann(title="NiFi version"),
)
@json_tool
async def nifi_about(params: ClusterIn = NO_CLUSTER) -> str:
    """NiFi version, build, and whether this is 2.x. Call first on a new session."""
    client = get_client()
    await client.authenticate()
    about = await client.about()
    major, minor, patch = NiFiClient.version_tuple(about)
    return _dump(
        {
            "version": f"{major}.{minor}.{patch}",
            "is_nifi_2x": major >= 2,
            "cluster": get_settings().cluster_name,
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
async def nifi_current_user(params: ClusterIn = NO_CLUSTER) -> str:
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
async def nifi_list_queue(params: ListQueueIn) -> str:
    """List queued FlowFiles without consuming or dropping them, including UUID, filename, size and node.

    NiFi returns a sample, not a paginated dump of the entire queue. limit bounds the returned sample.
    include_attributes reads metadata for each returned UUID using its clusterNodeId; this does not
    download payload bytes. Moving FlowFiles may disappear before inspection; retry after stopping
    the upstream/downstream processors if a stable diagnostic snapshot is needed.
    Requires NiFi read-source-data permission. Works with NIFI_READONLY=true.
    """
    client = get_client()
    await client.authenticate()
    data = await client.list_queue(params.connection_id, timeout_seconds=params.timeout_seconds)
    listing = client._envelope(data, "listingRequest", "request")
    summaries = listing.get("flowFileSummaries") or []
    listing["returnedCount"] = min(len(summaries), params.limit)
    listing["sampleTruncated"] = len(summaries) > params.limit
    listing["flowFileSummaries"] = summaries[: params.limit]
    if params.include_attributes:
        for summary in listing["flowFileSummaries"]:
            summary["details"] = await client.get_queued_flowfile(
                params.connection_id, summary["uuid"], cluster_node_id=summary.get("clusterNodeId")
            )
    return _dump(data)


def _with_note(
    payload: dict[str, Any], note: str | None, placement: dict[str, Any] | None = None
) -> dict[str, Any]:
    if note:
        payload["warnings"] = [note]
    if placement and placement.get("reason"):
        payload["placement"] = placement
    return payload


@mcp.tool(
    name="nifi_review_flow_design",
    annotations=_ann(title="Review input contract and flow efficiency", read_only=True),
)
@json_tool
async def nifi_review_flow_design(params: ReviewDesignIn) -> dict[str, Any]:
    """Required before designing/extending a flow, on a proposed spec, and again after creation.

    Reads the current canvas; checks declared ingress fields and flags consolidation/fan-out candidates.
    Returns the contract-first methodology and review obligations. No writes. Findings are advisory:
    document resolutions before building/starting; this cannot certify throughput or producer behavior.
    Supply field names only, never payloads or credentials. runtime_fields require a stated justification.
    """
    client = get_client()
    await client.authenticate()
    outline = await read_layout_outline(client, params.process_group_id)
    missing = sorted(set(params.required_fields) - set(params.supplied_fields) - set(params.runtime_fields))
    result = {
        "methodology": DESIGN_METHOD,
        "input_contract": {
            "status": "incomplete" if missing else "review_required",
            "missing_fields": missing,
            "runtime_fields_requiring_justification": params.runtime_fields,
            "declaration_supplied": bool(params.required_fields),
            "limitation": "Declared names only; verify the producer payload/schema and runtime justifications.",
        },
        "canvas": outline,
        "layout_issues": layout_issues(outline),
        "design_review": review_graph(outline),
    }
    if params.spec is not None:
        objects, connections = normalise_spec(params.spec)
        result["proposed_design_review"] = review_spec(objects, connections)
    return redact(result)


@mcp.tool(
    name="nifi_create_process_group",
    annotations=_ann(title="Create process group", read_only=False, idempotent=False),
)
@mutation_tool
async def nifi_create_process_group(params: CreateProcessGroupIn) -> dict[str, Any]:
    """Create an empty process group. Build every new flow inside one of these, not on root.

    Review the input contract and minimal stage graph with nifi_review_flow_design first.
    New root groups always fill the next free grid cell, even when x/y are supplied.
    Omit x/y to take the next free cell of the grid the sibling groups already sit on, else the next free
    424x288 cell; placement.reason says which. An x/y that
    would overlap another card is shifted down clear of it, and the result says so in warnings.
    This initial slot is provisional: automatic repair arranges related stages top-down and packs
    unrelated child-group clusters into a 16:9 navigation grid only on group-collection canvases.
    Processor and mixed-component flow canvases stay top-down without an aspect-ratio target.
    """
    _require_write()
    touch(params.parent_id)
    client = get_client()
    await client.authenticate()
    (x, y), note, reason = await place_card_why(client, params.parent_id, "process_group", params.x, params.y)
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
    return _with_note({"status": "ok", "process_group": view}, note, {"x": x, "y": y, "reason": reason})


@mcp.tool(
    name="nifi_create_processor",
    annotations=_ann(title="Create processor", read_only=False, idempotent=False),
)
@mutation_tool
async def nifi_create_processor(params: CreateProcessorIn) -> dict[str, Any]:
    """Add a processor to a process group. Prefer nifi_apply_flow_spec for a whole graph.

    First call nifi_review_flow_design: justify why this cannot be combined with an existing stage
    or supplied by the producer. Never add one processor per known field or per batch item.
    Omit x/y to take the first free 512x240 cell that clears every card already in the group. An
    x/y that would overlap another card is shifted down clear of it, and the result says so.
    """
    _require_write()
    touch(params.parent_id)
    client = get_client()
    await client.authenticate()
    (x, y), note, reason = await place_card_why(client, params.parent_id, "processor", params.x, params.y)
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
    return _with_note({"status": "ok", "processor": view}, note, {"x": x, "y": y, "reason": reason})


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
    touch(component.get("parentGroupId"))
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
    touch((current.get("component") or {}).get("parentGroupId"), repair=False)
    entity = await client.set_processor_run_status(params.component_id, params.state, _version(current))
    return {"status": "ok", "processor": C.compact_processor(entity)}


@mcp.tool(
    name="nifi_create_connection",
    annotations=_ann(title="Connect two components", read_only=False, idempotent=False),
)
@mutation_tool
async def nifi_create_connection(params: CreateConnectionIn) -> dict[str, Any]:
    """Connect a source to a destination. Processor sources need relationships; port sources take none.

    Leave the name out: NiFi labels an unnamed connection with its selected relationships, and a port
    source shows the port. Give a name only when it says something they do not (rule 14).

    Wiring child groups: connect an OUTPUT_PORT (source_group_id = its group) to an INPUT_PORT
    (destination_group_id = its group) with parent_id = the group that contains both children.
    """
    _require_write()
    touch(params.parent_id)
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
    touch(_parent_of(current))
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
    touch(params.parent_id, repair=False)
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
    touch(_parent_of(current), repair=False)
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
    touch(_parent_of(current), repair=False)
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
    touch(params.process_group_id, repair=False)
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

    A parameter context can only be deleted once no process group is bound to it. The parent group is
    laid out again afterwards (the result's `validation.layout_repaired`), so no gap is left. The result
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
    if kind != "parameter_context":
        touch(_parent_of(current))
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
    touch(params.parent_id)
    (x, y), note, reason = await place_card_why(client, params.parent_id, "process_group", params.x, params.y)
    entity = await client.import_flow(params.parent_id, params.snapshot, params.group_name, x=x, y=y)
    view = entity if params.verbose else C.compact_process_group(entity)
    return _with_note({"status": "ok", "process_group": view}, note, {"x": x, "y": y, "reason": reason})


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
    touch(params.process_group_id)
    result = await client.replace_flow(params.process_group_id, params.snapshot)
    return {"status": "ok", "result": redact(result)}


@mcp.tool(
    name="nifi_apply_flow_spec",
    annotations=_ann(title="Build a flow from a JSON spec", read_only=False, idempotent=False),
)
@mutation_tool
async def nifi_apply_flow_spec(params: ApplySpecIn) -> dict[str, Any]:
    """Create a complete flow from a declarative spec: process group, services, processors, ports, connections.

    First use nifi_review_flow_design for the input contract and proposed spec; resolve or justify
    its findings. The build returns a pre-write design_review; validation includes live design review.
    Review the actual group again after creation before starting it.

    Property values starting with @ (processor or controller_service properties) are resolved to a
    controller service created earlier in the same spec. Services are enabled in spec order.
    JSON booleans and numbers become NiFi text (true -> "true", 10 -> "10"); null unsets a property.
    auto_terminated and relationships take a list or a single name.
    Connections refer to components by name. A connection without a name stays unnamed (rule 14).
    Prefer this over many create_* calls.

    Every result, ok or error, has outcome (applied, not_applied or unknown: the failing request's)
    and lists created[] (kind, id, ref, outcome; services carry their state). ref is the item's
    place in the spec (process_group, objects[2], connections[0]). An ok result adds each item's
    name and name_map; an error names items by ref only, and never repeats a value from the spec.
    An error, or a build whose flow view could not be read, adds a hint saying how to
    roll back or inspect exactly what this call created. cause "spec" means the spec was refused
    before NiFi was asked (a missing name or type, an unknown connection end or @service, or an
    unknown key at the top level or in process_group): nothing was created. Top-level keys:
    process_group, objects, connections, parent_process_group_id, layout, layout_intent. process_group keys: name,
    comments, parameter_context_id, inherit_parameter_context, x, y, position. A request with no
    definite answer (a timeout, a 5xx, a lost connection) is in created[] with outcome "unknown":
    NiFi may have applied it, and the hint says what the server's read-back found. Check again
    before retrying. Only created[] items with an id are ever named for deletion.
    """
    _require_write()
    client = get_client()
    await client.authenticate()
    touch(params.parent_process_group_id or params.spec.get("parent_process_group_id") or "root")
    result = await apply_flow_spec(
        client, params.spec, parent_process_group_id=params.parent_process_group_id, renderer=Renderer(params)
    )
    for item in result.get("created") or []:
        if item.get("kind") == "process_group" and item.get("id"):
            touch(str(item["id"]))
    if result.get("flow") and not isinstance(result["flow"], str):
        result["flow"] = C.compact_flow(result["flow"])
    return redact(result)


@mcp.tool(
    name="nifi_layout_process_group",
    annotations=_ann(title="Auto-layout a process group", read_only=False),
)
@mutation_tool
async def nifi_layout_process_group(params: LayoutGroupIn) -> dict[str, Any]:
    """Lay flows top-to-bottom; use a navigation grid only for parent collections of child groups.

    Defaults to root and recursively lays out child canvases. One graph covers every component type;
    actual and logical dependencies define clusters. Forward stages sit below their prerequisites,
    forks spread below their parent and joins return towards the main axis. Only identified feedback returns up.
    Only parent navigation canvases of child groups pack unrelated group clusters for a 16:9 screen.
    Boundary ports may accompany those groups. Processor, funnel, remote-endpoint and mixed flow canvases
    stay top-down without an aspect-ratio target, including disconnected or explicitly independent stages.
    Grid rows outside related child-group clusters imply no workflow dependencies.
    Input/output ports still bound their owning canvas. Coordinate fan-in labels beneath source processors
    where practical. Clear shared stretches and card/label collisions; minimize proper line crossings,
    retaining a structured arrangement when crossings remain. Report those crossings in layout_advisories.
    Optional layout_intent declares dependencies, stages, feedback, independent areas, domain and roles.
    It is persisted in the owning group's comments, preserving other prose, for future automatic repair.
    Logical layout dependencies never create executable connections. Saved positions and routes are checked;
    layout_issues reports remaining errors, layout_advisories reports proper crossings, and
    layout_uncertainties reports unresolved order. Crossings alone do not invalidate the layout.
    """
    _require_write()
    client = get_client()
    await client.authenticate()
    touch(params.process_group_id, repair=False)
    if params.layout_intent is not None:
        await persist_layout_intent(
            client, params.process_group_id, params.layout_intent.model_dump(exclude_none=True)
        )
    result = await relayout_process_group(client, params.process_group_id, recursive=True)
    return result


@mcp.tool(
    name="nifi_get_version_state",
    annotations=_ann(title="Registry state of a versioned group"),
)
@json_tool
async def nifi_get_version_state(params: ProcessGroupIn) -> str:
    """Registry version and sync state (UP_TO_DATE, LOCALLY_MODIFIED, STALE, ...) of a process group and of
    every versioned group below it. Read this before nifi_commit_process_group."""
    client = get_client()
    await client.authenticate()
    return _dump(redact(await version_state(client, params.process_group_id)))


@mcp.tool(
    name="nifi_commit_process_group",
    annotations=_ann(title="Commit a versioned group to its registry", read_only=False, idempotent=False),
)
@mutation_tool
async def nifi_commit_process_group(params: CommitGroupIn) -> dict[str, Any]:
    """Commit a LOCALLY_MODIFIED versioned process group as a new registry version, with your comment.

    Versioned groups below it that are modified are committed first, deepest first, and each ancestor is
    then committed at the version its child just got. Refuses, writing nothing, when the group is not
    versioned, nothing is modified, or any versioned group in the tree is stale, in conflict or cannot
    sync. Returns group_id, previous_version and the new version (the commit SHA on a Git registry) for
    each group committed. After committing, repin target_version in the inventory that deploys the flow.
    """
    _require_write()
    client = get_client()
    await client.authenticate()
    return await commit_process_group(client, params.process_group_id, params.comment)


@mcp.tool(
    name="nifi_empty_queue",
    annotations=_ann(title="Drop a connection queue", read_only=False, destructive=True, idempotent=False),
)
@mutation_tool
async def nifi_empty_queue(params: EmptyQueueIn) -> dict[str, Any]:
    """Drop data only from the selected connection. An authorized disposable test reset permits this
    without repeated confirmation; cluster selection, read-only mode and NiFi policies still apply.
    Draining is another option before deletion. Report remaining queue size after completion;
    a producer can refill the queue. No automatic producer stop, repeat drop or cancellation on timeout.
    """
    _require_write()
    client = get_client()
    await client.authenticate()
    touch(_parent_of(await client.get_connection(params.connection_id)), repair=False)
    result = await client.empty_queue(params.connection_id, timeout_seconds=params.timeout_seconds)
    payload: dict[str, Any] = {
        "status": "ok", "connection_id": params.connection_id, "result": redact(result),
        "remaining_count": None, "empty": None,
    }
    try:
        current = await client.get_connection(params.connection_id)
        remaining = ((current.get("status") or {}).get("aggregateSnapshot") or {}).get("flowFilesQueued")
    except NiFiError:
        # The async drop completed. A failed status read cannot make that outcome unknown.
        remaining = None
    if isinstance(remaining, int) and not isinstance(remaining, bool) and remaining >= 0:
        payload.update(remaining_count=remaining, empty=remaining == 0)
        if remaining:
            payload["warnings"] = [
                "The drop completed but the queue is not empty; a producer may have refilled it. "
                "Stop the producer and wait for active threads to finish before another authorized reset. "
                "No producer was stopped and no second drop was submitted."
            ]
    else:
        payload["warnings"] = [
            "The drop completed; remaining queue size is unknown because verification was unavailable. "
            "Read the connection queue before deciding whether another drop is needed."
        ]
    return payload


@mcp.tool(
    name="nifi_list_parameter_contexts",
    annotations=_ann(title="Parameter contexts"),
)
@json_tool
async def nifi_list_parameter_contexts(params: ClusterIn = NO_CLUSTER) -> str:
    """Discover existing root contexts before creating parameters. Context gitlab owns gitlab.*;
    compose multiple roots through inherited_context_ids. Sensitive values are redacted.
    """
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
    """Create a root context whose name matches every owned parameter root.

    Use lowercase root.service.key[.key]: gitlab.pypi.username lives in context gitlab.
    Reuse an existing root context after listing contexts; never invent aliases or copy shared values.
    A flow needing several roots binds a composed context with inherited_context_ids.
    Mark credentials sensitive explicitly. Bind with nifi_bind_parameter_context.
    """
    _require_write()
    client = get_client()
    await client.authenticate()
    entity = await client.create_parameter_context(
        params.name, parameters=_as_dicts(params.parameters), description=params.description,
        inherited_context_ids=params.inherited_context_ids
    )
    return {"status": "ok", "parameter_context": _parameter_context_view(entity, params.verbose)}


@mcp.tool(
    name="nifi_update_parameter_context",
    annotations=_ann(title="Update parameter context", read_only=False),
)
@mutation_tool
async def nifi_update_parameter_context(params: UpdateParameterContextIn) -> dict[str, Any]:
    """Add, change, or remove parameters. NiFi restarts referencing components itself.

    New entries must use root.service.key[.key] and match the owning context name. Existing legacy
    names remain editable/removable. inherited_context_ids replaces inheritance; omit to preserve,
    [] to clear. Shared values belong in root contexts, not copied into composed flow contexts.
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
        inherited_context_ids=params.inherited_context_ids,
        name=params.name,
        description=params.description,
    )
    for group_id in C.compact_parameter_context(entity).get("bound_process_groups", []):
        touch(group_id, repair=False)
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
    touch(params.process_group_id, repair=False)
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
Required before selecting processors or building:
{methodology}

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
""".strip().replace("{methodology}", DESIGN_METHOD)


@mcp.prompt()
def nifi_debug_flow() -> str:
    """Debug a broken or stalled process group."""
    return """
1. nifi_get_health: INVALID processors and queued connections first.
2. nifi_get_bulletins: ERROR/WARNING messages.
3. For each INVALID processor: nifi_get_processor, then nifi_get_processor_definition.
4. Queues that never drain: nifi_list_queue, then inspect the downstream processor state.
5. NiFiConflictError (HTTP 409 or preflight): re-read the target and diagnose revision, lifecycle,
   queue or dependency blockers. Stop only required producers, drain queues, then stop consumers and
   wait for active tasks. Drop data only within authorized scope; selected disposable test queues may be emptied
   for an authorized reset without repeated confirmation. Never blindly retry; read back unknown outcomes first.
   A hint naming a refused field means fix that value; nothing needs stopping.
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
- Flow deployment can restore versioned groups from their registry. Prototype in an unversioned group.
""".strip()


def configure_http(settings: Settings) -> None:
    """Configure the registered tools as an authenticated, stateless resource server."""
    from mcp.server.auth.settings import AuthSettings
    from mcp.server.transport_security import TransportSecuritySettings

    # Load and validate every cluster's dedicated NiFi credential once before accepting callers.
    for cluster in clusters().values():
        _ = cluster.proxy_tls_context  # cached TLS context initialization
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
        configured = clusters()
    except (ValidationError, ValueError) as exc:
        LOG.error("Invalid configuration: %s", safe_error_message(exc))
        raise SystemExit(2) from None
    for cluster in configured.values():
        if cluster.auth != "mtls":
            continue
        try:
            cluster.client_tls_context  # noqa: B018 - validate certificate before accepting tool calls
        except (OSError, ssl.SSLError, ValueError):
            LOG.error(
                "Cannot configure certificate authentication for cluster %s; check CA, client certificate/key "
                "and key password",
                cluster.cluster_name,
            )
            raise SystemExit(2) from None
    LOG.info("Clusters: %s", ", ".join(f"{name} ({cfg.api_url})" for name, cfg in configured.items()))
    settings = get_settings()
    if settings.transport == "streamable-http":
        try:
            configure_http(settings)
        except (OSError, ssl.SSLError, ValueError):
            LOG.error("Cannot configure HTTP authentication; check OAuth URLs, CA and proxy certificate/key")
            raise SystemExit(2) from None
    mcp.run(transport=settings.transport)


if __name__ == "__main__":
    main()
