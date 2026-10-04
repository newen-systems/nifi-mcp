"""Async NiFi 2 REST client. Paths match nifi-web-api JAX-RS resources."""

from __future__ import annotations

import asyncio
import contextlib
import re
import time
from collections.abc import Awaitable
from contextvars import ContextVar
from typing import Any

import httpx

from nifi_mcp.auth import TokenStore
from nifi_mcp.config import Settings
from nifi_mcp.errors import (
    NiFiAuthError,
    NiFiConflictError,
    NiFiError,
    NiFiTimeoutError,
    NiFiUncertainError,
    Outcome,
)
from nifi_mcp.proxy_entities import encode_entity
from nifi_mcp.readback import ReadBack, read_back
from nifi_mcp.redaction import invalid_values, remember, versioned_id

_JSON_HEADERS = {"Accept": "application/json", "Content-Type": "application/json"}
_SAFE_RETRIES = 2
_RETRY_STATUSES = frozenset({502, 503, 504})
_MUTATING = frozenset({"POST", "PUT", "DELETE", "PATCH"})
# A gateway answered instead of NiFi: the request may or may not have reached NiFi and been applied.
_UNCERTAIN_STATUSES = frozenset({502, 504})
# The first word of an async request state that ended without completing (DropFlowFileState
# "Failed", "Canceled by user"; the update and replace requests' FAILURE and CANCELED).
_ASYNC_FAILED_STATES = frozenset({"failed", "failure", "canceled", "cancelled"})
# What NiFi itself returns for a sensitive value (DtoFactory.SENSITIVE_VALUE_MASK).
SENSITIVE_MASK = "********"


_SAFE_SEGMENT = re.compile(r"^[A-Za-z0-9._~-]+$")
_NIFI_ID = re.compile(r"^(root|[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})$")


# A NiFi UUID standing in text: not part of a longer run of hex digits or dashes.
_ID_IN_TEXT = re.compile(
    r"(?<![0-9A-Fa-f-])[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}(?![0-9A-Fa-f-])"
)


def nifi_id_spans(text: str) -> list[tuple[int, int]]:
    """Where each NiFi UUID in text starts and ends."""
    return [match.span() for match in _ID_IN_TEXT.finditer(text)]


def is_nifi_id(value: object) -> bool:
    """A NiFi component UUID or 'root': the only ids that go into a URL path."""
    return isinstance(value, str) and bool(_NIFI_ID.match(value))


def check_path(path: str) -> str:
    """Refuse any path whose segments could leave /nifi-api or smuggle a query.

    Ids are interpolated into paths, so '..', '?', '#', '%' or '/' inside an id would retarget
    the request (httpx resolves '../' against the /nifi-api base URL).
    """
    segments = path.split("/")
    if segments[0] != "" or any(
        seg in {".", ".."} or not _SAFE_SEGMENT.match(seg) for seg in segments[1:]
    ):
        raise NiFiError("Refusing an unsafe NiFi API path: ids must be NiFi UUIDs or 'root'")
    return path


# DataUnit.DATA_SIZE_REGEX, whole string: a number and B, KB, MB, GB or TB (NiFi upper-cases first).
_DATA_SIZE = re.compile(r"\s*\d+(?:\.\d+)?\s*(?:B|KB|MB|GB|TB)\s*", re.IGNORECASE)
DATA_SIZE_FIELD = "back_pressure_data_size_threshold"


def check_data_size(value: str, label: str = DATA_SIZE_FIELD) -> str:
    """A queue data size NiFi can parse. NiFi's own refusal quotes the value (DataUnit.parseDataSize)
    and comes after it has applied the connection's other queue fields, so a bad one never goes out.
    The error names the field, never the value."""
    if not _DATA_SIZE.fullmatch(value):
        raise ValueError(f"{label} must be a data size: a number and B, KB, MB, GB or TB, such as '10 MB'")
    return value


def relationships_for_source(source_type: str, relationships: list[str] | None) -> list[str]:
    """Relationship names to send for a connection, or raise if a processor source has none."""
    names = [name for name in relationships or [] if name]
    if source_type != "PROCESSOR":
        return []
    if not names:
        raise NiFiError(
            "A connection from a processor needs at least one relationship name in relationships. "
            "Port and funnel sources take none."
        )
    return names


# Every mutation one tool call sent and its outcome, oldest first. json_tool opens one per call, so
# a tool that fails after an earlier request landed can say which requests were applied.
LEDGER: ContextVar[list[dict[str, str]] | None] = ContextVar("nifi_mcp_ledger", default=None)


def _log(method: str, path: str, outcome: Outcome) -> None:
    ledger = LEDGER.get()
    if ledger is not None:
        ledger.append({"method": method.upper(), "path": path, "outcome": str(outcome)})


def _drop_last_log() -> None:
    """Take back the entry for a request whose outcome turned out other than its status said."""
    ledger = LEDGER.get()
    if ledger:
        ledger.pop()


def uncertain_error(
    method: str,
    path: str,
    what: str,
    read_back: ReadBack,
    *,
    status_code: int | None = None,
    body: str | None = None,
    timeout: bool = False,
) -> NiFiUncertainError:
    """The error for a mutation whose outcome is unknown, once its read-back has run."""
    retry = " Retrying a create before that can make a duplicate." if method.upper() == "POST" else ""
    message = f"{method} {path} {what}, so the request may have been applied or not."
    hint = (
        f"Read back with {read_back.read}: {read_back.finding}. "
        f"Call {read_back.read} again to confirm before retrying.{retry}"
    )
    kind = NiFiTimeoutError if timeout else NiFiUncertainError
    return kind(message, status_code=status_code, path=path, body=body, outcome=Outcome.UNKNOWN, hint=hint)


def _queue_settings(
    object_threshold: int | None, data_size_threshold: str | None, expiration: str | None
) -> dict[str, Any]:
    """ConnectionDTO queue fields that were set. NiFi keeps its defaults (10000, 1 GB, 0 sec) for the rest."""
    if data_size_threshold is not None:
        try:
            check_data_size(data_size_threshold)
        except ValueError as exc:
            raise NiFiError(str(exc)) from None
    fields = {
        "backPressureObjectThreshold": object_threshold,
        "backPressureDataSizeThreshold": data_size_threshold,
        "flowFileExpiration": expiration,
    }
    return {key: value for key, value in fields.items() if value is not None}


# Raised before any byte of the request is written: no connection (ConnectError, ConnectTimeout),
# none free in the pool (PoolTimeout), a proxy that refused the tunnel, or a URL httpx cannot send.
_NEVER_SENT = (httpx.ConnectError, httpx.ConnectTimeout, httpx.PoolTimeout, httpx.ProxyError, httpx.UnsupportedProtocol)
NOT_SENT_HINT = "Nothing reached NiFi, so the request changed nothing. Retry once NiFi is reachable."
REFUSED_HINT = "NiFi refused the request, so it changed nothing. Fix what the error names, then retry."
# NiFi's sentence for a setting it refused, and the setting it names: every sentence the DAOs'
# validateProposedConfiguration records before they change anything (StandardProcessorDAO,
# StandardConnectionDAO, StandardControllerServiceDAO), and the cron period it throws there. A cluster
# sends these back as HTTP 409 ("Node ... is unable to fulfill this request due to: ...",
# ThreadPoolRequestReplicator, IllegalClusterStateExceptionMapper), a single node as 400
# (ValidationExceptionMapper): either way a refused value, not a revision or state conflict. A field
# holding {0} names the property NiFi quoted (its display name, never its value).
_REFUSED_SETTINGS = tuple(
    (re.compile(pattern), field)
    for pattern, field in (
        (r"Scheduling period is not a valid", "scheduling_period"),
        (r"Scheduling Period '", "scheduling_period"),
        (r"Scheduling strategy: Value must be", "scheduling_strategy"),
        (r"Penalty duration is not a valid", "penalty_duration"),
        (r"Yield duration is not a valid", "yield_duration"),
        (r"Bulletin level: Value must be", "bulletin_level"),
        (r"Execution node: Value must be", "execution_node"),
        (r"Concurrent tasks must be", "concurrent_tasks"),
        (r"Cannot automatically terminate '", "auto_terminated"),
        # AbstractComponentNode.verifyCanUpdateProperties, for a processor and a controller service.
        (
            r"The property '([^'\n]{1,200})' (?:cannot reference more than one Parameter|is a sensitive property so)",
            "property '{0}'",
        ),
        (r"Flow file expiration is not a valid", "flow_file_expiration"),
        (r"Max queue size must be", "back_pressure_object_threshold"),
        (r"The label index must be positive", "label_index"),
        (r"When the destination is a remote input port its group id is required", "destination_group_id"),
        (r"Unable to find the specified remote process group", "source_group_id or destination_group_id"),
        (r"Unable to find the specified destination\.", "destination_id"),
    )
)


def refused_settings(body: str) -> list[str]:
    """The settings a NiFi refusal names, in the order this server knows them."""
    found = [
        field.format(*match.groups())
        for pattern, field in _REFUSED_SETTINGS
        for match in pattern.finditer(body)
    ]
    return list(dict.fromkeys(found))


def refused_hint(settings: list[str]) -> str:
    """The hint for a refusal: the fields NiFi named, never their values."""
    if not settings:
        return REFUSED_HINT
    names = ", ".join(settings)
    return (
        f"NiFi refused the value of {names}, so it changed nothing. "
        f"Set {names} to a value NiFi accepts, then retry; nothing needs stopping, refreshing or emptying."
    )


REAUTH_HINT = (
    "NiFi refused the request with HTTP 401, so it changed nothing, and logging in again failed. "
    "Fix the credentials or wait for the identity provider, then retry."
)


def _read_failure(method: str, path: str, exc: httpx.RequestError) -> NiFiError:
    """The error for a read that got no HTTP response."""
    if isinstance(exc, httpx.TimeoutException):
        return NiFiTimeoutError(f"Timeout calling {method} {path}", path=path)
    return NiFiError(f"Network error calling {method} {path} ({type(exc).__name__})", path=path)


def _remote_group_ids(status: Any) -> list[str]:
    """The ids of every remote process group in a recursive ProcessGroupStatusEntity."""
    if isinstance(status, list):
        return [found for item in status for found in _remote_group_ids(item)]
    if not isinstance(status, dict):
        return []
    ids = [
        item["id"]
        for item in status.get("remoteProcessGroupStatusSnapshots") or []
        if isinstance(item, dict) and isinstance(item.get("id"), str)
    ]
    return ids + [
        found
        for key, value in status.items()
        if key != "remoteProcessGroupStatusSnapshots"
        for found in _remote_group_ids(value)
    ]


class NiFiClient:
    """Thin httpx wrapper over /nifi-api. Fetches RevisionDTO before updates."""

    def __init__(self, settings: Settings, *, transport: httpx.AsyncBaseTransport | None = None,
                 proxy_identity: str | None = None, proxy_groups: list[str] | None = None) -> None:
        self.settings = settings
        self.tokens = TokenStore(settings)
        self._proxy_identity = proxy_identity
        self._proxy_groups = proxy_groups or []
        verify = settings.tls_verify_value()
        if proxy_identity is not None:
            verify = settings.proxy_tls_context
        elif settings.auth == "mtls":
            verify = settings.client_tls_context
        self._client = httpx.AsyncClient(
            base_url=settings.api_url,
            timeout=settings.timeout_seconds,
            verify=verify,
            transport=transport,
            headers={"Accept": "application/json"},
        )
        self._auth_lock = asyncio.Lock()

    async def aclose(self) -> None:
        await self._client.aclose()

    async def authenticate(self, *, force: bool = False) -> None:
        if self._proxy_identity is not None or self.settings.auth == "mtls":
            return  # TLS authenticates the certificate, with a proxy chain when present.
        async with self._auth_lock:
            if self.tokens.token and not force:
                return
            await self.tokens.authenticate(self._client)

    def _headers(self, extra_headers: dict[str, str] | None = None, files: Any = None) -> dict[str, str]:
        headers = {**_JSON_HEADERS, **self.tokens.authorization_header()}
        if extra_headers:
            headers.update(extra_headers)
        if files is not None:
            headers.pop("Content-Type", None)
        if self.settings.auth == "mtls":
            headers = {key: value for key, value in headers.items() if key.lower() not in {
                "authorization", "x-proxiedentitieschain", "x-proxiedentitygroups",
            }}
        if self._proxy_identity is not None:
            headers.pop("Authorization", None)
            headers["X-ProxiedEntitiesChain"] = encode_entity(self._proxy_identity)
            headers["X-ProxiedEntityGroups"] = "".join(encode_entity(group) for group in self._proxy_groups)
        return headers

    def _revision(self, version: int = 0) -> dict[str, Any]:
        return {"clientId": self.settings.client_id, "version": version}

    def _ack(self) -> bool:
        return self.settings.disconnected_node_ack

    def _mutating(self, method: str) -> bool:
        return method.upper() in _MUTATING

    async def _replay_after_401(
        self, method: str, path: str, response: httpx.Response, retry_on_401: bool, mutating: bool
    ) -> bool:
        """Log in again after a 401 so the request can be sent once more.

        A 401 is a refusal: NiFi changed nothing. A mutation is logged not_applied before the
        re-login, so if the re-login fails the error carries that outcome and never inherits the
        outcome of an earlier request of the same tool call."""
        if response.status_code != 401 or not retry_on_401 or self.settings.auth in {"bearer", "mtls"}:
            return False
        if mutating:
            _log(method, path, Outcome.NOT_APPLIED)
        try:
            await self.authenticate(force=True)
        except (NiFiError, httpx.HTTPError) as exc:
            if not mutating:
                raise
            error = exc if isinstance(exc, NiFiError) else NiFiAuthError(
                f"Logging in again failed ({type(exc).__name__})"
            )
            error.outcome, error.hint = Outcome.NOT_APPLIED, REAUTH_HINT
            raise error from exc
        if mutating:
            # The replay decides the outcome.
            _drop_last_log()
        return True

    async def request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: Any | None = None,
        data: Any | None = None,
        files: Any | None = None,
        extra_headers: dict[str, str] | None = None,
        retry_on_401: bool = True,
        read_group: str | None = None,
        mutation: bool | None = None,
    ) -> httpx.Response:
        """read_group: the process group an unknown change of a connection or port is read back from.

        mutation=False marks a POST or DELETE that changes no flow state (a queue listing, the
        cleanup of an async request): it gets no outcome, no read-back and no ledger entry."""
        check_path(path)
        headers = self._headers(extra_headers, files)
        mutating = self._mutating(method) if mutation is None else mutation
        attempts = _SAFE_RETRIES if method.upper() == "GET" else 1
        last_error: NiFiError | None = None
        for attempt in range(attempts):
            try:
                response = await self._client.request(
                    method,
                    path,
                    params=params,
                    json=json_body,
                    data=data,
                    files=files,
                    headers=headers,
                )
            except httpx.RequestError as exc:
                if mutating:
                    raise await self._send_failure(method, path, exc, json_body, params, read_group) from exc
                error = _read_failure(method, path, exc)
                # Only a read that timed out is tried again.
                if not isinstance(error, NiFiTimeoutError) or attempt + 1 >= attempts:
                    raise error from exc
                last_error = error
                await asyncio.sleep(0.4 * (attempt + 1))
                continue

            if await self._replay_after_401(method, path, response, retry_on_401, mutating):
                return await self.request(
                    method,
                    path,
                    params=params,
                    json_body=json_body,
                    data=data,
                    files=files,
                    extra_headers=extra_headers,
                    retry_on_401=False,
                    read_group=read_group,
                    mutation=mutation,
                )
            if mutating and response.status_code >= 500:
                # A 5xx is no definite answer: a gateway may have timed out on a request NiFi went on
                # to apply, and NiFi's own 500 can come after its DAO changed state.
                raise await self.uncertain(
                    method,
                    path,
                    f"got HTTP {response.status_code} instead of a definite answer",
                    json_body=json_body,
                    params=params,
                    read_group=read_group,
                    status_code=response.status_code,
                    body=response.text,
                )
            if response.status_code in _RETRY_STATUSES and not mutating and attempt + 1 < attempts:
                last_error = self._error_from_response(method, path, response)
                await asyncio.sleep(0.4 * (attempt + 1))
                continue
            return response
        raise last_error or NiFiError(f"No response for {method} {path}", path=path)

    async def _send_failure(
        self,
        method: str,
        path: str,
        exc: httpx.RequestError,
        json_body: Any,
        params: dict[str, Any] | None,
        read_group: str | None,
    ) -> NiFiError:
        """The error for a mutation that got no HTTP response at all."""
        if isinstance(exc, _NEVER_SENT):
            # Definite: NiFi never saw the request, so nothing changed and it is safe to retry.
            _log(method, path, Outcome.NOT_APPLIED)
            return NiFiError(
                f"{type(exc).__name__} calling {method} {path}: nothing was sent",
                path=path,
                outcome=Outcome.NOT_APPLIED,
                hint=NOT_SENT_HINT,
            )
        # ReadTimeout or WriteTimeout: the request was being written or had been. A connection lost
        # after connecting may have delivered it too.
        timeout = isinstance(exc, httpx.TimeoutException)
        what = "got no response in time" if timeout else f"lost its connection ({type(exc).__name__})"
        return await self.uncertain(
            method, path, what, json_body=json_body, params=params, read_group=read_group, timeout=timeout
        )

    async def uncertain(
        self,
        method: str,
        path: str,
        what: str,
        *,
        json_body: Any = None,
        params: dict[str, Any] | None = None,
        read_group: str | None = None,
        status_code: int | None = None,
        body: str | None = None,
        timeout: bool = False,
    ) -> NiFiUncertainError:
        """Record `method path` as unknown, read back the state it may have changed, and build the error."""
        _log(method, path, Outcome.UNKNOWN)
        found = await read_back(self.get_json, method, path, json_body, params, read_group)
        return uncertain_error(method, path, what, found, status_code=status_code, body=body, timeout=timeout)

    def _error_from_response(self, method: str, path: str, response: httpx.Response) -> NiFiError:
        body = response.text
        message = f"{method} {path} failed"
        if response.status_code == 401:
            return NiFiAuthError(message, status_code=401, path=path, body=body)
        if settings := refused_settings(body):
            return NiFiError(
                f"{message}: NiFi refused the value of {', '.join(settings)}",
                status_code=response.status_code,
                path=path,
                body=body,
            )
        if response.status_code == 409:
            if "initializing" in body.lower():
                return NiFiError(
                    "NiFi is still starting the data flow. Retry in a few seconds.",
                    status_code=409,
                    path=path,
                    body=body,
                )
            return NiFiConflictError(
                f"{message}. Stop running processors, refresh revision, or empty the queue first.",
                status_code=409,
                path=path,
                body=body,
            )
        if response.status_code == 403:
            return NiFiError(
                f"{message}: permission denied. Check the caller's NiFi policies.",
                status_code=403,
                path=path,
                body=body,
            )
        if response.status_code == 404:
            return NiFiError(
                f"{message}: not found. Confirm the component id.",
                status_code=404,
                path=path,
                body=body,
            )
        return NiFiError(message, status_code=response.status_code, path=path, body=body)

    @staticmethod
    def _envelope(payload: dict[str, Any], *keys: str) -> dict[str, Any]:
        for key in keys:
            inner = payload.get(key)
            if isinstance(inner, dict):
                return inner
        return payload

    async def _poll_until_done(
        self,
        get_path: str,
        started: dict[str, Any],
        *,
        envelope_keys: tuple[str, ...],
        timeout_seconds: float,
        interval: float,
    ) -> dict[str, Any]:
        """Poll an async request until finished/complete. Raises on timeout and leaves the request alone.

        NiFi treats DELETE on a drop, replace or parameter update request as cancel, so callers must
        not clean up a request that is still running.
        """
        deadline = time.monotonic() + timeout_seconds
        last = started
        while True:
            current = self._envelope(last, *envelope_keys)
            if current.get("finished") or current.get("complete"):
                return last
            if time.monotonic() >= deadline:
                percent = current.get("percentCompleted")
                state = current.get("state") or (f"{percent}% complete" if percent is not None else "not finished")
                raise NiFiError(
                    f"Request still running after {timeout_seconds:g}s ({state}). It was not cancelled; "
                    "check the component again before retrying.",
                    path=get_path,
                )
            await asyncio.sleep(interval)
            last = await self.get_json(get_path)

    @staticmethod
    def _async_failure(last: dict[str, Any], *envelope_keys: str) -> str | None:
        """Why a finished async request (a drop, a parameter update, a replace) did not complete: its
        failureReason, or a failed or cancelled state. NiFi marks those finished too (DtoFactory
        createDropRequestDTO; DropFlowFileState FAILURE "Failed", CANCELED "Canceled by user")."""
        request = NiFiClient._envelope(last, *envelope_keys)
        reason = request.get("failureReason")
        if reason:
            return str(reason)
        state = str(request.get("state") or "")
        if state.split(" ")[0].lower() in _ASYNC_FAILED_STATES:
            return f"NiFi reports the request as {state}"
        return None

    async def _delete_quietly(
        self, path: str, *, params: dict[str, Any] | None = None
    ) -> None:
        with contextlib.suppress(NiFiError):
            await self.json("DELETE", path, params=params, expected={200, 201, 202, 404}, mutation=False)

    async def _after_submit[T](self, method: str, path: str, json_body: Any, pending: Awaitable[T]) -> T:
        """Wait on an async request NiFi accepted (a drop, a parameter update, a replace). If the wait
        fails, NiFi may be applying it still or may have stopped part way: the outcome is unknown."""
        try:
            return await pending
        except NiFiUncertainError:
            raise
        except NiFiError as exc:
            what = f"was accepted, but following it failed ({exc.args[0]})"
            raise await self.uncertain(method, path, what, json_body=json_body) from exc

    async def json(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: Any | None = None,
        expected: set[int] | None = None,
        **kwargs: Any,
    ) -> Any:
        """One request, decoded. A mutation ends here in exactly one Outcome: applied (the expected
        2xx), not_applied (never sent, or a 4xx refusal) or unknown (raised with a read-back)."""
        allowed = expected or {200, 201}
        response = await self.request(method, path, params=params, json_body=json_body, **kwargs)
        mutating = self._mutating(method) if kwargs.get("mutation") is None else kwargs["mutation"]
        if response.status_code not in allowed:
            if mutating and not 400 <= response.status_code < 500:
                raise await self.uncertain(
                    method,
                    path,
                    f"got an unexpected HTTP {response.status_code}",
                    json_body=json_body,
                    params=params,
                    read_group=kwargs.get("read_group"),
                    status_code=response.status_code,
                    body=response.text,
                )
            error = self._error_from_response(method, path, response)
            if mutating:
                _log(method, path, Outcome.NOT_APPLIED)
                error.outcome, error.hint = Outcome.NOT_APPLIED, refused_hint(refused_settings(response.text))
            raise error
        if mutating:
            _log(method, path, Outcome.APPLIED)
        if not response.content:
            return {}
        try:
            decoded = response.json()
        except ValueError:
            return {"raw": response.text}
        # The values a component holds prove where they end in a body that lists several sentences.
        remember(decoded)
        return decoded

    async def get_json(self, path: str, params: dict[str, Any] | None = None) -> Any:
        return await self.json("GET", path, params=params)

    # ── identity ──────────────────────────────────────────────────────────

    async def about(self) -> dict[str, Any]:
        return await self.get_json("/flow/about")

    async def current_user(self) -> dict[str, Any]:
        return await self.get_json("/flow/current-user")

    @staticmethod
    def version_tuple(about: dict[str, Any]) -> tuple[int, int, int]:
        raw = ((about.get("about") or {}).get("version") or "0.0.0").split(".")
        nums = []
        for part in raw[:3]:
            digits = "".join(ch for ch in part if ch.isdigit())
            nums.append(int(digits) if digits else 0)
        while len(nums) < 3:
            nums.append(0)
        return nums[0], nums[1], nums[2]

    # ── process groups / flow ─────────────────────────────────────────────

    async def get_flow(self, process_group_id: str = "root") -> dict[str, Any]:
        return await self.get_json(f"/flow/process-groups/{process_group_id}")

    async def get_process_group(self, process_group_id: str) -> dict[str, Any]:
        return await self.get_json(f"/process-groups/{process_group_id}")

    async def search(self, query: str) -> dict[str, Any]:
        return await self.get_json("/flow/search-results", params={"q": query})

    async def create_process_group(
        self,
        parent_id: str,
        name: str,
        *,
        x: float = 0.0,
        y: float = 0.0,
        comments: str | None = None,
        parameter_context_id: str | None = None,
    ) -> dict[str, Any]:
        component: dict[str, Any] = {"name": name, "position": {"x": x, "y": y}}
        if comments:
            component["comments"] = comments
        if parameter_context_id:
            component["parameterContext"] = {"id": parameter_context_id}
        return await self.json(
            "POST",
            f"/process-groups/{parent_id}/process-groups",
            json_body={
                "revision": self._revision(0),
                "disconnectedNodeAcknowledged": self._ack(),
                "component": component,
            },
        )

    async def update_process_group(
        self,
        process_group_id: str,
        *,
        version: int,
        x: float | None = None,
        y: float | None = None,
        name: str | None = None,
        comments: str | None = None,
    ) -> dict[str, Any]:
        # Partial DTO. Versioned groups reject PUT if versionControlInformation is sent back.
        component: dict[str, Any] = {"id": process_group_id}
        if name is not None:
            component["name"] = name
        if comments is not None:
            component["comments"] = comments
        if x is not None or y is not None:
            component["position"] = await self._merged_position(f"/process-groups/{process_group_id}", x, y)
        return await self.json(
            "PUT",
            f"/process-groups/{process_group_id}",
            json_body={
                "revision": self._revision(version),
                "disconnectedNodeAcknowledged": self._ack(),
                "component": component,
            },
        )

    async def set_process_group_parameter_context(
        self,
        process_group_id: str,
        parameter_context_id: str | None,
        *,
        version: int,
        recursive: bool = False,
    ) -> dict[str, Any]:
        """Bind (or with None, unbind) a parameter context. StandardProcessGroupDAO applies parameterContext.id.

        recursive sends processGroupUpdateStrategy=ALL_DESCENDANTS, the UI's "Apply recursively":
        ProcessGroupResource.updateProcessGroup then rebinds every descendant in the same request,
        authorizing and verifying all of them under one write lock before changing any.
        """
        body: dict[str, Any] = {
            "revision": self._revision(version),
            "disconnectedNodeAcknowledged": self._ack(),
            "component": {"id": process_group_id, "parameterContext": {"id": parameter_context_id}},
        }
        if recursive:
            body["processGroupUpdateStrategy"] = "ALL_DESCENDANTS"
        return await self.json("PUT", f"/process-groups/{process_group_id}", json_body=body)

    async def schedule_process_group(self, process_group_id: str, state: str) -> dict[str, Any]:
        return await self.json(
            "PUT",
            f"/flow/process-groups/{process_group_id}",
            json_body={"id": process_group_id, "state": state},
        )

    async def enable_controller_services_in_group(
        self, process_group_id: str, state: str = "ENABLED"
    ) -> dict[str, Any]:
        return await self.json(
            "PUT",
            f"/flow/process-groups/{process_group_id}/controller-services",
            json_body={"id": process_group_id, "state": state},
        )

    async def delete_process_group(self, process_group_id: str, version: int) -> dict[str, Any]:
        return await self.json(
            "DELETE",
            f"/process-groups/{process_group_id}",
            params={
                "version": version,
                "clientId": self.settings.client_id,
                "disconnectedNodeAcknowledged": str(self._ack()).lower(),
            },
        )

    # ── processors ────────────────────────────────────────────────────────

    async def list_processor_types(self) -> dict[str, Any]:
        """Every installed type. FlowResource's `type` query param is an exact class-name match,
        so substring search happens in the caller."""
        return await self.get_json("/flow/processor-types")

    async def get_processor_definition(
        self, group: str, artifact: str, version: str, type_name: str
    ) -> dict[str, Any]:
        return await self.get_json(
            f"/flow/processor-definition/{group}/{artifact}/{version}/{type_name}"
        )

    async def get_processor(self, processor_id: str) -> dict[str, Any]:
        return await self.get_json(f"/processors/{processor_id}")

    async def create_processor(
        self,
        parent_id: str,
        processor_type: str,
        name: str,
        *,
        x: float = 0.0,
        y: float = 0.0,
        properties: dict[str, str | None] | None = None,
        auto_terminated: list[str] | None = None,
        bundle: dict[str, str] | None = None,
        scheduling_period: str | None = None,
        scheduling_strategy: str | None = None,
        comments: str | None = None,
    ) -> dict[str, Any]:
        component: dict[str, Any] = {
            "type": processor_type,
            "name": name,
            "position": {"x": x, "y": y},
        }
        if bundle:
            component["bundle"] = bundle
        config: dict[str, Any] = {}
        if properties:
            config["properties"] = properties
        if auto_terminated:
            config["autoTerminatedRelationships"] = auto_terminated
        if scheduling_period:
            config["schedulingPeriod"] = scheduling_period
        if scheduling_strategy:
            config["schedulingStrategy"] = scheduling_strategy
        if comments:
            config["comments"] = comments
        if config:
            component["config"] = config
        return await self.json(
            "POST",
            f"/process-groups/{parent_id}/processors",
            json_body={
                "revision": self._revision(0),
                "disconnectedNodeAcknowledged": self._ack(),
                "component": component,
            },
        )

    async def update_processor(
        self,
        processor_id: str,
        *,
        version: int,
        name: str | None = None,
        properties: dict[str, str | None] | None = None,
        auto_terminated: list[str] | None = None,
        scheduling_period: str | None = None,
        comments: str | None = None,
        x: float | None = None,
        y: float | None = None,
        scheduling_strategy: str | None = None,
    ) -> dict[str, Any]:
        """PUT a partial ProcessorDTO: only the fields the caller set.

        NiFi masks sensitive properties as "********" on GET and stores whatever a PUT sends,
        so echoing the GET entity back would overwrite every secret with the mask.
        Position-only updates also skip verifyCanUpdate, so running processors can move.
        """
        component: dict[str, Any] = {"id": processor_id}
        if name is not None:
            component["name"] = name
        config: dict[str, Any] = {}
        if comments is not None:
            config["comments"] = comments
        if properties is not None:
            config["properties"] = properties
        if auto_terminated is not None:
            config["autoTerminatedRelationships"] = auto_terminated
        if scheduling_period is not None:
            config["schedulingPeriod"] = scheduling_period
        if scheduling_strategy is not None:
            config["schedulingStrategy"] = scheduling_strategy
        if config:
            component["config"] = config
        if x is not None or y is not None:
            component["position"] = await self._merged_position(f"/processors/{processor_id}", x, y)
        return await self.json(
            "PUT",
            f"/processors/{processor_id}",
            json_body={
                "revision": self._revision(version),
                "disconnectedNodeAcknowledged": self._ack(),
                "component": component,
            },
        )

    async def _merged_position(self, path: str, x: float | None, y: float | None) -> dict[str, float]:
        if x is not None and y is not None:
            return {"x": x, "y": y}
        current = await self.get_json(path)
        pos = dict((current.get("component") or {}).get("position") or {})
        if x is not None:
            pos["x"] = x
        if y is not None:
            pos["y"] = y
        return pos

    async def set_processor_run_status(self, processor_id: str, state: str, version: int) -> dict[str, Any]:
        return await self.json(
            "PUT",
            f"/processors/{processor_id}/run-status",
            json_body={
                "revision": self._revision(version),
                "state": state,
                "disconnectedNodeAcknowledged": self._ack(),
            },
        )

    async def delete_processor(self, processor_id: str, version: int) -> dict[str, Any]:
        return await self.json(
            "DELETE",
            f"/processors/{processor_id}",
            params={
                "version": version,
                "clientId": self.settings.client_id,
                "disconnectedNodeAcknowledged": str(self._ack()).lower(),
            },
        )

    # ── connections / queues ──────────────────────────────────────────────

    async def get_connection(self, connection_id: str) -> dict[str, Any]:
        return await self.get_json(f"/connections/{connection_id}")

    async def create_connection(
        self,
        parent_id: str,
        *,
        source_id: str,
        source_group_id: str,
        source_type: str,
        destination_id: str,
        destination_group_id: str,
        destination_type: str,
        relationships: list[str] | None = None,
        name: str | None = None,
        bends: list[dict[str, float]] | None = None,
        label_index: int | None = None,
        back_pressure_object_threshold: int | None = None,
        back_pressure_data_size_threshold: str | None = None,
        flow_file_expiration: str | None = None,
    ) -> dict[str, Any]:
        """Processor sources need selectedRelationships. Port and funnel sources take none.

        LocalPort/AbstractPort.getConnections returns every outgoing connection for its single
        anonymous relationship, so a port connection carries no relationship names at all.
        """
        selected = relationships_for_source(source_type, relationships)
        component: dict[str, Any] = {
            "parentGroupId": parent_id,
            "source": {"id": source_id, "groupId": source_group_id, "type": source_type},
            "destination": {
                "id": destination_id,
                "groupId": destination_group_id,
                "type": destination_type,
            },
        }
        if selected:
            component["selectedRelationships"] = selected
        if name:
            component["name"] = name
        if bends:
            component["bends"] = bends
            component["labelIndex"] = 0 if label_index is None else label_index
        elif label_index is not None:
            component["labelIndex"] = label_index
        component.update(
            _queue_settings(back_pressure_object_threshold, back_pressure_data_size_threshold, flow_file_expiration)
        )
        return await self.json(
            "POST",
            f"/process-groups/{parent_id}/connections",
            json_body={
                "revision": self._revision(0),
                "disconnectedNodeAcknowledged": self._ack(),
                "component": component,
            },
        )

    async def update_connection(
        self,
        connection_id: str,
        *,
        version: int,
        bends: list[dict[str, float]] | None = None,
        label_index: int | None = None,
        name: str | None = None,
        back_pressure_object_threshold: int | None = None,
        back_pressure_data_size_threshold: str | None = None,
        flow_file_expiration: str | None = None,
        parent_id: str | None = None,
    ) -> dict[str, Any]:
        """parent_id, when known, is the group an uncertain outcome tells the model to read."""
        # Omit selectedRelationships: StandardConnectionDAO leaves them alone when null, and rejects
        # an empty list ("Cannot remove all relationships"), which is what a port connection reports.
        payload: dict[str, Any] = {"id": connection_id}
        if bends is not None:
            payload["bends"] = bends
        if label_index is not None:
            payload["labelIndex"] = label_index
        elif bends:
            payload["labelIndex"] = 0
        if name is not None:
            payload["name"] = name
        # StandardConnectionDAO.verifyUpdate checks the source and destination state only when the
        # destination changes, so queue settings can change on a running connection.
        payload.update(
            _queue_settings(back_pressure_object_threshold, back_pressure_data_size_threshold, flow_file_expiration)
        )
        path = f"/connections/{connection_id}"
        try:
            return await self.json(
                "PUT",
                path,
                json_body={
                    "revision": self._revision(version),
                    "disconnectedNodeAcknowledged": self._ack(),
                    "component": payload,
                },
                read_group=parent_id,
            )
        except NiFiUncertainError:
            raise
        except NiFiError as exc:
            applied = [
                label
                for label, value in (
                    ("flow_file_expiration", flow_file_expiration),
                    ("back_pressure_object_threshold", back_pressure_object_threshold),
                )
                if value is not None
            ]
            if exc.status_code != 400 or not applied or "Invalid data size" not in (exc.body or ""):
                raise
            # StandardConnectionDAO.configureConnection sets flowFileExpiration and
            # backPressureObjectThreshold before DataUnit.parseDataSize throws, and
            # StandardNiFiServiceFacade does not restore them, so this is not a clean failure.
            _drop_last_log()
            raise await self.uncertain(
                "PUT",
                path,
                f"was refused for {DATA_SIZE_FIELD}, but NiFi sets {' and '.join(applied)} before it checks "
                "that field, so those may already be applied",
                json_body={"revision": {"version": version}},
                read_group=parent_id,
                status_code=400,
            ) from None

    async def delete_connection(
        self, connection_id: str, version: int, *, parent_id: str | None = None
    ) -> dict[str, Any]:
        return await self.json(
            "DELETE",
            f"/connections/{connection_id}",
            params={
                "version": version,
                "clientId": self.settings.client_id,
                "disconnectedNodeAcknowledged": str(self._ack()).lower(),
            },
            read_group=parent_id,
        )

    async def list_queue(self, connection_id: str, *, timeout_seconds: float = 15.0) -> dict[str, Any]:
        # A listing changes no flow state: a failure is a plain read error.
        started = await self.json(
            "POST",
            f"/flowfile-queues/{connection_id}/listing-requests",
            expected={200, 201, 202},
            mutation=False,
        )
        request = self._envelope(started, "listingRequest", "request")
        request_id = request.get("id") or request.get("requestId")
        if not request_id:
            raise NiFiError("Queue listing did not return a request id")
        path = f"/flowfile-queues/{connection_id}/listing-requests/{request_id}"
        try:
            return await self._poll_until_done(
                path,
                started,
                envelope_keys=("listingRequest", "request"),
                timeout_seconds=timeout_seconds,
                interval=0.25,
            )
        finally:
            # A listing is read-only, so removing it even when unfinished loses nothing.
            await self._delete_quietly(path)

    async def empty_queue(self, connection_id: str, *, timeout_seconds: float = 30.0) -> dict[str, Any]:
        submit = f"/flowfile-queues/{connection_id}/drop-requests"
        started = await self.json("POST", submit, expected={200, 201, 202})
        request = self._envelope(started, "dropRequest", "request")
        request_id = request.get("id") or request.get("requestId")
        if not request_id:
            raise await self.uncertain("POST", submit, "was accepted without a drop request id")
        last = await self._after_submit(
            "POST",
            submit,
            None,
            self._poll_until_done(
                f"{submit}/{request_id}",
                started,
                envelope_keys=("dropRequest", "request"),
                timeout_seconds=timeout_seconds,
                interval=0.25,
            ),
        )
        failure = self._async_failure(last, "dropRequest", "request")
        await self._delete_quietly(f"/flowfile-queues/{connection_id}/drop-requests/{request_id}")
        if failure:
            # A drop can fail after it has dropped some FlowFiles (SwappablePriorityQueue.dropFlowFiles).
            raise await self.uncertain("POST", submit, "failed while NiFi applied it", body=failure)
        return last

    # ── controller services ───────────────────────────────────────────────

    async def list_controller_service_types(self) -> dict[str, Any]:
        """Every installed type; substring search happens in the caller."""
        return await self.get_json("/flow/controller-service-types")

    async def list_controller_services(self, process_group_id: str) -> dict[str, Any]:
        return await self.get_json(f"/flow/process-groups/{process_group_id}/controller-services")

    async def get_controller_service(self, service_id: str) -> dict[str, Any]:
        return await self.get_json(f"/controller-services/{service_id}")

    async def create_controller_service(
        self,
        parent_id: str,
        service_type: str,
        name: str,
        *,
        properties: dict[str, str | None] | None = None,
        bundle: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        component: dict[str, Any] = {"type": service_type, "name": name}
        if bundle:
            component["bundle"] = bundle
        if properties:
            component["properties"] = properties
        return await self.json(
            "POST",
            f"/process-groups/{parent_id}/controller-services",
            json_body={
                "revision": self._revision(0),
                "disconnectedNodeAcknowledged": self._ack(),
                "component": component,
            },
        )

    async def update_controller_service(
        self,
        service_id: str,
        *,
        version: int,
        properties: dict[str, str | None] | None = None,
        name: str | None = None,
    ) -> dict[str, Any]:
        # Partial DTO: sending the GET properties back would store the "********" mask as the secret.
        component: dict[str, Any] = {"id": service_id}
        if name is not None:
            component["name"] = name
        if properties is not None:
            component["properties"] = properties
        return await self.json(
            "PUT",
            f"/controller-services/{service_id}",
            json_body={
                "revision": self._revision(version),
                "disconnectedNodeAcknowledged": self._ack(),
                "component": component,
            },
        )

    async def set_controller_service_state(
        self, service_id: str, state: str, version: int
    ) -> dict[str, Any]:
        return await self.json(
            "PUT",
            f"/controller-services/{service_id}/run-status",
            json_body={
                "revision": self._revision(version),
                "state": state,
                "disconnectedNodeAcknowledged": self._ack(),
            },
        )

    async def delete_controller_service(self, service_id: str, version: int) -> dict[str, Any]:
        return await self.json(
            "DELETE",
            f"/controller-services/{service_id}",
            params={
                "version": version,
                "clientId": self.settings.client_id,
                "disconnectedNodeAcknowledged": str(self._ack()).lower(),
            },
        )

    # ── ports ─────────────────────────────────────────────────────────────

    async def create_port(
        self, parent_id: str, kind: str, name: str, *, x: float = 0.0, y: float = 0.0
    ) -> dict[str, Any]:
        segment = "input-ports" if kind == "INPUT_PORT" else "output-ports"
        return await self.json(
            "POST",
            f"/process-groups/{parent_id}/{segment}",
            json_body={
                "revision": self._revision(0),
                "disconnectedNodeAcknowledged": self._ack(),
                "component": {"name": name, "position": {"x": x, "y": y}},
            },
        )

    async def update_port(
        self,
        port_id: str,
        *,
        version: int,
        kind: str,
        x: float,
        y: float,
        parent_id: str | None = None,
    ) -> dict[str, Any]:
        segment = "input-ports" if kind == "INPUT_PORT" else "output-ports"
        # AbstractPortDAO calls verifyCanUpdate (fails while running) when name or comments are non-null.
        component = {"id": port_id, "position": {"x": x, "y": y}}
        return await self.json(
            "PUT",
            f"/{segment}/{port_id}",
            json_body={
                "revision": self._revision(version),
                "disconnectedNodeAcknowledged": self._ack(),
                "component": component,
            },
            read_group=parent_id,
        )

    @staticmethod
    def _port_segment(kind: str) -> str:
        return "input-ports" if kind == "INPUT_PORT" else "output-ports"

    async def get_port(self, port_id: str, kind: str) -> dict[str, Any]:
        return await self.get_json(f"/{self._port_segment(kind)}/{port_id}")

    async def delete_port(
        self, port_id: str, version: int, kind: str, *, parent_id: str | None = None
    ) -> dict[str, Any]:
        """InputPortResource / OutputPortResource.removeXPort: DELETE with the revision as query params."""
        return await self.json(
            "DELETE",
            f"/{self._port_segment(kind)}/{port_id}",
            params={
                "version": version,
                "clientId": self.settings.client_id,
                "disconnectedNodeAcknowledged": str(self._ack()).lower(),
            },
            read_group=parent_id,
        )

    # ── bulletins / parameters ────────────────────────────────────────────

    async def bulletins(self, after_id: int | None = None) -> dict[str, Any]:
        """FlowResource.getBulletinBoard: "after" includes bulletins with an id after this value."""
        params = {"after": after_id} if after_id is not None else None
        return await self.get_json("/flow/bulletin-board", params=params)

    async def list_parameter_contexts(self) -> dict[str, Any]:
        return await self.get_json("/flow/parameter-contexts")

    async def get_parameter_context(self, context_id: str) -> dict[str, Any]:
        return await self.get_json(f"/parameter-contexts/{context_id}")

    @staticmethod
    def _sensitive_names(context: dict[str, Any]) -> set[str]:
        return {name for name, sensitive in NiFiClient._sensitive_flags(context).items() if sensitive}

    @staticmethod
    def _sensitive_flags(context: dict[str, Any]) -> dict[str, bool]:
        """Name to sensitive flag for the context's own parameters (inherited ones are not in its map)."""
        flags: dict[str, bool] = {}
        for item in (context.get("component") or {}).get("parameters") or []:
            param = item.get("parameter") or item
            if param.get("inherited") is not True:
                flags[str(param.get("name"))] = param.get("sensitive") is True
        return flags

    @staticmethod
    def _refuse_unsupported_parameter_edits(
        parameters: list[dict[str, Any]] | None, remove: list[str] | None, flags: dict[str, bool]
    ) -> None:
        """Refuse what NiFi would reject or silently drop, before anything is posted.

        StandardParameterContext.validateSensitiveFlag rejects any change of an existing parameter's
        sensitive flag that carries a value, and StandardParameterContextDAO.getParameters fills a
        missing value from the context, so the flag cannot change in place at all. The DAO also keys
        one update by name, so a name both removed and re-added in one request keeps only one entry.
        """
        # Named by position: a parameter name is a submitted value.
        both = [f"parameters[{i}]" for i, item in enumerate(parameters or []) if item["name"] in set(remove or [])]
        if both:
            raise NiFiError(
                f"{', '.join(both)} also appear in remove; NiFi applies one entry per name. "
                "Remove it in one call, then add it in the next."
            )
        flipped = [
            f"parameters[{i}]"
            for i, item in enumerate(parameters or [])
            if item["name"] in flags
            and item.get("sensitive") is not None
            and bool(item["sensitive"]) != flags[item["name"]]
        ]
        if flipped:
            raise NiFiError(
                f"NiFi cannot change the sensitive flag of an existing parameter in place ({', '.join(flipped)}). "
                "Call nifi_update_parameter_context with the name in remove, then call it again to add the "
                "parameter with the new sensitive flag and a value. Referencing components are invalid "
                "between the two calls."
            )

    @staticmethod
    def _parameter_entities(
        parameters: list[dict[str, Any]] | None,
        remove: list[str] | None = None,
        *,
        sensitive_now: set[str] | None = None,
    ) -> list[dict[str, Any]]:
        """sensitive=None keeps the flag the parameter already has (False for a new one).

        NiFi adopts the submitted descriptor whenever a description is sent
        (StandardParameterContext.updateParameters), so a defaulted sensitive=false would demote a secret.
        """
        entities: list[dict[str, Any]] = []
        for item in parameters or []:
            sensitive = item.get("sensitive")
            if sensitive is None:
                sensitive = item["name"] in (sensitive_now or set())
            param: dict[str, Any] = {
                "name": item["name"],
                "value": item.get("value"),
                "sensitive": bool(sensitive),
            }
            if item.get("description") is not None:
                param["description"] = item["description"]
            entities.append({"parameter": param})
        # StandardParameterContextDAO treats a parameter with only a name as a deletion.
        entities.extend({"parameter": {"name": name}} for name in remove or [])
        return entities

    async def create_parameter_context(
        self,
        name: str,
        *,
        parameters: list[dict[str, Any]] | None = None,
        description: str | None = None,
    ) -> dict[str, Any]:
        component: dict[str, Any] = {"name": name, "parameters": self._parameter_entities(parameters)}
        if description is not None:
            component["description"] = description
        return await self.json(
            "POST",
            "/parameter-contexts",
            json_body={
                "revision": self._revision(0),
                "disconnectedNodeAcknowledged": self._ack(),
                "component": component,
            },
        )

    async def update_parameter_context(
        self,
        context_id: str,
        *,
        parameters: list[dict[str, Any]] | None = None,
        remove: list[str] | None = None,
        name: str | None = None,
        description: str | None = None,
        timeout_seconds: float = 60.0,
    ) -> dict[str, Any]:
        """Async update-request: NiFi stops and restarts referencing components itself."""
        current = await self.get_parameter_context(context_id)
        revision = current.get("revision") or {}
        flags = self._sensitive_flags(current)
        self._refuse_unsupported_parameter_edits(parameters, remove, flags)
        secret = {name for name, sensitive in flags.items() if sensitive}
        entities = self._parameter_entities(parameters, remove, sensitive_now=secret)
        component: dict[str, Any] = {"id": context_id, "parameters": entities}
        if name is not None:
            component["name"] = name
        if description is not None:
            component["description"] = description
        submit = f"/parameter-contexts/{context_id}/update-requests"
        body = {
            "revision": self._revision(int(revision.get("version") or 0)),
            "disconnectedNodeAcknowledged": self._ack(),
            "component": component,
        }
        started = await self.json("POST", submit, json_body=body, expected={200, 201, 202})
        request_id = self._envelope(started, "request").get("requestId")
        if not request_id:
            raise await self.uncertain("POST", submit, "was accepted without a requestId", json_body=body)
        path = f"{submit}/{request_id}"
        last = await self._after_submit(
            "POST",
            submit,
            body,
            self._poll_until_done(
                path, started, envelope_keys=("request",), timeout_seconds=timeout_seconds, interval=0.5
            ),
        )
        failure = self._async_failure(last, "request")
        await self._delete_quietly(path, params={"disconnectedNodeAcknowledged": str(self._ack()).lower()})
        if failure:
            # NiFi stopped part way: referencing components may have been stopped or restarted.
            raise await self.uncertain("POST", submit, "failed while NiFi applied it", json_body=body, body=failure)
        try:
            updated = await self.get_parameter_context(context_id)
        except NiFiError as exc:
            raise NiFiError(
                "The parameter context update was applied, but reading the context afterwards failed",
                path=exc.path,
                status_code=exc.status_code,
                outcome=Outcome.APPLIED,
                hint=f"Call nifi_get_parameter_context on {context_id} to see it.",
            ) from exc
        still_secret = secret - {str(e["parameter"]["name"]) for e in entities if not e["parameter"].get("sensitive")}
        return self._mask_values(updated, still_secret)

    @staticmethod
    def _mask_values(context: dict[str, Any], names: set[str]) -> dict[str, Any]:
        """Never hand back a value that was sensitive before this update and was not declassified."""
        for item in (context.get("component") or {}).get("parameters") or []:
            param = item.get("parameter") or item
            if param.get("name") in names and param.get("value") is not None:
                param["value"] = SENSITIVE_MASK
        return context

    async def delete_parameter_context(self, context_id: str, version: int) -> dict[str, Any]:
        return await self.json(
            "DELETE",
            f"/parameter-contexts/{context_id}",
            params={
                "version": version,
                "clientId": self.settings.client_id,
                "disconnectedNodeAcknowledged": str(self._ack()).lower(),
            },
        )

    # ── flow-as-code ──────────────────────────────────────────────────────

    async def download_flow(self, process_group_id: str, *, include_services: bool = False) -> dict[str, Any]:
        return await self.get_json(
            f"/process-groups/{process_group_id}/download",
            params={"includeReferencedServices": str(include_services).lower()},
        )

    async def invalid_values(
        self, process_group_id: str, *, include_ancestors: bool = False
    ) -> dict[str, dict[str, set[str]]]:
        """Every value NiFi reports invalid in a group and its descendants, by the identifier the
        component has in a flow snapshot (redaction.versioned_id), then by property name or field.

        A /download snapshot carries no validation state, so an export learns it here: the processors
        and controller services of every descendant group (the services of ancestor groups too when the
        export includes referenced services), and each remote process group the recursive status lists.
        Keyed by component, so a value is masked only where NiFi reports it invalid. Components that
        share a snapshot identifier (one flow imported twice) share one entry holding every such value.
        """
        processors = await self.get_json(
            f"/process-groups/{process_group_id}/processors", params={"includeDescendantGroups": "true"}
        )
        services = await self.get_json(
            f"/flow/process-groups/{process_group_id}/controller-services",
            params={"includeAncestorGroups": str(include_ancestors).lower(), "includeDescendantGroups": "true"},
        )
        status = await self.get_json(
            f"/flow/process-groups/{process_group_id}/status", params={"recursive": "true"}
        )
        remotes = [
            await self.get_json(f"/remote-process-groups/{remote_id}") for remote_id in _remote_group_ids(status)
        ]
        found: dict[str, dict[str, set[str]]] = {}
        for entity in [*processors.get("processors", []), *services.get("controllerServices", []), *remotes]:
            component = entity.get("component") if isinstance(entity, dict) else None
            if not isinstance(component, dict):
                continue
            values = invalid_values(component)
            # Components imported from one snapshot share a versionedComponentId: merge, never replace.
            for key in {versioned_id(component), component.get("id")} - {None}:
                for name, held in values.items():
                    found.setdefault(str(key), {}).setdefault(name, set()).update(held)
        return found

    async def import_flow(
        self,
        parent_id: str,
        snapshot: dict[str, Any],
        group_name: str,
        *,
        x: float = 0.0,
        y: float = 0.0,
    ) -> dict[str, Any]:
        body = {
            "groupId": parent_id,
            "groupName": group_name,
            "positionDTO": {"x": x, "y": y},
            "revisionDTO": self._revision(0),
            "disconnectedNodeAcknowledged": self._ack(),
            "flowSnapshot": snapshot,
        }
        return await self.json("POST", f"/process-groups/{parent_id}/process-groups/import", json_body=body)

    async def replace_flow(
        self,
        process_group_id: str,
        snapshot: dict[str, Any],
        *,
        timeout_seconds: float = 120.0,
    ) -> dict[str, Any]:
        details = await self.get_process_group(process_group_id)
        revision = details.get("revision") or {}
        submit = f"/process-groups/{process_group_id}/replace-requests"
        body = {
            "processGroupRevision": {
                "clientId": revision.get("clientId") or self.settings.client_id,
                "version": revision.get("version", 0),
            },
            "disconnectedNodeAcknowledged": self._ack(),
            "versionedFlowSnapshot": snapshot,
        }
        started = await self.json("POST", submit, json_body=body)
        request = self._envelope(started, "request")
        request_id = request.get("requestId")
        if not request_id:
            raise await self.uncertain("POST", submit, "was accepted without a requestId", json_body=body)
        last = await self._after_submit(
            "POST",
            submit,
            body,
            self._poll_until_done(
                f"/process-groups/replace-requests/{request_id}",
                started,
                envelope_keys=("request",),
                timeout_seconds=timeout_seconds,
                interval=0.5,
            ),
        )
        failure = self._async_failure(last, "request")
        await self._delete_quietly(
            f"/process-groups/replace-requests/{request_id}",
            params={"disconnectedNodeAcknowledged": str(self._ack()).lower()},
        )
        if failure:
            # A replace stops, removes and adds components in turn; a failure can leave part of it applied.
            raise await self.uncertain("POST", submit, "failed while NiFi applied it", json_body=body, body=failure)
        return last
