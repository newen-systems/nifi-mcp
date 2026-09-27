"""Typed errors for the NiFi REST client and MCP tools."""

from __future__ import annotations

import json
import re
from collections.abc import Iterable
from enum import StrEnum

from pydantic import ValidationError

from nifi_mcp.redaction import mask_echoed_values, redact

_MARK = "***REDACTED***"
# A run of key characters; a run holding a credential word is a key when [:=] follows it.
_KEY_RUN = re.compile(r"[\w.-]+")
_SECRET_WORD = re.compile(r"password|passwd|secret|token|api[_-]?key|private[_-]?key|credential", re.IGNORECASE)
_PAIR_JOIN = re.compile(r"""["']?\s*[:=]\s*""")
_BARE_VALUE = re.compile(r"[^\s,&}]+")


def scrub_text(text: str, known: Iterable[str] = ()) -> str:
    """Redact credential-looking values and NiFi's echoed values from free text or a JSON body.

    `known` are values the caller knows the call submitted: with the call's redaction.KNOWN, the proof of
    where each echoed value ends (mask_echoed_values). Linear in the length of the text, so a whole
    error body is scrubbed before it is cut."""
    try:
        parsed = json.loads(text)
    except ValueError:
        parsed = None
    if isinstance(parsed, dict | list):
        # Each string is echo-masked where it stands; a second pass over the dump would read the
        # sentences split there as values that cannot be told apart.
        return _mask_secret_pairs(json.dumps(redact(parsed, _known=frozenset(known))))
    return _mask_secret_pairs(mask_echoed_values(text, known))


def _mask_secret_pairs(text: str) -> str:
    """key=value or "key": "value" pairs whose key holds a credential word.

    Each key run is read once and each masked value is skipped, so the cost is linear in the text
    (a backtracking [\\w.-]* before the keyword was quadratic on a long unbroken token)."""
    last_quote = {quote: text.rfind(quote) for quote in "\"'"}
    out: list[str] = []
    pos = 0
    for run in _KEY_RUN.finditer(text):
        if run.start() < pos or not _SECRET_WORD.search(run.group()):
            continue
        join = _PAIR_JOIN.match(text, run.end())
        if join is None or join.end() >= len(text):
            continue
        start = join.end()
        quote = text[start] if text[start] in last_quote else ""
        end = text.find(quote, start + 1) + 1 if quote and last_quote[quote] > start else 0
        if not end:
            bare = _BARE_VALUE.match(text, start)
            if bare is None:
                continue
            end = bare.end()
        out += [text[pos:start], f"{quote}{_MARK}{quote}"]
        pos = end
    out.append(text[pos:])
    return "".join(out)


def describe_validation_error(exc: ValidationError, *, env_prefix: str = "") -> str:
    """Field names and messages only. str(ValidationError) embeds input_value, which can hold secrets."""
    parts: list[str] = []
    for err in exc.errors(include_input=False, include_url=False):
        loc = ".".join(str(part) for part in err.get("loc") or ()) or "(root)"
        hint = f" (set {env_prefix}{loc.upper()})" if env_prefix and err.get("type") == "missing" else ""
        parts.append(f"{loc}: {err.get('msg')}{hint}")
    return f"{exc.error_count()} validation error(s) for {exc.title}: " + "; ".join(parts)


def safe_error_message(exc: BaseException) -> str:
    """The only text an exception may contribute to tool output or logs."""
    if isinstance(exc, ValidationError):
        prefix = "NIFI_" if exc.title == "Settings" else ""
        return describe_validation_error(exc, env_prefix=prefix)
    return scrub_text(str(exc))


class Outcome(StrEnum):
    """What a mutation did to NiFi. Every mutation the client sends ends in exactly one of these."""

    # NiFi answered 2xx: the change is in.
    APPLIED = "applied"
    # Nothing changed: the request was never written (no connection, no pool slot) or NiFi refused it (4xx).
    NOT_APPLIED = "not_applied"
    # No definite answer (a timeout after sending, a lost connection, a 5xx): NiFi may have applied it.
    # The client reads the state back before it writes the hint.
    UNKNOWN = "unknown"


class NiFiError(Exception):
    """A NiFi API or MCP-layer failure with an actionable message.

    outcome is set on every error a mutation raises (None for a read). hint is what to do next,
    written from what this call knows: for an unknown outcome, what the read-back showed.
    """

    default_outcome: Outcome | None = None

    def __init__(
        self,
        message: str,
        *,
        status_code: int | None = None,
        path: str | None = None,
        body: str | None = None,
        outcome: Outcome | None = None,
        hint: str | None = None,
    ) -> None:
        self.status_code = status_code
        self.path = path
        self.body = body
        self.outcome = outcome or self.default_outcome
        self.hint = hint
        super().__init__(message)

    def __str__(self) -> str:
        parts = [super().__str__()]
        if self.status_code is not None:
            parts.append(f"HTTP {self.status_code}")
        if self.path:
            parts.append(self.path)
        if self.body:
            # Scrub first: a cut can split a value from the phrase that marks it as one.
            parts.append(scrub_text(self.body)[:2000])
        return ": ".join(parts)


class NiFiUncertainError(NiFiError):
    """A mutation got no answer that says whether NiFi applied it: a client timeout, a 5xx, or a
    connection lost after the request was sent. It is never retried. Its outcome is always unknown,
    and its hint says which read shows the current state and what that read showed."""

    default_outcome = Outcome.UNKNOWN


class NiFiTimeoutError(NiFiUncertainError):
    """No response in time. On a GET nothing changed (outcome None); on a mutation NiFi may or may
    not have applied it, and the client sets outcome unknown."""

    default_outcome = None


class NiFiAuthError(NiFiError):
    """Authentication failed or no credentials were configured."""


class NiFiReadOnlyError(NiFiError):
    """A mutating tool was called while NIFI_READONLY=true."""


class NiFiConflictError(NiFiError):
    """HTTP 409: revision mismatch, running component, or wrong state."""
