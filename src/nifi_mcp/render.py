"""The one place error, refusal and hint text is produced for the model.

A tool call's arguments are the values the model submitted. Any of them may be a secret pasted
into the wrong field, and NiFi, httpx, pydantic or this server's own wording could repeat one in an
error. A Renderer is built from those arguments, and every error payload a tool returns passes
through it: the text it emits holds key names, field names, types, status codes, ids and counts,
never a submitted value.

It works on values, not on patterns. Text from outside this server (a NiFi body, an exception
message) has every submitted value masked wherever it occurs. The payload as a whole then has each
submitted value masked where it stands as a word, which keeps this server's own wording intact. The
pattern masks in redaction.py and errors.scrub_text still run first, as defence in depth.
"""

from __future__ import annotations

import html
import json
import re
from collections.abc import Iterable
from typing import Any

from pydantic import BaseModel, ValidationError

from nifi_mcp.client import is_nifi_id, nifi_id_spans
from nifi_mcp.errors import NiFiError, describe_validation_error, scrub_text
from nifi_mcp.redaction import REDACT_MARK, Rendered, Vetted, mask_echoed_values

# Shorter strings are too common to mask without wrecking the text, and too short to be a secret.
_MIN_TEXT = 3
# A number is masked only when it is this many characters or more (a coordinate, a threshold),
# so a status code or a revision never is.
_MIN_NUMBER = 4
# Words this server and NiFi use for kinds, states and modes. A submitted value equal to one is the
# model naming a choice from a fixed list, never a secret, and masking it would wreck every sentence.
_VOCABULARY = frozenset(
    {
        "processor",
        "controller_service",
        "connection",
        "input_port",
        "output_port",
        "process_group",
        "parameter_context",
        "auto",
        "manual",
        "PROCESSOR",
        "FUNNEL",
        "INPUT_PORT",
        "OUTPUT_PORT",
        "REMOTE_INPUT_PORT",
        "REMOTE_OUTPUT_PORT",
        "RUNNING",
        "STOPPED",
        "DISABLED",
        "ENABLED",
        "RUN_ONCE",
        "TIMER_DRIVEN",
        "CRON_DRIVEN",
    }
)


# The structural fields of a result: this server's fixed vocabulary (status, outcome, cause, the
# exception type, a created[] item's kind, state and ref). They carry no submitted value, so a value
# equal to one of their words ("error", "unknown", "nifi") never blanks them.
STRUCTURAL_KEYS = frozenset({"status", "outcome", "cause", "type", "kind", "state", "ref"})


def _leaves(value: Any) -> Iterable[Any]:
    """Every scalar a tool call submitted: values of mappings (their keys are field names), items of
    lists, fields of argument models."""
    if isinstance(value, BaseModel):
        value = value.model_dump()
    if isinstance(value, dict):
        for item in value.values():
            yield from _leaves(item)
    elif isinstance(value, list | tuple | set):
        for item in value:
            yield from _leaves(item)
    else:
        yield value


def _number_forms(value: Any) -> set[str]:
    """A submitted number as it appears in text, if it is long enough to mask."""
    if isinstance(value, bool) or not isinstance(value, int | float):
        return set()
    text = json.dumps(value)
    return {text} if len(text) >= _MIN_NUMBER else set()


def _text_forms(value: Any) -> set[str]:
    """The ways a submitted string can appear in text: as sent, trimmed, line by line, JSON-escaped
    and HTML-escaped."""
    if not isinstance(value, str):
        return set()
    text = value.strip()
    if len(text) < _MIN_TEXT or text in _VOCABULARY or is_nifi_id(text):
        return set()
    bases = {text} | {line.strip() for line in text.splitlines()}
    forms = {
        form
        for base in bases
        for form in (
            base,
            json.dumps(base)[1:-1],
            html.escape(base),
            html.escape(base, quote=False),
            base.replace('"', "&quot;"),
        )
    }
    return {form for form in forms if len(form) >= _MIN_TEXT}


def _alternation(values: set[str]) -> str:
    # Longest first, so a value that contains another is masked whole.
    return "|".join(re.escape(value) for value in sorted(values, key=len, reverse=True))


def _mask(pattern: re.Pattern[str] | None, text: str) -> str:
    """Mask every match of pattern, except one that lies inside a NiFi id: an id is never a secret,
    and a submitted number (a coordinate, a threshold) can be a run of digits inside one."""
    if pattern is None:
        return text
    spans = nifi_id_spans(text)
    if not spans:
        return pattern.sub(REDACT_MARK, text)

    def replace(match: re.Match[str]) -> str:
        inside = any(start <= match.start() and match.end() <= end for start, end in spans)
        return match.group() if inside else REDACT_MARK

    return pattern.sub(replace, text)


class Renderer:
    """Builds every error, refusal and hint a tool returns, from the values its call submitted."""

    def __init__(self, *submitted: Any) -> None:
        leaves = list(_leaves(list(submitted)))
        # Whole submitted strings: where each echoed value ends when NiFi lists several sentences.
        self._known = frozenset(leaf for leaf in leaves if isinstance(leaf, str) and leaf)
        words = {form for leaf in leaves for form in _text_forms(leaf)}
        numbers = {form for leaf in leaves for form in _number_forms(leaf)}
        alternatives = _alternation(words)
        # Anywhere at all: for text from outside this server.
        self._anywhere = re.compile(alternatives, re.IGNORECASE) if words else None
        # Standing alone: for this server's own sentences, so "flow" never splits nifi_get_flow.
        self._as_word = re.compile(rf"(?<![\w-])(?:{alternatives})(?![\w-])", re.IGNORECASE) if words else None
        self._number = re.compile(rf"(?<![\d.])(?:{_alternation(numbers)})(?![\d.])") if numbers else None

    def foreign(self, text: str) -> str:
        """Text this server did not write (a NiFi body, an exception message): scrubbed by pattern,
        then every submitted value masked wherever it occurs."""
        text = scrub_text(text, self._known)
        text = _mask(self._anywhere, text)
        return _mask(self._number, text)

    def own(self, text: str) -> str:
        """This server's own wording: every submitted value that stands as a word is masked."""
        masked = _mask(self._number, _mask(self._as_word, text))
        return Rendered(masked) if isinstance(text, Rendered) else masked

    def error_text(self, exc: BaseException) -> str:
        """What an exception may say to the model."""
        if isinstance(exc, ValidationError):
            prefix = "NIFI_" if exc.title == "Settings" else ""
            return Rendered(self.foreign(describe_validation_error(exc, env_prefix=prefix)))
        if not isinstance(exc, NiFiError):
            return Rendered(self.foreign(str(exc)))
        message = str(exc.args[0]) if exc.args else type(exc).__name__
        parts = [self.own(mask_echoed_values(message, self._known))]
        if exc.status_code is not None:
            parts.append(f"HTTP {exc.status_code}")
        if exc.path:
            parts.append(self.foreign(exc.path))
        if exc.body:
            # Masked before the cut, so a cut can never leave part of a value behind.
            parts.append(self.foreign(exc.body)[:2000])
        # Every part is masked with this call's values; _dump's redact must not mask it again without them.
        return Rendered(" | ".join(parts))

    def payload(self, value: Any) -> Any:
        """An error payload with every string in it rendered as this server's own wording, except the
        structural fields, which are never prose."""
        if isinstance(value, Vetted):
            return Vetted({key: self.payload(item) for key, item in value.items()})
        if isinstance(value, dict):
            return {key: item if key in STRUCTURAL_KEYS else self.payload(item) for key, item in value.items()}
        if isinstance(value, list):
            return [self.payload(item) for item in value]
        if isinstance(value, str):
            return self.own(value)
        return value

    def tool_error(
        self,
        exc: BaseException,
        *,
        outcome: str | None = None,
        hint: str | None = None,
        applied: list[str] | None = None,
    ) -> dict[str, Any]:
        """The error payload of a tool call: {status, error, type} and, for a tool that changes NiFi,
        outcome, hint and the requests of this call that were applied before it failed."""
        out: dict[str, Any] = {"status": "error", "error": self.error_text(exc), "type": type(exc).__name__}
        if outcome is not None:
            out["outcome"] = outcome
        if applied:
            out["applied_requests"] = applied
        if hint:
            out["hint"] = hint
        return self.payload(out)
