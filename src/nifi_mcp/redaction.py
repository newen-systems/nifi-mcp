"""Strip secrets from NiFi JSON before it enters the LLM context."""

from __future__ import annotations

import bisect
import hashlib
import re
import uuid
from collections.abc import Iterable
from contextvars import ContextVar
from typing import Any

REDACT_MARK = "***REDACTED***"
_REDACT_KEYS = frozenset(
    {
        "password",
        "passwd",
        "passcode",
        "secret",
        "token",
        "apikey",
        "api-key",
        "api_key",
        "privatekey",
        "private-key",
        "keystorepasswd",
        "truststorepasswd",
        "sslkeystorepasswd",
        "kerberoskeytab",
        "accesskey",
        "secretkey",
        "awsaccesskeyid",
        "awssecretaccesskey",
    }
)
_DEFAULT_MAX_ITEMS = 200

# NiFi sentences that quote the value they rejected, as (opener, closer, mask to the end when no
# closer follows). The quoted value may be a secret pasted into the wrong field, and NiFi inserts it
# with no escaping, so the value can itself hold a closer, a reason and the next sentence's opener.
_ECHOES = (
    # nifi-api ValidationResult.toString: "'<subject>' validated against '<input>' is invalid because
    # <explanation>". DtoFactory copies it into validationErrors on every processor and service read, and
    # an error body can list several: "Unable to start: ['A' validated against ..., 'B' validated ...]".
    (re.compile(r"\bvalidated against '"), re.compile(r"' is (?:in)?valid because"), True),
    # StandardProcessorDAO: "Scheduling Period '<value>' is not a valid cron expression: ...".
    (re.compile(r"\bScheduling Period '"), re.compile(r"' is not a valid"), True),
    # JsonContentConversionExceptionMapper: "The provided <field> value '<raw>' is not of required type".
    (re.compile(r"\bThe provided \S+ value '"), re.compile(r"' is not of required type"), True),
    (re.compile(r"\bvalue '"), re.compile(r"' is not of required type"), False),
    # FormatUtils.getTimeDuration: "Value '<value>' is not a valid time duration".
    (re.compile(r"\bValue '"), re.compile(r"' is not a valid time"), False),
    # DataUnit.parseDataSize: "Invalid data size: <value>", the value ends the message.
    (re.compile(r"\bInvalid data size: "), None, True),
)
# Said in place of sentences whose values cannot be told apart from their reasons (a value that holds
# the opener of the next sentence), so no subject is paired with another sentence's reason.
UNSPLIT_NOTE = (
    " [NiFi listed several sentences that cannot be told apart safely; "
    "read validation_errors on the component for each one]"
)

# The values this tool call has seen components hold (client.remember) or submitted: the only proof of
# where a quoted value ends when a body lists several sentences. _tool in server.py opens one per call.
KNOWN: ContextVar[set[str] | None] = ContextVar("nifi_mcp_known_values", default=None)
# Stands for a value masked by an earlier shape while the shapes run, so a later shape knows it is whole.
# A text that already holds a NUL cannot use it, and is masked as if nothing were known.
_HOLD = "\x00" + "REDACTED" + "\x00"

# A masked value shorter than this is not looked for again in the rest of the text: too common to
# mask without wrecking the reason, and too short to be a secret.
_MIN_COPY = 3
# NiFi lists a handful of validation sentences in one body; a body with more is not NiFi's, and every
# quoted value in it is masked in place already.
_MAX_COPIES = 64
# A copy is masked only where it stands as a whole value: bounded by the start or end of the text,
# whitespace, a quote or punctuation. Inside a longer token ("flowfile-content", "a/b") it is kept.
_EDGE = r"\s'\"`,.;:!?()\[\]{}<>"
# The subject of a ValidationResult sentence: a property's display name, never masked as a copy.
_SUBJECT = re.compile(r"'([^']*)' validated against '")


class Rendered(str):
    """Text already masked with the values its call knows (render.Renderer.error_text).

    mask_echoed_values returns it as it is: a second pass has lost that proof, and would read the
    sentences the first pass split as values that cannot be told apart."""


def remember(obj: Any) -> None:
    """Record the property values and _INVALID_FIELDS values a NiFi entity holds in this call's KNOWN."""
    known = KNOWN.get()
    if known is not None:
        known |= held_values(obj)


def held_values(obj: Any) -> set[str]:
    """Every property value and _INVALID_FIELDS value in a NiFi entity, DTO or snapshot."""
    found: set[str] = set()
    stack = [obj]
    while stack:
        item = stack.pop()
        if isinstance(item, list):
            stack.extend(item)
        elif isinstance(item, dict):
            for key, value in item.items():
                if key == "properties" and isinstance(value, dict):
                    found |= {text for text in value.values() if isinstance(text, str)}
                elif key in _FIELDS and isinstance(value, str):
                    found.add(value)
                elif isinstance(value, dict | list):
                    stack.append(value)
    return found


def _last_closer(closers: list[int], start: int, end: int) -> int | None:
    """The last closer that begins in [start, end), or None."""
    k = bisect.bisect_left(closers, end) - 1
    return closers[k] if k >= 0 and closers[k] >= start else None


def _proven_ends(
    text: str, openers: list[re.Match[str]], closers: list[int], known: list[str]
) -> dict[int, int] | None:
    """Where each sentence's value ends, by the index of its opener, if a known value proves every one.

    A known value proves where the value at its opener ends only where it stands whole there: a closer
    follows it, and no other closer follows before the next sentence's opener or the end of the text.
    A second closer means the known value may be a proper prefix of the quoted one, with the rest of
    the value read as the reason. The longest known value that proves it is taken. One unproven value
    proves none: the value before it could run on through the text between them. Else None."""
    closer_at = set(closers)
    ends: dict[int, int] = {}
    pos = 0
    for i, match in enumerate(openers):
        if match.start() < pos:
            # Inside a value proven whole.
            continue
        start = match.end()
        for value in known:
            end = start + len(value)
            if end not in closer_at or not text.startswith(value, start):
                continue
            after = next((other.start() for other in openers[i + 1 :] if other.start() >= end), len(text))
            if _last_closer(closers, end + 1, after) is None:
                ends[i] = pos = end
                break
        else:
            return None
    return ends


def _mask_between(
    text: str,
    opener: re.Pattern[str],
    closer: re.Pattern[str] | None,
    to_end: bool,
    known: list[str],
) -> tuple[str, set[str]]:
    """Mask each sentence's own value; the subject and reason of every sentence stay with each other.

    NiFi does not escape the value, so a closer, a reason and a later opener can all be part of it.
    A sentence's value is proven to end at a closer only when a value this call knows (`known`, longest
    first) stands whole between its opener and that closer, no other closer follows before the next
    opener, and every other sentence's value is proven too (_proven_ends); the text after it is then
    NiFi's reason. A known proper prefix of the value proves nothing. An unproven value is masked to
    the last closer when no later opener follows, or to the end of the text when there is no closer
    and `to_end`. An unproven value with a later opener after it cannot be
    told apart from the sentences that follow: everything from that value on is masked and
    UNSPLIT_NOTE said instead. A matching subject or reason before the later opener proves nothing,
    since the value can carry both.

    Also returns the values it masked, so their copies elsewhere can be masked too."""
    openers = list(opener.finditer(text))
    if not openers:
        return text, set()
    closers = [match.start() for match in closer.finditer(text)] if closer is not None else []
    ends = _proven_ends(text, openers, closers, known) or {}
    out: list[str] = []
    masked: set[str] = set()
    pos = 0
    for i, match in enumerate(openers):
        start = match.end()
        if match.start() < pos:
            # Inside a value proven whole and masked already.
            continue
        end = ends.get(i)
        if end is not None:
            out += [text[pos:start], _HOLD]
            masked.add(text[start:end])
            pos = end
            continue
        close = _last_closer(closers, start, len(text))
        later = any(other.start() >= start for other in openers[i + 1 :])
        if later and (close is not None or to_end):
            for other in openers[i:]:
                piece = _last_closer(closers, other.end(), len(text))
                if piece is not None and other.end() <= piece:
                    masked.add(text[other.end() : piece])
            out += [text[pos:start], _HOLD, UNSPLIT_NOTE]
            pos = len(text)
            break
        if close is not None:
            out += [text[pos:start], _HOLD]
            masked.add(text[start:close])
            pos = close
        elif to_end:
            out += [text[pos:start], _HOLD]
            pos = len(text)
            break
    out.append(text[pos:])
    return "".join(out), masked


def _mask_copies(text: str, values: list[str]) -> str:
    """Mask each value where it stands as a whole value, except inside a sentence's subject."""
    pattern = re.compile(rf"(?<![^{_EDGE}])(?:{'|'.join(re.escape(value) for value in values)})(?![^{_EDGE}])")
    subjects = [match.span(1) for match in _SUBJECT.finditer(text)]

    def replace(match: re.Match[str]) -> str:
        k = bisect.bisect_right(subjects, (match.start(), len(text))) - 1
        inside = k >= 0 and subjects[k][0] <= match.start() and match.end() <= subjects[k][1]
        return match.group() if inside else REDACT_MARK

    return pattern.sub(replace, text)


def mask_echoed_values(text: str, known: Iterable[str] = ()) -> str:
    """Mask the value in every NiFi sentence that quotes a submitted value; keep the subject and the reason.

    `known` are values the caller knows a component holds or the call submitted; with this call's KNOWN
    they are the only proof of where a value ends when a body lists several sentences (_mask_between).

    NiFi can repeat the value in the reason too: a controller service property "validated against
    'x' is invalid because Invalid Controller Service: x is not a valid Controller Service Identifier"
    (AbstractComponentNode). Every copy of a masked value that stands as a whole value elsewhere in
    the text is masked as well; one inside a longer token (an allowed value "flowfile-content" when
    the value was "content") or inside a sentence's subject is kept."""
    if isinstance(text, Rendered) or not any(opener.search(text) for opener, _closer, _to_end in _ECHOES):
        return text
    values = set(known) | (KNOWN.get() or set())
    if "\x00" not in text:
        values.add(_HOLD)
    proof = sorted(values, key=len, reverse=True)
    masked: set[str] = set()
    for opener, closer, to_end in _ECHOES:
        text, found = _mask_between(text, opener, closer, to_end, proof)
        masked |= found
    text = text.replace(_HOLD, REDACT_MARK)
    copies = sorted(
        {
            value.strip()
            for value in masked
            if len(value.strip()) >= _MIN_COPY and REDACT_MARK not in value and _HOLD not in value
        },
        key=len,
        reverse=True,
    )
    if not copies:
        return text
    # One pass, longest first, so a value that contains another is masked whole.
    return _mask_copies(text, copies[:_MAX_COPIES])


class Vetted(dict[str, Any]):
    """A mapping whose keys are names the model chose and whose values are known not to be secrets
    (component ids in name_map). redact() returns it unchanged, so a processor named FetchToken
    keeps its id."""


def _key_is_sensitive(key: str) -> bool:
    lowered = key.lower().replace(" ", "").replace("_", "").replace("-", "")
    if lowered in _REDACT_KEYS:
        return True
    if any(word in lowered for word in ("password", "secret", "privatekey")):
        return True
    # "token" names a credential only as the last word ("Refresh Token", "api_token"). As a qualifier
    # it does not: "Token Endpoint URL" is a URL, "Access Token Provider" a controller service id.
    return lowered.endswith("token")


def _sensitive_properties(obj: dict[str, Any]) -> set[str]:
    """Property names a NiFi DTO's own descriptors mark sensitive.

    ProcessorConfigDTO and ControllerServiceDTO carry properties next to descriptors. DtoFactory
    already sends those values as ******** (SENSITIVE_VALUE_MASK); masking them here as well means a
    value never leaks through a DTO that NiFi did not mask.
    """
    descriptors = obj.get("descriptors")
    if not isinstance(obj.get("properties"), dict) or not isinstance(descriptors, dict):
        return set()
    return {
        str(key)
        for key, descriptor in descriptors.items()
        if isinstance(descriptor, dict) and descriptor.get("sensitive") is True
    }


def redact_properties(properties: dict[str, Any], sensitive: set[str]) -> dict[str, Any]:
    """A property map: mask values whose descriptor is sensitive or whose name is a credential."""
    return {
        key: REDACT_MARK if value is not None and (key in sensitive or _key_is_sensitive(key)) else value
        for key, value in properties.items()
    }


_SUBJECT_END = "' validated against '"


def _invalid_subjects(errors: list[Any]) -> set[str]:
    """The subjects of ValidationResult sentences ("'<subject>' validated against ..."): for a
    property, its display name."""
    subjects = set()
    for error in errors:
        if isinstance(error, str) and error.startswith("'") and (end := error.find(_SUBJECT_END)) > 0:
            subjects.add(error[1:end])
    return subjects


# Fields NiFi validates with a subject of their own rather than a property's display name, as
# (subject, the key of the DTO part that holds the field or None for the DTO itself, field).
_INVALID_FIELDS = (
    # StandardProcessorNode.validateConfig: RUN_SCHEDULE, input schedulingPeriod. DtoFactory copies
    # the period into ProcessorConfigDTO, and a VersionedProcessor holds it at its top level.
    ("Run Schedule", "config", "schedulingPeriod"),
    # StandardRemoteProcessGroup: "Network Interface Name", input the interface name.
    ("Network Interface Name", None, "localNetworkInterface"),
)
_FIELDS = frozenset(field for _subject, _key, field in _INVALID_FIELDS)


def invalid_values(component: dict[str, Any]) -> dict[str, set[str]]:
    """The values a component DTO holds that NiFi reports invalid, by property name or field.

    ProcessorDTO holds properties under config, ControllerServiceDTO on the component itself; both key
    them by name, while the validation sentence names the display name. A field NiFi validates with
    its own subject (_INVALID_FIELDS) is keyed by the field."""
    subjects = _invalid_subjects(component.get("validationErrors") or [])
    found: dict[str, set[str]] = {}
    if not subjects:
        return found
    for holder in (component, component.get("config")):
        if not isinstance(holder, dict) or not isinstance(holder.get("properties"), dict):
            continue
        descriptors = holder.get("descriptors") if isinstance(holder.get("descriptors"), dict) else {}
        for name, value in holder["properties"].items():
            descriptor = descriptors.get(name) if isinstance(descriptors.get(name), dict) else {}
            if isinstance(value, str) and (name in subjects or descriptor.get("displayName") in subjects):
                found.setdefault(name, set()).add(value)
    for subject, key, field in _INVALID_FIELDS:
        holder = component if key is None else component.get(key)
        if subject in subjects and isinstance(holder, dict) and isinstance(holder.get(field), str):
            found.setdefault(field, set()).add(holder[field])
    return found


def mask_values(obj: Any, found: dict[str, set[str]]) -> Any:
    """obj with every property or _INVALID_FIELDS field masked where it holds a value in `found` under
    the same name: a component DTO, or a flow snapshot whose components carry no validation state."""
    if not found:
        return obj
    if isinstance(obj, list):
        return [mask_values(item, found) for item in obj]
    if not isinstance(obj, dict):
        return obj
    out: dict[str, Any] = {}
    for key, value in obj.items():
        if key == "properties" and isinstance(value, dict):
            out[key] = {
                name: REDACT_MARK if isinstance(item, str) and item in found.get(name, ()) else item
                for name, item in value.items()
            }
        elif key in _FIELDS and isinstance(value, str) and value in found.get(key, ()):
            out[key] = REDACT_MARK
        else:
            out[key] = mask_values(value, found)
    return out


def versioned_id(component: dict[str, Any]) -> str | None:
    """The identifier a flow snapshot gives a live component DTO: its versionedComponentId, else the
    one NiFi derives from its instance id (ComponentIdLookup.VERSIONED_OR_GENERATE,
    VersionedComponentFlowMapper.generateVersionedComponentId: UUID.nameUUIDFromBytes, an MD5 UUID)."""
    if isinstance(component.get("versionedComponentId"), str):
        return component["versionedComponentId"]
    if not isinstance(component.get("id"), str):
        return None
    return str(uuid.UUID(bytes=hashlib.md5(component["id"].encode()).digest(), version=3))  # noqa: S324


def mask_components(obj: Any, by_component: dict[str, dict[str, set[str]]]) -> Any:
    """A flow snapshot with each component's values masked where NiFi reports them invalid on that
    component (mask_values), matched by its identifier (versioned_id) or its instanceIdentifier. A
    component that holds the same value validly keeps it."""
    if not by_component:
        return obj
    if isinstance(obj, list):
        return [mask_components(item, by_component) for item in obj]
    if not isinstance(obj, dict):
        return obj
    found = by_component.get(str(obj.get("identifier"))) or by_component.get(str(obj.get("instanceIdentifier")))
    if found:
        obj = mask_values(obj, found)
    return {key: mask_components(value, by_component) for key, value in obj.items()}


def mask_invalid_properties(component: dict[str, Any]) -> dict[str, Any]:
    """A component DTO whose values NiFi reports invalid are masked: a property whose display name is
    the subject of a validation sentence, and a field NiFi validates with its own subject (a processor's
    Run Schedule, a remote process group's Network Interface Name).

    NiFi keeps an invalid value and repeats it on every read; it may be a secret pasted into the wrong
    field. Every other value stays, so a read still shows the configuration the model set."""
    return mask_values(component, invalid_values(component))


def _value_is_secret(obj: dict[str, Any]) -> bool:
    """A {name, value} pair such as a ParameterDTO or VersionedParameter that holds a secret.

    NiFi returns a non-sensitive parameter's real value (DtoFactory), so a parameter named
    db_password keeps its secret under the key "value" unless the sibling name or flag is checked.
    """
    if obj.get("value") is None:
        return False
    if obj.get("sensitive") is True:
        return True
    name = obj.get("name")
    return isinstance(name, str) and _key_is_sensitive(name)


def redact(
    obj: Any, *, max_items: int = _DEFAULT_MAX_ITEMS, _echo: bool = True, _known: frozenset[str] = frozenset()
) -> Any:
    """Recursively redact sensitive keys, secret name/value pairs, and truncate long lists.

    Every other string is NiFi text or the model's own, so NiFi sentences that quote a submitted value
    (validationErrors, bulletins, error bodies) are masked here, at the one boundary every tool result
    passes. Property maps hold the model's values, not NiFi sentences, so they are left as sent,
    except a value NiFi reports invalid (mask_invalid_properties). The values a component DTO holds prove
    where each value in its validationErrors ends (mask_echoed_values).
    """
    if isinstance(obj, Vetted):
        return obj
    if isinstance(obj, dict):
        if isinstance(obj.get("validationErrors"), list):
            _known = _known | held_values(obj)
            obj = mask_invalid_properties(obj)
        secret_value = _value_is_secret(obj)
        sensitive = _sensitive_properties(obj)
        return {
            key: REDACT_MARK
            if _key_is_sensitive(key) or (secret_value and key == "value")
            else redact_properties(value, sensitive)
            if sensitive and key == "properties"
            else redact(value, max_items=max_items, _echo=_echo and key != "properties", _known=_known)
            for key, value in obj.items()
        }
    if isinstance(obj, list):
        trimmed = [redact(item, max_items=max_items, _echo=_echo, _known=_known) for item in obj[:max_items]]
        omitted = len(obj) - max_items
        if omitted > 0:
            trimmed.append({"truncated": True, "omitted_count": omitted})
        return trimmed
    if _echo and isinstance(obj, str):
        return mask_echoed_values(obj, _known)
    return obj
