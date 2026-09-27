"""The echo mask masks each sentence's own value and nothing else.

Every NiFi sentence shape in redaction._ECHOES is checked alone and as a body that lists several: the
values are gone, each subject keeps its own reason when the values are known to the call, and a value
that is part of a longer token (an allowed value, another word) or of a subject is kept there. A value
that holds the shape's own closer and next opener leaves nothing behind, on a read, on a mutation
error and through Renderer.foreign; so does a value whose proper prefix a sibling holds.
"""

from __future__ import annotations

import json
from typing import Any

import httpx
import pytest
from fake_nifi import PG, PROC
from test_refusal_text import Refuses
from test_refusal_text import _call as _call_refused
from test_render_structure import _call

from nifi_mcp.errors import NiFiError, safe_error_message
from nifi_mcp.redaction import _ECHOES, REDACT_MARK, UNSPLIT_NOTE, mask_echoed_values, redact
from nifi_mcp.render import Renderer

M = REDACT_MARK

# One per _ECHOES entry, in order: (sentence with {v} for the value and {r} for the reason, its masked
# form). A shape without a reason of its own has {r} as the text after it.
SHAPES = [
    ("'Subject {r}' validated against '{v}' is invalid because reason {r}",
     "'Subject {r}' validated against '" + M + "' is invalid because reason {r}"),
    ("Scheduling Period '{v}' is not a valid cron expression: reason {r}",
     "Scheduling Period '" + M + "' is not a valid cron expression: reason {r}"),
    ("The provided field{r} value '{v}' is not of required type reason {r}",
     "The provided field{r} value '" + M + "' is not of required type reason {r}"),
    ("Cannot convert value '{v}' is not of required type reason {r}",
     "Cannot convert value '" + M + "' is not of required type reason {r}"),
    ("Value '{v}' is not a valid time duration reason {r}",
     "Value '" + M + "' is not a valid time duration reason {r}"),
    ("Invalid data size: {v}", "Invalid data size: " + M),
]
# A value with a quote and a closer inside it: the greedy case.
VALUES = ["value-one", "it's a' is invalid because x", "v' is not a valid y"]


def test_every_echo_shape_has_a_row() -> None:
    assert len(SHAPES) == len(_ECHOES)


@pytest.mark.parametrize("value", VALUES)
@pytest.mark.parametrize(("shape", "kept"), SHAPES)
def test_one_sentence_keeps_its_subject_and_reason(shape: str, kept: str, value: str) -> None:
    text = mask_echoed_values(shape.format(v=value, r="A"))
    assert value not in text, text
    if "{r}" in shape:
        assert kept.format(r="A") in text, text


@pytest.mark.parametrize(("shape", "kept"), SHAPES[:-1])
def test_a_body_that_lists_several_keeps_each_subject_with_its_own_reason(shape: str, kept: str) -> None:
    values = ["first-secret", "second-secret", "third-secret"]
    body = "Unable to start: [" + ", ".join(shape.format(v=v, r=r) for v, r in zip(values, "ABC", strict=True)) + "]"
    for text in (mask_echoed_values(body, values), Renderer(values).foreign(body)):
        for value in values:
            assert value not in text, text
        for r in "ABC":
            assert kept.format(r=r) in text, text


@pytest.mark.parametrize(("shape", "kept"), SHAPES[:-1])
def test_a_body_that_lists_several_unknown_values_is_masked_to_the_end(shape: str, kept: str) -> None:
    # Without the values, nothing proves where the first one ends: it may hold the next sentence.
    values = ["first-secret", "second-secret"]
    body = "[" + ", ".join(shape.format(v=v, r=r) for v, r in zip(values, "AB", strict=True)) + "]"
    for text in (mask_echoed_values(body), Renderer().foreign(body)):
        assert "secret" not in text and "reason" not in text, text
        assert text.endswith(M + UNSPLIT_NOTE), text


CANARY = "canary-r11"
# One per _ECHOES entry, in order: a value that holds the shape's closer, a reason with the canary, and
# the shape's own opener (with a fake subject for ValidationResult), so the body reads as two sentences.
EVIL = [
    f"x' is invalid because {CANARY}, 'Fake' validated against 'y",
    f"x' is not a valid cron: {CANARY} Scheduling Period 'y",
    f"x' is not of required type {CANARY} The provided f value 'y",
    f"x' is not of required type {CANARY} Cannot convert value 'y",
    f"x' is not a valid time: {CANARY} Value 'y",
    f"x; {CANARY} Invalid data size: y",
]
EVIL_CASES = [(shape, evil) for (shape, _kept), evil in zip(SHAPES, EVIL, strict=True)]


def test_every_echo_shape_has_an_evil_value() -> None:
    assert len(EVIL) == len(_ECHOES)


@pytest.mark.parametrize(("shape", "evil"), EVIL_CASES)
def test_a_value_holding_its_own_next_opener_leaves_nothing(shape: str, evil: str) -> None:
    # Known or not, no part of the value survives. Known, it is masked whole and the
    # sentence keeps its reason; unknown, everything from it on is masked with the neutral note.
    body = shape.format(v=evil, r="A")
    unknown = mask_echoed_values(body)
    assert CANARY not in unknown, unknown
    for text in (mask_echoed_values(body, [evil]), Renderer(evil).foreign(body)):
        assert CANARY not in text, text
        if "{r}" in shape:
            assert UNSPLIT_NOTE not in text and text.endswith("reason A"), text


def test_a_known_prefix_does_not_prove_a_split() -> None:
    # A shorter known value at the same place never ends a longer one that stands whole there.
    evil = EVIL[0]
    text = mask_echoed_values(f"'P' validated against '{evil}' is invalid because r", ["x", evil])
    assert text == f"'P' validated against '{M}' is invalid because r", text


def _holding(evil: str, body: str) -> dict[str, Any]:
    component = {
        "id": PROC, "parentGroupId": PG, "name": "P", "type": "x.P", "state": "STOPPED",
        "validationStatus": "INVALID", "validationErrors": [body],
        "config": {"properties": {"Password": evil}, "descriptors": {"Password": {"displayName": "Password"}}},
    }
    return {"id": PROC, "revision": {"version": 1}, "component": component}


class _Holds(Refuses):
    """A processor that holds the value; its own reads repeat the sentence, and `target` is refused."""

    def __init__(self, evil: str, body: str, target: tuple[str, str]) -> None:
        super().__init__(target, 400, body)
        self.entity = _holding(evil, body)

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path.removeprefix("/nifi-api")
        if request.method == "GET" and path == f"/processors/{PROC}":
            return httpx.Response(200, json=self.entity)
        return await super().handle_async_request(request)


_NONE = ("POST", "/nowhere")


@pytest.mark.asyncio
@pytest.mark.parametrize("verbose", [False, True])
@pytest.mark.parametrize(("shape", "evil"), EVIL_CASES)
async def test_a_read_of_a_value_holding_its_own_next_opener_leaves_nothing(
    shape: str, evil: str, verbose: bool
) -> None:
    body = shape.format(v=evil, r="A")
    text = await _call("nifi_get_processor", {"params": {"component_id": PROC, "verbose": verbose}},
                       _Holds(evil, body, _NONE))
    assert CANARY not in text, text
    # Outside the tool call too: redact knows the DTO's own values.
    assert CANARY not in json.dumps(redact(_holding(evil, body))), body


@pytest.mark.asyncio
@pytest.mark.parametrize(("shape", "evil"), EVIL_CASES)
@pytest.mark.parametrize(
    ("tool", "params", "target"),
    [
        ("nifi_update_processor", {"processor_id": PROC, "properties": {"Password": "{evil}"}},
         ("PUT", f"/processors/{PROC}")),
        ("nifi_set_run_status", {"component_id": PROC, "state": "RUNNING"},
         ("PUT", f"/processors/{PROC}/run-status")),
    ],
)
async def test_a_mutation_error_of_a_value_holding_its_own_next_opener_leaves_nothing(
    shape: str, evil: str, tool: str, params: dict[str, Any], target: tuple[str, str]
) -> None:
    # The update submits the value; the start does not, and the value is known from the read before it.
    body = "Unable to apply: [" + shape.format(v=evil, r="A") + "]"
    args = json.loads(json.dumps(params).replace("{evil}", json.dumps(evil)[1:-1]))
    text, payload = await _call_refused(tool, args, _Holds(evil, body, target))
    assert payload["status"] == "error", payload
    assert CANARY not in text, text


def test_a_body_of_data_size_sentences_masks_every_value() -> None:
    text = mask_echoed_values("Invalid data size: first-secret; Invalid data size: second-secret")
    assert "first-secret" not in text and "second-secret" not in text, text


def test_a_value_holding_the_next_opener_gives_the_neutral_note() -> None:
    # The value itself reads "x validated against 'y": the sentences cannot be told apart, so none is
    # paired with another's reason and no part of the value survives.
    body = "['A' validated against 'sec validated against 'ret' is invalid because r1, 'B' validated against 'z']"
    text = mask_echoed_values(body)
    assert "sec" not in text and "ret" not in text, text
    assert text == "['A' validated against '" + M + UNSPLIT_NOTE


def test_a_copy_is_masked_only_where_it_stands_as_a_whole_value() -> None:
    sentence = (
        "'Destination' validated against 'content' is invalid because Given value not found in "
        "allowed set 'flowfile-attribute, flowfile-content'; contentious; content."
    )
    text = mask_echoed_values(sentence)
    assert "allowed set 'flowfile-attribute, flowfile-content'" in text, text
    assert "contentious" in text, text
    assert text.endswith(f"; {M}."), text


def test_a_copy_in_a_subject_is_kept() -> None:
    body = (
        "['Reader' validated against 'x' is invalid because r, "
        "'Record Reader' validated against 'Reader' is invalid because Reader is missing]"
    )
    text = mask_echoed_values(body, ["x", "Reader"])
    assert "'Record Reader' validated against" in text and "because r," in text, text
    assert f"because {M} is missing" in text, text


def test_the_explanation_copy_is_still_masked() -> None:
    sentence = (
        "'Record Reader' validated against 'canary-r10' is invalid because "
        "Invalid Controller Service: canary-r10 is not a valid Controller Service Identifier"
    )
    text = safe_error_message(NiFiError("PUT failed", status_code=400, body=sentence))
    assert "canary-r10" not in text, text
    assert "Invalid Controller Service: " + M + " is not a valid" in text, text


@pytest.mark.asyncio
async def test_starting_a_processor_keeps_each_property_with_its_reason() -> None:
    # The reported reproduction.
    body = (
        "Unable to start: ['Record Reader' validated against 'bad-reader' is invalid because reader service "
        "is missing, 'Record Writer' validated against 'bad-writer' is invalid because writer service is disabled]"
    )
    transport = _Holds("bad-reader", body, ("PUT", f"/processors/{PROC}/run-status"))
    transport.refusal = (transport.refusal[0], 409, body)
    transport.entity["component"]["config"]["properties"]["Record Writer"] = "bad-writer"
    text, payload = await _call_refused(
        "nifi_set_run_status", {"component_id": PROC, "state": "RUNNING"}, transport
    )
    assert "bad-reader" not in text and "bad-writer" not in text, text
    assert f"'Record Reader' validated against '{M}' is invalid because reader service is missing" in payload["error"]
    assert f"'Record Writer' validated against '{M}' is invalid because writer service is disabled" in payload["error"]


class _Destination(httpx.AsyncBaseTransport):
    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        sentence = (
            "'Destination' validated against 'content' is invalid because Given value not found in "
            "allowed set 'flowfile-attribute, flowfile-content'"
        )
        component = {
            "id": PROC, "parentGroupId": PG, "name": "A2J", "type": "x.AttributesToJSON", "state": "STOPPED",
            "validationStatus": "INVALID", "validationErrors": [sentence],
            "config": {"properties": {"Destination": "content"}, "descriptors": {}},
        }
        return httpx.Response(200, json={"id": PROC, "revision": {"version": 1}, "component": component})


@pytest.mark.asyncio
@pytest.mark.parametrize("verbose", [False, True])
async def test_a_read_keeps_the_allowed_values_nifi_lists(verbose: bool) -> None:
    # The reported reproduction, compact and verbose.
    text = await _call("nifi_get_processor", {"params": {"component_id": PROC, "verbose": verbose}}, _Destination())
    assert "allowed set 'flowfile-attribute, flowfile-content'" in text, text
    assert "'content'" not in text and '"content"' not in text, json.loads(text)


# A sibling holds a proper prefix of the quoted value, and the prefix ends where the shape's
# closer does. One per _ECHOES entry, in order: the value is the prefix, the closer and a canary tail.
PREFIX, TAIL = "prefix-known", "canary-r12"
PREFIXED = [
    f"{PREFIX}' is invalid because {TAIL}",
    f"{PREFIX}' is not a valid cron: {TAIL}",
    f"{PREFIX}' is not of required type {TAIL}",
    f"{PREFIX}' is not of required type {TAIL}",
    f"{PREFIX}' is not a valid time: {TAIL}",
    f"{PREFIX} {TAIL}",
]
# The same with the shape's next opener in the tail too: the prefix is known, the rest of the value is not.
PREFIXED_OPENERS = [evil.replace("x'", f"{PREFIX}'", 1).replace("x;", f"{PREFIX};", 1) for evil in EVIL]
PREFIX_CASES = [
    (shape, value)
    for (shape, _kept), values in zip(SHAPES, zip(PREFIXED, PREFIXED_OPENERS, strict=True), strict=True)
    for value in values
]


def test_every_echo_shape_has_a_prefixed_value() -> None:
    assert len(PREFIXED) == len(PREFIXED_OPENERS) == len(_ECHOES)
    assert all(value.startswith(PREFIX) for _shape, value in PREFIX_CASES)


def _tail_is_gone(text: str, value: str) -> None:
    for piece in (TAIL, CANARY):
        if piece in value:
            assert piece not in text, text


@pytest.mark.parametrize(("shape", "value"), PREFIX_CASES)
def test_a_known_proper_prefix_does_not_prove_where_a_value_ends(shape: str, value: str) -> None:
    body = shape.format(v=value, r="A")
    for text in (mask_echoed_values(body, [PREFIX]), Renderer({"Note": PREFIX}).foreign(body)):
        _tail_is_gone(text, value)
    # Control: the whole value known is masked whole, and the sentence keeps its reason.
    for text in (mask_echoed_values(body, [PREFIX, value]), Renderer({"Note": PREFIX, "P": value}).foreign(body)):
        _tail_is_gone(text, value)
        assert PREFIX not in text, text
        if "{r}" in shape:
            assert UNSPLIT_NOTE not in text and text.endswith("reason A"), text


def _sibling(body: str) -> dict[str, Any]:
    # NiFi sends the sensitive value as ******** and quotes it raw in the sentence (DtoFactory); only
    # the sibling property holds a known value, the prefix.
    component = {
        "id": PROC, "parentGroupId": PG, "name": "P", "type": "x.P", "state": "STOPPED",
        "validationStatus": "INVALID", "validationErrors": [body],
        "config": {
            "properties": {"Note": PREFIX, "Password": "********"},
            "descriptors": {
                "Password": {"displayName": "Password", "sensitive": True},
                "Note": {"displayName": "Note"},
            },
        },
    }
    return {"id": PROC, "revision": {"version": 1}, "component": component}


class _Sibling(_Holds):
    def __init__(self, body: str, target: tuple[str, str]) -> None:
        super().__init__(PREFIX, body, target)
        self.entity = _sibling(body)


@pytest.mark.asyncio
@pytest.mark.parametrize("verbose", [False, True])
@pytest.mark.parametrize(("shape", "value"), PREFIX_CASES)
async def test_a_read_with_a_known_prefix_leaves_no_tail(shape: str, value: str, verbose: bool) -> None:
    body = shape.format(v=value, r="A")
    text = await _call("nifi_get_processor", {"params": {"component_id": PROC, "verbose": verbose}},
                       _Sibling(body, _NONE))
    _tail_is_gone(text, value)
    _tail_is_gone(json.dumps(redact(_sibling(body))), value)


@pytest.mark.asyncio
@pytest.mark.parametrize(("shape", "value"), PREFIX_CASES)
@pytest.mark.parametrize(
    ("tool", "params", "target"),
    [
        ("nifi_update_processor", {"processor_id": PROC, "properties": {"Note": PREFIX}},
         ("PUT", f"/processors/{PROC}")),
        ("nifi_set_run_status", {"component_id": PROC, "state": "RUNNING"},
         ("PUT", f"/processors/{PROC}/run-status")),
    ],
)
async def test_a_mutation_error_with_a_known_prefix_leaves_no_tail(
    shape: str, value: str, tool: str, params: dict[str, Any], target: tuple[str, str]
) -> None:
    body = "Unable to start: [" + shape.format(v=value, r="A") + "]"
    transport = _Sibling(body, target)
    transport.refusal = (target, 409, body)
    text, payload = await _call_refused(tool, params, transport)
    assert payload["status"] == "error", payload
    _tail_is_gone(text, value)
