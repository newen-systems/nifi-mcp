import json
import os

import httpx
import pytest
from conftest import Router, settings

import nifi_mcp.server as server
from nifi_mcp.auth import TokenStore
from nifi_mcp.client import NiFiClient
from nifi_mcp.config import Settings
from nifi_mcp.errors import NiFiAuthError, NiFiError, safe_error_message, scrub_text

CANARY = "SYNTHETIC-SECRET-123"


@pytest.mark.asyncio
async def test_configuration_error_does_not_return_secret(monkeypatch: pytest.MonkeyPatch) -> None:
    for key in list(os.environ):
        if key.startswith("NIFI_"):
            monkeypatch.delenv(key)
    monkeypatch.setenv("NIFI_OIDC_PASSWORD", CANARY)
    monkeypatch.setenv("NIFI_OIDC_CLIENT_SECRET", CANARY)
    monkeypatch.setattr(server, "_settings", None)
    monkeypatch.setattr(server, "_client", None)
    monkeypatch.setattr(server, "Settings", lambda: Settings(_env_file=None))  # type: ignore[call-arg]
    payload = json.loads(await server.nifi_about())
    assert payload["status"] == "error"
    assert CANARY not in json.dumps(payload)
    assert "api_url" in payload["error"]
    assert "NIFI_API_URL" in payload["error"]


def test_startup_log_does_not_print_secret(monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture) -> None:
    for key in list(os.environ):
        if key.startswith("NIFI_"):
            monkeypatch.delenv(key)
    monkeypatch.setenv("NIFI_PASSWORD", CANARY)
    monkeypatch.setattr(server, "_settings", None)
    monkeypatch.setattr(server, "Settings", lambda: Settings(_env_file=None))  # type: ignore[call-arg]
    with pytest.raises(SystemExit):
        server.main()
    assert "Invalid configuration" in caplog.text
    assert CANARY not in caplog.text


@pytest.mark.asyncio
async def test_oidc_error_does_not_return_body() -> None:
    def handler(_request: httpx.Request) -> httpx.Response:
        return httpx.Response(400, json={"error": "invalid_request", "password": CANARY, "echo": CANARY})

    store = TokenStore(
        settings(
            auth="oidc",
            bearer_token=None,
            oidc_token_url="https://idp.example.test/token",
            oidc_client_id="nifi",
            oidc_client_secret="s",
            oidc_username="u",
            oidc_password=CANARY,
        )
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
        with pytest.raises(NiFiAuthError) as excinfo:
            await store.authenticate(client)
    assert CANARY not in str(excinfo.value)
    assert "HTTP 400" in str(excinfo.value)


@pytest.mark.asyncio
async def test_jwt_error_does_not_return_body(router: Router) -> None:
    router.add("POST", "/access/token", lambda _r: httpx.Response(400, text=f"bad password {CANARY}"))
    nifi = NiFiClient(
        settings(auth="jwt", username="alice", password=CANARY, bearer_token=None),
        transport=httpx.MockTransport(router.handle),
    )
    with pytest.raises(NiFiAuthError) as excinfo:
        await nifi.authenticate()
    assert CANARY not in str(excinfo.value)


def test_nifi_error_body_is_redacted() -> None:
    json_body = NiFiError("PUT failed", status_code=400, body=json.dumps({"properties": {"Password": CANARY}}))
    text_body = NiFiError("PUT failed", status_code=400, body=f"bad value password={CANARY} for x")
    assert CANARY not in str(json_body)
    assert CANARY not in str(text_body)
    assert CANARY not in safe_error_message(text_body)


@pytest.mark.parametrize("field", ["password", "name", "x"])
def test_nifi_type_conversion_body_keeps_field_and_type_but_not_the_value(field: str) -> None:
    # JsonContentConversionExceptionMapper.toResponse echoes the raw value for any field.
    canary = "canary-1234"
    body = f"The provided {field} value '{canary}' is not of required type class java.lang.String"
    text = safe_error_message(NiFiError("POST failed", status_code=400, path="/x", body=body))
    assert canary not in text, text
    assert f"The provided {field} value '***REDACTED***' is not of required type class java.lang.String" in text


@pytest.mark.parametrize("length", [10, 1990, 4000])
def test_a_long_conversion_value_is_masked_before_the_body_is_cut(length: int) -> None:
    # The body was cut to 2000 characters first, so the closing phrase was gone.
    secret = "canary-1234" + "A" * length
    body = f"The provided password value '{secret}' is not of required type class java.lang.String"
    text = safe_error_message(NiFiError("POST failed", status_code=400, path="/x", body=body))
    assert "canary-1234" not in text
    assert "The provided password value '***REDACTED***' is not of required type" in text


def test_a_conversion_value_cut_off_before_its_closing_phrase_is_masked_to_the_end() -> None:
    text = scrub_text("The provided password value 'canary-1234AAAA")
    assert "canary-1234" not in text
    assert text.startswith("The provided password value '***REDACTED***")


_ECHO_CANARY = "canary-1234"


@pytest.mark.parametrize(
    ("body", "kept"),
    [
        (
            "'Run Schedule' validated against 'canary-1234' is invalid because "
            "Scheduling Period is not a valid time duration",
            "'Run Schedule' validated against '***REDACTED***' is invalid because "
            "Scheduling Period is not a valid time duration",
        ),
        (
            "Scheduling Period 'canary-1234' is not a valid cron expression: Unexpected end of expression.",
            "Scheduling Period '***REDACTED***' is not a valid cron expression: Unexpected end of expression.",
        ),
        ("Invalid data size: canary-1234", "Invalid data size: ***REDACTED***"),
        ("Value 'canary-1234' is not a valid time duration", "Value '***REDACTED***' is not a valid time duration"),
        # Cut off before the closing phrase: masked to the end.
        ("'Password' validated against 'canary-1234AAAA", "'Password' validated against '***REDACTED***"),
    ],
)
def test_a_nifi_sentence_that_quotes_the_submitted_value_is_masked(body: str, kept: str) -> None:
    # ValidationResult.toString, StandardProcessorDAO, DataUnit.parseDataSize and
    # FormatUtils quote the value they rejected; IllegalArgumentExceptionMapper returns it as the body.
    text = safe_error_message(NiFiError("PUT /processors/x failed", status_code=400, body=body))
    assert _ECHO_CANARY not in text, text
    assert kept in text


def test_a_validation_error_on_a_read_does_not_return_the_value() -> None:
    # DtoFactory copies ValidationResult.toString into validationErrors on every read.
    from nifi_mcp.compact import compact_controller_service, compact_processor
    from nifi_mcp.server import _dump

    error = (
        "'Run Schedule' validated against 'canary-1234' is invalid because "
        "Scheduling Period is not a valid time duration"
    )
    component = {"name": "Gen", "validationStatus": "INVALID", "validationErrors": [error]}
    entity = {"id": "00000000-0000-0000-0000-000000000001", "component": component}
    for text in (
        _dump({"processor": compact_processor(entity)}),
        _dump({"service": compact_controller_service(entity)}),
        _dump({"processor": entity}),  # verbose=true returns the entity as NiFi sent it
    ):
        assert _ECHO_CANARY not in text, text
        assert "'Run Schedule' validated against '***REDACTED***' is invalid because Scheduling Period" in text


def test_a_property_value_is_not_rewritten_by_the_echo_mask() -> None:
    # Property values are the model's own text, not a NiFi sentence: only NiFi-built strings are masked.
    from nifi_mcp.redaction import redact

    script = "log('Invalid data size: 10 XB')"
    assert redact({"component": {"properties": {"Script Body": script}}})["component"]["properties"] == {
        "Script Body": script
    }


@pytest.mark.parametrize(
    "body",
    [
        "<html><body>upstream error " + "Zm9vYmFy" * 125_000 + "</body></html>",  # 1 MB base64 run
        "x" * 1_000_000,
        "token" * 200_000,
        "password=" * 100_000 + "x",
        "validated against '" * 50_000,
        "Scheduling Period '" * 50_000,
        "The provided a value '" * 40_000,
        'token:"' * 100_000,
    ],
)
def test_scrubbing_a_one_megabyte_body_is_linear(body: str) -> None:
    # _SECRET_PAIR was quadratic on a long unbroken run (22.9s at 40 KB).
    import time

    started = time.perf_counter()
    scrub_text(body)
    str(NiFiError("POST /p failed", status_code=502, path="/p", body=body))
    took = time.perf_counter() - started
    assert took < 1.0, f"scrubbing took {took:.2f}s on a {len(body)} character body"


@pytest.mark.parametrize(
    ("body", "kept"),
    [
        ("bad value password=canary-1234 for x", "password=***REDACTED*** for x"),
        ('{"x": 1, "api_key": "canary-1234"}', '"api_key": "***REDACTED***"'),
        ("x.client_secret : 'canary-1234' y", "x.client_secret : '***REDACTED***' y"),
        ("a=b&refresh_token=canary-1234&c=d", "refresh_token=***REDACTED***&c=d"),
        ("aaaa/TOKEN=canary-1234", "TOKEN=***REDACTED***"),
        ('token="canary-1234', 'token="***REDACTED***"'),
    ],
)
def test_a_credential_pair_is_still_masked(body: str, kept: str) -> None:
    text = scrub_text(body)
    assert _ECHO_CANARY not in text, text
    assert kept in text
