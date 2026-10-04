"""A personal certificate must not inherit a bearer or proxy identity."""

import ssl
from unittest.mock import Mock

import httpx
import pytest
from pydantic import ValidationError

from nifi_mcp.client import NiFiClient
from nifi_mcp.config import Settings


def mtls_settings(**overrides):
    values = {
        "api_url": "https://nifi.example.test",
        "auth": "mtls",
        "transport": "stdio",
        "client_cert": "user.crt",
        "client_key": "user.key",
        "_env_file": None,
    }
    values.update(overrides)
    return Settings(**values)


@pytest.mark.parametrize(
    "change",
    [
        {"client_cert": None},
        {"client_key": None},
        {"tls_verify": False},
        {"api_url": "http://nifi.test"},
        {"transport": "streamable-http"},
    ],
)
def test_certificate_mode_rejects_unsafe_configuration(change):
    with pytest.raises(ValidationError):
        mtls_settings(**change)


@pytest.mark.parametrize("password", [None, "private-key-password"])
async def test_certificate_identity_has_no_bearer_proxy_headers_or_relogin(monkeypatch, password):
    loaded = Mock()
    monkeypatch.setattr(ssl.SSLContext, "load_cert_chain", loaded)
    cfg = mtls_settings(
        client_key_password=password,
        bearer_token="stale-admin-token",
        oidc_username="admin",
        oidc_password="unused-password",
    )
    sent = []

    def nifi(request):
        sent.append(request)
        assert "authorization" not in request.headers
        assert "x-proxiedentitieschain" not in request.headers
        assert "x-proxiedentitygroups" not in request.headers
        return httpx.Response(401, text="Certificate user denied")

    client = NiFiClient(cfg, transport=httpx.MockTransport(nifi))
    try:
        await client.authenticate()
        response = await client.request(
            "PUT",
            "/process-groups/root",
            json_body={},
            extra_headers={
                "authorization": "Bearer injected",
                "X-ProxiedEntitiesChain": "<admin>",
                "X-ProxiedEntityGroups": "<admins>",
            },
        )
        assert response.status_code == 401
        assert client.tokens.token is None
        await client.authenticate(force=True)
    finally:
        await client.aclose()
    assert len(sent) == 1
    loaded.assert_called_once_with("user.crt", "user.key", password=password or "")
    assert cfg.client_tls_context.verify_mode == ssl.CERT_REQUIRED
    assert cfg.client_tls_context.check_hostname


async def test_real_current_user_tool_uses_certificate_identity(monkeypatch):
    import json

    from nifi_mcp import server

    cfg = mtls_settings()
    monkeypatch.setattr(ssl.SSLContext, "load_cert_chain", lambda *args, **kwargs: None)
    seen = []

    def nifi(request):
        seen.append(request)
        assert request.url.path.endswith("/flow/current-user")
        assert "Authorization" not in request.headers
        return httpx.Response(200, json={"identity": "CN=developer", "anonymous": False})

    client = NiFiClient(cfg, transport=httpx.MockTransport(nifi))
    monkeypatch.setattr(server, "_settings", cfg)
    monkeypatch.setattr(server, "_client", client)
    try:
        result = json.loads(await server.nifi_current_user())
        assert result["identity"] == "CN=developer"
    finally:
        await client.aclose()
    assert len(seen) == 1


def test_invalid_certificate_fails_startup_without_printing_key_password(monkeypatch, caplog):
    from nifi_mcp import server

    cfg = mtls_settings(client_key_password="private-key-password-canary")
    monkeypatch.setattr(server, "_settings", cfg)

    def invalid_certificate(*args, **kwargs):
        raise OSError("private-key-password-canary")

    monkeypatch.setattr(ssl.SSLContext, "load_cert_chain", invalid_certificate)
    run = Mock()
    monkeypatch.setattr(server.mcp, "run", run)
    with pytest.raises(SystemExit) as exc:
        server.main()
    assert exc.value.code == 2
    assert "private-key-password-canary" not in caplog.text
    run.assert_not_called()
