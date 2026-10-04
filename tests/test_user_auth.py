"""Security boundary: verified caller -> isolated client -> NiFi policy decision."""

import asyncio
import json
import ssl
import time

import httpx
import pytest
from mcp.server.auth.settings import AuthSettings
from mcp.server.fastmcp import FastMCP
from pydantic import ValidationError

from nifi_mcp import user_auth
from nifi_mcp.client import NiFiClient
from nifi_mcp.config import Settings
from nifi_mcp.user_auth import CALLER_CLIENT, UserTokenVerifier, caller_client


def settings(**overrides):
    values = {
        "api_url": "https://nifi.example.test",
        "auth": "passthrough",
        "transport": "streamable-http",
        "oauth_issuer_url": "https://id.example.test/realms/local",
        "oauth_resource_url": "https://mcp.example.test/mcp",
        "oauth_introspection_url": "https://id.example.test/realms/local/protocol/openid-connect/token/introspect",
        "oauth_client_id": "nifi-mcp",
        "oauth_client_secret": "secret",
        "oauth_audience": "https://mcp.example.test/mcp",
        "oauth_identity_claim": "preferred_username",
        "proxy_cert": "proxy.pem",
        "proxy_key": "proxy.key",
        "_env_file": None,
    }
    values.update(overrides)
    return Settings(**values)


def claims(identity="alice", **overrides):
    result = {
        "active": True,
        "scope": "openid profile email",
        "iss": "https://id.example.test/realms/local",
        "aud": ["https://mcp.example.test/mcp"],
        "exp": int(time.time()) + 300,
        "sub": identity,
        "preferred_username": identity,
        "groups": ["readers"],
    }
    result.update(overrides)
    return result


@pytest.mark.parametrize(
    "change",
    [
        {"active": False},
        {"active": "true"},
        {"iss": "https://wrong.test"},
        {"aud": ["other"]},
        {"aud": None},
        {"exp": 0},
        {"exp": "99999999999"},
        {"sub": None},
        {"preferred_username": "alice\r\nInjected: true"},
        {"preferred_username": "alice\\>admin"},
        {"preferred_username": ""},
        {"groups": "admins"},
        {"groups": None},
    ],
)
async def test_reject_bad_keycloak_claims(change):
    verifier = UserTokenVerifier(
        settings(), transport=httpx.MockTransport(lambda request: httpx.Response(200, json=claims(**change)))
    )
    assert await verifier.verify_token("untrusted") is None


@pytest.mark.parametrize(
    "change",
    [
        {"transport": "stdio"},
        {"auth": "oidc"},
        {"tls_verify": False},
        {"api_url": "http://nifi.test"},
        {"oauth_resource_url": "http://mcp.test"},
        {"oauth_resource_url": "https://"},
        {"oauth_issuer_url": "https://id.test/realm?secret=hidden"},
        {"oauth_introspection_url": "https://user:secret@id.test/introspect"},
        {"oauth_introspection_url": None},
        {"proxy_key": None},
        {"oauth_client_secret": None},
        {"oauth_audience": None},
        {"oauth_audience": "nifi-mcp"},
        {"oauth_scopes": ["profile"]},
        {"oauth_identity_claim": None},
    ],
)
def test_invalid_config_fails_closed(change):
    with pytest.raises(ValidationError):
        settings(**change)


async def test_introspection_failure_never_returns_body():
    for response in (
        httpx.Response(503, text="secret"),
        httpx.Response(200, text="secret"),
        httpx.Response(200, json=[]),
    ):
        verifier = UserTokenVerifier(
            settings(), transport=httpx.MockTransport(lambda request, response=response: response)
        )
        assert await verifier.verify_token("secret") is None


async def test_http_caller_isolation_and_nifi_denial(monkeypatch):
    cfg = settings()
    sent = []
    introspected = []

    def introspect(request):
        assert request.url.host == "id.example.test"
        introspected.append(request.content)
        who = "admin" if b"token=admin" in request.content else "alice"
        if b"token=expired" in request.content:
            return httpx.Response(200, json=claims(exp=0))
        return httpx.Response(200, json=claims(who, groups=["admins" if who == "admin" else "readers"]))

    async def nifi(request):
        sent.append(request)
        await asyncio.sleep(0)
        assert "authorization" not in request.headers
        if request.method == "PUT" and request.headers["X-ProxiedEntitiesChain"] != "<admin>":
            return httpx.Response(403, text="Permission denied")
        return httpx.Response(200, json={"ok": True})

    monkeypatch.setattr(ssl.SSLContext, "load_cert_chain", lambda *args: None)
    monkeypatch.setattr(
        user_auth,
        "NiFiClient",
        lambda config, **kwargs: NiFiClient(config, transport=httpx.MockTransport(nifi), **kwargs),
    )
    server = FastMCP(
        "test",
        stateless_http=True,
        json_response=True,
        token_verifier=UserTokenVerifier(cfg, transport=httpx.MockTransport(introspect)),
        auth=AuthSettings(
            issuer_url=cfg.oauth_issuer_url, resource_server_url=cfg.oauth_resource_url, validate_token_resource=False
        ),
    )

    @server.tool()
    async def mutate():
        async with caller_client(cfg):
            client = CALLER_CLIENT.get()
            # Hold both calls open to exercise concurrent context isolation.
            await asyncio.sleep(0.01)
            response = await client.request("PUT", "/process-groups/root", json_body={})
            return {"status": response.status_code}

    app = server.streamable_http_app()
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://127.0.0.1:8000",
            headers={"Accept": "application/json, text/event-stream"},
        ) as http,
    ):
        no_token = await http.post("/mcp", json={})
        assert no_token.status_code == 401
        assert "resource_metadata=" in no_token.headers["www-authenticate"]
        metadata = await http.get("/.well-known/oauth-protected-resource/mcp")
        assert metadata.status_code == 200
        assert metadata.json()["authorization_servers"] == [cfg.oauth_issuer_url]
        expired = await http.post("/mcp", headers={"Authorization": "Bearer expired"}, json={})
        assert expired.status_code == 401

        async def call(token):
            return await http.post(
                "/mcp",
                headers={
                    "Authorization": f"Bearer {token}",
                    "X-ProxiedEntitiesChain": "<admin>",
                    "X-ProxiedEntityGroups": "<admins>",
                    "MCP-Protocol-Version": "2025-06-18",
                },
                json={"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": "mutate", "arguments": {}}},
            )

        alice, admin = await asyncio.gather(call("alice"), call("admin"))
        assert alice.status_code == admin.status_code == 200
        assert json.loads(alice.json()["result"]["content"][0]["text"])["status"] == 403
        assert json.loads(admin.json()["result"]["content"][0]["text"])["status"] == 200
    assert CALLER_CLIENT.get() is None
    assert len(sent) == 2
    assert {r.headers["X-ProxiedEntitiesChain"] for r in sent} == {"<alice>", "<admin>"}
    assert {r.headers["X-ProxiedEntityGroups"] for r in sent} == {"<readers>", "<admins>"}
    assert len(introspected) == 3


async def test_missing_context_cannot_use_shared_credentials():
    from nifi_mcp.errors import NiFiAuthError

    with pytest.raises(NiFiAuthError):
        async with caller_client(settings(bearer_token="admin-token", oidc_password="admin-password")):
            pytest.fail("missing caller must never create a client")


async def test_real_tool_authenticates_proxy_without_admin_token(monkeypatch):
    from mcp.server.auth.middleware.auth_context import auth_context_var
    from mcp.server.auth.middleware.bearer_auth import AuthenticatedUser

    from nifi_mcp import server
    from nifi_mcp.user_auth import UserToken

    cfg = settings(bearer_token="shared-admin-token", oidc_password="admin-password")
    requests = []

    def nifi(request):
        requests.append(request)
        assert request.headers["X-ProxiedEntitiesChain"] == "<alice>"
        assert "Authorization" not in request.headers
        return httpx.Response(200, json={"identity": "alice", "anonymous": False})

    monkeypatch.setattr(ssl.SSLContext, "load_cert_chain", lambda *args: None)
    monkeypatch.setattr(server, "_settings", cfg)
    monkeypatch.setattr(
        user_auth,
        "NiFiClient",
        lambda config, **kwargs: NiFiClient(config, transport=httpx.MockTransport(nifi), **kwargs),
    )
    token = auth_context_var.set(
        AuthenticatedUser(UserToken(token="keycloak-user-token", client_id="vscode", scopes=[], identity="alice"))
    )
    try:
        result = json.loads(await server.nifi_current_user())
        assert result["identity"] == "alice"
    finally:
        auth_context_var.reset(token)
    assert len(requests) == 1
    assert CALLER_CLIENT.get() is None


@pytest.mark.parametrize(
    ("value", "encoded"),
    [
        ("alice", "<alice>"),
        ("josé", "<<am9zw6k=>>"),
        ("équipe", "<<w6lxdWlwZQ==>>"),
        ("readers><admins", "<readers\\>\\<admins>"),
    ],
)
def test_nifi_proxy_encoder_vectors(value, encoded):
    from nifi_mcp.proxy_entities import encode_entity

    assert encode_entity(value) == encoded


async def test_unicode_and_effective_groups_from_verified_claim():
    cfg = settings(oauth_groups_claim="nifi_effective_groups")
    payload = claims("josé", groups=["/teams/readers"], nifi_effective_groups=["équipe", "readers"])
    verifier = UserTokenVerifier(cfg, transport=httpx.MockTransport(lambda request: httpx.Response(200, json=payload)))
    token = await verifier.verify_token("user-token")
    assert token.identity == "josé"
    assert token.groups == ["équipe", "readers"]
    assert "user-token" not in repr(token)


async def test_production_http_wiring_accepts_public_host_and_runs_real_tool(monkeypatch):
    from nifi_mcp import server

    cfg = settings()
    sent = []
    loaded = []
    monkeypatch.setattr(ssl.SSLContext, "load_cert_chain", lambda *args: loaded.append(args[1:]))
    monkeypatch.setattr(server, "_settings", cfg)
    mcp = server._NiFiMCP("production-test", json_response=True)
    mcp.add_tool(server.nifi_current_user, name="nifi_current_user")
    monkeypatch.setattr(server, "mcp", mcp)
    server.configure_http(cfg)
    mcp._token_verifier = UserTokenVerifier(
        cfg, transport=httpx.MockTransport(lambda request: httpx.Response(200, json=claims()))
    )

    def nifi(request):
        sent.append(request)
        assert request.headers["X-ProxiedEntitiesChain"] == "<alice>"
        assert "Authorization" not in request.headers
        return httpx.Response(200, json={"identity": "alice", "anonymous": False})

    monkeypatch.setattr(
        user_auth,
        "NiFiClient",
        lambda config, **kwargs: NiFiClient(config, transport=httpx.MockTransport(nifi), **kwargs),
    )
    app = mcp.streamable_http_app()
    async with (
        app.router.lifespan_context(app),
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="https://mcp.example.test",
            headers={"Accept": "application/json, text/event-stream", "Authorization": "Bearer alice"},
        ) as http,
    ):
        body = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "tools/call",
            "params": {"name": "nifi_current_user", "arguments": {}},
        }
        first = await http.post("/mcp", json=body)
        second = await http.post("/mcp", json=body)
        assert first.status_code == second.status_code == 200
        assert json.loads(first.json()["result"]["content"][0]["text"])["identity"] == "alice"
        invalid_host = await http.post("/mcp", headers={"Host": "attacker.test"}, json=body)
        assert invalid_host.status_code == 421
    assert len(sent) == 2
    assert loaded == [("proxy.pem", "proxy.key")]
