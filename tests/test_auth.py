import httpx
import pytest
from conftest import settings

from nifi_mcp.auth import TokenStore
from nifi_mcp.errors import NiFiAuthError


@pytest.mark.asyncio
async def test_bearer_requires_token() -> None:
    store = TokenStore(settings(bearer_token=None, auth="bearer"))
    async with httpx.AsyncClient() as client:
        with pytest.raises(NiFiAuthError):
            await store.authenticate(client)


@pytest.mark.asyncio
async def test_jwt_mints_plain_token() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path.endswith("/access/token")
        assert b"username=alice" in request.content
        return httpx.Response(201, text="jwt-from-nifi")

    store = TokenStore(settings(auth="jwt", username="alice", password="secret", bearer_token=None))
    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(base_url="https://nifi.example.test/nifi-api", transport=transport) as client:
        token = await store.authenticate(client)
    assert token == "jwt-from-nifi"


@pytest.mark.asyncio
async def test_oidc_mints_access_token() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"access_token": "kc-token"})

    store = TokenStore(
        settings(
            auth="oidc",
            bearer_token=None,
            oidc_token_url="https://idp.example.test/token",
            oidc_client_id="nifi",
            oidc_client_secret="s",
            oidc_username="nifi-admin",
            oidc_password="p",
        )
    )
    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(transport=transport) as client:
        token = await store.authenticate(client)
    assert token == "kc-token"
    assert store.authorization_header()["Authorization"] == "Bearer kc-token"
