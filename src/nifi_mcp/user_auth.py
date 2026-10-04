"""Request-scoped Keycloak identity, authorized by NiFi's certificate proxy chain."""

from __future__ import annotations

import time
from contextlib import asynccontextmanager
from contextvars import ContextVar

import httpx
from mcp.server.auth.middleware.auth_context import get_access_token
from mcp.server.auth.provider import AccessToken, TokenVerifier
from pydantic import Field

from nifi_mcp.client import NiFiClient
from nifi_mcp.config import Settings
from nifi_mcp.errors import NiFiAuthError
from nifi_mcp.proxy_entities import safe_entity

CALLER_CLIENT: ContextVar[NiFiClient | None] = ContextVar("nifi_caller_client", default=None)


class UserToken(AccessToken):
    """Identity is derived only from authenticated upstream responses."""

    token: str = Field(repr=False)
    identity: str | None = Field(default=None, repr=False)
    groups: list[str] = Field(default_factory=list, repr=False)


def keycloak_identity(payload: dict, settings: Settings) -> str | None:
    """Reject inactive, foreign-audience, expired, or unsafe proxy identities."""
    audience = payload.get("aud", [])
    audience = [audience] if isinstance(audience, str) else audience
    expires = payload.get("exp")
    identity = payload.get(settings.oauth_identity_claim or "")
    if (
        payload.get("active") is not True
        or payload.get("iss") != settings.oauth_issuer_url
        or not isinstance(audience, list)
        or settings.oauth_audience not in audience
        or not isinstance(expires, int)
        or expires <= time.time()
        or not isinstance(payload.get("sub"), str)
        or not payload["sub"].strip()
    ):
        return None
    if not safe_entity(identity):
        return None
    return identity


class UserTokenVerifier(TokenVerifier):
    def __init__(self, settings: Settings, *, transport: httpx.AsyncBaseTransport | None = None) -> None:
        self.settings = settings
        self.transport = transport

    async def verify_token(self, token: str) -> AccessToken | None:
        cfg = self.settings
        try:
            async with httpx.AsyncClient(
                verify=cfg.tls_verify_value(), timeout=cfg.timeout_seconds, transport=self.transport
            ) as client:
                response = await client.post(
                    cfg.oauth_introspection_url or "",
                    auth=(cfg.oauth_client_id or "", cfg.oauth_client_secret or ""),
                    data={"token": token, "token_type_hint": "access_token"},
                )
                if response.status_code != 200:
                    return None
                payload = response.json()
                if not isinstance(payload, dict):
                    return None
                identity = keycloak_identity(payload, cfg)
                if identity is None:
                    return None
                groups = payload.get(cfg.oauth_groups_claim, []) if cfg.oauth_groups_claim else []
                if not isinstance(groups, list) or not all(safe_entity(group) for group in groups):
                    return None
                scope = payload.get("scope", "")
                if not isinstance(scope, str):
                    return None
                return UserToken(
                    token=token,
                    client_id=payload.get("azp") or "keycloak",
                    scopes=scope.split(),
                    subject=payload["sub"],
                    resource=cfg.oauth_resource_url,
                    expires_at=payload["exp"],
                    identity=identity,
                    groups=groups,
                )
        except (httpx.HTTPError, ValueError, TypeError):
            # No endpoint body or exception text: either can contain credentials.
            return None


@asynccontextmanager
async def caller_client(settings: Settings):
    """Never reuse a process-wide credential, cookie jar, or client between callers."""
    if settings.auth != "passthrough":
        yield
        return
    token = get_access_token()
    if not isinstance(token, UserToken) or token.identity is None:
        raise NiFiAuthError("A verified caller token is required")
    cfg = settings.model_copy(update={"auth": "bearer", "bearer_token": None})
    client = NiFiClient(cfg, proxy_identity=token.identity, proxy_groups=token.groups)
    context = CALLER_CLIENT.set(client)
    try:
        yield
    finally:
        CALLER_CLIENT.reset(context)
        await client.aclose()
