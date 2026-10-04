"""Mint and refresh NiFi API credentials: oidc, jwt or bearer."""

from __future__ import annotations

import httpx

from nifi_mcp.config import Settings
from nifi_mcp.errors import NiFiAuthError

_TOKEN_PATH = "/access/token"  # noqa: S105 - NiFi REST path, not a credential


class TokenStore:
    """Holds a Bearer token and can mint a replacement after HTTP 401."""

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._token: str | None = None if settings.auth == "mtls" else settings.bearer_token

    @property
    def token(self) -> str | None:
        return self._token

    def authorization_header(self) -> dict[str, str]:
        if not self._token:
            return {}
        return {"Authorization": f"Bearer {self._token}"}

    async def authenticate(self, client: httpx.AsyncClient) -> str:
        mode = self._settings.auth
        if mode == "bearer":
            if not self._settings.bearer_token:
                raise NiFiAuthError("NIFI_AUTH=bearer requires NIFI_BEARER_TOKEN")
            self._token = self._settings.bearer_token
            return self._token
        if mode == "jwt":
            self._token = await self._mint_nifi_jwt(client)
            return self._token
        if mode == "oidc":
            self._token = await self._mint_oidc(client)
            return self._token
        raise NiFiAuthError(f"Unsupported NIFI_AUTH={mode}")

    async def _mint_nifi_jwt(self, client: httpx.AsyncClient) -> str:
        if not self._settings.username or not self._settings.password:
            raise NiFiAuthError("NIFI_AUTH=jwt requires NIFI_USERNAME and NIFI_PASSWORD")
        response = await client.post(
            _TOKEN_PATH,
            data={
                "username": self._settings.username,
                "password": self._settings.password,
            },
            headers={"Accept": "text/plain"},
        )
        if response.status_code not in {200, 201}:
            # Never attach the body: token endpoints can echo submitted credentials.
            raise NiFiAuthError(
                "POST /access/token failed. Check NIFI_USERNAME / NIFI_PASSWORD.",
                status_code=response.status_code,
                path=_TOKEN_PATH,
            )
        token = response.text.strip()
        if not token:
            raise NiFiAuthError("POST /access/token returned an empty body")
        return token

    async def _mint_oidc(self, client: httpx.AsyncClient) -> str:
        missing = [
            name
            for name, value in (
                ("NIFI_OIDC_TOKEN_URL", self._settings.oidc_token_url),
                ("NIFI_OIDC_CLIENT_ID", self._settings.oidc_client_id),
                ("NIFI_OIDC_CLIENT_SECRET", self._settings.oidc_client_secret),
                ("NIFI_OIDC_USERNAME", self._settings.oidc_username),
                ("NIFI_OIDC_PASSWORD", self._settings.oidc_password),
            )
            if not value
        ]
        if missing:
            raise NiFiAuthError(f"NIFI_AUTH=oidc missing {', '.join(missing)}")
        response = await client.post(
            self._settings.oidc_token_url or "",
            data={
                "grant_type": "password",
                "client_id": self._settings.oidc_client_id,
                "client_secret": self._settings.oidc_client_secret,
                "username": self._settings.oidc_username,
                "password": self._settings.oidc_password,
                "scope": self._settings.oidc_scope,
            },
        )
        if response.status_code != 200:
            # Never attach the body: the IdP can echo the submitted password or client secret.
            raise NiFiAuthError(
                "OIDC token endpoint rejected the password grant. Check the NIFI_OIDC_* settings.",
                status_code=response.status_code,
                path=self._settings.oidc_token_url,
            )
        try:
            payload = response.json()
        except ValueError as exc:
            raise NiFiAuthError("OIDC token endpoint did not return JSON") from exc
        token = payload.get("access_token")
        if not token:
            raise NiFiAuthError("OIDC token response had no access_token")
        return str(token)
