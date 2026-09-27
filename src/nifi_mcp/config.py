"""Environment-driven settings."""

from __future__ import annotations

import ssl
from typing import Literal

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

AuthMode = Literal["oidc", "jwt", "bearer"]

NIFI_API_SUFFIX = "/nifi-api"


def normalize_api_url(raw: str) -> str:
    """Accept a UI origin or an /nifi-api URL; always return .../nifi-api."""
    url = raw.strip().rstrip("/")
    if url.endswith("/nifi"):
        url = url[: -len("/nifi")]
    if not url.endswith(NIFI_API_SUFFIX):
        url = f"{url}{NIFI_API_SUFFIX}"
    return url


class Settings(BaseSettings):
    """Process environment for one NiFi cluster."""

    model_config = SettingsConfigDict(
        env_prefix="NIFI_",
        env_file=".env",
        extra="ignore",
    )

    api_url: str = Field(..., description="NiFi origin or /nifi-api URL")
    readonly: bool = False
    auth: AuthMode = "oidc"
    client_id: str = "nifi-mcp"
    timeout_seconds: float = 30.0
    disconnected_node_ack: bool = True
    tls_verify: bool = True
    ca_bundle: str | None = None

    oidc_token_url: str | None = None
    oidc_client_id: str | None = None
    oidc_client_secret: str | None = None
    oidc_username: str | None = None
    oidc_password: str | None = None
    oidc_scope: str = "openid profile"

    username: str | None = None
    password: str | None = None
    bearer_token: str | None = None

    @field_validator("api_url")
    @classmethod
    def _normalize_url(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("NIFI_API_URL is required")
        return normalize_api_url(value)

    def tls_verify_value(self) -> bool | ssl.SSLContext:
        """Trust the default CA store plus an optional extra bundle.

        NiFi often uses an internal CA while the OIDC IdP uses a public cert.
        Passing only the internal bundle as httpx `verify=` replaces the default
        store and a public OIDC IdP fails TLS.
        """
        if not self.tls_verify and not self.ca_bundle:
            return False
        if not self.ca_bundle:
            return True
        ctx = ssl.create_default_context()
        ctx.load_verify_locations(cafile=self.ca_bundle)
        return ctx
