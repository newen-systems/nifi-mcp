"""Environment-driven settings."""

from __future__ import annotations

import ssl
from functools import cached_property
from typing import Literal
from urllib.parse import urlsplit

from pydantic import Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

AuthMode = Literal["oidc", "jwt", "bearer", "passthrough"]

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
    auth: AuthMode = "bearer"
    client_id: str = "nifi-mcp"
    timeout_seconds: float = 30.0
    disconnected_node_ack: bool = True
    tls_verify: bool = True
    ca_bundle: str | None = None

    oidc_token_url: str | None = None
    oidc_client_id: str | None = None
    oidc_client_secret: str | None = Field(default=None, repr=False)
    oidc_username: str | None = None
    oidc_password: str | None = Field(default=None, repr=False)
    oidc_scope: str = "openid profile"

    username: str | None = None
    password: str | None = Field(default=None, repr=False)
    bearer_token: str | None = Field(default=None, repr=False)
    transport: Literal["stdio", "streamable-http"] = "stdio"
    host: str = "127.0.0.1"
    port: int = 8000
    oauth_issuer_url: str | None = None
    oauth_resource_url: str | None = None
    oauth_introspection_url: str | None = None
    oauth_client_id: str | None = None
    oauth_client_secret: str | None = Field(default=None, repr=False)
    oauth_audience: str | None = None
    oauth_scopes: list[str] = Field(default_factory=lambda: ["openid", "profile", "email"])
    oauth_identity_claim: str | None = None
    oauth_groups_claim: str | None = "groups"
    proxy_cert: str | None = None
    proxy_key: str | None = Field(default=None, repr=False)

    @field_validator("oauth_issuer_url", "oauth_resource_url", "oauth_introspection_url")
    @classmethod
    def _oauth_endpoint(cls, value: str | None) -> str | None:
        if value is None:
            return value
        parsed = urlsplit(value)
        if (
            parsed.scheme != "https"
            or not parsed.hostname
            or parsed.username
            or parsed.password
            or parsed.query
            or parsed.fragment
            or any(char.isspace() for char in value)
        ):
            raise ValueError("OAuth URLs require an HTTPS host without credentials, query or fragment")
        return value

    @model_validator(mode="after")
    def _user_transport(self) -> Settings:
        if self.transport == "streamable-http" and self.auth != "passthrough":
            raise ValueError("HTTP requires NIFI_AUTH=passthrough; shared credentials are stdio-only")
        if self.auth != "passthrough":
            return self
        if self.transport != "streamable-http" or not self.tls_verify or not self.api_url.startswith("https://"):
            raise ValueError("Passthrough requires streamable-http, HTTPS NiFi and verified TLS")
        urls = (self.oauth_issuer_url, self.oauth_resource_url)
        if any(not url or not url.startswith("https://") for url in urls):
            raise ValueError("Passthrough requires HTTPS NIFI_OAUTH_ISSUER_URL and NIFI_OAUTH_RESOURCE_URL")
        required = (
            self.oauth_introspection_url,
            self.oauth_client_id,
            self.oauth_client_secret,
            self.oauth_audience,
            self.oauth_identity_claim,
            self.proxy_cert,
            self.proxy_key,
        )
        if not all(required):
            raise ValueError("Keycloak requires introspection, client credentials, audience and proxy certificate/key")
        if self.oauth_audience != self.oauth_resource_url:
            raise ValueError("NIFI_OAUTH_AUDIENCE must equal the MCP resource URL")
        if "openid" not in self.oauth_scopes:
            raise ValueError("NIFI_OAUTH_SCOPES must include openid")
        if not (self.oauth_introspection_url or "").startswith("https://"):
            raise ValueError("OAuth endpoints require HTTPS")
        return self

    @cached_property
    def proxy_tls_context(self) -> ssl.SSLContext:
        """Dedicated immutable NiFi TLS context; never presented to Keycloak."""
        context = ssl.create_default_context()
        if self.ca_bundle:
            context.load_verify_locations(cafile=self.ca_bundle)
        if not self.proxy_cert or not self.proxy_key:
            raise ValueError("NiFi proxy certificate and key are required")
        context.load_cert_chain(self.proxy_cert, self.proxy_key)
        return context

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
