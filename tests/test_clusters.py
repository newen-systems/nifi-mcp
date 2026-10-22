"""One server, several NiFi clusters: configuration, exact names, the required cluster and the prod guard."""

import json
import ssl

import httpx
import pytest
from conftest import settings
from pydantic import ValidationError

from nifi_mcp import server, user_auth
from nifi_mcp.client import NiFiClient
from nifi_mcp.config import Settings
from nifi_mcp.server import ClusterIn, mcp

PROD_META = {
    "environment": "prod",
    "ui_url": "https://nifi.prod.example.test/nifi/",
    "kube_cluster": "lab",
    "namespace": "nifi-prod",
}
DEV_ENTRY = {
    "api_url": "https://nifi-dev.example.test",
    "environment": "dev",
    "ui_url": "https://nifi.dev.example.test/nifi/",
    "kube_cluster": "lab",
    "namespace": "nifi-dev",
}
DEV_PROXY = {"api_url": DEV_ENTRY["api_url"], "proxy_cert": "dev.pem", "proxy_key": "dev.key"}


def _text(result) -> str:
    content = result[0] if isinstance(result, tuple) else result
    return "".join(getattr(block, "text", "") for block in content)


async def _call(tool: str, params: dict | None = None) -> dict:
    return json.loads(_text(await mcp.call_tool(tool, {} if params is None else {"params": params})))


def test_single_cluster_settings_are_a_one_entry_list() -> None:
    cfg = settings()
    assert list(cfg.cluster_settings()) == ["default"]
    assert cfg.cluster_settings()["default"] is cfg
    assert cfg.is_protected is False


def test_clusters_env_adds_entries_that_inherit_connection_settings_only(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NIFI_API_URL", "https://nifi-prod.example.test")
    monkeypatch.setenv("NIFI_AUTH", "bearer")
    monkeypatch.setenv("NIFI_BEARER_TOKEN", "t")
    monkeypatch.setenv("NIFI_CLUSTER_NAME", "nifi-prod")
    monkeypatch.setenv("NIFI_ENVIRONMENT", "prod")
    monkeypatch.setenv("NIFI_NAMESPACE", "nifi-prod")
    monkeypatch.setenv("NIFI_CLUSTERS", json.dumps({"nifi-dev": {"api_url": DEV_ENTRY["api_url"]}}))
    configured = Settings(_env_file=None).cluster_settings()  # type: ignore[call-arg]
    assert list(configured) == ["nifi-prod", "nifi-dev"]
    prod, dev = configured["nifi-prod"], configured["nifi-dev"]
    assert prod.api_url == "https://nifi-prod.example.test/nifi-api"
    assert dev.api_url == "https://nifi-dev.example.test/nifi-api"
    assert dev.bearer_token == "t"
    assert dev.clusters == {}
    # Metadata belongs to each cluster: dev takes neither prod's environment nor its protection.
    assert (prod.environment, prod.is_protected, prod.namespace) == ("prod", True, "nifi-prod")
    assert (dev.environment, dev.is_protected, dev.namespace) == (None, False, None)


def test_protected_follows_environment_unless_set() -> None:
    assert settings(environment="prod").is_protected is True
    assert settings(environment="dev").is_protected is False
    assert settings(environment="dev", protected=True).is_protected is True
    assert settings(environment="prod", protected=False).is_protected is False


@pytest.mark.parametrize(
    ("entry", "message"),
    [
        ({"api_ur": "https://x.test"}, "unknown settings: api_ur"),
        ({"oauth_audience": "https://x.test"}, "server settings: oauth_audience"),
        ({"transport": "stdio"}, "server settings: transport"),
    ],
)
def test_a_cluster_entry_sets_only_known_nifi_settings(entry: dict, message: str) -> None:
    cfg = settings(clusters={"nifi-dev": entry})
    with pytest.raises(ValueError, match=message):
        cfg.cluster_settings()


def test_cluster_names_are_plain() -> None:
    cfg = settings(clusters={"Nifi Dev": {}})
    with pytest.raises(ValidationError):
        cfg.cluster_settings()


def test_cluster_names_are_unique() -> None:
    cfg = settings(cluster_name="nifi-prod", clusters={"nifi-prod": {}})
    with pytest.raises(ValueError, match="twice"):
        cfg.cluster_settings()


class Nifi:
    """A NiFi that records what reached it."""

    def __init__(self, name: str) -> None:
        self.name = name
        self.requests: list[httpx.Request] = []

    def handle(self, request: httpx.Request) -> httpx.Response:
        self.requests.append(request)
        if request.url.path.endswith("/flow/about"):
            return httpx.Response(200, json={"about": {"version": "2.12.0", "title": self.name}})
        if request.method == "POST" and request.url.path.endswith("/process-groups"):
            body = {"id": "00000000-0000-0000-0000-00000000000a", "revision": {"version": 1},
                    "component": {"id": "00000000-0000-0000-0000-00000000000a", "name": "g"}}
            return httpx.Response(201, json=body)
        if "/processors/" in request.url.path:
            return httpx.Response(404, json={"message": "not found"})
        return httpx.Response(200, json={"processGroupFlow": {"flow": {}}})


@pytest.fixture
def two_clusters(monkeypatch: pytest.MonkeyPatch):
    cfg = settings(cluster_name="nifi-prod", **PROD_META, clusters={"nifi-dev": DEV_ENTRY})
    configured = cfg.cluster_settings()
    prod, dev = Nifi("nifi-prod"), Nifi("nifi-dev")
    clients = {
        name: NiFiClient(configured[name], transport=httpx.MockTransport(nifi.handle))
        for name, nifi in (("nifi-prod", prod), ("nifi-dev", dev))
    }
    monkeypatch.setattr(server, "_clusters", configured)
    monkeypatch.setattr(server, "_clients", clients)
    return prod, dev


async def test_list_clusters_returns_every_cluster_with_its_metadata(two_clusters) -> None:
    payload = await _call("nifi_list_clusters")
    assert payload["cluster_required"] is True
    assert payload["clusters"] == [
        {"name": "nifi-prod", "protected": True, "api_url": "https://nifi.example.test/nifi-api",
         "readonly": False, **PROD_META},
        {"name": "nifi-dev", "protected": False, "api_url": "https://nifi-dev.example.test/nifi-api",
         "readonly": False, **{k: v for k, v in DEV_ENTRY.items() if k != "api_url"}},
    ]


async def test_a_read_goes_to_the_named_cluster_and_names_it(two_clusters) -> None:
    prod, dev = two_clusters
    about = await _call("nifi_about", {"cluster": "nifi-dev"})
    assert about["cluster"] == "nifi-dev"
    assert about["api_url"] == "https://nifi-dev.example.test/nifi-api"
    assert (len(prod.requests), len(dev.requests)) == (0, 1)
    flow = await _call("nifi_get_flow", {"cluster": "nifi-prod"})
    assert flow["cluster"] == "nifi-prod"
    assert (len(prod.requests), len(dev.requests)) == (1, 1)


@pytest.mark.parametrize("tool", ["nifi_about", "nifi_get_flow", "nifi_create_process_group"])
async def test_a_call_without_a_cluster_is_refused_and_sends_nothing(two_clusters, tool: str) -> None:
    prod, dev = two_clusters
    payload = await _call(tool, {"name": "g"} if tool == "nifi_create_process_group" else {})
    assert payload["status"] == "error"
    assert "params.cluster" in payload["error"]
    assert "nifi-prod, nifi-dev" in payload["error"]
    assert "cluster" not in payload
    assert prod.requests == dev.requests == []


@pytest.mark.parametrize("name", ["dev", "prod", "NIFI-DEV", "nifi-de", "nifi-dev "])
async def test_a_cluster_name_matches_exactly_or_is_refused(two_clusters, name: str) -> None:
    prod, dev = two_clusters
    payload = await _call("nifi_about", {"cluster": name})
    assert payload["status"] == "error"
    assert "Valid names: nifi-prod, nifi-dev" in payload["error"]
    assert prod.requests == dev.requests == []


async def test_an_unknown_cluster_is_refused_without_repeating_it(two_clusters) -> None:
    text = _text(await mcp.call_tool("nifi_about", {"params": {"cluster": "zqx7canary"}}))
    assert "zqx7canary" not in text


@pytest.mark.parametrize(
    ("confirm", "message"),
    [(None, 'needs params.confirm_cluster="nifi-prod"'), ("nifi-dev", "does not match cluster nifi-prod"),
     ("prod", "does not match cluster nifi-prod")],
)
async def test_a_prod_change_without_the_matching_confirmation_is_refused(two_clusters, confirm, message) -> None:
    prod, dev = two_clusters
    params = {"name": "g", "cluster": "nifi-prod", "confirm_cluster": confirm}
    payload = await _call("nifi_create_process_group", params)
    assert payload["status"] == "error"
    assert payload["outcome"] == "not_applied"
    assert message in payload["error"]
    assert prod.requests == dev.requests == []


async def test_a_prod_change_with_the_confirmation_lands_on_prod(two_clusters) -> None:
    prod, dev = two_clusters
    params = {"name": "g", "cluster": "nifi-prod", "confirm_cluster": "nifi-prod"}
    payload = await _call("nifi_create_process_group", params)
    assert (payload["outcome"], payload["cluster"]) == ("applied", "nifi-prod")
    assert dev.requests == []
    assert any(r.method == "POST" for r in prod.requests)


async def test_a_dev_change_needs_no_confirmation(two_clusters) -> None:
    prod, dev = two_clusters
    payload = await _call("nifi_create_process_group", {"name": "g", "cluster": "nifi-dev"})
    assert (payload["outcome"], payload["cluster"]) == ("applied", "nifi-dev")
    assert prod.requests == []
    assert any(r.method == "POST" for r in dev.requests)


async def test_a_mismatched_confirmation_is_refused_on_a_read_too(two_clusters) -> None:
    payload = await _call("nifi_about", {"cluster": "nifi-dev", "confirm_cluster": "nifi-prod"})
    assert "does not match cluster nifi-dev" in payload["error"]


async def test_an_error_after_the_cluster_is_chosen_names_it(two_clusters) -> None:
    component = "00000000-0000-0000-0000-00000000000b"
    payload = await _call("nifi_get_processor", {"cluster": "nifi-dev", "component_id": component})
    assert payload["status"] == "error"
    assert payload["cluster"] == "nifi-dev"


async def test_one_cluster_needs_no_name(monkeypatch: pytest.MonkeyPatch) -> None:
    nifi = Nifi("only")
    cfg = settings()
    server.configure(NiFiClient(cfg, transport=httpx.MockTransport(nifi.handle)), cfg)
    payload = await _call("nifi_create_process_group", {"name": "g"})
    assert (payload["outcome"], payload["cluster"]) == ("applied", "default")


async def test_one_protected_cluster_still_needs_the_confirmation(monkeypatch: pytest.MonkeyPatch) -> None:
    nifi = Nifi("only")
    cfg = settings(cluster_name="nifi-prod", environment="prod")
    server.configure(NiFiClient(cfg, transport=httpx.MockTransport(nifi.handle)), cfg)
    assert "confirm_cluster" in (await _call("nifi_create_process_group", {"name": "g"}))["error"]
    payload = await _call("nifi_create_process_group", {"name": "g", "confirm_cluster": "nifi-prod"})
    assert payload["outcome"] == "applied"


async def test_passthrough_forwards_the_caller_to_the_named_cluster(monkeypatch: pytest.MonkeyPatch) -> None:
    from mcp.server.auth.middleware.auth_context import auth_context_var
    from mcp.server.auth.middleware.bearer_auth import AuthenticatedUser
    from test_user_auth import settings as passthrough_settings

    from nifi_mcp.user_auth import UserToken

    cfg = passthrough_settings(cluster_name="nifi-prod", clusters={"nifi-dev": DEV_PROXY})
    reached: list[tuple[str, str, str, str]] = []

    def nifi(config, **kwargs):
        def handle(request: httpx.Request) -> httpx.Response:
            reached.append((request.url.host, config.proxy_cert, request.headers["X-ProxiedEntitiesChain"],
                            request.headers["X-ProxiedEntityGroups"]))
            return httpx.Response(200, json={"identity": "alice", "anonymous": False})

        return NiFiClient(config, transport=httpx.MockTransport(handle), **kwargs)

    monkeypatch.setattr(ssl.SSLContext, "load_cert_chain", lambda *args: None)
    monkeypatch.setattr(server, "_clusters", cfg.cluster_settings())
    monkeypatch.setattr(user_auth, "NiFiClient", nifi)
    caller = UserToken(token="t", client_id="vscode", scopes=[], identity="alice", groups=["ug-admins"])
    token = auth_context_var.set(AuthenticatedUser(caller))
    try:
        await server.nifi_current_user(ClusterIn(cluster="nifi-dev"))
        await server.nifi_current_user(ClusterIn(cluster="nifi-prod"))
    finally:
        auth_context_var.reset(token)
    assert reached == [
        ("nifi-dev.example.test", "dev.pem", "<alice>", "<ug-admins>"),
        ("nifi.example.test", "proxy.pem", "<alice>", "<ug-admins>"),
    ]
