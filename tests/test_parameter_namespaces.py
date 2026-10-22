"""Root-owned names and inherited composition are enforced before writes."""
import json

import httpx
import pytest
from conftest import Router, settings

from nifi_mcp.client import NiFiClient
from nifi_mcp.compact import compact_parameter_context
from nifi_mcp.errors import NiFiError
from nifi_mcp.parameters import validate_parameter_ownership


@pytest.mark.parametrize("name", ["username", "pypi.username", "GitLab.pypi.username", "gitlab..username",
                                  "gitlab_pypi_username", "gitlab.pypi.user_name", " gitlab.pypi.username"])
def test_new_parameters_require_canonical_names(name: str) -> None:
    with pytest.raises(NiFiError, match=r"root\.service\.key"):
        validate_parameter_ownership("gitlab", [{"name": name}])


def test_service_and_nested_keys_share_root_context() -> None:
    validate_parameter_ownership("gitlab", [{"name": "gitlab.pypi.username"},
                                           {"name": "gitlab.container.registry.url"}])


def test_mixed_root_parameters_and_duplicates_are_refused() -> None:
    with pytest.raises(NiFiError, match="owning context"):
        validate_parameter_ownership("gitlab", [{"name": "redis.cache.host"}])
    with pytest.raises(NiFiError, match="duplicates"):
        validate_parameter_ownership("gitlab", [{"name": "gitlab.pypi.username"}] * 2)


@pytest.mark.asyncio
async def test_bad_create_sends_no_request(client: NiFiClient, router: Router) -> None:
    with pytest.raises(NiFiError, match="owning context"):
        await client.create_parameter_context("random", parameters=[{"name": "gitlab.pypi.username"}])
    assert router.calls == []


@pytest.mark.asyncio
async def test_composed_context_inherits_roots_without_copying_values(client: NiFiClient, router: Router) -> None:
    bodies = []

    def capture(request: httpx.Request) -> httpx.Response:
        bodies.append(json.loads(request.content))
        return httpx.Response(201, json={"id": "composed"})

    router.add("POST", "/parameter-contexts", capture)
    await client.create_parameter_context("image-transfer", inherited_context_ids=["gitlab-id", "redis-id"])
    assert bodies[0]["component"]["parameters"] == []
    assert bodies[0]["component"]["inheritedParameterContexts"] == [
        {"id": "gitlab-id", "component": {"id": "gitlab-id"}},
        {"id": "redis-id", "component": {"id": "redis-id"}},
    ]


@pytest.mark.asyncio
async def test_updates_keep_legacy_names_but_refuse_new_aliases(router: Router) -> None:
    router.json("GET", "/parameter-contexts/ctx", {"component": {"name": "gitlab", "parameters": [
        {"parameter": {"name": "old_username", "sensitive": False}}
    ]}})
    client = NiFiClient(settings(), transport=httpx.MockTransport(router.handle))
    try:
        with pytest.raises(NiFiError, match=r"root\.service\.key"):
            await client.update_parameter_context("ctx", parameters=[{"name": "another_username"}])
    finally:
        await client.aclose()
    assert router.calls == [("GET", "/parameter-contexts/ctx")]
    validate_parameter_ownership("gitlab", [{"name": "old_username"}], existing={"old_username"})


def test_context_view_sorts_names_and_exposes_inheritance() -> None:
    view = compact_parameter_context({"component": {"name": "transfer", "parameters": [
        {"parameter": {"name": "redis.cache.host"}}, {"parameter": {"name": "gitlab.pypi.username"}}
    ], "inheritedParameterContexts": [{"id": "gitlab-id", "component": {"name": "gitlab"}}]}})
    assert [p["name"] for p in view["parameters"]] == ["gitlab.pypi.username", "redis.cache.host"]
    assert view["inherited_contexts"] == [{"id": "gitlab-id", "name": "gitlab"}]


@pytest.mark.asyncio
@pytest.mark.parametrize("inheritance", [None, [], ["root-id"]])
async def test_update_inheritance_omitted_preserved_or_replaced(
    client: NiFiClient, router: Router, inheritance
) -> None:
    context = {"revision": {"version": 1}, "component": {"name": "transfer", "parameters": []}}
    router.json("GET", "/parameter-contexts/ctx", context)
    bodies = []

    def capture(request: httpx.Request) -> httpx.Response:
        bodies.append(json.loads(request.content))
        return httpx.Response(202, json={"request": {"requestId": "update", "complete": True}})

    router.add("POST", "/parameter-contexts/ctx/update-requests", capture)
    router.json("DELETE", "/parameter-contexts/ctx/update-requests/update", {})
    await client.update_parameter_context("ctx", inherited_context_ids=inheritance)
    component = bodies[0]["component"]
    if inheritance is None:
        assert "inheritedParameterContexts" not in component
    else:
        assert component["inheritedParameterContexts"] == [
            {"id": cid, "component": {"id": cid}} for cid in inheritance
        ]


@pytest.mark.asyncio
async def test_root_rename_cannot_orphan_owned_parameters(client: NiFiClient, router: Router) -> None:
    router.json("GET", "/parameter-contexts/ctx", {"component": {"name": "gitlab", "parameters": [
        {"parameter": {"name": "gitlab.pypi.username"}}
    ]}})
    with pytest.raises(NiFiError, match="owning context"):
        await client.update_parameter_context("ctx", name="random")
    assert router.calls == [("GET", "/parameter-contexts/ctx")]
