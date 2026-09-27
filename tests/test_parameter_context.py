import json

import httpx
import pytest
from conftest import Router, settings

from nifi_mcp.client import NiFiClient
from nifi_mcp.server import (
    BindParameterContextIn,
    CreateParameterContextIn,
    CreateProcessGroupIn,
    DeleteComponentIn,
    ParameterIn,
    UpdateParameterContextIn,
    configure,
    nifi_bind_parameter_context,
    nifi_create_parameter_context,
    nifi_create_process_group,
    nifi_delete_component,
    nifi_update_parameter_context,
)

CTX_ID = "00000000-0000-0000-0000-000000000001"
PG_ID = "00000000-0000-0000-0000-000000000002"
CTX = {
    "id": CTX_ID,
    "revision": {"version": 0},
    "component": {
        "id": CTX_ID,
        "name": "review-params",
        "parameters": [{"parameter": {"name": "env", "value": "lab", "sensitive": False}}],
    },
}


def _capture(router: Router, method: str, path: str, reply: dict, status: int = 200) -> dict:
    captured: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content.decode()) if request.content else None
        captured["params"] = dict(request.url.params)
        return httpx.Response(status, json=reply)

    router.add(method, path, handler)
    return captured


def _wire(router: Router) -> None:
    configure(NiFiClient(settings(), transport=httpx.MockTransport(router.handle)), settings())


@pytest.mark.asyncio
async def test_create_parameter_context_posts_revision_zero(router: Router) -> None:
    sent = _capture(router, "POST", "/parameter-contexts", CTX, status=201)
    _wire(router)
    raw = await nifi_create_parameter_context(
        CreateParameterContextIn(name="review-params", parameters=[ParameterIn(name="env", value="lab")])
    )
    payload = json.loads(raw)
    assert payload["status"] == "ok"
    assert payload["parameter_context"]["id"] == CTX_ID
    assert payload["parameter_context"]["parameters"] == [{"name": "env", "value": "lab", "sensitive": False}]
    body = sent["body"]
    assert body["revision"]["version"] == 0
    assert body["component"]["name"] == "review-params"
    assert body["component"]["parameters"] == [{"parameter": {"name": "env", "value": "lab", "sensitive": False}}]


@pytest.mark.asyncio
async def test_update_parameter_context_uses_update_request(router: Router) -> None:
    router.json("GET", f"/parameter-contexts/{CTX_ID}", {**CTX, "revision": {"version": 4}})
    sent = _capture(
        router,
        "POST",
        f"/parameter-contexts/{CTX_ID}/update-requests",
        {"request": {"requestId": "u1", "complete": False}},
    )
    router.json(
        "GET", f"/parameter-contexts/{CTX_ID}/update-requests/u1", {"request": {"requestId": "u1", "complete": True}}
    )
    router.json("DELETE", f"/parameter-contexts/{CTX_ID}/update-requests/u1", {})
    _wire(router)
    raw = await nifi_update_parameter_context(
        UpdateParameterContextIn(
            parameter_context_id=CTX_ID,
            parameters=[ParameterIn(name="db_pass", value="x", sensitive=True)],
            remove=["old"],
        )
    )
    assert json.loads(raw)["status"] == "ok"
    body = sent["body"]
    assert body["revision"]["version"] == 4
    assert body["component"]["id"] == CTX_ID
    assert body["component"]["parameters"] == [
        {"parameter": {"name": "db_pass", "value": "x", "sensitive": True}},
        {"parameter": {"name": "old"}},
    ]


@pytest.mark.asyncio
async def test_update_parameter_context_failure_is_an_error(router: Router) -> None:
    router.json("GET", f"/parameter-contexts/{CTX_ID}", CTX)
    router.json(
        "POST",
        f"/parameter-contexts/{CTX_ID}/update-requests",
        {"request": {"requestId": "u1", "complete": True, "failureReason": "processor is running"}},
    )
    router.json("DELETE", f"/parameter-contexts/{CTX_ID}/update-requests/u1", {})
    _wire(router)
    payload = json.loads(
        await nifi_update_parameter_context(UpdateParameterContextIn(parameter_context_id=CTX_ID))
    )
    assert payload["status"] == "error"
    assert "processor is running" in payload["error"]


@pytest.mark.asyncio
async def test_bind_parameter_context_puts_reference(router: Router) -> None:
    group = {"id": PG_ID, "revision": {"version": 7}, "component": {"id": PG_ID}}
    router.json("GET", f"/process-groups/{PG_ID}", group)
    router.json("GET", f"/flow/process-groups/{PG_ID}", {"processGroupFlow": {"id": PG_ID, "flow": {}}})
    sent = _capture(
        router,
        "PUT",
        f"/process-groups/{PG_ID}",
        {"id": PG_ID, "revision": {"version": 8}, "component": {"id": PG_ID, "parameterContext": {"id": CTX_ID}}},
    )
    _wire(router)
    payload = json.loads(
        await nifi_bind_parameter_context(
            BindParameterContextIn(process_group_id=PG_ID, parameter_context_id=CTX_ID)
        )
    )
    assert payload["process_group"]["parameter_context"] == CTX_ID
    assert sent["body"]["revision"]["version"] == 7
    assert sent["body"]["component"] == {"id": PG_ID, "parameterContext": {"id": CTX_ID}}


@pytest.mark.asyncio
async def test_unbind_sends_null_id(router: Router) -> None:
    router.json("GET", f"/process-groups/{PG_ID}", {"id": PG_ID, "revision": {"version": 1}, "component": {}})
    sent = _capture(router, "PUT", f"/process-groups/{PG_ID}", {"id": PG_ID, "revision": {"version": 2}})
    _wire(router)
    await nifi_bind_parameter_context(BindParameterContextIn(process_group_id=PG_ID, parameter_context_id=None))
    assert sent["body"]["component"]["parameterContext"] == {"id": None}


@pytest.mark.asyncio
async def test_delete_component_accepts_parameter_context(router: Router) -> None:
    router.json("GET", f"/parameter-contexts/{CTX_ID}", {**CTX, "revision": {"version": 3}})
    sent = _capture(router, "DELETE", f"/parameter-contexts/{CTX_ID}", CTX)
    _wire(router)
    payload = json.loads(
        await nifi_delete_component(DeleteComponentIn(kind="parameter_context", component_id=CTX_ID))
    )
    assert payload["status"] == "ok"
    assert sent["params"]["version"] == "3"


@pytest.mark.asyncio
async def test_create_process_group_can_bind_context(router: Router) -> None:
    router.json("GET", "/flow/process-groups/root", {"processGroupFlow": {"id": "root", "flow": {}}})
    sent = _capture(router, "POST", "/process-groups/root/process-groups", {"id": "pg-2"}, status=201)
    _wire(router)
    await nifi_create_process_group(CreateProcessGroupIn(name="review", parameter_context_id=CTX_ID))
    assert sent["body"]["component"]["parameterContext"] == {"id": CTX_ID}


CHILD_A = "00000000-0000-0000-0000-00000000000a"
CHILD_B = "00000000-0000-0000-0000-00000000000b"
OTHER = "00000000-0000-0000-0000-0000000000cc"


@pytest.mark.asyncio
async def test_recursive_bind_is_one_put_with_all_descendants(router: Router) -> None:
    router.json("GET", f"/process-groups/{PG_ID}", {"id": PG_ID, "revision": {"version": 1}, "component": {}})
    sent = _capture(
        router,
        "PUT",
        f"/process-groups/{PG_ID}",
        {"id": PG_ID, "revision": {"version": 2}, "component": {"id": PG_ID, "parameterContext": {"id": CTX_ID}}},
    )
    _wire(router)
    payload = json.loads(
        await nifi_bind_parameter_context(BindParameterContextIn(process_group_id=PG_ID, parameter_context_id=CTX_ID))
    )
    assert payload["status"] == "ok"
    assert payload["applied_to"] == "ALL_DESCENDANTS"
    assert sent["body"]["processGroupUpdateStrategy"] == "ALL_DESCENDANTS"
    assert sent["body"]["revision"]["version"] == 1
    assert sent["body"]["component"] == {"id": PG_ID, "parameterContext": {"id": CTX_ID}}
    # The server finds and rebinds every descendant under one write lock; the client walks nothing.
    assert router.calls == [("GET", f"/process-groups/{PG_ID}"), ("PUT", f"/process-groups/{PG_ID}")]


@pytest.mark.asyncio
async def test_non_recursive_bind_sends_no_update_strategy(router: Router) -> None:
    router.json("GET", f"/process-groups/{PG_ID}", {"id": PG_ID, "revision": {"version": 1}, "component": {}})
    sent = _capture(router, "PUT", f"/process-groups/{PG_ID}", {"id": PG_ID, "revision": {"version": 2}})
    _wire(router)
    payload = json.loads(
        await nifi_bind_parameter_context(
            BindParameterContextIn(process_group_id=PG_ID, parameter_context_id=CTX_ID, apply_recursively=False)
        )
    )
    assert payload["applied_to"] == "THIS_GROUP"
    assert "processGroupUpdateStrategy" not in sent["body"]


@pytest.mark.asyncio
async def test_failed_recursive_bind_is_a_single_rejected_request(router: Router) -> None:
    router.json("GET", f"/process-groups/{PG_ID}", {"id": PG_ID, "revision": {"version": 1}, "component": {}})
    router.json("PUT", f"/process-groups/{PG_ID}", {"message": "child is running"}, status=409)
    _wire(router)
    payload = json.loads(
        await nifi_bind_parameter_context(BindParameterContextIn(process_group_id=PG_ID, parameter_context_id=CTX_ID))
    )
    assert payload["status"] == "error"
    assert payload["type"] == "NiFiConflictError"
    assert [call for call in router.calls if call[0] == "PUT"] == [("PUT", f"/process-groups/{PG_ID}")]


@pytest.mark.asyncio
async def test_create_process_group_inherits_parent_context(router: Router) -> None:
    router.json("GET", f"/flow/process-groups/{PG_ID}", {"processGroupFlow": {"id": PG_ID, "flow": {}}})
    router.json(
        "GET",
        f"/process-groups/{PG_ID}",
        {"id": PG_ID, "component": {"id": PG_ID, "parameterContext": {"id": CTX_ID}}},
    )
    sent = _capture(router, "POST", f"/process-groups/{PG_ID}/process-groups", {"id": CHILD_A}, status=201)
    _wire(router)
    await nifi_create_process_group(CreateProcessGroupIn(parent_id=PG_ID, name="ingest"))
    assert sent["body"]["component"]["parameterContext"] == {"id": CTX_ID}


SECRET_CTX = {
    "id": CTX_ID,
    "revision": {"version": 2},
    "component": {
        "id": CTX_ID,
        "name": "review-params",
        "parameters": [{"parameter": {"name": "api", "value": None, "sensitive": True}}],
    },
}


def _update_request_routes(router: Router, after: dict) -> dict:
    """First GET returns SECRET_CTX, the GET after the update returns `after`."""
    replies = iter([SECRET_CTX, after])
    router.add("GET", f"/parameter-contexts/{CTX_ID}", lambda _req: httpx.Response(200, json=next(replies)))
    sent = _capture(
        router,
        "POST",
        f"/parameter-contexts/{CTX_ID}/update-requests",
        {"request": {"requestId": "u1", "complete": True}},
    )
    router.json("DELETE", f"/parameter-contexts/{CTX_ID}/update-requests/u1", {})
    return sent


@pytest.mark.asyncio
async def test_description_update_keeps_sensitive_flag(router: Router) -> None:
    after = {
        **SECRET_CTX,
        "component": {
            **SECRET_CTX["component"],
            "parameters": [
                {"parameter": {"name": "api", "value": "canary-1234", "sensitive": False, "description": "rotated"}}
            ],
        },
    }
    sent = _update_request_routes(router, after)
    _wire(router)
    raw = await nifi_update_parameter_context(
        UpdateParameterContextIn(
            parameter_context_id=CTX_ID, parameters=[ParameterIn(name="api", description="rotated")]
        )
    )
    assert json.loads(raw)["status"] == "ok", raw
    posted = sent["body"]["component"]["parameters"][0]["parameter"]
    assert posted["sensitive"] is True
    assert "canary-1234" not in raw


@pytest.mark.asyncio
async def test_clearing_sensitive_without_a_new_value_is_refused(router: Router) -> None:
    sent = _update_request_routes(router, SECRET_CTX)
    _wire(router)
    payload = json.loads(
        await nifi_update_parameter_context(
            UpdateParameterContextIn(
                parameter_context_id=CTX_ID, parameters=[ParameterIn(name="api", sensitive=False, description="x")]
            )
        )
    )
    assert payload["status"] == "error"
    assert "parameters[0]" in payload["error"]
    assert sent == {}


@pytest.mark.asyncio
async def test_new_parameter_without_sensitive_is_not_sensitive(router: Router) -> None:
    sent = _update_request_routes(router, SECRET_CTX)
    _wire(router)
    await nifi_update_parameter_context(
        UpdateParameterContextIn(parameter_context_id=CTX_ID, parameters=[ParameterIn(name="env", value="lab")])
    )
    posted = sent["body"]["component"]["parameters"]
    assert posted == [{"parameter": {"name": "env", "value": "lab", "sensitive": False}}]


PLAIN_CTX = {
    "id": CTX_ID,
    "revision": {"version": 2},
    "component": {
        "id": CTX_ID,
        "name": "review-params",
        "parameters": [{"parameter": {"name": "env", "value": "lab", "sensitive": False}}],
    },
}


async def _refused_update(router: Router, context: dict, **kwargs: object) -> dict:
    router.add("GET", f"/parameter-contexts/{CTX_ID}", lambda _req: httpx.Response(200, json=context))
    sent = _capture(
        router, "POST", f"/parameter-contexts/{CTX_ID}/update-requests", {"request": {"requestId": "u1"}}
    )
    _wire(router)
    payload = json.loads(
        await nifi_update_parameter_context(UpdateParameterContextIn(parameter_context_id=CTX_ID, **kwargs))
    )
    assert payload["status"] == "error"
    assert sent == {}
    return payload


@pytest.mark.asyncio
async def test_declassify_with_a_value_is_refused_and_points_at_remove_then_add(router: Router) -> None:
    # StandardParameterContext.validateSensitiveFlag rejects a flag change that carries a value.
    payload = await _refused_update(
        router, SECRET_CTX, parameters=[ParameterIn(name="api", value="plain", sensitive=False)]
    )
    assert "cannot change the sensitive flag" in payload["error"]
    assert "remove" in payload["error"]
    assert "needs a new value" not in payload["error"]


@pytest.mark.asyncio
async def test_promote_with_a_value_is_refused(router: Router) -> None:
    payload = await _refused_update(
        router, PLAIN_CTX, parameters=[ParameterIn(name="env", value="prod", sensitive=True)]
    )
    assert "(parameters[0])" in payload["error"]
    assert "cannot change the sensitive flag" in payload["error"]


@pytest.mark.asyncio
async def test_same_name_in_parameters_and_remove_is_refused(router: Router) -> None:
    # StandardParameterContextDAO.getParameters keys the update by name: only one entry survives.
    payload = await _refused_update(
        router, SECRET_CTX, parameters=[ParameterIn(name="api", value="plain", sensitive=False)], remove=["api"]
    )
    assert "parameters[0] also appear in remove" in payload["error"]


def test_parameter_sensitive_description_does_not_advertise_a_value_retry() -> None:
    description = ParameterIn.model_fields["sensitive"].description or ""
    assert "needs a new value" not in description
    assert "remove" in description
