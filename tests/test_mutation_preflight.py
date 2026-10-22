"""Lifecycle refusals happen before a write; read races retain existing outcomes."""

import json

import httpx
import pytest

from nifi_mcp.client import LEDGER
from nifi_mcp.errors import NiFiConflictError, NiFiError, Outcome
from nifi_mcp.mutation_preflight import Preflight
from nifi_mcp.render import Renderer


def processor(state="STOPPED", threads=0):
    return {
        "revision": {"version": 1},
        "component": {"id": "p", "parentGroupId": "g", "state": state},
        "status": {"aggregateSnapshot": {"activeThreadCount": threads}},
    }


def endpoint(identity="p", kind="PROCESSOR"):
    return {"id": identity, "groupId": "g", "type": kind}


def connection(queued=0, source=None, destination=None):
    return {
        "revision": {"version": 1},
        "component": {"id": "c", "source": source or endpoint(), "destination": destination or endpoint("q")},
        "status": {"aggregateSnapshot": {"flowFilesQueued": queued}},
    }


def setup(router, entity=None):
    router.json("GET", "/connections/c", entity or connection())
    router.json("GET", "/processors/p", processor())
    router.json("GET", "/processors/q", processor())
    router.json("DELETE", "/connections/c", {})
    router.json("PUT", "/connections/c", {})


@pytest.mark.parametrize("state,threads", [("RUNNING", 0), ("STOPPED", 1), ("STOPPING", 0)])
async def test_processor_configuration_refused_before_write(client, router, state, threads):
    router.json("GET", "/processors/p", processor(state, threads))
    with pytest.raises(NiFiConflictError) as error:
        await client.update_processor("p", version=1, properties={"Secret": "never echo"})
    assert error.value.outcome == Outcome.NOT_APPLIED
    assert all(method == "GET" for method, _ in router.calls)
    rendered = Renderer("never echo").tool_error(error.value, outcome=error.value.outcome, hint=error.value.hint)
    assert rendered["outcome"] == "not_applied"
    assert "active thread" in rendered["hint"]
    assert "never echo" not in json.dumps(rendered)


async def test_geometry_remains_available_without_lifecycle_reads(client, router):
    router.json("PUT", "/processors/p", {})
    router.json("PUT", "/connections/c", {})
    await client.update_processor("p", version=1, x=0, y=0)
    await client.update_connection("c", version=1, bends=[{"x": 1, "y": 2}])
    assert router.calls == [("PUT", "/processors/p"), ("PUT", "/connections/c")]


@pytest.mark.parametrize("queued", [2, None])
async def test_delete_queue_requires_confirmed_empty(client, router, queued):
    setup(router, connection(queued))
    with pytest.raises(NiFiConflictError, match="queue") as error:
        await client.delete_connection("c", 1)
    assert "user-authorized disposable test reset" in error.value.hint
    assert router.calls == [("GET", "/connections/c")]


@pytest.mark.parametrize("identity", ["p", "q"])
async def test_both_endpoints_must_be_quiescent(client, router, identity):
    setup(router)
    router.json("GET", f"/processors/{identity}", processor("STOPPED", 1))
    with pytest.raises(NiFiConflictError):
        await client.delete_connection("c", 1)
    assert all(method == "GET" for method, _ in router.calls)


async def test_empty_quiescent_connection_delete_once(client, router):
    setup(router)
    await client.delete_connection("c", 1)
    assert router.calls[-1] == ("DELETE", "/connections/c")
    assert len([m for m, _ in router.calls if m == "DELETE"]) == 1


async def test_stale_revision_not_silently_replaced(client, router):
    setup(router)
    with pytest.raises(NiFiConflictError, match="revision"):
        await client.delete_connection("c", 0)
    assert router.calls == [("GET", "/connections/c")]


async def test_read_failure_is_not_an_uncertain_mutation(client, router):
    def timeout(request):
        raise httpx.ReadTimeout("read failed", request=request)

    router.add("GET", "/connections/c", timeout)
    with pytest.raises(NiFiError) as error:
        await client.delete_connection("c", 1)
    assert error.value.outcome == Outcome.NOT_APPLIED
    assert "No mutation was sent" in error.value.hint
    assert all(method == "GET" for method, _ in router.calls)


async def test_revision_race_still_uses_server_refusal_without_retry(client, router):
    setup(router)
    router.json("DELETE", "/connections/c", {}, status=409)
    with pytest.raises(NiFiConflictError) as error:
        await client.delete_connection("c", 1)
    assert error.value.outcome == Outcome.NOT_APPLIED
    assert router.calls.count(("DELETE", "/connections/c")) == 1


async def test_write_timeout_remains_unknown_and_not_retried(client, router):
    setup(router)

    def timeout(request):
        raise httpx.ReadTimeout("write response lost", request=request)

    router.add("DELETE", "/connections/c", timeout)
    with pytest.raises(NiFiError) as error:
        await client.delete_connection("c", 1)
    assert error.value.outcome == Outcome.UNKNOWN
    assert router.calls.count(("DELETE", "/connections/c")) == 1


async def test_funnel_dependencies_walk_upstream_with_cycle_guard(client, router):
    setup(router, connection(source=endpoint("f", "FUNNEL")))
    router.json(
        "GET",
        "/flow/process-groups/g",
        {
            "processGroupFlow": {
                "flow": {
                    "connections": [
                        {"component": {"source": endpoint("f", "FUNNEL"), "destination": endpoint("f", "FUNNEL")}},
                        {"component": {"source": endpoint(), "destination": endpoint("f", "FUNNEL")}},
                    ]
                }
            }
        },
    )
    router.json("GET", "/processors/p", processor("RUNNING"))
    with pytest.raises(NiFiConflictError):
        await client.delete_connection("c", 1)
    assert router.calls.count(("GET", "/flow/process-groups/g")) == 1
    assert all(method == "GET" for method, _ in router.calls)


async def test_funnel_walk_is_bounded(client):
    checker = Preflight(client, "/connections/c")
    checker.seen = {("FUNNEL", str(i)) for i in range(128)}
    source = endpoint("next", "FUNNEL")
    with pytest.raises(NiFiConflictError, match="bounded"):
        await checker.source(source)


async def test_raw_reroute_checks_current_destination_not_new_one(client, router):
    setup(router)
    router.json("GET", "/processors/q", processor("RUNNING"))
    body = {"revision": {"version": 1}, "component": {"destination": endpoint("new")}}
    with pytest.raises(NiFiConflictError):
        await client.json("PUT", "/connections/c", json_body=body)
    assert ("GET", "/processors/q") in router.calls
    assert all(method == "GET" for method, _ in router.calls)


@pytest.mark.parametrize("kind", ["INPUT_PORT", "FUNNEL"])
async def test_connection_update_destination_exemptions(client, router, kind):
    setup(router, connection(destination=endpoint("port", kind)))
    await client.update_connection("c", version=1, back_pressure_object_threshold=10)
    assert router.calls == [("GET", "/connections/c"), ("GET", "/connections/c"), ("PUT", "/connections/c")]


async def test_input_port_not_exempt_from_delete(client, router):
    setup(router, connection(destination=endpoint("port", "INPUT_PORT")))
    router.json("GET", "/input-ports/port", processor("RUNNING"))
    with pytest.raises(NiFiConflictError):
        await client.delete_connection("c", 1)
    assert all(method == "GET" for method, _ in router.calls)


async def test_remote_endpoint_must_stop_transmitting(client, router):
    remote = {**endpoint("remote", "REMOTE_OUTPUT_PORT"), "running": True}
    setup(router, connection(source=remote))
    with pytest.raises(NiFiConflictError, match="transmitting"):
        await client.delete_connection("c", 1)


@pytest.mark.parametrize("attached", [True, False])
async def test_processor_delete_requires_explicit_detachment(client, router, attached):
    router.json("GET", "/processors/p", processor())
    router.json(
        "GET",
        "/flow/process-groups/g",
        {
            "processGroupFlow": {
                "flow": {
                    "connections": [connection(source=endpoint("other"), destination=endpoint())] if attached else []
                }
            }
        },
    )
    router.json("DELETE", "/processors/p", {})
    if attached:
        with pytest.raises(NiFiConflictError, match="incoming connections"):
            await client.delete_processor("p", 1)
        assert all(method == "GET" for method, _ in router.calls)
    else:
        await client.delete_processor("p", 1)
        assert router.calls[-1] == ("DELETE", "/processors/p")


async def test_preflight_preserves_earlier_applied_requests(client, router):
    setup(router, connection(queued=1))
    ledger = [{"method": "POST", "path": "/earlier", "outcome": "applied"}]
    token = LEDGER.set(ledger)
    try:
        with pytest.raises(NiFiConflictError):
            await client.delete_connection("c", 1)
        assert ledger == [
            {"method": "POST", "path": "/earlier", "outcome": "applied"},
            {"method": "DELETE", "path": "/connections/c", "outcome": "not_applied"},
        ]
    finally:
        LEDGER.reset(token)


@pytest.mark.parametrize("state", ["ENABLED", "ENABLING", "DISABLING"])
@pytest.mark.parametrize("action", ["delete", "update"])
async def test_service_requires_disabled(client, router, state, action):
    router.json("GET", "/controller-services/s", {"component": {"id": "s", "state": state}})
    operation = (client.delete_controller_service("s", 1) if action == "delete"
                 else client.update_controller_service("s", version=1, properties={}))
    with pytest.raises(NiFiConflictError, match="controller service"):
        await operation
    assert all(method == "GET" for method, _ in router.calls)


@pytest.mark.parametrize("state,threads", [("ENABLED", 0), ("ENABLING", 0), ("STOPPED", 1), ("RUNNING", 0)])
async def test_service_disable_waits_for_consumers(client, router, state, threads):
    router.json("GET", "/controller-services/s", {"component": {"id": "s", "state": "ENABLED"}})
    router.json(
        "GET",
        "/controller-services/s/references",
        {"controllerServiceReferencingComponents": [{"component": {"state": state, "activeThreadCount": threads}}]},
    )
    with pytest.raises(NiFiConflictError, match="consumer"):
        await client.set_controller_service_state("s", "DISABLED", 1)
    assert all(method == "GET" for method, _ in router.calls)


async def test_disabled_service_delete_does_not_require_removing_inactive_references(client, router):
    router.json("GET", "/controller-services/s", {"component": {"id": "s", "state": "DISABLED"}})
    router.json("DELETE", "/controller-services/s", {})
    await client.delete_controller_service("s", 1)
    assert router.calls[-1] == ("DELETE", "/controller-services/s")


async def test_port_delete_checks_parent_canvas_connections(client, router):
    router.json("GET", "/input-ports/p", processor())
    router.json("GET", "/flow/process-groups/g", {"processGroupFlow": {"flow": {}}})
    router.json("GET", "/process-groups/g", {"component": {"parentGroupId": "parent"}})
    router.json(
        "GET",
        "/flow/process-groups/parent",
        {
            "processGroupFlow": {
                "flow": {"connections": [connection(source=endpoint("outside"), destination=endpoint())]}
            }
        },
    )
    with pytest.raises(NiFiConflictError, match="incoming"):
        await client.delete_port("p", 1, "INPUT_PORT")
    assert all(method == "GET" for method, _ in router.calls)


async def test_funnel_delete_has_no_stoppable_state(client, router):
    router.json("GET", "/funnels/f", {"component": {"id": "f", "parentGroupId": "g"}})
    router.json("GET", "/flow/process-groups/g", {"processGroupFlow": {"flow": {}}})
    router.json("DELETE", "/funnels/f", {})
    await client.json("DELETE", "/funnels/f", params={"version": 1})
    assert router.calls[-1] == ("DELETE", "/funnels/f")


def group_setup(router, canvas=None, services=None):
    router.json("GET", "/process-groups/g", {"component": {"id": "g", "parentGroupId": "parent"}})
    router.json("GET", "/flow/process-groups/parent", {"processGroupFlow": {"flow": {}}})
    router.json("GET", "/flow/process-groups/g", {"processGroupFlow": {"flow": canvas or {}}})
    router.json("GET", "/flow/process-groups/g/controller-services", {"controllerServices": services or []})
    router.json("DELETE", "/process-groups/g", {})


@pytest.mark.parametrize(
    "canvas",
    [
        {"processors": [processor("RUNNING")]},
        {"processors": [processor("STOPPED", 1)]},
        {"connections": [connection(2)]},
        {"remoteProcessGroups": [{"component": {"transmitting": True}}]},
    ],
)
async def test_group_delete_rejects_active_or_queued_descendants(client, router, canvas):
    group_setup(router, canvas)
    with pytest.raises(NiFiConflictError):
        await client.delete_process_group("g", 1)
    assert all(method == "GET" for method, _ in router.calls)


async def test_group_delete_checks_owned_services_but_not_inherited(client, router):
    inherited = {"component": {"id": "s", "parentGroupId": "parent", "state": "ENABLED"}}
    group_setup(router, services=[inherited])
    await client.delete_process_group("g", 1)
    inherited["component"]["parentGroupId"] = "g"
    with pytest.raises(NiFiConflictError, match="controller service"):
        await client.delete_process_group("g", 1)


async def test_group_delete_checks_nested_canvas(client, router):
    group_setup(router, {"processGroups": [{"component": {"id": "child"}}]})
    router.json(
        "GET", "/flow/process-groups/child", {"processGroupFlow": {"flow": {"processors": [processor("RUNNING")]}}}
    )
    with pytest.raises(NiFiConflictError):
        await client.delete_process_group("g", 1)


async def test_group_delete_rejects_external_connection(client, router):
    group_setup(router)
    router.json("GET", "/flow/process-groups/parent", {"processGroupFlow": {"flow": {"connections": [connection()]}}})
    with pytest.raises(NiFiConflictError, match="external connections"):
        await client.delete_process_group("g", 1)


async def test_target_revision_changes_during_preflight(client, router):
    setup(router)
    calls = 0

    def changed(request):
        nonlocal calls
        calls += 1
        entity = connection()
        entity["revision"]["version"] = calls
        return httpx.Response(200, json=entity)

    router.add("GET", "/connections/c", changed)
    with pytest.raises(NiFiConflictError, match="revision"):
        await client.delete_connection("c", 1)
    assert all(method == "GET" for method, _ in router.calls)


async def test_preflight_read_failure_does_not_expose_remote_body(client, router):
    router.json("GET", "/connections/c", {"message": "password=do-not-echo"}, status=403)
    with pytest.raises(NiFiError) as error:
        await client.delete_connection("c", 1)
    assert error.value.outcome == Outcome.NOT_APPLIED
    assert "do-not-echo" not in str(error.value)
    assert error.value.body is None


@pytest.mark.parametrize(
    "proposed",
    [
        {"source": {"id": "different"}},
        {"selectedRelationships": []},
    ],
)
async def test_invalid_connection_rewire_refused_locally(client, router, proposed):
    setup(router)
    with pytest.raises(NiFiConflictError):
        await client.json("PUT", "/connections/c", json_body={"revision": {"version": 1}, "component": proposed})
    assert router.calls == [("GET", "/connections/c")]


async def test_group_walk_has_canvas_bound(client, router):
    group_setup(router, {"processGroups": [{"component": {"id": "child0"}}]})
    for index in range(32):
        router.json(
            "GET",
            f"/flow/process-groups/child{index}",
            {"processGroupFlow": {"flow": {"processGroups": [{"component": {"id": f"child{index + 1}"}}]}}},
        )
        router.json("GET", f"/flow/process-groups/child{index}/controller-services", {"controllerServices": []})
    with pytest.raises(NiFiConflictError, match="bounded"):
        await client.delete_process_group("g", 1)
    assert all(method == "GET" for method, _ in router.calls)
    assert len(router.calls) < 70


async def test_queue_becoming_nonempty_during_preflight_is_refused(client, router):
    setup(router)
    calls = 0

    def changed(request):
        nonlocal calls
        calls += 1
        return httpx.Response(200, json=connection(0 if calls == 1 else 1))

    router.add("GET", "/connections/c", changed)
    with pytest.raises(NiFiConflictError, match="queue changed"):
        await client.delete_connection("c", 1)
    assert all(method == "GET" for method, _ in router.calls)


async def test_service_disable_after_consumers_stop(client, router):
    router.json("GET", "/controller-services/s", {"component": {"id": "s", "state": "ENABLED"}})
    router.json(
        "GET",
        "/controller-services/s/references",
        {
            "controllerServiceReferencingComponents": [
                {"component": {"state": "STOPPED", "activeThreadCount": 0}},
                {"component": {"state": "DISABLED"}},
            ]
        },
    )
    router.json("PUT", "/controller-services/s/run-status", {})
    await client.set_controller_service_state("s", "DISABLED", 1)
    assert router.calls[-1] == ("PUT", "/controller-services/s/run-status")


async def test_blocked_endpoint_uuid_survives_renderer_without_names(client, router):
    identity = "00000000-0000-0000-0000-000000000123"
    setup(router, connection(source=endpoint(identity)))
    held = processor("RUNNING")
    held["component"].update({"id": identity, "name": "password=should-not-appear"})
    router.json("GET", f"/processors/{identity}", held)
    with pytest.raises(NiFiError) as error:
        await client.delete_connection("c", 1)
    payload = Renderer({"component_id": identity, "count": 123}).tool_error(
        error.value, outcome=error.value.outcome, hint=error.value.hint
    )
    assert identity in payload["hint"]
    assert "should-not-appear" not in json.dumps(payload)


async def test_untrusted_component_identifier_is_not_quoted(client, router):
    held = processor("RUNNING")
    held["component"]["id"] = "credential=should-not-appear"
    router.json("GET", "/processors/p", held)
    with pytest.raises(NiFiError) as error:
        await client.update_processor("p", version=1, name="rename")
    assert "should-not-appear" not in str(error.value)
    assert "should-not-appear" not in error.value.hint


@pytest.mark.parametrize(
    "body,expected",
    [
        ("Queue not empty for connection", "drain"),
        ("component is not disabled", "lifecycle blocker"),
        ("unexpected conflict password=should-not-appear", "identify whether"),
    ],
)
async def test_remote_conflict_guidance_is_specific_and_does_not_echo_body(client, router, body, expected):
    router.add("POST", "/process-groups/g/processors", lambda request: httpx.Response(409, text=body))
    with pytest.raises(NiFiConflictError) as error:
        await client.create_processor("g", "x.P", "P")
    assert expected in error.value.hint
    assert "should-not-appear" not in error.value.hint
    assert "Do not blindly retry" in error.value.hint
    assert error.value.outcome == Outcome.NOT_APPLIED
    rendered = Renderer().tool_error(error.value, hint=error.value.hint)
    assert "should-not-appear" not in json.dumps(rendered)
