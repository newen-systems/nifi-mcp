import json

import httpx
import pytest
from conftest import Router, settings

from nifi_mcp.client import NiFiClient
from nifi_mcp.errors import NiFiConflictError, NiFiError


@pytest.mark.asyncio
async def test_about_and_version(client: NiFiClient, router: Router) -> None:
    router.json("GET", "/flow/about", {"about": {"version": "2.10.0", "title": "NiFi"}})
    about = await client.about()
    assert client.version_tuple(about) == (2, 10, 0)


@pytest.mark.asyncio
async def test_create_processor_posts_revision_zero(client: NiFiClient, router: Router) -> None:
    captured: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content.decode())
        return httpx.Response(201, json={"id": "proc-1", "revision": {"version": 0}})

    router.add("POST", "/process-groups/pg-1/processors", handler)
    entity = await client.create_processor(
        "pg-1",
        "org.apache.nifi.processors.standard.LogAttribute",
        "Log it",
        properties={"Log Level": "info"},
        auto_terminated=["success"],
    )
    assert entity["id"] == "proc-1"
    body = captured["body"]
    assert isinstance(body, dict)
    assert body["revision"]["version"] == 0
    assert body["component"]["config"]["autoTerminatedRelationships"] == ["success"]


@pytest.mark.asyncio
async def test_409_is_conflict(client: NiFiClient, router: Router) -> None:
    router.json("DELETE", "/processors/p1", {"message": "running"}, status=409)
    with pytest.raises(NiFiConflictError):
        await client.delete_processor("p1", 1)


@pytest.mark.asyncio
async def test_get_retries_503_but_post_does_not(router: Router) -> None:
    hits = {"get": 0, "post": 0}

    def get_handler(_request: httpx.Request) -> httpx.Response:
        hits["get"] += 1
        if hits["get"] < 2:
            return httpx.Response(503, json={"message": "busy"})
        return httpx.Response(200, json={"ok": True})

    def post_handler(_request: httpx.Request) -> httpx.Response:
        hits["post"] += 1
        return httpx.Response(503, json={"message": "busy"})

    router.add("GET", "/flow/about", get_handler)
    router.add("POST", "/process-groups/root/process-groups", post_handler)
    transport = httpx.MockTransport(router.handle)
    nifi = NiFiClient(settings(), transport=transport)
    about = await nifi.get_json("/flow/about")
    assert about == {"ok": True}
    assert hits["get"] == 2
    with pytest.raises(NiFiError):
        await nifi.create_process_group("root", "sandbox")
    assert hits["post"] == 1


@pytest.mark.asyncio
async def test_401_refreshes_jwt_then_retries_get(router: Router) -> None:
    hits = {"token": 0, "about": 0}

    def token_handler(_request: httpx.Request) -> httpx.Response:
        hits["token"] += 1
        return httpx.Response(201, text=f"jwt-{hits['token']}")

    def about_handler(_request: httpx.Request) -> httpx.Response:
        hits["about"] += 1
        if hits["about"] == 1:
            return httpx.Response(401, json={"message": "expired"})
        return httpx.Response(200, json={"about": {"version": "2.10.0"}})

    router.add("POST", "/access/token", token_handler)
    router.add("GET", "/flow/about", about_handler)
    nifi = NiFiClient(
        settings(auth="jwt", username="alice", password="secret", bearer_token=None),
        transport=httpx.MockTransport(router.handle),
    )
    about = await nifi.about()
    assert about["about"]["version"] == "2.10.0"
    assert hits["token"] == 1
    assert hits["about"] == 2


@pytest.mark.asyncio
async def test_list_queue_polls_until_finished(client: NiFiClient, router: Router) -> None:
    router.json(
        "POST",
        "/flowfile-queues/c1/listing-requests",
        {"listingRequest": {"id": "lr-1", "finished": False}},
    )
    router.json(
        "GET",
        "/flowfile-queues/c1/listing-requests/lr-1",
        {"listingRequest": {"id": "lr-1", "finished": True, "flowFileSummaries": []}},
    )
    router.json("DELETE", "/flowfile-queues/c1/listing-requests/lr-1", {})
    result = await client.list_queue("c1")
    assert result["listingRequest"]["finished"] is True


@pytest.mark.asyncio
async def test_import_flow_json_body(client: NiFiClient, router: Router) -> None:
    captured: dict[str, object] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content.decode())
        return httpx.Response(201, json={"id": "pg-new", "component": {"name": "Imported"}})

    router.add("POST", "/process-groups/root/process-groups/import", handler)
    await client.import_flow("root", {"flowContents": {"name": "Imported"}}, "Imported")
    body = captured["body"]
    assert isinstance(body, dict)
    assert body["groupName"] == "Imported"
    assert body["flowSnapshot"]["flowContents"]["name"] == "Imported"


@pytest.mark.asyncio
async def test_list_queue_accepts_202_then_polls(client: NiFiClient, router: Router) -> None:
    router.json(
        "POST",
        "/flowfile-queues/c1/listing-requests",
        {"listingRequest": {"id": "lr-1", "finished": False}},
        status=202,
    )
    router.json(
        "GET",
        "/flowfile-queues/c1/listing-requests/lr-1",
        {"listingRequest": {"id": "lr-1", "finished": True, "flowFileSummaries": []}},
    )
    router.json("DELETE", "/flowfile-queues/c1/listing-requests/lr-1", {})
    result = await client.list_queue("c1")
    assert result["listingRequest"]["finished"] is True


@pytest.mark.asyncio
async def test_get_409_initializing_is_not_conflict(client: NiFiClient, router: Router) -> None:
    router.json(
        "GET",
        "/flow/current-user",
        {"message": "The Flow Controller is initializing the Data Flow."},
        status=409,
    )
    with pytest.raises(NiFiError, match="still starting") as excinfo:
        await client.current_user()
    assert not isinstance(excinfo.value, NiFiConflictError)


@pytest.mark.asyncio
async def test_authenticate_reuses_jwt(router: Router) -> None:
    hits = {"token": 0}

    def token_handler(_request: httpx.Request) -> httpx.Response:
        hits["token"] += 1
        return httpx.Response(201, text="jwt-1")

    router.add("POST", "/access/token", token_handler)
    nifi = NiFiClient(
        settings(auth="jwt", username="alice", password="secret", bearer_token=None),
        transport=httpx.MockTransport(router.handle),
    )
    await nifi.authenticate()
    await nifi.authenticate()
    assert hits["token"] == 1


def _capture_put(router: Router, path: str, reply: dict) -> dict:
    captured: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content.decode())
        return httpx.Response(200, json=reply)

    router.add("PUT", path, handler)
    return captured


@pytest.mark.asyncio
async def test_update_processor_does_not_echo_sensitive_mask(client: NiFiClient, router: Router) -> None:
    router.json(
        "GET",
        "/processors/p1",
        {
            "id": "p1",
            "revision": {"version": 3},
            "component": {
                "id": "p1",
                "name": "Fetch",
                "state": "RUNNING",
                "config": {"properties": {"Password": "********", "Hostname": "old"}},
            },
        },
    )
    sent = _capture_put(router, "/processors/p1", {"id": "p1", "revision": {"version": 4}})
    await client.update_processor("p1", version=3, properties={"Hostname": "new"})
    assert sent["body"]["component"] == {"id": "p1", "config": {"properties": {"Hostname": "new"}}}


@pytest.mark.asyncio
async def test_scheduling_and_move_updates_send_no_properties(client: NiFiClient, router: Router) -> None:
    sent = _capture_put(router, "/processors/p1", {"id": "p1", "revision": {"version": 4}})
    await client.update_processor("p1", version=3, scheduling_period="10 sec", x=0.0, y=272.0)
    assert sent["body"]["component"] == {
        "id": "p1",
        "config": {"schedulingPeriod": "10 sec"},
        "position": {"x": 0.0, "y": 272.0},
    }
    assert ("GET", "/processors/p1") not in router.calls


@pytest.mark.asyncio
async def test_update_controller_service_does_not_echo_sensitive_mask(client: NiFiClient, router: Router) -> None:
    router.json(
        "GET",
        "/controller-services/s1",
        {"id": "s1", "revision": {"version": 1}, "component": {"id": "s1", "properties": {"Password": "********"}}},
    )
    sent = _capture_put(router, "/controller-services/s1", {"id": "s1", "revision": {"version": 2}})
    await client.update_controller_service("s1", version=1, properties={"URL": "jdbc:x"})
    assert sent["body"]["component"] == {"id": "s1", "properties": {"URL": "jdbc:x"}}


@pytest.mark.asyncio
async def test_update_port_sends_position_only(client: NiFiClient, router: Router) -> None:
    router.json(
        "GET",
        "/input-ports/in1",
        {"id": "in1", "revision": {"version": 2}, "component": {"id": "in1", "name": "in", "state": "RUNNING"}},
    )
    sent = _capture_put(router, "/input-ports/in1", {"id": "in1", "revision": {"version": 3}})
    await client.update_port("in1", version=2, kind="INPUT_PORT", x=0.0, y=272.0)
    assert sent["body"]["component"] == {"id": "in1", "position": {"x": 0.0, "y": 272.0}}


@pytest.mark.asyncio
async def test_update_process_group_move_keeps_other_axis(client: NiFiClient, router: Router) -> None:
    router.json(
        "GET",
        "/process-groups/pg1",
        {"id": "pg1", "component": {"id": "pg1", "name": "g", "comments": "c", "position": {"x": 5, "y": 9}}},
    )
    sent = _capture_put(router, "/process-groups/pg1", {"id": "pg1"})
    await client.update_process_group("pg1", version=1, x=420.0)
    assert sent["body"]["component"] == {"id": "pg1", "position": {"x": 420.0, "y": 9}}


@pytest.mark.asyncio
async def test_empty_queue_timeout_is_an_error_and_does_not_cancel(client: NiFiClient, router: Router) -> None:
    router.json(
        "POST", "/flowfile-queues/c1/drop-requests", {"dropRequest": {"id": "d1", "finished": False}}, status=202
    )
    router.json(
        "GET",
        "/flowfile-queues/c1/drop-requests/d1",
        {"dropRequest": {"id": "d1", "finished": False, "percentCompleted": 40}},
    )
    router.json("DELETE", "/flowfile-queues/c1/drop-requests/d1", {})
    with pytest.raises(NiFiError, match="not cancelled"):
        await client.empty_queue("c1", timeout_seconds=0.3)
    assert ("DELETE", "/flowfile-queues/c1/drop-requests/d1") not in router.calls


@pytest.mark.asyncio
async def test_replace_flow_timeout_does_not_cancel(client: NiFiClient, router: Router) -> None:
    router.json("GET", "/process-groups/pg1", {"id": "pg1", "revision": {"version": 2}})
    router.json(
        "POST", "/process-groups/pg1/replace-requests", {"request": {"requestId": "rr-1", "complete": False}}
    )
    router.json("GET", "/process-groups/replace-requests/rr-1", {"request": {"requestId": "rr-1", "complete": False}})
    router.json("DELETE", "/process-groups/replace-requests/rr-1", {})
    with pytest.raises(NiFiError, match="still running"):
        await client.replace_flow("pg1", {"flowContents": {}}, timeout_seconds=0.6)
    assert ("DELETE", "/process-groups/replace-requests/rr-1") not in router.calls


@pytest.mark.asyncio
async def test_finished_drop_is_cleaned_up(client: NiFiClient, router: Router) -> None:
    router.json("POST", "/flowfile-queues/c1/drop-requests", {"dropRequest": {"id": "d1", "finished": False}})
    router.json("GET", "/flowfile-queues/c1/drop-requests/d1", {"dropRequest": {"id": "d1", "finished": True}})
    router.json("DELETE", "/flowfile-queues/c1/drop-requests/d1", {})
    result = await client.empty_queue("c1")
    assert result["dropRequest"]["finished"] is True
    assert ("DELETE", "/flowfile-queues/c1/drop-requests/d1") in router.calls


def _capture_post(router: Router, path: str) -> dict:
    captured: dict = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["body"] = json.loads(request.content.decode())
        return httpx.Response(201, json={"id": "c1", "revision": {"version": 0}})

    router.add("POST", path, handler)
    return captured


@pytest.mark.asyncio
async def test_port_connection_needs_no_relationships(client: NiFiClient, router: Router) -> None:
    sent = _capture_post(router, "/process-groups/parent/connections")
    await client.create_connection(
        "parent",
        source_id="out",
        source_group_id="ingest",
        source_type="OUTPUT_PORT",
        destination_id="in",
        destination_group_id="transform",
        destination_type="INPUT_PORT",
        relationships=[""],
    )
    assert "selectedRelationships" not in sent["body"]["component"]


@pytest.mark.asyncio
async def test_processor_connection_without_relationship_is_actionable(client: NiFiClient) -> None:
    with pytest.raises(NiFiError, match=r"needs at least one relationship"):
        await client.create_connection(
            "pg",
            source_id="p",
            source_group_id="pg",
            source_type="PROCESSOR",
            destination_id="q",
            destination_group_id="pg",
            destination_type="PROCESSOR",
            relationships=[],
        )


@pytest.mark.asyncio
async def test_connection_geometry_update_leaves_relationships_alone(client: NiFiClient, router: Router) -> None:
    router.json(
        "GET",
        "/connections/c1",
        {"id": "c1", "revision": {"version": 2}, "component": {"id": "c1", "source": {"type": "INPUT_PORT"}}},
    )
    sent = _capture_put(router, "/connections/c1", {"id": "c1", "revision": {"version": 3}})
    await client.update_connection("c1", version=2, bends=[])
    assert sent["body"]["component"] == {"id": "c1", "bends": []}


@pytest.mark.asyncio
@pytest.mark.parametrize("bad", ["../../../evil", "root?x=1", "%2e%2e", "abc#frag", ""])
async def test_process_group_id_cannot_leave_nifi_api(client: NiFiClient, router: Router, bad: str) -> None:
    with pytest.raises(NiFiError, match="unsafe NiFi API path"):
        await client.get_flow(bad)
    with pytest.raises(NiFiError, match="unsafe NiFi API path"):
        await client.create_processor(bad, "x.LogAttribute", "Log")
    assert router.calls == []


def test_tool_inputs_reject_non_uuid_ids() -> None:
    from pydantic import ValidationError

    from nifi_mcp.server import ComponentIdIn, ProcessGroupIn

    with pytest.raises(ValidationError):
        ProcessGroupIn(process_group_id="../../../evil")
    with pytest.raises(ValidationError):
        ComponentIdIn(component_id="p1?x=1")
    with pytest.raises(ValidationError):
        ComponentIdIn(component_id="a/b")
    assert ProcessGroupIn(process_group_id="root").process_group_id == "root"
    uuid = "e0c85b80-01a0-1000-ffff-ffffea8a5aa8"
    assert ComponentIdIn(component_id=uuid).component_id == uuid


@pytest.mark.asyncio
async def test_create_processor_sets_schedule(client: NiFiClient, router: Router) -> None:
    sent = _capture_post(router, "/process-groups/pg-1/processors")
    await client.create_processor(
        "pg-1", "x.GenerateFlowFile", "Gen", scheduling_period="10 sec", scheduling_strategy="TIMER_DRIVEN"
    )
    config = sent["body"]["component"]["config"]
    assert config == {"schedulingPeriod": "10 sec", "schedulingStrategy": "TIMER_DRIVEN"}
