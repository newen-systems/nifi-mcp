import json

import pytest
from conftest import settings

from nifi_mcp.errors import NiFiError, NiFiReadOnlyError
from nifi_mcp.server import (
    ApplySpecIn,
    CreateConnectionIn,
    CreateProcessGroupIn,
    ProcessGroupIn,
    ProcessorTypesIn,
    configure,
    nifi_about,
    nifi_apply_flow_spec,
    nifi_create_connection,
    nifi_create_process_group,
    nifi_get_flow,
    nifi_list_processor_types,
)


class FakeNiFi:
    def __init__(self) -> None:
        self.authenticated = False

    async def authenticate(self) -> None:
        self.authenticated = True

    async def get_flow(self, process_group_id: str) -> dict:
        return {
            "processGroupFlow": {
                "id": process_group_id,
                "breadcrumb": {"breadcrumb": {"id": process_group_id, "name": "NiFi Flow"}},
                "flow": {"processors": [], "connections": [], "processGroups": []},
            }
        }

    async def about(self) -> dict:
        return {"about": {"version": "2.10.0", "title": "NiFi"}}

    async def create_process_group(self, parent_id: str, name: str, **kwargs) -> dict:
        return {"id": "pg-9", "revision": {"version": 0}, "component": {"name": name}}

    async def get_process_group(self, process_group_id: str) -> dict:
        return {"id": process_group_id, "revision": {"version": 1}, "component": {"id": process_group_id}}


@pytest.mark.asyncio
async def test_about_reports_nifi_2() -> None:
    configure(FakeNiFi(), settings())  # type: ignore[arg-type]
    payload = json.loads(await nifi_about())
    assert payload["is_nifi_2x"] is True
    assert payload["version"] == "2.10.0"
    assert payload["readonly"] is False


@pytest.mark.asyncio
async def test_get_flow_compact() -> None:
    configure(FakeNiFi(), settings())  # type: ignore[arg-type]
    raw = await nifi_get_flow(ProcessGroupIn(process_group_id="root"))
    payload = json.loads(raw)
    assert payload["name"] == "NiFi Flow"
    assert payload["processors"] == []


@pytest.mark.asyncio
async def test_create_process_group_allowed_when_writable() -> None:
    configure(FakeNiFi(), settings())  # type: ignore[arg-type]
    raw = await nifi_create_process_group(CreateProcessGroupIn(name="sandbox"))
    payload = json.loads(raw)
    assert payload["status"] == "ok"
    assert payload["process_group"]["name"] == "sandbox"


@pytest.mark.asyncio
async def test_create_process_group_blocked_when_readonly() -> None:
    configure(FakeNiFi(), settings(readonly=True))  # type: ignore[arg-type]
    raw = await nifi_create_process_group(CreateProcessGroupIn(name="sandbox"))
    payload = json.loads(raw)
    assert payload["status"] == "error"
    assert payload["type"] == NiFiReadOnlyError.__name__


@pytest.mark.asyncio
async def test_apply_spec_requires_write() -> None:
    configure(FakeNiFi(), settings(readonly=True))  # type: ignore[arg-type]
    raw = await nifi_apply_flow_spec(ApplySpecIn(spec={"objects": []}))
    payload = json.loads(raw)
    assert payload["type"] == NiFiReadOnlyError.__name__


@pytest.mark.asyncio
async def test_create_connection_accepts_empty_relationships_for_ports() -> None:
    class Fake(FakeNiFi):
        async def create_connection(self, parent_id: str, **kwargs) -> dict:
            self.kwargs = kwargs
            return {"id": "c1", "component": {"source": {"id": kwargs["source_id"]}}}

    fake = Fake()
    configure(fake, settings())  # type: ignore[arg-type]
    raw = await nifi_create_connection(
        CreateConnectionIn(
            parent_id="00000000-0000-0000-0000-000000000010",
            source_id="00000000-0000-0000-0000-000000000011",
            source_group_id="00000000-0000-0000-0000-000000000012",
            source_type="OUTPUT_PORT",
            destination_id="00000000-0000-0000-0000-000000000013",
            destination_group_id="00000000-0000-0000-0000-000000000014",
            destination_type="INPUT_PORT",
        )
    )
    assert json.loads(raw)["status"] == "ok"
    assert fake.kwargs["relationships"] == []


@pytest.mark.asyncio
async def test_processor_type_substring_matches_short_name(router) -> None:
    import httpx

    from nifi_mcp.client import NiFiClient

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.params.get("type"):
            return httpx.Response(200, json={"processorTypes": []})
        return httpx.Response(
            200,
            json={
                "processorTypes": [
                    {"type": "org.apache.nifi.processors.standard.GenerateFlowFile"},
                    {"type": "org.apache.nifi.processors.standard.LogAttribute"},
                ]
            },
        )

    router.add("GET", "/flow/processor-types", handler)
    configure(NiFiClient(settings(), transport=httpx.MockTransport(router.handle)), settings())
    payload = json.loads(await nifi_list_processor_types(ProcessorTypesIn(type_filter="generateflowfile")))
    assert payload["count"] == 1
    assert payload["processor_types"][0]["type"].endswith(".GenerateFlowFile")


class PlacingNiFi(FakeNiFi):
    """Canvas that reports every processor created so far, plus one child group at (0,0)."""

    def __init__(self) -> None:
        super().__init__()
        self.placed: list[tuple[float, float]] = []

    async def get_flow(self, process_group_id: str) -> dict:
        procs = [
            {"id": f"p{i}", "component": {"id": f"p{i}", "name": f"P{i}", "position": {"x": x, "y": y}}}
            for i, (x, y) in enumerate(self.placed)
        ]
        group = {"id": "g0", "component": {"id": "g0", "name": "child", "position": {"x": 0, "y": 0}}}
        return {"processGroupFlow": {"id": process_group_id, "flow": {"processors": procs, "processGroups": [group]}}}

    async def create_processor(self, parent_id: str, processor_type: str, name: str, **kwargs) -> dict:
        self.placed.append((kwargs["x"], kwargs["y"]))
        return {"id": f"p{len(self.placed)}", "revision": {"version": 0}, "component": {"name": name}}


@pytest.mark.asyncio
async def test_create_processor_without_xy_takes_a_free_cell() -> None:
    from nifi_mcp.layout import CARD_HEIGHT, CARD_WIDTH, PG_HEIGHT, PG_WIDTH, boxes_overlap, rects_overlap
    from nifi_mcp.server import CreateProcessorIn, nifi_create_processor

    fake = PlacingNiFi()
    configure(fake, settings())  # type: ignore[arg-type]
    for name in ("Gen", "Log"):
        out = json.loads(
            await nifi_create_processor(
                CreateProcessorIn(parent_id="00000000-0000-0000-0000-000000000020", processor_type="x.T", name=name)
            )
        )
        assert out["status"] == "ok", out
    a, b = fake.placed
    assert not boxes_overlap(a, b, width=CARD_WIDTH, height=CARD_HEIGHT), fake.placed
    for point in (a, b):
        assert not rects_overlap((*point, CARD_WIDTH, CARD_HEIGHT), (0.0, 0.0, PG_WIDTH, PG_HEIGHT)), point


@pytest.mark.asyncio
async def test_create_processor_keeps_explicit_xy() -> None:
    from nifi_mcp.server import CreateProcessorIn, nifi_create_processor

    fake = PlacingNiFi()
    configure(fake, settings())  # type: ignore[arg-type]
    await nifi_create_processor(
        CreateProcessorIn(
            parent_id="00000000-0000-0000-0000-000000000020", processor_type="x.T", name="A", x=984, y=544
        )
    )
    assert fake.placed == [(984.0, 544.0)]


class HalfBuildingNiFi(FakeNiFi):
    def __init__(self) -> None:
        super().__init__()
        self.services: list[str] = []

    async def create_controller_service(self, parent_id: str, service_type: str, name: str, **kwargs) -> dict:
        self.services.append(name)
        return {"id": f"svc-{name}", "revision": {"version": 0}, "component": {"name": name}}

    async def set_controller_service_state(self, service_id: str, state: str, version: int) -> dict:
        return {"id": service_id, "revision": {"version": version + 1}, "component": {"state": state}}


@pytest.mark.asyncio
async def test_apply_flow_spec_tool_refuses_a_malformed_object_before_creating_anything() -> None:
    fake = HalfBuildingNiFi()
    configure(fake, settings())  # type: ignore[arg-type]
    spec = {
        "objects": [
            {"type": "controller_service", "service_type": "x.S", "name": "Ok", "properties": {"a": "1"}},
            {"type": "controller_service", "service_type": "x.S", "name": "Bad", "properties": ["not-a-dict"]},
        ]
    }
    parent = "11111111-1111-1111-1111-111111111111"
    payload = json.loads(await nifi_apply_flow_spec(ApplySpecIn(spec=spec, parent_process_group_id=parent)))
    assert fake.services == []
    assert payload["status"] == "error"
    assert payload["cause"] == "spec"
    assert payload["created"] == []


class BuildsThenFlowReadFails(FakeNiFi):
    def __init__(self) -> None:
        super().__init__()
        self.created: list[str] = []

    async def create_processor(self, parent_id: str, processor_type: str, name: str, **kwargs) -> dict:
        self.created.append(name)
        return {"id": f"proc-{name}", "revision": {"version": 0}, "component": {"name": name}}

    async def get_flow(self, process_group_id: str) -> dict:
        if self.created:  # the layout read before building succeeds; the summary read after it fails
            raise NiFiError("GET /flow/process-groups failed", status_code=503)
        return await super().get_flow(process_group_id)


async def test_created_is_reported_when_the_final_flow_read_fails() -> None:
    # Every object exists, so the model must still get their ids.
    fake = BuildsThenFlowReadFails()
    configure(fake, settings())  # type: ignore[arg-type]
    spec = {"objects": [{"type": "processor", "processor_type": "x.A", "name": "A"}]}
    payload = json.loads(await nifi_apply_flow_spec(ApplySpecIn(spec=spec, parent_process_group_id="root")))
    assert fake.created == ["A"]
    assert payload["status"] == "ok", payload
    assert [item["id"] for item in payload["created"]] == ["proc-A"]
    assert payload["name_map"] == {"A": "proc-A"}
    assert payload["process_group_id"] == "root"
    assert "flow" not in payload
    assert any("nifi_get_flow" in warning for warning in payload["warnings"])


@pytest.mark.asyncio
async def test_name_map_and_created_keep_ids_for_any_object_name() -> None:
    class Nifi(FakeNiFi):
        async def create_processor(self, parent_id: str, processor_type: str, name: str, **kwargs) -> dict:
            return {"id": f"proc-{name}", "revision": {"version": 0}, "component": {"name": name}}

    configure(Nifi(), settings())  # type: ignore[arg-type]
    spec = {"objects": [{"type": "processor", "processor_type": "x.InvokeHTTP", "name": "FetchToken"}]}
    payload = json.loads(await nifi_apply_flow_spec(ApplySpecIn(spec=spec, parent_process_group_id="root")))
    assert payload["name_map"] == {"FetchToken": "proc-FetchToken"}
    assert payload["created"] == [
        {"kind": "processor", "id": "proc-FetchToken", "ref": "objects[0]", "name": "FetchToken", "outcome": "applied"}
    ]
