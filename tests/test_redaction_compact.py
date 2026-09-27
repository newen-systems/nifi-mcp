import json

import pytest
from conftest import settings

from nifi_mcp.compact import compact_flow, compact_process_group, compact_processor
from nifi_mcp.redaction import REDACT_MARK, redact
from nifi_mcp.server import (
    ComponentIdIn,
    ProcessGroupIn,
    configure,
    nifi_get_processor,
    nifi_list_controller_services,
)


def test_redact_nested_secrets() -> None:
    payload = {
        "component": {
            "config": {
                "properties": {
                    "Password": "hunter2",
                    "Listening Port": "8080",
                    "AWS Secret Access Key": "abc",
                }
            }
        },
        "token": "nope",
    }
    out = redact(payload)
    assert out["token"] == REDACT_MARK
    props = out["component"]["config"]["properties"]
    assert props["Password"] == REDACT_MARK
    assert props["AWS Secret Access Key"] == REDACT_MARK
    assert props["Listening Port"] == "8080"


def test_compact_processor_strips_noise() -> None:
    entity = {
        "id": "abc",
        "revision": {"version": 3},
        "component": {
            "name": "Log it",
            "type": "org.apache.nifi.processors.standard.LogAttribute",
            "state": "STOPPED",
            "validationStatus": "VALID",
            "validationErrors": [],
            "position": {"x": 1, "y": 2},
        },
        "status": {"aggregateSnapshot": {"queued": "0 / 0 bytes"}},
        "bulletins": [],
    }
    compact = compact_processor(entity)
    assert compact["id"] == "abc"
    assert compact["type"] == "LogAttribute"
    assert compact["revision"] == 3
    assert "config" not in compact


def test_compact_flow_counts_children() -> None:
    flow = {
        "processGroupFlow": {
            "id": "root",
            "breadcrumb": {"breadcrumb": {"id": "root", "name": "NiFi Flow"}},
            "flow": {
                "processors": [
                    {
                        "id": "p1",
                        "component": {"name": "A", "type": "org.apache.nifi.GenerateFlowFile"},
                    }
                ],
                "connections": [],
                "processGroups": [],
                "inputPorts": [],
                "outputPorts": [],
            },
        }
    }
    outline = compact_flow(flow)
    assert outline["name"] == "NiFi Flow"
    assert len(outline["processors"]) == 1


def test_compact_process_group_clips_comments() -> None:
    entity = {
        "id": "pg1",
        "revision": {"version": 1},
        "runningCount": 2,
        "stoppedCount": 0,
        "invalidCount": 0,
        "disabledCount": 0,
        "component": {
            "name": "example-web-app",
            "comments": "Example passthrough. " + ("x" * 200),
            "position": {"x": 0, "y": 0},
        },
    }
    compact = compact_process_group(entity)
    assert compact["name"] == "example-web-app"
    assert compact["comments"].endswith("...")
    assert len(compact["comments"]) <= 160
    assert "parameter_context" not in compact


def test_compact_processor_omits_empty_errors() -> None:
    entity = {
        "id": "abc",
        "revision": {"version": 3},
        "component": {
            "name": "Log it",
            "type": "org.apache.nifi.processors.standard.LogAttribute",
            "validationErrors": [],
        },
    }
    compact = compact_processor(entity)
    assert "validation_errors" not in compact


def test_parameter_named_password_is_redacted() -> None:
    payload = {
        "parameterContexts": [
            {
                "component": {
                    "parameters": [
                        {"parameter": {"name": "db_password", "value": "canary-1234", "sensitive": False}},
                        {"parameter": {"name": "api", "value": "canary-5678", "sensitive": True}},
                        {"parameter": {"name": "env", "value": "lab", "sensitive": False}},
                    ]
                }
            }
        ]
    }
    out = json.dumps(redact(payload))
    assert "canary-1234" not in out
    assert "canary-5678" not in out
    assert '"lab"' in out


def test_versioned_snapshot_parameters_are_redacted() -> None:
    snapshot = {
        "parameterContexts": {
            "ctx": {"parameters": [{"name": "Kafka Secret", "value": "canary-1234", "sensitive": False}]}
        }
    }
    assert "canary-1234" not in json.dumps(redact(snapshot))


@pytest.mark.asyncio
async def test_verbose_processor_and_service_reads_are_redacted() -> None:
    entity = {
        "id": "p1",
        "revision": {"version": 1},
        "component": {
            "id": "p1",
            "properties": {"Password": "canary-1234"},
            "config": {"properties": {"Secret": "c-2"}},
        },
    }

    class Fake:
        async def authenticate(self) -> None:
            return None

        async def get_processor(self, _pid: str) -> dict:
            return entity

        async def list_controller_services(self, _pg: str) -> dict:
            return {"controllerServices": [entity]}

    configure(Fake(), settings())  # type: ignore[arg-type]
    raw = await nifi_get_processor(ComponentIdIn(component_id="00000000-0000-0000-0000-000000000003", verbose=True))
    group = ProcessGroupIn(process_group_id="00000000-0000-0000-0000-000000000004", verbose=True)
    raw += await nifi_list_controller_services(group)
    assert "canary-1234" not in raw
    assert "c-2" not in raw


def test_compact_flow_keeps_funnel_remote_group_and_label_geometry() -> None:
    flow = {
        "processGroupFlow": {
            "id": "g",
            "flow": {
                "funnels": [{"id": "f1", "component": {"id": "f1", "position": {"x": 10, "y": 20}}}],
                "remoteProcessGroups": [
                    {"id": "r1", "component": {"id": "r1", "name": "Remote", "position": {"x": 0, "y": 500}}}
                ],
                "labels": [
                    {
                        "id": "l1",
                        "component": {"id": "l1", "position": {"x": 900, "y": 0}, "width": 600, "height": 90},
                    }
                ],
            },
        }
    }
    outline = compact_flow(flow)
    assert outline["funnels"] == [{"id": "f1", "position": {"x": 10, "y": 20}}]
    assert outline["remote_process_groups"] == [{"id": "r1", "name": "Remote", "position": {"x": 0, "y": 500}}]
    assert outline["labels"] == [{"id": "l1", "position": {"x": 900, "y": 0}, "width": 600, "height": 90}]


def test_token_names_a_credential_only_as_the_last_word() -> None:
    props = {
        "Token Endpoint URL": "https://idp.example/token",
        "Access Token Provider": "33333333-3333-3333-3333-333333333333",
        "Refresh Token": "canary-1234",
        "api_token": "canary-5678",
    }
    out = redact({"properties": props})["properties"]
    assert out["Token Endpoint URL"] == "https://idp.example/token"
    assert out["Access Token Provider"] == "33333333-3333-3333-3333-333333333333"
    assert out["Refresh Token"] == REDACT_MARK
    assert out["api_token"] == REDACT_MARK


def test_descriptor_sensitive_flag_masks_a_property_whatever_its_name() -> None:
    config = {
        "properties": {"Authorization": "Bearer canary-1234", "URL": "https://x", "Password": "canary-5678"},
        "descriptors": {
            "Authorization": {"name": "Authorization", "sensitive": True},
            "URL": {"name": "URL", "sensitive": False},
            "Password": {"name": "Password", "sensitive": False},
        },
    }
    out = redact({"component": {"config": config}})["component"]["config"]["properties"]
    assert out["Authorization"] == REDACT_MARK
    assert out["URL"] == "https://x"
    # A credential-named value NiFi sends unmasked stays masked, as for parameters.
    assert out["Password"] == REDACT_MARK
