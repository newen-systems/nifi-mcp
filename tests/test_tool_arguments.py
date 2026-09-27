import json
from typing import Any

import pytest

from nifi_mcp.server import mcp

CANARY = "canary-1234"
PID = "00000000-0000-0000-0000-000000000001"


def _text(result: Any) -> str:
    """call_tool returns (content, structured) for str tools; the content is what the model reads."""
    content = result[0] if isinstance(result, tuple) else result
    return "".join(getattr(block, "text", "") for block in content)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("tool", "arguments", "secret"),
    [
        (
            "nifi_update_processor",
            {"params": {"processor_id": PID, "properties": {"Password": {"raw": CANARY}}}},
            CANARY,
        ),
        (
            "nifi_create_parameter_context",
            {"params": {"name": "ctx", "parameters": [{"name": "pin", "value": 991234, "sensitive": True}]}},
            "991234",
        ),
    ],
)
async def test_argument_errors_never_echo_the_submitted_value(tool: str, arguments: dict, secret: str) -> None:
    # FastMCP validates arguments before json_tool runs; its ToolError text embeds input_value.
    text = _text(await mcp.call_tool(tool, arguments))
    assert secret not in text
    payload = json.loads(text)
    assert payload["status"] == "error"
    assert payload["type"] == "ValidationError"
    assert "params." in payload["error"]
    assert "input_value" not in payload["error"]


@pytest.mark.asyncio
async def test_unknown_tool_still_raises() -> None:
    with pytest.raises(Exception, match="Unknown tool"):
        await mcp.call_tool("nifi_no_such_tool", {})


def test_tool_relationship_fields_take_a_single_name() -> None:
    # The single-object tools accept the same shapes as the spec.
    from nifi_mcp.server import CreateConnectionIn, CreateProcessorIn, UpdateProcessorIn

    pid = "00000000-0000-0000-0000-000000000001"
    created = CreateProcessorIn(parent_id=pid, processor_type="x.T", name="n", auto_terminated="success")
    assert created.auto_terminated == ["success"]
    assert UpdateProcessorIn(processor_id=pid, auto_terminated="failure").auto_terminated == ["failure"]
    conn = CreateConnectionIn(
        parent_id=pid, source_id=pid, source_group_id=pid, destination_id=pid, destination_group_id=pid,
        relationships="success",
    )
    assert conn.relationships == ["success"]


class _NoAuth:
    async def authenticate(self) -> None:
        return None


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "item",
    [
        {"type": "processor", "name": "Log", "processor_type": "x.Log", "auto_terminated": [CANARY, 1]},
        {"type": "processor", "name": "Log", "processor_type": "x.Log", "autoTerminatedRelationships": {CANARY: 1}},
        {"type": "connection", "source": "a", "target": "b", "relationships": [CANARY, None]},
        {"type": "processor", "name": "Log", "processor_type": "x.Log", "x": CANARY, "y": 0},
        {"type": "processor", "name": "Log", "processor_type": "x.Log", "position": {"x": 0, "y": [CANARY]}},
    ],
)
@pytest.mark.parametrize("layout", ["auto", "manual"])
async def test_spec_refusals_never_echo_the_submitted_value(item: dict[str, Any], layout: str) -> None:
    # A spec error names the field and the type, never the value, like argument errors.
    from nifi_mcp.config import Settings
    from nifi_mcp.server import configure

    settings = Settings(api_url="https://nifi.example.test/nifi-api", auth="bearer", bearer_token="t", tls_verify=True)
    configure(_NoAuth(), settings)  # type: ignore[arg-type]
    spec = {"layout": layout, "objects": [item]}
    text = _text(await mcp.call_tool("nifi_apply_flow_spec", {"params": {"spec": spec}}))
    assert CANARY not in text, text
    payload = json.loads(text)
    assert payload["status"] == "error"
    assert payload["cause"] == "spec"
    assert "must be" in payload["error"], payload
