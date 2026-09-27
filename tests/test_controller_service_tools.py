import json

import httpx
import pytest
from conftest import Router, settings

from nifi_mcp.client import NiFiClient
from nifi_mcp.server import (
    ServiceIdIn,
    UpdateServiceIn,
    configure,
    nifi_get_controller_service,
    nifi_update_controller_service,
)

SVC_ID = "00000000-0000-0000-0000-000000000031"
SERVICE = {
    "id": SVC_ID,
    "revision": {"version": 6},
    "component": {
        "id": SVC_ID,
        "name": "Writer",
        "type": "org.apache.nifi.json.JsonRecordSetWriter",
        "state": "DISABLED",
        "validationStatus": "INVALID",
        "validationErrors": ["'Schema Write Strategy' is invalid"],
        "properties": {"Schema Write Strategy": "bogus", "Password": "hunter2"},
    },
}


def _wire(router: Router) -> None:
    configure(NiFiClient(settings(), transport=httpx.MockTransport(router.handle)), settings())


@pytest.mark.asyncio
async def test_get_controller_service_shows_validation_and_redacted_properties(router: Router) -> None:
    router.json("GET", f"/controller-services/{SVC_ID}", SERVICE)
    _wire(router)
    raw = await nifi_get_controller_service(ServiceIdIn(service_id=SVC_ID))
    payload = json.loads(raw)
    assert payload["validation_status"] == "INVALID"
    assert payload["validation_errors"] == ["'Schema Write Strategy' is invalid"]
    assert payload["properties"]["Schema Write Strategy"] == "bogus"
    assert "hunter2" not in raw


OAUTH_ID = "00000000-0000-0000-0000-000000000032"
OAUTH = {
    "id": OAUTH_ID,
    "revision": {"version": 1},
    "component": {
        "id": OAUTH_ID,
        "name": "OAuth",
        "state": "DISABLED",
        "properties": {
            "Token Endpoint URL": "https://idp.example/token",
            "Access Token Provider": "00000000-0000-0000-0000-000000000033",
            "Client Secret": "********",
            "Audience": "canary-1234",
        },
        "descriptors": {
            "Token Endpoint URL": {"name": "Token Endpoint URL", "sensitive": False},
            "Access Token Provider": {"name": "Access Token Provider", "sensitive": False},
            "Client Secret": {"name": "Client Secret", "sensitive": True},
            "Audience": {"name": "Audience", "sensitive": True},
        },
    },
}


@pytest.mark.asyncio
@pytest.mark.parametrize("verbose", [False, True])
async def test_service_view_keeps_non_secret_token_properties(router: Router, verbose: bool) -> None:
    router.json("GET", f"/controller-services/{OAUTH_ID}", OAUTH)
    _wire(router)
    raw = await nifi_get_controller_service(ServiceIdIn(service_id=OAUTH_ID, verbose=verbose))
    payload = json.loads(raw)
    props = payload["component"]["properties"] if verbose else payload["properties"]
    assert props["Token Endpoint URL"] == "https://idp.example/token"
    assert props["Access Token Provider"] == "00000000-0000-0000-0000-000000000033"
    assert props["Client Secret"] in {"********", "***REDACTED***"}
    assert "canary-1234" not in raw


@pytest.mark.asyncio
async def test_update_controller_service_sends_partial_dto_with_current_revision(router: Router) -> None:
    router.json("GET", f"/controller-services/{SVC_ID}", SERVICE)
    sent: dict = {}

    def put(request: httpx.Request) -> httpx.Response:
        sent.update(json.loads(request.content.decode()))
        return httpx.Response(200, json={**SERVICE, "revision": {"version": 7}})

    router.add("PUT", f"/controller-services/{SVC_ID}", put)
    _wire(router)
    raw = await nifi_update_controller_service(
        UpdateServiceIn(service_id=SVC_ID, properties={"Schema Write Strategy": "no-schema", "Old": None})
    )
    payload = json.loads(raw)
    assert payload["status"] == "ok", payload
    assert payload["controller_service"]["revision"] == 7
    assert sent["revision"]["version"] == 6
    assert sent["component"] == {
        "id": SVC_ID,
        "properties": {"Schema Write Strategy": "no-schema", "Old": None},
    }


@pytest.mark.asyncio
async def test_update_controller_service_blocked_when_readonly(router: Router) -> None:
    configure(NiFiClient(settings(), transport=httpx.MockTransport(router.handle)), settings(readonly=True))
    payload = json.loads(await nifi_update_controller_service(UpdateServiceIn(service_id=SVC_ID, name="x")))
    assert payload["type"] == "NiFiReadOnlyError"
    assert router.calls == []
