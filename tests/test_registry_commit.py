"""nifi_commit_process_group: children first, parent re-pinned, nothing written when it would be wrong."""

import json
from typing import Any

import httpx
import pytest
from conftest import Router, settings

from nifi_mcp.client import NiFiClient
from nifi_mcp.server import configure, mcp

PARENT = "00000000-0000-0000-0000-0000000000b1"
CHILD = "00000000-0000-0000-0000-0000000000b2"
PLAIN = "00000000-0000-0000-0000-0000000000b3"


class Registry:
    """Two versioned groups, parent holding child. A commit moves that group to a new version and, for a
    child, leaves the parent modified (it now pins a different child version)."""

    def __init__(self, states: dict[str, str | None]) -> None:
        self.state = dict(states)
        self.version = {PARENT: "p1", CHILD: "c1"}
        self.commits: list[tuple[str, dict[str, Any]]] = []

    def entity(self, gid: str) -> dict[str, Any]:
        component: dict[str, Any] = {"id": gid, "name": gid[-2:]}
        if self.state[gid]:
            component["versionControlInformation"] = {
                "registryId": "reg", "bucketId": "b", "flowId": f"flow-{gid[-2:]}",
                "version": self.version[gid], "state": self.state[gid], "branch": "main",
            }
        return {"id": gid, "revision": {"version": 3}, "component": component}

    def wire(self, router: Router) -> None:
        for gid in (PARENT, CHILD):
            router.add("GET", f"/process-groups/{gid}", lambda _r, g=gid: httpx.Response(200, json=self.entity(g)))
            router.add("POST", f"/versions/process-groups/{gid}", lambda r, g=gid: self.commit(g, r))
        router.json("GET", f"/process-groups/{PARENT}/process-groups", {"processGroups": [{"id": CHILD}]})
        router.json("GET", f"/process-groups/{CHILD}/process-groups", {"processGroups": []})

    def commit(self, gid: str, request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content)
        self.commits.append((gid, body))
        self.version[gid] = f"new-{gid[-2:]}"
        self.state[gid] = "UP_TO_DATE"
        if gid == CHILD and self.state[PARENT]:
            self.state[PARENT] = "LOCALLY_MODIFIED"
        info = self.entity(gid)["component"]["versionControlInformation"]
        return httpx.Response(200, json={"versionControlInformation": info})


async def _call(router: Router, params: dict[str, Any]) -> dict[str, Any]:
    configure(NiFiClient(settings(), transport=httpx.MockTransport(router.handle)), settings())
    out = await mcp.call_tool("nifi_commit_process_group", {"params": params})
    blocks = out[0] if isinstance(out, tuple) else out
    return json.loads("".join(getattr(block, "text", "") for block in blocks))


@pytest.mark.asyncio
async def test_a_modified_child_is_committed_before_its_parent(router: Router) -> None:
    reg = Registry({PARENT: "LOCALLY_MODIFIED", CHILD: "LOCALLY_MODIFIED"})
    reg.wire(router)
    out = await _call(router, {"process_group_id": PARENT, "comment": " lay out "})
    assert [g for g, _b in reg.commits] == [CHILD, PARENT]
    assert reg.commits[0][1]["versionedFlow"]["comment"] == "lay out"
    assert reg.commits[0][1]["versionedFlow"]["action"] == "COMMIT"
    assert [(c["group_id"], c["previous_version"], c["version"]) for c in out["commits"]] == [
        (CHILD, "c1", "new-b2"),
        (PARENT, "p1", "new-b1"),
    ]


@pytest.mark.asyncio
async def test_an_unmodified_child_is_left_alone(router: Router) -> None:
    reg = Registry({PARENT: "LOCALLY_MODIFIED", CHILD: "UP_TO_DATE"})
    reg.wire(router)
    await _call(router, {"process_group_id": PARENT, "comment": "move"})
    assert [g for g, _b in reg.commits] == [PARENT]


@pytest.mark.asyncio
@pytest.mark.parametrize("bad", ["STALE", "LOCALLY_MODIFIED_AND_STALE", "SYNC_FAILURE"])
@pytest.mark.parametrize("where", [PARENT, CHILD])
async def test_a_group_out_of_sync_with_the_registry_refuses_and_writes_nothing(
    router: Router, bad: str, where: str
) -> None:
    states = {PARENT: "LOCALLY_MODIFIED", CHILD: "LOCALLY_MODIFIED", where: bad}
    reg = Registry(states)
    reg.wire(router)
    out = await _call(router, {"process_group_id": PARENT, "comment": "x"})
    assert out["outcome"] == "not_applied"
    assert reg.commits == []


@pytest.mark.asyncio
async def test_nothing_modified_refuses(router: Router) -> None:
    reg = Registry({PARENT: "UP_TO_DATE", CHILD: "UP_TO_DATE"})
    reg.wire(router)
    out = await _call(router, {"process_group_id": PARENT, "comment": "x"})
    assert out["outcome"] == "not_applied"
    assert "Nothing to commit" in json.dumps(out)
    assert reg.commits == []


@pytest.mark.asyncio
async def test_an_unversioned_group_refuses(router: Router) -> None:
    reg = Registry({PARENT: None, CHILD: None})
    reg.wire(router)
    out = await _call(router, {"process_group_id": PARENT, "comment": "x"})
    assert out["outcome"] == "not_applied"
    assert reg.commits == []


@pytest.mark.asyncio
async def test_a_blank_comment_is_rejected(router: Router) -> None:
    reg = Registry({PARENT: "LOCALLY_MODIFIED", CHILD: "UP_TO_DATE"})
    reg.wire(router)
    out = await _call(router, {"process_group_id": PARENT, "comment": "   "})
    assert "say what changed" in json.dumps(out)
    assert reg.commits == []


@pytest.mark.asyncio
async def test_the_version_state_lists_every_versioned_group_below(router: Router) -> None:
    reg = Registry({PARENT: "LOCALLY_MODIFIED", CHILD: "STALE"})
    reg.wire(router)
    configure(NiFiClient(settings(), transport=httpx.MockTransport(router.handle)), settings())
    out = await mcp.call_tool("nifi_get_version_state", {"params": {"process_group_id": PARENT}})
    blocks = out[0] if isinstance(out, tuple) else out
    groups = json.loads("".join(getattr(b, "text", "") for b in blocks))["versioned_groups"]
    assert [(g["group_id"], g["state"]) for g in groups] == [(CHILD, "STALE"), (PARENT, "LOCALLY_MODIFIED")]
