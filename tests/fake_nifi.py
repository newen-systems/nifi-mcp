"""A fake NiFi for tool-level tests: it answers every read and mutation the tools send, and fails
the requests a test targets with a transport error or an HTTP status whose body repeats the request
(method, URL and body) back, as a NiFi error message may."""

from __future__ import annotations

import json
import re
from typing import Any

import httpx

PG = "00000000-0000-0000-0000-0000000000a0"
CHILD = "00000000-0000-0000-0000-0000000000a1"
NEWPG = "00000000-0000-0000-0000-0000000000a9"
PROC = "00000000-0000-0000-0000-0000000000b0"
NEWPROC = "00000000-0000-0000-0000-0000000000b9"
CONN = "00000000-0000-0000-0000-0000000000c0"
NEWCONN = "00000000-0000-0000-0000-0000000000c9"
PORT_IN = "00000000-0000-0000-0000-0000000000d0"
PORT_OUT = "00000000-0000-0000-0000-0000000000d1"
NEWPORT = "00000000-0000-0000-0000-0000000000d9"
SVC = "00000000-0000-0000-0000-0000000000e0"
NEWSVC = "00000000-0000-0000-0000-0000000000e9"
CTX = "00000000-0000-0000-0000-0000000000f0"
NEWCTX = "00000000-0000-0000-0000-0000000000f9"

_NEW_IDS = {
    "process-groups": NEWPG,
    "processors": NEWPROC,
    "connections": NEWCONN,
    "input-ports": NEWPORT,
    "output-ports": NEWPORT,
    "controller-services": NEWSVC,
    "import": NEWPG,
}


def _entity(cid: str, parent: str = PG, name: str = "existing", **component: Any) -> dict[str, Any]:
    return {
        "id": cid,
        "revision": {"version": 1},
        "component": {"id": cid, "parentGroupId": parent, "name": name, **component},
    }


def _flow(group: str, name: str = "existing") -> dict[str, Any]:
    """A canvas with one of everything, off the layout grid, so a relayout moves each kind."""
    far = {"x": 3001.0, "y": 3001.0}
    return {
        "revision": {"version": 1},
        "processGroupFlow": {
            "id": group,
            "parameterContext": {"id": CTX},
            "flow": {
                "processors": [_entity(PROC, group, name, position=far, state="STOPPED")],
                "inputPorts": [_entity(PORT_IN, group, name, position={"x": 5001.0, "y": 5001.0})],
                "outputPorts": [_entity(PORT_OUT, group, name, position={"x": 7001.0, "y": 7001.0})],
                "processGroups": [_entity(CHILD, group, name, position={"x": 9001.0, "y": 9001.0})],
                "connections": [
                    {
                        **_entity(CONN, group),
                        "status": {"aggregateSnapshot": {"flowFilesQueued": 4}},
                        "component": {
                            "id": CONN,
                            "parentGroupId": group,
                            "name": name,
                            "source": {"id": PROC, "groupId": group, "type": "PROCESSOR"},
                            "destination": {"id": PORT_OUT, "groupId": group, "type": "OUTPUT_PORT"},
                        },
                    }
                ],
            },
        }
    }


_GETS: list[tuple[re.Pattern[str], Any]] = [
    (re.compile(r"^/flow/process-groups/([^/]+)/controller-services$"),
     lambda m, n: {"controllerServices": [_entity(SVC, m[1], n, state="DISABLED")]}),
    (re.compile(r"^/flow/process-groups/([^/]+)$"), lambda m, n: _flow(m[1], n)),
    (re.compile(r"^/flow/parameter-contexts$"), lambda m, n: {"parameterContexts": [_entity(CTX, PG, n)]}),
    (re.compile(r"^/(?:process-groups|parameter-contexts/[^/]+)/(?:replace|update)-requests/(\w+)$"),
     lambda m, n: {"request": {"requestId": m[1], "complete": True}}),
    (re.compile(r"^/flowfile-queues/[^/]+/drop-requests/(\w+)$"),
     lambda m, n: {"dropRequest": {"id": m[1], "finished": True}}),
    (re.compile(r"^/process-groups/([^/]+)$"),
     lambda m, n: _entity(m[1], PG, n, parameterContext={"id": CTX}, position={})),
    (re.compile(r"^/parameter-contexts/([^/]+)$"), lambda m, n: _entity(m[1], PG, n, parameters=[])),
    (re.compile(r"^/controller-services/([^/]+)$"), lambda m, n: _entity(m[1], PG, n, state="DISABLED")),
    (re.compile(r"^/processors/([^/]+)$"), lambda m, n: _entity(m[1], PG, n, state="STOPPED")),
    (re.compile(r"^/(?:connections|input-ports|output-ports)/([^/]+)$"), lambda m, n: _entity(m[1], PG, n)),
    (re.compile(r"^/flow/(?:about|current-user|processor-types|controller-service-types|bulletin-board)$"),
     lambda m, n: {}),
]


def _answer_mutation(method: str, path: str, body: dict[str, Any]) -> httpx.Response:
    if method == "DELETE":
        return httpx.Response(200, json={})
    if match := re.match(r"^/process-groups/([^/]+)/(\w[\w-]*)(/import)?$", path):
        if match[2] == "replace-requests":
            return httpx.Response(201, json={"request": {"requestId": "r1", "complete": False}})
        new_id = _NEW_IDS["import" if match[3] else match[2]]
        return httpx.Response(201, json=_entity(new_id, match[1], state="DISABLED"))
    if path == "/parameter-contexts":
        return httpx.Response(201, json=_entity(NEWCTX))
    if path.endswith("/update-requests"):
        return httpx.Response(200, json={"request": {"requestId": "r2", "complete": False}})
    if path.endswith("/drop-requests"):
        return httpx.Response(202, json={"dropRequest": {"id": "d1", "finished": False}})
    cid = path.split("/")[-2 if path.endswith("run-status") else -1]
    state = body.get("state") or ("ENABLED" if "controller-services" in path else "STOPPED")
    return httpx.Response(200, json={**_entity(cid, state=state), "revision": {"version": 2}})


class FakeNiFi(httpx.AsyncBaseTransport):
    """Answers every read and mutation. The requests `target` names fail with `failure`: one
    (method, path), "mutations" (every non-GET) or "all". `name` is the name of every component the
    reads return."""

    def __init__(self, target: tuple[str, str] | str, failure: str, name: str = "existing") -> None:
        self.target, self.failure, self.name = target, failure, name
        self.calls: list[tuple[str, str]] = []

    def _targets(self, method: str, path: str) -> bool:
        if self.target == "all":
            return True
        if self.target == "mutations":
            return method != "GET"
        return (method, path) == self.target

    async def handle_async_request(self, request: httpx.Request) -> httpx.Response:
        path = request.url.path.removeprefix("/nifi-api")
        self.calls.append((request.method, path))
        if self.failure != "2xx" and self._targets(request.method, path):
            echo = f"NiFi could not do that: {request.method} {request.url} {request.content.decode(errors='replace')}"
            if not self.failure.isdigit():
                raise getattr(httpx, self.failure)(echo, request=request)
            if int(self.failure) < 500:
                return httpx.Response(int(self.failure), json={"message": echo})
            return httpx.Response(int(self.failure), text=echo)
        if request.method == "GET":
            for pattern, build in _GETS:
                if match := pattern.match(path):
                    return httpx.Response(200, json=build(match, self.name))
            return httpx.Response(404, json={"message": "unhandled"})
        body = json.loads(request.content or b"{}")
        return _answer_mutation(request.method, path, body)
