"""Commit a versioned process group to its flow registry, children first.

A group under version control that differs from its registry version is LOCALLY_MODIFIED. A commit of a
parent pins its child groups' versions, so a modified child is committed first and the parent after it,
which is then committed at the version the child just got.
"""

from __future__ import annotations

from typing import Any

from nifi_mcp.client import NiFiClient
from nifi_mcp.errors import NiFiError, Outcome

MAX_GROUPS = 200
MODIFIED = "LOCALLY_MODIFIED"
# Every other state but UP_TO_DATE means the registry holds something this canvas does not (or cannot be read).
CLEAN = frozenset({"UP_TO_DATE", MODIFIED})


def _refuse(message: str, hint: str | None = None) -> NiFiError:
    return NiFiError(message, outcome=Outcome.NOT_APPLIED, hint=hint)


def _vci(entity: dict[str, Any]) -> dict[str, Any] | None:
    return (entity.get("component") or {}).get("versionControlInformation") or None


async def _post_order(client: NiFiClient, group_id: str, seen: list[str]) -> None:
    """Ids of the group and every group below it, descendants before their ancestors."""
    if len(seen) >= MAX_GROUPS:
        raise _refuse(f"More than {MAX_GROUPS} process groups below {group_id}; commit a smaller subtree.")
    children = (await client.get_json(f"/process-groups/{group_id}/process-groups")).get("processGroups") or []
    for child in children:
        await _post_order(client, str(child["id"]), seen)
    seen.append(group_id)


async def _versioned(client: NiFiClient, ids: list[str]) -> list[dict[str, Any]]:
    entities = [await client.get_process_group(gid) for gid in ids]
    return [e for e in entities if _vci(e)]


def _check(group: dict[str, Any], tree: list[dict[str, Any]]) -> None:
    bad = [
        f"{(g.get('component') or {}).get('name')} ({g['id']}): {(_vci(g) or {}).get('state')}"
        for g in tree
        if (_vci(g) or {}).get("state") not in CLEAN
    ]
    if bad:
        raise _refuse(
            "Not committed, a group is not in sync with its registry: " + "; ".join(bad),
            "Stale or conflicted groups need the operator: change the version in the UI or discard the local "
            "changes. Committing over them would overwrite the registry's newer version.",
        )
    if not any((_vci(g) or {}).get("state") == MODIFIED for g in tree):
        name = (group.get("component") or {}).get("name")
        raise _refuse(f"Nothing to commit: {name} and the versioned groups below it match their registry version.")


async def _commit_one(client: NiFiClient, entity: dict[str, Any], comment: str) -> dict[str, Any]:
    info = _vci(entity) or {}
    flow = {
        "action": "COMMIT",
        "registryId": info["registryId"],
        "bucketId": info["bucketId"],
        "flowId": info["flowId"],
        "comment": comment,
    }
    if info.get("branch"):
        flow["branch"] = info["branch"]
    revision = {"clientId": client.settings.client_id, "version": (entity.get("revision") or {}).get("version", 0)}
    done = await client.json(
        "POST",
        f"/versions/process-groups/{entity['id']}",
        json_body={"processGroupRevision": revision, "versionedFlow": flow},
    )
    after = done.get("versionControlInformation") or {}
    return {
        "group_id": entity["id"],
        "name": (entity.get("component") or {}).get("name"),
        "flow_id": info["flowId"],
        "previous_version": info.get("version"),
        "version": after.get("version"),
        "state": after.get("state"),
    }


async def commit_process_group(client: NiFiClient, group_id: str, comment: str) -> dict[str, Any]:
    """Commit the group and every modified versioned group below it, deepest first.

    Refuses, writing nothing, when the group is not versioned, nothing is modified, or any versioned
    group in the tree is stale, in conflict or cannot sync."""
    target = await client.get_process_group(group_id)
    if not _vci(target):
        raise _refuse(f"{group_id} is not under version control.")
    ids: list[str] = []
    await _post_order(client, str(target["id"]), ids)
    tree = await _versioned(client, ids)
    _check(target, tree)
    commits: list[dict[str, Any]] = []
    for gid in ids:
        # Read again: a child's commit changes the revision and the state of every ancestor.
        entity = await client.get_process_group(gid)
        if (_vci(entity) or {}).get("state") == MODIFIED:
            commits.append(await _commit_one(client, entity, comment))
    return {"status": "ok", "process_group_id": target["id"], "commits": commits}


async def version_state(client: NiFiClient, group_id: str) -> dict[str, Any]:
    """The registry state of the group and of every versioned group below it."""
    ids: list[str] = []
    await _post_order(client, (await client.get_process_group(group_id))["id"], ids)
    groups = [
        {
            "group_id": e["id"],
            "name": (e.get("component") or {}).get("name"),
            "registry_id": (_vci(e) or {}).get("registryId"),
            "bucket_id": (_vci(e) or {}).get("bucketId"),
            "flow_id": (_vci(e) or {}).get("flowId"),
            "branch": (_vci(e) or {}).get("branch"),
            "version": (_vci(e) or {}).get("version"),
            "state": (_vci(e) or {}).get("state"),
            "state_explanation": (_vci(e) or {}).get("stateExplanation"),
        }
        for e in await _versioned(client, ids)
    ]
    return {"process_group_id": group_id, "versioned_groups": groups}
