---
title: Absorb NiFi MCP work into a new tree, do not fork
type: decision
status: accepted
tags: [nifi, mcp]
updated: 2026-08-28
---

# Absorb, do not fork

## Context

Two Apache-2.0 NiFi MCP servers exist: `ms82119/NiFiMCP` and `cloudera/NiFi-MCP-Server`. The target is NiFi 2.x behind OIDC, JWT or bearer auth. The goal is natural-language flow development.

## Decision

Start a new repo (`nifi-mcp`). Read Apache NiFi `nifi-web-api` as the contract. Copy **ideas**, not the trees.

## Why not fork Cloudera

- Auth is Knox/CDP. The target is OIDC/JWT/bearer.
- `requests` inside `async` tools blocks the event loop.
- POST/PUT/DELETE are retried. That duplicates processors.
- Tool list includes Iceberg/SQL Server pattern matchers that are not the NiFi API.
- Responses dump full entities into the model context.

## Why not fork ms82119

- The repo is a FastAPI chat bot + Streamlit + PocketFlow + LLM provider keys.
- Claimed NiFi 2 support is "REST is probably the same".
- 30 tools plus documentation workflows are more product than MCP.

## What was absorbed

From Cloudera: `/flow/about`, secret redaction, bulk schedule, health summary.
Read-only default was absorbed then reversed (owner, 2026-08-28): writes on, `NIFI_READONLY=true` is the lock.

From ms82119: download/import/replace-requests, declarative spec with name wiring and `@ServiceName`, compact summaries, 401 refresh.

From Apache NiFi 2 source: every REST path the client calls, RevisionDTO on create (`version: 0`), `disconnectedNodeAcknowledged`, processor-definition, listing/drop requests on `/flowfile-queues`.
