# nifi-mcp

A Model Context Protocol (MCP) server for Apache NiFi 2.x. It lets an MCP client (Claude, or any
other MCP-capable assistant) discover processor types, build a process group, wire connections,
start it, and debug queues and bulletins through the NiFi REST API (`nifi-web-api`).

It runs over stdio, is written in Python 3.12 on FastMCP, and is not a fork: REST paths and entity
shapes come from Apache NiFi, and ideas were absorbed from two Apache-2.0 NiFi MCP servers
([ms82119/NiFiMCP](https://github.com/ms82119/NiFiMCP),
[cloudera/NiFi-MCP-Server](https://github.com/cloudera/NiFi-MCP-Server)). See `NOTICE` and
`docs/decisions/0001-absorb-not-fork.md`.

## Install

Requires [uv](https://docs.astral.sh/uv/) and Python 3.12.

```bash
git clone <this repo> nifi-mcp
cd nifi-mcp
uv sync --extra dev
```

## Configuration

All settings are environment variables with the `NIFI_` prefix. They can also go in a `.env` file
at the repo root (gitignored, mode 0600). `NIFI_READONLY` defaults false: writes are on.

| Variable | Default | Purpose |
|---|---|---|
| `NIFI_API_URL` | required | NiFi `/nifi-api` URL or UI origin, e.g. `https://nifi.example.com/nifi-api` |
| `NIFI_READONLY` | `false` | `true` blocks every create, update, delete and schedule |
| `NIFI_AUTH` | `oidc` | `oidc`, `jwt` or `bearer` |
| `NIFI_OIDC_TOKEN_URL` | | OIDC token endpoint for the password grant |
| `NIFI_OIDC_CLIENT_ID` | | OIDC client id |
| `NIFI_OIDC_CLIENT_SECRET` | | OIDC client secret |
| `NIFI_OIDC_USERNAME` | | OIDC user |
| `NIFI_OIDC_PASSWORD` | | OIDC password |
| `NIFI_OIDC_SCOPE` | `openid profile` | OIDC scope |
| `NIFI_USERNAME` | | `jwt` mode: user for `POST /nifi-api/access/token` |
| `NIFI_PASSWORD` | | `jwt` mode: password |
| `NIFI_BEARER_TOKEN` | | `bearer` mode: a pre-minted token |
| `NIFI_CA_BUNDLE` | | Extra CA bundle, added to the default trust store |
| `NIFI_TLS_VERIFY` | `true` | Prefer `NIFI_CA_BUNDLE` over turning this off |
| `NIFI_CLIENT_ID` | `nifi-mcp` | `RevisionDTO.clientId` for optimistic locking |
| `NIFI_TIMEOUT_SECONDS` | `30` | HTTP timeout |
| `NIFI_DISCONNECTED_NODE_ACK` | `true` | Acknowledge disconnected cluster nodes on mutations |

The server never prints tokens or passwords. Keep secrets in your secret store or `.env`, not in
MCP client config.

## Run

`start-server.sh` loads `.env`, requires an `https://` `NIFI_API_URL`, and starts the stdio server
unbuffered. Register it with your MCP client, for example:

```bash
claude mcp add nifi --scope user -- /path/to/nifi-mcp/start-server.sh
```

Or run it directly with `uv run nifi-mcp`.

## Tools

| Area | Tools |
|---|---|
| Server | `nifi_about`, `nifi_current_user`, `nifi_get_health`, `nifi_get_bulletins`, `nifi_search` |
| Discovery | `nifi_list_processor_types`, `nifi_get_processor_definition`, `nifi_list_controller_service_types` |
| Flow | `nifi_get_flow`, `nifi_apply_flow_spec`, `nifi_layout_process_group`, `nifi_export_flow`, `nifi_import_flow`, `nifi_replace_flow` |
| Components | `nifi_create_process_group`, `nifi_create_processor`, `nifi_get_processor`, `nifi_update_processor`, `nifi_set_run_status`, `nifi_create_connection`, `nifi_update_connection`, `nifi_delete_component`, `nifi_schedule_process_group` |
| Controller services | `nifi_list_controller_services`, `nifi_create_controller_service`, `nifi_get_controller_service`, `nifi_update_controller_service`, `nifi_set_controller_service_state` |
| Parameter contexts | `nifi_list_parameter_contexts`, `nifi_get_parameter_context`, `nifi_create_parameter_context`, `nifi_update_parameter_context`, `nifi_bind_parameter_context` |
| Queues | `nifi_list_queue`, `nifi_empty_queue` |

Prompts: `nifi_flow_builder`, `nifi_debug_flow`, `nifi_best_practices`.

A typical loop: `nifi_about`, then `nifi_list_processor_types` and `nifi_get_processor_definition`,
then one `nifi_apply_flow_spec` call, then `nifi_get_health`, fix anything INVALID, and
`nifi_schedule_process_group`. Build inside a process group, never on the root canvas. If your
deployment reconciles versioned process groups from a registry, prototype on an unversioned group.

## Flow spec

```json
{
  "process_group": {"name": "http-log"},
  "objects": [
    {"type": "controller_service", "service_type": "org.apache.nifi.http.StandardHttpContextMap", "name": "Ctx"},
    {
      "type": "processor",
      "processor_type": "org.apache.nifi.processors.standard.HandleHttpRequest",
      "name": "Listen",
      "properties": {"HTTP Context Map": "@Ctx", "Listening Port": "18080"}
    },
    {
      "type": "processor",
      "processor_type": "org.apache.nifi.processors.standard.LogAttribute",
      "name": "Log",
      "auto_terminated": ["success"]
    },
    {"type": "connection", "source": "Listen", "target": "Log", "relationships": ["success"]}
  ]
}
```

`@Ctx` resolves to the controller service created in the same spec. Unknown keys and tool
arguments are refused before anything is created. Every tool that changes NiFi returns an
`outcome` of `applied`, `not_applied` or `unknown`; for `unknown` the server reads the state back
before it answers. Error text never repeats a submitted value.

## Layout

Canvas placement is top-down and every spacing is derived from NiFi's real card sizes. The
formulas are in the `src/nifi_mcp/layout.py` docstring.

- A chain stays on one axis. Each card is centred on its axis by its own width.
- Rows are separated by one 112px gap (the tallest connection label plus 16px either side, on
  NiFi's 8px snap), added to the tallest card in the row.
- At a fork the main branch continues down the axis. Other children keep relationship order left
  to right, one per side on the fork's row; any further one drops a row into its own column.
- A fork of leaves spreads one row down, centred on the parent. A join returns to its fork's axis.
- Child process groups stack top-down in flow order.
- Connections bend only for exact duplicate pairs, self-loops, and a line or label that would
  otherwise cross a card; those are routed around it from the card's side.

`docs/layout-alternatives.md` records the layouts tried and set aside.

## Tests

```bash
uv run pytest
uv run ruff check src tests
```

The tests use an in-process fake NiFi and need no cluster.

## Licence

Apache-2.0. See `LICENSE` and `NOTICE`.
