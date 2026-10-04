# nifi-mcp

Design and debug Apache NiFi 2.x flows through MCP, with each HTTP caller's NiFi permissions.
See [QUICKSTART.md](QUICKSTART.md) for Keycloak and VS Code setup.

## Requirements

Python 3.12+, the dependencies pinned in `uv.lock`, and Apache NiFi 2.x.
VS Code's MCP-capable chat host handles OAuth independently of your chosen local model.

## Config

The full configuration is in `src/nifi_mcp/config.py`; `.env.example` is the Keycloak configuration.
Exported `NIFI_*` variables override `.env`.

| Req | Name | Default | Purpose |
|---|---|---|---|
| Required | `NIFI_API_URL` | — | NiFi origin or `/nifi-api` URL |
| Required | `NIFI_AUTH` | `bearer` | Personal stdio credentials or `passthrough` for HTTP |
| When HTTP | `NIFI_TRANSPORT` | `stdio` | Set `streamable-http` for VS Code OAuth |
| When Keycloak | `NIFI_OAUTH_IDENTITY_CLAIM` | — | Match NiFi's OIDC identifying claim |
| When Keycloak | `NIFI_OAUTH_GROUPS_CLAIM` | `groups` | Match NiFi's OIDC groups claim |
| Optional | `NIFI_READONLY` | `false` | Block writes in addition to NiFi's policies |

## Minimum configuration

Copy `.env.example` to `.env` and supply your internal endpoints, CA bundle, Keycloak introspection
client secret, and dedicated NiFi proxy certificate/key. Keep `.env` and the key readable only by
the service account. The confidential client secret belongs on the server; VS Code uses a public
PKCE client and stores its own login state.

## Usage

```bash
./start-server.sh
```

For a checked-out flow bucket, copy `examples/keycloak-vscode/mcp.json` into that checkout's
`.vscode/mcp.json` and change the URL and public client ID. Start the NiFi server from VS Code's
MCP commands and sign in to your internal Keycloak in the browser.

## Preconditions

- Your internal Keycloak exposes OIDC discovery and introspection over trusted HTTPS.
- Its access tokens contain the MCP audience, the same identity, and the same groups as NiFi login.
- NiFi trusts the dedicated proxy certificate and permits its identity to proxy user requests.
- NiFi already has the applicable user/group policies; MCP grants no NiFi policies.
- A TLS reverse proxy serves the MCP URL and passes the Authorization header to port 8000.
- You install the locked Python dependencies and the VS Code chat/model extensions before isolation.

## Behaviour

`NIFI_READONLY` defaults false. NiFi checks each user's policy for every REST operation.
HTTP accepts only `NIFI_AUTH=passthrough`; it never uses configured password/admin credentials.
Keycloak tokens are introspected on every HTTP request, with issuer, audience, expiration and
safe identity/group claims checked. Verified identity and groups reach NiFi through mTLS proxy
headers. Calls use separate client/cookie state and the HTTP transport is stateless.

For stdio, set `NIFI_AUTH=bearer`, `NIFI_TRANSPORT=stdio`, and your own `NIFI_BEARER_TOKEN`.
The `jwt` and `oidc` password modes are explicit stdio-only configurations.
The launcher reads no Vault credentials and installs/downloads nothing at startup.

Call `nifi_current_user` to check identity, then `nifi_about`, `nifi_get_flow`, and the build/debug tools.
Flow specs resolve `@ServiceName` to services created in the same spec. Tool arguments are strict.
Mutation results carry `outcome`: `applied`, `not_applied`, or `unknown`; read back before retrying
an unknown create. NiFi 403 remains a permission denial. Caller credentials never refresh into a
server account. Multi-step tools can apply permitted changes before a later operation is denied.

## Out of scope

- Provisioning your Keycloak realm, NiFi policies, TLS proxy, or certificate authority.
- Configuring the local model/chat extension or assuming every VS Code extension shares MCP auth.
- Modifying a versioned flow without its registry workflow; prototype in an unversioned group.

## Expected result

VS Code shows NiFi tools and `nifi_current_user` returns your own NiFi identity.
Compare permitted reads and denied writes using an actual directory-backed non-admin account.
Check server metadata through your trusted TLS endpoint:

```bash
curl --fail https://nifi-mcp.example.internal/.well-known/oauth-protected-resource/mcp
```
