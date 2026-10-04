## What this is

`src/nifi_mcp/user_auth.py` authenticates each MCP caller with your internal Keycloak and forwards their identity and groups to NiFi over its certificate proxy interface. NiFi enforces its own policies. `examples/keycloak-vscode/mcp.json` connects a flow-bucket checkout to that server; your local model's chat host must support VS Code MCP tools. Deployment requires internal TLS, Keycloak clients, and a trusted NiFi proxy certificate.

## How to use it

1. Install the locked dependencies before disconnecting: `uv sync --frozen`.
2. Open `examples/keycloak-vscode/SETUP.md` and configure the Keycloak clients, NiFi proxy trust, and internal TLS endpoint as specified there.
3. Copy the server settings: `cp .env.example .env`.
4. Open `.env` and fill your internal endpoints, matching NiFi claims, confidential client secret and certificate/key paths; restrict it and the private key to the service account.
5. Start the server: `./start-server.sh`.
6. Copy `examples/keycloak-vscode/mcp.json` to `.vscode/mcp.json` in your flow-bucket checkout and replace its endpoint/public client ID.
7. In VS Code, start the MCP server, sign in to local Keycloak, and call `nifi_current_user` with your MCP-capable local-model chat host.

You know it works when `nifi_current_user` shows your directory account and a write denied to that account in NiFi is also denied through MCP.
