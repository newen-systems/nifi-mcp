# Personal stdio authentication

Connect VS Code or a CLI MCP host directly to NiFi using your configured credentials.

## Requirements

An installed nifi-mcp and a NiFi HTTPS endpoint accepting your selected authentication method.

## Configuration

| NiFi authentication | Template | NiFi identity |
|---|---|---|
| Personal bearer | `bearer.env.example` | Token owner |
| Single-user/admin or LDAP password | `single-user.env.example` | Configured username |
| Client certificate | `mtls.env.example` | Certificate identity after NiFi mapping |

## Usage

Copy your selected template to `~/.config/nifi-mcp/user.env`, fill it and restrict it to your account.
Copy `mcp.json` into your flow checkout's `.vscode/mcp.json` and replace the command with your launcher path.
The envFile supplies credentials to the local process; keep it outside the checkout.

## Behaviour

No Keycloak is required. NiFi enforces the configured identity's permissions. Configuring an admin
credential gives this local instance admin access; use your own account for per-user permissions.
`NIFI_CA_BUNDLE` trusts NiFi's server certificate; it does not authenticate you. `mtls` also requires
an authorized client certificate and its PEM key. An encrypted key uses `NIFI_CLIENT_KEY_PASSWORD`.
