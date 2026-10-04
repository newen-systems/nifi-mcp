# Personal stdio authentication

Connect VS Code or a CLI MCP host to NiFi using your configured credentials.

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

## Behaviour

No Keycloak is required. NiFi enforces your configured identity's permissions; an admin credential gives this local instance admin access.
`NIFI_CA_BUNDLE` supplies server trust. Certificate authentication also requires a client certificate and PEM key.
An encrypted key uses `NIFI_CLIENT_KEY_PASSWORD`. Keep the credential file outside the checkout.
Legacy OIDC password-grant automation can authenticate as token `sub` rather than the browser username.
Inspect `nifi_current_user` and provision that API principal explicitly; the server grants no policies.
The per-user OAuth proxy preserves the configured browser identifying claim.
