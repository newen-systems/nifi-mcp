# Authentication integration

Run the real MCP server against isolated NiFi and Keycloak containers, including TLS and authorization decisions.

## Requirements

Python 3.12, the locked development environment, Docker, and free loopback ports 18443–18446.
The runner pins NiFi 2.11.0 and Keycloak 26.5.0 by digest. Cache those images before running offline.

## Usage

```sh
uv run --frozen python tests/integration/run_auth.py all
```

## Behaviour

The runner generates disposable credentials and a private CA under `~/.cache/nifi-mcp/auth-proof`.
Only its labelled containers/network are managed; `all` cleans them and their credentials on exit.
Fixture ports bind to loopback. Service configuration listens inside its own containers.
A fixture-only DNS alias routes the canonical issuer hostname to loopback; TLS, signatures,
PKCE, introspection, MCP transports, NiFi requests and policy checks all run against real services.
No fake HTTP transport or mocked certificate loading is used.
The username/password, personal bearer and certificate cases exercise real stdio tool calls.
The HTTP case exercises authorization-code PKCE with the MCP SDK, concurrent callers, real group
policies, denied reader writes, forged headers, wrong audiences and issuer session revocation.
This does not validate the VS Code UI, LDAP or production directory federation, or another NiFi version.
