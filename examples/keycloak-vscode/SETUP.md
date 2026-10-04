# Internal Keycloak and NiFi setup

## Keycloak

Use the same internal realm and directory federation as NiFi browser login.

1. Create a confidential client `nifi-mcp` for server-side token introspection. Enable client
   authentication; disable standard flow, direct access grants and service accounts for this client.
   Store its secret through your normal Vault process and inject it as `NIFI_OAUTH_CLIENT_SECRET`.
2. Create a public client `nifi-mcp-vscode`. Enable standard flow and require PKCE S256. Disable
   implicit flow, direct access grants and service accounts. No client secret belongs in VS Code.
3. Allow only the exact callback URI shown by your installed VS Code OAuth login. Include the
   corresponding URI for VS Code Remote if you use it. Do not wildcard all redirect URIs.
4. Add an audience mapper to the public client's default scope with **Included Custom Audience** set to the complete MCP resource URL
   (for example `https://nifi-mcp.example.internal/mcp`). Do not use the public client ID,
   the introspection client ID or Keycloak's `account` audience as this value.
   Include it in access tokens and introspection responses. Assign this dedicated scope only to
   your authorized MCP clients, not as a realm-wide default. `NIFI_OAUTH_AUDIENCE` must equal `NIFI_OAUTH_RESOURCE_URL` and this mapper value.
   Metadata advertises `NIFI_OAUTH_SCOPES` (default `openid`, `profile`, `email`); include
   any additional named client scopes your claim mappers require. Map the audience and groups
   in the public client's dedicated default scope so every OAuth token carries them.
5. Add the same identifying-user and group-membership mappers that NiFi's OIDC client uses.
   Include these in access tokens and introspection responses, not only ID tokens. Match
   `NIFI_OAUTH_IDENTITY_CLAIM` to `nifi.security.user.oidc.claim.identifying.user` and
   `NIFI_OAUTH_GROUPS_CLAIM` to `nifi.security.user.oidc.claim.groups`. The example uses
   `preferred_username`; use `email` if that is what NiFi uses. The identifying claim is required explicitly. Group names and full-path settings
   must match NiFi's group policies. MCP escapes angle brackets and uses NiFi's UTF-8 Base64 encoding for Unicode entities.
   Control characters and ambiguous backslashes are refused.
6. Make the identifying claim administrator-controlled and unique to the directory account.
   Disable self-service changes of the policy-bearing username/email and verify email ownership
   where email identifies the NiFi user. Set `NIFI_OAUTH_ISSUER_URL` to the exact realm issuer from its OIDC discovery document and
   `NIFI_OAUTH_INTROSPECTION_URL` to its `introspection_endpoint`.

VS Code reads MCP protected-resource metadata and the advertised internal issuer's discovery,
then performs browser authorization with PKCE. Pre-register its public client ID using the
`oauth.clientId` setting; dynamic registration is not required. VS Code handles token refresh.
The verifier checks the exact resource URL in the token audience; the MCP SDK also checks
the verified token resource. The server checks tokens on every HTTP request and fails closed if Keycloak is unavailable.
It deliberately caches no authentication result, so session revocation is checked on the next
request. The NiFi TLS context is loaded once; restart MCP when its certificate/key or CA rotates.

## NiFi

Use NiFi 2.x with a managed authorizer supporting proxied identities and IdP groups.
NiFi 2.10's X509 authentication provider consumes `X-ProxiedEntitiesChain` and
`X-ProxiedEntityGroups` and checks proxy authorization before authorizing the end user.

- Issue a dedicated client-auth certificate for the MCP service from a CA trusted by NiFi.
  Keep the private key accessible only to the MCP service account.
- Add that certificate's identity (after NiFi identity mapping) to the authorizer and grant it
  write on the `/proxy` resource, shown as **proxy user requests** in NiFi's policy UI.
  Do not give it administrator or flow policies. Every flow request carries an end-user chain.
- Supply PEM certificate/key paths in `NIFI_PROXY_CERT` and `NIFI_PROXY_KEY` and the CA in
  `NIFI_CA_BUNDLE`. The client certificate goes only to NiFi, never to Keycloak.
- Retain the existing user/group policies. Match identity claims and directory group names
  exactly to the NiFi browser-login path. Identity/group mapping rules can affect this parity;
  compare current-user identity and permitted operations through both paths before rollout.
  NiFi's OIDC handler maps group claims before authorization, but its X509 proxy handler does
  not map header-supplied IdP groups. If NiFi uses `nifi.security.group.mapping.*`, configure a
  dedicated Keycloak introspection claim (for example `nifi_effective_groups`) containing the
  exact final mapped names and set `NIFI_OAUTH_GROUPS_CLAIM` to it. Provision it through an
  administrator-controlled mapper/directory attribute, not an editable user profile attribute.
  Do not forward raw unmapped groups in that deployment. User identity mappings still run in
  NiFi's X509 provider, so send the raw identifying claim and do not apply its mapping twice.
- This service is a trusted identity proxy. Protect its certificate, confidential introspection
  secret, environment, host and code as an authentication component. The model and HTTP callers
  cannot select an identity or groups: those come only from Keycloak's verified response.

## Internal TLS and offline operation

Run the HTTP listener on loopback behind an internal TLS reverse proxy. Route `/mcp` and
`/.well-known/oauth-protected-resource/mcp` to port 8000. Preserve the Host and Authorization
headers. Strip externally supplied proxy identity/group headers at the edge; MCP ignores them
and constructs its own outbound headers. Do not expose the plain HTTP backend on another host.
Set `NIFI_OAUTH_RESOURCE_URL` to the complete external HTTPS URL ending in `/mcp`.
Trust the internal CA in Python, the reverse proxy, NiFi, VS Code, and the login browser.

Install from `uv.lock` or transfer a prepared environment/wheelhouse before isolation.
The launcher uses an installed environment, or `uv --offline --frozen --no-sync`; it never
retrieves credentials or packages. Runtime traffic remains between VS Code, your local model,
MCP, NiFi and internal Keycloak. Keep the model's chat extension offline-compatible too.
The server has no model-provider dependency and does not send OAuth tokens to the model.

## Permission acceptance

Use two actual directory-backed accounts: one with only the intended read policies, and one
with a narrowly granted write policy on a disposable unversioned test group.

1. Sign in independently from two VS Code profiles and call `nifi_current_user` from each.
2. Confirm the returned identities and groups match each account's NiFi browser session.
3. Read the same permitted group from both clients, then attempt the same harmless permitted
   mutation on that group. Expect success for the writer and NiFi HTTP 403 for the reader.
4. Repeat the calls concurrently and verify NiFi's audit trail attributes each operation correctly.
5. Sign out/revoke the reader's session and confirm its next MCP request requires reauthentication.
6. Repeat with a token for another audience and with no token; expect HTTP 401 before any NiFi call.

Repository tests mock Keycloak and NiFi and cover request isolation and denial propagation.
They do not certify your deployed clients, federation, certificate trust or NiFi policies.

Sources: [NiFi admin guide](https://nifi.apache.org/nifi-docs/administration-guide.html),
[NiFi 2.10 X509 provider](https://github.com/apache/nifi/blob/rel/nifi-2.10.0/nifi-framework-bundle/nifi-framework/nifi-web/nifi-web-security/src/main/java/org/apache/nifi/web/security/x509/X509AuthenticationProvider.java),
[Keycloak OIDC](https://www.keycloak.org/securing-apps/oidc-layers),
[VS Code MCP configuration](https://code.visualstudio.com/docs/agents/reference/mcp-configuration).
