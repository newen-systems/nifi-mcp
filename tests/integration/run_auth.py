"""Real NiFi authentication proof. Runtime keys/credentials stay outside the repository."""

from __future__ import annotations

import argparse
import asyncio
import io
import json
import os
import secrets
import shutil
import ssl
import subprocess
import tarfile
import time
from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.hazmat.primitives.serialization import pkcs12
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID
from mcp import ClientSession, StdioServerParameters
from mcp.client.stdio import stdio_client

RUNTIME = Path.home() / ".cache" / "nifi-mcp" / "auth-proof"
NIFI_IMAGE = "apache/nifi@sha256:74aebc23308be2b68ad8163b1f8f45c37bba506c569a1f9e0de25af9183430ea"
KEYCLOAK_IMAGE = "quay.io/keycloak/keycloak@sha256:5fdd7cda82e58775ed124294c7e16fabc33166d38dfc4aabebda7d64e7a964bf"
NIFI_URL = "https://localhost:18443/nifi-api"
ROOT = Path(__file__).resolve().parents[2]
DOCKER = shutil.which("docker") or "/usr/bin/docker"


class ProofFailure(RuntimeError):
    """Only messages authored by this runner, never credential-bearing endpoint bodies."""


def docker(*args: str) -> bytes:
    result = subprocess.run([DOCKER, *args], capture_output=True, check=False)  # noqa: S603 - controlled argv, no shell
    if result.returncode:
        raise ProofFailure(f"Docker operation failed: {args[0]} (exit {result.returncode})")
    return result.stdout


def save(state: dict) -> None:
    path = RUNTIME / "state.json"
    path.write_text(json.dumps(state))
    path.chmod(0o600)


def certificate(name, key, issuer, issuer_key, *, ca=False, server=False):
    subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, name)])
    builder = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(issuer or subject)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(datetime.now(UTC) - timedelta(minutes=5))
        .not_valid_after(datetime.now(UTC) + timedelta(days=2))
        .add_extension(x509.BasicConstraints(ca=ca, path_length=None), critical=True)
        .add_extension(x509.SubjectKeyIdentifier.from_public_key(key.public_key()), critical=False)
    )
    if ca:
        builder = builder.add_extension(x509.KeyUsage(True, False, False, False, False, True, True, False, False), True)
    else:
        builder = builder.add_extension(
            x509.ExtendedKeyUsage([ExtendedKeyUsageOID.SERVER_AUTH if server else ExtendedKeyUsageOID.CLIENT_AUTH]),
            False,
        )
    if server:
        import ipaddress

        builder = builder.add_extension(
            x509.SubjectAlternativeName(
                [
                    x509.DNSName("localhost"),
                    x509.DNSName("keycloak.auth.test"),
                    x509.IPAddress(ipaddress.ip_address("127.0.0.1")),
                ]
            ),
            False,
        )
    return builder.sign(issuer_key or key, hashes.SHA256())


def setup() -> dict:
    if (RUNTIME / "state.json").exists():
        return json.loads((RUNTIME / "state.json").read_text())
    RUNTIME.mkdir(mode=0o700, parents=True, exist_ok=True)
    state = {
        "id": secrets.token_hex(4),
        "store_password": secrets.token_urlsafe(24),
        "password": secrets.token_urlsafe(24),
        "username": "auth-fixture-admin",
    }
    root_key = ec.generate_private_key(ec.SECP256R1())
    ca = certificate("Auth fixture CA", root_key, None, None, ca=True)
    (RUNTIME / "ca.crt").write_bytes(ca.public_bytes(serialization.Encoding.PEM))
    for name, server in [("server", True), ("admin", False), ("proxy", False), ("reader", False), ("writer", False)]:
        key = ec.generate_private_key(ec.SECP256R1())
        cert = certificate(name if name != "admin" else "auth-proof-admin", key, ca.subject, root_key, server=server)
        (RUNTIME / f"{name}.crt").write_bytes(cert.public_bytes(serialization.Encoding.PEM))
        (RUNTIME / f"{name}.key").write_bytes(
            key.private_bytes(
                serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8, serialization.NoEncryption()
            )
        )
        if server:
            (RUNTIME / "keystore.p12").write_bytes(
                pkcs12.serialize_key_and_certificates(
                    b"nifi", key, cert, [ca], serialization.BestAvailableEncryption(state["store_password"].encode())
                )
            )
    for path in RUNTIME.iterdir():
        if path.is_file():
            path.chmod(0o600)
    env = RUNTIME / "store.env"
    env.write_text(f"FIXTURE_STORE_PASSWORD={state['store_password']}\n")
    env.chmod(0o600)
    docker(
        "run",
        "--rm",
        "--user",
        "0",
        "--env-file",
        str(env),
        "-v",
        f"{RUNTIME}:/fixture",
        "--entrypoint",
        "keytool",
        NIFI_IMAGE,
        "-importcert",
        "-noprompt",
        "-storetype",
        "PKCS12",
        "-alias",
        "root",
        "-file",
        "/fixture/ca.crt",
        "-keystore",
        "/fixture/truststore.p12",
        "-storepass:env",
        "FIXTURE_STORE_PASSWORD",
    )
    save(state)
    return state


def configure(state, mode):
    conf = RUNTIME / f"conf-{mode}"
    if conf.exists():
        return conf
    archive = docker(
        "run", "--rm", "--entrypoint", "tar", NIFI_IMAGE, "-C", "/opt/nifi/nifi-current", "-cf", "-", "conf"
    )
    target = RUNTIME / f"base-{mode}"
    target.mkdir()
    with tarfile.open(fileobj=io.BytesIO(archive)) as tar:
        tar.extractall(target, filter="data")
    (target / "conf").rename(conf)
    properties = conf / "nifi.properties"
    values = {
        "nifi.security.keystore": "/fixture/keystore.p12",
        "nifi.security.keystoreType": "PKCS12",
        "nifi.security.keystorePasswd": state["store_password"],
        "nifi.security.keyPasswd": state["store_password"],
        "nifi.security.truststore": "/fixture/truststore.p12",
        "nifi.security.truststoreType": "PKCS12",
        "nifi.security.truststorePasswd": state["store_password"],
        "nifi.web.https.host": "0.0.0.0",  # noqa: S104 - container only; published on loopback
        "nifi.web.https.port": "8443",
        "nifi.web.proxy.host": "localhost:18443,127.0.0.1:18443",
    }
    lines = properties.read_text().splitlines()
    lines = [
        f"{line.split('=', 1)[0]}={values.pop(line.split('=', 1)[0])}"
        if "=" in line and line.split("=", 1)[0] in values
        else line
        for line in lines
    ]
    properties.write_text("\n".join(lines + [f"{key}={value}" for key, value in values.items()]) + "\n")
    return conf


def start_single(state):
    conf = configure(state, "single")
    env = RUNTIME / "single.env"
    env.write_text(
        f"SINGLE_USER_CREDENTIALS_USERNAME={state['username']}\n"
        f"SINGLE_USER_CREDENTIALS_PASSWORD={state['password']}\n"
        "NIFI_WEB_HTTPS_HOST=0.0.0.0\nNIFI_WEB_PROXY_HOST=localhost:18443,127.0.0.1:18443\n"
        "NIFI_JVM_HEAP_INIT=256m\nNIFI_JVM_HEAP_MAX=512m\n"
    )
    env.chmod(0o600)
    name = f"nifi-mcp-auth-proof-{state['id']}-single"
    if activate_existing(state, "single"):
        return name
    docker(
        "run",
        "-d",
        "--name",
        name,
        "--label",
        f"org.nifi-mcp.auth-proof={state['id']}",
        "--user",
        "0",
        "--memory",
        "1536m",
        "--cpus",
        "2",
        "--env-file",
        str(env),
        "-p",
        "127.0.0.1:18443:8443",
        "-v",
        f"{RUNTIME}:/fixture:ro",
        "-v",
        f"{conf}:/opt/nifi/nifi-current/conf",
        NIFI_IMAGE,
    )
    return name


def tls():
    context = ssl.create_default_context()
    context.load_verify_locations(cafile=str(RUNTIME / "ca.crt"))
    return context


async def ready():
    deadline = time.monotonic() + 240
    async with httpx.AsyncClient(verify=tls(), trust_env=False, timeout=5) as http:
        while time.monotonic() < deadline:
            try:
                response = await http.get(f"{NIFI_URL}/access/config")
                if response.status_code in {200, 401, 403}:
                    print("Real NiFi HTTPS endpoint ready (CA verified)", flush=True)
                    return
            except httpx.HTTPError:
                pass
            await asyncio.sleep(5)
        raise ProofFailure("NiFi did not become ready within 240 seconds")


async def stdio_identity(auth, state, token=None):
    env = dict(
        os.environ,
        NIFI_API_URL=NIFI_URL,
        NIFI_AUTH=auth,
        NIFI_TRANSPORT="stdio",
        NIFI_CA_BUNDLE=str(RUNTIME / "ca.crt"),
        NIFI_USERNAME=state["username"],
        NIFI_PASSWORD=state["password"],
    )
    if token:
        env["NIFI_BEARER_TOKEN"] = token
    else:
        env["NIFI_BEARER_TOKEN"] = ""
    params = StdioServerParameters(command=str(ROOT / "start-server.sh"), env=env)
    async with stdio_client(params) as (read, write), ClientSession(read, write) as session:
        await session.initialize()
        result = await session.call_tool("nifi_current_user", {})
        text = json.loads(result.content[0].text)
        if text.get("identity") != state["username"] or text.get("anonymous") is not False:
            raise ProofFailure(f"{auth}: current-user did not match expected principal")
        about = await session.call_tool("nifi_about", {})
        if json.loads(about.content[0].text).get("is_nifi_2x") is not True:
            raise ProofFailure(f"{auth}: about failed")
        created = await session.call_tool(
            "nifi_create_process_group", {"params": {"name": f"auth-proof-{auth}", "inherit_parameter_context": False}}
        )
        payload = json.loads(created.content[0].text)
        if payload.get("outcome") != "applied":
            raise ProofFailure(f"{auth}: group creation did not apply")
        group_id = payload["process_group"]["id"]
        deleted = await session.call_tool(
            "nifi_delete_component", {"params": {"kind": "process_group", "component_id": group_id}}
        )
        if json.loads(deleted.content[0].text).get("outcome") != "applied":
            raise ProofFailure(f"{auth}: group cleanup failed")
        print(f"PASS {auth}: real stdio -> TLS -> NiFi identity, read, write and cleanup", flush=True)


async def single():
    state = setup()
    start_single(state)
    print("Starting isolated single-user NiFi fixture", flush=True)
    await ready()
    await stdio_identity("jwt", state)
    async with httpx.AsyncClient(verify=tls(), trust_env=False) as http:
        response = await http.post(
            f"{NIFI_URL}/access/token", data={"username": state["username"], "password": state["password"]}
        )
        if response.status_code not in {200, 201}:
            raise ProofFailure("Single-user token issuance failed")
        await stdio_identity("bearer", state, response.text.strip())


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=["single", "keycloak", "managed", "legacy", "oauth", "all", "cleanup"])
    args = parser.parse_args()
    try:
        if args.phase == "single":
            asyncio.run(single())
        elif args.phase == "keycloak":
            asyncio.run(keycloak())
        elif args.phase == "managed":
            asyncio.run(managed())
        elif args.phase == "legacy":
            asyncio.run(legacy())
        elif args.phase == "oauth":
            asyncio.run(oauth())
        elif args.phase == "all":
            asyncio.run(all_methods())
        else:
            cleanup()
    except Exception as exc:  # noqa: BLE001 - credential-safe process boundary
        # Exception/endpoint bodies and command argv can contain runtime credentials.
        reason = str(exc) if isinstance(exc, ProofFailure) else type(exc).__name__
        print(f"Authentication proof failed: {reason}", flush=True)
        raise SystemExit(1) from None


KEYCLOAK_URL = "https://keycloak.auth.test:18444"
ISSUER = f"{KEYCLOAK_URL}/realms/auth-proof"
MCP_URL = "https://localhost:18445/mcp"


def fixture_dns():
    import socket

    original = socket.getaddrinfo

    def lookup(host, *args, **kwargs):
        if host in {"keycloak.auth.test", b"keycloak.auth.test"}:
            host = "127.0.0.1"
        return original(host, *args, **kwargs)

    socket.getaddrinfo = lookup


def start_keycloak(state):
    if "oauth_secret" not in state:
        state.update(
            oauth_secret=secrets.token_urlsafe(24),
            legacy_secret=secrets.token_urlsafe(24),
            admin_password=secrets.token_urlsafe(24),
            reader_password=secrets.token_urlsafe(24),
            writer_password=secrets.token_urlsafe(24),
        )
        save(state)
    network = f"nifi-mcp-auth-proof-{state['id']}"
    if subprocess.run([DOCKER, "network", "inspect", network], capture_output=True).returncode:  # noqa: S603 - fixture-owned argv, no shell
        docker("network", "create", "--label", f"org.nifi-mcp.auth-proof={state['id']}", network)
    name = f"{network}-keycloak"
    if not subprocess.run([DOCKER, "inspect", name], capture_output=True).returncode:  # noqa: S603 - fixture-owned argv, no shell
        return name
    scope = {
        "name": "nifi-mcp",
        "protocol": "openid-connect",
        "protocolMappers": [
            {
                "name": "resource-audience",
                "protocol": "openid-connect",
                "protocolMapper": "oidc-audience-mapper",
                "config": {
                    "included.custom.audience": MCP_URL,
                    "access.token.claim": "true",
                    "introspection.token.claim": "true",
                },
            },
            {
                "name": "groups",
                "protocol": "openid-connect",
                "protocolMapper": "oidc-group-membership-mapper",
                "config": {
                    "claim.name": "groups",
                    "full.path": "false",
                    "access.token.claim": "true",
                    "id.token.claim": "true",
                    "userinfo.token.claim": "true",
                    "introspection.token.claim": "true",
                },
            },
            {
                "name": "principal",
                "protocol": "openid-connect",
                "protocolMapper": "oidc-usermodel-property-mapper",
                "config": {
                    "user.attribute": "username",
                    "claim.name": "preferred_username",
                    "jsonType.label": "String",
                    "access.token.claim": "true",
                    "id.token.claim": "true",
                    "introspection.token.claim": "true",
                },
            },
        ],
    }
    defaults = ["basic", "profile", "email", "nifi-mcp"]
    realm = {
        "realm": "auth-proof",
        "enabled": True,
        "sslRequired": "all",
        "groups": [{"name": "proof-readers"}, {"name": "proof-writers"}],
        "clientScopes": [scope],
        "users": [
            {
                "username": who,
                "enabled": True,
                "emailVerified": True,
                "email": f"{who}@example.test",
                "firstName": who,
                "lastName": "Fixture",
                "groups": [f"/proof-{who}s"],
                "credentials": [{"type": "password", "value": state[f"{who}_password"], "temporary": False}],
            }
            for who in ["reader", "writer"]
        ],
        "clients": [
            {
                "clientId": "nifi-mcp",
                "enabled": True,
                "publicClient": False,
                "secret": state["oauth_secret"],
                "standardFlowEnabled": False,
                "directAccessGrantsEnabled": False,
                "serviceAccountsEnabled": False,
            },
            {
                "clientId": "nifi-mcp-vscode",
                "enabled": True,
                "publicClient": True,
                "standardFlowEnabled": True,
                "directAccessGrantsEnabled": False,
                "redirectUris": ["http://localhost:18447/callback"],
                "attributes": {"pkce.code.challenge.method": "S256"},
                "defaultClientScopes": defaults,
            },
            {
                "clientId": "nifi-ui",
                "enabled": True,
                "publicClient": False,
                "secret": state["legacy_secret"],
                "standardFlowEnabled": True,
                "directAccessGrantsEnabled": True,
                "redirectUris": ["https://localhost:18443/nifi-api/access/oidc/callback/consumer"],
                "defaultClientScopes": defaults,
            },
        ],
    }
    path = RUNTIME / "realm.json"
    path.write_text(json.dumps(realm))
    path.chmod(0o600)
    env = RUNTIME / "keycloak.env"
    env.write_text(
        f"KC_BOOTSTRAP_ADMIN_USERNAME=fixture-control\nKC_BOOTSTRAP_ADMIN_PASSWORD={state['admin_password']}\n"
        "JAVA_OPTS_KC_HEAP=-Xms128m -Xmx256m\n"
    )
    env.chmod(0o600)
    docker(
        "run",
        "-d",
        "--name",
        name,
        "--label",
        f"org.nifi-mcp.auth-proof={state['id']}",
        "--network",
        network,
        "--network-alias",
        "keycloak.auth.test",
        "--user",
        "0",
        "--memory",
        "768m",
        "--env-file",
        str(env),
        "-p",
        "127.0.0.1:18444:18444",
        "-v",
        f"{RUNTIME}:/fixture:ro",
        "-v",
        f"{path}:/opt/keycloak/data/import/realm.json:ro",
        KEYCLOAK_IMAGE,
        "start-dev",
        "--http-enabled=false",
        "--https-port=18444",
        "--https-certificate-file=/fixture/server.crt",
        "--https-certificate-key-file=/fixture/server.key",
        "--hostname=" + KEYCLOAK_URL,
        "--import-realm",
    )
    return name


async def keycloak():
    state = setup()
    fixture_dns()
    start_keycloak(state)
    deadline = time.monotonic() + 180
    async with httpx.AsyncClient(verify=tls(), trust_env=False, timeout=5) as http:
        while time.monotonic() < deadline:
            try:
                response = await http.get(f"{ISSUER}/.well-known/openid-configuration")
                if response.status_code == 200 and response.json()["issuer"] == ISSUER:
                    await configure_builtin_scopes(http, state)
                    print("Real Keycloak OIDC discovery ready (CA and issuer verified)", flush=True)
                    return
            except httpx.HTTPError:
                pass
            await asyncio.sleep(5)
    raise ProofFailure("Fixture Keycloak discovery did not become ready")


def start_managed(state):
    conf = configure(state, "managed")
    props = conf / "nifi.properties"
    values = {
        "nifi.security.user.authorizer": "managed-authorizer",
        "nifi.security.user.login.identity.provider": "",
        "nifi.security.user.oidc.discovery.url": f"{ISSUER}/.well-known/openid-configuration",
        "nifi.security.user.oidc.client.id": "nifi-ui",
        "nifi.security.user.oidc.client.secret": state["legacy_secret"],
        "nifi.security.user.oidc.preferred.jwsalgorithm": "RS256",
        "nifi.security.user.oidc.additional.scopes": "profile,email",
        "nifi.security.user.oidc.claim.identifying.user": "preferred_username",
        "nifi.security.user.oidc.claim.groups": "groups",
        "nifi.security.user.oidc.truststore.strategy": "NIFI",
        "nifi.security.identity.mapping.pattern.dn": "^CN=(.+)$",
        "nifi.security.identity.mapping.value.dn": "$1",
        "nifi.security.identity.mapping.transform.dn": "NONE",
    }
    lines = props.read_text().splitlines()
    lines = [
        f"{line.split('=', 1)[0]}={values.pop(line.split('=', 1)[0])}"
        if "=" in line and line.split("=", 1)[0] in values
        else line
        for line in lines
    ]
    props.write_text("\n".join(lines + [f"{k}={v}" for k, v in values.items()]) + "\n")
    import xml.etree.ElementTree as ET

    file = conf / "authorizers.xml"
    tree = ET.parse(file)  # noqa: S314 - trusted config extracted from a digest-pinned image
    for prop in tree.findall(".//property"):
        if prop.get("name") in {"Initial User Identity 1", "Initial Admin Identity"}:
            prop.text = "CN=auth-proof-admin"
    tree.write(file, encoding="utf-8", xml_declaration=True)
    name = f"nifi-mcp-auth-proof-{state['id']}-managed"
    if activate_existing(state, "managed"):
        return name
    single_name = f"nifi-mcp-auth-proof-{state['id']}-single"
    if not subprocess.run([DOCKER, "inspect", single_name], capture_output=True).returncode:  # noqa: S603 - fixture-owned argv, no shell
        docker("stop", "--time", "20", single_name)
    env = RUNTIME / "managed.env"
    env.write_text(
        "NIFI_WEB_HTTPS_HOST=0.0.0.0\nNIFI_WEB_PROXY_HOST=localhost:18443,127.0.0.1:18443\n"
        "NIFI_JVM_HEAP_INIT=256m\nNIFI_JVM_HEAP_MAX=512m\n"
    )
    env.chmod(0o600)
    docker(
        "run",
        "-d",
        "--name",
        name,
        "--label",
        f"org.nifi-mcp.auth-proof={state['id']}",
        "--network",
        f"nifi-mcp-auth-proof-{state['id']}",
        "--user",
        "0",
        "--memory",
        "1536m",
        "--cpus",
        "2",
        "--env-file",
        str(env),
        "-p",
        "127.0.0.1:18443:8443",
        "-v",
        f"{RUNTIME}:/fixture:ro",
        "-v",
        f"{conf}:/opt/nifi/nifi-current/conf",
        NIFI_IMAGE,
    )
    return name


async def managed():
    await keycloak()
    state = setup()
    start_managed(state)
    print("Starting isolated multi-user NiFi fixture", flush=True)
    await ready()
    await certificate_checks(state)


async def configure_builtin_scopes(http, state):
    response = await http.post(
        f"{KEYCLOAK_URL}/realms/master/protocol/openid-connect/token",
        data={
            "grant_type": "password",
            "client_id": "admin-cli",
            "username": "fixture-control",
            "password": state["admin_password"],
        },
    )
    if response.status_code != 200:
        raise ProofFailure("Fixture Keycloak bootstrap login failed")
    headers = {"Authorization": f"Bearer {response.json()['access_token']}"}
    base = f"{KEYCLOAK_URL}/admin/realms/auth-proof"
    source = (await http.get(f"{KEYCLOAK_URL}/admin/realms/master/client-scopes", headers=headers)).json()
    existing = (await http.get(f"{base}/client-scopes", headers=headers)).json()
    await import_builtin_scopes(http, base, headers, source, existing)
    scopes = (await http.get(f"{base}/client-scopes", headers=headers)).json()
    target_scope = next(scope for scope in scopes if scope["name"] == "nifi-mcp")
    mapper_path = f"{base}/client-scopes/{target_scope['id']}/protocol-mappers/models"
    mappers = (await http.get(mapper_path, headers=headers)).json()
    resource_mapper = next(mapper for mapper in mappers if mapper["name"] == "resource-audience")
    resource_mapper["config"].pop("included.client.audience", None)
    resource_mapper["config"]["included.custom.audience"] = MCP_URL
    updated = await http.put(f"{mapper_path}/{resource_mapper['id']}", headers=headers, json=resource_mapper)
    if updated.status_code != 204:
        raise ProofFailure("Fixture resource-audience mapper update failed")
    clients = (await http.get(f"{base}/clients", headers=headers)).json()
    for client in clients:
        if client["clientId"] not in {"nifi-mcp-vscode", "nifi-ui"}:
            continue
        for scope in scopes:
            if scope["name"] not in {"basic", "profile", "email", "nifi-mcp"}:
                continue
            response = await http.put(
                f"{base}/clients/{client['id']}/default-client-scopes/{scope['id']}", headers=headers
            )
            if response.status_code != 204:
                raise ProofFailure("Assigning fixture client scope failed")


async def import_builtin_scopes(http, base, headers, source, existing):
    for wanted in ["basic", "profile", "email"]:
        if not any(scope["name"] == wanted for scope in existing):
            template = next(scope for scope in source if scope["name"] == wanted)
            template.pop("id", None)
            for mapper in template.get("protocolMappers", []):
                mapper.pop("id", None)
            response = await http.post(f"{base}/client-scopes", headers=headers, json=template)
            if response.status_code != 201:
                raise ProofFailure("Creating fixture client scope failed")


def certificate_tls(role):
    context = tls()
    context.load_cert_chain(str(RUNTIME / f"{role}.crt"), str(RUNTIME / f"{role}.key"))
    return context


async def api(http, method, path, body=None):
    response = await http.request(method, f"{NIFI_URL}{path}", json=body)
    if response.status_code not in {200, 201}:
        raise ProofFailure(f"Fixture provisioning {method} {path.split('/')[1]} failed (HTTP {response.status_code})")
    return response.json() if response.content else {}


async def seed_policies(state):
    if "user_parent" in state:
        return
    async with httpx.AsyncClient(verify=certificate_tls("admin"), trust_env=False) as http:
        principal = await api(http, "GET", "/flow/current-user")
        if principal["identity"] != "auth-proof-admin":
            raise ProofFailure("Certificate admin identity mapping mismatch")
        existing = (await api(http, "GET", "/tenants/users"))["users"]
        users = {"admin": next(u["id"] for u in existing if u["component"]["identity"] == "auth-proof-admin")}
        for identity in ["reader", "writer", "proxy"]:
            found = next((u for u in existing if u["component"]["identity"] == identity), None)
            result = found or await api(
                http, "POST", "/tenants/users", {"revision": {"version": 0}, "component": {"identity": identity}}
            )
            users[identity] = result["id"]
        existing_groups = (await api(http, "GET", "/tenants/user-groups"))["userGroups"]
        groups = {}
        for name in ["proof-readers", "proof-writers"]:
            found = next((g for g in existing_groups if g["component"]["identity"] == name), None)
            result = found or await api(
                http,
                "POST",
                "/tenants/user-groups",
                {"revision": {"version": 0}, "component": {"identity": name, "users": []}},
            )
            groups[name] = result["id"]

        async def policy(resource, action, identities=(), group_names=()):
            current_response = await http.get(f"{NIFI_URL}/policies/{action}{resource}")
            current = current_response.json() if current_response.status_code == 200 else None
            user_ids = {users["admin"], *(users[user] for user in identities)}
            group_ids = {groups[group] for group in group_names}
            if current:
                user_ids.update(u["id"] for u in current["component"].get("users", []))
                group_ids.update(g["id"] for g in current["component"].get("userGroups", []))
            component = {
                "resource": resource,
                "action": action,
                "users": [{"id": uid} for uid in user_ids],
                "userGroups": [{"id": gid} for gid in group_ids],
            }
            if current:
                component["id"] = current["id"]
                await api(
                    http, "PUT", f"/policies/{current['id']}", {"revision": current["revision"], "component": component}
                )
            else:
                await api(http, "POST", "/policies", {"revision": {"version": 0}, "component": component})

        await policy("/proxy", "write", ["proxy"])
        await policy("/flow", "read", ["reader", "writer"])
        root_flow = await api(http, "GET", "/flow/process-groups/root")
        root_id = root_flow["processGroupFlow"]["id"]
        await policy(f"/process-groups/{root_id}", "read", ["admin"])
        await policy(f"/process-groups/{root_id}", "write", ["admin"])
        for label in ["user", "group"]:
            flow = await api(http, "GET", "/flow/process-groups/root")
            existing_groups = flow["processGroupFlow"]["flow"]["processGroups"]
            found = next((g for g in existing_groups if g["component"]["name"] == f"auth-proof-{label}-policy"), None)
            result = found or await api(
                http,
                "POST",
                "/process-groups/root/process-groups",
                {
                    "revision": {"version": 0},
                    "component": {"name": f"auth-proof-{label}-policy", "position": {"x": 0, "y": 0}},
                },
            )
            parent = result["id"]
            state[f"{label}_parent"] = parent
            if label == "user":
                await policy(f"/process-groups/{parent}", "read", ["reader", "writer"])
                await policy(f"/process-groups/{parent}", "write", ["writer"])
            else:
                await policy(f"/process-groups/{parent}", "read", group_names=["proof-readers", "proof-writers"])
                await policy(f"/process-groups/{parent}", "write", group_names=["proof-writers"])
    save(state)
    print("Real NiFi user and IdP-group policies provisioned; proxy has /proxy only", flush=True)


async def certificate_rpc(state, role, *, encrypted=False):
    key = RUNTIME / f"{role}.key"
    if encrypted:
        private = serialization.load_pem_private_key(key.read_bytes(), password=None)
        key = RUNTIME / f"{role}-encrypted.key"
        key.write_bytes(
            private.private_bytes(
                serialization.Encoding.PEM,
                serialization.PrivateFormat.PKCS8,
                serialization.BestAvailableEncryption(state["password"].encode()),
            )
        )
        key.chmod(0o600)
    env = dict(
        os.environ,
        NIFI_API_URL=NIFI_URL,
        NIFI_AUTH="mtls",
        NIFI_TRANSPORT="stdio",
        NIFI_READONLY="false",
        NIFI_CA_BUNDLE=str(RUNTIME / "ca.crt"),
        NIFI_CLIENT_CERT=str(RUNTIME / f"{role}.crt"),
        NIFI_CLIENT_KEY=str(key),
        NIFI_CLIENT_KEY_PASSWORD=state["password"] if encrypted else "",
        NIFI_BEARER_TOKEN="stale-token-must-not-win",
    )
    params = StdioServerParameters(command=str(ROOT / "start-server.sh"), env=env)
    async with stdio_client(params) as (read, write), ClientSession(read, write) as session:
        await session.initialize()
        result = json.loads((await session.call_tool("nifi_current_user", {})).content[0].text)
        if result.get("identity") != role:
            raise ProofFailure("Direct certificate principal mismatch")
        flow = json.loads(
            (await session.call_tool("nifi_get_flow", {"params": {"process_group_id": state["user_parent"]}}))
            .content[0]
            .text
        )
        if flow.get("status") == "error":
            raise ProofFailure("Direct certificate authorized read failed")
        result = json.loads(
            (
                await session.call_tool(
                    "nifi_create_process_group",
                    {
                        "params": {
                            "parent_id": state["user_parent"],
                            "name": f"certificate-{role}-{int(encrypted)}",
                            "inherit_parameter_context": False,
                        }
                    },
                )
            )
            .content[0]
            .text
        )
        if role == "reader":
            if result.get("status") != "error" or result.get("outcome") != "not_applied":
                raise ProofFailure("Read-only certificate user unexpectedly wrote")
            if result.get("status_code") != 403 and "403" not in json.dumps(result):
                raise ProofFailure("Certificate denial was not NiFi 403")
        elif result.get("outcome") != "applied":
            raise ProofFailure("Certificate writer did not apply mutation")
    print(
        f"PASS mtls {role}{' encrypted-key' if encrypted else ''}: real certificate identity, read and policy decision",
        flush=True,
    )


async def certificate_checks(state):
    await seed_policies(state)
    await certificate_rpc(state, "reader")
    await certificate_rpc(state, "writer")
    await certificate_rpc(state, "writer", encrypted=True)


async def legacy():
    await keycloak()
    state = setup()
    await ready()
    # Make the test token legitimately audience-bound to the NiFi OIDC client too.
    async with httpx.AsyncClient(verify=tls(), trust_env=False) as http:
        token = await http.post(
            f"{KEYCLOAK_URL}/realms/master/protocol/openid-connect/token",
            data={
                "grant_type": "password",
                "client_id": "admin-cli",
                "username": "fixture-control",
                "password": state["admin_password"],
            },
        )
        headers = {"Authorization": f"Bearer {token.json()['access_token']}"}
        base = f"{KEYCLOAK_URL}/admin/realms/auth-proof"
        clients = (await http.get(f"{base}/clients", headers=headers)).json()
        client = next(c for c in clients if c["clientId"] == "nifi-ui")
        path = f"{base}/clients/{client['id']}/protocol-mappers/models"
        existing = (await http.get(path, headers=headers)).json()
        if not any(m["name"] == "nifi-audience" for m in existing):
            r = await http.post(
                path,
                headers=headers,
                json={
                    "name": "nifi-audience",
                    "protocol": "openid-connect",
                    "protocolMapper": "oidc-audience-mapper",
                    "config": {
                        "included.client.audience": "nifi-ui",
                        "access.token.claim": "true",
                        "introspection.token.claim": "true",
                    },
                },
            )
            if r.status_code != 201:
                raise ProofFailure("NiFi fixture audience mapper failed")
    dns = RUNTIME / "dns"
    dns.mkdir(exist_ok=True)
    (dns / "sitecustomize.py").write_text(
        "import socket\n_original=socket.getaddrinfo\n"
        "def _lookup(host,*args,**kwargs):\n"
        "    if host in {'keycloak.auth.test',b'keycloak.auth.test'}: host='127.0.0.1'\n"
        "    return _original(host,*args,**kwargs)\n"
        "socket.getaddrinfo=_lookup\n"
    )
    env = dict(
        os.environ,
        NIFI_API_URL=NIFI_URL,
        NIFI_AUTH="oidc",
        NIFI_TRANSPORT="stdio",
        NIFI_READONLY="false",
        NIFI_CA_BUNDLE=str(RUNTIME / "ca.crt"),
        NIFI_BEARER_TOKEN="",
        NIFI_OIDC_TOKEN_URL=f"{ISSUER}/protocol/openid-connect/token",
        NIFI_OIDC_CLIENT_ID="nifi-ui",
        NIFI_OIDC_CLIENT_SECRET=state["legacy_secret"],
        NIFI_OIDC_USERNAME="writer",
        NIFI_OIDC_PASSWORD=state["writer_password"],
        NIFI_OIDC_SCOPE="openid profile email",
        PYTHONPATH=str(dns),
    )
    async with httpx.AsyncClient(verify=tls(), trust_env=False) as http:
        response = await http.post(
            f"{ISSUER}/protocol/openid-connect/token",
            data={
                "grant_type": "password",
                "client_id": "nifi-ui",
                "client_secret": state["legacy_secret"],
                "username": "writer",
                "password": state["writer_password"],
                "scope": "openid profile email",
            },
        )
        if response.status_code != 200:
            raise ProofFailure("Legacy password grant failed")
        import base64

        claims = json.loads(base64.urlsafe_b64decode(response.json()["access_token"].split(".")[1] + "=="))
        expected_subject = claims["sub"]
    params = StdioServerParameters(command=str(ROOT / "start-server.sh"), env=env)
    async with stdio_client(params) as (read, write), ClientSession(read, write) as session:
        await session.initialize()
        result = json.loads((await session.call_tool("nifi_current_user", {})).content[0].text)
        if result.get("identity") != expected_subject:
            raise ProofFailure("Legacy principal did not match the token subject")
        denied = json.loads(
            (
                await session.call_tool(
                    "nifi_create_process_group",
                    {
                        "params": {
                            "parent_id": state["user_parent"],
                            "name": "legacy-username-policy-denial",
                            "inherit_parameter_context": False,
                        }
                    },
                )
            )
            .content[0]
            .text
        )
        if denied.get("status") != "error" or "403" not in json.dumps(denied):
            raise ProofFailure("Legacy token unexpectedly acquired username-only privileges")
        print(
            "PASS legacy constraint: token authenticates as sub; username-only policy does not cover it",
            flush=True,
        )
        # Explicitly provision this separate API principal, never do that in application code.
        await provision_legacy_subject(state, expected_subject)
        created = json.loads(
            (
                await session.call_tool(
                    "nifi_create_process_group",
                    {
                        "params": {
                            "parent_id": state["legacy_parent"],
                            "name": "legacy-subject-write",
                            "inherit_parameter_context": False,
                        }
                    },
                )
            )
            .content[0]
            .text
        )
        if created.get("outcome") != "applied":
            raise ProofFailure("Explicitly authorized legacy API principal failed to write")
        print("PASS legacy oidc: real password grant, subject identity, explicit API policy and write", flush=True)
        grouped = json.loads(
            (
                await session.call_tool(
                    "nifi_create_process_group",
                    {
                        "params": {
                            "parent_id": state["group_parent"],
                            "name": "legacy-idp-group-write",
                            "inherit_parameter_context": False,
                        }
                    },
                )
            )
            .content[0]
            .text
        )
        if grouped.get("outcome") != "applied":
            raise ProofFailure("Legacy IdP-group policy write failed")
        print("PASS legacy groups: real token groups authorize the group-only policy", flush=True)


def start_mcp_fixture(state):
    import threading
    from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

    dns = RUNTIME / "dns"
    dns.mkdir(exist_ok=True)
    (dns / "sitecustomize.py").write_text(
        "import socket\n_original=socket.getaddrinfo\n"
        "def _lookup(host,*args,**kwargs):\n"
        "    if host in {'keycloak.auth.test',b'keycloak.auth.test'}: host='127.0.0.1'\n"
        "    return _original(host,*args,**kwargs)\n"
        "socket.getaddrinfo=_lookup\n"
    )
    env = dict(
        os.environ,
        NIFI_API_URL=NIFI_URL,
        NIFI_AUTH="passthrough",
        NIFI_TRANSPORT="streamable-http",
        NIFI_HOST="127.0.0.1",
        NIFI_PORT="18446",
        NIFI_READONLY="false",
        NIFI_BEARER_TOKEN="",
        NIFI_CA_BUNDLE=str(RUNTIME / "ca.crt"),
        NIFI_OAUTH_ISSUER_URL=ISSUER,
        NIFI_OAUTH_RESOURCE_URL=MCP_URL,
        NIFI_OAUTH_AUDIENCE=MCP_URL,
        NIFI_OAUTH_INTROSPECTION_URL=f"{ISSUER}/protocol/openid-connect/token/introspect",
        NIFI_OAUTH_CLIENT_ID="nifi-mcp",
        NIFI_OAUTH_CLIENT_SECRET=state["oauth_secret"],
        NIFI_OAUTH_IDENTITY_CLAIM="preferred_username",
        NIFI_OAUTH_GROUPS_CLAIM="groups",
        NIFI_OAUTH_SCOPES='["openid","profile","email"]',
        NIFI_PROXY_CERT=str(RUNTIME / "proxy.crt"),
        NIFI_PROXY_KEY=str(RUNTIME / "proxy.key"),
        PYTHONPATH=str(dns),
    )
    output = (RUNTIME / "mcp.log").open("w")
    (RUNTIME / "mcp.log").chmod(0o600)
    process = subprocess.Popen([str(ROOT / "start-server.sh")], env=env, stdout=output, stderr=output)  # noqa: S603 - exact repository launcher

    class Bridge(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def forward(self):
            body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
            headers = {
                key: value for key, value in self.headers.items() if key.lower() not in {"connection", "content-length"}
            }
            try:
                response = httpx.request(
                    self.command,
                    "http://127.0.0.1:18446" + self.path,
                    headers=headers,
                    content=body,
                    timeout=120,
                    trust_env=False,
                )
            except httpx.HTTPError:
                self.send_error(502)
                return
            self.send_response(response.status_code)
            for key, value in response.headers.items():
                if key.lower() not in {"connection", "transfer-encoding", "content-length"}:
                    self.send_header(key, value)
            self.send_header("Content-Length", str(len(response.content)))
            self.end_headers()
            self.wfile.write(response.content)

        do_GET = forward
        do_POST = forward
        do_DELETE = forward

    bridge = ThreadingHTTPServer(("127.0.0.1", 18445), Bridge)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(str(RUNTIME / "server.crt"), str(RUNTIME / "server.key"))
    bridge.socket = context.wrap_socket(bridge.socket, server_side=True)
    thread = threading.Thread(target=bridge.serve_forever, daemon=True)
    thread.start()
    return process, bridge, thread, output


class MemoryTokens:
    def __init__(self):
        from mcp.shared.auth import OAuthClientInformationFull

        self.tokens = None
        self.client = OAuthClientInformationFull(
            client_id="nifi-mcp-vscode",
            redirect_uris=["http://localhost:18447/callback"],
            token_endpoint_auth_method="none",
            scope="openid profile email",
        )

    async def get_tokens(self):
        return self.tokens

    async def set_tokens(self, tokens):
        self.tokens = tokens

    async def get_client_info(self):
        return self.client

    async def set_client_info(self, client_info):
        self.client = client_info


async def browser_login(url, role, state):
    from html.parser import HTMLParser
    from urllib.parse import parse_qs, urljoin, urlsplit

    class Form(HTMLParser):
        def __init__(self):
            super().__init__()
            self.action = None
            self.hidden = {}

        def handle_starttag(self, tag, attrs):
            attrs = dict(attrs)
            if tag == "form" and attrs.get("id") == "kc-form-login":
                self.action = attrs.get("action")
            if tag == "input" and attrs.get("type") == "hidden" and attrs.get("name"):
                self.hidden[attrs["name"]] = attrs.get("value", "")

    async with httpx.AsyncClient(verify=tls(), trust_env=False, follow_redirects=False) as http:
        response = await http.get(url)
        for _ in range(5):
            if response.status_code not in {302, 303}:
                break
            response = await http.get(urljoin(str(response.url), response.headers["location"]))
        parser = Form()
        parser.feed(response.text)
        if not parser.action:
            raise ProofFailure("Real Keycloak sign-in form missing")
        response = await http.post(
            parser.action, data={**parser.hidden, "username": role, "password": state[f"{role}_password"]}
        )
        if response.status_code not in {302, 303}:
            raise ProofFailure("Real Keycloak sign-in failed")
        query = parse_qs(urlsplit(response.headers.get("location", "")).query)
        if "code" not in query:
            raise ProofFailure("Real PKCE authorization did not return a code")
        return query["code"][0], query.get("state", [None])[0]


async def oauth_rpc(state, role):
    from mcp.client.auth import OAuthClientProvider
    from mcp.client.streamable_http import streamable_http_client
    from mcp.shared.auth import OAuthClientMetadata

    store = MemoryTokens()
    callback = None

    async def redirect(url):
        nonlocal callback
        callback = await browser_login(url, role, state)

    async def receive():
        if callback is None:
            raise ProofFailure("PKCE callback missing")
        return callback

    auth = OAuthClientProvider(
        MCP_URL,
        OAuthClientMetadata(
            redirect_uris=["http://localhost:18447/callback"],
            token_endpoint_auth_method="none",
            scope="openid profile email",
        ),
        store,
        redirect,
        receive,
    )
    async with (
        httpx.AsyncClient(
            verify=tls(),
            trust_env=False,
            auth=auth,
            timeout=120,
            headers={"X-ProxiedEntitiesChain": "<writer>", "X-ProxiedEntityGroups": "<proof-writers>"},
        ) as http,
        streamable_http_client(MCP_URL, http_client=http) as (read, write, _),
        ClientSession(read, write) as session,
    ):
        await session.initialize()
        current = json.loads((await session.call_tool("nifi_current_user", {})).content[0].text)
        if current.get("identity") != role:
            raise ProofFailure("OAuth proxy principal mismatch")
        flow = json.loads(
            (await session.call_tool("nifi_get_flow", {"params": {"process_group_id": state["group_parent"]}}))
            .content[0]
            .text
        )
        if flow.get("status") == "error":
            raise ProofFailure("OAuth IdP-group authorized read failed")
        result = json.loads(
            (
                await session.call_tool(
                    "nifi_create_process_group",
                    {
                        "params": {
                            "parent_id": state["group_parent"],
                            "name": f"oauth-{role}",
                            "inherit_parameter_context": False,
                        }
                    },
                )
            )
            .content[0]
            .text
        )
        if role == "reader":
            if (
                result.get("status") != "error"
                or result.get("outcome") != "not_applied"
                or "403" not in json.dumps(result)
            ):
                raise ProofFailure("OAuth read-only user was not refused with NiFi 403")
        elif result.get("outcome") != "applied":
            raise ProofFailure("OAuth writer mutation failed")
    print(f"PASS OAuth {role}: real PKCE, HTTPS, introspection, group policy and forged-header isolation", flush=True)
    return store.tokens.access_token


async def oauth():
    await keycloak()
    state = setup()
    await ready()
    await seed_policies(state)
    process, bridge, thread, output = start_mcp_fixture(state)
    try:
        async with httpx.AsyncClient(verify=tls(), trust_env=False) as http:
            for _ in range(30):
                response = await http.get("https://localhost:18445/.well-known/oauth-protected-resource/mcp")
                if response.status_code == 200:
                    break
                await asyncio.sleep(1)
            else:
                raise ProofFailure("MCP fixture did not start")
        reader, writer = await asyncio.gather(oauth_rpc(state, "reader"), oauth_rpc(state, "writer"))
        await oauth_negative_checks(state, reader, writer)
    finally:
        process.terminate()
        process.wait(timeout=15)
        bridge.shutdown()
        bridge.server_close()
        thread.join(timeout=5)
        output.close()


async def provision_legacy_subject(state, subject):
    async with httpx.AsyncClient(verify=certificate_tls("admin"), trust_env=False) as http:
        existing = (await api(http, "GET", "/tenants/users"))["users"]
        admin = next(u["id"] for u in existing if u["component"]["identity"] == "auth-proof-admin")
        user = next((u for u in existing if u["component"]["identity"] == subject), None)
        user = user or await api(
            http, "POST", "/tenants/users", {"revision": {"version": 0}, "component": {"identity": subject}}
        )
        if "legacy_parent" not in state:
            group = await api(
                http,
                "POST",
                "/process-groups/root/process-groups",
                {
                    "revision": {"version": 0},
                    "component": {"name": "auth-proof-legacy-policy", "position": {"x": 0, "y": 0}},
                },
            )
            state["legacy_parent"] = group["id"]
            for action in ["read", "write"]:
                await api(
                    http,
                    "POST",
                    "/policies",
                    {
                        "revision": {"version": 0},
                        "component": {
                            "resource": f"/process-groups/{group['id']}",
                            "action": action,
                            "users": [{"id": admin}, {"id": user["id"]}],
                            "userGroups": [],
                        },
                    },
                )
            # The flow endpoint also has an independent global-read prerequisite.
            policy = await api(http, "GET", "/policies/read/flow")
            component = policy["component"]
            component["users"].append({"id": user["id"]})
            await api(
                http, "PUT", f"/policies/{policy['id']}", {"revision": policy["revision"], "component": component}
            )
            save(state)


def activate_existing(state, mode):
    other = "managed" if mode == "single" else "single"
    other_name = f"nifi-mcp-auth-proof-{state['id']}-{other}"
    inspected = subprocess.run([DOCKER, "inspect", other_name, "--format", "{{.State.Running}}"], capture_output=True)  # noqa: S603 - fixture-owned argv, no shell
    if inspected.returncode == 0 and inspected.stdout.strip() == b"true":
        docker("stop", "--time", "20", other_name)
    name = f"nifi-mcp-auth-proof-{state['id']}-{mode}"
    inspected = subprocess.run([DOCKER, "inspect", name, "--format", "{{.State.Running}}"], capture_output=True)  # noqa: S603 - fixture-owned argv, no shell
    if inspected.returncode:
        return False
    if inspected.stdout.strip() != b"true":
        docker("start", name)
    return True


async def oauth_negative_checks(state, reader, writer):
    request = {"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}}
    headers = {"Accept": "application/json, text/event-stream"}
    async with httpx.AsyncClient(verify=tls(), trust_env=False) as http:
        response = await http.post(MCP_URL, headers=headers, json=request)
        if response.status_code != 401:
            raise ProofFailure("Unauthenticated HTTP request was not rejected")
        # Use an existing real token but revoke its user's sessions at the issuer.
        login = await http.post(
            f"{KEYCLOAK_URL}/realms/master/protocol/openid-connect/token",
            data={
                "grant_type": "password",
                "client_id": "admin-cli",
                "username": "fixture-control",
                "password": state["admin_password"],
            },
        )
        admin = {"Authorization": f"Bearer {login.json()['access_token']}"}
        base = f"{KEYCLOAK_URL}/admin/realms/auth-proof"
        clients = (await http.get(f"{base}/clients?clientId=nifi-other-resource", headers=admin)).json()
        if not clients:
            response = await http.post(
                f"{base}/clients",
                headers=admin,
                json={
                    "clientId": "nifi-other-resource",
                    "publicClient": False,
                    "secret": state["legacy_secret"],
                    "directAccessGrantsEnabled": True,
                    "defaultClientScopes": ["basic", "profile", "email"],
                    "protocolMappers": [
                        {
                            "name": "other-audience",
                            "protocol": "openid-connect",
                            "protocolMapper": "oidc-audience-mapper",
                            "config": {
                                "included.custom.audience": "https://localhost:18445/other-resource",
                                "access.token.claim": "true",
                            },
                        }
                    ],
                },
            )
            if response.status_code != 201:
                raise ProofFailure("Other-resource fixture client creation failed")
        response = await http.post(
            f"{ISSUER}/protocol/openid-connect/token",
            data={
                "grant_type": "password",
                "client_id": "nifi-other-resource",
                "client_secret": state["legacy_secret"],
                "username": "writer",
                "password": state["writer_password"],
                "scope": "openid profile email",
            },
        )
        wrong_token = response.json()["access_token"]
        response = await http.post(MCP_URL, headers={**headers, "Authorization": f"Bearer {wrong_token}"}, json=request)
        if response.status_code != 401:
            raise ProofFailure("A genuine foreign-audience token was accepted")
        users = (
            await http.get(f"{KEYCLOAK_URL}/admin/realms/auth-proof/users?username=reader&exact=true", headers=admin)
        ).json()
        response = await http.post(
            f"{KEYCLOAK_URL}/admin/realms/auth-proof/users/{users[0]['id']}/logout", headers=admin
        )
        if response.status_code != 204:
            raise ProofFailure("Fixture session revocation failed")
        response = await http.post(MCP_URL, headers={**headers, "Authorization": f"Bearer {reader}"}, json=request)
        if response.status_code != 401:
            raise ProofFailure("Revoked user session still reached MCP tools")
        print("PASS real issuer revocation, foreign-audience and unauthenticated HTTP denial", flush=True)


def cleanup():
    if not (RUNTIME / "state.json").exists():
        return
    state = setup()
    names = (
        docker("ps", "-a", "--filter", f"label=org.nifi-mcp.auth-proof={state['id']}", "--format", "{{.Names}}")
        .decode()
        .splitlines()
    )
    for name in names:
        docker("rm", "-f", name)
    networks = (
        docker("network", "ls", "--filter", f"label=org.nifi-mcp.auth-proof={state['id']}", "--format", "{{.Name}}")
        .decode()
        .splitlines()
    )
    for network in networks:
        docker("network", "rm", network)
    import shutil

    shutil.rmtree(RUNTIME)
    print("Fixture containers/network and temporary credentials removed", flush=True)


async def all_methods():
    try:
        await single()
        await managed()
        await legacy()
        await oauth()
    finally:
        cleanup()


if __name__ == "__main__":
    main()
