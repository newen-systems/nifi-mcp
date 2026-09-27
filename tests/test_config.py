import ssl
from pathlib import Path

import certifi

from nifi_mcp.config import Settings, normalize_api_url

_CA_BUNDLE = Path(certifi.where())


def test_normalize_api_url_appends_suffix() -> None:
    assert normalize_api_url("https://nifi.example.test") == "https://nifi.example.test/nifi-api"
    assert normalize_api_url("https://nifi.example.test/nifi") == "https://nifi.example.test/nifi-api"
    assert normalize_api_url("https://nifi.example.test/nifi-api/") == "https://nifi.example.test/nifi-api"


def test_readonly_defaults_false() -> None:
    cfg = Settings(api_url="https://nifi.example.test/nifi-api", auth="bearer", bearer_token="x")
    assert cfg.readonly is False


def test_tls_verify_without_bundle_is_true() -> None:
    cfg = Settings(
        api_url="https://nifi.example.test/nifi-api",
        auth="bearer",
        bearer_token="x",
        tls_verify=True,
    )
    assert cfg.tls_verify_value() is True


def test_tls_verify_with_bundle_is_ssl_context() -> None:
    cfg = Settings(
        api_url="https://nifi.example.test/nifi-api",
        auth="bearer",
        bearer_token="x",
        ca_bundle=str(_CA_BUNDLE),
        tls_verify=True,
    )
    assert isinstance(cfg.tls_verify_value(), ssl.SSLContext)


def test_every_doc_states_the_readonly_default_the_setting_has() -> None:
    # A doc said NIFI_READONLY defaults true; the setting and the wrapper say false.
    import re

    root = Path(__file__).resolve().parent.parent
    default = str(Settings.model_fields["readonly"].default).lower()
    assert f'NIFI_READONLY="${{NIFI_READONLY:-{default}}}"' in (root / "start-server.sh").read_text()
    for doc in [root / "README.md"]:
        for said in re.findall(r"NIFI_READONLY`? defaults? (true|false)", doc.read_text()):
            assert said == default, f"{doc.name} says NIFI_READONLY defaults {said}"
    assert f"`NIFI_READONLY` defaults {default}" in (root / "README.md").read_text()
