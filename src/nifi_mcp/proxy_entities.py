"""NiFi 2.10 StandardProxiedEntityEncoder-compatible HTTP header encoding."""

import base64
import unicodedata


def safe_entity(value: object) -> bool:
    """Require a named entity; reject controls and ambiguous backslash escaping."""
    return (
        isinstance(value, str)
        and bool(value.strip())
        and all(unicodedata.category(char) not in {"Cc", "Cs"} and char != "\\" for char in value)
    )


def encode_entity(identity: str) -> str:
    """Escape delimiters and double-wrap UTF-8 Base64 exactly as NiFi does."""
    if not safe_entity(identity):
        raise ValueError("Unsafe proxy identity or group")
    escaped = identity.replace("<", "\\<").replace(">", "\\>")
    if not escaped.isascii():
        escaped = f"<{base64.b64encode(escaped.encode('utf-8')).decode('ascii')}>"
    return f"<{escaped}>"
