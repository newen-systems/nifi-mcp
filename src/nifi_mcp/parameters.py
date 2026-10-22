"""Parameter ownership: root.service.key[.key] lives in the root context."""
from __future__ import annotations

import re
from typing import Any

from nifi_mcp.errors import NiFiError

_NAME = re.compile(r"[a-z][a-z0-9]*(?:-[a-z0-9]+)*(?:\.[a-z][a-z0-9]*(?:-[a-z0-9]+)*){2,}\Z")


def validate_parameter_ownership(
    context_name: str, parameters: list[dict[str, Any]] | None, *, existing: set[str] | None = None
) -> None:
    """Keep legacy edits possible, but never introduce ambiguous new names or ownership."""
    seen: set[str] = set()
    for index, parameter in enumerate(parameters or []):
        name = parameter["name"]
        if name in seen:
            raise NiFiError(f"parameters[{index}].name duplicates another entry; nothing was sent.")
        seen.add(name)
        if name in (existing or set()):
            continue
        if not _NAME.fullmatch(name):
            raise NiFiError(
                f"parameters[{index}].name must use lowercase root.service.key segments separated by dots; "
                "each segment starts with a letter and may contain digits or hyphen-separated words."
            )
        if name.split(".", 1)[0] != context_name:
            raise NiFiError(
                f"parameters[{index}].name must start with the owning context name followed by a dot. "
                "Store each root in its matching context; compose roots using inherited_context_ids."
            )
