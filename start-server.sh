#!/usr/bin/env bash
# NiFi MCP. Credentials belong to the caller; no automatic admin/Vault fallback.
set -euo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"
# Settings reads .env itself; exported variables take precedence.
if [[ ! -f "$DIR/.env" ]]; then
  export NIFI_READONLY="${NIFI_READONLY:-false}"
fi
export PYTHONUNBUFFERED=1
cd "$DIR"
if [[ -x "$DIR/.venv/bin/python" ]]; then
  exec "$DIR/.venv/bin/python" -u -m nifi_mcp
fi
if command -v uv >/dev/null 2>&1; then
  exec uv run --offline --frozen --no-sync --directory "$DIR" python -u -m nifi_mcp
fi
exec python3.12 -u -m nifi_mcp
