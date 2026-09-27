#!/usr/bin/env bash
# stdio MCP for Apache NiFi 2.x. Settings come from the environment or .env; secrets are never printed.
set -euo pipefail
DIR="$(cd "$(dirname "$0")" && pwd)"
ENV_FILE="$DIR/.env"

if [[ -f "$ENV_FILE" ]]; then
  set -a
  # shellcheck disable=SC1091
  . "$ENV_FILE"
  set +a
fi

export NIFI_READONLY="${NIFI_READONLY:-false}"
export NIFI_AUTH="${NIFI_AUTH:-oidc}"

if [[ -z "${NIFI_API_URL:-}" ]]; then
  echo "NIFI_API_URL is required" >&2
  exit 1
fi
if [[ "${NIFI_API_URL}" != https://* ]]; then
  echo "NIFI_API_URL must be https://" >&2
  exit 1
fi

cd "$DIR"
# MCP stdio is JSON-RPC on stdout. Piped Python is block-buffered, so a client's
# initialize waits forever unless stdout is unbuffered.
export PYTHONUNBUFFERED=1
if [[ -x "$DIR/.venv/bin/python" ]]; then
  exec "$DIR/.venv/bin/python" -u -m nifi_mcp
fi
if command -v uv >/dev/null 2>&1; then
  exec uv run --directory "$DIR" python -u -m nifi_mcp
fi
exec python3.12 -u -m nifi_mcp
