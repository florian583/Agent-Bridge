#!/bin/sh
set -eu
if [ -n "${BRIDGE_AVAILABILITY_CLI:-}" ]; then
  cli=$BRIDGE_AVAILABILITY_CLI
else
  cli="$HOME/.claude-main/skills/bridge-live/bridge-availability.mjs"
fi
exec node "$cli" "$@"
