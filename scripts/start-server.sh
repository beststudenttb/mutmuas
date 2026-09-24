#!/usr/bin/env bash
# Run a NATS server from this repo in the foreground.
#   scripts/start-server.sh                 dev mode: no auth, localhost only, data in .local/jetstream
#   scripts/start-server.sh server/nats-server.conf   production config from `agent-node server-config`
set -euo pipefail
cd "$(dirname "$0")/.."
NATS="${NATS_SERVER_BIN:-.local/bin/nats-server}"
if [ $# -ge 1 ]; then
  exec "$NATS" -c "$1"
fi
echo "dev server on nats://127.0.0.1:4222 (no auth — do not expose)"
exec "$NATS" -js -a 127.0.0.1 -p 4222 -sd .local/jetstream
