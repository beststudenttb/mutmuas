#!/usr/bin/env bash
# Install mutmuas on this machine (macOS or Linux), entirely inside the repo:
#   .venv/            Python environment with agentctl + agent-node
#   .local/bin/       nats-server binary (only needed on the machine that hosts the server, and for tests)
# Nothing is written outside the repository. Add .venv/bin to PATH yourself if you want.
set -euo pipefail
cd "$(dirname "$0")/.."
ROOT="$(pwd)"
NATS_VERSION="${NATS_VERSION:-v2.15.0}"
PYTHON="${PYTHON:-python3}"

"$PYTHON" -c 'import sys; assert sys.version_info >= (3, 10), "Python >= 3.10 required"'
[ -d .venv ] || "$PYTHON" -m venv .venv
.venv/bin/pip install -q --upgrade pip
.venv/bin/pip install -q -e ".[test]"
echo "installed: $ROOT/.venv/bin/agentctl, $ROOT/.venv/bin/agent-node"

if [ "${SKIP_NATS:-0}" != "1" ] && [ ! -x .local/bin/nats-server ]; then
  case "$(uname -s)" in Darwin) os=darwin ;; Linux) os=linux ;; *) echo "unsupported OS"; exit 1 ;; esac
  case "$(uname -m)" in arm64|aarch64) arch=arm64 ;; x86_64|amd64) arch=amd64 ;; *) echo "unsupported arch"; exit 1 ;; esac
  name="nats-server-${NATS_VERSION}-${os}-${arch}"
  mkdir -p .local/bin
  curl -fsSL "https://github.com/nats-io/nats-server/releases/download/${NATS_VERSION}/${name}.tar.gz" \
    | tar xz -C .local
  mv ".local/${name}/nats-server" .local/bin/ && rm -rf ".local/${name}"
  echo "installed: $ROOT/.local/bin/nats-server ($(.local/bin/nats-server --version))"
fi

cat <<MSG

next:
  export PATH="$ROOT/.venv/bin:\$PATH"
  agent-node init --project <project> --node <A|B|...> --server nats://<server>:4222 --credentials <NODE>.env
  agent-node doctor && agent-node start
see docs/DEPLOYMENT.md
MSG
