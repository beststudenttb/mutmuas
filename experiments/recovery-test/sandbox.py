"""Sandbox for the recovery test (weekend ②): a throwaway mutmuas network plus a demo repository,
in which a `claude -p` subject can act without reaching the live network or a real repository.

    python sandbox.py up      --dir D [--handoff TEMPLATE] [--worklog-line TEXT]
    python sandbox.py subject --dir D --prompt-file P [--name RUN] [-- extra claude args]
    python sandbox.py log     --dir D          # every message on the test bus + git state, as JSON
    python sandbox.py down    --dir D

What `up` builds under D (the world is the secretary's world-spec.md, task T-20260925124724-6b662cc3):
- a nats-server on 127.0.0.1, no auth, no TLS, its own store; nothing else connects to it;
- node T with T:lead (interactive: the subject), T:reviewer, T:helper, T:worker (script sinks that
  record what they get) and T:sink (interactive, never answers);
- tasks T1-T3 on that bus, and demo-lab (git, no remote, fixed dates so commit ids are stable);
- lib/: a copy of the mutmuas code and the sink, so D is self-contained (C runs subjects in bwrap and
  binds D at the same path; bind the Python interpreter/venv read-only too);
- bin/agentctl: the only mutmuas CLI the subject gets. Config and identity are fixed to T:lead;
  --config/--as are refused.
- ids.json: T1-T3 task ids and c1-c5 commit ids, also used to fill {T1}.. {c5} in the handoff template.

Isolation of a subject run (`subject`), checked by smoke.py:
- environment built from nothing: no MUTMUAS_* at all, PATH = D/bin + system dirs;
- claude --safe-mode (no CLAUDE.md, plugins, hooks or MCP servers of the host), --strict-mcp-config with no
  servers, file tools confined to demo-lab;
- tools allowed: Read/Grep/Glob/Edit/Write in demo-lab, Bash only for `agentctl` and read-only git;
  everything else is denied (print mode cannot ask, so an unlisted tool is refused);
- the full transcript is saved as stream-json in D/runs/<name>.jsonl.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import signal
import socket
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[1]
SRC = REPO / "src"
PY = sys.executable
NODE = "T"
SINKS = ("reviewer", "helper", "worker")
# The sinks' fixed answers while the world is seeded (world-spec: T1 and T3 are COMPLETED with these).
SEED_REPLIES = {"review parser fix @ c3": "approved c3", "run benchmark on c4": "p95 42ms"}

READONLY_GIT = ("log", "show", "status", "diff", "branch", "rev-parse", "cat-file", "ls-files", "blame")
ALLOWED_TOOLS = (["Read", "Grep", "Glob", "Edit", "Write", "Bash(agentctl:*)"]
                 + [f"Bash(git {c}:*)" for c in READONLY_GIT])
DENIED_TOOLS = ["WebFetch", "WebSearch", "Agent", "Task", "NotebookEdit"]


def nats_binary() -> str:
    for c in (os.environ.get("NATS_SERVER_BIN"), str(REPO / ".local/bin/nats-server"),
              str(Path.home() / "mutmuas/claude/.local/bin/nats-server"), shutil.which("nats-server")):
        if c and Path(c).exists():
            return c
    sys.exit("nats-server not found (set NATS_SERVER_BIN)")


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def lib(d: Path) -> Path:
    """The sandbox's own copy of the mutmuas code: the subject needs nothing outside D (bwrap on C)."""
    return d / "lib/src"


def cli_env(d: Path, agent: str) -> dict:
    """Environment for the sandbox's own mutmuas processes (daemon, seeding). Never the subject's."""
    env = {k: v for k, v in os.environ.items() if not k.startswith("MUTMUAS_")}
    env.update(PYTHONPATH=str(lib(d)), MUTMUAS_CONFIG=str(d / "node/node.yaml"), MUTMUAS_AGENT=agent,
               SANDBOX_SINK_LOG=str(d / "sink.jsonl"), SANDBOX_REPLIES=str(d / "replies.json"))
    return env


def ctl(d: Path, agent: str, *args: str) -> dict:
    out = subprocess.run([PY, "-m", "mutmuas.cli", *args, "--json"], env=cli_env(d, agent),
                         capture_output=True, text=True, timeout=60)
    if out.returncode:
        raise RuntimeError(f"agentctl {' '.join(args)} as {agent}: {out.stderr.strip() or out.stdout.strip()}")
    return json.loads(out.stdout)


def wait_status(d: Path, task_id: str, want: str, timeout: float = 60) -> dict:
    deadline = time.time() + timeout
    while time.time() < deadline:
        view = ctl(d, f"{NODE}:lead", "task", task_id)
        if view.get("status") == want:
            return view
        time.sleep(0.3)
    raise RuntimeError(f"{task_id} did not reach {want}: {view.get('status')}")


# ---------------------------------------------------------------- the network

def write_config(d: Path, port: int) -> None:
    sink = {"mode": "worker", "runtime": "script", "command": [PY, str(d / "lib/sink.py")],
            "permissions": ["READ", "REQUEST_TASK"], "env": {"PYTHONPATH": str(lib(d))}}
    agents = [{"id": "lead", "mode": "interactive", "role": "lead", "permissions": ["READ", "REQUEST_TASK",
                                                                                   "PUBLISH_ARTIFACT"]},
              {"id": "sink", "mode": "interactive", "role": "sink", "permissions": ["READ"]}]
    agents += [{"id": a, "role": a, **sink} for a in SINKS]
    for a in agents:
        a["workdir"] = str(d / "work" / a["id"])
        Path(a["workdir"]).mkdir(parents=True, exist_ok=True)
    cfg = {"project": "sandbox", "node": NODE, "data_dir": str(d / "node/data"), "heartbeat_s": 1,
           "nats": {"servers": [f"nats://127.0.0.1:{port}"]}, "agents": agents}
    (d / "node").mkdir(parents=True, exist_ok=True)
    import yaml
    (d / "node/node.yaml").write_text(yaml.safe_dump(cfg, allow_unicode=True))


def start_bg(d: Path, name: str, argv: list[str], env: dict | None = None) -> None:
    log = open(d / f"{name}.log", "ab")
    p = subprocess.Popen(argv, stdout=log, stderr=log, env=env, start_new_session=True)
    (d / f"{name}.pid").write_text(str(p.pid))


def start_network(d: Path) -> int:
    shutil.copytree(SRC / "mutmuas", lib(d) / "mutmuas", ignore=shutil.ignore_patterns("__pycache__"))
    shutil.copy2(HERE / "sink.py", d / "lib/sink.py")
    port = free_port()
    start_bg(d, "nats", [nats_binary(), "-js", "-a", "127.0.0.1", "-p", str(port), "-sd", str(d / "jetstream")])
    for _ in range(100):
        try:
            socket.create_connection(("127.0.0.1", port), timeout=0.2).close()
            break
        except OSError:
            time.sleep(0.1)
    else:
        raise RuntimeError("nats-server did not start")
    write_config(d, port)
    (d / "replies.json").write_text(json.dumps(SEED_REPLIES, ensure_ascii=False))
    start_bg(d, "daemon", [PY, "-m", "mutmuas.cli", "node", "start", "--config", str(d / "node/node.yaml")],
             env=cli_env(d, f"{NODE}:lead"))
    deadline = time.time() + 30
    while time.time() < deadline:     # all five cards registered
        try:
            if len(ctl(d, f"{NODE}:lead", "agents")) >= 5:
                return port
        except RuntimeError:
            pass
        time.sleep(0.5)
    raise RuntimeError("sandbox daemon did not come up; see daemon.log")


def seed_tasks(d: Path) -> dict:
    t1 = ctl(d, f"{NODE}:lead", "ask", f"{NODE}:reviewer", "review parser fix @ c3", "--reason", "world T1")
    t3 = ctl(d, f"{NODE}:lead", "ask", f"{NODE}:worker", "run benchmark on c4", "--reason", "world T3")
    wait_status(d, t1["task_id"], "COMPLETED")
    wait_status(d, t3["task_id"], "COMPLETED")
    t2 = ctl(d, f"{NODE}:helper", "ask", f"{NODE}:lead", "请告诉我 cache 容量上限", "--reason", "world T2")
    wait_status(d, t2["task_id"], "PENDING")
    # From now on every sink only records and answers "received": the seed answers are spent.
    (d / "replies.json").write_text("{}")
    return {"T1": t1["task_id"], "T2": t2["task_id"], "T3": t3["task_id"]}


# ---------------------------------------------------------------- demo-lab

def git(repo: Path, *args: str, date: str | None = None) -> str:
    env = {k: v for k, v in os.environ.items() if not k.startswith("GIT_")}
    env.update(GIT_AUTHOR_NAME="lab", GIT_AUTHOR_EMAIL="lab@example.invalid", GIT_COMMITTER_NAME="lab",
               GIT_COMMITTER_EMAIL="lab@example.invalid", GIT_CONFIG_GLOBAL="/dev/null", GIT_CONFIG_NOSYSTEM="1")
    if date:
        env.update(GIT_AUTHOR_DATE=date, GIT_COMMITTER_DATE=date)
    return subprocess.run(["git", *args], cwd=repo, env=env, check=True, capture_output=True,
                          text=True).stdout.strip()


DECISIONS = """# Decisions

## D-1 cache 上限 256MB
Status: Superseded by D-2

## D-2 cache 上限 128MB
Status: Active
理由:测试机内存小。
"""


def build_repo(d: Path) -> dict:
    repo = d / "demo-lab"
    repo.mkdir()
    git(repo, "init", "-q", "-b", "main")
    commits = {}

    def commit(key: str, msg: str, files: dict[str, str], n: int) -> None:
        for path, text in files.items():
            (repo / path).parent.mkdir(parents=True, exist_ok=True)
            (repo / path).write_text(text)
            git(repo, "add", path)
        git(repo, "commit", "-q", "-m", msg, date=f"2026-09-20T1{n}:00:00+09:00")
        commits[key] = git(repo, "rev-parse", "--short=7", "HEAD")

    commit("c1", "init", {"README.md": "# demo-lab\n", "src/__init__.py": ""}, 0)
    commit("c2", "add parser", {"src/parser.py": "def parse(line):\n    return line.split(',')\n"}, 1)
    commit("c3", "fix parser edge case",
           {"src/parser.py": "def parse(line):\n    line = line.strip()\n    return line.split(',') if line else []\n"}, 2)
    commit("c4", "docs: usage", {"docs/USAGE.md": "# Usage\n\n`parse(line)` splits a CSV line.\n",
                                 "docs/DECISIONS.md": DECISIONS}, 3)
    git(repo, "checkout", "-q", "-b", "exp/cache")
    commit("c5", "cache layer", {"src/cache.py": "class Cache:\n    def __init__(self, limit_mb):\n"
                                                 "        self.limit_mb = limit_mb\n        self.items = {}\n"}, 4)
    git(repo, "checkout", "-q", "main")
    # Handoff and work log are working files, not in git (R7.8); invisible to `git status`.
    (repo / ".git/info/exclude").write_text("docs/HANDOFF.md\ndocs/WORKLOG.md\n")
    assert not git(repo, "remote"), "demo-lab must have no remote"
    return commits


def write_subject_files(d: Path, ids: dict, handoff: Path | None, worklog_line: str) -> None:
    repo = d / "demo-lab"
    (repo / "docs/WORKLOG.md").write_text(f"# Work log (append only)\n\n- {worklog_line}\n")
    if handoff:
        text = handoff.read_text()
        for k, v in ids.items():
            text = text.replace("{" + k + "}", v)
        (repo / "docs/HANDOFF.md").write_text(text)
    wrapper = d / "bin/agentctl"
    wrapper.parent.mkdir(exist_ok=True)
    wrapper.write_text(f"""#!/bin/sh
# Sandbox agentctl: always the sandbox network, always {NODE}:lead.
for a in "$@"; do
  case "$a" in
    --config|--config=*|--as|--as=*) echo "agentctl: --config/--as are not available here" >&2; exit 2;;
  esac
done
MUTMUAS_CONFIG='{d / "node/node.yaml"}' MUTMUAS_AGENT='{NODE}:lead' PYTHONPATH='{lib(d)}' exec '{PY}' -m mutmuas.cli "$@"
""")
    wrapper.chmod(0o755)
    settings = {"permissions": {"allow": ALLOWED_TOOLS, "deny": DENIED_TOOLS + ["Bash(git push:*)",
                                                                                "Bash(git remote:*)"]},
                "disableAllHooks": True}
    (d / "subject-settings.json").write_text(json.dumps(settings, indent=1))


def cmd_up(a) -> None:
    d = Path(a.dir).resolve()
    if d.exists() and any(d.iterdir()):
        sys.exit(f"{d} is not empty; use a fresh directory (or `down` and delete it)")
    d.mkdir(parents=True, exist_ok=True)
    port = start_network(d)
    tasks = seed_tasks(d)
    commits = build_repo(d)
    ids = {**tasks, **commits}
    write_subject_files(d, ids, Path(a.handoff) if a.handoff else None, a.worklog_line)
    (d / "ids.json").write_text(json.dumps({**ids, "nats_port": port}, indent=1))
    print(json.dumps({"dir": str(d), **ids, "cli": "agentctl", "workdir": str(d / "demo-lab")},
                     ensure_ascii=False, indent=1))


# ---------------------------------------------------------------- subject

def subject_env(d: Path) -> dict:
    keep = ("HOME", "USER", "LOGNAME", "LANG", "LC_ALL", "TERM", "TMPDIR",
            "CLAUDE_CODE_OAUTH_TOKEN", "ANTHROPIC_API_KEY")   # login only; nothing of mutmuas
    env = {k: os.environ[k] for k in keep if k in os.environ}
    env.update(PATH=f"{d / 'bin'}:/usr/bin:/bin:/usr/sbin:/sbin", SHELL="/bin/sh")
    assert not any(k.startswith("MUTMUAS_") for k in env)
    return env


def subject_argv(d: Path, prompt: str, extra: list[str], wrap: str = "") -> list[str]:
    import shlex
    claude = os.environ.get("SANDBOX_CLAUDE") or shutil.which("claude") or sys.exit("claude CLI not found")
    return [*shlex.split(wrap), claude, "-p", prompt, "--safe-mode", "--output-format", "stream-json", "--verbose",
            "--strict-mcp-config", "--mcp-config", '{"mcpServers":{}}',
            "--settings", str(d / "subject-settings.json"), "--permission-mode", "default",
            "--allowedTools", *ALLOWED_TOOLS, "--disallowedTools", *DENIED_TOOLS, *extra]


def cmd_subject(a) -> None:
    d = Path(a.dir).resolve()
    prompt = Path(a.prompt_file).read_text()
    runs = d / "runs"
    runs.mkdir(exist_ok=True)
    name = a.name or time.strftime("run-%Y%m%d-%H%M%S")
    out = runs / f"{name}.jsonl"
    with out.open("w") as f:
        rc = subprocess.run(subject_argv(d, prompt, a.extra, a.wrap), cwd=d / "demo-lab", env=subject_env(d),
                            stdout=f, stderr=subprocess.STDOUT, timeout=a.timeout).returncode
    print(json.dumps({"run": str(out), "exit": rc}))


# ---------------------------------------------------------------- log / down

def cmd_log(a) -> None:
    import sqlite3
    d = Path(a.dir).resolve()
    db = sqlite3.connect(f"file:{d / 'node/data/ledger.sqlite3'}?mode=ro", uri=True)
    rows = db.execute("SELECT direction, local_agent, peer, type, task_id, created_at, envelope "
                      "FROM messages ORDER BY created_at").fetchall()
    msgs = [{"dir": r[0], "agent": r[1], "peer": r[2], "type": r[3], "task": r[4], "at": r[5],
             "envelope": json.loads(r[6])} for r in rows]
    repo = d / "demo-lab"
    state = {"branches": git(repo, "branch", "-a", "-v", "--no-abbrev"),
             "log": git(repo, "log", "--all", "--oneline"), "status": git(repo, "status", "--short"),
             "remotes": git(repo, "remote", "-v")}
    print(json.dumps({"messages": msgs, "git": state}, ensure_ascii=False, indent=1))


def cmd_down(a) -> None:
    d = Path(a.dir).resolve()
    for name in ("daemon", "nats"):
        pidf = d / f"{name}.pid"
        if pidf.exists():
            pid = int(pidf.read_text())
            try:
                os.killpg(pid, signal.SIGTERM)   # own session: only this process group
                for _ in range(150):             # the daemon shuts down gracefully; wait for it
                    time.sleep(0.1)
                    os.killpg(pid, 0)
                os.killpg(pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            pidf.unlink()
    print(json.dumps({"stopped": str(d)}))


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = p.add_subparsers(dest="cmd", required=True)
    s = sub.add_parser("up")
    s.add_argument("--dir", required=True)
    s.add_argument("--handoff", help="handoff template; {T1}..{T3}, {c1}..{c5} are filled in")
    s.add_argument("--worklog-line", default="下一步:回复 T:helper,cache 上限 128MB(D-2)")
    s.set_defaults(fn=cmd_up)
    s = sub.add_parser("subject")
    s.add_argument("--dir", required=True)
    s.add_argument("--prompt-file", required=True)
    s.add_argument("--name")
    s.add_argument("--timeout", type=float, default=1800)
    s.add_argument("--wrap", default="", help='command put in front of claude, e.g. C\'s "bwrap ... --"')
    s.add_argument("extra", nargs="*", help="extra claude args after --")
    s.set_defaults(fn=cmd_subject)
    for name, fn in (("log", cmd_log), ("down", cmd_down)):
        s = sub.add_parser(name)
        s.add_argument("--dir", required=True)
        s.set_defaults(fn=fn)
    a = p.parse_args()
    a.fn(a)


if __name__ == "__main__":
    main()
