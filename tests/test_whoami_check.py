"""whoami lists the facts a new session checks its handoff against (weekend r3b ruling, P2):
sessions using this address, the workdir's commit against origin with the time of the last fetch, and this
address's own agentctl background processes. It cannot catch relational errors (what was never sent)."""

from __future__ import annotations

import os
import subprocess
import sys

from conftest import interactive
from mutmuas import tools
from mutmuas.node import session_alive


def git(cwd, *args):
    return subprocess.run(["git", "-c", "user.name=t", "-c", "user.email=t@example.invalid", *args], cwd=cwd,
                          check=True, capture_output=True, text=True).stdout.strip()


def spawn(*argv: str) -> subprocess.Popen:
    """A long-lived process whose command line looks like the given argv (python sleeping, argv as decoration)."""
    return subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)", *argv])


async def test_whoami_counts_sessions_using_the_address(make_config, cluster):
    cfg = make_config("B", [interactive("desk")])
    hub = await cluster.client(cfg)
    other = spawn("claude")
    try:
        assert hub.ledger.session_claim("B:desk", os.getpid(), "/w", session_alive) == os.getpid()      # we hold the lease
        assert (await tools.whoami(hub, "B:desk"))["sessions_live"] == 1
        assert hub.ledger.session_claim("B:desk", other.pid, "/elsewhere", session_alive) == os.getpid()  # a second session
        me = await tools.whoami(hub, "B:desk")
        assert me["sessions_live"] == 2 and other.pid in me["session_contenders"]
        assert "sessions" in me.get("warning", "")
    finally:
        other.kill()


async def test_whoami_shows_workdir_commit_against_origin_and_fetch_time(make_config, cluster, tmp_path):
    origin = tmp_path / "origin.git"
    git(tmp_path, "init", "-q", "--bare", "-b", "main", str(origin))
    seed = tmp_path / "seed"
    git(tmp_path, "clone", "-q", str(origin), str(seed))
    (seed / "f").write_text("1")
    git(seed, "add", "f")
    git(seed, "commit", "-qm", "one")
    git(seed, "push", "-q", "origin", "main")
    work = tmp_path / "work"
    git(tmp_path, "clone", "-q", str(origin), str(work))
    (work / "g").write_text("2")
    git(work, "add", "g")
    git(work, "commit", "-qm", "local")                      # one commit not on origin
    cfg = make_config("B", [interactive("desk", workdir=str(work))])
    hub = await cluster.client(cfg)

    repo = (await tools.whoami(hub, "B:desk"))["repo"]
    assert repo["branch"] == "main" and repo["upstream"] == "origin/main"
    assert repo["ahead"] == 1 and repo["behind"] == 0
    assert repo["fetched_at"] is None and "never fetched" in repo["note"]   # counts may be stale: say so

    git(work, "fetch", "-q")
    assert (await tools.whoami(hub, "B:desk"))["repo"]["fetched_at"] is not None


async def test_whoami_lists_own_agentctl_background_processes(make_config, cluster):
    cfg = make_config("B", [interactive("desk"), interactive("other")])
    hub = await cluster.client(cfg)
    mine = spawn("agentctl", "inbox", "--wait", "3600", "--as", "B:desk")
    theirs = spawn("agentctl", "inbox", "--wait", "3600", "--as", "B:other")
    try:
        procs = (await tools.whoami(hub, "B:desk"))["background"]
        pids = [p["pid"] for p in procs]
        assert mine.pid in pids and theirs.pid not in pids
        assert all("command" in p and "elapsed" in p for p in procs)
    finally:
        mine.kill()
        theirs.kill()
