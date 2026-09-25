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


async def test_whoami_shows_workdir_commit_against_origin(make_config, cluster, tmp_path):
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
    assert repo["upstream_as_of"] is None and "may be stale" in repo["note"]   # a fresh clone: unknown, say so

    git(work, "fetch", "-q")
    assert (await tools.whoami(hub, "B:desk"))["repo"]["upstream_as_of"] is not None


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


# C's review of af9b926 (task T-20260925131826-43eed3ac), one test per point.

def _origin_and_clone(tmp_path):
    origin = tmp_path / "origin.git"
    git(tmp_path, "init", "-q", "--bare", "-b", "main", str(origin))
    work = tmp_path / "work"
    git(tmp_path, "clone", "-q", str(origin), str(work))
    (work / "f").write_text("1")
    git(work, "add", "f")
    git(work, "commit", "-qm", "one")
    git(work, "push", "-q", "-u", "origin", "main")
    return origin, work


async def test_c1_code_checkout_is_reported_even_when_the_workdir_is_no_repo(make_config, cluster, tmp_path):
    cfg = make_config("B", [interactive("desk", workdir=str(tmp_path))])      # like C:claude's /home/tb
    hub = await cluster.client(cfg)
    me = await tools.whoami(hub, "B:desk")
    assert me["repo"] is None
    assert me["code"] and me["code"]["head"]                                  # the checkout the node runs from


async def test_c2_c3_mcp_processes_count_as_sessions_not_background(make_config, cluster):
    cfg = make_config("B", [interactive("desk")])
    hub = await cluster.client(cfg)
    mcp = spawn("agentctl", "mcp", "--config", "x.yaml", "--as", "B:desk")   # e.g. an old MCP not in the ledger
    try:
        me = await tools.whoami(hub, "B:desk")
        assert mcp.pid in me["session_pids"] and me["sessions_live"] == 1
        assert mcp.pid not in [p["pid"] for p in me["background"]]
        assert hub.ledger.session_claim("B:desk", mcp.pid, "/w", session_alive) == mcp.pid   # same session
        assert (await tools.whoami(hub, "B:desk"))["sessions_live"] == 1                      # counted once
    finally:
        mcp.kill()


async def test_c4_detached_head_says_which_remote_branches_contain_it(make_config, cluster, tmp_path):
    _, work = _origin_and_clone(tmp_path)
    git(work, "checkout", "-q", "--detach")
    cfg = make_config("B", [interactive("desk", workdir=str(work))])
    hub = await cluster.client(cfg)
    repo = (await tools.whoami(hub, "B:desk"))["repo"]
    assert repo["detached"] is True and repo["upstream"] is None
    assert "origin/main" in repo["contained_in"]


async def test_c5_upstream_time_ignores_fetches_of_other_branches(make_config, cluster, tmp_path):
    import time
    origin, work = _origin_and_clone(tmp_path)
    cfg = make_config("B", [interactive("desk", workdir=str(work))])
    hub = await cluster.client(cfg)
    before = (await tools.whoami(hub, "B:desk"))["repo"]["upstream_as_of"]
    assert before is not None
    time.sleep(1.1)
    git(work, "push", "-q", "origin", "HEAD:refs/heads/other")
    git(work, "fetch", "-q", "origin", "other")                # refreshes FETCH_HEAD, not origin/main
    assert (await tools.whoami(hub, "B:desk"))["repo"]["upstream_as_of"] == before


async def test_c6_git_missing_or_hanging_does_not_break_whoami(make_config, cluster, monkeypatch):
    import subprocess as sp
    cfg = make_config("B", [interactive("desk")])
    hub = await cluster.client(cfg)
    real = sp.run

    def broken(argv, *a, **k):
        if argv and argv[0] == "git":
            raise FileNotFoundError("git")
        return real(argv, *a, **k)

    monkeypatch.setattr(sp, "run", broken)
    me = await tools.whoami(hub, "B:desk")
    assert me["repo"] is None and me["code"] is None
    monkeypatch.setattr(sp, "run", lambda argv, *a, **k: (_ for _ in ()).throw(sp.TimeoutExpired(argv, 10))
                        if argv and argv[0] == "git" else real(argv, *a, **k))
    assert (await tools.whoami(hub, "B:desk"))["repo"] is None
