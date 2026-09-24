"""Code tasks run in their own git worktree; results come back as a branch ref + a patch any node can apply."""

import subprocess
from pathlib import Path

from conftest import interactive, worker

from mutmuas import tools


def sh(cwd, *cmd):
    return subprocess.run(cmd, cwd=cwd, check=True, capture_output=True, text=True).stdout.strip()


def make_repo(path):
    path.mkdir(parents=True)
    sh(path, "git", "init", "-q", "-b", "main")
    (path / "model.py").write_text("LR = 1e-3\n")
    sh(path, "git", "add", ".")
    sh(path, "git", "-c", "user.name=t", "-c", "user.email=t@example.invalid", "commit", "-qm", "init")
    return path


async def test_code_task_isolated_worktree_and_patch(make_config, cluster, tmp_path):
    repo_b = make_repo(tmp_path / "B-disk" / "visual_rl")
    repo_a = tmp_path / "A-disk" / "visual_rl"
    sh(tmp_path, "git", "clone", "-q", str(repo_b), str(repo_a))       # A has its own clone

    a = make_config("A", [interactive("main")])
    b = make_config("B", [worker("coder", "lab.py", repo=str(repo_b),
                                 permissions=["READ", "WRITE_WORKTREE", "PUBLISH_ARTIFACT", "REQUEST_TASK"])])
    await cluster.start(a)
    await cluster.start(b)
    hub = await cluster.client(a)
    sent = await tools.send_request(hub, "A:main", "B:coder", "lower the learning rate", "A's runs diverge",
                                    kind="code", inputs={"action": "commit", "file": "model.py",
                                                         "content": "LR = 3e-4\n", "message": "lower LR"})
    result = await tools.wait_for_result(hub, sent["task_id"], 30)
    assert result["result_status"] == "complete", result
    git = result["result"]["outputs"]["git"]
    assert git["branch"].startswith("mm/B-coder/") and len(git["commits"]) == 1

    # B's main checkout was never touched; the change lives on the task branch.
    assert (repo_b / "model.py").read_text() == "LR = 1e-3\n"
    assert sh(repo_b, "git", "show", f"{git['branch']}:model.py") == "LR = 3e-4"

    refs = {r["id"]: r for r in result["output_refs"]}
    assert refs["BRANCH"]["uri"].startswith("git://B")
    patch = await tools.fetch_artifact(hub, refs["PATCH"]["uri"], str(tmp_path / "A-dl"))
    sh(repo_a, "git", "-c", "user.name=a", "-c", "user.email=a@example.invalid", "am", "-q", patch["path"])
    assert (repo_a / "model.py").read_text() == "LR = 3e-4\n"


async def test_code_task_clone_is_isolated_from_the_main_repo(make_config, cluster, tmp_path):
    """The task clone's own git dir holds the commit; the daemon never runs its hooks/fsmonitor; main is untouched.

    What keeps a *sandboxed* agent away from <repo>/.git is the sandbox (writes limited to the task dir, and the
    clone's .git is inside it); a script worker has no sandbox, so that part is not something this test can show.
    """
    repo = make_repo(tmp_path / "B-disk" / "proj")
    main_before = sh(repo, "git", "rev-parse", "main")
    a = make_config("A", [interactive("main")])
    b = make_config("B", [worker("coder", "lab.py", repo=str(repo),
                                 permissions=["READ", "WRITE_WORKTREE", "PUBLISH_ARTIFACT", "REQUEST_TASK"])])
    await cluster.start(a)
    await cluster.start(b)
    hub = await cluster.client(a)
    marker = tmp_path / "pwned"
    sent = await tools.send_request(hub, "A:main", "B:coder", "change", "isolation test", kind="code",
                                    inputs={"action": "evil_commit", "file": "model.py", "content": "LR = 1\n",
                                            "marker": str(marker)})
    result = await tools.wait_for_result(hub, sent["task_id"], 30)
    git = result["result"]["outputs"]["git"]
    assert len(git["commits"]) == 1
    clone = Path(git["worktree"])
    assert (clone / ".git").is_dir()                                # a real clone, not a worktree pointer
    assert sh(repo, "git", "rev-parse", "main") == main_before       # importing the task branch left main alone
    assert sh(repo, "git", "show", f"{git['branch']}:model.py") == "LR = 1"   # task branch imported
    assert not Path(f"{marker}.fsmonitor").exists(), "daemon ran the clone's fsmonitor"
    assert not Path(f"{marker}.hook").exists(), "a hook ran"
