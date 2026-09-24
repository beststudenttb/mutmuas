"""Git isolation for code tasks: one worktree + branch per task, results travel as commits/patches.

Agents never edit each other's working directories. For a REQUEST of kind
``code`` on an agent with ``repo:`` configured, the daemon creates

    <repo>/../worktrees/<node>-<agent>-<task_id>    on branch mm/<node>-<agent>/<task_id>

runs the agent there, and afterwards exports the new commits as a patch
artifact (``git am``-able on any machine) plus a git reference. Merging stays
a human/MERGE-permission decision; no automatic merge queue in Phase 1.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from pathlib import Path


class GitError(RuntimeError):
    pass


async def git(repo: Path, *args: str) -> str:
    proc = await asyncio.create_subprocess_exec("git", "-C", str(repo), *args, stdout=asyncio.subprocess.PIPE,
                                                stderr=asyncio.subprocess.PIPE)
    out, err = await proc.communicate()
    if proc.returncode != 0:
        raise GitError(f"git {' '.join(args)} failed: {err.decode().strip()}")
    return out.decode().strip()


# The task clone is written by a sandboxed agent, so its config is untrusted: never let git run hooks or an
# fsmonitor from it when the daemon (unsandboxed) inspects it.
SAFE = ("-c", "core.hooksPath=/dev/null", "-c", "core.fsmonitor=false")


@dataclass
class Worktree:
    """A code task's private checkout: `git clone --shared` of the repo, on branch mm/<node>-<agent>/<task>.

    The agent can commit only inside this clone (its .git lies within the task directory, so a sandbox that
    allows writing the working directory is enough). The main repository is never writable by the agent:
    its refs, hooks and config stay out of reach, and MERGE stays a separate permission. The daemon imports
    the task branch back into the repo itself, and nothing else.
    """

    repo: Path
    path: Path
    branch: str
    base: str

    @classmethod
    async def create(cls, repo: Path, node: str, agent: str, task_id: str, base_ref: str = "HEAD") -> Worktree:
        repo = repo.resolve()
        base = await git(repo, "rev-parse", base_ref)
        branch = f"mm/{node}-{agent}/{task_id}"
        path = repo.parent / "worktrees" / f"{node}-{agent}-{task_id}"
        if path.exists():                      # resumed after a crash: keep the work done so far
            return cls(repo, path, branch, base)
        path.parent.mkdir(parents=True, exist_ok=True)
        await git(repo.parent, "clone", "--quiet", "--shared", "--no-checkout", str(repo), str(path))
        await git(path, *SAFE, "checkout", "--quiet", "-b", branch, base)
        return cls(repo, path, branch, base)

    async def summary(self) -> dict:
        head = await git(self.path, *SAFE, "rev-parse", "HEAD")
        log = await git(self.path, *SAFE, "log", "--format=%h %s", f"{self.base}..HEAD")
        dirty = await git(self.path, *SAFE, "status", "--porcelain")
        return {"branch": self.branch, "base": self.base, "head": head,
                "commits": [line for line in log.splitlines() if line],
                "uncommitted_changes": bool(dirty), "worktree": str(self.path)}

    async def write_patch(self, dest: Path) -> Path | None:
        if not await git(self.path, *SAFE, "log", "--format=%h", f"{self.base}..HEAD"):
            return None
        dest.write_text(await git(self.path, *SAFE, "format-patch", "--stdout", f"{self.base}..HEAD") + "\n")
        return dest

    async def import_branch(self) -> None:
        """Bring the task branch (and only it) into the main repo, so reviewers can `git show` it there."""
        ref = f"refs/heads/{self.branch}"
        await git(self.repo, *SAFE, "fetch", "--quiet", "--no-tags", str(self.path), f"+{ref}:{ref}")
