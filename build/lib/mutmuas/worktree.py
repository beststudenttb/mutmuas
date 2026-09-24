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


@dataclass
class Worktree:
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
        await git(repo, "worktree", "add", "-b", branch, str(path), base)
        return cls(repo, path, branch, base)

    async def summary(self) -> dict:
        head = await git(self.path, "rev-parse", "HEAD")
        log = await git(self.path, "log", "--format=%h %s", f"{self.base}..HEAD")
        dirty = await git(self.path, "status", "--porcelain")
        return {"branch": self.branch, "base": self.base, "head": head,
                "commits": [line for line in log.splitlines() if line],
                "uncommitted_changes": bool(dirty), "worktree": str(self.path)}

    async def write_patch(self, dest: Path) -> Path | None:
        if not await git(self.path, "log", "--format=%h", f"{self.base}..HEAD"):
            return None
        dest.write_text(await git(self.path, "format-patch", "--stdout", f"{self.base}..HEAD") + "\n")
        return dest
