"""scripts/mergecheck.py: a merge is allowed only for exactly the commit a completed review approved."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest

SCRIPT = Path(__file__).resolve().parents[1] / "scripts/mergecheck.py"


def git(repo: Path, *args: str) -> str:
    return subprocess.run(["git", "-c", "user.name=t", "-c", "user.email=t@example.invalid", *args], cwd=repo,
                          check=True, capture_output=True, text=True).stdout.strip()


@pytest.fixture
def repo(tmp_path):
    r = tmp_path / "repo"
    r.mkdir()
    git(r, "init", "-q", "-b", "main")
    for n in ("a", "b"):
        (r / n).write_text(n)
        git(r, "add", n)
        git(r, "commit", "-qm", n)
    git(r, "branch", "exp/x", "HEAD~1")
    return r


def review(tmp_path, *, commit: str | None, status="COMPLETED", result_status="complete", blocking=None) -> Path:
    outputs = {}
    if commit is not None:
        outputs["reviewed_commit"] = commit
    if blocking is not None:
        outputs["blocking"] = blocking
    f = tmp_path / "review.json"
    f.write_text(json.dumps({"task_id": "T-1", "status": status, "result_status": result_status,
                             "result": {"status": result_status, "summary": "ok", "outputs": outputs}}))
    return f


def check(repo: Path, review_file: Path, branch: str) -> subprocess.CompletedProcess:
    return subprocess.run([sys.executable, str(SCRIPT), "T-1", branch, "--repo", str(repo),
                           "--result-json", str(review_file)], capture_output=True, text=True)


def test_reviewed_commit_equals_branch_head_passes(repo, tmp_path):
    head = git(repo, "rev-parse", "exp/x")
    out = check(repo, review(tmp_path, commit=head[:7]), "exp/x")
    assert out.returncode == 0, out.stdout + out.stderr
    assert json.loads(out.stdout)["ok"] is True


def test_branch_moved_after_review_is_refused(repo, tmp_path):
    reviewed = git(repo, "rev-parse", "exp/x")
    out = check(repo, review(tmp_path, commit=reviewed), "main")      # main is one commit further
    assert out.returncode == 1
    assert "not the reviewed commit" in json.loads(out.stdout)["reason"]


def test_missing_reviewed_commit_is_refused(repo, tmp_path):
    out = check(repo, review(tmp_path, commit=None), "main")
    assert out.returncode == 1
    assert "reviewed_commit" in json.loads(out.stdout)["reason"]


@pytest.mark.parametrize("status,result_status", [("RUNNING", "complete"), ("COMPLETED", "partial"),
                                                   ("COMPLETED", "failed")])
def test_unfinished_or_partial_review_is_refused(repo, tmp_path, status, result_status):
    head = git(repo, "rev-parse", "main")
    out = check(repo, review(tmp_path, commit=head, status=status, result_status=result_status), "main")
    assert out.returncode == 1


def test_open_blocking_findings_are_refused(repo, tmp_path):
    head = git(repo, "rev-parse", "main")
    out = check(repo, review(tmp_path, commit=head, blocking=["lease bypass"]), "main")
    assert out.returncode == 1
    assert "blocking" in json.loads(out.stdout)["reason"]


def test_unknown_commit_is_refused(repo, tmp_path):
    out = check(repo, review(tmp_path, commit="deadbeef"), "main")
    assert out.returncode == 1
