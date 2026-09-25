#!/usr/bin/env python3
"""Refuse a merge unless a completed review approved exactly the commit being merged.

    scripts/mergecheck.py <review-task-id> <branch> [--repo PATH] [--config NODE_YAML] [--as ADDRESS]
    scripts/mergecheck.py <review-task-id> <branch> --result-json FILE    # offline / tests

Why: on 2026-09-25 A:codex's review covered exp/visibility up to 7d9269e, while the branch had moved on to
004c35a (weekend r3, error E2). Nothing compared "what was reviewed" with "what is merged"; this script does
(handbook R16.2, D-010: permission code needs an A:codex review of exactly the merged commit).

The review task's RESULT must carry, under outputs:
    reviewed_commit: <sha of the commit the reviewer read>        (required)
    blocking: [<open blocking findings>]                            (optional; must be empty)
It passes only if the task is COMPLETED with result status complete, no blocking finding is open, and
reviewed_commit resolves to the same commit as <branch>. Prints one JSON line; exit 0 = ok, 1 = refused.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys


def rev(repo: str, ref: str) -> str | None:
    out = subprocess.run(["git", "rev-parse", "--verify", "--quiet", f"{ref}^{{commit}}"], cwd=repo,
                         capture_output=True, text=True)
    return out.stdout.strip() or None


def load_review(a) -> dict:
    if a.result_json:
        with open(a.result_json) as f:
            return json.load(f)
    argv = ["agentctl", "result", a.task, "--json"]
    if a.config:
        argv += ["--config", a.config]
    if a.as_agent:
        argv += ["--as", a.as_agent]
    return json.loads(subprocess.run(argv, check=True, capture_output=True, text=True).stdout)


def check(review: dict, repo: str, branch: str) -> tuple[bool, str, dict]:
    result = review.get("result") or {}
    outputs = result.get("outputs") or {}
    facts = {"task": review.get("task_id"), "branch": branch}
    if review.get("status") != "COMPLETED" or (review.get("result_status") or result.get("status")) != "complete":
        return False, f"review is {review.get('status')}/{review.get('result_status')}, not COMPLETED/complete", facts
    reviewed = outputs.get("reviewed_commit")
    if not reviewed:
        return False, "the review RESULT has no outputs.reviewed_commit", facts
    blocking = outputs.get("blocking") or []
    if blocking:
        return False, f"{len(blocking)} blocking finding(s) still open: {blocking}", facts
    reviewed_sha, head_sha = rev(repo, str(reviewed)), rev(repo, branch)
    facts.update(reviewed_commit=reviewed_sha or str(reviewed), branch_head=head_sha)
    if not reviewed_sha:
        return False, f"reviewed_commit {reviewed} is not a commit in {repo}", facts
    if not head_sha:
        return False, f"branch {branch} not found in {repo}", facts
    if reviewed_sha != head_sha:
        return False, f"{branch} is at {head_sha[:12]}, not the reviewed commit {reviewed_sha[:12]}", facts
    return True, "branch head is the reviewed commit", facts


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("task", help="the review task id")
    p.add_argument("branch", help="the branch (or ref) about to be merged")
    p.add_argument("--repo", default=".")
    p.add_argument("--config")
    p.add_argument("--as", dest="as_agent")
    p.add_argument("--result-json", help="read the review from this `agentctl result --json` file")
    a = p.parse_args()
    ok, reason, facts = check(load_review(a), a.repo, a.branch)
    print(json.dumps({"ok": ok, "reason": reason, **facts}))
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
