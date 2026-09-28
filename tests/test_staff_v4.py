"""Staff system v4 (D-029..D-032): project directories, worker settings, dual mode."""

from __future__ import annotations

import os
from datetime import datetime, timezone

from mutmuas.node import session_fields


def _live(cwd) -> dict:
    return {"pid": os.getpid(), "cwd": str(cwd), "last_seen": datetime.now(timezone.utc).isoformat()}


# --------------------------------------------------------------------------- C1: sub-directories of the workdir


def test_session_in_a_subdirectory_of_the_workdir_is_not_warned(tmp_path):
    workdir = tmp_path / "paper"
    (workdir / "visualrl" / "notes").mkdir(parents=True)
    assert "session_warning" not in session_fields(_live(workdir / "visualrl"), workdir)
    assert "session_warning" not in session_fields(_live(workdir / "visualrl" / "notes"), workdir)
    link = tmp_path / "shortcut"
    link.symlink_to(workdir / "visualrl")
    assert "session_warning" not in session_fields(_live(link), workdir)


def test_session_outside_the_workdir_is_still_warned(tmp_path):
    workdir = tmp_path / "paper"
    workdir.mkdir()
    sibling = tmp_path / "paper-2"            # shares the prefix, but is not inside the workdir
    sibling.mkdir()
    assert "not in its workdir" in session_fields(_live(sibling), workdir)["session_warning"]
    assert "not in its workdir" in session_fields(_live(tmp_path), workdir)["session_warning"]
