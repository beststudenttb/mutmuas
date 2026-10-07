"""D-104 item 2: a deploy replaces the code on disk while runs started from the old code go on. A process loads all
of mutmuas when it starts and nothing later, so it never runs a mix of old and new code."""

from __future__ import annotations

import ast
import os
import pkgutil
import subprocess
import sys
from pathlib import Path

import mutmuas

SRC = Path(mutmuas.__file__).parent


def test_mutmuas_modules_are_imported_at_the_top_of_each_module_only():
    late = []
    for path in sorted(SRC.glob("*.py")):
        tree = ast.parse(path.read_text())
        top = {id(node) for node in tree.body}
        for node in ast.walk(tree):
            if isinstance(node, (ast.Import, ast.ImportFrom)) and id(node) not in top:
                module = node.module if isinstance(node, ast.ImportFrom) else node.names[0].name
                if getattr(node, "level", 0) or (module or "").startswith("mutmuas"):
                    late.append(f"{path.name}:{node.lineno}")
    assert late == []


def test_starting_any_command_loads_the_whole_package():
    names = sorted(m.name for m in pkgutil.iter_modules([str(SRC)]))
    code = ("import sys, mutmuas.cli; "
            f"print([n for n in {names!r} if 'mutmuas.' + n not in sys.modules])")
    out = subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, check=True,
                         env={**os.environ, "PYTHONPATH": os.pathsep.join(sys.path)})
    assert out.stdout.strip() == "[]"
