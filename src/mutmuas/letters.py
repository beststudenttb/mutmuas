"""Letter templates (D-109): every letter an agent writes follows a fixed template, letters.yaml. The framework
renders its title; the agent fills in the blanks; a required blank left empty refuses the letter at the MCP tool or
agentctl command, before anything is sent. The wording lives in letters.yaml alone."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

from .visibility import short

TEMPLATES: dict[str, dict[str, Any]] = yaml.safe_load(Path(__file__).with_name("letters.yaml").read_text())


def missing(kind: str, values: dict[str, Any]) -> list[str]:
    """The required blanks of template `kind` left empty, as '<key> (<label>)'."""
    return [f"{key} ({spec.get('label', key)})" for key, spec in TEMPLATES[kind]["fields"].items()
            if spec.get("required") and values.get(key) in (None, "", [], {})]


def check(kind: str, values: dict[str, Any]) -> None:
    """Refuse a letter whose template has required blanks left empty (ValueError, naming them)."""
    if gaps := missing(kind, values):
        name = TEMPLATES[kind]["name"]
        raise ValueError(f"{name} ({kind}) is missing: {', '.join(gaps)}")


def title(kind: str, values: dict[str, Any], original: str | None = None) -> str:
    """The letter's title from its template: its blanks, and {original} the title of the task it is about."""
    fill = {key: short(str(value)) for key, value in values.items() if value not in (None, "")}

    class Blank(dict):
        def __missing__(self, key):
            return ""
    return TEMPLATES[kind]["title"].format_map(Blank(fill, original=short(original or "")))
