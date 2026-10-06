"""Regression contract for cleanup #1: malformed /proc fields fail conservatively."""

from mutmuas import node as node_mod


class FakeStat:
    def __init__(self, contents):
        self.contents = contents

    def exists(self):
        return True

    def read_text(self):
        return self.contents


def test_malformed_proc_state_does_not_guess_zombie(monkeypatch):
    # Linux /proc state is one character. A longer value is malformed, not Z.
    monkeypatch.setattr(node_mod, "Path", lambda path: FakeStat("123 (worker) Zextra 1 0\n"))
    assert node_mod._zombie(123) is False
