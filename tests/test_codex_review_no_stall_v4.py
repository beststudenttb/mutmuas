"""Protocol-contract regressions found while reviewing ``exp/no-stall-v4``."""

from pathlib import Path


PROTOCOL_DOC = (Path(__file__).parents[1] / "docs" / "MESSAGE_PROTOCOL.md").read_text()


def test_default_deadline_marker_is_part_of_the_documented_request_contract():
    """The owner-visible wire field must be discoverable in the protocol contract."""
    assert "`deadline_default`" in PROTOCOL_DOC
    assert "default_reply_deadline_s" in PROTOCOL_DOC


def test_conditional_own_result_wake_is_part_of_the_documented_wake_contract():
    """Operators must be able to discover the per-seat exception to WAKE's RESULT rule."""
    assert "wake_on_own_results" in PROTOCOL_DOC
