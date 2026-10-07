"""Write a ledger and a set of messages with the mutmuas found on PYTHONPATH, for the compatibility test (D-104):
`PYTHONPATH=<that version>/src python tests/compat/make_fixtures.py <out dir>` writes <out>/ledger.sql (an SQLite
dump) and <out>/envelopes.json. Uses only what every version since 8bc01bb has (fixtures: tests/compat/<the version that wrote them>)."""

from __future__ import annotations

import json
import sqlite3
import sys
from pathlib import Path

from mutmuas.ledger import Ledger
from mutmuas.protocol import Envelope, request_body, result_body


def envelopes() -> list[Envelope]:
    req = Envelope(type="REQUEST", sender="A:main", to="B:desk", task_id="T-compat-1", priority="high",
                   body=request_body("train it", "compat", kind="experiment", inputs={"steps": 3},
                                     deadline="2030-01-01T00:00:00+00:00", observers=["C:obs"], leader=True))
    return [
        req,
        Envelope(type="ACK", sender="B:desk", to="A:main", task_id="T-compat-1",
                 body={"state": "ACCEPTED", "message": "accepted into queue", "eta": "2030-01-01T00:00:00+00:00"}),
        Envelope(type="UPDATE", sender="B:desk", to="A:main", task_id="T-compat-1",
                 body={"state": "WAITING", "message": "waiting on a job"}),
        Envelope(type="UPDATE", sender="A:main", to="B:desk", task_id="T-compat-1",
                 body={"message": "hold on", "pause": True}),
        Envelope(type="UPDATE", sender="B:secretary", to="B:desk", task_id="T-compat-1",
                 body={"message": "use env v2", "interrupt": True}),
        Envelope(type="QUESTION", sender="B:desk", to="A:main", task_id="T-compat-1",
                 body={"question": "which dataset?", "next": "A:main"}),
        Envelope(type="ANSWER", sender="A:main", to="B:desk", task_id="T-compat-1",
                 body={"answer": "v2", "next": "B:desk"}),
        Envelope(type="RESULT", sender="B:desk", to="A:main", task_id="T-compat-1",
                 body={**result_body("complete", "trained", outputs={"loss": 0.2}), "next": "A:main"}),
        Envelope(type="CANCEL", sender="A:main", to="B:desk", task_id="T-compat-2", body={"reason": "not needed"}),
    ]


def main(out: Path) -> None:
    out.mkdir(parents=True, exist_ok=True)
    envs = envelopes()
    (out / "envelopes.json").write_text(json.dumps([json.loads(e.to_json()) for e in envs], indent=1))
    db = out / "ledger.sqlite3"
    db.unlink(missing_ok=True)
    ledger = Ledger(db)
    req = envs[0]
    ledger.ingest(req)
    ledger.mark_handled(req.message_id)
    ledger.create_owned_task(req)
    ledger.update_task(req.task_id, "owner", status="WAITING", paused=1, interrupts=["hold on"])
    for env in envs[1:3] + envs[5:8]:
        ledger.ingest(env)
        ledger.mark_handled(env.message_id)
    asked = Envelope(type="REQUEST", sender="B:desk", to="C:far", task_id="T-compat-3",
                     body=request_body("label it", "compat"))
    ledger.queue_outgoing(asked)
    ledger.add_job(req.task_id, "B:desk", 99999, "start", None, "/tmp/log", "training")
    ledger.close()
    con = sqlite3.connect(db)
    (out / "ledger.sql").write_text("\n".join(con.iterdump()) + "\n")
    con.close()
    db.unlink()


if __name__ == "__main__":
    main(Path(sys.argv[1]))
