"""Progress ledger semantics: stall/wall budgets, coalescing, bounded readers."""

import json
import tempfile
import time
import unittest
from pathlib import Path

from zcode_supervisor import progress


class FakeClock:
    def __init__(self, start=1000.0):
        self.now = start

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


class ResolveRunBudget(unittest.TestCase):
    def test_defaults_preserve_legacy_semantics(self):
        self.assertEqual(progress.resolve_run_budget(None, None), (300.0, None))
        self.assertEqual(progress.resolve_run_budget(240, None), (240.0, None))

    def test_stall_raises_the_default_wall_fuse(self):
        self.assertEqual(progress.resolve_run_budget(None, 600), (86400.0, 600.0))
        self.assertEqual(progress.resolve_run_budget(1200, 600), (1200.0, 600.0))

    def test_range_validation(self):
        for bad in (0, -1, 86401, 10**9):
            with self.assertRaises(ValueError):
                progress.resolve_run_budget(bad, None)
            with self.assertRaises(ValueError):
                progress.resolve_run_budget(None, bad)
        # boundaries pass
        progress.resolve_run_budget(1, 1)
        progress.resolve_run_budget(86400, 86400)


class LedgerBehaviour(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.clock = FakeClock()
        self.path = Path(self.tmp.name) / "run.progress.jsonl"

    def tearDown(self):
        self.tmp.cleanup()

    def _ledger(self, **kwargs):
        return progress.ProgressLedger(
            self.path, clock=self.clock, **kwargs)

    def test_exclusive_create(self):
        self._ledger().close()
        with self.assertRaises(FileExistsError):
            self._ledger()

    def test_coalescing_and_lines(self):
        ledger = self._ledger()
        ledger.note("event", "tool:Edit")
        ledger.note("event", "tool:Edit")   # same key, inside window
        self.clock.advance(0.5)
        ledger.note("event", "tool:Edit")   # still inside 2s window
        self.clock.advance(3.0)
        ledger.note("event", "tool:Edit")   # window passed -> new line
        ledger.note("event", "tool:Bash")   # different detail -> new line
        ledger.close()
        lines = self.path.read_text().splitlines()
        self.assertEqual(len(lines), 3)
        summary = ledger.summary()
        self.assertEqual(summary["heartbeats"], 3)
        self.assertEqual(summary["coalesced"], 2)
        self.assertEqual(summary["last_detail"], "tool:Bash")

    def test_expires_only_without_heartbeats(self):
        ledger = self._ledger(stall_timeout_s=10, wall_timeout_s=100)
        self.assertIsNone(ledger.expired())
        self.clock.advance(9)
        ledger.note("event", "keepalive")
        self.clock.advance(9)
        self.assertIsNone(ledger.expired())  # heartbeat moved the window
        self.clock.advance(2)
        self.assertEqual(ledger.expired(), "stall")
        # wall still binds even with fresh heartbeats
        self.clock.advance(200)
        ledger.note("event", "keepalive")
        self.assertEqual(ledger.expired(), "wall")

    def test_wall_only_ledger_keeps_legacy_behaviour(self):
        ledger = self._ledger(wall_timeout_s=5)
        self.clock.advance(4)
        ledger.note("event", "busy")
        self.assertIsNone(ledger.expired())
        self.clock.advance(2)
        self.assertEqual(ledger.expired(), "wall")

    def test_effective_deadline_follows_heartbeats(self):
        ledger = self._ledger(stall_timeout_s=10)
        start = ledger.effective_deadline()
        self.clock.advance(6)
        ledger.note("event", "x")
        self.assertGreater(ledger.effective_deadline(), start)
        ledger.close()

    def test_finalize_is_terminal_and_idempotent(self):
        ledger = self._ledger()
        ledger.finalize("timeout")
        ledger.note("event", "late")     # clock ticks, no new line
        ledger.finalize("delivered")     # ignored
        ledger.close()
        lines = [json.loads(line) for line in self.path.read_text().splitlines()]
        self.assertEqual([entry["kind"] for entry in lines], ["final"])
        self.assertEqual(lines[0]["detail"], "timeout")

    def test_extra_values_are_bounded_scalars(self):
        ledger = self._ledger()
        ledger.note("event", "d", turn=3, tool="Edit", blob={"a": 1}, long="x" * 300)
        ledger.close()
        record = json.loads(self.path.read_text().splitlines()[0])
        self.assertEqual(record["turn"], 3)
        self.assertEqual(record["tool"], "Edit")
        self.assertNotIn("blob", record)
        self.assertEqual(len(record["long"]), 64)


class Readers(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()

    def _task(self, name="run"):
        ledger = progress.ProgressLedger(self.dir / f"{name}.progress.jsonl")
        ledger.note("event", "tool:Edit")
        ledger.note("event", "tool:Bash")
        return ledger

    def test_read_last_bounds(self):
        ledger = self._task()
        ledger.finalize("delivered")
        ledger.close()
        records = progress.read_last(ledger.path, 2)
        self.assertEqual([entry["kind"] for entry in records], ["event", "final"])

    def test_liveness_running_and_finished(self):
        ledger = self._task()
        ledger.close()
        verdict = progress.liveness(self.dir)
        self.assertEqual(verdict["state"], "running")
        ledger2 = progress.ProgressLedger(
            self.dir / "other.progress.jsonl")
        ledger2.note("event", "x")
        ledger2.finalize("timeout")
        ledger2.close()
        self.assertEqual(progress.liveness(ledger2.path)["state"], "finished")

    def test_liveness_finished_via_sibling_result_json(self):
        ledger = self._task()
        ledger.close()
        (self.dir / "run.json").write_text(
            json.dumps({"delivery_ok": False, "outcome": "evidence-rejected"}))
        verdict = progress.liveness(self.dir / "run.json")
        self.assertEqual(verdict["state"], "finished")
        self.assertEqual(verdict["outcome"], "evidence-rejected")

    def test_locate_ledger_variants(self):
        self._task().close()
        self.assertEqual(progress.locate_ledger(self.dir).name,
                         "run.progress.jsonl")
        self.assertEqual(
            progress.locate_ledger(self.dir / "run.json").name,
            "run.progress.jsonl")
        self.assertEqual(
            progress.locate_ledger(self.dir / "run.events.jsonl").name,
            "run.progress.jsonl")
        self.assertIsNone(progress.locate_ledger(self.dir / "missing.json"))

    def test_unknown_target(self):
        verdict = progress.liveness(self.dir / "nothing")
        self.assertEqual(verdict["state"], "unknown")


if __name__ == "__main__":
    unittest.main()
