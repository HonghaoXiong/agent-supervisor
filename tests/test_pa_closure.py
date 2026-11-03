"""P-A (0.2.4): monitoring closure — every worker channel writes a heartbeat
ledger, records carry the explicit ledger path, and hub restarts recover."""

import json
import os
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace

from zcode_supervisor import hub_server as H


def _task_record(config, task_id, status="running", ledger=None, worker="zcode"):
    record = {"task_id": task_id, "worker": worker, "mode": "cold",
              "status": status, "outcome": None, "updated": "now",
              "contract_id": f"c-{task_id}"}
    if ledger is not None:
        record["progress_ledger"] = str(ledger)
    path = Path(config.state_dir) / "hub" / "tasks" / f"{task_id}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(record))
    return path


class RecoveryAndLedgerPath(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.config = SimpleNamespace(state_dir=self.root / "state",
                                      work_root=self.root / "work")
        # 每个 тест 重置幂等旗，便于独立验证
        H._recovery_done = False

    def tearDown(self):
        H._recovery_done = False
        self.tmp.cleanup()

    def test_stale_running_marked_interrupted_once(self):
        stale = _task_record(self.config, "t-old")
        old = stale.stat().st_mtime - 3600
        os.utime(stale, (old, old))
        report = H.task_status_impl(self.config)
        by_id = {entry["task_id"]: entry for entry in report["tasks"]}
        self.assertEqual(by_id["t-old"]["status"], "interrupted")
        self.assertEqual(by_id["t-old"]["outcome"], "hub-restart")
        self.assertIn("hub process restarted", by_id["t-old"]["note"])
        # 幂等：第二次调用不再动它（updated 不再变化）
        again = H.task_status_impl(self.config)
        self.assertEqual(again["tasks"][0]["status"], "interrupted")

    def test_fresh_running_is_not_interrupted(self):
        _task_record(self.config, "t-live")
        report = H.task_status_impl(self.config)
        self.assertEqual(report["tasks"][0]["status"], "running")

    def test_explicit_ledger_path_is_authoritative(self):
        ledger = self.root / "t-run.progress.jsonl"
        ledger.write_text(json.dumps(
            {"t": 0.0, "kind": "event", "detail": "tool:Edit"}) + "\n")
        _task_record(self.config, "t-run", ledger=ledger)
        report = H.task_status_impl(self.config, task_id="t-run")
        self.assertEqual(report["tasks"][0]["heartbeat"]["state"], "running")

    def test_finished_ledger_reports_outcome(self):
        ledger = self.root / "t-fin.progress.jsonl"
        ledger.write_text(json.dumps(
            {"t": 1.0, "kind": "final", "detail": "delivered"}) + "\n")
        _task_record(self.config, "t-fin", ledger=ledger)
        report = H.task_status_impl(self.config, task_id="t-fin")
        self.assertEqual(report["tasks"][0]["heartbeat"]["outcome"], "delivered")


class CodexDriverLedger(unittest.TestCase):
    def test_exec_channel_writes_heartbeat_ledger(self):
        import sys
        tests = Path(__file__).parent
        sys.path.insert(0, str(tests))
        try:
            from test_codex_driver import FAKE_CODEX_SOURCE
        except ImportError:
            self.skipTest("codex_driver harness unavailable")
        finally:
            if str(tests) in sys.path:
                sys.path.remove(str(tests))
        from zcode_supervisor import codex_driver
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workspace = root / "ws"
            evidence = root / "ev"
            workspace.mkdir()
            codex = root / "fake-codex"
            codex.write_text(FAKE_CODEX_SOURCE)
            codex.chmod(0o755)
            ledger_path = root / "codex.progress.jsonl"
            schema = {"type": "object",
                      "properties": {"summary": {"type": "string"},
                                     "files": {"type": "array"}},
                      "required": ["summary", "files"]}
            receipt = codex_driver.run_readonly(
                codex, workspace, "hi", schema, evidence,
                timeout_s=30,
                env={"FAKE_CODEX_MODE": "delivered",
                     "CODEX_HOME": str(root / "codex-home")},
                progress_ledger=ledger_path)
            self.assertEqual(receipt.outcome, "delivered")
            self.assertTrue(ledger_path.is_file())
            lines = [json.loads(line)
                     for line in ledger_path.read_text().splitlines()]
            self.assertEqual(lines[-1]["kind"], "final")
            self.assertEqual(lines[-1]["detail"], "delivered")
            self.assertTrue(any(entry["kind"] == "codex" for entry in lines))


if __name__ == "__main__":
    unittest.main()
