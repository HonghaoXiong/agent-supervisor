"""dsh headless driver: event-stream success/error/stall against a fake CLI.

The fake executable reproduces the live-verified wire shape (2026-11-03):
NDJSON events on stdout, task text on stdin, and — critically — exit code 0
even on errors, so success must come from the event stream.
"""

import json
import stat
import tempfile
import unittest
from pathlib import Path

from zcode_supervisor import dsh_driver

SUCCESS_SCRIPT = """#!/bin/sh
cat > /dev/null
cat <<'EOF'
{"type":"session","sessionId":"session-abc","cwd":"/w"}
{"type":"status","phase":"turn_start","turn":1}
{"type":"status","phase":"step_start","turn":1,"step":1}
{"type":"status","phase":"step_end","turn":1,"step":1}
{"type":"status","phase":"turn_end","turn":1,"reason":{"kind":"success"}}
{"type":"final","text":"已完成实现"}
EOF
"""

ERROR_SCRIPT = """#!/bin/sh
cat > /dev/null
cat <<'EOF'
{"type":"session","sessionId":"session-err","cwd":"/w"}
{"type":"status","phase":"turn_start","turn":1}
{"type":"status","phase":"turn_end","turn":1,"reason":{"kind":"error","error":{"message":"MISSING_CREDENTIAL","code":"MISSING_CREDENTIAL"}}}
{"type":"final","text":""}
EOF
"""

STALL_SCRIPT = """#!/bin/sh
cat > /dev/null
echo '{"type":"session","sessionId":"session-stall","cwd":"/w"}'
sleep 30
"""


class DshDriverRuns(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.workspace = self.root / "workspace"
        self.evidence = self.root / "evidence"
        self.workspace.mkdir()

    def tearDown(self):
        self.tmp.cleanup()

    def _fake(self, name, body):
        path = self.root / name
        path.write_text(body)
        path.chmod(path.stat().st_mode | stat.S_IEXEC)
        return path

    def _run(self, executable, **kwargs):
        return dsh_driver.run_headless(
            executable, self.workspace, "写一个函数", self.evidence, **kwargs)

    def test_success_judged_from_events_not_exit_code(self):
        receipt = self._run(self._fake("dsh-ok", SUCCESS_SCRIPT),
                            wall_timeout_s=30)
        self.assertEqual(receipt["outcome"], "delivered")
        self.assertEqual(receipt["session_id"], "session-abc")
        self.assertEqual(receipt["final_message"], "已完成实现")
        self.assertEqual(receipt["exit_code"], 0)
        self.assertIsNone(receipt["error"])
        events = [json.loads(line)
                  for line in Path(receipt["events_file"]).read_text().splitlines()]
        self.assertEqual(events[0]["type"], "session")
        self.assertEqual(receipt["progress"]["final"], "delivered")
        ledger = self.evidence / "dsh.progress.jsonl"
        self.assertTrue(ledger.is_file())

    def test_error_event_surfaces_message(self):
        receipt = self._run(self._fake("dsh-err", ERROR_SCRIPT),
                            wall_timeout_s=30)
        self.assertEqual(receipt["outcome"], "error")
        self.assertIn("MISSING_CREDENTIAL", receipt["error"])
        self.assertEqual(receipt["turn_end_reason"]["kind"], "error")

    def test_stall_terminates_silent_worker(self):
        receipt = self._run(self._fake("dsh-stall", STALL_SCRIPT),
                            wall_timeout_s=60, stall_timeout_s=2)
        self.assertEqual(receipt["outcome"], "timeout-stall")
        self.assertEqual(receipt["timed_out"], "stall")
        self.assertLess(receipt["duration_s"], 30)

    def test_wall_fuse_bounds_chatty_worker(self):
        receipt = self._run(self._fake("dsh-stall", STALL_SCRIPT),
                            wall_timeout_s=2, stall_timeout_s=60)
        self.assertEqual(receipt["outcome"], "timeout-wall")

    def test_launch_error(self):
        receipt = self._run(self.root / "missing-dsh", wall_timeout_s=5)
        self.assertEqual(receipt["outcome"], "launch-error")
        self.assertIn("failed to start dsh", receipt["error"])

    def test_parameter_validation(self):
        with self.assertRaises(dsh_driver.DshRunError):
            dsh_driver.run_headless("x", self.workspace, "  ", self.evidence)
        with self.assertRaises(dsh_driver.DshRunError):
            dsh_driver.run_headless("x", self.workspace, "t", self.evidence,
                                    wall_timeout_s=0)
        with self.assertRaises(dsh_driver.DshRunError):
            dsh_driver.run_headless("x", None, "t", self.evidence)

    def test_reused_evidence_dir_truncates_partials(self):
        executable = self._fake("dsh-ok", SUCCESS_SCRIPT)
        first = self._run(executable, wall_timeout_s=30)
        self.assertEqual(first["outcome"], "delivered")
        second = dsh_driver.run_headless(
            executable, self.workspace, "再来一次",
            self.evidence, wall_timeout_s=30)
        self.assertEqual(second["outcome"], "delivered")
        # finalized evidence contains exactly one run's events
        events = Path(second["events_file"]).read_text().splitlines()
        self.assertEqual(len([line for line in events if '"session"' in line]), 1)


if __name__ == "__main__":
    unittest.main()
