"""health.py contracts: fail-closed status mapping, diff summaries in the
disable reason, atomic write/read roundtrip, hub-server format compatibility."""

import tempfile
import unittest
from pathlib import Path

from zcode_supervisor import health as Eh


def probe_result(**agents):
    return {"agents": agents, "comparison": {}}


class EvaluateTests(unittest.TestCase):
    def test_ok_with_unchanged_baseline_is_enabled(self):
        result = Eh.evaluate({
            "agents": {"codex": {"status": "ok", "installed": True,
                                 "version": "0.163.0"}},
            "comparison": {"codex": {"changed": False, "diff": {}}},
        }, probe_ref="job-9")
        worker = result["workers"]["codex"]
        self.assertTrue(worker["enabled"])
        self.assertEqual(worker["status"], "ok")
        self.assertEqual(worker["probe_ref"], "job-9")
        self.assertEqual(result["schema_version"], 1)
        self.assertIn("generated", result)
        self.assertEqual(result["routing"]["enabled_workers"], ["codex"])
        self.assertEqual(result["routing"]["disabled_workers"], [])
        self.assertEqual(result["routing"]["note"], Eh.ROUTING_NOTE)

    def test_changed_drift_disables_with_diff_summary_reason(self):
        result = Eh.evaluate({
            "agents": {"zcode": {"status": "ok", "installed": True,
                                 "version": "0.17.1"}},
            "comparison": {"zcode": {
                "changed": True,
                "diff": {"version": {"was": "0.17.0", "now": "0.17.1"},
                         "fingerprint": {"was": "aa", "now": "bb"},
                         "methods": {"added": ["m1", "m2"],
                                     "removed": ["m0"]}}}},
        })
        worker = result["workers"]["zcode"]
        self.assertFalse(worker["enabled"])
        self.assertEqual(worker["status"], "changed")
        self.assertIn("2 added method(s)", worker["reason"])
        self.assertIn("1 removed method(s)", worker["reason"])
        self.assertIn("version '0.17.0' -> '0.17.1'", worker["reason"])
        self.assertIn("fingerprint 'aa' -> 'bb'", worker["reason"])
        self.assertEqual(result["routing"]["disabled_workers"], ["zcode"])

    def test_absent_and_error_statuses_disable(self):
        result = Eh.evaluate({
            "agents": {"dsh": {"status": "absent", "installed": False},
                       "codex": {"status": "error", "installed": True,
                                 "error": "probe timed out after 60s"}},
            "comparison": {"dsh": {"changed": False, "diff": {}},
                           "codex": {"changed": False, "diff": {}}},
        })
        self.assertEqual(result["workers"]["dsh"]["status"], "absent")
        self.assertFalse(result["workers"]["dsh"]["enabled"])
        self.assertEqual(result["workers"]["codex"]["status"], "error")
        self.assertFalse(result["workers"]["codex"]["enabled"])
        self.assertIn("probe timed out", result["workers"]["codex"]["reason"])

    def test_missing_pieces_fail_closed_to_unknown(self):
        # no agent entry at all
        result = Eh.evaluate({"agents": {}, "comparison": {}})
        self.assertEqual(result["workers"], {})
        # ok status but no comparison verdict -> unknown, disabled
        result = Eh.evaluate({
            "agents": {"codex": {"status": "ok", "installed": True}},
            "comparison": {},
        })
        worker = result["workers"]["codex"]
        self.assertFalse(worker["enabled"])
        self.assertEqual(worker["status"], "unknown")
        self.assertIn("comparison missing", worker["reason"])
        # unrecognized status -> unknown; agent entry that is not a dict too
        result = Eh.evaluate({
            "agents": {"codex": {"status": "weird", "installed": True},
                       "zcode": None},
            "comparison": {"codex": {"changed": False, "diff": {}}},
        })
        self.assertEqual(result["workers"]["codex"]["status"], "unknown")
        self.assertEqual(result["workers"]["zcode"]["status"], "unknown")
        # absent/changed statuses from the probe side still disable even
        # when the comparison disagrees
        result = Eh.evaluate({
            "agents": {"codex": {"status": "changed", "installed": True}},
            "comparison": {"codex": {"changed": False, "diff": {}}},
        })
        self.assertFalse(result["workers"]["codex"]["enabled"])
        self.assertEqual(result["workers"]["codex"]["status"], "changed")
        # installed False overrides an ok status: absent wins
        result = Eh.evaluate({
            "agents": {"codex": {"status": "ok", "installed": False}},
            "comparison": {"codex": {"changed": False, "diff": {}}},
        })
        self.assertEqual(result["workers"]["codex"]["status"], "absent")
        # entirely malformed input still yields a valid document
        result = Eh.evaluate(None)
        self.assertEqual(result["workers"], {})
        self.assertEqual(result["routing"]["enabled_workers"], [])


class WriteReadTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.state_dir = Path(temporary.name)

    def test_write_read_roundtrip(self):
        document = Eh.evaluate({
            "agents": {"codex": {"status": "ok", "installed": True}},
            "comparison": {"codex": {"changed": False, "diff": {}}},
        }, probe_ref="job-1")
        path = Eh.write_health(self.state_dir, document)
        self.assertEqual(path, self.state_dir / "hub" / "health.json")
        self.assertTrue(path.is_file())
        self.assertEqual(Eh.read_health(self.state_dir), document)

    def test_missing_file_reads_as_none(self):
        self.assertIsNone(Eh.read_health(self.state_dir))

    def test_invalid_document_reads_as_none(self):
        target = self.state_dir / "hub"
        target.mkdir(parents=True)
        (target / "health.json").write_text("{not json", encoding="utf-8")
        self.assertIsNone(Eh.read_health(self.state_dir))

    def test_document_shape_matches_hub_server_health_tool(self):
        # hub_server.read_health_document expects {"generated", "workers":
        # {name: {"enabled": bool, "status", "reason", "probe_ref"}}}
        from zcode_supervisor import hub_server as H
        document = Eh.evaluate({
            "agents": {"codex": {"status": "ok", "installed": True},
                       "dsh": {"status": "absent", "installed": False}},
            "comparison": {"codex": {"changed": False, "diff": {}},
                           "dsh": {"changed": False, "diff": {}}},
        })
        Eh.write_health(self.state_dir, document)
        view = H.health_impl(self.config_like(), str(Path("/nonexistent/codex")))
        self.assertEqual(view["health_source"], "health.json")
        self.assertTrue(view["workers"]["codex"]["enabled"])
        self.assertEqual(view["workers"]["codex"]["status"], "ok")
        self.assertTrue(view["workers"]["codex"]["declared"])
        self.assertEqual(view["workers"]["dsh"]["status"], "absent")
        self.assertEqual(view["routing"]["enabled_workers"], ["codex"])

    def config_like(self):
        from types import SimpleNamespace
        return SimpleNamespace(state_dir=str(self.state_dir),
                               work_root=str(self.state_dir / "work"),
                               cache_dir=str(self.state_dir / "cache"),
                               cli_path=str(self.state_dir / "cli.cjs"),
                               node="node")


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
