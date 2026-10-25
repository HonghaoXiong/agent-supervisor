"""The exportable Mac check never starts a model or a ZCode worker."""
import importlib.util
import json
import os
from pathlib import Path
import stat
import sys
from tempfile import TemporaryDirectory
from types import SimpleNamespace
import unittest
from unittest.mock import patch


REPO = Path(__file__).resolve().parents[1]


def load_validator():
    spec = importlib.util.spec_from_file_location(
        "czs_mac_validation", REPO / "packaging/macos/validate.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@unittest.skipUnless((REPO / "packaging/macos/validate.py").exists(),
                     "Distribution source is absent from baseline exports")
class DistributionValidationTests(unittest.TestCase):
    def setUp(self):
        self.temporary = TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.validation = load_validator()

    def fake_codex(self, *, skills=None, errors=None, mode="healthy"):
        if skills is None:
            skills = [{"name": self.validation.SKILL, "enabled": True,
                       "pluginId": self.validation.PLUGIN_ID}]
        payload = {"data": [{"skills": skills, "errors": errors or []}]}
        executable = self.root / ("fake-codex-" + mode)
        self.methods = self.root / ("sent-methods-" + mode + ".jsonl")
        executable.write_text(
            "#!" + sys.executable + "\n"
            "import json,sys,time\n"
            f"payload=json.loads({json.dumps(payload)!r})\n"
            f"log={str(self.methods)!r}\n"
            "for line in sys.stdin:\n"
            " frame=json.loads(line)\n"
            " with open(log,'a') as output: output.write(json.dumps(frame)+'\\n')\n"
            f" if {mode!r} == 'timeout': time.sleep(10)\n"
            f" if {mode!r} == 'closed': break\n"
            " if frame['method']=='initialize':\n"
            "  print(json.dumps({'id':frame['id'],'result':{'codexHome':'/private/user-secret'}}),flush=True)\n"
            " if frame['method']=='skills/list':\n"
            f"  if {mode!r} == 'error': result={{'id':frame['id'],'error':{{'message':'/private/user-secret'}}}}\n"
            "  else: result={'id':frame['id'],'result':payload}\n"
            "  print(json.dumps(result),flush=True)\n"
        )
        executable.chmod(0o755)
        return str(executable)

    def test_native_catalog_filters_unrelated_skills_and_private_paths(self):
        codex = self.fake_codex(skills=[
            {"name": self.validation.SKILL, "enabled": True,
             "pluginId": self.validation.PLUGIN_ID, "path": "/private/user-secret"},
            {"name": "private-client-skill", "path": "/private/company", "enabled": True},
        ])
        report = self.validation.query_installed_skill(codex, timeout=2)
        self.assertEqual(report["status"], "passed")
        self.assertNotIn("private", json.dumps(report))
        self.assertNotIn("client-skill", json.dumps(report))
        frames = [json.loads(line) for line in self.methods.read_text().splitlines()]
        self.assertEqual([frame["method"] for frame in frames],
                         ["initialize", "initialized", "skills/list"])
        self.assertEqual(report["model_requests"], 0)
        self.assertTrue(frames[-1]["params"]["forceReload"])

    def test_missing_disabled_or_wrong_plugin_skill_cannot_pass(self):
        cases = [[], [{"name": self.validation.SKILL, "enabled": False,
                       "pluginId": self.validation.PLUGIN_ID}],
                 [{"name": self.validation.SKILL, "enabled": True,
                   "pluginId": "another-plugin"}]]
        for index, skills in enumerate(cases):
            with self.subTest(index=index):
                result = self.validation.query_installed_skill(
                    self.fake_codex(skills=skills), timeout=2)
                self.assertEqual(result["status"], "failed")

    def test_ambiguous_duplicate_target_and_catalog_errors_fail(self):
        target = {"name": self.validation.SKILL, "enabled": True,
                  "pluginId": self.validation.PLUGIN_ID}
        duplicate = self.validation.query_installed_skill(
            self.fake_codex(skills=[target, target]), timeout=2)
        self.assertEqual(duplicate["status"], "failed")
        errors = self.validation.query_installed_skill(
            self.fake_codex(errors=[{"message": "/private/user-secret"}]), timeout=2)
        self.assertEqual(errors["status"], "failed")
        self.assertEqual(errors["catalog_error_count"], 1)
        self.assertNotIn("private", json.dumps(errors))

    def test_rpc_error_closed_stream_and_timeout_are_retained_as_fixed_codes(self):
        for mode, expected in (("error", "app_server_rpc_error"),
                               ("closed", "app_server_closed"),
                               ("timeout", "app_server_timeout")):
            with self.subTest(mode=mode):
                result = self.validation.query_installed_skill(
                    self.fake_codex(mode=mode), timeout=0.15 if mode == "timeout" else 2)
                self.assertEqual(result["status"], "failed")
                self.assertEqual(result["error_code"], expected)
                self.assertNotIn("private", json.dumps(result))

    def write_receipt(self, **updates):
        receipt = {"delivery_ok": True, "process_exit_code": 0, "outcome": "delivered",
                   "session_id": "session-private", "turn_id": "turn-private",
                   "input_id": "input-private", "execution_problems": [],
                   "workspace": "/private/user-secret", "error": "private free text"}
        receipt.update(updates)
        path = self.root / "task.json"
        path.write_text(json.dumps(receipt))
        path.with_suffix(".events.jsonl").write_text('{"private":"raw"}\n')
        path.with_suffix(".stderr.log").touch()
        path.with_suffix(".before.json").write_text('{}')
        return path

    def test_receipt_review_reports_known_identity_and_raw_presence_without_content(self):
        path = self.write_receipt()
        result = self.validation.inspect_receipt(path)
        self.assertTrue(result["reported_delivery_complete"])
        self.assertFalse(result["validator_started_task"])
        self.assertEqual(result["acceptance"], "pending_independent_review")
        self.assertEqual(result["identity"]["session_id"]["status"], "known")
        self.assertNotIn("private", json.dumps(result))
        self.assertTrue(result["raw_evidence"]["events"]["present"])

    def test_receipt_rejection_unknown_identity_and_missing_raw_remain_incomplete(self):
        for updates in ({"delivery_ok": False}, {"delivery_ok": 1},
                        {"process_exit_code": 1}, {"process_exit_code": False},
                        {"turn_id": ""}, {"outcome": "private-unknown"},
                        {"execution_problems": ["private-tool-error"]}):
            with self.subTest(updates=updates):
                path = self.write_receipt(**updates)
                result = self.validation.inspect_receipt(path)
                self.assertFalse(result["reported_delivery_complete"])
                self.assertNotIn("private", json.dumps(result))
        path = self.write_receipt()
        path.with_suffix(".events.jsonl").unlink()
        self.assertFalse(self.validation.inspect_receipt(path)["reported_delivery_complete"])

    def test_missing_invalid_and_symlink_receipts_are_reported_without_exception(self):
        missing = self.validation.inspect_receipt(self.root / "missing.json")
        self.assertEqual(missing["status"], "invalid_receipt")
        source = self.write_receipt()
        linked = self.root / "linked.json"
        linked.symlink_to(source)
        self.assertEqual(self.validation.inspect_receipt(linked)["status"], "invalid_receipt")
        source.write_text("[]")
        self.assertEqual(self.validation.inspect_receipt(source)["status"], "invalid_receipt")
        fifo = self.root / "fifo.json"
        os.mkfifo(fifo)
        self.assertEqual(self.validation.inspect_receipt(fifo)["status"], "invalid_receipt")

    def test_default_task_status_is_not_run(self):
        self.assertEqual(self.validation.inspect_receipt(None)["status"], "not_run")

    def test_report_export_is_private_exclusive_and_refuses_relative_path(self):
        output = self.root / "report.json"
        self.validation.save_report(output, {"ok": True})
        self.assertEqual(stat.S_IMODE(output.stat().st_mode), 0o600)
        with self.assertRaises(FileExistsError):
            self.validation.save_report(output, {"ok": False})
        self.assertTrue(json.loads(output.read_text())["ok"])
        with self.assertRaises(ValueError):
            self.validation.save_report(Path("relative.json"), {})

    def test_build_report_never_passes_unknown_environment(self):
        diagnostics = SimpleNamespace(
            inspect_environment=lambda *_args, **_kwargs: {"ok": False, "model_requests": 0},
            find_codex=lambda _explicit: "/codex",
        )
        with patch.object(self.validation, "load_diagnostics", return_value=diagnostics), \
                patch.object(self.validation, "query_installed_skill", return_value={"status": "passed"}):
            report = self.validation.build_report(self.root)
        self.assertFalse(report["ok"])
        self.assertEqual(report["actual_task"]["status"], "not_run")
        self.assertEqual(report["model_requests"], 0)
        self.assertFalse(report["zcode_started"])

    def test_main_preserves_failure_report_and_does_not_overwrite(self):
        output = self.root / "failed-report.json"
        report = {"ok": False, "model_requests": 0}
        with patch.object(self.validation, "build_report", return_value=report):
            self.assertEqual(self.validation.main(["--output", str(output)]), 1)
            self.assertEqual(self.validation.main(["--output", str(output)]), 2)
        self.assertEqual(json.loads(output.read_text()), report)


if __name__ == "__main__":
    unittest.main()
