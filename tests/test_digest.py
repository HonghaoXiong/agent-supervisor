"""Digest contract tests: explicit failures preserved, gates fail closed."""

import json
from pathlib import Path
import tempfile
import unittest

from zcode_supervisor import digest as D

FULL = dict.fromkeys(D.REQUIRED_BINDINGS, "a" * 64)
FULL["profile.json"] = "b" * 64
BASELINES = dict(FULL)


def bindings(**overrides):
    value = dict(FULL)
    value.update(overrides)
    return {k: v for k, v in value.items() if v is not None}


def write_run(directory, name, payload):
    path = Path(directory) / name
    path.write_text(json.dumps(payload, ensure_ascii=False))
    return path


class DigestBaselineGateTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.dir = Path(temporary.name)
        self.run = write_run(self.dir, "run-t.json", {
            "outcome": "delivered", "delivery_ok": True, "session_id": "s1",
            "turn_id": "t1", "process_exit_code": 0, "commands": []})

    def check(self, **kwargs):
        args = {"baselines": BASELINES, "bindings": dict(FULL)}
        args.update(kwargs)
        return D.digest(self.run, events_path=self.dir / "missing.jsonl", **args)

    def test_clean_digest_requires_full_binding_match(self):
        d = self.check()
        self.assertEqual(d["mode"], "digest")
        self.assertNotIn("review_reasons", d)

    def test_empty_bindings_is_full_review(self):
        d = self.check(bindings={})
        self.assertEqual(d["mode"], "full-review")
        self.assertIn("baseline-mismatch", d["review_reasons"])
        self.assertIn("protocol.py", d["baseline_missing"])

    def test_missing_required_key_is_full_review(self):
        partial = dict(FULL)
        del partial["profile.json"]
        d = self.check(bindings=partial)
        self.assertEqual(d["mode"], "full-review")
        self.assertIn("profile.json", d["baseline_missing"])

    def test_unreviewed_extra_binding_is_full_review(self):
        d = self.check(bindings=bindings(runtime_bundle="deadbeef"))
        self.assertEqual(d["mode"], "full-review")
        self.assertEqual(d["baseline_unreviewed_extras"], ["runtime_bundle"])

    def test_no_baselines_is_full_review(self):
        d = self.check(baselines=None)
        self.assertEqual(d["mode"], "full-review")
        self.assertIn("bindings-or-baselines-not-captured", d["review_reasons"])


class DigestCompletenessTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.dir = Path(temporary.name)

    def digest_of(self, payload):
        run = write_run(self.dir, "run-c.json", payload)
        return D.digest(run, events_path=self.dir / "missing.jsonl",
                        baselines=BASELINES, bindings=dict(FULL))

    def test_error_is_preserved_bounded_with_flag(self):
        d = self.digest_of({"outcome": "preflight-rejected", "delivery_ok": False,
                            "session_id": None, "process_exit_code": 0,
                            "commands": [],
                            "error": "Provider Registry 中不存在 Model: "
                                     + "x" * 400})
        self.assertNotEqual(d["failures"]["error_class"], "unknown")
        self.assertTrue(d["failures"]["error_excerpt"])
        self.assertTrue(d["failures"]["error_excerpt_truncated"])  # 截断显式标记
        self.assertEqual(d["mode"], "digest")  # preflight 无会话合法

    def test_turn_bearing_run_missing_turn_id_is_full_review(self):
        d = self.digest_of({"outcome": "delivered", "delivery_ok": True,
                            "session_id": "s1", "turn_id": None, "commands": []})
        self.assertEqual(d["mode"], "full-review")
        self.assertIn("turn_id:type-or-missing", d["completeness_issues"])

    def test_turn_id_and_session_are_preserved(self):
        d = self.digest_of({"outcome": "delivered", "delivery_ok": True,
                            "session_id": "sess_x", "turn_id": "turn_y",
                            "commands": []})
        self.assertEqual((d["session_id"], d["turn_id"]), ("sess_x", "turn_y"))

    def test_command_fields_all_unknown_is_full_review(self):
        d = self.digest_of({"outcome": "delivered", "delivery_ok": True,
                            "session_id": "s1", "turn_id": "t1",
                            "commands": [{"command_id": "c1", "result": {}}]})
        self.assertEqual(d["mode"], "full-review")
        self.assertIn("command-fields:c1", d["completeness_issues"])

    def test_denial_reason_truncation_flagged(self):
        reason = "Not in this task contract: " + "z" * 300
        d = self.digest_of({"outcome": "evidence-rejected", "delivery_ok": False,
                            "session_id": "s1", "turn_id": "t1",
                            "permission_decisions": [
                                {"toolName": "Bash", "decision": "deny",
                                 "reason": reason}]})
        deny = d["failures"]["denials"][0]
        self.assertEqual(deny["tool"], "Bash")
        self.assertTrue(deny["reason_truncated"])


class DeltaTests(unittest.TestCase):
    def test_null_and_absent_are_distinct(self):
        changes = D.delta({"field": None}, {})
        self.assertEqual(len(changes), 1)
        self.assertEqual(changes[0], {"path": "field", "from": None,
                                      "to": "<absent>"})

    def test_added_and_removed_leaves_are_reported(self):
        changes = D.delta({"a": 1}, {"b": 2})
        paths = {c["path"] for c in changes}
        self.assertEqual(paths, {"a", "b"})

    def test_structural_equality_is_not_a_verdict(self):
        payload = {"outcome": "delivered", "delivery_ok": True,
                   "session_id": "s", "turn_id": "t", "commands": []}
        with tempfile.TemporaryDirectory() as tmp:
            run = write_run(tmp, "r.json", payload)
            kwargs = {"baselines": BASELINES, "bindings": dict(FULL)}
            a = D.digest(run, **kwargs)
            b = D.digest(run, **kwargs)
        self.assertEqual(D.delta(a, b), [])  # 相等 ≠ 验收通过（契约明示）


class BaselineDependencyAndFormatTests(unittest.TestCase):
    """基线声明的关键依赖也必检；身份值必须是 64 位小写十六进制。"""

    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.dir = Path(temporary.name)
        self.run = write_run(self.dir, "r.json", {
            "outcome": "delivered", "delivery_ok": True, "session_id": "s",
            "turn_id": "t", "commands": []})

    def test_baseline_declared_dependency_missing_from_bindings(self):
        declared = dict(BASELINES)
        declared["packet.json"] = "c" * 64  # 基线多声明 packet 依赖
        d = D.digest(self.run, events_path=self.dir / "m.jsonl",
                     baselines=declared, bindings=dict(FULL))
        self.assertEqual(d["mode"], "full-review")
        self.assertIn("packet.json", d["baseline_missing"])

    def test_both_sides_invalid_non_sha_value_is_full_review(self):
        bad = dict(BASELINES)
        bad["runtime.py"] = "not-a-sha256"
        bindings = dict(FULL)
        bindings["runtime.py"] = "not-a-sha256"  # 双方一致但非法
        d = D.digest(self.run, events_path=self.dir / "m.jsonl",
                     baselines=bad, bindings=bindings)
        self.assertEqual(d["mode"], "full-review")
        self.assertIn("runtime.py", d["baseline_invalid_format"])


class IllTypedFieldTests(unittest.TestCase):
    """类型/已知终态校验：ill-typed 回完整审查，摘要不改 verdict。"""

    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.dir = Path(temporary.name)

    def digest_of(self, payload):
        run = write_run(self.dir, "r.json", payload)
        return D.digest(run, events_path=self.dir / "m.jsonl",
                        baselines=BASELINES, bindings=dict(FULL))

    def test_string_delivery_ok_and_numeric_outcome_rejected(self):
        d = self.digest_of({"outcome": 99, "delivery_ok": "true",
                            "session_id": "s", "turn_id": "t", "commands": []})
        self.assertEqual(d["mode"], "full-review")
        self.assertIn("outcome:type-or-value", d["completeness_issues"])
        self.assertIn("delivery_ok:type", d["completeness_issues"])
        self.assertEqual(d["delivery_ok"], "true")  # verdict 原样保留

    def test_non_string_session_and_object_turn_rejected(self):
        d = self.digest_of({"outcome": "delivered", "delivery_ok": True,
                            "session_id": 123, "turn_id": {"x": 1},
                            "commands": []})
        self.assertEqual(d["mode"], "full-review")
        issues = d["completeness_issues"]
        self.assertIn("session_id:type-or-missing", issues)
        self.assertIn("turn_id:type-or-missing", issues)

    def test_bool_exit_code_and_string_timed_out_rejected(self):
        detail = {"perf": {"detail": {"command": {
            "exitCode": False, "timedOut": "false", "status": 123}}}}
        d = self.digest_of({"outcome": "delivered", "delivery_ok": True,
                            "session_id": "s", "turn_id": "t",
                            "commands": [{"command_id": "c1",
                                          "result": detail}]})
        self.assertEqual(d["mode"], "full-review")
        issues = d["completeness_issues"]
        self.assertIn("command-fields:c1:exit_code:type", issues)
        self.assertIn("command-fields:c1:timed_out:type", issues)
        self.assertIn("command-fields:c1:status:unknown-type-or-missing", issues)


class ExitEvidenceCompletenessTests(unittest.TestCase):
    """退出证据完整：三件套单独 unknown/错型、进程退出码、畸形容器统一受控。"""

    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.dir = Path(temporary.name)

    def digest_of(self, payload):
        run = write_run(self.dir, "r.json", payload)
        return D.digest(run, events_path=self.dir / "m.jsonl",
                        baselines=BASELINES, bindings=dict(FULL))

    def payload_with_command(self, command_detail, **extra):
        payload = {"outcome": "delivered", "delivery_ok": True,
                   "session_id": "s", "turn_id": "t",
                   "process_exit_code": 0,
                   "commands": [{"command_id": "c1", "result":
                       {"perf": {"detail": {"command": command_detail}}}}]}
        payload.update(extra)
        return payload

    def test_unknown_status_alone_is_full_review(self):
        d = self.digest_of(self.payload_with_command(
            {"exitCode": 0, "timedOut": False, "status": "unknown"}))
        self.assertEqual(d["mode"], "full-review")
        self.assertIn("command-fields:c1:status:unknown-type-or-missing",
                      d["completeness_issues"])

    def test_missing_or_boolean_process_exit_is_full_review(self):
        missing = self.payload_with_command(
            {"exitCode": 0, "timedOut": False, "status": "completed"})
        del missing["process_exit_code"]
        for payload in (missing,
                        self.payload_with_command(
                            {"exitCode": 0, "timedOut": False, "status": "completed"},
                            process_exit_code=False),
                        self.payload_with_command(
                            {"exitCode": 0, "timedOut": False, "status": "completed"},
                            process_exit_code="0")):
            d = self.digest_of(payload)
            self.assertEqual(d["mode"], "full-review")
            self.assertIn("process_exit_code:type-or-missing",
                          d["completeness_issues"])

    def test_malformed_result_containers_degrade_not_raise(self):
        for result in (None, "text", 42, {"perf": None},
                       {"perf": {"detail": "broken"}}):
            payload = {"outcome": "delivered", "delivery_ok": True,
                       "session_id": "s", "turn_id": "t",
                       "process_exit_code": 0,
                       "commands": [{"command_id": "c1", "result": result}]}
            d = self.digest_of(payload)  # 不得 AttributeError
            self.assertEqual(d["commands"][0]["exit_code"], "unknown")
            self.assertEqual(d["mode"], "full-review")

    def test_non_dict_command_entry_degrades_not_raises(self):
        payload = {"outcome": "delivered", "delivery_ok": True,
                   "session_id": "s", "turn_id": "t",
                   "process_exit_code": 0, "commands": ["not-a-dict"]}
        d = self.digest_of(payload)
        self.assertEqual(d["mode"], "full-review")


class CliProvenanceTests(unittest.TestCase):
    """CLI 身份取自明确配置的 cli_path 实际字节，不扫描缓存。"""

    def test_identity_follows_configured_cli_bytes_not_stale_cache(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        (root / ".czs").mkdir(parents=True)
        cli = root / "newly-selected-cli.cjs"
        cli.write_bytes(b"NEW CLI BYTES")
        (root / ".czs" / "config.json").write_text(
            '{"cli_path": "%s"}' % cli)
        stale = root / ".czs" / "cache" / "zcode-old"  # 旧 cache 并存
        stale.mkdir(parents=True)
        (stale / "manifest.json").write_text(
            '{"source_sha256": "%s"}' % ("b" * 64))
        import hashlib
        expected = hashlib.sha256(b"NEW CLI BYTES").hexdigest()
        self.assertEqual(D._bundle_sha(root), expected)

    def test_unresolvable_config_is_unknown(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.assertEqual(D._bundle_sha(Path(tmp)), "unknown")


if __name__ == "__main__":
    unittest.main()
