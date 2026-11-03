"""P-B/P-C (0.2.5): skills field closure, events cap, pin candidate,
codex attach/stop, optional jsonschema upgrade."""

import json
import tempfile
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace

from zcode_supervisor import contracts, hub_server as H


def _zcode_contract(workspace, **overrides):
    contract = {
        "contract_id": "zc-sk", "worker": "zcode",
        "workspace": str(workspace), "prompt": "use the skill",
        "allowed_files": [], "commands": [],
        "model": {"providerId": "glm", "modelId": "glm-5.3"},
        "max_timeout_ms": 60_000, "budget": {},
    }
    contract.update(overrides)
    return contract


class SkillsFieldValidation(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.ws = Path(self.tmp.name) / "ws"
        self.ws.mkdir()

    def tearDown(self):
        self.tmp.cleanup()

    def _validate(self, **overrides):
        return contracts.validate_contract(
            _zcode_contract(self.ws, **overrides))

    def test_valid_skills_register(self):
        contract = self._validate(skills=["codex-monitor", "graphify"])
        self.assertEqual(contract["skills"], ["codex-monitor", "graphify"])

    def test_rejects_non_zcode_workers(self):
        contract = _zcode_contract(self.ws, skills=["x"], worker="dsh")
        with self.assertRaises(contracts.ContractError) as caught:
            contracts.validate_contract(contract)
        self.assertIn("zcode-only", str(caught.exception))

    def test_rejects_bad_shapes(self):
        for bad in ([], "codex-monitor", ["a"] * 9, ["a", "a"],
                    [" padded "], [""]):
            with self.assertRaises(contracts.ContractError):
                self._validate(skills=bad)


class SkillsDispatchClosure(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.config = SimpleNamespace(state_dir=self.root / "state",
                                      work_root=self.root / "work")
        for name in ("state", "work"):
            (self.root / name).mkdir(exist_ok=True)
        self.evidence = self.root / "ev"
        self.workspace = self.root / "ws"
        self.workspace.mkdir()
        self.initiator = {"agent": "codex", "session_id": "s"}

    def tearDown(self):
        self.tmp.cleanup()

    def _register(self, **overrides):
        path = self.config.state_dir / "c.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        contract = _zcode_contract(self.workspace, **overrides)
        path.write_text(json.dumps(contract))
        contracts.register_contract(self.config.state_dir, path)
        return contract["contract_id"]

    def test_unknown_skill_refused_at_dispatch(self):
        contract_id = self._register(skills=["definitely-not-installed"])
        async def runner(*a, **k):
            raise AssertionError("must not run")
        import asyncio
        with self.assertRaises(H.HubError) as caught:
            asyncio.run(H.delegate_impl(
                self.config, self.evidence, contract_id, self.initiator,
                zcode_runner=runner))
        self.assertIn("not installed on this zcode worker",
                      str(caught.exception))

    def test_known_skill_dispatches_with_record_and_receipt(self):
        contract_id = self._register(skills=["codex-monitor"])
        async def runner(config, task_id, contract, timeout_s=None,
                         ui_emitter=None, resume=None):
            return {"delivery_ok": True, "session_id": "sess_x",
                    "outcome": "delivered", "run_path": None, "db_path": None}
        import asyncio
        result = asyncio.run(H.delegate_impl(
            self.config, self.evidence, contract_id, self.initiator,
            zcode_runner=runner))
        self.assertEqual(result["skills_enabled"], ["codex-monitor"])
        record = json.loads((self.config.state_dir / "hub" / "tasks"
                             / (result["task_id"] + ".json")).read_text())
        self.assertEqual(record["skills"], ["codex-monitor"])


class EventsCap(unittest.TestCase):
    def test_wire_write_caps_with_single_marker(self):
        from zcode_supervisor.protocol import ProtocolClient
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "run.json"
            client = ProtocolClient(Path(tmp), out, lambda *a, **k: {},
                                    SimpleNamespace())
            client.wire = (Path(tmp) / "run.events.jsonl").open("w")
            client._events_cap_bytes = 120
            client._events_capped = False
            for index in range(10):
                client._wire_write({"seq": index, "pad": "x" * 30})
            client.wire.close()
            text = (Path(tmp) / "run.events.jsonl").read_text()
            self.assertIn('"events_truncated": true', text)
            self.assertEqual(text.count("events_truncated"), 1)
            # 断流后所有后续写入被丢弃
            self.assertNotIn('"seq": 9', text)

    def test_no_cap_keeps_legacy_behavior(self):
        from zcode_supervisor.protocol import ProtocolClient
        with tempfile.TemporaryDirectory() as tmp:
            out = Path(tmp) / "run.json"
            client = ProtocolClient(Path(tmp), out, lambda *a, **k: {},
                                    SimpleNamespace())
            client.wire = (Path(tmp) / "run.events.jsonl").open("w")
            client._events_cap_bytes = 0
            client._events_capped = False
            for index in range(50):
                client._wire_write({"seq": index, "pad": "x" * 30})
            client.wire.close()
            text = (Path(tmp) / "run.events.jsonl").read_text()
            self.assertIn('"seq": 49', text)


class PinCandidate(unittest.TestCase):
    def test_static_candidate_from_fake_bundle(self):
        from zcode_supervisor import runtime
        with tempfile.TemporaryDirectory() as tmp:
            bundle = Path(tmp) / "zcode.cjs"
            fragment = runtime.FRAGMENT
            body = b"console.log(1);\n" + fragment + b"\nconsole.log(2);\n"
            bundle.write_bytes(body)
            config = SimpleNamespace(cli_path=bundle, node="node")
            candidate = runtime.pin_candidate(config)
            import hashlib
            self.assertTrue(candidate["fragment_ok"])
            self.assertEqual(candidate["bundle_sha256"],
                             hashlib.sha256(body).hexdigest())
            self.assertEqual(candidate["patched_sha256"],
                             hashlib.sha256(body.replace(fragment, b"", 1))
                             .hexdigest())
            self.assertIn("STATIC CANDIDATE ONLY", candidate["note"])

    def test_bad_fragment_count_flags_not_ok(self):
        from zcode_supervisor import runtime
        with tempfile.TemporaryDirectory() as tmp:
            bundle = Path(tmp) / "zcode.cjs"
            bundle.write_bytes(runtime.FRAGMENT + runtime.FRAGMENT)
            candidate = runtime.pin_candidate(SimpleNamespace(
                cli_path=bundle, node="node"))
            self.assertFalse(candidate["fragment_ok"])
            self.assertNotIn("patched_sha256", candidate)


class CodexResumeAndStop(unittest.TestCase):
    def test_build_argv_resume_uses_verified_flags_only(self):
        from zcode_supervisor.codex_driver import build_argv
        argv = build_argv("codex", Path("/w"), Path("/s.json"),
                          Path("/o.txt"), model="m1", resume="thread-9")
        self.assertEqual(argv[:4], ["codex", "exec", "resume", "thread-9"])
        self.assertNotIn("--output-schema", argv)
        self.assertNotIn("-s", argv)
        self.assertEqual(argv[-1], "-")

    def test_stop_event_yields_stopped_outcome(self):
        import time
        from zcode_supervisor import codex_driver
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            workspace = root / "ws"
            workspace.mkdir()
            fake = root / "codex"
            fake.write_text("#!/bin/sh\ncat >/dev/null\nsleep 60\n")
            fake.chmod(0o755)
            event = threading.Event()
            threading.Timer(1.0, event.set).start()
            started = time.monotonic()
            receipt = codex_driver.run_readonly(
                fake, workspace, "task",
                {"type": "object", "properties": {"a": {"type": "string"}},
                 "required": ["a"]},
                root / "ev", timeout_s=50, stop_event=event)
            self.assertEqual(receipt.outcome, "stopped")
            self.assertLess(time.monotonic() - started, 30)


class JsonSchemaOptional(unittest.TestCase):
    def test_schema_problems_catch_malformed_with_library(self):
        try:
            import jsonschema  # noqa: F401
        except ImportError:
            self.skipTest("jsonschema not installed")
        problems = contracts._output_schema_problems(
            {"type": "object", "properties": {
                "a": {"type": "strin"}},  # typo: invalid type keyword
             "required": ["a"]}, "output_schema")
        self.assertTrue(any("not a valid JSON Schema" in p for p in problems))

    def test_valid_schema_passes_both_paths(self):
        good = {"type": "object",
                "properties": {"a": {"type": "string"}},
                "required": ["a"], "additionalProperties": False}
        self.assertEqual(contracts._output_schema_problems(good, "s"), [])

    def test_instance_validation_stricter_with_library(self):
        try:
            import jsonschema  # noqa: F401
        except ImportError:
            self.skipTest("jsonschema not installed")
        from zcode_supervisor.codex_driver import _load_structured
        with tempfile.TemporaryDirectory() as tmp:
            msg = Path(tmp) / "last.json"
            msg.write_text(json.dumps({"a": 123}))  # wrong TYPE: old path passed
            payload, error = _load_structured(
                msg, {"type": "object",
                      "properties": {"a": {"type": "string"}},
                      "required": ["a"]})
            self.assertIsNone(payload)
            self.assertIn("failed schema", error)


if __name__ == "__main__":
    unittest.main()
