"""CodexDriver minimal-surface tests against a fake `codex` executable.

No real `codex exec` is ever invoked: the fake script validates the argv
contract (read-only sandbox, --ephemeral forbidden, prompt on stdin) and
emits canned JSONL events, with environment-switched failure modes.
"""

import dataclasses
import json
import tempfile
import unittest
from unittest import mock
from pathlib import Path

from zcode_supervisor import codex_driver

FAKE_THREAD_ID = "11111111-2222-3333-4444-555555555555"

FAKE_CODEX_SOURCE = '''#!/usr/bin/env python3
"""Fake codex CLI: validates the driver argv contract, emits canned events."""
import json
import os
import sys
import time

argv = sys.argv[1:]
THREAD_ID = "__FAKE_THREAD_ID__"


def fail(message):
    sys.stderr.write("fake-codex: %s\\n" % message)
    sys.exit(3)


def value_of(flag):
    return argv[argv.index(flag) + 1] if flag in argv else None

for token in ("exec", "--json", "read-only", "--skip-git-repo-check", "-"):
    if token not in argv:
        fail("missing argv token %r" % token)
if "--ephemeral" in argv:
    fail("--ephemeral must never be passed")
if "-s" not in argv or value_of("-s") != "read-only":
    fail("-s must be followed by read-only")
if argv[-1] != "-":
    fail("prompt must be requested from stdin via trailing '-'")
schema_file = value_of("--output-schema")
last_message = value_of("-o")
if not schema_file or not last_message:
    fail("--output-schema and -o are both required")
try:
    with open(schema_file) as handle:
        json.load(handle)
except OSError:
    fail("output-schema file %s was not written before launch" % schema_file)

task = sys.stdin.read()

diag_path = os.environ.get("FAKE_CODEX_DIAG")
if diag_path:
    with open(diag_path, "w") as handle:
        json.dump({
            "argv": argv,
            "task": task,
            "OPENBLAS_NUM_THREADS": os.environ.get("OPENBLAS_NUM_THREADS"),
            "OMP_NUM_THREADS": os.environ.get("OMP_NUM_THREADS"),
            "CODEX_HOME": os.environ.get("CODEX_HOME"),
        }, handle)

mode = os.environ.get("FAKE_CODEX_MODE", "delivered")


def emit(events):
    for event in events:
        sys.stdout.write(json.dumps(event) + "\\n")
    sys.stdout.flush()

if mode == "timeout":
    emit([{"type": "thread.started", "thread_id": THREAD_ID}])
    time.sleep(10)
    sys.exit(0)

if mode == "noisy":
    # Transport-layer retries plus one genuine error event (run-2 shape).
    emit([
        {"type": "thread.started", "thread_id": THREAD_ID},
        {"type": "turn.started", "model": "fake-model-x"},
        {"type": "error", "message": "Reconnecting... 1/5 (request timed out)"},
        {"type": "error", "message": "Reconnecting... 2/5 (request timed out)"},
        {"type": "error", "message": "stream disconnected before completion"},
        {"type": "turn.completed"},
    ])
    with open(last_message, "w") as handle:
        handle.write(json.dumps({"summary": "ok", "files": ["a"]}))
    sys.exit(0)

emit([
    {"type": "thread.started", "thread_id": THREAD_ID},
    {"type": "turn.started", "model": "fake-model-x"},
    {"type": "item.completed", "item": {"type": "command_execution", "command": "ls",
                                        "exit_code": 0, "status": "completed"}},
    {"type": "totally.unknown", "surprise": True},
    {"type": "turn.completed"},
])

if mode == "fail":
    sys.stderr.write("fake-codex simulated crash\\n")
    sys.exit(2)

payload = {"summary": "ok", "files": ["a"]}
if mode == "mismatch":
    payload = {"summary": "ok"}
with open(last_message, "w") as handle:
    handle.write(json.dumps(payload) if mode != "badjson" else "{not json")
sys.exit(0)
'''.replace("__FAKE_THREAD_ID__", FAKE_THREAD_ID)


class CodexDriverTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.workspace = self.root / "workspace"
        self.workspace.mkdir()
        self.codex = self.root / "fake-codex"
        self.codex.write_text(FAKE_CODEX_SOURCE)
        self.codex.chmod(0o755)
        self.schema = {
            "type": "object",
            "properties": {
                "summary": {"type": "string"},
                "files": {"type": "array"},
            },
            "required": ["summary", "files"],
        }

    def _run(self, mode="delivered", timeout_s=30.0, model=None):
        evidence = self.root / ("evidence-" + mode)
        env = {
            "FAKE_CODEX_MODE": mode,
            "CODEX_HOME": str(self.root / "codex-home"),
            "FAKE_CODEX_DIAG": str(self.root / ("diag-" + mode + ".json")),
        }
        receipt = codex_driver.run_readonly(
            self.codex,
            self.workspace,
            "do the read-only task",
            self.schema,
            evidence,
            timeout_s=timeout_s,
            model=model,
            env=env,
        )
        return receipt, evidence

    def test_build_argv_has_fixed_readonly_contract_without_ephemeral(self):
        argv = codex_driver.build_argv(
            "/usr/local/bin/codex", Path("/tmp/ws"), Path("/tmp/s.json"), Path("/tmp/lm.json")
        )
        self.assertEqual(argv[:3], ["/usr/local/bin/codex", "exec", "--json"])
        self.assertEqual(argv[argv.index("-s") + 1], "read-only")
        self.assertEqual(argv[argv.index("-C") + 1], "/tmp/ws")
        self.assertIn("--skip-git-repo-check", argv)
        self.assertEqual(argv[argv.index("--color") + 1], "never")
        self.assertEqual(argv[argv.index("--output-schema") + 1], "/tmp/s.json")
        self.assertEqual(argv[argv.index("-o") + 1], "/tmp/lm.json")
        self.assertEqual(argv[-1], "-")
        self.assertNotIn("--ephemeral", argv)
        self.assertNotIn("-m", argv)

        argv_model = codex_driver.build_argv(
            "/usr/local/bin/codex", Path("/tmp/ws"), Path("/tmp/s.json"),
            Path("/tmp/lm.json"), model="gpt-5.3-codex",
        )
        self.assertEqual(argv_model[argv_model.index("-m") + 1], "gpt-5.3-codex")
        self.assertEqual(argv_model[-1], "-")
        self.assertNotIn("--ephemeral", argv_model)

    def test_delivered_run_full_chain(self):
        receipt, evidence = self._run()
        self.assertTrue(receipt.ok)
        self.assertEqual(receipt.outcome, "delivered")
        self.assertEqual(receipt.exit_code, 0)
        self.assertEqual(receipt.thread_id, FAKE_THREAD_ID)
        self.assertEqual(receipt.structured, {"summary": "ok", "files": ["a"]})
        self.assertEqual(
            receipt.event_counts,
            {
                "thread.started": 1,
                "turn.started": 1,
                "item.completed": 1,
                "totally.unknown": 1,
                "turn.completed": 1,
            },
        )
        self.assertEqual(
            receipt.command_items,
            [{"command": "ls", "exit_code": 0, "status": "completed"}],
        )
        self.assertEqual(receipt.model_observed, "fake-model-x")
        self.assertEqual(receipt.sandbox, "read-only")
        self.assertIsNone(receipt.error)
        self.assertGreaterEqual(receipt.duration_s, 0.0)

        events_file = Path(receipt.events_path)
        self.assertTrue(events_file.is_file())
        raw_lines = events_file.read_text().splitlines()
        self.assertEqual(len(raw_lines), 5)
        self.assertEqual(json.loads(raw_lines[0]), {"type": "thread.started", "thread_id": FAKE_THREAD_ID})
        self.assertTrue(Path(receipt.last_message_path).is_file())

        saved = json.loads((evidence / "receipt.json").read_text())
        self.assertEqual(saved, dataclasses.asdict(receipt))
        self.assertTrue((evidence / "output-schema.json").is_file())

        diag = json.loads((self.root / "diag-delivered.json").read_text())
        self.assertEqual(diag["task"], "do the read-only task")
        self.assertEqual(diag["OPENBLAS_NUM_THREADS"], "1")
        self.assertEqual(diag["OMP_NUM_THREADS"], "1")
        self.assertTrue(diag["CODEX_HOME"].endswith("codex-home"))

    def test_timeout_run_terminates_and_keeps_persisted_events(self):
        receipt, evidence = self._run(mode="timeout", timeout_s=1.5)
        self.assertFalse(receipt.ok)
        self.assertEqual(receipt.outcome, "timeout")
        self.assertIn("timed out", receipt.error)
        self.assertIsNotNone(receipt.exit_code)
        self.assertNotEqual(receipt.exit_code, 0)
        self.assertGreaterEqual(receipt.duration_s, 1.5)
        self.assertLess(receipt.duration_s, 9.0)
        self.assertIsNone(receipt.structured)
        self.assertEqual(receipt.event_counts.get("thread.started"), 1)
        persisted = Path(receipt.events_path).read_text().splitlines()
        self.assertGreaterEqual(len(persisted), 1)
        self.assertTrue((evidence / "receipt.json").is_file())

    def test_nonzero_exit_is_failed_with_stderr_excerpt(self):
        receipt, _ = self._run(mode="fail")
        self.assertFalse(receipt.ok)
        self.assertEqual(receipt.outcome, "failed")
        self.assertEqual(receipt.exit_code, 2)
        self.assertIn("fake-codex simulated crash", receipt.error)
        self.assertIsNone(receipt.structured)
        self.assertEqual(receipt.event_counts.get("turn.completed"), 1)

    def test_missing_required_key_is_schema_mismatch(self):
        receipt, _ = self._run(mode="mismatch")
        self.assertFalse(receipt.ok)
        self.assertEqual(receipt.outcome, "schema_mismatch")
        self.assertIsNone(receipt.structured)
        self.assertIn("files", receipt.error)
        self.assertEqual(json.loads(Path(receipt.last_message_path).read_text()), {"summary": "ok"})

    def test_invalid_json_last_message_is_schema_mismatch(self):
        receipt, _ = self._run(mode="badjson")
        self.assertFalse(receipt.ok)
        self.assertEqual(receipt.outcome, "schema_mismatch")
        self.assertIn("not valid JSON", receipt.error)

    def test_run_readonly_rejects_driver_misuse(self):
        evidence = self.root / "evidence-misuse"
        with self.assertRaises(codex_driver.CodexExecError):
            codex_driver.run_readonly(self.codex, self.workspace, "  ", self.schema, evidence)
        with self.assertRaises(codex_driver.CodexExecError):
            codex_driver.run_readonly(
                self.codex, self.workspace, "task", ["not", "a", "schema"], evidence
            )
        with self.assertRaises(codex_driver.CodexExecError):
            codex_driver.run_readonly(
                self.codex, self.workspace, "task", self.schema, evidence, timeout_s=0
            )

    def test_transport_retries_are_separated_from_hard_errors(self):
        receipt, _ = self._run(mode="noisy")
        self.assertTrue(receipt.ok)
        self.assertEqual(receipt.outcome, "delivered")
        self.assertEqual(receipt.transport_retries, 2)
        self.assertEqual(len(receipt.hard_errors), 1)
        self.assertIn("stream disconnected", receipt.hard_errors[0])
        # event_counts keeps the raw counts: all three error events counted
        self.assertEqual(receipt.event_counts.get("error"), 3)
        saved = json.loads((self.root / "evidence-noisy" / "receipt.json").read_text())
        self.assertEqual(saved["transport_retries"], 2)
        self.assertEqual(len(saved["hard_errors"]), 1)

    def test_schema_is_strict_normalized_by_default(self):
        receipt, evidence = self._run()
        self.assertTrue(receipt.schema_normalized)  # setUp schema is loose
        written = json.loads((evidence / "output-schema.json").read_text())
        self.assertIs(written["additionalProperties"], False)
        self.assertEqual(written["required"], ["summary", "files"])
        # the receipt round-trips the new fields
        self.assertEqual(receipt.transport_retries, 0)
        self.assertEqual(receipt.hard_errors, [])

    def test_strict_schema_false_keeps_schema_verbatim(self):
        evidence = self.root / "evidence-verbatim"
        codex_driver.run_readonly(
            self.codex, self.workspace, "task", self.schema, evidence,
            env={"FAKE_CODEX_MODE": "delivered"}, strict_schema=False)
        written = json.loads((evidence / "output-schema.json").read_text())
        self.assertEqual(written, self.schema)
        receipt = json.loads((evidence / "receipt.json").read_text())
        self.assertFalse(receipt["schema_normalized"])


class StreamingEvidenceTests(unittest.TestCase):
    """P2.3: streaming stdout/stderr evidence and incremental parsing."""

    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.workspace = self.root / "workspace"
        self.workspace.mkdir()
        self.schema = {"type": "object",
                       "properties": {"summary": {"type": "string"},
                                      "files": {"type": "array"}},
                       "required": ["summary", "files"]}

    def _fake(self, source):
        fake = self.root / "fake-codex"
        fake.write_text(source)
        fake.chmod(0o755)
        return fake

    def _run(self, fake, mode, timeout_s=30.0, **kwargs):
        evidence = self.root / ("evidence-" + mode)
        env = {"FAKE_CODEX_MODE": mode,
               "FX_MODE": mode,
               "CODEX_HOME": str(self.root / "home")}
        return codex_driver.run_readonly(
            fake, self.workspace, "task", self.schema, evidence,
            timeout_s=timeout_s, env=env, **kwargs), evidence

    def test_accumulator_matches_reference_parser_on_edge_lines(self):
        lines = [
            '{"type": "thread.started", "thread_id": "t1"}',
            "{not json",
            "[1, 2]",
            '{"no_type": 1}',
            "null",
            '{"type": "error", "message": "Reconnecting... 1/5"}',
            '{"type": "error", "message": "real failure"}',
            '{"type": "item.completed", "item": {"type": "command_execution", "command": "ls", "exit_code": 0, "status": "completed"}}',
            '{"type": "turn.completed"}',
            '{"type": "turn.completed"}',  # no trailing newline variant
        ]
        counts, thread, model, items, retries, hard = codex_driver \
            ._parse_events(lines)
        self.assertEqual(counts, {"thread.started": 1, "<invalid-json>": 1,
                                  "<non-object>": 2, "<no-type>": 1,
                                  "error": 2, "item.completed": 1,
                                  "turn.completed": 2})
        self.assertEqual(thread, "t1")
        self.assertEqual(retries, 1)
        self.assertEqual(hard, ["real failure"])
        self.assertEqual(items, [{"command": "ls", "exit_code": 0,
                                  "status": "completed"}])
        # incremental feed order must not matter for the result
        acc = codex_driver._EventAccumulator()
        for line in reversed(lines):
            acc.feed(line)
        self.assertEqual(acc.result()[0], counts)

    def test_tail_frames_parse_identically(self):
        """A complete final line without newline parses; a truncated JSON
        tail counts as <invalid-json> — the acceptance set is exactly the
        old whole-list parser's."""
        complete = '{"type": "a"}\n{"type": "b"}'
        truncated = '{"type": "a"}\n{"type": "b'  # truly broken JSON
        import io
        def as_lines(text):
            return [line for line in io.StringIO(text)]
        self.assertEqual(codex_driver._parse_events(as_lines(complete))[0],
                         {"a": 1, "b": 1})
        self.assertEqual(codex_driver._parse_events(as_lines(truncated))[0],
                         {"a": 1, "<invalid-json>": 1})

    def test_stderr_excerpt_file_matches_old_join_rule(self):
        import tempfile as _tf
        with _tf.TemporaryDirectory() as tmp:
            raw = Path(tmp) / "stderr.partial"
            raw.write_text("  head \n" + "E" * 600 + "\nTAIL\n")
            got = codex_driver._stderr_excerpt_file(raw)
            want = ("  head \n" + "E" * 600 + "\nTAIL\n").strip()[-500:]
            self.assertEqual(got, want)
            self.assertFalse(raw.exists())  # unlinked after read
            # oversized stream: bounded tail read keeps the same excerpt
            big = Path(tmp) / "big.partial"
            big.write_text("X" * (2 * 1024 * 1024) + "\nTAIL-MARKER\n")
            got_big = codex_driver._stderr_excerpt_file(big)
            text = ("X" * (2 * 1024 * 1024) + "\nTAIL-MARKER\n")
            self.assertEqual(got_big, text.strip()[-500:])
            self.assertTrue(got_big.endswith("TAIL-MARKER"))

    def test_active_writer_partial_before_exit_and_one_awaited_exit(self):
        import threading, time
        source = '''#!/usr/bin/env python3
import json, sys, time
for event in ({"type": "thread.started", "thread_id": "t1"},
              {"type": "turn.started", "model": "m"}):
    sys.stdout.write(json.dumps(event) + "\\n")
sys.stdout.flush()
time.sleep(2.5)
sys.stdout.write(json.dumps({"type": "turn.completed"}) + "\\n")
sys.stdout.flush()
last = [a for a in sys.argv[1:] ]
idx = last.index("-o")
with open(last[idx + 1], "w") as h:
    h.write(json.dumps({"summary": "ok", "files": []}))
'''
        fake = self._fake(source)
        evidence = self.root / "evidence-active"
        exits = []
        state = {"done": False}
        observations = []

        def drive():
            try:
                codex_driver.run_readonly(
                    fake, self.workspace, "task", self.schema, evidence,
                    timeout_s=30, env={"FX_MODE": "active",
                                       "CODEX_HOME": str(self.root / "home")},
                    exit_event_sink=exits.append)
            except BaseException as error:  # surfaced by the assertions
                state["error"] = f"{type(error).__name__}: {error}"
            finally:
                state["done"] = True

        worker = threading.Thread(target=drive, daemon=True)
        worker.start()
        deadline = time.monotonic() + 15
        while not state["done"] and time.monotonic() < deadline:
            time.sleep(0.2)
            partial = [p for p in evidence.glob(".codex-events*")
                       if p.is_file() and p.stat().st_size > 0]
            observations.append({
                "partial_lines": (
                    len([l for l in partial[0].read_text().splitlines()
                         if l.strip()]) if partial else 0),
                "receipt": (evidence / "receipt.json").exists(),
                "final_events_nonempty": (
                    (evidence / "codex-events.jsonl").stat().st_size > 0
                    if (evidence / "codex-events.jsonl").exists() else False)})
        worker.join(timeout=35)
        self.assertNotIn("error", state, state.get("error"))
        self.assertTrue(any(o["partial_lines"] >= 2 and not o["receipt"]
                            for o in observations),
                        "no flushed partial observed before finalization: "
                        + repr(observations))
        # exactly one awaited exit, and only AFTER the run settled
        self.assertEqual(len(exits), 1)
        final = json.loads((evidence / "receipt.json").read_text())
        self.assertEqual(final["outcome"], "delivered")
        self.assertIn("turn.completed",
                      final["event_counts"])

    def test_metrics_sink_reports_partial_and_receipt_schema_is_untouched(self):
        receipt, evidence = self._run(
            self._fake(FAKE_CODEX_SOURCE), "delivered")
        metrics = {}
        receipt2, _ = self._run(self._fake(FAKE_CODEX_SOURCE),
                                "delivered-metrics", metrics_sink=metrics)
        self.assertIn("first_partial_s", metrics)
        self.assertIsNotNone(metrics["first_partial_s"])
        self.assertGreater(metrics["raw_stdout_bytes"], 0)
        self.assertIn("receipt_meta", metrics)
        # receipt dataclass fields unchanged by the metrics hook
        self.assertEqual(sorted(dataclasses.asdict(receipt)),
                         sorted(dataclasses.asdict(receipt2)))


class Repair1EvidenceIntegrityTests(unittest.TestCase):
    """P2.3 repair1: finalize immutability, honest pumps, exact excerpt."""

    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.workspace = self.root / "workspace"
        self.workspace.mkdir()
        self.schema = {"type": "object",
                       "properties": {"summary": {"type": "string"}},
                       "required": ["summary"]}

    def _fake(self, body):
        fake = self.root / ("fake-%d" % id(body))
        fake.write_text("#!/usr/bin/env python3\n" + body)
        fake.chmod(0o755)
        return fake

    def _run(self, fake, timeout_s=30.0, **kwargs):
        evidence = self.root / ("evidence-%d" % id(fake))
        return codex_driver.run_readonly(
            fake, self.workspace, "task", self.schema, evidence,
            timeout_s=timeout_s,
            env={"CODEX_HOME": str(self.root / "home")}, **kwargs), evidence

    def test_grandchild_late_cannot_mutate_finalized_evidence(self):
        """repair1 F1: a grandchild outliving the join window must never
        append to the finalized events file after run_readonly returns."""
        import time
        helper = self.root / "late_writer.py"
        helper.write_text(
            "#!/usr/bin/env python3\n"
            "import sys, time\n"
            "time.sleep(1.0)\n"
            "sys.stdout.write('{\"type\": \"late\"}\\n')\n"
            "sys.stdout.flush()\n")
        fake = self._fake(
            "import json, subprocess, sys\n"
            "sys.stdout.write(json.dumps("
            "{\"type\": \"thread.started\", \"thread_id\": \"g1\"}) + \"\\n\")\n"
            "sys.stdout.flush()\n"
            "subprocess.Popen([sys.executable, %r], stdout=sys.stdout)\n"
            "sys.exit(0)\n" % str(helper))
        with mock.patch.object(codex_driver, "_TERMINATE_GRACE_S", 0.3):
            receipt, evidence = self._run(fake)
        events_file = evidence / "codex-events.jsonl"
        bytes_at_return = events_file.read_bytes()
        receipt_counts = dict(receipt.event_counts)
        # after the grandchild's late-write window has fully elapsed, the
        # finalized file is byte-identical and the receipt still covers it
        time.sleep(1.5)
        self.assertEqual(events_file.read_bytes(), bytes_at_return)
        live_counts = codex_driver._parse_events(
            events_file.read_text().splitlines(keepends=True))[0]
        self.assertEqual(receipt_counts, live_counts)

    def test_recursion_bomb_raises_loudly_with_complete_evidence(self):
        """repair1 F2: a deep-JSON parse failure must surface as a raise
        (no receipt), with the raw evidence complete — never a truncated
        stream reported as delivered."""
        bomb = "[" * 30000 + "]" * 30000
        fake = self._fake(
            "import json, sys\n"
            "sys.stdout.write(json.dumps("
            "{\"type\": \"thread.started\", \"thread_id\": \"t1\"}) + \"\\n\")\n"
            "sys.stdout.write(%r + \"\\n\")\n"
            "sys.stdout.write(json.dumps({\"type\": \"turn.completed\"})"
            " + \"\\n\")\n"
            "sys.stdout.flush()\n"
            "sys.exit(0)\n" % bomb)
        with self.assertRaises(RecursionError):
            self._run(fake)
        evidence = self.root / ("evidence-%d" % id(fake))
        self.assertFalse((evidence / "receipt.json").exists())
        text = (evidence / "codex-events.jsonl").read_text()
        self.assertIn("turn.completed", text)      # tail line survived
        self.assertIn("thread.started", text)      # head line survived

    def test_metrics_sink_exception_is_isolated(self):
        """repair1 F3: a raising caller sink must never take the receipt
        down (same isolation as the exit-event sink)."""
        fake = self._fake(
            "import sys\n"
            "sys.stdout.write('{\"type\": \"turn.completed\"}\\n')\n"
            "sys.stdout.flush()\n"
            "sys.exit(0)\n")
        last_message_needed = self.root / "lm.json"

        def exploding_sink(_payload):
            raise RuntimeError("metrics sink exploded")

        # delivered path needs a last-message file: the fake writes none,
        # so accept schema_mismatch — the point is receipt persistence
        receipt, evidence = self._run(fake, metrics_sink=exploding_sink)
        self.assertTrue((evidence / "receipt.json").exists())
        self.assertIn(receipt.outcome, ("delivered", "schema_mismatch"))

    def test_excerpt_window_matches_old_formula_on_boundary_shapes(self):
        """repair1 R1/R2: exact equivalence including internal runs,
        multi-byte tails at 65536/65537 boundaries, oversized streams."""
        import io as _io

        def old_rule(text):
            return "".join(
                [line for line in _io.StringIO(text)]).strip()[-500:]

        cases = {
            "small": "abc\n",
            "tail_ws_65536": "X" * (1024 * 1024 + 16) + " " * 65536,
            "tail_ws_65537": "X" * (1024 * 1024 + 17) + " " * 65537,
            "internal_run_short_tail": "Y" * 900 + " " * 3000 + "z" * 8,
            "internal_run_big": "A" * 1024 * 1024 + " " * 2000 + "B" * 100,
            "multibyte_tail_65000": "文" * 400000 + " " * 65000,
            "multibyte_tail_65200": "字" * 400000 + " " * 65200,
            "all_whitespace": " " * 100000,
        }
        for name, text in cases.items():
            with self.subTest(shape=name):
                raw = self.root / ("stderr-%s.partial" % name)
                raw.write_text(text, encoding="utf-8")
                self.assertEqual(codex_driver._stderr_excerpt_file(raw),
                                 old_rule(text))
                self.assertFalse(raw.exists())

    def test_utf8_truncated_tail_is_old_parity_delivered(self):
        """repair2 N3/G2: a child ending mid-codepoint keeps the OLD
        acceptance set — silent tail drop, receipt delivered, no raise."""
        fake = self._fake(
            "import sys\n"
            "NL = chr(10)\n"
            "sys.stdout.write('{\"type\": \"thread.started\","
            " \"thread_id\": \"t1\"}' + NL)\n"
            "sys.stdout.flush()\n"
            "sys.stdout.buffer.write(b'{\"type\": \"turn.completed\"}'"
            " + bytes([0xe4, 0xb8]))\n"
            "sys.stdout.buffer.flush()\n"
            "sys.exit(0)\n")
        receipt, evidence = self._run(fake)
        # exit 0 + no last-message file written by this fake (it never
        # opens -o) -> the driver records the missing file; either way
        # NO raise and the decodable prefix is parsed
        self.assertIn(receipt.outcome,
                      ("delivered", "schema_mismatch", "failed"))
        self.assertIn("thread.started", receipt.event_counts)
        self.assertNotIn("turn.completed", receipt.event_counts)
        metrics = {}
        self._run(fake, metrics_sink=metrics)
        self.assertTrue(metrics["stdout_pump"].get("decode_drop"))

    def test_excerpt_gray_band_is_shorter_but_never_fabricates(self):
        """repair2 N2/G1: inside the registered gray band (run + 500-char
        bytes > window cap) the excerpt may be SHORTER than the old
        answer but never contains fabricated U+FFFD."""
        mib = 1024 * 1024
        text = "Q" * mib + " " * (17 * mib)  # run alone > 16MiB cap
        raw = self.root / "stderr-gray.partial"
        raw.write_text(text, encoding="utf-8")
        got = codex_driver._stderr_excerpt_file(raw)
        want = codex_driver._stderr_excerpt([text])
        self.assertEqual(len(want), 500)
        self.assertLess(len(got), 500)
        self.assertNotIn("\ufffd", got)

    def test_preset_directory_at_partial_path_degrades_not_crashes(self):
        """repair1 D2/R3: an uncreatable partial degrades to an empty
        final evidence file with the receipt still written."""
        fake = self._fake(
            "import sys\n"
            "sys.stdout.write('{\"type\": \"turn.completed\"}\\n')\n"
            "sys.stdout.flush()\n"
            "sys.exit(0)\n")
        evidence = self.root / "evidence-preset"
        (evidence / codex_driver._RAW_STDOUT_PARTIAL).mkdir(parents=True)
        receipt = codex_driver.run_readonly(
            fake, self.workspace, "task", self.schema, evidence,
            timeout_s=30, env={"CODEX_HOME": str(self.root / "home")})
        self.assertTrue((evidence / "receipt.json").exists())
        self.assertEqual((evidence / "codex-events.jsonl").read_text(), "")


class NormalizeStrictSchemaTests(unittest.TestCase):
    def test_normalize_nested_objects_arrays_and_combinators(self):
        schema = {
            "type": "object",
            "properties": {
                "summary": {"type": "string"},
                "details": {"type": "object",
                            "properties": {"a": {"type": "string"},
                                           "b": {"type": "integer"}}},
                "items": {"type": "array",
                          "items": {"type": "object",
                                    "properties": {"k": {"type": "string"}}}},
                "choice": {"anyOf": [{"type": "object",
                                      "properties": {"x": {"type": "string"}}},
                                     {"type": "string"}]},
            },
            "$defs": {"nested": {"type": "object",
                                 "properties": {"z": {"type": "boolean"}}}},
        }
        normalized = codex_driver.normalize_strict_schema(schema)
        self.assertIs(normalized["additionalProperties"], False)
        self.assertEqual(normalized["required"],
                         ["summary", "details", "items", "choice"])
        details = normalized["properties"]["details"]
        self.assertIs(details["additionalProperties"], False)
        self.assertEqual(details["required"], ["a", "b"])
        inner = normalized["properties"]["items"]["items"]
        self.assertIs(inner["additionalProperties"], False)
        self.assertEqual(inner["required"], ["k"])
        variant = normalized["properties"]["choice"]["anyOf"][0]
        self.assertIs(variant["additionalProperties"], False)
        self.assertEqual(variant["required"], ["x"])
        definition = normalized["$defs"]["nested"]
        self.assertIs(definition["additionalProperties"], False)
        self.assertEqual(definition["required"], ["z"])
        # the input schema is never mutated
        self.assertNotIn("additionalProperties", schema)
        self.assertNotIn("required", schema)
        # idempotent: a second pass changes nothing
        self.assertEqual(codex_driver.normalize_strict_schema(normalized), normalized)

    def test_existing_required_keys_keep_their_position(self):
        schema = {"type": "object",
                  "properties": {"alpha": {"type": "string"},
                                 "beta": {"type": "string"},
                                 "gamma": {"type": "string"}},
                  "required": ["gamma"]}
        normalized = codex_driver.normalize_strict_schema(schema)
        self.assertEqual(normalized["required"], ["gamma", "alpha", "beta"])

    def test_non_dict_input_is_returned_unchanged(self):
        for value in (None, [], "schema", 7):
            self.assertEqual(codex_driver.normalize_strict_schema(value), value)


class LocateRolloutTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.codex_home = self.root / "codex-home"
        self.thread_id = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"

    def _write_rollout(self, relative: str, session_id: str | None, own_id: str | None = None):
        path = self.codex_home / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = {"cwd": "/tmp/w", "cli_version": "0.163.0"}
        if session_id is not None:
            payload["session_id"] = session_id
        if own_id is not None:
            payload["id"] = own_id
        path.write_text(
            json.dumps({"timestamp": "2026-10-25T09:00:00.000Z", "ordinal": 0,
                        "type": "session_meta", "payload": payload})
            + "\n"
            + json.dumps({"type": "event_msg", "payload": {"type": "user_message"}})
            + "\n",
            encoding="utf-8",
        )
        return path

    def test_locates_rollout_by_filename_and_session_meta(self):
        rollout = self._write_rollout(
            "sessions/2026/10/25/rollout-2026-10-25T09-00-00-" + self.thread_id + ".jsonl",
            session_id=self.thread_id,
        )
        result = codex_driver.locate_rollout(self.thread_id, self.codex_home)
        self.assertEqual(result["status"], "located")
        self.assertEqual(result["path"], str(rollout))
        self.assertEqual(result["cwd"], "/tmp/w")
        self.assertEqual(result["cli_version"], "0.163.0")

    def test_locates_rollout_matching_payload_id_from_codex_0163(self):
        # Real codex 0.163 rollouts carry the rollout uuid in payload.id while
        # payload.session_id may reference a parent session.
        self._write_rollout(
            "sessions/2026/10/25/rollout-2026-10-25T09-00-00-" + self.thread_id + ".jsonl",
            session_id="00000000-0000-0000-0000-000000000000",
            own_id=self.thread_id,
        )
        result = codex_driver.locate_rollout(self.thread_id, self.codex_home)
        self.assertEqual(result["status"], "located")

    def test_session_meta_mismatch_is_unlocatable(self):
        self._write_rollout(
            "sessions/2026/10/25/rollout-2026-10-25T09-00-00-" + self.thread_id + ".jsonl",
            session_id="00000000-0000-0000-0000-000000000000",
        )
        result = codex_driver.locate_rollout(self.thread_id, self.codex_home)
        self.assertEqual(result["status"], "unlocatable")
        self.assertIn("session_meta", result["reason"])

    def test_missing_rollout_is_unlocatable(self):
        result = codex_driver.locate_rollout(self.thread_id, self.codex_home)
        self.assertEqual(result["status"], "unlocatable")

    def test_blank_thread_id_is_unlocatable_without_guessing(self):
        result = codex_driver.locate_rollout("", self.codex_home)
        self.assertEqual(result, {"status": "unlocatable", "reason": "no thread id"})
        self.assertEqual(
            codex_driver.locate_rollout(None, self.codex_home)["reason"], "no thread id"
        )

    def test_session_index_path_hint_is_honored_when_validated(self):
        outside = self._write_rollout(
            "elsewhere/rollout-2026-10-25T09-00-00-" + self.thread_id + ".jsonl",
            session_id=self.thread_id,
        )
        (self.codex_home / "session_index.jsonl").write_text(
            json.dumps({"id": self.thread_id, "path": str(outside)}) + "\n",
            encoding="utf-8",
        )
        result = codex_driver.locate_rollout(self.thread_id, self.codex_home)
        self.assertEqual(result["status"], "located")
        self.assertEqual(result["path"], str(outside))

    def test_session_index_hint_with_unvalidated_file_falls_through(self):
        # Index entry exists but its file's session_meta names another id:
        # the hint must be rejected, and the sessions tree has no candidate.
        outside = self._write_rollout(
            "elsewhere/rollout-2026-10-25T09-00-00-" + self.thread_id + ".jsonl",
            session_id="00000000-0000-0000-0000-000000000000",
        )
        (self.codex_home / "session_index.jsonl").write_text(
            json.dumps({"session_id": self.thread_id, "path": str(outside)}) + "\n",
            encoding="utf-8",
        )
        result = codex_driver.locate_rollout(self.thread_id, self.codex_home)
        self.assertEqual(result["status"], "unlocatable")


class SummarizeRolloutTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.rollout = self.directory / "rollout-test.jsonl"
        long_command = "cargo test " + "x" * 300
        lines = [
            {
                "timestamp": "2026-10-25T09:00:00.000Z",
                "ordinal": 0,
                "type": "session_meta",
                "payload": {
                    "session_id": "sid-1",
                    "cwd": "/private/secret/work",
                    "cli_version": "0.163.0",
                    "creator_account_id": "SECRET-ACCOUNT",
                },
            },
            {
                "type": "event_msg",
                "payload": {
                    "type": "agent_message",
                    "role": "assistant",
                    "api_key": "SECRET-KEY",
                    "text": "y" * 300,
                },
            },
            {"type": "response_item", "payload": {"command": long_command, "name": "shell"}},
        ]
        self.rollout.write_text(
            "".join(json.dumps(line) + "\n" for line in lines), encoding="utf-8"
        )

    def test_summary_uses_whitelist_and_truncates_text(self):
        summary = codex_driver.summarize_rollout(self.rollout)
        self.assertEqual(summary["total_lines"], 3)
        self.assertEqual(
            summary["type_counts"],
            {"session_meta": 1, "event_msg": 1, "response_item": 1},
        )
        self.assertEqual(
            summary["meta"],
            {"session_id": "sid-1", "cwd": "/private/secret/work",
             "cli_version": "0.163.0", "timestamp": "2026-10-25T09:00:00.000Z"},
        )
        self.assertEqual(len(summary["items"]), 3)
        third = summary["items"][2]
        self.assertEqual(third["type"], "response_item")
        self.assertEqual(len(third["command"]), 200)
        self.assertTrue(third["command"].startswith("cargo test "))
        self.assertEqual(summary["items"][1].get("role"), "assistant")
        dumped = json.dumps(summary)
        for forbidden in ("SECRET-KEY", "SECRET-ACCOUNT", "api_key", "creator_account_id"):
            self.assertNotIn(forbidden, dumped)
        for item in summary["items"]:
            self.assertTrue(set(item) <= {"type", "role", "name", "command"})

    def test_max_items_bounds_the_item_list(self):
        summary = codex_driver.summarize_rollout(self.rollout, max_items=2)
        self.assertEqual(len(summary["items"]), 2)
        self.assertEqual(summary["total_lines"], 3)

    def test_unreadable_path_reports_error(self):
        summary = codex_driver.summarize_rollout(self.directory / "missing.jsonl")
        self.assertIn("error", summary)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
