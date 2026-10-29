"""Hub server tests: real stdio MCP handshake plus function-level contracts.

No real codex/zcode task is ever dispatched: workers are injected fakes, the
codex home and zcode database are temp fixtures, and the only subprocess run
is the hub server itself (MCP stdio)."""

import asyncio
import threading
import contextlib
import io
import json
import os
import sqlite3
import sys
import tempfile
import time
import unittest
from unittest import mock
from types import SimpleNamespace
from dataclasses import dataclass
from pathlib import Path
from unittest.mock import AsyncMock, patch

from zcode_supervisor import contracts, hub_server as H
from zcode_supervisor.authority import Authority
from zcode_supervisor.codex_driver import CodexRunReceipt
from zcode_supervisor.runtime import Config

TOOL_NAMES = ["delegate", "health", "list_sessions", "list_skills",
              "list_workers", "read_context"]


class StreamingAdmissionTests(unittest.TestCase):
    """P2.4 repair1 F-A/F-B/F-C: admission parity, fd ownership, and the
    str.splitlines separator set on the streamed index read."""

    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.tasks = self.root / "hub" / "tasks"
        self.tasks.mkdir(parents=True)
        self.config = SimpleNamespace(state_dir=str(self.root),
                                      work_root=str(self.root))

    def _write(self, text):
        (self.tasks / "index.jsonl").write_text(text, encoding="utf-8")

    def test_hardlink_index_is_refused_like_read_regular(self):
        self._write(json.dumps({"task_id": "t1", "worker": "codex",
                                "status": "running"}) + "\n")
        # a SECOND hard link to the same inode: the index path itself
        # now reports st_nlink == 2 — read_regular refused this shape
        (self.tasks / "second-link.jsonl").hardlink_to(
            self.tasks / "index.jsonl")
        with self.assertRaises(H.HubError) as caught:
            H.list_sessions_impl(self.config)
        self.assertEqual(caught.exception.code, "index-unreadable")
        self.assertIn("Expected an ordinary single-link file",
                      caught.exception.detail)

    def test_fifo_index_race_refused_not_silently_consumed(self):
        # standing FIFOs are invisible to the old is_file() gate too —
        # the admission matters in the is_file->open TOCTOU window
        os.mkfifo(self.tasks / "index.jsonl")
        real_is_file = Path.is_file
        with mock.patch.object(
                Path, "is_file",
                lambda self: True if self.name == "index.jsonl"
                else real_is_file(self)):
            with self.assertRaises(H.HubError) as caught:
                H.list_sessions_impl(self.config)
        self.assertEqual(caught.exception.code, "index-unreadable")

    def test_directory_race_leaks_no_fd(self):
        # simulate the is_file->open TOCTOU: the path is a directory by
        # open time; the failed fdopen path must close the fd
        (self.tasks / "index.jsonl").mkdir()
        real_is_file = Path.is_file

        def racing_is_file(self):
            if self.name == "index.jsonl":
                return True  # race: passes the check, becomes a dir
            return real_is_file(self)

        def fd_count():
            return len(os.listdir("/dev/fd"))
        import unittest.mock as _mock
        with _mock.patch.object(Path, "is_file", racing_is_file):
            before = fd_count()
            for _ in range(5):
                with self.assertRaises(H.HubError):
                    H.list_sessions_impl(self.config)
            import gc
            gc.collect()
            self.assertEqual(fd_count() - before, 0)

    def test_exotic_separators_keep_old_record_boundaries(self):
        lines = [json.dumps({"task_id": "t%d" % i, "worker": "codex",
                             "status": "running", "seq": i})
                 for i in range(4)]
        shapes = {
            "ff": lines[0] + "\x0c" + lines[1] + "\n",
            "vt": lines[0] + "\x0b" + lines[1] + "\n",
            "fs_gs_rs": lines[0] + "\x1c" + lines[1] + "\x1d"
                        + lines[2] + "\x1e" + lines[3] + "\n",
            "nel": lines[0] + "\x85" + lines[1] + "\n",
            "ls_ps": lines[0] + "\u2028" + lines[1] + "\u2029",
        }
        for name, text in shapes.items():
            with self.subTest(shape=name):
                self._write(text)
                result = H.list_sessions_impl(self.config)
                self.assertEqual(result["total"],
                                 len(text.splitlines()))
                self.assertEqual(result["malformed_index_lines"], 0)
                self.assertEqual(
                    [r["task_id"] for r in result["sessions"]],
                    [json.loads(line)["task_id"]
                     for line in text.splitlines()])


class Repair2LinearityTests(unittest.TestCase):
    """P2.4 repair2 R1/R2: giant-line wall is linear; multibyte chars
    crossing the 256KiB chunk boundary survive in both readers."""

    def test_giant_single_line_wall_is_linear(self):
        import time as _time
        walls = {}
        for size_mb in (5, 10, 20):
            text = '{"blob": "' + "X" * (size_mb * 1024 * 1024) + '"}'
            with tempfile.TemporaryDirectory() as tmp:
                f = Path(tmp) / "index.jsonl"
                f.write_text(text, encoding="utf-8")
                root = Path(tmp)
                (root / "hub" / "tasks").mkdir(parents=True)
                (root / "hub" / "tasks" / "index.jsonl").write_text(
                    text, encoding="utf-8")
                config = SimpleNamespace(state_dir=str(root),
                                         work_root=str(root))
                t0 = _time.monotonic()
                result = H.list_sessions_impl(config)
                walls[size_mb] = _time.monotonic() - t0
                self.assertEqual(result["total"], 1)
        # wall gate (dispatch: 30MB ~<=1s class -> 20MB must be well
        # under a second) AND linear-ish growth (doubling ~doubles)
        self.assertLess(walls[20], 1.0)
        self.assertLess(walls[10] / walls[5], 3.0)
        self.assertLess(walls[20] / walls[10], 3.0)

    def test_multibyte_straddling_chunk_boundary_survives(self):
        """R2: 中 (E4 B8 AD) split across the 256KiB chunk edge must
        survive in _index_hints (old read_text parity) and in the hub
        streaming splitter."""
        from zcode_supervisor.codex_driver import _index_hints
        chunk = 256 * 1024
        lead = b'{"session_id":"s1","path":"'
        target = chunk - 1 - len(lead)
        data = (lead + b"p" * target
                + "中".encode() + b'"}\n')
        with tempfile.TemporaryDirectory() as tmp:
            home = Path(tmp)
            (home / "session_index.jsonl").write_bytes(data)
            hints = _index_hints(home, "s1")
        self.assertEqual(len(hints), 1)
        self.assertIn("中", str(hints[0]))
        self.assertNotIn("\ufffd", str(hints[0]))
        # hub side: same shape through the streaming splitter
        with tempfile.TemporaryDirectory() as tmp:
            f = Path(tmp) / "x"
            f.write_bytes(data)
            lines = list(H._splitlines_streaming(f))
        self.assertEqual(len(lines), 1)
        self.assertIn("中", lines[0])
        self.assertNotIn("\ufffd", lines[0])


class Repair3DanglingCrBoundaryTests(unittest.TestCase):
    """P2.4 repair3 R3/N6: a lone ``\\r`` dangling at a chunk boundary
    (block ends, no segment emitted) must stay INSIDE the scan window —
    it still acts as a separator (or pairs into ``\\r\\n``) once the next
    chunk arrives. repair2 folded it into the scanned prefix, gluing
    records across the boundary (consumer-visible: total=0/malformed=1
    at the real 256KiB boundary; hints lost both records)."""

    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)

    # ---- drivers over the REAL product entry points ----------------
    def _hub_segments(self, data, chunk):
        path = self.root / "corpus.bin"
        path.write_bytes(data)
        return list(H._splitlines_streaming(path, chunk_bytes=chunk))

    def _hints_segments(self, data, chunk):
        """Real _index_hints loop driven at an arbitrary chunk size via
        a capped reader (raw segments recorded past the JSON gate)."""
        from zcode_supervisor import codex_driver
        home = self.root / "hint-home"
        home.mkdir(exist_ok=True)
        (home / "session_index.jsonl").write_bytes(data)
        recorded: list = []

        class _Capped:
            def __init__(self, real):
                self._real = real

            def read(self, size):
                return self._real.read(min(size, chunk))

            def __enter__(self):
                return self

            def __exit__(self, *exc):
                self._real.close()
                return False

        real_open = open

        def capped_open(file, mode="r", *args, **kwargs):
            assert "b" in mode
            return _Capped(real_open(file, mode, *args, **kwargs))

        with mock.patch.object(
                codex_driver, "_absorb_index_line",
                lambda line, tid, hints: recorded.append(line)), \
                mock.patch("builtins.open", capped_open):
            codex_driver._index_hints(home, "th1")
        return recorded

    def _assert_both_sides_exact(self, data):
        truth = data.decode("utf-8", "replace").splitlines()
        for chunk in range(1, len(data) + 2):
            with self.subTest(chunk=chunk, data=data):
                self.assertEqual(self._hub_segments(data, chunk), truth)
                self.assertEqual(self._hints_segments(data, chunk), truth)

    # ---- dispatch shape 1: \r at last block byte + \n crossing -----
    def test_cr_last_byte_then_lf_crossing(self):
        # the \r must pair into \r\n — no residual \r inside a segment
        self._assert_both_sides_exact(b"abcdef\r\ndef")

    # ---- dispatch shape 2: dangling \r + non-\n crossing -----------
    def test_cr_last_byte_then_nonlf_crossing(self):
        for data in (b"abcdef\rx",          # standalone \r splits
                     b"abcdef\r\rx",        # \r\r then content
                     b"a\r\r\nb",           # \r \r\n mix
                     b"a\n\r\rb",           # \n \r \r
                     b"a\r\rb"):            # \r\r mid-file
            with self.subTest(data=data):
                self._assert_both_sides_exact(data)

    # ---- dispatch shape 3: dangling \r + multibyte crossing --------
    def test_cr_last_byte_then_multibyte_crossing(self):
        for text in ("\r中x",               # \r then a 3-byte char
                     "中\rx",               # multibyte then dangling \r
                     "前中\r后",            # \r deep inside multibyte run
                     "\r😀x"):              # \r then a 4-byte char
            with self.subTest(text=text):
                self._assert_both_sides_exact(text.encode())

    # ---- dispatch shape 4: \r\r\r is three separator events -------
    def test_cr_cr_cr_is_three_empty_segments(self):
        for data, want in ((b"\r\r\rQ", ["", "", "", "Q"]),
                           (b"a\r\r\rb", ["a", "", "", "b"])):
            truth = data.decode("utf-8", "replace").splitlines()
            self.assertEqual(truth, want)  # the oracle agrees: 3 empties
            for chunk in range(1, len(data) + 2):
                with self.subTest(chunk=chunk, data=data):
                    self.assertEqual(self._hub_segments(data, chunk), want)
                    self.assertEqual(self._hints_segments(data, chunk), want)

    def test_eof_trailing_lone_cr_loses_only_one_tail_empty(self):
        """Registered exemption R4/N3: a lone trailing \\r at EOF loses
        exactly ONE trailing empty segment (stream == truth[:-1] with
        truth[-1] == '') — the consumer layers skip blank lines, so the
        loss is invisible there. This asserts the exemption shape itself
        so any widening of the divergence fails here."""
        data = b"\r\r\r"
        decoded = data.decode("utf-8", "replace")
        truth = decoded.splitlines()
        self.assertEqual(truth, ["", "", ""])
        for chunk in range(1, len(data) + 2):
            with self.subTest(chunk=chunk):
                self.assertTrue(decoded.endswith("\r") and truth[-1] == "")
                self.assertEqual(self._hub_segments(data, chunk),
                                 truth[:-1])
                self.assertEqual(self._hints_segments(data, chunk),
                                 truth[:-1])

    # ---- dispatch shape 5: real 256KiB boundary, end to end --------
    def test_real_256kib_boundary_end_to_end(self):
        block = 256 * 1024
        # hub: record A is a VALID json line of exactly 262143 bytes,
        # so the lone \r is the LAST byte of the first 256KiB block;
        # the next byte is not \n — the two records must stay two
        # (repair2 merged them: total=0, malformed=1)
        head = '{"task_id":"a1","worker":"codex","blob":"'
        record1 = head + "x" * (block - 1 - len(head) - 2) + '"}'
        self.assertEqual(len(record1.encode()), block - 1)
        record2 = '{"task_id":"a2","worker":"codex"}'
        data = record1.encode() + b"\r" + record2.encode() + b"\n"
        self.assertEqual(data[block - 1:block], b"\r")
        tasks = self.root / "hub" / "tasks"
        tasks.mkdir(parents=True)
        (tasks / "index.jsonl").write_bytes(data)
        config = SimpleNamespace(state_dir=str(self.root),
                                 work_root=str(self.root))
        result = H.list_sessions_impl(config)
        self.assertEqual(result["total"], 2)
        self.assertEqual(result["malformed_index_lines"], 0)
        self.assertEqual([r["task_id"] for r in result["sessions"]],
                         ["a1", "a2"])
        # hints: same boundary shape through the real _index_hints
        # (real file, unpatched open, real 256KiB reads) — both records
        # must survive (repair2 lost both)
        from zcode_supervisor.codex_driver import _index_hints
        home = self.root / "codex-home"
        home.mkdir()
        head2 = '{"id":"th1","path":"/a.jsonl","pad":"'
        line1 = head2 + "x" * (block - 1 - len(head2) - 2) + '"}'
        line1_bytes = line1.encode()
        self.assertEqual(len(line1_bytes), block - 1)
        (home / "session_index.jsonl").write_bytes(
            line1_bytes + b"\r"
            + b'{"id":"th1","path":"/b.jsonl"}' + b"\n")
        hints = [str(path) for path in _index_hints(home, "th1")]
        self.assertEqual(hints, ["/a.jsonl", "/b.jsonl"])


class BoundedReadOffloadTests(unittest.TestCase):
    """P2.4: dedicated 2-worker executor + 4-semaphore read offload —
    cancellation, saturation and shutdown behaviour."""

    def test_executor_is_bounded_and_shared(self):
        pool = H.read_executor_pool()
        self.assertIs(pool, H.read_executor_pool())
        self.assertEqual(pool._max_workers, H._READ_EXECUTOR_WORKERS)
        self.assertEqual(H._READ_EXECUTOR_WORKERS, 2)
        self.assertEqual(H._READ_CONCURRENCY, 4)

    def test_offload_runs_off_loop_and_returns_value(self):
        import threading

        async def main():
            seen = {}

            def probe():
                seen['thread'] = threading.current_thread()
                return 41

            result = await H._offload_read(lambda: 0) or None
            value = await H._offload_read(probe)
            return value, seen.get('thread')

        value, thread = asyncio.run(main())
        self.assertEqual(value, 41)
        self.assertIsNotNone(thread)
        self.assertNotEqual(thread, threading.main_thread())

    def test_cancellation_does_not_kill_the_pool(self):
        import time

        started = threading.Event()

        def slow():
            started.set()
            time.sleep(1.0)
            return 'done'

        async def main():
            task = asyncio.get_running_loop().create_task(
                H._offload_read(slow))
            await asyncio.get_running_loop().run_in_executor(
                None, started.wait, 2)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
            # the pool still serves reads after a cancelled caller
            return await H._offload_read(lambda: 'ok')

        self.assertEqual(asyncio.run(main()), 'ok')

    def test_saturation_caps_concurrent_reads(self):
        import time
        active = []
        peak = []

        def record(name):
            active.append(name)
            peak.append(len(active))
            time.sleep(0.05)
            active.remove(name)
            return name

        async def main():
            tasks = [asyncio.get_running_loop().create_task(
                H._offload_read(record, i)) for i in range(10)]
            return await asyncio.gather(*tasks)

        results = asyncio.run(main())
        self.assertEqual(sorted(results), list(range(10)))
        # executor bound: never more than 2 workers inside the reads
        self.assertLessEqual(max(peak), H._READ_EXECUTOR_WORKERS)

    def test_second_event_loop_lifetime_does_not_break(self):
        """repair1 F-D: a process-level semaphore bound to its first
        loop broke any SECOND loop lifetime; per-loop laziness must let
        repeated asyncio.run compete without RuntimeError."""
        import time

        def saturate(tag):
            active = []
            peak = []

            def record(name):
                active.append(name)
                peak.append(len(active))
                time.sleep(0.02)
                active.remove(name)
                return name

            async def main():
                tasks = [asyncio.get_running_loop().create_task(
                    H._offload_read(record, i)) for i in range(12)]
                return await asyncio.gather(*tasks)

            results = asyncio.run(main())
            return results, max(peak)

        first, peak1 = saturate('first')
        second, peak2 = saturate('second')  # previously RuntimeError
        self.assertEqual(len(first), 12)
        self.assertEqual(len(second), 12)
        self.assertLessEqual(peak1, H._READ_EXECUTOR_WORKERS)
        self.assertLessEqual(peak2, H._READ_EXECUTOR_WORKERS)

    def test_shutdown_then_fresh_pool(self):
        H.read_executor_pool()  # ensure it exists
        H.shutdown_read_executor(wait=True)
        self.assertIsNone(H._read_executor)
        # the next call lazily builds a fresh, working pool
        async def main():
            return await H._offload_read(lambda: 'rebuilt')
        self.assertEqual(asyncio.run(main()), 'rebuilt')
        H.shutdown_read_executor(wait=True)


class HubFixture(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.work_root = self.root / "work"
        self.state_dir = self.root / "state"
        self.cache_dir = self.root / "cache"
        for path in (self.work_root, self.state_dir, self.cache_dir):
            path.mkdir()
        self.workspace = self.work_root / "task-ws"
        self.workspace.mkdir()
        self.fake_cli = self.root / "zcode.cjs"
        self.fake_cli.write_text("#!/bin/sh\n", encoding="utf-8")
        self.fake_codex = self.root / "fake-codex"
        self.fake_codex.write_text("#!/bin/sh\n", encoding="utf-8")
        self.fake_codex.chmod(0o755)
        self.evidence_root = self.root / "hub-evidence"
        self.codex_home = self.root / "codex-home"
        self.config = Config(work_root=self.work_root, state_dir=self.state_dir,
                             cache_dir=self.cache_dir, cli_path=self.fake_cli,
                             node="node")
        # Hermetic discovery: tri_probe's live discovery sees the real
        # machine install (e.g. the ChatGPT.app codex bundle); pin all three
        # probes to fakes so no hub test depends on this host.
        for target, value in (
            ("zcode_supervisor.tri_probe.discover_codex",
             {"installed": False, "path": None, "candidates": []}),
            ("zcode_supervisor.tri_probe.discover_zcode",
             {"installed": True, "path": str(self.fake_cli),
              "candidates": [str(self.fake_cli)]}),
            ("zcode_supervisor.tri_probe.discover_dsh",
             {"installed": False, "path": None, "candidates": []}),
        ):
            patcher = patch(target, return_value=value)
            patcher.start()
            self.addCleanup(patcher.stop)

    # -- helpers ---------------------------------------------------------
    def strict_schema(self):
        return {"type": "object",
                "properties": {"answer": {"type": "string"}},
                "required": ["answer"], "additionalProperties": False}

    def codex_contract(self, **overrides):
        contract = {
            "contract_id": "hub-codex-1",
            "worker": "codex",
            "workspace": str(self.workspace),
            "prompt": "Read TASK.md and answer.",
            "allowed_files": [],
            "commands": [],
            "model": {"model": "gpt-5-codex"},
            "max_timeout_ms": 60_000,
            "budget": {"max_tokens": None, "max_cost_usd": 1.0, "note": "p0"},
            "output_schema": self.strict_schema(),
        }
        contract.update(overrides)
        return contract

    def zcode_contract(self, **overrides):
        contract = {
            "contract_id": "hub-zcode-1",
            "worker": "zcode",
            "workspace": str(self.workspace),
            "prompt": "Read TASK.md and report.",
            "allowed_files": [],
            "commands": [],
            "model": {"providerId": "glm", "modelId": "glm-5.3"},
            "max_timeout_ms": 60_000,
            "budget": {"max_tokens": None, "max_cost_usd": None, "note": "p0"},
        }
        contract.update(overrides)
        return contract

    def register(self, contract):
        source = self.root / "incoming"
        source.mkdir(exist_ok=True)
        path = source / f"{contract['contract_id']}.json"
        path.write_text(json.dumps(contract), encoding="utf-8")
        return contracts.register_contract(self.state_dir, path)

    def write_health(self, workers: dict):
        hub = self.state_dir / "hub"
        hub.mkdir(parents=True, exist_ok=True)
        (hub / "health.json").write_text(
            json.dumps({"generated": "2026-10-24T00:00:00+00:00",
                        "workers": workers}), encoding="utf-8")

    def make_rollout(self, thread_id: str) -> None:
        sessions = self.codex_home / "sessions" / "2026" / "10" / "24"
        sessions.mkdir(parents=True, exist_ok=True)
        path = sessions / f"rollout-20261024T120000-{thread_id}.jsonl"
        path.write_text(json.dumps({
            "type": "session_meta",
            "payload": {"id": thread_id, "session_id": thread_id,
                        "cwd": str(self.workspace), "cli_version": "0.2.0"},
        }) + "\n", encoding="utf-8")

    def make_zcode_db(self, session_id: str = "sess_hub1") -> Path:
        """Real 0.17.0 shape: message.data is metadata-only; the body text
        lives in a part row of type "text"."""
        db_path = self.root / "zcode-db.sqlite"
        connection = sqlite3.connect(db_path)
        try:
            connection.executescript(
                """
                CREATE TABLE session (id TEXT PRIMARY KEY, directory TEXT,
                    title TEXT, time_created INTEGER, time_updated INTEGER);
                CREATE TABLE message (id TEXT PRIMARY KEY, session_id TEXT,
                    time_created INTEGER, data TEXT, sequence INTEGER);
                CREATE TABLE part (id TEXT PRIMARY KEY, message_id TEXT,
                    session_id TEXT, time_created INTEGER, time_updated INTEGER,
                    data TEXT, sequence INTEGER);
                CREATE TABLE tool_usage (session_id TEXT, tool_name TEXT,
                    read_only INTEGER, destructive INTEGER,
                    approval_status TEXT, status TEXT, exit_code INTEGER,
                    duration_ms INTEGER);
                """)
            connection.execute("INSERT INTO session VALUES (?,?,?,?,?)",
                               (session_id, str(self.workspace), "hub fixture",
                                1, 2))
            connection.execute(
                "INSERT INTO message VALUES (?,?,?,?,?)",
                ("m0", session_id, 1,
                 json.dumps({"role": "assistant", "api_key": "sk-LEAK"}), 0))
            connection.execute(
                "INSERT INTO part VALUES (?,?,?,?,?,?,?)",
                ("p0", "m0", session_id, 1, 1,
                 json.dumps({"type": "text", "text": "done"}), 0))
            connection.commit()
        finally:
            connection.close()
        return db_path

    def delegate(self, contract_id, runner=None, worker="codex", **kwargs):
        fake_codex = runner or self.default_fake_codex()
        return asyncio.run(H.delegate_impl(
            self.config, self.evidence_root, contract_id,
            {"agent": "codex-control", "session_id": "sess_ctrl1"},
            codex_path=str(self.fake_codex), codex_home=self.codex_home,
            codex_runner=fake_codex if worker == "codex" else None,
            zcode_runner=fake_codex if worker == "zcode" else None,
            **kwargs))

    def default_fake_codex(self):
        def fake_codex_runner(codex_path, workspace, prompt, output_schema,
                              evidence_dir, timeout_s, model=None):
            Path(evidence_dir).mkdir(parents=True, exist_ok=True)
            (Path(evidence_dir) / "last-message.json").write_text(
                json.dumps({"answer": "ok"}), encoding="utf-8")
            return CodexRunReceipt(
                ok=True, outcome="delivered", thread_id="thread-hub-1",
                exit_code=0, events_path=str(Path(evidence_dir) / "events.jsonl"),
                last_message_path=str(Path(evidence_dir) / "last-message.json"),
                structured={"answer": "ok"}, event_counts={"thread.started": 1},
                command_items=[], model_observed=model, sandbox="read-only",
                duration_s=0.01, error=None)
        return fake_codex_runner


class HealthAndWorkersTests(HubFixture):
    def test_health_without_health_json_reports_unknown_and_enabled(self):
        view = H.health_impl(self.config, str(self.fake_codex))
        self.assertEqual(view["hub_tool_version"], 1)
        workers = view["workers"]
        for name in ("codex", "zcode"):
            self.assertTrue(workers[name]["installed"])
            self.assertEqual(workers[name]["status"], "unknown")
            self.assertTrue(workers[name]["enabled"])
        self.assertEqual(workers["dsh"]["status"], "absent")
        self.assertIn("degraded routing does not guarantee worker equivalence",
                      view["routing_note"])

    def test_health_reports_structured_write_capability(self):
        view = H.health_impl(self.config, str(self.fake_codex))
        self.assertEqual(view["write_capability"], {
            "zcode": "cold+resident (contract files + exact commands)",
            "codex_exec": "read-only only (no approval channel)",
            "codex_app_server": "write via untrusted per-command approvals"})
        self.assertNotIn("read_only_p0", view)  # flat P0 flag retired

    def test_health_json_view_is_served_with_defaults_fallback(self):
        self.write_health({"codex": {"enabled": False, "status": "error",
                                     "reason": "cli drift", "probe_ref": "job-7"}})
        view = H.health_impl(self.config, str(self.fake_codex))
        self.assertEqual(view["health_source"], "health.json")
        self.assertEqual(view["workers"]["codex"]["status"], "error")
        self.assertFalse(view["workers"]["codex"]["enabled"])
        self.assertEqual(view["workers"]["codex"]["reason"], "cli drift")
        self.assertEqual(view["workers"]["codex"]["probe_ref"], "job-7")
        # undeclared zcode falls back to defaults: enabled, unknown
        self.assertTrue(view["workers"]["zcode"]["enabled"])
        self.assertEqual(view["workers"]["zcode"]["status"], "unknown")

    def test_list_workers_shape(self):
        listing = H.list_workers_impl(self.config, str(self.fake_codex))
        self.assertEqual(sorted(listing["workers"]), ["codex", "dsh", "zcode"])
        self.assertEqual(listing["workers"]["codex"]["path"], str(self.fake_codex))
        self.assertFalse(listing["workers"]["dsh"]["installed"])

    def test_injected_codex_path_pins_discovery_over_tri_probe(self):
        machine_codex = ("/Applications/ChatGPT.app/Contents/Resources/"
                         "codex-cli/bin/codex")
        real_machine = {"installed": True, "path": machine_codex,
                        "candidates": [machine_codex]}
        with patch("zcode_supervisor.tri_probe.discover_codex",
                   return_value=real_machine):
            pinned = H.list_workers_impl(self.config, str(self.fake_codex))
        self.assertEqual(pinned["workers"]["codex"]["path"], str(self.fake_codex))
        with patch("zcode_supervisor.tri_probe.discover_codex",
                   return_value=real_machine):
            live = H.list_workers_impl(self.config, None)
        self.assertEqual(live["workers"]["codex"]["path"],
                         real_machine["path"])

    def test_health_json_routing_block_is_passed_through(self):
        hub = self.state_dir / "hub"
        hub.mkdir(parents=True, exist_ok=True)
        (hub / "health.json").write_text(json.dumps({
            "generated": "2026-10-25T00:00:00+00:00",
            "workers": {"codex": {"enabled": True, "status": "ok",
                                  "reason": "", "probe_ref": "job-1"}},
            "routing": {"enabled_workers": ["codex"],
                        "disabled_workers": ["zcode"],
                        "note": H.ROUTING_NOTE}}), encoding="utf-8")
        view = H.health_impl(self.config, str(self.fake_codex))
        self.assertEqual(view["routing"]["enabled_workers"], ["codex"])
        self.assertEqual(view["routing"]["disabled_workers"], ["zcode"])


class DelegateRejectionTests(HubFixture):
    def test_unknown_contract_is_refused_with_requirements(self):
        with self.assertRaises(H.HubError) as caught:
            self.delegate("never-registered")
        self.assertEqual(caught.exception.code, "unknown-contract")
        self.assertIn("MCP exposure is not approval", caught.exception.detail)
        self.assertIn("output_schema", caught.exception.detail)
        # no task was created for a refused dispatch
        tasks = self.state_dir / "hub" / "tasks"
        self.assertFalse((tasks / "index.jsonl").exists())

    def test_out_of_scope_contract_is_refused_at_delegate(self):
        # a contract that bypassed register validation must still be caught
        # by load_contract's re-validation inside delegate
        target = self.state_dir / "hub" / "contracts"
        target.mkdir(parents=True, exist_ok=True)
        (target / "sneaky.json").write_text(json.dumps(
            self.codex_contract(contract_id="sneaky", allowed_files=["X.md"])),
            encoding="utf-8")
        with self.assertRaises(H.HubError) as caught:
            self.delegate("sneaky")
        self.assertEqual(caught.exception.code, "unknown-contract")
        self.assertIn("codex exec mode has no approval callback",
                      caught.exception.detail)

    def test_disabled_worker_is_refused_with_remaining_workers(self):
        self.register(self.codex_contract())
        self.write_health({"codex": {"enabled": False, "status": "error",
                                     "reason": "cli drift", "probe_ref": "j"}})
        with self.assertRaises(H.HubError) as caught:
            self.delegate("hub-codex-1")
        self.assertEqual(caught.exception.code, "worker-disabled")
        self.assertIn("cli drift", caught.exception.detail)
        self.assertIn("'zcode'", caught.exception.detail)  # remaining_workers
        self.assertIn("equivalence", caught.exception.detail)

    def test_health_declared_absent_worker_is_refused(self):
        self.register(self.codex_contract())
        self.write_health({"codex": {"enabled": True, "status": "absent",
                                     "reason": "uninstalled", "probe_ref": "j"}})
        with self.assertRaises(H.HubError) as caught:
            self.delegate("hub-codex-1")
        self.assertEqual(caught.exception.code, "worker-absent")

    def test_invalid_initiator_is_refused(self):
        with self.assertRaises(H.HubError) as caught:
            asyncio.run(H.delegate_impl(
                self.config, self.evidence_root, "any",
                {"agent": "", "session_id": "s"}))
        self.assertEqual(caught.exception.code, "invalid-argument")


class DelegateCodexTests(HubFixture):
    def test_successful_codex_dispatch_records_task_index_and_return(self):
        self.register(self.codex_contract())
        self.make_rollout("thread-hub-1")
        result = self.delegate("hub-codex-1")
        self.assertEqual(result["worker"], "codex")
        self.assertEqual(result["outcome"], "delivered")
        self.assertEqual(result["structured"], {"answer": "ok"})
        self.assertEqual(result["session_locator"]["agent"], "codex")
        self.assertEqual(result["session_locator"]["thread_id"], "thread-hub-1")
        self.assertTrue(result["session_locator"]["path"].endswith(
            "rollout-20261024T120000-thread-hub-1.jsonl"))
        self.assertTrue(Path(result["evidence_dir"]).is_dir())
        record = json.loads((self.state_dir / "hub" / "tasks" /
                             f"{result['task_id']}.json").read_text())
        self.assertEqual(record["status"], "completed")
        self.assertEqual(record["initiator"],
                         {"agent": "codex-control", "session_id": "sess_ctrl1"})
        self.assertEqual(record["contract_sha256"],
                         contracts.contract_digest(self.codex_contract()))
        index_lines = (self.state_dir / "hub" / "tasks" / "index.jsonl"
                       ).read_text().splitlines()
        self.assertEqual(len(index_lines), 1)
        self.assertEqual(json.loads(index_lines[0])["task_id"], result["task_id"])

    def test_failed_codex_outcome_is_returned_not_raised(self):
        def failing_runner(codex_path, workspace, prompt, output_schema,
                           evidence_dir, timeout_s, model=None):
            Path(evidence_dir).mkdir(parents=True, exist_ok=True)
            return {"ok": False, "outcome": "failed", "thread_id": None,
                    "exit_code": 3, "events_path": "", "last_message_path": "",
                    "structured": None, "event_counts": {}, "command_items": [],
                    "model_observed": None, "sandbox": "read-only",
                    "duration_s": 0.0, "error": "codex exec exited 3"}
        self.register(self.codex_contract())
        result = self.delegate("hub-codex-1", runner=failing_runner)
        self.assertEqual(result["outcome"], "failed")
        self.assertEqual(result["error"], "codex exec exited 3")
        record = json.loads((self.state_dir / "hub" / "tasks" /
                             f"{result['task_id']}.json").read_text())
        self.assertEqual(record["status"], "completed")
        self.assertEqual(record["outcome"], "failed")

    def test_runner_crash_is_recorded_as_runner_error(self):
        def crashing_runner(*args, **kwargs):
            raise RuntimeError("boom")
        self.register(self.codex_contract())
        result = self.delegate("hub-codex-1", runner=crashing_runner)
        self.assertEqual(result["outcome"], "runner-error")
        self.assertIn("RuntimeError: boom", result["error"])
        record = json.loads((self.state_dir / "hub" / "tasks" /
                             f"{result['task_id']}.json").read_text())
        self.assertEqual(record["status"], "failed")


class DelegateZcodeTests(HubFixture):
    def test_successful_zcode_dispatch_uses_injected_runner(self):
        db_path = self.make_zcode_db()

        async def fake_zcode_runner(config, task_id, contract, timeout_s):
            self.assertEqual(contract["prompt"], "Read TASK.md and report.")
            self.assertEqual(timeout_s, 60)
            task_dir = Path(config.work_root) / "hub-tasks" / task_id
            task_dir.mkdir(parents=True, exist_ok=True)
            run_path = task_dir / "run.json"
            run_path.write_text(json.dumps({
                "delivery_ok": True, "session_id": "sess_hub1",
                "execution_problems": [], "changed_files": []}),
                encoding="utf-8")
            return {"delivery_ok": True, "session_id": "sess_hub1",
                    "execution_problems": [], "changed_files": [],
                    "outcome": "delivered", "run_path": str(run_path),
                    "db_path": str(db_path)}

        self.register(self.zcode_contract())
        result = self.delegate("hub-zcode-1", runner=fake_zcode_runner,
                               worker="zcode")
        self.assertEqual(result["worker"], "zcode")
        self.assertEqual(result["outcome"], "delivered")
        self.assertTrue(result["delivery_ok"])
        self.assertEqual(result["changed_files"], [])
        self.assertEqual(result["problems"], [])
        self.assertEqual(result["session_locator"],
                         {"agent": "zcode", "session_id": "sess_hub1",
                          "db_path": str(db_path)})
        self.assertEqual(result["evidence_dir"],
                         str(self.work_root / "hub-tasks" / result["task_id"]))


class DelegateZcodeResidentTests(HubFixture):
    """zcode+resident dispatch: contract -> czs packet on the shared resident."""

    def setUp(self):
        super().setUp()
        self.command = {"id": "list-files", "command": "/bin/ls",
                        "max_uses": 1, "max_timeout_ms": 120000, "required": True}

    class FakeResident:
        def __init__(self):
            self.starts = 0
            self.stops = 0
            self.packets = []

        async def start(self):
            self.starts += 1

        async def stop(self):
            self.stops += 1

        async def run_packet(self, packet_path, out, *, timeout_s, resume=None):
            self.packets.append({"packet_path": Path(packet_path),
                                 "out": Path(out), "timeout_s": timeout_s,
                                 "resume": resume})
            return {"delivery_ok": True, "session_id": "sess_res1",
                    "outcome": "delivered", "changed_files": ["OUT.md"],
                    "execution_problems": []}

    def delegate_resident(self, contract_id, resident):
        return asyncio.run(H.delegate_impl(
            self.config, self.evidence_root, contract_id,
            {"agent": "codex-control", "session_id": "sess_ctrl1"},
            codex_path=str(self.fake_codex),
            resident_factory=lambda: resident))

    def test_resident_delegate_converts_contract_into_packet(self):
        resident = self.FakeResident()
        self.register(self.zcode_contract(
            mode="resident", allowed_files=["OUT.md"],
            commands=[self.command], prompt="Write OUT.md then stop."))
        result = self.delegate_resident("hub-zcode-1", resident)
        self.assertEqual(result["worker"], "zcode")
        self.assertEqual(result["outcome"], "delivered")
        self.assertTrue(result["delivery_ok"])
        self.assertEqual(result["changed_files"], ["OUT.md"])
        self.assertEqual(result["problems"], [])
        self.assertEqual(result["session_locator"],
                         {"agent": "zcode", "session_id": "sess_res1"})
        # the packet handed to run_packet carries the contract payload
        self.assertEqual(len(resident.packets), 1)
        call = resident.packets[0]
        self.assertEqual(call["timeout_s"], 60)
        self.assertIsNone(call["resume"])
        packet = json.loads(call["packet_path"].read_text())
        self.assertEqual(packet["workspace"], str(self.workspace))
        self.assertEqual(packet["prompt"], "Write OUT.md then stop.")
        self.assertEqual(packet["allowed_files"], ["OUT.md"])
        self.assertEqual(packet["commands"], [self.command])
        self.assertEqual(packet["model"], {"providerId": "glm",
                                           "modelId": "glm-5.3"})
        self.assertEqual(packet["trusted_hashes"], {})
        # packet lives under the hub evidence tree, run.json under work_root
        self.assertEqual(call["packet_path"],
                         self.evidence_root / "tasks" / result["task_id"] / "packet.json")
        self.assertEqual(call["out"],
                         self.work_root / "hub-tasks" / result["task_id"] / "run.json")
        record = json.loads((self.state_dir / "hub" / "tasks" /
                             f"{result['task_id']}.json").read_text())
        self.assertEqual(record["status"], "completed")
        self.assertEqual(record["outcome"], "delivered")
        self.assertEqual(record["session_locator"],
                         {"agent": "zcode", "session_id": "sess_res1"})

    def test_resident_singleton_starts_once_and_serves_repeated_delegates(self):
        resident = self.FakeResident()
        self.register(self.zcode_contract(mode="resident"))
        workers = H.ResidentWorkers(self.config, self.evidence_root,
                                    resident_factory=lambda: resident)
        for expected_session in ("sess_res1", "sess_res1"):
            result = asyncio.run(H.delegate_impl(
                self.config, self.evidence_root, "hub-zcode-1",
                {"agent": "a", "session_id": "s"},
                codex_path=str(self.fake_codex), workers=workers))
            self.assertEqual(result["outcome"], expected_session and "delivered")
            self.assertEqual(result["session_locator"]["session_id"], expected_session)
        self.assertEqual(resident.starts, 1)  # one shared start, reused
        self.assertEqual(len(resident.packets), 2)

    def test_resident_start_failure_is_a_runner_error_task(self):
        class BrokenResident:
            async def start(self):
                raise RuntimeError("resident boot failed")

        self.register(self.zcode_contract(mode="resident"))
        result = asyncio.run(H.delegate_impl(
            self.config, self.evidence_root, "hub-zcode-1",
            {"agent": "a", "session_id": "s"},
            codex_path=str(self.fake_codex),
            resident_factory=BrokenResident))
        self.assertEqual(result["outcome"], "runner-error")
        self.assertIn("RuntimeError: resident boot failed", result["error"])
        record = json.loads((self.state_dir / "hub" / "tasks" /
                             f"{result['task_id']}.json").read_text())
        self.assertEqual(record["status"], "failed")
        self.assertEqual(record["outcome"], "runner-error")

    def test_explicit_cold_mode_keeps_the_injected_cold_runner(self):
        captured = {}

        async def fake_cold_runner(config, task_id, contract, timeout_s):
            captured["contract"] = contract
            return {"delivery_ok": True, "session_id": "sess_cold1",
                    "execution_problems": [], "changed_files": [],
                    "outcome": "delivered", "run_path": None, "db_path": None}

        self.register(self.zcode_contract(mode="cold"))
        result = self.delegate("hub-zcode-1", runner=fake_cold_runner,
                               worker="zcode")
        self.assertEqual(result["outcome"], "delivered")
        self.assertEqual(captured["contract"]["mode"], "cold")


class DelegateCodexAppServerTests(HubFixture):
    """codex+app-server dispatch: real Authority + shared client + gate."""

    def setUp(self):
        super().setUp()
        self.command = {"id": "list-files", "command": "/bin/ls",
                        "max_uses": 1, "max_timeout_ms": 120000, "required": True}

    class FakeClient:
        def __init__(self):
            self.starts = 0
            self.closes = 0

        async def start(self):
            self.starts += 1
            return {"userAgent": "fake-appserver"}

        async def close(self):
            self.closes += 1

    def appserver_contract(self, **overrides):
        contract = self.codex_contract(mode="app-server", sandbox="workspace-write",
                                       allowed_files=["OUT.md"],
                                       commands=[self.command], **overrides)
        del contract["output_schema"]  # exec-only mechanism
        return contract

    def test_appserver_delegate_builds_real_authority_and_passes_policy(self):
        captured = {}

        class FakeDriver:
            def __init__(self, client):
                captured["client"] = client

            async def run_task(self, authority, workspace, prompt, *, model=None,
                               sandbox="read-only", approval_policy="on-request",
                               timeout_s=600):
                captured.update({"authority": authority, "workspace": workspace,
                                 "prompt": prompt, "model": model,
                                 "sandbox": sandbox,
                                 "approval_policy": approval_policy,
                                 "timeout_s": timeout_s})
                return {"outcome": "delivered", "thread_id": "th_app1",
                        "turn_id": "turn_app1", "final_message": "done text",
                        "command_items": [{"command": "/bin/ls", "exit_code": 0,
                                           "status": "completed"}],
                        "approvals": [{"method": "item/commandExecution/requestApproval",
                                       "decision": "allow"}]}

        client = self.FakeClient()
        self.register(self.appserver_contract())
        real_inventory = H.inventory
        with patch("zcode_supervisor.hub_server.inventory",
                   side_effect=lambda ws: real_inventory(ws)) as inventory_spy:
            result = asyncio.run(H.delegate_impl(
                self.config, self.evidence_root, "hub-codex-1",
                {"agent": "codex-control", "session_id": "sess_ctrl1"},
                codex_path=str(self.fake_codex),
                appserver_factory=lambda: client,
                appserver_driver_factory=lambda c: FakeDriver(c)))
        self.assertEqual(inventory_spy.call_count, 1)  # frozen pre-run inventory
        authority = captured["authority"]
        self.assertIsInstance(authority, Authority)  # real Authority, not a stub
        self.assertEqual(authority.allowed, {"OUT.md"})
        packet = json.loads(authority.packet_path.read_text())
        self.assertEqual(packet["workspace"], str(self.workspace))
        self.assertEqual(packet["allowed_files"], ["OUT.md"])
        self.assertEqual(packet["commands"], [self.command])
        self.assertEqual(packet["model"], {"model": "gpt-5-codex"})
        # driver contract: untrusted approvals, contract sandbox, model name
        self.assertIs(captured["client"], client)
        self.assertEqual(captured["workspace"], self.workspace)
        self.assertEqual(captured["prompt"], "Read TASK.md and answer.")
        self.assertEqual(captured["approval_policy"], "untrusted")
        self.assertEqual(captured["sandbox"], "workspace-write")
        self.assertEqual(captured["model"], "gpt-5-codex")
        self.assertEqual(captured["timeout_s"], 60.0)
        # result/locator mapping
        self.assertEqual(result["worker"], "codex")
        self.assertEqual(result["outcome"], "delivered")
        self.assertEqual(result["session_locator"],
                         {"agent": "codex", "thread_id": "th_app1", "path": None})
        self.assertEqual(result["thread_id"], "th_app1")
        self.assertEqual(result["final_message"], "done text")
        self.assertEqual(result["command_items"][0]["command"], "/bin/ls")
        self.assertEqual(result["approvals"][0]["decision"], "allow")
        # per-task receipt copy lands next to the packet in the evidence tree
        task_dir = self.evidence_root / "tasks" / result["task_id"]
        receipt = json.loads((task_dir / "appserver-receipt.json").read_text())
        self.assertEqual(receipt["thread_id"], "th_app1")
        record = json.loads((self.state_dir / "hub" / "tasks" /
                             f"{result['task_id']}.json").read_text())
        self.assertEqual(record["status"], "completed")
        self.assertEqual(record["result"]["final_message"], "done text")

    def test_concurrent_appserver_delegates_overlap_by_default(self):
        # 2026-10-26 P3: the shared app-server client runs concurrent
        # delegates; the default gate is a no-op. Two DISTINCT workspaces are
        # required — the per-workspace advisory lock still serializes writes
        # to one workspace across vendors.
        entries = []
        release = asyncio.Event()
        windows = []  # (workspace, entered_at, exited_at) monotonic triples

        class OverlapDriver:
            def __init__(self, client):
                pass

            async def run_task(self, authority, workspace, prompt, *, model=None,
                               sandbox="read-only", approval_policy="on-request",
                               timeout_s=600):
                entered_at = time.monotonic()
                entries.append(workspace)
                await release.wait()  # hold the window open until both are in
                windows.append((workspace, entered_at, time.monotonic()))
                return {"outcome": "delivered", "thread_id": "th_ov",
                        "final_message": None, "command_items": [], "approvals": []}

        client = self.FakeClient()
        ws_b = self.work_root / "task-ws-b"
        ws_b.mkdir()
        self.register(self.appserver_contract())
        self.register(self.appserver_contract(contract_id="hub-codex-b",
                                              workspace=str(ws_b)))
        workers = H.ResidentWorkers(
            self.config, self.evidence_root, codex_path=str(self.fake_codex),
            appserver_factory=lambda: client,
            appserver_driver_factory=lambda c: OverlapDriver(c))

        async def two_at_once():
            tasks = [asyncio.create_task(H.delegate_impl(
                self.config, self.evidence_root, contract_id,
                {"agent": "a", "session_id": "s"},
                codex_path=str(self.fake_codex), workers=workers))
                for contract_id in ("hub-codex-1", "hub-codex-b")]

            async def wait_for_both():
                while len(entries) < 2:
                    await asyncio.sleep(0.01)
            # both delegates must be inside run_task at the same time; under
            # the old P2 lock this wait would deadlock into the timeout
            await asyncio.wait_for(wait_for_both(), 5)
            self.assertEqual(len(entries), 2)  # both in flight simultaneously
            release.set()
            return await asyncio.gather(*tasks)

        results = asyncio.run(two_at_once())
        self.assertEqual([r["outcome"] for r in results],
                         ["delivered", "delivered"])
        self.assertEqual(client.starts, 1)  # one shared client start
        # belt and braces: the recorded execution windows overlap
        self.assertLess(max(w[1] for w in windows), min(w[2] for w in windows))

    def test_appserver_concurrency_one_restores_serial_gate(self):
        # Rollback path: appserver_concurrency=1 makes delegates queue (the
        # P2 serial semantics), so run_task windows never overlap.
        entries = []
        release = asyncio.Event()
        windows = []

        class SerialDriver:
            def __init__(self, client):
                pass

            async def run_task(self, authority, workspace, prompt, **kwargs):
                entered_at = time.monotonic()
                entries.append(workspace)
                await release.wait()
                windows.append((workspace, entered_at, time.monotonic()))
                return {"outcome": "delivered", "thread_id": "th_ser",
                        "final_message": None, "command_items": [], "approvals": []}

        client = self.FakeClient()
        ws_b = self.work_root / "task-ws-b"
        ws_b.mkdir()
        self.register(self.appserver_contract())
        self.register(self.appserver_contract(contract_id="hub-codex-b",
                                              workspace=str(ws_b)))
        workers = H.ResidentWorkers(
            self.config, self.evidence_root, codex_path=str(self.fake_codex),
            appserver_factory=lambda: client,
            appserver_driver_factory=lambda c: SerialDriver(c),
            appserver_concurrency=1)

        async def queued():
            tasks = [asyncio.create_task(H.delegate_impl(
                self.config, self.evidence_root, contract_id,
                {"agent": "a", "session_id": "s"},
                codex_path=str(self.fake_codex), workers=workers))
                for contract_id in ("hub-codex-1", "hub-codex-b")]

            async def wait_for_first():
                while not entries:
                    await asyncio.sleep(0.01)
            await asyncio.wait_for(wait_for_first(), 5)
            await asyncio.sleep(0.05)  # the second delegate must NOT enter
            self.assertEqual(len(entries), 1)
            release.set()
            return await asyncio.gather(*tasks)

        results = asyncio.run(queued())
        self.assertEqual([r["outcome"] for r in results],
                         ["delivered", "delivered"])
        ordered = sorted(windows, key=lambda w: w[1])
        self.assertLessEqual(ordered[0][2], ordered[1][1])  # no overlap

    def test_appserver_client_start_failure_is_a_runner_error_task(self):
        class BrokenClient:
            async def start(self):
                raise RuntimeError("appserver boot failed")

        self.register(self.appserver_contract())
        result = asyncio.run(H.delegate_impl(
            self.config, self.evidence_root, "hub-codex-1",
            {"agent": "a", "session_id": "s"},
            codex_path=str(self.fake_codex),
            appserver_factory=BrokenClient))
        self.assertEqual(result["outcome"], "runner-error")
        self.assertIn("appserver boot failed", result["error"])

    def test_appserver_delegate_shares_one_budget_between_start_and_run(self):
        # P0.1 review spec-I5 (RED): the lazy app-server start and the
        # driver's run must live on ONE primary budget owned by this
        # delegate. A 0.25 s start plus a turn that only finishes within a
        # FRESH 0.4 s budget must time out on the shared budget — and the
        # delegate must bound the lazy initialize instead of starting it
        # unbounded.
        seen = {}

        class SlowStartClient:
            def __init__(self):
                self.starts = 0

            async def start(self, budget=None, initialize_timeout=None):
                self.starts += 1
                seen["start_budget"] = budget
                seen["initialize_timeout"] = initialize_timeout
                await asyncio.sleep(0.25)
                return {"userAgent": "fake-appserver"}

            async def close(self):
                pass

        class BudgetAwareDriver:
            def __init__(self, client):
                pass

            async def run_task(self, authority, workspace, prompt, *, model=None,
                               sandbox="read-only", approval_policy="on-request",
                               timeout_s=600, budget=None):
                seen["run_budget"] = budget
                deadline = (budget.primary_deadline if budget is not None
                            else time.monotonic() + timeout_s)
                await asyncio.sleep(0.20)  # turn finishes at +0.20 s
                if time.monotonic() < deadline:
                    return {"outcome": "delivered", "thread_id": "th_shared",
                            "turn_id": "turn_shared", "final_message": "ok",
                            "command_items": [], "approvals": []}
                return {"outcome": "timeout", "thread_id": "th_shared",
                        "turn_id": "turn_shared", "final_message": None,
                        "command_items": [], "approvals": []}

        client = SlowStartClient()
        self.register(self.appserver_contract(max_timeout_ms=400))
        result = asyncio.run(H.delegate_impl(
            self.config, self.evidence_root, "hub-codex-1",
            {"agent": "a", "session_id": "s"},
            codex_path=str(self.fake_codex),
            appserver_factory=lambda: client,
            appserver_driver_factory=lambda c: BudgetAwareDriver(c)))
        # the SAME CallBudget OBJECT reached the lazy start and the driver —
        # not a numeric copy, not a second full budget
        from zcode_supervisor.deadline import CallBudget
        self.assertIsInstance(seen.get("start_budget"), CallBudget)
        self.assertIsInstance(seen.get("run_budget"), CallBudget)
        self.assertIs(seen["start_budget"], seen["run_budget"])
        self.assertLessEqual(seen["run_budget"].timeout_s, 0.4)
        # shared budget: 0.25 s start + 0.20 s turn > 0.4 s -> timeout
        self.assertEqual(result["outcome"], "timeout")

    def test_explicit_exec_mode_keeps_the_exec_runner_path(self):
        self.register(self.codex_contract(mode="exec"))
        self.make_rollout("thread-hub-1")
        result = self.delegate("hub-codex-1")
        # the exec branch maps structured/schema fields the app-server branch
        # never produces — proof the original path ran
        self.assertEqual(result["structured"], {"answer": "ok"})
        self.assertTrue(result["session_locator"]["path"].endswith(
            "rollout-20261024T120000-thread-hub-1.jsonl"))

    def test_appserver_instance_resolves_codex_path_from_discovery(self):
        import asyncio
        from zcode_supervisor import hub_server as H
        async def scenario():
            workers = H.ResidentWorkers(self.config, self.evidence_root, codex_path=None)
            with patch.object(H, "_fallback_codex", return_value={"path": "/usr/local/bin/fake-codex"}) \
                 as fallback, \
                 patch("zcode_supervisor.codex_appserver.CodexAppServerClient") as client_cls:
                client_cls.return_value.start = AsyncMock()
                instance = await workers.appserver_instance()
                client_cls.assert_called_once_with(
                    "/usr/local/bin/fake-codex", self.evidence_root / "appserver")
                self.assertIs(instance, client_cls.return_value)
                self.assertTrue(fallback.called)
        asyncio.run(scenario())

    def test_appserver_failed_start_uses_fresh_evidence_dir_on_retry(self):
        import asyncio
        from zcode_supervisor import hub_server as H
        async def scenario():
            workers = H.ResidentWorkers(self.config, self.evidence_root, codex_path=None)
            with patch.object(H, "_fallback_codex", return_value={"path": "/usr/local/bin/fake-codex"}), \
                 patch("zcode_supervisor.codex_appserver.CodexAppServerClient") as client_cls:
                first = client_cls.return_value
                first.start = AsyncMock(side_effect=RuntimeError("boom"))
                with self.assertRaises(RuntimeError):
                    await workers.appserver_instance()
                second = client_cls.return_value
                second.start = AsyncMock()
                instance = await workers.appserver_instance()
                calls = client_cls.call_args_list
                self.assertEqual(calls[0].args[1].name, "appserver")
                self.assertEqual(calls[1].args[1].name, "appserver-2")
                self.assertIs(instance, second)
        asyncio.run(scenario())

    def test_appserver_without_codex_path_and_factory_is_unavailable(self):
        self.register(self.appserver_contract())
        # Hermetic: without this patch the fallback discovery would find the
        # real machine codex bundle and dispatch a REAL app-server.
        with patch("zcode_supervisor.hub_server._fallback_codex",
                   return_value={"installed": False, "path": None}), \
                self.assertRaises(H.HubError) as caught:
            asyncio.run(H.delegate_impl(
                self.config, self.evidence_root, "hub-codex-1",
                {"agent": "a", "session_id": "s"},
                codex_runner=self.default_fake_codex()))
        self.assertEqual(caught.exception.code, "worker-unavailable")


class ReadContextTests(HubFixture):
    def test_codex_task_context_comes_from_session_reader(self):
        self.register(self.codex_contract())
        self.make_rollout("thread-hub-1")
        result = self.delegate("hub-codex-1")
        context = H.read_context_impl(self.config, result["task_id"])
        self.assertEqual(context["hub_tool_version"], 1)
        self.assertEqual(context["worker"], "codex")
        summary = context["context"]["summary"]
        self.assertEqual(summary["total_lines"], 1)

    def test_zcode_task_context_is_conservative(self):
        db_path = self.make_zcode_db()

        async def fake_zcode_runner(config, task_id, contract, timeout_s):
            return {"delivery_ok": True, "session_id": "sess_hub1",
                    "execution_problems": [], "changed_files": [],
                    "outcome": "delivered", "run_path": None,
                    "db_path": str(db_path)}

        self.register(self.zcode_contract())
        result = self.delegate("hub-zcode-1", runner=fake_zcode_runner,
                               worker="zcode")
        context = H.read_context_impl(self.config, result["task_id"], offset=0,
                                      max_items=10)
        body = context["context"]
        self.assertEqual(body["status"], "located")
        self.assertEqual(body["session"]["id"], "sess_hub1")
        self.assertEqual(body["messages"][0]["text"], "done")
        # the planted credential must not survive the whitelist
        self.assertNotIn("sk-LEAK", json.dumps(context))

    def test_non_hub_task_is_refused(self):
        with self.assertRaises(H.HubError) as caught:
            H.read_context_impl(self.config, "not-a-hub-task")
        self.assertEqual(caught.exception.code, "not-hub-task")
        self.assertIn("not a hub-dispatched task", caught.exception.detail)

    def test_task_without_session_reports_no_session_context(self):
        def runner_without_thread(*args, **kwargs):
            return {"outcome": "failed", "thread_id": None, "structured": None,
                    "error": "no thread"}
        self.register(self.codex_contract())
        result = self.delegate("hub-codex-1", runner=runner_without_thread)
        with self.assertRaises(H.HubError) as caught:
            H.read_context_impl(self.config, result["task_id"])
        self.assertEqual(caught.exception.code, "no-session-context")


class ListSessionsTests(HubFixture):
    def test_index_pagination_and_worker_filter(self):
        def make_runner(name):
            def runner(codex_path, workspace, prompt, output_schema,
                       evidence_dir, timeout_s, model=None):
                Path(evidence_dir).mkdir(parents=True, exist_ok=True)
                return {"outcome": "delivered", "thread_id": name,
                        "structured": {"answer": "ok"}, "error": None}
            return runner
        self.register(self.codex_contract())
        self.register(self.zcode_contract())
        self.delegate("hub-codex-1", runner=make_runner("t1"))

        async def zcode_run(config, task_id, contract, timeout_s):
            return {"delivery_ok": True, "session_id": "s1",
                    "execution_problems": [], "changed_files": [],
                    "outcome": "delivered", "run_path": None, "db_path": None}
        asyncio.run(H.delegate_impl(
            self.config, self.evidence_root, "hub-zcode-1",
            {"agent": "a", "session_id": "s"}, codex_path=str(self.fake_codex),
            zcode_runner=zcode_run))

        first = H.list_sessions_impl(self.config, None, 0, 1)
        self.assertEqual(first["total"], 2)
        self.assertEqual(len(first["sessions"]), 1)
        self.assertEqual(first["next_cursor"], 1)
        second = H.list_sessions_impl(self.config, None, 1, 1)
        self.assertEqual(second["returned"], 1)
        self.assertIsNone(second["next_cursor"])
        only_codex = H.list_sessions_impl(self.config, "codex", 0, 50)
        self.assertEqual(only_codex["total"], 1)
        self.assertEqual(only_codex["sessions"][0]["worker"], "codex")
        with self.assertRaises(H.HubError):
            H.list_sessions_impl(self.config, None, 0, 51)
        with self.assertRaises(H.HubError):
            H.list_sessions_impl(self.config, "not-a-worker")


class ListSkillsTests(HubFixture):
    def test_enumerates_skill_names_per_root_without_reading_bodies(self):
        roots = []
        for source in ("user", "agents"):
            base = self.root / f"{source}-skills"
            for name in ("alpha", "beta"):
                skill = base / name
                skill.mkdir(parents=True)
                (skill / "SKILL.md").write_text("secret body", encoding="utf-8")
            roots.append((base, "*/SKILL.md"))
        cache = self.root / "plugins" / "cache"
        nested = cache / "p1" / "0.1.0" / "skills" / "gamma"
        nested.mkdir(parents=True)
        (nested / "SKILL.md").write_text("x", encoding="utf-8")
        roots.append((cache, "**/skills/*/SKILL.md"))
        listing = H.list_skills_impl("zcode", roots=roots)
        # the same name under two different roots is listed once per root
        self.assertEqual([entry["name"] for entry in listing["skills"]],
                         ["alpha", "alpha", "beta", "beta", "gamma"])
        roots_seen = {entry["root"] for entry in listing["skills"]}
        self.assertEqual(roots_seen, {str(roots[0][0]), str(roots[1][0]), str(cache)})
        gamma = listing["skills"][-1]
        self.assertEqual(gamma["root"], str(cache))
        self.assertNotIn("secret body", json.dumps(listing))

    def test_invalid_worker_is_refused(self):
        with self.assertRaises(H.HubError):
            H.list_skills_impl("dsh")


class StdoutIsolationTests(HubFixture):
    """Under the stdio MCP transport, stdout is the wire: any print from a
    runner must land on stderr, never on the process stdout."""

    def _captured(self):
        return io.StringIO(), io.StringIO()

    def test_sync_runner_print_stays_off_stdout(self):
        def leaking_runner(codex_path, workspace, prompt, output_schema,
                           evidence_dir, timeout_s, model=None):
            print("LEAK-SYNC")
            Path(evidence_dir).mkdir(parents=True, exist_ok=True)
            return {"outcome": "delivered", "thread_id": None,
                    "structured": {"answer": "ok"}, "error": None}
        self.register(self.codex_contract())
        out, err = self._captured()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            result = self.delegate("hub-codex-1", runner=leaking_runner)
        self.assertEqual(result["outcome"], "delivered")
        self.assertNotIn("LEAK-SYNC", out.getvalue())
        self.assertIn("LEAK-SYNC", err.getvalue())

    def test_async_runner_print_stays_off_stdout(self):
        async def leaking_zcode_runner(config, task_id, contract, timeout_s):
            print("LEAK-ASYNC")
            return {"delivery_ok": True, "session_id": None,
                    "execution_problems": [], "changed_files": [],
                    "outcome": "delivered", "run_path": None, "db_path": None}
        self.register(self.zcode_contract())
        out, err = self._captured()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            result = self.delegate("hub-zcode-1", runner=leaking_zcode_runner,
                                   worker="zcode")
        self.assertEqual(result["outcome"], "delivered")
        self.assertNotIn("LEAK-ASYNC", out.getvalue())
        self.assertIn("LEAK-ASYNC", err.getvalue())


class DefaultZcodeRunnerTests(HubFixture):
    def test_default_runner_passes_quiet_to_protocol_execute(self):
        self.register(self.zcode_contract())
        contract = contracts.load_contract(self.state_dir, "hub-zcode-1")
        captured = {}

        async def fake_execute(args, config):
            captured["args"] = args
            out_path = Path(args.out)
            out_path.parent.mkdir(parents=True, exist_ok=True)
            out_path.write_text(json.dumps({
                "delivery_ok": True, "session_id": "sess-quiet",
                "execution_problems": [], "changed_files": [],
                "outcome": "delivered"}), encoding="utf-8")
            return True

        with patch("zcode_supervisor.protocol.execute", fake_execute):
            summary = asyncio.run(H.default_zcode_runner(
                self.config, "task-quiet-1", contract, None))
        self.assertTrue(captured["args"].quiet)  # keeps the run summary off stdout
        self.assertTrue(summary["delivery_ok"])
        self.assertEqual(summary["session_id"], "sess-quiet")
        self.assertEqual(summary["outcome"], "delivered")
        self.assertTrue(Path(summary["run_path"]).is_file())
        # the packet written for the executor carries the contract payload
        packet = json.loads((Path(self.work_root) / "hub-tasks" / "task-quiet-1"
                             / "packet.json").read_text())
        self.assertEqual(packet["prompt"], contract["prompt"])


class ErrorSurfaceTests(HubFixture):
    """The call_tool dispatcher must answer every failure with an is_error
    CallToolResult carrying JSON — never a JSON-RPC protocol error (the
    client SDK rejects those when error.data is a string)."""

    def _call(self, server, name, arguments):
        import mcp.types as types
        handler = server._request_handlers["tools/call"].handler
        return asyncio.run(handler(
            None, types.CallToolRequestParams(name=name, arguments=arguments)))

    def test_unexpected_exception_becomes_internal_error_result(self):
        server = H.build_server(self.config, self.evidence_root)
        with patch.object(H, "read_context_impl",
                          side_effect=RuntimeError("boom")):
            result = self._call(server, "read_context", {"task_id": "anytask"})
        self.assertTrue(result.is_error)
        body = json.loads(result.content[0].text)
        self.assertEqual(body["hub_tool_version"], 1)
        self.assertEqual(body["error"], "internal-error")
        self.assertEqual(body["detail"], "RuntimeError: boom")
        self.assertNotIn("Traceback", result.content[0].text)  # no stack

    def test_hub_error_becomes_is_error_result_with_code(self):
        server = H.build_server(self.config, self.evidence_root)
        result = self._call(server, "read_context", {"task_id": "not-a-hub-task"})
        self.assertTrue(result.is_error)
        body = json.loads(result.content[0].text)
        self.assertEqual(body["error"], "not-hub-task")
        self.assertIn("not a hub-dispatched task", body["detail"])

    def test_unknown_tool_is_an_is_error_result_not_a_protocol_error(self):
        server = H.build_server(self.config, self.evidence_root)
        result = self._call(server, "no-such-tool", {})
        self.assertTrue(result.is_error)
        self.assertEqual(json.loads(result.content[0].text)["error"],
                         "unknown-tool")

    def test_runner_crash_through_dispatcher_is_a_recorded_outcome(self):
        # a runner that raises is a normal task outcome (runner-error), kept
        # inside a successful tool result — not an internal-error surface
        def crashing_runner(*args, **kwargs):
            raise RuntimeError("boom")
        self.register(self.codex_contract())
        server = H.build_server(self.config, self.evidence_root,
                                codex_path=str(self.fake_codex),
                                codex_runner=crashing_runner)
        result = self._call(server, "delegate", {
            "contract_id": "hub-codex-1",
            "initiator": {"agent": "codex", "session_id": "sess-x"}})
        self.assertFalse(result.is_error)
        body = json.loads(result.content[0].text)
        self.assertEqual(body["outcome"], "runner-error")
        self.assertIn("RuntimeError: boom", body["error"])


class MainTests(HubFixture):
    def _config_file(self) -> Path:
        value = {
            "schema_version": 1,
            "work_root": str(self.work_root),
            "state_dir": str(self.state_dir),
            "cache_dir": str(self.cache_dir),
            "cli_path": str(self.fake_cli),
            "node": "node",
            "compatibility": "pinned-0.16.9",
        }
        path = self.root / "config.json"
        path.write_text(json.dumps(value), encoding="utf-8")
        return path

    def test_main_rejects_invalid_config_without_serving(self):
        bad = self.root / "bad.json"
        bad.write_text("{}", encoding="utf-8")
        self.assertEqual(H.main(["--config", str(bad)]), 2)


@dataclass
class _StdioProbe:
    passed: bool


class StdioRoundTripTests(HubFixture):
    """Real MCP stdio transport against a subprocess hub server."""

    def _config_file(self) -> Path:
        value = {
            "schema_version": 1,
            "work_root": str(self.work_root),
            "state_dir": str(self.state_dir),
            "cache_dir": str(self.cache_dir),
            "cli_path": str(self.fake_cli),
            "node": "node",
            "compatibility": "pinned-0.16.9",
        }
        path = self.root / "config.json"
        path.write_text(json.dumps(value), encoding="utf-8")
        return path

    def test_initialize_list_six_tools_and_call_two(self):
        import anyio
        from mcp import ClientSession, StdioServerParameters
        from mcp.client.stdio import stdio_client

        config_path = self._config_file()

        async def scenario():
            params = StdioServerParameters(
                command=sys.executable,
                args=["-m", "zcode_supervisor.hub_server",
                      "--config", str(config_path),
                      "--evidence-root", str(self.evidence_root)],
                cwd=str(Path(__file__).resolve().parents[1]))
            async with stdio_client(params) as (read, write):
                async with ClientSession(read, write) as session:
                    init = await session.initialize()
                    self.assertEqual(init.server_info.name, "czs-hub")
                    tools = await session.list_tools()
                    self.assertEqual(sorted(t.name for t in tools.tools),
                                     TOOL_NAMES)
                    health = await session.call_tool("health", {})
                    self.assertFalse(health.is_error)
                    body = json.loads(health.content[0].text)
                    self.assertEqual(body["hub_tool_version"], 1)
                    self.assertIn("workers", body)
                    refused = await session.call_tool(
                        "delegate", {"contract_id": "nope",
                                     "initiator": {"agent": "codex",
                                                   "session_id": "s1"}})
                    self.assertTrue(refused.is_error)
                    error_body = json.loads(refused.content[0].text)
                    self.assertEqual(error_body["error"], "unknown-contract")
                    return True

        self.assertTrue(anyio.run(scenario))
        # startup created the confined hub state directories
        self.assertTrue((self.state_dir / "hub" / "contracts").is_dir())
        self.assertTrue((self.state_dir / "hub" / "tasks").is_dir())
        self.assertTrue(self.evidence_root.is_dir())

    def test_read_context_without_session_is_is_error_not_protocol_error(self):
        # Real-machine regression (2026-10-25): read_context on a task with no
        # locatable session used to surface as a JSON-RPC error whose data was
        # a string, which the client SDK rejects on validation. It must come
        # back as a parseable is_error tool result.
        import anyio
        from mcp import ClientSession, StdioServerParameters
        from mcp.client.stdio import stdio_client

        config_path = self._config_file()
        tasks_dir = self.state_dir / "hub" / "tasks"
        tasks_dir.mkdir(parents=True, exist_ok=True)
        (tasks_dir / "nosession1.json").write_text(json.dumps({
            "task_id": "nosession1", "worker": "codex", "status": "completed",
            "outcome": "failed", "created": "2026-10-25T00:00:00+00:00",
            "updated": "2026-10-25T00:00:01+00:00"}), encoding="utf-8")

        async def scenario():
            params = StdioServerParameters(
                command=sys.executable,
                args=["-m", "zcode_supervisor.hub_server",
                      "--config", str(config_path),
                      "--evidence-root", str(self.evidence_root)],
                cwd=str(Path(__file__).resolve().parents[1]))
            async with stdio_client(params) as (read, write):
                async with ClientSession(read, write) as session:
                    await session.initialize()
                    # no McpError may escape: the answer is a tool result
                    result = await session.call_tool(
                        "read_context", {"task_id": "nosession1"})
                    self.assertTrue(result.is_error)
                    body = json.loads(result.content[0].text)
                    self.assertEqual(body["hub_tool_version"], 1)
                    self.assertEqual(body["error"], "no-session-context")
                    self.assertIn("detail", body)
                    return True

        self.assertTrue(anyio.run(scenario))


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
