"""Bench tests: every timing path is faked; no real CLI, node or codex runs.

ProtocolClient is replaced by a fake class, codex spawns by patched helpers,
and the parallel-agent modules (zcode_resident / codex_appserver) are either
forced to fail importing or replaced by injected fakes, so this suite stays
hermetic even after those modules land in the package.
"""
import asyncio
from contextlib import contextmanager
import hashlib
import io
from contextlib import redirect_stdout
import json
from pathlib import Path
import subprocess
import sys
import time
from tempfile import TemporaryDirectory
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from zcode_supervisor import bench  # noqa: E402


@contextmanager
def _patch_submodule_entry(name, replacement):
    """Swap sys.modules[name] AND the parent-package attribute.

    ``from package import leaf`` resolves the parent attribute first, so a
    module imported earlier in the same process (the parallel agents' own
    tests do exactly that) would bypass a plain sys.modules swap.
    """
    parent_name, _, leaf = name.rpartition(".")
    parent = sys.modules.get(parent_name)
    saved_module = sys.modules.get(name)
    saved_attr = getattr(parent, leaf, None) if parent is not None else None
    sys.modules[name] = replacement
    if parent is not None:
        try:
            delattr(parent, leaf)
        except AttributeError:
            pass
        if replacement is not None:
            setattr(parent, leaf, replacement)
    try:
        yield
    finally:
        if saved_module is None:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = saved_module
        if parent is not None:
            if saved_attr is None:
                try:
                    delattr(parent, leaf)
                except AttributeError:
                    pass
            else:
                setattr(parent, leaf, saved_attr)


@contextmanager
def forced_import_failure(name):
    """Make ``import <name>`` fail (None in sys.modules) for the block."""
    with _patch_submodule_entry(name, None):
        yield


@contextmanager
def injected_module(name, **attributes):
    """Serve a fake module for ``from <package> import <name>`` inside the block."""
    module = ModuleType(name)
    for key, value in attributes.items():
        setattr(module, key, value)
    with _patch_submodule_entry(name, module):
        yield module


class FakeClient:
    """Duck-typed ProtocolClient: create/subscribe/setModel/send answer at once."""

    instances = []

    def __init__(self, workspace, output, permission, config=None, orchestration=None):
        self.workspace = workspace
        self.output = Path(output)
        self.config = config
        self.events = []
        self.failure = None
        self.started = 0
        self.closed = 0
        self.calls = []
        self.creates = 0
        self.session_id = None
        type(self).instances.append(self)

    async def start(self):
        self.started += 1
        if self.started > 1:
            raise ValueError("cannot start twice")

    async def close(self):
        self.closed += 1

    async def request(self, method, params):
        self.calls.append(method)
        if method == "session/create":
            self.creates += 1
            self.session_id = f"sess-{id(self)}-{self.creates}"
            return {"session": {"sessionId": self.session_id}}
        if method == "session/setModel":
            return {}
        if method == "session/subscribe":
            return {"eventSeq": 0}
        if method == "session/send":
            self.events.append({"type": "turn.completed", "sessionId": self.session_id,
                                "seq": 1, "payload": {"resultType": "success"}})
            return {}
        raise ValueError(f"unexpected method {method}")


class FakeJsonl:
    """Duck-typed codex app-server client with optional ephemeral rejection."""

    instances = []

    def __init__(self, codex_path, reject_ephemeral=False):
        self.codex_path = codex_path
        self.reject_ephemeral = reject_ephemeral
        self.started = 0
        self.closed = 0
        self.requests = []
        type(self).instances.append(self)

    async def start(self):
        self.started += 1

    async def close(self):
        self.closed += 1

    async def request(self, method, params, timeout=None):
        self.requests.append((method, params))
        if method == "thread/start" and self.reject_ephemeral \
                and params.get("ephemeral"):
            raise ValueError("thread/start failed: ephemeral not supported")
        return {}


def fake_codex_file(root: Path) -> Path:
    path = root / "codex"
    path.write_text("#!/bin/sh\nexit 0\n")
    return path


class PercentileStatsTests(unittest.TestCase):
    def test_percentile_interpolates_linearly(self):
        samples = [1.0, 2.0, 3.0, 4.0]
        self.assertEqual(bench.percentile(samples, 0), 1.0)
        self.assertEqual(bench.percentile(samples, 50), 2.5)
        self.assertAlmostEqual(bench.percentile(samples, 99), 3.97)
        self.assertEqual(bench.percentile(samples, 100), 4.0)

    def test_percentile_single_sample_and_empty_list(self):
        self.assertEqual(bench.percentile([7.0], 99), 7.0)
        with self.assertRaises(ValueError):
            bench.percentile([], 50)

    def test_percentile_rejects_bad_point(self):
        for bad in (-1, 101, True, float("nan")):
            with self.subTest(bad=bad):
                with self.assertRaises(ValueError):
                    bench.percentile([1.0, 2.0], bad)

    def test_stats_keys_and_values(self):
        result = bench.stats([2.0, 4.0])
        self.assertEqual(result["n"], 2)
        self.assertEqual(result["min"], 2.0)
        self.assertEqual(result["p50"], 3.0)
        self.assertEqual(result["p90"], 3.8)
        self.assertEqual(result["p99"], 4.0)  # 3.98 rounded to one decimal
        self.assertEqual(result["max"], 4.0)
        self.assertEqual(result["mean"], 3.0)
        self.assertEqual(sorted(result), ["max", "mean", "min", "n", "p50", "p90",
                                          "p99"])

    def test_stats_single_and_empty(self):
        single = bench.stats([3.0])
        self.assertTrue(all(single[key] == 3.0 for key in ("min", "p50", "p90",
                                                           "p99", "max", "mean")))
        with self.assertRaises(ValueError):
            bench.stats([])


class MeasureTests(unittest.TestCase):
    def run_async(self, coroutine):
        return asyncio.run(coroutine)

    def test_warmup_calls_are_discarded(self):
        calls = {"n": 0}

        async def fn():
            calls["n"] += 1
            return float(calls["n"])

        samples = self.run_async(bench.measure(fn, 3, warmup=1))
        self.assertEqual(samples, [2.0, 3.0, 4.0])
        self.assertEqual(calls["n"], 4)

    def test_failed_trials_are_none_and_counted(self):
        calls = {"n": 0}

        async def fn():
            calls["n"] += 1
            if calls["n"] % 2 == 0:
                raise RuntimeError("boom")
            return 5.0

        samples = self.run_async(bench.measure(fn, 4, warmup=0))
        self.assertEqual(len(samples), 4)
        self.assertEqual([sample for sample in samples if sample is None], [None, None])
        self.assertEqual([sample for sample in samples if sample is not None],
                         [5.0, 5.0])
        self.assertEqual(calls["n"], 4)

    def test_none_return_falls_back_to_wall_clock(self):
        async def fn():
            await asyncio.sleep(0.001)

        samples = self.run_async(bench.measure(fn, 2, warmup=0))
        self.assertEqual(len(samples), 2)
        self.assertTrue(all(sample > 0.0 for sample in samples))

    def test_nonnegative_integer_arguments_required(self):
        async def fn():
            return 1.0

        for trials, warmup in ((-1, 1), (1, -1), (1.5, 1), (True, 0)):
            with self.subTest(trials=trials, warmup=warmup):
                with self.assertRaises(ValueError):
                    self.run_async(bench.measure(fn, trials, warmup=warmup))


class ZcodeBenchTests(unittest.TestCase):
    def setUp(self):
        self.temporary = TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.config = SimpleNamespace(work_root=self.root)
        FakeClient.instances = []

    def test_cold_new_client_per_trial_with_start_and_close(self):
        with patch.object(bench.protocol, "ProtocolClient", FakeClient):
            result = asyncio.run(bench.bench_zcode_cold(
                self.config, 4, scratch_dir=self.root / "scratch"))
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["n"], 4)
        for key in ("min", "p50", "p90", "p99", "max", "mean"):
            self.assertIn(key, result)
        self.assertEqual(len(FakeClient.instances), 4)
        outputs = set()
        for client in FakeClient.instances:
            self.assertEqual(client.started, 1)
            self.assertEqual(client.closed, 1)
            self.assertIn("session/create", client.calls)
            self.assertIn("session/subscribe", client.calls)
            outputs.add(client.output)
        self.assertEqual(len(outputs), 4)
        self.assertIn("session record", result["notes"])

    def test_cold_optional_set_model_is_timed(self):
        with patch.object(bench.protocol, "ProtocolClient", FakeClient):
            result = asyncio.run(bench.bench_zcode_cold(
                self.config, 2, set_model={"providerId": "p", "modelId": "m"},
                scratch_dir=self.root / "scratch"))
        self.assertEqual(result["n"], 2)
        self.assertTrue(all("session/setModel" in client.calls
                            for client in FakeClient.instances))
        self.assertIn("session/setModel", result["notes"])

    def test_resident_degrades_to_one_long_lived_client(self):
        with patch.object(bench.protocol, "ProtocolClient", FakeClient), \
                forced_import_failure("zcode_supervisor.zcode_resident"):
            result = asyncio.run(bench.bench_zcode_resident(
                self.config, 5, scratch_dir=self.root / "scratch"))
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["n"], 5)
        self.assertIn("degraded", result["notes"])
        self.assertEqual(len(FakeClient.instances), 1)
        client = FakeClient.instances[0]
        self.assertEqual(client.started, 1)
        self.assertEqual(client.closed, 1)
        # warmup + 5 trials on the same client
        self.assertEqual(client.calls.count("session/create"), 6)
        self.assertEqual(client.calls.count("session/subscribe"), 6)

    def test_resident_prefers_parallel_module_client(self):
        class FakeResident:
            instances = []

            def __init__(self, config=None):
                self.config = config
                self.client = None
                self.closed = 0
                type(self).instances.append(self)

            async def start(self):
                self.client = FakeClient("workspace", Path("resident.json"),
                                         None, self.config)

            async def close(self):
                self.closed += 1

        with patch.object(bench.protocol, "ProtocolClient", FakeClient), \
                injected_module("zcode_supervisor.zcode_resident",
                                ResidentZCode=FakeResident):
            result = asyncio.run(bench.bench_zcode_resident(
                self.config, 3, scratch_dir=self.root / "scratch"))
        self.assertIn("resident-module", result["notes"])
        self.assertEqual(result["n"], 3)
        # only the module-owned client exists; no bench-owned second client
        self.assertEqual(len(FakeClient.instances), 1)
        client = FakeClient.instances[0]
        self.assertEqual(client.calls.count("session/create"), 4)  # warmup + 3
        self.assertEqual(client.closed, 1)
        self.assertEqual(FakeResident.instances[0].closed, 1)

    def test_resident_module_failure_degrades(self):
        class BrokenResident:
            def __init__(self, config=None):
                pass

            async def start(self):
                raise RuntimeError("not ready")

            async def close(self):
                pass

        with patch.object(bench.protocol, "ProtocolClient", FakeClient), \
                injected_module("zcode_supervisor.zcode_resident",
                                ResidentZCode=BrokenResident):
            result = asyncio.run(bench.bench_zcode_resident(
                self.config, 2, scratch_dir=self.root / "scratch"))
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["n"], 2)
        self.assertIn("degraded", result["notes"])
        self.assertIn("RuntimeError", result["notes"])
        self.assertEqual(len(FakeClient.instances), 1)

    def test_resident_against_landed_module_with_fake_client(self):
        """Adapter smoke against the REAL ResidentZCode once it has landed.

        The real holder builds its client through protocol.ProtocolClient,
        which stays patched to FakeClient, so no process is spawned; if the
        landed constructor drifts away from every attempted shape the bench
        degrades, and that outcome is accepted too (covered above).
        """
        try:
            from zcode_supervisor import zcode_resident  # noqa: F401
        except Exception:
            self.skipTest("zcode_resident has not landed yet")
        state = self.root / "state"
        state.mkdir()
        config = SimpleNamespace(state_dir=str(state), work_root=str(self.root))
        with patch.object(bench.protocol, "ProtocolClient", FakeClient):
            result = asyncio.run(bench.bench_zcode_resident(
                config, 2, scratch_dir=self.root / "scratch"))
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["n"], 2)
        if "degraded" in result["notes"]:
            return  # constructor shape drifted; degraded path already asserted
        self.assertIn("resident-module", result["notes"])
        self.assertGreaterEqual(len(FakeClient.instances), 1)
        client = FakeClient.instances[0]
        self.assertEqual(client.started, 1)  # the resident started it; bench did not
        self.assertGreaterEqual(client.closed, 1)  # stop() and belt-and-braces close
        self.assertGreaterEqual(client.calls.count("session/create"), 3)


class CodexSpawnTests(unittest.TestCase):
    def setUp(self):
        self.temporary = TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.codex = fake_codex_file(self.root)

    def test_spawn_times_patched_helper(self):
        calls = []

        async def fake_spawn(path):
            calls.append(path)
            return 12.5

        with patch.object(bench, "_spawn_version", fake_spawn):
            result = asyncio.run(bench.bench_codex_spawn(self.codex, 3))
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["n"], 3)
        self.assertEqual(result["p50"], 12.5)
        self.assertEqual(calls, [self.codex] * 3)
        self.assertIn("--version", result["notes"])
        self.assertNotIn("errors", result)

    def test_spawn_missing_binary_raises(self):
        with self.assertRaises(ValueError):
            asyncio.run(bench.bench_codex_spawn(self.root / "missing", 2))


class AppServerTests(unittest.TestCase):
    def setUp(self):
        self.temporary = TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.codex = fake_codex_file(self.root)
        FakeJsonl.instances = []

    def test_init_section_spawns_closes_and_degrades_without_module(self):
        with forced_import_failure("zcode_supervisor.codex_appserver"), \
                patch.object(bench, "_AppServerJsonl", FakeJsonl):
            result = asyncio.run(bench.bench_codex_appserver(
                self.codex, 3, self.root / "out"))
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["n"], 3)
        self.assertIn("degraded", result["notes"])
        self.assertEqual(len(FakeJsonl.instances), 3)
        for client in FakeJsonl.instances:
            self.assertEqual(client.started, 1)
            self.assertEqual(client.closed, 1)
            self.assertEqual(client.requests[0][0], "initialize")

    def test_thread_section_ephemeral_accepted(self):
        with forced_import_failure("zcode_supervisor.codex_appserver"), \
                patch.object(bench, "_AppServerJsonl", FakeJsonl):
            result = asyncio.run(bench.bench_codex_appserver_thread(
                self.codex, 2, self.root / "out"))
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["n"], 2)
        self.assertIn("ephemeral=true", result["notes"])
        self.assertEqual(len(FakeJsonl.instances), 1)
        client = FakeJsonl.instances[0]
        self.assertEqual(client.started, 1)
        self.assertEqual(client.closed, 1)
        starts = [params for method, params in client.requests
                  if method == "thread/start"]
        # one probe + one warmup + two timed trials
        self.assertEqual(len(starts), 4)
        self.assertTrue(all(params.get("ephemeral") is True for params in starts))
        self.assertTrue(all(params.get("sandbox") == "read-only"
                            for params in starts))

    def test_thread_section_ephemeral_rejected_falls_back(self):
        with forced_import_failure("zcode_supervisor.codex_appserver"), \
                patch.object(bench, "_AppServerJsonl",
                             lambda path: FakeJsonl(path, reject_ephemeral=True)):
            result = asyncio.run(bench.bench_codex_appserver_thread(
                self.codex, 2, self.root / "out"))
        self.assertEqual(result["status"], "ok")
        self.assertIn("fallback: persisted threads", result["notes"])
        self.assertIn("rollout records", result["notes"])
        client = FakeJsonl.instances[0]
        starts = [params for method, params in client.requests
                  if method == "thread/start"]
        self.assertEqual(len(starts), 4)
        self.assertIn("ephemeral", starts[0])  # the probe carries ephemeral
        self.assertTrue(all("ephemeral" not in params for params in starts[1:]))

    def test_module_preferred_when_surface_matches(self):
        class ModuleClient(FakeJsonl):
            def __init__(self, codex_path, evidence_dir=None):
                super().__init__(codex_path)
                self.evidence_dir = evidence_dir

        with injected_module("zcode_supervisor.codex_appserver",
                             CodexAppServerClient=ModuleClient):
            result = asyncio.run(bench.bench_codex_appserver(
                self.codex, 2, self.root / "out"))
        self.assertIn("using codex_appserver.CodexAppServerClient", result["notes"])
        self.assertEqual(len(ModuleClient.instances), 2)
        self.assertTrue(all(client.evidence_dir is not None
                            and "evidence-" in str(client.evidence_dir)
                            for client in ModuleClient.instances))

    def test_thread_module_client_that_self_initializes(self):
        class SelfInitializing:
            instances = []

            def __init__(self, codex_path, evidence_dir=None):
                self.codex_path = codex_path
                self.requests = []
                type(self).instances.append(self)

            async def start(self):
                return {'userAgent': 'codex-bench fake'}  # handshake already done

            async def close(self):
                pass

            async def request(self, method, params, timeout=None):
                self.requests.append((method, params))
                return {}

        with injected_module("zcode_supervisor.codex_appserver",
                             CodexAppServerClient=SelfInitializing):
            result = asyncio.run(bench.bench_codex_appserver_thread(
                self.codex, 2, self.root / "out"))
        self.assertEqual(result["status"], "ok")
        self.assertEqual(len(SelfInitializing.instances), 1)
        client = SelfInitializing.instances[0]
        methods = [method for method, _ in client.requests]
        self.assertNotIn("initialize", methods)  # start() self-initialized
        self.assertEqual(methods[0], "thread/start")

    def test_module_with_wrong_surface_degrades(self):
        class HalfClient:
            async def start(self):
                pass

            async def close(self):
                pass

        with injected_module("zcode_supervisor.codex_appserver",
                             CodexAppServerClient=HalfClient), \
                patch.object(bench, "_AppServerJsonl", FakeJsonl):
            result = asyncio.run(bench.bench_codex_appserver(
                self.codex, 2, self.root / "out"))
        self.assertIn("lacks the start/request/close surface", result["notes"])
        self.assertEqual(len(FakeJsonl.instances), 2)

    def test_module_without_class_degrades(self):
        with injected_module("zcode_supervisor.codex_appserver"), \
                patch.object(bench, "_AppServerJsonl", FakeJsonl):
            result = asyncio.run(bench.bench_codex_appserver(
                self.codex, 1, self.root / "out"))
        self.assertIn("no CodexAppServerClient", result["notes"])
        self.assertEqual(len(FakeJsonl.instances), 1)


class WithModelTests(unittest.TestCase):
    def setUp(self):
        self.temporary = TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.config = SimpleNamespace(work_root=self.root)
        FakeClient.instances = []

    def test_zcode_with_model_end_to_end_fake_turn(self):
        with patch.object(bench.protocol, "ProtocolClient", FakeClient):
            result = asyncio.run(bench.bench_zcode_with_model(
                self.config, 2, scratch_dir=self.root / "scratch"))
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["n"], 2)
        self.assertIn("CONSUMES MODEL QUOTA", result["notes"])
        for client in FakeClient.instances:
            self.assertIn("session/send", client.calls)
            self.assertEqual(client.closed, 1)

    def test_codex_exec_with_model_uses_driver_receipt(self):
        self.codex = fake_codex_file(self.root)
        receipts = []

        def fake_run_readonly(path, workspace, task, schema, evidence_dir,
                              timeout_s=300, **kwargs):
            receipt = SimpleNamespace(outcome="delivered", duration_s=1.5)
            receipts.append((task, schema["required"], evidence_dir.name))
            return receipt

        with patch("zcode_supervisor.codex_driver.run_readonly",
                   fake_run_readonly):
            result = asyncio.run(bench.bench_codex_exec_with_model(
                self.codex, 2, self.root / "out"))
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["n"], 2)
        self.assertEqual(result["p50"], 1500.0)
        self.assertEqual(len(receipts), 2)
        self.assertTrue(all(item[1] == ["reply"] for item in receipts))
        self.assertIn("CONSUMES MODEL QUOTA", result["notes"])


def ok_section(**overrides):
    entry = {"status": "ok", "n": 2, "min": 1.0, "p50": 2.0, "p90": 3.0,
             "p99": 4.0, "max": 5.0, "mean": 2.5, "notes": "fake"}
    entry.update(overrides)
    return entry


class RunBenchTests(unittest.TestCase):
    def setUp(self):
        self.temporary = TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.codex = fake_codex_file(self.root)
        self.config = SimpleNamespace(work_root=self.root)

    def test_sections_written_to_json_and_markdown(self):
        async def fake_cold(config, trials, scratch_dir=None):
            return ok_section(n=trials)

        async def fake_spawn(path, trials):
            return ok_section(n=trials)

        with patch.object(bench, "bench_zcode_cold", fake_cold), \
                patch.object(bench, "bench_codex_spawn", fake_spawn):
            result = asyncio.run(bench.run_bench(
                self.config, self.root / "out", trials=4,
                sections=["zcode-cold", "codex-spawn"], codex_path=self.codex))
        self.assertEqual(list(result["sections"]), ["zcode-cold", "codex-spawn"])
        self.assertEqual(result["sections"]["zcode-cold"]["n"], 4)
        document = json.loads((self.root / "out" / "bench.json").read_text())
        self.assertEqual(document["sections"]["codex-spawn"]["status"], "ok")
        markdown = (self.root / "out" / "bench.md").read_text()
        self.assertIn("| section | n | min | p50 | p90 | p99 | max | notes |",
                      markdown)
        self.assertIn("| zcode-cold | 4 |", markdown)
        self.assertIn("| codex-spawn | 4 |", markdown)

    def test_single_section_failure_does_not_sink_the_run(self):
        async def broken(config, trials, scratch_dir=None):
            raise ValueError("boom")

        async def fine(path, trials):
            return ok_section()

        with patch.object(bench, "bench_zcode_cold", broken), \
                patch.object(bench, "bench_codex_spawn", fine):
            result = asyncio.run(bench.run_bench(
                self.config, self.root / "out", trials=2,
                sections=["zcode-cold", "codex-spawn"], codex_path=self.codex))
        self.assertEqual(result["sections"]["zcode-cold"]["status"], "error")
        self.assertIn("ValueError: boom", result["sections"]["zcode-cold"]["error"])
        self.assertEqual(result["sections"]["codex-spawn"]["status"], "ok")
        self.assertTrue((self.root / "out" / "bench.json").is_file())
        self.assertIn("ValueError: boom", (self.root / "out" / "bench.md").read_text())

    def test_unknown_requested_section_is_an_error_entry(self):
        async def fake_cold(config, trials, scratch_dir=None):
            return ok_section()

        with patch.object(bench, "bench_zcode_cold", fake_cold):
            result = asyncio.run(bench.run_bench(
                self.config, self.root / "out", trials=1,
                sections=["zcode-cold", "nope"], codex_path=self.codex))
        self.assertEqual(result["sections"]["zcode-cold"]["status"], "ok")
        self.assertEqual(result["sections"]["nope"]["status"], "error")
        self.assertIn("unknown section", result["sections"]["nope"]["error"])

    def test_missing_codex_skips_codex_sections(self):
        async def fake_cold(config, trials, scratch_dir=None):
            return ok_section()

        with patch.object(bench, "DEFAULT_CODEX", Path("/nonexistent-codex")), \
                patch("shutil.which", return_value=None), \
                patch.object(bench, "bench_zcode_cold", fake_cold):
            result = asyncio.run(bench.run_bench(
                self.config, self.root / "out", trials=1,
                sections=["zcode-cold", "codex-spawn"], codex_path=None))
        self.assertEqual(result["codex"], None)
        self.assertEqual(result["sections"]["zcode-cold"]["status"], "ok")
        self.assertEqual(result["sections"]["codex-spawn"]["status"], "skipped")
        self.assertIn("--codex", result["sections"]["codex-spawn"]["error"])


class MainTests(unittest.TestCase):
    def setUp(self):
        self.temporary = TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def result_document(self, entries):
        return {"generated": "now", "trials": 4, "with_model": False,
                "codex": "/fake/codex", "sections": entries,
                "output": {"json": str(self.root / "bench.json"),
                           "markdown": str(self.root / "bench.md")}}

    def test_main_prints_summary_and_exits_zero(self):
        async def fake_run_bench(*args, **kwargs):
            return self.result_document({
                "zcode-cold": ok_section(),
                "codex-spawn": {"status": "error", "error": "ValueError: x"}})

        output = io.StringIO()
        with patch.object(bench, "load_config", lambda path: SimpleNamespace()), \
                patch.object(bench, "run_bench", fake_run_bench), \
                redirect_stdout(output):
            code = bench.main(["--out", str(self.root / "out"), "--trials", "4"])
        self.assertEqual(code, 0)
        printed = json.loads(output.getvalue())
        self.assertTrue(printed["ok"])
        self.assertEqual(printed["sections"]["zcode-cold"]["p50"], 2.0)
        self.assertEqual(printed["sections"]["codex-spawn"]["status"], "error")

    def test_main_exits_one_when_every_section_failed(self):
        async def fake_run_bench(*args, **kwargs):
            return self.result_document({
                "zcode-cold": {"status": "error", "error": "a"},
                "codex-spawn": {"status": "error", "error": "b"}})

        with patch.object(bench, "load_config", lambda path: SimpleNamespace()), \
                patch.object(bench, "run_bench", fake_run_bench), \
                redirect_stdout(io.StringIO()):
            self.assertEqual(
                bench.main(["--out", str(self.root / "out")]), 1)

    def test_main_config_error_exits_one(self):
        def broken(path):
            raise ValueError("bad config")

        output = io.StringIO()
        with patch.object(bench, "load_config", broken), \
                redirect_stdout(output):
            code = bench.main(["--out", str(self.root / "out")])
        self.assertEqual(code, 1)
        self.assertIn("bad config", output.getvalue())


class UiFixtureContractTests(unittest.TestCase):
    """P0.0: the fixed UI fixture (PLAN §5.3.2) and comm-fault fixtures."""

    def setUp(self):
        self.fixture_dir = Path(__file__).resolve().parent / "fixtures" / "ui"
        self.fake_server = self.fixture_dir / "fake_server.py"

    def test_ui_fixture_spec_pinned_and_regeneration_deterministic(self):
        spec = json.loads((self.fixture_dir / "ui-fixture-spec.json").read_text())
        first = bench.build_ui_fixture(spec)
        second = bench.build_ui_fixture(spec)
        self.assertEqual(bench.canonical_fixture_bytes(first),
                         bench.canonical_fixture_bytes(second))
        digest = hashlib.sha256(bench.canonical_fixture_bytes(first)).hexdigest()
        self.assertEqual(digest, spec["fixture_sha256"])
        self.assertEqual(len(first["nodes"]), 100)
        self.assertEqual(len(first["edges"]), 300)
        self.assertEqual(len(first["events"]), 10000)
        self.assertEqual(first["replay"]["events_per_s"], 100)
        pairs = {tuple(sorted(edge)) for edge in first["edges"]}
        self.assertEqual(len(pairs), 300)  # unique, no parallel duplicates
        self.assertTrue(all(a != b for a, b in first["edges"]))

    def test_fixture_carries_no_credential_shapes(self):
        spec = json.loads((self.fixture_dir / "ui-fixture-spec.json").read_text())
        blob = bench.canonical_fixture_bytes(bench.build_ui_fixture(spec)).decode()
        lowered = blob.lower()
        for banned in ("token", "secret", "password", "authorization", "api_key",
                       "apikey", "bearer", "credential"):
            self.assertNotIn(banned, lowered)

    def test_comm_fault_manifest_pinned_and_modes_supported_by_fake(self):
        manifest = json.loads((self.fixture_dir / "comm-faults-v1.json").read_text())
        self.assertEqual(manifest["version"], "comm-faults-v1")
        ids = {scenario["id"] for scenario in manifest["scenarios"]}
        self.assertLessEqual({"blocked-drain", "delayed-start", "delayed-turn-start",
                              "cancel-mid-poll", "cancel-after-send"}, ids)
        modes = {scenario["fake_server_fault"]["mode"]
                 for scenario in manifest["scenarios"]}
        listing = subprocess.run(
            [sys.executable, str(self.fake_server), "--list-faults"],
            capture_output=True, text=True, timeout=60)
        self.assertEqual(listing.returncode, 0, listing.stderr)
        supported = set(json.loads(listing.stdout)["fault_modes"])
        self.assertLessEqual(modes, supported)

    def test_fake_server_source_is_hermetic_and_compilable(self):
        source = self.fake_server.read_text()
        self.assertTrue(source.startswith("#!"))
        for banned in ("import socket", "import urllib", "import requests",
                       "import subprocess"):
            self.assertNotIn(banned, source)
        compile(source, str(self.fake_server), "exec")


class FakeColdLifecycleTests(unittest.TestCase):
    """P0.0: cold spawn / session create / subscribe / close against the fake."""

    def setUp(self):
        self.temporary = TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.server = Path(__file__).resolve().parent / "fixtures" / "ui" / "fake_server.py"

    def test_cold_split_phases_and_zero_model_requests(self):
        result = asyncio.run(bench.bench_fake_cold(
            self.server, 3, scratch_dir=self.root / "scratch"))
        self.assertEqual(result["status"], "ok")
        for phase in ("spawn", "session_create", "session_subscribe", "close"):
            self.assertEqual(result["phases"][phase]["n"], 3, phase)
            self.assertIn("median", result["phases"][phase])
            self.assertIn("p95", result["phases"][phase])
            self.assertIn("samples", result["phases"][phase])
            self.assertEqual(len(result["phases"][phase]["samples"]), 3)
        self.assertEqual(result["model_requests"], 0)
        self.assertEqual(result["server_counters"]["model_requests"], 0)
        self.assertEqual(result["server_counters"]["session_sends"], 0)
        # Aggregated across every spawned child (3 trials, 1 create each).
        self.assertEqual(result["server_counters"]["session_creates"], 3)
        self.assertEqual(result["server_counters"]["session_subscribes"], 3)
        self.assertIn("fake", result["notes"])


class FakeAppServerTests(unittest.TestCase):
    """P0.0: codex app-server initialize/thread rounds + retention, all fake."""

    def setUp(self):
        self.temporary = TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.server = Path(__file__).resolve().parent / "fixtures" / "ui" / "fake_server.py"

    def test_spawn_initialize_thread_rounds_and_retention(self):
        result = asyncio.run(bench.bench_fake_appserver(
            self.server, 2, rounds=3, scratch_dir=self.root / "scratch"))
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["phases"]["spawn_initialize"]["n"], 2)
        self.assertEqual(result["phases"]["thread_start"]["n"], 3)
        self.assertEqual(result["model_requests"], 0)
        self.assertEqual(result["server_counters"]["model_requests"], 0)
        self.assertEqual(result["server_counters"]["thread_starts"], 3)
        retention = result["retention_after_rounds"]
        # Each round's production-shaped turn emits four notifications and
        # one reverse approval request. Notifications are still retained
        # (the shared evidence store); completed reverse tasks are
        # DISCARDED from the active registry since P2.2 — retention of
        # finished reverse work is zero, not unbounded.
        self.assertEqual(retention["notifications"], 3 * 4)
        self.assertEqual(retention["reverse_tasks"], 0)
        self.assertEqual(retention["pending"], 0)


class FakeResidentLifecycleTests(unittest.TestCase):
    """P0.0: resident first/steady/cancel rounds with sync-segment marks."""

    def setUp(self):
        self.temporary = TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.server = Path(__file__).resolve().parent / "fixtures" / "ui" / "fake_server.py"

    def test_rounds_marks_inventory_comm_and_retention(self):
        result = asyncio.run(bench.bench_fake_resident(
            self.server, 2, scratch_dir=self.root / "scratch"))
        self.assertEqual(result["status"], "ok")
        kinds = [round_["kind"] for round_ in result["rounds"]]
        self.assertEqual(kinds[0], "first-create")
        self.assertEqual(kinds[1], "steady-resume")
        self.assertEqual(kinds[-1], "cancel-never-complete")
        first = result["rounds"][0]
        # 5 inventories per clean round, all measured: before, inside each of
        # the three check_frozen calls, and the finally "after" scan.
        self.assertEqual(first["inventory_calls"], 5)
        self.assertEqual(first["hashed_bytes"], 5 * 8 * 1024)
        self.assertLessEqual(first["comm_rpc_ms"], first["total_wall_ms"])
        self.assertTrue(first["session_read_ms"])
        marks = first["sync_segments_ms"]
        for key in ("first_inventory_ms", "pre_check_frozen_ms",
                    "post_success_check_frozen_ms", "finally_check_frozen_ms",
                    "finally_after_inventory_ms", "finally_write_ms"):
            self.assertGreaterEqual(marks[key], 0.0, key)
        # Production drain actually processed this round's own started+completed.
        self.assertEqual(first["drain_events_evaluated"], 2)
        self.assertEqual(first["round_events_emitted"], 2)
        # The post-turn evidence scan re-reads the whole shared history.
        self.assertEqual(first["evidence_scan_events"], 2)
        steady = result["rounds"][1]
        self.assertEqual(steady["round_events_emitted"], 2)
        self.assertEqual(steady["events_retained_after"], 4)
        self.assertEqual(steady["evidence_scan_events"], 4)
        self.assertEqual(steady["drain_events_evaluated"], 2)
        cancel = result["rounds"][-1]
        for key in ("cancel_to_finish_ms", "reset_rpc_ms",
                    "post_cancel_finally_ms", "post_cancel_check_frozen_ms",
                    "post_cancel_after_inventory_ms", "post_cancel_write_ms"):
            self.assertGreaterEqual(cancel[key], 0.0, key)
        # Mid-cancel over a multi-event stream: the drain processed a strict
        # subset of the emitted events; retention keeps every one of them.
        self.assertEqual(cancel["round_events_emitted"], 11)
        self.assertTrue(0 < cancel["drain_events_evaluated"]
                        < cancel["round_events_emitted"], cancel)
        self.assertEqual(cancel["events_retained_after"]
                         - cancel["round_events_before"], 11)
        self.assertIsNone(cancel["evidence_scan_events"])
        self.assertEqual(result["model_requests"], 0)
        counters = result["server_counters"]
        self.assertEqual(counters["model_requests"], 0)
        self.assertEqual(counters["session_sends"], 4)  # first + 2 steady + cancel
        self.assertEqual(counters["turns_completed"], 3)

    def test_event_loop_lag_rss_and_wall_gate_marker(self):
        result = asyncio.run(bench.bench_fake_resident(
            self.server, 1, scratch_dir=self.root / "scratch"))
        self.assertEqual(result["status"], "ok")
        self.assertGreater(result["phases"]["event_loop_lag_ms"]["n"], 0)
        self.assertGreater(result["peak_rss"]["self_maxrss"], 0)
        self.assertGreater(result["peak_rss"]["children_maxrss"], 0)
        # P0.0 records baselines only; the P0.1 whole-call wall gate is NOT
        # satisfied by this run and must not be reported as such.
        self.assertEqual(result["whole_call_wall_time_gate"], "baseline-only")


class ClockSidecarTests(unittest.TestCase):
    """P0.0: same-source CLOCK_MONOTONIC sidecar; observed_at display-only."""

    def setUp(self):
        self.temporary = TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.server = Path(__file__).resolve().parent / "fixtures" / "ui" / "fake_server.py"

    def test_cross_process_same_source_and_monotonic_only(self):
        result = asyncio.run(bench.bench_fake_clock_sidecar(
            self.server, 3, scratch_dir=self.root / "scratch"))
        self.assertEqual(result["status"], "ok")
        self.assertTrue(result["same_source"])  # t0 <= child mark <= t2, all probes
        self.assertEqual(result["phases"]["round_trip_ms"]["n"], 3)
        self.assertEqual(len(result["phases"]["round_trip_ms"]["samples"]), 3)
        # Presentation stays NOT_RUN this round; no synthetic self-proof.
        self.assertTrue(result["presentation_probe"].startswith("NOT_RUN"),
                        result["presentation_probe"])
        self.assertEqual(result["source_to_present"], "baseline-pending")
        self.assertNotIn("observed_at_used_in_timing", result)
        self.assertEqual(result["model_requests"], 0)

    def test_clock_raw_probes_recompute_without_observed_at(self):
        result = asyncio.run(bench.bench_fake_clock_sidecar(
            self.server, 3, scratch_dir=self.root / "scratch"))
        probes = result["probes_raw"]
        self.assertEqual(len(probes), 3)
        recomputed = [(probe["t2_ns"] - probe["t0_ns"]) / 1e6 for probe in probes]
        self.assertEqual([round(value, 3) for value in recomputed],
                         [round(value, 3) for value in
                          result["phases"]["round_trip_ms"]["samples"]])
        for probe in probes:
            self.assertLessEqual(probe["t0_ns"], probe["child_ns"])
            self.assertLessEqual(probe["child_ns"], probe["t2_ns"])
            self.assertIn("observed_wall_ns", probe)  # display-only, unused above

    def test_blocked_drain_genuinely_gates_client_drain(self):
        result = asyncio.run(bench.bench_fake_clock_sidecar(
            self.server, 1, scratch_dir=self.root / "scratch"))
        blocked = result["blocked_drain"]
        self.assertTrue(blocked["blocked"],
                        "the big frame's drain must block on pipe backpressure")
        self.assertTrue(blocked["recovered"])
        self.assertGreaterEqual(blocked["gate_ms"], 0.0)
        self.assertGreater(blocked["recovery_ms"], blocked["gate_ms"])

    def test_observed_at_is_display_only(self):
        base = {"source": 1000, "journal_enqueued": 2000, "ui_received": 3000,
                "reducer_committed": 4000, "frame_submitted": 5000}
        variant_a = bench.ui_event_latency({**base, "observed_at": 111})
        variant_b = bench.ui_event_latency({**base, "observed_at": 10 ** 12})
        variant_c = bench.ui_event_latency(dict(base))
        self.assertEqual(variant_a, variant_b)
        self.assertEqual(variant_a, variant_c)
        self.assertEqual(variant_a["source_to_present_ns"], 4000)
        self.assertFalse(variant_a["observed_at_used"])
        missing = bench.ui_event_latency({"source": 1000, "ui_received": 3000})
        self.assertEqual(missing["end_to_end_latency"], "unavailable")
        self.assertEqual(missing["segments_ns"], {"source__to__ui_received": 2000})


class FakeBaselineDocTests(unittest.TestCase):
    """P0.0: the raw-sample JSON document shape (samples/env/command/exit)."""

    def setUp(self):
        self.temporary = TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.server = Path(__file__).resolve().parent / "fixtures" / "ui" / "fake_server.py"

    def test_document_shape_zero_model_and_exit(self):
        out = self.root / "out"
        result = asyncio.run(bench.run_fake_baseline(
            self.server, out, steady_rounds=1, init_trials=2))
        document = json.loads((out / "fake-baseline.json").read_text())
        self.assertEqual(result["model_requests"], 0)
        self.assertEqual(document["model_requests"], 0)
        self.assertEqual(document["exit_code"], 0)
        self.assertTrue(document["command"][-1].endswith("fake_server.py"))
        for key in ("python", "platform", "machine"):
            self.assertIn(key, document["environment"])
        for name in ("fake-cold-lifecycle", "fake-appserver",
                     "fake-resident-lifecycle", "fake-clock-sidecar",
                     "fake-read-list"):
            self.assertEqual(document["sections"][name]["status"], "ok", name)
        self.assertTrue(document["output"]["json"].endswith("fake-baseline.json"))


class BenchUiToolTests(unittest.TestCase):
    """P0.0: the tools/bench_ui.py entry point (fixture check + smoke CLI)."""

    def setUp(self):
        self.repo = Path(__file__).resolve().parents[1]
        self.tool_path = self.repo / "tools" / "bench_ui.py"

    def _load_tool(self):
        import importlib.util
        spec = importlib.util.spec_from_file_location("czs_bench_ui", self.tool_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module

    def test_cli_help_exits_zero(self):
        completed = subprocess.run(
            [sys.executable, str(self.tool_path), "--help"],
            capture_output=True, text=True, timeout=60)
        self.assertEqual(completed.returncode, 0, completed.stderr)

    def test_verify_fixtures_reports_pinned_digest(self):
        module = self._load_tool()
        report = module.verify_fixtures(self.repo)
        self.assertTrue(report["ok"], report)
        self.assertTrue(report["fixture_sha256_match"])
        self.assertEqual(report["counts"], {"nodes": 100, "edges": 300,
                                            "events": 10000})
        self.assertTrue(report["fault_modes_supported"])


class RawSamplesTests(unittest.TestCase):
    """Review-fix: unrounded raw samples survive into JSON; stats recompute."""

    def setUp(self):
        self.temporary = TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.server = Path(__file__).resolve().parent / "fixtures" / "ui" \
            / "fake_server.py"

    def test_phase_stats_embed_unrounded_samples(self):
        stats = bench.phase_stats([1.23456789, 2.0, 9.87654321])
        self.assertEqual(stats["samples"], [1.23456789, 2.0, 9.87654321])
        self.assertEqual(stats["n"], 3)
        self.assertEqual(round(bench.percentile(stats["samples"], 50), 3),
                         stats["median"])
        self.assertEqual(round(bench.percentile(stats["samples"], 95), 3),
                         stats["p95"])

    def test_baseline_document_recomputes_median_and_p95_from_samples(self):
        out = self.root / "out"
        asyncio.run(bench.run_fake_baseline(
            self.server, out, steady_rounds=1, init_trials=2))
        document = json.loads((out / "fake-baseline.json").read_text())
        checked = 0
        for section in document["sections"].values():
            for phase in section.get("phases", {}).values():
                samples = phase.get("samples")
                if not samples:
                    self.assertEqual(phase.get("status", ""), "baseline-pending",
                                     phase)
                    continue
                self.assertEqual(len(samples), phase["n"], phase)
                self.assertEqual(
                    round(bench.percentile(samples, 50), 3), phase["median"], phase)
                self.assertEqual(
                    round(bench.percentile(samples, 95), 3), phase["p95"], phase)
                checked += 1
        self.assertGreater(checked, 8)  # every measured phase kept its samples


class RepeatOutDirTests(unittest.TestCase):
    """Review-fix: the same --out must rerun cleanly without clobbering."""

    def setUp(self):
        self.temporary = TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.server = Path(__file__).resolve().parent / "fixtures" / "ui" \
            / "fake_server.py"

    def test_two_runs_same_out_dir_both_exit_zero_baseline_preserved(self):
        out = self.root / "out"
        first = asyncio.run(bench.run_fake_baseline(
            self.server, out, steady_rounds=1, init_trials=1))
        self.assertEqual(first["exit_code"], 0)
        first_bytes = (out / "fake-baseline.json").read_bytes()
        second = asyncio.run(bench.run_fake_baseline(
            self.server, out, steady_rounds=1, init_trials=1))
        self.assertEqual(second["exit_code"], 0)
        # The good baseline is never overwritten; the new run lands elsewhere.
        self.assertEqual((out / "fake-baseline.json").read_bytes(), first_bytes)
        self.assertNotEqual(second["output"]["json"],
                            str(out / "fake-baseline.json"))
        self.assertTrue(Path(second["output"]["json"]).is_file())
        scratch_dirs = {entry.name for entry in out.iterdir()
                        if entry.is_dir() and entry.name.startswith("scratch-")}
        self.assertEqual(len(scratch_dirs), 2)  # one unique scratch per run


class FakeCodexContractTests(unittest.TestCase):
    """Review-fix: the fake speaks production Codex shapes; driver contract."""

    def setUp(self):
        self.temporary = TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.server = Path(__file__).resolve().parent / "fixtures" / "ui" \
            / "fake_server.py"

    def test_thread_start_returns_production_thread_shape(self):
        frame = '{"id": 7, "method": "thread/start", "params": {}}\n'
        pipe = subprocess.run(
            [sys.executable, str(self.server), "--role", "codex"],
            input=frame, capture_output=True, text=True, timeout=60)
        reply = json.loads(pipe.stdout.splitlines()[0])
        self.assertIn("thread", reply["result"])
        self.assertIn("id", reply["result"]["thread"])

    def test_production_driver_contract_against_fake(self):
        from zcode_supervisor.codex_appserver import (CodexAppServerClient,
                                                      CodexAppServerDriver,
                                                      SimpleContractAuthority)

        async def scenario():
            evidence = self.root / "evidence"
            evidence.mkdir()
            client = CodexAppServerClient(self.server, evidence)
            await client.start()
            try:
                authority = SimpleContractAuthority(approved_commands={"printf fake"})
                driver = CodexAppServerDriver(client)
                receipt = await driver.run_task(
                    authority, self.root, "fixture prompt", timeout_s=30)
                counters = await client.request("bench/counters", {}, 30)
                return receipt, counters
            finally:
                await client.close()

        receipt, counters = asyncio.run(scenario())
        self.assertEqual(receipt["outcome"], "delivered", receipt.get("error"))
        self.assertTrue(receipt["thread_id"])
        self.assertTrue(receipt["turn_id"])
        self.assertEqual(receipt["approvals"][0]["decision"], "allow")
        self.assertEqual(receipt["approvals"][0]["method"],
                         "item/commandExecution/requestApproval")
        self.assertEqual(len(receipt["command_items"]), 1)
        self.assertEqual(receipt["final_message"], "fake reply")
        self.assertEqual(receipt["notification_counts"]["turn/completed"], 1)
        self.assertEqual(counters["model_requests"], 0)

    def test_appserver_start_is_bounded_against_silent_child(self):
        wrapper = self.root / "silent-codex"
        wrapper.write_text(
            "#!/usr/bin/env python3\n"
            "import os, sys\n"
            "os.execv(sys.executable, [sys.executable, %r, '--fault', "
            "'silent-initialize'] + sys.argv[1:])\n" % str(self.server))
        wrapper.chmod(0o755)
        started = time.monotonic()
        result = asyncio.run(bench.bench_fake_appserver(
            wrapper, 1, rounds=1, scratch_dir=self.root / "scratch",
            start_timeout=0.5))
        self.assertLess(time.monotonic() - started, 10.0)  # bounded, not hung
        self.assertEqual(result["status"], "error")
        self.assertIn("initialize", result["error"])


class CancelResetSeparationTests(unittest.TestCase):
    """Review-fix: cancel timing excludes the post-join fault-reset RPC."""

    def test_delayed_reset_does_not_pollute_cancel_to_finish(self):
        temporary = TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        server = Path(__file__).resolve().parent / "fixtures" / "ui" \
            / "fake_server.py"
        result = asyncio.run(bench.bench_fake_resident(
            server, 1, scratch_dir=Path(temporary.name) / "scratch",
            cancel_reset_reply_delay_s=0.3))
        self.assertEqual(result["status"], "ok")
        cancel = result["rounds"][-1]
        self.assertTrue(cancel["cancelled"])
        self.assertLess(cancel["cancel_to_finish_ms"], 250.0, cancel)
        self.assertGreaterEqual(cancel["reset_rpc_ms"], 250.0, cancel)


class ProductionReadListTests(unittest.TestCase):
    """Review-fix: read/list latency measured on the real production paths."""

    def setUp(self):
        self.temporary = TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)

    def test_production_list_and_read_measured_over_synthetic_records(self):
        result = asyncio.run(bench.bench_fake_read_list(
            self.root / "scratch", tasks=2, runs_per_task=30,
            messages=40, trials=3))
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["phases"]["list_runs_ms"]["n"], 3)
        self.assertEqual(result["phases"]["read_context_ms"]["n"], 3)
        self.assertEqual(result["records_listed"], 60)
        self.assertEqual(result["messages_read"], 40)
        self.assertIn("list_runs_impl", " ".join(result["production_paths"]))
        self.assertIn("read_zcode_context", " ".join(result["production_paths"]))
        self.assertEqual(result["model_requests"], 0)


if __name__ == "__main__":
    unittest.main()
