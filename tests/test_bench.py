"""Bench tests: every timing path is faked; no real CLI, node or codex runs.

ProtocolClient is replaced by a fake class, codex spawns by patched helpers,
and the parallel-agent modules (zcode_resident / codex_appserver) are either
forced to fail importing or replaced by injected fakes, so this suite stays
hermetic even after those modules land in the package.
"""
import asyncio
from contextlib import contextmanager
import io
from contextlib import redirect_stdout
import json
from pathlib import Path
import sys
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


if __name__ == "__main__":
    unittest.main()
