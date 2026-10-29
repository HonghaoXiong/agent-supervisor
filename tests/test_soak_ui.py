"""P3.2 tests: tools/soak_ui.py window recording + threshold reading logic.

The soak harness itself is exercised only through its pure helpers and one
short (~8 s) end-to-end workload run against the real in-process
UiEventServer; the 5-minute baseline and >=30-minute soak are CLI evidence
runs, not unit tests. No model, no real CLI (model_requests stays 0).
"""

import asyncio
import importlib.util
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
TOOL_PATH = REPO / "tools" / "soak_ui.py"


def load_tool():
    spec = importlib.util.spec_from_file_location("czs_soak_ui", TOOL_PATH)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def make_window(**overrides):
    """One synthetic window record with the keys compute/evaluate read."""
    base = {
        "rss_main_kb": 30_000, "ru_maxrss_raw": 40_000_000,
        "children_rss_sum_kb": 1_000, "heap_current_bytes": 2_000_000,
        "heap_window_peak_bytes": 2_000_000, "lag_p50_ms": 1.0,
        "lag_p95_ms": 3.0, "lag_max_ms": 9.0, "queue_peak_events": 12,
        "queue_peak_bytes": 9_000, "connections": 6,
        "journal_total_events": 5_000, "journal_total_bytes": 900_000,
        "journal_streams": 200, "journal_cursors": 10,
        "journal_subscriptions": 4, "journal_dropped_events": 0,
        "events_emitted": 1_800, "events_per_second": 30.0,
        "emit_failures": 0, "frames_sent": 900, "reconnects": 4,
        "resets_received": 0, "heartbeats_received": 3,
        "gap_frames_total": 0, "expected_protocol_errors_total": 0,
        "op_timeouts_total": 0, "resume_violations_total": 0,
        "client_loss_violations_total": 0, "cross_stream_frames_total": 0,
        "selfcheck": {"ok": True, "mismatched_streams": []},
        "receipts": {"completed_total": 3, "retained_deep_bytes": 3_000},
        "events_emitted_total": 1_800, "reconnects_total": 4,
        "resets_total": 0, "receipts_completed": 3,
        "index": 0, "window_s": 60.0, "wall_utc": "2026-10-29T00:00:00.000000Z",
        "process_tree": [], "heap_global_peak_bytes": 2_000_000,
        "lag_samples": 600,
    }
    base.update(overrides)
    return base


class PureHelperTests(unittest.TestCase):
    def setUp(self):
        self.tool = load_tool()

    def test_percentile_nearest_rank_and_empty(self):
        values = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
        self.assertEqual(self.tool.percentile(values, 50), 5)
        self.assertEqual(self.tool.percentile(values, 95), 10)
        self.assertEqual(self.tool.percentile(values, 100), 10)
        self.assertIsNone(self.tool.percentile([], 95))
        self.assertEqual(self.tool.median([4, 1, 3]), 3)

    def test_deep_size_counts_shared_reference_once(self):
        shared = {"k": "v" * 100}
        structure = {"a": shared, "b": shared}
        once = self.tool.deep_size({"a": shared})
        twice_naive = 2 * once
        measured = self.tool.deep_size(structure)
        self.assertLess(measured, twice_naive)
        self.assertGreater(measured, once)

    def test_rate_per_hour(self):
        rate = self.tool._rate_per_hour(1000, 2000, 1800.0)
        self.assertEqual(rate, 2000.0)  # +1000 over half an hour
        self.assertEqual(self.tool._rate_per_hour(5, 5, 0.0), 0.0)


class ThresholdTests(unittest.TestCase):
    def setUp(self):
        self.tool = load_tool()
        self.baseline = {
            "duration_s": 360.0,
            "windows": [make_window() for _ in range(6)],
        }

    def test_thresholds_from_baseline_with_floors_and_clamps(self):
        thresholds = self.tool.compute_thresholds(self.baseline)
        # RSS: max 30000 KiB = 30.72 MiB -> max(1.5x, +64 MiB) floor wins
        rss_max = thresholds["rss_max_bytes"]
        self.assertEqual(rss_max["cap"], 30_000 * 1024 + 64 * self.tool.MI)
        self.assertIn("design_bound", thresholds["queue_peak_events_max"])
        self.assertEqual(
            thresholds["queue_peak_events_max"]["design_bound"],
            self.tool.ui_journal.CLIENT_QUEUE_MAX_EVENTS + 1)
        # queue peak 12 -> 2x+4 = 28 <= design bound, unclamped
        self.assertEqual(thresholds["queue_peak_events_max"]["cap"], 28)
        # growth floors are enforced even with a flat baseline
        self.assertEqual(thresholds["rss_growth_per_hour_bytes"]["cap"],
                         32 * self.tool.MI)
        self.assertEqual(thresholds["heap_growth_per_hour_bytes"]["cap"],
                         16 * self.tool.MI)
        # lag floors: 3 x 3ms = 9 < 100ms floor
        self.assertEqual(thresholds["loop_lag_p95_ms_max"]["cap"], 100.0)
        # zero-tolerance evidence gates stay zero
        self.assertEqual(thresholds["evidence_mismatch_total_max"]["cap"], 0)
        self.assertEqual(thresholds["resume_violations_total_max"]["cap"], 0)
        # every threshold documents its basis
        for name, item in thresholds.items():
            self.assertIn("basis", item, name)
            self.assertIn("cap", item, name)

    def test_thresholds_clamp_to_design_bound(self):
        busy = dict(self.baseline)
        busy["windows"] = [
            make_window(queue_peak_events=250, queue_peak_bytes=1_000_000,
                        journal_total_events=9_999)
            for _ in range(6)]
        thresholds = self.tool.compute_thresholds(busy)
        self.assertEqual(thresholds["queue_peak_events_max"]["cap"],
                         self.tool.ui_journal.CLIENT_QUEUE_MAX_EVENTS + 1)
        self.assertEqual(thresholds["retained_events_max"]["cap"],
                         self.tool.ui_journal.JOURNAL_MAX_EVENTS + 1)

    def test_thresholds_require_windows(self):
        with self.assertRaises(ValueError):
            self.tool.compute_thresholds({"duration_s": 1, "windows": []})


class FrozenDocumentTests(unittest.TestCase):
    def setUp(self):
        self.tool = load_tool()

    def _frozen(self, **overrides):
        baseline = {
            "duration_s": 360.0,
            "windows": [make_window() for _ in range(6)],
        }
        frozen = {
            "schema": 1,
            "frozen_at": "2026-10-29T01:00:00.000000Z",
            "baseline": {"started_at": "2026-10-29T00:00:00.000000Z",
                         "ended_at": "2026-10-29T00:06:00.000000Z",
                         "duration_s": 360.0, "windows": 6,
                         "samples_file": "x.json", "samples_sha256": "0" * 64},
            "workload_shape": self.tool.WORKLOAD_SHAPE,
            "thresholds": self.tool.compute_thresholds(baseline),
        }
        frozen.update(overrides)
        return frozen

    def test_validate_frozen_accepts_and_rejects(self):
        self.assertEqual(self.tool.validate_frozen(self._frozen()), [])
        problems = self.tool.validate_frozen(self._frozen(schema=2))
        self.assertTrue(problems)
        problems = self.tool.validate_frozen(
            self._frozen(workload_shape={"sessions": 99}))
        self.assertTrue(any("workload_shape" in p for p in problems))
        bad_thresholds = self._frozen()["thresholds"]
        del bad_thresholds["rss_max_bytes"]["cap"]
        self.assertTrue(self.tool.validate_frozen(
            self._frozen(thresholds=bad_thresholds)))

    def test_load_frozen_roundtrip(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "frozen.json"
            path.write_text(json.dumps(self._frozen()), encoding="utf-8")
            loaded = self.tool.load_frozen(path)
            self.assertEqual(loaded["schema"], 1)
            path.write_text(json.dumps(self._frozen(schema=7)),
                            encoding="utf-8")
            with self.assertRaises(ValueError):
                self.tool.load_frozen(path)

    def test_freeze_precedes_soak_audit_field(self):
        frozen = self._frozen()
        self.assertTrue(self.tool.freeze_precedes_soak(
            frozen, "2026-10-29T02:00:00.000000Z"))
        self.assertFalse(self.tool.freeze_precedes_soak(
            frozen, "2026-10-29T00:30:00.000000Z"))  # soak before freeze
        self.assertFalse(self.tool.freeze_precedes_soak(
            frozen, frozen["frozen_at"]))  # equal is NOT before
        self.assertFalse(self.tool.freeze_precedes_soak(
            {"frozen_at": ""}, "2026-10-29T02:00:00.000000Z"))

    def test_evaluate_gates_pass_and_fail(self):
        frozen = self._frozen()
        samples = {
            "duration_s": 1800.0,
            "windows": [make_window() for _ in range(30)],
            "totals": {"reconnects": 900, "resets_received": 0,
                       "evidence_mismatch": 0, "resume_violations": 0,
                       "client_loss_violations": 0},
        }
        result = self.tool.evaluate_gates(samples, frozen)
        self.assertTrue(result["all_pass"])
        self.assertEqual(len(result["gates"]), 18)
        # one breached gate flips the verdict and is reported honestly
        breach = dict(samples)
        breach["windows"] = [make_window(queue_peak_events=290)
                             for _ in range(30)]
        gates = {g["gate"]: g for g in
                 self.tool.evaluate_gates(breach, frozen)["gates"]}
        self.assertFalse(gates["queue_peak_events_max"]["pass"])
        self.assertEqual(gates["queue_peak_events_max"]["observed"], 290)
        # totals-only samples (no windows) still evaluate the total gates
        partial = {"duration_s": 60.0, "windows": [],
                   "totals": {"reconnects": 10, "resets_received": 0,
                              "evidence_mismatch": 1, "resume_violations": 0,
                              "client_loss_violations": 0}}
        gates = {g["gate"]: g for g in
                 self.tool.evaluate_gates(partial, frozen)["gates"]}
        self.assertFalse(gates["evidence_mismatch_total_max"]["pass"])

    def test_performance_report_observability_only(self):
        samples = {"windows": [make_window(lag_p95_ms=2.0 + i)
                               for i in range(10)]}
        report = self.tool.performance_report(samples)
        self.assertEqual(report["window_count"], 10)
        self.assertEqual(report["lag_p95_ms"]["samples"], 10)
        self.assertEqual(report["lag_p95_ms"]["median"], 6.0)  # nearest-rank
        self.assertEqual(report["lag_p95_ms"]["p95"], 11.0)
        self.assertIn("raw", report["rss_main_kb"])
        self.assertIn("observation-only", report["note"])


class AccountingHelperTests(unittest.TestCase):
    def setUp(self):
        self.tool = load_tool()
        self.accounting = self.tool.new_accounting()
        self.client_map = {}

    def _frame(self, stream, seq):
        return {"type": "event",
                "event": {"stream_id": stream, "seq": seq}}

    def test_first_sight_has_no_baseline(self):
        self.tool._track_event_frame(self._frame("s", 5), self.client_map,
                                     self.accounting)
        self.assertEqual(self.client_map["s"], 6)
        self.assertEqual(self.accounting["client_loss_violations"], 0)

    def test_consecutive_passes_skip_and_dup_fail(self):
        self.tool._track_event_frame(self._frame("s", 1), self.client_map,
                                     self.accounting)
        self.tool._track_event_frame(self._frame("s", 2), self.client_map,
                                     self.accounting)
        self.assertEqual(self.accounting["client_loss_violations"], 0)
        self.tool._track_event_frame(self._frame("s", 4), self.client_map,
                                     self.accounting)  # silent skip
        self.tool._track_event_frame(self._frame("s", 4), self.client_map,
                                     self.accounting)  # duplicate
        self.assertEqual(self.accounting["client_loss_violations"], 2)

    def test_gap_frame_advances_expectation(self):
        self.tool._track_event_frame(self._frame("s", 1), self.client_map,
                                     self.accounting)
        self.tool._apply_gap_frame(
            {"type": "gap", "gap": {"stream_id": "s", "from_seq": 2,
                                    "to_seq": 7, "count": 6}},
            self.client_map, self.accounting)
        self.assertEqual(self.accounting["gap_frames"], 1)
        self.tool._track_event_frame(self._frame("s", 8), self.client_map,
                                     self.accounting)
        self.assertEqual(self.accounting["client_loss_violations"], 0)

    def test_malformed_event_frame_counts_violation(self):
        self.tool._track_event_frame({"type": "event", "event": {}},
                                     self.client_map, self.accounting)
        self.assertEqual(self.accounting["client_loss_violations"], 1)

    def test_workload_shape_sanity(self):
        shape = self.tool.WORKLOAD_SHAPE
        consumers = shape["consumers"]
        self.assertEqual(sum(consumers.values()), 6)
        self.assertGreater(shape["events_per_second_total"], 0)
        self.assertEqual(shape["reader_stall_s"] * shape[
            "events_per_second_total"], 90)  # stall backlog ~90 < 256 bound


class JournalSelfcheckTests(unittest.TestCase):
    def setUp(self):
        self.tool = load_tool()
        from zcode_supervisor import ui_journal, ui_projection
        self.emitter = ui_projection.UiEmitter(enabled=True)
        self.journal = ui_journal.UiJournal(self.emitter)

    def test_equivalence_ok_on_consistent_journal(self):
        for seq_source in range(2):
            self.emitter.emit(
                "turn.started", f"task{seq_source}", task_id=f"task{seq_source}",
                worker="codex", mode="app-server", session_id=f"s{seq_source}",
                turn_id="t1")
            self.emitter.emit(
                "turn.completed", f"task{seq_source}",
                task_id=f"task{seq_source}", worker="codex", mode="app-server",
                session_id=f"s{seq_source}", turn_id="t1", status="completed")
        result = self.tool.journal_selfcheck_equivalence(self.journal)
        self.assertTrue(result["ok"], result)
        self.assertEqual(result["states"], 2)
        self.assertEqual(result["events"], 4)

    def test_equivalence_ok_on_empty_journal(self):
        result = self.tool.journal_selfcheck_equivalence(self.journal)
        self.assertTrue(result["ok"], result)
        self.assertEqual(result["states"], 0)

    def test_hub_stream_excluded_from_state_comparison(self):
        # hub events fold to a state but never appear in state pages; the
        # selfcheck must not count that as a divergence
        self.emitter.emit(
            "turn.started", "taskA", task_id="taskA", worker="codex",
            mode="app-server", session_id="s", turn_id="t")
        self.emitter.emit(
            "stream.announced", "hub", task_id="taskA", worker="codex",
            mode="app-server",
            data={"task_stream_id": "taskA", "accepted_event_id": "e/1"})
        result = self.tool.journal_selfcheck_equivalence(self.journal)
        self.assertTrue(result["ok"], result)


class WindowRecorderTests(unittest.TestCase):
    def test_close_window_records_markers_and_deltas(self):
        from zcode_supervisor import ui_journal, ui_projection

        async def scenario():
            tool = load_tool()
            with tempfile.TemporaryDirectory() as tmp:
                state = Path(tmp)
                state.chmod(0o700)
                emitter = ui_projection.UiEmitter(enabled=True)
                journal = ui_journal.UiJournal(emitter)
                monitor = tool.SubscriptionMonitor()
                monitor.install(journal)
                from zcode_supervisor import ui_server as ui_server_mod
                server = ui_server_mod.UiEventServer(journal,
                                                     state_dir=state)
                await server.start()
                accounting = tool.new_accounting()
                recorder = tool.WindowRecorder(server, journal, accounting,
                                               1.0, monitor)
                for i in range(5):
                    emitter.emit(
                        "turn.started", f"t{i}", task_id=f"t{i}",
                        worker="codex", mode="app-server",
                        session_id="s", turn_id="x")
                    accounting["events_emitted"] += 1
                accounting["reconnects"] = 3
                window = recorder.close_window(
                    0, 1.0, {"completed_total": 0},
                    {"ok": True, "mismatched_streams": []})
                await server.close()
                journal.detach()
                return window

        window = asyncio.run(scenario())
        self.assertEqual(window["events_emitted"], 5)
        self.assertEqual(window["events_per_second"], 5.0)
        self.assertEqual(window["reconnects"], 3)
        self.assertEqual(window["journal_total_events"], 5)
        self.assertEqual(window["receipts_completed"], 0)
        self.assertIn("wall_utc", window)
        self.assertGreaterEqual(window["lag_samples"], 0)
        # repair2: queue peaks come from the exact per-event instrument
        self.assertEqual(window["queue_hwm_instrument"], "exact-per-event")
        self.assertEqual(window["events_emitted_total"], 5)


class WorkloadSmokeTests(unittest.TestCase):
    """One short end-to-end workload run (~8 s) against the real stack."""

    def test_short_workload_run_evidence_ok(self):
        tool = load_tool()

        async def run():
            with tempfile.TemporaryDirectory() as tmp:
                return await tool.run_workload(
                    "baseline", 6.0, 2.0, Path(tmp), None)

        summary = asyncio.run(run())
        self.assertEqual(summary["model_requests"], 0)
        self.assertIsNone(summary["interrupted"])
        self.assertTrue(summary["evidence_ok"], summary["totals"])
        self.assertGreaterEqual(summary["windows_recorded"], 2)
        self.assertGreaterEqual(summary["full_windows"], 1)
        self.assertEqual(summary["totals"]["resume_violations"], 0)
        self.assertEqual(summary["totals"]["client_loss_violations"], 0)
        self.assertEqual(summary["totals"]["emit_failures"], 0)
        # F-01 symptom observation is recorded (>= 0), never gated
        self.assertGreaterEqual(summary["totals"]["cross_stream_frames"], 0)
        # freeze audit field is None for a baseline (no frozen doc)
        self.assertIsNone(summary["baseline_frozen_at"])


class QueueHighWaterInstrumentTests(unittest.TestCase):
    """repair2: exact per-event queue high-watermarks (no sampling)."""

    def setUp(self):
        self.tool = load_tool()

    def test_monitored_deque_tracks_exact_peaks_and_resets(self):
        monitor = self.tool.SubscriptionMonitor()
        queue = self.tool.MonitoredDeque([], monitor=monitor)
        frames = [{"type": "event", "event": {"seq": i}} for i in range(4)]
        for frame in frames[:3]:
            queue.append(frame)
        self.assertEqual(queue.window_max_events, 3)
        queue.popleft()
        queue.append(frames[3])
        self.assertEqual(queue.window_max_events, 3)  # popleft first
        peaks = monitor.window_peaks()
        self.assertEqual(peaks["events"], 3)
        self.assertGreater(peaks["bytes"], 0)
        # window reset: the peak counters restart at zero (the undrained
        # 3-frame DEPTH carries over, so the next append raises the new
        # window's depth peak to 4 — depth-based, not append-count-based)
        self.assertEqual(queue.window_max_events, 0)
        queue.append({"type": "event", "event": {"seq": 99}})
        self.assertEqual(queue.window_max_events, 4)
        self.assertGreaterEqual(queue.lifetime_max_events, 3)

    def test_monitored_deque_close_finalizes_into_next_window(self):
        monitor = self.tool.SubscriptionMonitor()
        queue = self.tool.MonitoredDeque([], monitor=monitor)
        queue.append({"type": "event", "event": {"seq": 1}})
        queue.append({"type": "event", "event": {"seq": 2}})
        queue.clear()  # subscription closed
        self.assertTrue(queue.closed)
        peaks = monitor.window_peaks()  # final contribution collected...
        self.assertEqual(peaks["events"], 2)
        # ...and the closed queue is pruned from the registry
        queue.append({"type": "event", "event": {"seq": 3}})  # post-close
        self.assertEqual(monitor.window_peaks()["events"], 0)

    def test_monitor_installs_on_journal_registry(self):
        from zcode_supervisor import ui_journal, ui_projection

        async def scenario():
            emitter = ui_projection.UiEmitter(enabled=True)
            journal = ui_journal.UiJournal(emitter)
            monitor = self.tool.SubscriptionMonitor()
            monitor.install(journal)
            emitter.emit("turn.started", "taskA", task_id="taskA",
                         worker="codex", mode="app-server",
                         session_id="s", turn_id="t")
            journal.subscribe_stream("taskA", 0)  # backfill enqueued
            return monitor

        monitor = asyncio.run(scenario())
        self.assertEqual(len(monitor._queues), 1)
        peaks = monitor.window_peaks()
        self.assertEqual(peaks["events"], 1)  # the replay backfill frame

    def test_monitor_refuses_late_install(self):
        from zcode_supervisor import ui_journal, ui_projection
        emitter = ui_projection.UiEmitter(enabled=True)
        journal = ui_journal.UiJournal(emitter)
        journal.subscribe_stream("taskZ", 0)
        monitor = self.tool.SubscriptionMonitor()
        with self.assertRaises(RuntimeError):
            monitor.install(journal)


class ThresholdV2Tests(unittest.TestCase):
    """repair2: mechanism-derived queue gates + verbatim v1 inheritance."""

    def setUp(self):
        self.tool = load_tool()
        self.baseline = {
            "duration_s": 360.0,
            "windows": [make_window() for _ in range(6)],
            "stall_s_observed_max": 3.002,
            "queue_frame_size_stats": {"mean_frame_bytes": 556.25},
        }

    def _v1_frozen(self):
        # a minimal in-memory v1 document (same caps as the real formula)
        baseline = {"duration_s": 360.0,
                    "windows": [make_window() for _ in range(6)]}
        return {"schema": 1, "frozen_at": "2026-10-29T05:37:10.000000Z",
                "baseline": {"started_at": "2026-10-29T05:30:07Z",
                             "ended_at": "2026-10-29T05:36:10Z",
                             "duration_s": 363.2, "windows": 7},
                "workload_shape": self.tool.WORKLOAD_SHAPE,
                "thresholds": self.tool.compute_thresholds(baseline)}

    def test_v2_formula_derived_not_fitted(self):
        v2 = self.tool.compute_thresholds_v2(self.baseline, self._v1_frozen())
        formula = v2["formula"]
        # 3.002s x 30.0 ev/s x 2.0 + 1 = 181 events
        self.assertEqual(formula["stall_s_observed_max"], 3.002)
        self.assertEqual(formula["inflow_rate_events_per_s_max_window"], 30.0)
        self.assertEqual(formula["safety"], 2.0)
        self.assertEqual(v2["thresholds"]["queue_peak_events_max"]["cap"],
                         int(3.002 * 30.0 * 2.0) + 1)
        # bytes: same product over mean frame bytes (+1024), clamped by design
        expected_bytes = int(3.002 * 30.0 * 556.25 * 2.0) + 1024
        self.assertEqual(v2["thresholds"]["queue_peak_bytes_max"]["cap"],
                         min(expected_bytes, self.tool.DESIGN_QUEUE_MAX_BYTES))
        self.assertIn("no_fit_note", formula)

    def test_v2_inherits_sixteen_v1_gates_verbatim(self):
        v1 = self._v1_frozen()
        v2 = self.tool.compute_thresholds_v2(self.baseline, v1)
        self.assertEqual(len(v2["inherited_gates"]), 16)
        for name in v2["inherited_gates"]:
            self.assertEqual(v2["thresholds"][name]["cap"],
                             v1["thresholds"][name]["cap"], name)
            self.assertEqual(v2["thresholds"][name]["source"],
                             "inherited-v1")
        for name in ("queue_peak_events_max", "queue_peak_bytes_max"):
            self.assertEqual(v2["thresholds"][name]["source"],
                             "v2-mechanism")

    def test_v2_requires_stall_and_frame_measurements(self):
        broken = dict(self.baseline)
        broken["stall_s_observed_max"] = 0.0
        with self.assertRaises(ValueError):
            self.tool.compute_thresholds_v2(broken, self._v1_frozen())
        broken = dict(self.baseline)
        broken["queue_frame_size_stats"] = {}
        with self.assertRaises(ValueError):
            self.tool.compute_thresholds_v2(broken, self._v1_frozen())

    def test_validate_frozen_accepts_schema2_and_rejects_broken(self):
        v1 = self._v1_frozen()
        v2_doc = {
            "schema": 2, "kind": "recalibrated-queue-gates",
            "frozen_at": "2026-10-29T08:00:00.000000Z",
            "inherited_from": {"file": "x", "sha256": "0" * 64,
                               "frozen_at": v1["frozen_at"],
                               "baseline_started_at": "2026-10-29T05:30:07Z"},
            "baseline": {"started_at": "x", "ended_at": "y",
                         "duration_s": 363.0, "windows": 7,
                         "samples_file": "s", "samples_sha256": "0" * 64},
            "workload_shape": self.tool.WORKLOAD_SHAPE,
            "thresholds": self.tool.compute_thresholds_v2(
                self.baseline, v1)["thresholds"],
        }
        self.assertEqual(self.tool.validate_frozen(v2_doc), [])
        # wrong: a v2 doc whose queue gates are not v2-mechanism
        broken = json.loads(json.dumps(v2_doc))
        broken["thresholds"]["queue_peak_events_max"]["source"] = \
            "inherited-v1"
        self.assertTrue(self.tool.validate_frozen(broken))
        # wrong: 15 inherited gates
        broken = json.loads(json.dumps(v2_doc))
        del broken["thresholds"]["rss_max_bytes"]
        problems = self.tool.validate_frozen(broken)
        self.assertTrue(any("16" in p for p in problems))
        # wrong: missing inherited_from
        broken = json.loads(json.dumps(v2_doc))
        del broken["inherited_from"]
        self.assertTrue(self.tool.validate_frozen(broken))
        # v1 (schema 1) documents still validate unchanged
        self.assertEqual(self.tool.validate_frozen(v1), [])

    def test_evaluate_gates_against_v2_document(self):
        v1 = self._v1_frozen()
        thresholds = self.tool.compute_thresholds_v2(
            self.baseline, v1)["thresholds"]
        frozen2 = {"schema": 2, "frozen_at": "2026-10-29T08:00:00.000000Z",
                   "thresholds": thresholds}
        samples = {
            "duration_s": 1800.0,
            "windows": [make_window(queue_peak_events=88,
                                    queue_peak_bytes=55000)
                        for _ in range(30)],
            "totals": {"reconnects": 900, "resets_received": 0,
                       "evidence_mismatch": 0, "resume_violations": 0,
                       "client_loss_violations": 0},
        }
        gates = {g["gate"]: g for g in
                 self.tool.evaluate_gates(samples, frozen2)["gates"]}
        self.assertTrue(gates["queue_peak_events_max"]["pass"])  # 88 <= 181
        self.assertTrue(gates["queue_peak_bytes_max"]["pass"])
        result = self.tool.evaluate_gates(samples, frozen2)
        self.assertTrue(result["all_pass"])


class CliTests(unittest.TestCase):
    def test_help_exits_zero(self):
        completed = subprocess.run(
            [sys.executable, str(TOOL_PATH), "--help"],
            capture_output=True, text=True, timeout=60)
        self.assertEqual(completed.returncode, 0, completed.stderr)

    def test_baseline_duration_floor_enforced(self):
        completed = subprocess.run(
            [sys.executable, str(TOOL_PATH), "run", "--role", "baseline",
             "--duration", "299", "--out", "/tmp/never"],
            capture_output=True, text=True, timeout=60)
        self.assertEqual(completed.returncode, 2)
        self.assertIn(">= 300", completed.stderr)

    def test_soak_duration_floor_and_frozen_required(self):
        completed = subprocess.run(
            [sys.executable, str(TOOL_PATH), "run", "--role", "soak",
             "--duration", "1799", "--out", "/tmp/never"],
            capture_output=True, text=True, timeout=60)
        self.assertEqual(completed.returncode, 2)
        self.assertIn(">= 1800", completed.stderr)
        completed = subprocess.run(
            [sys.executable, str(TOOL_PATH), "run", "--role", "soak",
             "--duration", "1800", "--out", "/tmp/never"],
            capture_output=True, text=True, timeout=60)
        self.assertEqual(completed.returncode, 2)
        self.assertIn("--frozen", completed.stderr)


if __name__ == "__main__":
    unittest.main()
