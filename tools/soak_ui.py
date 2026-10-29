#!/usr/bin/env python3
"""P3.2 soak harness: multi-session UI-event soak over the REAL P1.1 stack.

Drives the production ``UiEmitter -> UiJournal -> UiEventServer`` pipeline
(real owner-only UDS, real bounded journal) with FAKE producers and FAKE
consumers only — no model, no real CLI, no remote (``model_requests == 0``
always). The workload shape is IDENTICAL for the short baseline dry-run and
the >=30 min soak; only the duration differs, so thresholds frozen from the
baseline are meaningful for the soak.

Workload (one asyncio loop shared with the server):

* 4 producer "sessions", each cycling task lifecycles through the REAL
  projection helpers (``project_task_accepted`` / ``session_bound`` /
  ``turn_event`` / ``tool_event`` / ``approval_hook`` /
  ``project_task_completed``) at a combined ~30 events/s — dense
  notifications with constant task churn (retention/eviction pressure).
* A receipt pump that every 20 s runs one REAL ``codex_driver.run_readonly``
  dispatch against a FAKE ``codex`` child (same argv contract as the test
  fixtures; modes rotate delivered/noisy/delivered/mismatch/fail), wired
  with the production ``make_exit_sink`` (real ``process.exited`` hub
  events) and the P2.3 ``metrics_sink`` for raw stdout/stderr bytes and the
  receipt metadata split (``event_counts`` / ``hard_errors`` /
  ``command_items``).
* 6 fake UI consumers over the real socket: two persistent subscribe-all
  readers, one single-stream follower that periodically disconnects and
  RESUMES with ``after_seq`` (also exercising the documented ``gap``
  recovery path on evicted streams), one throttled reader with periodic
  stalls (queue-peak pressure), and two churn clients looping hello ->
  full snapshot pages -> full snapshot-events pages -> abrupt disconnect
  (UI refresh + reconnect churn; every third cycle reuses a stale cursor
  to exercise ``cursor_expired``/``cursor_superseded``).

Fixed windows (default 60 s) record: RSS (main + whole process tree via
``ps``), ru_maxrss, tracemalloc heap, event-loop lag percentiles (10 Hz
probe), journal stats, EXACT per-event per-subscription queue high-water
marks (repair2: a monitored deque installed at the journal's subscription
registry — no 1 Hz sampling), retained frames/streams, an
evidence-equivalence self-check (same-cut snapshot states vs
``reduce_stream`` fold of the event pages), recovery counts, and the
receipt/raw-buffer itemisation.

CLI:
  run    --role baseline|soak --duration S --out DIR [--frozen FILE]
  freeze --baseline FILE --out FILE          (freeze thresholds from baseline)

Exit codes: 0 = contract met (duration, full windows, gates if soak);
2 = gate/validation failure; an exception mid-run is captured in the
summary with ``interrupted`` and exits 3.
"""
from __future__ import annotations

import argparse
import asyncio
import contextlib
import hashlib
import json
import os
import resource
import shutil
import subprocess
import sys
import tempfile
import time
import tracemalloc
import uuid
from collections import deque
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src") not in sys.path:
    sys.path.insert(0, str(ROOT / "src"))

from zcode_supervisor import (
    codex_driver,
    ui_events,
    ui_journal,
    ui_projection,
)
from zcode_supervisor import (
    ui_server as ui_server_mod,
)

# ---------------------------------------------------------------------------
# Workload shape (identical for baseline and soak — hashed into the frozen
# file and re-verified at soak start; a mismatched shape refuses to run).
# ---------------------------------------------------------------------------
WORKLOAD_SHAPE = {
    "sessions": 4,
    "events_per_second_total": 30.0,
    "turns_per_task": 3,
    "tool_pairs_per_turn": 2,
    "consumers": {
        "subscribe_all": 2,
        "resume_follower": 1,
        "throttled_reader": 1,
        "churn_refresh": 2,
    },
    "resume_cycle_s": 10.0,
    "churn_cycle_s": 4.0,
    "reader_stall_every_s": 45.0,
    "reader_stall_s": 3.0,
    "receipt_interval_s": 20.0,
    "heartbeat_s": 15.0,
}
WINDOW_S_DEFAULT = 60.0
MI = 1024 * 1024
#: design bounds the frozen thresholds may never exceed
DESIGN_QUEUE_MAX_EVENTS = ui_journal.CLIENT_QUEUE_MAX_EVENTS + 1  # + marker
DESIGN_QUEUE_MAX_BYTES = ui_journal.CLIENT_QUEUE_MAX_BYTES + 4096
DESIGN_RETAINED_MAX = ui_journal.JOURNAL_MAX_EVENTS


# ---------------------------------------------------------------------------
# Pure helpers (unit-tested; no I/O)
# ---------------------------------------------------------------------------
def percentile(values, pct: float):
    """Nearest-rank percentile of a sequence (pct in 0-100); None if empty."""
    items = list(values)
    if not items:
        return None
    ordered = sorted(items)
    rank = max(1, min(len(ordered), int(round(pct / 100.0 * len(ordered)))))
    return ordered[rank - 1]


def median(values):
    return percentile(values, 50)


def deep_size(obj) -> int:
    """Approximate in-memory bytes of a JSON-ish structure (bounded walk).

    Memoised by id() so shared/diamond structures are counted once; the
    walk is budget-capped so pathological inputs stay cheap.
    """
    seen: set = set()
    budget = [0]

    def walk(node):
        if budget[0] > 200_000:
            return 0
        budget[0] += 1
        marker = id(node)
        if marker in seen:
            return 0
        seen.add(marker)
        if node is None or isinstance(node, (bool, int, float)):
            return 8
        if isinstance(node, str):
            return 49 + len(node.encode("utf-8", "replace"))
        if isinstance(node, bytes):
            return 33 + len(node)
        if isinstance(node, dict):
            total = 64
            for key, value in node.items():
                total += walk(key) + walk(value)
            return total
        if isinstance(node, (list, tuple, set, frozenset)):
            total = 56 + 8 * len(node)
            for item in node:
                total += walk(item)
            return total
        return 16

    return walk(obj)


def _rate_per_hour(first, last, duration_s: float) -> float:
    """Linear growth/count rate per hour from boundary values."""
    if duration_s <= 0:
        return 0.0
    return (last - first) * 3600.0 / duration_s


def compute_thresholds(baseline_summary: dict) -> dict:
    """Freeze soak thresholds from the BASELINE dry-run only.

    Every cap records its basis (observed baseline value + margin formula)
    and is clamped by the design bounds where one exists. RSS/heap growth
    floors are deliberately generous: page-granular RSS and allocator
    arenas on macOS make a 10x extrapolation of a 5-minute window the
    tightest honest margin; the gate's job is to catch runaway growth, not
    micro-drift.
    """
    windows = baseline_summary.get("windows") or []
    if not windows:
        raise ValueError("baseline summary has no windows")
    duration_s = float(baseline_summary["duration_s"])
    hours = max(duration_s / 3600.0, 1e-9)
    rss = [w["rss_main_kb"] * 1024 for w in windows if w["rss_main_kb"]]
    heap = [w["heap_current_bytes"] for w in windows]
    lag_p95 = [w["lag_p95_ms"] for w in windows
               if w.get("lag_p95_ms") is not None]
    lag_max = [w["lag_max_ms"] for w in windows
               if w.get("lag_max_ms") is not None]
    q_ev = [w["queue_peak_events"] for w in windows]
    q_by = [w["queue_peak_bytes"] for w in windows]
    subs = [w["journal_subscriptions"] for w in windows]
    conns = [w["connections"] for w in windows]
    retained = [w["journal_total_events"] for w in windows]
    streams = [w["journal_streams"] for w in windows]
    dropped = [w["journal_dropped_events"] for w in windows]
    if not rss or not lag_p95 or not lag_max:
        raise ValueError("baseline windows lack rss/lag observations")

    rss_rate = _rate_per_hour(rss[0], rss[-1], duration_s)
    heap_rate = _rate_per_hour(heap[0], heap[-1], duration_s)
    total_reconnects = sum(w["reconnects"] for w in windows)
    total_resets = sum(w["resets_received"] for w in windows)

    def entry(cap, basis, design=None):
        item = {"cap": cap, "basis": basis}
        if design is not None:
            item["design_bound"] = design
        return item

    return {
        "rss_max_bytes": entry(
            int(max(max(rss) * 1.5, max(rss) + 64 * MI)),
            f"baseline max {int(max(rss))} B -> max(1.5x, +64 MiB)"),
        "rss_growth_per_hour_bytes": entry(
            int(max(rss_rate * 10.0, 32 * MI)),
            f"baseline rate {int(rss_rate)} B/h -> max(10x, 32 MiB/h floor)"),
        "heap_max_bytes": entry(
            int(max(max(heap) * 2.0, max(heap) + 16 * MI)),
            f"baseline max {int(max(heap))} B -> max(2x, +16 MiB)"),
        "heap_growth_per_hour_bytes": entry(
            int(max(heap_rate * 10.0, 16 * MI)),
            f"baseline rate {int(heap_rate)} B/h -> max(10x, 16 MiB/h floor)"),
        "loop_lag_p95_ms_max": entry(
            round(max(max(lag_p95) * 3.0, 100.0), 1),
            f"baseline max-window p95 {max(lag_p95)} ms -> max(3x, 100 ms)"),
        "loop_lag_max_ms_max": entry(
            round(max(max(lag_max) * 3.0, 500.0), 1),
            f"baseline max-window max {max(lag_max)} ms -> max(3x, 500 ms)"),
        "queue_peak_events_max": entry(
            int(min(max(max(q_ev) * 2 + 4, 8), DESIGN_QUEUE_MAX_EVENTS)),
            f"baseline peak {max(q_ev)} -> 2x+4, clamped to design bound",
            design=DESIGN_QUEUE_MAX_EVENTS),
        "queue_peak_bytes_max": entry(
            int(min(max(max(q_by) * 2, 4096), DESIGN_QUEUE_MAX_BYTES)),
            f"baseline peak {int(max(q_by))} B -> 2x, clamped to design bound",
            design=DESIGN_QUEUE_MAX_BYTES),
        "active_subscriptions_max": entry(
            int(max(max(subs) + 2, 8)),
            f"baseline max {max(subs)} -> +2, floor 8"),
        "connections_max": entry(
            int(max(max(conns) + 4, 12)),
            f"baseline max {max(conns)} -> +4, floor 12"),
        "retained_events_max": entry(
            int(min(int(max(retained) * 1.05) + 50, DESIGN_RETAINED_MAX + 1)),
            f"baseline max {max(retained)} -> 1.05x+50, clamped to journal "
            f"bound",
            design=DESIGN_RETAINED_MAX + 1),
        "retained_streams_max": entry(
            int(max(int(max(streams) * 1.5), 32)),
            f"baseline max {max(streams)} -> 1.5x, floor 32"),
        "journal_dropped_events_max": entry(
            int(max(dropped)),
            f"baseline max {max(dropped)} (producer emits no dup/oversize; "
            f"expected 0)"),
        "reconnects_per_hour_max": entry(
            int(max(total_reconnects / hours * 1.5, 2400)),
            f"baseline {total_reconnects} in {int(duration_s)}s -> 1.5x, "
            f"floor 2400/h (churn design ~1800/h)"),
        "resets_per_hour_max": entry(
            int(max(total_resets / hours * 2.0, 120)),
            f"baseline {total_resets} in {int(duration_s)}s -> 2x, "
            f"floor 120/h"),
        "evidence_mismatch_total_max": entry(
            0, "silent loss / cross-view divergence must be exactly 0"),
        "resume_violations_total_max": entry(
            0, "resume must never skip or duplicate a seq"),
        "client_loss_violations_total_max": entry(
            0, "subscribe-all readers must see every seq or an explicit gap"),
    }


QUEUE_GATE_SAFETY = 2.0
_QUEUE_SAFETY_BASIS = (
    "safety=2.0: covers (a) emission-rate burst variance up to 2x the "
    "measured max-window rate under uniform producer pacing, (b) the "
    "throttled subscriber's resubscribe initial backfill (bounded by one "
    "task-cycle history plus in-flight backlog, both < one stall window), "
    "and (c) a stall straddling a window boundary contributing its full "
    "depth to each window's high-water")


def compute_thresholds_v2(baseline_summary: dict, v1_frozen: dict) -> dict:
    """v2 recalibration: the two queue gates from a MECHANISM formula
    (stall duration x inflow rate x explicit safety), everything else
    inherited verbatim from the v1 frozen document.

    Formula inputs are measured in the v2 baseline run: the actual stall
    duration (max observed), the max-window event emission rate (the
    subscribe-all subscriber's inflow equals the total emission rate), and
    the exact mean queued-frame size from the high-watermark instrument's
    cumulative append accounting. None of them is a queue-DEPTH
    observation — the cap is derived, never fitted to observed peaks.
    """
    v1_thresholds = v1_frozen["thresholds"]
    windows = baseline_summary.get("windows") or []
    if not windows:
        raise ValueError("baseline summary has no windows")
    stall_s = float(baseline_summary.get("stall_s_observed_max") or 0.0)
    if stall_s <= 0:
        raise ValueError("v2 baseline recorded no stall observation — "
                         "the throttled reader never stalled")
    rate = max(w["events_per_second"] for w in windows)
    mean_frame_bytes = float(
        baseline_summary.get("queue_frame_size_stats", {}).get(
            "mean_frame_bytes") or 0.0)
    if mean_frame_bytes <= 0:
        raise ValueError("v2 baseline has no queue frame-size accounting")
    cap_events = int(min(
        stall_s * rate * QUEUE_GATE_SAFETY + 1, DESIGN_QUEUE_MAX_EVENTS))
    cap_bytes = int(min(
        stall_s * rate * mean_frame_bytes * QUEUE_GATE_SAFETY + 1024,
        DESIGN_QUEUE_MAX_BYTES))
    formula = {
        "stall_s_observed_max": round(stall_s, 3),
        "inflow_rate_events_per_s_max_window": round(rate, 3),
        "mean_frame_bytes": round(mean_frame_bytes, 3),
        "safety": QUEUE_GATE_SAFETY,
        "safety_basis": _QUEUE_SAFETY_BASIS,
        "cap_events": cap_events,
        "cap_bytes": cap_bytes,
        "design_bound_events": DESIGN_QUEUE_MAX_EVENTS,
        "design_bound_bytes": DESIGN_QUEUE_MAX_BYTES,
        "no_fit_note": "caps derived from the formula; observed queue "
                       "high-watermarks were NOT inputs",
    }
    thresholds = {}
    for name, item in v1_thresholds.items():
        thresholds[name] = {
            "cap": item["cap"], "basis": item["basis"],
            "source": "inherited-v1",
        }
        if "design_bound" in item:
            thresholds[name]["design_bound"] = item["design_bound"]
    thresholds["queue_peak_events_max"] = {
        "cap": cap_events,
        "basis": f"mechanism: {stall_s:.3f}s stall x {rate:.3f} ev/s x "
                 f"safety {QUEUE_GATE_SAFETY} (+1), clamped to design bound",
        "source": "v2-mechanism", "formula": formula,
        "design_bound": DESIGN_QUEUE_MAX_EVENTS}
    thresholds["queue_peak_bytes_max"] = {
        "cap": cap_bytes,
        "basis": f"mechanism: {stall_s:.3f}s x {rate:.3f} ev/s x "
                 f"{mean_frame_bytes:.1f} B/frame x safety "
                 f"{QUEUE_GATE_SAFETY} (+1024), clamped to design bound",
        "source": "v2-mechanism", "formula": formula,
        "design_bound": DESIGN_QUEUE_MAX_BYTES}
    inherited = sorted(name for name, item in thresholds.items()
                       if item["source"] == "inherited-v1")
    return {"thresholds": thresholds, "inherited_gates": inherited,
            "formula": formula}


def validate_frozen(frozen: dict) -> list:
    """Structural validation of a frozen-threshold document (errors back).

    Accepts schema 1 (v1) and schema 2 (repair2 recalibration: 16 gates
    inherited from v1 + the two queue gates from the mechanism formula).
    """
    problems = []
    if not isinstance(frozen, dict) or frozen.get("schema") not in (1, 2):
        return ["schema must be 1 or 2"]
    for field in ("frozen_at", "thresholds", "workload_shape"):
        if field not in frozen:
            problems.append(f"missing field: {field}")
    if problems:
        return problems
    if frozen.get("schema") == 2:
        for field in ("baseline", "inherited_from"):
            if field not in frozen:
                problems.append(f"missing field: {field}")
        if problems:
            return problems
        inherited = frozen.get("inherited_from") or {}
        if not isinstance(inherited.get("sha256"), str) \
                or not inherited.get("frozen_at"):
            problems.append("inherited_from needs the v1 sha256/frozen_at")
    if not isinstance(frozen["thresholds"], dict) \
            or not frozen["thresholds"]:
        problems.append("thresholds must be a non-empty object")
    for name, item in (frozen.get("thresholds") or {}).items():
        if not isinstance(item, dict) or "cap" not in item \
                or "basis" not in item:
            problems.append(f"threshold {name!r} lacks cap/basis")
        elif frozen.get("schema") == 2 and "source" not in item:
            problems.append(f"threshold {name!r} lacks source (v2)")
    if frozen.get("schema") == 2:
        for gate in ("queue_peak_events_max", "queue_peak_bytes_max"):
            item = (frozen.get("thresholds") or {}).get(gate) or {}
            if item.get("source") != "v2-mechanism" \
                    or "formula" not in item:
                problems.append(f"{gate} must be v2-mechanism with formula")
        inherited = [name for name, item
                     in (frozen.get("thresholds") or {}).items()
                     if item.get("source") == "inherited-v1"]
        if len(inherited) != 16:
            problems.append(f"expected 16 inherited-v1 gates, got "
                            f"{len(inherited)}")
    if frozen["workload_shape"] != WORKLOAD_SHAPE:
        problems.append("workload_shape differs from this tool's shape")
    return problems


def load_frozen(path) -> dict:
    frozen = json.loads(Path(path).read_text(encoding="utf-8"))
    problems = validate_frozen(frozen)
    if problems:
        raise ValueError("invalid frozen document: " + "; ".join(problems))
    return frozen


def freeze_precedes_soak(frozen: dict, soak_started_at: str) -> bool:
    """Audit invariant: the freeze timestamp strictly precedes the soak."""
    return bool(frozen.get("frozen_at")) \
        and bool(soak_started_at) \
        and frozen["frozen_at"] < soak_started_at


def evaluate_gates(samples: dict, frozen: dict) -> dict:
    """Compare soak samples against frozen thresholds -> gate table."""
    windows = samples.get("windows") or []
    duration_s = float(samples.get("duration_s") or 0.0)
    hours = max(duration_s / 3600.0, 1e-9)
    thresholds = frozen["thresholds"]
    gates = []

    def add(name, observed):
        passed = observed <= thresholds[name]["cap"]
        gates.append({"gate": name, "observed": observed,
                      "cap": thresholds[name]["cap"],
                      "pass": bool(passed)})

    if windows:
        rss = [w["rss_main_kb"] * 1024 for w in windows if w["rss_main_kb"]]
        heap = [w["heap_current_bytes"] for w in windows]
        add("rss_max_bytes", int(max(rss)))
        add("rss_growth_per_hour_bytes",
            int(_rate_per_hour(rss[0], rss[-1], duration_s)))
        add("heap_max_bytes", int(max(heap)))
        add("heap_growth_per_hour_bytes",
            int(_rate_per_hour(heap[0], heap[-1], duration_s)))
        add("loop_lag_p95_ms_max",
            max(w["lag_p95_ms"] for w in windows
                if w.get("lag_p95_ms") is not None))
        add("loop_lag_max_ms_max",
            max(w["lag_max_ms"] for w in windows
                if w.get("lag_max_ms") is not None))
        add("queue_peak_events_max",
            max(w["queue_peak_events"] for w in windows))
        add("queue_peak_bytes_max",
            max(w["queue_peak_bytes"] for w in windows))
        add("active_subscriptions_max",
            max(w["journal_subscriptions"] for w in windows))
        add("connections_max", max(w["connections"] for w in windows))
        add("retained_events_max",
            max(w["journal_total_events"] for w in windows))
        add("retained_streams_max",
            max(w["journal_streams"] for w in windows))
        add("journal_dropped_events_max",
            max(w["journal_dropped_events"] for w in windows))
    totals = samples.get("totals") or {}
    add("reconnects_per_hour_max", int(totals.get("reconnects", 0) / hours))
    add("resets_per_hour_max",
        int(totals.get("resets_received", 0) / hours))
    add("evidence_mismatch_total_max",
        int(totals.get("evidence_mismatch", 0)))
    add("resume_violations_total_max",
        int(totals.get("resume_violations", 0)))
    add("client_loss_violations_total_max",
        int(totals.get("client_loss_violations", 0)))
    return {"gates": gates, "all_pass": all(g["pass"] for g in gates)}


def performance_report(samples: dict) -> dict:
    """Observation statistics (samples/median/p95/max + raw series).

    No thresholds are applied here: metrics without a frozen gate are
    observations only and never imply a performance pass.
    """
    windows = samples.get("windows") or []
    series = {
        "rss_main_kb": [w["rss_main_kb"] for w in windows],
        "children_rss_kb": [w["children_rss_sum_kb"] for w in windows],
        "ru_maxrss_raw": [w["ru_maxrss_raw"] for w in windows],
        "heap_current_bytes": [w["heap_current_bytes"] for w in windows],
        "heap_window_peak_bytes":
            [w["heap_window_peak_bytes"] for w in windows],
        "lag_p50_ms": [w["lag_p50_ms"] for w in windows
                       if w.get("lag_p50_ms") is not None],
        "lag_p95_ms": [w["lag_p95_ms"] for w in windows
                       if w.get("lag_p95_ms") is not None],
        "lag_max_ms": [w["lag_max_ms"] for w in windows
                       if w.get("lag_max_ms") is not None],
        "queue_peak_events": [w["queue_peak_events"] for w in windows],
        "queue_peak_bytes": [w["queue_peak_bytes"] for w in windows],
        "journal_total_events": [w["journal_total_events"] for w in windows],
        "journal_streams": [w["journal_streams"] for w in windows],
        "journal_total_bytes": [w["journal_total_bytes"] for w in windows],
        "events_emitted": [w["events_emitted"] for w in windows],
        "events_per_second": [w["events_per_second"] for w in windows],
        "frames_sent": [w["frames_sent"] for w in windows],
        "heartbeats_received": [w["heartbeats_received"] for w in windows],
        "receipts_completed": [w["receipts_completed"] for w in windows],
        "receipts_retained_deep_bytes":
            [w["receipts"]["retained_deep_bytes"] for w in windows],
    }
    report = {"window_count": len(windows), "note": "observation-only; "
              "gates live in gates.json against baseline-frozen.json"}
    for name, values in series.items():
        report[name] = {
            "samples": len(values),
            "median": median(values),
            "p95": percentile(values, 95),
            "max": max(values) if values else None,
            "raw": values,
        }
    return report


def utc_now_iso() -> str:
    return ui_events.utc_now()


def atomic_write_json(path: Path, payload: dict) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2,
                              default=str) + "\n", encoding="utf-8")
    os.replace(tmp, path)


def sha256_file(path) -> str:
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


# ---------------------------------------------------------------------------
# Fake codex child (argv contract per tests/test_codex_driver.py; modes
# rotate the receipt metadata shapes: command_items / hard_errors /
# transport_retries / raw stderr bytes)
# ---------------------------------------------------------------------------
FAKE_CODEX = '''#!/usr/bin/env python3
"""Fake codex CLI for the P3.2 soak: canned JSONL, env-switched modes."""
import json
import os
import sys

argv = sys.argv[1:]
for token in ("exec", "--json", "read-only", "--skip-git-repo-check", "-"):
    if token not in argv:
        sys.stderr.write("fake-codex: missing argv token %r\\n" % token)
        sys.exit(3)
if "--ephemeral" in argv:
    sys.exit(3)


def value_of(flag):
    return argv[argv.index(flag) + 1] if flag in argv else None


try:
    with open(value_of("--output-schema")) as handle:
        json.load(handle)
except OSError:
    sys.exit(3)
sys.stdin.read()
mode = os.environ.get("SOAK_FAKE_CODEX_MODE", "delivered")
THREAD = "00000000-1111-2222-3333-444444444444"


def emit(events):
    for event in events:
        sys.stdout.write(json.dumps(event) + "\\n")
    sys.stdout.flush()


if mode == "noisy":
    emit([
        {"type": "thread.started", "thread_id": THREAD},
        {"type": "turn.started", "model": "soak-fake-model"},
        {"type": "error", "message": "Reconnecting... 1/5 (request timed out)"},
        {"type": "error", "message": "Reconnecting... 2/5 (request timed out)"},
        {"type": "error", "message": "stream disconnected before completion"},
        {"type": "item.completed", "item": {"type": "command_execution",
            "command": "git status", "exit_code": 0, "status": "completed"}},
        {"type": "turn.completed"},
    ])
    sys.exit(0)
if mode == "fail":
    sys.stderr.write("fake-codex soak simulated crash\\n")
    sys.exit(2)

emit([
    {"type": "thread.started", "thread_id": THREAD},
    {"type": "turn.started", "model": "soak-fake-model"},
    {"type": "item.completed", "item": {"type": "command_execution",
        "command": "ls", "exit_code": 0, "status": "completed"}},
    {"type": "totally.unknown", "surprise": True},
    {"type": "turn.completed"},
])
payload = {"summary": "ok", "files": ["a"]}
if mode == "mismatch":
    payload = {"summary": "ok"}
with open(value_of("-o"), "w") as handle:
    handle.write(json.dumps(payload) if mode != "badjson" else "{not json")
sys.exit(0)
'''

FAKE_MODES = ("delivered", "noisy", "delivered", "mismatch", "fail")


def new_accounting() -> dict:
    return {
        "events_emitted": 0,
        "emit_failures": 0,
        "planned_tasks": set(),
        "reconnects": 0,
        "resets_received": 0,
        "gap_frames": 0,
        "heartbeats_received": 0,
        "expected_protocol_errors": 0,
        "op_timeouts": 0,
        "resume_violations": 0,
        "client_loss_violations": 0,
        "cross_stream_frames": 0,
        "frames_received": 0,
        "stall_s_observed_max": 0.0,
    }


# ---------------------------------------------------------------------------
# repair2: exact per-event queue high-watermark instrument (harness-side,
# zero src changes). The journal appends every new subscription to
# ``journal._subscriptions``; replacing that list with a monitored subclass
# lets the harness swap each subscription's ``_queue`` deque for a counting
# deque at creation. Every append/popleft/clear updates tracked depth, so
# the window high-water is EXACT (no 1 Hz sampling phase error — the
# repair1 FAIL mode). Frame bytes are accounted with the same canonical
# JSON encoding the journal itself uses.
# ---------------------------------------------------------------------------
def _queue_frame_bytes(frame) -> int:
    """Byte size of one queued frame — identical encoding to the journal's
    ``_event_bytes`` (compact separators, ensure_ascii=False)."""
    return len(json.dumps(frame, ensure_ascii=False,
                          separators=(",", ":")).encode("utf-8"))


class MonitoredDeque(deque):
    """A deque that tracks its own exact depth/bytes high-water marks."""

    def __init__(self, iterable=(), monitor=None):
        super().__init__(iterable)
        self.monitor = monitor
        self.window_max_events = 0
        self.window_max_bytes = 0
        self.lifetime_max_events = 0
        self.lifetime_max_bytes = 0
        self.closed = False
        # adopt any pre-existing contents (subscribe_* enqueues backfill
        # BEFORE appending the subscription to the registry)
        current_bytes = sum(_queue_frame_bytes(frame) for frame in self)
        self._tracked_bytes = current_bytes
        self._note(len(self), current_bytes)
        if monitor is not None:
            monitor.register(self)

    def _note(self, events: int, bytes_now: int) -> None:
        self.window_max_events = max(self.window_max_events, events)
        self.window_max_bytes = max(self.window_max_bytes, bytes_now)
        self.lifetime_max_events = max(self.lifetime_max_events, events)
        self.lifetime_max_bytes = max(self.lifetime_max_bytes, bytes_now)

    def append(self, frame) -> None:
        super().append(frame)
        size = _queue_frame_bytes(frame)
        self._tracked_bytes += size
        if self.monitor is not None:
            self.monitor.frames_enqueued += 1
            self.monitor.bytes_enqueued += size
        self._note(len(self), self._tracked_bytes)

    def popleft(self):
        frame = super().popleft()
        self._tracked_bytes -= _queue_frame_bytes(frame)
        return frame

    def clear(self) -> None:
        super().clear()
        self._tracked_bytes = 0
        self.closed = True
        if self.monitor is not None:
            self.monitor.finalize(self)


class _MonitoredSubscriptionRegistry(list):
    """The journal's subscription list: on append, wrap the new
    subscription's queue in a MonitoredDeque (creation interception)."""

    def __init__(self, monitor: SubscriptionMonitor):
        super().__init__()
        self._monitor = monitor

    def append(self, subscription) -> None:
        queue = getattr(subscription, "_queue", None)
        if isinstance(queue, MonitoredDeque) or queue is None:
            super().append(subscription)
            return
        # the constructor self-registers with the monitor
        monitored = MonitoredDeque(queue, monitor=self._monitor)
        subscription._queue = monitored
        super().append(subscription)


class SubscriptionMonitor:
    """Collects exact window peaks across all live+freshly-closed queues."""

    def __init__(self):
        self._queues: list[MonitoredDeque] = []
        self.frames_enqueued = 0
        self.bytes_enqueued = 0

    def install(self, journal) -> None:
        """Take over the journal's subscription registry (before any
        subscription exists — call right after journal construction)."""
        if journal._subscriptions:
            raise RuntimeError("monitor must be installed before the first "
                               "subscription is created")
        journal._subscriptions = _MonitoredSubscriptionRegistry(self)

    def register(self, queue: MonitoredDeque) -> None:
        self._queues.append(queue)

    def finalize(self, queue: MonitoredDeque) -> None:
        # closed subscriptions stay until the next window collects their
        # final contribution, then get pruned
        pass

    def window_peaks(self) -> dict:
        """Exact per-window peaks; resets window counters and prunes the
        closed queues whose final contribution this window just consumed."""
        events = 0
        bytes_now = 0
        for queue in self._queues:
            events = max(events, queue.window_max_events)
            bytes_now = max(bytes_now, queue.window_max_bytes)
        for queue in self._queues:
            queue.window_max_events = 0
            queue.window_max_bytes = 0
        self._queues = [q for q in self._queues if not q.closed]
        return {"events": events, "bytes": bytes_now}

    def mean_frame_bytes(self) -> float:
        """Mean queued-frame size — an exact SIZE statistic (cumulative
        appended bytes / frames over all queues), independent of any
        queue-depth observation; feeds the v2 bytes formula."""
        if self.frames_enqueued == 0:
            return 0.0
        return self.bytes_enqueued / self.frames_enqueued


# ---------------------------------------------------------------------------
# Fake producer: task lifecycles via the REAL projection helpers
# ---------------------------------------------------------------------------
class ProducerSessions:
    def __init__(self, emitter, shape, accounting):
        self.emitter = emitter
        self.shape = shape
        self.accounting = accounting
        self.task_counter = 0
        self.current_streams: dict[int, str] = {}
        self.retired_streams: list[str] = []

    def _lifecycle(self, task_id, session_id, worker, mode):
        turns = self.shape["turns_per_task"]
        pairs = self.shape["tool_pairs_per_turn"]
        resolver = ui_projection.default_initiator_resolver()
        steps = [lambda: ui_projection.project_task_accepted(
            self.emitter, resolver, task_id=task_id, worker=worker,
            mode=mode, contract_id=f"contract_{task_id}",
            contract_digest=hashlib.sha256(task_id.encode()).hexdigest(),
            created_at=ui_events.utc_now(),
            initiator={"agent": worker, "session_id": session_id}),
            lambda: ui_projection.session_bound(
                self.emitter, task_id, worker, mode, session_id,
                "resident")]
        hook = ui_projection.approval_hook(self.emitter, task_id, worker,
                                           mode)
        for turn in range(turns):
            turn_id = f"turn_{session_id}_{turn}"
            steps.append(lambda t=turn_id: ui_projection.turn_event(
                self.emitter, task_id, worker, mode, session_id, t))
            for pair in range(pairs):
                call_id = f"call_{turn}_{pair}"
                steps.append(lambda t=turn_id, c=call_id:
                             ui_projection.tool_event(
                                 self.emitter, task_id, worker, mode,
                                 session_id, t, c, "scheduled", "shell_cmd"))
                steps.append(lambda t=turn_id, c=call_id:
                             ui_projection.tool_event(
                                 self.emitter, task_id, worker, mode,
                                 session_id, t, c, "completed", "shell_cmd",
                                 exit_code=0))
            request_id = f"req_{turn_id}"
            appr_call = f"call_{turn}_appr"
            steps.append(lambda r=request_id, t=turn_id, c=appr_call:
                         hook({"phase": "requested",
                               "session_id": session_id, "turn_id": t,
                               "request_id": r, "tool_call_id": c,
                               "tool_name": "shell_cmd",
                               "requested_variant": "acceptForSession"}))
            steps.append(lambda r=request_id, t=turn_id, c=appr_call:
                         hook({"phase": "decided",
                               "session_id": session_id, "turn_id": t,
                               "request_id": r, "tool_call_id": c,
                               "tool_name": "shell_cmd", "decision": "allow",
                               "requested_variant": "acceptForSession"}))
            steps.append(lambda t=turn_id: ui_projection.turn_event(
                self.emitter, task_id, worker, mode, session_id, t,
                completed=True, status="completed"))
        steps.append(lambda: ui_projection.session_state(
            self.emitter, task_id, worker, mode, session_id, "resident",
            "exited", last_source_seq=turns))
        steps.append(lambda: ui_projection.project_task_completed(
            self.emitter, task_id=task_id, worker=worker, mode=mode,
            status="completed", outcome="delivered", delivery_ok=True,
            duration_ms=1234.5))
        return steps

    async def run_session(self, index, stop: asyncio.Event):
        interval = self.shape["sessions"] \
            / self.shape["events_per_second_total"]
        workers = ("codex", "zcode")
        modes = ("readonly", "contract")
        while not stop.is_set():
            self.task_counter += 1
            task_id = f"task{self.task_counter:06d}s{index}"
            session_id = f"sess_{uuid.uuid4().hex[:12]}"
            worker = workers[self.task_counter % len(workers)]
            mode = modes[index % len(modes)]
            self.accounting["planned_tasks"].add(task_id)
            previous = self.current_streams.get(index)
            if previous is not None:
                self.retired_streams.append(previous)
                del self.retired_streams[:-200]
            self.current_streams[index] = task_id
            for step in self._lifecycle(task_id, session_id, worker, mode):
                if stop.is_set():
                    return
                failures_before = len(self.emitter.failures)
                try:
                    step()
                except Exception:
                    self.accounting["emit_failures"] += 1
                if len(self.emitter.failures) > failures_before:
                    self.accounting["emit_failures"] += 1
                self.accounting["events_emitted"] += 1
                await asyncio.sleep(interval)


# ---------------------------------------------------------------------------
# Fake consumers over the real UiEventServer socket
# ---------------------------------------------------------------------------
class WireClient:
    """One UDS connection: bounded-op NDJSON client for the UI protocol."""

    def __init__(self, socket_path, label, accounting):
        self.socket_path = socket_path
        self.label = label
        self.accounting = accounting
        self.reader = None
        self.writer = None

    async def connect(self):
        self.reader, self.writer = await asyncio.wait_for(
            asyncio.open_unix_connection(path=str(self.socket_path)), 10.0)
        self.accounting["reconnects"] += 1

    async def close(self):
        if self.writer is not None:
            self.writer.close()
            with contextlib.suppress(ConnectionError, OSError):
                await asyncio.wait_for(self.writer.wait_closed(), 5.0)
            self.writer = None
            self.reader = None

    async def op(self, payload: dict, timeout: float = 10.0) -> dict:
        self.writer.write(
            (json.dumps(payload, ensure_ascii=False) + "\n").encode())
        line = await asyncio.wait_for(self.reader.readline(), timeout)
        self.accounting["frames_received"] += 1
        text = line.decode().strip()
        if not text:
            raise ConnectionError("empty line from server")
        return json.loads(text)

    async def hello(self) -> dict:
        return await self.op({"op": "hello", "v": 1, "client": self.label})

    async def subscribe_all(self, cursor: dict) -> dict:
        return await self.op({"op": "subscribe", "v": 1, "stream_id": "*",
                              "snapshot_cursor": cursor})

    async def subscribe_stream(self, stream_id: str, after_seq: int) -> dict:
        return await self.op({"op": "subscribe", "v": 1,
                              "stream_id": stream_id,
                              "after_seq": after_seq})


def _track_event_frame(frame: dict, client_map: dict, accounting: dict):
    """Strict no-silent-loss accounting for one streamed event frame."""
    event = frame.get("event") or {}
    stream = event.get("stream_id")
    seq = event.get("seq")
    if not isinstance(stream, str) or not isinstance(seq, int):
        accounting["client_loss_violations"] += 1
        return
    expected = client_map.get(stream)
    if expected is None:
        client_map[stream] = seq + 1  # first sight: no baseline to check
        return
    if seq != expected:
        accounting["client_loss_violations"] += 1
    else:
        client_map[stream] = seq + 1


def _apply_gap_frame(frame: dict, client_map: dict, accounting: dict):
    accounting["gap_frames"] += 1
    gap = frame.get("gap") or {}
    stream, to_seq = gap.get("stream_id"), gap.get("to_seq")
    if isinstance(stream, str) and isinstance(to_seq, int):
        client_map[stream] = to_seq + 1  # range honestly unavailable


class SubscribeAllConsumer:
    """Persistent subscribe-all reader: every seq or an explicit gap."""

    def __init__(self, socket_path, label, accounting):
        self.socket_path = socket_path
        self.label = label
        self.accounting = accounting
        self.client_map: dict = {}
        self.events = 0
        self.closed_by_reset = False

    async def run(self, stop: asyncio.Event):
        client = WireClient(self.socket_path, self.label, self.accounting)
        await client.connect()
        hello = await client.hello()
        response = await client.subscribe_all(
            hello["initial_snapshot_cursor"])
        if not response.get("ok"):
            self.accounting["expected_protocol_errors"] += 1
            await client.close()
            return
        try:
            while not stop.is_set():
                try:
                    line = await asyncio.wait_for(client.reader.readline(),
                                                  30.0)
                except asyncio.TimeoutError:
                    continue  # liveness probe; heartbeats flow every 15 s
                if not line:
                    break
                self.accounting["frames_received"] += 1
                frame = json.loads(line.decode())
                kind = frame.get("type")
                if kind == "event":
                    self.events += 1
                    _track_event_frame(frame, self.client_map,
                                       self.accounting)
                elif kind == "gap":
                    _apply_gap_frame(frame, self.client_map, self.accounting)
                elif kind == "reset_required":
                    self.accounting["resets_received"] += 1
                    self.closed_by_reset = True
                    break
                elif kind == "heartbeat":
                    self.accounting["heartbeats_received"] += 1
        except (ConnectionError, OSError, json.JSONDecodeError):
            self.accounting["op_timeouts"] += 1
        finally:
            await client.close()


class ResumeConsumer:
    """Single-stream follower with periodic disconnect + after_seq resume.

    Exercises the documented recovery paths: subscribing below the
    retention floor of an evicted stream answers ``gap`` (never a silent
    jump); the client re-subscribes at the reported high-water mark.

    FINDING F-01 (see tasks/P3.2/findings.md): the current journal fans
    out OTHER streams' live events into a single-stream subscription.
    Per-stream seq continuity still holds for every stream, so the
    continuity accounting is per-stream; frames whose stream differs from
    the subscription target are counted in ``cross_stream_frames`` (a
    defect symptom observation, deliberately NOT a frozen gate).
    """

    def __init__(self, socket_path, label, accounting, producers, shape):
        self.socket_path = socket_path
        self.label = label
        self.accounting = accounting
        self.producers = producers
        self.shape = shape
        self.target = None
        self.stream_last_seq: dict[str, int] = {}
        self.events = 0
        self.cycle = 0

    def _target_stream(self):
        session = self.shape["sessions"] - 1
        retired = self.producers.retired_streams
        if self.cycle % 4 == 3 and retired:
            return retired[0], True  # old stream: gap path possible
        return self.producers.current_streams.get(session), False

    async def _consume(self, client, stop):
        while not stop.is_set():
            try:
                line = await asyncio.wait_for(client.reader.readline(), 5.0)
            except asyncio.TimeoutError:
                return "cycle"
            if not line:
                return "disconnected"
            self.accounting["frames_received"] += 1
            frame = json.loads(line.decode())
            if frame.get("type") == "event":
                event = frame.get("event") or {}
                stream = event.get("stream_id")
                seq = event.get("seq")
                if not isinstance(stream, str) or not isinstance(seq, int):
                    self.accounting["resume_violations"] += 1
                    continue
                if stream != self.target:
                    self.accounting["cross_stream_frames"] += 1
                last = self.stream_last_seq.get(stream)
                if last is not None and seq != last + 1:
                    self.accounting["resume_violations"] += 1
                self.stream_last_seq[stream] = seq
                self.events += 1
            elif frame.get("type") == "gap":
                self.accounting["gap_frames"] += 1
                gap = frame.get("gap") or {}
                stream = gap.get("stream_id")
                to_seq = gap.get("to_seq")
                if isinstance(stream, str) and isinstance(to_seq, int):
                    self.stream_last_seq[stream] = to_seq
            elif frame.get("type") == "reset_required":
                self.accounting["resets_received"] += 1
                return "reset"

    async def run(self, stop: asyncio.Event):
        while not stop.is_set():
            self.cycle += 1
            stream, is_old = self._target_stream()
            if stream is None:
                await asyncio.sleep(0.5)
                continue
            client = WireClient(self.socket_path, self.label, self.accounting)
            try:
                await client.connect()
                await client.hello()
                after = self.stream_last_seq.get(stream, 0)
                response = await client.subscribe_stream(stream, after)
                if not response.get("ok"):
                    self.accounting["expected_protocol_errors"] += 1
                    if response.get("error") == "gap":
                        # documented recovery: continue at the high-water
                        high = response.get("high_watermark")
                        if isinstance(high, int):
                            self.stream_last_seq[stream] = high
                        await asyncio.sleep(self.shape["resume_cycle_s"])
                        continue
                    await asyncio.sleep(1.0)
                    continue
                self.target = stream
                await self._consume(client, stop)
            except (ConnectionError, OSError, asyncio.TimeoutError,
                    json.JSONDecodeError):
                self.accounting["op_timeouts"] += 1
            finally:
                await client.close()
            await asyncio.sleep(self.shape["resume_cycle_s"])


class ThrottledConsumer:
    """Subscribe-all with a slow drain and periodic stalls (queue peaks).

    Drain capacity (~80 frames/s in 32-frame batches) exceeds the ~30
    events/s inflow, so the bounded queue only grows during the deliberate
    stalls (3 s -> ~90 events, under the 256-event design bound). A reset
    (overflow) is handled as a recovery: reconnect and resubscribe.
    """

    def __init__(self, socket_path, label, accounting, shape):
        self.socket_path = socket_path
        self.label = label
        self.accounting = accounting
        self.shape = shape
        self.client_map: dict = {}
        self.events = 0

    async def _read_frames(self, client):
        """Read one batch of up to 32 frames; False on reset/disconnect."""
        for _ in range(32):
            try:
                line = await asyncio.wait_for(client.reader.readline(), 0.05)
            except asyncio.TimeoutError:
                return True
            if not line:
                return False
            self.accounting["frames_received"] += 1
            frame = json.loads(line.decode())
            kind = frame.get("type")
            if kind == "event":
                self.events += 1
                _track_event_frame(frame, self.client_map, self.accounting)
            elif kind == "gap":
                _apply_gap_frame(frame, self.client_map, self.accounting)
            elif kind == "reset_required":
                self.accounting["resets_received"] += 1
                return False
            elif kind == "heartbeat":
                self.accounting["heartbeats_received"] += 1
        return True

    async def run(self, stop: asyncio.Event):
        while not stop.is_set():
            client = WireClient(self.socket_path, self.label, self.accounting)
            try:
                await client.connect()
                hello = await client.hello()
                response = await client.subscribe_all(
                    hello["initial_snapshot_cursor"])
                if not response.get("ok"):
                    self.accounting["expected_protocol_errors"] += 1
                    await asyncio.sleep(2.0)
                    continue
                last_stall = time.monotonic()
                while not stop.is_set():
                    if time.monotonic() - last_stall \
                            > self.shape["reader_stall_every_s"]:
                        last_stall = time.monotonic()
                        stall_started = time.monotonic()
                        await asyncio.sleep(self.shape["reader_stall_s"])
                        # v2 formula input: the ACTUAL stall duration
                        self.accounting["stall_s_observed_max"] = max(
                            self.accounting["stall_s_observed_max"],
                            time.monotonic() - stall_started)
                    if not await self._read_frames(client):
                        break  # reset/disconnect -> recovery reconnect
                    await asyncio.sleep(0.4)
            except (ConnectionError, OSError, json.JSONDecodeError,
                    asyncio.TimeoutError):
                self.accounting["op_timeouts"] += 1
                await asyncio.sleep(1.0)
            finally:
                await client.close()


class ChurnConsumer:
    """UI-refresh churn: full paged snapshot + events, then abrupt close."""

    def __init__(self, socket_path, label, accounting, phase=0.0):
        self.socket_path = socket_path
        self.label = label
        self.accounting = accounting
        self.phase = phase
        self.stale_cursor = None
        self.cycles = 0

    async def _page_all(self, client, cursor, op, max_pages=400):
        pages, page = 0, None
        while pages < max_pages:
            response = await client.op(
                {"op": op, "v": 1, "snapshot_cursor": cursor,
                 "page_cursor": page})
            if not response.get("ok"):
                self.accounting["expected_protocol_errors"] += 1
                return pages
            pages += 1
            page = response.get("next_page_cursor")
            if response.get("done") or page is None:
                return pages
        return pages

    async def run(self, stop: asyncio.Event):
        cycle_s = WORKLOAD_SHAPE["churn_cycle_s"]
        await asyncio.sleep(self.phase)
        while not stop.is_set():
            client = WireClient(self.socket_path, self.label, self.accounting)
            try:
                await client.connect()
                hello = await client.hello()
                cursor = hello["initial_snapshot_cursor"]
                if self.cycles % 3 == 2 and self.stale_cursor is not None:
                    # deliberate stale-cursor read: the documented
                    # cursor_expired / cursor_superseded error path
                    response = await client.op(
                        {"op": "snapshot", "v": 1,
                         "snapshot_cursor": self.stale_cursor,
                         "page_cursor": None})
                    if not response.get("ok"):
                        self.accounting["expected_protocol_errors"] += 1
                self.stale_cursor = cursor
                await self._page_all(client, cursor, "snapshot")
                await self._page_all(client, cursor, "snapshot-events")
                self.cycles += 1
            except (ConnectionError, OSError, asyncio.TimeoutError,
                    json.JSONDecodeError, KeyError):
                self.accounting["op_timeouts"] += 1
            finally:
                await client.close()  # abrupt: no unsubscribe op exists
            await asyncio.sleep(cycle_s)


# ---------------------------------------------------------------------------
# Process-tree / resource sampling
# ---------------------------------------------------------------------------
def snapshot_process_tree(main_pid: int) -> dict:
    """Main RSS + full descendant tree via one ps call (macOS/Linux).

    ``ru_maxrss_raw`` is ru_maxrss as reported by the OS (bytes on macOS,
    KiB on Linux) — recorded verbatim, never unit-converted.
    """
    result = {"rss_main_kb": None, "ru_maxrss_raw": None,
              "children": [], "children_rss_sum_kb": 0}
    result["ru_maxrss_raw"] = resource.getrusage(
        resource.RUSAGE_SELF).ru_maxrss
    try:
        output = subprocess.run(
            ["ps", "-axo", "pid=,ppid=,rss=,command="],
            capture_output=True, text=True, timeout=20).stdout
    except (OSError, subprocess.SubprocessError):
        return result
    parents: dict[int, int] = {}
    rss_kb: dict[int, int] = {}
    commands: dict[int, str] = {}
    for line in output.splitlines():
        parts = line.split(None, 3)
        if len(parts) < 3 or not parts[0].isdigit():
            continue
        pid, ppid, rss = int(parts[0]), int(parts[1]), int(parts[2])
        parents[pid] = ppid
        rss_kb[pid] = rss
        commands[pid] = parts[3] if len(parts) > 3 else ""
    if main_pid in rss_kb:
        result["rss_main_kb"] = rss_kb[main_pid]
    frontier, seen = [main_pid], {main_pid}
    while frontier:
        nxt = []
        for pid in frontier:
            for child, parent in parents.items():
                if parent == pid and child not in seen:
                    seen.add(child)
                    nxt.append(child)
                    result["children"].append(
                        {"pid": child, "ppid": parent,
                         "rss_kb": rss_kb.get(child, 0),
                         "command": commands.get(child, "")[:120]})
        frontier = nxt
    result["children_rss_sum_kb"] = sum(c["rss_kb"] for c in
                                        result["children"])
    return result


# ---------------------------------------------------------------------------
# Evidence equivalence: same-cut snapshot states vs event-page fold
# ---------------------------------------------------------------------------
def journal_selfcheck_equivalence(journal) -> dict:
    """Same-cut equivalence: state pages vs fold of ALL event pages.

    A mid-pagination retention slide answers ``reset_required`` BY DESIGN;
    the check retries on a fresh cut (bounded to 3). Any state/fold
    mismatch is a real divergence.
    """
    for _ in range(3):
        hello = journal.hello()
        cursor = hello["initial_snapshot_cursor"]
        states, page, problem = [], None, None
        while True:
            response = journal.snapshot(cursor, page, client="soak-audit")
            if not response.get("ok"):
                problem = response.get("error")
                break
            states.extend(response["states"])
            page = response.get("next_page_cursor")
            if response.get("done") or page is None:
                break
        if problem is not None:
            if problem in ("reset_required", "cursor_expired"):
                continue
            return {"ok": False, "error": problem}
        grouped: dict[str, list] = {}
        epage, events_count = None, 0
        while True:
            response = journal.snapshot_events(cursor, epage,
                                               client="soak-audit")
            if not response.get("ok"):
                problem = response.get("error")
                break
            for event in response["events"]:
                events_count += 1
                grouped.setdefault(event["stream_id"], []).append(event)
            epage = response.get("next_page_cursor")
            if response.get("done") or epage is None:
                break
        if problem is not None:
            if problem in ("reset_required", "cursor_expired"):
                continue
            return {"ok": False, "error": problem}
        mismatched_streams = []
        state_by_task = {state["task_id"]: state for state in states}
        for stream, events in grouped.items():
            if stream == ui_events.HUB_STREAM_ID:
                continue  # hub events fold but are excluded from state
                # pages by design (stream announcements / process exits)
            folded = ui_journal.reduce_stream(events)
            if folded is None:
                continue
            if state_by_task.get(stream) != folded:
                if len(mismatched_streams) < 5:
                    mismatched_streams.append(stream)
        # every non-hub state must also have a folded counterpart
        for task in state_by_task:
            if task != ui_events.HUB_STREAM_ID \
                    and task not in grouped \
                    and len(mismatched_streams) < 5:
                mismatched_streams.append(task)
        return {"ok": len(mismatched_streams) == 0,
                "states": len(states), "events": events_count,
                "streams_folded": len(grouped),
                "mismatched_streams": mismatched_streams}
    return {"ok": False, "error": "reset_required after retries"}


# ---------------------------------------------------------------------------
# Window recorder
# ---------------------------------------------------------------------------
class WindowRecorder:
    """10 Hz lag probe + window boundary records (repair2: queue peaks are
    EXACT per-event high-water marks collected from the SubscriptionMonitor
    at each window close — the 1 Hz sampler is gone)."""

    def __init__(self, server, journal, accounting, window_s: float,
                 queue_monitor: SubscriptionMonitor):
        self.server = server
        self.journal = journal
        self.accounting = accounting
        self.window_s = window_s
        self.queue_monitor = queue_monitor
        self.windows: list[dict] = []
        self._lag_ms: list[float] = []
        self._frames_sent_marker = 0
        self._events_marker = 0
        self._reconnects_marker = 0
        self._resets_marker = 0
        self._heartbeats_marker = 0
        self._receipts_marker = 0
        self._emitter_failure_marker = 0
        self._heap_global_peak = 0

    async def lag_probe(self, stop: asyncio.Event):
        loop = asyncio.get_running_loop()
        while not stop.is_set():
            start = loop.time()
            await asyncio.sleep(0.1)
            self._lag_ms.append(max(0.0, (loop.time() - start - 0.1) * 1000))

    def close_window(self, index, window_elapsed, receipts_state,
                     selfcheck) -> dict:
        tree = snapshot_process_tree(os.getpid())
        heap_current, heap_peak = tracemalloc.get_traced_memory()
        self._heap_global_peak = max(self._heap_global_peak, heap_peak)
        stats = self.journal.stats()
        frames = sum(c.frames_sent for c in self.server.connections)
        accounting = self.accounting
        queue_peaks = self.queue_monitor.window_peaks()
        window = {
            "index": index,
            "window_s": round(window_elapsed, 3),
            "wall_utc": utc_now_iso(),
            "rss_main_kb": tree["rss_main_kb"],
            "ru_maxrss_raw": tree["ru_maxrss_raw"],
            "children_rss_sum_kb": tree["children_rss_sum_kb"],
            "process_tree": tree["children"],
            "heap_current_bytes": heap_current,
            "heap_window_peak_bytes": heap_peak,
            "heap_global_peak_bytes": self._heap_global_peak,
            "lag_samples": len(self._lag_ms),
            "lag_p50_ms": round(median(self._lag_ms), 3)
            if self._lag_ms else None,
            "lag_p95_ms": round(percentile(self._lag_ms, 95), 3)
            if self._lag_ms else None,
            "lag_max_ms": round(max(self._lag_ms), 3)
            if self._lag_ms else None,
            "queue_peak_events": queue_peaks["events"],
            "queue_peak_bytes": queue_peaks["bytes"],
            "queue_hwm_instrument": "exact-per-event",
            "connections": self.server.stats()["connections"],
            "journal_total_events": stats["total_events"],
            "journal_total_bytes": stats["total_bytes"],
            "journal_streams": stats["streams"],
            "journal_cursors": stats["cursors"],
            "journal_subscriptions": stats["subscriptions"],
            "journal_dropped_events": stats["dropped_events"],
            "events_emitted": accounting["events_emitted"]
            - self._events_marker,
            "events_per_second": round(
                (accounting["events_emitted"] - self._events_marker)
                / max(window_elapsed, 1e-9), 3),
            "emit_failures": accounting["emit_failures"]
            - self._emitter_failure_marker,
            "frames_sent": frames - self._frames_sent_marker,
            "reconnects": accounting["reconnects"] - self._reconnects_marker,
            "resets_received": accounting["resets_received"]
            - self._resets_marker,
            "heartbeats_received": accounting["heartbeats_received"]
            - self._heartbeats_marker,
            "gap_frames_total": accounting["gap_frames"],
            "expected_protocol_errors_total":
                accounting["expected_protocol_errors"],
            "op_timeouts_total": accounting["op_timeouts"],
            "resume_violations_total": accounting["resume_violations"],
            "client_loss_violations_total":
                accounting["client_loss_violations"],
            "cross_stream_frames_total": accounting["cross_stream_frames"],
            "selfcheck": selfcheck,
            "receipts": receipts_state,
            "events_emitted_total": accounting["events_emitted"],
            "reconnects_total": accounting["reconnects"],
            "resets_total": accounting["resets_received"],
            "receipts_completed": receipts_state["completed_total"]
            - self._receipts_marker,
        }
        self._events_marker = accounting["events_emitted"]
        self._reconnects_marker = accounting["reconnects"]
        self._resets_marker = accounting["resets_received"]
        self._heartbeats_marker = accounting["heartbeats_received"]
        self._frames_sent_marker = frames
        self._receipts_marker = receipts_state["completed_total"]
        self._emitter_failure_marker = accounting["emit_failures"]
        self._lag_ms = []
        tracemalloc.reset_peak()
        self.windows.append(window)
        return window


# ---------------------------------------------------------------------------
# Receipt pump: real run_readonly over a fake child (itemised split)
# ---------------------------------------------------------------------------
class ReceiptPump:
    def __init__(self, out_dir: Path, emitter, shape):
        self.out_dir = out_dir
        self.emitter = emitter
        self.shape = shape
        self.fake_bin = out_dir / "fake-codex"
        self.fake_bin.write_text(FAKE_CODEX)
        self.fake_bin.chmod(0o755)
        self.workspace = out_dir / "workspace"
        self.workspace.mkdir(exist_ok=True)
        self.records: list[dict] = []
        self.completed_total = 0
        self.pump_errors = 0
        self.raw_stdout_bytes_total = 0
        self.raw_stderr_bytes_total = 0
        self.stdout_lines_total = 0
        self.exit_sink = ui_projection.make_exit_sink(
            self.emitter, "cold", "codex")

    def _run_one(self, index: int) -> None:
        mode = FAKE_MODES[index % len(FAKE_MODES)]
        evidence = self.out_dir / "receipts" / f"run-{index:04d}"
        metrics: dict = {}  # run_readonly calls metrics_sink.update(payload)
        receipt = codex_driver.run_readonly(
            str(self.fake_bin), self.workspace,
            f"soak fake task {index}",
            {"type": "object",
             "properties": {"summary": {"type": "string"},
                            "files": {"type": "array",
                                      "items": {"type": "string"}}},
             "required": ["summary", "files"],
             "additionalProperties": False},
            evidence, timeout_s=60.0,
            env={"SOAK_FAKE_CODEX_MODE": mode},
            exit_event_sink=self.exit_sink,
            metrics_sink=metrics)
        meta = metrics.get("receipt_meta") or {}
        record = {
            "index": index, "mode": mode, "outcome": receipt.outcome,
            "ok": receipt.ok, "exit_code": receipt.exit_code,
            "raw_stdout_bytes": metrics.get("raw_stdout_bytes", 0),
            "raw_stderr_bytes": metrics.get("raw_stderr_bytes", 0),
            "event_counts": meta.get("event_counts") or {},
            "command_items": meta.get("command_items") or [],
            "hard_errors": meta.get("hard_errors") or [],
            "transport_retries": meta.get("transport_retries") or 0,
        }
        self.records.append(record)
        self.completed_total += 1
        self.raw_stdout_bytes_total += record["raw_stdout_bytes"]
        self.raw_stderr_bytes_total += record["raw_stderr_bytes"]
        self.stdout_lines_total += sum(record["event_counts"].values())

    def state(self) -> dict:
        """Receipt/raw-buffer itemisation for the window record.

        In-memory (RSS-attribution) split = deep size of the retained
        receipt metadata and its components. The raw stdout/stderr buffers
        are DISK-streamed by the driver (P2.3 semantics): itemised in
        bytes/lines above, not resident in this process.
        """
        return {
            "completed_total": self.completed_total,
            "pump_errors": self.pump_errors,
            "raw_stdout_bytes_total": self.raw_stdout_bytes_total,
            "raw_stderr_bytes_total": self.raw_stderr_bytes_total,
            "stdout_lines_total": self.stdout_lines_total,
            "event_count_entries": sum(len(r["event_counts"])
                                       for r in self.records),
            "event_count_events": sum(sum(r["event_counts"].values())
                                      for r in self.records),
            "command_items_count": sum(len(r["command_items"])
                                       for r in self.records),
            "hard_errors_count": sum(len(r["hard_errors"])
                                     for r in self.records),
            "transport_retries_total": sum(r["transport_retries"]
                                           for r in self.records),
            "retained_deep_bytes": deep_size(self.records),
            "event_counts_deep_bytes": deep_size(
                [r["event_counts"] for r in self.records]),
            "command_items_deep_bytes": deep_size(
                [r["command_items"] for r in self.records]),
            "hard_errors_deep_bytes": deep_size(
                [r["hard_errors"] for r in self.records]),
        }

    async def run(self, stop: asyncio.Event):
        loop = asyncio.get_running_loop()
        index = 0
        while not stop.is_set():
            try:
                await asyncio.wait_for(
                    loop.run_in_executor(None, self._run_one, index), 90.0)
            except asyncio.TimeoutError:
                self.pump_errors += 1  # honest: reported, never hidden
            except Exception:
                self.pump_errors += 1
            index += 1
            await asyncio.sleep(self.shape["receipt_interval_s"])


# ---------------------------------------------------------------------------
# Main workload driver
# ---------------------------------------------------------------------------
async def run_workload(role: str, duration_s: float, window_s: float,
                       out_dir: Path, frozen: dict | None) -> dict:
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    accounting = new_accounting()
    tracemalloc.start(1)
    state_dir = Path(tempfile.mkdtemp(prefix="soak-ui-"))
    state_dir.chmod(0o700)

    emitter = ui_projection.UiEmitter(enabled=True)
    journal = ui_journal.UiJournal(emitter)
    queue_monitor = SubscriptionMonitor()
    queue_monitor.install(journal)  # before the first subscription exists
    server = ui_server_mod.UiEventServer(
        journal, state_dir=state_dir,
        heartbeat_s=WORKLOAD_SHAPE["heartbeat_s"])
    await server.start()
    producers = ProducerSessions(emitter, WORKLOAD_SHAPE, accounting)
    receipts = ReceiptPump(out_dir, emitter, WORKLOAD_SHAPE)
    recorder = WindowRecorder(server, journal, accounting, window_s,
                              queue_monitor)
    suball_consumers: list[SubscribeAllConsumer] = []

    if frozen is not None and not freeze_precedes_soak(
            frozen, utc_now_iso()):
        raise ValueError("frozen_at does not precede the soak start "
                         "(audit invariant violated; refusing to run)")

    started_wall = utc_now_iso()
    started_mono = time.monotonic()
    stop = asyncio.Event()
    tasks: list[asyncio.Task] = []
    interrupted: dict | None = None
    try:
        for index in range(WORKLOAD_SHAPE["sessions"]):
            tasks.append(asyncio.create_task(
                producers.run_session(index, stop)))
        socket_path = str(server.socket_path)
        consumers = WORKLOAD_SHAPE["consumers"]
        for i in range(consumers["subscribe_all"]):
            consumer = SubscribeAllConsumer(
                socket_path, f"suball-{'AB'[i]}", accounting)
            suball_consumers.append(consumer)
            tasks.append(asyncio.create_task(consumer.run(stop)))
        tasks.append(asyncio.create_task(
            ResumeConsumer(socket_path, "resume-C", accounting, producers,
                           WORKLOAD_SHAPE).run(stop)))
        tasks.append(asyncio.create_task(
            ThrottledConsumer(socket_path, "slow-D", accounting,
                              WORKLOAD_SHAPE).run(stop)))
        for i in range(consumers["churn_refresh"]):
            tasks.append(asyncio.create_task(
                ChurnConsumer(socket_path, f"churn-{'EF'[i]}", accounting,
                              phase=i
                              * (WORKLOAD_SHAPE["churn_cycle_s"] / 2)
                              ).run(stop)))
        tasks.append(asyncio.create_task(recorder.lag_probe(stop)))
        tasks.append(asyncio.create_task(receipts.run(stop)))

        window_index = 0
        next_boundary = started_mono + window_s
        while True:
            elapsed = time.monotonic() - started_mono
            if elapsed >= duration_s:
                break
            wait_s = min(next_boundary - time.monotonic(),
                         duration_s - elapsed)
            await asyncio.sleep(max(wait_s, 0.0))
            if time.monotonic() >= next_boundary:
                recorder.close_window(
                    window_index, window_s, receipts.state(),
                    journal_selfcheck_equivalence(journal))
                window_index += 1
                next_boundary += window_s

        stop.set()
        await asyncio.sleep(2.0)  # settle: drain queues for final catch-up
        tail_elapsed = time.monotonic() - (next_boundary - window_s)
        if tail_elapsed > 1.0:
            tail = recorder.close_window(
                window_index, tail_elapsed, receipts.state(),
                journal_selfcheck_equivalence(journal))
            tail["partial"] = True
            tail["closed_by"] = "shutdown"
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    except Exception as error:
        interrupted = {"error": f"{type(error).__name__}: {error}"}
        stop.set()
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
    finally:
        await server.close()
        journal.detach()
        shutil.rmtree(state_dir, ignore_errors=True)
    ended_wall = utc_now_iso()
    ended_mono = time.monotonic()

    # final catch-up: every mapped stream of the persistent subscribe-all
    # readers must equal the journal high-water (or the client was reset)
    catchup = {"checked": 0, "equal": 0, "reset_clients": [],
               "examples_unequal": []}
    for consumer in suball_consumers:
        if consumer.closed_by_reset:
            catchup["reset_clients"].append(consumer.label)
            continue
        for stream, expected_next in list(consumer.client_map.items())[:800]:
            catchup["checked"] += 1
            if expected_next == journal.high_water(stream) + 1:
                catchup["equal"] += 1
            elif len(catchup["examples_unequal"]) < 5:
                catchup["examples_unequal"].append(
                    {"stream": stream, "expected_next": expected_next,
                     "journal_high_water": journal.high_water(stream)})

    def _selfcheck_ok(window):
        check = window.get("selfcheck") or {}
        return bool(check.get("ok")) \
            and not check.get("mismatched_streams")

    totals = {
        "events_emitted": accounting["events_emitted"],
        "emit_failures": accounting["emit_failures"],
        "planned_tasks": len(accounting["planned_tasks"]),
        "frames_received": accounting["frames_received"],
        "reconnects": accounting["reconnects"],
        "resets_received": accounting["resets_received"],
        "gap_frames": accounting["gap_frames"],
        "heartbeats_received": accounting["heartbeats_received"],
        "expected_protocol_errors": accounting["expected_protocol_errors"],
        "op_timeouts": accounting["op_timeouts"],
        "resume_violations": accounting["resume_violations"],
        "client_loss_violations": accounting["client_loss_violations"],
        "cross_stream_frames": accounting["cross_stream_frames"],
        "evidence_mismatch": sum(1 for w in recorder.windows
                                 if not _selfcheck_ok(w)),
        "selfcheck_failed_windows": sum(
            1 for w in recorder.windows if not _selfcheck_ok(w)),
        "receipts_completed": receipts.completed_total,
        "receipt_pump_errors": receipts.pump_errors,
        "emitter_failures": len(emitter.failures),
    }
    evidence_ok = (totals["evidence_mismatch"] == 0
                   and totals["resume_violations"] == 0
                   and totals["client_loss_violations"] == 0
                   and totals["emit_failures"] == 0
                   and totals["emitter_failures"] == 0)
    summary = {
        "role": role,
        "model_requests": 0,
        "started_at": started_wall,
        "ended_at": ended_wall,
        "duration_s": round(ended_mono - started_mono, 3),
        "requested_duration_s": duration_s,
        "window_s": window_s,
        "windows_recorded": len(recorder.windows),
        "full_windows": sum(1 for w in recorder.windows
                            if not w.get("partial")),
        "partial_windows": sum(1 for w in recorder.windows
                               if w.get("partial")),
        "interrupted": interrupted,
        "evidence_ok": evidence_ok,
        "workload_shape": WORKLOAD_SHAPE,
        "totals": totals,
        "final_catchup": catchup,
        "baseline_frozen_at": (frozen or {}).get("frozen_at"),
        "freeze_before_soak": (
            freeze_precedes_soak(frozen, started_wall)
            if frozen is not None else None),
    }
    samples = {
        "role": role,
        "started_at": started_wall,
        "ended_at": ended_wall,
        "duration_s": summary["duration_s"],
        "window_s": window_s,
        "queue_hwm_instrument": "exact-per-event (repair2)",
        "queue_frame_size_stats": {
            "frames_enqueued": queue_monitor.frames_enqueued,
            "bytes_enqueued": queue_monitor.bytes_enqueued,
            "mean_frame_bytes": round(queue_monitor.mean_frame_bytes(), 3),
        },
        "stall_s_observed_max": round(
            accounting["stall_s_observed_max"], 3),
        "windows": recorder.windows,
        "totals": totals,
        "final_catchup": catchup,
        "emitter_failures": emitter.failures[:50],
    }
    samples_file = out_dir / f"samples-{role}.json"
    atomic_write_json(samples_file, samples)
    summary["samples_file"] = str(samples_file)
    atomic_write_json(out_dir / f"summary-{role}.json", summary)
    return summary


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------
def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="P3.2 soak harness (fake producers/consumers, "
                    "model_requests=0)")
    sub = parser.add_subparsers(dest="command", required=True)

    run_parser = sub.add_parser("run", help="run the soak workload")
    run_parser.add_argument("--role", choices=("baseline", "soak"),
                            required=True)
    run_parser.add_argument("--duration", type=float, required=True,
                            help="seconds (baseline >= 300, soak >= 1800)")
    run_parser.add_argument("--window", type=float, default=WINDOW_S_DEFAULT)
    run_parser.add_argument("--out", type=Path, required=True)
    run_parser.add_argument("--frozen", type=Path, default=None,
                            help="frozen thresholds (required for soak)")

    freeze_parser = sub.add_parser(
        "freeze", help="freeze thresholds from the baseline dry-run")
    freeze_parser.add_argument("--baseline", type=Path, required=True,
                               help="baseline summary-baseline.json")
    freeze_parser.add_argument("--out", type=Path, required=True)
    freeze_parser.add_argument("--v1-frozen", type=Path, default=None,
                               dest="v1_frozen",
                               help="produce the repair2 v2 document: 16 "
                                    "gates inherited from this v1 file + "
                                    "queue gates from the mechanism formula")
    args = parser.parse_args(argv)

    if args.command == "run":
        if args.role == "baseline" and args.duration < 300:
            parser.error("--duration for baseline must be >= 300 s")
        if args.role == "soak" and args.duration < 1800:
            parser.error("--duration for soak must be >= 1800 s")
        frozen = None
        if args.role == "soak":
            if args.frozen is None:
                parser.error("soak requires --frozen baseline-frozen.json")
            frozen = load_frozen(args.frozen)
        summary = asyncio.run(run_workload(
            args.role, args.duration, args.window, args.out, frozen))
        ok = (summary["interrupted"] is None
              and summary["model_requests"] == 0
              and summary["duration_s"] >= args.duration - 1.0
              and summary["evidence_ok"])
        if args.role == "soak":
            frozen_doc = load_frozen(args.frozen)
            samples = json.loads(Path(summary["samples_file"]).read_text(
                encoding="utf-8"))
            audit = {
                "baseline_frozen_at": frozen_doc["frozen_at"],
                "frozen_schema": frozen_doc["schema"],
                "soak_started_at": summary["started_at"],
                "freeze_before_soak": freeze_precedes_soak(
                    frozen_doc, summary["started_at"]),
                "frozen_sha256": sha256_file(args.frozen),
                "baseline_started_at": frozen_doc["baseline"]["started_at"],
            }
            atomic_write_json(Path(args.out) / "audit.json", audit)
            gates = evaluate_gates(samples, frozen_doc)
            atomic_write_json(Path(args.out) / "gates.json", gates)
            atomic_write_json(Path(args.out) / "performance.json",
                              performance_report(samples))
            ok = ok and audit["freeze_before_soak"] and gates["all_pass"]
        return 0 if ok else 2

    # freeze (v1 by default; --v1-frozen produces the repair2 v2 document:
    # 16 gates inherited verbatim + the two queue gates from the mechanism
    # formula measured in THIS baseline run)
    baseline = json.loads(args.baseline.read_text(encoding="utf-8"))
    if baseline.get("role") != "baseline":
        print("refusing: --baseline is not a baseline run", file=sys.stderr)
        return 2
    if baseline.get("interrupted") is not None:
        print("refusing: baseline run was interrupted", file=sys.stderr)
        return 2
    if not baseline.get("evidence_ok"):
        print("refusing: baseline run had evidence violations",
              file=sys.stderr)
        return 2
    samples_path = Path(baseline["samples_file"])
    samples = json.loads(samples_path.read_text(encoding="utf-8"))
    merged = dict(baseline)
    merged["windows"] = samples["windows"]
    if args.v1_frozen is not None:
        v1_frozen = load_frozen(args.v1_frozen)
        if v1_frozen.get("schema") != 1:
            print("refusing: --v1-frozen must be a schema-1 (v1) document",
                  file=sys.stderr)
            return 2
        merged["stall_s_observed_max"] = samples.get("stall_s_observed_max")
        merged["queue_frame_size_stats"] = samples.get(
            "queue_frame_size_stats")
        v2 = compute_thresholds_v2(merged, v1_frozen)
        frozen = {
            "schema": 2,
            "kind": "recalibrated-queue-gates",
            "frozen_at": utc_now_iso(),
            "inherited_from": {
                "file": str(args.v1_frozen),
                "sha256": sha256_file(args.v1_frozen),
                "frozen_at": v1_frozen["frozen_at"],
                "baseline_started_at":
                    v1_frozen["baseline"]["started_at"],
            },
            "baseline": {
                "started_at": baseline["started_at"],
                "ended_at": baseline["ended_at"],
                "duration_s": baseline["duration_s"],
                "windows": baseline["windows_recorded"],
                "samples_file": str(samples_path),
                "samples_sha256": sha256_file(samples_path),
            },
            "workload_shape": WORKLOAD_SHAPE,
            "thresholds": v2["thresholds"],
            "inherited_gates": v2["inherited_gates"],
            "queue_gate_formula": v2["formula"],
        }
    else:
        thresholds = compute_thresholds(merged)
        frozen = {
            "schema": 1,
            "frozen_at": utc_now_iso(),
            "baseline": {
                "started_at": baseline["started_at"],
                "ended_at": baseline["ended_at"],
                "duration_s": baseline["duration_s"],
                "windows": baseline["windows_recorded"],
                "samples_file": str(samples_path),
                "samples_sha256": sha256_file(samples_path),
            },
            "workload_shape": WORKLOAD_SHAPE,
            "thresholds": thresholds,
        }
    problems = validate_frozen(frozen)
    if problems:
        print("frozen document invalid: " + "; ".join(problems),
              file=sys.stderr)
        return 2
    atomic_write_json(args.out, frozen)
    print(json.dumps({"ok": True, "frozen_at": frozen["frozen_at"],
                      "schema": frozen["schema"],
                      "out": str(args.out)}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
