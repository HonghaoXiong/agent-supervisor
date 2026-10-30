"""Minimal read-only Codex CLI driver.

Single-shot task dispatch through the official non-interactive entry point
``codex exec --json``:

* prompts travel over stdin (trailing ``-`` argv token), never over argv;
* the sandbox is pinned to ``-s read-only`` and ``--ephemeral`` is never
  passed, so every run leaves a rollout file under ``$CODEX_HOME`` for
  post-hoc inspection by the decision layer;
* the JSONL event stream is persisted verbatim as evidence before parsing,
  unknown event types are counted, never rejected;
* the final agent reply is recaptured through ``--output-schema`` plus
  ``-o <file>`` and validated against the caller-supplied schema's
  ``required`` keys.

WP-C receipt metadata governance (PLAN §9.8 contract): the receipt's
``event_counts`` / ``command_items`` / ``hard_errors`` are capped at
``EVENT_COUNTS_ENTRY_CAP`` / ``COMMAND_ITEMS_ENTRY_CAP`` /
``HARD_ERRORS_ENTRY_CAP`` entries (256 / 64 / 32); over-cap truncation is
keep-first and loss-visible via ``<name>_truncated`` / ``<name>_dropped``
marker keys that appear in the persisted receipt ONLY on truncation, so
under-threshold receipts stay byte-identical to the pre-governance shape.

Public surface: :class:`CodexExecError`, :class:`CodexRunReceipt`,
:func:`build_argv`, :func:`normalize_strict_schema`, :func:`run_readonly`,
:func:`locate_rollout`, :func:`summarize_rollout`.
"""

from __future__ import annotations

import contextlib
import copy
import dataclasses
import itertools
import json
import os
import re
import subprocess
import threading
import time
from collections import Counter
from pathlib import Path

from .splitlines_stream import eof_segments, scan_new
from .worker_health import ExitProjection

SANDBOX_MODE = "read-only"

# P0.4 round 2: cold child processes get an independent generation stamp so
# each real awaited exit projects exactly one UI process.exited
# ({process_scope, generation} is the emission key; PLAN §4.3).
_COLD_GENERATIONS = itertools.count(1)

SCHEMA_FILENAME = "output-schema.json"
LAST_MESSAGE_FILENAME = "last-message.json"
EVENTS_FILENAME = "codex-events.jsonl"
RECEIPT_FILENAME = "receipt.json"

_THREAD_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]*$")
_INDEX_ID_FIELDS = ("id", "session_id", "thread_id")
_INDEX_PATH_FIELDS = ("path", "rollout_path", "file")
_ITEM_TEXT_LIMIT = 200
_STDERR_EXCERPT_LIMIT = 500
_HARD_ERROR_LIMIT = 300
_TERMINATE_GRACE_S = 5.0
_COMBINATOR_KEYS = ("anyOf", "oneOf", "allOf")
_DEFINITION_KEYS = ("$defs", "definitions")

# WP-C receipt metadata governance (the PLAN §9.8 "separate contract"):
# per-field ENTRY caps on the output-driven receipt metadata that P2.3's
# raw-buffer bounding deliberately did NOT cover. Over-cap fields are
# truncated keep-first (arrival order) and the loss is made explicit —
# the receipt gains ``<name>_truncated: true`` plus
# ``<name>_dropped: <int>`` (source events not retained: for the lists one
# dropped entry == one dropped event; for ``event_counts`` the count of
# occurrences whose type key is no longer retained — an exact dropped-KEY
# count would need an unbounded distinct-type set, defeating the cap).
# Caps sized from the P3.2 soak distribution (92 tasks / 370 total
# event_counts entries ≈ 4 entries per task; command_items ≈ 1/task;
# hard_errors ≈ 0-3/task) with a wide safety margin; no env switch —
# rollback is a code revert. Under-threshold receipts are byte-identical
# to the pre-governance shape (markers appear ONLY on truncation).
EVENT_COUNTS_ENTRY_CAP = 256
COMMAND_ITEMS_ENTRY_CAP = 64
HARD_ERRORS_ENTRY_CAP = 32
_RECEIPT_MARKER_KEYS = (
    "event_counts_truncated", "event_counts_dropped",
    "command_items_truncated", "command_items_dropped",
    "hard_errors_truncated", "hard_errors_dropped",
)


class CodexExecError(Exception):
    """Driver misuse (bad arguments); never raised for a codex run outcome."""


@dataclasses.dataclass(frozen=True)
class CodexRunReceipt:
    """Outcome evidence for one ``codex exec`` run."""

    ok: bool
    outcome: str  # "delivered" | "timeout" | "failed" | "schema_mismatch"
    thread_id: str | None  # from the thread.started event
    exit_code: int | None
    events_path: str
    last_message_path: str
    structured: dict | None  # -o file JSON validated against output_schema
    event_counts: dict  # {type: n}, unknown types included
    command_items: list  # [{command, exit_code, status}] best-effort
    model_observed: str | None
    sandbox: str  # fixed "read-only"
    duration_s: float
    error: str | None
    transport_retries: int = 0  # "Reconnecting..." events: transport layer, not failures
    hard_errors: list = dataclasses.field(default_factory=list)  # other error-event messages
    schema_normalized: bool = False  # strict normalization changed the schema
    # WP-C governance markers: present in the persisted receipt ONLY when
    # the same-named field was truncated (falsy markers are stripped at
    # write time, so under-threshold receipts stay byte-identical).
    event_counts_truncated: bool = False
    event_counts_dropped: int = 0
    command_items_truncated: bool = False
    command_items_dropped: int = 0
    hard_errors_truncated: bool = False
    hard_errors_dropped: int = 0


def normalize_strict_schema(schema: dict) -> dict:
    """Return a strict-normalized deep copy of a JSON Schema.

    OpenAI's structured-output endpoint rejects object nodes that omit
    ``"additionalProperties": false`` with ``invalid_json_schema``, so every
    ``type == "object"`` node gets ``additionalProperties`` forced to False
    and ``required`` widened to the full property-key list — keys already
    listed in ``required`` keep their relative position, the remaining
    property keys follow in declaration order. Recursion covers property
    values, ``items``, the anyOf/oneOf/allOf combinators and
    $defs/definitions. Non-dict inputs are returned unchanged.
    """
    normalized = copy.deepcopy(schema)
    _normalize_schema_objects(normalized)
    return normalized


def _normalize_schema_objects(node) -> None:
    if not isinstance(node, dict):
        return
    if node.get("type") == "object":
        node["additionalProperties"] = False
        properties = node.get("properties")
        if isinstance(properties, dict):
            previous = node.get("required")
            required = ([key for key in previous
                         if isinstance(key, str) and key in properties]
                        if isinstance(previous, list) else [])
            seen = set(required)
            required.extend(key for key in properties if key not in seen)
            node["required"] = required
    properties = node.get("properties")
    if isinstance(properties, dict):
        for value in properties.values():
            _normalize_schema_objects(value)
    items = node.get("items")
    if isinstance(items, list):
        for value in items:
            _normalize_schema_objects(value)
    elif isinstance(items, dict):
        _normalize_schema_objects(items)
    for key in _COMBINATOR_KEYS:
        variants = node.get(key)
        if isinstance(variants, list):
            for value in variants:
                _normalize_schema_objects(value)
    for key in _DEFINITION_KEYS:
        definitions = node.get(key)
        if isinstance(definitions, dict):
            for value in definitions.values():
                _normalize_schema_objects(value)


def build_argv(
    codex_path,
    workspace,
    schema_file,
    last_message_file,
    model=None,
) -> list[str]:
    """Assemble the exact ``codex exec`` argument vector.

    Flag order is fixed; ``--ephemeral`` is never emitted and its accidental
    presence is a hard driver error.
    """
    argv = [
        str(codex_path),
        "exec",
        "--json",
        "-s",
        SANDBOX_MODE,
        "-C",
        str(workspace),
        "--skip-git-repo-check",
        "--color",
        "never",
        "--output-schema",
        str(schema_file),
        "-o",
        str(last_message_file),
    ]
    if model is not None:
        argv.extend(["-m", str(model)])
    argv.append("-")  # prompt on stdin
    if "--ephemeral" in argv:
        raise CodexExecError("internal error: --ephemeral must never be passed")
    return argv


def _build_environment(env: dict | None) -> dict:
    merged = dict(os.environ)
    merged["OPENBLAS_NUM_THREADS"] = "1"
    merged["OMP_NUM_THREADS"] = "1"
    if env:
        merged.update({str(key): str(value) for key, value in env.items()})
    return merged


def _pump_stream(stream, sink: list) -> None:
    """Drain a text pipe into a list; runs in a daemon thread."""
    sink.extend(stream)
    try:
        stream.close()
    except OSError:
        pass


# P2.3: private, dot-prefixed partial evidence inside the run's evidence
# directory. The finalized names appear ONLY via the atomic rename after
# the awaited exit; a crash mid-run leaves these (or nothing) behind and
# NEVER a half-written final evidence file.
_RAW_STDOUT_PARTIAL = ".codex-events.raw.jsonl.partial"
_RAW_STDERR_PARTIAL = ".codex-stderr.partial"
# stderr excerpt memory cap (repair2 N2/G1): the EOF-anchored window
# never grows past this. The deviation envelope is precise: the result
# can differ from the old whole-text formula only when the trailing
# whitespace run PLUS the bytes of the 500 characters before it exceed
# this cap (the gray band); inside the band the result is the (shorter)
# capped window's tail, with any window-edge U+FFFD trimmed.
_STDERR_WINDOW_CAP = 16 * 1024 * 1024


def _pump_stdout_streaming(stream, raw_path, accumulator,
                           first_partial, pump_error) -> None:
    """P2.3 stdout pump: iterate the pipe line by line, append each line
    to the private partial raw-NDJSON file (flushed per line so an
    in-flight observer sees exactly the complete segments the child has
    flushed) and feed the incremental parser. Memory: one line at a
    time; receipt metadata growth is itemised separately, not bounded
    here.

    repair1 F2: a parse exception (e.g. RecursionError on a deep-JSON
    bomb) is RECORDED and the loop keeps reading/writing — the evidence
    file ends up exactly as complete as the old whole-buffer
    implementation's, and the recorded error is raised loudly by the
    caller after finalize (old raised from its main-thread parse). A
    pump that dies silently can never produce a truncated-but-delivered
    receipt again.

    repair2 N3/G2 (F4 parity, declared): a UnicodeDecodeError (child
    ended mid-codepoint) stops the pump SILENTLY, exactly like the old
    whole-buffer pump — the old run delivered its receipt with the
    undecodable tail dropped and no signal; that acceptance set is
    preserved here on purpose. The drop is flagged for metrics only.

    WP-A item 1 (P2.3 remark 2, declared parity): an ABANDONED pump
    (stuck at the join window, see run_readonly) that records an error
    AFTER the run returned never has that error raised — the old
    whole-buffer pump was a plain daemon thread whose late death was
    equally invisible to the caller (parity accepted in P2.3 repair2);
    the late error only lands in the metrics sink's captured
    ``stdout_pump``/``stderr_pump`` snapshots. The former
    ``pump_error['closing']`` guards (set by a caller force-close that
    repair2 N1 removed with ``stream.close``) were dead code and are
    deleted (WP-A item 1 remark 1)."""
    try:
        with open(raw_path, "w", encoding="utf-8") as raw:
            for line in stream:
                raw.write(line)
                raw.flush()
                if pump_error["error"] is None:
                    try:
                        accumulator.feed(line)
                    except Exception as error:  # noqa: BLE001 — recorded
                        pump_error["error"] = error
                if first_partial is not None and not first_partial["seen"] \
                        and line.strip():
                    first_partial["seen"] = True
                    first_partial["at_s"] = time.monotonic() - \
                        first_partial["t0"]
    except UnicodeDecodeError:
        # repair2 N3/G2: old-parity silent tail drop (child died
        # mid-codepoint); flagged for metrics, never raised
        pump_error["decode_drop"] = True
    except OSError as error:
        # repair1 D2/R3: the partial evidence path itself failed
        # (uncreatable/unwritable) — degrade, never crash past the exit
        pump_error["degraded"] = pump_error.get("degraded") or str(error)
    except Exception as error:  # noqa: BLE001 — recorded, never silent
        pump_error["error"] = pump_error["error"] or error
    finally:
        try:
            stream.close()
        except OSError:
            pass


def _pump_stderr_streaming(stream, raw_path, pump_error) -> None:
    """P2.3 stderr pump: stream lines into the private partial file —
    the whole stderr NEVER lives in memory (bounded structure = the disk
    file plus the bounded excerpt read at finalize time). Errors are
    recorded into ``pump_error`` (repair1 F2), never swallowed. The
    late-error and dead-``closing``-guard notes of the stdout pump
    (WP-A item 1) apply identically here."""
    try:
        with open(raw_path, "w", encoding="utf-8") as raw:
            for line in stream:
                raw.write(line)
                raw.flush()
    except UnicodeDecodeError:
        pump_error["decode_drop"] = True  # repair2 N3/G2: old parity
    except OSError as error:
        pump_error["degraded"] = pump_error.get("degraded") or str(error)
    except Exception as error:  # noqa: BLE001 — recorded, never silent
        pump_error["error"] = pump_error["error"] or error
    finally:
        try:
            stream.close()
        except OSError:
            pass


def _stderr_excerpt_file(raw_path) -> str:
    """The old ``"".join(lines).strip()[-500:]`` rule, exactly, read with
    bounded memory from the partial stderr file (then unlinked).

    Exact-equivalence window (repair1 R1, cap widened repair2 N2/G1):
    grow a tail window from EOF (doubling) until its rstripped decode
    reaches the excerpt limit, then return its last ``limit``
    characters. Correctness while the window is not capped: its right
    edge is EOF, so its trailing whitespace run equals the full text's;
    once ``len(window.rstrip()) >= limit`` the last ``limit`` characters
    of the rstripped window are IDENTICAL to the old whole-text
    formula's. A left-edge codepoint split decodes to U+FFFD, which sits
    left of the surviving ``limit`` characters. At BOF the whole
    (sub-cap) text is in hand and the formula applies directly.

    Precise deviation envelope (the accepted F5 class, repair2-registered):
    a difference from the old formula is possible ONLY when the trailing
    whitespace run plus the bytes of the ``limit`` characters before it
    exceed ``_STDERR_WINDOW_CAP`` (the gray band). Inside the band the
    result is the capped window's (shorter) rstripped tail with any
    window-edge U+FFFD trimmed — never a fabricated character, but
    shorter than the old answer. Memory stays capped at the window cap."""
    try:
        size = raw_path.stat().st_size
    except OSError:
        return ""
    try:
        with open(raw_path, "rb") as handle:
            window = 64 * 1024
            while True:
                take = min(window, size)
                handle.seek(size - take)
                text = handle.read().decode("utf-8", "replace")
                stripped = text.strip()
                if len(stripped) >= _STDERR_EXCERPT_LIMIT or take >= size \
                        or take >= _STDERR_WINDOW_CAP:
                    break
                window *= 2
    except OSError:
        return ""
    finally:
        try:
            raw_path.unlink()
        except OSError:
            pass
    result = stripped[-_STDERR_EXCERPT_LIMIT:]
    if take < size:
        # capped (gray band): a left-edge split codepoint may have
        # decoded to U+FFFD — trim fabricated replacement chars
        result = result.lstrip("\ufffd")
    return result


def _write_stdin(stream, data: str) -> None:
    """Write the prompt and close stdin; never raises into the main thread."""
    try:
        stream.write(data)
    except (BrokenPipeError, OSError):
        pass  # codex died before reading the prompt; outcome surfaces via exit code
    finally:
        try:
            stream.close()
        except (BrokenPipeError, OSError):
            pass


class _EventAccumulator:
    """P2.3: the JSONL event parser, fed ONE line at a time as the stdout
    pump flushes it. Identical whitelist semantics to the former
    whole-list parse: unknown types are counted, never fatal;
    ``Reconnecting`` errors are transport retries; every other error
    contributes a truncated message to ``hard_errors``.

    WP-C governance: the three output-driven metadata structures are
    capped DURING accumulation (never only at receipt time — a hostile
    stream cannot grow the accumulator past the caps). Over-cap entries
    are dropped keep-first; the ``*_dropped`` counters make the loss
    visible (source events not retained, see the module constants)."""

    def __init__(self):
        self.counts: Counter = Counter()
        self.thread_id = None
        self.thread_id_any = None
        self.model_observed = None
        self.command_items: list = []
        self.transport_retries = 0
        self.hard_errors: list = []
        self.event_counts_dropped = 0
        self.command_items_dropped = 0
        self.hard_errors_dropped = 0

    def _count_type(self, key: str) -> None:
        """Count one event under the ``event_counts`` entry cap.

        A retained key (or a new key while the cap has room) counts
        normally; an event whose type key is past the cap is NOT silently
        counted — it bumps ``event_counts_dropped`` so the receipt can
        flag the truncation."""
        if key in self.counts or len(self.counts) < EVENT_COUNTS_ENTRY_CAP:
            self.counts[key] += 1
        else:
            self.event_counts_dropped += 1

    def feed(self, raw) -> None:
        try:
            event = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            self._count_type("<invalid-json>")
            return
        if not isinstance(event, dict):
            self._count_type("<non-object>")
            return
        etype = event.get("type")
        self._count_type(str(etype) if etype is not None else "<no-type>")
        if etype == "error":
            message = event.get("message")
            text = message if isinstance(message, str) else "<error without message>"
            if text.startswith("Reconnecting"):
                self.transport_retries += 1
            elif len(self.hard_errors) < HARD_ERRORS_ENTRY_CAP:
                self.hard_errors.append(text[:_HARD_ERROR_LIMIT])
            else:
                self.hard_errors_dropped += 1
        candidate = event.get("thread_id")
        if self.thread_id_any is None and isinstance(candidate, str) and candidate:
            self.thread_id_any = candidate
        if etype == "thread.started" and isinstance(candidate, str) and candidate:
            self.thread_id = self.thread_id or candidate
        nested = (event.get("payload"), event.get("item"))
        for source in (event, *(inner for inner in nested if isinstance(inner, dict))):
            model = source.get("model")
            if isinstance(model, str) and model:
                self.model_observed = model
        item = event.get("item")
        if (
            etype == "item.completed"
            and isinstance(item, dict)
            and item.get("type") == "command_execution"
        ):
            if len(self.command_items) < COMMAND_ITEMS_ENTRY_CAP:
                self.command_items.append({
                    key: item[key]
                    for key in ("command", "exit_code", "status") if key in item
                })
            else:
                self.command_items_dropped += 1

    def result(self) -> tuple[dict, str | None, str | None, list, int, list,
                             int, int, int]:
        return (dict(self.counts), self.thread_id or self.thread_id_any,
                self.model_observed, self.command_items,
                self.transport_retries, self.hard_errors,
                self.event_counts_dropped, self.command_items_dropped,
                self.hard_errors_dropped)


def _parse_events(lines) -> tuple[dict, str | None, str | None, list, int, list,
                                  int, int, int]:
    """Whitelist-parse JSONL events (whole-list compat wrapper).

    WP-C: the result carries the three governance drop counters after
    the six pre-governance slots."""
    accumulator = _EventAccumulator()
    for raw in lines:
        accumulator.feed(raw)
    return accumulator.result()


def _receipt_payload(receipt: CodexRunReceipt) -> dict:
    """WP-C: ``asdict`` with falsy governance markers stripped.

    This IS the persisted-receipt serialization rule: the
    ``<name>_truncated`` / ``<name>_dropped`` pair appears beside a
    field ONLY when that field was actually truncated, so an
    under-threshold receipt stays byte-identical to the pre-governance
    shape."""
    payload = dataclasses.asdict(receipt)
    for marker_key in _RECEIPT_MARKER_KEYS:
        if not payload.get(marker_key):
            del payload[marker_key]
    return payload


def _stderr_excerpt(stderr_lines: list) -> str:
    """Old whole-list excerpt rule (kept for reference/tests): the file
    path uses the bounded read in _stderr_excerpt_file instead."""
    text = "".join(stderr_lines).strip()
    return text[-_STDERR_EXCERPT_LIMIT:]


def run_readonly(
    codex_path,
    workspace: Path,
    task: str,
    output_schema: dict,
    evidence_dir: Path,
    timeout_s: float = 300,
    model: str | None = None,
    env: dict | None = None,
    strict_schema: bool = True,
    exit_event_sink: callable | None = None,
    metrics_sink: dict | None = None,
) -> CodexRunReceipt:
    """Run one read-only ``codex exec`` dispatch and persist its evidence.

    ``env`` entries are merged over the inherited environment (which always
    pins ``OPENBLAS_NUM_THREADS=1`` / ``OMP_NUM_THREADS=1``) so callers can
    point ``CODEX_HOME`` at an isolated tree. With ``strict_schema`` (the
    default) the schema is strict-normalized via
    :func:`normalize_strict_schema` before it is written, and the receipt
    records whether that changed anything in ``schema_normalized``.
    """
    workspace = Path(workspace)
    evidence_dir = Path(evidence_dir)
    # P0.4 round 2: reuse the P0.3 exactly-once exit projection — only an
    # integer returncode from a REAL waited child (normal wait or the
    # terminate/kill gradient's final wait) ever fires the sink.
    exit_projection = (ExitProjection(on_event=exit_event_sink)
                       if exit_event_sink is not None else None)
    if not isinstance(task, str) or not task.strip():
        raise CodexExecError("task must be a non-empty prompt string")
    if not isinstance(output_schema, dict):
        raise CodexExecError("output_schema must be a JSON Schema dict")
    if not isinstance(timeout_s, (int, float)) or timeout_s <= 0:
        raise CodexExecError("timeout_s must be a positive number of seconds")

    if strict_schema:
        effective_schema = normalize_strict_schema(output_schema)
        schema_normalized = effective_schema != output_schema
    else:
        effective_schema = output_schema
        schema_normalized = False

    evidence_dir.mkdir(parents=True, exist_ok=True)
    schema_file = evidence_dir / SCHEMA_FILENAME
    last_message_file = evidence_dir / LAST_MESSAGE_FILENAME
    events_file = evidence_dir / EVENTS_FILENAME
    schema_file.write_text(json.dumps(effective_schema) + "\n", encoding="utf-8")

    argv = build_argv(codex_path, workspace, schema_file, last_message_file, model=model)
    started = time.monotonic()

    timed_out = False
    # P2.3 streaming evidence: the placeholder keeps the finalized name
    # observable from launch — and, repair1 D3, TRUNCATES any stale
    # content when an evidence directory is reused, exactly like the old
    # eager open('w') — while the real bytes stream into the PRIVATE
    # partial raw file, flushed per line; the rename happens only after
    # the awaited exit (atomic finalize — a crash never leaves a
    # half-written final evidence).
    events_file.write_text("")
    raw_stdout_path = evidence_dir / _RAW_STDOUT_PARTIAL
    raw_stderr_path = evidence_dir / _RAW_STDERR_PARTIAL
    accumulator = _EventAccumulator()
    first_partial = {"seen": False, "at_s": None, "t0": started}
    stdout_pump_error = {"error": None}
    stderr_pump_error = {"error": None}
    process = subprocess.Popen(  # argv is fully driver-controlled
        argv,
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        cwd=str(workspace),
        env=_build_environment(env),
    )
    stdout_pump = threading.Thread(
        target=_pump_stdout_streaming,
        args=(process.stdout, raw_stdout_path, accumulator, first_partial,
              stdout_pump_error),
        daemon=True,
    )
    stderr_pump = threading.Thread(
        target=_pump_stderr_streaming,
        args=(process.stderr, raw_stderr_path, stderr_pump_error),
        daemon=True,
    )
    stdin_writer = threading.Thread(
        target=_write_stdin, args=(process.stdin, task), daemon=True
    )
    stdout_pump.start()
    stderr_pump.start()
    stdin_writer.start()

    try:
        process.wait(timeout=timeout_s)
    except subprocess.TimeoutExpired:
        timed_out = True
        process.terminate()
        try:
            process.wait(timeout=_TERMINATE_GRACE_S)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
    stdout_pump.join(timeout=_TERMINATE_GRACE_S)
    stderr_pump.join(timeout=_TERMINATE_GRACE_S)
    stdin_writer.join(timeout=_TERMINATE_GRACE_S)
    # repair2 N1 — bounded settle: NEVER close the read end from here
    # (a TextIOWrapper close blocks on the buffered-IO lock the stuck
    # pump's readline holds — an orphan holding the pipe would hang the
    # run past any budget). The daemon pump is simply abandoned, exactly
    # like the old implementation's whole-buffer pump: it can only ever
    # append to the PRIVATE partial, never to finalized evidence.
    #
    # WP-A item 1 remark 3 (declared boundary, not "eliminated"): the
    # snapshot below is a byte-PREFIX of the still-growing private
    # partial, and the leftover dot-partial may outgrow the finalized
    # file (an orphan keeps appending after the run returns). P2.3
    # repair2 measured the tear window as unhittable in practice
    # (per-line writes + flush; 727k polling rounds and 300 concurrent
    # snapshots, 0 torn tails). Eliminating the prefix property would
    # need either an unbounded wait (violates the bounded settle above)
    # or unlinking the path mid-flight — which re-enters the FIFO-preset
    # shape below with platform-dependent open() semantics for no
    # parity anchor; the private dot-name never impersonates evidence.
    #
    # WP-A item 1 remark 4 (declared platform semantics): when the
    # private partial path is a preset FIFO (join-window worst shape),
    # bounded finalize relies on the platform's FIFO open-pairing
    # semantics (verified safe on macOS/CPython: copyfile ends with a
    # 0-byte regular finalized file and the run returns bounded). A
    # cross-platform hard guarantee would need an S_ISFIFO precheck in
    # the D2 degrade family — deliberately NOT added here (behavior
    # change without a parity anchor; registered boundary instead).
    finalize_note = None
    if stdout_pump.is_alive() or stderr_pump.is_alive():
        finalize_note = "pump(s) stuck at join window; snapshot delivered"
    # atomic finalize: only after the real awaited exit do the private
    # partials become the final evidence. repair1 D2/R3: a missing or
    # unreadable partial degrades to an empty final file (receipt still
    # written), never a crash past the awaited exit.
    try:
        if stdout_pump.is_alive():
            import shutil as _shutil
            _shutil.copyfile(raw_stdout_path, events_file)
        else:
            os.replace(raw_stdout_path, events_file)
    except OSError:
        finalize_note = finalize_note or "raw stdout partial unavailable"
        with contextlib.suppress(OSError):
            events_file.write_text("")
    if exit_projection is not None:
        # the wait gradient above completed (or the process was terminated
        # and waited): project the real awaited exit exactly once
        exit_projection.observe_awaited_exit(
            process.returncode, generation=next(_COLD_GENERATIONS))
    duration = time.monotonic() - started
    exit_code = process.returncode

    event_counts, thread_id, model_observed, command_items, transport_retries, \
        hard_errors, counts_dropped, command_dropped, hard_dropped = \
        accumulator.result()

    raw_stderr_size = (raw_stderr_path.stat().st_size
                       if raw_stderr_path.exists() else 0)
    error: str | None = None
    structured: dict | None = None
    if timed_out:
        outcome = "timeout"
        error = f"codex exec timed out after {timeout_s}s (exit {exit_code})"
    elif exit_code != 0:
        outcome = "failed"
        excerpt = _stderr_excerpt_file(raw_stderr_path)
        error = f"codex exec exited {exit_code}"
        if excerpt:
            error += f": {excerpt}"
    else:
        structured, struct_error = _load_structured(last_message_file, output_schema)
        if struct_error is None:
            outcome = "delivered"
        else:
            outcome = "schema_mismatch"
            error = struct_error

    receipt = CodexRunReceipt(
        ok=outcome == "delivered",
        outcome=outcome,
        thread_id=thread_id,
        exit_code=exit_code,
        events_path=str(events_file),
        last_message_path=str(last_message_file),
        structured=structured,
        event_counts=event_counts,
        command_items=command_items,
        model_observed=model_observed,
        sandbox=SANDBOX_MODE,
        duration_s=round(duration, 3),
        error=error,
        transport_retries=transport_retries,
        hard_errors=hard_errors,
        schema_normalized=schema_normalized,
        event_counts_truncated=counts_dropped > 0,
        event_counts_dropped=counts_dropped,
        command_items_truncated=command_dropped > 0,
        command_items_dropped=command_dropped,
        hard_errors_truncated=hard_dropped > 0,
        hard_errors_dropped=hard_dropped,
    )
    if raw_stderr_path.exists():
        # no excerpt was taken (delivered / timeout paths): the private
        # stderr partial still must not linger as finalized-looking state
        try:
            raw_stderr_path.unlink()
        except OSError:
            pass
    # repair1 F2: a recorded pump/parse error is raised LOUDLY after the
    # evidence is finalized — the old implementation's parse blew up in
    # the main thread after writelines, leaving complete evidence and no
    # receipt; this restores that contract (never a truncated "delivered").
    for _holder in (stdout_pump_error, stderr_pump_error):
        if _holder["error"] is not None:
            raise _holder["error"]
    if metrics_sink is not None:
        # P2.3 measurements only — the receipt schema is untouched. Raw
        # buffer bounding is this item's scope; receipt metadata bytes
        # are itemised, NOT judged bounded here.
        # repair1 F3: the sink is caller-injected — a raising sink must
        # never take the receipt down (same isolation as the exit sink).
        with contextlib.suppress(Exception):
            receipt_meta = {
                "event_counts": event_counts,
                "command_items": command_items,
                "hard_errors": hard_errors,
                "transport_retries": transport_retries,
            }
            # WP-C: truncation markers ride along ONLY when a field was
            # actually truncated, so the common (under-cap) metrics shape
            # is unchanged.
            if counts_dropped:
                receipt_meta["event_counts_truncated"] = True
                receipt_meta["event_counts_dropped"] = counts_dropped
            if command_dropped:
                receipt_meta["command_items_truncated"] = True
                receipt_meta["command_items_dropped"] = command_dropped
            if hard_dropped:
                receipt_meta["hard_errors_truncated"] = True
                receipt_meta["hard_errors_dropped"] = hard_dropped
            metrics_sink.update({
                "first_partial_s": (round(first_partial["at_s"], 3)
                                    if first_partial["at_s"] is not None
                                    else None),
                "raw_stdout_bytes": (events_file.stat().st_size
                                     if events_file.exists() else 0),
                "raw_stderr_bytes": raw_stderr_size,
                "receipt_meta": receipt_meta,
                # repair2 N4/G3: degraded/settle/decode observations are
                # CONSUMED here (receipt schema stays untouched)
                "finalize_note": finalize_note,
                "stdout_pump": dict(stdout_pump_error),
                "stderr_pump": dict(stderr_pump_error),
            })
    # WP-C: persisted receipt — falsy governance markers are STRIPPED, so
    # an under-threshold receipt is byte-identical to the pre-governance
    # shape (the marker pair appears beside a field only on truncation).
    (evidence_dir / RECEIPT_FILENAME).write_text(
        json.dumps(_receipt_payload(receipt), indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return receipt


def _load_structured(last_message_file: Path, output_schema: dict) -> tuple[dict | None, str | None]:
    """Load and validate the -o file; returns (structured, error)."""
    if not last_message_file.exists():
        return None, f"codex wrote no last-message file: {last_message_file}"
    try:
        payload = json.loads(last_message_file.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        return None, f"last-message file is not valid JSON: {exc}"
    if not isinstance(payload, dict):
        return None, "last-message file did not contain a JSON object"
    required = output_schema.get("required")
    if isinstance(required, list) and required:
        missing = [key for key in required if key not in payload]
        if missing:
            return None, "last-message JSON is missing required keys: " + ", ".join(
                str(key) for key in missing
            )
    return payload, None


def _default_codex_home() -> Path:
    configured = os.environ.get("CODEX_HOME")
    if configured:
        return Path(configured).expanduser()
    return Path.home() / ".codex"


def _read_first_json_line(path: Path) -> dict | None:
    """Read only the first JSON line of a rollout file (its session_meta)."""
    try:
        with open(path, "r", encoding="utf-8", errors="replace") as handle:
            first = handle.readline()
        parsed = json.loads(first)
    except (OSError, json.JSONDecodeError):
        return None
    return parsed if isinstance(parsed, dict) else None


def _index_hints(codex_home: Path, thread_id: str) -> list[Path]:
    """Candidate rollout paths taken from session_index.jsonl, if it lists any."""
    hints: list[Path] = []
    index_path = codex_home / "session_index.jsonl"
    if not index_path.is_file():
        return hints
    # repair1 F-C + repair2 R1/R2: the OLD reader used read_text() then
    # str.splitlines() — exotic separators split records there, and the
    # whole-file decode never split a multibyte character. Stream in
    # bounded chunks through an INCREMENTAL utf-8 decoder (a straddling
    # multibyte char is held by the decoder, never replaced) and split
    # each chunk scanning ONLY its new bytes (linear in the new input).
    # Admission is unchanged (the old read_text had none here).
    #
    # WP-A item 3: the scanning engine is the SHARED
    # zcode_supervisor.splitlines_stream implementation (converged with
    # hub_server's copy; segment order and boundaries are byte-identical).
    # WP-A item 4: the EOF rule is now EXACTLY str.splitlines — a lone
    # trailing \r at EOF terminates one (possibly empty) segment; the
    # repair3 R4/N3 "one lost trailing empty" exemption is closed. The
    # empty segments are consumer-neutral here (json.loads('') fails,
    # the entry is skipped).
    try:
        import codecs
        decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
        pending = ""
        scanned = 0
        with open(index_path, "rb") as handle:
            while True:
                chunk = handle.read(256 * 1024)
                if not chunk:
                    break
                pending += decoder.decode(chunk)
                segments, rest, scanned = scan_new(pending, scanned)
                if segments:
                    pending = rest
                    for line in segments:
                        _absorb_index_line(line, thread_id, hints)
        # EOF: flush the decoder, then the exact splitlines tail rule
        pending += decoder.decode(b"", True)
        for line in eof_segments(pending, scanned):
            _absorb_index_line(line, thread_id, hints)
    except OSError:
        return hints
    return hints


def _absorb_index_line(line, thread_id, hints):
    """One splitlines-style segment -> hint path when it matches."""
    try:
        entry = json.loads(line)
    except json.JSONDecodeError:
        return
    if not isinstance(entry, dict):
        return
    if not any(entry.get(field) == thread_id
               for field in _INDEX_ID_FIELDS):
        return
    for field in _INDEX_PATH_FIELDS:
        value = entry.get(field)
        if isinstance(value, str) and value:
            hints.append(Path(value))
            break


def locate_rollout(thread_id: str, codex_home: Path | None = None) -> dict:
    """Locate the persisted rollout file for a thread id; never guesses paths."""
    if not thread_id:
        return {"status": "unlocatable", "reason": "no thread id"}
    if not isinstance(thread_id, str) or not _THREAD_ID_PATTERN.match(thread_id):
        return {"status": "unlocatable", "reason": "invalid thread id"}

    home = Path(codex_home).expanduser() if codex_home is not None else _default_codex_home()
    sessions_root = home / "sessions"

    candidates: list[Path] = []
    for hint in _index_hints(home, thread_id):
        if hint.is_file() and hint.name.endswith(f"-{thread_id}.jsonl"):
            candidates.append(hint)
    if sessions_root.is_dir():
        for path in sorted(sessions_root.rglob(f"*-{thread_id}.jsonl")):
            if path not in candidates:
                candidates.append(path)

    rejection = "rollout file found but session_meta does not match thread id"
    for path in candidates:
        meta = _read_first_json_line(path)
        if meta is None or meta.get("type") != "session_meta":
            rejection = "rollout file found but its first line is not session_meta"
            continue
        payload = meta.get("payload")
        if not isinstance(payload, dict):
            rejection = "rollout file found but session_meta payload is malformed"
            continue
        # Spec checks payload.session_id; real codex 0.163 rollouts also carry
        # the rollout's own uuid in payload.id (session_id may be a parent
        # session), so accept an exact match on either identifier.
        if payload.get("session_id") == thread_id or payload.get("id") == thread_id:
            return {
                "status": "located",
                "path": str(path),
                "cwd": payload.get("cwd"),
                "cli_version": payload.get("cli_version"),
            }
    if candidates:
        return {"status": "unlocatable", "reason": rejection}
    return {
        "status": "unlocatable",
        "reason": f"no rollout file ending with -{thread_id}.jsonl under {sessions_root}",
    }


_SUMMARY_ITEM_KEYS = ("type", "role", "name", "command")
_SUMMARY_META_KEYS = ("session_id", "cwd", "cli_version", "timestamp")


def _truncate(value) -> object:
    return value[:_ITEM_TEXT_LIMIT] if isinstance(value, str) else value


def summarize_rollout(path: Path, max_items: int = 50) -> dict:
    """Conservative, whitelist-only rollout summary for the decision layer.

    Emits per-type line counts, the session_meta identifiers, and at most
    ``max_items`` items whose payload fields are restricted to
    type/role/name/command (strings truncated); no other raw field is ever
    exposed, so secrets such as API keys cannot leak into a summary.
    """
    path = Path(path)
    counts: Counter = Counter()
    meta: dict = {}
    items: list = []
    total = 0
    try:
        # P2.4: line-streamed rollout read — identical total/type/meta/
        # items semantics (line iteration yields a trailing no-newline
        # tail exactly like the former readlines); only the capped items
        # list is held in memory.
        with open(path, "r", encoding="utf-8", errors="replace") as handle:
            for line in handle:
                total += 1
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    counts["<invalid-json>"] += 1
                    continue
                if not isinstance(record, dict):
                    counts["<non-object>"] += 1
                    continue
                rtype = record.get("type")
                counts[str(rtype) if rtype is not None else "<no-type>"] += 1
                if total == 1 and rtype == "session_meta":
                    payload = record.get("payload")
                    payload = payload if isinstance(payload, dict) else {}
                    meta = {
                        key: payload.get(key) if key != "timestamp" else record.get(key)
                        for key in _SUMMARY_META_KEYS
                    }
                if len(items) < max_items:
                    item = {"type": rtype}
                    payload = record.get("payload")
                    if isinstance(payload, dict):
                        for key in _SUMMARY_ITEM_KEYS:
                            if key != "type" and key in payload:
                                item[key] = _truncate(payload[key])
                    items.append(item)
    except OSError as exc:
        return {"path": str(path), "error": f"unreadable rollout: {exc}"}
    return {
        "path": str(path),
        "total_lines": total,
        "type_counts": dict(counts),
        "meta": meta,
        "items": items,
    }
