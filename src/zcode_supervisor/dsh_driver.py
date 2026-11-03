"""dsh worker channel: one ``dsh --profile headless --json`` dispatch.

Live-verified facts (2026-11-03 probe, dsh at /opt/homebrew/bin/dsh):

* stdout with ``--json`` is newline-delimited JSON:
  ``{"type":"session","sessionId":…}``, heartbeat
  ``{"type":"status","phase":"turn_start|step_start|step_end|turn_end",…}``
  (``turn_end`` carries ``reason.kind`` — ``"error"`` plus a message on
  failure), and the terminal ``{"type":"final","text":…}``;
* the PROCESS exit code is 0 even on errors (e.g. MISSING_CREDENTIAL), so
  success is judged from the event stream, never from the exit code;
* the task text is read from stdin when the task argument is ``-``.

Honesty boundary (mirrors the codex exec channel): dsh headless has NO
per-command approval callback — the process runs with the launching user's
permissions inside the contract workspace. The control is the human-registered
contract plus before/after inventory evidence recorded by the hub; this
driver claims no brokering. Credentials stay in the dsh profile's own store;
a missing key surfaces verbatim as an ``error`` outcome.

Process-tree discipline (review P1-1): dsh runs as its OWN process-group
leader; a timeout terminates the whole group, and a receipt is published as
``delivered`` only when BOTH pump threads have finished — a descendant that
inherits the pipes and keeps writing would otherwise mutate finalized
evidence after the fact. If the streams cannot be settled, the private
partials are quarantined (never renamed into final evidence names) and the
run fails with ``error``.
"""

from __future__ import annotations

import json
import math
import os
import signal
import socket
import struct
import subprocess
import threading
import time
from pathlib import Path

from .progress import ProgressLedger, safe_token
from .transport import MSG_PROGRESS as _MSG_PROGRESS
from .transport import MSG_RESULT as _MSG_RESULT

__all__ = ["DSH_MODES", "DshRunError", "run_headless"]

DSH_MODES = ("headless",)
_EVENTS_NAME = "dsh-events.jsonl"
_STDERR_NAME = "dsh-stderr.log"
_LEDGER_NAME = "dsh.progress.jsonl"
_RAW_STDOUT_PARTIAL = ".dsh-stdout.partial"
_RAW_STDERR_PARTIAL = ".dsh-stderr.partial"
_TERMINATE_GRACE_S = 5.0
_MAX_LINE_BYTES = 1 << 20
_OVERSIZE_NOTE = "[oversize line discarded]"
#: Generous default: one max-thinking generation of a large file can stay
#: silent for minutes; only real silence beyond this is a stall.
DEFAULT_STALL_TIMEOUT_S = 900.0


class DshRunError(ValueError):
    """Invalid dispatch parameters (never a worker failure)."""


# L1 telemetry: lightweight sync UDS push (no asyncio — runs in pump threads).
# Frame format matches transport.py: [type:1B][len:4B][payload:msgpack/json].
_TELEM_HEADER = struct.Struct("<BI")


class _TelemetryPusher:
    """Sync UDS client for pushing progress updates to the hub's telemetry.

    Fire-and-forget: if the socket is unreachable or the send fails, the
    push is silently dropped (the file-based ledger remains the evidence
    authority; UDS is a latency optimization for status reads only).
    """

    def __init__(self, socket_path: str | None):
        self._path = socket_path
        self._sock: socket.socket | None = None

    def _ensure_connected(self) -> bool:
        if self._sock is not None:
            return True
        if not self._path:
            return False
        try:
            sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
            sock.settimeout(0.5)
            sock.connect(self._path)
            self._sock = sock
            return True
        except (OSError, socket.timeout):
            self._sock = None
            return False

    def push(self, msg_type: int, payload: dict) -> None:
        if not self._ensure_connected():
            return
        try:
            from . import transport as _t
            frame = _t.encode_envelope(_t.MessageEnvelope(msg_type, payload))
            # Stream framing: [type:1B][len:4B][payload]
            header = _TELEM_HEADER.pack(msg_type, len(frame) - 1)
            self._sock.sendall(header + frame[1:])
        except (OSError, Exception):
            # Drop silently; file ledger is the evidence authority
            self._close()

    def _close(self):
        if self._sock is not None:
            try:
                self._sock.close()
            except OSError:
                pass
            self._sock = None


def _iter_lines_bounded(stream, cap=_MAX_LINE_BYTES):
    """Yield newline-terminated lines; a pathological line yields ``None``.

    ``readline(cap)`` keeps streaming semantics (returns on newline), while
    the cap bounds buffering when a worker emits megabytes without a break
    (review P2-4): one ``None`` marks the discarded line, reading then
    resumes at the next newline.
    """
    buffer = ""
    while True:
        chunk = stream.readline(cap - len(buffer) if buffer else cap)
        if not chunk:
            if buffer:
                yield None
            return
        buffer += chunk
        if buffer.endswith("\n"):
            yield buffer[:-1]
            buffer = ""
        elif len(buffer) >= cap:
            yield None
            buffer = ""
            while True:
                chunk = stream.readline(cap)
                if not chunk:
                    return
                if chunk.endswith("\n"):
                    break


def _pump_stdout(stream, raw_path: Path, state: dict, ledger: ProgressLedger,
                 pump_error: dict, pusher=None, telemetry_id=None) -> None:
    """Stream stdout lines to the raw file; note one heartbeat per event."""
    try:
        with raw_path.open("a", encoding="utf-8") as sink:
            for line in _iter_lines_bounded(stream):
                if line is None:
                    sink.write(_OVERSIZE_NOTE + "\n")
                    sink.flush()
                    ledger.note("dsh", "oversize-line")
                    continue
                sink.write(line + "\n")
                sink.flush()
                stripped = line.strip()
                if not stripped:
                    continue
                try:
                    event = json.loads(stripped)
                except ValueError:
                    ledger.note("dsh", "unparsed-line")
                    continue
                if not isinstance(event, dict):
                    continue
                etype = safe_token(event.get("type") or "?", 16)
                phase = safe_token(event.get("phase"), 12)
                detail = f"{etype}:{phase}" if phase != "?" else etype
                ledger.note("dsh", detail, turn=event.get("turn"))
                if pusher is not None:
                    pusher.push(_MSG_PROGRESS,
                                {"task_id": telemetry_id or "",
                                 "state": "running", "detail": detail})
                if etype == "session" and not state.get("session_id"):
                    state["session_id"] = event.get("sessionId")
                elif etype == "status" and phase == "turn_end":
                    reason = event.get("reason")
                    state["turn_end_reason"] = reason \
                        if isinstance(reason, dict) else {"kind": "unknown"}
                elif etype == "final":
                    state["final_text"] = event.get("text") \
                        if isinstance(event.get("text"), str) else ""
    except Exception as error:  # noqa: BLE001 - recorded, never fatal
        pump_error["error"] = str(error)


def _pump_stderr(stream, raw_path: Path, pump_error: dict) -> None:
    try:
        with raw_path.open("a", encoding="utf-8") as sink:
            for line in _iter_lines_bounded(stream):
                sink.write((line if line is not None else _OVERSIZE_NOTE) + "\n")
                sink.flush()
    except Exception as error:  # noqa: BLE001
        pump_error["error"] = str(error)


def _write_stdin(stream, data: str) -> None:
    try:
        stream.write(data)
        stream.flush()
    finally:
        try:
            stream.close()
        except Exception:  # noqa: BLE001 - broken pipe just ends input
            pass


def _kill_group(process: subprocess.Popen, sig: int) -> None:
    """Signal the child's whole process group.

    ``start_new_session=True`` made the child its own session AND group
    leader, so its pid IS the pgid — usable even after the leader itself was
    reaped (``os.getpgid`` on a reaped pid fails, but the group with its
    surviving members persists under the same id).
    """
    try:
        os.killpg(process.pid, sig)
        return
    except (ProcessLookupError, PermissionError, OSError):
        pass
    if process.returncode is None:
        try:
            if sig == signal.SIGKILL:
                process.kill()
            else:
                process.terminate()
        except OSError:
            pass


def run_headless(dsh_path, workspace, task, evidence_dir, *,
                 wall_timeout_s=3600.0, stall_timeout_s=DEFAULT_STALL_TIMEOUT_S,
                 env=None, session_id=None, stop_event=None,
                 telemetry_socket=None, task_id=None) -> dict:
    """Run one dsh headless dispatch and persist its evidence; returns a receipt.

    The receipt (never an exception for worker outcomes) carries ``outcome``
    in {"delivered", "error", "timeout-stall", "timeout-wall", "launch-error",
    "stopped"}, the dsh ``session_id``, the final message text, the
    ``turn_end`` reason, exit code, duration and the heartbeat ledger summary.

    ``session_id`` (2026-11-03 P2 attach): pass a previous receipt's session
    to continue THAT conversation (``dsh --session-id``) instead of booting a
    fresh one — the super-agent retry-reuse pattern. ``stop_event`` (P2
    stop): a :class:`threading.Event` checked by the watchdog; when set, the
    whole process group is terminated and the run settles as ``stopped``.
    """
    for label, value in (("workspace", workspace), ("task", task),
                         ("evidence_dir", evidence_dir)):
        if value is None:
            raise DshRunError(f"{label} must be provided")
    if not str(task).strip():
        raise DshRunError("task must be a non-empty prompt string")
    for label, value in (("wall_timeout_s", wall_timeout_s),
                         ("stall_timeout_s", stall_timeout_s)):
        if (isinstance(value, bool) or not isinstance(value, (int, float))
                or not math.isfinite(value) or value <= 0):
            raise DshRunError(f"{label} must be finite positive seconds")
    workspace = Path(workspace)
    evidence_dir = Path(evidence_dir)
    evidence_dir.mkdir(parents=True, exist_ok=True)
    # Truncate stale partials when an evidence dir is reused, then stream.
    events_file = evidence_dir / _EVENTS_NAME
    stderr_file = evidence_dir / _STDERR_NAME
    raw_stdout = evidence_dir / _RAW_STDOUT_PARTIAL
    raw_stderr = evidence_dir / _RAW_STDERR_PARTIAL
    for stale in (raw_stdout, raw_stderr, evidence_dir / _LEDGER_NAME):
        if stale.exists():
            stale.unlink()
    events_file.write_text("")
    stderr_file.write_text("")
    # L1 telemetry: UDS push channel (fire-and-forget, never blocks evidence)
    pusher = _TelemetryPusher(telemetry_socket)
    telemetry_id = task_id or evidence_dir.name
    def _push(msg_type, **fields):
        pusher.push(msg_type, {"task_id": telemetry_id, **fields})

    ledger = ProgressLedger(
        evidence_dir / _LEDGER_NAME,
        stall_timeout_s=float(stall_timeout_s),
        wall_timeout_s=float(wall_timeout_s))
    started = time.monotonic()
    state: dict = {}
    stdout_error: dict = {}
    stderr_error: dict = {}
    argv = [str(dsh_path), "--profile", "headless", "--json"]
    if session_id is not None:
        if not isinstance(session_id, str) or not session_id.strip() \
                or session_id.strip() != session_id:
            raise DshRunError("session_id must be a trimmed non-empty string")
        argv += ["--session-id", session_id]
    argv.append("-")
    environment = dict(env) if env else None
    try:
        process = subprocess.Popen(  # argv is fully driver-controlled
            argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, text=True, cwd=str(workspace),
            env=environment, start_new_session=True)
    except OSError as error:
        ledger.finalize("launch-error")
        receipt = {"outcome": "launch-error",
                   "error": f"failed to start dsh: {error}",
                   "duration_s": round(time.monotonic() - started, 3),
                   "progress": ledger.summary()}
        ledger.close()
        return receipt
    stdout_pump = threading.Thread(
        target=_pump_stdout,
        args=(process.stdout, raw_stdout, state, ledger, stdout_error),
        kwargs={"pusher": pusher, "telemetry_id": telemetry_id},
        daemon=True)
    stderr_pump = threading.Thread(
        target=_pump_stderr, args=(process.stderr, raw_stderr, stderr_error),
        daemon=True)
    stdin_writer = threading.Thread(
        target=_write_stdin, args=(process.stdin, str(task)), daemon=True)
    stdout_pump.start()
    stderr_pump.start()
    stdin_writer.start()
    ledger.note("dsh", "launched", pid=process.pid)

    timed_out = None
    stopped = False
    while True:
        try:
            process.wait(timeout=0.5)
            break
        except subprocess.TimeoutExpired:
            if stop_event is not None and stop_event.is_set():
                stopped = True
                timed_out = None
                _kill_group(process, signal.SIGTERM)
                try:
                    process.wait(timeout=_TERMINATE_GRACE_S)
                except subprocess.TimeoutExpired:
                    _kill_group(process, signal.SIGKILL)
                    process.wait()
                break
            reason = ledger.expired()
            if reason is None:
                continue
            timed_out = reason
            _kill_group(process, signal.SIGTERM)
            try:
                process.wait(timeout=_TERMINATE_GRACE_S)
            except subprocess.TimeoutExpired:
                _kill_group(process, signal.SIGKILL)
                process.wait()
            break
    stdin_writer.join(timeout=1.0)
    stdout_pump.join(timeout=_TERMINATE_GRACE_S)
    stderr_pump.join(timeout=_TERMINATE_GRACE_S)
    if stdout_pump.is_alive() or stderr_pump.is_alive():
        # A descendant inherited the pipes and still holds them open: kill
        # the whole group again, then give the pumps a final window. A
        # receipt must never be published while evidence can still change.
        _kill_group(process, signal.SIGKILL)
        stdout_pump.join(timeout=_TERMINATE_GRACE_S)
        stderr_pump.join(timeout=_TERMINATE_GRACE_S)
    streams_settled = not stdout_pump.is_alive() and not stderr_pump.is_alive()
    if streams_settled:
        # Atomic finalize: rename the private partials onto their final names
        # only once no writer can exist anymore.
        for partial, final in ((raw_stdout, events_file),
                               (raw_stderr, stderr_file)):
            if partial.exists():
                partial.replace(final)
        evidence_note = None
    else:
        # Quarantine (review P1-1): keep the private partials under their
        # dot-names — they are NOT the finalized evidence — and fail the run.
        evidence_note = ("evidence streams still open (a descendant holds the "
                         "dsh pipes); partial evidence retained unpublished")

    turn_end = state.get("turn_end_reason")
    final_text = state.get("final_text")
    exit_code = process.returncode
    if not streams_settled:
        outcome = "error"
        error_text = evidence_note
    elif stopped:
        outcome = "stopped"
        error_text = None
    elif timed_out == "stall":
        outcome = "timeout-stall"
        error_text = None
    elif timed_out == "wall":
        outcome = "timeout-wall"
        error_text = None
    elif isinstance(turn_end, dict) and turn_end.get("kind") == "error":
        outcome = "error"
        error_text = None
    elif exit_code != 0:
        outcome = "error"
        error_text = None
    elif isinstance(final_text, str) and final_text.strip():
        outcome = "delivered"
        error_text = None
    else:
        outcome = "error"
        error_text = None
    if outcome == "error" and error_text is None:
        if isinstance(turn_end, dict):
            inner = turn_end.get("error")
            error_text = (inner.get("message") if isinstance(inner, dict)
                          else json.dumps(turn_end))[:500]
        else:
            error_text = f"exit {exit_code} with empty final message"
    ledger.finalize(outcome)
    # L1 telemetry: terminal push so task_status sees completion instantly
    pusher.push(_MSG_RESULT, {
        "task_id": telemetry_id,
        "state": "finished", "outcome": outcome,
        "detail": (state.get("final_text") or error_text or "")[:200],
        "session_id": state.get("session_id"),
        "duration_s": round(time.monotonic() - started, 3)})
    pusher._close()
    receipt = {
        "outcome": outcome,
        "session_id": state.get("session_id"),
        "final_message": final_text if isinstance(final_text, str) else None,
        "turn_end_reason": turn_end,
        "exit_code": exit_code,
        "timed_out": timed_out,
        "error": error_text,
        "events_file": str(events_file if streams_settled else raw_stdout),
        "stderr_file": str(stderr_file if streams_settled else raw_stderr),
        "duration_s": round(time.monotonic() - started, 3),
        "progress": ledger.summary(),
    }
    ledger.close()
    if stdout_error.get("error"):
        receipt["stdout_pump_error"] = stdout_error["error"]
    if stderr_error.get("error"):
        receipt["stderr_pump_error"] = stderr_error["error"]
    return receipt
