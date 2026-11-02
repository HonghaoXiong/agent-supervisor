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
"""

from __future__ import annotations

import json
import subprocess
import threading
import time
from pathlib import Path

from .progress import ProgressLedger

__all__ = ["DSH_MODES", "DshRunError", "run_headless"]

DSH_MODES = ("headless",)
_EVENTS_NAME = "dsh-events.jsonl"
_STDERR_NAME = "dsh-stderr.log"
_LEDGER_NAME = "dsh.progress.jsonl"
_RAW_STDOUT_PARTIAL = ".dsh-stdout.partial"
_RAW_STDERR_PARTIAL = ".dsh-stderr.partial"
_TERMINATE_GRACE_S = 5.0
#: Generous default: one max-thinking generation of a large file can stay
#: silent for minutes; only real silence beyond this is a stall.
DEFAULT_STALL_TIMEOUT_S = 900.0


class DshRunError(ValueError):
    """Invalid dispatch parameters (never a worker failure)."""


def _pump_stdout(stream, raw_path: Path, state: dict, ledger: ProgressLedger,
                 pump_error: dict) -> None:
    """Stream stdout lines to the raw file; note one heartbeat per event."""
    try:
        with raw_path.open("a", encoding="utf-8") as sink:
            for line in stream:
                sink.write(line)
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
                etype = str(event.get("type") or "?")
                phase = event.get("phase")
                detail = f"{etype}:{phase}" if phase else etype
                ledger.note("dsh", detail, turn=event.get("turn"))
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
            for line in stream:
                sink.write(line)
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


def run_headless(dsh_path, workspace, task, evidence_dir, *,
                 wall_timeout_s=3600.0, stall_timeout_s=DEFAULT_STALL_TIMEOUT_S,
                 env=None) -> dict:
    """Run one dsh headless dispatch and persist its evidence; returns a receipt.

    The receipt (never an exception for worker outcomes) carries ``outcome``
    in {"delivered", "error", "timeout-stall", "timeout-wall", "launch-error"},
    the dsh ``session_id``, the final message text, the ``turn_end`` reason,
    exit code, duration and the heartbeat ledger summary.
    """
    for label, value in (("workspace", workspace), ("task", task),
                         ("evidence_dir", evidence_dir)):
        if value is None:
            raise DshRunError(f"{label} must be provided")
    if not str(task).strip():
        raise DshRunError("task must be a non-empty prompt string")
    for label, value in (("wall_timeout_s", wall_timeout_s),
                         ("stall_timeout_s", stall_timeout_s)):
        if not isinstance(value, (int, float)) or value <= 0:
            raise DshRunError(f"{label} must be positive seconds")
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
    ledger = ProgressLedger(
        evidence_dir / _LEDGER_NAME,
        stall_timeout_s=float(stall_timeout_s),
        wall_timeout_s=float(wall_timeout_s))
    started = time.monotonic()
    state: dict = {}
    stdout_error: dict = {}
    stderr_error: dict = {}
    argv = [str(dsh_path), "--profile", "headless", "--json", "-"]
    environment = dict(env) if env else None
    try:
        process = subprocess.Popen(  # argv is fully driver-controlled
            argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, text=True, cwd=str(workspace),
            env=environment)
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
    while True:
        try:
            process.wait(timeout=0.5)
            break
        except subprocess.TimeoutExpired:
            reason = ledger.expired()
            if reason is None:
                continue
            timed_out = reason
            process.terminate()
            try:
                process.wait(timeout=_TERMINATE_GRACE_S)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
            break
    stdout_pump.join(timeout=_TERMINATE_GRACE_S)
    stderr_pump.join(timeout=_TERMINATE_GRACE_S)
    stdin_writer.join(timeout=1.0)
    # Atomic finalize: rename the private partials onto their final names so
    # a crash never leaves a half-written evidence file behind.
    for partial, final in ((raw_stdout, events_file),
                           (raw_stderr, stderr_file)):
        if partial.exists():
            partial.replace(final)

    turn_end = state.get("turn_end_reason")
    final_text = state.get("final_text")
    exit_code = process.returncode
    if timed_out == "stall":
        outcome = "timeout-stall"
    elif timed_out == "wall":
        outcome = "timeout-wall"
    elif isinstance(turn_end, dict) and turn_end.get("kind") == "error":
        outcome = "error"
    elif exit_code != 0:
        outcome = "error"
    elif isinstance(final_text, str) and final_text.strip():
        outcome = "delivered"
    else:
        outcome = "error"
    error_text = None
    if outcome == "error":
        if isinstance(turn_end, dict):
            inner = turn_end.get("error")
            error_text = (inner.get("message") if isinstance(inner, dict)
                          else json.dumps(turn_end))[:500]
        else:
            error_text = f"exit {exit_code} with empty final message"
    ledger.finalize(outcome)
    receipt = {
        "outcome": outcome,
        "session_id": state.get("session_id"),
        "final_message": final_text if isinstance(final_text, str) else None,
        "turn_end_reason": turn_end,
        "exit_code": exit_code,
        "timed_out": timed_out,
        "error": error_text,
        "events_file": str(events_file),
        "stderr_file": str(stderr_file),
        "duration_s": round(time.monotonic() - started, 3),
        "progress": ledger.summary(),
    }
    ledger.close()
    if stdout_error.get("error"):
        receipt["stdout_pump_error"] = stdout_error["error"]
    if stderr_error.get("error"):
        receipt["stderr_pump_error"] = stderr_error["error"]
    return receipt
