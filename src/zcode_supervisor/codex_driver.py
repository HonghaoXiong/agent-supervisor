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

Public surface: :class:`CodexExecError`, :class:`CodexRunReceipt`,
:func:`build_argv`, :func:`normalize_strict_schema`, :func:`run_readonly`,
:func:`locate_rollout`, :func:`summarize_rollout`.
"""

from __future__ import annotations

import copy
import dataclasses
import json
import os
import re
import subprocess
import threading
import time
from collections import Counter
from pathlib import Path

SANDBOX_MODE = "read-only"

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


def _parse_events(lines) -> tuple[dict, str | None, str | None, list, int, list]:
    """Whitelist-parse JSONL events; unknown types are counted, never fatal.

    ``type == "error"`` events whose message starts with ``"Reconnecting"``
    are transport-layer retries (counted in ``transport_retries``), not task
    failures; every other error event contributes its message (truncated to
    300 chars) to ``hard_errors``. ``event_counts`` keeps the raw per-type
    counts either way.
    """
    counts: Counter = Counter()
    thread_id = None
    thread_id_any = None
    model_observed = None
    command_items: list = []
    transport_retries = 0
    hard_errors: list = []
    for raw in lines:
        try:
            event = json.loads(raw)
        except (json.JSONDecodeError, TypeError):
            counts["<invalid-json>"] += 1
            continue
        if not isinstance(event, dict):
            counts["<non-object>"] += 1
            continue
        etype = event.get("type")
        counts[str(etype) if etype is not None else "<no-type>"] += 1
        if etype == "error":
            message = event.get("message")
            text = message if isinstance(message, str) else "<error without message>"
            if text.startswith("Reconnecting"):
                transport_retries += 1
            else:
                hard_errors.append(text[:_HARD_ERROR_LIMIT])
        candidate = event.get("thread_id")
        if thread_id_any is None and isinstance(candidate, str) and candidate:
            thread_id_any = candidate
        if etype == "thread.started" and isinstance(candidate, str) and candidate:
            thread_id = thread_id or candidate
        nested = (event.get("payload"), event.get("item"))
        for source in (event, *(s for s in nested if isinstance(s, dict))):
            model = source.get("model")
            if isinstance(model, str) and model:
                model_observed = model
        item = event.get("item")
        if (
            etype == "item.completed"
            and isinstance(item, dict)
            and item.get("type") == "command_execution"
        ):
            summary = {
                key: item[key] for key in ("command", "exit_code", "status") if key in item
            }
            command_items.append(summary)
    return (dict(counts), thread_id or thread_id_any, model_observed,
            command_items, transport_retries, hard_errors)


def _stderr_excerpt(stderr_lines: list) -> str:
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

    stdout_lines: list = []
    stderr_lines: list = []
    timed_out = False
    with open(events_file, "w", encoding="utf-8") as events_log:
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
            target=_pump_stream, args=(process.stdout, stdout_lines), daemon=True
        )
        stderr_pump = threading.Thread(
            target=_pump_stream, args=(process.stderr, stderr_lines), daemon=True
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
        events_log.writelines(stdout_lines)  # persist raw lines only after they all arrived
    duration = time.monotonic() - started
    exit_code = process.returncode

    event_counts, thread_id, model_observed, command_items, transport_retries, hard_errors = \
        _parse_events(stdout_lines)

    error: str | None = None
    structured: dict | None = None
    if timed_out:
        outcome = "timeout"
        error = f"codex exec timed out after {timeout_s}s (exit {exit_code})"
    elif exit_code != 0:
        outcome = "failed"
        excerpt = _stderr_excerpt(stderr_lines)
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
    )
    (evidence_dir / RECEIPT_FILENAME).write_text(
        json.dumps(dataclasses.asdict(receipt), indent=2, sort_keys=True) + "\n",
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
    try:
        lines = index_path.read_text(encoding="utf-8", errors="replace").splitlines()
    except OSError:
        return hints
    for line in lines:
        try:
            entry = json.loads(line)
        except json.JSONDecodeError:
            continue
        if not isinstance(entry, dict):
            continue
        if not any(entry.get(field) == thread_id for field in _INDEX_ID_FIELDS):
            continue
        for field in _INDEX_PATH_FIELDS:
            value = entry.get(field)
            if isinstance(value, str) and value:
                hints.append(Path(value))
                break
    return hints


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
        with open(path, "r", encoding="utf-8", errors="replace") as handle:
            lines = handle.readlines()
    except OSError as exc:
        return {"path": str(path), "error": f"unreadable rollout: {exc}"}
    for line in lines:
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
    return {
        "path": str(path),
        "total_lines": total,
        "type_counts": dict(counts),
        "meta": meta,
        "items": items,
    }
