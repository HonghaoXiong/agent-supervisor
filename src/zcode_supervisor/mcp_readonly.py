"""Read-only MCP evidence interface for the control side (Codex).

Serves five confined tools over stdio (``python -m zcode_supervisor.mcp_readonly
--evidence-root DIR``). The server only reads frozen czs run records below one
evidence root; there are no write, execution, or arbitrary-path tools.

Contract invariants (reviewer-feedback-1 counterexamples all closed):

* **One constrained read path.** Every file reaches memory through
  :func:`snapshot`, which walks each component with ``lstat`` (a symlink
  anywhere on the chain is rejected), requires a regular file, and returns the
  exact bytes it hashes — no resolve-then-read split, so a swapped fixture can
  never serve old content under a new hash, and symlinks can never leak bytes
  from outside the root.
* **Listing is exact and continuable.** Only files named ``run-<N>.json`` are
  runs (``run-17.before.json`` is ignored, visibly counted); an opaque cursor
  continues past any cap, so 41 runs are all listable.
* **Pages bind an evidence version.** Every page carries ``evidence_version``
  (sha256 of the run.json snapshot it was cut from). Follow-up pages must echo
  ``expected_version``; a changed file is an explicit ``evidence-drift``
  rejection, never silently mixed content.
* **Full retrievability.** ``get_evidence_page`` pages the raw run.json and
  events bytes; ``get_decisions`` pages permission decisions (malformed
  entries rendered as ``{"malformed_entry": true}``); receipts with duplicate
  ``command_id`` are addressed by occurrence ``index``. A full-review run can
  therefore be completely retrieved through the interface.
* **The budget is the serialized payload.** Every response is measured as the
  serialized JSON text actually returned; pages shrink automatically so the
  inner payload stays within ``INNER_BUDGET_BYTES`` (MCP framing adds its own
  envelope on top, which is reported, not guessed).

Digests come from the accepted :mod:`zcode_supervisor.digest` contract;
serving never judges delivery and historical bindings stay honestly unknown
(full-review). No registration anywhere; per-process stdio only.
"""
from __future__ import annotations

import argparse
import errno
import hashlib
import json
import re
import sys
from pathlib import Path
from typing import Any

from . import __version__
# _command_detail is deliberately reused from the accepted digest contract so
# receipt exit/timeout/status facts and their malformed-container guards stay
# single-sourced instead of being re-implemented here.
from .digest import _command_detail, digest

# Budget layers: the tool result content is the serialized JSON text below;
# the MCP JSON-RPC envelope roughly doubles it on the wire (measured live:
# 6735 inner bytes -> 12950 wire bytes). Both layers are reported.
INNER_BUDGET_BYTES = 4096
WIRE_BUDGET_NOTE = "inner payload budget; MCP JSON-RPC framing adds ~2x on the wire (measured 6735B inner -> 12950B wire)"
PAGE_MAX_CHARS = 2000
PAGE_DEFAULT_CHARS = 1000
LIST_PAGE = 40
RUN_ID_MAX = 128
COMMAND_ID_MAX = 128

_RUN_ID = re.compile(r"^([A-Za-z0-9][A-Za-z0-9._-]{0,62})/run-([0-9]{1,8})$")
_RUN_FILE = re.compile(r"^run-([0-9]{1,8})\.json$")
_COMMAND_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,126}$")
_VERSION = re.compile(r"^[0-9a-f]{64}$")


class EvidenceError(Exception):
    """Controlled, caller-visible rejection with a machine-readable code."""

    def __init__(self, code: str, detail: str = ""):
        super().__init__(f"{code}: {detail}" if detail else code)
        self.code = code
        self.detail = detail

    def payload(self) -> dict[str, Any]:
        value = {"error": self.code}
        if self.detail:
            value["detail"] = self.detail[:300]
        return value


def snapshot(path: Path, root: Path, label: str) -> bytes:
    """The single constrained read: symlink-free, root-confined, read==hashed.

    Components below the root are walked openat-style with O_NOFOLLOW, the
    final file is opened once with O_NOFOLLOW, fstat'ed on the SAME descriptor
    and read through that descriptor: a path swapped between check and read
    cannot serve different bytes than the ones hashed (no lstat/read split, no
    re-resolution). The returned bytes are exactly what callers hash and serve.
    """
    import os
    import stat as stat_module
    parts = path.relative_to(root).parts if path != root else ()
    fd = os.open(root, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        for index, part in enumerate(parts):
            is_last = index == len(parts) - 1
            flags = (os.O_RDONLY | os.O_NOFOLLOW | (0 if is_last else os.O_DIRECTORY))
            try:
                nxt = os.open(part, flags, dir_fd=fd)
            except OSError as error:
                if error.errno in (errno.ELOOP, errno.EMLINK) if 'errno' in dir() else False:
                    raise EvidenceError("symlink-rejected",
                                        f"{label}: {part} is a symlink") from error
                raise EvidenceError("evidence-missing",
                                    f"{label}: {part}: {type(error).__name__}") from error
            os.close(fd)
            fd = nxt
        info = os.fstat(fd)
        if not stat_module.S_ISREG(info.st_mode):
            raise EvidenceError("evidence-missing", f"{label}: not a regular file")
        chunks = []
        while True:
            chunk = os.read(fd, 1 << 20)
            if not chunk:
                break
            chunks.append(chunk)
        return b"".join(chunks)
    finally:
        os.close(fd)


def validate_root(root: Path) -> Path:
    """Resolve the single evidence root once (the root itself may be a link)."""
    resolved = Path(root).resolve()
    if not resolved.is_dir():
        raise EvidenceError("root-not-found", str(resolved))
    return resolved


def resolve_run(root: Path, run_id: Any) -> tuple[Path, Path]:
    """Map a validated run_id to (run.json, run.events.jsonl) inside root."""
    root = Path(root).resolve()
    if not isinstance(run_id, str) or not run_id or len(run_id) > RUN_ID_MAX:
        raise EvidenceError("invalid-run-id", "run_id must be a short '<task>/run-<N>' string")
    match = _RUN_ID.match(run_id)
    if not match:
        raise EvidenceError(
            "invalid-run-id",
            "run_id must be '<task>/run-<N>' with [A-Za-z0-9._-] task names; "
            "traversal, absolute paths and nested paths are rejected",
        )
    task, number = match.groups()
    directory = (root / task).resolve()
    if directory == root or not directory.is_relative_to(root):
        raise EvidenceError("invalid-run-id", "resolved task directory escapes the evidence root")
    run_path = (directory / f"run-{number}.json").resolve()
    if run_path != directory / f"run-{number}.json":
        raise EvidenceError("invalid-run-id", "run file path is not canonical inside its task directory")
    if not run_path.is_relative_to(root):
        raise EvidenceError("invalid-run-id", "resolved run file escapes the evidence root")
    if not run_path.is_file():
        raise EvidenceError("run-not-found", f"{task}/run-{number}.json is not a file inside the root")
    events_path = run_path.with_name(f"run-{number}.events.jsonl")
    return run_path, events_path


def _versioned_run_bytes(root: Path, run_id: str, expected_version: Any,
                         continuation: bool = False) -> tuple[bytes, Path, Path, str]:
    """Snapshot run.json, verify an echoed version, and return the binding.

    Continuation pages (offset > 0) MUST echo the evidence_version: serving
    drifted content because the caller omitted the version is exactly the
    race the pagination contract exists to close.
    """
    run_path, events_path = resolve_run(root, run_id)
    data = snapshot(run_path, root, "run.json")
    version = hashlib.sha256(data).hexdigest()
    if continuation and expected_version is None:
        raise EvidenceError("version-required",
                            "continuation pages must echo the evidence_version from the first page")
    if expected_version is not None:
        if not isinstance(expected_version, str) or not _VERSION.match(expected_version):
            raise EvidenceError("invalid-argument", "expected_version must be a 64-hex sha256")
        if expected_version != version:
            raise EvidenceError("evidence-drift",
                                f"run.json changed since your last page: was {expected_version}, now {version}; "
                                "restart paging from page one against the new version")
    return data, run_path, events_path, version


def _budgeted(value: Any, what: str) -> str:
    """Serialize within the inner budget or refuse loudly, never silently."""
    text = json.dumps(value, ensure_ascii=False)
    if len(text.encode("utf-8")) > INNER_BUDGET_BYTES:
        raise EvidenceError("output-budget-exceeded",
                            f"{what} exceeded the {INNER_BUDGET_BYTES}-byte inner budget; "
                            "request a smaller page")
    return text


def _fit(build):
    """Build a payload with a page limit that fits the serialized budget.

    ``build(limit)`` returns the payload for a given page size; the limit is
    halved (down to a floor) until the serialized form fits, so an emoji page
    can never exceed the budget — it shrinks.
    """
    limit = None
    payload = build(limit)
    text = json.dumps(payload, ensure_ascii=False)
    if len(text.encode("utf-8")) <= INNER_BUDGET_BYTES:
        return json.loads(text)
    for candidate in (1024, 512, 256, 128, 64):
        try:
            payload = build(candidate)
        except EvidenceError as error:
            if error.code == "offset-out-of-range":
                raise
            continue
        text = json.dumps(payload, ensure_ascii=False)
        if len(text.encode("utf-8")) <= INNER_BUDGET_BYTES:
            return json.loads(text)
    raise EvidenceError("output-budget-exceeded",
                        f"even a {64}-character page exceeds the {INNER_BUDGET_BYTES}-byte inner budget")


def _fit_count(build, default_count):
    """Build a payload with an item count that fits the serialized budget."""
    payload = build(default_count)
    if len(json.dumps(payload, ensure_ascii=False).encode("utf-8")) <= INNER_BUDGET_BYTES:
        return payload
    for count in (max(1, default_count // 2), max(1, default_count // 4), 1):
        payload = build(count)
        if len(json.dumps(payload, ensure_ascii=False).encode("utf-8")) <= INNER_BUDGET_BYTES:
            return payload
    raise EvidenceError("output-budget-exceeded",
                        "even a single item exceeds the inner budget")


def _parse_run(data: bytes, label: str) -> dict[str, Any]:
    try:
        parsed = json.loads(data)
    except ValueError as error:
        raise EvidenceError("malformed-evidence", f"{label} unreadable JSON: {type(error).__name__}") from error
    if not isinstance(parsed, dict):
        raise EvidenceError("malformed-evidence", f"{label} top level is not an object")
    return parsed


def _task_name(value: Any) -> bool:
    return (isinstance(value, str) and bool(re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9._-]{0,62}", value))
            and value not in {".", ".."})


def _run_summary(run_id: str, data: dict[str, Any], size: int) -> dict[str, Any]:
    return {
        "run_id": run_id,
        "outcome": data.get("outcome", "unknown") if isinstance(data.get("outcome"), str) else "unknown",
        "delivery_ok": data.get("delivery_ok", "unknown") if isinstance(data.get("delivery_ok"), (bool, str)) else "unknown",
        "session_id": data.get("session_id") if isinstance(data.get("session_id"), str) else None,
        "turn_id": data.get("turn_id") if isinstance(data.get("turn_id"), str) else None,
        "run_bytes": size,
    }


def list_runs_impl(root: Path, task: Any = None, cursor: Any = None) -> dict[str, Any]:
    """Exact, continuable listing: only run-<N>.json counts as a run."""
    root = validate_root(root)
    if cursor is not None and not isinstance(cursor, str):
        raise EvidenceError("invalid-argument", "cursor must be an opaque string from a previous page")
    if task is not None and (not isinstance(task, str) or not _task_name(task)):
        raise EvidenceError("invalid-argument", "task must be a plain [A-Za-z0-9._-] directory name")
    directories = ([validate_task_dir(root, task)] if task is not None
                   else sorted((entry for entry in root.iterdir()
                                if entry.is_dir() and not entry.name.startswith(".")),
                               key=lambda p: p.name))
    entries: list[dict[str, Any]] = []
    ignored = 0
    for directory in directories:
        for path in sorted(directory.iterdir(), key=lambda p: p.name) if directory.is_dir() else []:
            if not _RUN_FILE.match(path.name):
                if path.name.endswith(".json"):
                    ignored += 1
                continue
            try:
                data = _parse_run(snapshot(path, root, path.name), path.name)
            except EvidenceError:
                data = {}
                entries.append({"run_id": f"{directory.name}/{path.stem}",
                                "outcome": "malformed", "delivery_ok": "unknown",
                                "session_id": None, "turn_id": None,
                                "run_bytes": path.stat().st_size})
                continue
            entries.append(_run_summary(f"{directory.name}/{path.stem}", data, path.stat().st_size))
    # Numeric order within a task, task order across tasks; the cursor is the
    # last run_id of the previous page.
    def sort_key(entry):
        task_part, _, number = entry["run_id"].partition("/run-")
        return (task_part, int(number) if number.isdigit() else 1 << 30)
    entries.sort(key=sort_key)
    start = 0
    if cursor is not None:
        ids = [entry["run_id"] for entry in entries]
        if cursor not in ids:
            raise EvidenceError("cursor-drift",
                                "cursor names a run that is not in the current listing; restart from page one")
        start = ids.index(cursor) + 1
    def build(count):
        page = entries[start:start + count]
        built: dict[str, Any] = {
            "runs": page,
            "total_runs": len(entries),
            "returned": len(page),
            "ignored_non_run_json_files": ignored,
            "read_only": True,
            "root": str(root),
        }
        if start + len(page) < len(entries):
            built["next_cursor"] = page[-1]["run_id"]
            built["continuation"] = "pass next_cursor as cursor to continue"
        return built
    return json.loads(json.dumps(_fit_count(build, LIST_PAGE), ensure_ascii=False))


def validate_task_dir(root: Path, task: str) -> Path:
    directory = (root / task).resolve()
    if directory == root or not directory.is_relative_to(root):
        raise EvidenceError("invalid-run-id", "resolved task directory escapes the evidence root")
    if not directory.is_dir():
        raise EvidenceError("task-not-found", f"no task directory named {task!r} inside the root")
    return directory


def get_digest_impl(root: Path, run_id: Any) -> dict[str, Any]:
    """Serve one digest; both evidence files come from constrained snapshots.

    The digest contract reads paths itself, so it is fed private temp copies
    of the exact snapshot bytes we hashed (read==hashed==served); the served
    locators and hashes are then rewritten to the real paths and snapshots.
    """
    import tempfile
    root = validate_root(root)
    data, run_path, events_path, version = _versioned_run_bytes(root, run_id, None)
    _parse_run(data, "run.json")
    events_bytes = None
    events_state = "unknown"
    if events_path.is_symlink():
        events_state = "rejected:symlink-rejected"
    elif events_path.exists():
        try:
            events_bytes = snapshot(events_path, root, "events.jsonl")
            events_state = "present"
        except EvidenceError as error:
            events_state = f"rejected:{error.code}"
    temps = []
    try:
        with tempfile.NamedTemporaryFile(suffix=".json", delete=False) as handle:
            handle.write(data)
            run_tmp = Path(handle.name)
        temps.append(run_tmp)
        if events_bytes is not None:
            with tempfile.NamedTemporaryFile(suffix=".jsonl", delete=False) as handle:
                handle.write(events_bytes)
                events_tmp = Path(handle.name)
            temps.append(events_tmp)
        else:
            events_tmp = run_tmp.parent / (run_tmp.name + ".absent.events.jsonl")
        try:
            result = digest(run_tmp, events_tmp, None, None,
                            bindings_note="read-only serving; no execution-time binding capture is wired, "
                                          "so no trusted association to historical execution bindings is claimed")
        except (ValueError, TypeError, KeyError, AttributeError) as error:
            raise EvidenceError("malformed-evidence",
                                f"digest contract rejected the record: {type(error).__name__}") from error
    finally:
        for temp in temps:
            temp.unlink(missing_ok=True)
    result["run_file"] = {"path": str(run_path), "bytes": len(data),
                          "sha256": hashlib.sha256(data).hexdigest()}
    if events_bytes is not None:
        result["events_file"] = {"path": str(events_path),
                                 "bytes": len(events_bytes),
                                 "sha256": hashlib.sha256(events_bytes).hexdigest()}
    else:
        result["events_file"] = events_state
    result["evidence_version"] = version
    result["full_review_retrieval"] = {
        "run_bytes": "get_evidence_page(run_id, 'run')",
        "events_bytes": "get_evidence_page(run_id, 'events')",
        "permission_decisions": "get_decisions(run_id, offset, limit, expected_version)",
        "note": "a full-review run can be retrieved completely through these paged tools",
    }
    failures = result.get("failures")
    if isinstance(failures, dict) and isinstance(failures.get("denials"), list):
        all_denials = failures["denials"]
        total_denials = len(all_denials)
        for shown in (10, 4, 1, 0):
            failures["denials"] = all_denials[:shown]
            failures["denials_summary"] = {
                "shown": min(shown, total_denials),
                "total": total_denials,
                "note": "digest shows a bounded excerpt; retrieve every decision via get_receipt "
                        "with object='decisions'",
                "retrievable_via": "get_receipt(run_id, object='decisions', ...)",
            }
            try:
                return json.loads(_budgeted(result, "get_digest"))
            except EvidenceError:
                if shown == 0:
                    raise
        raise EvidenceError("output-budget-exceeded", "digest exceeds the inner budget even without denials")
    return json.loads(_budgeted(result, "get_digest"))


def _receipt_entries(parsed: dict[str, Any], command_id: str) -> list[tuple[int, dict[str, Any]]]:
    """All receipt occurrences for a command_id, guarded per container entry."""
    commands = parsed.get("commands")
    if not isinstance(commands, list):
        raise EvidenceError("malformed-evidence", "commands is not a list")
    found: list[tuple[int, dict[str, Any]]] = []
    for position, candidate in enumerate(commands):
        if isinstance(candidate, dict) and candidate.get("command_id") == command_id:
            found.append((position, candidate))
    if not found:
        known = sorted({c.get("command_id") for c in commands
                        if isinstance(c, dict) and isinstance(c.get("command_id"), str)})
        raise EvidenceError("command-not-found", f"available: {', '.join(known) or '(none)'}")
    return found


def get_receipt_impl(root: Path, run_id: Any, command_id: Any, index: Any = 0,
                     offset: Any = 0, limit: Any = PAGE_DEFAULT_CHARS,
                     expected_version: Any = None) -> dict[str, Any]:
    """One receipt page, occurrence-indexed and version-bound."""
    root = validate_root(root)
    if not isinstance(command_id, str) or not command_id or len(command_id) > COMMAND_ID_MAX \
            or not _COMMAND_ID.match(command_id):
        raise EvidenceError("invalid-command-id", "command_id must be a short [A-Za-z0-9._-] string")
    if isinstance(index, bool) or not isinstance(index, int) or index < 0:
        raise EvidenceError("invalid-argument", "index must be a non-negative integer (occurrence of command_id)")
    if isinstance(offset, bool) or not isinstance(offset, int) or offset < 0:
        raise EvidenceError("invalid-argument", "offset must be a non-negative integer")
    if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= PAGE_MAX_CHARS:
        raise EvidenceError("invalid-argument", f"limit must be an integer in 1..{PAGE_MAX_CHARS}")
    data, run_path, events_path, version = _versioned_run_bytes(
        root, run_id, expected_version, continuation=offset > 0 or index > 0)
    parsed = _parse_run(data, "run.json")
    occurrences = _receipt_entries(parsed, command_id)
    if index >= len(occurrences):
        raise EvidenceError("index-out-of-range",
                            f"command_id has {len(occurrences)} occurrence(s); index {index} is out of range")
    position, entry = occurrences[index]
    result_container = entry.get("result") if isinstance(entry.get("result"), dict) else {}
    content = result_container.get("content")
    if not isinstance(content, str):
        raise EvidenceError("malformed-evidence", "receipt content is not a string")
    detail = _command_detail(entry)
    if offset > len(content):
        raise EvidenceError("offset-out-of-range",
                            f"offset {offset} is beyond the receipt length of {len(content)} characters")
    def build(fit_limit):
        cut = limit if fit_limit is None else fit_limit
        page = content[offset:offset + cut]
        return {
        "run_id": f"{run_path.parent.name}/{run_path.stem}",
        "command_id": command_id,
        "occurrence_index": index,
        "commands_position": position,
        "tool_call_id": entry.get("tool_call_id") if isinstance(entry.get("tool_call_id"), str) else None,
        "receipt": {
            "content_page": page,
            "units": "characters",
            "offset": offset,
            "limit": limit,
            "returned_chars": len(page),
            "content_chars": len(content),
            "content_sha256": hashlib.sha256(content.encode("utf-8", "surrogatepass")).hexdigest(),
            "has_more": offset + len(page) < len(content),
            "next_offset": offset + len(page) if offset + len(page) < len(content) else None,
            "effective_limit": cut,
            "exit_code": detail.get("exitCode", "unknown"),
            "timed_out": detail.get("timedOut", "unknown"),
            "status": detail.get("status", "unknown"),
            "truncated_by_recorder": bool(result_container.get("truncated")),
            "original_bytes": result_container.get("originalBytes", "unknown"),
            "returned_bytes": result_container.get("returnedBytes", "unknown"),
        },
        "evidence_version": version,
        "occurrences_total": len(occurrences),
        "events_file_present": events_path.is_file() and not events_path.is_symlink(),
        "read_only": True,
        }
    return _fit(build)


def _unused_receipt_tail():  # pragma: no cover - documentation anchor
    return None


def get_evidence_page_impl(root: Path, run_id: Any, object_name: Any,
                           offset: Any = 0, limit: Any = PAGE_DEFAULT_CHARS,
                           expected_version: Any = None, encoding: Any = "text",
                           expected_object_version: Any = None) -> dict[str, Any]:
    """Raw evidence bytes as text pages — full-review runs stay fully retrievable."""
    root = validate_root(root)
    if object_name not in ("run", "events"):
        raise EvidenceError("invalid-argument", "object must be 'run' or 'events'")
    if isinstance(offset, bool) or not isinstance(offset, int) or offset < 0:
        raise EvidenceError("invalid-argument", "offset must be a non-negative integer")
    if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= PAGE_MAX_CHARS:
        raise EvidenceError("invalid-argument", f"limit must be an integer in 1..{PAGE_MAX_CHARS}")
    data, run_path, events_path, version = _versioned_run_bytes(
        root, run_id, expected_version, continuation=offset > 0)
    if object_name == "events":
        if events_path.is_symlink():
            raise EvidenceError("symlink-rejected", "events file is a symlink; not served")
        raw = snapshot(events_path, root, "events.jsonl")
        events_version = hashlib.sha256(raw).hexdigest()
        if offset > 0 and expected_object_version is None:
            raise EvidenceError("version-required",
                                "events continuation pages must echo the object_version from "
                                "the first events page")
        if expected_object_version is not None:
            if not _VERSION.match(expected_object_version):
                raise EvidenceError("invalid-argument", "expected_object_version must be 64-hex")
            if expected_object_version != events_version:
                raise EvidenceError("evidence-drift",
                                    f"events file changed since your last page: was "
                                    f"{expected_object_version}, now {events_version}")
        label = str(events_path)
    else:
        raw = data
        label = str(run_path)
    if encoding not in ("text", "base64"):
        raise EvidenceError("invalid-argument", "encoding must be text or base64")
    if encoding == "base64":
        # byte-exact paging: whole-file base64 in chunks of raw bytes; decoded
        # concatenation reproduces the original bytes exactly (no replacement).
        if offset > len(raw):
            raise EvidenceError("offset-out-of-range",
                                f"offset {offset} beyond {len(raw)} bytes")
        def build_b64(fit_limit):
            cut = limit if fit_limit is None else fit_limit
            chunk = raw[offset:offset + cut]
            import base64 as _b64
            return {
                "run_id": f"{run_path.parent.name}/{run_path.stem}",
                "object": object_name, "encoding": "base64",
                "page": _b64.b64encode(chunk).decode("ascii"),
                "offset": offset, "limit": cut,
                "returned_bytes": len(chunk),
                "total_bytes": len(raw),
                "object_sha256": hashlib.sha256(raw).hexdigest(),
                "has_more": offset + len(chunk) < len(raw),
                "next_offset": offset + len(chunk) if offset + len(chunk) < len(raw) else None,
                "evidence_version": version,
                "object_version": events_version if object_name == "events" else version,
                "version_scope": ("events file" if object_name == "events" else "run.json"),
                "read_only": True,
            }
        return _fit(build_b64)
    text = raw.decode("utf-8", "replace")
    if offset > len(text):
        raise EvidenceError("offset-out-of-range",
                            f"offset {offset} is beyond the {len(text)} characters of {object_name}")
    def build(fit_limit):
        cut = limit if fit_limit is None else fit_limit
        page = text[offset:offset + cut]
        return {
            "run_id": f"{run_path.parent.name}/{run_path.stem}",
            "object": object_name,
            "page": page,
            "offset": offset,
            "limit": cut,
            "returned_chars": len(page),
            "total_chars": len(text),
            "total_bytes": len(raw),
            "object_sha256": hashlib.sha256(raw).hexdigest(),
            "has_more": offset + len(page) < len(text),
            "next_offset": offset + len(page) if offset + len(page) < len(text) else None,
            "evidence_version": version,
            "object_version": events_version if object_name == "events" else version,
            "version_scope": ("events file" if object_name == "events" else "run.json"),
            "decoded": "utf-8 with replacement for non-utf8 bytes; object_sha256 is over the raw bytes; "
                       "binary evidence must be retrieved via the base64 variant before any claim of "
                       "byte-exact reproduction",
            "path": label,
            "read_only": True,
        }
    return _fit(build)


def get_decisions_impl(root: Path, run_id: Any, offset: Any = 0, limit: Any = 20,
                       expected_version: Any = None) -> dict[str, Any]:
    """Page permission decisions; malformed entries stay visible, never crash."""
    root = validate_root(root)
    if isinstance(offset, bool) or not isinstance(offset, int) or offset < 0:
        raise EvidenceError("invalid-argument", "offset must be a non-negative integer")
    if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 50:
        raise EvidenceError("invalid-argument", "limit must be an integer in 1..50")
    data, run_path, events_path, version = _versioned_run_bytes(
        root, run_id, expected_version, continuation=offset > 0)
    parsed = _parse_run(data, "run.json")
    decisions = parsed.get("permission_decisions")
    if not isinstance(decisions, list):
        state = "absent" if decisions is None else "malformed"
        raise EvidenceError("malformed-evidence",
                            f"permission_decisions is {state}; retrieve the raw run via get_evidence_page")
    cleaned = []
    malformed = 0
    unknown = 0
    for entry in decisions:
        if isinstance(entry, dict):
            cleaned.append({
                "decision": entry.get("decision"),
                "tool": entry.get("tool"),
                "command_id": entry.get("command_id"),
                "reason": entry.get("reason") if isinstance(entry.get("reason"), str) else None,
                "origin_child_session": ((entry.get("origin") or {}).get("childSessionId")
                                         if isinstance(entry.get("origin"), dict) else None),
            })
        else:
            malformed += 1
            cleaned.append({"malformed_entry": True, "repr": repr(entry)[:80]})
            continue  # mutually exclusive: malformed entries count as malformed only
        decision_value = entry.get("decision")
        if not isinstance(decision_value, str):
            unknown += 1
            cleaned[-1]["unknown_decision"] = True
    total = len(cleaned)
    if offset > total:
        raise EvidenceError("offset-out-of-range", f"offset {offset} beyond {total} decisions")
    counts: dict[str, int] = {}
    unknown = 0
    for entry in cleaned:
        if entry.get("malformed_entry"):
            continue  # counted as malformed already — never double-counted
        value = entry.get("decision")
        if isinstance(value, str):
            counts[value] = counts.get(value, 0) + 1
        else:
            unknown += 1
    def build(count):
        page = cleaned[offset:offset + count]
        return {
        "run_id": f"{run_path.parent.name}/{run_path.stem}",
        "decisions": page,
        "offset": offset,
        "limit": count,
        "returned": len(page),
        "total": total,
        "counts": counts,
        "unknown_or_malformed": {"unknown_decision_values": unknown,
                                 "malformed_entries": malformed},
        "malformed_entries": malformed,
        "has_more": offset + len(page) < total,
        "next_offset": offset + len(page) if offset + len(page) < total else None,
        "evidence_version": version,
        "read_only": True,
        }
    return json.loads(json.dumps(_fit_count(build, min(limit, 50)), ensure_ascii=False))


def _tools() -> list[dict[str, Any]]:
    return [
        {
            "name": "list_runs",
            "description": "Exact, continuable listing of frozen czs run records below the confined "
                           "evidence root (only run-<N>.json counts; continuation via next_cursor).",
            "inputSchema": {"type": "object", "properties": {
                "task": {"type": "string", "description": "optional task directory name"},
                "cursor": {"type": "string", "description": "next_cursor from the previous page"},
            }, "additionalProperties": False},
        },
        {
            "name": "get_digest",
            "description": "Compact evidence digest for one run (accepted digest contract; historical "
                           "runs are full-review with bindings honestly unknown). Includes "
                           "evidence_version and paged-retrieval pointers for everything it summarizes.",
            "inputSchema": {"type": "object", "properties": {
                "run_id": {"type": "string", "description": "<task>/run-<N>"},
            }, "required": ["run_id"], "additionalProperties": False},
        },
        {
            "name": "get_receipt",
            "description": "One raw command receipt page (exit/timeout/status/output text). Duplicate "
                           "command_id occurrences are addressed by index; pages are bound to the "
                           "run.json evidence_version they were cut from. With object='run'|'events' "
                           "it pages the raw evidence (base64=true for byte-exact retrieval); with "
                           "object='decisions' it pages permission decisions with counts.",
            "inputSchema": {"type": "object", "properties": {
                "run_id": {"type": "string", "description": "<task>/run-<N>"},
                "command_id": {"type": "string"},
                "object": {"type": "string", "enum": ["receipt", "run", "events", "decisions"]},
                "encoding": {"type": "string", "enum": ["text", "base64"]},
                "index": {"type": "integer", "minimum": 0, "description": "occurrence of command_id (default first)"},
                "offset": {"type": "integer", "minimum": 0},
                "limit": {"type": "integer", "minimum": 1, "maximum": PAGE_MAX_CHARS},
                "expected_version": {"type": "string", "description": "evidence_version from a previous page"},
                "expected_object_version": {"type": "string", "description": "object_version (events) from a previous page"},
            }, "required": ["run_id"], "additionalProperties": False},
        },
    ]

    # The evidence/decision surfaces are controlled variants of get_receipt:
    # exposed as get_receipt with object=run|events|decisions (the authorized
    # three-tool surface stays list_runs / get_digest / get_receipt).
    _TOOLS_RECEIPT_VARIANTS_NOTE = ("get_receipt also serves object='run'|'events' "
                                    "(base64=true for byte-exact) and object='decisions'")


def build_server(root: Path):
    """Wire the five read-only tools onto one MCP lowlevel server."""
    from mcp.server import Server
    import mcp.types as types

    async def on_list_tools(ctx, params):  # noqa: ANN001 - SDK context type
        return types.ListToolsResult(tools=[
            types.Tool(name=tool["name"], description=tool["description"],
                       input_schema=tool["inputSchema"]) for tool in _tools()
        ])

    async def on_call_tool(ctx, params):  # noqa: ANN001 - SDK context type
        name = params.name
        arguments = dict(params.arguments or {})
        try:
            if name == "list_runs":
                value = list_runs_impl(root, arguments.get("task"), arguments.get("cursor"))
            elif name == "get_digest":
                value = get_digest_impl(root, arguments.get("run_id"))
            elif name == "get_receipt":
                obj = arguments.get("object", "receipt")
                if obj == "receipt":
                    value = get_receipt_impl(root, arguments.get("run_id"),
                                             arguments.get("command_id"),
                                             arguments.get("index", 0), arguments.get("offset", 0),
                                             arguments.get("limit", PAGE_DEFAULT_CHARS),
                                             arguments.get("expected_version"))
                elif obj in ("run", "events"):
                    value = get_evidence_page_impl(root, arguments.get("run_id"), obj,
                                                   arguments.get("offset", 0),
                                                   arguments.get("limit", PAGE_DEFAULT_CHARS),
                                                   arguments.get("expected_version"),
                                                   encoding=arguments.get("encoding", "text"),
                                                   expected_object_version=arguments.get(
                                                       "expected_object_version"))
                elif obj == "decisions":
                    value = get_decisions_impl(root, arguments.get("run_id"),
                                               arguments.get("offset", 0),
                                               arguments.get("limit", 20),
                                               arguments.get("expected_version"))
                else:
                    raise EvidenceError("invalid-argument", "object must be receipt|run|events|decisions")
            else:
                raise EvidenceError("unknown-tool", name)
        except EvidenceError as error:
            return types.CallToolResult(
                content=[types.TextContent(type="text", text=json.dumps(error.payload(), ensure_ascii=False))],
                is_error=True)
        except (ValueError, TypeError, KeyError, AttributeError, OSError) as error:
            return types.CallToolResult(
                content=[types.TextContent(type="text", text=json.dumps(
                    {"error": "unexpected-evidence-failure",
                     "detail": f"{type(error).__name__}: {error}"[:300]}, ensure_ascii=False))],
                is_error=True)
        return types.CallToolResult(
            content=[types.TextContent(type="text", text=json.dumps(value, ensure_ascii=False))])

    return Server("czs-readonly-evidence", version=__version__,
                  on_list_tools=on_list_tools, on_call_tool=on_call_tool)


async def serve(root: Path) -> None:
    """Run the stdio server for one process lifetime."""
    import anyio
    from mcp.server.stdio import stdio_server

    root = validate_root(root)
    server = build_server(root)
    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream, server.create_initialization_options())


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    result.add_argument("--evidence-root", type=Path, required=True,
                        help="Directory holding <task>/run-<N>.json records; the only readable tree")
    return result


def main(argv=None) -> int:
    args = parser().parse_args(argv)
    import anyio
    try:
        root = validate_root(args.evidence_root)
    except EvidenceError as error:
        print(json.dumps(error.payload()), file=sys.stderr)
        return 2
    anyio.run(serve, root)
    return 0


if __name__ == "__main__":  # pragma: no cover - process entry
    raise SystemExit(main())
