"""Three-way orchestration hub MCP server (stdio): Codex / ZCode / dsh workers.

The hub is the single dispatch point a decision-layer agent (any installed
agent) drives over MCP. Shape:

* **Two workers configured, each with two modes** — zcode (``cold`` via the
  czs protocol executor, ``resident`` on one shared resident process) and
  codex (``exec`` via ``codex exec -s read-only``, ``app-server`` with
  approval callbacks brokered through the contract Authority); ``dsh`` is
  always reported absent and is never dispatched;
* **Write paths follow the contract layer's live-verified mode rules** (see
  :mod:`zcode_supervisor.contracts`): zcode brokers allowed_files/commands in
  both modes, codex only in app-server mode under workspace-write;
* **Shared frozen workspaces** — the czs inventory-hash freeze still governs
  zcode runs, and codex runs take the same per-workspace advisory lock so
  writes to one workspace stay serialized across vendors;
* **Resident singletons** — one resident ZCode process and one codex
  app-server client serve every delegate for the server lifetime (lazy start
  on first use, best-effort close at shutdown); app-server delegates run
  concurrently on the shared client behind thread-filtered approval
  subscriptions (2026-10-26 P3; ``appserver_concurrency=1`` restores the P2
  serial gate);
* **Credentials stay with each vendor** — the hub never reads or writes
  auth.json, API keys, or any credential file;
* **MCP exposure is not approval** — ``delegate`` only runs a contract a human
  previously registered under ``state_dir/hub/contracts`` (see
  :mod:`zcode_supervisor.contracts`).

Cross-vendor session data leaves only as conservative structured summaries
(:mod:`zcode_supervisor.session_reader`): field whitelists, truncated text,
pagination — raw session payloads never leave the evidence stores.

State layout (all hub writes are confined to these trees):

* ``state_dir/hub/contracts/<contract_id>.json`` — human-registered contracts;
* ``state_dir/hub/tasks/<task_id>.json`` — one record per dispatched task;
* ``state_dir/hub/tasks/index.jsonl`` — append-only task index;
* ``state_dir/hub/health.json`` — written by the (later) revalidate tool; the
  format read here is
  ``{"generated": "<iso>", "workers": {"<name>": {"enabled": bool,
  "status": "ok|changed|absent|error", "reason": str, "probe_ref": str}}}``;
  a missing file means every *installed* worker is ``status "unknown"`` with
  ``enabled true``;
* ``evidence_root/tasks/<task_id>/`` — codex dispatch evidence (exec receipts,
  app-server packets/receipts);
* ``evidence_root/appserver/`` — shared codex app-server client evidence
  (frames, stderr; one client per server lifetime);
* ``work_root/hub-tasks/<task_id>/`` — zcode packet + run.json evidence.
"""

from __future__ import annotations

import argparse
import asyncio
import concurrent.futures
import contextlib
import inspect
import json
import os
import re
import shutil
import stat
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

from . import __version__, common, contracts
from .authority import Authority, inventory
from .codex_driver import locate_rollout, run_readonly
from .deadline import CallBudget
from .session_reader import read_context as read_session_context
from . import ui_projection
from . import worker_health

__all__ = [
    "HubError",
    "ROUTING_NOTE",
    "ResidentWorkers",
    "build_server",
    "serve",
    "parser",
    "main",
    "health_impl",
    "list_workers_impl",
    "list_sessions_impl",
    "read_context_impl",
    "list_skills_impl",
    "delegate_impl",
    "default_zcode_runner",
]

HUB_TOOL_VERSION = 1
ROUTING_NOTE = ("degraded routing does not guarantee worker equivalence "
                "(model/skills/workflows differ)")
HEALTH_FILENAME = "health.json"
SESSIONS_PAGE_DEFAULT = 20
SESSIONS_PAGE_MAX = 50
CONTEXT_MAX_ITEMS_MAX = 50
SKILLS_NOTE = "names and source roots only; SKILL.md bodies are never read"

DEFAULT_SKILL_ROOTS: dict[str, tuple[tuple[Path, str], ...]] = {
    "codex": (
        (Path.home() / ".codex" / "skills", "*/SKILL.md"),
        (Path.home() / ".agents" / "skills", "*/SKILL.md"),
    ),
    "zcode": (
        (Path.home() / ".zcode" / "skills", "*/SKILL.md"),
        (Path("/Applications/ZCode.app/Contents/Resources/glm/packages/"
              "bundled-skills/skills"), "*/SKILL.md"),
        (Path.home() / ".zcode" / "cli" / "plugins" / "cache",
         "**/skills/*/SKILL.md"),
    ),
}

_KNOWN_CODEX_APP_PATH = Path(
    "/Applications/ChatGPT.app/Contents/Resources/codex-cli/CodexCLI.app/"
    "Contents/MacOS/codex")
_TASK_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9-]{0,63}$")


class HubError(Exception):
    """Controlled, caller-visible rejection with a machine-readable code."""

    def __init__(self, code: str, detail: str = ""):
        super().__init__(f"{code}: {detail}" if detail else code)
        self.code = code
        self.detail = detail

    def payload(self) -> dict:
        value = {"hub_tool_version": HUB_TOOL_VERSION, "error": self.code}
        if self.detail:
            value["detail"] = self.detail[:500]
        return value


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# --------------------------------------------------------------------------
# Worker discovery: tri_probe when the module is importable, minimal fallback
# discovery otherwise. The fallback never guesses beyond the documented
# locations (known app bundle path, PATH lookup, czs-configured cli_path).
# --------------------------------------------------------------------------

def _normalized_discovery(found, fallback: dict) -> dict:
    if not isinstance(found, dict):
        return fallback
    merged = dict(fallback)
    merged["installed"] = bool(found.get("installed", fallback["installed"]))
    for key in ("path", "version", "source", "detail"):
        if isinstance(found.get(key), str):
            merged[key] = found[key]
    return merged


def _fallback_codex(codex_path) -> dict:
    candidates = [Path(codex_path)] if codex_path else [_KNOWN_CODEX_APP_PATH]
    for candidate in candidates:
        if candidate.is_file() and os.access(candidate, os.X_OK):
            return {"installed": True, "path": str(candidate)}
    which = shutil.which("codex")
    if which:
        return {"installed": True, "path": which}
    return {"installed": False, "path": None}


def _fallback_zcode(config) -> dict:
    path = getattr(config, "cli_path", None)
    if path is not None and Path(path).exists():
        return {"installed": True, "path": str(path)}
    return {"installed": False, "path": None}


def _fallback_dsh() -> dict:
    which = shutil.which("dsh")
    return {"installed": bool(which), "path": which}


def discover_workers(config, codex_path=None) -> dict:
    """Live discovery per worker; any tri_probe failure degrades to fallback.

    An explicitly injected ``codex_path`` pins the codex worker: tri_probe
    live discovery (which sees the real machine install) never overrides the
    caller's choice, so tests and callers that manage their own CLI copy get
    exactly the path they handed in.
    """
    try:
        from . import tri_probe as probe
    except ImportError:
        probe = None

    def call(name, *args):
        if probe is None:
            return None
        function = getattr(probe, name, None)
        if function is None:
            return None
        for attempt in (lambda: function(*args), lambda: function()):
            try:
                value = attempt()
            except Exception:  # noqa: BLE001 - degrade, never crash the tool
                continue
            if isinstance(value, dict):
                return value
        return None

    if codex_path is not None:
        codex = _fallback_codex(codex_path)
    else:
        codex = _normalized_discovery(call("discover_codex"),
                                      _fallback_codex(None))
    return {
        "codex": codex,
        "zcode": _normalized_discovery(call("discover_zcode", config),
                                       _fallback_zcode(config)),
        "dsh": _normalized_discovery(call("discover_dsh"), _fallback_dsh()),
    }


# --------------------------------------------------------------------------
# health.json view
# --------------------------------------------------------------------------

def read_health_document(state_dir) -> tuple[dict | None, str | None]:
    path = Path(state_dir) / "hub" / HEALTH_FILENAME
    if not path.is_file():
        return None, None
    try:
        document = json.loads(common.read_regular(path))
    except (OSError, ValueError) as error:
        return None, f"health.json unreadable: {error}"
    if not isinstance(document, dict):
        return None, "health.json is not a JSON object"
    return document, None


def resolve_worker_health(config, codex_path=None) -> dict:
    """Merge the (optional) human-approved health.json with live discovery.

    health.json stays authoritative for ``enabled``/``status``; without it,
    installed workers are ``status "unknown"`` / ``enabled true`` and absent
    ones report ``absent``. Workers explicitly declared by health.json are
    tracked so a declared ``absent`` can be distinguished from a discovery
    miss.
    """
    discovery = discover_workers(config, codex_path)
    document, file_error = read_health_document(config.state_dir)
    declared: dict = {}
    if isinstance(document, dict) and isinstance(document.get("workers"), dict):
        declared = document["workers"]
    workers: dict[str, dict] = {}
    for name in ("codex", "zcode", "dsh"):
        found = dict(discovery.get(name) or {"installed": False, "path": None})
        entry = declared.get(name)
        if isinstance(entry, dict) and isinstance(entry.get("enabled"), bool):
            health = {
                "enabled": entry["enabled"],
                "status": (entry["status"]
                           if isinstance(entry.get("status"), str) else "unknown"),
                "reason": (entry["reason"]
                           if isinstance(entry.get("reason"), str) else ""),
                "probe_ref": (entry["probe_ref"]
                              if isinstance(entry.get("probe_ref"), str) else ""),
                "declared": True,
            }
        else:
            health = {
                "enabled": True,
                "status": "unknown" if found.get("installed") else "absent",
                "reason": "",
                "probe_ref": "",
                "declared": False,
            }
        workers[name] = {**found, **health}
    view: dict = {
        "workers": workers,
        "health_file": str(Path(config.state_dir) / "hub" / HEALTH_FILENAME),
        "health_source": "health.json" if document is not None
                         else "defaults (no readable health.json)",
    }
    if isinstance(document, dict) and isinstance(document.get("generated"), str):
        view["generated"] = document["generated"]
    if isinstance(document, dict) and isinstance(document.get("routing"), dict):
        view["routing"] = document["routing"]
    if file_error:
        view["health_file_error"] = file_error
    return view


def health_impl(config, codex_path=None) -> dict:
    resolution = resolve_worker_health(config, codex_path)
    return {
        "hub_tool_version": HUB_TOOL_VERSION,
        "workers": resolution["workers"],
        "health_file": resolution["health_file"],
        "health_source": resolution["health_source"],
        **({"generated": resolution["generated"]}
           if "generated" in resolution else {}),
        **({"routing": resolution["routing"]}
           if "routing" in resolution else {}),
        **({"health_file_error": resolution["health_file_error"]}
           if "health_file_error" in resolution else {}),
        "routing_note": ROUTING_NOTE,
        # Structured per-path write capability (replaces the flat P0
        # read_only_p0 flag; no external consumer of the old key exists).
        "write_capability": {
            "zcode": "cold+resident (contract files + exact commands)",
            "codex_exec": "read-only only (no approval channel)",
            "codex_app_server": "write via untrusted per-command approvals",
        },
    }


def list_workers_impl(config, codex_path=None) -> dict:
    resolution = resolve_worker_health(config, codex_path)
    workers = {
        name: {
            "installed": entry.get("installed", False),
            "path": entry.get("path"),
            **({"version": entry["version"]}
               if isinstance(entry.get("version"), str) else {}),
            "status": entry.get("status", "unknown"),
        }
        for name, entry in resolution["workers"].items()
    }
    return {"hub_tool_version": HUB_TOOL_VERSION, "workers": workers,
            "routing_note": ROUTING_NOTE}


# --------------------------------------------------------------------------
# Task index and per-task records
# --------------------------------------------------------------------------

def _tasks_dir(config) -> Path:
    return Path(config.state_dir) / "hub" / "tasks"


def _task_record_path(config, task_id: str) -> Path:
    return _tasks_dir(config) / f"{task_id}.json"


def _write_task_record(path: Path, record: dict) -> None:
    common.write_json(path, record)


def _append_index(config, entry: dict) -> None:
    path = _tasks_dir(config) / "index.jsonl"
    with open(path, "a", encoding="utf-8") as stream:
        stream.write(json.dumps(entry, ensure_ascii=False) + "\n")
        stream.flush()
        os.fsync(stream.fileno())


# P2.4: synchronous index/SQLite/disk reads ride a DEDICATED bounded
# executor (2 workers) behind a UI/read request semaphore (4) — the MCP
# event loop never blocks on them, and concurrent reads are capped.
_READ_EXECUTOR_WORKERS = 2
_READ_CONCURRENCY = 4
_read_executor: concurrent.futures.ThreadPoolExecutor | None = None
_read_semaphore_state: tuple[object, asyncio.Semaphore] | None = None


def read_executor_pool() -> concurrent.futures.ThreadPoolExecutor:
    """The process-wide bounded executor for hub read paths (lazy)."""
    global _read_executor
    if _read_executor is None:
        _read_executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=_READ_EXECUTOR_WORKERS,
            thread_name_prefix='czs-hub-read')
    return _read_executor


def shutdown_read_executor(wait: bool = True) -> None:
    """Close the bounded read executor (idempotent; for tests/shutdown)."""
    global _read_executor, _read_semaphore_state
    pool, _read_executor = _read_executor, None
    _read_semaphore_state = None
    if pool is not None:
        pool.shutdown(wait=wait)


async def _offload_read(func, *args):
    """Run one synchronous read on the bounded pool under the semaphore.

    repair1 F-D: the semaphore is PER-RUNNING-LOOP (lazily re-created
    when a different event loop competes — asyncio primitives bind to
    their first waiting loop; a process-level one breaks any second
    loop lifetime, e.g. repeated asyncio.run in one process).
    Concurrency within one loop stays capped at _READ_CONCURRENCY.
    """
    global _read_semaphore_state
    loop = asyncio.get_running_loop()
    if _read_semaphore_state is None \
            or _read_semaphore_state[0] is not loop:
        _read_semaphore_state = (loop, asyncio.Semaphore(_READ_CONCURRENCY))
    semaphore = _read_semaphore_state[1]
    async with semaphore:
        return await loop.run_in_executor(
            read_executor_pool(), func, *args)


# P2.4-repair1 F-C: the OLD readers used str.splitlines(), which splits
# on \n \r \r\n \v \f \x1c \x1d \x1e \x85 \u2028 \u2029 — a plain
# text-mode line iteration only splits on \n/\r/\r\n. These helpers
# stream the file in bounded chunks through an incremental decoder and
# split on the FULL splitlines set, so exotic separators keep the old
# record boundaries (and malformed classifications).
_LINE_SEPARATORS = frozenset("\n\r\v\f\x1c\x1d\x1e\x85\u2028\u2029")


def _split_segments(text, _scanned=0):
    """Split ``text[ _scanned: ]`` on the str.splitlines set — single
    pass, LINEAR in the NEW bytes only (repair2 R1).

    ``_scanned`` is the prefix length already known separator-free
    (only a trailing lone ``\r`` can pair into ``\r\n`` across a
    chunk boundary, so at most one character of history matters);
    callers must keep that dangling trailing ``\r`` OUT of the scanned
    prefix (see _scan_new) or it stops acting as a separator.
    Returns ``(segments, tail_wait)``: complete lines plus the
    remainder that cannot yet be emitted.
    """
    segments = []
    rest = text
    start = _scanned
    while True:
        cut = None
        for index in range(start, len(rest)):
            if rest[index] in _LINE_SEPARATORS:
                cut = index
                break
        if cut is None:
            break
        if rest[cut] == "\r":
            if cut + 1 == len(rest):
                break  # may become \r\n with the next chunk
            if rest[cut + 1] == "\n":
                segments.append(rest[:cut])
                rest = rest[cut + 2:]
                start = 0
                continue
        segments.append(rest[:cut])
        rest = rest[cut + 1:]
        start = 0
    return segments, rest


def _scan_new(buffer, scanned):
    """Split ``buffer`` continuing from ``scanned`` (see
    _split_segments); returns (segments, rest, new_scanned)."""
    segments, rest = _split_segments(buffer, scanned)
    if segments:
        return segments, rest, 0
    # repair3 R3/N6: with no segment emitted, a DANGLING trailing \r
    # (the "may become \r\n" break) must stay OUT of the scanned
    # prefix — it still acts as a separator (or pairs into \r\n) once
    # the next chunk arrives. Folding it into the scanned prefix made
    # it inert and glued records across the chunk boundary.
    if rest.endswith("\r"):
        return segments, rest, len(rest) - 1
    return segments, rest, len(rest)


def _splitlines_streaming(path, chunk_bytes=256 * 1024):
    """Yield lines of ``path`` split on the str.splitlines separator set.

    Admission mirrors common.read_regular exactly: O_NOFOLLOW open,
    fstat S_ISREG + st_nlink == 1 (single-link), errors wrapped as
    ValueError with the SAME message shapes. The fd is closed on every
    failure path including an os.fdopen raise (repair1 F-B).
    """
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except OSError as error:
        raise ValueError(
            f"Cannot read ordinary file {path}: {error.strerror}") from error
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
            raise ValueError(f"Expected an ordinary single-link file: {path}")
        import codecs
        decoder = codecs.getincrementaldecoder("utf-8")(errors="replace")
        # repair2 R1: the pending buffer keeps its already-scanned
        # prefix length, so each chunk scans ONLY its new bytes (the
        # quadratic re-scan/concat of the repair1 shape is gone); the
        # buffer is compacted only when a segment is actually emitted
        pending = ""
        scanned = 0
        while True:
            chunk = os.read(fd, chunk_bytes)
            if not chunk:
                break
            pending += decoder.decode(chunk)
            segments, rest, scanned = _scan_new(pending, scanned)
            if segments:
                pending = rest
                yield from segments
        # EOF: flush the decoder, then emit the remainder (a trailing
        # separator would leave a final empty segment — drop it; a lone
        # trailing \r IS a separator for splitlines and was already
        # flushed by the sentinel split)
        pending += decoder.decode(b"", True)
        segments, _tail, _ = _scan_new(pending + "\n", scanned)
        if segments and segments[-1] == "":
            segments.pop()
        yield from segments
    finally:
        try:
            os.close(fd)
        except OSError:
            pass


def list_sessions_impl(config, worker=None, cursor=0, limit=SESSIONS_PAGE_DEFAULT) -> dict:
    """Page through hub-dispatched tasks (index.jsonl only)."""
    if worker is not None and worker not in ("codex", "zcode", "dsh"):
        raise HubError("invalid-argument", 'worker must be "codex", "zcode" or "dsh"')
    if isinstance(cursor, bool) or not isinstance(cursor, int) or cursor < 0:
        raise HubError("invalid-argument", "cursor must be a non-negative integer")
    if isinstance(limit, bool) or not isinstance(limit, int) \
            or not 1 <= limit <= SESSIONS_PAGE_MAX:
        raise HubError("invalid-argument",
                       f"limit must be an integer in 1..{SESSIONS_PAGE_MAX}")
    page: list[dict] = []
    matched = 0
    malformed = 0
    index_path = _tasks_dir(config) / "index.jsonl"
    if index_path.is_file():
        # P2.4: line-streamed index read — total/malformed/filter/paging
        # semantics are IDENTICAL to the former whole-buffer scan; only
        # the page window is ever held in memory. repair1 F-A/F-C: the
        # read_regular admission (O_NOFOLLOW + fstat single-link) and
        # the full str.splitlines separator set are preserved by the
        # shared helper; errors carry read_regular's exact message
        # shapes (ValueError text, same as the old code path).
        try:
            for line in _splitlines_streaming(index_path):
                if not line.strip():
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    malformed += 1
                    continue
                if not isinstance(record, dict):
                    malformed += 1
                    continue
                if worker is not None and record.get("worker") != worker:
                    continue
                # cursor/limit window against the filtered stream
                if cursor <= matched < cursor + limit:
                    page.append(record)
                matched += 1
        except ValueError as error:
            raise HubError("index-unreadable", str(error)) from error
        except OSError as error:
            raise HubError(
                "index-unreadable",
                f"Cannot read ordinary file {index_path}: "
                f"{error.strerror}") from error
    more = cursor + len(page) < matched
    return {
        "hub_tool_version": HUB_TOOL_VERSION,
        "sessions": page,
        "total": matched,
        "returned": len(page),
        "cursor": cursor,
        "malformed_index_lines": malformed,
        "next_cursor": cursor + len(page) if more else None,
        "note": "hub-dispatched tasks only; vendor-native sessions outside "
                "the hub are not listed",
    }


def read_context_impl(config, task_id, offset=0, max_items=30) -> dict:
    """Conservative session summary for one hub-dispatched task."""
    if not isinstance(task_id, str) or not _TASK_ID.match(task_id):
        raise HubError("invalid-argument", "task_id must be a short hub task identifier")
    if isinstance(offset, bool) or not isinstance(offset, int) or offset < 0:
        raise HubError("invalid-argument", "offset must be a non-negative integer")
    if isinstance(max_items, bool) or not isinstance(max_items, int) \
            or not 1 <= max_items <= CONTEXT_MAX_ITEMS_MAX:
        raise HubError("invalid-argument",
                       f"max_items must be an integer in 1..{CONTEXT_MAX_ITEMS_MAX}")
    record_path = _task_record_path(config, task_id)
    if not record_path.is_file():
        raise HubError("not-hub-task", "not a hub-dispatched task")
    try:
        record = json.loads(common.read_regular(record_path))
    except (OSError, ValueError) as error:
        raise HubError("task-record-unreadable", str(error)) from error
    if not isinstance(record, dict):
        raise HubError("task-record-unreadable", "task record is not a JSON object")
    locator = record.get("session_locator")
    if not isinstance(locator, dict) or not isinstance(locator.get("agent"), str):
        raise HubError(
            "no-session-context",
            f"task {task_id} completed without a locatable session "
            f"(outcome {record.get('outcome')!r}); no context can be served")
    try:
        context = read_session_context(locator, max_items=max_items, offset=offset)
    except ValueError as error:
        raise HubError("invalid-argument", str(error)) from error
    return {
        "hub_tool_version": HUB_TOOL_VERSION,
        "task_id": task_id,
        "worker": record.get("worker"),
        "context": context,
        "conservative_summary": "field whitelist + truncation + pagination; "
                                "raw session payloads stay in the evidence stores",
    }


# --------------------------------------------------------------------------
# Skills
# --------------------------------------------------------------------------

def list_skills_impl(worker, roots=None) -> dict:
    """Enumerate skill names per source root; SKILL.md bodies are never read."""
    if worker not in DEFAULT_SKILL_ROOTS:
        raise HubError("invalid-argument", 'worker must be "codex" or "zcode"')
    scanned = list(roots) if roots is not None else DEFAULT_SKILL_ROOTS[worker]
    skills: list[dict] = []
    for root, pattern in scanned:
        root = Path(root)
        if not root.is_dir():
            continue
        for marker in sorted(root.glob(pattern)):
            if marker.name == "SKILL.md":
                skills.append({"name": marker.parent.name, "root": str(root)})
    skills.sort(key=lambda entry: (entry["name"], entry["root"]))
    return {"hub_tool_version": HUB_TOOL_VERSION, "worker": worker,
            "skills": skills, "count": len(skills), "note": SKILLS_NOTE}


# --------------------------------------------------------------------------
# Workers
# --------------------------------------------------------------------------

async def default_zcode_runner(config, task_id: str, contract: dict,
                               timeout_s=None, ui_emitter=None) -> dict:
    """Translate the contract into a czs packet and run protocol.execute.

    The packet is written under ``work_root/hub-tasks/<task_id>/`` (outside the
    frozen workspace), the run evidence lands next to it, and the summary
    fields are read back from run.json. P0.4 round 2: ``ui_emitter`` (the hub
    passes it only on the production runner) links the cold Authority's
    approval projection and the child's awaited-exit projection.
    """
    from math import ceil

    from . import protocol

    task_dir = Path(config.work_root) / "hub-tasks" / task_id
    common.ensure_directory(task_dir)
    packet = {
        "workspace": contract["workspace"],
        "prompt": contract["prompt"],
        "allowed_files": list(contract.get("allowed_files", [])),
        "commands": list(contract.get("commands", [])),
        "trusted_hashes": {},
    }
    if isinstance(contract.get("model"), dict):
        packet["model"] = contract["model"]
    packet_path = task_dir / "packet.json"
    common.write_json(packet_path, packet)
    out_path = task_dir / "run.json"
    timeout = timeout_s or max(1, ceil(contract["max_timeout_ms"] / 1000))
    # quiet=True: protocol.execute's final run-summary print must stay off
    # this process's stdout — under the hub's stdio MCP transport, stdout is
    # the wire (see _invoke for the binding-timing justification).
    # test doubles may predate the P0.4 parameter; pass it only when the
    # target accepts it (the production execute always does)
    try:
        execute_params = inspect.signature(protocol.execute).parameters
    except (TypeError, ValueError):
        execute_params = {}
    await protocol.execute(
        SimpleNamespace(packet=str(packet_path), out=str(out_path),
                        resume=None, timeout=timeout, quiet=True),
        config,
        **({"ui_emitter": ui_emitter} if "ui_emitter" in execute_params else {}))
    run: dict = {}
    if out_path.is_file():
        try:
            run = json.loads(common.read_regular(out_path))
        except ValueError:
            run = {}
    delivery_ok = bool(run.get("delivery_ok"))
    return {
        "delivery_ok": delivery_ok,
        "session_id": (run.get("session_id")
                       if isinstance(run.get("session_id"), str) else None),
        "execution_problems": (run.get("execution_problems")
                               if isinstance(run.get("execution_problems"), list) else []),
        "changed_files": (run.get("changed_files")
                          if isinstance(run.get("changed_files"), list) else []),
        "outcome": (run.get("outcome")
                    if isinstance(run.get("outcome"), str) and run.get("outcome")
                    else ("delivered" if delivery_ok else "failed")),
        "run_path": str(out_path),
        "db_path": None,
    }


async def _invoke(runner, *args, **kwargs):
    """Await async runners; run sync runners off the event loop.

    Every runner executes with ``sys.stdout`` redirected to ``sys.stderr``:
    under the stdio MCP transport, stdout is the protocol wire, and a stray
    ``print()`` from a runner would inject a malformed frame into the client
    stream (observed in the 2026-10-25 live validation, where
    protocol.execute's run summary reached the MCP stream). This is safe for
    the transport because mcp's ``stdio_server()`` binds its writer exactly
    once at startup, before any tool handler runs — in the installed SDK
    (mcp 2.3.0, ``mcp/server/stdio.py``) ``_claim_fd(1, sys.stdout, ...)``
    wraps either a private fd duplicate or the startup-time
    ``sys.stdout.buffer`` (fallback path), and in older SDKs
    ``anyio.wrap_file(sys.stdout...)`` captured the object at the same
    startup point; either way the transport never re-reads ``sys.stdout``
    afterwards, so ``redirect_stdout`` (which only rebinds the module-global
    name) cannot touch the wire.
    """
    with contextlib.redirect_stdout(sys.stderr):
        if inspect.iscoroutinefunction(runner):
            return await runner(*args, **kwargs)
        return await asyncio.to_thread(runner, *args, **kwargs)


async def _start_appserver_client(client, budget):
    """Start one app-server client on the delegate's budget (P0.1 spec-I5).

    The production ``CodexAppServerClient.start`` accepts the budget OBJECT,
    so the lazy initialize and the delegate's run_task share ONE primary
    window. Injected test doubles without the parameter fall back to the
    numeric bound and then to their own start — the production path never
    silently gets a second full budget.
    """
    if budget is None:
        await _invoke(client.start)
        return
    try:
        params = inspect.signature(client.start).parameters
    except (TypeError, ValueError):
        params = {}
    if "budget" in params:
        await _invoke(client.start, budget=budget)
    elif "initialize_timeout" in params:
        await _invoke(client.start, initialize_timeout=budget.wait_s(cap=120))
    else:
        await _invoke(client.start)


def _field(source, name: str, default=None):
    if isinstance(source, dict):
        return source.get(name, default)
    return getattr(source, name, default)


def _zcode_packet(contract: dict) -> dict:
    """Translate a contract into a czs task packet.

    Same shape :func:`default_zcode_runner` writes for the cold path; the
    resident branch reuses it so both zcode modes hand the Authority identical
    packets.
    """
    packet = {
        "workspace": contract["workspace"],
        "prompt": contract["prompt"],
        "allowed_files": list(contract.get("allowed_files", [])),
        "commands": list(contract.get("commands", [])),
        "trusted_hashes": {},
    }
    if isinstance(contract.get("model"), dict):
        packet["model"] = contract["model"]
    return packet


class ResidentWorkers:
    """Lazy, server-lifetime singletons for resident-mode dispatch (P2).

    The first zcode+resident (or codex+app-server) delegate starts ONE shared
    instance; every later delegate of the same kind reuses it for the rest of
    the server lifetime — the fixed process-startup cost is paid once (the
    measured zcode setup latency drops from ~604 ms cold to 94 ms first /
    68 ms steady on the shared process).

    Concurrency: the resident ZCode process isolates sessions itself and
    keeps per-workspace locks, so concurrent ``run_packet`` calls on the
    shared instance are allowed; app-server delegates run concurrently on the
    shared :class:`~codex_appserver.CodexAppServerClient` since the
    2026-10-26 P3 subscriber-isolation work (each ``run_task`` registers
    thread-filtered request/notification subscriptions and unsubscribes on
    completion). ``appserver_concurrency`` tunes the delegate gate: ``0``
    (default) is unlimited concurrency, ``>0`` caps it with a semaphore, and
    ``1`` restores the P2 serial gate as the rollback path.
    """

    def __init__(self, config, evidence_root, *, codex_path=None,
                 resident_factory=None, appserver_factory=None,
                 appserver_driver_factory=None, appserver_concurrency=0,
                 ui_emitter=None):
        self.config = config
        self.evidence_root = Path(evidence_root)
        self.codex_path = codex_path
        self.resident_factory = resident_factory
        self.appserver_factory = appserver_factory
        self.appserver_driver_factory = appserver_driver_factory
        # P0.4: optional sideband UI emitter — shared-worker process-exit
        # projections and per-task driver projections; None keeps the
        # pre-P0.4 behaviour exactly.
        self.ui_emitter = ui_emitter
        self.resident = None
        self.appserver = None
        # 2026-10-26 P3: concurrent app-server delegates unlocked after the
        # main controller's live validation; set appserver_concurrency=1 to
        # roll back to the P2 serial gate (semaphore(1) below). >1 caps
        # concurrency. USAGE.md's serial-dispatch wording is pending a
        # main-controller doc pass.
        self.appserver_concurrency = max(0, int(appserver_concurrency))
        self._appserver_semaphore = (
            asyncio.Semaphore(self.appserver_concurrency)
            if self.appserver_concurrency else None)
        # Retained object for attribute compatibility with the P2 layout;
        # the delegate path no longer acquires it — the semaphore (or no
        # gate at all) above is the live concurrency policy.
        self.appserver_lock = asyncio.Lock()
        # Serializes lazy creation only (double-checked); run-level
        # concurrency policy is per-worker-kind as documented above.
        self.instance_lock = asyncio.Lock()
        # P0.3: one generation gate per shared worker kind. get_or_start
        # keeps the single-start invariant under one lock; a dead generation
        # (evidence-based health, see settle_*) is invalidated so the NEXT
        # delegate starts a fresh one — never mid-task, never replayed.
        self._resident_gate = worker_health.WorkerGeneration(
            factory=self._new_resident)
        self._appserver_gate = worker_health.WorkerGeneration(
            factory=self._new_appserver, starter=self._start_appserver)

    def _new_resident(self):
        if self.resident_factory is not None:
            return self.resident_factory()
        from .zcode_resident import ResidentZCode
        if self.ui_emitter is not None:
            return ResidentZCode(self.config, self.evidence_root,
                                 ui_emitter=self.ui_emitter)
        return ResidentZCode(self.config, self.evidence_root)

    def _new_appserver(self):
        if self.appserver_factory is not None:
            return self.appserver_factory()
        from .codex_appserver import CodexAppServerClient
        # codex_path may be unset here while the exec path resolves
        # discovery lazily; resolve it the same way instead of spawning a
        # literal None (live 2026-10-26: FileNotFoundError 'None', then
        # FileExistsError on the retried 'x'-mode evidence files).
        resolved = self.codex_path
        if not resolved:
            fallback = _fallback_codex(None)
            resolved = fallback.get("path") if isinstance(fallback, dict) else None
        if not resolved:
            raise HubError(
                "worker-unavailable",
                "codex app-server dispatch needs a codex binary: "
                "pass codex_path or install Codex")
        # one fresh evidence dir per construction attempt so a failed start
        # never poisons the retry with FileExists
        self._appserver_attempts = getattr(self, "_appserver_attempts", 0) + 1
        subdir = ("appserver" if self._appserver_attempts == 1
                  else f"appserver-{self._appserver_attempts}")
        if self.ui_emitter is not None:
            return CodexAppServerClient(
                resolved, self.evidence_root / subdir,
                exit_event_sink=ui_projection.make_exit_sink(
                    self.ui_emitter, "shared-app-server", "codex"))
        return CodexAppServerClient(resolved, self.evidence_root / subdir)

    async def _start_appserver(self, instance, budget=None):
        await _start_appserver_client(instance, budget)

    def appserver_gate(self):
        """Async context manager around one app-server ``run_task``.

        Unlimited when ``appserver_concurrency`` is 0, a semaphore of that
        size otherwise (1 = the P2 serial rollback gate).
        """
        if self._appserver_semaphore is not None:
            return self._appserver_semaphore
        return contextlib.nullcontext()

    async def resident_instance(self):
        """Start once (lazily), then reuse: the shared ResidentZCode driver."""
        return (await self.resident_generation())['instance']

    async def resident_generation(self):
        """``{'instance', 'generation'}`` for the P0.3 post-delegate gate."""
        current = await self._resident_gate.get_or_start()
        self.resident = current['instance']  # P2 layout compat
        return current

    async def appserver_instance(self, budget=None):
        """Start once (lazily), then reuse: the shared app-server client.

        The client keeps its own evidence tree under ``evidence_root/appserver``
        (frames/stderr for the whole server lifetime); per-task packets and
        receipt copies land under ``evidence_root/tasks/<task_id>/``.

        P0.1 (review spec-I5): the FIRST delegate's :class:`CallBudget`
        bounds the lazy initialize — the SAME object that delegate's
        run_task runs on. P0.3: the lazy start lives inside the generation
        gate, so a dead generation is replaced on the NEXT delegate while
        live generations keep the single-start invariant.
        """
        return (await self.appserver_generation(budget=budget))['instance']

    async def appserver_generation(self, budget=None):
        """``{'instance', 'generation'}`` for the P0.3 post-delegate gate."""
        current = await self._appserver_gate.get_or_start(budget=budget)
        self.appserver = current['instance']  # P2 layout compat
        return current

    async def settle_resident(self, info):
        """P0.3 health gate after one resident delegate (see _settle)."""
        return await self._settle(self._resident_gate, info)

    async def settle_appserver(self, info):
        """P0.3 health gate after one app-server delegate (see _settle)."""
        return await self._settle(self._appserver_gate, info)

    @staticmethod
    async def _settle(gate, info):
        """Evidence-check the instance THIS delegate used.

        An unhealthy shared worker (client failure, ended reader, or a real
        observed returncode — never a merely finished task) is invalidated
        now with a bounded old-generation close, so the NEXT delegate starts
        a fresh generation. The current task's outcome is never replayed or
        rewritten; a stale settle from an already-replaced generation is a
        no-op inside ``WorkerGeneration.invalidate``.
        """
        health = worker_health.instance_health(info['instance'])
        if not health['healthy']:
            await gate.invalidate(info['generation'], health)
        return health

    def appserver_driver(self, client, task_id=None):
        """Build one app-server driver (P0.4: project onto ``task_id``'s
        stream when a UI emitter is configured; injected factories and the
        no-emitter path keep the exact pre-P0.4 shape)."""
        if self.appserver_driver_factory is not None:
            return self.appserver_driver_factory(client)
        from .codex_appserver import CodexAppServerDriver
        if self.ui_emitter is not None and task_id is not None:
            return CodexAppServerDriver(client, ui_emitter=self.ui_emitter,
                                        task_id=task_id)
        return CodexAppServerDriver(client)

    async def close(self):
        """Best-effort stop of the shared instances at server shutdown."""
        for instance, stop_name in ((self.resident, "stop"),
                                    (self.appserver, "close")):
            closer = getattr(instance, stop_name, None) if instance is not None else None
            if callable(closer):
                # best-effort: shutdown must never raise
                with contextlib.suppress(Exception):
                    await _invoke(closer)
        self.resident = None
        self.appserver = None


async def delegate_impl(config, evidence_root, contract_id, initiator, *,
                        codex_path=None, codex_home=None,
                        zcode_runner=None, codex_runner=None,
                        resident_factory=None, appserver_factory=None,
                        appserver_driver_factory=None,
                        appserver_concurrency=0,
                        workers=None,
                        ui_emitter=None, ui_initiator_resolver=None) -> dict:
    """Execute one registered contract on its worker; async coroutine.

    Dispatch follows the contract's ``(worker, mode)`` pair: zcode ``cold``
    runs the czs protocol executor (``zcode_runner``), zcode ``resident``
    runs the packet on the shared resident process, codex ``exec`` runs the
    single-shot read-only driver (``codex_runner``), and codex ``app-server``
    drives the shared app-server client with a real contract Authority and
    the untrusted approval policy. ``workers`` carries the server-lifetime
    resident singletons (build_server wires its own; direct callers get a
    per-call holder unless they pass one).

    Refusals raise :class:`HubError` (unknown contract, disabled/absent
    worker, mode-rule violations caught by contract re-validation). A
    dispatched run that *fails* (non-zero exit, timeout, schema mismatch) is
    a normal outcome, recorded and returned — never an exception.
    """
    if not isinstance(initiator, dict) \
            or not isinstance(initiator.get("agent"), str) \
            or not initiator.get("agent", "").strip() \
            or not isinstance(initiator.get("session_id"), str) \
            or not initiator.get("session_id", "").strip():
        raise HubError(
            "invalid-argument",
            'initiator must be {"agent": "<non-empty>", "session_id": "<non-empty>"}')
    if not isinstance(contract_id, str) or not contract_id.strip():
        raise HubError("invalid-argument", "contract_id must be a non-empty string")

    try:
        contract = contracts.load_contract(Path(config.state_dir), contract_id)
    except contracts.ContractError as error:
        raise HubError(
            "unknown-contract",
            f"{error}; delegate requires a human-registered contract "
            f"(fields: {', '.join(contracts.REQUIRED)}; plus a strict "
            "output_schema for codex workers) — MCP exposure is not approval"
        ) from error

    worker = contract["worker"]
    mode = contract.get("mode") or ("cold" if worker == "zcode" else "exec")
    # P0.4 sideband UI projection (never part of the delegate result): the
    # default emitter is env-gated and OFF by default (rollback switch
    # CZS_UI_EVENTS=0); the default initiator resolver has no trusted
    # registry, so initiator identity stays unverified in v1 production.
    emitter = ui_emitter if ui_emitter is not None \
        else ui_projection.default_emitter()
    initiator_resolver = ui_initiator_resolver \
        if ui_initiator_resolver is not None \
        else ui_projection.default_initiator_resolver()
    if workers is None:
        workers = ResidentWorkers(
            config, evidence_root, codex_path=codex_path,
            resident_factory=resident_factory,
            appserver_factory=appserver_factory,
            appserver_driver_factory=appserver_driver_factory,
            appserver_concurrency=appserver_concurrency,
            ui_emitter=emitter)
    resolution = resolve_worker_health(config, codex_path)
    entry = resolution["workers"].get(worker, {})
    remaining = sorted(
        name for name in ("codex", "zcode")
        if name != worker and resolution["workers"].get(name, {}).get("enabled") is not False)
    if worker == "dsh":
        raise HubError("worker-absent",
                       "worker 'dsh' is absent on this host; dispatch refused")
    if entry.get("enabled") is False:
        raise HubError(
            "worker-disabled",
            f"{worker} is disabled by health.json "
            f"({entry.get('reason') or 'no reason recorded'}); "
            f"remaining_workers: {remaining}; {ROUTING_NOTE}")
    if entry.get("declared") and entry.get("status") == "absent":
        raise HubError(
            "worker-absent",
            f"{worker} is declared absent by health.json; "
            f"remaining_workers: {remaining}; {ROUTING_NOTE}")
    # Write-path rules (allowed_files/commands per worker mode) live in the
    # contract layer and are enforced by load_contract's re-validation above.

    if codex_runner is None:
        codex_runner = run_readonly
    if zcode_runner is None:
        zcode_runner = default_zcode_runner
    effective_codex_path = codex_path or resolution["workers"].get("codex", {}).get("path")
    if worker == "codex" and effective_codex_path is None \
            and codex_runner is run_readonly:
        raise HubError("worker-unavailable",
                       "codex CLI not found on this host; cannot dispatch")
    if (worker == "codex" and mode == "app-server"
            and effective_codex_path is None and workers.appserver_factory is None):
        raise HubError("worker-unavailable",
                       "codex CLI not found on this host; "
                       "cannot start the app-server client")

    task_id = uuid.uuid4().hex
    tasks_dir = _tasks_dir(config)
    common.ensure_directory(tasks_dir)
    evidence_dir = Path(evidence_root) / "tasks" / task_id
    digest = contracts.contract_digest(contract)
    started = time.monotonic()
    record: dict = {
        "task_id": task_id,
        "worker": worker,
        "status": "running",
        "initiator": {"agent": initiator["agent"],
                      "session_id": initiator["session_id"]},
        "contract_id": contract["contract_id"],
        "contract_sha256": digest,
        "created": _now(),
    }
    _write_task_record(_task_record_path(config, task_id), record)
    # P0.4 sideband: the running record is readable — project task.accepted
    # and only then the hub-stream announcement referencing it (failures are
    # recorded inside the emitter and never reach the delegate result).
    ui_projection.project_task_accepted(
        emitter, initiator_resolver, task_id=task_id, worker=worker,
        mode=mode, contract_id=contract["contract_id"],
        contract_digest=digest, created_at=record["created"],
        initiator=initiator)

    error_text: str | None = None
    error_code: str | None = None  # exception TYPE name (trusted enumeration)
    locator: dict | None = None
    result_payload: dict = {}
    outcome = "runner-error"
    status = "failed"
    # P0.3: which shared generation this delegate ran on (for the post-run
    # health gate); None until the delegate actually obtained one.
    used: tuple[str, dict] | None = None

    async def _settle_shared():
        """P0.3 review I2: cancellation-safe post-run health gate.

        Runs on normal completion, runner errors AND cancellation, so a dead
        generation is never left cached. A settle failure is suppressed and
        can never mask the first result or a pending cancellation; a cancel
        absorbed by the bounded retirement close is re-raised by
        WorkerGeneration.invalidate and passes through untouched
        (CancelledError is a BaseException, not caught by suppress).
        """
        if used is not None:
            with contextlib.suppress(Exception):
                await (workers.settle_appserver(used[1])
                       if used[0] == "appserver"
                       else workers.settle_resident(used[1]))

    def _write_records():
        """I1/I2: the task records survive every path — normal, runner-error
        and cancellation — and are never rewritten into a success return."""
        duration = round(time.monotonic() - started, 3)
        record.update({
            "status": status,
            "outcome": outcome,
            "session_locator": locator,
            "evidence_dir": str(evidence_dir),
            "duration_s": duration,
            "updated": _now(),
            **({"error": error_text} if error_text else {}),
            "result": result_payload,
        })
        _write_task_record(_task_record_path(config, task_id), record)
        _append_index(config, {
            "ts": _now(),
            "task_id": task_id,
            "worker": worker,
            "status": status,
            "outcome": outcome,
            "contract_id": contract["contract_id"],
        })
        # P0.4 sideband: terminal records written — project task.completed
        # (safe fields only; the result payload stays in the records).
        ui_projection.project_task_completed(
            emitter, task_id=task_id, worker=worker, mode=mode,
            status=status, outcome=outcome,
            delivery_ok=result_payload.get("delivery_ok"),
            duration_ms=round((time.monotonic() - started) * 1000, 3),
            error_code=error_code, session_locator=locator)

    try:
        if worker == "codex":
            model = contract.get("model")
            model_name = model.get("model") if isinstance(model, dict) else None
            common.ensure_directory(evidence_dir)
            if mode == "app-server":
                # P0.1 review spec-I5: ONE CallBudget for the whole delegate —
                # the lazy app-server start (first delegate only) and this
                # delegate's run_task share the same primary window; the
                # synchronous preflight/inventory work here consumes it too.
                delegate_budget = CallBudget(contract["max_timeout_ms"] / 1000)
                # Real contract Authority against a frozen pre-run inventory:
                # every approval the bridge relays is decided by this packet,
                # never by the model's own claims.
                packet = {
                    "workspace": contract["workspace"],
                    "prompt": contract["prompt"],
                    "allowed_files": list(contract.get("allowed_files", [])),
                    "commands": list(contract.get("commands", [])),
                }
                if isinstance(model, dict):
                    packet["model"] = model
                packet_path = evidence_dir / "packet.json"
                common.write_json(packet_path, packet)
                before = inventory(Path(contract["workspace"]))
                # P0.4 round 2: codex approval events project at the
                # ApprovalBridge (the APPLIED post-worker_response decision,
                # with the requested variant); the Authority itself stays
                # unwired here so nothing is emitted twice.
                authority = Authority(packet, packet_path, before)
                generation = await workers.appserver_generation(budget=delegate_budget)
                used = ("appserver", generation)
                client = generation["instance"]
                driver = workers.appserver_driver(client, task_id)
                # 2026-10-26 P3: delegates run concurrently on the shared
                # client — thread-filtered subscriptions (codex_appserver)
                # isolate approvals per task. The gate is a no-op unless the
                # workers holder was built with appserver_concurrency>0
                # (1 = P2 serial rollback); docs pending a main-controller
                # pass. The per-workspace advisory lock still applies inside.
                run_kwargs = {
                    "model": model_name,
                    "sandbox": contract.get("sandbox") or "read-only",
                    "approval_policy": "untrusted",
                    "timeout_s": contract["max_timeout_ms"] / 1000,
                }
                try:
                    accepts_budget = "budget" in inspect.signature(
                        driver.run_task).parameters
                except (TypeError, ValueError):
                    accepts_budget = False
                if accepts_budget:
                    # production drivers: the SAME budget object the lazy
                    # start used (injected doubles without the parameter keep
                    # their own timeout handling)
                    run_kwargs["budget"] = delegate_budget
                async with workers.appserver_gate():
                    with common.workspace_lock(Path(contract["workspace"]),
                                               Path(config.state_dir)):
                        receipt = await _invoke(
                            driver.run_task, authority,
                            Path(contract["workspace"]), contract["prompt"],
                            **run_kwargs)
                receipt = receipt if isinstance(receipt, dict) else {}
                if receipt:
                    # best-effort per-task copy; the task outcome stands
                    with contextlib.suppress(OSError, ValueError, TypeError):
                        common.write_json(evidence_dir / "appserver-receipt.json",
                                          receipt)
                outcome = str(_field(receipt, "outcome", "failed"))
                thread_id = _field(receipt, "thread_id")
                locator = None
                if isinstance(thread_id, str) and thread_id:
                    locator = {"agent": "codex", "thread_id": thread_id,
                               "path": None}
                result_payload = {
                    "thread_id": thread_id,
                    "final_message": _field(receipt, "final_message"),
                    "command_items": _field(receipt, "command_items") or [],
                    "approvals": _field(receipt, "approvals") or [],
                }
                error_text = _field(receipt, "error")
                status = "completed"
            else:
                # Same per-workspace advisory lock as the czs executor: one
                # workspace is never driven by two vendors at once.
                exec_kwargs = {}
                if emitter.enabled:
                    try:
                        runner_params = inspect.signature(
                            codex_runner).parameters
                    except (TypeError, ValueError):
                        runner_params = {}
                    if "exit_event_sink" in runner_params:
                        # P0.4 round 2: cold child exit projection (the
                        # driver observes only a real awaited wait)
                        exec_kwargs["exit_event_sink"] = \
                            ui_projection.make_exit_sink(
                                emitter, "cold", "codex")
                with common.workspace_lock(Path(contract["workspace"]),
                                           Path(config.state_dir)):
                    receipt = await _invoke(
                        codex_runner, effective_codex_path,
                        Path(contract["workspace"]), contract["prompt"],
                        contract["output_schema"], evidence_dir,
                        timeout_s=contract["max_timeout_ms"] / 1000,
                        model=model_name, **exec_kwargs)
                outcome = str(_field(receipt, "outcome", "failed"))
                thread_id = _field(receipt, "thread_id")
                locator = None
                if isinstance(thread_id, str) and thread_id:
                    locator = {"agent": "codex", "thread_id": thread_id, "path": None}
                    if codex_home is not None:
                        locator["codex_home"] = str(codex_home)
                    located = locate_rollout(thread_id, codex_home)
                    if located.get("status") == "located":
                        locator["path"] = located.get("path")
                result_payload = {
                    "structured": _field(receipt, "structured"),
                    "error": _field(receipt, "error"),
                }
                error_text = _field(receipt, "error")
                status = "completed"
        elif mode == "resident":
            from math import ceil

            # Packet under the hub evidence tree; run.json stays under
            # work_root (checked_locations requires it outside the workspace).
            task_dir = Path(config.work_root) / "hub-tasks" / task_id
            common.ensure_directory(task_dir)
            packet_dir = evidence_dir
            common.write_json(packet_dir / "packet.json", _zcode_packet(contract))
            timeout_s = max(1, ceil(contract["max_timeout_ms"] / 1000))
            generation = await workers.resident_generation()
            used = ("resident", generation)
            resident = generation["instance"]
            run = await _invoke(resident.run_packet, packet_dir / "packet.json",
                                task_dir / "run.json", timeout_s=timeout_s)
            run = run if isinstance(run, dict) else {}
            outcome = (run.get("outcome")
                       if isinstance(run.get("outcome"), str) and run.get("outcome")
                       else ("delivered" if run.get("delivery_ok") else "failed"))
            session_id = run.get("session_id")
            locator = None
            if isinstance(session_id, str) and session_id:
                locator = {"agent": "zcode", "session_id": session_id}
                if isinstance(run.get("db_path"), str):
                    locator["db_path"] = run["db_path"]
            evidence_dir = task_dir
            result_payload = {
                "delivery_ok": bool(run.get("delivery_ok")),
                "changed_files": run.get("changed_files") or [],
                "problems": run.get("execution_problems") or [],
            }
            status = "completed"
        else:
            from math import ceil

            timeout_s = max(1, ceil(contract["max_timeout_ms"] / 1000))
            cold_kwargs = {}
            if zcode_runner is default_zcode_runner and emitter.enabled:
                # production cold path only: injected test runners keep
                # their exact pre-P0.4 call shape
                cold_kwargs["ui_emitter"] = emitter
            run = await _invoke(zcode_runner, config, task_id, contract,
                                timeout_s, **cold_kwargs)
            run = run if isinstance(run, dict) else {}
            outcome = (run.get("outcome")
                       if isinstance(run.get("outcome"), str) and run.get("outcome")
                       else ("delivered" if run.get("delivery_ok") else "failed"))
            session_id = run.get("session_id")
            locator = None
            if isinstance(session_id, str) and session_id:
                locator = {"agent": "zcode", "session_id": session_id}
                if isinstance(run.get("db_path"), str):
                    locator["db_path"] = run["db_path"]
            run_path = run.get("run_path")
            evidence_dir = (Path(run_path).parent
                            if isinstance(run_path, str) and run_path
                            else Path(config.work_root) / "hub-tasks" / task_id)
            result_payload = {
                "delivery_ok": bool(run.get("delivery_ok")),
                "changed_files": run.get("changed_files") or [],
                "problems": run.get("execution_problems") or [],
            }
            status = "completed"
    except Exception as error:  # noqa: BLE001 - a runner crash is a task outcome
        error_text = f"{type(error).__name__}: {error}"[:500]
        error_code = type(error).__name__
        outcome = "runner-error"
        status = "failed"
    except asyncio.CancelledError:
        # P0.3 review I2: an outer cancel during the run. The shared
        # generation still settles and the task records are still written
        # (the finally below) — the evidence of what already happened is
        # preserved — but the cancellation itself always propagates; a
        # cancelled delegate never returns a result.
        error_text = error_text or "delegate cancelled"
        error_code = error_code or "CancelledError"
        outcome = "runner-error"
        status = "failed"
        raise
    finally:
        # I2: settle first (cancellation-safe, never masking the first
        # result), then the records — the records survive even when settle
        # re-raises a cancel that arrived during the bounded close (I1).
        try:
            await _settle_shared()
        finally:
            _write_records()
    result = {
        "hub_tool_version": HUB_TOOL_VERSION,
        "task_id": task_id,
        "worker": worker,
        "outcome": outcome,
        "session_locator": locator,
        "evidence_dir": str(evidence_dir),
        "duration_s": round(time.monotonic() - started, 3),
        **result_payload,
    }
    if error_text:
        result["error"] = error_text
    return result


# --------------------------------------------------------------------------
# MCP server wiring (stdio), following mcp_readonly's build_server pattern
# --------------------------------------------------------------------------

def _tools() -> list[dict]:
    return [
        {
            "name": "health",
            "description": "Worker health view: human-approved health.json merged with "
                           "live discovery (codex/zcode configured in P0, dsh absent), "
                           "plus the degraded-routing non-equivalence note.",
            "inputSchema": {"type": "object", "properties": {},
                            "additionalProperties": False},
        },
        {
            "name": "list_workers",
            "description": "Installed workers with path/version/status; dsh is always "
                           "reported absent on this host in P0.",
            "inputSchema": {"type": "object", "properties": {},
                            "additionalProperties": False},
        },
        {
            "name": "list_sessions",
            "description": "Paged listing of hub-dispatched tasks (tasks/index.jsonl); "
                           "vendor-native sessions outside the hub are not listed.",
            "inputSchema": {"type": "object", "properties": {
                "worker": {"type": "string", "enum": ["codex", "zcode", "dsh"]},
                "cursor": {"type": "integer", "minimum": 0},
                "limit": {"type": "integer", "minimum": 1, "maximum": SESSIONS_PAGE_MAX},
            }, "additionalProperties": False},
        },
        {
            "name": "read_context",
            "description": "Conservative session summary for one hub-dispatched task "
                           "(field whitelist + truncation + pagination; raw payloads "
                           "stay in the evidence stores).",
            "inputSchema": {"type": "object", "properties": {
                "task_id": {"type": "string"},
                "offset": {"type": "integer", "minimum": 0},
                "max_items": {"type": "integer", "minimum": 1,
                              "maximum": CONTEXT_MAX_ITEMS_MAX},
            }, "required": ["task_id"], "additionalProperties": False},
        },
        {
            "name": "list_skills",
            "description": "Skill names per source root for one worker; SKILL.md bodies "
                           "are never read.",
            "inputSchema": {"type": "object", "properties": {
                "worker": {"type": "string", "enum": ["codex", "zcode"]},
            }, "required": ["worker"], "additionalProperties": False},
        },
        {
            "name": "delegate",
            "description": "Dispatch one human-registered contract to its worker. "
                           "zcode: mode cold (czs protocol executor) or resident "
                           "(shared resident process); codex: mode exec "
                           "(codex exec -s read-only, strict output_schema) or "
                           "app-server (approval callbacks brokered through the "
                           "contract Authority under the untrusted approval "
                           "policy). MCP exposure is not approval: an "
                           "unregistered or incomplete contract is refused.",
            "inputSchema": {"type": "object", "properties": {
                "contract_id": {"type": "string"},
                "initiator": {"type": "object", "properties": {
                    "agent": {"type": "string"},
                    "session_id": {"type": "string"},
                }, "required": ["agent", "session_id"],
                   "additionalProperties": False},
            }, "required": ["contract_id", "initiator"],
               "additionalProperties": False},
        },
    ]


def build_server(config, evidence_root: Path, *, codex_path=None,
                 zcode_runner=None, codex_runner=None, codex_home=None,
                 resident_factory=None, appserver_factory=None,
                 appserver_driver_factory=None, appserver_concurrency=0):
    """Wire the six hub tools onto one MCP lowlevel server.

    ``codex_runner`` / ``zcode_runner`` are injection points for tests; the
    production defaults are :func:`codex_driver.run_readonly` and
    :func:`default_zcode_runner`. ``resident_factory`` /
    ``appserver_factory`` / ``appserver_driver_factory`` build the
    server-lifetime resident singletons (lazy start on first resident-mode
    delegate; production defaults are
    :class:`zcode_resident.ResidentZCode` and
    :class:`codex_appserver.CodexAppServerClient` /
    :class:`~codex_appserver.CodexAppServerDriver`);
    ``appserver_concurrency`` configures the app-server delegate gate (0 =
    unlimited, 1 = P2 serial rollback; see :class:`ResidentWorkers`). The
    shared holder is attached as ``server.czs_workers`` so :func:`serve` can
    close it at shutdown.
    """
    from mcp.server import Server
    import mcp.types as types

    evidence_root = Path(evidence_root)
    workers = ResidentWorkers(
        config, evidence_root, codex_path=codex_path,
        resident_factory=resident_factory, appserver_factory=appserver_factory,
        appserver_driver_factory=appserver_driver_factory,
        appserver_concurrency=appserver_concurrency,
        ui_emitter=ui_projection.default_emitter())

    def error_result(code: str, detail: str) -> types.CallToolResult:
        """Uniform tool-error surface: is_error result with a JSON body.

        Every failure path (controlled HubError, contract rejection, any
        unexpected exception) becomes a CallToolResult — never a JSON-RPC
        protocol error, which clients reject when error.data is a string.
        Unexpected exceptions surface only their type and message, no stack.
        """
        return types.CallToolResult(
            content=[types.TextContent(type="text", text=json.dumps(
                {"hub_tool_version": HUB_TOOL_VERSION,
                 "error": code, "detail": detail[:500]},
                ensure_ascii=False))],
            is_error=True)

    async def on_list_tools(ctx, params):  # noqa: ANN001 - SDK context type
        return types.ListToolsResult(tools=[
            types.Tool(name=tool["name"], description=tool["description"],
                       input_schema=tool["inputSchema"]) for tool in _tools()
        ])

    async def on_call_tool(ctx, params):  # noqa: ANN001 - SDK context type
        name = params.name
        arguments = dict(params.arguments or {})
        try:
            if name == "health":
                value = health_impl(config, codex_path)
            elif name == "list_workers":
                value = list_workers_impl(config, codex_path)
            elif name == "list_sessions":
                # P2.4: bounded offload — never blocks the MCP loop
                value = await _offload_read(
                    list_sessions_impl,
                    config, arguments.get("worker"),
                    arguments.get("cursor", 0),
                    arguments.get("limit", SESSIONS_PAGE_DEFAULT))
            elif name == "read_context":
                value = await _offload_read(
                    read_context_impl,
                    config, arguments.get("task_id"),
                    arguments.get("offset", 0),
                    arguments.get("max_items", 30))
            elif name == "list_skills":
                value = list_skills_impl(arguments.get("worker"))
            elif name == "delegate":
                value = await delegate_impl(
                    config, evidence_root,
                    arguments.get("contract_id"), arguments.get("initiator"),
                    codex_path=codex_path, codex_home=codex_home,
                    zcode_runner=zcode_runner, codex_runner=codex_runner,
                    workers=workers)
            else:
                raise HubError("unknown-tool", name)
        except HubError as error:
            return error_result(error.code, error.detail)
        except contracts.ContractError as error:
            return error_result("contract-rejected", str(error))
        except Exception as error:  # noqa: BLE001 - never leak a protocol-level error
            return error_result("internal-error",
                                f"{type(error).__name__}: {error}")
        return types.CallToolResult(
            content=[types.TextContent(
                type="text", text=json.dumps(value, ensure_ascii=False))])

    server = Server("czs-hub", version=__version__,
                    on_list_tools=on_list_tools, on_call_tool=on_call_tool)
    server.czs_workers = workers  # serve() closes the singletons at shutdown
    return server


async def serve(config, evidence_root: Path, **injectables) -> None:
    """Run the stdio server for one process lifetime."""
    from mcp.server.stdio import stdio_server

    server = build_server(config, Path(evidence_root), **injectables)
    workers = getattr(server, "czs_workers", None)
    # P1.1: the read-only UI event surface — created ONLY when CZS_UI_EVENTS
    # is explicitly enabled. It attaches to the SAME default projection
    # emitter (no second task state) and any failure leaves the UI simply
    # unavailable: the control plane, approvals and evidence are untouched.
    ui_server = None
    try:
        from .ui_server import start_ui_service
        ui_server = await start_ui_service(
            state_dir=Path(config.state_dir))
    except Exception:  # noqa: BLE001 - UI availability never blocks the hub
        ui_server = None
    try:
        async with stdio_server() as (read_stream, write_stream):
            await server.run(read_stream, write_stream,
                             server.create_initialization_options())
    finally:
        # Best-effort close of the resident singletons (never started, or
        # already dead, are both fine).
        if ui_server is not None:
            with contextlib.suppress(Exception):
                await ui_server.close()
        if workers is not None:
            await workers.close()


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(
        description="Codex/ZCode/dsh orchestration hub MCP server (stdio; "
                    "zcode cold/resident, codex exec/app-server)")
    result.add_argument("--config", type=Path, default=Path(".czs/config.json"),
                        help="czs config.json (default .czs/config.json)")
    result.add_argument("--evidence-root", type=Path, default=None,
                        help="evidence root for hub dispatches "
                             "(default <work_root>/hub-evidence)")
    return result


def main(argv=None) -> int:
    args = parser().parse_args(argv)
    from .runtime import load_config

    try:
        config = load_config(args.config)
        evidence_root = (args.evidence_root if args.evidence_root is not None
                         else Path(config.work_root) / "hub-evidence")
        common.ensure_directory(Path(config.state_dir) / "hub" / "contracts")
        common.ensure_directory(Path(config.state_dir) / "hub" / "tasks")
        common.ensure_directory(evidence_root)
    except (OSError, ValueError) as error:
        print(json.dumps({"error": "invalid-startup",
                          "detail": f"{type(error).__name__}: {error}"[:300]}),
              file=sys.stderr)
        return 2
    import anyio
    anyio.run(serve, config, evidence_root)
    return 0


if __name__ == "__main__":  # pragma: no cover - process entry
    raise SystemExit(main())
