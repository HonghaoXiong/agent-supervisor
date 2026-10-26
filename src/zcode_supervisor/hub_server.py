"""Three-way orchestration hub MCP server (stdio): Codex / ZCode / dsh workers.

The hub is the single dispatch point a decision-layer agent (any installed
agent) drives over MCP. P0 shape:

* **Read-only tasks only** — contracts must carry ``allowed_files: []``;
* **Two workers configured** — zcode (via the czs protocol executor) and codex
  (via ``codex exec -s read-only``); ``dsh`` is always reported absent and is
  never dispatched;
* **Shared frozen workspaces** — the czs inventory-hash freeze still governs
  zcode runs, and codex runs take the same per-workspace advisory lock so
  writes to one workspace stay serialized across vendors;
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
* ``evidence_root/tasks/<task_id>/`` — codex dispatch evidence;
* ``work_root/hub-tasks/<task_id>/`` — zcode packet + run.json evidence.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import inspect
import json
import os
import re
import shutil
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

from . import __version__, common, contracts
from .codex_driver import locate_rollout, run_readonly
from .session_reader import read_context as read_session_context

__all__ = [
    "HubError",
    "ROUTING_NOTE",
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
        "read_only_p0": True,
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
    records: list[dict] = []
    malformed = 0
    index_path = _tasks_dir(config) / "index.jsonl"
    if index_path.is_file():
        try:
            lines = common.read_regular(index_path).decode("utf-8", "replace").splitlines()
        except (OSError, ValueError) as error:
            raise HubError("index-unreadable", str(error)) from error
        for line in lines:
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
            records.append(record)
    if worker is not None:
        records = [r for r in records if r.get("worker") == worker]
    page = records[cursor:cursor + limit]
    more = cursor + len(page) < len(records)
    return {
        "hub_tool_version": HUB_TOOL_VERSION,
        "sessions": page,
        "total": len(records),
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
                               timeout_s=None) -> dict:
    """Translate the contract into a czs packet and run protocol.execute.

    The packet is written under ``work_root/hub-tasks/<task_id>/`` (outside the
    frozen workspace), the run evidence lands next to it, and the summary
    fields are read back from run.json.
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
    await protocol.execute(
        SimpleNamespace(packet=str(packet_path), out=str(out_path),
                        resume=None, timeout=timeout, quiet=True),
        config)
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


def _field(source, name: str, default=None):
    if isinstance(source, dict):
        return source.get(name, default)
    return getattr(source, name, default)


async def delegate_impl(config, evidence_root, contract_id, initiator, *,
                        codex_path=None, codex_home=None,
                        zcode_runner=None, codex_runner=None) -> dict:
    """Execute one registered contract on its worker; async coroutine.

    Refusals raise :class:`HubError` (unknown contract, disabled/absent
    worker, out-of-P0 write path). A dispatched run that *fails* (non-zero
    exit, timeout, schema mismatch) is a normal outcome, recorded and
    returned — never an exception.
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
    if contract.get("allowed_files"):
        raise HubError("p0-readonly",
                       "write path is out of P0 scope: allowed_files must be empty")

    if codex_runner is None:
        codex_runner = run_readonly
    if zcode_runner is None:
        zcode_runner = default_zcode_runner
    effective_codex_path = codex_path or resolution["workers"].get("codex", {}).get("path")
    if worker == "codex" and effective_codex_path is None \
            and codex_runner is run_readonly:
        raise HubError("worker-unavailable",
                       "codex CLI not found on this host; cannot dispatch")

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

    error_text: str | None = None
    locator: dict | None = None
    result_payload: dict = {}
    outcome = "runner-error"
    status = "failed"
    try:
        if worker == "codex":
            model = contract.get("model")
            model_name = model.get("model") if isinstance(model, dict) else None
            common.ensure_directory(evidence_dir)
            # Same per-workspace advisory lock as the czs executor: one
            # workspace is never driven by two vendors at once.
            with common.workspace_lock(Path(contract["workspace"]),
                                       Path(config.state_dir)):
                receipt = await _invoke(
                    codex_runner, effective_codex_path,
                    Path(contract["workspace"]), contract["prompt"],
                    contract["output_schema"], evidence_dir,
                    timeout_s=contract["max_timeout_ms"] / 1000,
                    model=model_name)
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
        else:
            from math import ceil

            timeout_s = max(1, ceil(contract["max_timeout_ms"] / 1000))
            run = await _invoke(zcode_runner, config, task_id, contract, timeout_s)
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
        outcome = "runner-error"
        status = "failed"

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
    result = {
        "hub_tool_version": HUB_TOOL_VERSION,
        "task_id": task_id,
        "worker": worker,
        "outcome": outcome,
        "session_locator": locator,
        "evidence_dir": str(evidence_dir),
        "duration_s": duration,
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
            "description": "Dispatch one human-registered contract to its worker "
                           "(read-only P0; codex via codex exec -s read-only, zcode via "
                           "the czs protocol executor). MCP exposure is not approval: "
                           "an unregistered or incomplete contract is refused.",
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
                 zcode_runner=None, codex_runner=None, codex_home=None):
    """Wire the six hub tools onto one MCP lowlevel server.

    ``codex_runner`` / ``zcode_runner`` are injection points for tests; the
    production defaults are :func:`codex_driver.run_readonly` and
    :func:`default_zcode_runner`.
    """
    from mcp.server import Server
    import mcp.types as types

    evidence_root = Path(evidence_root)

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
                value = list_sessions_impl(
                    config, arguments.get("worker"),
                    arguments.get("cursor", 0),
                    arguments.get("limit", SESSIONS_PAGE_DEFAULT))
            elif name == "read_context":
                value = read_context_impl(
                    config, arguments.get("task_id"),
                    arguments.get("offset", 0), arguments.get("max_items", 30))
            elif name == "list_skills":
                value = list_skills_impl(arguments.get("worker"))
            elif name == "delegate":
                value = await delegate_impl(
                    config, evidence_root,
                    arguments.get("contract_id"), arguments.get("initiator"),
                    codex_path=codex_path, codex_home=codex_home,
                    zcode_runner=zcode_runner, codex_runner=codex_runner)
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

    return Server("czs-hub", version=__version__,
                  on_list_tools=on_list_tools, on_call_tool=on_call_tool)


async def serve(config, evidence_root: Path, **injectables) -> None:
    """Run the stdio server for one process lifetime."""
    from mcp.server.stdio import stdio_server

    server = build_server(config, Path(evidence_root), **injectables)
    async with stdio_server() as (read_stream, write_stream):
        await server.run(read_stream, write_stream,
                         server.create_initialization_options())


def parser() -> argparse.ArgumentParser:
    result = argparse.ArgumentParser(
        description="Codex/ZCode/dsh orchestration hub MCP server (stdio, P0 read-only)")
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
