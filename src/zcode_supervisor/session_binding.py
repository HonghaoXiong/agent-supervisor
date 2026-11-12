"""Native ZCode session binding.

``bind_session`` grants observe/control over an EXISTING native session
after read-only verification. Control additionally requires an idle session
— no running tool of the CURRENT turn, no pending approval, and a turn that
is provably finished — plus an explicit handoff proof (the hub task record
whose session locator names this session), because dispatch may only ever
CONTINUE that session: a control binding never falls back to starting a new
one. A ``bound_existing`` binding pins the session's OWN directory and
records the pre-existing file state as the baseline.

Turn classification uses only runtime-written fields: ``time.completed`` and
``finish`` on the newest user-visible assistant message, and tool rows
attributed to that message's ``anchor.turnId``. Prose, a trailing assistant
message that is still streaming, or a failed tool left over from an earlier
turn never decide a turn boundary.

``read_session_delta`` pages the full turn text under a byte budget. The
cursor carries the message sequence, the parts digest and a byte offset into
that message, so a single oversized report is continued instead of being
emitted past the budget or dropped. Cursor defaults to the binding's own
stored cursor: re-reading always resumes where the last page ended.

Candidate fingerprints come from the repository's own change set (tracked
modifications plus untracked, non-ignored files) or an explicit manifest.
Coverage is reported; an incomplete scan never yields a drift verdict.
"""

from __future__ import annotations

import hashlib
import json
import os
import shlex
import sqlite3
import stat
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

from .session_reader import (TEXT_PART_TYPES, _default_zcode_db, _part_rows, _scalar,
                             _zcode_session_row)
from .work_contracts import canonical_sha256
from .work_models import WorkError

MODES = ("desktop-bound", "managed-reviewer")
INITIAL_REVIEW = ("none", "latest_completed_turn")
WORKSPACE_POLICIES = ("isolated_candidate", "bound_existing")
DEFAULT_PAGE_BYTES = 32768
MIN_PAGE_BYTES = 1024
MAX_PAGE_BYTES = 1024 * 1024
MIN_CHUNK_BYTES = 512
INITIAL_CURSOR = {"message_sequence": 0, "parts_hash": None, "text_offset": 0}
CANDIDATE_FILE_BUDGET = 4096
CANDIDATE_BYTE_BUDGET = 32 * 1024 * 1024
_RUNNING_TOOL_STATUSES = ("running", "pending", "in_progress")
# finish values written by the runtime when a turn is over. Anything else —
# "tool-calls", a missing finish, a stop_recovery marker — is not terminal.
_FINISH_TERMINAL = ("stop", "completed", "end_turn")
_GIT_SKIP_PARTS = {".git"}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _connect_ro(db_path) -> sqlite3.Connection:
    path = Path(db_path).expanduser()
    connection = sqlite3.connect(f"file:{quote(str(path))}?mode=ro", uri=True)
    connection.row_factory = sqlite3.Row
    return connection


def _message_data(row) -> dict | None:
    try:
        data = json.loads(row["data"]) if row["data"] is not None else None
    except (json.JSONDecodeError, TypeError):
        return None
    return data if isinstance(data, dict) else None


def _visible(data) -> bool:
    """A hidden message (model-only reminder) is not a user-visible turn end."""
    if not isinstance(data, dict):
        return False
    semantics = data.get("semantics")
    if isinstance(semantics, dict) and semantics.get("uiVisibility") == "hidden":
        return False
    return True


def _turn_tools(connection, session_id, turn_id) -> list:
    if not isinstance(turn_id, str) or not turn_id:
        return []
    try:
        return connection.execute(
            "SELECT status, exit_code, approval_status FROM tool_usage "
            "WHERE session_id=? AND turn_id=? ORDER BY rowid",
            (session_id, turn_id)).fetchall()
    except sqlite3.Error:
        return []


def _classify_session(connection, session_id, *, with_identity=False):
    """Authoritative turn state from runtime-written fields only.

    Returns ``state`` (or ``(state, identity)`` when ``with_identity``):
    ``idle`` (no visible message yet), ``working`` (streaming assistant text,
    tool-calls phase, or a user message awaiting the turn), ``waiting_tool``,
    ``waiting_approval``, ``turn_completed``, ``failed`` (the turn ended with
    an error), ``interrupted`` (the turn was abandoned without a final
    message). ``identity`` is ``{turn_id, message_sequence, parts_hash}``.
    """
    newest = None
    newest_row = None
    for candidate in connection.execute(
            "SELECT id, sequence, data FROM message WHERE session_id=? "
            "ORDER BY sequence DESC", (session_id,)):
        data = _message_data(candidate)
        if _visible(data):
            newest, newest_row = data, candidate
            break
    if newest is None:
        identity = {"turn_id": None, "message_sequence": 0, "parts_hash": None}
        return ("idle", identity) if with_identity else "idle"

    anchor = newest.get("anchor") if isinstance(newest.get("anchor"), dict) else {}
    turn_id = anchor.get("turnId")
    time_block = newest.get("time") if isinstance(newest.get("time"), dict) else {}
    completed = time_block.get("completed")
    finish = newest.get("finish")
    role = newest.get("role")
    tools = _turn_tools(connection, session_id, turn_id)
    state = "working"
    if role == "user":
        state = "working"
    elif role == "assistant":
        terminal = completed is not None and finish in _FINISH_TERMINAL
        if terminal:
            state = "failed" if newest.get("error") else "turn_completed"
        elif finish in ("cancelled", "interrupted") and completed is not None:
            state = "interrupted"
        else:
            state = "working"
    elif role is None:
        state = "unknown"
    else:
        state = "working"

    if state in ("working", "turn_completed", "failed", "interrupted"):
        if any(tool["status"] in _RUNNING_TOOL_STATUSES for tool in tools):
            state = "waiting_tool"
        elif any(tool["approval_status"] == "pending" for tool in tools):
            state = "waiting_approval"
    if not with_identity:
        return state
    parts = _part_rows(connection, session_id, [newest_row["id"]])
    identity = {"turn_id": turn_id, "message_sequence": newest_row["sequence"],
                "parts_hash": _parts_digest(parts)}
    return state, identity


def session_status(session_id, db_path=None) -> dict:
    """Read-only located/busy view of one native session."""
    if not isinstance(session_id, str) or not session_id.strip():
        return {"status": "unlocatable", "reason": "no session id"}
    path = Path(db_path).expanduser() if db_path is not None else _default_zcode_db()
    if not path.is_file():
        return {"status": "unlocatable",
                "reason": f"zcode session database not found: {path}"}
    try:
        connection = _connect_ro(path)
    except sqlite3.Error as error:
        return {"status": "unlocatable",
                "reason": f"cannot open zcode session database read-only: {error}"}
    try:
        row = _zcode_session_row(connection, session_id)
        if row is None:
            return {"status": "unlocatable",
                    "reason": f"no session row for id {session_id!r} in {path}"}
        state, identity = _classify_session(connection, session_id,
                                            with_identity=True)
        running = _running_count(connection, session_id)
        approvals = _scalar(
            connection,
            "SELECT COUNT(*) FROM tool_usage WHERE session_id=? AND approval_status='pending'",
            session_id)
    except sqlite3.Error as error:
        return {"status": "unlocatable",
                "reason": f"zcode session database query failed: {error}"}
    finally:
        connection.close()
    return {
        "status": "located",
        "session": {"id": row["id"], "directory": row["directory"],
                    "title": row["title"], "time_created": row["time_created"],
                    "time_updated": row["time_updated"]},
        "session_state": state,
        "turn": identity,
        "running_tools": running,
        "pending_approvals": approvals,
        # Control needs a provably finished turn: no running tool, no pending
        # approval, and a state that is terminal (or an empty session).
        "idle": running == 0 and approvals == 0
                and state in ("turn_completed", "failed", "interrupted", "idle"),
    }


def _running_count(connection, session_id) -> int:
    placeholders = ",".join("?" for _ in _RUNNING_TOOL_STATUSES)
    row = connection.execute(
        f"SELECT COUNT(*) FROM tool_usage WHERE session_id=? AND status IN ({placeholders})",
        (session_id, *_RUNNING_TOOL_STATUSES)).fetchone()
    return int(row[0]) if row and row[0] is not None else 0


# ------------------------------------------------------------------ candidate


def _git_workspace(root: Path) -> str | None:
    """HEAD sha when ``root`` is a git work tree, else None."""
    try:
        probe = subprocess.run(
            ["git", "-C", str(root), "rev-parse", "--is-inside-work-tree"],
            capture_output=True, text=True, timeout=10)
    except (OSError, subprocess.SubprocessError):
        return None
    if probe.returncode != 0 or probe.stdout.strip() != "true":
        return None
    head = subprocess.run(["git", "-C", str(root), "rev-parse", "HEAD"],
                          capture_output=True, text=True, timeout=10)
    return head.stdout.strip() if head.returncode == 0 else ""


def _git_changes(root: Path) -> tuple[list[str], list[str]]:
    """The repository's change set: present paths AND deletions.

    A deletion is a real change to what the review approved: it must move
    the fingerprint, not vanish from the scan. Type changes (a file replaced
    by a symlink or a directory) cannot be fingerprinted by content alone and
    are refused instead of silently skipped.
    """
    result = subprocess.run(
        ["git", "-C", str(root), "status", "--porcelain", "-z",
         "--untracked-files=all", "--no-renames"],
        capture_output=True, timeout=30)
    if result.returncode != 0:
        raise WorkError("candidate-scan-failed",
                        "git status failed in the bound workspace")
    paths: list[str] = []
    removed: list[str] = []
    for token in result.stdout.split(b"\0"):
        if not token:
            continue
        entry = token.decode("utf-8", "surrogateescape")
        status, name = entry[:2], entry[3:]
        if not name:
            continue
        # R6-A: structure before any cache exemption. A tracked type change
        # is refused even when the name looks like a cache.
        if "T" in status:
            raise WorkError(
                "candidate-scan-failed",
                f"type change at {name!r} cannot be fingerprinted by content")
        # R6-B: only an UNTRACKED regular file at the exact
        # ``__pycache__/<name>.pyc|.pyo`` shape is a runtime cache. Tracked
        # modifications/deletions of bytecode, and real source inside
        # ``__pycache__``, stay in the fingerprint.
        if "D" in status:
            removed.append(name)
            continue
        paths.append(name)
    return sorted(set(paths)), sorted(set(removed))


def _walk_all(root: Path, budget: int) -> tuple[list[str], str | None]:
    """Every regular file under ``root`` except VCS metadata.

    Delivery diffs must see what the worker actually wrote. Bytecode caches
    are ordinary workspace files here: the verification dispatch runs with
    PYTHONDONTWRITEBYTECODE so its imports do not create any.
    """
    paths = []
    for path in sorted(root.rglob("*")):
        relative = path.relative_to(root)
        if ".git" in relative.parts:
            continue
        if path.is_symlink():
            raise WorkError(
                "candidate-scan-failed",
                f"symlink {relative.as_posix()!r} cannot be fingerprinted by "
                "content")
        if not path.is_file():
            continue
        paths.append(relative.as_posix())
        if len(paths) > budget:
            return paths[:budget], f"more than {budget} files"
    return paths, None


def _walk_changes(root: Path, budget: int) -> tuple[list[str], str | None]:
    """Full traversal with an explicit cap: over the cap the scan is
    INCOMPLETE (no fingerprint), never a silently truncated one."""
    ignored = {".git", "node_modules", ".venv", "venv", "target",
               "dist", "build", ".pixi", ".mypy_cache", ".pytest_cache"}
    paths = []
    for path in sorted(root.rglob("*")):
        relative = path.relative_to(root)
        if any(part in ignored for part in relative.parts):
            continue
        if path.is_symlink():
            # A link's "content" is its target string; two different links
            # would hash the same and a swapped target would go unnoticed.
            raise WorkError(
                "candidate-scan-failed",
                f"symlink {relative.as_posix()!r} cannot be fingerprinted by "
                "content")
        if not path.is_file():
            continue
        # R6-B: only the exact __pycache__/<name>.pyc|.pyo shape is a cache;
        # real source under __pycache__ and bytecode elsewhere stay in the
        # fingerprint.
        paths.append(relative.as_posix())
        if len(paths) > budget:
            return paths[:budget], f"more than {budget} files"
    return paths, None


def _manifest_changes(root: Path, manifest) -> list[str]:
    if manifest is None:
        return None
    if not isinstance(manifest, (list, tuple)) or not manifest:
        raise WorkError(
            "candidate-manifest-required",
            "the manifest must be a non-empty list of workspace-relative paths")
    paths = []
    for name in manifest:
        if not isinstance(name, str) or not name:
            raise WorkError("schema-invalid", "manifest entries must be paths")
        relative = Path(name)
        if relative.is_absolute() or ".." in relative.parts:
            raise WorkError("scope-conflict",
                            f"manifest entry escapes the workspace: {name}")
        paths.append(relative.as_posix())
    return sorted(set(paths))


def build_candidate(root, *, baseline=None, manifest=None,
                    file_budget=CANDIDATE_FILE_BUDGET,
                    byte_budget=CANDIDATE_BYTE_BUDGET,
                    candidate_root=None) -> dict:
    """Content-pinned candidate fingerprint of the bound workspace.

    The file set is the repository's own change set (or an explicit
    manifest), never a truncated directory walk: a budget overrun marks
    ``coverage: incomplete`` and yields NO sha256, so it can never be used as
    a drift verdict. ``candidate_root`` pins the changed bytes under
    ``<candidate_root>/<sha256>/`` so review and tests address one version.
    """
    root = Path(root).expanduser().resolve(strict=False)
    if not root.is_dir():
        raise WorkError("candidate-scan-failed",
                        f"bound workspace is not a directory: {root}")
    head = _git_workspace(root)
    scan_reason = None
    removed: list[str] = []
    base_files = (baseline or {}).get("files") \
        if isinstance(baseline, dict) else None
    if isinstance(base_files, dict):
        if (baseline or {}).get("coverage") != "complete":
            # A diff against a truncated baseline can silently drop an
            # out-of-scope deletion; refuse the candidate instead.
            raise WorkError(
                "candidate-baseline-incomplete",
                "the frozen baseline was not a complete scan; the delivery "
                "diff cannot be trusted")
        # The BASELINE is the authority, not `git status`: a worker that
        # commits an out-of-scope file moves HEAD with the commit, and the
        # file silently drops out of the change set. The scan covers every
        # tracked and untracked path (filters like build/ bypassed) so the
        # diff is complete before it is narrowed against the baseline.
        # The baseline is the authority; the CURRENT tree is walked in full
        # (including ignored and nested-repository files) so nothing the
        # worker wrote can hide behind a filter.
        # Every workspace file is scanned: bytecode caches included (the
        # verification dispatch runs with PYTHONDONTWRITEBYTECODE, so imports
        # leave none behind, and a file that IS there is a real change).
        paths, scan_reason = _walk_all(root, file_budget)
    elif head is not None:
        paths, removed = _git_changes(root)
    elif manifest is not None:
        paths = _manifest_changes(root, manifest)
    else:
        paths, scan_reason = _walk_changes(root, file_budget)
    files: dict[str, str] = {}
    modes: dict[str, str] = {}
    total = 0
    coverage = "incomplete" if scan_reason else "complete"
    reason = scan_reason
    for name in paths:
        path = root / name
        if path.is_symlink():
            # A link's target is part of what the review approved; hashing
            # only the (unread) bytes would silently drop the change.
            raise WorkError(
                "candidate-scan-failed",
                f"symlink {name!r} cannot be fingerprinted by content")
        if not path.is_file():
            continue
        if len(files) >= file_budget:
            coverage, reason = "incomplete", f"more than {file_budget} files"
            break
        data = path.read_bytes()
        total += len(data)
        if total > byte_budget:
            coverage, reason = "incomplete", f"more than {byte_budget} bytes"
            break
        files[name] = hashlib.sha256(data).hexdigest()
        modes[name] = "100755" if path.stat().st_mode & 0o111 else "100644"
    if isinstance(base_files, dict):
        # Keep only what differs from the frozen baseline: the candidate IS
        # the delta, and a committed out-of-scope file cannot fall out of it.
        base_modes = (baseline or {}).get("modes") or {}
        kept = {name: digest for name, digest in files.items()
                if base_files.get(name) != digest
                or (base_modes and base_modes.get(name) != modes.get(name))}
        removed = sorted(set(base_files) - set(files))
        files = kept
        modes = {name: modes[name] for name in kept}
        total = sum((root / name).stat().st_size for name in kept)
    document = {"head": head, "files": files, "modes": modes,
                "coverage": coverage,
                "file_count": len(files), "byte_count": total}
    if removed:
        # deletions are part of what a candidate IS, so they change its sha
        document["removed"] = removed
    digest = canonical_sha256(document) if coverage == "complete" else None
    result = {**document, "workspace": str(root), "sha256": digest}
    if baseline is not None:
        if baseline.get("coverage") != "complete":
            raise WorkError(
                "candidate-baseline-incomplete",
                "the frozen baseline is incomplete; a diff against it could "
                "hide an out-of-scope write or deletion, so nothing is "
                "delivered against it")
        base = baseline.get("files") or {}
        base_modes = baseline.get("modes") or {}
        base_complete = baseline.get("coverage", "complete") == "complete"
        if isinstance(base_files, dict):
            # `files` was already narrowed to the baseline delta above.
            result["changes"] = {
                "changed": sorted(n for n in files if n in base),
                "added": sorted(n for n in files if n not in base),
                "removed": sorted(removed),
            }
        elif coverage == "complete" and base_complete:
            result["changes"] = {
                "changed": sorted(
                    n for n in base.keys() & files.keys()
                    if base[n] != files[n]
                    or (base_modes and base_modes.get(n) != modes.get(n))),
                "added": sorted(files.keys() - base.keys()),
                "removed": sorted((base.keys() - files.keys()) | set(removed)),
            }
        else:
            result["changes"] = None
            result["drift"] = "coverage-incomplete"
    if candidate_root is not None and digest is not None:
        target = Path(candidate_root) / digest
        manifest_path = target / "manifest.json"
        if not manifest_path.exists():
            (target / "files").mkdir(parents=True, exist_ok=True)
            for name in files:
                destination = target / "files" / name
                destination.parent.mkdir(parents=True, exist_ok=True)
                destination.write_bytes((root / name).read_bytes())
                if modes.get(name) == "100755":
                    os.chmod(destination, 0o755)
            manifest_path.write_text(
                json.dumps(document, sort_keys=True, indent=2), encoding="utf-8")
        result["artifact"] = {"root": str(target),
                              "manifest": str(manifest_path)}
    if reason is not None:
        result["reason"] = reason
    return result



def _head_entries(root: Path):
    """(mode, object id, name) for every blob in the HEAD tree."""
    listing = subprocess.run(
        ["git", "-C", str(root), "ls-tree", "-r", "-z", "HEAD"],
        check=True, capture_output=True)
    entries = []
    for record in listing.stdout.decode("utf-8", "surrogateescape").split("\0"):
        if not record:
            continue
        meta, _, name = record.partition("\t")
        parts = meta.split()
        if len(parts) < 3:
            continue
        entries.append((parts[0], parts[2], name))
    return entries


def _blob_bytes(root: Path, oids) -> dict:
    """All requested blobs in one `git cat-file --batch` round trip."""
    wanted = sorted(set(oids))
    if not wanted:
        return {}
    proc = subprocess.run(
        ["git", "-C", str(root), "cat-file", "--batch"],
        input=("\n".join(wanted) + "\n").encode(),
        check=True, capture_output=True)
    output = proc.stdout
    offset, result = 0, {}
    while offset < len(output):
        line_end = output.find(b"\n", offset)
        if line_end < 0:
            break
        header = output[offset:line_end].decode("utf-8", "surrogateescape")
        offset = line_end + 1
        parts = header.split()
        if len(parts) < 3:
            continue
        oid, kind, size = parts[0], parts[1], int(parts[2])
        data = output[offset:offset + size]
        offset += size + 1
        if kind == "blob":
            result[oid] = data
    return result


def _baseline_scan(root: Path, budget: int) -> tuple[list[str], str | None]:
    """Paths a candidate may touch: tracked + untracked, filters bypassed.

    A committed ``build/extra.txt`` or a new symlink must stay visible even
    though the ordinary walk skips those directories: the baseline diff is
    the delivery's write set, not a best-effort scan.
    """
    if _git_workspace(root) is not None:
        try:
            listing = subprocess.run(
                ["git", "-C", str(root), "ls-files", "-z"],
                check=True, capture_output=True)
            tracked = [name for name in listing.stdout.decode(
                "utf-8", "surrogateescape").split("\0") if name]
            # NO --exclude-standard: a .gitignore'd out-of-scope file is
            # still a file the delivery wrote; the baseline diff must see it.
            untracked = subprocess.run(
                ["git", "-C", str(root), "ls-files", "-z", "--others"],
                check=True, capture_output=True)
            fresh = []
            for name in untracked.stdout.decode(
                    "utf-8", "surrogateescape").split("\0"):
                if not name:
                    continue
                fresh.append(name)
            # `git ls-files --others` reports a nested repository (and any
            # untracked directory it collapses) with a trailing slash.
            names = sorted(dict.fromkeys(name.rstrip("/")
                                         for name in tracked + fresh
                                         if name.rstrip("/")))
            expanded = []
            for name in names:
                path = root / name
                if path.is_dir() and not path.is_symlink():
                    # `git ls-files --others` reports a nested repository as
                    # ONE directory; its files are still the delivery's writes
                    inner, reason = _walk_changes(path, budget)
                    if reason:
                        return expanded + [f"{name}/{item}"
                                           for item in inner], reason
                    expanded.extend(f"{name}/{item.lstrip('/')}"
                                    for item in inner)
                else:
                    expanded.append(name)
            if len(expanded) > budget:
                return expanded[:budget], f"more than {budget} files"
            return expanded, None
        except (OSError, subprocess.CalledProcessError):
            pass
    return _walk_changes(root, budget)


def build_baseline(root, *, file_budget=CANDIDATE_FILE_BUDGET,
                   byte_budget=CANDIDATE_BYTE_BUDGET) -> dict:
    """A FULL snapshot of the workspace: the attempt's frozen baseline.

    ``build_candidate`` without a baseline answers "what changed since HEAD",
    which is empty on a clean checkout and stays empty when the worker
    COMMITS their changes. The baseline therefore has to describe every file
    the review approved; candidates are diffed against it.
    """
    root = Path(root).expanduser().resolve(strict=False)
    if not root.is_dir():
        raise WorkError("candidate-scan-failed",
                        f"bound workspace is not a directory: {root}")
    head = _git_workspace(root)
    entries = None
    if head is not None:
        try:
            entries = _head_entries(root)
        except (OSError, subprocess.CalledProcessError):
            entries = None
    files: dict[str, str] = {}
    modes: dict[str, str] = {}
    total = 0
    coverage, reason = "complete", None
    if entries is not None:
        # The baseline is the COMMITTED object graph, not the worktree: a
        # modification made before the claim cannot be absorbed into the
        # approved state. Untracked deliveries stay outside it, so they
        # remain visible as candidates even after a later `git commit`.
        blobs = _blob_bytes(root, [oid for _, oid, _ in entries])
        for mode, oid, name in entries:
            if mode == "120000":
                raise WorkError(
                    "candidate-scan-failed",
                    f"symlink {name!r} cannot be fingerprinted by content")
            data = blobs.get(oid)
            if data is None:
                continue
            if len(files) >= file_budget:
                coverage, reason = "incomplete", f"more than {file_budget} files"
                break
            total += len(data)
            if total > byte_budget:
                coverage, reason = "incomplete", f"more than {byte_budget} bytes"
                break
            files[name] = hashlib.sha256(data).hexdigest()
            modes[name] = "100755" if int(mode, 8) & 0o111 else "100644"
    else:
        # The baseline must use the SAME traversal as the candidate diff:
        # filtering build/ here would hide a later deletion of a file that
        # existed before the claim (and make an empty diff look clean).
        paths, reason = _walk_all(root, file_budget)
        coverage = "incomplete" if reason else "complete"
        for name in paths:
            path = root / name
            if path.is_symlink():
                raise WorkError(
                    "candidate-scan-failed",
                    f"symlink {name!r} cannot be fingerprinted by content")
            if not path.is_file():
                continue
            if len(files) >= file_budget:
                coverage, reason = "incomplete", f"more than {file_budget} files"
                break
            data = path.read_bytes()
            total += len(data)
            if total > byte_budget:
                coverage, reason = "incomplete", f"more than {byte_budget} bytes"
                break
            files[name] = hashlib.sha256(data).hexdigest()
            modes[name] = "100755" if path.stat().st_mode & 0o111 else "100644"
    result = {"workspace": str(root), "head": head,
              "files": files, "modes": modes, "coverage": coverage,
              "file_count": len(files), "byte_count": total}
    document = {key: result[key] for key in
                ("head", "files", "modes", "coverage", "file_count",
                 "byte_count")}
    result["sha256"] = canonical_sha256(document) \
        if coverage == "complete" else None
    if reason is not None:
        result["reason"] = reason
    return result


def _require_complete_candidate(candidate) -> str:
    digest = candidate.get("sha256")
    if not digest or candidate.get("coverage") != "complete":
        raise WorkError(
            "candidate-coverage-incomplete",
            "the candidate scan is incomplete "
            f"({candidate.get('reason') or 'unknown reason'}); an incomplete "
            "scan cannot decide drift or drive a round")
    return digest


def workspace_changes(binding: dict) -> dict:
    """Baseline vs now, or an explicit "coverage-incomplete" — never a guess."""
    baseline = (binding.get("evidence") or {}).get("baseline")
    if not baseline:
        return {"changed": [], "added": [], "removed": [],
                "baseline_truncated": True, "reason": "no baseline recorded"}
    current = build_candidate(binding["workspace"], baseline=baseline)
    changes = current.get("changes")
    if changes is None:
        return {"changed": [], "added": [], "removed": [],
                "baseline_truncated": True, "reason": current.get("reason"),
                "drift": "coverage-incomplete"}
    return {**changes, "baseline_truncated": False}


# ------------------------------------------------------------------- binding


def _verify_handoff(hub_state_dir, session_id, proof) -> dict:
    """Protocol-verified takeover proof: OUR OWN hub task record whose
    session locator names this session (the same locator ``attach``
    re-validates before resuming)."""
    if not hub_state_dir:
        raise WorkError(
            "control-unproven",
            "no hub state directory is configured, so no takeover of this "
            "session can be proven; bind observe-only")
    root = Path(hub_state_dir) / "hub" / "tasks"
    if not root.is_dir():
        # Older callers pass the hub's own task directory directly.
        root = Path(hub_state_dir) / "tasks"
    if not root.is_dir():
        raise WorkError("control-unproven",
                        "no hub task records exist, so no session takeover can "
                        "be proven; bind observe-only")
    candidates = []
    if isinstance(proof, dict) and proof.get("task_id"):
        path = root / f"{proof['task_id']}.json"
        candidates = [path] if path.is_file() else []
        if not candidates:
            raise WorkError("handoff-unverified",
                            f"no hub task record {proof['task_id']!r}")
    else:
        candidates = sorted(root.glob("*.json"),
                            key=lambda item: item.stat().st_mtime, reverse=True)
    for path in candidates:
        try:
            record = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        locator = record.get("session_locator")
        if not isinstance(locator, dict):
            continue
        if locator.get("session_id") != session_id:
            continue
        if record.get("worker") != "zcode":
            continue
        return {"task_id": record.get("task_id") or path.stem,
                "worker": record.get("worker"),
                "session_id": session_id,
                "workspace": record.get("workspace"),
                "verified_at": _now()}
    raise WorkError(
        "control-unproven",
        "no recorded hub task resumes this session, so control cannot be "
        "proven; bind observe-only (dispatch would have to start a new "
        "session, which this binding must never do)")


def claim_native_session(store, request, actor, *,
                         hub_state_dir=None) -> dict:
    """Claim a user-created native session for review, without hub proof.

    A hub handoff proves control because the hub itself dispatched into the
    session. A native GUI session was created by the user, so the proof here
    is what the host can actually observe: the live session id, its own
    directory, and a finished turn. The claim grants ``review`` -- the loop
    may read turns and deliver the review prompt through a desktop channel --
    and it never grants ``control``: no host channel exists that inserts an
    instruction into a native chat, so a dispatched round would be a lie.
    """
    if not isinstance(request, dict):
        raise WorkError("schema-invalid", "claim request must be an object")
    actor.require("work:authorize")
    session_id = request.get("session_id")
    db_path = request.get("db_path")
    workspace = request.get("workspace")
    work_id = request.get("work_id")
    if not isinstance(work_id, str) or not work_id:
        raise WorkError("schema-invalid", "a native claim must name its work")
    status = session_status(session_id, db_path)
    if status["status"] != "located":
        raise WorkError("session-unlocatable",
                        status.get("reason", "session not found"))
    if not status["idle"]:
        raise WorkError(
            "session-busy",
            f"session is {status.get('session_state')!r}; a native claim "
            "waits for a finished turn so the observed identity is stable")
    session = status["session"]
    directory = Path(session["directory"]).resolve(strict=False)
    if not directory.is_dir():
        raise WorkError("session-unlocatable",
                        "session directory is not a readable directory")
    if workspace is not None \
            and Path(workspace).resolve(strict=False) != directory:
        raise WorkError(
            "scope-conflict",
            "a native claim pins the session's own directory exactly")
    # The session that REGISTERS the identity and the repository that will be
    # scanned are different things: this session lives in a science tree, the
    # authorized work happens in code_root. Never scan the session directory
    # unless the caller explicitly says that is the code root.
    code_root = request.get("code_root")
    root = (Path(code_root).expanduser().resolve(strict=False)
            if code_root is not None else directory)
    if not root.is_dir():
        raise WorkError("scope-conflict",
                        "code_root must be an existing directory")
    reviewer_thread = request.get("reviewer_thread")
    if not isinstance(reviewer_thread, str) or not reviewer_thread:
        raise WorkError(
            "schema-invalid",
            "a native claim must name the paired reviewer_thread (the Codex "
            "chat is a different identity from the ZCode session)")
    reviewer_host = request.get("reviewer_host", "local")
    # The claim reviews the LATEST completed turn, not the whole history:
    # locate THAT turn through its authoritative anchor, never by table/role
    # guesses and never by silently falling back to zero.
    cursor = dict(INITIAL_CURSOR)
    native_db = (Path(db_path).expanduser() if db_path is not None
                 else _default_zcode_db())
    turn_id = (status.get("turn") or {}).get("turn_id")
    if not isinstance(turn_id, str) or not turn_id:
        raise WorkError("native-cursor-unresolved",
                        "the claimed session exposes no completed turn identity")
    try:
        with _connect_ro(native_db) as connection:
            rows = connection.execute(
                "SELECT sequence, data FROM message WHERE session_id=? "
                "ORDER BY sequence", (session_id,)).fetchall()
    except sqlite3.Error as error:
        raise WorkError("native-cursor-unresolved",
                        f"cannot read the native message table: {error}")
    sequences = []
    for row in rows:
        data = _message_data(row)
        anchor = (data or {}).get("anchor") or {}
        if isinstance(anchor, dict) and anchor.get("turnId") == turn_id:
            sequences.append(int(row["sequence"]))
    if not sequences:
        raise WorkError(
            "native-cursor-unresolved",
            f"no message rows anchor turn {turn_id!r}; refusing to hand the "
            "whole history to the reviewer")
    cursor["message_sequence"] = max(0, min(sequences) - 1)
    binding_id = "native-" + canonical_sha256({
        "session_id": session_id, "mode": "desktop-bound",
        "workspace_policy": "bound_existing",
        "workspace": str(root), "reviewer_thread": reviewer_thread})[:24]
    existing = store.get_session_binding(binding_id)
    if existing is not None:
        # A repeated claim with the same terms must NOT rewind the review
        # cursor: turns already processed stay processed, so the same turn is
        # never reviewed twice. Refresh only what legitimately changed.
        merged = dict(existing)
        merged["work_id"] = work_id
        merged["authorization_ref"] = (request.get("authorization_ref")
                                       or existing.get("authorization_ref"))
        merged["updated"] = _now()
        merged_evidence = dict(existing.get("evidence") or {})
        # The baseline frozen at the first claim is the delivery comparison
        # point. Refreshing it here would let a file written outside the
        # authorized scope disappear from the next candidate diff.
        merged_evidence.setdefault("baseline", build_baseline(root))
        merged_evidence["work_id"] = work_id
        claim = dict(merged_evidence.get("native_claim") or {})
        claim.update({"observed_turn": status.get("turn"),
                      "claimed_at": _now(), "code_root": str(root),
                      "session_id": session_id,
                      "reviewer_thread": reviewer_thread})
        merged_evidence["native_claim"] = claim
        merged["evidence"] = merged_evidence
        store.save_session_binding(merged)
        return merged
    evidence = {
        "session_directory": session["directory"],
        "code_root": str(root),
        "session_time_updated": session["time_updated"],
        "bound_at": _now(),
        "turn": status.get("turn"),
        "workspace_identity": workspace_identity(root),
        "baseline": build_baseline(root),
        "reviewer": {"thread_id": reviewer_thread, "host": reviewer_host,
                     "kind": "desktop", "source": "pairing"},
        "native_claim": {
            "session_id": session_id,
            "directory": str(directory),
            "code_root": str(root),
            "reviewer_thread": reviewer_thread,
            "observed_turn": status.get("turn"),
            "provenance": "user-authorized native session; no hub task record",
            "claimed_at": _now(),
        },
        "work_id": work_id,
    }
    record = {
        "binding_id": binding_id, "session_id": session_id, "work_id": work_id,
        "db_path": str(native_db),
        "mode": "desktop-bound", "state": "bound",
        "capabilities": ["observe", "review"],
        "workspace": str(root), "workspace_policy": "bound_existing",
        "authorization_ref": request.get("authorization_ref"),
        "initial_review": "latest_completed_turn",
        "cursor": cursor, "revision": 1,
        "evidence": evidence, "created": _now(), "updated": _now(),
    }
    store.save_session_binding(record)
    return record


def bind_session(store, request, actor, *, hub_state_dir=None) -> dict:
    """Bind an existing native session (host-granted; observe or control)."""
    if not isinstance(request, dict):
        raise WorkError("schema-invalid", "bind request must be an object")
    actor.require("work:authorize")
    session_id = request.get("session_id")
    db_path = request.get("db_path")
    mode = request.get("mode", "managed-reviewer")
    initial_review = request.get("initial_review", "none")
    policy = request.get("workspace_policy", "isolated_candidate")
    if mode not in MODES:
        raise WorkError("schema-invalid", f"mode must be one of {MODES}")
    if initial_review not in INITIAL_REVIEW:
        raise WorkError("schema-invalid",
                        "initial_review must be none or latest_completed_turn")
    if policy not in WORKSPACE_POLICIES:
        raise WorkError("schema-invalid",
                        f"workspace_policy must be one of {WORKSPACE_POLICIES}")

    status = session_status(session_id, db_path)
    if status["status"] != "located":
        raise WorkError("session-unlocatable", status.get("reason", "session not found"))
    session = status["session"]

    workspace = None
    if policy == "bound_existing":
        directory = Path(session["directory"]).resolve(strict=False)
        if not directory.is_dir():
            raise WorkError("session-unlocatable",
                            "session directory is not a readable directory")
        requested = request.get("workspace")
        if requested is not None \
                and Path(requested).resolve(strict=False) != directory:
            raise WorkError(
                "scope-conflict",
                "bound_existing must pin the session's own directory exactly")
        workspace = str(directory)

    binding_id = "bind-" + canonical_sha256({
        "session_id": session_id, "mode": mode, "control": bool(request.get("control")),
        "workspace_policy": policy, "workspace": workspace})[:24]

    capabilities = ["observe"]
    handoff = None
    if request.get("control"):
        # Control demands a provably finished turn, an idle runtime AND
        # exclusivity, and it demands the handoff proof that lets dispatch
        # CONTINUE this session instead of starting a new one.
        if not status["idle"]:
            raise WorkError(
                "session-busy",
                f"session is {status.get('session_state')!r} with "
                f"{status['running_tools']} running tools and "
                f"{status['pending_approvals']} pending approvals; "
                "bind observe-only and retry control once the turn is over")
        for other in store.session_bindings(session_id):
            if other["binding_id"] != binding_id \
                    and "control" in other["capabilities"] and other["state"] == "bound":
                raise WorkError(
                    "binding-conflict",
                    f"session already controlled by binding {other['binding_id']}")
        handoff = _verify_handoff(hub_state_dir, session_id, request.get("handoff"))
        capabilities.append("control")
    existing = store.get_session_binding(binding_id)
    evidence = {
        "session_directory": session["directory"],
        "session_time_updated": session["time_updated"],
        "bound_at": _now(),
        "turn": status.get("turn"),
        "db_path": str(Path(db_path).expanduser()) if db_path else None,
        "workspace_identity": (workspace_identity(workspace)
                               if workspace else None),
    }
    if handoff is not None:
        evidence["handoff"] = handoff
    if policy == "bound_existing":
        evidence["baseline"] = build_baseline(Path(workspace))
    if request.get("work_id"):
        # the work this binding drives: the pump derives its dispatch source
        # from it (the table's fixed column list has no room for the field)
        evidence["work_id"] = request["work_id"]
    record = {
        "binding_id": binding_id, "session_id": session_id,
        "work_id": request.get("work_id"),
        "db_path": str(db_path) if db_path is not None else None,
        "mode": mode, "state": "bound", "capabilities": capabilities,
        "workspace": workspace, "workspace_policy": policy,
        "authorization_ref": request.get("authorization_ref"),
        "initial_review": initial_review,
        "cursor": dict(INITIAL_CURSOR),
        "revision": 1, "evidence": evidence,
        "created": _now(), "updated": _now(),
    }
    if existing is not None:
        if existing["capabilities"] != capabilities or existing["state"] != "bound":
            raise WorkError("binding-conflict",
                            f"binding {binding_id} already exists with different terms")
        return {**existing, "idempotent": True}
    store.save_session_binding(record)
    return {**record, "idempotent": False}


def workspace_identity(path) -> dict:
    """Device/inode identity of a bound workspace.

    Two owners can share a path string while one of them deletes and recreates
    the directory; the inode is what tells the reviewer it is looking at a
    different directory than the one it bound.
    """
    try:
        stat = Path(path).stat()
    except OSError as error:
        raise WorkError("workspace-unreadable",
                        f"cannot stat the bound workspace {str(path)!r}: {error}")
    return {"dev": int(stat.st_dev), "ino": int(stat.st_ino)}


def session_model(session_id, db_path=None) -> dict:
    """The model the session itself recorded for its latest turn.

    A resumed session keeps its own model; a contract that names a different
    model cannot be enforced by the CLI, so the caller must refuse instead of
    pretending the contract won.
    """
    path = Path(db_path or _default_zcode_db())
    connection = sqlite3.connect(f"file:{path}?mode=ro", uri=True, timeout=5)
    connection.row_factory = sqlite3.Row
    try:
        row = connection.execute(
            "SELECT data FROM message WHERE session_id=? "
            "ORDER BY sequence DESC LIMIT 1", (session_id,)).fetchone()
    finally:
        connection.close()
    if row is None:
        return {}
    try:
        data = json.loads(row["data"]) if row["data"] else {}
    except (TypeError, json.JSONDecodeError):
        return {}
    if not isinstance(data, dict) or not data.get("modelId"):
        return {}
    return {"model": data["modelId"], "provider": data.get("providerId")}


def _parts_digest(rows) -> str:
    return hashlib.sha256(
        "\n".join(row["data"] or "" for row in rows).encode("utf-8")).hexdigest()


def _slice_text(text: str, start: int, budget: int) -> tuple[str, int, bool]:
    """UTF-8 safe byte slice: ``(chunk, consumed_bytes, complete)``."""
    encoded = text.encode("utf-8")
    if start >= len(encoded):
        return "", start, True
    end = min(start + budget, len(encoded))
    while end < len(encoded):
        try:
            encoded[:end].decode("utf-8")
            break
        except UnicodeDecodeError:
            end -= 1
    return encoded[start:end].decode("utf-8", "ignore"), end, end >= len(encoded)


def read_session_delta(binding, cursor=None, max_bytes=DEFAULT_PAGE_BYTES,
                       db_path=None) -> dict:
    """Page new message text after ``cursor`` under a byte budget.

    Cursor: ``{"message_sequence": N, "parts_hash": h, "text_offset": b}``.
    Messages with a higher sequence are new; if the stored parts digest of
    message N changed (streaming parts update IN PLACE) that message is
    re-emitted from its start. A message whose text does not fit the budget
    is continued at ``text_offset`` with ``continuation=True`` and
    ``text_truncated=True`` — text is never silently dropped.
    """
    if not isinstance(binding, dict) or not binding.get("session_id"):
        raise WorkError("schema-invalid", "binding must carry a session_id")
    if cursor is None:
        stored = binding.get("cursor")
        cursor = dict(stored) if isinstance(stored, dict) else dict(INITIAL_CURSOR)
    if not isinstance(cursor, dict) or not isinstance(cursor.get("message_sequence"), int):
        raise WorkError("schema-invalid", "cursor must carry message_sequence")
    start_offset = cursor.get("text_offset") or 0
    if not isinstance(start_offset, int) or start_offset < 0:
        raise WorkError("schema-invalid", "cursor text_offset must be a byte count")
    budget = max(min(int(max_bytes), MAX_PAGE_BYTES), MIN_PAGE_BYTES)

    path = Path(db_path).expanduser() if db_path is not None \
        else Path(binding.get("db_path") or _default_zcode_db())
    if not path.is_file():
        return {"status": "unlocatable",
                "reason": f"zcode session database not found: {path}",
                "cursor": cursor, "messages": [], "session_state": "unknown"}
    try:
        connection = _connect_ro(path)
    except sqlite3.Error as error:
        return {"status": "unlocatable",
                "reason": f"cannot open zcode session database read-only: {error}",
                "cursor": cursor, "messages": [], "session_state": "unknown"}

    session_state = "unknown"
    try:
        row = _zcode_session_row(connection, binding["session_id"])
        if row is None:
            return {"status": "unlocatable",
                    "reason": "session row disappeared",
                    "cursor": cursor, "messages": [], "session_state": "unknown"}
        session_state, identity = _classify_session(
            connection, binding["session_id"], with_identity=True)
        after = connection.execute(
            "SELECT id, sequence, time_created, data FROM message "
            "WHERE session_id=? AND sequence>? ORDER BY sequence",
            (binding["session_id"], cursor["message_sequence"])).fetchall()
        recheck = connection.execute(
            "SELECT id, sequence, time_created, data FROM message "
            "WHERE session_id=? AND sequence=?",
            (binding["session_id"], cursor["message_sequence"])).fetchone() \
            if cursor["message_sequence"] > 0 else None
        part_ids = [item["id"] for item in after]
        if recheck is not None:
            part_ids.append(recheck["id"])
        parts_by_message: dict[str, list] = {}
        for part in _part_rows(connection, binding["session_id"], part_ids):
            parts_by_message.setdefault(part["message_id"], []).append(part)
    except sqlite3.Error as error:
        return {"status": "unlocatable",
                "reason": f"zcode session database query failed: {error}",
                "cursor": cursor, "messages": [], "session_state": "unknown"}
    finally:
        connection.close()

    def _entry(message_row) -> dict:
        parts = parts_by_message.get(message_row["id"], [])
        data = _message_data(message_row)
        text_parts = []
        counts: dict[str, int] = {}
        for part in parts:
            try:
                part_data = json.loads(part["data"]) if part["data"] is not None else None
            except (json.JSONDecodeError, TypeError):
                part_data = None
                part_type = None
            else:
                part_type = part_data.get("type") if isinstance(part_data, dict) else None
            key = str(part_type) if part_type is not None else "<no-type>"
            counts[key] = counts.get(key, 0) + 1
            if isinstance(part_data, dict) and part_type in TEXT_PART_TYPES \
                    and isinstance(part_data.get("text"), str):
                text_parts.append(part_data["text"])
        return {
            "sequence": message_row["sequence"],
            "time_created": message_row["time_created"],
            "role": data.get("role") if isinstance(data, dict) else None,
            "type": data.get("type") if isinstance(data, dict) else None,
            "text": "".join(text_parts),
            "part_types": counts,
        }

    messages: list[dict] = []
    used = 0
    continuation = False
    new_cursor = {"message_sequence": cursor["message_sequence"],
                  "parts_hash": cursor.get("parts_hash"),
                  "text_offset": start_offset}

    def _emit(message_row, *, offset: int, re_emitted: bool = False) -> bool:
        nonlocal used, continuation, new_cursor
        entry = _entry(message_row)
        text = entry["text"]
        total_bytes = len(text.encode("utf-8"))
        remaining = budget - used
        if remaining < MIN_CHUNK_BYTES and (messages or used):
            continuation = True
            return False
        chunk, consumed, complete = _slice_text(text, offset, max(remaining, 1))
        entry["text"] = chunk
        entry["text_offset"] = offset
        entry["text_bytes"] = total_bytes
        entry["text_truncated"] = not complete
        if re_emitted:
            entry["re_emitted"] = True
        messages.append(entry)
        used += len(chunk.encode("utf-8"))
        digest = _parts_digest(parts_by_message.get(message_row["id"], []))
        if complete:
            new_cursor = {"message_sequence": message_row["sequence"],
                          "parts_hash": digest, "text_offset": 0}
            return True
        new_cursor = {"message_sequence": message_row["sequence"],
                      "parts_hash": digest, "text_offset": consumed}
        continuation = True
        return False

    if recheck is not None and start_offset > 0:
        # the cursor points INTO this message: emit the next slice from the
        # recorded byte offset, never re-send its head
        if not _emit(recheck, offset=start_offset):
            return _page(row, session_state, messages, used, continuation, new_cursor,
                         identity)
    elif recheck is not None:
        digest = _parts_digest(parts_by_message.get(recheck["id"], []))
        if cursor.get("parts_hash") is not None and digest != cursor["parts_hash"] \
                and not _emit(recheck, offset=0, re_emitted=True):
            return _page(row, session_state, messages, used, continuation, new_cursor,
                         identity)

    for message_row in after:
        if not _emit(message_row, offset=0):
            break

    return _page(row, session_state, messages, used, continuation, new_cursor, identity)


def _page(row, session_state, messages, used, continuation, cursor, identity) -> dict:
    return {
        "status": "ok",
        "session": {"id": row["id"], "directory": row["directory"],
                    "time_updated": row["time_updated"]},
        "session_state": session_state,
        "turn": identity,
        "messages": messages,
        "page_bytes": used,
        "continuation": continuation,
        "cursor": cursor,
    }


def session_turn_identity(binding, db_path=None) -> dict:
    """Latest user-visible message identity, without paging any text.

    Decision staleness compares THIS (turn id + sequence) — an independent
    query that cannot be confused by a page cursor or by re-emitted text.
    """
    if not isinstance(binding, dict) or not binding.get("session_id"):
        raise WorkError("schema-invalid", "binding must carry a session_id")
    path = Path(db_path).expanduser() if db_path is not None \
        else Path(binding.get("db_path") or _default_zcode_db())
    if not path.is_file():
        return {"status": "unlocatable", "turn": None}
    try:
        connection = _connect_ro(path)
    except sqlite3.Error as error:
        return {"status": "unlocatable", "reason": str(error), "turn": None}
    try:
        state, identity = _classify_session(connection, binding["session_id"],
                                           with_identity=True)
    except sqlite3.Error as error:
        return {"status": "unlocatable", "reason": str(error), "turn": None}
    finally:
        connection.close()
    return {"status": "located", "session_state": state, "turn": identity}


def next_round_page(binding, *, max_bytes=DEFAULT_PAGE_BYTES, db_path=None) -> dict:
    """One page from the binding's stored cursor (never from message 0)."""
    return read_session_delta(binding, max_bytes=max_bytes, db_path=db_path)


# ------------------------------------------------------------------ reviewer


def desktop_wakeup_capability() -> dict:
    """Can an EXTERNAL process wake the desktop Codex chat?

    Honest boundary: the in-chat ``send_message_to_thread`` tool is NOT an
    externally callable API — a chat-internal tool cannot be invoked by a
    background Python service. Automatic desktop wakeup therefore requires
    a HOST-VALIDATED wake entry point (``CZS_DESKTOP_WAKE_CMD``); without
    one the capability is explicitly ``desktop-wakeup-unsupported`` and
    no managed reviewer runs in its place.
    """
    command = os.environ.get("CZS_DESKTOP_WAKE_CMD", "").strip()
    if not command:
        return {"capability": "desktop-wakeup-unsupported",
                "reason": "no host-validated desktop wake entry point is "
                          "configured (CZS_DESKTOP_WAKE_CMD); the desktop "
                          "chat continues only while active"}
    return {"capability": "desktop-wake-command", "wake_command": command}


def wake_desktop(prompt_path, *, timeout_s=600, capability=None) -> dict:
    """Deliver one review request through the host-validated desktop entry.

    The command is expected to hand ``prompt_path`` to the DESKTOP chat; a
    non-zero exit or timeout is a failure, never a delivered review.
    """
    capability = capability or desktop_wakeup_capability()
    if capability.get("capability") != "desktop-wake-command":
        raise WorkError("desktop-wakeup-unsupported",
                        capability.get("reason", "desktop wakeup is unavailable"))
    argv = shlex.split(capability["wake_command"]) + [str(prompt_path)]
    try:
        result = subprocess.run(argv, capture_output=True, text=True,
                                timeout=timeout_s)
    except subprocess.TimeoutExpired as error:
        raise WorkError("desktop-wakeup-failed",
                        f"desktop wake command timed out after {timeout_s}s") from error
    except OSError as error:
        raise WorkError("desktop-wakeup-failed",
                        f"desktop wake command could not run: {error}") from error
    if result.returncode != 0:
        detail = (result.stderr or result.stdout or "").strip()[-500:]
        raise WorkError("desktop-wakeup-failed",
                        f"desktop wake command exited {result.returncode}: {detail}")
    return {"outcome": "delivered", "kind": "desktop",
            "command": argv[0], "exit_code": result.returncode,
            "stdout": (result.stdout or "").strip()[-500:]}


class SessionReviewer:
    """Managed reviewer: ONE fixed Codex app-server thread across rounds.

    The pinned identity is the REVIEW THREAD, not a connection: round 1
    starts the thread on a fresh app-server connection; every later round
    opens its own bounded connection and continues the SAME thread via
    run_task(resume_thread=...) — review context accumulates server-side,
    and no per-connection router state leaks across rounds (a second
    run_task on one connection cannot resubscribe the same thread key).
    A failed/interrupted/timeout reviewer turn is NEVER a decision.
    """

    kind = "managed"

    def __init__(self, driver_factory, workspace, *, model=None, thread_id=None):
        self._driver_factory = driver_factory
        self.workspace = workspace
        self.model = model
        # A persisted thread id lets a restarted owner continue the SAME
        # review thread instead of silently opening a new one.
        self.thread_id = thread_id
        self.rounds: list[dict] = []

    async def review_round(self, authority, prompt, *, timeout_s=600,
                           submission_id=None) -> dict:
        driver = self._driver_factory()
        receipt = {"outcome": "failed"}
        if submission_id:
            # The submission id makes the review turn findable after a crash:
            # the id travels INSIDE the prompt and as the user message's
            # client id, so recovery can ask the thread what happened instead
            # of guessing.
            prompt = f"[zsc-review-submission {submission_id}]\n{prompt}"
        try:
            starter = getattr(getattr(driver, "client", None), "start", None)
            if starter is not None:
                await starter()
            receipt = await driver.run_task(
                authority, self.workspace, prompt,
                model=self.model, timeout_s=timeout_s,
                resume_thread=self.thread_id, expect_model=self.model,
                client_message_id=submission_id)
        finally:
            import contextlib
            closer = getattr(getattr(driver, "client", None), "close", None)
            if closer is not None:
                with contextlib.suppress(Exception):
                    await closer()
        if receipt.get("outcome") != "delivered":
            raise WorkError(
                "review-round-failed",
                f"the reviewer turn ended {receipt.get('outcome')!r} "
                "(failed/interrupted/timeout) — no decision is inferred")
        thread_id = receipt.get("thread_id")
        if self.thread_id is None:
            self.thread_id = thread_id  # round 1 pins the review thread
        elif thread_id != self.thread_id:
            raise WorkError(
                "review-thread-drift",
                f"the reviewer answered from thread {thread_id!r}, "
                f"expected the pinned {self.thread_id!r}")
        record = {"round": len(self.rounds) + 1, "thread_id": thread_id,
                  "turn_id": receipt.get("turn_id")}
        self.rounds.append(record)
        return {**record, "final_message": receipt.get("final_message"),
                "command_items": receipt.get("command_items", [])}

    async def find_finished(self, submission_id: str):
        """Ask the pinned review thread what happened to one submission.

        ``None`` means the request never reached the thread (a restart before
        the send): the caller may retry the SAME submission id. A found turn
        either still answers (``answering``) or carries the final message the
        crashed owner never read — no second review is sent for it.
        """
        if not self.thread_id:
            return None
        driver = self._driver_factory()
        client = getattr(driver, "client", None)
        if client is None:
            return None
        try:
            starter = getattr(client, "start", None)
            if starter is not None:
                await starter()
            result = await client.request(
                "thread/turns/list",
                {"threadId": self.thread_id, "limit": 100})
            for turn in result.get("data") or []:
                items = turn.get("items") or []
                if not any(item.get("type") == "userMessage"
                           and item.get("clientId") == submission_id
                           for item in items):
                    continue
                agent = next((item for item in reversed(items)
                              if item.get("type") == "agentMessage"
                              and item.get("phase") == "final_answer"), None)
                # A final answer on an INTERRUPTED turn is a fragment, not
                # a completed review: keep waiting for the host to finish.
                status = turn.get("status")
                interrupted = status != "completed"
                if agent is not None and not interrupted:
                    return {"status": "final", "turn_id": turn.get("id"),
                            "final_message": agent.get("text") or ""}
                return {"status": "answering", "turn_id": turn.get("id")}
            return None
        finally:
            import contextlib
            closer = getattr(client, "close", None)
            if closer is not None:
                with contextlib.suppress(Exception):
                    await closer()


# ---------------------------------------------------- pause / resume / unbind


def _require_binding(store, request):
    if not isinstance(request, dict):
        raise WorkError("schema-invalid", "session request must be an object")
    binding = store.get_session_binding(request.get("binding_id"))
    if binding is None:
        raise WorkError("binding-missing",
                        f"unknown binding {request.get('binding_id')!r}")
    return binding


def _update_binding(store, binding, **changes):
    record = dict(binding)
    record.update(changes)
    record["revision"] = binding.get("revision", 1) + 1
    record["updated"] = _now()
    store.save_session_binding(record)
    return record


def record_reviewer(store, binding, *, thread_id, turn_id, kind="managed") -> dict:
    """Persist the reviewer identity on the binding (survives a restart)."""
    # re-read: the caller's snapshot predates this round's cursor advance
    binding = store.get_session_binding(binding["binding_id"]) or binding
    evidence = dict(binding.get("evidence") or {})
    evidence["reviewer"] = {"kind": kind, "thread_id": thread_id,
                            "turn_id": turn_id, "recorded_at": _now()}
    return _update_binding(store, binding, evidence=evidence)


def pause_session(store, request, actor) -> dict:
    """User pause wins over every new dispatch; idempotent."""
    actor.require("work:authorize")
    binding = _require_binding(store, request)
    if binding["state"] == "paused":
        return {"binding_id": binding["binding_id"], "state": "paused",
                "idempotent": True}
    if binding["state"] != "bound":
        raise WorkError("binding-state",
                        f"cannot pause a binding in state {binding['state']!r}")
    _update_binding(store, binding, state="paused")
    return {"binding_id": binding["binding_id"], "state": "paused"}


def resume_session(store, request, actor) -> dict:
    """Resume reactivates the SAME binding and cursor — consumed rounds are
    never replayed (round records and the binding cursor survive)."""
    actor.require("work:authorize")
    binding = _require_binding(store, request)
    if binding["state"] == "bound":
        return {"binding_id": binding["binding_id"], "state": "bound",
                "idempotent": True}
    if binding["state"] != "paused":
        raise WorkError("binding-state",
                        f"cannot resume a binding in state {binding['state']!r}")
    _update_binding(store, binding, state="bound")
    rounds = store.session_rounds(binding["binding_id"])
    return {"binding_id": binding["binding_id"], "state": "bound",
            "replayed_rounds": 0, "recorded_rounds": len(rounds)}


def unbind_session(store, request, actor) -> dict:
    """Release the binding (control included); a later control bind of the
    same session starts fresh under its own terms."""
    actor.require("work:authorize")
    binding = _require_binding(store, request)
    if binding["state"] == "unbound":
        return {"binding_id": binding["binding_id"], "state": "unbound",
                "idempotent": True}
    if binding["state"] not in ("bound", "paused"):
        raise WorkError("binding-state",
                        f"cannot unbind a binding in state {binding['state']!r}")
    record = _update_binding(store, binding, state="unbound")
    record["capabilities"] = [cap for cap in binding.get("capabilities", [])
                              if cap != "control"]
    store.save_session_binding(record)
    return {"binding_id": binding["binding_id"], "state": "unbound"}
