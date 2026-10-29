"""Live-UI discovery registry: ``~/.czs/ui-registry.json`` (one-click app).

A tiny per-user registry of RUNNING UI event servers so the desktop app
can find a hub without ``CZS_UI_SOCKET`` or a matching CWD. One JSON
object keyed by ``str(pid)``; each value::

    {"socket": "<absolute path>", "pid": <int>,
     "project_root": "<cwd at bind>", "started_at": <epoch s>,
     "heartbeat_at": <epoch s>}

Writers are the UI event servers themselves (``register`` after a
successful bind, a 30 s heartbeat, ``unregister`` on close — all
best-effort: a registry failure NEVER affects the UI service itself).
Readers are the desktop app's discovery scan and tests.

Consistency model (deliberately simple, races tolerated):

* every write is a short read-modify-write window closed by an atomic
  ``os.replace`` — no locking anywhere;
* a write that loses a concurrent registration is HEALED by that
  server's next heartbeat rewrite (``refresh`` re-asserts the entry);
* readers may drop provably-dead entries (dead pid per ``os.kill(pid, 0)``
  — ``PermissionError`` means another user's LIVE process — or a missing
  socket file); the cleanup rewrite uses the same tolerated window.

The registry is a convenience, not a security boundary: the socket
itself stays owner-only (0600 + parent 0700 + ``getpeereid`` verified by
``ui_server``), and clients still run their own liveness probe before
trusting an entry. The registry directory is created 0700 and the file
is written 0600 so the discovery surface does not leak socket paths to
other users. ``CZS_UI_REGISTRY`` overrides the file location (tests).
"""

from __future__ import annotations

import contextlib
import json
import os
import time
from pathlib import Path

__all__ = ["ENV_REGISTRY", "HEARTBEAT_S", "REGISTRY_NAME",
           "pid_alive", "read_registry", "refresh", "register",
           "registry_path", "unregister"]

#: Test/isolation hook: when set, the registry lives at this path instead.
ENV_REGISTRY = "CZS_UI_REGISTRY"
REGISTRY_NAME = "ui-registry.json"
#: Heartbeat cadence for ``refresh`` (self-heal window for lost updates).
HEARTBEAT_S = 30.0
_ENTRY_KEYS = ("socket", "pid", "project_root", "started_at", "heartbeat_at")


def registry_path() -> Path:
    """The registry file location (``CZS_UI_REGISTRY`` override wins)."""
    override = os.environ.get(ENV_REGISTRY, "").strip()
    if override:
        return Path(override)
    return Path.home() / ".czs" / REGISTRY_NAME


def pid_alive(pid: int) -> bool:
    """True when ``pid`` names a live process (zombies count as alive).

    ``PermissionError`` means the pid EXISTS but belongs to another user
    — reported alive, exactly like a same-user live pid; readers relying
    on socket existence/connect probes make the final call anyway.

    ``OverflowError`` (pid past the C ``pid_t`` range — macOS raises it
    for ``pid >= 2**31``, and it is NOT an ``OSError`` subclass) and
    ``ValueError`` read as DEAD: a hand-corrupted registry entry must
    never crash a reader (polish1 N1).
    """
    if not isinstance(pid, int) or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except PermissionError:
        return True
    except (OSError, OverflowError, ValueError):
        return False
    return True


# -- file plumbing -----------------------------------------------------------
def _ensure_private_dir(directory: Path) -> None:
    """Create the registry dir (0700, umask-proof); best-effort tighten."""
    directory.mkdir(parents=True, exist_ok=True)
    with contextlib.suppress(OSError):
        info = directory.stat()
        if info.st_uid == os.geteuid():
            os.chmod(directory, 0o700)


def _load(path: Path) -> dict:
    """The raw entries dict; any missing/corrupt file reads as empty."""
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    if not isinstance(raw, dict):
        return {}
    return {key: value for key, value in raw.items()
            if isinstance(key, str) and isinstance(value, dict)}


def _atomic_write(path: Path, entries: dict) -> None:
    """Write 0600 via tmp+``os.replace``; leftover tmp files are removed."""
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as handle:
            json.dump(entries, handle, ensure_ascii=False, sort_keys=True)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, path)
    except OSError:
        with contextlib.suppress(OSError):
            os.unlink(tmp)
        raise


def _valid_entry(value) -> bool:
    return (isinstance(value, dict)
            and isinstance(value.get("socket"), str)
            and bool(value["socket"])
            and isinstance(value.get("pid"), int)
            and isinstance(value.get("project_root"), str)
            and isinstance(value.get("started_at"), (int, float))
            and not isinstance(value.get("started_at"), bool))


# -- writer API (UI event servers) --------------------------------------------
def register(socket_path, *, pid=None, project_root=None, path=None) -> dict:
    """Write/refresh THIS process's entry; returns the stored entry.

    Keyed by pid, so a same-pid rebind replaces (refreshes) the old
    entry instead of accumulating one per bind.
    """
    pid = os.getpid() if pid is None else int(pid)
    project_root = os.getcwd() if project_root is None else str(project_root)
    now = time.time()
    entry = {"socket": os.path.abspath(str(socket_path)),
             "pid": pid,
             "project_root": project_root,
             "started_at": now,
             "heartbeat_at": now}
    file = Path(path) if path is not None else registry_path()
    _ensure_private_dir(file.parent)
    entries = _load(file)
    entries[str(pid)] = entry
    _atomic_write(file, entries)
    return entry


def refresh(entry: dict, *, path=None) -> dict:
    """Re-assert one entry with a fresh ``heartbeat_at``.

    ``started_at`` is preserved verbatim so ordering stays stable. This
    is the self-heal write: an entry lost to a concurrent registry write
    reappears here on the next heartbeat.
    """
    if not _valid_entry(entry):
        raise ValueError("registry entry is malformed")
    updated = dict(entry)
    updated["heartbeat_at"] = time.time()
    file = Path(path) if path is not None else registry_path()
    _ensure_private_dir(file.parent)
    entries = _load(file)
    entries[str(entry["pid"])] = updated
    _atomic_write(file, entries)
    return updated


def unregister(pid=None, *, path=None) -> None:
    """Remove this process's entry (best-effort, idempotent)."""
    pid = os.getpid() if pid is None else int(pid)
    file = Path(path) if path is not None else registry_path()
    entries = _load(file)
    if entries.pop(str(pid), None) is not None:
        _atomic_write(file, entries)


# -- reader API (app discovery / tests) ----------------------------------------
def read_registry(*, path=None, cleanup=True) -> list[dict]:
    """Live entries, newest (largest ``started_at``) first.

    Drops malformed entries, dead pids and entries whose socket file no
    longer exists. With ``cleanup`` the dropped entries are also removed
    from the file (atomic rewrite inside the tolerated short window — a
    live server's heartbeat heals any registration lost to the race).
    """
    file = Path(path) if path is not None else registry_path()
    entries = _load(file)
    kept: list[dict] = []
    stale_keys: list[str] = []
    for key, value in entries.items():
        if not _valid_entry(value):
            stale_keys.append(key)
            continue
        if not pid_alive(value["pid"]) or not os.path.exists(value["socket"]):
            stale_keys.append(key)
            continue
        kept.append(value)
    kept.sort(key=lambda entry: entry["started_at"], reverse=True)
    if cleanup and stale_keys:
        with contextlib.suppress(OSError):
            _atomic_write(file, {str(entry["pid"]): entry for entry in kept})
    return kept
