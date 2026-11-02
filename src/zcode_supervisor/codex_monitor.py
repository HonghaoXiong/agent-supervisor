"""One-glance, read-only view of live agent activity on this machine.

``czs codex-status`` answers "what is Codex (and its dsh/zcode helpers)
doing right now" WITHOUT opening any agent's context: every source is a
file the agents already persist for themselves —

* Codex rollout JSONL under ``~/.codex/sessions`` (format live-verified
  2026-11-03, cli 0.166: user/assistant text lives in
  ``response_item/message``, subagent traffic in ``response_item/agent_message``;
  ``event_msg`` carries no user text);
* dsh per-task workspaces under ``~/.dsh/sessions`` (directory name embeds
  the task label and thinking tier: ``...-dsh-<label>-max-<N>-workspace--``);
* in-flight czs runs, detected from their ``.progress.jsonl`` heartbeat
  ledgers and terminal result JSONs.

Whitelist discipline matches session_reader: only bounded text snippets of
message/agent records are exposed (``TEXT_LIMIT`` chars), everything else
contributes counts. No file is ever written.
"""

from __future__ import annotations

import json
import re
import time
from collections import Counter
from pathlib import Path

from .progress import liveness

__all__ = [
    "TEXT_LIMIT",
    "dsh_activity",
    "fresh_rollouts",
    "render_text",
    "status",
    "tail_summary",
]

TEXT_LIMIT = 200
_TAIL_BYTES = 192 * 1024
_DSH_LABEL = re.compile(r"-dsh-(?P<label>.+)-max-(?P<tier>\d+)-workspace--$")
_MESSAGE_TYPES = {"message", "agent_message"}
# Harness frames inside user-role records (multi-agent wire copies such as
# "[143] tool send_message call: {…}") — internal plumbing, not chat text.
_HARNESS_FRAME = re.compile(r"^\[\d+\] ")


def _clip_text(value) -> str | None:
    """Clip a display string; non-strings (agent-controlled shapes) drop."""
    if not isinstance(value, str):
        return None
    return " ".join(value.split())[:TEXT_LIMIT]


_DISPLAY_CHARS = set("abcdefghijklmnopqrstuvwxyz"
                     "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_./-")


def _display_token(value, limit=64) -> str:
    """Bounded identifier for DISPLAY columns (adds '/' to the token set).

    Distinct from progress.safe_token on purpose: the ledger's fixed-charset
    whitelist is evidence discipline, while these one-line view fields only
    need agent-shaped garbage reduced and length bounded.
    """
    if not isinstance(value, str):
        return "?"
    kept = "".join(ch for ch in value[:limit * 4] if ch in _DISPLAY_CHARS)[:limit]
    return kept or "?"


def _default_codex_home(explicit=None) -> Path:
    return Path(explicit).expanduser() if explicit is not None \
        else Path.home() / ".codex"


def _default_dsh_home(explicit=None) -> Path:
    return Path(explicit).expanduser() if explicit is not None \
        else Path.home() / ".dsh"


def fresh_rollouts(codex_home=None, fresh_s=600, limit=5):
    """Codex rollout files whose mtime falls inside the activity window."""
    sessions = _default_codex_home(explicit=codex_home) / "sessions"
    if not sessions.is_dir():
        return []
    cutoff = time.time() - max(1, fresh_s)
    found = []
    for path in sessions.rglob("*.jsonl"):
        try:
            mtime = path.stat().st_mtime
        except OSError:
            continue
        if mtime >= cutoff:
            found.append((mtime, path))
    found.sort(reverse=True)
    return [{"mtime": mtime, "path": path} for mtime, path in found[:limit]]


def _first_line(path: Path):
    try:
        with path.open("rb") as handle:
            raw = handle.readline(64 * 1024)
    except OSError:
        return None
    try:
        return json.loads(raw.decode("utf-8", errors="replace"))
    except ValueError:
        return None


def _clip(value) -> str:
    return " ".join(str(value).split())[:TEXT_LIMIT]


def _ts(record) -> str:
    """Bounded hh:mm:ss slice of a record timestamp; never trusts the type."""
    value = record.get("timestamp")
    if not isinstance(value, str):
        return ""
    return value[11:19] if len(value) >= 19 else value


def _content_text(payload) -> str:
    """Concatenate ONLY string text parts; agent-shaped garbage drops."""
    parts = payload.get("content")
    if not isinstance(parts, list):
        return ""
    return "\n".join(part.get("text") for part in parts
                     if isinstance(part, dict)
                     and isinstance(part.get("text"), str)
                     and part.get("text"))


def tail_summary(path, max_items=6):
    """Bounded tail digest of one rollout: meta + last message-ish records.

    Only ``response_item`` records of type ``message``/``agent_message``
    contribute text (clipped); every record type is counted. Raw payloads
    (tokens, provider blobs, secrets) never leave this function. Malformed
    shapes degrade to counts/omissions — one bad rollout never raises.
    """
    path = Path(path)
    meta_record = _first_line(path)
    meta = {}
    if isinstance(meta_record, dict) and meta_record.get("type") == "session_meta":
        payload = meta_record.get("payload")
        payload = payload if isinstance(payload, dict) else {}
        git = payload.get("git")
        git = git if isinstance(git, dict) else {}
        meta = {"cwd": _clip_text(payload.get("cwd")),
                "branch": _clip_text(git.get("branch")),
                "cli_version": _clip_text(payload.get("cli_version"))}
    try:
        size = path.stat().st_size
        with path.open("rb") as handle:
            if size > _TAIL_BYTES:
                handle.seek(max(0, size - _TAIL_BYTES))
                handle.readline()  # drop the possibly-cut first line
            raw = handle.read(_TAIL_BYTES).decode("utf-8", errors="replace")
    except OSError as error:
        return {"meta": meta, "error": f"unreadable tail: {error}",
                "tail_type_counts": {}, "recent": []}
    counts: Counter = Counter()
    items = []
    last_ts = None
    for line in raw.splitlines():
        try:
            record = json.loads(line)
        except ValueError:
            counts["<invalid-json>"] += 1
            continue
        if not isinstance(record, dict):
            continue
        rtype = record.get("type")
        payload = record.get("payload")
        payload = payload if isinstance(payload, dict) else {}
        ptype = payload.get("type")
        key = f"{_display_token(rtype, 24)}/{_display_token(ptype, 24)}"
        counts[key] += 1
        timestamp = record.get("timestamp")
        if isinstance(timestamp, str):
            last_ts = timestamp
        if rtype != "response_item" or ptype not in _MESSAGE_TYPES:
            continue
        if ptype == "message":
            role = payload.get("role")
            if role not in ("user", "assistant"):
                continue
            text = _content_text(payload)
            # Skip harness-injected frames (instructions/annotations and
            # multi-agent wire copies). Real user turns here are plain text.
            visible = text.lstrip()
            if role == "user" and (visible.startswith("#") or visible.startswith("<")
                                   or _HARNESS_FRAME.match(visible)):
                continue
            items.append({"ts": _ts(record), "kind": role, "text": _clip(text)})
        else:
            text = _content_text(payload)
            kind = (f"agent:{_display_token(payload.get('author') or '?', 48)}"
                    f"->{_display_token(payload.get('recipient') or '?', 48)}")
            items.append({"ts": _ts(record), "kind": kind, "text": _clip(text)})
    return {"meta": meta, "last_timestamp": last_ts,
            "tail_type_counts": dict(counts.most_common(8)),
            "recent": items[-max(1, max_items):]}


def dsh_activity(dsh_home=None, fresh_s=600, limit=5):
    """dsh per-task workspaces touched inside the window (name-derived labels)."""
    sessions = _default_dsh_home(explicit=dsh_home) / "sessions"
    if not sessions.is_dir():
        return []
    cutoff = time.time() - max(1, fresh_s)
    found = []
    for entry in sessions.iterdir():
        if not entry.is_dir():
            continue
        try:
            mtime = entry.stat().st_mtime
        except OSError:
            continue
        if mtime < cutoff:
            continue
        match = _DSH_LABEL.search(entry.name)
        label = match.group("label") if match else entry.name[-60:]
        tier = match.group("tier") if match else None
        found.append({"mtime": mtime, "label": label, "tier": tier,
                      "dir": entry.name[-100:]})
    found.sort(key=lambda item: item["mtime"], reverse=True)
    return found[:limit]


def _czs_runs(config, fresh_s=600):
    """In-flight and recently finished czs runs under the configured work root."""
    if config is None:
        return {"available": False,
                "reason": "no readable .czs config; pass --config to include runs"}
    root = Path(config.work_root)
    if not root.is_dir():
        return {"available": False, "reason": "work_root missing"}
    cutoff = time.time() - max(1, fresh_s)
    entries = []
    for ledger in root.rglob("*.progress.jsonl"):
        try:
            mtime = ledger.stat().st_mtime
        except OSError:
            continue
        if mtime < cutoff:
            continue
        verdict = liveness(ledger)
        entries.append({"run": str(ledger.parent.name), **verdict})
    entries.sort(key=lambda item: item.get("age_s", 0))
    return {"available": True, "runs": entries[:10]}


def status(config=None, *, fresh_s=600, max_sessions=5, codex_home=None,
           dsh_home=None):
    """Compose the full view; pure read, safe to call from any side."""
    now = time.time()
    rollouts = []
    for entry in fresh_rollouts(codex_home, fresh_s=fresh_s, limit=max_sessions):
        record = {
            "file": entry["path"].name,
            "age_s": round(now - entry["mtime"], 1),
        }
        try:
            record.update(tail_summary(entry["path"]))
        except Exception as error:  # noqa: BLE001 - one bad file never kills the view
            record["error"] = f"summarize failed: {error}"[:200]
        rollouts.append(record)
    return {
        "generated": time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(now)),
        "fresh_window_s": fresh_s,
        "codex_sessions": rollouts,
        "dsh_workspaces": dsh_activity(dsh_home, fresh_s=fresh_s),
        "czs_runs": _czs_runs(config, fresh_s=fresh_s),
    }


def render_text(document: dict) -> str:
    """Compact human/ZCode-readable rendering of :func:`status`."""
    lines = [f"codex-status @ {document.get('generated')} "
             f"(window {document.get('fresh_window_s')}s)"]
    sessions = document.get("codex_sessions") or []
    if not sessions:
        lines.append("codex: no active sessions in window")
    for session in sessions:
        meta = session.get("meta") or {}
        lines.append(f"codex: {session.get('file')} · age {session.get('age_s')}s "
                     f"· {meta.get('branch') or '?'} · {meta.get('cwd') or '?'}")
        for item in session.get("recent") or []:
            lines.append(f"  [{item.get('ts')}] {item.get('kind')}: {item.get('text')}")
    workspaces = document.get("dsh_workspaces") or []
    if workspaces:
        for workspace in workspaces:
            lines.append(f"dsh: {workspace.get('label')} · max-{workspace.get('tier')} "
                         f"· age {round(time.time() - workspace.get('mtime', 0))}s")
    else:
        lines.append("dsh: no active workspaces in window")
    runs = document.get("czs_runs") or {}
    if runs.get("available"):
        for run in runs.get("runs") or []:
            lines.append(f"czs: {run.get('run')} · {run.get('state')}"
                         + (f" · {run.get('outcome')}" if run.get("outcome") else "")
                         + f" · last heartbeat {run.get('age_s')}s ago")
    else:
        lines.append(f"czs: {runs.get('reason')}")
    return "\n".join(lines)
