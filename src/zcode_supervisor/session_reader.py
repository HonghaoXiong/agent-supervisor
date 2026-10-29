"""Conservative cross-vendor session summaries for the decision layer.

Cross-vendor data flow is default-conservative: only structured summaries
leave the evidence stores. Field whitelists apply everywhere, text is
truncated, listings are paginated, and raw session payloads never leave this
module — a message row's ``data`` JSON contributes at most its ``role``/``type``
keys, and its text comes from the associated ``part`` rows: only parts whose
``data.type`` is a text type contribute their ``text`` string (concatenated
in part order, truncated); every part's type is counted. Every other key of
either payload (tokens, provider payloads, secrets such as API keys) is
dropped before output.

ZCode store shape (2026-10-25 本机 0.17.0 实测, ``~/.zcode/cli/db/db.sqlite``
opened with ``mode=ro``):

* ``message`` rows carry only metadata in ``data`` — a JSON object with
  ``role`` (always present), ``time``, ``parentID``, ``modelId``,
  ``providerId`` etc., but **no** ``type``/``text``/``content`` keys, which
  is why text must come from parts;
* ``part`` rows (``id`` PK, ``message_id``, ``session_id``, ``sequence``,
  ``data`` JSON) are ordered by ``sequence`` per message; observed
  ``data.type`` values: ``tool``, ``step-start``, ``step-finish``,
  ``reasoning``, ``text``, ``timeline``, ``compaction``, ``file`` — only
  ``text`` parts carry the visible message body (``{"type": "text",
  "text": ...}``; ``reasoning`` parts carry a private ``text`` of their own
  that stays behind the whitelist and is only counted).
"""

from __future__ import annotations

import json
import sqlite3
from collections import Counter
from pathlib import Path
from urllib.parse import quote

from .codex_driver import locate_rollout, summarize_rollout

__all__ = [
    "TEXT_LIMIT",
    "TEXT_PART_TYPES",
    "read_codex_context",
    "read_zcode_context",
    "read_context",
]

TEXT_LIMIT = 200
DEFAULT_MAX_ITEMS = 30
# part.data.type values whose ``text`` string becomes the message body.
TEXT_PART_TYPES = ("text",)


def _bounded_max_items(max_items) -> int:
    if isinstance(max_items, bool) or not isinstance(max_items, int) or max_items < 1:
        return DEFAULT_MAX_ITEMS
    return min(max_items, 50)


def read_codex_context(thread_id, codex_home=None, max_items=30) -> dict:
    """Whitelist summary of a persisted codex rollout.

    Wraps :func:`codex_driver.locate_rollout` plus
    :func:`codex_driver.summarize_rollout` (itself whitelist-only). An
    unlocatable thread is passed through verbatim; a located thread yields the
    rollout locators plus the bounded summary.
    """
    limit = _bounded_max_items(max_items)
    located = locate_rollout(thread_id, codex_home)
    if located.get("status") != "located":
        return located
    summary = summarize_rollout(Path(located["path"]), max_items=limit)
    return {
        "status": "located",
        "thread_id": thread_id,
        "path": located.get("path"),
        "cwd": located.get("cwd"),
        "cli_version": located.get("cli_version"),
        "summary": summary,
    }


def _default_zcode_db() -> Path:
    return Path.home() / ".zcode" / "cli" / "db" / "db.sqlite"


def _zcode_session_row(connection, session_id):
    return connection.execute(
        "SELECT id, directory, title, time_created, time_updated "
        "FROM session WHERE id = ?", (session_id,)
    ).fetchone()


def _scalar(connection, sql: str, session_id: str) -> int:
    row = connection.execute(sql, (session_id,)).fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def _message_entry(row, parts) -> dict:
    """Whitelist one message row plus its part rows.

    Out of the message row only ``sequence``/``time_created`` and the
    ``role``/``type`` keys of the ``data`` JSON are kept. Text comes from
    the associated parts: parts whose ``data.type`` is in TEXT_PART_TYPES
    contribute their ``text`` string, concatenated in part order and
    truncated to TEXT_LIMIT; every part's type is counted under
    ``part_types``. No other key of either payload ever leaves this
    function (no callIDs, tool inputs/outputs, credentials).
    """
    entry = {
        "sequence": row["sequence"],
        "time_created": row["time_created"],
        "role": None,
        "type": None,
        "text": None,
        "text_truncated": False,
        "part_types": {},
    }
    try:
        data = json.loads(row["data"]) if row["data"] is not None else None
    except (json.JSONDecodeError, TypeError):
        data = None
    if isinstance(data, dict):
        for key in ("role", "type"):
            value = data.get(key)
            if isinstance(value, str):
                entry[key] = value
    # P2.4: the concatenated text is accumulated with a HARD ceiling of
    # TEXT_LIMIT+1 characters (anything beyond only sets the seen-extra
    # flag) — the output is byte-identical to the former join-then-
    # truncate, but a session with huge parts no longer buffers them.
    buffer: list[str] = []
    buffered = 0
    seen_extra = False
    had_text = False
    counts: Counter = Counter()
    for part in parts:
        try:
            part_data = json.loads(part["data"]) if part["data"] is not None else None
        except (json.JSONDecodeError, TypeError):
            part_data = None
        part_type = part_data.get("type") if isinstance(part_data, dict) else None
        counts[str(part_type) if part_type is not None else "<no-type>"] += 1
        if (isinstance(part_data, dict) and isinstance(part_type, str)
                and part_type in TEXT_PART_TYPES
                and isinstance(part_data.get("text"), str)):
            had_text = True
            if seen_extra:
                continue
            text = part_data["text"]
            room = (TEXT_LIMIT + 1) - buffered
            if len(text) > room:
                buffer.append(text[:max(room, 0)])
                buffered += max(room, 0)
                seen_extra = True
            else:
                buffer.append(text)
                buffered += len(text)
    entry["part_types"] = dict(counts)
    if had_text:  # matches the old `if texts:` even for empty strings
        full_text = "".join(buffer)
        entry["text"] = full_text[:TEXT_LIMIT]
        entry["text_truncated"] = len(full_text) > TEXT_LIMIT or seen_extra
    return entry


_TOOL_USAGE_FIELDS = ("tool_name", "status", "approval_status", "read_only", "exit_code")


def _part_rows(connection, session_id: str, message_ids: list) -> list:
    """Fetch the part rows for one page of message ids, in part order.

    Older stores without a ``part`` table yield no rows (their messages then
    read as metadata-only, which is honest for the real 0.17.0 shape anyway:
    message.data never carries the body text).
    """
    if not message_ids:
        return []
    has_part_table = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'part'"
    ).fetchone()
    if has_part_table is None:
        return []
    placeholders = ",".join("?" for _ in message_ids)
    return connection.execute(
        f"SELECT message_id, sequence, data FROM part "  # fixed table name; ids stay parameterized
        f"WHERE session_id = ? AND message_id IN ({placeholders}) "
        "ORDER BY sequence",
        (session_id, *message_ids),
    ).fetchall()


def read_zcode_context(session_id, db_path=None, max_items=30, offset=0) -> dict:
    """Whitelist summary of one ZCode session from the CLI sqlite store.

    The database is opened read-only (``mode=ro``); both the message list and
    the tool-usage list are windowed with the same ``offset``/``max_items``
    page against their own totals. A missing database or session id yields an
    ``unlocatable`` result, never an exception.
    """
    limit = _bounded_max_items(max_items)
    if isinstance(offset, bool) or not isinstance(offset, int) or offset < 0:
        offset = 0
    if not isinstance(session_id, str) or not session_id.strip():
        return {"status": "unlocatable", "reason": "no session id"}

    path = Path(db_path).expanduser() if db_path is not None else _default_zcode_db()
    if not path.is_file():
        return {"status": "unlocatable",
                "reason": f"zcode session database not found: {path}"}
    try:
        # Read-only URI open: the hub never writes to the ZCode store.
        connection = sqlite3.connect(f"file:{quote(str(path))}?mode=ro", uri=True)
    except sqlite3.Error as error:
        return {"status": "unlocatable",
                "reason": f"cannot open zcode session database read-only: {error}"}
    try:
        connection.row_factory = sqlite3.Row
        row = _zcode_session_row(connection, session_id)
        if row is None:
            return {"status": "unlocatable",
                    "reason": f"no session row for id {session_id!r} in {path}"}

        message_total = _scalar(
            connection, "SELECT COUNT(*) FROM message WHERE session_id = ?", session_id)
        tool_total = _scalar(
            connection, "SELECT COUNT(*) FROM tool_usage WHERE session_id = ?", session_id)

        message_rows = connection.execute(
            "SELECT id, sequence, time_created, data FROM message "
            "WHERE session_id = ? ORDER BY sequence LIMIT ? OFFSET ?",
            (session_id, limit, offset),
        ).fetchall()
        tool_rows = connection.execute(
            "SELECT tool_name, status, approval_status, read_only, exit_code "
            "FROM tool_usage WHERE session_id = ? ORDER BY rowid LIMIT ? OFFSET ?",
            (session_id, limit, offset),
        ).fetchall()
        part_rows = _part_rows(connection, session_id,
                               [row["id"] for row in message_rows])
    except sqlite3.Error as error:
        return {"status": "unlocatable",
                "reason": f"zcode session database query failed: {error}"}
    finally:
        connection.close()

    parts_by_message: dict[str, list] = {}
    for part in part_rows:
        parts_by_message.setdefault(part["message_id"], []).append(part)
    messages = [_message_entry(item, parts_by_message.get(item["id"], []))
                for item in message_rows]
    tool_usage = [
        {field: item[field] for field in _TOOL_USAGE_FIELDS} for item in tool_rows
    ]
    has_more = (offset + limit < message_total) or (offset + limit < tool_total)
    return {
        "status": "located",
        "session": {
            "id": row["id"],
            "directory": row["directory"],
            "title": row["title"],
            "time_created": row["time_created"],
            "time_updated": row["time_updated"],
        },
        "messages": messages,
        "tool_usage": tool_usage,
        "message_total": message_total,
        "tool_usage_total": tool_total,
        "offset": offset,
        "max_items": limit,
        "next_offset": offset + limit if has_more else None,
        "truncated": has_more,
    }


def read_context(locator: dict, max_items=30, offset=0) -> dict:
    """Dispatch on a session locator: {"agent": "codex", "thread_id": ...}
    or {"agent": "zcode", "session_id": ...} (optionally carrying
    ``codex_home`` / ``db_path`` overrides)."""
    if not isinstance(locator, dict):
        raise ValueError("locator must be an object with agent and thread_id/session_id")
    agent = locator.get("agent")
    if agent == "codex":
        thread_id = locator.get("thread_id")
        return read_codex_context(
            thread_id,
            codex_home=locator.get("codex_home"),
            max_items=max_items,
        )
    if agent == "zcode":
        session_id = locator.get("session_id")
        return read_zcode_context(
            session_id,
            db_path=locator.get("db_path"),
            max_items=max_items,
            offset=offset,
        )
    raise ValueError(
        "locator.agent must be 'codex' (thread_id) or 'zcode' (session_id)")
