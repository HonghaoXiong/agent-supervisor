"""Out-of-band progress ledger + stall-aware run budgets.

The ledger is the ONLY live progress channel a running task exposes. The
controlling agent still blocks on its single dispatch command and reads only
the final result JSON, so heartbeats never enter its context; any side may
peek at the ledger file with bounded reads (:func:`read_last` /
:func:`liveness` — the ``czs progress`` subcommand prints the last line(s),
never the whole file).

Timeout semantics (2026-11-03, the "short absolute wall limit" repair):

* ``wall_timeout_s`` is the absolute fuse (the legacy ``--timeout``);
* ``stall_timeout_s`` (opt-in) turns the budget into "no progress for N
  seconds": every heartbeat moves the effective deadline to
  ``last_heartbeat + stall``, so a worker that keeps emitting events stays
  alive indefinitely and only a silent/hung one is terminated.

Whitelist discipline (same standard as session_reader): ``kind`` is a short
fixed token, ``detail`` a bounded one-line summary, extras are counters or
short identifiers. Prompts, file contents and model output NEVER enter the
ledger. Busy streams are coalesced: a repeat of the previous line's
(kind, detail) within ``COALESCE_WINDOW_S`` updates the heartbeat clock but
does not append, bounding the file to distinct activity.
"""

from __future__ import annotations

import json
import math
import time
from pathlib import Path

__all__ = [
    "COALESCE_WINDOW_S",
    "DEFAULT_STALL_TIMEOUT_S",
    "ProgressLedger",
    "resolve_run_budget",
    "read_last",
    "liveness",
    "locate_ledger",
    "safe_token",
]

COALESCE_WINDOW_S = 2.0
#: Default stall budget when a caller enables stall semantics without an
#: explicit value. Single long LLM generations can legitimately stay silent
#: for minutes, so this floor is deliberately generous.
DEFAULT_STALL_TIMEOUT_S = 600
LEDGER_SUFFIX = ".progress.jsonl"
_FINAL_KIND = "final"


def resolve_run_budget(timeout_arg, stall_arg):
    """Resolve (wall_timeout_s, stall_timeout_s) from CLI-style arguments.

    ``timeout_arg`` may be ``None`` (the "not explicitly set" sentinel):
    without stall semantics the historic default 300 applies; WITH
    ``stall_arg`` set the wall fuse defaults to the 86400 ceiling, so
    opting into stall never reintroduces the short absolute limit the
    caller was trying to escape. Explicit values always win.
    """
    stall = None if stall_arg is None else float(stall_arg)
    if timeout_arg is None:
        wall = 86400.0 if stall is not None else 300.0
    else:
        wall = float(timeout_arg)
    for label, value in (("--timeout", wall), ("--stall-timeout", stall)):
        if value is not None and (not math.isfinite(value)
                                  or not 1 <= value <= 86400):
            raise ValueError(f"{label} must be 1–86400 seconds")
    return wall, stall


# Fixed character allowlist for ledger tokens: worker-controlled strings are
# reduced to this set so no prompt/code/model text can ride into the ledger
# through an event type, tool name or phase (review P1-3).
_TOKEN_CHARS = set("abcdefghijklmnopqrstuvwxyz"
                   "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.-")


def safe_token(value, limit=24) -> str:
    """Whitelist-reduce any value to a short fixed-charset token."""
    if not isinstance(value, str):
        return "?"
    kept = "".join(ch for ch in value[:limit * 4] if ch in _TOKEN_CHARS)[:limit]
    return kept or "?"


class ProgressLedger:
    """Append-only heartbeat ledger with stall/wall expiry for one run."""

    def __init__(self, path, *, stall_timeout_s=None, wall_timeout_s=None,
                 clock=time.monotonic):
        self.path = Path(path)
        self.stall_timeout_s = (None if stall_timeout_s is None
                                else float(stall_timeout_s))
        self.wall_timeout_s = (None if wall_timeout_s is None
                               else float(wall_timeout_s))
        self._clock = clock
        self._started = self._clock()
        self._last_heartbeat = self._started
        self._lines = 0
        self._coalesced = 0
        self._last_key = None
        self._last_line_at = self._started
        self._final = None
        self._handle = self.path.open("x", encoding="utf-8")

    # -- writing ----------------------------------------------------------
    def note(self, kind, detail="", **extra):
        """Record one heartbeat; repeats inside the window only tick the clock.

        ``extra`` values are reduced to short scalars (int/float/bool kept,
        strings truncated to 64 chars, everything else dropped) — the ledger
        is evidence metadata, never a content channel.
        """
        now = self._clock()
        self._last_heartbeat = now
        if self._final is not None:
            # Terminal: late frames only tick the liveness clock, never
            # append (the ledger's last line must stay the final verdict).
            return
        key = (str(kind)[:24], str(detail)[:120])
        if (self._final is None and key == self._last_key
                and now - self._last_line_at < COALESCE_WINDOW_S):
            self._coalesced += 1
            return
        record = {"t": round(now - self._started, 3), "kind": key[0],
                  "detail": key[1]}
        for name, value in extra.items():
            if isinstance(value, bool) or isinstance(value, (int, float)):
                record[str(name)[:16]] = value
            elif isinstance(value, str):
                record[str(name)[:16]] = value[:64]
        line = json.dumps(record, ensure_ascii=False, separators=(",", ":"))
        self._handle.write(line + "\n")
        self._handle.flush()
        self._lines += 1
        self._last_key = key
        self._last_line_at = now

    def finalize(self, outcome):
        """Write the terminal line; later notes are ignored (clock still ticks)."""
        if self._final is not None:
            return
        outcome = str(outcome)[:64]
        now = self._clock()
        record = {"t": round(now - self._started, 3), "kind": _FINAL_KIND,
                  "detail": outcome}
        self._handle.write(json.dumps(record, ensure_ascii=False,
                                      separators=(",", ":")) + "\n")
        self._handle.flush()
        self._lines += 1
        self._last_key = (_FINAL_KIND, outcome)
        self._last_line_at = now
        self._last_heartbeat = now
        self._final = outcome

    def close(self):
        if not self._handle.closed:
            self._handle.close()

    # -- budgets ----------------------------------------------------------
    def expired(self, now=None):
        """Why the run must stop: 'wall', 'stall', or None while it may run."""
        now = self._clock() if now is None else now
        if (self.wall_timeout_s is not None
                and now - self._started >= self.wall_timeout_s):
            return "wall"
        if (self.stall_timeout_s is not None
                and now - self._last_heartbeat >= self.stall_timeout_s):
            return "stall"
        return None

    def effective_deadline(self, now=None):
        """Monotonic instant the run dies at if no further heartbeat arrives."""
        now = self._clock() if now is None else now
        candidates = []
        if self.wall_timeout_s is not None:
            candidates.append(self._started + self.wall_timeout_s)
        if self.stall_timeout_s is not None:
            candidates.append(self._last_heartbeat + self.stall_timeout_s)
        return min(candidates) if candidates else now + 3600.0

    def summary(self):
        now = self._clock()
        return {
            "heartbeats": self._lines,
            "coalesced": self._coalesced,
            "elapsed_s": round(now - self._started, 3),
            "last_kind": self._last_key[0] if self._last_key else None,
            "last_detail": self._last_key[1] if self._last_key else None,
            "last_heartbeat_age_s": round(now - self._last_heartbeat, 3),
            "stall_timeout_s": self.stall_timeout_s,
            "wall_timeout_s": self.wall_timeout_s,
            "final": self._final,
        }

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()
        return False


# -- standalone readers (czs progress / codex-status; no run in flight) ------

_TAIL_BYTES = 64 * 1024


def locate_ledger(target):
    """Find the ledger for a run output path or its task directory."""
    path = Path(target).expanduser()
    if path.is_dir():
        ledgers = sorted(path.glob("*" + LEDGER_SUFFIX))
        return ledgers[-1] if ledgers else None
    if path.name.endswith(LEDGER_SUFFIX):
        return path
    # run.json / run.events.jsonl / packet.json share the run stem; a
    # multi-dot name (run.events.jsonl) also tries its first segment.
    stems = [path.stem]
    if "." in path.stem:
        stems.append(path.stem.rsplit(".", 1)[0])
    for stem in stems:
        sibling = path.parent / (stem + LEDGER_SUFFIX)
        if sibling.is_file():
            return sibling
    return None


def read_last(path, count=5):
    """Parse the last ``count`` ledger lines with one bounded tail read.

    The single ``read`` is capped at ``_TAIL_BYTES`` so an actively growing
    ledger cannot blow the promised bound (review P2-3); stat/open races
    surface as an empty result, never an exception.
    """
    path = Path(path)
    try:
        size = path.stat().st_size
        with path.open("rb") as handle:
            if size > _TAIL_BYTES:
                handle.seek(size - _TAIL_BYTES)
                handle.readline()  # drop the possibly-cut first line
            raw = handle.read(_TAIL_BYTES).decode("utf-8", errors="replace")
    except OSError:
        return []
    records = []
    for line in raw.splitlines():
        try:
            value = json.loads(line)
        except ValueError:
            continue
        if isinstance(value, dict):
            records.append(value)
    return records[-max(1, count):]


def liveness(target, *, now=None, finished_fallback=None):
    """One-glance verdict for a task dir / run output / ledger path.

    ``finished`` needs either the terminal ``final`` ledger line or a sibling
    result JSON whose outcome field is present. Age is measured from the last
    heartbeat to the file's mtime (readers have no clock shared with the
    writer, so mtime is the honest observable). A ledger that disappears or
    turns unreadable mid-read reports ``unknown``, never raises.
    """
    path = locate_ledger(target)
    if path is None or not path.is_file():
        return {"state": "unknown", "reason": "no progress ledger found"}
    try:
        records = read_last(path, 1)
        mtime = path.stat().st_mtime
    except OSError as error:
        return {"state": "unknown", "reason": f"ledger unreadable: {error}"}
    reference = time.time() if now is None else now
    age_s = max(0.0, round(reference - mtime, 3))
    last = records[0] if records else None
    if last and last.get("kind") == _FINAL_KIND:
        state = "finished"
    else:
        result = finished_fallback if finished_fallback is not None \
            else _sibling_outcome(path)
        state = "finished" if result is not None else "running"
    verdict = {"state": state, "ledger": str(path), "age_s": age_s,
               "last": last}
    if state == "finished" and not (last and last.get("kind") == _FINAL_KIND):
        verdict["outcome"] = _sibling_outcome(path)
    elif state == "finished":
        verdict["outcome"] = last.get("detail")
    return verdict


def _sibling_outcome(ledger_path):
    """Terminal outcome from the run/result JSON next to the ledger, if any."""
    stem = ledger_path.name[:-len(LEDGER_SUFFIX)]
    candidate = ledger_path.parent / (stem + ".json")
    if not candidate.is_file():
        return None
    try:
        # Bounded read: a huge final message must not blow the peek budget.
        raw = candidate.open("rb").read(256 * 1024)
        value = json.loads(raw.decode("utf-8", errors="replace"))
    except (OSError, ValueError):
        return None
    if isinstance(value, dict) and isinstance(value.get("outcome"), str):
        return value["outcome"]
    if isinstance(value, dict) and value.get("delivery_ok") is True:
        return "delivered"
    return None
