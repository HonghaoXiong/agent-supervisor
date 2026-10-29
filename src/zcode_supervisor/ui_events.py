"""Versioned v1 UI event envelope, allowlists and sequence/time discipline.

PLAN §4.2/§4.3 (P0.4): the in-process schema layer for the read-only UI event
projection. This module owns ONLY the data contract — the versioned envelope,
the per-kind data allowlists, per-stream sequence allocation, supervisor UTC
RFC3339 observation time, and the value gates that keep untrusted strings out
of events. It deliberately contains no journal, no socket, no transport and no
policy: wiring authoritative state to events is :mod:`ui_projection`'s job
(P1.1 owns the journal/UDS layers).

Security posture: minimal disclosure by construction. Every field an event may
carry is enumerated here (envelope keys, per-kind data keys, per-key value
shapes); anything not enumerated fails validation with :class:`UiEventError`.
There is no ``result_preview`` in v1 and no code path that could add one
without extending the allowlists in this module.
"""

from __future__ import annotations

import json
import math
import re
from datetime import UTC, datetime, timedelta

__all__ = [
    "ENVELOPE_FIELDS",
    "EVENT_KINDS",
    "HUB_STREAM_ID",
    "KNOWN_AGENTS",
    "TRUSTED_ERROR_CODES",
    "UiEventError",
    "UiEventStream",
    "find_fragments",
    "normalize_rfc3339",
    "safe_error_code",
    "safe_hex64",
    "safe_opaque",
    "safe_token",
    "utc_now",
    "validate_event",
]

#: The fixed v1 envelope, in canonical order (PLAN §4.2). Exactly these keys —
#: a missing field is ``null``, never fabricated; an extra key is a contract
#: violation and fails :func:`validate_event`.
ENVELOPE_FIELDS = (
    "v", "event_id", "stream_id", "seq", "kind", "observed_at", "task_id",
    "worker", "mode", "session_id", "turn_id", "tool_call_id", "request_id",
    "source_event_seq", "status", "outcome", "delivery_ok", "duration_ms",
    "error", "data",
)

#: Business event kinds of the §4.3 catalogue. Protocol control kinds
#: (hello/snapshot/subscribe/gap/reset_required/heartbeat) are journal-layer
#: concerns (P1.1) and intentionally absent from the business validator.
EVENT_KINDS = frozenset({
    "task.accepted", "stream.announced",
    "session.bound", "session.state",
    "turn.started", "turn.completed",
    "tool.scheduled", "tool.completed",
    "approval.requested", "approval.decided",
    "task.completed", "process.exited",
})

#: The fixed known-agent enumeration for initiator verification (PLAN §4.2:
#: a session id may only be echoed when the agent belongs to this set AND a
#: trusted registry confirms it; ui_projection enforces the registry half).
KNOWN_AGENTS = ("codex", "zcode")

#: Stream id of hub-level events (stream announcements, process exits).
HUB_STREAM_ID = "hub"

#: Fixed reason codes for approval.decided (PLAN §4.3): the applied decision
#: plus WHERE it came from — never free text.
APPROVAL_REASON_CODES = ("allowed_by_contract", "denied_by_contract",
                         "denied_early", "denied_persistent_variant")

#: The ONLY strings that may appear in ``error`` (round-2 review Critical):
#: a fixed, reviewed enumeration of the exception type names this codebase
#: actually raises. Native error messages, ``str(error)``, base64 or any
#: other free text NEVER qualify — an unclassifiable error is emitted as
#: ``error=null`` plus ``data.reason_code="error_unclassified"``.
TRUSTED_ERROR_CODES = frozenset({
    "AppServerError", "AttributeError", "BudgetExhausted", "CancelledError",
    "CodexExecError", "ContractError", "EOFError", "FileNotFoundError",
    "HubError", "IndexError", "IsADirectoryError", "JSONDecodeError",
    "KeyError", "NotADirectoryError", "OSError", "PermissionError",
    "ResidentError", "RuntimeError", "TimeoutError", "TypeError",
    "UnicodeDecodeError", "ValueError", "WorkerFailed",
})

# Value gates. Opaque ids must be single-line, printable, length-bounded and
# free of separators that could smuggle paths or structured text.
_OPAQUE_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")
_STREAM_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$")
_INSTANCE_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$")
_TOKEN_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")
_HEX64_RE = re.compile(r"^[0-9a-f]{64}$")
_RFC3339_UTC_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z$")
_EVENT_ID_RE = re.compile(
    r"^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}/[0-9]+/"
    r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}/[0-9]+$")


class UiEventError(ValueError):
    """A v1 envelope contract violation (programmer error at an emit site)."""


def utc_now() -> str:
    """Supervisor observation time: UTC RFC3339, microseconds, ``Z`` suffix.

    Display-only by contract (PLAN §4.2); durations come from
    ``time.monotonic()`` at the projection layer, never from this clock.
    """
    return datetime.now(UTC).isoformat(timespec="microseconds").replace("+00:00", "Z")


def normalize_rfc3339(value) -> str | None:
    """Re-shape any tz-aware RFC3339 timestamp to the Z-microsecond form."""
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value)  # 3.11+ parses the Z suffix
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    return parsed.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def safe_opaque(value) -> str | None:
    """Pass through a well-shaped opaque id; anything else becomes ``None``.

    Used on every id copied from an untrusted surface (wire request ids, task
    record fields). Failing values are OMITTED (null), never truncated into a
    partial leak: a path, a newline, a 300-char blob all fail whole.
    """
    if isinstance(value, str) and _OPAQUE_RE.fullmatch(value):
        return value
    return None


def safe_token(value) -> str | None:
    """Same gate for status/outcome-style tokens."""
    if isinstance(value, str) and _TOKEN_RE.fullmatch(value):
        return value
    return None


def safe_hex64(value) -> str | None:
    """Pass through a lowercase 64-hex digest; anything else becomes None."""
    if isinstance(value, str) and _HEX64_RE.fullmatch(value):
        return value
    return None


def safe_error_code(code) -> str | None:
    """Pass through a member of the fixed trusted enumeration, else ``None``.

    Round-2 review Critical: a shape-based gate cannot distinguish a secret
    from an id-like token, so classification is by EXACT membership in
    :data:`TRUSTED_ERROR_CODES` (reviewed exception type names) and nothing
    else. Native error text never enters events; it stays in evidence.
    """
    return code if isinstance(code, str) and code in TRUSTED_ERROR_CODES \
        else None


def find_fragments(value, fragments) -> list:
    """Return the fragments of ``fragments`` occurring in ``value``'s JSON.

    The recursive zero-leak assertion helper: an event (or any nested
    structure) is serialized whole, so a fragment hidden at any depth in any
    string value is found. Pure function, no side effects.
    """
    try:
        blob = json.dumps(value, ensure_ascii=False, default=repr)
    except (TypeError, ValueError):  # pragma: no cover - defensive
        blob = repr(value)
    return [fragment for fragment in fragments
            if isinstance(fragment, str) and fragment and fragment in blob]


def _is_int(value) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


# Per-kind data allowlists (PLAN §4.3). Each key maps to a value shape; keys
# absent from a kind's mapping may not appear in that kind's data at all.
_DATA_VALUE_CHECKS = {
    "opaque": lambda v: v is None or (isinstance(v, str)
                                      and bool(_OPAQUE_RE.fullmatch(v))),
    "bool_or_none": lambda v: v is None or isinstance(v, bool),
    "hex64": lambda v: isinstance(v, str) and bool(_HEX64_RE.fullmatch(v)),
    "rfc3339": lambda v: isinstance(v, str)
                          and bool(_RFC3339_UTC_RE.fullmatch(v)),
    "event_id": lambda v: isinstance(v, str)
                           and bool(_EVENT_ID_RE.fullmatch(v)),
    "bool": lambda v: isinstance(v, bool),
    "int": _is_int,
    "int_or_none": lambda v: v is None or _is_int(v),
}
_KIND_DATA_FIELDS: dict[str, dict[str, tuple[str, tuple | None]]] = {
    "task.accepted": {
        "contract_id": ("opaque", None),
        "contract_digest": ("hex64", None),
        "created_at": ("rfc3339", None),
        "initiator_agent": ("token", KNOWN_AGENTS),
        "initiator_session_id": ("opaque", None),
        "initiator_verified": ("bool", None),
        # PLAN §4.2: unverified initiator => reason_code=initiator_unverified
        "reason_code": ("token", ("initiator_unverified",)),
        # only a verified parent record may create the edge (ui_projection)
        "parent_session_id": ("opaque", None),
    },
    "stream.announced": {
        "task_stream_id": ("opaque", None),
        "accepted_event_id": ("event_id", None),
    },
    "session.bound": {
        "locator_type": ("token", ("resident", "app-server-thread")),
    },
    "session.state": {
        "locator_type": ("token", ("resident", "app-server-thread")),
        "last_source_seq": ("int_or_none", None),
        "reason_code": ("token", ("clean_exit", "lost")),
    },
    "turn.started": {},
    "turn.completed": {},
    "tool.scheduled": {
        "tool_name": ("opaque", None),
        # PLAN §4.3 tool fields, nullable when not reliably knowable at the
        # observation point (never fabricated)
        "command_id": ("opaque", None),
        "read_only": ("bool_or_none", None),
    },
    "tool.completed": {
        "tool_name": ("opaque", None),
        "command_id": ("opaque", None),
        "read_only": ("bool_or_none", None),
        "exit_code": ("int_or_none", None),
        "timed_out": ("bool_or_none", None),
    },
    "approval.requested": {
        "tool_name": ("opaque", None),
        # nullable-by-construction: no Authority decision exists yet
        "command_id": ("opaque", None),
        # carried in pairs with approval.decided (round-2 review item 4)
        "requested_variant": ("token", ("acceptWithExecpolicyAmendment",
                                        "acceptForSession",
                                        "approved_for_session")),
    },
    "approval.decided": {
        "tool_name": ("opaque", None),
        "command_id": ("opaque", None),
        "decision": ("token", ("allow", "deny")),
        "reason_code": ("token", APPROVAL_REASON_CODES),
        "requested_variant": ("token", ("acceptWithExecpolicyAmendment",
                                        "acceptForSession",
                                        "approved_for_session")),
    },
    "task.completed": {
        # set only when an error existed but was not classifiable
        "reason_code": ("token", ("error_unclassified",)),
    },
    "process.exited": {
        "process_scope": ("token", ("cold", "shared-resident",
                                    "shared-app-server")),
        "process_generation": ("int_or_none", None),
        "exit_code": ("int", None),
        "reason": ("token", ("close",)),
    },
}


#: Envelope fields that MUST be non-null per kind (round-2 review item 9:
#: PLAN §4.3 required associations/status). Kinds absent here have no
#: beyond-the-envelope requirements.
_REQUIRED_ENVELOPE: dict[str, tuple[str, ...]] = {
    "task.accepted": ("task_id", "worker", "mode"),
    "stream.announced": ("task_id", "worker", "mode"),
    "session.bound": ("session_id",),
    "session.state": ("session_id", "status"),
    "turn.started": ("session_id", "turn_id"),
    "turn.completed": ("session_id", "turn_id"),
    "tool.scheduled": ("session_id", "turn_id", "tool_call_id"),
    "tool.completed": ("session_id", "turn_id", "tool_call_id"),
    "approval.requested": ("session_id", "request_id", "tool_call_id"),
    "approval.decided": ("session_id", "request_id", "tool_call_id"),
    "task.completed": ("task_id", "worker", "mode", "status", "outcome",
                       "duration_ms"),
    "process.exited": ("worker", "status"),
}

#: Data keys that must be PRESENT per kind; True additionally requires a
#: non-null value. A required fact that cannot be obtained reliably means
#: the event is not emitted at all (projection omits it).
_REQUIRED_DATA: dict[str, dict[str, bool]] = {
    "task.accepted": {"contract_id": True, "contract_digest": True,
                      "created_at": True, "initiator_verified": True},
    "stream.announced": {"task_stream_id": True, "accepted_event_id": True},
    "session.bound": {"locator_type": True},
    "session.state": {"locator_type": True, "reason_code": True},
    "tool.scheduled": {"tool_name": True, "command_id": False,
                      "read_only": False},
    "tool.completed": {"tool_name": True, "command_id": False,
                      "read_only": False, "exit_code": False,
                      "timed_out": False},
    "approval.requested": {"tool_name": True, "command_id": False},
    "approval.decided": {"tool_name": True, "decision": True,
                         "reason_code": True, "command_id": False},
    "process.exited": {"process_scope": True, "process_generation": True,
                       "exit_code": True, "reason": True},
}


def _data_problem(kind: str, data) -> str | None:
    if not isinstance(data, dict):
        return "data must be an object"
    allowed = _KIND_DATA_FIELDS[kind]
    for key in data:
        if key not in allowed:
            return f"data key {key!r} is not allowlisted for {kind}"
    for key, value in data.items():
        shape, allowed_values = allowed[key]
        if shape == "token":
            if value is not None and value not in allowed_values:
                return f"data[{key!r}] value is not a fixed token"
            continue
        if not _DATA_VALUE_CHECKS[shape](value):
            return f"data[{key!r}] fails the {shape} value gate"
    for key, non_null in _REQUIRED_DATA.get(kind, {}).items():
        if key not in data:
            return f"data[{key!r}] is required for {kind}"
        if non_null and data[key] is None:
            return f"data[{key!r}] must not be null for {kind}"
    return None


def validate_event(event) -> None:
    """Strictly validate one v1 envelope; raise :class:`UiEventError`.

    Checks the exact key set, version, kind, per-stream identity threading of
    ``event_id``/``seq``/``stream_id``, the UTC RFC3339 observation time, the
    null-or-well-shaped rule for every id/status field, and the per-kind data
    allowlist with per-key value gates.
    """
    if not isinstance(event, dict):
        raise UiEventError("event must be a dict")
    keys = set(event)
    expected = set(ENVELOPE_FIELDS)
    if keys != expected:
        missing = sorted(expected - keys)
        extra = sorted(keys - expected)
        raise UiEventError(f"envelope keys mismatch (missing={missing}, "
                           f"extra={extra})")
    if type(event["v"]) is not int or event["v"] != 1:
        raise UiEventError("v must be the exact integer 1 (no bool/float)")
    kind = event["kind"]
    if kind not in EVENT_KINDS:
        raise UiEventError(f"unknown kind {kind!r}")
    stream_id = event["stream_id"]
    if not isinstance(stream_id, str) or not _STREAM_ID_RE.fullmatch(stream_id):
        raise UiEventError("stream_id must be a short opaque stream identifier")
    seq = event["seq"]
    if not _is_int(seq) or seq < 1:
        raise UiEventError("seq must be a positive integer (per-stream, from 1)")
    parts = event["event_id"].split("/") if isinstance(event["event_id"],
                                                       str) else []
    if (len(parts) != 4
            or not _INSTANCE_ID_RE.fullmatch(parts[0])
            or not (parts[1].isascii() and parts[1].isdigit())
            or int(parts[1]) < 1
            or parts[2] != stream_id or parts[3] != str(seq)):
        raise UiEventError("event_id must be "
                           "<hub_instance_id>/<journal_epoch>/<stream_id>/<seq>"
                           " with an ASCII decimal epoch")
    observed_at = event["observed_at"]
    if not isinstance(observed_at, str) or not _RFC3339_UTC_RE.fullmatch(
            observed_at):
        raise UiEventError("observed_at must be UTC RFC3339 with microseconds "
                           "and a Z suffix")
    try:  # semantic calendar check; fromisoformat yields an aware UTC time
        parsed = datetime.fromisoformat(observed_at)
    except ValueError as error:
        raise UiEventError(f"observed_at is not a real UTC datetime: "
                           f"{error}") from error
    if parsed.utcoffset() != timedelta(0):
        raise UiEventError("observed_at must be UTC (zero offset)")
    if event["task_id"] is not None and not (
            isinstance(event["task_id"], str)
            and _OPAQUE_RE.fullmatch(event["task_id"])):
        raise UiEventError("task_id must be null or an opaque id")
    if event["worker"] not in (None, *KNOWN_AGENTS):
        raise UiEventError("worker must be null or a known agent")
    if event["mode"] is not None and not (
            isinstance(event["mode"], str)
            and re.fullmatch(r"[a-z][a-z0-9-]{0,31}", event["mode"])):
        raise UiEventError("mode must be null or a known safe mode name")
    for key in ("session_id", "turn_id", "tool_call_id", "request_id"):
        if event[key] is not None and safe_opaque(event[key]) is None:
            raise UiEventError(f"{key} must be null or an opaque id")
    if event["source_event_seq"] is not None and (
            not _is_int(event["source_event_seq"])
            or event["source_event_seq"] < 0):
        raise UiEventError("source_event_seq must be null or a non-negative "
                           "integer native seq (never fabricated)")
    for key in ("status", "outcome"):
        if event[key] is not None and safe_token(event[key]) is None:
            raise UiEventError(f"{key} must be null or a short token")
    if event["delivery_ok"] is not None and not isinstance(
            event["delivery_ok"], bool):
        raise UiEventError("delivery_ok must be null or a boolean")
    duration = event["duration_ms"]
    if duration is not None:
        if isinstance(duration, bool) \
                or not isinstance(duration, (int, float)) or duration < 0:
            raise UiEventError("duration_ms must be null or a non-negative "
                               "number computed from time.monotonic()")
        try:
            duration_float = float(duration)
        except OverflowError as error:
            raise UiEventError("duration_ms is outside the representable "
                               "range") from error
        if not math.isfinite(duration_float):
            raise UiEventError("duration_ms must be finite (no NaN/Infinity)")
    if event["error"] is not None and event["error"] not in TRUSTED_ERROR_CODES:
        raise UiEventError("error must be null or a member of the fixed "
                           "TRUSTED_ERROR_CODES enumeration")
    for field in _REQUIRED_ENVELOPE.get(kind, ()):
        if event[field] is None:
            raise UiEventError(f"{field} is required (non-null) for {kind}")
    problem = _data_problem(kind, event["data"])
    if problem is not None:
        raise UiEventError(problem)


class UiEventStream:
    """The single-writer sequence allocator and envelope builder.

    One instance represents one journal pipeline (P0.4: one hub process, in
    memory; P1.1 will move this behind the real journal). ``seq`` is allocated
    per stream, monotonically and gap-free from 1, under the caller's
    serialization — the hub calls this only from its own control flow.
    """

    def __init__(self, hub_instance_id: str, journal_epoch: int = 1):
        if not isinstance(hub_instance_id, str) \
                or not _INSTANCE_ID_RE.fullmatch(hub_instance_id):
            raise UiEventError("hub_instance_id must be an opaque token")
        if not _is_int(journal_epoch) or journal_epoch < 1:
            raise UiEventError("journal_epoch must be a positive integer")
        self.hub_instance_id = hub_instance_id
        self.journal_epoch = journal_epoch
        self._seqs: dict[str, int] = {}

    def next_seq(self, stream_id: str) -> int:
        """Allocate the next sequence number for one stream (from 1)."""
        current = self._seqs.get(stream_id, 0) + 1
        self._seqs[stream_id] = current
        return current

    def build(self, kind: str, stream_id: str, *, task_id=None, worker=None,
              mode=None, session_id=None, turn_id=None, tool_call_id=None,
              request_id=None, source_event_seq=None, status=None,
              outcome=None, delivery_ok=None, duration_ms=None, error=None,
              data=None) -> dict:
        """Build and validate one envelope; ``observed_at`` is assigned here.

        Id-like fields from untrusted surfaces must already be gated by the
        caller via :func:`safe_opaque` / :func:`safe_token` — this builder
        re-validates the whole envelope, so an ungated value surfaces as a
        :class:`UiEventError` at the emit site instead of leaking.

        Round-2 review item 3: the candidate is fully validated BEFORE the
        sequence number is committed, so a failed build leaves the visible
        stream state untouched (the next valid event keeps the next seq).
        """
        candidate_seq = self._seqs.get(stream_id, 0) + 1
        event = {
            "v": 1,
            "event_id": f"{self.hub_instance_id}/{self.journal_epoch}/"
                        f"{stream_id}/{candidate_seq}",
            "stream_id": stream_id,
            "seq": candidate_seq,
            "kind": kind,
            "observed_at": utc_now(),
            "task_id": task_id,
            "worker": worker,
            "mode": mode,
            "session_id": session_id,
            "turn_id": turn_id,
            "tool_call_id": tool_call_id,
            "request_id": request_id,
            "source_event_seq": source_event_seq,
            "status": status,
            "outcome": outcome,
            "delivery_ok": delivery_ok,
            "duration_ms": duration_ms,
            "error": error,
            "data": dict(data) if isinstance(data, dict) else {},
        }
        validate_event(event)  # raises BEFORE any state is committed
        self._seqs[stream_id] = candidate_seq
        return event
