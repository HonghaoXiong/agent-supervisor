"""Authoritative-state to minimal-UI-event projection (P0.4, in-process only).

Every function here is a SIDEBAND: the authoritative state (task records,
session/turn bindings, Authority decisions, registry entries, P0.3 exit
projections) always migrates FIRST, then the projection runs, and no emit
path can change a delegate result, an approval decision, native evidence, or
process cleanup — the emitter never raises and records its own failures.
There is no journal, socket or UI client in P0.4 (P1.1/P1.2); the rollback
switch is ``CZS_UI_EVENTS`` (default ON since 2026-10-24 — the UI surface
ships enabled so double-clicking the app just works; rolling back sets
``CZS_UI_EVENTS=0``, PLAN §7 P0.4).

Initiator identity is fail-closed (:class:`UiInitiatorResolver`): a canonical
opaque session id is only emitted when the agent belongs to the fixed known
enumeration AND a trusted registry (explicitly injected by the controller;
none exists in production today) returns an exact match for the same agent.
A caller-declared initiator, or any parent claim inside it, is never trusted;
a parent edge additionally requires the registry's verified parent record.

Events are only projected from transitions that actually happened:
``task.accepted`` is emitted once the running task record is readable and
precedes the hub-stream ``stream.announced`` referencing its event id;
``process.exited`` is only the P0.3 ExitProjection's awaited-exit record
(exactly once per process identity); finer-grained tool state is omitted
where it cannot be observed reliably — never reconstructed from outcomes.
"""

from __future__ import annotations

import copy
import json
import math
import os
import uuid

from . import ui_events

__all__ = [
    "UiEmitter",
    "UiInitiatorResolver",
    "approval_hook",
    "attribute_notification",
    "authority_hook",
    "default_emitter",
    "default_initiator_resolver",
    "make_exit_sink",
    "notification_identity_set",
    "project_task_accepted",
    "project_task_completed",
    "session_bound",
    "session_state",
    "split_notification_counts",
    "tool_event",
    "turn_event",
]

#: Rollback switch (PLAN §7 P0.4; default flipped ON 2026-10-24 by product
#: decision): the UI event surface is enabled unless the flag carries an
#: explicit false value ("0"/"false"/"no"/"off", stripped, case-insensitive).
#: Unset or empty enables; any other non-empty value counts as an explicit
#: enable. The notification-count correctness fix is independent of this
#: switch and always applies.
ENV_FLAG = "CZS_UI_EVENTS"
_FALSY = ("0", "false", "no", "off")

REASON_INITIATOR_UNVERIFIED = "initiator_unverified"
_LOCATOR_TYPES = ("resident", "app-server-thread")
_IDENTITY_KEYS = ("threadId", "thread_id", "conversationId")


def _env_enabled() -> bool:
    """Default ON (double-click the app and it just works).

    Only a non-empty explicit false value — ``0``/``false``/``no``/``off``,
    after ``strip().lower()`` — disables the surface; unset or empty means
    ON, and every other non-empty value counts as an explicit enable.
    """
    return os.environ.get(ENV_FLAG, "").strip().lower() not in _FALSY


def _active(emitter) -> bool:
    return emitter is not None and emitter.enabled


class UiInitiatorResolver:
    """Fail-closed initiator resolution against a trusted registry.

    ``registry`` is an explicitly injected TRUSTED registry (duck-typed:
    ``canonical_session(agent, session_id) -> canonical | None`` for exact
    matches under the same agent, ``verified_parent_of(canonical) ->
    canonical parent | None``). This class offers no registration path — a
    task record's ``initiator`` dict is a caller claim and is only ever used
    as a lookup key, never as identity. Without a registry (the production
    state today) every resolution is unverified.

    The resolver result field ``initiator_unverified_reason`` maps to the
    event's ``data.reason_code`` (PLAN §4.2 names the key ``reason_code``;
    the value is the fixed short code ``initiator_unverified``).
    """

    def __init__(self, registry=None):
        self._registry = registry

    def resolve(self, agent, session_id) -> dict:
        unverified = {
            "initiator_agent": None,
            "initiator_session_id": None,
            "initiator_verified": False,
            "initiator_unverified_reason": REASON_INITIATOR_UNVERIFIED,
            "parent_session_id": None,
        }
        if not isinstance(agent, str) or not isinstance(session_id, str) \
                or not session_id:
            return unverified
        if agent not in ui_events.KNOWN_AGENTS:
            return unverified
        if self._registry is None:
            return unverified  # no trusted cross-worker registry exists (v1)
        try:
            canonical = self._registry.canonical_session(agent, session_id)
        except Exception:  # noqa: BLE001 - registry trouble fails closed
            return unverified
        canonical = ui_events.safe_opaque(canonical) if isinstance(
            canonical, str) else None
        if not canonical:
            return unverified
        parent = None
        try:
            raw_parent = self._registry.verified_parent_of(canonical)
        except Exception:  # noqa: BLE001
            raw_parent = None
        if isinstance(raw_parent, str):
            parent = ui_events.safe_opaque(raw_parent)
        return {
            "initiator_agent": agent,
            "initiator_session_id": canonical,
            "initiator_verified": True,
            "initiator_unverified_reason": None,
            "parent_session_id": parent,
        }


class UiEmitter:
    """In-process sideband bus: build, validate, serialize, deliver.

    ``build``/``publish``/``emit`` never raise — construction or subscriber
    failures are recorded in :attr:`failures` (bounded) and the envelope is
    skipped. When ``enabled`` is false (the ``CZS_UI_EVENTS=0`` rollback
    state) nothing is built and no sequence numbers are consumed.
    """

    def __init__(self, hub_instance_id=None, journal_epoch=1, enabled=None,
                 subscribers=()):
        self.stream = ui_events.UiEventStream(
            hub_instance_id or uuid.uuid4().hex, journal_epoch)
        self.enabled = _env_enabled() if enabled is None else bool(enabled)
        self.failures: list[str] = []
        self._subscribers = list(subscribers)

    def subscribe(self, subscriber):
        self._subscribers.append(subscriber)

        def unsubscribe():
            try:
                self._subscribers.remove(subscriber)
            except ValueError:
                pass
        return unsubscribe

    def build(self, kind, stream_id, **fields):
        if not self.enabled:
            return None
        try:
            return self.stream.build(kind, stream_id, **fields)
        except Exception as error:  # noqa: BLE001 - sideband, never raises
            self._note(f"build {kind}: {type(error).__name__}")
            return None

    def publish(self, event) -> bool:
        """Deliver one envelope to every subscriber; never raises.

        Round-2 review Critical: subscribers are UNTRUSTED — each receives
        its own deep copy, so a subscriber that mutates or clears its event
        (or raises) can never touch the authoritative envelope, later
        subscribers, or the ids the projection still needs. Serialization is
        probed once with ``allow_nan=False`` (strict JSON only) before any
        copy is handed out.
        """
        if not self.enabled or not isinstance(event, dict):
            return False
        try:
            json.dumps(event, ensure_ascii=False, allow_nan=False)
        except Exception as error:  # noqa: BLE001
            self._note(f"serialize: {type(error).__name__}")
            return False
        delivered = True
        for subscriber in list(self._subscribers):
            try:
                subscriber(copy.deepcopy(event))
            except Exception as error:  # noqa: BLE001 - subscriber isolation
                delivered = False
                self._note(f"subscriber: {type(error).__name__}")
        return delivered

    def emit(self, kind, stream_id, **fields):
        event = self.build(kind, stream_id, **fields)
        if event is not None:
            self.publish(event)
        return event

    def _note(self, text: str) -> None:
        if len(self.failures) < 50:
            self.failures.append(text)


_default_emitter: UiEmitter | None = None
_default_resolver: UiInitiatorResolver | None = None


def default_emitter() -> UiEmitter:
    """Process-wide default emitter (env-gated; one hub_instance_id)."""
    global _default_emitter
    if _default_emitter is None:
        _default_emitter = UiEmitter()
    return _default_emitter


def default_initiator_resolver() -> UiInitiatorResolver:
    """Process-wide resolver without a registry (always unverified in v1)."""
    global _default_resolver
    if _default_resolver is None:
        _default_resolver = UiInitiatorResolver()
    return _default_resolver


# ---------------------------------------------------------------------------
# Task projection points (hub delegate lifecycle)
# ---------------------------------------------------------------------------

def project_task_accepted(emitter, resolver, *, task_id, worker, mode,
                          contract_id, contract_digest, created_at,
                          initiator) -> dict | None:
    """Emit ``task.accepted`` then the hub-stream ``stream.announced``.

    Call ONLY after the running task record is written (readable): the
    announcement references the accepted event id, so a client can never be
    pointed at a stream whose accepted event does not exist yet. Returns the
    accepted envelope (or None when disabled/unbuildable).
    """
    if not _active(emitter):
        return None
    resolution = (resolver if resolver is not None
                  else default_initiator_resolver()).resolve(
        initiator.get("agent") if isinstance(initiator, dict) else None,
        initiator.get("session_id") if isinstance(initiator, dict) else None)
    data = {
        "contract_id": ui_events.safe_opaque(contract_id),
        "contract_digest": ui_events.safe_hex64(contract_digest),
        "created_at": ui_events.normalize_rfc3339(created_at),
        "initiator_agent": resolution["initiator_agent"],
        "initiator_session_id": resolution["initiator_session_id"],
        "initiator_verified": resolution["initiator_verified"],
    }
    if not resolution["initiator_verified"]:
        data["reason_code"] = resolution["initiator_unverified_reason"]
    elif resolution.get("parent_session_id"):
        data["parent_session_id"] = resolution["parent_session_id"]
    accepted = emitter.build("task.accepted", task_id, task_id=task_id,
                             worker=worker, mode=mode, data=data)
    if accepted is None:
        return None
    # save the authoritative id BEFORE any subscriber can observe the event
    # (subscribers get deep copies, but the reference we keep is ours alone)
    accepted_event_id = accepted["event_id"]
    emitter.publish(accepted)
    emitter.emit("stream.announced", ui_events.HUB_STREAM_ID, task_id=task_id,
                 worker=worker, mode=mode,
                 data={"task_stream_id": ui_events.safe_opaque(task_id),
                       "accepted_event_id": accepted_event_id})
    return accepted


def _safe_duration_ms(value):
    """Finite monotonic milliseconds, or None (huge values never raise)."""
    if isinstance(value, bool) or not isinstance(value, (int, float)) \
            or value < 0:
        return None
    try:
        as_float = float(value)
    except OverflowError:
        return None
    if not math.isfinite(as_float):
        return None
    return round(as_float, 3)


def project_task_completed(emitter, *, task_id, worker, mode, status, outcome,
                           delivery_ok, duration_ms, error_code=None,
                           session_locator=None) -> dict | None:
    """Emit ``task.completed`` from the WRITTEN terminal task record.

    Carries the runner status, business outcome, delivery flag and monotonic
    duration. ``error_code`` is only ever echoed when it is a member of the
    fixed :data:`~zcode_supervisor.ui_events.TRUSTED_ERROR_CODES` enumeration
    (an exception TYPE name from trusted hub code — never ``str(error)``,
    native messages or base64); anything else degrades to ``error=null`` plus
    ``data.reason_code="error_unclassified"``. Result payloads and final
    messages stay in the evidence records.
    """
    if not _active(emitter):
        return None
    session_id = None
    if isinstance(session_locator, dict):
        for key in ("session_id", "thread_id"):
            candidate = session_locator.get(key)
            if isinstance(candidate, str) and candidate:
                session_id = ui_events.safe_opaque(candidate)
                break
    data = {}
    trusted_error = ui_events.safe_error_code(error_code)
    if error_code is not None and trusted_error is None:
        data["reason_code"] = "error_unclassified"
    return emitter.emit(
        "task.completed", task_id, task_id=task_id, worker=worker, mode=mode,
        session_id=session_id, status=ui_events.safe_token(status),
        outcome=ui_events.safe_token(outcome),
        delivery_ok=delivery_ok if isinstance(delivery_ok, bool) else None,
        duration_ms=_safe_duration_ms(duration_ms),
        error=trusted_error, data=data)


# ---------------------------------------------------------------------------
# Authority decision projection (read-only, worker-agnostic)
# ---------------------------------------------------------------------------

_VARIANT_TOKENS = ("acceptWithExecpolicyAmendment", "acceptForSession",
                   "approved_for_session")


def _approval_reason_code(record, decision):
    """The fixed decided reason code: an explicit trusted token from the
    caller (bridge paths), else derived from the applied decision alone
    (zcode Authority paths). Free text never qualifies."""
    reason = record.get("reason_code")
    if reason in ui_events.APPROVAL_REASON_CODES:
        return reason
    return ("allowed_by_contract" if decision == "allow"
            else "denied_by_contract")


def approval_hook(emitter, stream_id, worker, mode):
    """Build the sideband hook projecting paired approval events.

    Records carry only safe facts: ids, the caller's OWN session/turn
    bindings, the packet command id of the CURRENT decision (None when no
    Authority decision belongs to this request), the APPLIED decision
    (callers must pass the post-``worker_response`` decision — a
    persistent-variant downgrade is a deny), a fixed reason code and the
    schema-fixed requested variant. Both events of the pair carry
    ``requested_variant`` and a (nullable) ``command_id``; the hook never
    sees tool input, params summaries or reason text.
    """
    def hook(record):
        if not _active(emitter) or not isinstance(record, dict):
            return
        base = {
            "task_id": stream_id,
            "worker": worker,
            "mode": mode,
            "session_id": ui_events.safe_opaque(record.get("session_id")),
            "turn_id": ui_events.safe_opaque(record.get("turn_id")),
            "request_id": ui_events.safe_opaque(record.get("request_id")),
            "tool_call_id": ui_events.safe_opaque(record.get("tool_call_id")),
        }
        variant = record.get("requested_variant")
        variant = variant if variant in _VARIANT_TOKENS else None
        phase = record.get("phase")
        if phase == "requested":
            emitter.emit("approval.requested", stream_id, data={
                "tool_name": ui_events.safe_opaque(record.get("tool_name")),
                "command_id": ui_events.safe_opaque(record.get("command_id")),
                "requested_variant": variant,
            }, **base)
        elif phase == "decided":
            decision = record.get("decision")
            decision = decision if decision in ("allow", "deny") else None
            emitter.emit("approval.decided", stream_id, data={
                "tool_name": ui_events.safe_opaque(record.get("tool_name")),
                "command_id": ui_events.safe_opaque(record.get("command_id")),
                "decision": decision,
                "reason_code": _approval_reason_code(record, decision),
                "requested_variant": variant,
            }, **base)
    return hook


def authority_hook(emitter, stream_id, worker, mode):
    """ZCode-side approval projection at the Authority decision point.

    The zcode permission callback returns the Authority decision unchanged
    (no persistent-variant downgrade on that path), so the authority's own
    decision IS the applied decision.
    """
    return approval_hook(emitter, stream_id, worker, mode)


# ---------------------------------------------------------------------------
# Session / turn / tool binding projection (worker drivers)
# ---------------------------------------------------------------------------

def session_bound(emitter, stream_id, worker, mode, session_id, locator_type):
    """Emit ``session.bound`` after the authoritative session binding."""
    if not _active(emitter):
        return
    emitter.emit("session.bound", stream_id, task_id=stream_id, worker=worker,
                 mode=mode, session_id=ui_events.safe_opaque(session_id),
                 data={"locator_type": locator_type
                       if locator_type in _LOCATOR_TYPES else None})


def session_state(emitter, stream_id, worker, mode, session_id, locator_type,
                  state, last_source_seq=None):
    """Emit ``session.state`` (exited/lost) after the registry transition."""
    if not _active(emitter):
        return
    reason = {"exited": "clean_exit", "lost": "lost"}.get(state)
    seq = last_source_seq if isinstance(last_source_seq, int) \
        and not isinstance(last_source_seq, bool) else None
    emitter.emit("session.state", stream_id, task_id=stream_id, worker=worker,
                 mode=mode, session_id=ui_events.safe_opaque(session_id),
                 status=state if reason else None,
                 data={"locator_type": locator_type
                       if locator_type in _LOCATOR_TYPES else None,
                       "last_source_seq": seq,
                       "reason_code": reason})


def turn_event(emitter, stream_id, worker, mode, session_id, turn_id,
               completed=False, source_event_seq=None, outcome=None,
               status=None):
    """Emit ``turn.started``/``turn.completed`` at the binding transition.

    ``source_event_seq`` is the worker's OWN native event seq when one exists
    (zcode resident); codex has none and must pass None — never fabricated.
    """
    if not _active(emitter):
        return
    emitter.emit("turn.completed" if completed else "turn.started", stream_id,
                 task_id=stream_id, worker=worker, mode=mode,
                 session_id=ui_events.safe_opaque(session_id),
                 turn_id=ui_events.safe_opaque(turn_id),
                 source_event_seq=source_event_seq
                 if isinstance(source_event_seq, int)
                 and not isinstance(source_event_seq, bool) else None,
                 status=ui_events.safe_token(status),
                 outcome=ui_events.safe_token(outcome))


def tool_event(emitter, stream_id, worker, mode, session_id, turn_id,
               tool_call_id, phase, tool_name, status=None, command_id=None,
               read_only=None, exit_code=None, timed_out=None):
    """Emit ``tool.scheduled``/``tool.completed`` from observed tool events.

    Only ids, the tool name and the PLAN tool facts travel — command_id /
    read_only (scheduled) and exit_code / timed_out (completed) are carried
    as EXPLICIT NULL keys when not reliably knowable at the observation
    point (PLAN §4.3: null, never fabricated). Commands, inputs,
    outputs and file lists never travel. A missing required association
    (session, turn, tool call id or tool name) omits the WHOLE event.
    """
    if not _active(emitter):
        return
    if not (ui_events.safe_opaque(session_id) and ui_events.safe_opaque(turn_id)
            and ui_events.safe_opaque(tool_call_id)
            and ui_events.safe_opaque(tool_name)):
        return
    data = {
        "tool_name": ui_events.safe_opaque(tool_name),
        "command_id": ui_events.safe_opaque(command_id),
        "read_only": read_only if isinstance(read_only, bool) else None,
    }
    if phase == "completed":
        data["exit_code"] = (exit_code if isinstance(exit_code, int)
                             and not isinstance(exit_code, bool) else None)
        data["timed_out"] = (timed_out if isinstance(timed_out, bool)
                             else None)
    emitter.emit("tool.completed" if phase == "completed" else "tool.scheduled",
                 stream_id, task_id=stream_id, worker=worker, mode=mode,
                 session_id=ui_events.safe_opaque(session_id),
                 turn_id=ui_events.safe_opaque(turn_id),
                 tool_call_id=ui_events.safe_opaque(tool_call_id),
                 status=ui_events.safe_token(status),
                 data=data)


# ---------------------------------------------------------------------------
# Process exit projection (P0.3 ExitProjection -> UI event)
# ---------------------------------------------------------------------------

def make_exit_sink(emitter, process_scope, worker):
    """Sink for :class:`worker_health.ExitProjection` ``on_event`` callbacks.

    Only ever receives records of REAL awaited waits (the projection itself
    is exactly-once per instance and rejects non-integer codes); this sink
    additionally guards one emission per ``(process_scope, generation)`` so a
    reused emitter can never duplicate an exit across driver shapes. An exit
    without a generation stamp is omitted (not guessed) — the required
    process_generation fact is unavailable.
    """
    emitted: set = set()

    def sink(record):
        if not _active(emitter) or not isinstance(record, dict):
            return
        code = record.get("returncode")
        if isinstance(code, bool) or not isinstance(code, int):
            return
        generation = record.get("generation")
        if isinstance(generation, bool) or not isinstance(generation, int):
            return
        key = (process_scope, generation)
        if key in emitted:
            return
        emitted.add(key)
        emitter.emit("process.exited", ui_events.HUB_STREAM_ID, worker=worker,
                     status="exited",
                     data={"process_scope": process_scope,
                           "process_generation": generation,
                           "exit_code": code,
                           "reason": record.get("source")
                           if record.get("source") in ("close",) else None})
    return sink


# ---------------------------------------------------------------------------
# Notification attribution (current-task filtering)
# ---------------------------------------------------------------------------

def notification_identity_set(params) -> set:
    """Every distinct non-empty thread/conversation identity in one frame.

    Round-2 review: attribution must consider ALL candidate identity fields
    of the native envelope, not just the first — a frame carrying conflicting
    identities (``threadId=th_A, conversationId=th_B``) is not attributable
    to either.
    """
    if not isinstance(params, dict):
        return set()
    return {params[key] for key in _IDENTITY_KEYS
            if isinstance(params.get(key), str) and params[key]}


def notification_identity(params):
    """The single thread/conversation identity, or None (legacy helper)."""
    identities = notification_identity_set(params)
    return next(iter(identities)) if len(identities) == 1 else None


def split_notification_counts(notifications, thread_id):
    """Split shared-client notification frames for ONE task's receipt.

    Returns ``(own, unattributed)`` (round-2 semantics, PLAN §4.2/222):

    - ``own``: frames whose identity set is EXACTLY ``{thread_id}`` — the
      only frames that may enter this task's receipt or timeline;
    - ``unattributed``: frames with NO identity or CONFLICTING identities —
      they belong to the independent run-level view (the caller merges this
      into the shared client's aggregate), never to any task;
    - a frame with exactly one identity that is NOT this task's is neither:
      it is attributable, and its owning task's receipt counts it.
    """
    own: dict = {}
    unattributed: dict = {}
    for frame in notifications if isinstance(notifications, list) else []:
        if not isinstance(frame, dict):
            continue
        method = frame.get("method")
        if not isinstance(method, str) or not method:
            continue
        identities = notification_identity_set(frame.get("params"))
        if len(identities) == 1 and thread_id in identities:
            own[method] = own.get(method, 0) + 1
        elif len(identities) != 1:
            unattributed[method] = unattributed.get(method, 0) + 1
    return own, unattributed


def attribute_notification(method, params, bindings):
    """Attribute one notification to a task, or explain why it cannot be.

    ``bindings`` maps thread/conversation identity -> task id. Returns
    ``(task_id, reason_code)``: a match, or ``(None, code)`` with a fixed
    short code (``identity_missing`` / ``identity_unknown`` /
    ``identity_ambiguous``). Missing identity is unattributed, never guessed.
    """
    if not isinstance(params, dict):
        return None, "identity_missing"
    identities = {params[key] for key in _IDENTITY_KEYS
                  if isinstance(params.get(key), str) and params[key]}
    if not identities:
        return None, "identity_missing"
    if len(identities) > 1:
        return None, "identity_ambiguous"
    identity = next(iter(identities))
    if not isinstance(bindings, dict) or identity not in bindings:
        return None, "identity_unknown"
    return bindings[identity], None
