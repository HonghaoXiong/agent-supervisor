"""Session-loop coordination: delivery → review → decision → the SAME session.

Round states: observed → review_queued → reviewing → decision_ready →
instruction_queued → worker_running → settled, plus blocked and
needs_reconciliation. One round per (binding, turn identity, candidate):
duplicate completion events re-read the SAME round — never a second review
or send.

Two review transports, routed BEFORE any model call and never substituted
for one another: ``managed-reviewer`` drives the pinned Codex app-server
thread; ``desktop-bound`` calls the host-validated desktop wake command and
reports ``desktop-wakeup-unsupported`` when none is configured.

Dispatch never starts a session: a control binding must prove the hub task
it attaches to resumes the bound session, and the proof is re-checked
against the persisted task record before every enqueue. The loop routes
DETERMINISTICALLY — the reviewer's structured decision is schema-validated
(one repair round, then blocked), and acceptance authority is never inferred.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
import shlex
import threading
from datetime import datetime, timezone
from pathlib import Path

from .contract_compile import compile_stage_contract
from .session_binding import (build_candidate, desktop_wakeup_capability,
                              next_round_page, read_session_delta, record_reviewer,
                              session_turn_identity, wake_desktop)
from .work_contracts import canonical_sha256
from .work_models import WorkError

DECISIONS = ("continue", "rework", "answer", "done", "need_user", "blocked")
DEFAULT_MAX_ROUNDS = 20
DEFAULT_MAX_NO_PROGRESS = 3
TERMINAL_SESSION_STATES = ("turn_completed", "failed", "interrupted")
# A turn whose round could not be created is retried after this long, not on
# every watcher tick.
RETRY_BACKOFF_S = 60.0
_CHECK_ID = re.compile(r"[A-Za-z0-9._:-]{1,64}")
# A prompt larger than this carries head+tail and points at the full report
# artifact instead of silently dropping the middle.
PROMPT_REPORT_BYTES = 256 * 1024
_DECISION_FIELDS = {"schema_version", "binding_id", "round_id", "source_turn_id",
                    "candidate_sha256", "design_revision", "decision", "findings",
                    "question_answers", "next_instruction", "required_checks",
                    "after_answer", "decision_refs"}
_DECISION_SCHEMA = {
    "schema_version": "zsc.review-decision/v1",
    "binding_id": "<the packet's binding_id, verbatim>",
    "round_id": "<the packet's round_id, verbatim>",
    "source_turn_id": "<the packet's source.turn.turn_id>",
    "candidate_sha256": "<the packet's candidate.sha256, verbatim>",
    "design_revision": "<integer or null>",
    "decision": "continue|rework|answer|done|need_user|blocked",
    "findings": [{"severity": "blocker|major|minor",
                  "file": "<path>", "line": "<int or null>",
                  "problem": "<one sentence>", "required_change": "<one sentence>"}],
    "question_answers": [{"question": "<verbatim>", "answer": "<text>"}],
    "next_instruction": "<what the worker must do next, or null>",
    "required_checks": ["<command or check the next delivery must show>"],
    "after_answer": "continue|wait|null",
    "decision_refs": ["<evidence refs>"],
}


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _bound_work(binding) -> str | None:
    """The work id recorded at bind time (evidence JSON, not a DB column)."""
    evidence = binding.get("evidence") or {}
    return binding.get("work_id") or evidence.get("work_id")


def _recorded_contributors(source) -> list:
    """The bound session that produced the delivery is a contributor."""
    session_id = (source or {}).get("worker_session_id")
    return [{"actor_id": "zcode", "session_id": session_id}] if session_id else []


class SessionLoop:
    """Coordinates review rounds for one binding against the durable owner."""

    def __init__(self, store, dispatcher, collaboration, *, reviewer,
                 authority, owner_actor, authorization, stage_bindings,
                 hub_state_dir, profile, authorization_root=None,
                 desktop_runner=None,
                 desktop_bridge=None,
                 native_bridge=None,
                 native_gui_bridge=None,
                 max_rounds=DEFAULT_MAX_ROUNDS,
                 max_no_progress=DEFAULT_MAX_NO_PROGRESS,
                 report_root=None):
        self.store = store
        self.dispatcher = dispatcher
        self.collaboration = collaboration
        self.reviewer = reviewer
        self.desktop_runner = desktop_runner or wake_desktop
        # Desktop chat delivery (queue+wake). None keeps the legacy custom
        # CZS_DESKTOP_WAKE_CMD channel; a configured bridge means the literal
        # desktop chat is the reviewer.
        self.desktop_bridge = desktop_bridge
        # Delivery into a GUI-created ZCode session (CLI resume). None keeps
        # native claims review-only.
        self.native_bridge = native_bridge
        # Last-resort delivery through the ZCode window itself; used only when
        # the CLI bridge is absent, and still proven by the persisted message.
        self.native_gui_bridge = native_gui_bridge
        self.authority = authority
        self.owner_actor = owner_actor  # the owner runtime actor (internal submits)
        self.authorization = authorization
        self.stage_bindings = stage_bindings  # base bindings for the work stage
        self.hub_state_dir = Path(hub_state_dir)  # contracts/tasks register here
        # the OWNER's artifact store: a binding's authorization_ref is looked
        # up there, not in the hub state tree
        self.authorization_root = authorization_root
        self.profile = profile  # host worker profile (work_runtime shape)
        self.report_root = Path(report_root or (self.hub_state_dir / "sessions"))
        self.max_rounds = max_rounds
        self.max_no_progress = max_no_progress
        self._lock = threading.RLock()

    # ------------------------------------------------------------ advance

    def advance(self, binding_id, source=None) -> dict:
        """One pump step: read the session, review a completed turn, route.

        ``source`` is optional: when absent the loop derives it from the
        owner's persisted execution evidence (the last instruction's task
        record) and verifies that record resumes THIS session. A caller-
        supplied source is verified the same way — an unverifiable task is
        refused rather than recorded as "replied in the original session".
        """
        with self._lock:
            binding = self.store.get_session_binding(binding_id)
            if binding is None:
                raise WorkError("binding-missing", f"unknown binding {binding_id!r}")
            if binding["state"] != "bound":
                return {"status": "binding-not-active",
                        "binding_state": binding["state"]}
            capabilities = set(binding.get("capabilities", []))
            if not ({"control", "review"} & capabilities):
                # Reading a turn is harmless, but a round ends in a dispatched
                # instruction; without the proven handoff that dispatch would
                # have to start a NEW session. Refuse before any model call.
                return {"status": "observe-only",
                        "detail": "this binding has no proven control; rebind "
                                  "with control and a hub task for this session"}

            resolved = self._resolve_source(binding, source)
            if resolved.get("status") == "mismatch":
                return resolved
            replaced = self._workspace_replaced(binding)
            if replaced:
                # Two owners can share a path string. If the directory was
                # deleted and recreated, the old binding is looking at a
                # different tree with the same name; reviewing it would pair
                # this session's report with another workspace's candidate.
                return {"status": "blocked", "error": "workspace-replaced",
                        "detail": replaced}
            # A send that completed while the owner was down left a dispatch
            # without its turn. Settle it BEFORE a new round: the turn it
            # started belongs to that attempt, not to a fresh review.
            self._settle_dispatch(binding)
            if self._unconfirmed_dispatch(binding["binding_id"]):
                return {"status": "blocked", "error": "dispatch-unconfirmed",
                        "detail": "a native delivery was sent but its turn is "
                                  "not confirmed; review waits for "
                                  "reconciliation instead of attributing a "
                                  "human turn to this attempt"}
            source = resolved["source"]
            # Freeze the design this round observed: `done` later proves the
            # accepted design is the reviewed one, not a newer edit.
            try:
                work_context = self.collaboration.context(source.get("work_id")) or {}
            except WorkError:
                work_context = {}
            if work_context.get("design_sha256"):
                source.setdefault("design_sha256", work_context["design_sha256"])

            delta = next_round_page(binding)
            pages = [delta]
            while delta.get("continuation"):
                delta = read_session_delta(binding, cursor=delta["cursor"])
                pages.append(delta)
            if delta.get("status") != "ok":
                return {"status": "observed", "reason": delta.get("reason")}
            state = delta.get("session_state")
            if state not in TERMINAL_SESSION_STATES:
                return {"status": "observed", "session_state": state}

            turn = delta.get("turn") or {}
            # A completed turn belongs to exactly ONE round, and this check
            # must run BEFORE the workspace scan: a re-claim with a rewound
            # cursor would otherwise rebuild a candidate and review the same
            # turn twice. An existing round for this turn is resumed when this
            # binding owns it, and reported as already observed otherwise.
            turn_id = turn.get("turn_id")
            if turn_id:
                for other in self.store.session_rounds_all():
                    other_source = other.get("source") or {}
                    if (other_source.get("turn") or {}).get("turn_id") != turn_id:
                        continue
                    if other_source.get("session_id") != binding["session_id"]:
                        continue
                    if other["binding_id"] == binding_id:
                        return self._resume_round(other, binding)
                    return {"status": "already-observed",
                            "detail": f"turn {turn_id!r} is owned by round "
                                      f"{other['round_id']!r} of binding "
                                      f"{other['binding_id']!r}"}

            native = bool((binding.get("evidence") or {}).get("native_claim"))
            last_delivery = self._last_dispatch_turn(binding_id)
            if native and turn_id and last_delivery \
                    and turn_id != last_delivery:
                return {"status": "blocked", "error": "human-interjection",
                        "detail": "the latest turn was not started by the "
                                  "owner's native delivery; a human turn "
                                  "pauses the loop"}
            candidate = self._candidate(binding, turn_id=turn_id)
            # The identity was checked before resolve; a workspace swapped
            # DURING the scan would pair this candidate with another tree, so
            # the recorded inode is compared once more before it is used.
            if self._workspace_replaced(binding):
                return {"status": "blocked", "error": "workspace-replaced",
                        "detail": "the bound workspace changed while the "
                                  "candidate was being scanned"}
            try:
                digest = candidate["sha256"]
                if not digest:
                    raise WorkError(
                        "candidate-coverage-incomplete",
                        "the candidate scan is incomplete "
                        f"({candidate.get('reason') or 'unknown'}); refusing to "
                        "review a state whose full contents are unknown")
            except WorkError as error:
                return {"status": "blocked", "error": error.code,
                        "detail": error.message}

            cursor = delta["cursor"]
            round_id = "round-" + canonical_sha256({
                "binding_id": binding_id, "message_sequence": cursor["message_sequence"],
                "parts_hash": cursor["parts_hash"],
                "candidate_sha256": digest})[:24]
            existing = self.store.get_session_round(round_id)
            if existing is not None:
                return self._resume_round(existing, binding)

            seq = self.store.next_round_seq(binding_id)
            report = "\n".join(entry["text"] or "" for page in pages
                               for entry in page["messages"]
                               if entry.get("role") == "assistant")
            if not report.strip():
                return {"status": "blocked", "error": "review-packet-empty",
                        "detail": "the delta between the binding cursor and "
                                  "the turn carried no assistant text; "
                                  "refusing to send an empty report"}
            report_path = self._write_report(binding_id, round_id, report)
            packet = {
                "schema_version": "zsc.review-packet/v1",
                "binding_id": binding_id, "round_id": round_id, "round": seq,
                "work_id": source.get("work_id"),
                "goal_state": {"design_revision": source.get("design_revision")},
                "source": {
                    "session_id": binding["session_id"],
                    "work_id": source.get("work_id"),
                    "turn": {"turn_id": turn.get("turn_id"),
                             "message_sequence": cursor["message_sequence"]},
                    "session_state": state,
                    "task_id": source.get("task_id"),
                    "attempt_id": source.get("attempt_id"),
                    "worker_session_id": source.get("worker_session_id"),
                    "session_match": source.get("session_match"),
                    "contributors": list(source.get("contributors") or []),
                },
                "candidate": {"sha256": digest, "changes": candidate.get("changes"),
                              "head": candidate.get("head"),
                              "file_count": candidate.get("file_count"),
                              "artifact": candidate.get("artifact")},
                "report": {"artifact": str(report_path),
                            "sha256": _sha256_text(report),
                            "byte_count": len(report.encode("utf-8")),
                            "text_chars": len(report)},
                "reviewer_mode": binding["mode"],
            }
            packet["report_text"] = self._report_for_prompt(report)
            record = {
                "round_id": round_id, "binding_id": binding_id, "seq": seq,
                "state": "review_queued", "source": packet["source"],
                "candidate_sha256": digest,
                "design_revision": source.get("design_revision"),
                "reviewer": None, "packet": packet, "decision": None,
                "instruction": None, "error": None,
                "review_intent": None,
                "created": _now(), "updated": _now(),
            }
            self._note_delivery(record, source.get("work_id"), candidate, source)
            self.store.save_session_round(record)
            self._advance_binding_cursor(binding, cursor)
            return self._run_round(record, binding)

    # ------------------------------------------------------- round driving

    def _run_round(self, record, binding) -> dict:
        """Send the review request, persist intent/receipt, then route."""
        packet = record["packet"]
        prompt_text = self._prompt(packet)
        prompt_path = self._write_prompt(record, prompt_text)
        record["review_intent"] = {
            "mode": binding["mode"],
            "prompt_sha256": _sha256_text(prompt_text),
            "prompt_path": str(prompt_path),
            "sent_at": _now(),
        }
        record["state"] = "reviewing"
        submission_id = f"zsc-review-{record['round_id'][:20]}"
        # Persist the CHANNEL and the id BEFORE anything is sent: a crash
        # between the send and the first receipt must resume by QUERYING this
        # id, not by guessing the channel from the managed reviewer.
        reviewer = ((binding.get("evidence") or {}).get("reviewer") or {})
        record["review"] = {
            "submission_id": submission_id, "started": _now(),
            "mode": binding.get("mode"),
            "thread_id": reviewer.get("thread_id"),
            "delivery_id": submission_id,
            # A retry counter must survive the re-run that follows a
            # review_queued resume; resetting it here let the same delivery
            # be sent four times.
            "refreshes": int((record.get("review") or {}).get("refreshes") or 0)}
        self.store.save_session_round(record)

        outcome = None
        for attempt in (1, 2):
            try:
                outcome = self._review_once(binding, prompt_text, prompt_path,
                                            attempt=attempt, record=record,
                                            submission_id=submission_id)
            except WorkError as error:
                if error.code in ("desktop-wakeup-unsupported", "observe-only"):
                    record["state"] = "blocked"
                    record["error"] = f"{error.code}: {error.message}"
                    self.store.save_session_round(record)
                    return {"status": "blocked", "round": record["round_id"],
                            "error": error.code, "detail": error.message}
                raise
            if outcome.get("outcome") in ("in-progress", "failed"):
                record["review"] = {
                    **(record.get("review") or {}),
                    "mode": "desktop",
                    "delivery_id": outcome.get("delivery_id") or submission_id,
                    "thread_id": outcome.get("thread_id"),
                    "turn_id": outcome.get("turn_id"),
                    "state": outcome["outcome"],
                }
                if outcome["outcome"] == "failed":
                    record["state"] = "needs_reconciliation"
                    record["error"] = "review-turn-failed"
                    self.store.save_session_round(record)
                    return {"status": "needs_reconciliation",
                            "round": record["round_id"],
                            "error": "review-turn-failed"}
                record["state"] = "reviewing"
                self.store.save_session_round(record)
                return {"status": "review-pending",
                        "round": record["round_id"],
                        "turn_id": outcome.get("turn_id")}
            record["reviewer"] = {**outcome, "attempt": attempt}
            if outcome.get("kind") == "managed" and outcome.get("thread_id"):
                record_reviewer(self.store, binding, thread_id=outcome["thread_id"],
                                turn_id=outcome.get("turn_id"))
            decision, problems = self._parse_decision(
                outcome.get("final_message"), record)
            if decision is None and attempt == 1:
                prompt_text = (
                    "Your previous answer was not a valid "
                    "zsc.review-decision/v1 object:\n" + "; ".join(problems)
                    + "\nAnswer again with exactly ONE JSON object.")
                prompt_path = self._write_prompt(record, prompt_text, suffix="-repair")
                continue
            break
        if decision is None:
            record["state"] = "blocked"
            record["error"] = "decision-unparseable: " + "; ".join(problems)[:300]
            self.store.save_session_round(record)
            return {"status": "blocked", "round": record["round_id"]}
        record["decision"] = decision
        record["state"] = "decision_ready"
        self.store.save_session_round(record)
        result = self.submit_decision(decision, actor=self.owner_actor)
        result["reviewer"] = self._reviewer_identity(record, binding)
        return result

    @staticmethod
    def _reviewer_identity(record, binding) -> dict:
        """What actually reviewed this round (never a substitute mode)."""
        receipt = record.get("reviewer") or {}
        return {"requested_mode": binding["mode"],
                "kind": receipt.get("kind"),
                "thread_id": receipt.get("thread_id"),
                "turn_id": receipt.get("turn_id")}

    def _resume_round(self, record, binding) -> dict:
        """A round that already exists is continued, never re-created.

        ``review_queued`` never left the owner (nothing was sent), so it is
        safe to drive. ``reviewing`` means a request WAS sent and its outcome
        is unknown: the loop records that explicitly instead of blindly
        sending the prompt a second time.
        """
        state = record.get("state")
        if state == "review_queued":
            return self._run_round(record, binding)
        if state in ("reviewing", "needs_reconciliation"):
            review = record.get("review") or {}
            if not review.get("submission_id"):
                # Nothing was sent (or the id was never persisted): a blind
                # resend is not allowed, keep the explicit reconciliation.
                record["state"] = "needs_reconciliation"
                record["error"] = "review-outcome-unknown"
                self.store.save_session_round(record)
                return {"status": "needs_reconciliation",
                        "round": record["round_id"],
                        "error": "review-outcome-unknown"}
            submission_id = review.get("submission_id")
            if review.get("mode") in ("desktop", "desktop-bound")                     and self.desktop_bridge is not None:
                return self._resume_desktop_review(record, binding, review)
            finder = getattr(self.reviewer, "find_finished", None)
            if submission_id and finder is not None:
                persisted = ((binding.get("evidence") or {}).get("reviewer")
                             or {}).get("thread_id")
                if persisted and getattr(self.reviewer, "thread_id", None) is None:
                    self.reviewer.thread_id = persisted
                found = asyncio.run(finder(submission_id))
                if found is None:
                    # The request never reached the reviewer; the SAME
                    # submission id is retried, so a late duplicate cannot
                    # produce a second review turn.
                    record["state"] = "review_queued"
                    self.store.save_session_round(record)
                    return {"status": "review-retried",
                            "round": record["round_id"],
                            "detail": "the review submission never landed; "
                                      "retrying it once under the same id"}
                if found.get("status") == "final":
                    decision, problems = self._parse_decision(
                        found.get("final_message"), record)
                    if decision is None:
                        record["state"] = "blocked"
                        record["error"] = ("decision-unparseable: "
                                           + "; ".join(problems)[:300])
                        self.store.save_session_round(record)
                        return {"status": "blocked",
                                "round": record["round_id"]}
                    record["decision"] = decision
                    record["review"] = {**review, "recovered": True,
                                        "turn_id": found.get("turn_id")}
                    persisted_thread = (
                        (binding.get("evidence") or {}).get("reviewer")
                        or {}).get("thread_id") or getattr(
                            self.reviewer, "thread_id", None)
                    record["reviewer"] = {
                        **(record.get("reviewer") or {}),
                        "kind": "managed",
                        "thread_id": persisted_thread,
                        "turn_id": found.get("turn_id")}
                    record["state"] = "decision_ready"
                    self.store.save_session_round(record)
                    result = self.submit_decision(decision,
                                                  actor=self.owner_actor)
                    result["reviewer"] = self._reviewer_identity(record, binding)
                    return result
                # still answering: keep the round in `reviewing` so the next
                # tick polls the SAME submission; a late final answer routes
                # normally instead of being stranded here.
                record["review"] = {**(record.get("review") or {}),
                                    "last_poll": _now()}
                self.store.save_session_round(record)
                return {"status": "review-pending",
                        "round": record["round_id"],
                        "turn_id": found.get("turn_id")}
            record["state"] = "needs_reconciliation"
            record["error"] = "review-outcome-unknown"
            self.store.save_session_round(record)
            return {"status": "needs_reconciliation", "round": record["round_id"],
                    "error": "review-outcome-unknown",
                    "detail": "the review request was sent before the owner "
                              "stopped and no receipt was recorded; retry "
                              "explicitly instead of resending blindly"}
        if state == "decision_ready" and not record.get("instruction"):
            # The decision was parsed but routing it failed; a rerun of the
            # reviewer would only pay for the same answer again.
            if not record.get("decision"):
                return {"status": "already-observed", "round": record}
            result = self.submit_decision(record["decision"],
                                          actor=self.owner_actor)
            result["reviewer"] = self._reviewer_identity(record, binding)
            return result
        if state in ("decision_ready", "instruction_queued", "worker_running",
                     "settled", "done", "need_user", "blocked",
                     "needs_reconciliation"):
            return {"status": "already-observed", "round": record}
        return self._run_round(record, binding)

    def _review_once(self, binding, prompt_text, prompt_path, *, attempt,
                     record=None, submission_id=None) -> dict:
        mode = binding["mode"]
        if mode == "desktop-bound":
            if self.desktop_bridge is not None:
                return self._review_desktop_bridge(
                    binding, prompt_text, attempt, record,
                    submission_id=submission_id)
            # Routed BEFORE any model call: no capability, no round, and no
            # silent fallback to the managed reviewer.
            capability = desktop_wakeup_capability()
            if capability.get("capability") != "desktop-wake-command":
                raise WorkError("desktop-wakeup-unsupported",
                                capability.get("reason", "desktop wakeup unavailable"))
            outcome = self.desktop_runner(prompt_path)
            return {**outcome, "kind": "desktop"}
        if "control" not in binding.get("capabilities", []) \
                and "observe" not in binding.get("capabilities", []):
            raise WorkError("observe-only", "the binding grants no review mode")
        persisted = ((binding.get("evidence") or {}).get("reviewer") or {}).get("thread_id")
        if persisted and getattr(self.reviewer, "thread_id", None) is None:
            # a restarted owner continues the SAME review thread
            self.reviewer.thread_id = persisted
        outcome = asyncio.run(self.reviewer.review_round(
            self.authority, prompt_text, submission_id=submission_id))
        return {**outcome, "kind": "managed"}

    def _review_desktop_bridge(self, binding, prompt_text, attempt, record,
                               submission_id=None):
        """Deliver the review prompt into the literal desktop chat.

        The delivery id is derived from the prompt (and the repair attempt),
        so a restarted owner replays the SAME id: the bridge returns the
        recorded turn instead of queueing the prompt twice.
        """
        # The SAME id the round persisted is the one the bridge sends and the
        # one a restarted owner looks up: a separate derivation here made the
        # sent marker and the stored submission id disagree.
        delivery_id = submission_id or f"zsc-{_sha256_text(prompt_text)[:16]}-{attempt}"
        # The bound session is ZCode (sess_...); the desktop chat that reviews
        # it is a SEPARATE paired Codex thread. Never feed one id to the other.
        reviewer = ((binding.get("evidence") or {}).get("reviewer") or {})
        thread_id = reviewer.get("thread_id")
        if not isinstance(thread_id, str) or not thread_id.startswith("01"):
            raise WorkError(
                "reviewer-thread-missing",
                "the binding records no paired Codex reviewer thread; a "
                "desktop review cannot guess one from the ZCode session id")
        host = reviewer.get("host", "local")
        if host != "local":
            raise WorkError(
                "reviewer-host-unsupported",
                f"the paired reviewer runs on {host!r}; only the local desktop "
                "chat has a delivery channel")
        latest = asyncio.run(self.desktop_bridge.latest_turn(thread_id))
        if latest is not None and latest.get("client_id") != delivery_id \
                and self._after(record, latest.get("started_at_ms")):
            raise WorkError(
                "human-interjection",
                "a human turn arrived after this round was prepared; the "
                "review prompt is held until the round is reconciled")
        receipt = asyncio.run(self.desktop_bridge.deliver(
            thread_id, prompt_text, delivery_id=delivery_id))
        if not receipt.get("delivered"):
            if receipt.get("status") in ("in-progress", "failed"):
                # The message IS in the chat. Persisting its turn lets the
                # next advance look the SAME turn up instead of re-sending a
                # second review prompt beside it.
                return {"outcome": receipt["status"], "kind": "desktop",
                        "thread_id": thread_id,
                        "turn_id": receipt.get("turn_id"),
                        "delivery_id": delivery_id,
                        "final_message": None}
            raise WorkError(
                "desktop-delivery-failed",
                f"the desktop chat produced no reply ({receipt.get('status')}; "
                f"delivery {delivery_id})")
        return {"outcome": "delivered", "kind": "desktop",
                "thread_id": thread_id,
                "turn_id": receipt.get("turn_id"),
                "message_id": receipt.get("agent_message_id"),
                "final_message": receipt.get("final_message"),
                "delivery": receipt}

    def _resume_desktop_review(self, record, binding, review):
        """Continue a desktop round by the SAME delivery id after a restart.

        A half turn is not evidence: only a delivery whose persisted turn has
        a final answer is parsed. A failed turn is reconciled, and an
        in-progress one keeps waiting on the turn that is already in the chat.
        """
        delivery_id = review.get("delivery_id") or review.get("submission_id")
        reviewer = ((binding.get("evidence") or {}).get("reviewer") or {})
        thread_id = review.get("thread_id") or reviewer.get("thread_id")
        if not delivery_id or not thread_id or self.desktop_bridge is None:
            record["state"] = "needs_reconciliation"
            record["error"] = "review-outcome-unknown"
            self.store.save_session_round(record)
            return {"status": "needs-reconciliation",
                    "round": record["round_id"],
                    "error": "review-outcome-unknown"}
        receipt = asyncio.run(self.desktop_bridge.find(thread_id, delivery_id))
        status = (receipt or {}).get("status")
        if receipt is not None and status == "already-delivered" \
                and receipt.get("final_message"):
            decision, problems = self._parse_decision(
                receipt["final_message"], record)
            if decision is None:
                record["state"] = "blocked"
                record["error"] = ("decision-unparseable: "
                                   + "; ".join(problems))
                self.store.save_session_round(record)
                return {"status": "blocked", "round": record["round_id"],
                        "error": record["error"]}
            record["decision"] = decision
            record["state"] = "decision_ready"
            record["review"] = {**review, "recovered": True,
                                "turn_id": receipt.get("turn_id")}
            self.store.save_session_round(record)
            result = self.submit_decision(decision, actor=self.owner_actor)
            result["reviewer"] = self._reviewer_identity(record, binding)
            return result
        if receipt is not None and status == "failed":
            record["state"] = "needs_reconciliation"
            record["error"] = "review-turn-failed"
            self.store.save_session_round(record)
            return {"status": "needs-reconciliation",
                    "round": record["round_id"],
                    "error": "review-turn-failed"}
        if receipt is not None:
            # the prompt is in the chat but its turn has no final answer yet
            record["state"] = "reviewing"
            self.store.save_session_round(record)
            return {"status": "review-pending", "round": record["round_id"]}
        refreshes = int(review.get("refreshes") or 0)
        if refreshes >= 1:
            # Exactly ONE re-delivery: a second blind send could duplicate a
            # review prompt the user already sees.
            record["state"] = "needs_reconciliation"
            record["error"] = "review-delivery-unconfirmed"
            self.store.save_session_round(record)
            return {"status": "needs-reconciliation",
                    "round": record["round_id"],
                    "error": "review-delivery-unconfirmed"}
        record["review"] = {**review, "refreshes": refreshes + 1}
        record["state"] = "review_queued"
        self.store.save_session_round(record)
        return {"status": "review-retried", "round": record["round_id"]}

    @staticmethod
    def _after(record, started_at_ms):
        """True when ``started_at_ms`` is after the round was created."""
        if not isinstance(started_at_ms, (int, float)) or not record:
            return False
        created = record.get("created")
        if not isinstance(created, str):
            return False
        try:
            at = datetime.fromisoformat(created)
        except ValueError:
            return False
        return started_at_ms > int(at.timestamp() * 1000) + 1000

    def _prompt(self, packet) -> str:
        # One JSON object only (the packet): the decision schema is spelled
        # out as fields so a reviewer never has to seek past an example.
        schema_lines = "\n".join(f"  {key}: {value}"
                                 for key, value in _DECISION_SCHEMA.items())
        return ("Review this delivery round. Answer with ONE JSON object whose "
                "keys are exactly:\n" + schema_lines
                + "\n\nzsc.review-packet/v1:\n"
                + json.dumps(packet, ensure_ascii=False))

    def _report_for_prompt(self, text: str) -> str:
        encoded = text.encode("utf-8")
        if len(encoded) <= PROMPT_REPORT_BYTES:
            return text
        head = encoded[:PROMPT_REPORT_BYTES // 2].decode("utf-8", "ignore")
        tail = encoded[-(PROMPT_REPORT_BYTES // 2):].decode("utf-8", "ignore")
        return (head + "\n\n[… middle omitted from the prompt; the FULL report is "
                "at the packet's report.artifact with the packet's report.sha256 "
                "…]\n\n" + tail)

    def _write_report(self, binding_id, round_id, text) -> Path:
        directory = self.report_root / binding_id
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / f"{round_id}.txt"
        path.write_text(text, encoding="utf-8")
        return path

    def _write_prompt(self, record, text, *, suffix="-prompt") -> Path:
        directory = self.report_root / record["binding_id"]
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / f"{record['round_id']}{suffix}.json.txt"
        path.write_text(text, encoding="utf-8")
        return path

    def _advance_binding_cursor(self, binding, cursor):
        from .session_binding import _update_binding
        evidence = dict(binding.get("evidence") or {})
        evidence["last_round_at"] = _now()
        _update_binding(self.store, binding, cursor=dict(cursor),
                        evidence=evidence)

    # ---------------------------------------------------- source resolution

    def _task_record(self, task_id):
        if not isinstance(task_id, str) or not task_id:
            return None
        for root in (self.hub_state_dir / "hub" / "tasks",
                     self.hub_state_dir / "tasks"):
            path = root / f"{task_id}.json"
            try:
                record = json.loads(path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                continue
            if isinstance(record, dict):
                return record
        return None

    def _recorded_task(self, binding, task_id):
        """A round of THIS binding that dispatched ``task_id`` (durable evidence)."""
        for item in reversed(self.store.session_rounds(binding["binding_id"])):
            instruction = item.get("instruction") or {}
            if instruction.get("task_id") == task_id:
                return dict(instruction, round_id=item.get("round_id"))
        return None

    def _session_of(self, task_id):
        record = self._task_record(task_id) or {}
        locator = record.get("session_locator")
        if not isinstance(locator, dict):
            return None, record
        return locator.get("session_id"), record

    def _resolve_source(self, binding, source) -> dict:
        """Derive (or verify) the source from persisted execution evidence."""
        if source is None:
            rounds = [item for item in self.store.session_rounds(binding["binding_id"])
                      if (item.get("instruction") or {}).get("task_id")]
            latest = rounds[-1]["instruction"] if rounds else {}
            handoff = ((binding.get("evidence") or {}).get("handoff") or {})
            task_id = latest.get("task_id") or handoff.get("task_id")
            source = {
                "work_id": latest.get("work_id") or _bound_work(binding),
                "task_id": task_id,
                "attempt_id": latest.get("attempt_id"),
                "design_revision": latest.get("design_revision"),
            }
        else:
            if not isinstance(source, dict):
                raise WorkError("schema-invalid", "source must be an object")
            source = dict(source)
        task_id = source.get("task_id")
        if task_id:
            session_id, record = self._session_of(task_id)
            if session_id is None:
                # no hub task record: the loop's OWN durable round record is
                # the other admissible provenance (it names the attempt the
                # dispatcher ran and the session the worker reported back)
                recorded = self._recorded_task(binding, task_id)
                if recorded is None:
                    raise WorkError(
                        "source-unverified",
                        f"no persisted task record for {task_id!r}; the loop "
                        "only reports deliveries it can point at")
                reported = recorded.get("worker_session_id")
                source.setdefault("attempt_id", recorded.get("attempt_id"))
                source["worker_session_id"] = reported
                source["session_match"] = reported in (None, binding["session_id"])
                source.setdefault("contributors", recorded.get("contributors") or [])
                if not source["session_match"]:
                    return {"status": "mismatch", "error": "session-mismatch",
                            "detail": f"round {recorded.get('round_id')!r} ran in "
                                      f"session {reported!r}, not the bound "
                                      f"{binding['session_id']!r}",
                            "binding_id": binding["binding_id"]}
                return {"status": "ok", "source": source}
            source["worker_session_id"] = session_id
            source["session_match"] = session_id == binding["session_id"]
            source.setdefault("contributors",
                              record.get("contributors") or [])
            if not source["session_match"]:
                return {"status": "mismatch", "error": "session-mismatch",
                        "detail": f"task {task_id!r} resumed session "
                                  f"{session_id!r}, not the bound "
                                  f"{binding['session_id']!r}",
                        "binding_id": binding["binding_id"]}
        # the delivery text was just READ out of the bound session: that
        # session is the worker of record even when no task record exists
        source.setdefault("worker_session_id", binding["session_id"])
        source.setdefault("session_match",
                          source["worker_session_id"] == binding["session_id"])
        source.setdefault("contributors", [])
        return {"status": "ok", "source": source}

    # ------------------------------------------------- decision handling

    @staticmethod
    def _parse_decision(message, record=None):
        if not isinstance(message, str):
            return None, ["reviewer produced no final message"]
        text = message.strip()
        if text.startswith("```"):
            text = text.strip("`").lstrip("json").strip()
        try:
            start, end = text.index("{"), text.rindex("}") + 1
            document = json.loads(text[start:end])
        except (ValueError, json.JSONDecodeError) as error:
            return None, [f"no JSON object found: {error}"]
        problems = []
        if not isinstance(document, dict):
            return None, ["decision is not an object"]
        required = {"schema_version", "binding_id", "round_id", "source_turn_id",
                    "candidate_sha256", "design_revision", "decision",
                    "next_instruction", "required_checks"}
        missing = required - set(document)
        if missing:
            problems.append(f"missing fields: {sorted(missing)}")
        if document.get("schema_version") != "zsc.review-decision/v1":
            problems.append("schema_version must be zsc.review-decision/v1")
        if document.get("decision") not in DECISIONS:
            problems.append(f"decision must be one of {DECISIONS}")
        if document.get("decision") == "answer" \
                and document.get("after_answer") not in ("continue", "wait"):
            problems.append("answer requires after_answer=continue|wait")
        if record is not None:
            for field in ("binding_id", "round_id", "candidate_sha256"):
                if field in document and document[field] != record.get(field):
                    problems.append(
                        f"{field} {document[field]!r} does not match the round's "
                        f"{record.get(field)!r} (stale or forged decision)")
        if problems:
            return None, problems
        return {key: document[key] for key in document if key in _DECISION_FIELDS}, []

    def submit_decision(self, request, actor) -> dict:
        if not isinstance(request, dict):
            raise WorkError("schema-invalid", "decision must be an object")
        decision, problems = self._parse_decision(json.dumps(request))
        if decision is None:
            raise WorkError("decision-invalid", "; ".join(problems))
        actor.require("review:submit")
        round_id = decision["round_id"]
        record = self.store.get_session_round(round_id)
        if record is None:
            raise WorkError("round-missing", f"unknown round {round_id!r}")
        binding = self.store.get_session_binding(record["binding_id"])
        if binding is None:
            raise WorkError("binding-missing", "the round's binding disappeared")
        # R8: the unconfirmed-dispatch gate lives at EVERY send entry, not
        # only advance(). A decision submitted while the previous native
        # delivery is still unaccounted for would launch a second one.
        if self._unconfirmed_dispatch(binding["binding_id"]):
            raise WorkError(
                "dispatch-unconfirmed",
                "a native delivery for this binding is sent but its turn is "
                "not confirmed yet; reconcile it before deciding again")
        # A decision routes into a dispatch or a rejection. If the directory
        # under the binding was replaced, that dispatch would target a
        # workspace nobody reviewed, even when its content looks identical.
        replaced = self._workspace_replaced(binding)
        if replaced is not None:
            record["state"] = "needs_reconciliation"
            record["error"] = f"workspace-replaced: {replaced}"
            self.store.save_session_round(record)
            raise WorkError("workspace-replaced",
                            f"the bound workspace was replaced ({replaced}); "
                            "the review no longer covers this directory")

        # staleness (1): the candidate must be the reviewed one — re-scan
        # against the baseline that framed THIS round's review, not the
        # newest dispatch (a later delivery must not move the comparison).
        reviewed_turn = ((record.get("source") or {}).get("turn") or {}).get(
            "turn_id")
        current = self._candidate(binding, reviewed_turn)
        if not current["sha256"]:
            raise WorkError("candidate-coverage-incomplete",
                            "the candidate scan is incomplete; a drift verdict "
                            "is impossible")
        if decision["candidate_sha256"] != current["sha256"]:
            record["state"] = "needs_reconciliation"
            record["error"] = "candidate drifted after the review"
            self.store.save_session_round(record)
            raise WorkError("decision-expired",
                            "the candidate changed after this review; "
                            "re-review the new state")
        # staleness (2): an INDEPENDENT identity query — a human message (or a
        # new turn) after the review outranks the queued decision
        latest = session_turn_identity(binding)
        reviewed = (record.get("source") or {}).get("turn") or {}
        if latest.get("status") == "located" and latest.get("turn"):
            latest_turn = latest["turn"]
            if latest_turn.get("turn_id") != reviewed.get("turn_id") \
                    or latest_turn.get("message_sequence") \
                    != reviewed.get("message_sequence"):
                record["state"] = "needs_reconciliation"
                record["error"] = "session advanced after the review"
                self.store.save_session_round(record)
                raise WorkError(
                    "decision-expired",
                    "the session's latest turn changed after this review "
                    "(user input wins); re-review before sending")

        # user pause wins over any pending dispatch
        if binding["state"] == "paused" and decision["decision"] in (
                "continue", "rework", "answer") \
                and decision.get("after_answer") != "wait":
            raise WorkError("binding-paused",
                            "the binding is paused; resume before dispatching")
        # scheduling caps
        rounds = self.store.session_rounds(record["binding_id"])
        dispatched = [item for item in rounds if item.get("instruction")]
        if len(dispatched) >= self.max_rounds:
            raise WorkError("round-limit-reached",
                            f"{self.max_rounds} dispatched rounds reached; "
                            "raise the limit explicitly or close the goal")
        same_candidate = [item for item in dispatched
                          if item["candidate_sha256"] == current["sha256"]]
        if len(same_candidate) >= self.max_no_progress - 1:
            raise WorkError("no-progress",
                            f"{self.max_no_progress - 1} dispatched rounds "
                            "produced no candidate change; stopping for "
                            "review instead of looping")

        if decision["decision"] == "done":
            pending = self._prepare_done_verification(record, decision)
            if pending is not None:
                return pending
            self._check_done_authority(record, actor)

        record["decision"] = decision
        route = {
            "continue": self._route_continue,
            "rework": self._route_rework,
            "answer": self._route_answer,
            "need_user": self._route_stop,
            "blocked": self._route_stop,
            "done": self._route_done,
        }[decision["decision"]]
        return route(record, decision, actor)

    def _check_done_authority(self, record, actor):
        source = record["source"] or {}
        worker_session = source.get("worker_session_id")
        if worker_session and actor.session_id == worker_session:
            raise WorkError("worker-cannot-self-accept",
                            "the delivering worker cannot record acceptance "
                            "of its own delivery")
        contributors = {entry.get("session_id")
                        for entry in source.get("contributors") or []
                        if isinstance(entry, dict)}
        reviewer_thread = (record.get("reviewer") or {}).get("thread_id")
        if reviewer_thread and reviewer_thread in contributors:
            raise WorkError("reviewer-conflict",
                            "the review thread contributed to this candidate; "
                            "final independent verification needs another verifier")
        context = self.collaboration.context(source.get("work_id"))
        if context.get("state") != "completed":
            raise WorkError("acceptance-not-recorded",
                            "done requires the work's acceptance to be already "
                            f"recorded (current state {context.get('state')!r}); "
                            "route verification first, or ask the user")
        # The work being completed is not enough: the acceptance must be the
        # one recorded for THIS round's candidate. An acceptance of an older
        # candidate (same work, previous batch) must never close this one.
        accepted = ((context.get("acceptance") or {}).get("candidate_sha256")
                    or (context.get("candidate") or {}).get("sha256"))
        current = record.get("candidate_sha256")
        if not accepted or accepted != current:
            raise WorkError(
                "acceptance-not-recorded",
                f"the recorded acceptance covers candidate {str(accepted)[:12]!r}, "
                f"not this round's {str(current)[:12]!r}; verify and accept the "
                "current candidate")
        design = context.get("design_sha256")
        reviewed = (record.get("source") or {}).get("design_sha256")
        if design and reviewed and design != reviewed:
            raise WorkError(
                "acceptance-not-recorded",
                "the accepted design revision differs from the reviewed one")
        verifier = context.get("verifier") or {}
        verifier_key = verifier.get("session_id") or verifier.get("actor_id")
        if verifier_key and verifier_key in (contributors | {worker_session,
                                                            reviewer_thread}):
            raise WorkError(
                "reviewer-conflict",
                "the recorded verifier contributed to this candidate; final "
                "acceptance needs an independent verifier")

    # ------------------------------------- automatic verification

    def _prepare_done_verification(self, record, decision):
        """Run the work's own verification lifecycle before `done`.

        The reviewer's `done` is not acceptance. When the authorization
        registers a verification stage the owner dispatches it exactly once,
        consumes the worker's real check report on a later tick, and records
        acceptance as the host. A missing stage fails with the field that is
        missing instead of stopping at `acceptance-not-recorded`.
        """
        source = record.get("source") or {}
        work_id = source.get("work_id")
        if not work_id:
            return None
        try:
            context = self.collaboration.context(work_id)
        except WorkError:
            return None
        if context.get("state") == "completed":
            return None
        info = record.get("verification")
        if not isinstance(info, dict) or not info:
            if context.get("state") != "candidate_ready":
                return None
            info = self._start_verification(record, decision, context, work_id)
            # the start_verification lifecycle step bumped the revision; the
            # acceptance below must use the CURRENT context
            context = self.collaboration.context(work_id)
        attempt_id = info.get("attempt_id")
        if not attempt_id:
            raise WorkError(
                "verification-unrecoverable",
                "the saved verification plan has no attempt id")
        try:
            status = self.dispatcher.status(attempt_id)
        except WorkError as error:
            if error.code != "attempt-missing":
                raise
            status = None
        if status is None:
            # a previous tick saved the plan but never committed the enqueue
            # (crash between the two): resume the SAME attempt id once
            info = self._resume_verification(record, work_id)
            status = self.dispatcher.status(info["attempt_id"])
        state = (status or {}).get("status")
        if state in (None, "pending", "running"):
            return {"status": "verification-pending",
                    "round": record["round_id"],
                    "verification": {"attempt_id": attempt_id,
                                     "state": state or "missing"}}
        if state != "completed":
            raise WorkError(
                "verification-failed",
                f"verification dispatch {attempt_id} ended {state!r}; no "
                f"acceptance was recorded (result: "
                f"{str((status or {}).get('result'))[:300]})")
        self._record_verification_acceptance(record, context, status, work_id)
        return None

    def _verification_plan(self, record, decision):
        """Build the verification attempt; nothing is persisted or dispatched."""
        stage = "verification"
        definition = (self.authorization.get("stages") or {}).get(stage)
        if not isinstance(definition, dict):
            raise WorkError(
                "verification-unconfigured",
                "the authorization registers no 'verification' stage; add one "
                "with models, template_version, allowed_files, prompt_template "
                "and command so done can dispatch an independent check")
        binding = self.store.get_session_binding(record["binding_id"])
        if binding is None:
            raise WorkError("binding-missing", "the round's binding disappeared")
        model = self.stage_bindings.get("model")
        models = definition.get("models") or []
        if isinstance(models, list) and models and model not in models:
            model = models[0]
        template_version = definition.get("template_version")
        if not isinstance(template_version, str) or not template_version:
            raise WorkError(
                "verification-unconfigured",
                "the verification stage declares no template_version")
        declared = definition.get("allowed_files")
        allowed = list(declared) if isinstance(declared, list) else []
        verifier = {"actor_id": self.owner_actor.actor_id,
                    "session_id": getattr(self.owner_actor, "session_id", None)}
        # The review decision's required checks keep their identity; the fixed
        # name is only a fallback for a decision that names none.
        raw_required = [item for item in (decision.get("required_checks") or [])
                        if isinstance(item, str) and item]
        # A decision names check IDENTIFIERS. Reviewer commands are requests
        # to run something, not ids a verification report can carry: keep
        # them in the materials, and require the stage's own check when no
        # identifier survives.
        required = [item for item in raw_required if _CHECK_ID.fullmatch(item)]
        if not required:
            required = ["verification"]
        attempt_id = f"verify-{record['round_id'][:20]}"
        materials = {
            "candidate_sha256": record["candidate_sha256"],
            "round_id": record["round_id"],
            "required_checks": required,
            "reviewer_checks": raw_required,
            "instruction": ("Run EXACTLY the command in materials.command with "
                            "the Bash tool, copying it verbatim — do not add a "
                            "cd, a pipe or any other command. Reply with one "
                            "JSON object {\"checks\": ["
                            + ", ".join('{"id": "%s", "status": "passed"}'
                                        % item for item in required)
                            + "]} where "
                            "status is passed only if that command exited 0.")}
        bindings = {
            "workspace": binding["workspace"], "model": model,
            "template_version": template_version,
            "materials": materials,
            "allowed_files": allowed,
            "timeout_sec": self.stage_bindings.get("timeout_sec", 600),
            "concurrency": 1,
            "argv_slots": self.stage_bindings.get("argv_slots") or {},
            # the verification command is an obligation, not a capability
            "require_command": True,
        }
        derived = compile_stage_contract(self.authorization, stage, bindings)
        # The broker approves only the exact registered argv; hand the worker
        # the verbatim command so its Bash call can match that approval.
        argv = (derived.get("command") or {}).get("argv")
        if isinstance(argv, list) and argv:
            materials["command"] = shlex.join(argv)
            derived = compile_stage_contract(self.authorization, stage, bindings)
        return {"stage": stage, "attempt_id": attempt_id,
                "candidate_sha256": record["candidate_sha256"],
                "verifier": verifier, "required_checks": required,
                "status": "starting", "started": _now(), "contract": derived}

    def _start_verification(self, record, decision, context, work_id):
        info = self._verification_plan(record, decision)
        record["verification"] = info
        self.store.save_session_round(record)
        if context.get("state") == "candidate_ready":
            self._apply_lifecycle(context, work_id, "start_verification", {
                "attempt_id": info["attempt_id"],
                "candidate_sha256": record["candidate_sha256"],
                "verifier": info["verifier"],
                "required_checks": info["required_checks"]})
        return self._dispatch_verification(record, work_id)

    def _dispatch_verification(self, record, work_id):
        """Enqueue the persisted plan; the attempt id makes a retry a no-op."""
        info = record["verification"]
        derived = info.get("contract")
        if not isinstance(derived, dict):
            raise WorkError(
                "verification-unrecoverable",
                "the saved verification plan carries no contract to dispatch")
        from .work_runtime import plan_dispatch
        contract_sha256 = plan_dispatch(self.hub_state_dir, derived,
                                        self.profile)["contract_sha256"]
        handle = self.dispatcher.enqueue({
            "schema_version": "zsc.dispatch-intent/v1",
            "work_id": work_id, "attempt_id": info["attempt_id"],
            "contract": derived, "contract_sha256": contract_sha256})
        info.update({
            "attempt_id": handle.attempt_id, "task_id": handle.task_id,
            "intent_id": handle.intent_id, "status": "running",
            "command": derived.get("command"), "enqueued": _now()})
        record["verification"] = info
        self.store.save_session_round(record)
        return info

    def _resume_verification(self, record, work_id):
        """A crash landed between the saved plan and the enqueue.

        The lifecycle step is applied only if it never happened; the enqueue
        reuses the SAME attempt id and contract, so its intent identity makes
        the retry idempotent and never starts a second worker.
        """
        info = record["verification"]
        context = self.collaboration.context(work_id)
        if context.get("state") == "candidate_ready":
            self._apply_lifecycle(context, work_id, "start_verification", {
                "attempt_id": info["attempt_id"],
                "candidate_sha256": info["candidate_sha256"],
                "verifier": info["verifier"],
                "required_checks": info.get("required_checks") or ["verification"]})
        elif context.get("state") != "verification_running":
            raise WorkError(
                "verification-unrecoverable",
                f"the saved verification plan cannot resume from work state "
                f"{context.get('state')!r}")
        info["resumed_at"] = _now()
        return self._dispatch_verification(record, work_id)

    def _record_verification_acceptance(self, record, context, status, work_id):
        info = dict(record["verification"])
        result = status.get("result") or {}
        checks = self._verification_checks(result, info, record)
        context = self.collaboration.context(work_id)
        accepted = self._apply_lifecycle(context, work_id, "record_acceptance", {
            "candidate_sha256": record["candidate_sha256"],
            "verifier": info["verifier"], "dependencies": [],
            "dimensions": {"code": "passed"}, "checks": checks})
        current = self.collaboration.context(work_id)
        self._apply_lifecycle(current, work_id, "complete",
                              {"candidate_sha256": record["candidate_sha256"]})
        info.update({
            "status": "accepted", "accepted_at": _now(),
            "session_id": (result.get("session_locator") or {}).get("session_id"),
            "evidence_dir": result.get("evidence_dir"),
            "outcome": result.get("outcome"), "checks": checks,
            "acceptance_revision": accepted.get("revision")})
        record["verification"] = info
        self.store.save_session_round(record)

    def _verification_checks(self, result, info, record):
        """Turn the WORKER's reported checks into the acceptance evidence.

        A failed/cancelled dispatch, a missing report, or any non-passed
        check never becomes an acceptance: the round stays parked with a
        recorded error instead of hardcoding `passed`.
        """
        if result.get("outcome") != "delivered" \
                or result.get("delivery_ok") is not True:
            raise WorkError(
                "verification-failed",
                "the verification dispatch delivered no result "
                f"(outcome={result.get('outcome')!r}, "
                f"delivery_ok={result.get('delivery_ok')!r}); evidence: "
                f"{result.get('evidence_dir')}")
        locator = result.get("session_locator") or {}
        worker_session = (record.get("source") or {}).get("worker_session_id")
        if worker_session and locator.get("session_id") == worker_session:
            raise WorkError(
                "verification-failed",
                "the verification ran in the delivering worker's session")
        evidence = Path(str(result.get("evidence_dir") or ""))
        run_path = evidence / "run.json"
        try:
            run = json.loads(run_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise WorkError(
                "verification-failed",
                f"the verification evidence is unreadable at {run_path}: {error}") from error
        response = (run.get("completion") or {}).get("response") \
            if isinstance(run, dict) else None
        reported = None
        if isinstance(response, str):
            try:
                reported = json.loads(response)
            except json.JSONDecodeError:
                # tolerate a fenced/annotated reply: the report itself must
                # still be a JSON object with a checks list
                start, end = response.find("{"), response.rfind("}")
                if start >= 0 and end > start:
                    try:
                        reported = json.loads(response[start:end + 1])
                    except json.JSONDecodeError:
                        reported = None
        # R14-A: the check must have actually RUN. A model may explain the
        # result; the registered command's receipt is the evidence. A
        # zero-tool turn that only says "passed" is not acceptance.
        command_id = f"stage-{info.get('stage') or 'verification'}"
        receipt = None
        for entry in run.get("commands") or []:
            if not isinstance(entry, dict) or entry.get("command_id") != command_id:
                continue
            outcome = entry.get("result") or {}
            detail = ((outcome.get("perf") or {}).get("detail") or {}) \
                .get("command") or {}
            if outcome.get("success") is not True \
                    or detail.get("exitCode") != 0 \
                    or detail.get("timedOut") is not False:
                raise WorkError(
                    "verification-failed",
                    f"the registered command {command_id!r} did not complete "
                    f"successfully (exit={detail.get('exitCode')!r}, "
                    f"timed_out={detail.get('timedOut')!r}); evidence: {run_path}")
            receipt = {
                "command_id": command_id,
                "tool_call_id": entry.get("tool_call_id"),
                "exit_code": detail.get("exitCode"),
                "timed_out": detail.get("timedOut"),
                "output": str(outcome.get("content") or "")[:2000],
            }
            break
        if receipt is None:
            raise WorkError(
                "verification-failed",
                f"the verification worker ran no registered command "
                f"({command_id!r}); a self-reported passed is not execution "
                f"evidence: {run_path}")
        checks = reported.get("checks") if isinstance(reported, dict) else None
        if not isinstance(checks, list) or not checks:
            raise WorkError(
                "verification-failed",
                f"the verification worker reported no checks; evidence: {run_path}")
        normalized = []
        for item in checks:
            if not isinstance(item, dict) or not isinstance(item.get("id"), str) \
                    or not item["id"]:
                raise WorkError("verification-failed",
                                "the verification report contains an invalid check")
            if item.get("status") != "passed":
                raise WorkError(
                    "verification-failed",
                    f"verification check {item['id']!r} is {item.get('status')!r}")
            normalized.append({"id": item["id"], "required": False,
                               "status": "passed"})
        reported_ids = {item["id"] for item in normalized}
        required = [item for item in (info.get("required_checks") or [])
                    if isinstance(item, str) and item]
        if not required:
            required = ["verification"]
        missing = [item for item in required if item not in reported_ids]
        if missing:
            raise WorkError(
                "verification-failed",
                f"the verification report omits required check(s) {missing}; "
                f"the command receipt alone does not stand in for them")
        proof = {
            "attempt_id": info.get("attempt_id"),
            "task_id": info.get("task_id"),
            "session_id": locator.get("session_id"),
            "candidate_sha256": record["candidate_sha256"],
            "command": info.get("command"),
            "command_receipt": receipt,
            "evidence_dir": str(evidence),
            "reported": [item for item in normalized if item["id"] not in required],
        }
        required_entries = [{"id": item, "required": True, "status": "passed",
                             "evidence": dict(proof)} for item in required]
        extras = [item for item in normalized if item["id"] not in required]
        return [*required_entries, *extras]

    # ------------------------------------------------ work lifecycle

    def _apply_lifecycle(self, context, work_id, action, payload):
        return self.collaboration.apply_action(
            work_id, action, payload,
            expected_revision=context["revision"],
            idempotency_key=f"loop-{work_id}-{action}-{context['revision']}",
            actor=self.owner_actor)

    def _note_implementation_start(self, record, binding, work_id, attempt_id):
        """Keep the work's own lifecycle in step with the round we dispatch.

        Only states that may legitimately start an implementation attempt are
        moved. A work parked in `created` (no accepted design yet) belongs to
        the host, so the round records why it was left alone instead of
        pretending the work advanced.
        """
        note = record.setdefault("collaboration", {})
        try:
            context = self.collaboration.context(work_id)
            state = context.get("state")
            design = (context.get("design") or {}).get("sha256")
            if not design:
                note["dispatch"] = "no accepted design; lifecycle stays host-owned"
                return
            if state == "candidate_ready":
                self._apply_lifecycle(context, work_id, "rework", {
                    "reason": "review requested another round",
                    "baseline": "B1", "resume_phase": "implementation"})
                context = self.collaboration.context(work_id)
                state = context.get("state")
            if state in ("design_ready", "rework_required"):
                self._apply_lifecycle(context, work_id, "start_implementation", {
                    "attempt_id": attempt_id, "design_sha256": design,
                    "workspace": binding["workspace"],
                    "binding_ref": binding["binding_id"],
                    "allowed_files": list(self.stage_bindings.get("allowed_files") or []),
                    "base_manifest": {"schema_version": "zsc.source-manifest/v1"}})
                context = self.collaboration.context(work_id)
                self._apply_lifecycle(context, work_id, "implementation_running",
                                      {"attempt_id": attempt_id})
                note["dispatch"] = "implementation_running"
            else:
                note["dispatch"] = f"lifecycle left at {state!r}"
        except WorkError as error:
            note["dispatch_error"] = f"{error.code}: {error.message}"

    def _note_delivery(self, record, work_id, candidate, source):
        """Record the reviewed candidate as the work's delivery, when the work
        is really in an implementation attempt."""
        note = record.setdefault("collaboration", {})
        try:
            context = self.collaboration.context(work_id)
            if context.get("state") != "implementation_running":
                note["delivery"] = f"work is {context.get('state')!r}"
                return
            # The dispatch that DELIVERED this round's source turn owns the
            # baseline: after a crash between the send and the receipt the
            # round's own instruction is still None, and skipping the native
            # check there would let the delivery escape its attempt.
            dispatch = self._dispatch_for_turn(record)
            if dispatch is None and record.get("binding_id"):
                pending = any(
                    (other.get("dispatch") or {}).get("delivery_id")
                    and not (other.get("dispatch") or {}).get("turn_id")
                    for other in self.store.session_rounds(
                        record["binding_id"]))
                if pending:
                    note["delivery_error"] = (
                        "dispatch-unconfirmed: a native delivery was sent but "
                        "its turn was never recorded; reconcile before "
                        "accepting a delivery")
                    return
            native = bool(dispatch) or bool(
                (record.get("instruction") or {}).get("native"))
            frozen = dispatch or (record.get("dispatch") if native else None)
            if native:
                manifest = (frozen or {}).get("base_manifest")
                if not isinstance(manifest, dict) or not manifest.get("sha256"):
                    note["delivery_error"] = (
                        "baseline-missing: the native dispatch froze no "
                        "baseline for this delivery")
                    return
            if candidate.get("coverage") != "complete":
                note["delivery_error"] = (
                    "candidate-coverage-incomplete: a partial scan cannot "
                    "prove what the worker changed")
                return
            if native and candidate.get("baseline_sha256") \
                    != frozen["base_manifest"]["sha256"]:
                note["delivery_error"] = (
                    "baseline-mismatch: the candidate was measured against "
                    f"{candidate.get('baseline_sha256')!r}, not the dispatch "
                    f"baseline {frozen['base_manifest']['sha256']!r}")
                return
            allowed = set(context.get("allowed_files") or
                          (context.get("design") or {}).get("allowed_files") or [])
            changes = candidate.get("changes") or {}
            touched = (set(changes.get("added") or [])
                       | set(changes.get("changed") or [])
                       | set(changes.get("removed") or []))
            # Cumulative write-set check against the CLAIM baseline: a file
            # that was already out of scope in round 1 must not disappear
            # into the next round's frozen snapshot.
            binding = (self.store.get_session_binding(record["binding_id"])
                       if record.get("binding_id") else None)
            claim_base = ((binding or {}).get("evidence") or {}).get("baseline")
            if isinstance(claim_base, dict) and claim_base.get("files") is not None:
                from .session_binding import build_candidate as _scan
                cumulative = _scan(binding["workspace"], baseline=claim_base)
                if cumulative.get("coverage") != "complete":
                    note["delivery_error"] = (
                        "candidate-coverage-incomplete: the cumulative diff "
                        "against the claim baseline is incomplete")
                    return
                cchanges = cumulative.get("changes") or {}
                # UNION, not replacement: the cumulative view catches files
                # that stayed out of scope, the round view catches one that a
                # rework put BACK to its claimed content.
                touched |= (set(cchanges.get("added") or [])
                            | set(cchanges.get("changed") or [])
                            | set(cchanges.get("removed") or []))
            if not allowed:
                note["delivery_error"] = (
                    "allowed-files-unknown: the work records no authorized "
                    "write set, so the candidate cannot be accepted")
                return
            unexpected = sorted(touched - allowed)
            if unexpected:
                note["delivery_error"] = (
                    f"unexpected-writes: the candidate touches {unexpected}, "
                    f"outside the authorized write set {sorted(allowed)}")
                return
            self._apply_lifecycle(context, work_id, "submit_delivery", {
                "status": "delivered",
                "candidate": {"sha256": record["candidate_sha256"]},
                "candidate_manifest": {
                    "sha256": record["candidate_sha256"],
                    "coverage": candidate.get("coverage"),
                    "files": candidate.get("files") or {},
                    "base_manifest_sha256":
                        (frozen or {}).get("base_manifest", {}).get("sha256"),
                    "attempt_id": (frozen or {}).get("base_manifest", {}).get(
                        "attempt_id"),
                },
                "contributors": source.get("contributors") or [],
                "unexpected_writes": unexpected,
                "target_drift": False})
            note["delivery"] = "candidate_ready"
        except WorkError as error:
            note["delivery_error"] = f"{error.code}: {error.message}"

    # ---------------------------------------------------------- routing

    def _native_instruction(self, record, decision, binding):
        """Continue a native session through the configured CLI bridge.

        The receipt is the persisted user message + the turn it started, not
        the bridge's exit code. The delivery id is derived from the round, so
        a restarted owner finds the message instead of sending it twice.
        """
        instruction = decision.get("next_instruction")
        if not isinstance(instruction, str) or not instruction.strip():
            raise WorkError("decision-invalid",
                            "a rework needs a non-empty next_instruction")
        delivery_id = f"zsc-rework-{record['round_id'][:24]}"
        attempt_id = f"native-{record['round_id'][:20]}"
        work_id = (record.get("source") or {}).get("work_id")
        contract = self._verify_native_contract(binding, instruction)
        # The FIRST approved contract of a round is fixed: a retry after a
        # crash cannot swap in a different contract or attempt for the same
        # round, and the frozen baseline travels with that first record.
        existing = record.get("dispatch") or {}
        if existing.get("contract_sha256") and \
                existing["contract_sha256"] != contract.get("contract_sha256"):
            raise WorkError(
                "native-dispatch-mismatch",
                "this round already recorded a different approved contract; "
                "a retry cannot replace it")
        if existing.get("attempt_id") and existing["attempt_id"] != attempt_id:
            raise WorkError(
                "native-dispatch-mismatch",
                f"this round already recorded attempt "
                f"{existing['attempt_id']!r}, not {attempt_id!r}")
        if existing.get("delivery_id"):
            # R8: the first send owns this round. A concurrent decision must
            # not type the same instruction a second time; an interrupted
            # send is settled by _settle_dispatch, never re-delivered.
            return {"status": "instruction-queued", "native": True,
                    "round_id": record["round_id"],
                    "turn_id": existing.get("turn_id"),
                    "channel": existing.get("channel")
                    or self._native_channel(),
                    "delivery_id": existing["delivery_id"]}
        from .session_binding import build_baseline
        if isinstance(existing.get("base_manifest"), dict) and \
                existing["base_manifest"].get("sha256"):
            frozen = existing["base_manifest"]
        else:
            frozen = build_baseline(binding["workspace"])
        if frozen.get("coverage") != "complete":
            raise WorkError(
                "candidate-baseline-incomplete",
                "the dispatch baseline is incomplete; a delivery cannot be "
                f"measured against it ({frozen.get('reason')})")
        if work_id:
            # the work's own lifecycle moves BEFORE the delivery so the
            # round-2 candidate can become ITS delivery, not a side note
            self._note_implementation_start(record, binding, work_id,
                                            attempt_id)
            context = self.collaboration.context(work_id)
            if context.get("state") != "implementation_running":
                raise WorkError(
                    "native-lifecycle-unproven",
                    f"the work is {context.get('state')!r}, not "
                    "implementation_running; a native continuation is only "
                    "delivered after an accepted design put it there")
        # Last gate before anything leaves: the directory that was verified
        # when the contract was checked must still be the same directory.
        reason = self._workspace_replaced(binding)
        if reason:
            raise WorkError("workspace-replaced", reason)
        # Persist the attempt, its frozen baseline and the contract digest
        # BEFORE the session is touched: a crash during the turn resumes the
        # SAME attempt against the SAME snapshot.
        body = (contract or {}).get("prompt") or instruction
        typed = f"{body}\n\n[zsc-native-delivery {delivery_id}]"
        draft = existing or {
            "attempt_id": attempt_id, "delivery_id": delivery_id,
            "session_id": binding["session_id"],
            "channel": self._native_channel(),
            "body_sha256": hashlib.sha256(typed.encode("utf-8")).hexdigest(),
            "expected_model": contract.get("model"),
            "contract_sha256": contract.get("contract_sha256"),
            "source_turn_id": ((record.get("source") or {}).get("turn") or {}
                               ).get("turn_id"),
            "base_manifest": {**frozen, "attempt_id": attempt_id},
            "frozen_at": _now()}
        # The FIRST writer owns the delivery identity for this round. A
        # concurrent decision that races here must reuse it (or fail on a
        # different digest), never overwrite it.
        stored, created = self.store.claim_dispatch(record["round_id"], draft)
        if stored.get("contract_sha256") != draft.get("contract_sha256") \
                or stored.get("attempt_id") != draft.get("attempt_id"):
            raise WorkError(
                "native-dispatch-mismatch",
                "another decision already registered a different dispatch "
                "for this round; the first delivery identity wins")
        if not created:
            # A concurrent decision won the claim; only the creator may type
            # the message. Return BEFORE saving this stale snapshot: writing
            # it would erase the winner's turn_id/completed_at/instruction.
            return {"status": "instruction-queued", "native": True,
                    "round_id": record["round_id"],
                    "turn_id": stored.get("turn_id"),
                    "channel": stored.get("channel") or self._native_channel(),
                    "delivery_id": stored["delivery_id"]}
        record["dispatch"] = stored
        self.store.save_session_round(record)
        receipt = self._native_deliver(
            binding, (contract or {}).get("prompt") or instruction, delivery_id,
            timeout_s=(contract or {}).get("timeout_sec"),
            contract=contract, attempt_id=attempt_id)
        # The receipt must belong to THIS attempt, THIS session and THIS
        # contract; a receipt from another delivery proves nothing.
        if receipt.get("session_id") != binding["session_id"]:
            raise WorkError(
                "native-receipt-mismatch",
                f"the receipt names session {receipt.get('session_id')!r}, "
                f"not the bound {binding['session_id']!r}")
        approved = (contract or {}).get("contract_sha256")
        if not approved:
            raise WorkError(
                "native-contract-mismatch",
                "the round carries no digest of the contract it approved")
        # Compare against the record that was PERSISTED before the send, not
        # against the request the bridge just echoed back: the durable row is
        # the only copy a restart (or another round) cannot rewrite.
        persisted = (self.store.get_session_round(record["round_id"]) or {}
                     ).get("dispatch") or {}
        if not persisted:
            raise WorkError(
                "native-dispatch-unrecorded",
                "the pre-send dispatch record did not survive persistence")
        if persisted.get("contract_sha256") != approved:
            raise WorkError(
                "native-dispatch-mismatch",
                "the persisted dispatch approved a different contract")
        if receipt.get("contract_sha256") != persisted.get("contract_sha256"):
            raise WorkError(
                "native-receipt-mismatch",
                "the receipt does not carry the contract this round approved")
        if receipt.get("attempt_id") != persisted.get("attempt_id"):
            raise WorkError(
                "native-receipt-mismatch",
                f"the receipt belongs to attempt "
                f"{receipt.get('attempt_id')!r}, not "
                f"{persisted.get('attempt_id')!r}")
        if str(receipt.get("model") or "").lower() != str(
                (contract or {}).get("model") or "").lower():
            raise WorkError(
                "native-receipt-mismatch",
                "the delivered turn ran a different model than the contract")
        if not receipt.get("delivered"):
            raise WorkError(
                "native-delivery-incomplete",
                f"the native session has not finished the delivered turn yet "
                f"({receipt.get('status')}; delivery {delivery_id})")
        record["dispatch"] = {**(record.get("dispatch") or {}),
                              "turn_id": receipt.get("turn_id"),
                              "completed_at": _now()}
        record["instruction"] = {
            "attempt_id": f"native-{record['round_id'][:20]}",
            "task_id": receipt.get("turn_id"), "native": True,
            "delivery_id": delivery_id, "delivery": receipt,
            "instruction": instruction,
            "channel": self._native_channel(),
            "worker_session_id": binding["session_id"],
            "contract": contract,
            "contract_sha256": (contract or {}).get("contract_sha256"),
            "base_manifest": frozen,
        }
        record["state"] = "instruction_queued"
        self.store.save_session_round(record)
        return {"status": "instruction-queued", "native": True,
                "round_id": record["round_id"],
                "turn_id": receipt.get("turn_id"),
                "channel": self._native_channel(),
                "delivery_id": delivery_id}

    def _native_channel(self):
        return "cli-resume" if self.native_bridge is not None else "gui"

    def _verify_native_contract(self, binding, instruction):
        """The native instruction is still a contract the authorization owns.

        A GUI/CLI delivery bypasses the dispatcher, so nothing else would
        compile the stage contract; without this the bridge could type any
        prompt into the session the claim authorized.
        """
        from .contract_compile import (compile_stage_contract,
                                       verify_against_authorization)
        from .session_binding import session_model
        stage = self.stage_bindings.get("stage", "implementation")
        claim = (binding.get("evidence") or {}).get("native_claim") or {}
        recorded = session_model(
            binding["session_id"],
            binding.get("db_path") or claim.get("db_path")
            or (binding.get("evidence") or {}).get("db_path"))
        model = recorded.get("model")
        if not isinstance(model, str) or not model.strip():
            raise WorkError(
                "native-model-unknown",
                "the session records no model; a continuation cannot claim "
                "the contract's model is the one it will run")
        allow = {str(item).lower()
                 for item in (self.authorization.get("model_allowlist") or [])}
        forms = {model.lower()}
        if recorded.get("provider"):
            forms.add(f"{recorded['provider']}/{model}".lower())
        if not (forms & allow):
            raise WorkError(
                "model-not-authorized",
                f"the session runs {model!r}, outside this authorization's "
                f"allowlist {sorted(allow)}; a contract naming another model "
                "cannot be enforced by a resumed session")
        bindings = {**self.stage_bindings,
                    "workspace": binding["workspace"],
                    "model": model,
                    "materials": {"instruction": instruction,
                                  "binding_id": binding.get("binding_id")}}
        derived = compile_stage_contract(self.authorization, stage, bindings)
        verify_against_authorization(self.authorization, derived)
        declared = (binding.get("authorization_ref")
                    or (binding.get("evidence") or {}).get("authorization_ref")
                    or derived["authorization_sha256"])
        if declared:
            if declared != derived["authorization_sha256"]:
                raise WorkError(
                    "authorization-unverified",
                    "the binding was claimed under authorization "
                    f"{declared!r}, not the one this loop holds "
                    f"({derived['authorization_sha256']!r})")
            root = self.authorization_root
            if root is None and self.hub_state_dir:
                root = Path(self.hub_state_dir) / "objects"
            if root is None:
                raise WorkError(
                    "authorization-unverified",
                    "the binding references a registered authorization object "
                    "but the loop has no artifact root to resolve it from")
            import hashlib as _hashlib
            object_path = (Path(root) / "sha256" / declared[:2] / declared)
            if not object_path.is_file():
                raise WorkError(
                    "authorization-unverified",
                    f"no registered work authorization object {declared[:12]}")
            data = object_path.read_bytes()
            if _hashlib.sha256(data).hexdigest() != declared:
                raise WorkError(
                    "authorization-unverified",
                    f"the authorization object {declared[:12]} does not match "
                    "its digest")
            document = json.loads(data)
            verify_against_authorization(document, derived)
        from .work_contracts import canonical_sha256
        derived = {**derived,
                   "contract_sha256": canonical_sha256(derived)}
        return derived

    def _native_deliver(self, binding, instruction, delivery_id,
                        timeout_s=None, contract=None, attempt_id=None):
        """Deliver through the configured native channel.

        The CLI resume bridge is exact and cheap, so it wins whenever it is
        configured; the GUI bridge is the fallback when only a window exists.
        ``timeout_s`` is the CONTRACT's limit, so the bridge cannot run longer
        than the authorization the round was reviewed under. The CLI bridge
        also receives the contract and its authorization: it re-verifies them
        and sends the compiled prompt, never the raw instruction.
        """
        reason = self._workspace_replaced(binding)
        if reason:
            # The directory must still be the one the contract was verified
            # against; a swap between the check and this call would otherwise
            # deliver into a different tree.
            raise WorkError("workspace-replaced", reason)
        if self.native_bridge is not None:
            return self.native_bridge.deliver(
                binding["session_id"], instruction, delivery_id=delivery_id,
                code_root=binding["workspace"], timeout_s=timeout_s,
                contract=contract, authorization=self.authorization,
                attempt_id=attempt_id,
                expected_model=(contract or {}).get("model"),
                # the identity the OWNER checked, so a swap between its check
                # and the adapter's own stat cannot slip through
                expected_identity=(binding.get("evidence") or {}).get(
                    "workspace_identity"))
        if self.native_gui_bridge is not None:
            return self.native_gui_bridge.deliver(
                binding["session_id"], instruction, delivery_id,
                timeout_s=timeout_s, contract=contract,
                attempt_id=attempt_id)
        raise WorkError(
            "native-session-dispatch-unsupported",
            "this native claim may review and record a decision, but no "
            "native delivery bridge is configured; the session owner must "
            "continue it")

    def _enqueue_instruction(self, record, decision, *, instruction_extra,
                             work_id=None):
        """Compile + enqueue the next round back into the ORIGINAL session.

        A control binding may ONLY continue the session it was bound to: the
        attach target is the persisted task record whose session locator
        names that session (the handoff proven at bind time, or the task the
        previous round ran on). Without such a target the loop refuses —
        starting a new session and labelling it "the original" is exactly the
        failure this check exists to prevent.
        """
        source = record["source"]
        binding = self.store.get_session_binding(record["binding_id"])
        if binding is None:
            raise WorkError("binding-missing", "the round's binding disappeared")
        if "control" not in binding.get("capabilities", []):
            if "review" in binding.get("capabilities", []) \
                    and (self.native_bridge is not None
                         or self.native_gui_bridge is not None):
                return self._native_instruction(record, decision, binding)
            if "review" in binding.get("capabilities", []):
                raise WorkError(
                    "native-session-dispatch-unsupported",
                    "this native claim may review and record a decision, but "
                    "no native delivery bridge is configured; the session "
                    "owner must continue it")
            raise WorkError("observe-only",
                            "an observe-only binding never dispatches; bind "
                            "with control (which requires the handoff proof)")
        handoff = ((binding.get("evidence") or {}).get("handoff") or {})
        attach_to = source.get("task_id") or handoff.get("task_id")
        if not attach_to:
            raise WorkError(
                "native-session-control-unproven",
                "no hub task record resumes this session, so the instruction "
                "cannot be attached to it; a control binding must not start a "
                "new session")
        session_id, task_record = self._session_of(attach_to)
        if session_id != binding["session_id"]:
            raise WorkError(
                "session-mismatch",
                f"task {attach_to!r} resumes session {session_id!r}, not the "
                f"bound {binding['session_id']!r}; refusing to dispatch")
        if task_record.get("worker") != "zcode":
            raise WorkError(
                "session-mismatch",
                f"task {attach_to!r} ran on worker {task_record.get('worker')!r}; "
                "attach only resumes a zcode session")
        materials = {
            "instruction": decision.get("next_instruction"),
            "round_id": record["round_id"],
            "binding_id": record["binding_id"],
            **instruction_extra,
        }
        bindings = {**self.stage_bindings, "materials": materials,
                    "workspace": binding["workspace"]}
        derived = compile_stage_contract(
            self.authorization,
            self.stage_bindings.get("stage", "implementation"), bindings)
        derived["attach_to"] = attach_to
        attempt_id = f"loop-{record['round_id'][:20]}"
        from .work_runtime import plan_dispatch
        contract_sha256 = plan_dispatch(self.hub_state_dir, derived,
                                        self.profile)["contract_sha256"]
        intent = {"schema_version": "zsc.dispatch-intent/v1",
                  "work_id": work_id or source["work_id"],
                  "attempt_id": attempt_id,
                  "contract": derived, "contract_sha256": contract_sha256}
        self._note_implementation_start(record, binding, intent["work_id"],
                                        attempt_id)
        handle = self.dispatcher.enqueue(intent)
        instruction = {
            "schema_version": "zsc.session-instruction/v1",
            "binding_id": record["binding_id"], "round_id": record["round_id"],
            "session_id": binding["session_id"],
            "source_task_id": source.get("task_id"),
            "attach_to": attach_to,
            "candidate_sha256": record["candidate_sha256"],
            "design_revision": record["design_revision"],
            "work_id": intent["work_id"],
            "attempt_id": attempt_id, "task_id": handle.task_id,
            "intent_id": handle.intent_id,
            "decision": decision["decision"],
            "next_instruction": decision.get("next_instruction"),
        }
        record["instruction"] = instruction
        record["state"] = "instruction_queued"
        self.store.save_session_round(record)
        return {"status": "instruction-queued", "instruction": instruction}

    def _route_continue(self, record, decision, actor):
        source = record["source"]
        context = self.collaboration.context(source.get("work_id"))
        work_id = source.get("work_id")
        if context.get("state") == "completed":
            # the batch is CLOSED and stays closed: the next batch reuses
            # the binding, importing the accepted design/candidate through
            # the adoption path (no fabricated recon, no reopened work).
            spec = dict(context["spec"])
            spec["title"] = f"{spec['title']} · batch {record['seq'] + 1}"
            created = self.collaboration.create_work(
                spec, idempotency_key=f"loop-next-{record['round_id']}",
                actor=actor)
            work_id = created["work_id"]
            revision = self.collaboration.apply_action(
                work_id, "start_adoption",
                {"source": {"binding_id": record["binding_id"],
                            "round_id": record["round_id"]}},
                expected_revision=created["revision"],
                idempotency_key=f"adopt-{record['round_id']}", actor=actor)["revision"]
            self.collaboration.apply_action(
                work_id, "adopt_candidate",
                {"design": context.get("design") or {},
                 "candidate": context.get("candidate") or {},
                 "contributors": (context.get("contributors")
                                  or record["source"].get("contributors")
                                  or _recorded_contributors(record["source"])),
                 "source": {"binding_id": record["binding_id"],
                            "round_id": record["round_id"]}},
                expected_revision=revision,
                idempotency_key=f"adopt-candidate-{record['round_id']}", actor=actor)
        result = self._enqueue_instruction(
            record, decision, instruction_extra={"mode": "continue"},
            work_id=work_id)
        result["work_id"] = work_id
        return result

    def _route_rework(self, record, decision, actor):
        return self._enqueue_instruction(
            record, decision,
            instruction_extra={"mode": "rework", "findings": decision.get("findings")})

    def _route_answer(self, record, decision, actor):
        if decision.get("after_answer") == "wait":
            record["state"] = "settled"
            self.store.save_session_round(record)
            return {"status": "answered-wait", "round": record["round_id"]}
        return self._enqueue_instruction(
            record, decision,
            instruction_extra={"mode": "answer",
                               "question_answers": decision.get("question_answers")})

    def _route_done(self, record, decision, actor):
        record["state"] = "settled"
        self.store.save_session_round(record)
        return {"status": "done", "round": record["round_id"],
                "required_checks": decision.get("required_checks")}

    def _route_stop(self, record, decision, actor):
        record["state"] = decision["decision"]  # need_user | blocked
        self.store.save_session_round(record)
        return {"status": decision["decision"], "round": record["round_id"]}

    # ---------------------------------------------------------- candidate

    def _workspace_replaced(self, binding) -> str | None:
        """Return a reason when the bound directory is no longer the same one.

        The identity is the (device, inode) pair captured at bind/claim time.
        A deleted-and-recreated path is a different directory even though the
        string is the same; reviewing its files would attribute another
        workspace's candidate to this session.
        """
        from .session_binding import workspace_identity
        recorded = (binding.get("evidence") or {}).get("workspace_identity")
        if not isinstance(recorded, dict) or not recorded.get("ino"):
            return ("the binding records no workspace identity; a delivery "
                    "cannot prove it still points at the reviewed directory")
        try:
            current = workspace_identity(binding["workspace"])
        except WorkError as error:
            return f"{error.code}: {error.message}"
        moved = (current.get("dev") != recorded.get("dev")
                 or current.get("ino") != recorded.get("ino"))
        if moved:
            return (f"the bound workspace {binding['workspace']!r} was replaced "
                    f"(dev/ino {recorded.get('dev')}/{recorded.get('ino')} -> "
                    f"{current.get('dev')}/{current.get('ino')}); refusing to "
                    f"review or dispatch a different directory at the same path")
        return None

    def _candidate(self, binding, turn_id=None) -> dict:
        """The candidate measured against the DISPATCH baseline.

        The claim-time baseline proves what existed before the work started;
        every native continuation freezes a new baseline at the moment of
        dispatch, and the next candidate is measured against THAT snapshot.
        Falling back to the claim baseline would let a file the worker changed
        between rounds disappear into the baseline.
        """
        baseline = None
        for record in reversed(self.store.session_rounds(binding["binding_id"])):
            dispatch = record.get("dispatch") or {}
            frozen = dispatch.get("base_manifest")
            if not (isinstance(frozen, dict) and frozen.get("sha256")):
                frozen = (record.get("instruction") or {}).get("base_manifest")
            if not (isinstance(frozen, dict) and frozen.get("sha256")):
                continue
            if turn_id is not None:
                delivered = (dispatch.get("turn_id")
                             or (record.get("instruction") or {}).get("task_id"))
                if delivered != turn_id:
                    continue
            baseline = frozen
            break
        if baseline is None:
            baseline = (binding.get("evidence") or {}).get("baseline")
        candidate = build_candidate(
            binding["workspace"], baseline=baseline,
            candidate_root=self.hub_state_dir / "candidates")
        if isinstance(baseline, dict) and baseline.get("sha256"):
            candidate["baseline_sha256"] = baseline["sha256"]
        return candidate

    def _settle_dispatch(self, binding):
        """Give a sent-but-unrecorded delivery its turn, once the bridge can
        prove where it landed. Returns True when a pending dispatch settled."""
        pending = []
        for record in self.store.session_rounds(binding["binding_id"]):
            dispatch = record.get("dispatch") or {}
            if dispatch.get("delivery_id") and not dispatch.get("turn_id"):
                pending.append(record)
        if not pending:
            return False
        if self.native_bridge is None and self.native_gui_bridge is None:
            return False
        for record in reversed(pending):
            dispatch = dict(record["dispatch"])
            bridge = (self.native_gui_bridge
                      if dispatch.get("channel") == "gui" else self.native_bridge)
            if bridge is None:
                continue
            try:
                found = bridge.find(
                    binding["session_id"], dispatch["delivery_id"],
                    expected_sha256=dispatch.get("body_sha256"))
            except WorkError:
                continue
            if not isinstance(found, dict) or not found.get("turn_id"):
                continue
            # The recovered receipt must prove the SAME delivery the first
            # receipt would: the exact typed body, a completed turn, and the
            # contract's model. A mention of the id or another model is not it.
            text = found.get("user_text") or found.get("text")
            if not dispatch.get("body_sha256") or not \
                    dispatch.get("expected_model"):
                # A pre-digest dispatch cannot be settled safely: the two
                # checks below would be skipped and any later turn would do.
                continue
            if text is None:
                continue
            if hashlib.sha256(str(text).encode("utf-8")).hexdigest() != \
                    dispatch["body_sha256"]:
                continue
            try:
                state = bridge.turn_state(binding["session_id"],
                                          found["turn_id"])
            except WorkError:
                continue
            if not state.get("completed") or state.get("failed"):
                continue
            expected = dispatch.get("expected_model")
            if expected and str(state.get("model") or "").lower() != \
                    str(expected).lower():
                continue
            # Re-read the row before writing the settle result: the caller's
            # snapshot may predate a concurrent completion.
            record = self.store.get_session_round(record["round_id"])
            dispatch = self.store.update_dispatch(
                record["round_id"], turn_id=found["turn_id"],
                completed_at=found.get("completed_at") or _now())
            record["dispatch"] = dispatch
            record["instruction"] = {
                "attempt_id": dispatch.get("attempt_id"),
                "task_id": found["turn_id"], "native": True,
                "delivery_id": dispatch["delivery_id"],
                "delivery": found, "channel": self._native_channel(),
                "worker_session_id": binding["session_id"]}
            self.store.save_session_round(record)
            return True
        return False

    def _unconfirmed_dispatch(self, binding_id):
        """A dispatched delivery whose turn was never recorded."""
        for record in self.store.session_rounds(binding_id):
            dispatch = record.get("dispatch") or {}
            if dispatch.get("delivery_id") and not dispatch.get("turn_id"):
                return True
        return False

    def _dispatch_for_turn(self, record):
        """The dispatch whose delivered turn IS this round's source turn."""
        turn_id = ((record.get("source") or {}).get("turn") or {}).get("turn_id")
        binding_id = record.get("binding_id")
        if not turn_id or not binding_id:
            return None
        for other in reversed(self.store.session_rounds(binding_id)):
            dispatch = other.get("dispatch") or {}
            if dispatch.get("turn_id") == turn_id:
                return dispatch
        return None

    def _last_dispatch_turn(self, binding_id):
        """The turn the newest native delivery started, if any."""
        for record in reversed(self.store.session_rounds(binding_id)):
            dispatch = record.get("dispatch") or {}
            if dispatch.get("turn_id"):
                return dispatch["turn_id"]
        return None

    # ------------------------------------------------------------ adoption

    def adopt_existing_work(self, request, actor) -> dict:
        """Import an ALREADY-EXISTING delivery: created → adoption_pending →
        candidate_ready once design/candidate/contributors are complete."""
        actor.require("work:create")
        if not isinstance(request, dict) or not isinstance(request.get("spec"), dict):
            raise WorkError("schema-invalid", "adoption requires an explicit work spec")
        source = request.get("source")
        if not isinstance(source, dict) or not source.get("binding_id"):
            raise WorkError("schema-invalid",
                            "adoption requires source refs (binding_id)")
        created = self.collaboration.create_work(
            request["spec"], idempotency_key=source.get("idempotency_key")
            or f"adopt-{source.get('binding_id')}-{source.get('round_id', 'x')}",
            actor=actor)
        work_id = created["work_id"]
        revision = self.collaboration.apply_action(
            work_id, "start_adoption", {"source": source},
            expected_revision=created["revision"],
            idempotency_key=f"start-adoption-{work_id}", actor=actor)["revision"]
        design = request.get("design")
        candidate = request.get("candidate")
        contributors = request.get("contributors")
        if not (isinstance(design, dict) and design.get("sha256")
                and isinstance(candidate, dict) and candidate.get("sha256")
                and isinstance(contributors, list) and contributors):
            return {"work_id": work_id, "state": "adoption_pending",
                    "missing": [name for name, ok in (
                        ("design", isinstance(design, dict) and design.get("sha256")),
                        ("candidate", isinstance(candidate, dict) and candidate.get("sha256")),
                        ("contributors", isinstance(contributors, list) and contributors))
                        if not ok]}
        self.collaboration.apply_action(
            work_id, "adopt_candidate",
            {"design": design, "candidate": candidate, "source": source,
             "contributors": contributors},
            expected_revision=revision,
            idempotency_key=f"adopt-candidate-{work_id}", actor=actor)
        return {"work_id": work_id, "state": "candidate_ready"}


class SessionPump:
    """Owner-side watcher: turns real completion events into rounds.

    No external ``advance`` call is needed. Each tick reads the session's
    authoritative turn identity (a read-only DB query, never a model call);
    a terminal turn that has not been pumped yet is advanced exactly once.
    Failures are recorded on the binding instead of being lost, and a paused
    binding is never pumped. ``session_loop`` may be a callable provider so
    the loop is built lazily (a misconfigured runner cannot crash the owner
    at startup).
    """

    def __init__(self, session_loop, store, *, interval_s=1.0, logger=None):
        self._loop_provider = session_loop
        self.store = store
        self.interval_s = interval_s
        self.logger = logger
        self._stop = threading.Event()
        self._thread = None

    @property
    def loop(self):
        return self._loop_provider() if callable(self._loop_provider) \
            else self._loop_provider

    def start(self):
        if self._thread is not None:
            return
        self._thread = threading.Thread(target=self._run, name="zsc-session-pump",
                                        daemon=True)
        self._thread.start()

    def stop(self, timeout=5.0):
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout)

    def _run(self):
        while not self._stop.wait(self.interval_s):
            try:
                self.tick()
            except Exception as error:  # a watcher must never kill the owner
                if self.logger is not None:
                    self.logger(f"session pump tick failed: {error}")

    def tick(self) -> list[dict]:
        results = []
        for binding in self._active_bindings():
            result = self.pump_once(binding)
            if result is not None:
                results.append(result)
        return results

    def _active_bindings(self) -> list[dict]:
        seen = {}
        for session_id in self._sessions_with_bindings():
            for binding in self.store.session_bindings(session_id):
                if binding["state"] == "bound" \
                        and ({"control", "review"}
                             & set(binding.get("capabilities", []))):
                    seen[binding["binding_id"]] = binding
        return list(seen.values())

    def _sessions_with_bindings(self):
        rows = self.store._connection.execute(  # read-only listing
            "SELECT DISTINCT session_id FROM session_bindings").fetchall()
        return [row[0] for row in rows]

    POLL_INTERVAL_S = 5.0

    def _poll_due(self, record) -> bool:
        """A review that is still answering is polled, but not on every tick:
        the persisted last_poll bounds how often the host is asked."""
        stamp = (record.get("review") or {}).get("last_poll")
        if not stamp:
            return True
        try:
            at = datetime.fromisoformat(stamp)
        except (TypeError, ValueError):
            return True
        return (datetime.now(timezone.utc) - at).total_seconds() \
            >= self.POLL_INTERVAL_S

    def pump_once(self, binding) -> dict | None:
        mismatch = self._check_dispatched_attempt(binding)
        if mismatch is not None:
            return mismatch
        identity = session_turn_identity(binding)
        if identity.get("status") != "located" or not identity.get("turn"):
            return None
        if identity.get("session_state") not in TERMINAL_SESSION_STATES:
            return None
        turn = identity["turn"]
        evidence = dict(binding.get("evidence") or {})
        pumped = evidence.get("pumped_turn") or {}
        if pumped.get("turn_id") == turn.get("turn_id") \
                and pumped.get("message_sequence") == turn.get("message_sequence"):
            # A round parked in review_queued still needs its (same-id) retry
            # even though the turn was marked pumped: otherwise a restart in
            # the retry window leaves it stuck forever.
            pending = [item for item in
                       self.store.session_rounds(binding["binding_id"])
                       if (item.get("state") in ("review_queued",
                                                 "needs_reconciliation")
                           or (item.get("state") == "reviewing"
                               and self._poll_due(item)))
                       and ((item.get("source") or {}).get("turn") or {}).get(
                           "turn_id") == turn.get("turn_id")]
            if not pending:
                return None
        # A review that is between polls is not advanced on every tick: the
        # persisted last_poll is the throttle, whether or not the turn was
        # already marked pumped (a restart writes no pumped mark).
        reviewing = [item for item in
                     self.store.session_rounds(binding["binding_id"])
                     if item.get("state") == "reviewing"
                     and ((item.get("source") or {}).get("turn") or {}).get(
                         "turn_id") == turn.get("turn_id")]
        if reviewing and not self._poll_due(reviewing[-1]):
            return None
        last_error = evidence.get("last_error") or {}
        if last_error.get("turn_id") == turn.get("turn_id") \
                and self._recent(last_error.get("at"), RETRY_BACKOFF_S):
            # A turn that failed (unparseable decision, incomplete candidate)
            # is not retried on every tick; the failure stays visible.
            return None
        try:
            result = self.loop.advance(binding["binding_id"])
        except WorkError as error:
            self._record_binding_error(
                binding, error.code, getattr(error, "message", str(error)),
                turn=turn)
            return {"binding_id": binding["binding_id"], "status": "error",
                    "error": error.code}
        except Exception as error:  # noqa: BLE001 - a watcher fault must land
            # on the binding record, never vanish into a log line
            self._record_binding_error(
                binding, "round-failed", f"{type(error).__name__}: {error}",
                turn=turn)
            return {"binding_id": binding["binding_id"], "status": "error",
                    "error": "round-failed"}
        if result.get("status") == "verification-pending":
            # The owner dispatched (or is polling) the work's own independent
            # verification. This is progress, not a failure: no error and no
            # pumped mark — the next tick reads the result and records
            # acceptance before the done route runs.
            return {"binding_id": binding["binding_id"], **result}
        if result.get("status") in ("review-pending", "review-retried"):
            # The review is still answering, or its submission never landed:
            # retry the SAME id on the next tick. Marking the turn pumped here
            # would make the pump skip the round forever once the host
            # finishes, so neither outcome writes pumped_turn.
            return {"binding_id": binding["binding_id"], **result}
        if result.get("status") == "already-observed":
            # Nothing new happened for this turn; keep any recorded failure
            # visible instead of clearing it on a no-op tick.
            return {"binding_id": binding["binding_id"], **result}
        if result.get("status") in ("observed", "blocked", "error"):
            self._record_binding_error(binding, result.get("status", "blocked"),
                                       (result.get("detail")
                                        or result.get("error") or "no round"),
                                       turn=turn)
            return {"binding_id": binding["binding_id"], **result}
        from .session_binding import _update_binding
        # re-read: advance() moved the cursor AND record_reviewer persisted
        # the review thread on this binding. Merging into the tick's stale
        # snapshot would undo both.
        current = self.store.get_session_binding(binding["binding_id"]) or binding
        evidence = dict(current.get("evidence") or {})
        evidence["pumped_turn"] = turn
        evidence.pop("last_error", None)
        _update_binding(self.store, current, evidence=evidence)
        return {"binding_id": binding["binding_id"], **result}

    @staticmethod
    def _recent(stamp, seconds) -> bool:
        if not isinstance(stamp, str):
            return False
        try:
            then = datetime.fromisoformat(stamp)
        except ValueError:
            return False
        return (datetime.now(timezone.utc) - then).total_seconds() < seconds

    def _record_binding_error(self, binding, code, detail, *, turn=None):
        from .session_binding import _update_binding
        # re-read: the tick's snapshot predates any cursor/round advance
        current = self.store.get_session_binding(binding["binding_id"]) or binding
        evidence = dict(current.get("evidence") or {})
        evidence["last_error"] = {"code": code, "message": str(detail)[:300],
                                  "at": _now(),
                                  "turn_id": (turn or {}).get("turn_id")}
        _update_binding(self.store, current, evidence=evidence)

    def _check_dispatched_attempt(self, binding) -> dict | None:
        """Verify the worker's REAL session after dispatch.

        The instruction claims the bound session; the attempt result must
        prove it (a different worker session is a mismatch, not a delivery).
        """
        for item in reversed(self.store.session_rounds(binding["binding_id"])):
            instruction = item.get("instruction") or {}
            if instruction.get("native"):
                # the native delivery already produced its persisted turn;
                # there is no dispatcher attempt to poll
                continue
            attempt_id = instruction.get("attempt_id")
            if not attempt_id or item.get("state") not in ("instruction_queued",
                                                           "worker_running"):
                continue
            try:
                status = self.loop.dispatcher.status(attempt_id)
            except WorkError as error:
                if error.code == "attempt-missing":
                    continue
                raise
            if status.get("status") not in ("completed", "failed"):
                return None
            result = status.get("result") or {}
            locator = result.get("session_locator") or {}
            actual = locator.get("session_id") or locator.get("thread_id")
            if actual and actual != binding["session_id"]:
                item["state"] = "needs_reconciliation"
                item["error"] = (f"session-mismatch: the worker replied in "
                                 f"{actual!r}, not the bound "
                                 f"{binding['session_id']!r}")
                self.store.save_session_round(item)
                return {"binding_id": binding["binding_id"],
                        "status": "needs_reconciliation",
                        "error": "session-mismatch", "actual_session": actual}
            if item.get("state") == "instruction_queued":
                item["state"] = "worker_running"
                item["instruction"] = {**instruction,
                                       "worker_session_id": actual,
                                       "execution_status": status.get("status")}
                self.store.save_session_round(item)
            return None
        return None
