"""P0.4 UI event projection contracts (PLAN §4.2/§4.3, in-process sideband).

Covers the versioned v1 envelope, per-stream sequence and UTC/monotonic time
discipline, fail-closed initiator resolution, accepted-before-announced
ordering, current-task notification attribution (sequential and interleaved),
emit-failure transparency, the P0.3 exactly-once process-exit projection, and
recursive no-leak guarantees against malicious prompt/params/initiator
samples. No model, real CLI, network, journal or socket is involved: this is
the in-process projection contract only (journal/UDS are P1.1, UI is P1.2).

New-module imports stay INSIDE tests so the RED run against unchanged
production separates behavioural failures (notification isolation on existing
symbols) from new-symbol import errors.
"""

import asyncio
import json
import os
import re
import signal
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from zcode_supervisor import contracts
from zcode_supervisor.authority import Authority, inventory
from zcode_supervisor.codex_appserver import (
    CodexAppServerClient,
    CodexAppServerDriver,
    SimpleContractAuthority,
)
from zcode_supervisor.worker_health import ExitProjection
from zcode_supervisor.zcode_resident import ResidentZCode


def _ui_modules():
    """Import the P0.4 modules lazily (RED: ImportError before implementation)."""
    from zcode_supervisor import ui_events, ui_projection
    return ui_events, ui_projection


# Malicious corpus: every marker below must NEVER appear inside any projected
# event (recursive scan over the serialized envelope), whatever the caller,
# prompt, tool input or initiator claimed.
MARKERS = [
    "PROMPT-MARKER-DO-NOT-LEAK",
    "/Users/evil/workspace/secret-ws",
    "hub-evidence/tasks/evil-task",
    "COOKIE=monster; session=stolen",
    "AWS_SECRET_ACCESS_KEY=sk-123",
    "Traceback (most recent call last)",
    "aGVsbG8gc2VjcmV0IGJhc2U2NCBibG9i",
    "grantRoot=/evil/grant",
    "sess_FORGED_PARENT_9",
    "header-injection-attempt",
    "raw-params-nested-secret",
]

RFC3339_UTC = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z$")


class RegistryFixture:
    """Test-only trusted registry double: exact (agent, session_id) matches.

    Production ships NO registry (the resolver then stays unverified); a real
    trusted cross-worker registry would be injected by the controller, never
    registered from caller input.
    """

    def __init__(self, records, parents=None):
        self.records = dict(records)          # (agent, session_id) -> canonical
        self.parents = dict(parents or {})    # canonical -> canonical parent

    def canonical_session(self, agent, session_id):
        return self.records.get((agent, session_id))

    def verified_parent_of(self, canonical_session_id):
        return self.parents.get(canonical_session_id)


# Minimal fake codex app-server (stdio JSONL). Modes via FAKE_UI_APPSERVER_MODE:
# "single" completes its own thread; "dual" completes both threads (plus one
# identity-less notification) when the second turn/start arrives.
FAKE_APPSERVER = '''#!/usr/bin/env python3
import json, os, sys

MODE = os.environ.get("FAKE_UI_APPSERVER_MODE", "single")
SINGLE = "th_ui_single"


def send(frame):
    sys.stdout.write(json.dumps(frame) + "\\n")
    sys.stdout.flush()


def notify(method, params):
    send({"method": method, "params": params})


def complete_turn(thread):
    turn = "turn_" + thread
    notify("item/completed", {"threadId": thread, "turnId": turn,
                              "item": {"type": "commandExecution",
                                       "command": "cat TASK.md", "exitCode": 0,
                                       "status": "completed"}})
    notify("turn/completed", {"threadId": thread,
                              "turn": {"id": turn, "status": "completed"}})


state = {"turns": 0, "fired": False}
for line in sys.stdin:
    frame = json.loads(line)
    method = frame.get("method")
    params = frame.get("params") or {}
    if method == "initialize":
        send({"id": frame["id"], "result": {"codexHome": "/tmp/fake",
                                            "userAgent": "fake-ui/1"}})
    elif method == "thread/start":
        thread = SINGLE
        if MODE in ("dual", "seq"):
            thread = "th_ui_A" if str(params.get("cwd", "")).endswith("wsA") \\
                else "th_ui_B"
        send({"id": frame["id"], "result": {"thread": {"id": thread}}})
    elif method == "turn/start":
        if MODE == "seq":
            thread = params["threadId"]
            send({"id": frame["id"],
                  "result": {"turn": {"id": "turn_" + thread,
                                      "status": "inProgress"}}})
            notify("item/completed", {"threadId": thread,
                                      "turnId": "turn_" + thread,
                                      "item": {"type": "agentMessage",
                                               "text": "done"}})
            notify("turn/completed", {"threadId": thread,
                                      "turn": {"id": "turn_" + thread,
                                               "status": "completed"}})
            notify("orphan/notification", {"free": "no-identity"})
        elif MODE == "dual":
            state["turns"] += 1
            thread = params["threadId"]
            send({"id": frame["id"], "result": {"turn": {"id": "turn_" + thread,
                                                         "status": "inProgress"}}})
            if state["turns"] == 2 and not state["fired"]:
                state["fired"] = True
                notify("orphan/notification", {"free": "no-identity"})
                complete_turn("th_ui_A")
                complete_turn("th_ui_B")
        elif MODE == "amend":
            send({"id": frame["id"],
                  "result": {"turn": {"id": "turn_" + SINGLE,
                                      "status": "inProgress"}}})
            # persistent-variant v2 approval: the wire answer must be the
            # downgraded per-request decline, and the UI must carry the
            # applied deny + the requested variant.
            send({"id": "srv-amend-1",
                  "method": "item/commandExecution/requestApproval",
                  "params": {"threadId": SINGLE, "turnId": "turn_" + SINGLE,
                             "itemId": "item_amend", "startedAtMs": 1,
                             "kind": "command", "cwd": "/ws",
                             "command": "/bin/zsh -lc 'cat TASK.md'",
                             "proposedExecpolicyAmendment": ["cat TASK.md*"],
                             "availableDecisions": ["acceptForSession",
                                                    "decline"]}})
            for line in sys.stdin:
                response = json.loads(line)
                if response.get("id") == "srv-amend-1":
                    break
            notify("item/completed", {"threadId": SINGLE,
                                      "turnId": "turn_" + SINGLE,
                                      "item": {"type": "commandExecution",
                                               "command": "cat TASK.md",
                                               "exitCode": 0,
                                               "status": "completed"}})
            notify("turn/completed", {"threadId": SINGLE,
                                      "turn": {"id": "turn_" + SINGLE,
                                               "status": "completed"}})
            notify("orphan/notification", {"free": "no-identity"})
        else:
            send({"id": frame["id"],
                  "result": {"turn": {"id": "turn_" + SINGLE,
                                      "status": "inProgress"}}})
            notify("item/completed", {"threadId": SINGLE, "turnId": "turn_" + SINGLE,
                                      "item": {"type": "agentMessage",
                                               "text": "done"}})
            notify("turn/completed", {"threadId": SINGLE,
                                      "turn": {"id": "turn_" + SINGLE,
                                               "status": "completed"}})
            notify("orphan/notification", {"free": "no-identity"})
    elif method == "thread/read":
        send({"id": frame["id"], "result": {"thread": {"id": "x", "turns": []}}})
    else:
        send({"id": frame["id"], "error": {"code": -32601,
                                           "message": "no " + str(method)}})
'''


def write_fake_codex(root: Path, mode: str = "single") -> Path:
    path = root / f"fake-codex-{mode}"
    path.write_text(FAKE_APPSERVER, encoding="utf-8")
    path.chmod(0o755)
    return path


@contextmanager
def collector_emitter(**kwargs):
    """One enabled UiEmitter plus a collecting subscriber (fresh per test)."""
    _ui_events, ui_projection = _ui_modules()
    events: list = []
    emitter = ui_projection.UiEmitter(enabled=True, subscribers=[events.append],
                                      **kwargs)
    yield emitter, events


# ---------------------------------------------------------------------------
# 1. Envelope schema, sequence, time and safe-value gates
# ---------------------------------------------------------------------------

class EnvelopeSchemaTests(unittest.TestCase):
    def test_envelope_fields_are_exactly_the_v1_contract(self):
        ui_events, _ = _ui_modules()
        with collector_emitter() as (emitter, events):
            emitter.emit("turn.started", "t1", task_id="t1", worker="codex",
                         mode="exec", session_id="sess_1", turn_id="turn_1")
        self.assertEqual(len(events), 1)
        self.assertEqual(set(events[0]), set(ui_events.ENVELOPE_FIELDS))
        # fixed v1, ordered contract in the module constant
        self.assertEqual(len(ui_events.ENVELOPE_FIELDS), 20)

    def test_event_id_is_hub_epoch_stream_seq(self):
        ui_events, _ = _ui_modules()
        stream = ui_events.UiEventStream("hubinst01", journal_epoch=7)
        event = stream.build("turn.started", "task_a", task_id="task_a",
                             worker="zcode", mode="cold",
                             session_id="sess_1", turn_id="turn_1")
        self.assertEqual(event["event_id"], "hubinst01/7/task_a/1")
        self.assertEqual(event["seq"], 1)
        ui_events.validate_event(event)

    def test_seq_is_per_stream_monotonic_from_one(self):
        ui_events, _ = _ui_modules()
        stream = ui_events.UiEventStream("hub1", 1)
        first_a = stream.build("turn.started", "task_a", session_id="s",
                               turn_id="t")
        second_a = stream.build("turn.completed", "task_a", session_id="s",
                                turn_id="t")
        first_b = stream.build("turn.started", "task_b", session_id="s",
                               turn_id="t")
        self.assertEqual([first_a["seq"], second_a["seq"]], [1, 2])
        self.assertEqual(first_b["seq"], 1)  # independent stream
        self.assertNotEqual(first_a["event_id"], first_b["event_id"])

    def test_observed_at_is_utc_rfc3339_microseconds_z(self):
        _ui_events, _ui_projection = _ui_modules()
        with collector_emitter() as (emitter, events):
            emitter.emit("turn.started", "t1", session_id="sess_1",
                         turn_id="turn_1")
        self.assertRegex(events[0]["observed_at"], RFC3339_UTC)

    def test_missing_fields_are_null_never_fabricated(self):
        ui_events, _ = _ui_modules()
        stream = ui_events.UiEventStream("hub1", 1)
        event = stream.build("turn.started", "t1", session_id="sess_1",
                             turn_id="turn_1", data={})
        for key in ("task_id", "worker", "mode", "tool_call_id",
                    "request_id", "source_event_seq", "status", "outcome",
                    "delivery_ok", "duration_ms", "error"):
            self.assertIsNone(event[key], key)
        self.assertEqual(event["data"], {})

    def test_validator_rejects_extra_and_missing_keys(self):
        ui_events, _ = _ui_modules()
        stream = ui_events.UiEventStream("hub1", 1)
        event = stream.build("turn.started", "t1", session_id="s",
                             turn_id="t")
        with_extra = {**event, "prompt": "leak"}
        self.assertRaises(ui_events.UiEventError,
                          ui_events.validate_event, with_extra)
        missing = {k: v for k, v in event.items() if k != "error"}
        self.assertRaises(ui_events.UiEventError,
                          ui_events.validate_event, missing)

    def test_validator_rejects_bad_kind_ids_times_and_types(self):
        ui_events, _ = _ui_modules()
        stream = ui_events.UiEventStream("hub1", 1)
        base = stream.build("turn.started", "t1", session_id="sess1",
                            turn_id="turn1")
        bad_kind = {**base, "kind": "not.a.kind"}
        bad_seq = {**base, "seq": 0}
        bad_bool_seq = {**base, "seq": True}
        bad_time = {**base, "observed_at": "2026-10-24T00:00:00+00:00"}
        bad_id = {**base, "session_id": "sess 1\nnewline"}
        bad_event_id = {**base, "event_id": "hub1/1/t1/2"}
        bad_worker = {**base, "worker": "vendor-x"}
        bad_delivery = {**base, "delivery_ok": "yes"}
        bad_duration = {**base, "duration_ms": -1.0}
        for candidate in (bad_kind, bad_seq, bad_bool_seq, bad_time, bad_id,
                          bad_event_id, bad_worker, bad_delivery, bad_duration):
            self.assertRaises(ui_events.UiEventError,
                              ui_events.validate_event, candidate)

    def test_data_allowlist_is_enforced_per_kind(self):
        ui_events, _ = _ui_modules()
        stream = ui_events.UiEventStream("hub1", 1)
        with self.assertRaises(ui_events.UiEventError):
            stream.build("task.accepted", "t1", data={"prompt": "leak"})
        with self.assertRaises(ui_events.UiEventError):
            stream.build("turn.started", "t1", data={"tool_name": "Bash"})
        with self.assertRaises(ui_events.UiEventError):
            stream.build("approval.decided", "t1",
                         data={"decision": "maybe"})
        # a stray non-scalar value fails even under an allowed key
        with self.assertRaises(ui_events.UiEventError):
            stream.build("tool.scheduled", "t1",
                         data={"tool_name": {"nested": "object"}})

    def test_safe_gates_null_out_untrusted_values(self):
        ui_events, _ = _ui_modules()
        self.assertIsNone(ui_events.safe_opaque("has space"))
        self.assertIsNone(ui_events.safe_opaque("line\nbreak"))
        self.assertIsNone(ui_events.safe_opaque("x" * 200))
        self.assertIsNone(ui_events.safe_opaque("/Users/evil/path"))
        self.assertEqual(ui_events.safe_opaque("sess_ok-1.2"), "sess_ok-1.2")
        # round 2: EXACT membership in the fixed trusted enumeration only
        self.assertEqual(ui_events.safe_error_code("ValueError"), "ValueError")
        self.assertIsNone(ui_events.safe_error_code("ValueError: boom /secret"))
        self.assertIsNone(ui_events.safe_error_code("SECRETMARKER"))
        self.assertIsNone(ui_events.safe_error_code("PROMPT-MARKER-DO-NOT-LEAK"))
        self.assertIsNone(ui_events.safe_error_code(None))

    def test_find_fragments_scans_serialized_events(self):
        ui_events, _ = _ui_modules()
        event = {"data": {"nested": "x PROMPT-MARKER-DO-NOT-LEAK y"}}
        found = ui_events.find_fragments(event, MARKERS)
        self.assertEqual(found, ["PROMPT-MARKER-DO-NOT-LEAK"])
        self.assertEqual(ui_events.find_fragments({"data": {}}, MARKERS), [])

    def test_source_event_seq_passthrough_and_null(self):
        ui_events, _ = _ui_modules()
        stream = ui_events.UiEventStream("hub1", 1)
        native = stream.build("turn.completed", "t1", session_id="s",
                              turn_id="t", source_event_seq=41)
        self.assertEqual(native["source_event_seq"], 41)
        codex = stream.build("turn.completed", "t2", session_id="s",
                             turn_id="t")  # codex has no native seq
        self.assertIsNone(codex["source_event_seq"])
        with self.assertRaises(ui_events.UiEventError):
            stream.build("turn.completed", "t3", session_id="s",
                         turn_id="t", source_event_seq="41")


# ---------------------------------------------------------------------------
# 2. Fail-closed initiator resolution (trusted/untrusted/parent matrix)
# ---------------------------------------------------------------------------

class InitiatorMatrixTests(unittest.TestCase):
    def resolver(self, registry=None):
        _, ui_projection = _ui_modules()
        return ui_projection.UiInitiatorResolver(registry=registry)

    def test_no_registry_means_unverified(self):
        result = self.resolver().resolve("codex", "sess_real_1")
        self.assertIsNone(result["initiator_agent"])
        self.assertIsNone(result["initiator_session_id"])
        self.assertFalse(result["initiator_verified"])
        self.assertEqual(result["initiator_unverified_reason"],
                         "initiator_unverified")
        self.assertIsNone(result["parent_session_id"])

    def test_unknown_agent_is_unverified_even_with_registry(self):
        registry = RegistryFixture({("codex", "sess_real_1"): "sess_real_1"})
        result = self.resolver(registry).resolve("claude", "sess_real_1")
        self.assertFalse(result["initiator_verified"])
        self.assertIsNone(result["initiator_session_id"])

    def test_registry_miss_and_agent_mismatch_are_unverified(self):
        registry = RegistryFixture({("codex", "sess_real_1"): "sess_real_1"})
        for agent, session in (("codex", "sess_UNKNOWN"),
                               ("zcode", "sess_real_1"),   # registered under codex
                               ("codex", "sess_real_1 "),   # not an exact match
                               ("codex", "SESS_REAL_1")):
            result = self.resolver(registry).resolve(agent, session)
            self.assertFalse(result["initiator_verified"], (agent, session))
            self.assertIsNone(result["initiator_session_id"], (agent, session))
            self.assertEqual(result["initiator_unverified_reason"],
                             "initiator_unverified")

    def test_malformed_initiator_ids_are_unverified(self):
        registry = RegistryFixture({("codex", "sess_real_1"): "sess_real_1"})
        for bad in ("/Users/evil/path", "line\nbreak", "x" * 300,
                    "aGVsbG8gc2VjcmV0IGJhc2U2NCBibG9i", "", None, 7):
            result = self.resolver(registry).resolve("codex", bad)
            self.assertFalse(result["initiator_verified"], bad)
            self.assertIsNone(result["initiator_session_id"], bad)

    def test_exact_registry_hit_yields_canonical_id(self):
        registry = RegistryFixture({("codex", "sess_real_1"): "sess_canon_9"})
        result = self.resolver(registry).resolve("codex", "sess_real_1")
        self.assertTrue(result["initiator_verified"])
        self.assertEqual(result["initiator_agent"], "codex")
        self.assertEqual(result["initiator_session_id"], "sess_canon_9")
        self.assertIsNone(result["initiator_unverified_reason"])

    def test_parent_edge_requires_a_verified_parent_record(self):
        registry = RegistryFixture({("codex", "sess_real_1"): "sess_real_1"},
                                   {"sess_real_1": "sess_parent_9"})
        result = self.resolver(registry).resolve("codex", "sess_real_1")
        self.assertEqual(result["parent_session_id"], "sess_parent_9")
        # no parent record -> no edge, even though the session itself verifies
        bare = RegistryFixture({("codex", "sess_real_1"): "sess_real_1"})
        self.assertIsNone(self.resolver(bare).resolve("codex", "sess_real_1")
                          ["parent_session_id"])

    def test_self_declared_parent_is_never_sufficient(self):
        # a caller claiming a parent in the initiator payload changes nothing
        registry = RegistryFixture({("codex", "sess_real_1"): "sess_real_1"})
        _, ui_projection = _ui_modules()
        with collector_emitter() as (emitter, events):
            ui_projection.project_task_accepted(
                emitter, self.resolver(registry), task_id="task_p1",
                worker="codex", mode="exec", contract_id="c1",
                contract_digest="a" * 64, created_at="2026-10-24T00:00:00Z",
                initiator={"agent": "codex", "session_id": "sess_real_1",
                           "parent_session_id": "sess_FORGED_PARENT_9"})
        accepted = events[0]
        self.assertIsNone(accepted["data"].get("parent_session_id"))
        blob = json.dumps(accepted)
        self.assertNotIn("sess_FORGED_PARENT_9", blob)

    def test_registry_errors_fail_closed(self):
        class ExplodingRegistry:
            def canonical_session(self, agent, session_id):
                raise RuntimeError("registry down")

        result = self.resolver(ExplodingRegistry()).resolve("codex", "sess_1")
        self.assertFalse(result["initiator_verified"])
        self.assertIsNone(result["initiator_session_id"])


# ---------------------------------------------------------------------------
# 3. task.accepted / stream.announced ordering and minimal data
# ---------------------------------------------------------------------------

class TaskAcceptedAnnouncedTests(unittest.TestCase):
    def test_accepted_precedes_announced_with_reference_id(self):
        _, ui_projection = _ui_modules()
        with collector_emitter() as (emitter, events):
            ui_projection.project_task_accepted(
                emitter, ui_projection.UiInitiatorResolver(), task_id="task_a",
                worker="zcode", mode="resident", contract_id="ctr-1",
                contract_digest="b" * 64,
                created_at="2026-10-24T01:02:03+00:00",
                initiator={"agent": "codex", "session_id": "sess_x"})
        self.assertEqual([e["kind"] for e in events],
                         ["task.accepted", "stream.announced"])
        accepted, announced = events
        self.assertEqual(accepted["stream_id"], "task_a")
        self.assertEqual(accepted["seq"], 1)
        self.assertEqual(announced["stream_id"], "hub")
        self.assertEqual(announced["data"]["task_stream_id"], "task_a")
        self.assertEqual(announced["data"]["accepted_event_id"],
                         accepted["event_id"])
        self.assertEqual(announced["task_id"], "task_a")

    def test_second_task_gets_fresh_task_seq_and_next_hub_seq(self):
        _, ui_projection = _ui_modules()
        with collector_emitter() as (emitter, events):
            for task in ("task_a", "task_b"):
                ui_projection.project_task_accepted(
                    emitter, ui_projection.UiInitiatorResolver(), task_id=task,
                    worker="zcode", mode="cold", contract_id="c",
                    contract_digest="c" * 64,
                    created_at="2026-10-24T00:00:00Z",
                    initiator={"agent": "zcode", "session_id": "s"})
        kinds = [(e["stream_id"], e["kind"], e["seq"]) for e in events]
        self.assertEqual(kinds, [("task_a", "task.accepted", 1),
                                 ("hub", "stream.announced", 1),
                                 ("task_b", "task.accepted", 1),
                                 ("hub", "stream.announced", 2)])

    def test_accepted_data_is_allowlisted_and_unverified_initiator_is_null(self):
        _, ui_projection = _ui_modules()
        with collector_emitter() as (emitter, events):
            ui_projection.project_task_accepted(
                emitter, ui_projection.UiInitiatorResolver(), task_id="t",
                worker="codex", mode="app-server", contract_id="ctr-1",
                contract_digest="d" * 64,
                created_at="2026-10-24T00:00:00Z",
                initiator={"agent": "codex", "session_id": "/Users/evil/x"})
        data = events[0]["data"]
        self.assertEqual(set(data), {"contract_id", "contract_digest",
                                     "created_at", "initiator_agent",
                                     "initiator_session_id",
                                     "initiator_verified", "reason_code"})
        self.assertIsNone(data["initiator_agent"])
        self.assertIsNone(data["initiator_session_id"])
        self.assertFalse(data["initiator_verified"])
        self.assertEqual(data["reason_code"], "initiator_unverified")
        self.assertEqual(data["contract_id"], "ctr-1")
        self.assertEqual(data["contract_digest"], "d" * 64)
        self.assertRegex(data["created_at"], RFC3339_UTC)

    def test_verified_initiator_carries_canonical_id_and_parent(self):
        _, ui_projection = _ui_modules()
        registry = RegistryFixture({("codex", "sess_real_1"): "sess_real_1"},
                                   {"sess_real_1": "sess_parent_9"})
        with collector_emitter() as (emitter, events):
            ui_projection.project_task_accepted(
                emitter, ui_projection.UiInitiatorResolver(registry),
                task_id="t", worker="codex", mode="exec", contract_id="c1",
                contract_digest="e" * 64,
                created_at="2026-10-24T00:00:00Z",
                initiator={"agent": "codex", "session_id": "sess_real_1"})
        data = events[0]["data"]
        self.assertTrue(data["initiator_verified"])
        self.assertEqual(data["initiator_agent"], "codex")
        self.assertEqual(data["initiator_session_id"], "sess_real_1")
        self.assertEqual(data["parent_session_id"], "sess_parent_9")
        self.assertNotIn("reason_code", data)


# ---------------------------------------------------------------------------
# 4. task.completed minimal terminal projection
# ---------------------------------------------------------------------------

class TaskCompletedTests(unittest.TestCase):
    def test_terminal_envelope_carries_only_safe_fields(self):
        _, ui_projection = _ui_modules()
        with collector_emitter() as (emitter, events):
            ui_projection.project_task_completed(
                emitter, task_id="t9", worker="zcode", mode="resident",
                status="completed", outcome="delivered", delivery_ok=True,
                duration_ms=1234.5,
                error_code="ValueError",
                session_locator={"agent": "zcode", "session_id": "sess_r1",
                                 "db_path": "/tmp/should-not-appear"})
        event = events[0]
        self.assertEqual(event["kind"], "task.completed")
        self.assertEqual(event["stream_id"], "t9")
        self.assertEqual(event["status"], "completed")
        self.assertEqual(event["outcome"], "delivered")
        self.assertTrue(event["delivery_ok"])
        self.assertEqual(event["duration_ms"], 1234.5)
        self.assertEqual(event["error"], "ValueError")
        self.assertEqual(event["session_id"], "sess_r1")
        blob = json.dumps(event)
        self.assertNotIn("/tmp/should-not-appear", blob)
        self.assertNotIn("PROMPT-MARKER-DO-NOT-LEAK", blob)

    def test_duration_is_monotonic_and_none_when_unknown(self):
        _, ui_projection = _ui_modules()
        import time as _time
        start = _time.monotonic()
        _time.sleep(0.01)
        with collector_emitter() as (emitter, events):
            ui_projection.project_task_completed(
                emitter, task_id="t", worker="codex", mode="exec",
                status="completed", outcome="failed", delivery_ok=None,
                duration_ms=round((_time.monotonic() - start) * 1000, 3),
                error_code=None)
        self.assertGreaterEqual(events[0]["duration_ms"], 0.0)
        # duration is a required fact of task.completed: an unavailable
        # duration omits the WHOLE event (never fabricated as 0)
        with collector_emitter() as (emitter, events):
            ui_projection.project_task_completed(
                emitter, task_id="t", worker="codex", mode="exec",
                status="failed", outcome="runner-error", delivery_ok=None,
                duration_ms=None, error_code="totally-untrusted-text")
        self.assertEqual(events, [])
        with collector_emitter() as (emitter, events):
            ui_projection.project_task_completed(
                emitter, task_id="t", worker="codex", mode="exec",
                status="failed", outcome="runner-error", delivery_ok=None,
                duration_ms=5.0, error_code="totally-untrusted-text")
        self.assertEqual(events[0]["duration_ms"], 5.0)
        self.assertIsNone(events[0]["error"])
        self.assertEqual(events[0]["data"]["reason_code"],
                         "error_unclassified")


# ---------------------------------------------------------------------------
# 5. Authority decision projection (read-only, sideband)
# ---------------------------------------------------------------------------

class AuthorityProjectionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="ui auth ")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.workspace = self.root / "ws"
        self.workspace.mkdir()
        (self.workspace / "TASK.md").write_text("task", encoding="utf-8")
        self.packet_path = self.root / "packet.json"
        self.packet = {
            "workspace": str(self.workspace),
            "prompt": "PROMPT-MARKER-DO-NOT-LEAK do work",
            "allowed_files": ["OUT.md"],
            "commands": [{"id": "cmd_ok", "command": "echo ok",
                          "max_uses": 1, "max_timeout_ms": 5000}],
        }
        self.packet_path.write_text(json.dumps(self.packet), encoding="utf-8")
        self.before = inventory(self.workspace)

    def authority(self, ui_emit=None):
        authority = Authority(self.packet, self.packet_path, self.before,
                              ui_emit=ui_emit)
        authority.session_id = "sess_auth_1"
        authority.turn_id = "turn_auth_1"
        return authority

    def decide_params(self, command="echo ok", request_id="rq_1",
                      tool_call_id="tc_1"):
        return {"sessionId": "sess_auth_1", "turnId": "turn_auth_1",
                "requestId": request_id, "toolCallId": tool_call_id,
                "toolName": "Bash",
                "input": {"command": command, "timeout": 1000}}

    def test_allow_decision_projects_requested_then_decided(self):
        _, ui_projection = _ui_modules()
        with collector_emitter() as (emitter, events):
            authority = self.authority(
                ui_emit=ui_projection.authority_hook(emitter, "task_auth",
                                                     "zcode", "resident"))
            decision = authority.decide(self.decide_params())
        self.assertEqual(decision["decision"], "allow")
        self.assertEqual([e["kind"] for e in events],
                         ["approval.requested", "approval.decided"])
        requested, decided = events
        self.assertEqual(requested["stream_id"], "task_auth")
        self.assertEqual(requested["session_id"], "sess_auth_1")
        self.assertEqual(requested["turn_id"], "turn_auth_1")
        self.assertEqual(requested["request_id"], "rq_1")
        self.assertEqual(requested["tool_call_id"], "tc_1")
        self.assertEqual(requested["data"]["tool_name"], "Bash")
        self.assertEqual(decided["data"]["decision"], "allow")
        self.assertEqual(decided["data"]["command_id"], "cmd_ok")

    def test_decisions_are_unchanged_with_and_without_projection(self):
        _, ui_projection = _ui_modules()
        plain = self.authority()
        with collector_emitter() as (emitter, _events):
            wired = self.authority(
                ui_emit=ui_projection.authority_hook(emitter, "t", "zcode",
                                                     "resident"))
        cases = [self.decide_params(),
                 self.decide_params(command="rm -rf /", request_id="rq_2",
                                    tool_call_id="tc_2"),
                 self.decide_params(request_id="rq 3\nbad", tool_call_id="tc_3")]
        for params in cases:
            self.assertEqual(plain.decide(dict(params)),
                             wired.decide(dict(params)))
        self.assertEqual(len(plain.decisions), len(wired.decisions))
        self.assertEqual(plain.decisions, wired.decisions)

    def test_forged_ids_do_not_reach_events(self):
        _, ui_projection = _ui_modules()
        with collector_emitter() as (emitter, events):
            authority = self.authority(
                ui_emit=ui_projection.authority_hook(emitter, "t", "zcode",
                                                     "resident"))
            authority.decide(self.decide_params(
                command="cat COOKIE=monster; session=stolen",
                request_id="PROMPT MARKER /Users/evil/path\nx",
                tool_call_id="/Users/evil/path"))
        for event in events:
            self.assertIsNone(event["request_id"])
            self.assertEqual(ui_events_find_fragments(event), [])
        # native decision evidence is untouched
        self.assertEqual(authority.decisions[-1]["decision"], "deny")

    def test_subscriber_failure_cannot_change_decisions(self):
        _, ui_projection = _ui_modules()

        def boomer(_event):
            raise RuntimeError("subscriber exploded")

        emitter = ui_projection.UiEmitter(enabled=True, subscribers=[boomer])
        authority = self.authority(
            ui_emit=ui_projection.authority_hook(emitter, "t", "zcode",
                                                 "resident"))
        decision = authority.decide(self.decide_params())
        self.assertEqual(decision["decision"], "allow")
        self.assertEqual(len(authority.decisions), 1)
        self.assertTrue(emitter.failures)


def ui_events_find_fragments(event):
    ui_events, _ = _ui_modules()
    return ui_events.find_fragments(event, MARKERS)


# ---------------------------------------------------------------------------
# 6. Notification attribution: current-task filtering (behavioural + helper)
# ---------------------------------------------------------------------------

class NotificationIsolationTests(unittest.TestCase):
    """Behavioural family: runs against the CURRENT production driver.

    The dual fake interleaves two threads' notifications (plus one
    identity-less frame) on ONE shared client; each task's receipt must count
    only its own thread, and identity-less frames must not land in any task.
    """

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="ui notif ")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.evidence = self.root / "evidence"
        self.evidence.mkdir()

    def client(self, mode):
        script = write_fake_codex(self.root, mode)
        return CodexAppServerClient(
            script, self.evidence / mode,
            env={"FAKE_UI_APPSERVER_MODE": mode})

    def test_single_task_receipt_counts_only_own_and_reports_unattributed(self):
        # PLAN:222 (prevails over the older receipt shape): the task receipt
        # counts ONLY its own frames; identity-less frames live in the
        # independent client-level unattributed view.
        async def go():
            client = self.client("single")
            try:
                await client.start()
                receipt = await CodexAppServerDriver(client).run_task(
                    SimpleContractAuthority([], []), self.root, "p",
                    timeout_s=10)
            finally:
                await client.close()
            return client, receipt

        client, receipt = asyncio.run(go())
        self.assertEqual(receipt["notification_counts"],
                         {"item/completed": 1, "turn/completed": 1})
        self.assertNotIn("notification_unattributed", receipt)
        self.assertEqual(client.notification_unattributed,
                         {"orphan/notification": 1})

    def test_interleaved_threads_never_cross_associate_counts(self):
        async def go():
            client = self.client("dual")
            try:
                await client.start()
                driver = CodexAppServerDriver(client)
                return await asyncio.gather(
                    driver.run_task(SimpleContractAuthority([], []),
                                    self.root / "wsA", "task A", timeout_s=15),
                    driver.run_task(SimpleContractAuthority([], []),
                                    self.root / "wsB", "task B", timeout_s=15))
            finally:
                await client.close()

        receipt_a, receipt_b = asyncio.run(go())
        for receipt in (receipt_a, receipt_b):
            self.assertEqual(receipt["notification_counts"],
                             {"item/completed": 1, "turn/completed": 1},
                             receipt)
            self.assertNotIn("notification_unattributed", receipt)
        self.assertEqual(receipt_a["thread_id"], "th_ui_A")
        self.assertEqual(receipt_b["thread_id"], "th_ui_B")


class NotificationAttributionHelperTests(unittest.TestCase):
    def test_split_counts_isolate_own_and_unattributed(self):
        _, ui_projection = _ui_modules()
        frames = [
            {"method": "item/completed", "params": {"threadId": "th_A"}},
            {"method": "item/completed", "params": {"threadId": "th_B"}},
            {"method": "orphan/x", "params": {"free": True}},
            {"method": "turn/completed", "params": {"threadId": "th_A"}},
            {"method": "v1/shape", "params": {"conversationId": "th_B"}},
            "not-a-frame",
            {"params": {"threadId": "th_A"}},  # no method
        ]
        own, unattributed = ui_projection.split_notification_counts(
            frames, "th_A")
        self.assertEqual(own, {"item/completed": 1, "turn/completed": 1})
        self.assertEqual(unattributed, {"orphan/x": 1})

    def test_sequential_tasks_do_not_inherit_earlier_frames(self):
        _, ui_projection = _ui_modules()
        shared = [{"method": "item/completed", "params": {"threadId": "th_1"}},
                  {"method": "turn/completed", "params": {"threadId": "th_1"}}]
        first, first_unattributed = ui_projection.split_notification_counts(
            shared, "th_1")
        # a LATER task on the same client sees the whole history
        second, second_unattributed = ui_projection.split_notification_counts(
            shared + [{"method": "turn/completed",
                       "params": {"threadId": "th_2"}}], "th_2")
        self.assertEqual(first, {"item/completed": 1, "turn/completed": 1})
        self.assertEqual(first_unattributed, {})
        self.assertEqual(second, {"turn/completed": 1})
        self.assertEqual(second_unattributed, {})

    def test_attribution_reason_codes(self):
        _, ui_projection = _ui_modules()
        bindings = {"th_A": "task_a", "th_B": "task_b"}
        self.assertEqual(
            ui_projection.attribute_notification("m", {"threadId": "th_A"},
                                                 bindings),
            ("task_a", None))
        self.assertEqual(
            ui_projection.attribute_notification("m", {}, bindings),
            (None, "identity_missing"))
        self.assertEqual(
            ui_projection.attribute_notification("m", {"threadId": "th_X"},
                                                 bindings),
            (None, "identity_unknown"))
        self.assertEqual(
            ui_projection.attribute_notification(
                "m", {"threadId": "th_A", "conversationId": "th_B"}, bindings),
            (None, "identity_ambiguous"))


# ---------------------------------------------------------------------------
# 7. process.exited: exactly once, only from a P0.3 awaited exit
# ---------------------------------------------------------------------------

class ProcessExitProjectionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="ui exit ")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.evidence = self.root / "evidence"
        self.evidence.mkdir()

    def test_appserver_real_close_projects_exactly_one_exit(self):
        _, ui_projection = _ui_modules()
        events: list = []
        emitter = ui_projection.UiEmitter(enabled=True, subscribers=[events.append])
        client = CodexAppServerClient(
            write_fake_codex(self.root), self.evidence / "app",
            exit_event_sink=ui_projection.make_exit_sink(
                emitter, "shared-app-server", "codex"))

        async def go():
            await client.start()
            client.generation = 1  # the hub's WorkerGeneration stamp
            receipt = await CodexAppServerDriver(client).run_task(
                SimpleContractAuthority([], []), self.root, "p", timeout_s=10)
            await client.close()   # real awaited close
            await client.close()   # repeated close emits nothing
            return receipt

        receipt = asyncio.run(go())
        exits = [e for e in events if e["kind"] == "process.exited"]
        self.assertEqual(receipt["outcome"], "delivered")
        self.assertEqual(len(exits), 1)
        event = exits[0]
        self.assertEqual(event["stream_id"], "hub")
        self.assertEqual(event["data"]["process_scope"], "shared-app-server")
        self.assertEqual(event["data"]["exit_code"], 0)
        self.assertIs(event["data"]["process_generation"], 1)
        ui_events, _ = _ui_modules()
        ui_events.validate_event(event)

    def test_eof_and_completed_task_are_not_process_exits(self):
        _, ui_projection = _ui_modules()
        events: list = []
        emitter = ui_projection.UiEmitter(enabled=True, subscribers=[events.append])
        client = CodexAppServerClient(
            write_fake_codex(self.root), self.evidence / "eof",
            exit_event_sink=ui_projection.make_exit_sink(
                emitter, "shared-app-server", "codex"))

        async def go():
            await client.start()
            client.generation = 1  # the hub's WorkerGeneration stamp
            receipt = await CodexAppServerDriver(client).run_task(
                SimpleContractAuthority([], []), self.root, "p", timeout_s=10)
            after_task = [e["kind"] for e in events]
            # kill the child out-of-band: reader EOF isolates but is NOT exit
            os.kill(client.process.pid, signal.SIGKILL)
            await asyncio.sleep(0.2)
            after_eof = [e["kind"] for e in events]
            await client.close()  # the close's wait IS the awaited exit
            return receipt, after_task, after_eof

        receipt, after_task, after_eof = asyncio.run(go())
        self.assertEqual(receipt["outcome"], "delivered")
        self.assertNotIn("process.exited", after_task)
        self.assertNotIn("process.exited", after_eof)
        exits = [e for e in events if e["kind"] == "process.exited"]
        self.assertEqual(len(exits), 1)
        self.assertEqual(exits[0]["data"]["exit_code"], -signal.SIGKILL)

    def test_sink_rejects_non_integer_and_duplicate_records(self):
        _, ui_projection = _ui_modules()
        events: list = []
        emitter = ui_projection.UiEmitter(enabled=True, subscribers=[events.append])
        sink = ui_projection.make_exit_sink(emitter, "shared-resident", "zcode")
        projection = ExitProjection(on_event=sink)
        self.assertFalse(projection.observe_awaited_exit(None, generation=1))
        self.assertFalse(projection.observe_awaited_exit(True, generation=1))
        self.assertEqual([e["kind"] for e in events], [])
        self.assertTrue(projection.observe_awaited_exit(3, generation=1))
        self.assertFalse(projection.observe_awaited_exit(0, generation=1))
        exits = [e for e in events if e["kind"] == "process.exited"]
        self.assertEqual(len(exits), 1)
        self.assertEqual(exits[0]["data"]["exit_code"], 3)
        self.assertEqual(exits[0]["data"]["process_scope"], "shared-resident")

    def test_resident_wiring_projects_on_awaited_close(self):
        _, ui_projection = _ui_modules()
        events: list = []
        emitter = ui_projection.UiEmitter(enabled=True, subscribers=[events.append])
        config = SimpleNamespace(work_root=self.root / "work",
                                 state_dir=self.root / "state",
                                 cache_dir=self.root / "cache", notify=False)
        (self.root / "work").mkdir(exist_ok=True)
        client_holder = {}

        class FakeResidentClient:
            def __init__(self, workspace, output, permission, config=None):
                self.generation = None
                self.events = []
                self.failure = None
                self.process = SimpleNamespace(pid=31, returncode=None)
                client_holder["client"] = self

            async def start(self):
                pass

            async def close(self):
                # mimic the real ProtocolClient close: only an awaited wait
                # observes the exit
                self.process.returncode = 0
                self.exit_projection.observe_awaited_exit(
                    self.process.returncode, generation=self.generation)

        resident = ResidentZCode(
            config, self.root / "work",
            client_factory=lambda *args: FakeResidentClient(*args),
            ui_emitter=emitter)

        async def go():
            resident.generation = 1  # the hub's WorkerGeneration stamp
            await resident.start()
            await resident.stop()
            await resident.stop()  # repeated stop: still exactly one exit

        asyncio.run(go())
        exits = [e for e in events if e["kind"] == "process.exited"]
        self.assertEqual(len(exits), 1)
        self.assertEqual(exits[0]["data"]["process_scope"], "shared-resident")
        self.assertEqual(exits[0]["data"]["exit_code"], 0)
        self.assertEqual(exits[0]["data"]["process_generation"], 1)
        self.assertEqual(exits[0]["worker"], "zcode")

    def test_hub_workers_wire_the_appserver_exit_sink(self):
        _, ui_projection = _ui_modules()
        from zcode_supervisor.hub_server import ResidentWorkers
        events: list = []
        emitter = ui_projection.UiEmitter(enabled=True, subscribers=[events.append])
        config = SimpleNamespace(work_root=self.root, state_dir=self.root / "state",
                                 cache_dir=self.root / "cache",
                                 cli_path=str(write_fake_codex(self.root)))
        workers = ResidentWorkers(config, self.evidence,
                                  codex_path=str(write_fake_codex(self.root,
                                                                  "dual")),
                                  ui_emitter=emitter)

        async def go():
            current = await workers.appserver_generation()
            await workers.close()
            return current

        current = asyncio.run(go())
        exits = [e for e in events if e["kind"] == "process.exited"]
        self.assertEqual(len(exits), 1)
        self.assertEqual(exits[0]["data"]["process_generation"],
                         current["generation"])


# ---------------------------------------------------------------------------
# 8. Driver-level session/turn projection (codex app-server)
# ---------------------------------------------------------------------------

class DriverProjectionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="ui driver ")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.evidence = self.root / "evidence"
        self.evidence.mkdir()

    def test_session_and_turn_events_follow_authoritative_bindings(self):
        _, ui_projection = _ui_modules()
        events: list = []
        emitter = ui_projection.UiEmitter(enabled=True, subscribers=[events.append])
        client = CodexAppServerClient(write_fake_codex(self.root),
                                      self.evidence / "c")

        async def go():
            await client.start()
            driver = CodexAppServerDriver(client, ui_emitter=emitter,
                                          task_id="task_ui_1")
            receipt = await driver.run_task(
                SimpleContractAuthority([], []), self.root, "p", timeout_s=10)
            await client.close()
            return receipt

        receipt = asyncio.run(go())
        self.assertEqual(receipt["outcome"], "delivered")
        self.assertEqual([e["kind"] for e in events],
                         ["session.bound", "turn.started", "turn.completed"])
        bound, started, completed = events
        for event in (bound, started, completed):
            self.assertEqual(event["stream_id"], "task_ui_1")
            self.assertEqual(event["task_id"], "task_ui_1")
            self.assertEqual(event["worker"], "codex")
            self.assertEqual(event["mode"], "app-server")
        self.assertEqual(bound["session_id"], "th_ui_single")
        self.assertEqual(bound["data"]["locator_type"], "app-server-thread")
        self.assertEqual(started["turn_id"], "turn_th_ui_single")
        self.assertEqual(completed["status"], "completed")
        self.assertIsNone(completed["source_event_seq"])  # codex has no seq

    def test_unconfigured_driver_is_silent(self):
        client = CodexAppServerClient(write_fake_codex(self.root),
                                      self.evidence / "bare")

        async def go():
            await client.start()
            receipt = await CodexAppServerDriver(client).run_task(
                SimpleContractAuthority([], []), self.root, "p", timeout_s=10)
            await client.close()
            return receipt

        receipt = asyncio.run(go())
        self.assertEqual(receipt["outcome"], "delivered")
        self.assertEqual(receipt["notification_counts"],
                         {"item/completed": 1, "turn/completed": 1})
        self.assertNotIn("notification_unattributed", receipt)


# ---------------------------------------------------------------------------
# 9. Resident session/turn/tool projection
# ---------------------------------------------------------------------------

class ScriptedResidentClient:
    """Resident app-server double: one approved command turn, scripted live."""

    def __init__(self, workspace, output, permission, config=None):
        self.generation = None
        self.events = []
        self.failure = None
        self.process = SimpleNamespace(pid=31, returncode=None)
        self.exit_projection = ExitProjection()
        self.permission = permission
        self.tasks = []

    async def start(self):
        pass

    async def close(self):
        pass

    async def _run_turn(self, input_id):
        sid, turn = "sess_ui_A", "turn_1"
        self.events.append({"type": "turn.started", "sessionId": sid,
                            "turnId": turn, "seq": 1,
                            "payload": {"inputId": input_id}})
        self.events.append({"type": "tool.updated", "sessionId": sid,
                            "turnId": turn, "seq": 2,
                            "payload": {"kind": "scheduled", "toolName": "Bash",
                                        "toolCallId": "call_1",
                                        "input": {"command": "echo hi",
                                                  "timeout": 1000}}})
        await asyncio.sleep(0.1)  # let the run loop bind the live turn id
        self.permission({"sessionId": sid, "turnId": turn,
                         "requestId": "req_1", "toolCallId": "call_1",
                         "toolName": "Bash",
                         "input": {"command": "echo hi", "timeout": 1000}})
        self.events.append({"type": "tool.updated", "sessionId": sid,
                            "turnId": turn, "seq": 3,
                            "payload": {"kind": "result", "toolCallId": "call_1",
                                        "result": {
                                            "success": True,
                                            "perf": {"detail": {
                                                "kind": "command",
                                                "command": {
                                                    "exitCode": 0,
                                                    "timedOut": False,
                                                    "status": "completed",
                                                    "runMs": 3}}}}}})
        self.events.append({"type": "turn.completed", "sessionId": sid,
                            "turnId": turn, "seq": 4,
                            "payload": {"resultType": "success"}})

    async def request(self, method, params):
        if method == "session/create":
            return {"session": {"sessionId": "sess_ui_A", "status": "idle"},
                    "runtime": {"pendingRequestIds": []},
                    "settings": {"model": {"current": None}}}
        if method == "session/subscribe":
            return {"eventSeq": 0}
        if method == "session/send":
            self.tasks.append(asyncio.create_task(self._run_turn(
                params["inputId"])))
            return {}
        if method == "session/read":
            return {"session": {"sessionId": "sess_ui_A", "status": "idle"},
                    "runtime": {"pendingRequestIds": []}}
        return {}


class ResidentProjectionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="ui resident ")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        for name in ("work", "state", "cache"):
            (self.root / name).mkdir()
        self.config = SimpleNamespace(work_root=self.root / "work",
                                      state_dir=self.root / "state",
                                      cache_dir=self.root / "cache",
                                      notify=False)
        self.workspace = self.root / "work" / "ws"
        self.workspace.mkdir()

    def test_resident_run_projects_bindings_turns_tools_and_state(self):
        _, ui_projection = _ui_modules()
        events: list = []
        emitter = ui_projection.UiEmitter(enabled=True, subscribers=[events.append])
        resident = ResidentZCode(
            self.config, self.root / "work",
            client_factory=lambda *args: ScriptedResidentClient(*args),
            ui_emitter=emitter)
        packet_path = self.root / "packet.json"
        packet = {"workspace": str(self.workspace),
                  "prompt": "PROMPT-MARKER-DO-NOT-LEAK run",
                  "allowed_files": [],
                  "commands": [{"id": "cmd_1", "command": "echo hi",
                                "max_uses": 1, "max_timeout_ms": 5000}]}
        packet_path.write_text(json.dumps(packet), encoding="utf-8")
        out = self.root / "work" / "runUI.json"

        async def go():
            await resident.start()
            try:
                return await resident.run_packet(packet_path, out, timeout_s=10)
            finally:
                await resident.stop()

        result = asyncio.run(go())
        self.assertTrue(result["delivery_ok"], result.get("error"))
        kinds = [e["kind"] for e in events]
        self.assertEqual(kinds, ["session.bound", "turn.started",
                                 "tool.scheduled", "approval.requested",
                                 "approval.decided", "tool.completed",
                                 "turn.completed", "session.state"])
        bound, started, scheduled, _req, decided, tool_done, turn_done, state \
            = events
        for event in events:
            self.assertEqual(event["stream_id"], "runUI")
            self.assertEqual(event["task_id"], "runUI")
            self.assertEqual(event["worker"], "zcode")
            self.assertEqual(event["mode"], "resident")
        self.assertEqual([e["seq"] for e in events], list(range(1, 9)))
        self.assertEqual(bound["session_id"], "sess_ui_A")
        self.assertEqual(bound["data"]["locator_type"], "resident")
        self.assertEqual(started["source_event_seq"], 1)
        self.assertEqual(scheduled["tool_call_id"], "call_1")
        self.assertEqual(scheduled["data"]["tool_name"], "Bash")
        self.assertEqual(decided["data"]["decision"], "allow")
        self.assertEqual(decided["data"]["command_id"], "cmd_1")
        self.assertEqual(tool_done["status"], "result")
        self.assertEqual(turn_done["source_event_seq"], 4)
        self.assertEqual(turn_done["outcome"], "success")
        self.assertEqual(state["status"], "exited")
        self.assertEqual(state["data"]["reason_code"], "clean_exit")
        self.assertEqual(state["data"]["last_source_seq"], 4)
        registry = json.loads(
            (self.root / "state" / "hub" / "resident-sessions.json").read_text())
        self.assertEqual(registry["sess_ui_A"]["state"], "exited")


# ---------------------------------------------------------------------------
# 10. Hub delegate projection: accepted/announced/completed + transparency
# ---------------------------------------------------------------------------

class HubUiFixture(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="ui hub ")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.work_root = self.root / "work"
        self.state_dir = self.root / "state"
        for path in (self.work_root, self.state_dir):
            path.mkdir()
        self.workspace = self.work_root / "ws"
        self.workspace.mkdir()
        self.fake_codex = self.root / "fake-codex"
        self.fake_codex.write_text("#!/bin/sh\n", encoding="utf-8")
        self.fake_codex.chmod(0o755)
        self.evidence_root = self.root / "hub-evidence"
        self.config = SimpleNamespace(work_root=self.work_root,
                                      state_dir=self.state_dir,
                                      cache_dir=self.root / "cache",
                                      cli_path=str(self.root / "zcode.cjs"))
        for target, value in (
            ("zcode_supervisor.tri_probe.discover_codex",
             {"installed": False, "path": None, "candidates": []}),
            ("zcode_supervisor.tri_probe.discover_zcode",
             {"installed": True, "path": str(self.root / "zcode.cjs"),
              "candidates": [str(self.root / "zcode.cjs")]}),
            ("zcode_supervisor.tri_probe.discover_dsh",
             {"installed": False, "path": None}),
        ):
            patcher = patch(target, return_value=value)
            patcher.start()
            self.addCleanup(patcher.stop)

    def contract(self, drop=(), **overrides):
        body = {
            "contract_id": "hub-ui-1",
            "worker": "codex",
            "workspace": str(self.workspace),
            "prompt": "Read TASK.md and answer. PROMPT-MARKER-DO-NOT-LEAK",
            "allowed_files": [],
            "commands": [],
            "model": {"model": "gpt-5-codex"},
            "max_timeout_ms": 60_000,
            "budget": {"max_tokens": None, "max_cost_usd": 1.0, "note": "p0"},
            "output_schema": {"type": "object",
                              "properties": {"answer": {"type": "string"}},
                              "required": ["answer"],
                              "additionalProperties": False},
        }
        body.update(overrides)
        for key in drop:
            body.pop(key, None)
        source = self.root / "incoming"
        source.mkdir(exist_ok=True)
        path = source / "hub-ui-1.json"
        path.write_text(json.dumps(body), encoding="utf-8")
        return contracts.register_contract(self.state_dir, path), body

    @staticmethod
    def fake_codex_runner(codex_path, workspace, prompt, output_schema,
                          evidence_dir, timeout_s, model=None):
        Path(evidence_dir).mkdir(parents=True, exist_ok=True)
        return {"outcome": "delivered", "thread_id": "th_ui_hub",
                "structured": {"answer": "ok"}, "error": None}

    def delegate(self, contract_id, **kwargs):
        from zcode_supervisor import hub_server
        return asyncio.run(hub_server.delegate_impl(
            self.config, self.evidence_root, contract_id,
            kwargs.pop("initiator",
                       {"agent": "codex", "session_id": "sess_ctrl_1"}),
            codex_path=str(self.fake_codex),
            codex_runner=self.fake_codex_runner, **kwargs))


class HubDelegateProjectionTests(HubUiFixture):
    def test_delegate_projects_accepted_announced_completed_in_order(self):
        _ui_events, _ui_projection = _ui_modules()
        registration, body = self.contract()
        with collector_emitter() as (emitter, events):
            result = self.delegate(registration["contract_id"],
                                   ui_emitter=emitter)
        self.assertEqual(result["outcome"], "delivered")
        self.assertEqual([e["kind"] for e in events],
                         ["task.accepted", "stream.announced", "task.completed"])
        accepted, announced, completed = events
        task_id = result["task_id"]
        self.assertEqual(accepted["stream_id"], task_id)
        self.assertEqual(accepted["worker"], "codex")
        self.assertEqual(accepted["mode"], "exec")
        self.assertEqual(accepted["data"]["contract_id"], "hub-ui-1")
        self.assertEqual(accepted["data"]["contract_digest"],
                         contracts.contract_digest(body))
        self.assertEqual(announced["data"]["task_stream_id"], task_id)
        self.assertEqual(announced["data"]["accepted_event_id"],
                         accepted["event_id"])
        self.assertEqual(completed["seq"], 2)
        self.assertEqual(completed["status"], "completed")
        self.assertEqual(completed["outcome"], "delivered")
        self.assertEqual(completed["session_id"], "th_ui_hub")
        self.assertGreaterEqual(completed["duration_ms"], 0.0)

    def test_delegate_with_verified_registry_initiator(self):
        _, ui_projection = _ui_modules()
        registration, _body = self.contract()
        registry = RegistryFixture(
            {("codex", "sess_ctrl_1"): "sess_ctrl_1"},
            {"sess_ctrl_1": "sess_parent_9"})
        with collector_emitter() as (emitter, events):
            self.delegate(registration["contract_id"], ui_emitter=emitter,
                          ui_initiator_resolver=ui_projection.UiInitiatorResolver(
                              registry=registry))
        data = events[0]["data"]
        self.assertTrue(data["initiator_verified"])
        self.assertEqual(data["initiator_session_id"], "sess_ctrl_1")
        self.assertEqual(data["parent_session_id"], "sess_parent_9")

    def test_default_emitter_is_env_gated_and_on_by_default(self):
        # 2026-10-24 product decision: unset/empty flag = ON (double-click
        # the app and it just works); ONLY a non-empty explicit false value
        # (0/false/no/off) rolls back; any other non-empty value = explicit
        # enable.
        _, ui_projection = _ui_modules()
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("CZS_UI_EVENTS", None)
            self.assertTrue(ui_projection.UiEmitter().enabled)
            registration, _body = self.contract()
            result = self.delegate(registration["contract_id"])
        self.assertEqual(result["outcome"], "delivered")
        with patch.dict(os.environ, {"CZS_UI_EVENTS": ""}):
            self.assertTrue(ui_projection.UiEmitter().enabled)  # empty = on
        for rollback in ("0", "false", "no", "off", " 0 ", "OFF"):
            with patch.dict(os.environ, {"CZS_UI_EVENTS": rollback}):
                self.assertFalse(ui_projection.UiEmitter().enabled,
                                 f"CZS_UI_EVENTS={rollback!r} must disable")
        for explicit in ("1", "true", "yes", "on", "enabled"):
            with patch.dict(os.environ, {"CZS_UI_EVENTS": explicit}):
                self.assertTrue(ui_projection.UiEmitter().enabled,
                                f"CZS_UI_EVENTS={explicit!r} must enable")
        self.assertIs(ui_projection.default_emitter(),
                      ui_projection.default_emitter())

    def test_emit_failures_never_change_the_delegate_result(self):
        _, ui_projection = _ui_modules()
        registration, _body = self.contract()

        def boomer(_event):
            raise RuntimeError("subscriber exploded")

        broken = ui_projection.UiEmitter(enabled=True, subscribers=[boomer])
        with collector_emitter() as (healthy, _events):
            healthy_result = self.delegate(registration["contract_id"],
                                           ui_emitter=healthy)
            broken_result = self.delegate(registration["contract_id"],
                                          ui_emitter=broken)
        self.assertEqual(broken_result["outcome"], healthy_result["outcome"])
        self.assertEqual(broken_result["worker"], healthy_result["worker"])
        self.assertTrue(broken.failures)
        record = json.loads(
            (self.state_dir / "hub" / "tasks" /
             f"{broken_result['task_id']}.json").read_text())
        self.assertEqual(record["status"], "completed")

    def test_disabled_emitter_changes_nothing(self):
        registration, _body = self.contract()
        emitter = _ui_modules()[1].UiEmitter(enabled=False)
        result = self.delegate(registration["contract_id"], ui_emitter=emitter)
        self.assertEqual(result["outcome"], "delivered")

    def test_appserver_delegate_projects_approval_events(self):
        _ui_events, _ui_projection = _ui_modules()
        captured = {}

        class FakeDriver:
            def __init__(self, client):
                captured["client"] = client

            async def run_task(self, authority, workspace, prompt, *, model=None,
                               sandbox="read-only", approval_policy="on-request",
                               timeout_s=600, budget=None):
                captured["authority"] = authority
                return {"outcome": "delivered", "thread_id": "th_app1",
                        "turn_id": "turn_app1", "final_message": "done",
                        "command_items": [], "approvals": []}

        # app-server mode: no exec-only output_schema
        _registration, _body = self.contract(
            drop=("output_schema",),
            mode="app-server", sandbox="workspace-write",
            allowed_files=["OUT.md"],
            commands=[{"id": "cmd_ok", "command": "echo ok", "max_uses": 1,
                       "max_timeout_ms": 5000}])
        (self.workspace / "TASK.md").write_text("t", encoding="utf-8")
        from zcode_supervisor import hub_server

        with collector_emitter() as (emitter, events):
            result = asyncio.run(hub_server.delegate_impl(
                self.config, self.evidence_root, "hub-ui-1",
                {"agent": "codex", "session_id": "sess_ctrl_1"},
                codex_path=str(self.fake_codex),
                appserver_factory=lambda: FakeClientStub(),
                appserver_driver_factory=lambda c: FakeDriver(c),
                ui_emitter=emitter))
        self.assertEqual(result["outcome"], "delivered")
        kinds = [e["kind"] for e in events]
        # codex approvals project at the ApprovalBridge (applied decision),
        # so the hub timeline here is accepted/announced/completed
        self.assertEqual(kinds, ["task.accepted", "stream.announced",
                                 "task.completed"])
        self.assertEqual(events[2]["session_id"], "th_app1")
        # the production driver construction wires the task stream for the
        # bridge-level projection
        workers = hub_server.ResidentWorkers(self.config, self.evidence_root,
                                             ui_emitter=emitter)
        driver = workers.appserver_driver(FakeClientStub(), "task_wired")
        self.assertEqual(driver.ui_task_id, "task_wired")
        self.assertIs(driver.ui_emitter, emitter)


class FakeClientStub:
    async def start(self, budget=None):
        return {}

    async def close(self):
        pass


# ---------------------------------------------------------------------------
# 11. Recursive no-leak guarantees
# ---------------------------------------------------------------------------

class RecursiveNoLeakTests(HubUiFixture):
    def malicious_initiator(self):
        return {"agent": "codex",
                "session_id": "/Users/evil/workspace/secret-ws "
                              "COOKIE=monster; session=stolen",
                "parent_session_id": "sess_FORGED_PARENT_9"}

    def test_delegate_events_leak_nothing_from_prompt_or_initiator(self):
        _ui_events, _ui_projection = _ui_modules()
        registration, _body = self.contract()
        with collector_emitter() as (emitter, events):
            result = self.delegate(registration["contract_id"],
                                   initiator=self.malicious_initiator(),
                                   ui_emitter=emitter)
        self.assertEqual(result["outcome"], "delivered")
        self.assertTrue(events)
        for event in events:
            self.assertEqual(ui_events_find_fragments(event), [],
                             json.dumps(event))
        accepted = events[0]
        self.assertIsNone(accepted["data"]["initiator_session_id"])
        self.assertEqual(accepted["data"]["reason_code"],
                         "initiator_unverified")
        self.assertNotIn("parent_session_id", accepted["data"])

    def test_runner_error_text_becomes_a_safe_code_only(self):
        _ui_events, _ui_projection = _ui_modules()
        _registration, _body = self.contract()
        boom_marker = "PROMPT-MARKER-DO-NOT-LEAK /Users/evil/workspace/secret-ws"

        def exploding_runner(codex_path, workspace, prompt, output_schema,
                             evidence_dir, timeout_s, model=None):
            raise RuntimeError(boom_marker)

        from zcode_supervisor import hub_server
        with collector_emitter() as (emitter, events):
            result = asyncio.run(hub_server.delegate_impl(
                self.config, self.evidence_root, "hub-ui-1",
                self.malicious_initiator(), codex_path=str(self.fake_codex),
                codex_runner=exploding_runner, ui_emitter=emitter))
        self.assertEqual(result["outcome"], "runner-error")
        completed = next(e for e in events
                         if e["kind"] == "task.completed")
        self.assertEqual(completed["error"], "RuntimeError")
        self.assertEqual(ui_events_find_fragments(completed), [])

    def test_malicious_authority_inputs_never_reach_events(self):
        _, ui_projection = _ui_modules()
        temp = tempfile.TemporaryDirectory(prefix="ui leak ")
        self.addCleanup(temp.cleanup)
        root = Path(temp.name).resolve()
        workspace = root / "ws"
        workspace.mkdir()
        (workspace / "TASK.md").write_text("t", encoding="utf-8")
        packet = {"workspace": str(workspace), "prompt": MARKERS[0],
                  "allowed_files": [],
                  "commands": [{"id": "cmd_ok", "command": "echo ok",
                                "max_uses": 1, "max_timeout_ms": 5000}]}
        packet_path = root / "packet.json"
        packet_path.write_text(json.dumps(packet), encoding="utf-8")
        malicious = {"sessionId": "sess_1", "turnId": "turn_1",
                     "requestId": "rq_x", "toolCallId": "tc_x",
                     "toolName": "Bash",
                     "input": {"command": "echo " + MARKERS[0],
                               "timeout": 1000,
                               "junk": {"nested": [MARKERS[1], MARKERS[2],
                                                   {"b64": MARKERS[6]}]}}}
        with collector_emitter() as (emitter, events):
            wired = Authority(packet, packet_path, inventory(workspace),
                              ui_emit=ui_projection.authority_hook(
                                  emitter, "task_leak", "zcode", "resident"))
            wired.session_id = "sess_1"
            wired.turn_id = "turn_1"
            decision = wired.decide(malicious)
        self.assertEqual(decision["decision"], "deny")
        for event in events:
            self.assertEqual(ui_events_find_fragments(event), [],
                             json.dumps(event))




# ---------------------------------------------------------------------------
# 12. Review-fix round 2 (spec-review + quality-review + controller round 1)
# ---------------------------------------------------------------------------

FAKE_EXEC_CODEX = '''#!/usr/bin/env python3
"""Fake codex exec child: writes the -o last-message file, prints one
NDJSON event line, exits 0 ("hang" mode ignores stdin until terminated)."""
import json, sys, time

MODE = "ok"
if "--hang" in sys.argv:
    MODE = "hang"
if MODE == "hang":
    time.sleep(60)
    sys.exit(0)
out = None
for index, arg in enumerate(sys.argv):
    if arg == "-o" and index + 1 < len(sys.argv):
        out = sys.argv[index + 1]
if out:
    with open(out, "w") as handle:
        json.dump({"answer": "ok"}, handle)
print(json.dumps({"type": "thread.started", "thread_id": "th_cold_1"}))
sys.exit(0)
'''


class TrustedErrorTests(unittest.TestCase):
    """Item 1: no native error text/base64 may enter task.completed.error."""

    def _complete(self, emitter, error_code):
        ui_projection = _ui_modules()[1]
        return ui_projection.project_task_completed(
            emitter, task_id="t_e", worker="codex", mode="exec",
            status="failed", outcome="runner-error", delivery_ok=None,
            duration_ms=1.0, error_code=error_code)

    def test_trusted_type_names_only(self):
        ui_events, _ = _ui_modules()
        self.assertIn("ValueError", ui_events.TRUSTED_ERROR_CODES)
        with collector_emitter() as (emitter, events):
            self._complete(emitter, "ValueError")
        self.assertEqual(events[0]["error"], "ValueError")
        self.assertEqual(events[0]["data"], {})

    def test_untrusted_text_becomes_null_with_fixed_reason(self):
        import base64
        secrets = ["SECRETMARKER",
                   base64.b64encode(b"SECRETMARKER").decode(),
                   "ValueError: /Users/evil PROMPT-MARKER-DO-NOT-LEAK",
                   "x" * 300, "", None]
        for bad in secrets:
            if bad is None:
                continue  # no error at all: no reason code either
            with collector_emitter() as (emitter, events):
                self._complete(emitter, bad)
            self.assertIsNone(events[0]["error"], bad)
            self.assertEqual(events[0]["data"].get("reason_code"),
                             "error_unclassified", bad)
            self.assertEqual(ui_events_find_fragments(events[0]), [], bad)
        with collector_emitter() as (emitter, events):
            self._complete(emitter, None)
        self.assertIsNone(events[0]["error"])
        self.assertEqual(events[0]["data"], {})

    def test_validator_rejects_untrusted_error_tokens(self):
        ui_events, _ = _ui_modules()
        stream = ui_events.UiEventStream("hub1", 1)
        event = stream.build("task.completed", "t1", task_id="t1",
                             worker="zcode", mode="cold", status="completed",
                             outcome="delivered", duration_ms=1.0)
        for bad in ("SECRETMARKER", "aGVsbG8=", "Value Error"):
            with self.assertRaises(ui_events.UiEventError):
                ui_events.validate_event({**event, "error": bad})


class SubscriberIsolationTests(HubUiFixture):
    """Item 2: subscribers are untrusted; authoritative ids survive them."""

    @staticmethod
    def evil(_event):
        _event.clear()
        _event["kind"] = "tampered"
        raise RuntimeError("subscriber exploded")

    def test_malicious_subscriber_cannot_break_announced_chain(self):
        _, ui_projection = _ui_modules()
        collected: list = []
        emitter = ui_projection.UiEmitter(enabled=True,
                                          subscribers=[self.evil,
                                                       collected.append])
        accepted = ui_projection.project_task_accepted(
            emitter, ui_projection.UiInitiatorResolver(), task_id="task_iso",
            worker="zcode", mode="cold", contract_id="c1",
            contract_digest="a" * 64, created_at="2026-10-24T00:00:00Z",
            initiator={"agent": "zcode", "session_id": "sess_x"})
        self.assertEqual([e["kind"] for e in collected],
                         ["task.accepted", "stream.announced"])
        self.assertEqual(collected[1]["data"]["accepted_event_id"],
                         collected[0]["event_id"])
        # the authoritative envelope the projection kept is intact
        self.assertEqual(accepted["kind"], "task.accepted")
        self.assertTrue(accepted["event_id"])

    def test_delegate_survives_malicious_subscriber(self):
        _, ui_projection = _ui_modules()
        registration, _body = self.contract()
        emitter = ui_projection.UiEmitter(enabled=True,
                                          subscribers=[self.evil])
        result = self.delegate(registration["contract_id"], ui_emitter=emitter)
        self.assertEqual(result["outcome"], "delivered")
        record = json.loads(
            (self.state_dir / "hub" / "tasks" /
             f"{result['task_id']}.json").read_text())
        self.assertEqual(record["status"], "completed")
        self.assertTrue(emitter.failures)


class SeqCommitTests(unittest.TestCase):
    """Item 3: a failed build must not consume a sequence number."""

    def test_validation_failure_leaves_stream_state_unchanged(self):
        ui_events, _ = _ui_modules()
        stream = ui_events.UiEventStream("hub1", 1)
        first = stream.build("turn.started", "t1", session_id="sess_1",
                             turn_id="turn_1")
        with self.assertRaises(ui_events.UiEventError):
            stream.build("turn.started", "t1", session_id="sess_1",
                         turn_id="t", source_event_seq="bad")
        with self.assertRaises(ui_events.UiEventError):
            stream.build("turn.started", "t1", session_id="sess_1",
                         turn_id="t", data={"not_allowlisted": "x"})
        second = stream.build("turn.started", "t1", session_id="sess_1",
                              turn_id="turn_1")
        self.assertEqual([first["seq"], second["seq"]], [1, 2])

    def test_single_writer_continuous_seq_across_many_builds(self):
        ui_events, _ = _ui_modules()
        stream = ui_events.UiEventStream("hub1", 1)
        seqs = [stream.build("turn.started", "t1", session_id="s",
                             turn_id="t")["seq"]
                for _ in range(25)]
        self.assertEqual(seqs, list(range(1, 26)))


class StrictSchemaTests(unittest.TestCase):
    """Item 9: strict v/epoch/UTC-time/finite/per-kind required validation."""

    def _base(self):
        ui_events, _ = _ui_modules()
        stream = ui_events.UiEventStream("hub1", 1)
        return ui_events, stream, stream.build(
            "task.completed", "t1", task_id="t1", worker="zcode", mode="cold",
            status="completed", outcome="delivered", duration_ms=1.0)

    def test_v_must_be_exact_integer_one(self):
        ui_events, _stream, base = self._base()
        for bad in (True, 1.0, "1", 2, None):
            with self.assertRaises(ui_events.UiEventError):
                ui_events.validate_event({**base, "v": bad})

    def test_epoch_must_be_ascii_digits(self):
        ui_events, _stream, base = self._base()
        fullwidth = base["event_id"].replace("/1/", "/\uff11/")
        with self.assertRaises(ui_events.UiEventError):
            ui_events.validate_event({**base, "event_id": fullwidth})

    def test_observed_at_must_be_real_utc_calendar_time(self):
        ui_events, _stream, base = self._base()
        for bad in ("2026-13-45T25:61:61.000000Z",
                    "2026-10-24T00:00:00.000000+00:00",
                    "2026-02-30T00:00:00.000000Z"):
            with self.assertRaises(ui_events.UiEventError):
                ui_events.validate_event({**base, "observed_at": bad})

    def test_duration_must_be_finite_and_bounded(self):
        ui_events, _stream, base = self._base()
        for bad in (float("inf"), float("-inf"), float("nan"),
                    10 ** 400, True):
            with self.assertRaises(ui_events.UiEventError):
                ui_events.validate_event({**base, "duration_ms": bad})

    def test_events_serialize_with_allow_nan_false(self):
        _ui_events, _stream, base = self._base()
        json.dumps(base, allow_nan=False)  # every valid event is strict JSON

    def test_per_kind_required_fields_are_enforced(self):
        ui_events, stream = self._base()[:2]
        cases = [
            ("task.accepted", {"task_id": "t", "worker": "zcode",
                               "mode": "cold"}, {}),
            ("stream.announced", {"task_id": "t", "worker": "zcode",
                                  "mode": "cold"}, {}),
            ("session.bound", {}, {"locator_type": "resident"}),
            ("turn.started", {}, {}),
            ("tool.scheduled", {"session_id": "s", "turn_id": "u"},
             {"tool_name": "Bash"}),
            ("approval.decided", {"request_id": "r", "tool_call_id": "c"},
             {"tool_name": "Bash"}),
            ("process.exited", {"status": "exited", "worker": "zcode"},
             {}),
            ("task.completed", {"task_id": "t", "worker": "zcode",
                                "mode": "cold", "status": "completed",
                                "outcome": "delivered"}, {}),
        ]
        for kind, envelope, data in cases:
            with self.assertRaises(ui_events.UiEventError, msg=kind):
                stream.build(kind, "t1", data=data, **envelope)

    def test_tool_projection_omits_events_without_reliable_ids(self):
        _ui_events, ui_projection = _ui_modules()
        with collector_emitter() as (emitter, events):
            ui_projection.tool_event(emitter, "t1", "zcode", "resident",
                                     "sess_1", "turn_1", None, "scheduled",
                                     "Bash")
            ui_projection.tool_event(emitter, "t1", "zcode", "resident",
                                     "sess_1", None, "call_1", "scheduled",
                                     "Bash")
        self.assertEqual(events, [])



class BridgeApprovalProjectionTests(unittest.TestCase):
    """Item 4: bridge-level paired projection with the APPLIED decision."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="ui bridge ")
        self.addCleanup(self.temp.cleanup)

    def bridge(self, emitter, approved=("cat A.md",)):
        from zcode_supervisor.codex_appserver import ApprovalBridge
        _ui_events, ui_projection = _ui_modules()
        bridge = ApprovalBridge(
            SimpleContractAuthority(list(approved), []),
            "th_bridge_1",
            ui=ui_projection.approval_hook(emitter, "task_bridge", "codex",
                                           "app-server"))
        bridge.turn_id = "turn_bridge_1"
        return bridge

    def command_params(self, command, **extra):
        params = {"threadId": "th_bridge_1", "turnId": "turn_bridge_1",
                  "itemId": "item_cmd_x", "startedAtMs": 1, "kind": "command",
                  "cwd": "/ws", "command": f"/bin/zsh -lc '{command}'",
                  "availableDecisions": ["accept", "decline"]}
        params.update(extra)
        return params

    def test_persistent_variant_downgrade_is_projected_as_deny(self):
        with collector_emitter() as (emitter, events):
            bridge = self.bridge(emitter)
            response = bridge.handle(
                "item/commandExecution/requestApproval",
                self.command_params(
                    "cat A.md",
                    proposedExecpolicyAmendment=["cat A.md*"],
                    availableDecisions=["acceptForSession", "decline"]))
        self.assertEqual(response["decision"], "decline")  # the wire truth
        self.assertEqual([e["kind"] for e in events],
                         ["approval.requested", "approval.decided"])
        decided = events[1]
        self.assertEqual(decided["data"]["decision"], "deny")
        self.assertEqual(decided["data"]["requested_variant"],
                         "acceptWithExecpolicyAmendment")
        self.assertEqual(events[0]["data"]["requested_variant"],
                         "acceptWithExecpolicyAmendment")
        # consistent with the native authority evidence record
        self.assertEqual(bridge.decisions[-1]["decision"], "deny")

    def test_every_handle_emits_exactly_one_pair(self):
        with collector_emitter() as (emitter, events):
            bridge = self.bridge(emitter)
            bridge.handle("item/commandExecution/requestApproval",
                          self.command_params("cat A.md"))
            bridge.handle("item/commandExecution/requestApproval",
                          self.command_params("rm -rf /"))
            bridge.handle("item/commandExecution/requestApproval",
                          {"threadId": "th_bridge_1", "itemId": "i_empty",
                           "startedAtMs": 1, "kind": "command", "cwd": "/ws",
                           "command": ""})
            bridge.handle("item/permissions/requestApproval",
                          {"threadId": "th_bridge_1",
                           "turnId": "turn_bridge_1",
                           "itemId": "i_perm", "permissions": {}})
        kinds = [e["kind"] for e in events]
        self.assertEqual(kinds.count("approval.requested"), 4)
        self.assertEqual(kinds.count("approval.decided"), 4)
        self.assertEqual(kinds[0::2], ["approval.requested"] * 4)
        for index in range(0, len(events), 2):
            requested, decided = events[index], events[index + 1]
            self.assertEqual(requested["request_id"],
                             decided["request_id"])
            self.assertIsNotNone(decided["data"]["decision"])
            self.assertEqual(requested["session_id"], "th_bridge_1")
            self.assertEqual(decided["turn_id"], "turn_bridge_1")


class DriverAmendProjectionTests(unittest.TestCase):
    """Item 4 driver level: applied deny + requested_variant through the
    production driver on the fake app-server (amend mode)."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="ui amend ")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.evidence = self.root / "evidence"
        self.evidence.mkdir()

    def test_driver_projects_applied_decision_with_variant(self):
        _, ui_projection = _ui_modules()
        events: list = []
        emitter = ui_projection.UiEmitter(enabled=True,
                                          subscribers=[events.append])
        client = CodexAppServerClient(
            write_fake_codex(self.root, "amend"), self.evidence / "amend",
            env={"FAKE_UI_APPSERVER_MODE": "amend"})

        async def go():
            await client.start()
            driver = CodexAppServerDriver(client, ui_emitter=emitter,
                                          task_id="task_amend")
            receipt = await driver.run_task(
                SimpleContractAuthority(["cat TASK.md"], []),
                self.root, "p", timeout_s=10)
            await client.close()
            return receipt

        receipt = asyncio.run(go())
        self.assertEqual(receipt["approvals"][0]["decision"], "deny")
        decided = next(e for e in events
                       if e["kind"] == "approval.decided")
        self.assertEqual(decided["data"]["decision"], "deny")
        self.assertEqual(decided["data"]["requested_variant"],
                         "acceptWithExecpolicyAmendment")


class ColdCodexExitTests(unittest.TestCase):
    """Item 5 (codex half): real child wait -> exactly one cold exit."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="ui cold codex ")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.fake = self.root / "fake-codex-exec"
        self.fake.write_text(FAKE_EXEC_CODEX, encoding="utf-8")
        self.fake.chmod(0o755)
        self.workspace = self.root / "ws"
        self.workspace.mkdir()
        self.evidence = self.root / "evidence"
        self.schema = {"type": "object",
                       "properties": {"answer": {"type": "string"}},
                       "required": ["answer"], "additionalProperties": False}

    def _run(self, sink_factory, timeout_s=20, hang=False):
        from zcode_supervisor.codex_driver import run_readonly
        _ui_events, ui_projection = _ui_modules()
        events: list = []
        emitter = ui_projection.UiEmitter(enabled=True,
                                          subscribers=[events.append])
        script = self.fake
        if hang:
            script = self.root / "fake-codex-exec-hang"
            script.write_text(
                FAKE_EXEC_CODEX.replace('MODE = "ok"', 'MODE = "hang"'),
                encoding="utf-8")
            script.chmod(0o755)
        receipt = run_readonly(
            str(script), self.workspace, "prompt", self.schema,
            self.evidence, timeout_s=timeout_s,
            exit_event_sink=sink_factory(emitter))
        return events, receipt

    def test_normal_exit_projects_one_cold_process_exited(self):
        ui_projection = _ui_modules()[1]
        events, receipt = self._run(
            lambda emitter: ui_projection.make_exit_sink(
                emitter, "cold", "codex"))
        self.assertEqual(receipt.outcome, "delivered")
        exits = [e for e in events if e["kind"] == "process.exited"]
        self.assertEqual(len(exits), 1)
        self.assertEqual(exits[0]["data"]["process_scope"], "cold")
        self.assertEqual(exits[0]["data"]["exit_code"], 0)
        self.assertIsInstance(exits[0]["data"]["process_generation"], int)

    def test_terminated_child_still_projects_exactly_one_exit(self):
        ui_projection = _ui_modules()[1]
        events, receipt = self._run(
            lambda emitter: ui_projection.make_exit_sink(
                emitter, "cold", "codex"), timeout_s=1.0, hang=True)
        self.assertEqual(receipt.outcome, "timeout")
        exits = [e for e in events if e["kind"] == "process.exited"]
        self.assertEqual(len(exits), 1)
        self.assertIsInstance(exits[0]["data"]["exit_code"], int)


class ColdZCodeWiringTests(unittest.TestCase):
    """Item 5 (zcode half): protocol.execute wires cold Authority hook and
    ExitProjection sink (fake client injected at the ProtocolClient seam)."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="ui cold zcode ")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        for name in ("work", "state", "cache"):
            (self.root / name).mkdir()
        self.config = SimpleNamespace(work_root=self.root / "work",
                                      state_dir=self.root / "state",
                                      cache_dir=self.root / "cache",
                                      notify=False)
        self.workspace = self.root / "work" / "ws"
        self.workspace.mkdir()
        (self.workspace / "TASK.md").write_text("t", encoding="utf-8")

    def test_cold_zcode_authority_and_exit_projection_are_wired(self):
        import zcode_supervisor.protocol as protocol_module
        _ui_events, ui_projection = _ui_modules()
        events: list = []
        emitter = ui_projection.UiEmitter(enabled=True,
                                          subscribers=[events.append])
        packet = {"workspace": str(self.workspace),
                  "prompt": "PROMPT-MARKER-DO-NOT-LEAK run",
                  "allowed_files": [],
                  "commands": [{"id": "cmd_cold", "command": "echo hi",
                                "max_uses": 1, "max_timeout_ms": 5000}]}
        packet_path = self.root / "packet.json"
        packet_path.write_text(json.dumps(packet), encoding="utf-8")
        out = self.root / "work" / "runCold.json"

        class FakeColdClient:
            instances = []  # noqa: RUF012 - deliberate shared test registry

            def __init__(self, workspace, output, permission, config=None,
                         orchestration=None, exit_event_sink=None):
                self.permission = permission
                self.events = []
                self.on_event = None
                self.failure = None
                self.process = SimpleNamespace(pid=77, returncode=None)
                self.generation = 1
                self.exit_projection = ExitProjection(
                    on_event=exit_event_sink)
                FakeColdClient.instances.append(self)

            async def start(self):
                pass

            async def close(self):
                self.process.returncode = 0
                self.exit_projection.observe_awaited_exit(
                    self.process.returncode, generation=self.generation)

            async def _run_turn(self, input_id):
                sid, turn = "sess_cold_1", "turn_cold_1"
                for event in [
                    {"type": "turn.started", "sessionId": sid,
                     "turnId": turn, "seq": 1,
                     "payload": {"inputId": input_id}},
                    {"type": "tool.updated", "sessionId": sid,
                     "turnId": turn, "seq": 2,
                     "payload": {"kind": "scheduled", "toolName": "Bash",
                                 "toolCallId": "call_cold",
                                 "input": {"command": "echo hi",
                                           "timeout": 1000}}},
                ]:
                    self.events.append(event)
                    if self.on_event:
                        self.on_event(event)
                await asyncio.sleep(0.1)
                self.permission({"sessionId": sid, "turnId": turn,
                                 "requestId": "req_cold",
                                 "toolCallId": "call_cold",
                                 "toolName": "Bash",
                                 "input": {"command": "echo hi",
                                           "timeout": 1000}})
                for event in [
                    {"type": "tool.updated", "sessionId": sid,
                     "turnId": turn, "seq": 3,
                     "payload": {"kind": "result", "toolCallId": "call_cold",
                                 "result": {
                                     "success": True,
                                     "perf": {"detail": {
                                         "kind": "command",
                                         "command": {"exitCode": 0,
                                                     "timedOut": False,
                                                     "status": "completed",
                                                     "runMs": 2}}}}}},
                    {"type": "turn.completed", "sessionId": sid,
                     "turnId": turn, "seq": 4,
                     "payload": {"resultType": "success"}},
                ]:
                    self.events.append(event)
                    if self.on_event:
                        self.on_event(event)

            async def request(self, method, params):
                if method == "session/create":
                    return {"session": {"sessionId": "sess_cold_1",
                                        "status": "idle"},
                            "runtime": {"pendingRequestIds": []},
                            "settings": {"model": {"current": None}}}
                if method == "session/subscribe":
                    return {"eventSeq": 0}
                if method == "session/send":
                    self._turn = asyncio.create_task(
                        self._run_turn(params["inputId"]))
                    return {}
                if method == "session/read":
                    return {"session": {"sessionId": "sess_cold_1",
                                        "status": "idle"},
                            "runtime": {"pendingRequestIds": []}}
                return {}

        original = protocol_module.ProtocolClient
        protocol_module.ProtocolClient = FakeColdClient
        self.addCleanup(lambda: setattr(protocol_module, "ProtocolClient",
                                        original))
        args = SimpleNamespace(packet=str(packet_path), out=str(out),
                               resume=None, timeout=10, quiet=True)
        delivered = asyncio.run(protocol_module.execute(args, self.config,
                                                        ui_emitter=emitter))
        self.assertTrue(delivered)
        result = json.loads(out.read_text())
        self.assertTrue(result["delivery_ok"], result.get("error"))
        kinds = [e["kind"] for e in events]
        self.assertIn("approval.requested", kinds)
        self.assertIn("approval.decided", kinds)
        decided = next(e for e in events if e["kind"] == "approval.decided")
        self.assertEqual(decided["data"]["decision"], "allow")
        self.assertEqual(decided["data"]["command_id"], "cmd_cold")
        self.assertEqual(decided["stream_id"], "runCold")
        exits = [e for e in events if e["kind"] == "process.exited"]
        self.assertEqual(len(exits), 1)
        self.assertEqual(exits[0]["data"]["process_scope"], "cold")
        self.assertEqual(exits[0]["data"]["exit_code"], 0)
        for event in events:
            self.assertEqual(ui_events_find_fragments(event), [])


class ResidentFailureTerminalTests(unittest.TestCase):
    """Item 6: resident turn.failed gets a fixed-outcome terminal event."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="ui resfail ")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        for name in ("work", "state", "cache"):
            (self.root / name).mkdir()
        self.config = SimpleNamespace(work_root=self.root / "work",
                                      state_dir=self.root / "state",
                                      cache_dir=self.root / "cache",
                                      notify=False)
        self.workspace = self.root / "work" / "ws"
        self.workspace.mkdir()

    def test_failed_turn_projects_terminal_then_keeps_failure_semantics(self):
        _, ui_projection = _ui_modules()
        events: list = []
        emitter = ui_projection.UiEmitter(enabled=True,
                                          subscribers=[events.append])

        class FailingClient(ScriptedResidentClient):
            async def _run_turn(self, input_id):
                sid, turn = "sess_ui_A", "turn_1"
                self.events.append({"type": "turn.started",
                                    "sessionId": sid, "turnId": turn,
                                    "seq": 1,
                                    "payload": {"inputId": input_id}})
                await asyncio.sleep(0.05)
                self.events.append({"type": "turn.failed",
                                    "sessionId": sid, "turnId": turn,
                                    "seq": 2,
                                    "payload": {"error":
                                                "PROMPT-MARKER-DO-NOT-LEAK "
                                                "boom"}})

        resident = ResidentZCode(
            self.config, self.root / "work",
            client_factory=lambda *args: FailingClient(*args),
            ui_emitter=emitter)
        packet_path = self.root / "packet.json"
        packet_path.write_text(json.dumps(
            {"workspace": str(self.workspace), "prompt": "p",
             "allowed_files": []}), encoding="utf-8")
        out = self.root / "work" / "runFail.json"

        async def go():
            await resident.start()
            try:
                return await resident.run_packet(packet_path, out,
                                                 timeout_s=10)
            finally:
                await resident.stop()

        result = asyncio.run(go())
        self.assertEqual(result["outcome"], "worker-failed")
        self.assertFalse(result["delivery_ok"])
        kinds = [e["kind"] for e in events]
        self.assertEqual(kinds, ["session.bound", "turn.started",
                                 "turn.completed", "session.state"])
        terminal = events[2]
        self.assertEqual(terminal["outcome"], "failed")
        self.assertEqual(terminal["source_event_seq"], 2)
        self.assertEqual(events[3]["data"]["reason_code"], "lost")
        self.assertEqual(ui_events_find_fragments(terminal), [])


class IdentitySetAttributionTests(unittest.TestCase):
    """Item 7: identity-set semantics; independent unattributed view."""

    def test_conflicting_identities_never_count_as_own(self):
        _, ui_projection = _ui_modules()
        frames = [
            {"method": "own/one", "params": {"threadId": "th_A"}},
            {"method": "conflict/x",
             "params": {"threadId": "th_A", "conversationId": "th_B"}},
            {"method": "foreign/one", "params": {"threadId": "th_B"}},
            {"method": "missing/one", "params": {}},
        ]
        own, unattributed = ui_projection.split_notification_counts(
            frames, "th_A")
        self.assertEqual(own, {"own/one": 1})
        self.assertEqual(unattributed, {"conflict/x": 1, "missing/one": 1})

    def test_identity_set_helper(self):
        _, ui_projection = _ui_modules()
        self.assertEqual(
            ui_projection.notification_identity_set(
                {"threadId": "a", "conversationId": "a", "thread_id": ""}),
            {"a"})
        self.assertEqual(
            ui_projection.notification_identity_set(
                {"threadId": "a", "conversationId": "b"}), {"a", "b"})
        self.assertEqual(ui_projection.notification_identity_set(None),
                         set())


class NotificationViewTests(unittest.TestCase):
    """Item 7 integration: receipts count own frames only; identity-less and
    conflicting frames land in the CLIENT-level (run) unattributed view —
    never in a task receipt or timeline."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="ui view ")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.evidence = self.root / "evidence"
        self.evidence.mkdir()

    def client(self, mode):
        return CodexAppServerClient(
            write_fake_codex(self.root, mode), self.evidence / mode,
            env={"FAKE_UI_APPSERVER_MODE": mode})

    def test_receipt_has_own_counts_only_and_client_holds_unattributed(self):
        async def go():
            client = self.client("single")
            try:
                await client.start()
                receipt = await CodexAppServerDriver(client).run_task(
                    SimpleContractAuthority([], []), self.root, "p",
                    timeout_s=10)
            finally:
                await client.close()
            return client, receipt

        client, receipt = asyncio.run(go())
        self.assertEqual(receipt["notification_counts"],
                         {"item/completed": 1, "turn/completed": 1})
        self.assertNotIn("notification_unattributed", receipt)
        self.assertEqual(client.notification_unattributed,
                         {"orphan/notification": 1})

    def test_conflicting_frame_lands_in_client_view_not_either_task(self):
        async def go():
            client = self.client("dual")
            try:
                await client.start()
                driver = CodexAppServerDriver(client)
                return await asyncio.gather(
                    driver.run_task(SimpleContractAuthority([], []),
                                    self.root / "wsA", "task A",
                                    timeout_s=15),
                    driver.run_task(SimpleContractAuthority([], []),
                                    self.root / "wsB", "task B",
                                    timeout_s=15))
            finally:
                await client.close()

        receipt_a, receipt_b = asyncio.run(go())
        for receipt in (receipt_a, receipt_b):
            self.assertEqual(receipt["notification_counts"],
                             {"item/completed": 1, "turn/completed": 1})
            self.assertNotIn("notification_unattributed", receipt)


class NotificationCutoffSoakTests(unittest.TestCase):
    """Item 8: deterministic cutoff via the thread/read barrier — 60 runs."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="ui soak ")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.evidence = self.root / "evidence"
        self.evidence.mkdir()

    def test_sixty_runs_are_deterministic(self):
        async def one_round():
            client = CodexAppServerClient(
                write_fake_codex(self.root, "single"),
                self.evidence / f"s{index}",
                env={"FAKE_UI_APPSERVER_MODE": "single"})
            try:
                await client.start()
                receipt = await CodexAppServerDriver(client).run_task(
                    SimpleContractAuthority([], []), self.root, "p",
                    timeout_s=10)
            finally:
                await client.close()
            return receipt

        for index in range(60):
            receipt = asyncio.run(one_round())
            self.assertEqual(
                receipt["notification_counts"],
                {"item/completed": 1, "turn/completed": 1},
                f"round {index}: {receipt['notification_counts']}")
            self.assertNotIn("notification_unattributed", receipt)


class ExitCallbackChainTests(unittest.TestCase):
    """Item 10: disabled emitter leaves custom projections untouched;
    enabled chains the prior callback with the UI sink (once each)."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="ui chain ")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        (self.root / "work").mkdir()
        self.config = SimpleNamespace(work_root=self.root / "work",
                                      state_dir=self.root / "state",
                                      cache_dir=self.root / "cache",
                                      notify=False)

    def _resident(self, emitter):
        holder = {"records": []}

        class ChainedClient:
            def __init__(self, workspace, output, permission, config=None):
                self.generation = 1
                self.events = []
                self.failure = None
                self.process = SimpleNamespace(pid=91, returncode=None)
                self.exit_projection = ExitProjection(
                    on_event=holder["records"].append)

            async def start(self):
                pass

            async def close(self):
                self.process.returncode = 0
                self.exit_projection.observe_awaited_exit(
                    self.process.returncode, generation=self.generation)

        def factory(*args):
            client = ChainedClient(*args)
            holder["client"] = client
            return client

        resident = ResidentZCode(self.config, self.root / "work",
                                 client_factory=factory,
                                 ui_emitter=emitter)
        return resident, holder

    def test_disabled_emitter_leaves_custom_projection_untouched(self):
        _, _ui_modules_ref = _ui_modules()
        ui_projection = _ui_modules_ref
        emitter = ui_projection.UiEmitter(enabled=True, subscribers=[])
        emitter.enabled = False  # the CZS_UI_EVENTS=0 rollback state
        resident, holder = self._resident(emitter)
        original_projection = None

        async def go():
            await resident.start()
            nonlocal original_projection
            original_projection = holder["client"].exit_projection
            await resident.stop()
            await resident.stop()

        asyncio.run(go())
        self.assertIs(holder["client"].exit_projection, original_projection)
        self.assertEqual(len(holder["records"]), 1)  # custom callback intact

    def test_enabled_emitter_chains_prior_callback_with_ui_sink(self):
        ui_events, ui_projection = _ui_modules()
        events: list = []
        emitter = ui_projection.UiEmitter(enabled=True,
                                          subscribers=[events.append])
        resident, holder = self._resident(emitter)

        async def go():
            await resident.start()
            await resident.stop()
            await resident.stop()

        asyncio.run(go())
        self.assertEqual(len(holder["records"]), 1)  # prior callback, once
        exits = [e for e in events if e["kind"] == "process.exited"]
        self.assertEqual(len(exits), 1)
        self.assertEqual(exits[0]["data"]["process_scope"],
                         "shared-resident")
        ui_events.validate_event(exits[0])




# ---------------------------------------------------------------------------
# 13. Approval pairs, client aggregate, cold ZCode generations
# ---------------------------------------------------------------------------


class Round3ApprovalPairTests(unittest.TestCase):
    """Single-pair everywhere, no stale command_id, schema keys."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="ui r3 approval ")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.workspace = self.root / "ws"
        self.workspace.mkdir()
        (self.workspace / "TASK.md").write_text("t", encoding="utf-8")
        self.packet = {
            "workspace": str(self.workspace),
            "prompt": "PROMPT-MARKER-DO-NOT-LEAK",
            "allowed_files": [],
            "commands": [{"id": "cmd_ok", "command": "echo ok",
                          "max_uses": 8, "max_timeout_ms": 120000}],
        }
        self.packet_path = self.root / "packet.json"
        self.packet_path.write_text(json.dumps(self.packet), encoding="utf-8")

    def bridge(self, emitter):
        from zcode_supervisor.codex_appserver import ApprovalBridge
        authority = Authority(self.packet, self.packet_path,
                              inventory(self.workspace))
        authority.session_id = "th_r3"
        authority.turn_id = "turn_r3"
        ui_projection = _ui_modules()[1]
        bridge = ApprovalBridge(
            authority, "th_r3",
            ui=ui_projection.approval_hook(emitter, "task_r3", "codex",
                                           "app-server"))
        bridge.turn_id = "turn_r3"
        return bridge

    def test_persistent_request_routes_through_authority_single_pair(self):
        with collector_emitter() as (emitter, events):
            bridge = self.bridge(emitter)
            decisions_before = len(bridge.authority.decisions)
            response = bridge.handle("item/commandExecution/requestApproval",
                                     {"command": "echo ok",
                                      "grantRoot": "/persistent"})
        # wire + applied: persistent variant downgraded to a per-request deny
        self.assertEqual(response["decision"], "decline")
        self.assertEqual(bridge.decisions[-1]["decision"], "deny")
        self.assertEqual(bridge.decisions[-1]["requested_variant"],
                         "acceptForSession")
        # the Authority really decided it (handle semaphore path kept)
        self.assertEqual(len(bridge.authority.decisions), decisions_before + 1)
        self.assertEqual([e["kind"] for e in events],
                         ["approval.requested", "approval.decided"])
        decided = events[1]
        self.assertEqual(decided["data"]["decision"], "deny")
        self.assertEqual(decided["data"]["requested_variant"],
                         "acceptForSession")
        self.assertEqual(decided["data"]["reason_code"],
                         "denied_persistent_variant")
        self.assertEqual(events[0]["request_id"], decided["request_id"])

    def test_early_deny_after_prior_decision_is_pair_with_null_command(self):
        with collector_emitter() as (emitter, events):
            bridge = self.bridge(emitter)
            # a prior ALLOWED decision exists (its cmd_ok must not leak into
            # later synthetic denies)
            bridge.handle("item/commandExecution/requestApproval",
                          {"threadId": "th_r3", "turnId": "turn_r3",
                           "itemId": "item_prior", "command": "echo ok",
                           "availableDecisions": ["accept", "decline"]})
            prior_count = len(events)
            self.assertEqual(events[prior_count - 1]["data"]["command_id"],
                             "cmd_ok")
            # early deny: no command text at all
            bridge.handle("item/commandExecution/requestApproval", {})
        new_events = events[prior_count:]
        self.assertEqual([e["kind"] for e in new_events],
                         ["approval.requested", "approval.decided"])
        decided = new_events[1]
        self.assertIsNone(decided["data"]["command_id"])
        self.assertEqual(decided["data"]["reason_code"], "denied_early")
        self.assertEqual(new_events[0]["request_id"], decided["request_id"])
        self.assertEqual(new_events[0]["tool_call_id"],
                         decided["tool_call_id"])

    def test_empty_file_change_is_exactly_one_pair(self):
        with collector_emitter() as (emitter, events):
            bridge = self.bridge(emitter)
            response = bridge.handle("item/fileChange/requestApproval",
                                     {"threadId": "th_r3",
                                      "turnId": "turn_r3"})
        # v2 wire deny contract: exactly worker_response('codex-v2', deny)
        # (the alert is evidence-record-only; the wire stays Response-shaped)
        from zcode_supervisor.authority import worker_response
        self.assertEqual(response,
                         worker_response("codex-v2", {"decision": "deny"}))
        self.assertEqual([e["kind"] for e in events],
                         ["approval.requested", "approval.decided"])
        requested, decided = events
        self.assertEqual(requested["request_id"], decided["request_id"])
        self.assertEqual(requested["tool_call_id"],
                         decided["tool_call_id"])
        self.assertIsNone(decided["data"]["command_id"])
        self.assertEqual(decided["data"]["reason_code"], "denied_early")
        self.assertEqual(requested["data"]["tool_name"], "Write")

    def test_approval_and_tool_schema_carry_plan_fields(self):
        ui_events, ui_projection = _ui_modules()
        events: list = []
        emitter = ui_projection.UiEmitter(enabled=True,
                                          subscribers=[events.append])
        bridge = self.bridge(emitter)
        bridge.handle("item/commandExecution/requestApproval",
                      {"threadId": "th_r3", "turnId": "turn_r3",
                       "itemId": "item_schema", "command": "echo ok",
                       "availableDecisions": ["accept", "decline"]})
        requested, decided = events
        # requested carries command_id as a nullable key (no decision yet)
        self.assertIn("command_id", requested["data"])
        self.assertIsNone(requested["data"]["command_id"])
        # decided carries the CURRENT decision + a fixed reason code
        self.assertEqual(decided["data"]["decision"], "allow")
        self.assertEqual(decided["data"]["reason_code"],
                         "allowed_by_contract")
        # tool events carry the PLAN tool fields as nullable keys
        tool_events: list = []
        tool_emitter = ui_projection.UiEmitter(
            enabled=True, subscribers=[tool_events.append])
        ui_projection.tool_event(tool_emitter, "t", "zcode", "resident",
                                 "sess_1", "turn_1", "call_1", "scheduled",
                                 "Bash", status="scheduled")
        ui_projection.tool_event(tool_emitter, "t", "zcode", "resident",
                                 "sess_1", "turn_1", "call_1", "completed",
                                 "Bash", status="result")
        scheduled, completed = tool_events
        for key in ("command_id", "read_only"):
            self.assertIn(key, scheduled["data"])
            self.assertIn(key, completed["data"])
        for key in ("exit_code", "timed_out"):
            self.assertIn(key, completed["data"])

    def test_unknown_reason_code_is_rejected(self):
        ui_events, _ = _ui_modules()
        stream = ui_events.UiEventStream("hub1", 1)
        with self.assertRaises(ui_events.UiEventError):
            stream.build("approval.decided", "t1", request_id="r",
                         tool_call_id="c", session_id="s",
                         data={"tool_name": "Bash", "decision": "deny",
                               "reason_code": "free-text-reason"})
        with self.assertRaises(ui_events.UiEventError):
            stream.build("tool.completed", "t1", session_id="s",
                         turn_id="u", tool_call_id="c",
                         data={"tool_name": "Bash"})  # missing tool facts


class Round3ClientAggregateTests(unittest.TestCase):
    """The client aggregate counts each PHYSICAL frame exactly once."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="ui r3 aggregate ")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.evidence = self.root / "evidence"
        self.evidence.mkdir()

    def client(self, mode, name):
        return CodexAppServerClient(
            write_fake_codex(self.root, mode), self.evidence / name,
            env={"FAKE_UI_APPSERVER_MODE": mode})

    def test_sequential_tasks_count_each_new_orphan_once(self):
        async def go():
            client = self.client("seq", "seqmode")
            receipts = []
            try:
                await client.start()
                for workspace in (self.root / "wsA", self.root / "wsB"):
                    workspace.mkdir(exist_ok=True)
                    receipts.append(
                        await CodexAppServerDriver(client).run_task(
                            SimpleContractAuthority([], []),
                            workspace, "p", timeout_s=10))
            finally:
                await client.close()
            return client, receipts

        client, receipts = asyncio.run(go())
        # two runs -> two physical orphan frames on the shared client
        self.assertEqual(client.notification_unattributed,
                         {"orphan/notification": 2})
        for receipt in receipts:
            self.assertEqual(receipt["notification_counts"],
                             {"item/completed": 1, "turn/completed": 1})

    def test_concurrent_tasks_share_one_orphan_frame_once(self):
        async def go():
            client = self.client("dual", "conc")
            try:
                await client.start()
                driver = CodexAppServerDriver(client)
                receipts = await asyncio.gather(
                    driver.run_task(SimpleContractAuthority([], []),
                                    self.root / "wsA", "task A",
                                    timeout_s=15),
                    driver.run_task(SimpleContractAuthority([], []),
                                    self.root / "wsB", "task B",
                                    timeout_s=15))
            finally:
                await client.close()
            return client, receipts

        client, receipts = asyncio.run(go())
        # the fake fires ONE identity-less frame total; both tasks scan the
        # shared list, but the aggregate must count the physical frame once
        self.assertEqual(client.notification_unattributed,
                         {"orphan/notification": 1})
        for receipt in receipts:
            self.assertEqual(receipt["notification_counts"],
                             {"item/completed": 1, "turn/completed": 1})

    def test_fresh_client_each_run_aggregates_exactly_one(self):
        async def one_round(index):
            client = self.client("single", f"f{index}")
            try:
                await client.start()
                await CodexAppServerDriver(client).run_task(
                    SimpleContractAuthority([], []), self.root, "p",
                    timeout_s=10)
            finally:
                await client.close()
            return client.notification_unattributed

        for index in range(20):
            self.assertEqual(asyncio.run(one_round(index)),
                             {"orphan/notification": 1}, f"round {index}")


class AwaitedProcess:
    """Minimal child double: a REAL awaited wait per close() gradient call."""

    def __init__(self, returncode=0):
        self.returncode = returncode
        self.stdin = None
        self.wait_calls = 0

    async def wait(self):
        self.wait_calls += 1
        if self.returncode is None:
            self.returncode = 0
        return self.returncode


class Round3ColdZcodeGenerationTests(unittest.TestCase):
    """Default cold ZCode integer generation, once per process."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="ui r3 cold ")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()

    def test_default_cold_client_has_integer_generation_and_projects_once(self):
        from zcode_supervisor.protocol import ProtocolClient
        ui_events, ui_projection = _ui_modules()
        events: list = []
        emitter = ui_projection.UiEmitter(enabled=True,
                                          subscribers=[events.append])
        client = ProtocolClient(
            self.root, self.root / "run.json", None,
            exit_event_sink=ui_projection.make_exit_sink(
                emitter, "cold", "zcode"))
        # production cold default: a NON-NULL integer generation (P0.3
        # per-process identity semantics) without any external stamping
        self.assertIsInstance(client.generation, int)
        self.assertNotIsInstance(client.generation, bool)
        generation = client.generation
        client.process = AwaitedProcess(0)

        async def go():
            await client.close()
            await client.close()  # duplicate close adds nothing

        asyncio.run(go())
        self.assertEqual(len(client.exit_projection.emitted), 1)
        self.assertEqual(client.exit_projection.emitted[0]["generation"],
                         generation)
        exits = [e for e in events if e["kind"] == "process.exited"]
        self.assertEqual(len(exits), 1)
        self.assertEqual(exits[0]["data"]["process_generation"], generation)
        self.assertEqual(exits[0]["data"]["process_scope"], "cold")
        ui_events.validate_event(exits[0])

    def test_sink_still_omits_unstamped_exits(self):
        from zcode_supervisor.protocol import ProtocolClient
        _ui_events, ui_projection = _ui_modules()
        events: list = []
        emitter = ui_projection.UiEmitter(enabled=True,
                                          subscribers=[events.append])
        # a bare client WITHOUT the UI sink keeps generation None (test
        # doubles / shared-resident stamping own the identity there)
        client = ProtocolClient(self.root, self.root / "bare.json", None)
        self.assertIsNone(client.generation)
        client.process = AwaitedProcess(0)
        sink = ui_projection.make_exit_sink(emitter, "cold", "zcode")
        sink(client.exit_projection.observe_awaited_exit(
            0, generation=client.generation))
        self.assertEqual([e for e in events if e["kind"] == "process.exited"],
                         [])




# ---------------------------------------------------------------------------
# 14. Round 4: cancelled/exception exits fold physical unattributed frames
# ---------------------------------------------------------------------------


def write_delayed_read_fake(root: Path, mode: str) -> Path:
    """Fake app-server whose thread/read stalls, so a task stays pending at
    the evidence barrier AFTER physical frames have already arrived (the
    quality probe's exact injection)."""
    path = root / f"fake-codex-{mode}-delay"
    source = write_fake_codex(root, mode).read_text(encoding="utf-8")
    needle = '    elif method == "thread/read":\n'
    replacement = ('    elif method == "thread/read":\n'
                   '        __import__("time").sleep(60)\n')
    if needle not in source:
        raise AssertionError("fake thread/read injection point changed")
    path.write_text(source.replace(needle, replacement, 1), encoding="utf-8")
    path.chmod(0o755)
    return path


class Round4CancelMergeTests(unittest.TestCase):
    """Arrived physical orphan frames must survive cancel/exception exits
    exactly once (idempotent per-client cursor; no new waits on those paths).

    Pinned to the explicit CZS_EVENT_ROUTER=legacy rollback (WP-B default
    flip, 2026-10-30): these tests observe the shared notification STORE
    (poll for the retained physical frame; the _BarrierRequest stub also
    injects frames straight into it), which is the legacy retention
    shape. The fold-once semantics they verify are mode-independent —
    under the incremental default the same aggregate equality is gated
    by the P2.2 golden (codex-unattributed / codex-merge-window, both
    modes compared) and the WP-B codex soak (unattributed aggregate must
    equal the task count exactly).
    """

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="ui r4 cancel ")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.evidence = self.root / "evidence"
        self.evidence.mkdir()
        self._prior_router = os.environ.get("CZS_EVENT_ROUTER")
        os.environ["CZS_EVENT_ROUTER"] = "legacy"
        self.addCleanup(self._restore_router_mode)

    def _restore_router_mode(self):
        if self._prior_router is None:
            os.environ.pop("CZS_EVENT_ROUTER", None)
        else:
            os.environ["CZS_EVENT_ROUTER"] = self._prior_router

    def test_cancelled_task_folds_orphan_once_and_close_never_doubles(self):
        script = write_delayed_read_fake(self.root, "single")

        async def go():
            client = CodexAppServerClient(
                script, self.evidence / "cancel",
                env={"FAKE_UI_APPSERVER_MODE": "single"})
            after_cancel = None
            try:
                await client.start()
                task = asyncio.create_task(CodexAppServerDriver(
                    client).run_task(SimpleContractAuthority([], []),
                                     self.root, "p", timeout_s=10))

                async def wait_for_orphan():
                    while not any(f.get("method") == "orphan/notification"
                                  for f in client.notifications):
                        await asyncio.sleep(0.001)

                await asyncio.wait_for(wait_for_orphan(), 5)
                self.assertFalse(task.done())  # pending at the barrier
                task.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await task
                after_cancel = dict(client.notification_unattributed)
            finally:
                await client.close()
                after_close = dict(client.notification_unattributed)
                await client.close()  # repeated close: nothing doubles
                after_second_close = dict(client.notification_unattributed)
            return (after_cancel, after_close, after_second_close)

        after_cancel, after_close, after_second_close = asyncio.run(go())
        expected = {"orphan/notification": 1}
        self.assertEqual(after_cancel, expected)
        self.assertEqual(after_close, expected)
        self.assertEqual(after_second_close, expected)

    def test_subsequent_normal_task_does_not_recount_prior_frame(self):
        client = _barrier_client(self.root, "subsequent")

        async def go():
            try:
                pending = asyncio.create_task(CodexAppServerDriver(
                    client).run_task(SimpleContractAuthority([], []),
                                     self.root / "wsA", "p1", timeout_s=10))
                await _wait_for_orphan(client)
                pending.cancel()
                try:
                    await pending
                    raise AssertionError("cancel must propagate")
                except asyncio.CancelledError:
                    pass
                after_cancel = dict(client.notification_unattributed)
                # a later NORMAL task on the same client: its own new orphan
                # arrives (+1) and the prior physical frame is NOT recounted;
                # its receipt counts only its OWN thread's frames (the second
                # thread/read responds without the gate)
                client.request.open_gate()
                receipt = await CodexAppServerDriver(client).run_task(
                    SimpleContractAuthority([], []),
                    self.root / "wsB", "p2", timeout_s=10)
                after_normal = dict(client.notification_unattributed)
            finally:
                await client.close()
            return after_cancel, after_normal, receipt

        (self.root / "wsA").mkdir(exist_ok=True)
        (self.root / "wsB").mkdir(exist_ok=True)
        after_cancel, after_normal, receipt = asyncio.run(go())
        self.assertEqual(after_cancel, {"orphan/notification": 1})
        # run 2 pushed no new orphan (first-turn-only) and never recounted
        # run 1's physical frame
        self.assertEqual(after_normal, {"orphan/notification": 1})
        self.assertEqual(receipt["notification_counts"],
                         {"item/completed": 1, "turn/completed": 1})
        self.assertNotIn("notification_unattributed", receipt)

    def test_concurrent_normal_and_cancelled_share_orphan_once(self):
        client = _barrier_client(self.root, "concurrent")

        async def go():
            try:
                driver = CodexAppServerDriver(client)
                first = asyncio.create_task(driver.run_task(
                    SimpleContractAuthority([], []),
                    self.root / "wsA", "task A", timeout_s=15))
                second = asyncio.create_task(driver.run_task(
                    SimpleContractAuthority([], []),
                    self.root / "wsB", "task B", timeout_s=15))
                await _wait_for_orphan(client)
                # exactly one task's thread/read is served; the other is
                # pending at the delayed first read — cancel THAT one once
                # its sibling has completed
                done, pending = await asyncio.wait(
                    {first, second}, return_when=asyncio.FIRST_COMPLETED)
                self.assertEqual(len(done), 1)
                self.assertEqual(len(pending), 1)
                stuck = next(iter(pending))
                stuck.cancel()
                results = await asyncio.gather(first, second,
                                               return_exceptions=True)
                outcomes = sorted(
                    "cancelled" if isinstance(r, asyncio.CancelledError)
                    else r["outcome"] for r in results)
                after = dict(client.notification_unattributed)
            finally:
                await client.close()
                after_close = dict(client.notification_unattributed)
            return outcomes, after, after_close

        (self.root / "wsA").mkdir(exist_ok=True)
        (self.root / "wsB").mkdir(exist_ok=True)
        outcomes, after, after_close = asyncio.run(go())
        self.assertEqual(outcomes, ["cancelled", "delivered"])
        self.assertEqual(after, {"orphan/notification": 1})
        self.assertEqual(after_close, {"orphan/notification": 1})

    def test_close_only_tail_frame_is_merged_exactly_once(self):
        """A frame retained AFTER the task's own cleanup (no further task,
        no new thread/read) must still land in the client aggregate at
        close() — once, and never again on repeated close."""
        client = _barrier_client(self.root, "closetail")
        (self.root / "wsA").mkdir(exist_ok=True)

        async def go():
            try:
                client.request.open_gate()
                await CodexAppServerDriver(client).run_task(
                    SimpleContractAuthority([], []),
                    self.root / "wsA", "p", timeout_s=10)
                self.assertEqual(client.notification_unattributed,
                                 {"orphan/notification": 1})
                # a late server push after the task returned (the reader is
                # still alive; nothing rescans until close)
                client.notifications.append(
                    {"method": "orphan/notification",
                     "params": {"free": "late-tail"}})
                self.assertEqual(client.notification_unattributed,
                                 {"orphan/notification": 1})
            finally:
                await client.close()
                after_close = dict(client.notification_unattributed)
                await client.close()
                after_second = dict(client.notification_unattributed)
            return after_close, after_second

        after_close, after_second = asyncio.run(go())
        self.assertEqual(after_close, {"orphan/notification": 2})
        self.assertEqual(after_second, {"orphan/notification": 2})


def _wait_for_orphan(client):
    async def wait():
        while not any(f.get("method") == "orphan/notification"
                      for f in client.notifications):
            await asyncio.sleep(0.001)
    return asyncio.wait_for(wait(), 5)


class _BarrierRequest:
    """Deterministic in-process replacement for the client's wire request.

    Real CodexAppServerClient instance (real cursor aggregate, real close
    path); only ``request`` is stubbed: thread ids are per-run, each
    turn/start pushes that thread's terminal frames plus one identity-less
    orphan, and the FIRST thread/read waits on a gate that starts CLOSED
    (=> that task stays pending at the evidence barrier, exactly like the
    quality probe's delayed fake); later reads respond immediately.
    """

    def __init__(self, client):
        self.client = client
        self.gate = asyncio.Event()  # closed: first read blocks until opened
        self.reads_served = 0
        self._threads = 0

    def open_gate(self):
        self.gate.set()

    async def __call__(self, method, params, timeout=None, on_written=None):
        client = self.client
        if method == "thread/start":
            self._threads += 1
            thread = f"th_barrier_{self._threads}"
            return {"thread": {"id": thread}}
        if method == "turn/start":
            thread = params.get("threadId")
            if on_written is not None:
                on_written()
            frames = [
                {"method": "item/completed",
                 "params": {"threadId": thread, "turnId": "turn_x",
                            "item": {"type": "agentMessage",
                                     "text": "done"}}},
                {"method": "turn/completed",
                 "params": {"threadId": thread,
                            "turn": {"id": "turn_x",
                                     "status": "completed"}}},
            ]
            if self._threads == 1:
                frames.append({"method": "orphan/notification",
                               "params": {"free": "x"}})
            for frame in frames:
                client.notifications.append(frame)
                for handler in list(client._notification_handlers):
                    handler(frame["method"], frame["params"])
            return {"turn": {"id": "turn_x", "status": "inProgress"}}
        if method == "turn/interrupt":
            return {}
        if method == "thread/read":
            self.reads_served += 1
            if self.reads_served == 1:
                await self.gate.wait()
            return {"thread": {"id": "late", "turns": []}}
        return {}


def _barrier_client(root: Path, name: str):
    """A real (unstarted) CodexAppServerClient with a stubbed request."""
    evidence = root / "evidence" / name
    evidence.mkdir(parents=True, exist_ok=True)
    client = CodexAppServerClient(str(root / "codex"), evidence)
    client.request = _BarrierRequest(client)
    return client




# ---------------------------------------------------------------------------
# 15. Round 5: post-terminal cancellation must merge retained frames now
# ---------------------------------------------------------------------------


def write_post_terminal_orphan_fake(root: Path) -> Path:
    """Fake app-server that pushes ONE identity-less orphan during the FIRST
    post-terminal thread/read and then never answers that read (the quality
    round-4 I-1 shape: the frame is retained while the driver sits in the
    evidence tail)."""
    path = root / "fake-codex-single-postterminal"
    source = write_fake_codex(root, "single").read_text(encoding="utf-8")
    # drop the SINGLE-mode branch's turn/start orphan (the ONLY
    # identity-less frame must be the post-terminal one — the quality
    # probe's shape); other mode branches keep theirs
    needle_single = (
        '            notify("turn/completed", {"threadId": SINGLE,\n'
        '                                      "turn": {"id": "turn_" + SINGLE,\n'
        '                                               "status": "completed"}})\n'
        '            notify("orphan/notification", {"free": "no-identity"})\n')
    replacement_single = (
        '            notify("turn/completed", {"threadId": SINGLE,\n'
        '                                      "turn": {"id": "turn_" + SINGLE,\n'
        '                                               "status": "completed"}})\n')
    # both the single and amend branches share this exact tail; the fixture
    # only ever runs MODE=single, so removing the orphan from both is safe
    if source.count(needle_single) != 2:
        raise AssertionError("fake turn/start orphan injection point changed")
    source = source.replace(needle_single, replacement_single)
    needle = '    elif method == "thread/read":\n'
    replacement = (
        '    elif method == "thread/read":\n'
        '        if not state.get("post_orphan_pushed"):\n'
        '            state["post_orphan_pushed"] = True\n'
        '            notify("orphan/notification", {"free": "post-terminal"})\n'
        '            __import__("time").sleep(60)\n'
        '        send({"id": frame["id"], "result": {"thread": {"id": "x",\n'
        '                                            "turns": []}}})\n')
    if needle not in source:
        raise AssertionError("fake thread/read injection point changed")
    path.write_text(source.replace(needle, replacement, 1), encoding="utf-8")
    path.chmod(0o755)
    return path


class Round5PostTerminalCancelTests(unittest.TestCase):
    """A cancellation during the post-terminal evidence tail must fold the
    retained physical orphan IMMEDIATELY (not only at close)."""

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="ui r5 postterm ")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.evidence = self.root / "evidence"
        self.evidence.mkdir()

    def test_cancel_during_post_terminal_read_merges_immediately(self):
        script = write_post_terminal_orphan_fake(self.root)

        async def go():
            client = CodexAppServerClient(
                script, self.evidence / "postterminal",
                env={"FAKE_UI_APPSERVER_MODE": "single"})
            try:
                await client.start()
                task = asyncio.create_task(CodexAppServerDriver(
                    client).run_task(SimpleContractAuthority([], []),
                                     self.root, "p", timeout_s=10))

                async def wait_for_orphan():
                    # the fake's ONLY identity-less frame is pushed by the
                    # blocked post-terminal read itself: seeing it proves the
                    # driver is parked in the evidence tail
                    while not any(f.get("method") == "orphan/notification"
                                  for f in client.notifications):
                        await asyncio.sleep(0.001)

                await asyncio.wait_for(wait_for_orphan(), 5)
                self.assertFalse(task.done())  # parked at the evidence read
                task.cancel()
                try:
                    await task
                    raise AssertionError("cancel must propagate")
                except asyncio.CancelledError:
                    pass
                after_cancel = dict(client.notification_unattributed)
            finally:
                await client.close()
                after_close = dict(client.notification_unattributed)
                await client.close()
                after_second_close = dict(client.notification_unattributed)
            return after_cancel, after_close, after_second_close

        after_cancel, after_close, after_second_close = asyncio.run(go())
        expected = {"orphan/notification": 1}
        # the immediate post-cancel aggregate is the round-5 contract: the
        # single retained physical frame is folded the moment the
        # cancellation returns (round-4 code left it for close())
        self.assertEqual(after_cancel, expected)
        self.assertEqual(after_close, expected)
        self.assertEqual(after_second_close, expected)


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
