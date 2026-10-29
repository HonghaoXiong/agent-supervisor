"""CodexAppServerClient/Driver tests against a scripted fake `codex`.

No real codex binary or model is ever contacted: the fake script speaks the
observed JSONL JSON-RPC shape (no "jsonrpc" member) over stdio, answers
initialize/thread/start/turn/start/thread/read, fires v1+v2 approval
requests plus an unknown server request, and records every client response
into a diag file for assertion.
"""

import asyncio
import json
import os
import tempfile
import unittest
from unittest import mock
from pathlib import Path

from zcode_supervisor.codex_appserver import (
    ApprovalBridge,
    AppServerError,
    CodexAppServerClient,
    CodexAppServerDriver,
    SimpleContractAuthority,
    normalize_command,
    server_argv,
)

SECRET = "super-secret-token-xyz"

FAKE_SOURCE = '''#!/usr/bin/env python3
"""Fake codex app-server: JSONL JSON-RPC over stdio, mode-switched."""
import json
import os
import sys

MODE = os.environ.get("FAKE_APPSERVER_MODE", "default")
DIAG = os.environ.get("FAKE_APPSERVER_DIAG")
SECRET = os.environ.get("FAKE_APPSERVER_SECRET", "no-secret-set")
THREAD = "th_fake_11111"
TURN = "turn_fake_1"
observed = {"mode": MODE, "server_responses": [], "interrupt": None}
# "dual" mode: two concurrent threads on one client. thread ids are pinned
# by the thread/start cwd (wsA/wsB) so each driver maps to its own thread.
DUAL = {"threads": [], "turns": [], "fired": False}


def send(frame):
    sys.stdout.write(json.dumps(frame) + "\\n")
    sys.stdout.flush()


def note(method, params):
    send({"method": method, "params": params})


def finish(code):
    if DIAG:
        with open(DIAG, "w") as handle:
            json.dump(observed, handle, indent=1)
    sys.exit(code)


def ask(rid, method, params):
    send({"id": rid, "method": method, "params": params})
    line = sys.stdin.readline()
    if not line:
        finish(1)
    response = json.loads(line)
    observed["server_responses"].append(
        {"method": method, "id": rid, "response": response})
    return response


def run_turn():
    if MODE == "p05":
        # P0.5: ONE identity-required approval request shaped by the test
        # env (missing / non-dict / conflicting / unknown / valid identity),
        # then the turn completes with NO other approval.
        extra_method = os.environ.get(
            "FAKE_P05_METHOD", "item/commandExecution/requestApproval")
        observed["p05_asks"] = observed.get("p05_asks", 0) + 1
        rid = "srv-p05-%d" % observed["p05_asks"]
        if os.environ.get("FAKE_P05_OMIT_PARAMS"):
            send({"id": rid, "method": extra_method})
            line = sys.stdin.readline()
            if not line:
                finish(1)
            observed["server_responses"].append(
                {"method": extra_method, "id": rid,
                 "response": json.loads(line)})
        else:
            ask(rid, extra_method,
                json.loads(os.environ.get("FAKE_P05_PARAMS", "{}")))
        note("item/completed", {"threadId": THREAD, "turnId": TURN,
                                "completedAtMs": 10,
                                "item": {"type": "commandExecution",
                                         "command": "cat TASK.md",
                                         "exitCode": 0,
                                         "status": "completed"}})
        note("turn/completed", {"threadId": THREAD,
                                "turn": {"id": TURN,
                                         "status": "completed"}})
        return
    if MODE == "p05cancel":
        # turn/start carries NO turn id, then ONE valid-identity approval
        # WITHOUT turnId: the driver's handler parks in the missing-turn
        # wait (the deterministic cancellation window).
        ask("srv-p05", "item/commandExecution/requestApproval",
            {"threadId": THREAD, "itemId": "item_x", "kind": "command",
             "command": "/bin/zsh -lc 'cat TASK.md'",
             "availableDecisions": ["accept", "decline"]})
        note("turn/completed", {"threadId": THREAD,
                                "turn": {"id": "turn_x",
                                         "status": "completed"}})
        return
    if MODE == "p05two":
        # ONE rejected-shaped ask, then ONE valid ask, then complete.
        ask("srv-a", os.environ.get("FAKE_P05_METHOD",
                                    "item/commandExecution/requestApproval"),
            json.loads(os.environ.get("FAKE_P05_PARAMS", "{}")))
        ask("srv-b", "item/commandExecution/requestApproval",
            {"threadId": THREAD, "turnId": "turn_x", "itemId": "item_b",
             "kind": "command", "command": "/bin/zsh -lc 'cat TASK.md'",
             "availableDecisions": ["accept", "decline"]})
        note("turn/completed", {"threadId": THREAD,
                                "turn": {"id": "turn_x",
                                         "status": "completed"}})
        return
    if MODE == "p05dup":
        # The SAME request id twice with a VALID identity: the second frame
        # must be dropped (no second decision, no second response).
        params = {"threadId": THREAD, "turnId": "turn_x", "itemId": "item_d",
                  "kind": "command", "command": "/bin/zsh -lc 'cat TASK.md'",
                  "availableDecisions": ["accept", "decline"]}
        ask("srv-dup", "item/commandExecution/requestApproval", params)
        send({"id": "srv-dup", "method": "item/commandExecution/requestApproval",
              "params": params})  # replay: fire-and-forget
        note("turn/completed", {"threadId": THREAD,
                                "turn": {"id": "turn_x",
                                         "status": "completed"}})
        return
    if MODE == "p05dupbad":
        # The SAME request id twice with NO identity: one -32601, then drop.
        ask("srv-dup", "item/commandExecution/requestApproval",
            {"itemId": "item_d", "cwd": "SECRET-P05-MARKER"})
        send({"id": "srv-dup", "method": "item/commandExecution/requestApproval",
              "params": {"itemId": "item_d", "cwd": "SECRET-P05-MARKER"}})
        note("turn/completed", {"threadId": THREAD,
                                "turn": {"id": "turn_x",
                                         "status": "completed"}})
        return
    if MODE == "p05duptype":
        # string id "1" and int id 1 are DISTINCT request ids.
        params = {"threadId": THREAD, "turnId": "turn_x", "itemId": "item_t",
                  "kind": "command", "command": "/bin/zsh -lc 'cat TASK.md'",
                  "availableDecisions": ["accept", "decline"]}
        ask("1", "item/commandExecution/requestApproval", params)
        ask(1, "item/commandExecution/requestApproval", params)
        note("turn/completed", {"threadId": THREAD,
                                "turn": {"id": "turn_x",
                                         "status": "completed"}})
        return
    if MODE == "p05badid":
        # an identity request whose id is a LIST (unhashable): the client
        # cannot own it, so it is dropped; the following valid ask proves
        # the client stays usable.
        send({"id": ["unhashable"], "method":
              "item/commandExecution/requestApproval",
              "params": {"threadId": THREAD, "turnId": "turn_x",
                         "itemId": "item_u", "kind": "command",
                         "command": "/bin/zsh -lc 'cat TASK.md'",
                         "availableDecisions": ["accept", "decline"]}})
        ask("srv-ok", "item/commandExecution/requestApproval",
            {"threadId": THREAD, "turnId": "turn_x", "itemId": "item_ok",
             "kind": "command", "command": "/bin/zsh -lc 'cat TASK.md'",
             "availableDecisions": ["accept", "decline"]})
        note("turn/completed", {"threadId": THREAD,
                                "turn": {"id": "turn_x",
                                         "status": "completed"}})
        return
    if MODE == "failed":
        note("error", {"error": {"message": "fake model failure"},
                       "threadId": THREAD, "turnId": TURN, "willRetry": False})
        note("turn/completed", {"threadId": THREAD,
                                "turn": {"id": TURN, "status": "failed",
                                         "error": {"message": "fake model failure"}}})
        return
    if MODE == "timeout":
        return  # never completes; the driver must interrupt
    v2 = {"threadId": THREAD, "turnId": TURN, "itemId": "item_cmd_1",
          "startedAtMs": 1, "kind": "command", "cwd": "/ws",
          "command": "/bin/zsh -lc 'cat TASK.md'",
          "availableDecisions": ["accept", "decline"]}
    if MODE == "persistent":
        v2["proposedExecpolicyAmendment"] = ["cat TASK.md*"]
        v2["availableDecisions"] = ["accept", "acceptForSession", "decline"]
    if MODE == "noturn":
        # One command approval that carries NO turnId: the bridge must fall
        # back to the driver-recorded live turn id, never a placeholder.
        ask("srv-1", "item/commandExecution/requestApproval", {
            "threadId": THREAD, "itemId": "item_cmd_1", "startedAtMs": 1,
            "kind": "command", "cwd": "/ws",
            "command": "/bin/zsh -lc 'cat TASK.md'",
            "availableDecisions": ["accept", "decline"]})
        note("item/completed", {"threadId": THREAD, "turnId": TURN,
                                "completedAtMs": 10,
                                "item": {"type": "commandExecution",
                                         "command": "cat TASK.md", "exitCode": 0,
                                         "status": "completed"}})
        note("turn/completed", {"threadId": THREAD,
                                "turn": {"id": TURN, "status": "completed"}})
        return
    ask("srv-1", "item/commandExecution/requestApproval", v2)
    ask("srv-2", "execCommandApproval",
        {"callId": "call_1", "command": ["touch", "x"],
         "conversationId": THREAD, "cwd": "/ws", "parsedCmd": []})
    ask("srv-3", "item/fileChange/requestApproval",
        {"threadId": THREAD, "turnId": TURN, "itemId": "item_file_1",
         "startedAtMs": 2, "fileChanges": {"OUT.md": {"type": "add", "content": "x"}}})
    ask("srv-4", "item/permissions/requestApproval",
        {"threadId": THREAD, "turnId": TURN, "itemId": "item_perm_1",
         "startedAtMs": 3, "cwd": "/ws",
         "permissions": {"fileSystem": None, "network": None}})
    ask("srv-5", "totally/unknownMethod", {"authorization": "Bearer " + SECRET})
    note("totally/unknownNotification", {"apikey": SECRET})
    note("item/completed", {"threadId": THREAD, "turnId": TURN, "completedAtMs": 10,
                            "item": {"type": "commandExecution",
                                     "command": "cat TASK.md", "exitCode": 0,
                                     "status": "completed"}})
    note("item/completed", {"threadId": THREAD, "turnId": TURN, "completedAtMs": 11,
                            "item": {"type": "agentMessage", "text": "done summary"}})
    note("thread/tokenUsage/updated",
         {"threadId": THREAD, "turnId": TURN,
          "tokenUsage": {"last": {"input": 10, "output": 5},
                         "total": {"input": 100, "output": 50}}})
    note("turn/completed", {"threadId": THREAD,
                            "turn": {"id": TURN, "status": "completed"}})


def dual_sequence():
    # Interleaved two-thread turn, fired once BOTH turns started:
    # T1 approval, T2 approval, T1 item/completed, T2 item/completed,
    # T1 agent message, T2 agent message, T1 turn/completed, T2 turn/completed.
    t1, t2 = DUAL["threads"][0], DUAL["threads"][1]
    if MODE == "p05dual":
        # P0.5: an identity-less approval while TWO handlers are active.
        ask("srv-p05", os.environ.get(
            "FAKE_P05_METHOD", "item/commandExecution/requestApproval"),
            json.loads(os.environ.get("FAKE_P05_PARAMS", "{}")))

    def approval(item, thread, command):
        return {"threadId": thread, "turnId": "turn_" + thread,
                "itemId": item, "startedAtMs": 1, "kind": "command",
                "cwd": "/ws", "command": command,
                "availableDecisions": ["accept", "decline"]}

    ask("srv-d1", "item/commandExecution/requestApproval",
        approval("item_A", t1, "/bin/zsh -lc 'cat A.md'"))
    ask("srv-d2", "item/commandExecution/requestApproval",
        approval("item_B", t2, "/bin/zsh -lc 'cat B.md'"))
    for thread, command in ((t1, "cat A.md"), (t2, "cat B.md")):
        note("item/completed", {"threadId": thread, "turnId": "turn_" + thread,
                                "completedAtMs": 10,
                                "item": {"type": "commandExecution",
                                         "command": command, "exitCode": 0,
                                         "status": "completed"}})
    for thread, text in ((t1, "done A"), (t2, "done B")):
        note("item/completed", {"threadId": thread, "turnId": "turn_" + thread,
                                "completedAtMs": 11,
                                "item": {"type": "agentMessage", "text": text}})
    for thread in (t1, t2):
        note("turn/completed", {"threadId": thread,
                                "turn": {"id": "turn_" + thread,
                                         "status": "completed"}})


for line in sys.stdin:
    frame = json.loads(line)
    method = frame.get("method")
    params = frame.get("params") or {}
    if method == "initialize":
        observed["initialize_params"] = params
        send({"id": frame["id"],
              "result": {"codexHome": "/tmp/fake-codex-home",
                         "platformFamily": "unix", "platformOs": "macos",
                         "userAgent": "fake-appserver/0.164"}})
        if MODE in ("presub", "stranger-window"):
            # repair2 F1/F2 shapes: notifications BEFORE any run_task
            # subscribes (router not yet created -> legacy store branch)
            note("turn/progress", {"message": "pre-subscribe noise"})
            note("turn/progress", {"message": "pre-subscribe noise 2"})
    elif method == "thread/start":
        observed["thread_params"] = params
        thread = THREAD
        if MODE in ("dual", "p05dual"):
            thread = ("th_dual_A" if str(params.get("cwd", "")).endswith("wsA")
                      else "th_dual_B")
            if thread not in DUAL["threads"]:
                DUAL["threads"].append(thread)
        send({"id": frame["id"],
              "result": {"thread": {"id": thread},
                         "approvalPolicy": params.get("approvalPolicy"),
                         "approvalsReviewer": params.get("approvalsReviewer"),
                         "cwd": params.get("cwd"),
                         "model": params.get("model") or "fake-model",
                         "modelProvider": "fake",
                         "sandbox": params.get("sandbox")}})
        if MODE == "window":
            # P2.2-repair1 G1/A: the own-thread notification is batched
            # in the SAME flush as the thread/start response — the read
            # loop processes it before run_task resumes to rekey
            note("item/completed",
                 {"threadId": thread,
                  "item": {"type": "agentMessage",
                           "text": "EARLY_MESSAGE_IN_WINDOW"}})
        if MODE == "stranger-window":
            # repair2 F2: a foreign-thread frame rides every placeholder
            # window — ownerless once the window closes
            note("item/completed",
                 {"threadId": "th_stranger",
                  "item": {"type": "agentMessage",
                           "text": "STRANGER_IN_WINDOW"}})
    elif method == "turn/start":
        observed["turn_params"] = params
        if MODE in ("dual", "p05dual"):
            thread = params.get("threadId")
            if thread not in DUAL["turns"]:
                DUAL["turns"].append(thread)
            send({"id": frame["id"], "result": {"turn": {"id": "turn_" + str(thread),
                                                         "status": "inProgress"}}})
            if len(DUAL["turns"]) == 2 and not DUAL["fired"]:
                DUAL["fired"] = True
                dual_sequence()
        elif MODE == "p05cancel":
            # no turn id in the response: the driver never learns a live
            # turn, so the turnId-less approval parks in the 5s wait
            send({"id": frame["id"], "result": {"turn": {}}})
            run_turn()
        else:
            send({"id": frame["id"], "result": {"turn": {"id": TURN,
                                                         "status": "inProgress"}}})
            run_turn()
    elif method == "thread/read":
        if MODE in ("dual", "p05dual"):
            thread = str(params.get("threadId"))
            send({"id": frame["id"],
                  "result": {"thread": {"id": thread, "turns": [
                      {"id": "turn_" + thread, "status": "completed",
                       "items": [{"type": "agentMessage",
                                  "text": ("done A" if thread.endswith("A")
                                           else "done B")}]}]},
                      "readState": {}}})
        else:
            send({"id": frame["id"],
                  "result": {"thread": {"id": THREAD, "turns": [
                      {"id": TURN, "status": "completed",
                       "items": [{"type": "agentMessage",
                                  "text": "done summary"}]}]},
                      "readState": {}}})
    elif method == "turn/interrupt":
        observed["interrupt"] = params
        send({"id": frame["id"], "result": {}})
        finish(0)
    else:
        send({"id": frame["id"],
              "error": {"code": -32601,
                        "message": "no such method " + str(method)}})
finish(0)
'''


class PureUnitTests(unittest.TestCase):
    def test_server_argv_transports(self):
        self.assertEqual(server_argv("/usr/bin/codex", "stdio"),
                         ["/usr/bin/codex", "app-server", "--listen", "stdio://"])
        self.assertEqual(server_argv("/usr/bin/codex", "proxy"),
                         ["/usr/bin/codex", "app-server", "proxy"])

    def test_unknown_transport_rejected(self):
        with self.assertRaises(AppServerError):
            server_argv("/usr/bin/codex", "tcp://elsewhere")

    def test_normalize_command_forms(self):
        self.assertEqual(normalize_command(["touch", "x"]), "touch x")
        self.assertEqual(normalize_command(["/bin/zsh", "-lc", "cat TASK.md"]),
                         "cat TASK.md")
        self.assertEqual(normalize_command("/bin/zsh -lc 'cat TASK.md'"),
                         "cat TASK.md")
        self.assertEqual(normalize_command("cat TASK.md"), "cat TASK.md")
        self.assertEqual(normalize_command(None), "")

    def test_simple_contract_authority(self):
        authority = SimpleContractAuthority(["ls -la"], ["OUT.md"])
        self.assertEqual(authority.decide(
            {"toolName": "Bash", "input": {"command": "ls -la"}})["decision"], "allow")
        self.assertEqual(authority.decide(
            {"toolName": "Bash", "input": {"command": "rm -rf /"}})["decision"], "deny")
        self.assertEqual(authority.decide(
            {"toolName": "Write",
             "input": {"file_path": "/tmp/ws/OUT.md", "content": ""}})["decision"], "allow")
        self.assertEqual(authority.decide(
            {"toolName": "Write",
             "input": {"file_path": "/tmp/ws/OTHER.md", "content": ""}})["decision"], "deny")

    def test_needs_turn_identity_predicates(self):
        # Only approval-family methods without a wire turnId need the gate.
        for method in ("execCommandApproval",
                       "item/commandExecution/requestApproval",
                       "item/fileChange/requestApproval"):
            self.assertTrue(ApprovalBridge.needs_turn_identity(method, {}))
            self.assertTrue(ApprovalBridge.needs_turn_identity(
                method, {"turnId": ""}))
            self.assertFalse(ApprovalBridge.needs_turn_identity(
                method, {"turnId": "turn_x"}))
            self.assertFalse(ApprovalBridge.needs_turn_identity(
                method, {"turn_id": "turn_x"}))
        self.assertFalse(ApprovalBridge.needs_turn_identity(
            "totally/unknownMethod", {}))
        self.assertFalse(ApprovalBridge.needs_turn_identity(
            "execCommandApproval", None))


class FakeAppServerTests(unittest.TestCase):
    def _prepare(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name)
        fake = root / "fake-codex"
        fake.write_text(FAKE_SOURCE)
        fake.chmod(0o755)
        (root / "ws").mkdir()
        return root, fake, root / "diag.json"

    def _drive(self, mode, timeout_s=8.0, authority=None):
        root, fake, diag_path = self._prepare()
        if authority is None:
            authority = SimpleContractAuthority(["cat TASK.md"], ["OUT.md"])

        async def go():
            client = CodexAppServerClient(
                str(fake), root / "evidence",
                env={"FAKE_APPSERVER_MODE": mode,
                     "FAKE_APPSERVER_DIAG": str(diag_path),
                     "FAKE_APPSERVER_SECRET": SECRET})
            try:
                await client.start()
                receipt = await CodexAppServerDriver(client).run_task(
                    authority, root / "ws", "do the task", timeout_s=timeout_s)
            finally:
                await client.close()
            return receipt

        receipt = asyncio.run(go())
        diag = json.loads(diag_path.read_text())
        return receipt, root, diag

    @staticmethod
    def _frames(evidence_dir):
        lines = (evidence_dir / "appserver-frames.jsonl").read_text().splitlines()
        return [json.loads(line) for line in lines]

    def test_handshake_frames_and_initialize_result(self):
        root, fake, diag_path = self._prepare()

        async def go():
            client = CodexAppServerClient(
                str(fake), root / "evidence",
                env={"FAKE_APPSERVER_MODE": "default",
                     "FAKE_APPSERVER_DIAG": str(diag_path),
                     "FAKE_APPSERVER_SECRET": SECRET})
            result = await client.start()
            await client.close()
            return result

        result = asyncio.run(go())
        self.assertEqual(sorted(result), ["codexHome", "platformFamily",
                                          "platformOs", "userAgent"])
        self.assertEqual(result["userAgent"], "fake-appserver/0.164")
        frames = self._frames(root / "evidence")
        sent = [entry for entry in frames if entry["dir"] == "send"]
        received = [entry for entry in frames if entry["dir"] == "recv"]
        self.assertEqual(sent[0]["frame"]["method"], "initialize")
        self.assertEqual(sent[0]["frame"]["params"]["clientInfo"]["name"], "czs-hub")
        self.assertEqual(received[0]["frame"]["id"], sent[0]["frame"]["id"])
        for entry in frames:
            self.assertNotIn("jsonrpc", entry["frame"])

    def test_approval_bridge_command_and_file_decisions(self):
        receipt, _root, diag = self._drive("default")
        self.assertEqual(receipt["outcome"], "delivered")
        frames_by_method = {entry["method"]: entry["response"]
                            for entry in diag["server_responses"]}
        responses = {method: frame.get("result")
                     for method, frame in frames_by_method.items()}
        # v2 command in the approved set -> accept (string form normalized).
        self.assertEqual(responses["item/commandExecution/requestApproval"],
                         {"decision": "accept"})
        # v1 command not in the approved set -> denied with a rejection string.
        denial = responses["execCommandApproval"]["decision"]
        self.assertEqual(set(denial), {"denied"})
        self.assertIn("not in this run contract",
                      denial["denied"]["rejection"])
        # whitelisted file -> accept; permission escalation -> empty grant.
        self.assertEqual(responses["item/fileChange/requestApproval"],
                         {"decision": "accept"})
        self.assertEqual(responses["item/permissions/requestApproval"],
                         {"permissions": {}})
        records = {record["method"]: record for record in receipt["approvals"]}
        self.assertEqual(records["item/commandExecution/requestApproval"]["decision"],
                         "allow")
        self.assertEqual(records["execCommandApproval"]["decision"], "deny")
        self.assertEqual(records["item/fileChange/requestApproval"]["decision"],
                         "allow")
        self.assertIn("alert", records["item/permissions/requestApproval"])

    def test_persistent_variant_downgraded_with_alert(self):
        receipt, _root, diag = self._drive("persistent")
        self.assertEqual(receipt["outcome"], "delivered")
        responses = {entry["method"]: entry["response"].get("result")
                     for entry in diag["server_responses"]}
        # Live wire shape (2026-10-26 run3): a proposed execpolicy amendment is
        # offered ALONGSIDE a plain per-request accept. The bridge must answer
        # non-persistently (accept), never the amendment variant.
        self.assertEqual(responses["item/commandExecution/requestApproval"],
                         {"decision": "accept"})
        record = next(item for item in receipt["approvals"]
                      if item["method"] == "item/commandExecution/requestApproval")
        self.assertIsNone(record["requested_variant"])
        self.assertNotIn("alert", record)
        self.assertEqual(record["decision"], "allow")

    def test_persistent_only_offer_is_refused_with_alert(self):
        from zcode_supervisor.codex_appserver import ApprovalBridge
        bridge = ApprovalBridge(None, "thread-x")
        hint = bridge._persistent_hint(
            "codex-v2",
            {"availableDecisions": [{"acceptWithExecpolicyAmendment": {
                "execpolicy_amendment": ["/bin/sh", "-c", "x"]}}],
             "proposedExecpolicyAmendment": ["/bin/sh", "-c", "x"]})
        self.assertEqual(hint, "acceptWithExecpolicyAmendment")
        hint_v1 = bridge._persistent_hint("codex-v1", {"grantRoot": "/ws"})
        self.assertEqual(hint_v1, "approved_for_session")
        self.assertIsNone(bridge._persistent_hint(
            "codex-v2", {"availableDecisions": ["accept"]}))

    def test_receipt_evidence_files_and_redaction(self):
        receipt, root, diag = self._drive("default")
        for key in ("outcome", "thread_id", "turn_id", "final_message",
                    "command_items", "approvals", "notification_counts",
                    "duration_s", "evidence"):
            self.assertIn(key, receipt)
        self.assertEqual(receipt["thread_id"], "th_fake_11111")
        self.assertEqual(receipt["turn_id"], "turn_fake_1")
        self.assertEqual(receipt["command_items"],
                         [{"command": "cat TASK.md", "exit_code": 0,
                           "status": "completed"}])
        self.assertEqual(receipt["final_message"], "done summary")
        self.assertEqual(receipt["notification_counts"].get("item/completed"), 2)
        self.assertEqual(receipt["notification_counts"].get("turn/completed"), 1)
        # thread/start honored the driver contract.
        self.assertEqual(diag["thread_params"]["cwd"], str(root / "ws"))
        self.assertEqual(diag["thread_params"]["sandbox"], "read-only")
        self.assertEqual(diag["thread_params"]["approvalsReviewer"], "user")
        self.assertIs(diag["thread_params"]["ephemeral"], False)
        # thread/read landed on disk with the turn history.
        thread_read = Path(receipt["evidence"]["thread_read"])
        self.assertTrue(thread_read.is_file())
        payload = json.loads(thread_read.read_text())
        self.assertEqual(payload["thread"]["turns"][0]["items"][0]["text"],
                         "done summary")
        # the receipt and frames exist and carry no transport secrets.
        receipt_file = root / "evidence" / "appserver-receipt.json"
        self.assertEqual(json.loads(receipt_file.read_text())["outcome"],
                         "delivered")
        frames_text = (root / "evidence" / "appserver-frames.jsonl").read_text()
        self.assertNotIn(SECRET, frames_text)
        self.assertNotIn(SECRET, receipt_file.read_text())

    def test_turn_failed_outcome(self):
        receipt, _root, diag = self._drive("failed")
        self.assertEqual(receipt["outcome"], "failed")
        self.assertIn("fake model failure", receipt["error"])

    def test_timeout_sends_turn_interrupt(self):
        receipt, root, diag = self._drive("timeout", timeout_s=1.5)
        self.assertEqual(receipt["outcome"], "timeout")
        self.assertEqual(diag["interrupt"],
                         {"threadId": "th_fake_11111", "turnId": "turn_fake_1"})
        sent = [entry["frame"].get("method")
                for entry in self._frames(root / "evidence") if entry["dir"] == "send"]
        self.assertIn("turn/interrupt", sent)
        self.assertEqual(json.loads(
            (root / "evidence" / "appserver-receipt.json").read_text())["outcome"],
            "timeout")

    def test_unknown_server_request_answered_with_32601(self):
        receipt, _root, diag = self._drive("default")
        responses = {entry["method"]: entry["response"]
                     for entry in diag["server_responses"]}
        unknown = responses["totally/unknownMethod"]
        self.assertEqual(unknown["error"]["code"], -32601)
        # the client kept serving the rest of the turn afterwards.
        self.assertEqual(receipt["outcome"], "delivered")
        self.assertEqual(receipt["final_message"], "done summary")

    def test_reverse_tasks_leave_the_active_registry_after_completion(self):
        """P2.2: a completed reverse task is discarded from the client's
        registry — the default mode's unknown server request completes
        (answered -32601), and no finished task may accumulate."""
        root, fake, diag_path = self._prepare()
        import asyncio as _asyncio

        async def go():
            client = CodexAppServerClient(
                str(fake), root / "evidence",
                env={"FAKE_APPSERVER_MODE": "default",
                     "FAKE_APPSERVER_DIAG": str(diag_path),
                     "FAKE_APPSERVER_SECRET": SECRET})
            try:
                await client.start()
                receipt = await CodexAppServerDriver(client).run_task(
                    SimpleContractAuthority(["cat TASK.md"], ["OUT.md"]),
                    root / "ws", "do the task", timeout_s=8.0)
                self.assertEqual(receipt["outcome"], "delivered")
                self.assertEqual(client.tasks, [])
            finally:
                await client.close()

        _asyncio.run(go())

    def test_rekey_window_notification_reaches_receipt_both_modes(self):
        """G1 (repair1 A): an own-thread item/completed batched with the
        thread/start response must reach the receipt in BOTH router modes
        with identical evidence fields."""
        receipts = {}
        for mode in ("legacy", "incremental"):
            with mock.patch.dict(os.environ,
                                 {"CZS_EVENT_ROUTER": mode}):
                receipt, _root, _diag = self._drive("window")
            receipts[mode] = {
                "outcome": receipt["outcome"],
                "final_message": receipt["final_message"],
                "notification_counts": receipt["notification_counts"],
                "command_items": receipt["command_items"],
            }
        self.assertEqual(receipts["legacy"], receipts["incremental"])
        # the fake's default flow emits two item/completed agent
        # messages; the window frame adds a third — in BOTH modes (a
        # lost window frame would make the incremental count 2)
        self.assertEqual(
            receipts["incremental"]["notification_counts"]
            .get("item/completed"), 3)

    def test_pre_subscribe_frames_keep_store_seqs_aligned(self):
        """repair2 F1: notifications arriving before the first routed
        subscription (legacy store branch) must carry seq records —
        store/seqs stay index-parallel and replay fabricates no
        duplicate seqs."""
        root, fake, diag_path = self._prepare()
        import asyncio as _asyncio

        async def go():
            client = CodexAppServerClient(
                str(fake), root / "evidence",
                env={"FAKE_APPSERVER_MODE": "presub",
                     "FAKE_APPSERVER_DIAG": str(diag_path),
                     "FAKE_APPSERVER_SECRET": SECRET,
                     "PATH": os.environ.get("PATH", "")})
            try:
                await client.start()
                with mock.patch.dict(os.environ,
                                     {"CZS_EVENT_ROUTER": "incremental"}):
                    await CodexAppServerDriver(client).run_task(
                        SimpleContractAuthority(["cat TASK.md"], ["OUT.md"]),
                        root / "ws", "do the task", timeout_s=8.0)
                self.assertEqual(len(client.notifications),
                                 len(client._store_seqs))
                router = client._notification_router
                self.assertIsNotNone(router)
                pairs = router.replay("th_stranger", 0)
                seqs = [seq for seq, _f in pairs]
                self.assertEqual(len(seqs), len(set(seqs)),
                                 "replay fabricated duplicate seqs")
            finally:
                await client.close()

        _asyncio.run(go())

    def test_stranger_frames_do_not_accumulate_across_windows(self):
        """repair2 F2: foreign-thread frames riding placeholder windows
        are reclaimed — the store must not grow monotonically with the
        task count on one client."""
        root, fake, diag_path = self._prepare()
        import asyncio as _asyncio

        async def go():
            client = CodexAppServerClient(
                str(fake), root / "evidence",
                env={"FAKE_APPSERVER_MODE": "stranger-window",
                     "FAKE_APPSERVER_DIAG": str(diag_path),
                     "FAKE_APPSERVER_SECRET": SECRET,
                     "PATH": os.environ.get("PATH", "")})
            sizes = []
            try:
                await client.start()
                with mock.patch.dict(os.environ,
                                     {"CZS_EVENT_ROUTER": "incremental"}):
                    for index in range(6):
                        await CodexAppServerDriver(client).run_task(
                            SimpleContractAuthority(["cat TASK.md"],
                                                    ["OUT.md"]),
                            root / f"ws{index}", "do the task",
                            timeout_s=8.0)
                        sizes.append(len(client.notifications))
            finally:
                await client.close()
            # bounded: every window's strangers are reclaimed (release +
            # sweep + merge prune) — an unbounded implementation would
            # leave >= 6 frames (one stranger per task)
            self.assertLess(sizes[-1], len(sizes))
            self.assertLessEqual(sizes[-1], 4)

        _asyncio.run(go())

    def test_routed_mode_receipt_matches_key_evidence_fields(self):
        """P2.2: CZS_EVENT_ROUTER=incremental keeps the receipt's core
        evidence fields identical to the legacy expectations."""
        with mock.patch.dict(os.environ,
                             {"CZS_EVENT_ROUTER": "incremental"}):
            receipt, _root, diag = self._drive("default")
        self.assertEqual(receipt["outcome"], "delivered")
        self.assertEqual(receipt["final_message"], "done summary")
        responses = {entry["method"]: entry["response"]
                     for entry in diag["server_responses"]}
        self.assertEqual(responses["totally/unknownMethod"]["error"]["code"],
                         -32601)

    def test_real_turn_id_reaches_authority_when_request_omits_it(self):
        # "noturn" mode: the approval request carries NO turnId, so the
        # bridge's authority params must fall back to the driver-recorded
        # live turn id from turn/start — never the 'codex-turn' placeholder
        # (the real Authority denies unknown turn ids, fail-closed).
        class RecordingAuthority(SimpleContractAuthority):
            def __init__(self):
                super().__init__(["cat TASK.md"], [])
                self.session_id = None  # real authorities carry this slot
                self.seen = []

            def decide(self, params):
                self.seen.append(params)
                return super().decide(params)

        authority = RecordingAuthority()
        receipt, _root, diag = self._drive("noturn", authority=authority)
        self.assertEqual(receipt["outcome"], "delivered")
        self.assertEqual(authority.session_id, "th_fake_11111")
        self.assertTrue(authority.seen)
        for params in authority.seen:
            self.assertEqual(params["turnId"], "turn_fake_1")
            self.assertNotEqual(params["turnId"], "codex-turn")
            self.assertEqual(params["sessionId"], "th_fake_11111")
        # the wire decision used the approved command (turn id was the only
        # identity the request omitted)
        responses = {entry["method"]: entry["response"].get("result")
                     for entry in diag["server_responses"]}
        self.assertEqual(responses["item/commandExecution/requestApproval"],
                         {"decision": "accept"})

    def test_authority_without_identity_slots_still_runs(self):
        # SimpleContractAuthority defines neither session_id nor note_turn:
        # the driver's identity binding must be a no-op for it, not a crash.
        authority = SimpleContractAuthority(["cat TASK.md"], ["OUT.md"])
        self.assertFalse(hasattr(authority, "session_id"))
        self.assertFalse(hasattr(authority, "note_turn"))
        receipt, _root, _diag = self._drive("default", authority=authority)
        self.assertEqual(receipt["outcome"], "delivered")
        self.assertEqual(receipt["final_message"], "done summary")
        # ...and no binding slot was invented on it by the driver
        self.assertFalse(hasattr(authority, "session_id"))

    # -- P3 subscriber model -------------------------------------------------

    def _client(self, root, fake, diag_path, mode):
        return CodexAppServerClient(
            str(fake), root / "evidence",
            env={"FAKE_APPSERVER_MODE": mode,
                 "FAKE_APPSERVER_DIAG": str(diag_path),
                 "FAKE_APPSERVER_SECRET": SECRET})

    def test_async_request_handlers_are_awaited_in_order(self):
        # The driver's dispatch is an async handler; _reverse must await
        # awaitable answers instead of leaking a coroutine to the wire or
        # skipping the subscriber. An async observer returning None must be
        # consulted (registration order) while the sync owner still answers.
        root, fake, diag_path = self._prepare()

        async def go():
            client = self._client(root, fake, diag_path, "default")
            try:
                await client.start()
                seen = []

                async def observer(method, params):
                    seen.append(method)
                    await asyncio.sleep(0)  # force a real suspension
                    return None  # observer only: never claims a request

                client.add_request_handler(observer)
                receipt = await CodexAppServerDriver(client).run_task(
                    SimpleContractAuthority(["cat TASK.md"], []),
                    root / "ws", "go", timeout_s=8)
                return receipt, seen
            finally:
                await client.close()

        receipt, seen = asyncio.run(go())
        self.assertEqual(receipt["outcome"], "delivered")
        self.assertEqual(receipt["final_message"], "done summary")
        # P0.5: identity-required approval methods are routed by the
        # client's unique-candidate resolution and are no longer offered to
        # matcher-less observers; the observer is still consulted (in
        # registration order, awaited) for non-identity methods
        self.assertIn("totally/unknownMethod", seen)
        self.assertNotIn("item/commandExecution/requestApproval", seen)
        # the approval was still answered on the wire by the ASYNC owner
        # through the identity path
        responses = {entry["method"]: entry["response"].get("result")
                     for entry in json.loads(diag_path.read_text())
                     ["server_responses"]}
        self.assertEqual(responses["item/commandExecution/requestApproval"],
                         {"decision": "accept"})

    def test_concurrent_run_tasks_isolate_threads_on_shared_client(self):
        # "dual" fake: two concurrent run_task calls on ONE client, with the
        # two threads' approvals/notifications interleaved by the server.
        # Each task's authority approves only its own command, so any
        # cross-thread routing shows up as a deny in the wrong receipt.
        root, fake, diag_path = self._prepare()
        (root / "wsA").mkdir()
        (root / "wsB").mkdir()

        async def go():
            client = self._client(root, fake, diag_path, "dual")
            try:
                await client.start()
                driver = CodexAppServerDriver(client)
                receipts = await asyncio.gather(
                    driver.run_task(SimpleContractAuthority(["cat A.md"], []),
                                    root / "wsA", "task A", timeout_s=10),
                    driver.run_task(SimpleContractAuthority(["cat B.md"], []),
                                    root / "wsB", "task B", timeout_s=10))
                leftover = (len(client._request_handlers),
                            len(client._notification_handlers))
            finally:
                await client.close()
            return receipts, leftover

        (receipt_a, receipt_b), leftover = asyncio.run(go())
        for receipt, thread, message in ((receipt_a, "th_dual_A", "done A"),
                                         (receipt_b, "th_dual_B", "done B")):
            self.assertEqual(receipt["outcome"], "delivered")
            self.assertEqual(receipt["thread_id"], thread)
            self.assertEqual(receipt["final_message"], message)
            self.assertEqual([record["decision"] for record in receipt["approvals"]],
                             ["allow"])
            self.assertEqual(receipt["approvals"][0]["params"]["threadId"], thread)
        # both subscriptions were released when the tasks ended
        self.assertEqual(leftover, (0, 0))
        # wire: each approval was ACCEPTED — i.e. answered by the handler of
        # its own thread (a swapped handler would have denied the command)
        responses = {entry["id"]: entry["response"].get("result")
                     for entry in json.loads(diag_path.read_text())["server_responses"]}
        self.assertEqual(responses["srv-d1"], {"decision": "accept"})
        self.assertEqual(responses["srv-d2"], {"decision": "accept"})

    def test_run_task_unsubscribes_its_handlers_on_completion(self):
        root, fake, diag_path = self._prepare()

        async def go():
            client = self._client(root, fake, diag_path, "default")
            try:
                await client.start()
                probe_seen = []

                def probe_request(method, params):
                    probe_seen.append(("request", method))
                    return None  # observer only: never claims a request

                def probe_notification(method, params):
                    probe_seen.append(("notification", method))

                client.add_request_handler(probe_request)
                client.add_notification_handler(probe_notification)
                receipt = await CodexAppServerDriver(client).run_task(
                    SimpleContractAuthority(["cat TASK.md"], ["OUT.md"]),
                    root / "ws", "go", timeout_s=8)
                leftover = (list(client._request_handlers),
                            list(client._notification_handlers))
            finally:
                await client.close()
            return receipt, leftover, probe_seen

        receipt, leftover, probe_seen = asyncio.run(go())
        self.assertEqual(receipt["outcome"], "delivered")
        # exactly the two probes survive: the driver unsubscribed its pair
        self.assertEqual([len(handlers) for handlers in leftover], [1, 1])
        self.assertEqual(leftover[0][0].__name__, "probe_request")
        self.assertEqual(leftover[1][0].__name__, "probe_notification")
        # the probes really were subscribed during the run (broadcast proof)
        self.assertTrue(any(kind == "request" for kind, _ in probe_seen))
        self.assertTrue(any(kind == "notification" for kind, _ in probe_seen))

    def test_compat_single_slot_properties_stay_exclusive(self):
        # Legacy single-user usage: assigning on_server_request/
        # on_notification registers ONE exclusive subscription (clearing all
        # others) and the getter returns the last assigned callable.
        root, fake, diag_path = self._prepare()

        async def go():
            client = self._client(root, fake, diag_path, "default")
            self.assertIsNone(client.on_server_request)
            self.assertIsNone(client.on_notification)
            try:
                await client.start()
                probe_requests, probe_notes = [], []

                def probe_request(method, params):
                    probe_requests.append(method)
                    return None

                def probe_notification(method, params):
                    probe_notes.append(method)

                client.add_request_handler(probe_request)
                client.add_notification_handler(probe_notification)
                bridge = ApprovalBridge(
                    SimpleContractAuthority(["cat TASK.md"], []), "th_fake_11111")
                # P0.5: the legacy exclusive slot serves identity-required
                # approvals only when the slot function carries the trusted
                # identity matcher (bound methods cannot carry attributes,
                # so wrap)
                def exclusive_handler(method, params):
                    return bridge.handle(method, params)

                exclusive_handler.czs_request_identity = \
                    lambda: "th_fake_11111"
                client.on_server_request = exclusive_handler  # drops probe
                recorded_notes = []

                def record_notification(method, params):
                    recorded_notes.append(method)

                client.on_notification = record_notification
                self.assertEqual(client.on_server_request,
                                 exclusive_handler)
                self.assertEqual(client.on_notification, record_notification)
                thread = (await client.request("thread/start", {
                    "cwd": str(root / "ws"), "sandbox": "read-only",
                    "approvalPolicy": "on-request", "approvalsReviewer": "user",
                    "ephemeral": False}))["thread"]["id"]
                await client.request("turn/start", {
                    "threadId": thread,
                    "input": [{"type": "text", "text": "go"}]})
                for _ in range(200):  # the fake runs the turn inline
                    if "turn/completed" in recorded_notes:
                        break
                    await asyncio.sleep(0.02)
                client.on_server_request = None  # None clears the slot again
                self.assertIsNone(client.on_server_request)
            finally:
                await client.close()
            return probe_requests, probe_notes, recorded_notes, bridge.decisions

        probe_requests, probe_notes, recorded_notes, decisions = asyncio.run(go())
        self.assertEqual(probe_requests, [])   # cleared by the exclusive assignment
        self.assertEqual(probe_notes, [])
        self.assertIn("turn/completed", recorded_notes)  # compat slot fired
        self.assertTrue(any(record["decision"] == "allow" for record in decisions))

    def test_raising_subscriber_never_breaks_dispatch_or_read_loop(self):
        # A broken subscriber counts as "not handled": later subscribers still
        # answer, notifications still broadcast, and the read loop survives.
        root, fake, diag_path = self._prepare()

        async def go():
            client = self._client(root, fake, diag_path, "default")
            try:
                await client.start()

                def boom(method, params):
                    raise RuntimeError("subscriber exploded")

                client.add_request_handler(boom)
                client.add_notification_handler(boom)
                receipt = await CodexAppServerDriver(client).run_task(
                    SimpleContractAuthority(["cat TASK.md"], ["OUT.md"]),
                    root / "ws", "go", timeout_s=8)
            finally:
                await client.close()
            return receipt

        receipt = asyncio.run(go())
        self.assertEqual(receipt["outcome"], "delivered")
        self.assertEqual(receipt["final_message"], "done summary")
        # the driver's handler (registered after the raiser) still answered
        responses = {entry["method"]: entry["response"].get("result")
                     for entry in json.loads(diag_path.read_text())["server_responses"]}
        self.assertEqual(responses["item/commandExecution/requestApproval"],
                         {"decision": "accept"})
        # the foreign raiser is not the driver's handler: no handler_errors
        self.assertNotIn("handler_errors", receipt)


if __name__ == "__main__":
    unittest.main()


# ---------------------------------------------------------------------------
# P0.5: identity/ambiguous approval server-requests are fail-closed
# ---------------------------------------------------------------------------

P05_METHODS = ("execCommandApproval", "applyPatchApproval",
               "item/commandExecution/requestApproval",
               "item/fileChange/requestApproval",
               "item/permissions/requestApproval")


class CountingAuthority(SimpleContractAuthority):
    """SimpleContractAuthority that counts every decide() call."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.decide_calls = 0

    def decide(self, params):
        self.decide_calls += 1
        return super().decide(params)


class P05IdentityFailClosedTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.fake = self.root / "fake-codex"
        self.fake.write_text(FAKE_SOURCE)
        self.fake.chmod(0o755)
        (self.root / "ws").mkdir()
        self.diag = self.root / "diag.json"

    def _client(self, mode, p05_params='{"cwd": "SECRET-P05-MARKER"}',
                p05_method="item/commandExecution/requestApproval",
                omit_params=False, suffix="run"):
        env = {"FAKE_APPSERVER_MODE": mode,
               "FAKE_APPSERVER_DIAG": str(self.diag),
               "FAKE_P05_METHOD": p05_method,
               "FAKE_P05_PARAMS": p05_params}
        if omit_params:
            env["FAKE_P05_OMIT_PARAMS"] = "1"
        evidence = self.root / f"evidence-{suffix}"
        evidence.mkdir(parents=True, exist_ok=True)
        return CodexAppServerClient(str(self.fake), evidence, env=env)

    def _drive_single(self, authority, suffix="run", **client_kwargs):
        client = self._client("p05", suffix=suffix, **client_kwargs)

        async def go():
            try:
                await client.start()
                receipt = await CodexAppServerDriver(client).run_task(
                    authority, self.root / "ws", "task", timeout_s=8)
            finally:
                await client.close()
            return receipt

        return client, asyncio.run(go()), json.loads(self.diag.read_text())

    def _p05_response(self, diag, method):
        entries = [entry for entry in diag["server_responses"]
                   if entry["method"] == method]
        self.assertEqual(len(entries), 1)  # exactly ONE response, ever
        return entries[0]["response"]

    def _assert_rejected(self, authority, client, diag,
                         method="item/commandExecution/requestApproval"):
        self.assertEqual(authority.decide_calls, 0)  # never reached Authority
        response = self._p05_response(diag, method)
        self.assertIn("error", response)  # single unsupported/error reply
        self.assertEqual(response["error"]["code"], -32601)
        self.assertIn(method, response["error"]["message"])
        self.assertNotIn("result", response)
        # the rejection leaks no raw params
        self.assertNotIn("SECRET-P05-MARKER", json.dumps(diag))

    def test_identity_method_enumeration_is_exact(self):
        from zcode_supervisor.codex_appserver import \
            request_requires_task_identity
        for method in P05_METHODS:
            self.assertTrue(request_requires_task_identity(method), method)
        for method in ("item/tool/requestUserInput",
                       "mcpServer/elicitation/request", "item/tool/call",
                       "totally/unknownMethod", "turn/start"):
            self.assertFalse(request_requires_task_identity(method), method)

    def test_missing_identity_request_is_rejected_not_decided(self):
        authority = CountingAuthority(["cat TASK.md"], [])
        _client, receipt, diag = self._drive_single(
            authority, p05_params='{"itemId": "item_x", "cwd": '
                                  '"SECRET-P05-MARKER"}')
        self.assertEqual(receipt["outcome"], "delivered")
        self.assertEqual(receipt["approvals"], [])  # no decision recorded
        self._assert_rejected(authority, _client, diag)

    def test_single_handler_non_dict_params_are_rejected(self):
        for index, bad in enumerate(("[1, 2]", '"a-string"', "null", "3")):
            with self.subTest(raw=bad):
                authority = CountingAuthority(["cat TASK.md"], [])
                _client, receipt, diag = self._drive_single(
                    authority, suffix=f"nondict{index}", p05_params=bad)
                self.assertEqual(receipt["outcome"], "delivered")
                self.assertEqual(receipt["approvals"], [])
                self._assert_rejected(authority, _client, diag)

    def test_missing_params_key_is_rejected(self):
        authority = CountingAuthority(["cat TASK.md"], [])
        _client, receipt, diag = self._drive_single(authority,
                                                    omit_params=True)
        self.assertEqual(receipt["approvals"], [])
        self._assert_rejected(authority, _client, diag)

    def test_conflicting_identities_are_rejected(self):
        authority = CountingAuthority(["cat TASK.md"], [])
        _client, receipt, diag = self._drive_single(
            authority,
            p05_params='{"threadId": "th_fake_11111", '
                       '"conversationId": "th_other", '
                       '"itemId": "item_x", "cwd": "SECRET-P05-MARKER"}')
        self.assertEqual(receipt["approvals"], [])
        self._assert_rejected(authority, _client, diag)

    def test_unknown_identity_zero_match_is_rejected(self):
        authority = CountingAuthority(["cat TASK.md"], [])
        _client, receipt, diag = self._drive_single(
            authority,
            p05_params='{"threadId": "th_unknown_xyz", '
                       '"itemId": "item_x", "cwd": "SECRET-P05-MARKER"}')
        self.assertEqual(receipt["approvals"], [])
        self._assert_rejected(authority, _client, diag)

    def test_ambiguous_double_match_is_rejected(self):
        authority = CountingAuthority(["cat TASK.md"], [])
        client = self._client(
            "p05",
            p05_params='{"threadId": "th_fake_11111", "itemId": "item_x", '
                       '"command": "/bin/zsh -lc \'cat TASK.md\'", '
                       '"availableDecisions": ["accept", "decline"], '
                       '"cwd": "SECRET-P05-MARKER"}')
        squatter_calls = []

        async def go():
            try:
                await client.start()

                def squatter(method, params):
                    squatter_calls.append(method)
                    return {"decision": "accept"}  # must NEVER win here

                squatter.czs_request_identity = lambda: "th_fake_11111"
                client.add_request_handler(squatter)
                receipt = await CodexAppServerDriver(client).run_task(
                    authority, self.root / "ws", "task", timeout_s=8)
            finally:
                await client.close()
            return receipt

        receipt = asyncio.run(go())
        diag = json.loads(self.diag.read_text())
        self.assertEqual(receipt["approvals"], [])
        self.assertEqual(squatter_calls, [])  # neither handler may claim
        self._assert_rejected(authority, client, diag)

    def test_two_concurrent_handlers_missing_identity_rejects_for_both(self):
        authority_a = CountingAuthority(["cat A.md"], [])
        authority_b = CountingAuthority(["cat B.md"], [])
        client = self._client("p05dual")

        async def go():
            try:
                await client.start()
                driver = CodexAppServerDriver(client)
                return await asyncio.gather(
                    driver.run_task(authority_a, self.root / "wsA",
                                    "task A", timeout_s=10),
                    driver.run_task(authority_b, self.root / "wsB",
                                    "task B", timeout_s=10))
            finally:
                await client.close()

        (self.root / "wsA").mkdir()
        (self.root / "wsB").mkdir()
        receipt_a, receipt_b = asyncio.run(go())
        diag = json.loads(self.diag.read_text())
        for authority, receipt in ((authority_a, receipt_a),
                                   (authority_b, receipt_b)):
            # exactly ONE decision each — their own VALID dual approval;
            # the identity-less request reached neither (it would be a 2nd)
            self.assertEqual(authority.decide_calls, 1)
            self.assertEqual(len(receipt["approvals"]), 1)
            self.assertEqual(receipt["outcome"], "delivered")
        # the identity-less request itself (rid srv-p05 — the two VALID
        # dual approvals use the same method): one error reply, no leak
        injected = [entry for entry in diag["server_responses"]
                    if entry["id"] == "srv-p05"]
        self.assertEqual(len(injected), 1)
        response = injected[0]["response"]
        self.assertIn("error", response)
        self.assertEqual(response["error"]["code"], -32601)
        self.assertNotIn("SECRET-P05-MARKER", json.dumps(diag))

    def test_mixed_illegal_identity_values_are_rejected(self):
        for raw in ('{"threadId": "th_fake_11111", "conversationId": 123, '
                    '"itemId": "item_x", "cwd": "SECRET-P05-MARKER"}',
                    '{"threadId": "th_fake_11111", "thread_id": "", '
                    '"itemId": "item_x", "cwd": "SECRET-P05-MARKER"}',
                    '{"threadId": null, "conversationId": "th_fake_11111", '
                    '"itemId": "item_x", "cwd": "SECRET-P05-MARKER"}'):
            with self.subTest(raw=raw):
                authority = CountingAuthority(["cat TASK.md"], [])
                _client, receipt, diag = self._drive_single(
                    authority, suffix=f"mixed{abs(hash(raw)) % 1000}",
                    p05_params=raw)
                self.assertEqual(receipt["approvals"], [])
                self._assert_rejected(authority, _client, diag)

    def test_equal_value_alias_identity_still_routes(self):
        authority = CountingAuthority(["cat TASK.md"], [])
        _client, receipt, diag = self._drive_single(
            authority,
            p05_params='{"threadId": "th_fake_11111", '
                       '"thread_id": "th_fake_11111", '
                       '"conversationId": "th_fake_11111", '
                       '"turnId": "turn_fake_1", "itemId": "item_x", '
                       '"kind": "command", '
                       '"command": "/bin/zsh -lc \'cat TASK.md\'", '
                       '"availableDecisions": ["accept", "decline"]}')
        self.assertEqual(authority.decide_calls, 1)
        response = self._p05_response(
            diag, "item/commandExecution/requestApproval")
        self.assertEqual(response["result"], {"decision": "accept"})

    def test_valid_identity_still_routes_and_decides_unchanged(self):
        authority = CountingAuthority(["cat TASK.md"], [])
        _client, receipt, diag = self._drive_single(
            authority,
            p05_params='{"threadId": "th_fake_11111", "turnId": "turn_fake_1",'
                       ' "itemId": "item_x", "kind": "command", '
                       '"command": "/bin/zsh -lc \'cat TASK.md\'", '
                       '"availableDecisions": ["accept", "decline"]}')
        self.assertEqual(authority.decide_calls, 1)  # routed exactly once
        response = self._p05_response(
            diag, "item/commandExecution/requestApproval")
        self.assertEqual(response["result"], {"decision": "accept"})
        self.assertEqual(receipt["approvals"][0]["decision"], "allow")

    def test_v1_conversation_id_still_routes(self):
        authority = CountingAuthority(["touch x"], [])
        _client, receipt, diag = self._drive_single(
            authority, p05_method="execCommandApproval",
            p05_params='{"callId": "call_x", "command": ["touch", "x"], '
                       '"conversationId": "th_fake_11111", "cwd": "/ws"}')
        self.assertEqual(authority.decide_calls, 1)
        response = self._p05_response(diag, "execCommandApproval")
        self.assertEqual(response["result"], {"decision": "approved"})
        self.assertEqual(receipt["approvals"][0]["decision"], "allow")


# ---------------------------------------------------------------------------
# P0.5 repair: ingress log sanitization, matcher failure boundary, request-id
# dedup, and cancel/unsubscribe invalidation of in-flight identity handlers
# ---------------------------------------------------------------------------

MARKER = "SECRET-P05-MARKER"


class RaisingMatcherHandler:
    """A subscriber whose identity matcher property raises on access."""

    @property
    def czs_request_identity(self):
        raise RuntimeError("raising identity matcher probe")

    def __call__(self, method, params):
        raise AssertionError("a raising-matcher subscriber must never run")


class P05RepairTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.fake = self.root / "fake-codex"
        self.fake.write_text(FAKE_SOURCE)
        self.fake.chmod(0o755)
        (self.root / "ws").mkdir()
        self.diag = self.root / "diag.json"

    def _client(self, mode, suffix="run", extra_env=None):
        evidence = self.root / f"evidence-{suffix}"
        evidence.mkdir(parents=True, exist_ok=True)
        env = {"FAKE_APPSERVER_MODE": mode,
               "FAKE_APPSERVER_DIAG": str(self.diag),
               "FAKE_P05_METHOD": "item/commandExecution/requestApproval",
               "FAKE_P05_PARAMS": '{"itemId": "item_x", "cwd": "%s"}'
                                  % MARKER}
        if extra_env:
            env.update(extra_env)
        return CodexAppServerClient(str(self.fake), evidence, env=env)

    def _drive(self, mode, authority, suffix="run", extra_env=None,
               before_run=None):
        client = self._client(mode, suffix, extra_env)

        async def go():
            try:
                await client.start()
                if before_run is not None:
                    before_run(client)
                receipt = await CodexAppServerDriver(client).run_task(
                    authority, self.root / "ws", "task", timeout_s=10)
            finally:
                await client.close()
            return receipt

        return client, asyncio.run(go()), json.loads(self.diag.read_text())

    def _recv_frames(self, client):
        lines = (client.evidence_dir / "appserver-frames.jsonl"
                 ).read_text().splitlines()
        return [json.loads(line)["frame"] for line in lines
                if json.loads(line)["dir"] == "recv"]

    # -- Q1: inbound identity params never reach appserver-frames.jsonl ----

    def test_identity_request_params_are_sanitized_in_ingress_log(self):
        authority = CountingAuthority(["cat TASK.md"], [])
        client, receipt, _diag = self._drive("p05", authority)
        self.assertEqual(authority.decide_calls, 0)
        recv = self._recv_frames(client)
        logged = [f for f in recv if f.get("method")
                  == "item/commandExecution/requestApproval"]
        self.assertEqual(len(logged), 1)
        blob = json.dumps(logged)
        self.assertNotIn(MARKER, blob)          # raw params never logged
        self.assertNotIn("itemId", blob)        # no params content at all
        self.assertIn("item/commandExecution/requestApproval", blob)

    # -- Q2: raising matcher property is inside the failure boundary ------

    def test_raising_matcher_property_single_rejection_and_client_usable(self):
        authority = CountingAuthority(["cat TASK.md"], [])
        holder = {}

        def install(client):
            holder["handler"] = RaisingMatcherHandler()
            client.add_request_handler(holder["handler"])

        client, receipt, diag = self._drive("p05two", authority,
                                            suffix="raising",
                                            before_run=install)
        # BOTH asks in p05two are identity requests and the raising matcher
        # poisons candidate collection: single -32601 each, Authority=0
        self.assertEqual(authority.decide_calls, 0)
        self.assertEqual(receipt["approvals"], [])
        errors = [e["response"] for e in diag["server_responses"]
                  if "error" in e["response"]]
        self.assertEqual(len(errors), 2)  # one per ask, nothing else
        for response in errors:
            self.assertEqual(response["error"]["code"], -32601)

    def test_after_raising_matcher_removed_valid_request_routes(self):
        authority = CountingAuthority(["cat TASK.md"], [])
        client = self._client("p05two", suffix="raising-reuse")

        async def go():
            uninstall = None
            try:
                await client.start()
                handler = RaisingMatcherHandler()
                uninstall = client.add_request_handler(handler)
                receipt = await CodexAppServerDriver(client).run_task(
                    authority, self.root / "ws", "task", timeout_s=10)
                return receipt
            finally:
                if uninstall is not None:
                    uninstall()
                await client.close()

        receipt = asyncio.run(go())
        # raising matcher rejected both asks during the first run; the
        # second run (after uninstall) proves the client stayed usable
        self.assertEqual(receipt["outcome"], "delivered")
        self.assertEqual(authority.decide_calls, 0)  # both p05two asks hit
        # the raising matcher while registered; no silent fallback candidate

    # -- Q3: duplicate identity request ids ---------------------------------

    def test_duplicate_valid_request_id_dropped_after_first(self):
        authority = CountingAuthority(["cat TASK.md"], [])
        client, receipt, diag = self._drive("p05dup", authority,
                                            suffix="dup")
        self.assertEqual(authority.decide_calls, 1)  # NOT two
        self.assertEqual(len(receipt["approvals"]), 1)
        responses = [e for e in diag["server_responses"]
                     if e["id"] == "srv-dup"]
        self.assertEqual(len(responses), 1)  # exactly one response
        self.assertEqual(responses[0]["response"]["result"],
                         {"decision": "accept"})

    def test_duplicate_invalid_request_id_single_rejection(self):
        authority = CountingAuthority(["cat TASK.md"], [])
        client, receipt, diag = self._drive("p05dupbad", authority,
                                            suffix="dupbad")
        self.assertEqual(authority.decide_calls, 0)
        self.assertEqual(receipt["approvals"], [])
        responses = [e for e in diag["server_responses"]
                     if e["id"] == "srv-dup"]
        self.assertEqual(len(responses), 1)
        self.assertEqual(responses[0]["response"]["error"]["code"], -32601)
        self.assertNotIn(MARKER, json.dumps(diag))

    def test_request_id_type_distinction_is_preserved(self):
        authority = CountingAuthority(["cat TASK.md"], [])
        client, receipt, diag = self._drive("p05duptype", authority,
                                            suffix="duptype")
        # "1" (string) and 1 (int) are DIFFERENT ids: both routed once
        self.assertEqual(authority.decide_calls, 2)
        self.assertEqual(len(receipt["approvals"]), 2)

    # -- C1: cancel/unsubscribe invalidates the selected handler -----------

    def test_cancel_during_missing_turn_wait_never_reaches_authority(self):
        authority = CountingAuthority(["cat TASK.md"], [])
        client = self._client("p05cancel", suffix="cancelwait")

        async def go():
            task = None
            try:
                await client.start()
                task = asyncio.create_task(CodexAppServerDriver(
                    client).run_task(authority, self.root / "ws", "task",
                                     timeout_s=10))

                async def wait_for_reverse_task():
                    while not client.tasks:
                        await asyncio.sleep(0.001)

                await asyncio.wait_for(wait_for_reverse_task(), 5)
                await asyncio.sleep(0.1)  # the handler parks in the 5s wait
                self.assertFalse(task.done())
                task.cancel()
                try:
                    await task
                    raise AssertionError("cancel must propagate")
                except asyncio.CancelledError:
                    pass
                self.assertEqual(authority.decide_calls, 0)  # NEVER decided

                async def wait_error_response_sent():
                    # the parked handler resumes at its 5s timeout, sees the
                    # revocation and the -32601 goes out — deterministically
                    # observable in the OUTBOUND frame log
                    while True:
                        lines = (client.evidence_dir / "appserver-frames.jsonl"
                                 ).read_text().splitlines()
                        sent = [json.loads(line)["frame"] for line in lines
                                if json.loads(line)["dir"] == "send"]
                        if any(f.get("id") == "srv-p05" and "error" in f
                               for f in sent):
                            return
                        await asyncio.sleep(0.02)

                await asyncio.wait_for(wait_error_response_sent(), 10)
                # the client stays usable after the revocation (the fake is
                # unblocked once its ask was answered)
                snapshot = await asyncio.wait_for(client.request(
                    "thread/read", {"threadId": "th_fake_11111"}), 5)
                self.assertIn("thread", snapshot)
            finally:
                await client.close()
            return None

        asyncio.run(go())
        diag = json.loads(self.diag.read_text())
        responses = [e for e in diag["server_responses"]
                     if e["id"] == "srv-p05"]
        self.assertEqual(len(responses), 1)  # exactly one reply, eventually
        response = responses[0]["response"]
        self.assertIn("error", response)     # the revocation became -32601
        self.assertEqual(response["error"]["code"], -32601)
        self.assertEqual(authority.decide_calls, 0)

    def test_cancel_between_authority_and_response_suppresses_accept(self):
        # this authority cancels the run_task DURING decide(): the handler
        # already passed its pre-Authority check while active
        client = self._client(
            "p05", suffix="cancelret",
            extra_env={"FAKE_P05_PARAMS":
                       '{"threadId": "th_fake_11111", '
                       '"turnId": "turn_fake_1", "itemId": "item_x", '
                       '"kind": "command", '
                       '"command": "/bin/zsh -lc \'cat TASK.md\'", '
                       '"availableDecisions": ["accept", "decline"]}'})
        state = {}

        class CancellingAuthority(CountingAuthority):
            def decide(self, params):
                state["task"].cancel()
                return super().decide(params)

        cancelling = CancellingAuthority(["cat TASK.md"], [])

        async def go():
            try:
                await client.start()
                state["task"] = asyncio.create_task(CodexAppServerDriver(
                    client).run_task(cancelling, self.root / "ws", "task",
                                     timeout_s=10))
                try:
                    await state["task"]
                except asyncio.CancelledError:
                    pass
            finally:
                await client.close()

        asyncio.run(go())
        diag = json.loads(self.diag.read_text())
        responses = [e for e in diag["server_responses"]
                     if e["id"] == "srv-p05-1"]
        self.assertEqual(len(responses), 1)
        response = responses[0]["response"]
        # Authority ran once (it was active at its check), but the accept
        # must never reach the wire after the revocation
        self.assertEqual(cancelling.decide_calls, 1)
        self.assertIn("error", response)
        self.assertEqual(response["error"]["code"], -32601)
        self.assertNotEqual(response.get("result"), {"decision": "accept"})

    # -- subsequent valid request after a routing rejection -----------------

    def test_valid_request_after_missing_identity_rejection_routes(self):
        authority = CountingAuthority(["cat TASK.md"], [])
        client, receipt, diag = self._drive("p05two", authority,
                                            suffix="two")
        # first ask: identity-less -> rejected; second ask: VALID -> routed
        self.assertEqual(authority.decide_calls, 1)
        self.assertEqual(len(receipt["approvals"]), 1)
        self.assertEqual(receipt["approvals"][0]["decision"], "allow")
        by_id = {e["id"]: e["response"] for e in diag["server_responses"]}
        self.assertIn("error", by_id["srv-a"])
        self.assertEqual(by_id["srv-b"]["result"], {"decision": "accept"})


class RaisingRevocationHandler:
    """Selected identity handler whose revocation probe property raises."""

    @property
    def czs_request_identity(self):
        return "th_fake_11111"

    @property
    def czs_check_revoked(self):
        raise RuntimeError("raising revocation probe")

    def __call__(self, method, params):
        return {"decision": "accept"}


class P05RepairFollowUpTests(unittest.TestCase):
    """Mid-review additions: raising revocation property and unhashable
    request ids on identity methods."""

    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.fake = self.root / "fake-codex"
        self.fake.write_text(FAKE_SOURCE)
        self.fake.chmod(0o755)
        (self.root / "ws").mkdir()
        self.diag = self.root / "diag.json"

    def test_raising_revocation_property_single_rejection_client_usable(self):
        env = {"FAKE_APPSERVER_MODE": "p05",
               "FAKE_APPSERVER_DIAG": str(self.diag),
               "FAKE_P05_METHOD": "item/commandExecution/requestApproval",
               "FAKE_P05_PARAMS":
                   '{"threadId": "th_fake_11111", "turnId": "turn_fake_1", '
                   '"itemId": "item_x", "kind": "command", '
                   '"command": "/bin/zsh -lc \'cat TASK.md\'", '
                   '"availableDecisions": ["accept", "decline"]}'}
        evidence = self.root / "evidence-raising-rev"
        evidence.mkdir(parents=True, exist_ok=True)
        client = CodexAppServerClient(str(self.fake), evidence, env=env)

        def sent_frames():
            lines = (evidence / "appserver-frames.jsonl"
                     ).read_text().splitlines()
            return [json.loads(line)["frame"] for line in lines
                    if json.loads(line)["dir"] == "send"]

        async def wait_sent(predicate, timeout=10):
            while True:
                if any(predicate(f) for f in sent_frames()):
                    return
                await asyncio.sleep(0.02)

        async def go():
            try:
                await client.start()
                bad = RaisingRevocationHandler()
                client.add_request_handler(bad)

                def good(method, params):
                    return {"decision": "accept"}

                good.czs_request_identity = lambda: "th_fake_11111"
                thread = (await client.request("thread/start", {
                    "cwd": str(self.root / "ws"), "sandbox": "read-only",
                    "approvalPolicy": "on-request",
                    "approvalsReviewer": "user",
                    "ephemeral": False}))["thread"]["id"]
                # turn 1: the raising-revocation handler is the only
                # candidate; its accept is suppressed -> single -32601
                await client.request("turn/start", {
                    "threadId": thread,
                    "input": [{"type": "text", "text": "go"}]})
                await asyncio.wait_for(
                    wait_sent(lambda f: f.get("id") == "srv-p05-1"
                              and "error" in f), 10)
                # turn 2 (fresh ask id): remove the poison, serve normally
                client._request_handlers.remove(bad)
                client.add_request_handler(good)
                await client.request("turn/start", {
                    "threadId": thread,
                    "input": [{"type": "text", "text": "again"}]})
                await asyncio.wait_for(
                    wait_sent(lambda f: f.get("id") == "srv-p05-2"
                              and f.get("result") == {"decision": "accept"}),
                    10)
            finally:
                await client.close()

        asyncio.run(go())
        # the poisoned turn's answer never leaked an accept to the wire
        self.assertTrue(all(
            not (f.get("id") == "srv-p05-1"
                 and f.get("result") == {"decision": "accept"})
            for f in sent_frames()))

    def test_unhashable_request_id_is_dropped_fail_closed(self):
        env = {"FAKE_APPSERVER_MODE": "p05badid",
               "FAKE_APPSERVER_DIAG": str(self.diag),
               "FAKE_P05_METHOD": "item/commandExecution/requestApproval",
               "FAKE_P05_PARAMS": "{}"}
        evidence = self.root / "evidence-badid"
        evidence.mkdir(parents=True, exist_ok=True)
        client = CodexAppServerClient(str(self.fake), evidence, env=env)
        authority = CountingAuthority(["cat TASK.md"], [])

        async def go():
            try:
                await client.start()
                receipt = await CodexAppServerDriver(client).run_task(
                    authority, self.root / "ws", "task", timeout_s=8)
                return receipt
            finally:
                await client.close()

        receipt = asyncio.run(go())
        diag = json.loads(self.diag.read_text())
        # the unhashable-id frame was DROPPED: no response for it at all,
        # no decision, no task attribution — and the valid follow-up ask
        # routed normally (client fully usable)
        self.assertEqual(authority.decide_calls, 1)
        self.assertEqual(len(receipt["approvals"]), 1)
        by_id = {e["id"]: e["response"] for e in diag["server_responses"]
                 if isinstance(e["id"], (str, int))}
        self.assertNotIn("unhashable", json.dumps(
            [e["id"] for e in diag["server_responses"]]))
        self.assertEqual(by_id["srv-ok"]["result"], {"decision": "accept"})


# ---------------------------------------------------------------------------
# repair2 (2026-10-27, adjudication `tasks/P0.5/repair2-adjudication.md`):
# BEFORE Authority (missing-turn wait, precheck, earlier) a revoke or a
# broken revocation probe must block with Authority/Bridge = 0 and no
# receipt/UI attribution. AFTER Authority has started, the ONE real audit
# record stays in the ledger; the gates then owe: no wire accept, no second
# decision, completed cleanup, usable client. Assertions cover counts,
# receipt shape, wire shape and the sensitive marker — a suppressed response
# alone is not a pass.
# ---------------------------------------------------------------------------

REPAIR2_PARAMS = json.dumps({
    "threadId": "th_fake_11111", "turnId": "turn_fake_1",
    "itemId": "item_r2", "kind": "command",
    "command": "/bin/zsh -lc 'cat SECRET-REPAIR2-MARKER.md'",
    "availableDecisions": ["accept", "decline"]})


class Repair2PoisonedHandler:
    """Unique identity candidate whose revocation probe is pre-broken.

    ``mode="property"``: the ``czs_check_revoked`` attribute access itself
    raises. ``mode="callable"``: the probe is reached but its call raises.
    The handler delegates to a real ApprovalBridge so a run past the gate is
    observable as Authority/bridge activity, never just a lost response.
    """

    def __init__(self, mode, bridge):
        self.mode = mode
        self.bridge = bridge

    def czs_request_identity(self):
        return "th_fake_11111"

    @property
    def czs_check_revoked(self):
        if self.mode == "property":
            raise RuntimeError("repair2 raising revocation property")
        return self._raising_check

    def _raising_check(self):
        raise RuntimeError("repair2 raising revocation callable")

    def __call__(self, method, params):
        return self.bridge.handle(method, params)


class Repair2HookAuthority(CountingAuthority):
    """CountingAuthority with an on_decide hook (runs inside decide)."""

    def __init__(self):
        super().__init__(["cat SECRET-REPAIR2-MARKER.md"], [])
        self.on_decide = None

    def decide(self, params):
        if self.on_decide is not None:
            self.on_decide()
        return super().decide(params)


class P05Repair2Tests(unittest.TestCase):
    """repair2 RED suite: pre-Authority revocation gate and explicit-
    unsubscribe active gate; late-window controls keep the real audit."""

    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.fake = self.root / "fake-codex"
        self.fake.write_text(FAKE_SOURCE)
        self.fake.chmod(0o755)
        (self.root / "ws").mkdir()
        self.diag = self.root / "diag.json"

    def _client(self, mode, suffix):
        evidence = self.root / f"evidence-r2-{suffix}"
        evidence.mkdir(parents=True, exist_ok=True)
        return CodexAppServerClient(
            str(self.fake), evidence,
            env={"FAKE_APPSERVER_MODE": mode,
                 "FAKE_APPSERVER_DIAG": str(self.diag),
                 "FAKE_P05_METHOD": "item/commandExecution/requestApproval",
                 "FAKE_P05_PARAMS": REPAIR2_PARAMS})

    def _sent_frames(self, client):
        lines = (client.evidence_dir / "appserver-frames.jsonl"
                 ).read_text().splitlines()
        return [json.loads(line)["frame"] for line in lines
                if json.loads(line)["dir"] == "send"]

    async def _wait_sent(self, client, predicate, timeout=10):
        while True:
            if any(predicate(f) for f in self._sent_frames(client)):
                return
            await asyncio.sleep(0.02)

    async def _assert_usable(self, client):
        snapshot = await asyncio.wait_for(client.request(
            "thread/read", {"threadId": "th_fake_11111"}), 5)
        self.assertIn("thread", snapshot)

    def _assert_error_response(self, response, method):
        self.assertIn("error", response)
        self.assertNotIn("result", response)
        self.assertEqual(response["error"]["code"], -32601)
        self.assertIn(method, response["error"]["message"])
        self.assertNotIn("SECRET-REPAIR2-MARKER", json.dumps(response))

    def test_raising_revocation_probe_precedes_authority_and_bridge(self):
        for mode in ("property", "callable"):
            with self.subTest(mode=mode):
                authority = Repair2HookAuthority()
                bridge = ApprovalBridge(authority, "th_fake_11111")
                client = self._client("p05", f"probe-{mode}")

                async def go():
                    try:
                        await client.start()
                        poisoned = Repair2PoisonedHandler(mode, bridge)
                        unsubscribe = client.add_request_handler(poisoned)

                        def good(method, params):
                            return {"decision": "accept"}

                        good.czs_request_identity = lambda: "th_fake_11111"
                        thread = (await client.request("thread/start", {
                            "cwd": str(self.root / "ws"),
                            "sandbox": "read-only",
                            "approvalPolicy": "on-request",
                            "approvalsReviewer": "user",
                            "ephemeral": False}))["thread"]["id"]
                        # turn 1: the poisoned handler is the ONLY candidate;
                        # its broken probe must refuse the request BEFORE any
                        # handler/Authority code runs
                        await client.request("turn/start", {
                            "threadId": thread,
                            "input": [{"type": "text", "text": "go"}]})
                        await asyncio.wait_for(self._wait_sent(
                            client, lambda f: f.get("id") == "srv-p05-1"
                            and "error" in f), 10)
                        # turn 2 (fresh ask id): poison removed, clean handler
                        # serves normally — the client stays fully usable
                        unsubscribe()
                        client.add_request_handler(good)
                        await client.request("turn/start", {
                            "threadId": thread,
                            "input": [{"type": "text", "text": "again"}]})
                        await asyncio.wait_for(self._wait_sent(
                            client, lambda f: f.get("id") == "srv-p05-2"
                            and f.get("result") == {"decision": "accept"}),
                            10)
                        return thread
                    finally:
                        await client.close()

                asyncio.run(go())
                # the poison never reached Authority nor the bridge
                self.assertEqual(authority.decide_calls, 0)
                self.assertEqual(bridge.decisions, [])
                diag = json.loads(self.diag.read_text())
                by_id = {e["id"]: e["response"]
                         for e in diag["server_responses"]}
                self._assert_error_response(
                    by_id["srv-p05-1"], "item/commandExecution/requestApproval")
                self.assertEqual(by_id["srv-p05-2"]["result"],
                                 {"decision": "accept"})
                # no accept for the poisoned ask ever reached the wire
                self.assertTrue(all(
                    not (f.get("id") == "srv-p05-1" and "result" in f)
                    for f in self._sent_frames(client)))

    def test_explicit_unsubscribe_during_missing_turn_wait(self):
        authority = Repair2HookAuthority()
        client = self._client("p05cancel", "unsub-wait")

        async def go():
            unsubscribe = []
            original_add = client.add_request_handler

            def capture(handler):
                fn = original_add(handler)
                unsubscribe.append(fn)
                return fn

            client.add_request_handler = capture
            try:
                await client.start()
                task = asyncio.create_task(CodexAppServerDriver(
                    client).run_task(authority, self.root / "ws", "task",
                                     timeout_s=12))
                while not client.tasks or not unsubscribe:
                    await asyncio.sleep(0.01)
                await asyncio.sleep(0.1)  # the handler parks in the 5s wait
                unsubscribe[0]()  # explicit unsubscribe, NO task cancellation
                receipt = await asyncio.wait_for(task, 12)
                self.assertEqual(len(client._request_handlers), 0)
                await self._assert_usable(client)
                return receipt
            finally:
                await client.close()

        receipt = asyncio.run(go())
        # the unsubscribed handler never started an Authority call
        self.assertEqual(authority.decide_calls, 0)
        # and nothing was attributed to the task's receipt
        self.assertEqual(receipt["approvals"], [])
        self.assertEqual(receipt["outcome"], "delivered")
        diag = json.loads(self.diag.read_text())
        responses = [e for e in diag["server_responses"]
                     if e["id"] == "srv-p05"]
        self.assertEqual(len(responses), 1)  # exactly one reply, eventually
        self._assert_error_response(
            responses[0]["response"], "item/commandExecution/requestApproval")
        self.assertTrue(all(
            not (f.get("id") == "srv-p05" and "result" in f)
            for f in self._sent_frames(client)))

    def test_explicit_unsubscribe_inside_authority_preserves_audit(self):
        """Adjudication B: a revoke landing mid-Authority keeps the ONE real
        audit record; the gates owe no wire accept and no second decision."""
        authority = Repair2HookAuthority()
        client = self._client("p05", "unsub-auth")

        async def go():
            unsubscribe = []
            original_add = client.add_request_handler

            def capture(handler):
                fn = original_add(handler)
                unsubscribe.append(fn)
                return fn

            client.add_request_handler = capture
            try:
                await client.start()

                def unsubscribe_during_decide():
                    unsubscribe[0]()  # explicit, mid-Authority

                authority.on_decide = unsubscribe_during_decide
                receipt = await asyncio.wait_for(CodexAppServerDriver(
                    client).run_task(authority, self.root / "ws", "task",
                                     timeout_s=10), 12)
                self.assertEqual(len(client._request_handlers), 0)  # cleanup
                await self._assert_usable(client)
                return receipt
            finally:
                await client.close()

        receipt = asyncio.run(go())
        # the Authority call legitimately started while the handler was
        # active; the revoke landed mid-decide — that ONE decision stays in
        # the audit ledger, and there is no second one
        self.assertEqual(authority.decide_calls, 1)
        self.assertEqual(len(receipt["approvals"]), 1)  # audit preserved
        self.assertEqual(receipt["approvals"][0]["decision"], "allow")
        self.assertEqual(receipt["outcome"], "delivered")
        diag = json.loads(self.diag.read_text())
        responses = [e for e in diag["server_responses"]
                     if e["id"] == "srv-p05-1"]
        self.assertEqual(len(responses), 1)  # no second decision/response
        self._assert_error_response(
            responses[0]["response"], "item/commandExecution/requestApproval")
        self.assertTrue(all(
            not (f.get("id") == "srv-p05-1" and "result" in f)
            for f in self._sent_frames(client)))

    def test_cancel_inside_authority_preserves_audit_suppresses_accept(self):
        """Adjudication B: cancel mid-Authority — one real audit record may
        stay; CancelledError propagation is not owed; accept stays off the
        wire and no second decision happens."""
        client = self._client("p05", "cancel-auth")
        state = {}

        class CancellingAuthority(Repair2HookAuthority):
            def decide(self, params):
                state["task"].cancel()
                return super().decide(params)

        authority = CancellingAuthority()

        async def go():
            try:
                await client.start()
                state["task"] = asyncio.create_task(CodexAppServerDriver(
                    client).run_task(authority, self.root / "ws", "task",
                                     timeout_s=10))
                try:
                    receipt = await state["task"]
                except asyncio.CancelledError:
                    receipt = None  # propagation is allowed, not required
                await asyncio.wait_for(self._wait_sent(
                    client, lambda f: f.get("id") == "srv-p05-1"
                    and "error" in f), 10)
                self.assertEqual(len(client._request_handlers), 0)  # cleanup
                await self._assert_usable(client)
                return receipt
            finally:
                await client.close()

        receipt = asyncio.run(go())
        self.assertEqual(authority.decide_calls, 1)  # exactly one decision
        # the accept never reached the wire after the revocation
        diag = json.loads(self.diag.read_text())
        responses = [e for e in diag["server_responses"]
                     if e["id"] == "srv-p05-1"]
        self.assertEqual(len(responses), 1)  # no second decision/response
        self._assert_error_response(
            responses[0]["response"], "item/commandExecution/requestApproval")
        self.assertTrue(all(
            not (f.get("id") == "srv-p05-1" and "result" in f)
            for f in self._sent_frames(client)))
        # a returned receipt keeps exactly the one real audit record
        self.assertTrue(receipt is None or receipt["approvals"] is None
                        or len(receipt["approvals"]) == 1)

    def test_unsubscribe_after_handler_return_suppresses_accept_wire_only(self):
        authority = Repair2HookAuthority()
        bridge = ApprovalBridge(authority, "th_fake_11111")
        client = self._client("p05", "post-ret")
        holder = {}

        def handler(method, params):
            response = bridge.handle(method, params)
            holder["unsubscribe"]()  # explicit unsubscribe AFTER the decision
            return response

        handler.czs_request_identity = lambda: "th_fake_11111"

        async def go():
            try:
                await client.start()
                holder["unsubscribe"] = client.add_request_handler(handler)
                await client.request("thread/start", {
                    "cwd": str(self.root / "ws"), "sandbox": "read-only",
                    "approvalPolicy": "on-request", "approvalsReviewer": "user",
                    "ephemeral": False})
                await client.request("turn/start", {
                    "threadId": "th_fake_11111",
                    "input": [{"type": "text", "text": "go"}]})
                await asyncio.wait_for(self._wait_sent(
                    client, lambda f: f.get("id") == "srv-p05-1"
                    and "error" in f), 10)
                await self._assert_usable(client)
                return None
            finally:
                await client.close()

        asyncio.run(go())
        # control boundary: the decide legitimately finished while active —
        # the decision stands — but its accept never reached the wire
        self.assertEqual(authority.decide_calls, 1)
        self.assertEqual(len(bridge.decisions), 1)
        diag = json.loads(self.diag.read_text())
        responses = [e for e in diag["server_responses"]
                     if e["id"] == "srv-p05-1"]
        self.assertEqual(len(responses), 1)
        self._assert_error_response(
            responses[0]["response"], "item/commandExecution/requestApproval")
        self.assertTrue(all(
            not (f.get("id") == "srv-p05-1" and "result" in f)
            for f in self._sent_frames(client)))
