"""CodexAppServerClient/Driver tests against a scripted fake `codex`.

No real codex binary or model is ever contacted: the fake script speaks the
observed JSONL JSON-RPC shape (no "jsonrpc" member) over stdio, answers
initialize/thread/start/turn/start/thread/read, fires v1+v2 approval
requests plus an unknown server request, and records every client response
into a diag file for assertion.
"""

import asyncio
import json
import tempfile
import unittest
from pathlib import Path

from zcode_supervisor.codex_appserver import (
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
    elif method == "thread/start":
        observed["thread_params"] = params
        send({"id": frame["id"],
              "result": {"thread": {"id": THREAD},
                         "approvalPolicy": params.get("approvalPolicy"),
                         "approvalsReviewer": params.get("approvalsReviewer"),
                         "cwd": params.get("cwd"),
                         "model": params.get("model") or "fake-model",
                         "modelProvider": "fake",
                         "sandbox": params.get("sandbox")}})
    elif method == "turn/start":
        observed["turn_params"] = params
        send({"id": frame["id"], "result": {"turn": {"id": TURN,
                                                     "status": "inProgress"}}})
        run_turn()
    elif method == "thread/read":
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

    def _drive(self, mode, timeout_s=8.0):
        root, fake, diag_path = self._prepare()

        async def go():
            client = CodexAppServerClient(
                str(fake), root / "evidence",
                env={"FAKE_APPSERVER_MODE": mode,
                     "FAKE_APPSERVER_DIAG": str(diag_path),
                     "FAKE_APPSERVER_SECRET": SECRET})
            authority = SimpleContractAuthority(["cat TASK.md"], ["OUT.md"])
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


if __name__ == "__main__":
    unittest.main()
