"""Broker regression contracts; no live CLI, network, SSH, or scientific runtime."""
import asyncio
import copy
import io
import json
import math
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import AsyncMock, Mock, patch
from types import SimpleNamespace


from zcode_supervisor import protocol

class AuthorityFixture(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.workspace = self.root / "workspace"
        self.workspace.mkdir()
        self.frozen = self.workspace / "frozen.txt"
        self.frozen.write_text("Frozen input\n")
        self.runner = self.root / "trusted_runner.py"
        self.runner.write_text("print('real test result')\n")
        self.command = f"/usr/bin/python3 {self.runner} check"
        self.packet_path = self.root / "packet.json"
        self.packet = {
            "workspace": str(self.workspace),
            "allowed_files": ["result.txt", "editable.py"],
            "commands": [{"id": "check", "command": self.command,
                          "max_uses": 2, "max_timeout_ms": 5000}],
            "trusted_hashes": {str(self.runner): protocol.hashlib.sha256(
                self.runner.read_bytes()).hexdigest()},
        }
        self.packet_path.write_text(json.dumps(self.packet))
        self.authority = protocol.Authority(self.packet, self.packet_path,
                                            protocol.inventory(self.workspace))
        self.authority.session_id = "owned-session"
        self.authority.turn_id = "owned-turn"

    def request(self, **changes):
        value = {"sessionId": "owned-session", "turnId": "owned-turn",
                 "requestId": "request-1", "toolCallId": "call-1",
                 "toolName": "Bash", "input": {"command": self.command, "timeout": 1000}}
        value.update(changes)
        return value

    def decide(self, **changes):
        return self.authority.decide(self.request(**changes))["decision"]


class PermissionTests(AuthorityFixture):
    def test_exact_command_is_one_call_permission_without_persistent_grants(self):
        result = self.authority.decide(self.request())
        self.assertEqual(result["decision"], "allow")
        self.assertNotIn("permissionUpdates", result)
        self.assertNotIn("modifiedInput", result)

    def test_semicolon_newline_pipeline_substitution_and_prefix_are_denied(self):
        for index, command in enumerate([
            self.command + "; touch /tmp/unauthorized", self.command + "\necho extra",
            self.command + " | cat", self.command + " && true", self.command + " &",
            self.command + " $(echo added)", self.command + " > result.txt",
            "env PYTHONPATH=/tmp " + self.command, " " + self.command,
        ]):
            with self.subTest(command=command):
                self.assertEqual(self.decide(requestId=f"r-{index}", toolCallId=f"c-{index}",
                                            input={"command": command, "timeout": 1000}), "deny")
        self.assertEqual(self.authority.uses, {})

    def test_background_sandbox_and_unknown_fields_are_denied(self):
        for index, extra in enumerate([
            {"run_in_background": True}, {"dangerouslyDisableSandbox": True},
            {"cwd": "/tmp"}, {"env": {"PYTHONPATH": "/tmp"}},
        ]):
            with self.subTest(extra=extra):
                self.assertEqual(self.decide(requestId=f"r-{index}", toolCallId=f"c-{index}",
                    input={"command": self.command, "timeout": 1000, **extra}), "deny")

    def test_invalid_or_over_budget_timeouts_are_denied(self):
        for index, timeout in enumerate([True, False, -1, 0, math.nan, math.inf, "1000", 5001]):
            with self.subTest(timeout=timeout):
                self.assertEqual(self.decide(requestId=f"r-{index}", toolCallId=f"c-{index}",
                    input={"command": self.command, "timeout": timeout}), "deny")

    def test_command_budget_counts_approved_attempts(self):
        self.assertEqual(self.decide(), "allow")
        self.assertEqual(self.decide(requestId="r2", toolCallId="c2"), "allow")
        self.assertEqual(self.decide(requestId="r3", toolCallId="c3"), "deny")
        self.assertEqual(self.authority.uses["check"], 2)

    def test_wrong_or_missing_session_and_turn_are_denied(self):
        for extra in ({"sessionId": "someone-else"}, {"sessionId": None},
                      {"turnId": "old-turn"}, {"turnId": None}):
            with self.subTest(extra=extra):
                self.assertEqual(self.decide(**extra), "deny")
        self.assertEqual(self.authority.uses, {})

    def test_missing_request_or_tool_call_identity_is_denied(self):
        for extra in ({"requestId": None}, {"requestId": ""},
                      {"toolCallId": None}, {"toolCallId": ""}):
            with self.subTest(extra=extra):
                self.assertEqual(self.decide(**extra), "deny")

    def test_identity_fields_must_be_strings_without_crashing(self):
        for field in ("sessionId", "turnId", "requestId", "toolCallId"):
            for value in (True, 1, 1.0, [], {}, ["identity"], {"id": "identity"}):
                with self.subTest(field=field, value=value):
                    self.assertEqual(self.decide(**{field: value}), "deny")
        self.assertEqual(self.authority.uses, {})

    def test_repeated_request_id_is_denied(self):
        self.assertEqual(self.decide(), "allow")
        self.assertEqual(self.decide(toolCallId="a-new-call"), "deny")

    def test_repeated_tool_call_under_new_request_id_is_denied(self):
        self.assertEqual(self.decide(), "allow")
        self.assertEqual(self.decide(requestId="a-new-request"), "deny")

    def test_packet_mutation_refuses_even_exact_command(self):
        self.packet_path.write_text(self.packet_path.read_text() + "\n")
        self.assertEqual(self.decide(), "deny")
        self.assertIn("packet changed", self.authority.decisions[-1]["reason"])

    def test_trusted_runner_mutation_refuses_even_exact_command(self):
        self.runner.write_text("print('fake green')\n")
        self.assertEqual(self.decide(), "deny")
        self.assertIn("Trusted", self.authority.decisions[-1]["reason"])

    def test_frozen_input_mutation_or_deletion_refuses_command(self):
        self.frozen.write_text("changed")
        self.assertEqual(self.decide(), "deny")
        self.frozen.unlink()
        self.assertEqual(self.decide(requestId="r2", toolCallId="c2"), "deny")

    def test_allowed_output_change_does_not_hide_frozen_input_change(self):
        (self.workspace / "result.txt").write_text("ok")
        self.authority.check_frozen()
        self.frozen.write_text("also changed")
        with self.assertRaisesRegex(ValueError, "Frozen"):
            self.authority.check_frozen()

    def test_declared_file_write_allowed_and_outside_write_denied(self):
        self.assertEqual(self.decide(toolName="Write", input={"file_path": "result.txt", "content": "ok"}), "allow")
        self.assertEqual(self.decide(requestId="r2", toolCallId="c2", toolName="Write",
                                    input={"file_path": str(self.runner), "content": "bad"}), "deny")
        self.assertEqual(self.decide(requestId="r3", toolCallId="c3", toolName="Edit",
                                    input={"file_path": "frozen.txt", "old_string": "Frozen", "new_string": "Bad"}), "deny")

    def test_path_traversal_and_unlisted_tool_are_denied(self):
        self.assertEqual(self.decide(toolName="Write", input={"file_path": "../trusted_runner.py"}), "deny")
        self.assertEqual(self.decide(requestId="r2", toolCallId="c2", toolName="ExecutePython"), "deny")


class InventoryTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.workspace = self.root / "workspace"
        self.workspace.mkdir()

    def test_covers_large_files_and_permissions(self):
        (self.workspace / "build").mkdir()
        path = self.workspace / "build/large"
        path.write_bytes(b"x" * 1_100_001)
        before = protocol.inventory(self.workspace)
        with path.open("r+b") as stream:
            stream.seek(-1, 2)
            stream.write(b"y")
        self.assertNotEqual(before["build/large"], protocol.inventory(self.workspace)["build/large"])
        previous = protocol.inventory(self.workspace)
        path.chmod(0o700)
        self.assertNotEqual(previous["build/large"], protocol.inventory(self.workspace)["build/large"])

    def test_symbolic_links_are_rejected(self):
        (self.workspace / "link").symlink_to(self.root / "missing")
        with self.assertRaisesRegex(ValueError, "Symlink"):
            protocol.inventory(self.workspace)

    def test_hard_links_are_rejected(self):
        outside = self.root / "outside"
        outside.write_text("shared inode")
        os.link(outside, self.workspace / "hardlink")
        with self.assertRaisesRegex(ValueError, "unshared"):
            protocol.inventory(self.workspace)

    def test_special_files_are_rejected_without_opening_them(self):
        os.mkfifo(self.workspace / "pipe")
        with self.assertRaisesRegex(ValueError, "ordinary"):
            protocol.inventory(self.workspace)

    def test_file_as_inventory_root_is_rejected(self):
        path = self.root / "not-directory"
        path.write_text("file")
        with self.assertRaises(ValueError):
            protocol.inventory(path)


class ExecutionEvidenceTests(AuthorityFixture):
    def event(self, kind, **changes):
        payload = {"kind": kind, "toolCallId": "call-1"}
        if kind == "scheduled":
            payload.update(toolName="Bash", input={"command": self.command, "timeout": 1000})
        if kind == "result":
            payload["result"] = {"success": True, "content": json.dumps({
                "stdout": "ok", "stderr": "", "status": "completed", "exitCode": 0,
                "interrupted": False}), "truncated": False,
                "perf": {"detail": {"kind": "command", "command": {
                    "exitCode": 0, "timedOut": False, "status": "completed", "runMs": 82}}}}
        payload.update(changes)
        return {"sessionId": "owned-session", "turnId": "owned-turn", "type": "tool.updated",
                "payload": payload}

    def model_call(self, **changes):
        event = self.event("scheduled")
        event["type"] = "model.streaming"
        event["payload"].update(kind="tool_call", **changes)
        return event

    def omitted_schedule(self):
        event = self.event("scheduled", inputOmitted=True, inputRef="model_stream")
        del event["payload"]["input"]
        return event

    def test_pending_command_is_not_successful_evidence(self):
        self.decide()
        commands, problems = protocol.execution_evidence([self.event("scheduled")], self.authority)
        self.assertEqual(commands, [])
        self.assertTrue(problems)

    def test_tool_error_is_not_successful_evidence(self):
        self.decide()
        commands, problems = protocol.execution_evidence(
            [self.event("scheduled"), self.event("error", error="permission failed")], self.authority)
        self.assertEqual(commands, [])
        self.assertTrue(problems)

    def test_command_executed_without_broker_approval_is_rejected(self):
        commands, problems = protocol.execution_evidence(
            [self.event("scheduled"), self.event("result")], self.authority)
        self.assertEqual(commands, [])
        self.assertTrue(problems)

    def test_events_from_other_session_or_turn_do_not_satisfy_approval(self):
        self.decide()
        for changed in ({"sessionId": "foreign"}, {"turnId": "previous"}):
            with self.subTest(changed=changed):
                events = [{**self.event(kind), **changed} for kind in ("scheduled", "result")]
                commands, _ = protocol.execution_evidence(events, self.authority)
                self.assertEqual(commands, [])

    def test_tool_result_success_false_is_not_accepted(self):
        self.decide()
        commands, problems = protocol.execution_evidence(
            [self.event("scheduled"), self.event("result", result={"success": False, "content": "failed"})],
            self.authority)
        self.assertEqual(commands, [])
        self.assertTrue(problems)

    def test_scheduled_command_must_match_approved_input(self):
        self.decide()
        commands, problems = protocol.execution_evidence(
            [self.event("scheduled", input={"command": self.command + "; unexpected"}), self.event("result")],
            self.authority)
        self.assertEqual(commands, [])
        self.assertTrue(problems)

    def test_input_omitted_projection_still_binds_approval_and_tool_call(self):
        self.decide()
        scheduled = self.event("scheduled", inputOmitted=True)
        del scheduled["payload"]["input"]
        commands, problems = protocol.execution_evidence(
            [scheduled, self.event("result")], self.authority)
        self.assertEqual(problems, [])
        self.assertEqual([item["command_id"] for item in commands], ["check"])

    def test_edit_without_replace_all_matches_schema_default_approval(self):
        edit_input = {"file_path": str(self.workspace.resolve() / "editable.py"),
                      "old_string": "a", "new_string": "b"}
        decision = self.authority.decide(self.request(
            toolName="Edit", input={**edit_input, "replace_all": False}))
        self.assertEqual(decision["decision"], "allow")
        scheduled = self.event("scheduled", toolName="Edit", input=edit_input)
        streamed = self.model_call(toolName="Edit", input=edit_input)
        _, problems = protocol.execution_evidence(
            [streamed, scheduled, self.event("result")], self.authority)
        self.assertEqual(problems, [])

    def test_edit_streamed_text_change_is_still_rejected(self):
        edit_input = {"file_path": str(self.workspace.resolve() / "editable.py"),
                      "old_string": "a", "new_string": "b"}
        self.authority.decide(self.request(
            toolName="Edit", input={**edit_input, "replace_all": False}))
        changed = {**edit_input, "new_string": "unexpected"}
        scheduled = self.event("scheduled", toolName="Edit", input=changed)
        streamed = self.model_call(toolName="Edit", input=changed)
        _, problems = protocol.execution_evidence(
            [streamed, scheduled, self.event("result")], self.authority)
        self.assertTrue(problems)

    def test_duplicate_scheduled_or_terminal_event_is_rejected(self):
        self.decide()
        for duplicate in ("scheduled", "result"):
            with self.subTest(duplicate=duplicate):
                _, problems = protocol.execution_evidence(
                    [self.event("scheduled"), self.event("result"), self.event(duplicate)],
                    self.authority)
                self.assertTrue(problems)

    def test_approved_call_missing_all_execution_events_is_rejected(self):
        self.decide()
        commands, problems = protocol.execution_evidence([], self.authority)
        self.assertEqual(commands, [])
        self.assertTrue(problems)

    def test_terminal_event_without_scheduling_is_rejected(self):
        self.decide()
        commands, problems = protocol.execution_evidence([self.event("result")], self.authority)
        self.assertEqual(commands, [])
        self.assertTrue(problems)

    def test_actual_structured_command_exit_zero_is_accepted(self):
        self.decide()
        commands, problems = protocol.execution_evidence(
            [self.event("scheduled"), self.event("result")], self.authority)
        self.assertEqual(problems, [])
        self.assertEqual([entry["command_id"] for entry in commands], ["check"])

    def test_command_exit_code_requires_exact_integer_zero(self):
        self.decide()
        for exit_code in (1, -9, False, True, 0.0, "0", None):
            with self.subTest(exit_code=exit_code):
                result = self.event("result")
                result["payload"]["result"]["perf"]["detail"]["command"]["exitCode"] = exit_code
                commands, problems = protocol.execution_evidence(
                    [self.event("scheduled"), result], self.authority)
                self.assertEqual(commands, [])
                self.assertTrue(problems)

    def test_timeout_flag_requires_explicit_false(self):
        self.decide()
        for timed_out in (True, None, 0, "false"):
            with self.subTest(timed_out=timed_out):
                result = self.event("result")
                result["payload"]["result"]["perf"]["detail"]["command"]["timedOut"] = timed_out
                commands, problems = protocol.execution_evidence(
                    [self.event("scheduled"), result], self.authority)
                self.assertEqual(commands, [])
                self.assertTrue(problems)

    def test_terminal_status_must_be_completed_not_background_or_failure(self):
        self.decide()
        for status in ("failed", "timed_out", "cancelled", "spawn_error", "backgrounded", None):
            with self.subTest(status=status):
                result = self.event("result")
                result["payload"]["result"]["perf"]["detail"]["command"]["status"] = status
                commands, problems = protocol.execution_evidence(
                    [self.event("scheduled"), result], self.authority)
                self.assertEqual(commands, [])
                self.assertTrue(problems)

    def test_missing_exit_evidence_cannot_be_replaced_by_reported_stdout(self):
        self.decide()
        for field in ("exitCode", "timedOut", "status"):
            with self.subTest(field=field):
                result = self.event("result")
                del result["payload"]["result"]["perf"]["detail"]["command"][field]
                commands, problems = protocol.execution_evidence(
                    [self.event("scheduled"), result], self.authority)
                self.assertEqual(commands, [])
                self.assertTrue(problems)
        result = self.event("result")
        del result["payload"]["result"]["perf"]
        # content still contains exitCode=0; stdout is not trusted execution metadata.
        commands, problems = protocol.execution_evidence(
            [self.event("scheduled"), result], self.authority)
        self.assertEqual(commands, [])
        self.assertTrue(problems)

    def test_malformed_or_wrong_kind_command_metadata_fails_closed(self):
        self.decide()
        for perf in (None, [], {"detail": None}, {"detail": []},
                     {"detail": {"kind": "command", "command": []}},
                     {"detail": {"kind": "filesystem", "command": {
                         "exitCode": 0, "timedOut": False, "status": "completed"}}}):
            with self.subTest(perf=perf):
                result = self.event("result")
                result["payload"]["result"]["perf"] = perf
                commands, problems = protocol.execution_evidence(
                    [self.event("scheduled"), result], self.authority)
                self.assertEqual(commands, [])
                self.assertTrue(problems)

    def test_full_model_tool_call_matches_approval_when_scheduled_input_omitted(self):
        self.decide()
        commands, problems = protocol.execution_evidence(
            [self.model_call(), self.omitted_schedule(), self.event("result")], self.authority)
        self.assertEqual(problems, [])
        self.assertEqual([item["command_id"] for item in commands], ["check"])

    def test_mismatched_model_input_is_rejected_despite_omitted_schedule(self):
        self.decide()
        for value in ({"command": self.command + "; true", "timeout": 1000}, None, []):
            with self.subTest(input=value):
                commands, problems = protocol.execution_evidence(
                    [self.model_call(input=value), self.omitted_schedule(), self.event("result")],
                    self.authority)
                self.assertEqual(commands, [])
                self.assertTrue(problems)

    def test_model_tool_name_must_match_scheduled_and_approved_tool(self):
        self.decide()
        commands, problems = protocol.execution_evidence(
            [self.model_call(toolName="Read"), self.omitted_schedule(), self.event("result")],
            self.authority)
        self.assertEqual(commands, [])
        self.assertTrue(problems)

    def test_foreign_model_stream_does_not_contaminate_current_turn(self):
        self.decide()
        for identity in ({"sessionId": "foreign"}, {"turnId": "previous-turn"}):
            with self.subTest(identity=identity):
                foreign = {**self.model_call(input={"command": "wrong"}), **identity}
                commands, problems = protocol.execution_evidence(
                    [foreign, self.model_call(), self.omitted_schedule(), self.event("result")],
                    self.authority)
                self.assertEqual(problems, [])
                self.assertEqual([item["command_id"] for item in commands], ["check"])

    def test_conflicting_stream_inputs_cannot_be_hidden_by_last_value_wins(self):
        self.decide()
        matching = self.model_call()
        mismatched = self.model_call(input={"command": "unexpected"})
        for streams in ([matching, mismatched], [mismatched, matching]):
            with self.subTest(streams=streams):
                _, problems = protocol.execution_evidence(
                    [*streams, self.omitted_schedule(), self.event("result")], self.authority)
                self.assertTrue(problems)


class TransportRedactionTests(unittest.TestCase):
    def test_recursive_headers_and_normalized_secret_field_names_are_redacted(self):
        secret_names = ("headers", "requestHeaders", "response_headers", "AUTHORIZATION",
                        "api-key", "access_token", "RefreshToken", "Cookie", "set-cookie",
                        "clientSecret", "Password")
        value = {"events": [{"nested": {name: "private-value" for name in secret_names}}]}
        cleaned = protocol.redact_transport(value)
        self.assertNotIn("private-value", json.dumps(cleaned))
        for name in secret_names:
            self.assertEqual(cleaned["events"][0]["nested"][name], "[redacted]")

    def test_redaction_preserves_execution_evidence_and_does_not_mutate_input(self):
        value = {"requestHeaders": {"Authorization": "private"}, "requestHeaderCount": 1,
                 "toolCallId": "call-x", "input": {"command": "/trusted/run test"},
                 "perf": {"detail": {"kind": "command", "command": {
                     "exitCode": 0, "timedOut": False, "status": "completed"}}},
                 "usage": {"inputTokens": 100}, "other": [None, False, 7]}
        cleaned = protocol.redact_transport(value)
        self.assertEqual(value["requestHeaders"]["Authorization"], "private")
        self.assertEqual(cleaned["requestHeaders"], "[redacted]")
        for key in ("requestHeaderCount", "toolCallId", "input", "perf", "usage", "other"):
            self.assertEqual(cleaned[key], value[key])

    def test_repeated_redaction_is_idempotent(self):
        value = {"headers": {"Authorization": "private"}, "exitCode": 0}
        once = protocol.redact_transport(value)
        self.assertEqual(protocol.redact_transport(once), once)


class ProtocolStreamTests(unittest.IsolatedAsyncioTestCase):
    async def stream_failure(self, payload):
        client = protocol.ProtocolClient(Path("/tmp/frozen"), Path("/tmp/not-written"), lambda _: {})
        client.wire = io.StringIO()
        client.process = Mock()
        client.process.stdout = asyncio.StreamReader()
        client.process.stdout.feed_data(payload)
        client.process.stdout.feed_eof()
        future = asyncio.get_running_loop().create_future()
        client.pending["host-1"] = future
        await client.read_loop()
        self.assertTrue(client.failure)
        with self.assertRaises(Exception):
            await future
        self.assertEqual(client.pending, {})

    async def test_non_json_stdout_fails_pending_requests(self):
        await self.stream_failure(b"debug accidentally printed to stdout\n")

    async def test_unmatched_response_fails_pending_requests(self):
        await self.stream_failure(b'{"id":"unknown","result":{}}\n')

    async def test_eof_fails_pending_requests(self):
        await self.stream_failure(b"")

    async def test_persisted_session_events_redact_transport_headers(self):
        client = protocol.ProtocolClient(Path("/tmp/frozen"), Path("/tmp/not-written"), lambda _: {})
        client.wire = io.StringIO()
        client.process = Mock()
        client.process.stdout = asyncio.StreamReader()
        frame = {"method": "session/event", "params": {"type": "session.updated", "payload": {
            "requestHeaders": {"Authorization": "fixture-secret"}, "requestHeaderCount": 1,
            "toolCallId": "call-1", "result": {"exitCode": 0}}}}
        client.process.stdout.feed_data((json.dumps(frame) + "\n").encode())
        client.process.stdout.feed_eof()
        await client.read_loop()
        saved = client.wire.getvalue()
        self.assertNotIn("fixture-secret", saved)
        payload = json.loads(saved)["params"]["payload"]
        self.assertEqual(payload["requestHeaders"], "[redacted]")
        self.assertEqual(payload["requestHeaderCount"], 1)
        self.assertEqual(payload["result"]["exitCode"], 0)

    async def test_turn_callback_precedes_immediately_following_permission(self):
        observed = []
        state = {"turn_id": None}
        def decide(params):
            observed.append(state["turn_id"])
            return {"decision": "deny", "reason": "unit fixture"}
        client = protocol.ProtocolClient(Path("/tmp/frozen"), Path("/tmp/not-written"), decide)
        client.wire = io.StringIO()
        client.process = Mock()
        client.process.stdout = asyncio.StreamReader()
        client.on_event = lambda event: state.update(turn_id=event.get("turnId"))
        async def no_transport(_frame):
            return None
        client.send = no_transport
        frames = [
            {"method": "session/event", "params": {"type": "turn.started", "turnId": "new-turn"}},
            {"id": "permission-frame", "method": "interaction/requestPermission", "params": {}},
        ]
        client.process.stdout.feed_data(("\n".join(json.dumps(frame) for frame in frames) + "\n").encode())
        client.process.stdout.feed_eof()
        await client.read_loop()
        await asyncio.gather(*client.tasks)
        self.assertEqual(observed, ["new-turn"])


class PacketValidationTests(unittest.TestCase):
    def setUp(self):
        self.packet = {
            "workspace": "/work/worker", "prompt": "Run the exact command.",
            "allowed_files": ["REPORT.md"],
            "commands": [{"id": "unit-tests", "command": "/bin/true"}],
            "trusted_hashes": {"/work/runner.py": "a" * 64},
        }

    def test_minimal_valid_packet_accepts_an_empty_mutation_scope(self):
        protocol.validate_packet(self.packet)
        protocol.validate_packet({"workspace": "/work/worker", "prompt": "Read input.",
                                  "allowed_files": []})

    def test_packet_shape_prompt_and_workspace_are_required(self):
        for value in (None, [], "task"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                protocol.validate_packet(value)
        for field, values in {
                "prompt": [None, False, "", "  "],
                "workspace": [None, ".", "../worker", "/work/../worker", "/work//worker"],
                "allowed_files": [None, "REPORT.md", {}, [1], [None], [["nested"]]],
                "commands": [None, "command", {}, [None], ["command"]],
                "trusted_hashes": [None, [], "hash"],
        }.items():
            for value in values:
                with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                    protocol.validate_packet({**self.packet, field: value})
        with self.assertRaises(ValueError):
            protocol.validate_packet({**self.packet, "surprise_permission": True})

    def test_allowed_paths_are_unique_normalized_relative_files(self):
        for value in ("", ".", "..", "../outside", "/absolute", "a/../b", "a//b", "a/", "a/./b"):
            with self.subTest(value=value), self.assertRaises(ValueError):
                protocol.validate_packet({**self.packet, "allowed_files": [value]})
        with self.assertRaises(ValueError):
            protocol.validate_packet({**self.packet, "allowed_files": ["same", "same"]})

    def test_command_identifiers_and_text_are_unique(self):
        for field, values in {"id": [None, 1, "", "a b", "../x"],
                              "command": [None, True, "", "  ", "a\x00b"]}.items():
            for value in values:
                command = {**self.packet["commands"][0], field: value}
                with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                    protocol.validate_packet({**self.packet, "commands": [command]})
        for command in ({"id": "unit-tests", "command": "/bin/false"},
                        {"id": "second", "command": "/bin/true"}):
            with self.subTest(command=command), self.assertRaises(ValueError):
                protocol.validate_packet({**self.packet, "commands": self.packet["commands"] + [command]})

    def test_command_budgets_are_positive_finite_and_typed(self):
        for field, values in {
                "max_uses": [0, -1, 1.0, True, False, None, "1"],
                "max_timeout_ms": [0, -1, True, False, None, "1", math.inf, math.nan],
                "required": [None, 0, 1, "true"],
        }.items():
            for value in values:
                command = {**self.packet["commands"][0], field: value}
                with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                    protocol.validate_packet({**self.packet, "commands": [command]})

    def test_trusted_hashes_require_absolute_paths_and_exact_digests(self):
        for name, value in (("relative", "a" * 64), ("/good", "a" * 63),
                            ("/good", "A" * 64), ("/good", None), ("/good", [])):
            with self.subTest(name=name, value=value), self.assertRaises(ValueError):
                protocol.validate_packet({**self.packet, "trusted_hashes": {name: value}})


class LocationValidationTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.workspace = self.root / "worker"
        self.workspace.mkdir()
        self.output = self.root / "evidence/run.json"
        self.packet = self.root / "packet.json"
        self.config = SimpleNamespace(work_root=self.root, state_dir=self.root / "state",
                                      cache_dir=self.root / "cache")

    def test_worker_and_controller_paths_are_separate(self):
        self.assertEqual(protocol.checked_locations(self.workspace, self.output, self.config, self.packet),
                         (self.workspace, self.output))

    def test_worker_cannot_equal_work_root_or_live_outside_it(self):
        for workspace in (self.root, self.root.parent):
            with self.subTest(workspace=workspace), self.assertRaises(ValueError):
                protocol.checked_locations(workspace, self.output, self.config)

    def test_worker_path_symlink_is_rejected(self):
        alias = self.root / "alias"
        alias.symlink_to(self.workspace, target_is_directory=True)
        with self.assertRaises(ValueError):
            protocol.checked_locations(alias, self.output, self.config)
        nested = self.workspace / "nested"
        nested.mkdir()
        with self.assertRaises(ValueError):
            protocol.checked_locations(alias / "nested", self.output, self.config)

    def test_all_controller_locations_must_remain_outside_worker(self):
        for field in ("state_dir", "cache_dir"):
            with self.subTest(field=field), self.assertRaises(ValueError):
                config = copy.copy(self.config)
                setattr(config, field, self.workspace / "controller")
                protocol.checked_locations(self.workspace, self.output, config)
        with self.assertRaises(ValueError):
            protocol.checked_locations(self.workspace, self.workspace / "out.json", self.config)
        with self.assertRaises(ValueError):
            protocol.checked_locations(self.workspace, self.output, self.config,
                                       self.workspace / "packet.json")
        for field in ("state_dir", "cache_dir"):
            config = copy.copy(self.config)
            setattr(config, field, self.root)
            with self.subTest(field=field), self.assertRaises(ValueError):
                protocol.checked_locations(self.workspace, self.output, config)

    def test_evidence_requires_fresh_json_basename_under_work_root(self):
        for output in (self.root.parent / "outside.json", self.root / "not-json.txt"):
            with self.subTest(output=output), self.assertRaises(ValueError):
                protocol.checked_locations(self.workspace, output, self.config)
        self.output.parent.mkdir()
        for suffix in (".json", ".events.jsonl", ".stderr.log", ".before.json"):
            path = self.output.with_suffix(suffix)
            path.touch()
            with self.subTest(suffix=suffix), self.assertRaises(ValueError):
                protocol.checked_locations(self.workspace, self.output, self.config)
            path.unlink()


class FileEvidenceTests(AuthorityFixture):
    event = ExecutionEvidenceTests.event
    model_call = ExecutionEvidenceTests.model_call

    def file_events(self):
        data = {"file_path": "result.txt", "content": "ok"}
        self.decide(toolName="Write", input=data)
        return [self.event("scheduled", toolName="Write", input=data),
                self.event("result", result={"success": True})]

    def test_file_result_malformed_or_failed_is_rejected_without_crashing(self):
        events = self.file_events()
        for result in (None, [], "ok", False, {}, {"success": False}, {"success": 1}):
            candidate = copy.deepcopy(events)
            candidate[-1]["payload"]["result"] = result
            with self.subTest(result=result):
                commands, problems = protocol.execution_evidence(candidate, self.authority)
                self.assertEqual(commands, [])
                self.assertTrue(problems)

    def test_file_approval_without_actual_events_is_not_success(self):
        self.file_events()
        _, problems = protocol.execution_evidence([], self.authority)
        self.assertTrue(problems)

    def test_file_stream_tool_mismatch_and_missing_input_are_rejected(self):
        events = self.file_events()
        for stream in (self.model_call(toolName="Bash", input=events[0]["payload"]["input"]),
                       self.model_call(toolName="Write", input=None)):
            with self.subTest(stream=stream):
                _, problems = protocol.execution_evidence([stream, *events], self.authority)
                self.assertTrue(problems)

    def test_unknown_executed_tool_is_rejected(self):
        events = [self.event("scheduled", toolName="ExecutePython"), self.event("result")]
        _, problems = protocol.execution_evidence(events, self.authority)
        self.assertTrue(problems)

    def test_malformed_event_ids_and_payloads_fail_closed(self):
        for value in (None, [], {}, True):
            events = [self.event("scheduled", toolCallId=value)]
            _, problems = protocol.execution_evidence(events, self.authority)
            self.assertTrue(problems)
        event = self.event("scheduled")
        event["payload"] = None
        _, problems = protocol.execution_evidence([event], self.authority)
        self.assertTrue(problems)


class ProtocolLifecycleTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.config = Mock()
        self.config.command.return_value = ["chosen-node", "chosen-cli.cjs"]
        self.config.environment.return_value = {"FIXTURE": "1"}

    async def test_missing_config_never_starts_a_process(self):
        client = protocol.ProtocolClient(self.root, self.root / "run.json", lambda _: {})
        with self.assertRaises(ValueError):
            await client.start()
        await client.close()

    async def test_spawn_failure_closes_partially_opened_evidence(self):
        client = protocol.ProtocolClient(self.root, self.root / "run.json", lambda _: {}, self.config)
        with patch.object(protocol.asyncio, "create_subprocess_exec", new=AsyncMock(
                side_effect=OSError("fixture spawn failure"))):
            with self.assertRaises(OSError):
                await client.start()
        self.assertTrue(client.wire.closed)
        self.assertTrue(client.stderr.closed)
        self.assertIsNone(client.process)
        await client.close()

    async def test_failed_second_log_open_still_closes_first_log(self):
        output = self.root / "run.json"
        output.with_suffix(".events.jsonl").touch()
        client = protocol.ProtocolClient(self.root, output, lambda _: {}, self.config)
        with self.assertRaises(FileExistsError):
            await client.start()
        self.assertTrue(client.stderr.closed)
        self.assertIsNone(client.wire)

    async def test_process_receives_only_configured_command_and_environment(self):
        child = Mock()
        child.stdout = asyncio.StreamReader()
        child.stdout.feed_eof()
        child.wait = AsyncMock(return_value=0)
        client = protocol.ProtocolClient(self.root, self.root / "run.json", lambda _: {}, self.config)
        with patch.object(protocol.asyncio, "create_subprocess_exec", new=AsyncMock(return_value=child)) as spawn:
            await client.start()
        self.assertEqual(spawn.call_args.args,
                         ("chosen-node", "chosen-cli.cjs", "app-server", "--stdio", "--cwd", str(self.root)))
        self.assertEqual(spawn.call_args.kwargs["env"], {"FIXTURE": "1"})
        await client.close()
        self.assertTrue(client.wire.closed)
        self.assertTrue(client.stderr.closed)
        child.stdin.close.assert_called_once()

    async def test_send_failure_removes_pending_request(self):
        client = protocol.ProtocolClient(self.root, self.root / "run.json", lambda _: {})
        with self.assertRaises(EOFError):
            await client.request("session/read", {})
        self.assertEqual(client.pending, {})

    async def test_reverse_callback_exception_is_reported(self):
        def broken(_params):
            raise ValueError("permission callback failed")
        client = protocol.ProtocolClient(self.root, self.root / "run.json", broken)
        client.wire = io.StringIO()
        task = asyncio.create_task(client.reverse({"id": "p", "method": "interaction/requestPermission",
                                                   "params": {}}))
        task.add_done_callback(client.reverse_done)
        await asyncio.gather(task, return_exceptions=True)
        self.assertIn("permission callback failed", client.failure)


if __name__ == "__main__":
    unittest.main()
