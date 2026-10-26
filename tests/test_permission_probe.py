"""P1-4 permission probe harness contracts; no live CLI, no model task.

FakeClient replaces zcode_supervisor.permission_probe.ProtocolClient, so the
event/callback flow is exercised without ever launching zcode.cjs.
"""

import io
import json
import os
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from typing import ClassVar
from unittest.mock import patch

from zcode_supervisor import permission_probe as pp

SESSION_ID = "sess-probe-1"


class FakeClient:
    """Duck-typed ProtocolClient emitting the fixed probe event sequence.

    Scenarios (class attributes):
    - bypass_writes: Bash(touch) and Write never reach the permission callback
      and report successful execution (the FAIL bypass shape).
    - actually_write: the worker really writes PROBE-WRITE-MARKER.txt during
      send even though callbacks fire and are denied (delta-only FAIL shape).
    """

    bypass_writes = False
    actually_write = False
    instances: ClassVar[list["FakeClient"]] = []

    def __init__(self, workspace, output, permission, config=None, orchestration=None):
        self.workspace = Path(workspace)
        self.output = output
        self.permission = permission
        self.config = config
        self.orchestration = orchestration
        self.events = []
        self.requests = []
        self.permission_responses = []
        self.failure = None
        self.started = False
        self.closed = False
        type(self).instances.append(self)

    async def start(self):
        self.started = True

    async def close(self):
        self.closed = True

    async def request(self, method, params):
        self.requests.append((method, params))
        if method == "session/create":
            return {"session": {"sessionId": SESSION_ID}}
        if method == "session/subscribe":
            return {"eventSeq": 0}
        if method == "session/send":
            self.emit_probe_events()
            return {}
        raise AssertionError(f"unexpected request: {method}")

    def _event(self, etype, payload):
        self.events.append(
            {
                "type": etype,
                "sessionId": SESSION_ID,
                "seq": len(self.events) + 1,
                "payload": payload,
            }
        )

    def _stream(self, call_id, tool, tool_input):
        self._event(
            "model.streaming",
            {
                "kind": "tool_call",
                "toolCallId": call_id,
                "toolName": tool,
                "input": tool_input,
            },
        )

    def _updated(self, call_id, tool, kind, result=None):
        payload = {"kind": kind, "toolCallId": call_id, "toolName": tool}
        if result is not None:
            payload["result"] = result
        self._event("tool.updated", payload)

    def _ask_permission(self, call_id, tool, tool_input):
        response = self.permission(
            {
                "sessionId": SESSION_ID,
                "requestId": f"req-{call_id}",
                "toolCallId": call_id,
                "toolName": tool,
                "input": tool_input,
            }
        )
        self.permission_responses.append((call_id, response))

    def emit_probe_events(self):
        # 1) Read TASK.md: read-only, executes without any callback.
        self._stream("call-read", "Read", {"file_path": "TASK.md"})
        self._updated("call-read", "Read", "scheduled")
        self._updated("call-read", "Read", "result", {"success": True})
        # 2) Bash echo: known low-risk native auto-pass, no callback.
        echo = {"command": "/bin/sh -c 'echo probe-readonly'"}
        self._stream("call-echo", "Bash", echo)
        self._updated("call-echo", "Bash", "scheduled")
        self._updated(
            "call-echo",
            "Bash",
            "result",
            {"success": True, "perf": {"detail": {"command": {"exitCode": 0}}}},
        )
        # 3) Bash touch: a write path; must be denied via the callback.
        touch = {"command": "/bin/sh -c 'touch PROBE-WRITE-MARKER.txt'"}
        self._stream("call-touch", "Bash", touch)
        self._updated("call-touch", "Bash", "scheduled")
        if self.bypass_writes:
            self._updated(
                "call-touch",
                "Bash",
                "result",
                {"success": True, "perf": {"detail": {"command": {"exitCode": 0}}}},
            )
        else:
            self._ask_permission("call-touch", "Bash", touch)
            self._updated(
                "call-touch",
                "Bash",
                "result",
                {"success": False, "perf": {"detail": {"command": {"exitCode": 1}}}},
            )
        if self.actually_write:
            (self.workspace / "PROBE-WRITE-MARKER.txt").write_text("")
        # 4) Write: a write path; must be denied via the callback.
        write = {"file_path": "PROBE-OUT.md", "content": "probe"}
        self._stream("call-write", "Write", write)
        self._updated("call-write", "Write", "scheduled")
        if self.bypass_writes:
            self._updated("call-write", "Write", "result", {"success": True})
        else:
            self._ask_permission("call-write", "Write", write)
            self._event(
                "tool.updated",
                {"kind": "error", "toolCallId": "call-write", "toolName": "Write"},
            )
        self._event("turn.completed", {"resultType": "success"})


class BypassingClient(FakeClient):
    bypass_writes = True


class ActuallyWritingClient(FakeClient):
    actually_write = True


class ProbeFixture(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.workspace = self.root / "workspace"
        self.workspace.mkdir()
        self.out_dir = self.root / "out"
        # Fake CLI file so probe_config never touches the real install.
        self.cli = self.root / "zcode.cjs"
        self.cli.write_text("// fake bundle\n")
        self.config_dir = self.root / "czs-config"
        self.config_dir.mkdir()
        env = {pp.CLI_ENV: str(self.cli), pp.CONFIG_DIR_ENV: str(self.config_dir)}
        self._env_patch = patch.dict(os.environ, env)
        self._env_patch.start()
        self.addCleanup(self._env_patch.stop)


class DenyAllRecorderTests(ProbeFixture):
    def test_records_every_callback_and_always_denies(self):
        recorder = pp.DenyAllRecorder()
        first = recorder(
            {
                "toolName": "Bash",
                "toolCallId": "c1",
                "requestId": "r1",
                "input": {"command": "touch PROBE-OUT.md"},
            }
        )
        second = recorder(
            {
                "toolName": "Write",
                "toolCallId": "c2",
                "input": {"file_path": "PROBE-OUT.md"},
            }
        )
        self.assertEqual(
            first, {"decision": "deny", "reason": "permission probe: deny all"}
        )
        self.assertEqual(second, first)
        self.assertEqual(
            [record["decision"] for record in recorder.records], ["deny", "deny"]
        )
        self.assertEqual(recorder.records[0]["tool"], "Bash")
        self.assertEqual(recorder.records[0]["tool_call_id"], "c1")
        self.assertIn("touch PROBE-OUT.md", recorder.records[0]["input_summary"])
        self.assertEqual(recorder.records[1]["subject"], "PROBE-OUT.md")


class ReadonlyClassificationTests(ProbeFixture):
    def test_bare_readonly_commands_are_not_write_paths(self):
        for command in (
            "cat TASK.md",
            "ls -la",
            "echo hi",
            "head -3 x",
            "tail -3 x",
            "grep foo bar",
            "find . -name x",
            "pwd",
            "wc -l x",
        ):
            self.assertTrue(pp.is_readonly_bash(command), command)

    def test_probe_and_write_commands_are_write_paths(self):
        for command in (
            "/bin/sh -c 'echo probe-readonly'",
            "/bin/sh -c 'touch PROBE-WRITE-MARKER.txt'",
            "echo x && touch y",
            "rm -rf /tmp/x",
            "node s.js",
            "",
        ):
            self.assertEqual(
                pp.is_readonly_bash(command),
                command == "/bin/sh -c 'echo probe-readonly'",
                command,
            )


class CallbackMatchingTests(ProbeFixture):
    def test_exact_tool_call_id_match(self):
        events = [
            {
                "type": "tool.updated",
                "sessionId": SESSION_ID,
                "seq": 1,
                "payload": {
                    "kind": "scheduled",
                    "toolCallId": "c1",
                    "toolName": "Bash",
                },
            }
        ]
        records = [
            {
                "tool": "Bash",
                "tool_call_id": "c1",
                "request_id": "r",
                "subject": None,
                "input_summary": "",
                "decision": "deny",
            }
        ]
        rows = pp.collect_tool_calls(events, records)
        self.assertTrue(rows[0]["callback_seen"])

    def test_approximate_match_without_tool_call_id(self):
        events = [
            {
                "type": "model.streaming",
                "sessionId": SESSION_ID,
                "seq": 1,
                "payload": {
                    "kind": "tool_call",
                    "toolCallId": "c9",
                    "toolName": "Bash",
                    "input": {"command": "touch X.txt"},
                },
            },
            {
                "type": "tool.updated",
                "sessionId": SESSION_ID,
                "seq": 2,
                "payload": {
                    "kind": "result",
                    "toolCallId": "c9",
                    "toolName": "Bash",
                    "result": {"success": True},
                },
            },
        ]
        records = [
            {
                "tool": "Bash",
                "tool_call_id": None,
                "request_id": "r",
                "subject": "touch X.txt",
                "input_summary": '{"command": "touch X.txt"}',
                "decision": "deny",
            }
        ]
        rows = pp.collect_tool_calls(events, records)
        self.assertTrue(rows[0]["callback_seen"])
        self.assertTrue(rows[0]["write_path"])
        self.assertTrue(rows[0]["executed"])


class CreateParamsTests(ProbeFixture):
    def test_native_noallowlist_omits_tool_allowlist_key(self):
        params = pp.create_params("native-noallowlist", self.workspace)
        self.assertNotIn("toolAllowlist", params)
        self.assertNotIn("toolDenylist", params)
        self.assertEqual(params["mode"], "build")
        self.assertEqual(params["workspace"]["workspacePath"], str(self.workspace))

    def test_patched_control_carries_the_four_tool_allowlist(self):
        params = pp.create_params("patched-control", self.workspace)
        self.assertEqual(params["toolAllowlist"], ["Read", "Write", "Edit", "Bash"])

    def test_denylist_subtracts_unrelated_tools_only(self):
        params = pp.create_params("denylist", self.workspace)
        self.assertNotIn("toolAllowlist", params)
        self.assertEqual(params["toolDenylist"], list(pp.DENYLIST_TOOLS))
        self.assertFalse(set(pp.DENYLIST_TOOLS) & set(pp.PROBE_TOOLS))

    def test_model_passes_through(self):
        model = {"providerId": "p", "modelId": "m"}
        params = pp.create_params("native-noallowlist", self.workspace, model)
        self.assertEqual(params["model"], model)


class ProbeConfigTests(ProbeFixture):
    def test_native_mode_uses_probe_cli_env(self):
        config = pp.probe_config("native-noallowlist", self.config_dir)
        self.assertEqual(config.command(), ["node", str(self.cli)])
        self.assertEqual(config.cli_path, self.cli)
        environment = config.environment()
        # Parity with runtime.Config.environment() without a provider config:
        # ambient environment plus thread guards, nothing added or removed
        # (an ambient provider variable is inherited, never overridden).
        expected = os.environ.copy()
        expected["OPENBLAS_NUM_THREADS"] = "1"
        expected["OMP_NUM_THREADS"] = "1"
        self.assertEqual(environment, expected)

    def test_native_mode_rejects_missing_cli(self):
        with (
            patch.dict(os.environ, {pp.CLI_ENV: str(self.root / "missing.cjs")}),
            self.assertRaises(pp.ProbeError),
        ):
            pp.probe_config("denylist", self.config_dir)

    def test_patched_control_requires_czs_config(self):
        with self.assertRaises(pp.ProbeError):
            pp.probe_config("patched-control", self.config_dir)

    def test_unknown_mode_is_rejected(self):
        with self.assertRaises(pp.ProbeError):
            pp.probe_config("yolo", self.config_dir)

    def test_czs_config_contributes_node_and_provider_env(self):
        roots = self.root / "roots"
        provider = self.root / "provider.json"
        provider.write_text("{}")
        (self.config_dir / "config.json").write_text(
            json.dumps(
                {
                    "schema_version": 1,
                    "work_root": str(roots / "work"),
                    "state_dir": str(roots / "state"),
                    "cache_dir": str(roots / "cache"),
                    "cli_path": str(self.root / "installed.cjs"),
                    "node": "/usr/bin/env",
                    "compatibility": "pinned-0.16.9",
                    "provider_config": str(provider),
                }
            )
        )
        config = pp.probe_config("native-noallowlist", self.config_dir)
        self.assertEqual(config.command(), ["/usr/bin/env", str(self.cli)])
        expected = os.environ.copy()
        expected["OPENBLAS_NUM_THREADS"] = "1"
        expected["OMP_NUM_THREADS"] = "1"
        expected["ZCODE_BUILTIN_PROVIDER_CONFIG_FILE"] = str(provider)
        self.assertEqual(config.environment(), expected)


class HookModeTests(unittest.IsolatedAsyncioTestCase, ProbeFixture):
    async def test_hook_reports_unsupported_without_launching_anything(self):
        FakeClient.instances.clear()
        with patch.object(pp, "ProtocolClient", FakeClient):
            report = await pp.run_probe("hook", self.workspace, self.out_dir)
        self.assertEqual(report["mode"], "hook")
        self.assertEqual(report["status"], "unsupported")
        self.assertIn("reason", report)
        self.assertEqual(FakeClient.instances, [])
        saved = json.loads((self.out_dir / "probe-hook.json").read_text())
        self.assertEqual(saved["status"], "unsupported")


class RunProbeTests(unittest.IsolatedAsyncioTestCase, ProbeFixture):
    async def test_pass_verdict_when_all_write_paths_hit_the_callback(self):
        FakeClient.instances.clear()
        with patch.object(pp, "ProtocolClient", FakeClient):
            report = await pp.run_probe(
                "native-noallowlist", self.workspace, self.out_dir
            )
        self.assertEqual(report["verdict"], "PASS")
        self.assertEqual(report["turn_outcome"], "completed")
        self.assertEqual(report["bypass"], [])
        self.assertEqual(
            report["workspace_delta"], {"new": [], "changed": [], "removed": []}
        )
        client = FakeClient.instances[0]
        self.assertTrue(client.started and client.closed)
        # create params actually sent: no toolAllowlist key in this mode.
        method, params = client.requests[0]
        self.assertEqual(method, "session/create")
        self.assertNotIn("toolAllowlist", params)
        self.assertEqual(
            client.permission_responses,
            [
                (
                    "call-touch",
                    {"decision": "deny", "reason": "permission probe: deny all"},
                ),
                (
                    "call-write",
                    {"decision": "deny", "reason": "permission probe: deny all"},
                ),
            ],
        )
        by_id = {row["tool_call_id"]: row for row in report["tool_calls"]}
        self.assertEqual(
            sorted(by_id), ["call-echo", "call-read", "call-touch", "call-write"]
        )
        self.assertFalse(by_id["call-echo"]["callback_seen"])  # auto-pass
        self.assertFalse(by_id["call-echo"]["write_path"])
        self.assertTrue(by_id["call-echo"]["executed"])
        self.assertEqual(by_id["call-echo"]["exit_code"], 0)
        self.assertFalse(by_id["call-read"]["write_path"])
        for call_id in ("call-touch", "call-write"):
            self.assertTrue(by_id[call_id]["callback_seen"])
            self.assertTrue(by_id[call_id]["write_path"])
            self.assertFalse(by_id[call_id]["executed"])
        self.assertEqual(
            [record["decision"] for record in report["callbacks"]], ["deny", "deny"]
        )

    async def test_fail_verdict_when_touch_bypasses_the_callback(self):
        BypassingClient.instances.clear()
        with patch.object(pp, "ProtocolClient", BypassingClient):
            report = await pp.run_probe(
                "native-noallowlist", self.workspace, self.out_dir
            )
        self.assertEqual(report["verdict"], "FAIL")
        self.assertEqual(
            [entry["tool"] for entry in report["bypass"]], ["Bash", "Write"]
        )
        self.assertTrue(all(entry["executed"] for entry in report["bypass"]))

    async def test_workspace_delta_alone_is_a_fail(self):
        ActuallyWritingClient.instances.clear()
        with patch.object(pp, "ProtocolClient", ActuallyWritingClient):
            report = await pp.run_probe("denylist", self.workspace, self.out_dir)
        self.assertEqual(report["verdict"], "FAIL")
        # Every write path was callback-routed, yet the file appeared.
        self.assertEqual(report["bypass"], [])
        self.assertEqual(report["workspace_delta"]["new"], ["PROBE-WRITE-MARKER.txt"])
        self.assertTrue(any("workspace changed" in note for note in report["notes"]))

    async def test_report_json_is_written_to_out_dir(self):
        FakeClient.instances.clear()
        with patch.object(pp, "ProtocolClient", FakeClient):
            report = await pp.run_probe(
                "native-noallowlist", self.workspace, self.out_dir
            )
        path = self.out_dir / "probe-native-noallowlist.json"
        self.assertTrue(path.is_file())
        saved = json.loads(path.read_text())
        self.assertEqual(saved["verdict"], report["verdict"])
        for key in (
            "mode",
            "verdict",
            "turn_outcome",
            "tool_calls",
            "callbacks",
            "bypass",
            "workspace_delta",
            "notes",
        ):
            self.assertIn(key, saved)

    async def test_workspace_receives_task_md_fixture(self):
        FakeClient.instances.clear()
        with patch.object(pp, "ProtocolClient", FakeClient):
            await pp.run_probe("native-noallowlist", self.workspace, self.out_dir)
        task = self.workspace / "TASK.md"
        self.assertTrue(task.is_file())
        self.assertIn("PROBE-WRITE-MARKER.txt", task.read_text())

    async def test_unknown_mode_raises_probe_error(self):
        with self.assertRaises(pp.ProbeError):
            await pp.run_probe("nope", self.workspace, self.out_dir)


class InventoryDeltaTests(ProbeFixture):
    def test_delta_reports_new_changed_and_removed(self):
        marker = self.workspace / "TASK.md"
        marker.write_text("v1")
        before = pp.inventory(self.workspace)
        marker.write_text("v2")
        (self.workspace / "new.txt").write_text("n")
        after = pp.inventory(self.workspace)
        self.assertEqual(
            pp.inventory_delta(before, after),
            {"new": ["new.txt"], "changed": ["TASK.md"], "removed": []},
        )
        self.assertEqual(
            pp.inventory_delta(before, before),
            {"new": [], "changed": [], "removed": []},
        )


class MainTests(ProbeFixture):
    def _main(self, argv):
        with redirect_stdout(io.StringIO()):
            return pp.main(argv)

    def _run_main(self, mode, client_cls):
        client_cls.instances.clear()
        with patch.object(pp, "ProtocolClient", client_cls):
            return self._main(
                [
                    "run",
                    "--mode",
                    mode,
                    "--workspace",
                    str(self.workspace),
                    "--out",
                    str(self.out_dir),
                ]
            )

    def test_exit_zero_only_on_pass_verdict(self):
        self.assertEqual(self._run_main("native-noallowlist", FakeClient), 0)
        self.assertEqual(self._run_main("native-noallowlist", BypassingClient), 1)
        self.assertEqual(self._run_main("hook", FakeClient), 1)

    def test_invalid_model_json_exits_two(self):
        code = self._main(
            [
                "run",
                "--mode",
                "native-noallowlist",
                "--workspace",
                str(self.workspace),
                "--out",
                str(self.out_dir),
                "--model",
                "{not-json",
            ]
        )
        self.assertEqual(code, 2)

    def test_probe_error_exits_two(self):
        with (
            patch.dict(os.environ, {pp.CLI_ENV: str(self.root / "gone.cjs")}),
            patch.object(pp, "ProtocolClient", FakeClient),
        ):
            code = self._main(
                [
                    "run",
                    "--mode",
                    "native-noallowlist",
                    "--workspace",
                    str(self.workspace),
                    "--out",
                    str(self.out_dir),
                ]
            )
        self.assertEqual(code, 2)


if __name__ == "__main__":
    unittest.main()
