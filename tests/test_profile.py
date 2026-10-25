"""Habit profile loading, injection and conflict tests; nothing touches a live CLI."""

import asyncio
import copy
import hashlib
import io
import json
import shlex
from contextlib import redirect_stdout
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import patch

from zcode_supervisor import profile, protocol, runtime


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


class ProfileFixture(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.source = self.root / "AGENTS.md"
        self.source.write_text("# user habits\nrun tests after changes\n", encoding="utf-8")
        self.executor = self.root / "receipt.py"
        self.executor.write_text("print('receipt')\n", encoding="utf-8")
        self.profile_path = self.root / "profile.json"
        self.rules = [{"id": "tests-after-change",
                       "text": "改动后必须运行适用的检查命令并如实报告结果。",
                       "sources": [{"path": str(self.source), "sha256": sha(self.source)}],
                       "applies_to": ["*.py"]}]
        self.tools = [{"id": "sympy-receipt", "description": "sympy 可执行性回执",
                       "kind": "executor",
                       "command": f"/usr/bin/python3 -I -B {self.executor}",
                       "executor": {"path": str(self.executor), "sha256": sha(self.executor)},
                       "applies_to": ["*.py"], "max_uses": 1, "max_timeout_ms": 60000,
                       "required": False, "unavailable_reason": None}]
        self.packet = {"workspace": str(self.root / "workspace"), "prompt": "Do work",
                       "allowed_files": []}

    def write_profile(self, rules=None, tools=None):
        value = {"schema_version": 1, "rules": self.rules if rules is None else rules,
                 "tools": self.tools if tools is None else tools}
        self.profile_path.write_text(json.dumps(value, ensure_ascii=False), encoding="utf-8")
        return value

    def loaded(self):
        return profile.load_profile(self.profile_path)


class ProfileLoadingTests(ProfileFixture):
    def test_valid_profile_loads(self):
        self.write_profile()
        self.assertEqual(self.loaded()["rules"][0]["id"], "tests-after-change")

    def test_schema_violations_fail_closed(self):
        self.write_profile()
        for mutate in (
            {"schema_version": 2}, {"extra": 1},
            lambda v: v.update(rules=[{"id": "x"}]),
            lambda v: v["rules"][0].update(applies_to=[]),
            lambda v: v["rules"][0].update(text="  "),
            lambda v: v["rules"][0]["sources"][0].update(sha256="0" * 64),
            lambda v: v["tools"][0].update(kind="mcp"),
            lambda v: v["tools"][0]["executor"].update(sha256="1" * 64),
            lambda v: v["tools"][0].update(command="python {workspace} {evil}.py"),
            lambda v: v["tools"][0].update(command='/usr/bin/python3 h.py "{workspace}"'),
            lambda v: v["tools"][0].update(command="/usr/bin/python3 h.py '{workspace}'"),
            lambda v: v["tools"][0].update(command="/usr/bin/python3 h.py --path={workspace}"),
            lambda v: v["tools"][0].update(command="/usr/bin/python3 h.py {workspace}/x"),
        ):
            with self.subTest(mutate=mutate):
                value = self.write_profile()
                if isinstance(mutate, dict):
                    value.update(mutate)
                else:
                    mutate(value)
                self.profile_path.write_text(json.dumps(value, ensure_ascii=False), encoding="utf-8")
                with self.assertRaises(ValueError):
                    profile.load_profile(self.profile_path)

    def test_missing_or_drifted_files_fail_closed(self):
        self.write_profile()
        drifted = copy.deepcopy(self.rules)
        self.source.write_text("# changed\n", encoding="utf-8")
        self.write_profile(rules=drifted)
        with self.assertRaisesRegex(ValueError, "source changed"):
            profile.load_profile(self.profile_path)
        self.source.unlink()
        with self.assertRaisesRegex(ValueError, "missing"):
            profile.load_profile(self.profile_path)

    def test_unavailable_tools_require_reason_and_never_execute(self):
        self.write_profile(tools=[{"id": "serena", "description": "symbolic navigation",
                                   "kind": "unavailable", "command": None, "executor": None,
                                   "applies_to": ["*"], "max_uses": 1, "max_timeout_ms": 1000,
                                   "required": False,
                                   "unavailable_reason": "MCP-only; sandbox keeps mcpServers=[]"}])
        entry = self.loaded()["tools"][0]
        self.assertEqual(entry["kind"], "unavailable")

    def test_resolution_prefers_explicit_and_discovers_default(self):
        self.write_profile()
        none = SimpleNamespace(profile=None, config_dir=self.root / "elsewhere")
        self.assertIsNone(profile.resolve_profile_path(none))
        discovered = SimpleNamespace(profile=None, config_dir=self.root)
        self.assertEqual(profile.resolve_profile_path(discovered), self.profile_path)
        explicit = SimpleNamespace(profile=self.root / "missing.json", config_dir=self.root)
        with self.assertRaisesRegex(ValueError, "missing"):
            profile.resolve_profile_path(explicit)
        self.assertEqual(profile.resolve_profile_path(
            SimpleNamespace(profile=self.profile_path, config_dir=None)), self.profile_path)


class ProfileApplicationTests(ProfileFixture):
    def apply(self, paths, packet=None, tools=None):
        self.write_profile(tools=self.tools if tools is None else tools)
        source = packet or copy.deepcopy(self.packet)
        return profile.apply_profile(self.loaded(), source, Path(source["workspace"]), paths)

    def test_rules_append_to_prompt_with_report_and_budget(self):
        extended, report = self.apply(["main.py"])
        self.assertIn("[habit:tests-after-change]", extended["prompt"])
        self.assertIn("Do work", extended["prompt"])
        self.assertTrue(report["rules"][0]["applies"])
        self.assertGreater(report["prompt_appended_chars"], 0)
        skipped, report2 = self.apply(["notes.txt"])
        self.assertFalse(report2["rules"][0]["applies"])
        self.assertEqual(skipped["prompt"], "Do work")

    def test_applicable_executor_is_injected_with_trusted_hash(self):
        extended, report = self.apply(["main.py"])
        entry = extended["commands"][0]
        self.assertEqual(entry["id"], "habit-sympy-receipt")
        self.assertFalse(entry["required"])
        self.assertEqual(extended["trusted_hashes"][str(self.executor)], sha(self.executor))
        self.assertEqual(report["tools"][0]["command_id"], "habit-sympy-receipt")
        self.assertIs(report["tools"][0]["executed"], False)
        # The exact command text is disclosed to the worker for verbatim copying.
        self.assertIn(entry["command"], extended["prompt"])
        self.assertIn("逐字复制", extended["prompt"])
        protocol.validate_packet(extended)

    def test_conflicts_with_packet_authority_fail_closed(self):
        command = self.tools[0]["command"]
        packet = {**copy.deepcopy(self.packet),
                  "commands": [{"id": "own", "command": command, "max_uses": 1,
                                "max_timeout_ms": 60000}]}
        with self.assertRaisesRegex(ValueError, "exact habit command"):
            self.apply(["main.py"], packet=packet)
        clash = {**copy.deepcopy(self.packet),
                 "commands": [{"id": "habit-sympy-receipt", "command": "other",
                               "max_uses": 1, "max_timeout_ms": 60000}]}
        with self.assertRaisesRegex(ValueError, "refusing to shadow"):
            self.apply(["main.py"], packet=clash)

    def test_trusted_hash_disagreement_fails_closed(self):
        packet = {**copy.deepcopy(self.packet),
                  "trusted_hashes": {str(self.executor): "2" * 64}}
        with self.assertRaisesRegex(ValueError, "disagree"):
            self.apply(["main.py"], packet=packet)

    def test_workspace_placeholder_is_substituted_only(self):
        self.tools[0]["command"] = f"/usr/bin/python3 -I -B {self.executor} {{workspace}}"
        extended, _ = self.apply(["main.py"])
        self.assertIn(str(self.packet["workspace"]), extended["commands"][0]["command"])
        protocol.validate_packet(extended)

    def test_workspace_placeholder_is_shell_quoted_against_metacharacters(self):
        self.tools[0]["command"] = f"/bin/cat {{workspace}}"
        hostile = "/tmp/ws with space;rm -rf /tmp/evil 'quoted'\"and\"double"
        packet = dict(self.packet, workspace=hostile)
        extended, _ = self.apply(["main.py"], packet=packet)
        rendered = extended["commands"][0]["command"]
        self.assertNotEqual(rendered, f"/bin/cat {hostile}")
        self.assertIn(shlex.quote(hostile), rendered)
        suffix = rendered[len("/bin/cat "):]
        self.assertEqual(shlex.split(suffix)[0], hostile)
        protocol.validate_packet(extended)

    def test_task_parameter_files_must_stay_immutable_for_injected_tools(self):
        # 缺陷3反例：allowed_files 含 .habit-params/ 下文件（worker 可写）而注入执行器
        # 会消费该命名空间 → 派发前必须拒绝，防止未经复审的代码经已批准精确命令执行。
        packet = dict(self.packet, allowed_files=[".habit-params/formula.py"],
                      commands=[])
        with self.assertRaisesRegex(ValueError, "must stay immutable"):
            self.apply(["main.py"], packet=packet)
        packet_ok = dict(self.packet, allowed_files=["out.txt"], commands=[])
        extended, _ = self.apply(["main.py"], packet=packet_ok)
        protocol.validate_packet(extended)
        # 无适用执行器时（该命名空间无人消费）不误伤普通任务
        extended_none, _ = self.apply(["notes.txt"])
        self.assertEqual(extended_none["prompt"], "Do work")

    def test_dispatch_guard_rejects_prequoted_placeholder_without_load(self):
        hand_built = {"rules": [], "tools": [
            dict(self.tools[0], command="/bin/sh -c 'echo {workspace}'")]}
        with self.assertRaisesRegex(ValueError, "standalone unquoted"):
            profile.apply_profile(hand_built, copy.deepcopy(self.packet),
                                  Path(self.packet["workspace"]), ["main.py"])


class ProfileExecuteWiringTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.control = self.root / "ctrl"
        self.control.mkdir()
        self.source = self.control / "AGENTS.md"
        self.source.write_text("# user habits\nrun tests after changes\n", encoding="utf-8")
        self.executor = self.control / "receipt.py"
        self.executor.write_text("print('receipt')\n", encoding="utf-8")
        self.profile_path = self.control / "profile.json"
        rules = [{"id": "tests-after-change",
                  "text": "改动后必须运行适用的检查命令并如实报告结果。",
                  "sources": [{"path": str(self.source), "sha256": sha(self.source)}],
                  "applies_to": ["*.py"]}]
        tools = [{"id": "sympy-receipt", "description": "sympy 可执行性回执",
                  "kind": "executor",
                  "command": f"/usr/bin/python3 -I -B {self.executor}",
                  "executor": {"path": str(self.executor), "sha256": sha(self.executor)},
                  "applies_to": ["*.py"], "max_uses": 1, "max_timeout_ms": 60000,
                  "required": False, "unavailable_reason": None}]
        self.profile_path.write_text(json.dumps(
            {"schema_version": 1, "rules": rules, "tools": tools}, ensure_ascii=False),
            encoding="utf-8")
        self.work_root = self.root / "work"
        self.workspace = self.work_root / "task" / "workspace"
        self.workspace.mkdir(parents=True)
        (self.workspace / "TASK.md").write_text("task\n", encoding="utf-8")
        (self.workspace / "probe.py").write_text("x = 1\n", encoding="utf-8")
        self.packet_path = self.control / "packet.json"
        self.packet_path.write_text(json.dumps(
            {"workspace": str(self.workspace), "prompt": "Run the provided receipt command once.",
             "allowed_files": [], "commands": []}), encoding="utf-8")
        self.output = self.work_root / "task" / "run-1.json"
        self.config = SimpleNamespace(work_root=self.work_root, state_dir=self.control / "state",
                                      cache_dir=self.control / "cache", notify=False,
                                      profile=self.profile_path)
        self.args = SimpleNamespace(packet=str(self.packet_path), out=str(self.output),
                                    resume=None, timeout=5)
        self.sent_prompt = None
        owner = self

        class FakeClient:
            def __init__(self, *args):
                self.events = []
                self.failure = None
                self.on_event = None
                self.process = SimpleNamespace(pid=987654, returncode=None)

            async def start(self):
                pass

            async def close(self):
                self.process.returncode = 0

            async def request(self, method, params):
                if method in ("session/create", "session/read"):
                    return {"session": {"sessionId": "sess_fixture", "status": "idle"},
                            "runtime": {"pendingRequestIds": []}, "settings": {}}
                if method == "session/subscribe":
                    return {"eventSeq": 0}
                if method == "session/send":
                    owner.sent_prompt = params["content"]
                    started = {"type": "turn.started", "sessionId": "sess_fixture",
                               "turnId": "turn_fixture", "seq": 1,
                               "payload": {"inputId": params["inputId"]}}
                    self.on_event(started)
                    self.events.append(started)
                    self.events.append({"type": "turn.completed", "sessionId": "sess_fixture",
                                        "turnId": "turn_fixture", "seq": 3,
                                        "payload": {"resultType": "success"}})
                return {}

        self.fake_client = FakeClient

    def capture_permission(self):
        owner = self

        class CapturingClient(self.fake_client):
            def __init__(self, *args):
                super().__init__(*args)
                owner.captured_permission = args[2]
        return CapturingClient

    async def run_execute(self):
        client_cls = self.capture_permission()
        with patch.object(protocol, "ProtocolClient", client_cls), \
                redirect_stdout(io.StringIO()):
            await protocol.execute(self.args, self.config)
        return json.loads(self.output.read_text())

    async def test_broker_authority_receives_injected_habit_commands(self):
        # 顺序回归守卫：Authority 必须拿到扩展后的 packet——注入命令可获批、
        # 执行器哈希进受信表；若先建 Authority 后扩展，此处直接失败。
        await self.run_execute()
        authority = self.captured_permission.__self__
        commands = {entry["id"]: entry for entry in authority.packet["commands"]}
        self.assertIn("habit-sympy-receipt", commands)
        self.assertEqual(authority.packet["trusted_hashes"][str(self.executor)],
                         sha(self.executor))
        injected = commands["habit-sympy-receipt"]["command"]
        decision = self.captured_permission({
            "sessionId": "sess_fixture", "turnId": authority.turn_id,
            "requestId": "req-broker-1", "toolCallId": "call-broker-1",
            "toolName": "Bash",
            "input": {"command": injected, "timeout": 30000,
                      "run_in_background": False, "dangerouslyDisableSandbox": False}})
        self.assertEqual(decision["decision"], "allow")
        denial = self.captured_permission({
            "sessionId": "sess_fixture", "turnId": authority.turn_id,
            "requestId": "req-broker-2", "toolCallId": "call-broker-2",
            "toolName": "Bash",
            "input": {"command": injected + " --tampered", "timeout": 30000}})
        self.assertEqual(denial["decision"], "deny")

    def test_profile_inside_work_root_is_refused(self):
        async def attempt():
            with patch.object(protocol, "ProtocolClient", self.fake_client), \
                    redirect_stdout(io.StringIO()):
                await protocol.execute(self.args, SimpleNamespace(
                    work_root=self.root.parent, state_dir=self.root / "state",
                    cache_dir=self.root / "cache", notify=False, profile=self.profile_path))
        with self.assertRaisesRegex(ValueError, "outside work_root"):
            asyncio.run(attempt())

    async def test_execute_appends_rules_and_injects_advisory_command(self):
        result = await self.run_execute()
        self.assertTrue(result["delivery_ok"])
        self.assertIn("[habit:tests-after-change]", self.sent_prompt)
        habits = result["habits"]
        self.assertEqual(habits["profile"]["sha256"], sha(self.profile_path))
        self.assertTrue(habits["rules"][0]["applies"])
        tool = habits["tools"][0]
        self.assertEqual(tool["command_id"], "habit-sympy-receipt")
        self.assertIs(tool["executed"], False)  # fake worker never ran it; advisory only

    async def test_planned_allowed_files_trigger_habits_on_new_files(self):
        (self.workspace / "probe.py").unlink()  # no .py exists yet in the inventory
        self.packet_path.write_text(json.dumps(
            {"workspace": str(self.workspace), "prompt": "Create new.py.",
             "allowed_files": ["new.py"], "commands": []}), encoding="utf-8")
        result = await self.run_execute()
        self.assertTrue(result["delivery_ok"])
        self.assertIn("[habit:tests-after-change]", self.sent_prompt)
        self.assertEqual(result["habits"]["applicability_paths"],
                         {"inventory": 1, "planned": ["new.py"]})
        self.assertTrue(result["habits"]["tools"][0]["applies"])


if __name__ == "__main__":
    unittest.main()
