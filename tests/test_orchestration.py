"""Contract tests for the reviewed native-orchestration packet extension."""
import json
import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from zcode_supervisor import orchestration as O  # noqa: E402
from zcode_supervisor.protocol import Authority  # noqa: E402


def packet(with_orchestration=None, **overrides):
    value = {
        "workspace": "/tmp/czs-o-test/workspace",
        "prompt": "Do the reviewed work.",
        "allowed_files": ["OUT.md"],
        "commands": [{"id": "run-check", "command": "python check.py", "max_uses": 1}],
    }
    if with_orchestration is not None:
        value["orchestration"] = with_orchestration
    value.update(overrides)
    return value


VALID_GOAL = {"goal": {"objective": "Deliver the pinned checklist; stop after the integrated report."}}
VALID_AGENTS = {"agents": {
    "profiles": [{"name": "audit-reader", "description": "Read-only lane worker",
                  "systemPrompt": "You are a read-only audit agent.", "tools": ["Read"],
                  "max_turns": 4}],
    "allowed_types": ["audit-reader"]}}
VALID_WORKFLOW = {"workflow": {
    "name": "eight-lane-audit", "script": "phase('lanes'); const r = await agent('l1').ask('x'); report(r);",
    "max_concurrency": 8}}
PATH_WORKFLOW = {"workflow": {
    "name": "pinned", "path": "lane.dwf.ts", "max_concurrency": 8,
    "subagent_model": "e3d876b0/GLM-5.3"}}
VALID_SKILLS = {"skills": {"allowed": ["superpowers:verification-before-completion"]}}


class SchemaTests(unittest.TestCase):
    def test_rejects_unknown_fields_and_non_dict(self):
        for bad in (None, [], "goal", {"unknown": 1}, {}):
            with self.assertRaises(ValueError):
                O.validate_orchestration(bad)

    def test_goal_rules(self):
        self.assertEqual(O.validate_orchestration(VALID_GOAL)["goal"]["objective"],
                         VALID_GOAL["goal"]["objective"])
        for bad in ({"goal": {}}, {"goal": {"objective": "  "}},
                    {"goal": {"objective": "x", "budget": 5}}):
            with self.assertRaises(ValueError):
                O.validate_orchestration(bad)

    def test_agent_rules(self):
        value = O.validate_orchestration(VALID_AGENTS)
        self.assertEqual(value["agents"]["allowed_types"], VALID_AGENTS["agents"]["allowed_types"])
        bad_blocks = [
            {"agents": {"profiles": [], "allowed_types": ["general-purpose"]}},
            {**VALID_AGENTS, "agents": {**VALID_AGENTS["agents"],
                                        "allowed_types": ["Explore"]}},  # yolo substitute
            {**VALID_AGENTS, "agents": {**VALID_AGENTS["agents"],
                                        "allowed_types": ["undeclared-type"]}},
            {**VALID_AGENTS, "agents": {**VALID_AGENTS["agents"],
                                        "profiles": [{**VALID_AGENTS["agents"]["profiles"][0],
                                                      "tools": []}]}},
            {**VALID_AGENTS, "agents": {**VALID_AGENTS["agents"],
                                        "profiles": [{**VALID_AGENTS["agents"]["profiles"][0],
                                                      "max_turns": True}]}}]
        for bad in bad_blocks:
            with self.assertRaises(ValueError):
                O.validate_orchestration(bad)

    def test_workflow_rules(self):
        value = O.validate_orchestration(VALID_WORKFLOW)
        self.assertEqual(value["workflow"]["max_concurrency"], 8)
        bad_blocks = [
            {"workflow": {"script": "x", "max_concurrency": 0}},
            {"workflow": {"script": "x", "max_concurrency": 65}},
            {"workflow": {"script": "x", "max_concurrency": True}},
            {"workflow": {"max_concurrency": 8}},
            {"workflow": {"script": " ", "max_concurrency": 8}},
            {"workflow": {"script": "x", "max_concurrency": 8, "saved": {"name": "y"}}},
        ]
        for bad in bad_blocks:
            with self.assertRaises(ValueError):
                O.validate_orchestration(bad)

    def test_options_builder(self):
        base = {"mcpServers": [], "toolAllowlist": ["Read", "Write", "Edit", "Bash"],
                "dynamicWorkflowEnabled": False}
        self.assertEqual(O.session_options(base, None), base)
        goal_only = O.session_options(base, O.validate_orchestration(VALID_GOAL))
        self.assertEqual(set(goal_only["toolAllowlist"]), set(base["toolAllowlist"]))
        self.assertFalse(goal_only["dynamicWorkflowEnabled"])
        agents = O.session_options(base, O.validate_orchestration(VALID_AGENTS))
        self.assertIn("Agent", agents["toolAllowlist"])
        self.assertNotIn("CreateWorkflow", agents["toolAllowlist"])
        full = O.session_options(base, O.validate_orchestration(
            {**VALID_GOAL, **VALID_AGENTS, **VALID_WORKFLOW, **VALID_SKILLS}))
        self.assertEqual(full["dynamicWorkflowEnabled"], True)
        for tool in ("Agent", "TaskOutput", "TaskStop", "CreateWorkflow", "GetWorkflowRun",
                     "ListWorkflowRuns", "Skill"):
            self.assertIn(tool, full["toolAllowlist"])
        self.assertIn("Read", full["toolAllowlist"])

    def test_runtime_config_answer(self):
        self.assertIsNone(O.runtime_config_answer(None))
        self.assertIsNone(O.runtime_config_answer(O.validate_orchestration(VALID_GOAL)))
        answer = O.runtime_config_answer(O.validate_orchestration(VALID_AGENTS))
        self.assertEqual(answer["kind"], "ready")
        profile = answer["profiles"][0]
        self.assertEqual(profile["source"], "user")
        self.assertEqual(profile["permissionMode"], "default")
        self.assertEqual(profile["tools"], ["Read"])
        self.assertEqual(answer["builtInModelSelectionOverrides"], {})
        self.assertEqual(answer["pluginAgentModelSelectionOverrides"], {})


class OrchestrationAuthorityTests(unittest.TestCase):
    """Child-origin permission decisions on top of the accepted Authority."""

    def make(self, orch=None, tmp=None):
        import tempfile
        tmp = tmp or tempfile.TemporaryDirectory()
        workspace = Path(tmp.name) / "workspace"
        workspace.mkdir()
        packet_path = Path(tmp.name) / "packet.json"
        full = packet(O.validate_orchestration(orch) if orch else None)
        full["workspace"] = str(workspace)
        packet_path.write_text(__import__("json").dumps(full))
        self.addCleanup(tmp.cleanup)
        authority = Authority(full, packet_path, {"OUT.md": "hash0"})
        authority.session_id = "sess_parent"
        authority.turn_id = "turn_main"
        if authority.orchestration:
            authority.note_turn("turn_main")
        return authority

    _seq = 0

    def child_request(self, child="sess_child_a", tool="Bash", data=None, **origin_extra):
        OrchestrationAuthorityTests._seq += 1
        seq = OrchestrationAuthorityTests._seq
        return {
            "requestId": f"r-{seq}-{child}-{tool}", "sessionId": "sess_parent",
            "turnId": "turn_child", "toolCallId": f"tc-{seq}-{child}-{tool}",
            "toolName": tool, "input": data or {"command": "python check.py"},
            "origin": {"kind": "subagent", "agentId": "agent_a", "agentType": "audit-reader",
                       "childSessionId": child, "childTurnId": "turn_child",
                       "parentToolCallId": "tc_dispatch_a", "parentTurnId": "turn_main",
                       **origin_extra},
        }

    def test_registered_child_exact_command_is_allowed_with_origin_recorded(self):
        authority = self.make({**VALID_AGENTS})
        authority.note_event({"type": "subagent_spawned", "payload": {
            "agentId": "agent_a", "childSessionId": "sess_child_a",
            "parentToolCallId": "tc_dispatch_a", "subagentType": "audit-reader", "background": False}})
        response = authority.decide(self.child_request())
        self.assertEqual(response["decision"], "allow")
        decision = authority.decisions[-1]
        self.assertEqual(decision["origin"]["childSessionId"], "sess_child_a")
        self.assertEqual(authority.children["sess_child_a"]["subagentType"], "audit-reader")

    def test_unknown_child_session_and_disallowed_type_are_denied(self):
        authority = self.make({**VALID_AGENTS})
        authority.note_event({"type": "subagent_spawned", "payload": {
            "agentId": "agent_b", "childSessionId": "sess_child_b",
            "parentToolCallId": "tc_dispatch_b", "subagentType": "Explore", "background": False}})
        self.assertEqual(authority.decide(self.child_request())["decision"], "deny")
        forged = self.child_request(child="sess_not_spawned")
        self.assertEqual(authority.decide(forged)["decision"], "deny")

    def test_child_accepted_via_authoritative_poll_only(self):
        authority = self.make({**VALID_AGENTS})
        authority.note_turn("turn_main")
        authority.note_children_poll(
            [{"childSessionId": "sess_child_a", "agentId": "agent_a",
              "toolCallId": "tc_dispatch_a", "subagentType": "audit-reader",
              "status": "running"}], [])
        request = self.child_request()  # parentToolCallId tc_dispatch_a
        self.assertEqual(authority.decide(request)["decision"], "allow")
        # A dispatch id this parent never scheduled is rejected.
        stranger = self.child_request(child="sess_stranger",
                                      origin_extra={"parentToolCallId": "tc_never"})
        self.assertEqual(authority.decide(stranger)["decision"], "deny")

    def test_dispatch_observation_alone_cannot_register_child(self):
        # Reviewer counterexample: quoting a scheduled parent dispatch id
        # must not let an invented child/session/turn self-register.
        authority = self.make({**VALID_AGENTS})
        authority.note_turn("turn_main")
        authority.note_event({"type": "tool.updated", "payload": {
            "kind": "scheduled", "toolCallId": "tc_dispatch_a", "toolName": "Agent",
            "input": {"description": "d", "prompt": "p"}}})
        self.assertEqual(authority.decide(self.child_request())["decision"], "deny")
        self.assertEqual(authority.children, {})

    def test_unapproved_run_id_quote_cannot_register_actor(self):
        # Reviewer counterexample: an ordinary tool result quoting a dwfrun id
        # must not create a workflow launch credential.
        authority = self.make({**VALID_AGENTS, **VALID_WORKFLOW})
        authority.note_turn("turn_main")
        authority.note_event({"type": "tool.updated", "payload": {
            "kind": "result", "toolCallId": "ordinary-read",
            "result": {"success": True,
                       "content": "read something mentioning dwfrun-00000000-0000-0000-0000-000000000001"}}})
        forged = self.child_request(child="sess_dwf-dwfrun-00000000-0000-0000-0000-000000000001-actor_9_9",
                                    origin_extra={"agentType": "zcode-workflow",
                                                  "parentToolCallId": None})
        forged["origin"]["agentId"] = "sess_dwf-dwfrun-00000000-0000-0000-0000-000000000001-actor_9_9"
        self.assertEqual(authority.decide(forged)["decision"], "deny")
        self.assertEqual(authority.children, {})
        explore = self.child_request(child="sess_child_b",
                                     origin_extra={"agentType": "Explore"})
        self.assertEqual(authority.decide(explore)["decision"], "deny")

    def test_child_write_outside_allowed_files_denied_inside_allowed(self):
        authority = self.make({**VALID_AGENTS})
        authority.note_event({"type": "subagent_spawned", "payload": {
            "agentId": "agent_a", "childSessionId": "sess_child_a",
            "parentToolCallId": "tc_dispatch_a", "subagentType": "audit-reader", "background": False}})
        outside = authority.decide(self.child_request(
            tool="Write", data={"file_path": "evidence/x", "content": "no"}))
        self.assertEqual(outside["decision"], "deny")
        inside = authority.decide(self.child_request(
            tool="Write", data={"file_path": "OUT.md", "content": "lane result"}))
        self.assertEqual(inside["decision"], "allow")

    def test_multi_turn_only_with_orchestration(self):
        plain = self.make(None)
        request = {"requestId": "r2", "sessionId": "sess_parent", "turnId": "turn_goal",
                   "toolCallId": "tc2", "toolName": "Bash",
                   "input": {"command": "python check.py"}}
        self.assertEqual(plain.decide(request)["decision"], "deny")
        authority = self.make({**VALID_GOAL})
        authority.note_turn("turn_goal")
        self.assertEqual(authority.decide(request)["decision"], "allow")
        unregistered = dict(request, turnId="turn_unknown", requestId="r3")
        self.assertEqual(authority.decide(unregistered)["decision"], "deny")

    def test_create_workflow_request_pinned_bytes(self):
        authority = self.make({**VALID_WORKFLOW})
        authority.note_turn("turn_main")
        good = {"requestId": "rw1", "sessionId": "sess_parent", "turnId": "turn_main",
                "toolCallId": "tcw1", "toolName": "CreateWorkflow",
                "input": {"script": VALID_WORKFLOW["workflow"]["script"], "max_concurrency": 8}}
        self.assertEqual(authority.decide(good)["decision"], "allow")
        bad_script = dict(good, requestId="rw2", toolCallId="tcw2",
                          input={"script": "phase('evil'); await agent('x').ask('steal')",
                                 "max_concurrency": 8})
        self.assertEqual(authority.decide(bad_script)["decision"], "deny")
        bad_concurrency = dict(good, requestId="rw3", toolCallId="tcw3",
                               input={"script": VALID_WORKFLOW["workflow"]["script"],
                                      "max_concurrency": 16})
        self.assertEqual(authority.decide(bad_concurrency)["decision"], "deny")
        bad_fields = dict(good, requestId="rw4", toolCallId="tcw4",
                          input={"script": VALID_WORKFLOW["workflow"]["script"], "path": "x"})
        self.assertEqual(authority.decide(bad_fields)["decision"], "deny")

    def test_subagent_stopped_updates_registry_status(self):
        authority = self.make({**VALID_AGENTS})
        authority.note_event({"type": "subagent_spawned", "payload": {
            "agentId": "agent_a", "childSessionId": "sess_child_a",
            "parentToolCallId": "tc_dispatch_a", "subagentType": "audit-reader"}})
        authority.note_event({"type": "subagent_stopped", "payload": {
            "agentId": "agent_a", "childSessionId": "sess_child_a", "status": "failed"}})
        self.assertEqual(authority.children["sess_child_a"]["status"], "failed")


if __name__ == "__main__":  # pragma: no cover
    unittest.main()


class ReverseHandlerTests(unittest.TestCase):
    """subagents/readRuntimeConfig answers come from the packet, fail-closed."""

    def make_client(self, orch):
        import io
        from zcode_supervisor.protocol import ProtocolClient
        client = object.__new__(ProtocolClient)
        client.orchestration = orch
        client.permission = lambda params: {"decision": "deny", "reason": "test"}
        client.wire = io.StringIO()
        client.sent = []
        async def fake_send(frame):
            client.sent.append(frame)
        client.send = fake_send
        return client

    def test_runtime_config_request_answered_from_packet(self):
        import asyncio, json
        from zcode_supervisor import orchestration as O
        orch = O.validate_orchestration(VALID_AGENTS)
        client = self.make_client(orch)
        frame = {"id": "req-1", "method": "subagents/readRuntimeConfig", "params": {"sessionId": "s"}}
        asyncio.run(client.reverse(frame))
        reply = client.sent[-1]
        self.assertEqual(reply["id"], "req-1")
        answer = reply["result"]
        self.assertEqual(answer["kind"], "ready")
        self.assertEqual(answer["profiles"][0]["permissionMode"], "default")
        logged = [json.loads(line) for line in client.wire.getvalue().splitlines()]
        self.assertTrue(any(entry.get("method") == "subagents/readRuntimeConfig" for entry in logged))

    def test_runtime_config_request_refused_without_agents(self):
        import asyncio
        from zcode_supervisor import orchestration as O
        client = self.make_client(O.validate_orchestration(VALID_GOAL))
        frame = {"id": "req-2", "method": "subagents/readRuntimeConfig", "params": {"sessionId": "s"}}
        asyncio.run(client.reverse(frame))
        reply = client.sent[-1]
        self.assertIn("error", reply)
        self.assertEqual(reply["error"]["code"], -32601)

    def test_preferences_enable_subagent_config_only_with_agents(self):
        import asyncio
        from zcode_supervisor import orchestration as O
        for orch, expected in ((None, False),
                               (O.validate_orchestration(VALID_GOAL), False),
                               (O.validate_orchestration(VALID_AGENTS), True)):
            client = self.make_client(orch)
            asyncio.run(client.reverse({"id": "req-3", "method": "session/requestRuntimePreferences",
                                        "params": {"sessionId": "s"}}))
            self.assertEqual(client.sent[-1]["result"]["subagentRuntimeConfigEnabled"], expected)


class OrchestrationEvidenceAdmissionTests(unittest.TestCase):
    """Child/multi-turn events are admitted only through the registry."""

    def authority_with_child(self):
        import json as jsonlib
        import tempfile
        authority = OrchestrationAuthorityTests().make({**VALID_AGENTS, **VALID_WORKFLOW})
        authority.note_turn("turn_main")
        authority.note_event({"type": "subagent_spawned", "payload": {
            "agentId": "agent_a", "childSessionId": "sess_child_a",
            "parentToolCallId": "tc", "subagentType": "audit-reader"}})
        return authority

    def test_child_and_multiturn_events_admitted_strangers_denied(self):
        from zcode_supervisor.protocol import authorized_event
        authority = self.authority_with_child()
        self.assertTrue(authorized_event(
            {"sessionId": "sess_child_a", "turnId": "turn_c", "type": "tool.updated"}, authority))
        self.assertTrue(authorized_event(
            {"sessionId": authority.session_id, "turnId": "turn_main", "type": "tool.updated"}, authority))
        self.assertFalse(authorized_event(
            {"sessionId": "sess_stranger", "turnId": "t", "type": "tool.updated"}, authority))
        self.assertFalse(authorized_event(
            {"sessionId": authority.session_id, "turnId": "turn_unregistered", "type": "tool.updated"}, authority))

    def test_orchestration_tools_receipted_without_generic_success_field(self):
        from zcode_supervisor import protocol as P
        authority = self.authority_with_child()
        events = [
            {"sessionId": authority.session_id, "turnId": "turn_main", "seq": 1, "type": "tool.updated",
             "payload": {"kind": "scheduled", "toolCallId": "ag1", "toolName": "Agent",
                         "input": {"prompt": "lane", "description": "d"}}},
            {"sessionId": authority.session_id, "turnId": "turn_main", "seq": 2, "type": "tool.updated",
             "payload": {"kind": "result", "toolCallId": "ag1", "result": {"status": "async_launched"}}},
        ]
        commands, problems = P.execution_evidence(events, authority)
        receipts = [c for c in commands if c.get("kind") == "orchestration-tool"]
        self.assertEqual(len(receipts), 1)
        self.assertEqual(receipts[0]["tool"], "Agent")
        self.assertEqual(problems, [])

    def test_unknown_tool_still_rejected_in_evidence(self):
        from zcode_supervisor import protocol as P
        authority = self.authority_with_child()
        events = [
            {"sessionId": authority.session_id, "turnId": "turn_main", "seq": 1, "type": "tool.updated",
             "payload": {"kind": "scheduled", "toolCallId": "x1", "toolName": "WebSearch",
                         "input": {}}},
            {"sessionId": authority.session_id, "turnId": "turn_main", "seq": 2, "type": "tool.updated",
             "payload": {"kind": "result", "toolCallId": "x1", "result": {"success": True}}},
        ]
        _, problems = P.execution_evidence(events, authority)
        self.assertTrue(any("outside the exposed tool allowlist" in p for p in problems))


class PathModeWorkflowTests(unittest.TestCase):
    """Reviewer finding 4/5 coverage: the path-mode launch branch."""

    def make(self, orch, script_text):
        import json as jsonlib, tempfile
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        ws = Path(tmp.name) / "workspace"
        ws.mkdir()
        (ws / "lane.dwf.ts").write_text(script_text)
        pp = Path(tmp.name) / "packet.json"
        full = packet(orch)
        full["workspace"] = str(ws)
        pp.write_text(jsonlib.dumps(full))
        from zcode_supervisor.protocol import inventory
        authority = Authority(full, pp, inventory(ws))
        authority.session_id = "sess_parent"
        authority.turn_id = "turn_main"
        authority.note_turn("turn_main")
        return authority, ws

    def request(self, n, **data):
        return {"requestId": f"pw{n}", "sessionId": "sess_parent", "turnId": "turn_main",
                "toolCallId": f"tcw{n}", "toolName": "CreateWorkflow", "input": data}

    def test_path_mode_accepts_exact_file_and_rejects_drift(self):
        from zcode_supervisor import orchestration as O
        orch = O.validate_orchestration(PATH_WORKFLOW)
        script = "phase('x'); return 1;"
        authority, ws = self.make(orch, script)
        good = self.request(1, name="pinned", max_concurrency=8, path="lane.dwf.ts",
                            subagent_model="e3d876b0/GLM-5.3", script=script,
                            adjustable_settings={})
        self.assertEqual(authority.decide(good)["decision"], "allow")
        tampered = self.request(2, name="pinned", max_concurrency=8, path="lane.dwf.ts",
                                subagent_model="e3d876b0/GLM-5.3",
                                script="phase('evil'); return 2;")
        self.assertEqual(authority.decide(tampered)["decision"], "deny")
        wrong_path = self.request(3, name="pinned", max_concurrency=8, path="other.dwf.ts")
        self.assertEqual(authority.decide(wrong_path)["decision"], "deny")
        with_args = self.request(4, name="pinned", max_concurrency=8, path="lane.dwf.ts",
                                 args={"x": 1})
        self.assertEqual(authority.decide(with_args)["decision"], "deny")
        missing_model = self.request(5, name="pinned", max_concurrency=8, path="lane.dwf.ts")
        self.assertEqual(authority.decide(missing_model)["decision"], "deny")

    def test_path_mode_absolute_path_accepted(self):
        from zcode_supervisor import orchestration as O
        orch = O.validate_orchestration(PATH_WORKFLOW)
        authority, ws = self.make(orch, "phase('x'); return 1;")
        absolute = self.request(6, name="pinned", max_concurrency=8,
                                path=str(ws / "lane.dwf.ts"),
                                subagent_model="e3d876b0/GLM-5.3")
        self.assertEqual(authority.decide(absolute)["decision"], "allow")


class BaselineAndChildGuardTests(unittest.TestCase):
    def test_baseline_failed_read_is_a_problem_without_orchestration(self):
        import json as jsonlib, tempfile
        from zcode_supervisor import protocol as P
        tmp = tempfile.TemporaryDirectory(); self.addCleanup(tmp.cleanup)
        ws = Path(tmp.name) / "workspace"; ws.mkdir()
        pp = Path(tmp.name) / "packet.json"
        full = packet(None); full["workspace"] = str(ws)
        pp.write_text(jsonlib.dumps(full))
        authority = Authority(full, pp, {})
        authority.session_id = "sess_p"; authority.turn_id = "turn_t"
        events = [
            {"sessionId": "sess_p", "turnId": "turn_t", "seq": 1, "type": "model.streaming",
             "payload": {"kind": "tool_call", "toolCallId": "r1", "toolName": "Read",
                         "input": {"file_path": "x"}}},
            {"sessionId": "sess_p", "turnId": "turn_t", "seq": 2, "type": "tool.updated",
             "payload": {"kind": "scheduled", "toolCallId": "r1", "toolName": "Read",
                         "input": {"file_path": "x"}}},
            {"sessionId": "sess_p", "turnId": "turn_t", "seq": 3, "type": "tool.updated",
             "payload": {"kind": "result", "toolCallId": "r1", "toolName": "Read",
                         "result": {"success": False}}},
        ]
        _, problems = P.execution_evidence(events, authority)
        self.assertTrue(any("reports failure or is malformed" in p for p in problems))

    def test_child_request_with_wrong_parent_session_denied_and_stopped_child_denied(self):
        authority = OrchestrationAuthorityTests().make({**VALID_AGENTS})
        authority.note_event({"type": "subagent_spawned", "payload": {
            "agentId": "a", "childSessionId": "sess_child_a", "parentToolCallId": "tc",
            "subagentType": "audit-reader"}})
        wrong_session = {"requestId": "cx1", "sessionId": "sess_other", "turnId": "t",
                         "toolCallId": "t1", "toolName": "Bash",
                         "input": {"command": "python check.py"},
                         "origin": {"kind": "subagent", "agentId": "a", "agentType": "audit-reader",
                                    "childSessionId": "sess_child_a", "parentToolCallId": "tc"}}
        self.assertEqual(authority.decide(wrong_session)["decision"], "deny")
        authority.note_event({"type": "subagent_stopped", "payload": {
            "agentId": "a", "childSessionId": "sess_child_a", "status": "failed"}})
        stopped = dict(wrong_session, sessionId="sess_parent", requestId="cx2", toolCallId="t2")
        self.assertEqual(authority.decide(stopped)["decision"], "deny")


class WorkflowActorAssociationTests(unittest.TestCase):
    """Actors associate through the observed launch run id, nothing else."""

    def make(self):
        import json as jsonlib, tempfile
        from zcode_supervisor.protocol import inventory
        tmp = tempfile.TemporaryDirectory(); self.addCleanup(tmp.cleanup)
        ws = Path(tmp.name) / "workspace"; ws.mkdir()
        (ws / "lane.dwf.ts").write_text("phase('x'); return 1;")
        pp = Path(tmp.name) / "packet.json"
        full = packet({**VALID_WORKFLOW})
        full["workspace"] = str(ws.resolve())
        full["allowed_files"] = ["OUT.md"]
        pp.write_text(jsonlib.dumps(full))
        authority = Authority(full, pp, inventory(ws))
        authority.session_id = "sess_parent"; authority.turn_id = "turn_main"
        authority.note_turn("turn_main")
        authority._approved_workflow_tids.add("tcw_launch")
        authority.note_event({"type": "tool.updated", "payload": {
            "kind": "result", "toolCallId": "tcw_launch",
            "result": {"success": True,
                       "content": "run started with ID: dwfrun-11111111-2222-3333-4444-555555555555"}}})
        authority.note_children_poll(
            [{"childSessionId": "sess_dwf-dwfrun-11111111-2222-3333-4444-555555555555-actor_1_1",
              "agentId": "sess_dwf-dwfrun-11111111-2222-3333-4444-555555555555-actor_1_1",
              "toolCallId": "tcw_launch", "subagentType": "zcode-workflow",
              "status": "running"}], [])
        # the live shape: result events carry NO toolName, only the toolCallId
        authority.note_event({"type": "tool.updated", "payload": {
            "kind": "result", "toolCallId": "tcw_launch",
            "result": {"success": True, "content":
                       "The workflow script compiled cleanly and the run started in the "
                       "background with ID: dwfrun-11111111-2222-3333-4444-555555555555"}}})
        assert "dwfrun-11111111-2222-3333-4444-555555555555" in authority.workflow_launches.values()
        return authority

    def actor_request(self, n, tool="Write", data=None):
        child = f"sess_dwf-dwfrun-11111111-2222-3333-4444-555555555555-actor_{n}_1"
        return {"requestId": f"ar{n}{tool}", "sessionId": "sess_parent", "turnId": "t",
                "toolCallId": f"atc{n}{tool}", "toolName": tool,
                "input": data or {"file_path": "OUT.md", "content": "lane"},
                "origin": {"kind": "subagent", "agentId": child, "agentType": "zcode-workflow",
                           "childSessionId": child, "childTurnId": f"turn_actor_{n}",
                           "parentSessionId": "sess_parent"}}

    def test_actor_write_inside_allowed_and_bash_denied(self):
        authority = self.make()
        self.assertEqual(authority.decide(self.actor_request(1))["decision"], "allow")
        bash = authority.decide(self.actor_request(2, tool="Bash",
                                                   data={"command": "echo x"}))
        self.assertEqual(bash["decision"], "deny")

    def test_actor_of_unknown_run_and_forged_suffix_denied(self):
        authority = self.make()
        stranger = self.actor_request(3)
        stranger["origin"]["childSessionId"] = "sess_dwf-dwfrun-99999999-2222-3333-4444-555555555555-actor_9_1"
        stranger["origin"]["agentId"] = stranger["origin"]["childSessionId"]
        self.assertEqual(authority.decide(stranger)["decision"], "deny")
        forged = self.actor_request(4)
        forged["origin"]["agentType"] = "general-purpose"  # right session id, wrong type
        self.assertEqual(authority.decide(forged)["decision"], "deny")


class SkillPinningAndNeutralizationTests(unittest.TestCase):
    """Guidance §1/§2: pinned per-child skills and built-in neutralization."""

    def test_pinned_skills_and_skill_tool_rules(self):
        from zcode_supervisor import orchestration as O
        base = {"agents": {"profiles": [
            {"name": "lane-x", "description": "d", "systemPrompt": "s",
             "tools": ["Read", "Skill"], "max_turns": 4,
             "skills": ["superpowers:systematic-debugging"]}],
            "allowed_types": ["lane-x"]}}
        value = O.validate_orchestration(base)
        answer = O.runtime_config_answer(value)
        self.assertEqual(answer["profiles"][0]["skills"], ["superpowers:systematic-debugging"])
        for bad in (
            {**base, "agents": {**base["agents"], "profiles": [
                {**base["agents"]["profiles"][0], "skills": []}]}},          # [] == unlimited
            {**base, "agents": {**base["agents"], "profiles": [
                {**base["agents"]["profiles"][0], "skills": ["a", "a"]}]}},  # duplicates
            {**base, "agents": {**base["agents"], "profiles": [
                {**base["agents"]["profiles"][0], "tools": ["Read", "Skill"],
                 "skills": None}] if False else {**base["agents"]["profiles"][0]} | {"skills": None}} if False else [
                {k: v for k, v in {**base["agents"]["profiles"][0], "tools": ["Read", "Skill"]}.items()
                 if k != "skills"}]},                                         # Skill without skills
            {**base, "agents": {**base["agents"], "profiles": [
                {**base["agents"]["profiles"][0], "tools": ["Read", "Bash"]}]}},  # Bash still unsafe
        ):
            with self.assertRaises(ValueError):
                O.validate_orchestration(bad)

    def test_neutralize_builtins_shadows_explore_read_only(self):
        from zcode_supervisor import orchestration as O
        base = {"agents": {"profiles": [
            {"name": "lane-x", "description": "d", "systemPrompt": "s",
             "tools": ["Read"], "max_turns": 4}],
            "allowed_types": ["lane-x"]}}
        value = O.validate_orchestration(base)
        profiles = O.agent_profiles(value)
        explore = [p for p in profiles if p["name"] == "Explore"]
        self.assertEqual(len(explore), 1)
        self.assertEqual(explore[0]["tools"], ["Read"])
        self.assertEqual(explore[0]["permissionMode"], "default")
        self.assertEqual(explore[0]["source"], "user")
        # an explicit same-name profile is not duplicated
        custom = {"agents": {"profiles": [
            {"name": "lane-x", "description": "d", "systemPrompt": "s", "tools": ["Read"], "max_turns": 4},
            {"name": "Explore", "description": "own shadow", "systemPrompt": "s2",
             "tools": ["Read"], "max_turns": 2}],
            "allowed_types": ["lane-x", "Explore"]}}
        profiles2 = O.agent_profiles(O.validate_orchestration(custom))
        self.assertEqual(len([p for p in profiles2 if p["name"] == "Explore"]), 1)
        self.assertEqual([p for p in profiles2 if p["name"] == "Explore"][0]["maxTurns"], 2)

    def test_expected_children_validated(self):
        from zcode_supervisor import orchestration as O
        base = {"agents": {"profiles": [
            {"name": "lane-x", "description": "d", "systemPrompt": "s",
             "tools": ["Read"], "max_turns": 4}],
            "allowed_types": ["lane-x"], "expected_children": 8}}
        self.assertEqual(O.validate_orchestration(base)["agents"]["expected_children"], 8)
        for bad in (0, -1, True, "8", 2.5):
            with self.assertRaises(ValueError):
                O.validate_orchestration({**base, "agents": {**base["agents"],
                                                             "expected_children": bad}})


class SettleSchemaReplayTests(unittest.TestCase):
    """Replay the frozen lanes3 real RPCs through the settle logic."""

    RAW = Path("/Users/honghaoxiong/underworld2-2.17.3/out/czs-native-orchestration-20261014/"
               "reviewer-20261016-candidate-1/raw/orch-lanes3/run-lanes3.json")

    def test_goal_final_and_background_jobs_read_from_real_projection(self):
        from zcode_supervisor.protocol import goal_state_from_snapshot, workflow_jobs_from_snapshot
        if not self.RAW.exists():
            self.skipTest("frozen reviewer raw snapshot not present on this machine")
        raw = json.loads(self.RAW.read_bytes())
        goal_final = raw["orchestration"]["goal_final"]
        snapshot = goal_final.get("snapshot") or {}
        target = goal_state_from_snapshot(snapshot)
        self.assertIsNotNone(target, "projection.target missing")
        self.assertEqual(target.get("status"), "active")
        jobs = workflow_jobs_from_snapshot(snapshot)
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0].get("taskId"), "dwfrun-790dfc59-87e3-4a6b-81b6-46ac34571078")
        self.assertEqual(jobs[0].get("status"), "running")
        # the settle rules must reject both facts
        problems = []
        if target.get("status") not in ("verified", "complete"):
            problems.append(f"goal is not verified: {target.get('status')!r}")
        for job in jobs:
            if job.get("status") in ("running", "pending"):
                problems.append(f"still running: {job.get('taskId')}")
        self.assertEqual(len(problems), 2)

    def test_wrong_schemas_are_not_silently_accepted(self):
        from zcode_supervisor.protocol import goal_state_from_snapshot, workflow_jobs_from_snapshot
        self.assertIsNone(goal_state_from_snapshot({}))
        self.assertIsNone(goal_state_from_snapshot({"projection": {}}))
        self.assertIsNone(goal_state_from_snapshot({"session": {"workflowRuns": {"runs": []}}}))
        self.assertEqual(workflow_jobs_from_snapshot(
            {"session": {"workflowRuns": {"runs": [{"runId": "x"}]}}}), [])


class ForgedActorUnderApprovedLaunchTests(unittest.TestCase):
    def test_forged_actor_under_real_launch_denied(self):
        import json as jsonlib, tempfile
        from zcode_supervisor.protocol import inventory
        tmp = tempfile.TemporaryDirectory(); self.addCleanup(tmp.cleanup)
        ws = Path(tmp.name) / "workspace"; ws.mkdir()
        (ws / "lane.dwf.ts").write_text("phase('x'); return 1;")
        pp = Path(tmp.name) / "packet.json"
        full = packet({**VALID_WORKFLOW}); full["workspace"] = str(ws.resolve())
        full["allowed_files"] = ["OUT.md"]
        pp.write_text(jsonlib.dumps(full))
        authority = Authority(full, pp, inventory(ws))
        authority.session_id = "sess_parent"; authority.turn_id = "turn_main"
        authority.note_turn("turn_main")
        authority._approved_workflow_tids.add("tcw_launch")
        authority.note_event({"type": "tool.updated", "payload": {
            "kind": "result", "toolCallId": "tcw_launch",
            "result": {"success": True,
                       "content": "run started with ID: dwfrun-11111111-2222-3333-4444-555555555555"}}})
        forged = {"requestId": "fa1", "sessionId": "sess_parent", "turnId": "t",
                  "toolCallId": "fatc1", "toolName": "Write",
                  "input": {"file_path": str(ws / "OUT.md"), "content": "x"},
                  "origin": {"kind": "subagent",
                             "childSessionId": "sess_dwf-dwfrun-11111111-2222-3333-4444-555555555555-actor_999_999",
                             "agentId": "invented-actor", "agentType": "zcode-workflow",
                             "childTurnId": "turn_invented",
                             "parentToolCallId": "wrong-dispatch"}}
        self.assertEqual(authority.decide(forged)["decision"], "deny")
        self.assertEqual(authority.children, {})


class LaunchResultReplayTests(unittest.TestCase):
    """N2 (round6): the FIRST observation of an approved launch result fixes
    the outcome — bytes captured, or the entry factually missing — and an
    identical replayed result must never sample later bytes."""

    RUN = "dwfrun-00000000-0000-0000-0000-000000000001"

    def _authority(self):
        import tempfile
        from zcode_supervisor.protocol import inventory
        tmp = tempfile.TemporaryDirectory(); self.addCleanup(tmp.cleanup)
        ws = Path(tmp.name) / "workspace"; ws.mkdir()
        pp = Path(tmp.name) / "packet.json"
        full = packet({**VALID_WORKFLOW}); full["workspace"] = str(ws.resolve())
        pp.write_text(json.dumps(full))
        authority = Authority(full, pp, inventory(ws))
        authority.session_id = "sess_parent"; authority.turn_id = "turn_main"
        authority.note_turn("turn_main")
        authority._approved_workflow_tids.add("tcw_launch")
        return authority, ws

    def _result_event(self):
        return {"type": "tool.updated", "payload": {
            "kind": "result", "toolCallId": "tcw_launch",
            "result": {"success": True, "content": f"Launched {self.RUN}"}}}

    def test_missing_at_first_result_stays_missing_on_replay(self):
        authority, ws = self._authority()
        event = self._result_event()
        authority.note_event(event)
        self.assertEqual(authority.runtime_entry_bytes, {})
        self.assertIn(self.RUN, authority.launch_results_observed)
        # An unknown file with the run's name appears AFTER the result.
        entry = ws / ".zcode" / "workflow-runs" / f"{self.RUN}.mjs"
        entry.parent.mkdir(parents=True)
        entry.write_text("new unknown contents AFTER the original launch result")
        with self.assertRaises(ValueError):
            authority.check_frozen()
        # Replaying the IDENTICAL result event must not adopt those bytes.
        authority.note_event(event)
        self.assertEqual(authority.runtime_entry_bytes, {})
        with self.assertRaises(ValueError):
            authority.check_frozen()

    def test_write_before_result_binds_once_and_survives_replay(self):
        authority, ws = self._authority()
        entry = ws / ".zcode" / "workflow-runs" / f"{self.RUN}.mjs"
        entry.parent.mkdir(parents=True)
        entry.write_text("entry bytes the reviewed runtime just wrote")
        event = self._result_event()
        authority.note_event(event)   # subscription-callback observation
        authority.note_event(event)   # event-drain observation (replay)
        bound = dict(authority.runtime_entry_bytes)
        self.assertEqual(list(bound), [self.RUN])
        authority.check_frozen()      # exact bytes still match: accepted
        entry.write_text("post-bind rewrite")
        with self.assertRaises(ValueError):
            authority.check_frozen()
        authority.note_event(event)   # a third replay adopts nothing
        self.assertEqual(authority.runtime_entry_bytes, bound)


class SkillsClosureBindingTests(unittest.IsolatedAsyncioTestCase):
    """Packets exposing the Skill tool must bind the same controller-approved
    bundled-skills closure as workflow packets (the worker discovers skills at
    startup and the cached CLI resolves bundled skills through the cache-level
    link). Baseline packets stay unbound."""

    async def _run(self, orchestration=None):
        import asyncio
        import io as io_module
        import json as json_module
        import shutil
        import tempfile
        from contextlib import redirect_stdout
        from types import SimpleNamespace
        from unittest.mock import patch
        from zcode_supervisor import protocol as P, runtime as R
        root = Path(tempfile.mkdtemp(prefix='czs-skills-bind-')).resolve()
        self.addCleanup(shutil.rmtree, root, True)
        app = root / 'app'
        skill = app / 'skills' / 'dynamic-workflows'
        skill.mkdir(parents=True)
        for member, text in ((app / 'README.md', 'root'),
                             (skill / 'SKILL.md', 'v'), (skill / 'patterns.md', 'p'),
                             (skill / 'examples.md', 'e')):
            member.write_text(text)
        config = R.Config(work_root=root / 'work', state_dir=root / 'state',
                          cache_dir=root / 'cache', cli_path=root / 'cli')
        config.cache_dir.mkdir()
        config.work_root.mkdir()
        R.prepare_bundled_skills(config, source=app)
        workspace = config.work_root / 'workspace'
        workspace.mkdir()
        packet = {'workspace': str(workspace), 'prompt': 'offline', 'allowed_files': []}
        if orchestration is not None:
            packet['orchestration'] = orchestration
        packet_file = root / 'packet.json'
        packet_file.write_text(json_module.dumps(packet))
        out = config.work_root / 'run.json'

        class Client:
            def __init__(self, *a, orchestration=None):
                self.events = []
                self.failure = None
                self.on_event = None
                self.process = SimpleNamespace(pid=987654, returncode=None)

            async def start(self):
                pass

            async def close(self):
                self.process.returncode = 0

            def _event(self, typ, payload, seq):
                event = {'type': typ, 'sessionId': 's', 'turnId': 't', 'seq': seq,
                         'payload': payload}
                self.events.append(event)
                if self.on_event:
                    self.on_event(event)

            async def request(self, method, params):
                if method in ('session/create', 'session/read'):
                    return {'session': {'sessionId': 's', 'status': 'idle'},
                            'runtime': {'pendingRequestIds': []},
                            'projection': {}, 'settings': {}}
                if method == 'session/subscribe':
                    return {'eventSeq': 0}
                if method == 'session/subagents':
                    return {'running': [], 'ended': {'items': []}}
                if method == 'session/send':
                    self._event('turn.started', {'inputId': params['inputId']}, 1)
                    self._event('turn.completed', {'resultType': 'success'}, 5)
                return {}

        real_sleep = asyncio.sleep

        async def zero(*a, **kw):
            await real_sleep(0)

        args = SimpleNamespace(packet=str(packet_file), out=str(out), resume=None,
                               timeout=5)
        with patch.object(P, 'ProtocolClient', Client),              patch.object(P, 'send_notification', lambda **kw: None),              patch('asyncio.sleep', side_effect=zero),              redirect_stdout(io_module.StringIO()):
            await P.execute(args, config)
        return json_module.loads(out.read_text())

    async def test_skills_packet_binds_and_baseline_does_not(self):
        receipt = await self._run({'skills': {'allowed': ['dynamic-workflows']}})
        self.assertTrue(receipt['delivery_ok'])
        binding = receipt['skill_closure_binding']
        self.assertTrue(Path(binding['target']).name.startswith('skills-'))
        self.assertEqual(len(binding['digest']), 64)
        baseline = await self._run()
        self.assertTrue(baseline['delivery_ok'])
        self.assertNotIn('skill_closure_binding', baseline)
