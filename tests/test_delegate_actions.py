"""P2 (0.2.3): delegate action=start/attach/stop — sub-agent semantics."""

import json
import stat
import tempfile
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace

from zcode_supervisor import contracts, hub_server as H


def _register(config, contract: dict) -> str:
    path = Path(config.state_dir) / "src-contract.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(contract))
    contracts.register_contract(Path(config.state_dir), path)
    return contract["contract_id"]


def _dsh_contract(workspace: Path, **overrides) -> dict:
    contract = {
        "contract_id": "dsh-follow", "worker": "dsh",
        "workspace": str(workspace), "prompt": "continue the work",
        "allowed_files": [], "commands": [], "model": None,
        "max_timeout_ms": 60_000, "budget": {},
    }
    contract.update(overrides)
    return contract


def _zcode_contract(workspace: Path, **overrides) -> dict:
    contract = {
        "contract_id": "zc-follow", "worker": "zcode",
        "workspace": str(workspace), "prompt": "continue the work",
        "allowed_files": [], "commands": [],
        "model": {"providerId": "glm", "modelId": "glm-5.3"},
        "max_timeout_ms": 60_000, "budget": {},
    }
    contract.update(overrides)
    return contract


def _prior_task(config, task_id, worker, session_id, contract_id, workspace):
    record = {
        "task_id": task_id, "worker": worker, "status": "completed",
        "outcome": "delivered", "contract_id": contract_id,
        "session_locator": {"agent": worker, "session_id": session_id},
        "evidence_dir": str(Path(config.work_root) / "hub-tasks" / task_id),
        "result": {},
    }
    path = Path(config.state_dir) / "hub" / "tasks" / f"{task_id}.json"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(record))
    return record


class DelegateActions(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.config = SimpleNamespace(
            state_dir=self.root / "state", work_root=self.root / "work",
            cache_dir=self.root / "cache")
        for name in ("state", "work", "cache"):
            (self.root / name).mkdir(exist_ok=True)
        self.evidence = self.root / "evidence"
        self.workspace = self.root / "ws"
        self.workspace.mkdir()
        self.initiator = {"agent": "codex", "session_id": "s1"}

    def tearDown(self):
        self.tmp.cleanup()

    def _delegate(self, **kwargs):
        import asyncio
        kwargs.setdefault("dsh_runner", self._noop_dsh_runner)
        return asyncio.run(H.delegate_impl(self.config, self.evidence,
                                           kwargs.pop("contract_id", "dsh-follow"),
                                           self.initiator, **kwargs))

    @staticmethod
    async def _noop_dsh_runner(dsh_path, workspace, task, evidence_dir, *,
                               wall_timeout_s, stall_timeout_s=None,
                               session_id=None, stop_event=None):
        return {"outcome": "delivered", "session_id": session_id or "session-new",
                "final_message": "ok", "exit_code": 0,
                "progress": {"final": "delivered"}}

    def test_attach_dsh_passes_prior_session(self):
        _register(self.config, _dsh_contract(self.workspace))
        _register(self.config, _dsh_contract(self.workspace,
                                             contract_id="dsh-first"))
        _prior_task(self.config, "t1", "dsh", "session-abc", "dsh-first",
                    self.workspace)
        result = self._delegate(action="attach", attach_to="t1")
        self.assertEqual(result["outcome"], "delivered")
        # the continued conversation keeps its identity (dsh --session-id)
        self.assertEqual(result["session_locator"]["session_id"], "session-abc")
        record = json.loads((self.config.state_dir / "hub" / "tasks"
                             / (result["task_id"] + ".json")).read_text())
        self.assertEqual(record["attached_to"], "t1")
        self.assertEqual(record["resumed_session"], "session-abc")

    def test_attach_refuses_worker_mismatch(self):
        _register(self.config, _dsh_contract(self.workspace))
        _register(self.config, _zcode_contract(self.workspace,
                                               contract_id="zc-first"))
        _prior_task(self.config, "t1", "zcode", "sess_1", "zc-first",
                    self.workspace)
        with self.assertRaises(H.HubError) as caught:
            self._delegate(action="attach", attach_to="t1")
        self.assertIn("worker mismatch", str(caught.exception))

    def test_attach_refuses_workspace_mismatch(self):
        other = self.root / "ws2"
        other.mkdir()
        _register(self.config, _zcode_contract(self.workspace))
        _register(self.config, _zcode_contract(other, contract_id="zc-first"))
        _prior_task(self.config, "t1", "zcode", "sess_1", "zc-first", other)
        with self.assertRaises(H.HubError) as caught:
            import asyncio
            asyncio.run(H.delegate_impl(
                self.config, self.evidence, "zc-follow", self.initiator,
                action="attach", attach_to="t1",
                zcode_runner=self._fail_runner()))
        self.assertIn("workspace mismatch", str(caught.exception))

    @staticmethod
    def _fail_runner():
        async def runner(*args, **kwargs):
            raise AssertionError("runner must not be reached on refusal")
        return runner

    def test_attach_zcode_same_workspace_resumes(self):
        _register(self.config, _zcode_contract(self.workspace))
        _register(self.config, _zcode_contract(self.workspace,
                                               contract_id="zc-first"))
        _prior_task(self.config, "t1", "zcode", "sess_1", "zc-first",
                    self.workspace)
        calls = {}

        async def fake_zcode_runner(config, task_id, contract, timeout_s=None,
                                    ui_emitter=None, resume=None):
            calls["resume"] = resume
            return {"delivery_ok": True, "session_id": "sess_2",
                    "outcome": "delivered", "run_path": None, "db_path": None}

        import asyncio
        result = asyncio.run(H.delegate_impl(
            self.config, self.evidence, "zc-follow", self.initiator,
            action="attach", attach_to="t1", zcode_runner=fake_zcode_runner))
        self.assertEqual(calls["resume"], "sess_1")
        self.assertEqual(result["outcome"], "delivered")

    def test_attach_codex_is_unsupported(self):
        contract = {
            "contract_id": "cx-follow", "worker": "codex",
            "workspace": str(self.workspace),
            "prompt": "continue", "allowed_files": [], "commands": [],
            "model": None, "max_timeout_ms": 60_000, "budget": {},
            "output_schema": {"type": "object",
                              "properties": {"a": {"type": "string"}},
                              "required": ["a"], "additionalProperties": False},
        }
        _register(self.config, contract)
        with self.assertRaises(H.HubError) as caught:
            self._delegate(contract_id="cx-follow", action="attach",
                           attach_to="t9")
        self.assertIn("codex attach is not implemented", str(caught.exception))

    def test_stop_unknown_task_is_rejected(self):
        with self.assertRaises(H.HubError) as caught:
            self._delegate(action="stop", attach_to="missing")
        self.assertEqual(caught.exception.code, "unknown-task")

    def test_stop_signals_registered_event(self):
        event = threading.Event()
        with H._running_tasks_lock:
            H._running_tasks["live-1"] = {"stop": event, "worker": "dsh"}
        try:
            result = self._delegate(action="stop", attach_to="live-1")
        finally:
            with H._running_tasks_lock:
                H._running_tasks.pop("live-1", None)
        self.assertTrue(result["stop_requested"])
        self.assertTrue(event.is_set())


class DshDriverSessionAndStop(unittest.TestCase):
    """Driver-level P2: --session-id argv and the stop_event gradient."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.workspace = self.root / "ws"
        self.evidence = self.root / "ev"
        self.workspace.mkdir()

    def tearDown(self):
        self.tmp.cleanup()

    def _fake(self, name, body):
        path = self.root / name
        path.write_text(body)
        path.chmod(path.stat().st_mode | stat.S_IEXEC)
        return path

    def test_session_id_reaches_argv(self):
        script = ('#!/bin/sh\ncat > /dev/null\necho \'{"type":"session",'
                  '"sessionId":"session-x","cwd":"/w"}\'\n'
                  'echo "{\\"type\\":\\"final\\",\\"text\\":\\"argv:$*\\"}"\n')
        from zcode_supervisor import dsh_driver
        receipt = dsh_driver.run_headless(
            self._fake("dsh-argv", script), self.workspace, "go", self.evidence,
            wall_timeout_s=30, session_id="session-x")
        self.assertEqual(receipt["outcome"], "delivered")
        self.assertIn("--session-id", receipt["final_message"])
        self.assertIn("session-x", receipt["final_message"])

    def test_stop_event_terminates_as_stopped(self):
        import threading
        import time
        from zcode_supervisor import dsh_driver
        script = ('#!/bin/sh\ncat > /dev/null\necho \'{"type":"session",'
                  '"sessionId":"session-s","cwd":"/w"}\'\nsleep 60\n')
        event = threading.Event()
        threading.Timer(1.0, event.set).start()
        started = time.monotonic()
        receipt = dsh_driver.run_headless(
            self._fake("dsh-stop", script), self.workspace, "go", self.evidence,
            wall_timeout_s=60, stall_timeout_s=50, stop_event=event)
        self.assertEqual(receipt["outcome"], "stopped")
        self.assertIsNone(receipt["timed_out"])
        self.assertLess(receipt["duration_s"], 30)
        self.assertLess(time.monotonic() - started, 30)

    def test_bad_session_id_rejected(self):
        from zcode_supervisor import dsh_driver
        with self.assertRaises(dsh_driver.DshRunError):
            dsh_driver.run_headless(
                "x", self.workspace, "t", self.evidence,
                wall_timeout_s=5, session_id="  ")


if __name__ == "__main__":
    unittest.main()
