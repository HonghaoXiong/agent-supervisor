"""L1 telemetry: UDS push channel — TelemetryHub cache + dsh_driver push."""

import asyncio
import json
import tempfile
import threading
import time
import unittest
from pathlib import Path
from types import SimpleNamespace

from zcode_supervisor import hub_server as H
from zcode_supervisor import transport as T


class TelemetryHubTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)

    def tearDown(self):
        self.tmp.cleanup()

    def test_push_and_read(self):
        async def scenario():
            hub = H.TelemetryHub(self.root)
            await hub.start()
            # Simulate a worker pushing progress
            client = T.UdsTransport(socket_path=hub.socket_path)
            await client.connect()
            await asyncio.sleep(0.05)
            await client.send(T.MessageEnvelope(
                T.MSG_PROGRESS,
                {"task_id": "task-1", "state": "running",
                 "detail": "dsh:turn_start"}))
            await asyncio.sleep(0.1)
            result = hub.get_status("task-1")
            # Push a terminal result
            await client.send(T.MessageEnvelope(
                T.MSG_RESULT,
                {"task_id": "task-1", "state": "finished",
                 "outcome": "delivered"}))
            await asyncio.sleep(0.1)
            final = hub.get_status("task-1")
            await client.close()
            await hub.stop()
            return result, final
        result, final = asyncio.run(scenario())
        self.assertIsNotNone(result)
        self.assertEqual(result["state"], "running")
        self.assertEqual(result["source"], "uds-push")
        self.assertEqual(final["state"], "finished")
        self.assertEqual(final["outcome"], "delivered")

    def test_unknown_task_returns_none(self):
        hub = H.TelemetryHub(self.root)
        self.assertIsNone(hub.get_status("no-such-task"))

    def test_task_status_prefers_telemetry(self):
        """task_status_impl reads UDS cache before falling back to files."""
        # Reset the module-level telemetry singleton for isolation
        H._telemetry = None

        async def scenario():
            hub = H.TelemetryHub(self.root)
            await hub.start()
            # Push a status update
            client = T.UdsTransport(socket_path=hub.socket_path)
            await client.connect()
            await asyncio.sleep(0.05)
            await client.send(T.MessageEnvelope(
                T.MSG_PROGRESS,
                {"task_id": "uds-task", "state": "running",
                 "detail": "active"}))
            await asyncio.sleep(0.1)
            await client.close()
            return hub

        hub = asyncio.run(scenario())
        H._telemetry = hub  # inject into the module-level singleton

        # Create a fake task record so task_status can find it
        config = SimpleNamespace(state_dir=self.root, work_root=self.root)
        tasks_dir = self.root / "hub" / "tasks"
        tasks_dir.mkdir(parents=True)
        record = {"task_id": "uds-task", "worker": "dsh", "mode": "headless",
                  "status": "running", "contract_id": "c1"}
        (tasks_dir / "uds-task.json").write_text(json.dumps(record))

        report = H.task_status_impl(config, task_id="uds-task")
        self.assertEqual(len(report["tasks"]), 1)
        entry = report["tasks"][0]
        self.assertIn("heartbeat", entry)
        self.assertEqual(entry["heartbeat"]["source"], "uds-push")
        self.assertEqual(entry["heartbeat"]["state"], "running")

        H._telemetry = None  # cleanup


class DshTelemetryPush(unittest.TestCase):
    """The dsh driver pushes progress to a UDS telemetry socket."""

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        H._telemetry = None

    def tearDown(self):
        H._telemetry = None
        self.tmp.cleanup()

    def test_driver_pushes_to_telemetry(self):
        async def scenario():
            from zcode_supervisor import dsh_driver
            hub = H.TelemetryHub(self.root / "state")
            (self.root / "state").mkdir(exist_ok=True)
            await hub.start()

            workspace = self.root / "ws"
            evidence = self.root / "ev"
            workspace.mkdir()
            evidence.mkdir()

            fake = self.root / "fake-dsh"
            fake.write_text(
                "#!/bin/sh\ncat > /dev/null\n"
                'echo \'{"type":"session","sessionId":"s-tel","cwd":"/w"}\'\n'
                'echo \'{"type":"status","phase":"turn_start","turn":1}\'\n'
                'echo \'{"type":"status","phase":"turn_end","turn":1,'
                '"reason":{"kind":"success"}}\'\n'
                'echo \'{"type":"final","text":"done"}\'\n')
            fake.chmod(0o755)

            receipt = dsh_driver.run_headless(
                fake, workspace, "task", evidence,
                wall_timeout_s=30,
                telemetry_socket=hub.socket_path,
                task_id="dsh-tel-test")
            await asyncio.sleep(0.1)
            status = hub.get_status("dsh-tel-test")
            await hub.stop()
            return receipt, status

        receipt, status = asyncio.run(scenario())
        self.assertEqual(receipt["outcome"], "delivered")
        self.assertIsNotNone(status, "telemetry should have received pushes")
        self.assertEqual(status["state"], "finished")
        self.assertEqual(status["outcome"], "delivered")
        self.assertIsNotNone(status.get("last_detail"))

    def test_driver_without_telemetry_still_works(self):
        """telemetry_socket=None keeps the original behavior."""
        from zcode_supervisor import dsh_driver
        workspace = self.root / "ws"
        evidence = self.root / "ev"
        workspace.mkdir()
        evidence.mkdir()
        fake = self.root / "fake-dsh"
        fake.write_text(
            "#!/bin/sh\ncat > /dev/null\n"
            'echo \'{"type":"session","sessionId":"s1"}\'\n'
            'echo \'{"type":"final","text":"ok"}\'\n')
        fake.chmod(0o755)
        receipt = dsh_driver.run_headless(
            fake, workspace, "task", evidence, wall_timeout_s=30)
        self.assertEqual(receipt["outcome"], "delivered")


if __name__ == "__main__":
    unittest.main()
