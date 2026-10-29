"""ui_registry contract tests (one-click app discovery).

Register/heartbeat/unregister, atomic private writes, stale cleanup,
foreign-pid PermissionError semantics and the CZS_UI_EVENTS gating of
the production wiring (start_ui_service). Local temp registries only —
the real ``~/.czs`` is never touched (CZS_UI_REGISTRY override).
"""

import asyncio
import json
import os
import stat
import subprocess
import tempfile
import unittest
import unittest.mock
from pathlib import Path

from zcode_supervisor import ui_registry
from zcode_supervisor.ui_projection import UiEmitter

HEX64 = "3" * 64
RFC = "2070-01-01T00:00:00.000000Z"


def secure_dir(root: Path) -> Path:
    """A 0700 owner-only directory exactly as the hub state_dir promises."""
    state = root / "state"
    state.mkdir(mode=0o700, parents=True)
    os.chmod(state, 0o700)
    return state


def make_emitter():
    return UiEmitter(hub_instance_id="hubreg01", journal_epoch=1,
                     enabled=True)


def emit_task(emitter, task_id):
    emitter.emit(
        "task.accepted", task_id, task_id=task_id, worker="codex",
        mode="app-server",
        data={"contract_id": f"contract-{task_id}", "contract_digest": HEX64,
              "created_at": RFC, "initiator_verified": False})


def reaped_pid() -> int:
    """A pid that is provably dead (child completed AND was reaped)."""
    proc = subprocess.Popen(["/bin/sleep", "0"])
    pid = proc.pid
    proc.wait()
    assert not ui_registry.pid_alive(pid), \
        "precondition: reaped pid must be dead"
    return pid


class TempRegistryTestCase(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.registry = self.root / "ui-registry.json"

    def write_raw(self, payload) -> None:
        self.registry.write_text(payload if isinstance(payload, str)
                                 else json.dumps(payload), encoding="utf-8")

    def load_raw(self) -> dict:
        return json.loads(self.registry.read_text(encoding="utf-8"))


class RegistryFileTests(TempRegistryTestCase):
    def test_register_roundtrip_permissions_and_atomicity(self):
        socket = self.root / "hubs.sock"
        socket.touch()  # live socket file for the reader's existence check
        entry = ui_registry.register(socket, path=self.registry)
        self.assertEqual(entry["pid"], os.getpid())
        self.assertEqual(entry["socket"], os.path.abspath(socket))
        self.assertEqual(entry["project_root"], os.getcwd())
        self.assertEqual(entry["started_at"], entry["heartbeat_at"])
        # the file is keyed by str(pid) and written 0600 in a 0700 dir
        self.assertEqual(set(self.load_raw()), {str(os.getpid())})
        self.assertEqual(stat.S_IMODE(self.registry.stat().st_mode), 0o600)
        self.assertEqual(
            stat.S_IMODE(self.registry.parent.stat().st_mode), 0o700)
        # atomic write leaves no tmp siblings behind (the socket file we
        # touched for the existence check may share the dir — fine)
        self.assertEqual(
            [p.name for p in self.registry.parent.iterdir()
             if p.name.endswith(".tmp")], [])
        live = ui_registry.read_registry(path=self.registry)
        self.assertEqual([item["socket"] for item in live],
                         [os.path.abspath(socket)])

    def test_creates_missing_registry_directory_private(self):
        nested = self.root / "deep" / "czs" / "ui-registry.json"
        socket = self.root / "s.sock"
        socket.touch()
        ui_registry.register(socket, path=nested)
        self.assertTrue(nested.exists())
        self.assertEqual(
            stat.S_IMODE(nested.parent.stat().st_mode), 0o700)

    def test_unregister_is_idempotent_and_best_effort(self):
        socket = self.root / "s.sock"
        socket.touch()
        ui_registry.register(socket, path=self.registry)
        ui_registry.unregister(path=self.registry)
        self.assertEqual(self.load_raw(), {})
        ui_registry.unregister(path=self.registry)  # absent: no error
        self.assertFalse(self.registry.parent.joinpath(
            f".{self.registry.name}.{os.getpid()}.tmp").exists())

    def test_same_pid_rebind_replaces_entry(self):
        first = self.root / "one.sock"
        first.touch()
        second = self.root / "two.sock"
        second.touch()
        entry_one = ui_registry.register(first, path=self.registry)
        entry_two = ui_registry.register(second, path=self.registry)
        self.assertEqual(len(self.load_raw()), 1)
        live = ui_registry.read_registry(path=self.registry)
        self.assertEqual([item["socket"] for item in live],
                         [os.path.abspath(second)])
        self.assertGreaterEqual(entry_two["started_at"],
                                entry_one["started_at"])

    def test_refresh_preserves_started_at_and_self_heals(self):
        socket = self.root / "s.sock"
        socket.touch()
        entry = ui_registry.register(socket, path=self.registry)
        # simulate a lost update: another writer drops our entry
        self.write_raw({"999999": {"socket": "/nowhere", "pid": 999999,
                                   "project_root": "/", "started_at": 1.0,
                                   "heartbeat_at": 1.0}})
        refreshed = ui_registry.refresh(entry, path=self.registry)
        self.assertEqual(refreshed["started_at"], entry["started_at"])
        self.assertGreater(refreshed["heartbeat_at"],
                           entry["heartbeat_at"])
        # our entry is back in the FILE next to the foreign one (the
        # reader then drops the foreign one: its socket does not exist)
        self.assertEqual(set(self.load_raw()), {str(os.getpid()), "999999"})
        live = ui_registry.read_registry(path=self.registry)
        self.assertEqual([item["pid"] for item in live], [os.getpid()])


class StaleCleanupTests(TempRegistryTestCase):
    def test_dead_pid_entry_filtered_and_cleaned(self):
        dead = reaped_pid()
        socket = self.root / "s.sock"
        socket.touch()
        self.write_raw({str(dead): {"socket": str(socket), "pid": dead,
                                    "project_root": "/tmp",
                                    "started_at": 1.0, "heartbeat_at": 1.0}})
        self.assertEqual(ui_registry.read_registry(path=self.registry), [])
        # the cleanup removed it from the file
        self.assertEqual(self.load_raw(), {})

    def test_missing_socket_filtered_pid_alive(self):
        socket = self.root / "vanishing.sock"
        socket.touch()
        ui_registry.register(socket, path=self.registry)
        socket.unlink()
        self.assertEqual(ui_registry.read_registry(path=self.registry), [])

    def test_cleanup_opt_out_keeps_file(self):
        dead = reaped_pid()
        self.write_raw({str(dead): {"socket": "/x", "pid": dead,
                                    "project_root": "/", "started_at": 1.0,
                                    "heartbeat_at": 1.0}})
        self.assertEqual(
            ui_registry.read_registry(path=self.registry, cleanup=False), [])
        self.assertIn(str(dead), self.load_raw())

    def test_newest_started_at_first(self):
        socket = self.root / "s.sock"
        socket.touch()
        entries = {f"10000{i}": {"socket": str(socket), "pid": 10000 + i,
                                 "project_root": f"/p{i}",
                                 "started_at": float(i),
                                 "heartbeat_at": float(i)}
                   for i in range(3)}
        self.write_raw(entries)
        # pids 10000..10002 are not ours: patch pid_alive so they read live
        with unittest.mock.patch.object(ui_registry, "pid_alive",
                                        return_value=True):
            live = ui_registry.read_registry(path=self.registry)
        self.assertEqual([item["project_root"] for item in live],
                         ["/p2", "/p1", "/p0"])

    def test_malformed_entries_and_bad_json_tolerated(self):
        socket = self.root / "s.sock"
        socket.touch()
        ui_registry.register(socket, path=self.registry)
        good = self.load_raw()[str(os.getpid())]
        self.write_raw({"junk": "not-an-object",
                        "incomplete": {"socket": "/x"},
                        str(os.getpid()): good,
                        "bad-started": {"socket": str(socket),
                                        "pid": os.getpid(),
                                        "project_root": "/",
                                        "started_at": True,
                                        "heartbeat_at": 1.0}})
        self.assertEqual(len(
            ui_registry.read_registry(path=self.registry)), 1)
        self.write_raw("{ this is not json")
        self.assertEqual(ui_registry.read_registry(path=self.registry), [])
        # a corrupt file does not block the next registration
        again = ui_registry.register(socket, path=self.registry)
        self.assertEqual(again["pid"], os.getpid())


class PidAliveTests(unittest.TestCase):
    def test_pid_alive_semantics(self):
        self.assertTrue(ui_registry.pid_alive(os.getpid()))
        self.assertFalse(ui_registry.pid_alive(-1))
        self.assertFalse(ui_registry.pid_alive(0))
        self.assertFalse(ui_registry.pid_alive("not-a-pid"))

        def refused(pid, sig):
            # simulate a kernel answer for a nonexistent pid
            raise ProcessLookupError()

        def foreign(pid, sig):
            # EPERM: the pid EXISTS but belongs to another user
            raise PermissionError()

        with unittest.mock.patch.object(ui_registry.os, "kill",
                                        side_effect=refused):
            self.assertFalse(ui_registry.pid_alive(1234567))
        with unittest.mock.patch.object(ui_registry.os, "kill",
                                        side_effect=foreign):
            self.assertTrue(ui_registry.pid_alive(1234567))


class RegistryPathTests(TempRegistryTestCase):
    def test_env_override_and_home_default(self):
        with unittest.mock.patch.dict(
                os.environ, {"CZS_UI_REGISTRY": str(self.registry)}):
            self.assertEqual(ui_registry.registry_path(), self.registry)
            socket = self.root / "s.sock"
            socket.touch()
            ui_registry.register(socket)  # lands in the override
            self.assertTrue(self.registry.exists())
        with unittest.mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("CZS_UI_REGISTRY", None)
            self.assertEqual(
                ui_registry.registry_path(),
                Path.home() / ".czs" / "ui-registry.json")


class ServiceWiringTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.registry = self.root / "ui-registry.json"

    def write_raw(self, payload) -> None:
        self.registry.write_text(payload if isinstance(payload, str)
                                 else json.dumps(payload), encoding="utf-8")

    def load_raw(self) -> dict:
        return json.loads(self.registry.read_text(encoding="utf-8"))

    async def _service(self, state, emitter):
        from zcode_supervisor import ui_server
        return await ui_server.start_ui_service(
            state_dir=state, emitter=emitter)

    async def test_flag_off_writes_no_registry(self):
        state = secure_dir(self.root)
        emitter = make_emitter()
        with unittest.mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("CZS_UI_EVENTS", None)
            os.environ["CZS_UI_REGISTRY"] = str(self.registry)
            service = await self._service(state, emitter)
        self.assertIsNone(service)
        self.assertFalse(self.registry.exists())

    async def test_direct_server_construction_never_registers(self):
        from zcode_supervisor import ui_server
        from zcode_supervisor.ui_journal import UiJournal
        state = secure_dir(self.root)
        emitter = make_emitter()
        journal = UiJournal(emitter)
        with unittest.mock.patch.dict(os.environ,
                                      {"CZS_UI_EVENTS": "1",
                                       "CZS_UI_REGISTRY":
                                           str(self.registry)}):
            server = ui_server.UiEventServer(journal, state_dir=state)
            await server.start()
            try:
                self.assertFalse(self.registry.exists())
            finally:
                await server.close()
        self.assertFalse(self.registry.exists())

    async def test_service_registers_and_unregisters(self):
        state = secure_dir(self.root)
        emitter = make_emitter()
        with unittest.mock.patch.dict(os.environ,
                                      {"CZS_UI_EVENTS": "1",
                                       "CZS_UI_REGISTRY":
                                           str(self.registry)}):
            service = await self._service(state, emitter)
            self.assertIsNotNone(service)
            live = ui_registry.read_registry(path=self.registry)
            self.assertEqual(len(live), 1)
            self.assertEqual(live[0]["pid"], os.getpid())
            self.assertEqual(live[0]["project_root"], os.getcwd())
            self.assertEqual(
                live[0]["socket"], os.path.abspath(service.socket_path))
            # the heartbeat task exists and dies with close()
            self.assertIsNotNone(service._registry_task)
            await service.close()
            self.assertIsNone(service._registry_task)
            self.assertEqual(
                ui_registry.read_registry(path=self.registry), [])

    async def test_heartbeat_reasserts_lost_entry(self):
        state = secure_dir(self.root)
        emitter = make_emitter()
        # patch BEFORE start so the heartbeat loop's FIRST sleep reads
        # the accelerated cadence (the loop samples the attribute each
        # iteration, but the in-flight sleep may already be 30 s)
        with unittest.mock.patch.dict(os.environ,
                                      {"CZS_UI_EVENTS": "1",
                                       "CZS_UI_REGISTRY":
                                           str(self.registry)}), \
                unittest.mock.patch.object(ui_registry, "HEARTBEAT_S", 0.01):
            service = await self._service(state, emitter)
            self.assertIsNotNone(service)
            try:
                # another writer wipes our entry (lost-update race)
                self.write_raw({})
                await asyncio.sleep(0.08)
                live = ui_registry.read_registry(path=self.registry)
                self.assertEqual([item["pid"] for item in live],
                                 [os.getpid()])
            finally:
                await service.close()

    async def test_registry_failure_never_blocks_the_service(self):
        state = secure_dir(self.root)
        emitter = make_emitter()
        blocker = self.root / "blocker"
        blocker.write_text("a regular file in the way")
        broken = blocker / "ui-registry.json"
        with unittest.mock.patch.dict(os.environ,
                                      {"CZS_UI_EVENTS": "1",
                                       "CZS_UI_REGISTRY": str(broken)}):
            service = await self._service(state, emitter)
            self.assertIsNotNone(service)  # registry best-effort only
            self.assertTrue(service.socket_path.exists())
            self.assertIsNone(service._registry_entry)
            await service.close()

    async def test_second_start_rebinds_same_pid_entry(self):
        state = secure_dir(self.root)
        emitter = make_emitter()
        with unittest.mock.patch.dict(os.environ,
                                      {"CZS_UI_EVENTS": "1",
                                       "CZS_UI_REGISTRY":
                                           str(self.registry)}):
            first = await self._service(state, emitter)
            self.assertIsNotNone(first)
            await first.close()
            # same process, second bind: ONE entry again, not two
            state2 = secure_dir(self.root / "again")
            second = await self._service(state2, emitter)
            self.assertIsNotNone(second)
            try:
                self.assertEqual(len(self.load_raw()), 1)
                live = ui_registry.read_registry(path=self.registry)
                self.assertEqual(live[0]["socket"],
                                 os.path.abspath(second.socket_path))
            finally:
                await second.close()


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
