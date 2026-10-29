"""UiEventServer contract tests (P1.1): secure owner-only UDS, read-only
NDJSON protocol, three discovery time-points, slow clients, heartbeats and
fault isolation. Local sockets only — no model, production CLI or network."""

import asyncio
import contextlib
import json
import os
import socket
import stat
import tempfile
import unittest
import unittest.mock
from pathlib import Path

from zcode_supervisor.ui_events import HUB_STREAM_ID
from zcode_supervisor.ui_journal import UiJournal
from zcode_supervisor.ui_projection import UiEmitter
from zcode_supervisor.ui_server import UiEventServer, UiTransportError

HEX64 = "3" * 64
RFC = "2070-01-01T00:00:00.000000Z"


def make_parts(**journal_options):
    emitter = UiEmitter(hub_instance_id="hubtest01", journal_epoch=1,
                        enabled=True)
    options = dict(max_events=200, max_bytes=1 << 20, max_event_bytes=1 << 16,
                   page_max_records=3, page_max_bytes=64 * 1024,
                   registry_max_cursors=8, registry_ttl_s=300.0,
                   queue_max_events=256, queue_max_bytes=1 << 20)
    options.update(journal_options)
    journal = UiJournal(emitter, **options)
    return emitter, journal


def emit_task(emitter, task_id):
    accepted = emitter.emit(
        "task.accepted", task_id, task_id=task_id, worker="codex",
        mode="app-server",
        data={"contract_id": f"contract-{task_id}", "contract_digest": HEX64,
              "created_at": RFC, "initiator_verified": False})
    emitter.emit("stream.announced", HUB_STREAM_ID, task_id=task_id,
                 worker="codex", mode="app-server",
                 data={"task_stream_id": task_id,
                       "accepted_event_id": accepted["event_id"]})
    emitter.emit("turn.started", task_id, task_id=task_id, worker="codex",
                 mode="app-server", session_id=f"sess-{task_id}",
                 turn_id=f"turn-{task_id}")


def secure_dir(root: Path) -> Path:
    """A 0700 owner-only directory exactly as the hub state_dir promises."""
    state = root / "state"
    state.mkdir(mode=0o700)
    os.chmod(state, 0o700)
    return state


class UiClient:
    """Minimal read-only NDJSON test client (same-user peer)."""

    def __init__(self, reader, writer):
        self.reader = reader
        self.writer = writer

    @classmethod
    async def connect(cls, path):
        # plain system-default buffers: a non-reading peer backs up fast,
        # which is exactly the slow-consumer shape under test
        reader, writer = await asyncio.open_unix_connection(path)
        return cls(reader, writer)

    async def send(self, payload):
        self.writer.write((json.dumps(payload) + "\n").encode())
        await self.writer.drain()

    async def recv(self, timeout=5.0):
        line = await asyncio.wait_for(self.reader.readline(), timeout)
        if not line:
            raise ConnectionError("server closed the stream")
        return json.loads(line)

    async def request(self, payload, timeout=5.0):
        await self.send(payload)
        return await self.recv(timeout)

    async def collect(self, count, timeout=5.0):
        return [await self.recv(timeout) for _ in range(count)]

    async def close(self):
        self.writer.close()
        try:
            await self.writer.wait_closed()
        except (ConnectionError, OSError):
            pass


class UiServerTestCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.state = secure_dir(self.root)
        self.emitter, self.journal = make_parts()
        self.server = UiEventServer(self.journal,
                                    socket_path=self.state / "ui-events.sock")
        await self.server.start()
        self.addAsyncCleanup(self.server.close)
        self.path = self.server.socket_path

    async def client(self):
        client = await UiClient.connect(self.path)
        self.addAsyncCleanup(client.close)
        return client

    async def hello(self, client):
        response = await client.request({"op": "hello", "v": 1})
        self.assertTrue(response["ok"], response)
        return response


class UiServerHelloSnapshotTests(UiServerTestCase):
    async def test_hello_returns_atomic_initial_cursor(self):
        emit_task(self.emitter, "task0001")
        response = await self.hello(await self.client())
        cursor = response["initial_snapshot_cursor"]
        self.assertEqual(cursor["v"], 1)
        self.assertEqual(cursor["hub_instance_id"], "hubtest01")
        self.assertEqual(cursor["journal_epoch"], 1)
        self.assertEqual(response["initial_cut_digest"],
                         self.journal.cut_entry(cursor)["cut_digest"])
        streams = {entry["stream_id"]: entry
                   for entry in response["streams"]}
        self.assertEqual(streams["task0001"]["high_watermark"], 2)
        self.assertEqual(streams[HUB_STREAM_ID]["high_watermark"], 1)

    async def test_snapshot_pages_over_socket(self):
        for task in ("task0001", "task0002", "task0003"):
            emit_task(self.emitter, task)
        client = await self.client()
        hello = await self.hello(client)
        cursor = hello["initial_snapshot_cursor"]
        states, page_cursor = [], None
        while True:
            payload = {"op": "snapshot", "snapshot_cursor": cursor}
            if page_cursor is not None:
                payload["page_cursor"] = page_cursor
            response = await client.request(payload)
            self.assertTrue(response["ok"], response)
            states.extend(response["states"])
            self.assertEqual(response["cut_digest"],
                             hello["initial_cut_digest"])
            if response["done"]:
                self.assertIsNone(response["next_page_cursor"])
                break
            page_cursor = response["next_page_cursor"]
        self.assertEqual({s["task_id"] for s in states},
                         {"task0001", "task0002", "task0003"})


class UiServerDiscoveryTests(UiServerTestCase):
    async def subscribe_all(self, client, cursor):
        response = await client.request(
            {"op": "subscribe", "stream_id": "*",
             "snapshot_cursor": cursor})
        self.assertTrue(response["ok"], response)
        return response

    async def test_after_subscribe_discovers_new_task(self):
        client = await self.client()
        hello = await self.hello(client)
        cursor = hello["initial_snapshot_cursor"]
        page = await client.request({"op": "snapshot",
                                     "snapshot_cursor": cursor})
        self.assertTrue(page["ok"])
        events_page = await client.request(
            {"op": "snapshot-events", "snapshot_cursor": cursor})
        self.assertTrue(events_page["ok"])
        await self.subscribe_all(client, cursor)
        # the task appears AFTER subscribe — no re-hello, no task subscribe
        emit_task(self.emitter, "task0099")
        frames = await client.collect(3)
        kinds = [frame["event"]["kind"] for frame in frames]
        self.assertEqual(kinds, ["task.accepted", "stream.announced",
                                 "turn.started"])
        self.assertEqual(frames[0]["event"]["seq"], 1)

    async def test_hello_only_discovers_new_task(self):
        client = await self.client()
        hello = await self.hello(client)
        await self.subscribe_all(client, hello["initial_snapshot_cursor"])
        emit_task(self.emitter, "task0098")
        frames = await client.collect(3)
        self.assertEqual(frames[0]["event"]["stream_id"], "task0098")
        self.assertEqual(frames[0]["event"]["seq"], 1)

    async def test_between_hello_and_snapshot_discovers_new_task(self):
        client = await self.client()
        hello = await self.hello(client)
        cursor = hello["initial_snapshot_cursor"]
        # the task is created BETWEEN hello and the state pages: it is not
        # in the old cut's pages (correct) and is found by subscribe-all
        emit_task(self.emitter, "task0097")
        page = await client.request({"op": "snapshot",
                                     "snapshot_cursor": cursor})
        self.assertTrue(page["ok"])
        self.assertNotIn("task0097", {s["task_id"] for s in page["states"]})
        await self.subscribe_all(client, cursor)
        frames = await client.collect(3)
        # both discovery signals arrive from seq 1: the hub announcement
        # (hub was also created after the empty cut) and the task's own
        # accepted event — order follows stream order, both are replayed
        self.assertEqual([f["event"]["kind"] for f in frames],
                         ["stream.announced", "task.accepted",
                          "turn.started"])
        self.assertEqual(frames[1]["event"]["stream_id"], "task0097")
        self.assertEqual(frames[1]["event"]["seq"], 1)
        self.assertEqual(frames[0]["event"]["data"]["task_stream_id"],
                         "task0097")

    async def test_subscribe_requires_valid_cursor_no_latest(self):
        client = await self.client()
        await self.hello(client)
        for payload in (
                {"op": "subscribe", "stream_id": "*"},
                {"op": "subscribe", "stream_id": "*",
                 "snapshot_cursor": {"v": 1, "hub_instance_id": "hubtest01",
                                     "journal_epoch": 1,
                                     "snapshot_version": 1,
                                     "snapshot_id": "nope0e0e0e0e"}},
                {"op": "subscribe", "stream_id": "*", "snapshot_cursor":
                 {"v": 1, "hub_instance_id": "hubtest01", "journal_epoch": 1,
                  "snapshot_version": 999, "snapshot_id": "whatever"}}):
            response = await client.request(payload)
            self.assertFalse(response["ok"], payload)
            self.assertIn(response["error"],
                          ("invalid_request", "cursor_superseded",
                           "cursor_expired"))


class UiServerRejectTests(UiServerTestCase):
    async def test_control_and_unknown_ops_refused(self):
        client = await self.client()
        await self.hello(client)
        hostile = [
            {"op": "delegate", "contract": {}},
            {"op": "approval.response", "decision": "accept"},
            {"op": "tool.call", "name": "Bash", "input": {}},
            {"op": "shell", "command": "cat /etc/passwd"},
            {"op": "read.path", "path": "/etc/passwd"},
            {"op": "subscribe", "stream_id": "*", "snapshot_cursor":
             None, "write": True},
            {"op": "make-coffee", "v": 1},
            {"op": "hello", "v": 2},
            {"v": 1},
        ]
        for payload in hostile:
            response = await client.request(payload)
            self.assertFalse(response["ok"], payload)
            self.assertIn(response["error"],
                          ("forbidden", "unknown_op", "unsupported_version",
                           "invalid_request"))
        # the connection survives refusals and still serves hello
        again = await client.request({"op": "hello", "v": 1})
        self.assertTrue(again["ok"])

    async def test_oversized_line_is_refused_bounded(self):
        client = await self.client()
        # just past the bounded reader limit (256 KiB + 2); written in
        # chunks so the refusal can arrive before the whole line drains
        line = (json.dumps(
            {"op": "hello", "v": 1, "junk": "x" * (260 * 1024)})
            + "\n").encode()
        read_task = asyncio.create_task(client.recv(timeout=10))
        try:
            for start in range(0, len(line), 8192):
                client.writer.write(line[start:start + 8192])
                await asyncio.wait_for(client.writer.drain(), 2)
                if read_task.done():
                    break
        except (ConnectionError, OSError, asyncio.TimeoutError):
            pass  # the bounded close may cut the remaining write short
        response = await read_task
        self.assertFalse(response["ok"])
        self.assertEqual(response["error"], "invalid_request")
        client.writer.close()  # abort any unsent remainder

    async def test_fragmented_and_malformed_ndjson_tolerated(self):
        client = await self.client()
        # a request split across two partial writes
        payload = json.dumps({"op": "hello", "v": 1})
        client.writer.write((payload[:7]).encode())
        await client.writer.drain()
        client.writer.write((payload[7:] + "\n").encode())
        await client.writer.drain()
        self.assertTrue((await client.recv())["ok"])
        # malformed JSON and non-object lines never kill the connection
        client.writer.write(b"not-json\n\n[1, 2, 3]\n\"string\"\n")
        await client.writer.drain()
        response = await client.request({"op": "hello", "v": 1})
        self.assertTrue(response["ok"])


class UiServerSecurityTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)

    def make_server(self, path):
        emitter, journal = make_parts()
        return UiEventServer(journal, socket_path=path)

    async def expect_unavailable(self, server):
        with self.assertRaises(UiTransportError) as caught:
            await server.start()
        self.assertEqual(caught.exception.code, "transport_unavailable")
        return caught.exception

    async def test_long_path_refused_before_bind(self):
        deep = self.root / ("x" * 200) / "ui-events.sock"
        self.assertIn("path", str(await self.expect_unavailable(
            self.make_server(deep))))

    async def test_symlinked_or_non_directory_parent_refused(self):
        real = secure_dir(self.root)
        link = self.root / "link-dir"
        link.symlink_to(real)
        await self.expect_unavailable(self.make_server(link / "s.sock"))
        plain = self.root / "a-file"
        plain.write_text("not a dir")
        await self.expect_unavailable(self.make_server(plain / "s.sock"))

    async def test_insecure_parent_mode_refused(self):
        loose = self.root / "loose"
        loose.mkdir(mode=0o700)
        os.chmod(loose, 0o757)  # world-writable
        await self.expect_unavailable(self.make_server(loose / "s.sock"))
        os.chmod(loose, 0o770)  # group-writable
        await self.expect_unavailable(self.make_server(loose / "s.sock"))

    async def test_existing_non_socket_path_refused(self):
        state = secure_dir(self.root)
        blocker = state / "ui-events.sock"
        blocker.write_text("occupied")
        await self.expect_unavailable(self.make_server(blocker))

    async def test_socket_and_directory_modes(self):
        state = secure_dir(self.root)
        server = self.make_server(state / "ui-events.sock")
        await server.start()
        self.addAsyncCleanup(server.close)
        mode = stat.S_IMODE(os.stat(server.socket_path).st_mode)
        self.assertEqual(mode, 0o600)
        dir_mode = stat.S_IMODE(os.stat(state).st_mode)
        self.assertEqual(dir_mode, 0o700)
        # close removes the socket; the state dir survives
        await server.close()
        self.assertFalse(server.socket_path.exists())
        self.assertTrue(state.exists())

    async def test_non_owner_parent_refused(self):
        state = secure_dir(self.root)
        with unittest.mock.patch(
                "zcode_supervisor.ui_server._euid", return_value=987654):
            await self.expect_unavailable(
                self.make_server(state / "ui-events.sock"))
        # nothing was created or replaced inside the foreign-owned dir
        self.assertEqual(list(p.name for p in state.iterdir()), [])

    async def test_unwritable_parent_refused(self):
        state = secure_dir(self.root)
        os.chmod(state, 0o500)  # owner read+execute, no write: bind fails
        try:
            await self.expect_unavailable(
                self.make_server(state / "ui-events.sock"))
        finally:
            os.chmod(state, 0o700)

    async def test_second_server_never_steals_live_socket(self):
        state = secure_dir(self.root)
        path = state / "ui-events.sock"
        first = self.make_server(path)
        await first.start()
        self.addAsyncCleanup(first.close)
        # a second server on the SAME path must fail closed: the live
        # listener is never unlinked or replaced
        with self.assertRaises(UiTransportError) as caught:
            await self.make_server(path).start()
        self.assertEqual(caught.exception.code, "transport_unavailable")
        self.assertTrue(path.exists())
        reader, writer = await asyncio.open_unix_connection(path)
        writer.write((json.dumps({"op": "hello", "v": 1}) + "\n").encode())
        await writer.drain()
        self.assertTrue(json.loads(
            await asyncio.wait_for(reader.readline(), 5))["ok"])
        writer.close()

    async def test_stale_own_socket_rebound_and_reconnect(self):
        state = secure_dir(self.root)
        path = state / "ui-events.sock"
        # a leftover socket inode OWNED BY US is replaced, not refused
        stale = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        stale.bind(str(path))
        stale.close()
        self.assertTrue(path.exists())
        server = self.make_server(path)
        await server.start()
        self.addAsyncCleanup(server.close)
        reader, writer = await asyncio.open_unix_connection(path)
        writer.write((json.dumps({"op": "hello", "v": 1}) + "\n").encode())
        await writer.drain()
        self.assertTrue(json.loads(
            await asyncio.wait_for(reader.readline(), 5))["ok"])
        writer.close()
        # a full stop/start cycle on the same path serves a reconnect too
        await server.close()
        server2 = self.make_server(path)
        await server2.start()
        self.addAsyncCleanup(server2.close)
        reader, writer = await asyncio.open_unix_connection(path)
        writer.write((json.dumps({"op": "hello", "v": 1}) + "\n").encode())
        await writer.drain()
        self.assertTrue(json.loads(
            await asyncio.wait_for(reader.readline(), 5))["ok"])
        writer.close()

    async def test_foreign_peer_rejected_same_socket(self):
        state = secure_dir(self.root)
        server = self.make_server(state / "ui-events.sock")
        await server.start()
        self.addAsyncCleanup(server.close)
        with unittest.mock.patch(
                "zcode_supervisor.ui_server.peer_euid",
                return_value=987654):
            reader, writer = await asyncio.open_unix_connection(
                server.socket_path)
            # rejected before a single response byte; EOF, not an answer
            line = await asyncio.wait_for(reader.readline(), 2)
            self.assertEqual(line, b"")
            writer.close()
        # a same-user peer still works afterwards
        reader, writer = await asyncio.open_unix_connection(
            server.socket_path)
        writer.write((json.dumps({"op": "hello", "v": 1}) + "\n").encode())
        await writer.drain()
        line = await asyncio.wait_for(reader.readline(), 5)
        self.assertTrue(json.loads(line)["ok"])
        writer.close()


class UiServerStreamingTests(UiServerTestCase):
    async def test_slow_client_reset_does_not_touch_others(self):
        # deterministic wire backpressure: the slow connection's writes are
        # gated OFF until the burst is done (no reliance on kernel buffer
        # sizes); its journal queue (256 events) must overflow into exactly
        # one reset marker, while the reading client never resets
        release_slow = asyncio.Event()
        self.server.write_gate = self._gate_slow(release_slow)
        fast = await self.client()
        hello = await self.hello(fast)
        cursor = hello["initial_snapshot_cursor"]
        slow = await UiClient.connect(self.path)
        slow_response = await slow.request({"op": "hello", "v": 1,
                                            "client": "slow"})
        self.assertTrue(slow_response["ok"])
        self.assertTrue(await slow.request(
            {"op": "subscribe", "stream_id": "*", "snapshot_cursor": cursor}))
        fast_response = await fast.request(
            {"op": "subscribe", "stream_id": "*", "snapshot_cursor": cursor})
        self.assertTrue(fast_response["ok"])
        received = []

        async def fast_reader():
            while True:
                try:
                    received.append(await fast.recv(timeout=10))
                except (ConnectionError, asyncio.TimeoutError):
                    return

        reader_task = asyncio.create_task(fast_reader())
        emit_task(self.emitter, "task0051")
        for index in range(600):  # far past the 256-event queue bound
            self.emitter.emit(
                "turn.started", "task0051", task_id="task0051",
                worker="codex", mode="app-server",
                session_id="sess-51", turn_id=f"burst-{index}")
            if index % 64 == 0:
                # realistic hub cadence: a real 10ms breath lets the
                # reading client's pump keep up; the gated one cannot
                await asyncio.sleep(0.01)
        await asyncio.sleep(0.05)
        # release the gated slow writer: it flushes its bounded remainder
        # (reset marker included) and the connection closes
        release_slow.set()
        saw_reset = False
        while True:
            try:
                frame = await slow.recv(timeout=5)
            except (ConnectionError, asyncio.TimeoutError):
                break
            if frame.get("type") == "reset_required":
                saw_reset = True
        self.assertTrue(saw_reset)
        await slow.close()
        # the healthy subscriber saw everything WITHOUT a reset
        self.assertTrue(all(frame.get("type") != "reset_required"
                            for frame in received))
        # ...and still receives the terminal event afterwards
        self.emitter.emit("turn.completed", "task0051", task_id="task0051",
                          worker="codex", mode="app-server",
                          session_id="sess-51", turn_id="turn-end",
                          status="completed")
        for _ in range(600):
            if any(frame.get("type") == "event"
                   and frame["event"].get("kind") == "turn.completed"
                   and frame["event"].get("turn_id") == "turn-end"
                   for frame in received):
                break
            await asyncio.sleep(0.02)
        reader_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await reader_task
        self.assertTrue(any(frame.get("type") == "event"
                            and frame["event"].get("kind") == "turn.completed"
                            and frame["event"].get("turn_id") == "turn-end"
                            for frame in received))

    @staticmethod
    def _gate_slow(release: asyncio.Event):
        async def gate(connection):
            if getattr(connection, "label", None) == "slow":
                await release.wait()
        return gate

    async def test_heartbeat_is_protocol_liveness_only(self):
        emitter, journal = make_parts()
        server = UiEventServer(journal,
                               socket_path=self.state / "hb.sock",
                               heartbeat_s=0.05)
        await server.start()
        self.addAsyncCleanup(server.close)
        client = await UiClient.connect(server.socket_path)
        self.addAsyncCleanup(client.close)
        hello = await client.request({"op": "hello", "v": 1})
        await client.request(
            {"op": "subscribe", "stream_id": "*",
             "snapshot_cursor": hello["initial_snapshot_cursor"]})
        beat = await client.recv(timeout=5)
        self.assertEqual(beat["type"], "heartbeat")
        self.assertIn("snapshot_version", beat)
        self.assertNotIn("process_exit_code", beat)


class UiServerRepair2Tests(UiServerTestCase):
    """repair2: wire filter enforcement, disconnect slot cleanup and
    failed-start containment — one regression per reviewer failure."""

    async def test_wire_filters_apply_to_replay_and_live(self):
        emit_task(self.emitter, "taskF")
        client = await self.client()
        await self.hello(client)
        ack = await client.request(
            {"op": "subscribe", "v": 1, "stream_id": "taskF",
             "after_seq": 0,
             "filters": {"kinds": ["task.accepted", "task.completed"]}})
        self.assertTrue(ack["ok"], ack)
        # replay: only the accepted event passes the filter
        first = await client.recv(timeout=5)
        self.assertEqual((first["event"]["kind"], first["event"]["seq"]),
                         ("task.accepted", 1))
        # live: an excluded kind is skipped, the next match keeps its seq
        self.emitter.emit("turn.started", "taskF", task_id="taskF",
                          worker="codex", mode="app-server",
                          session_id="sess-F", turn_id="live-t")
        self.emitter.emit("task.completed", "taskF", task_id="taskF",
                          worker="codex", mode="app-server",
                          status="completed", outcome="ok",
                          delivery_ok=True, duration_ms=1.0)
        second = await client.recv(timeout=5)
        self.assertEqual((second["event"]["kind"], second["event"]["seq"]),
                         ("task.completed", 4))

    async def test_wire_filters_must_be_object(self):
        client = await self.client()
        await self.hello(client)
        response = await client.request(
            {"op": "subscribe", "v": 1, "stream_id": "taskF",
             "after_seq": 0, "filters": ["not", "a", "dict"]})
        self.assertFalse(response["ok"])
        self.assertEqual(response["error"], "invalid_request")

    async def test_disconnect_releases_unfinished_page_slots(self):
        for task in ("taskD1", "taskD2", "taskD3", "taskD4"):
            emit_task(self.emitter, task)  # 4 tasks > page bound of 3
        for _ in range(3):
            client = await UiClient.connect(self.path)
            hello = await client.request({"op": "hello", "v": 1})
            page = await client.request(
                {"op": "snapshot", "v": 1,
                 "snapshot_cursor": hello["initial_snapshot_cursor"]})
            self.assertFalse(page["done"], page)
            await client.close()
        for _ in range(100):
            if not self.server.connections:
                break
            await asyncio.sleep(0.01)
        self.assertEqual(self.server.connections, set())
        self.assertEqual(self.journal._active_pages, {})

    async def test_repeated_failed_starts_leave_no_attachments(self):
        with tempfile.TemporaryDirectory() as raw:
            loose = Path(raw) / "loose"
            loose.mkdir(mode=0o700)
            os.chmod(loose, 0o757)  # deterministic policy refusal
            emitter = UiEmitter(hub_instance_id="repair2",
                                journal_epoch=1, enabled=False)
            with unittest.mock.patch.dict(
                    os.environ, {"CZS_UI_EVENTS": "1"}):
                services = [
                    await __import__(
                        "zcode_supervisor.ui_server",
                        fromlist=["start_ui_service"]).start_ui_service(
                        state_dir=loose, emitter=emitter)
                    for _ in range(2)]
            self.assertEqual(services, [None, None])
            self.assertEqual(len(emitter._subscribers), 0)
            self.assertFalse(emitter.enabled)
            self.assertEqual(list(p.name for p in loose.iterdir()), [])


class UiServiceWiringTests(unittest.IsolatedAsyncioTestCase):
    async def test_service_off_by_default_and_on_with_flag(self):
        import zcode_supervisor.ui_server as ui_server

        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        state = secure_dir(Path(temporary.name))
        # registry isolation: the enabled service registers into
        # ~/.czs/ui-registry.json by default — pin it into the temp dir
        # so this test never touches (or depends on) the real one
        registry_file = Path(temporary.name) / "ui-registry.json"
        with unittest.mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop("CZS_UI_EVENTS", None)
            os.environ["CZS_UI_REGISTRY"] = str(registry_file)
            service = await ui_server.start_ui_service(state_dir=state)
            self.assertIsNone(service)
            self.assertFalse(registry_file.exists())
        with unittest.mock.patch.dict(os.environ, {"CZS_UI_EVENTS": "1",
                                                   "CZS_UI_REGISTRY":
                                                       str(registry_file)}):
            service = await ui_server.start_ui_service(state_dir=state)
            self.assertIsNotNone(service)
            # one-click discovery: the gated service registered itself,
            # and close() removes the entry again (best-effort)
            from zcode_supervisor import ui_registry
            live = ui_registry.read_registry(path=registry_file)
            self.assertEqual([entry["socket"] for entry in live],
                             [os.path.abspath(service.socket_path)])
            try:
                self.assertTrue(service.socket_path.exists())
                # the journal shares the DEFAULT emitter's projection — the
                # identical object the hub's delegate path emits through
                emitter = ui_projection_default()
                self.assertIs(service.journal.emitter, emitter)
                accepted = emitter.emit(
                    "task.accepted", "taskW", task_id="taskW", worker="codex",
                    mode="app-server",
                    data={"contract_id": "cW", "contract_digest": HEX64,
                          "created_at": RFC, "initiator_verified": False})
                self.assertIsNotNone(accepted)
                # and the journal actually recorded the shared emit
                self.assertEqual(service.journal.high_water("taskW"), 1)
                client = await UiClient.connect(service.socket_path)
                hello = await client.request({"op": "hello", "v": 1})
                self.assertTrue(hello["ok"])
                await client.close()
            finally:
                await service.close()
            self.assertEqual(
                ui_registry.read_registry(path=registry_file), [])

    async def test_service_failure_leaves_ui_unavailable_only(self):
        # a path that cannot ever bind: the service reports None instead of
        # raising, and no fallback socket appears anywhere
        import zcode_supervisor.ui_server as ui_server

        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        state = secure_dir(Path(temporary.name))
        blocker = state / "ui-events.sock"
        blocker.write_text("occupied")
        with unittest.mock.patch.dict(os.environ, {"CZS_UI_EVENTS": "1"}):
            service = await ui_server.start_ui_service(state_dir=state)
            self.assertIsNone(service)
        self.assertEqual(sorted(p.name for p in state.iterdir()),
                         ["ui-events.sock"])


def ui_projection_default():
    from zcode_supervisor import ui_projection
    return ui_projection.default_emitter()


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
