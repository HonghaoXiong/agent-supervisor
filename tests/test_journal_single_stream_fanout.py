"""repair1 (P3.2 F-01) regression tests: single-stream fanout isolation.

F-01 (tasks/P3.2/findings.md): ``UiJournal._fanout`` forwarded OTHER
streams' live events into single-stream subscriptions (the ``None`` branch
of the ``_start`` lookup exists for subscribe-all post-cut discovery), and
``_fanout_marker`` reset single-stream subscribers on OTHER streams'
oversize losses. These tests pin the repaired shapes — and were RED on the
unfixed journal (verified in repair1 before the fix landed):

1. a single-stream subscription receives ONLY its target stream's events
   (replay backlog AND live fanout), hub included in "other streams";
2. another stream's oversize loss must NOT reset a single-stream
   subscriber; its OWN stream's oversize loss still does (bounded marker);
3. subscribe-all behavior is unchanged: post-cut streams replay from seq 1
   and any oversize loss still resets it (existing P1.1 contract).

Journal-level cases use the same fake emitters as test_ui_journal; the
wire-level case drives the REAL UiEventServer socket.
"""

import asyncio
import contextlib
import json
import tempfile
import unittest
from pathlib import Path

from zcode_supervisor.ui_events import HUB_STREAM_ID
from zcode_supervisor.ui_journal import UiJournal
from zcode_supervisor.ui_projection import UiEmitter

HEX64 = "3" * 64
RFC = "2070-01-01T00:00:00.000000Z"


def make_emitter():
    return UiEmitter(hub_instance_id="hubf01test", journal_epoch=1,
                     enabled=True)


def accepted(emitter, task_id):
    return emitter.emit(
        "task.accepted", task_id, task_id=task_id, worker="codex",
        mode="app-server",
        data={"contract_id": f"contract-{task_id}",
              "contract_digest": HEX64, "created_at": RFC,
              "initiator_verified": False})


def turn_started(emitter, task_id, session_id, turn_id):
    return emitter.emit(
        "turn.started", task_id, task_id=task_id, worker="codex",
        mode="app-server", session_id=session_id, turn_id=turn_id)


def small_journal(**overrides):
    emitter = make_emitter()
    options = {"max_events": 50, "max_bytes": 64 * 1024,
               "max_event_bytes": 4096, "page_max_records": 3,
               "page_max_bytes": 2048, "registry_max_cursors": 2,
               "registry_ttl_s": 300.0, "queue_max_events": 4,
               "queue_max_bytes": 8192}
    options.update(overrides)
    journal = UiJournal(emitter, **options)
    return emitter, journal


def emit_oversized(emitter, stream, **fields):
    """Build a valid envelope, then oversize its data past the journal cap."""
    envelope = dict(emitter.stream.build("turn.started", stream, **fields))
    envelope["data"] = {"padding": "x" * 4096}
    emitter.publish(envelope)  # seq allocated; journal drops + records loss


class SingleStreamFanoutTests(unittest.TestCase):
    def test_live_fanout_isolated_to_target_stream(self):
        # the F-01 findings repro: taskB's live event must NOT reach a
        # taskA single-stream subscriber (RED on the unfixed journal)
        emitter, journal = small_journal()
        accepted(emitter, "taskA")
        subscription = journal.subscribe_stream("taskA", 0)
        self.assertEqual([f["event"]["seq"] for f in subscription.drain()],
                         [1])  # replay backlog: only taskA, as before
        turn_started(emitter, "taskB", "sess-B", "turn-B1")
        self.assertEqual(subscription.drain(), [])  # no cross-stream leak
        self.assertFalse(subscription.closed)
        # the target's own live events still flow, seq untouched
        turn_started(emitter, "taskA", "sess-A", "turn-A1")
        live = subscription.drain()
        self.assertEqual([f["event"]["seq"] for f in live], [2])

    def test_hub_stream_not_delivered_to_single_stream_subscriber(self):
        emitter, journal = small_journal()
        accepted(emitter, "taskA")
        subscription = journal.subscribe_stream("taskA", 0)
        subscription.drain()
        emitter.emit("stream.announced", HUB_STREAM_ID, task_id="taskB",
                     worker="codex", mode="app-server",
                     data={"task_stream_id": "taskB",
                           "accepted_event_id": "hubf01test/1/taskB/1"})
        self.assertEqual(subscription.drain(), [])

    def test_after_seq_cut_still_honored_for_target(self):
        emitter, journal = small_journal()
        accepted(emitter, "taskA")
        turn_started(emitter, "taskA", "sess-A", "turn-A1")
        subscription = journal.subscribe_stream("taskA", 1)
        # replay only after seq 1 …
        self.assertEqual([f["event"]["seq"] for f in subscription.drain()],
                         [2])
        # … and live events at/below the cut never re-arrive
        turn_started(emitter, "taskA", "sess-A", "turn-A2")
        self.assertEqual([f["event"]["seq"] for f in subscription.drain()],
                         [3])

    def test_other_stream_oversize_loss_does_not_reset_single_stream(self):
        # marker shape (RED on the unfixed journal): taskHuge's loss must
        # not touch a taskA single-stream subscriber
        emitter, journal = small_journal(max_event_bytes=2048)
        accepted(emitter, "taskA")
        subscription = journal.subscribe_stream("taskA", 0)
        subscription.drain()
        emit_oversized(emitter, "taskHuge", task_id="taskHuge",
                       worker="codex", mode="app-server", session_id="s",
                       turn_id="t")
        self.assertEqual(journal.dropped_events, 1)
        self.assertFalse(subscription.reset_pending)
        self.assertFalse(subscription.closed)
        # the subscriber keeps receiving its own stream afterwards
        turn_started(emitter, "taskA", "sess-A", "turn-A1")
        frames = subscription.drain()
        self.assertEqual([f["event"]["seq"] for f in frames], [2])

    def test_target_stream_oversize_loss_resets_single_stream(self):
        # own-stream loss genuinely breaks continuity: bounded marker, the
        # existing single-stream reset semantics (pinned, unchanged)
        emitter, journal = small_journal(max_event_bytes=2048)
        accepted(emitter, "taskA")
        subscription = journal.subscribe_stream("taskA", 0)
        subscription.drain()
        emit_oversized(emitter, "taskA", task_id="taskA", worker="codex",
                       mode="app-server", session_id="s", turn_id="t")
        self.assertTrue(subscription.reset_pending)
        self.assertEqual(subscription.drain(100),
                         [{"type": "reset_required"}])

    def test_subscribe_all_post_cut_discovery_unchanged(self):
        # over-fix guard: subscribe-all still replays post-cut NEW streams
        # from seq 1 (P1.1 three-timepoint discovery contract)
        emitter, journal = small_journal()
        accepted(emitter, "taskA")
        cut = journal.new_cut()
        subscription = journal.subscribe_all(cut["snapshot_cursor"])
        accepted(emitter, "taskNew")
        turn_started(emitter, "taskNew", "sess-N", "turn-N1")
        frames = [f["event"]["seq"] for f in subscription.drain(100)
                  if f.get("type") == "event"]
        self.assertEqual(frames, [1, 2])  # taskNew from seq 1, no taskA
        self.assertFalse(subscription.closed)

    def test_subscribe_all_reset_on_any_oversize_loss_unchanged(self):
        # over-fix guard: an oversize loss anywhere still resets a
        # subscribe-all subscriber (existing P1.1 marker contract)
        emitter, journal = small_journal(max_event_bytes=2048)
        accepted(emitter, "taskA")
        cut = journal.new_cut()
        subscription = journal.subscribe_all(cut["snapshot_cursor"])
        emit_oversized(emitter, "taskHuge", task_id="taskHuge",
                       worker="codex", mode="app-server", session_id="s",
                       turn_id="t")
        self.assertTrue(subscription.reset_pending)
        self.assertEqual(subscription.drain(100),
                         [{"type": "reset_required"}])


class WireSingleStreamIsolationTests(unittest.TestCase):
    """The same isolation through the REAL UiEventServer socket."""

    def test_wire_single_stream_receives_only_target_frames(self):

        async def scenario():
            from zcode_supervisor.ui_server import UiEventServer
            with tempfile.TemporaryDirectory() as tmp:
                state = Path(tmp)
                state.chmod(0o700)
                emitter = make_emitter()
                journal = UiJournal(emitter)
                server = UiEventServer(journal, state_dir=state)
                await server.start()
                try:
                    accepted(emitter, "taskA")
                    reader, writer = await asyncio.open_unix_connection(
                        path=str(server.socket_path))
                    try:
                        async def request(payload):
                            writer.write((json.dumps(payload) + "\n")
                                         .encode())
                            await writer.drain()
                            return json.loads(
                                (await asyncio.wait_for(reader.readline(),
                                                        5.0)).decode())

                        hello = await request({"op": "hello", "v": 1,
                                               "client": "wire-f01"})
                        self.assertIn("initial_snapshot_cursor", hello)
                        ack = await request(
                            {"op": "subscribe", "v": 1, "stream_id": "taskA",
                             "after_seq": 0})
                        self.assertTrue(ack["ok"], ack)
                        replay = json.loads(
                            (await asyncio.wait_for(reader.readline(),
                                                    5.0)).decode())
                        self.assertEqual(replay["event"]["stream_id"],
                                         "taskA")
                        # other streams' live events (and hub announcements)
                        # must NOT arrive on this subscription
                        turn_started(emitter, "taskB", "sess-B", "turn-B1")
                        emitter.emit(
                            "stream.announced", HUB_STREAM_ID,
                            task_id="taskB", worker="codex",
                            mode="app-server",
                            data={"task_stream_id": "taskB",
                                  "accepted_event_id": "x/1/taskB/1"})
                        with self.assertRaises(asyncio.TimeoutError):
                            while True:
                                line = await asyncio.wait_for(
                                    reader.readline(), 0.5)
                                self.fail(
                                    f"cross-stream frame on single-stream "
                                    f"subscription: {line!r}")
                        # the target's own live event still arrives
                        turn_started(emitter, "taskA", "sess-A", "turn-A1")
                        live = json.loads(
                            (await asyncio.wait_for(reader.readline(),
                                                    5.0)).decode())
                        self.assertEqual(
                            (live["event"]["stream_id"],
                             live["event"]["seq"]),
                            ("taskA", 2))
                    finally:
                        writer.close()
                        with contextlib.suppress(ConnectionError, OSError):
                            await writer.wait_closed()
                finally:
                    await server.close()
                    journal.detach()

        asyncio.run(scenario())


if __name__ == "__main__":
    unittest.main()
