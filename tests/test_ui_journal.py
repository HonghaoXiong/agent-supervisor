"""UiJournal contract tests (P1.1): bounded retention, atomic cut points,
paged snapshot/snapshot-events, cursor registry, replay gaps and bounded
subscribe-all queues. Fake/in-memory only — no model, CLI or network."""

import asyncio
import json
import tempfile
import unittest
import unittest.mock
from pathlib import Path

from zcode_supervisor.ui_events import HUB_STREAM_ID
from zcode_supervisor.ui_projection import UiEmitter
from zcode_supervisor.ui_journal import (
    CLIENT_QUEUE_MAX_BYTES,
    CLIENT_QUEUE_MAX_EVENTS,
    JOURNAL_MAX_BYTES,
    JOURNAL_MAX_EVENTS,
    PAGE_MAX_BYTES,
    PAGE_MAX_RECORDS,
    REGISTRY_MAX_CURSORS,
    REGISTRY_TTL_S,
    UiJournal,
    reduce_stream,
)

HEX64 = "3" * 64
RFC = "2070-01-01T00:00:00.000000Z"


def make_emitter():
    return UiEmitter(hub_instance_id="hubtest01", journal_epoch=1,
                     enabled=True)


def accepted(emitter, task_id, worker="codex", mode="app-server"):
    return emitter.emit("task.accepted", task_id, task_id=task_id,
                        worker=worker, mode=mode,
                        data={"contract_id": f"contract-{task_id}",
                              "contract_digest": HEX64, "created_at": RFC,
                              "initiator_verified": False})


def announced(emitter, task_id, accepted_event_id, worker="codex",
              mode="app-server"):
    return emitter.emit("stream.announced", HUB_STREAM_ID, task_id=task_id,
                        worker=worker, mode=mode,
                        data={"task_stream_id": task_id,
                              "accepted_event_id": accepted_event_id})


def turn_started(emitter, task_id, session_id, turn_id):
    return emitter.emit("turn.started", task_id, task_id=task_id,
                        worker="codex", mode="app-server",
                        session_id=session_id, turn_id=turn_id)


def turn_completed(emitter, task_id, session_id, turn_id):
    return emitter.emit("turn.completed", task_id, task_id=task_id,
                        worker="codex", mode="app-server",
                        session_id=session_id, turn_id=turn_id,
                        status="completed")


def completed(emitter, task_id, outcome="ok"):
    return emitter.emit("task.completed", task_id, task_id=task_id,
                        worker="codex", mode="app-server", status="completed",
                        outcome=outcome, delivery_ok=True, duration_ms=1.0)


def small_journal(**overrides):
    """A journal with tiny injectable bounds (the §4.4 values are defaults)."""
    emitter = make_emitter()
    options = dict(max_events=50, max_bytes=64 * 1024, max_event_bytes=4096,
                   page_max_records=3, page_max_bytes=2048,
                   registry_max_cursors=2, registry_ttl_s=300.0,
                   queue_max_events=4, queue_max_bytes=8192)
    options.update(overrides)
    journal = UiJournal(emitter, **options)
    return emitter, journal


def seed_two_streams(emitter):
    """Two task streams with accepted events plus the hub announcement."""
    first = accepted(emitter, "task0001")
    announced(emitter, "task0001", first["event_id"])
    accepted(emitter, "task0002")
    turn_started(emitter, "task0002", "sess-0002", "turn-0002")
    turn_completed(emitter, "task0002", "sess-0002", "turn-0002")


class UiJournalBasicsTests(unittest.TestCase):
    def test_shared_projection_seq_and_high_water(self):
        emitter, journal = small_journal()
        seed_two_streams(emitter)
        # seq allocation stays in the ONE shared stream (no second source)
        self.assertEqual(emitter.stream._seqs["task0001"], 1)
        self.assertEqual(emitter.stream._seqs[HUB_STREAM_ID], 1)
        self.assertEqual(journal.high_water("task0001"), 1)
        self.assertEqual(journal.high_water(HUB_STREAM_ID), 1)
        self.assertEqual(journal.high_water("task0002"), 3)
        self.assertEqual(journal.retention_floor("task0002"), 1)
        self.assertEqual(journal.stats()["total_events"], 5)

    def test_ingest_never_raises_and_skips_malformed(self):
        emitter, journal = small_journal()
        journal.ingest({"not": "an envelope"})
        journal.ingest(None)
        journal.ingest({"v": 1, "seq": "x", "stream_id": 7, "kind": "?",
                        "event_id": "?", "observed_at": "?"})
        accepted(emitter, "task0001")
        self.assertEqual(journal.stats()["total_events"], 1)

    def test_default_bounds_match_plan_4_4(self):
        emitter = make_emitter()
        journal = UiJournal(emitter)
        self.assertEqual(journal.max_events, JOURNAL_MAX_EVENTS)
        self.assertEqual(journal.max_bytes, JOURNAL_MAX_BYTES)
        self.assertEqual(journal.page_max_records, PAGE_MAX_RECORDS)
        self.assertEqual(journal.page_max_bytes, PAGE_MAX_BYTES)
        self.assertEqual(journal.registry_max_cursors, REGISTRY_MAX_CURSORS)
        self.assertEqual(journal.registry_ttl_s, REGISTRY_TTL_S)
        self.assertEqual(journal.queue_max_events, CLIENT_QUEUE_MAX_EVENTS)
        self.assertEqual(journal.queue_max_bytes, CLIENT_QUEUE_MAX_BYTES)


class UiJournalRetentionTests(unittest.TestCase):
    def test_retention_evicts_oldest_and_reports_gap(self):
        emitter, journal = small_journal(max_events=5)
        accepted(emitter, "task0001")
        for index in range(6):
            turn_started(emitter, "task0001", "sess-0001", f"turn-{index}")
        # 7 events into a 5-event journal: the two oldest are evicted
        self.assertEqual(journal.stats()["total_events"], 5)
        self.assertEqual(journal.retention_floor("task0001"), 3)
        self.assertEqual(journal.high_water("task0001"), 7)
        # replay inside the retained window works
        page = journal.replay("task0001", 4)
        self.assertTrue(page["ok"])
        self.assertEqual([event["seq"] for event in page["events"]], [5, 6, 7])
        # replay below the floor is an explicit, precise gap — never latest
        gap = journal.replay("task0001", 0)
        self.assertFalse(gap["ok"])
        self.assertEqual(gap["error"], "gap")
        self.assertEqual(gap["gap"], {"stream_id": "task0001",
                                      "from_seq": 1, "to_seq": 2, "count": 2})

    def test_byte_bound_evicts_and_cannot_serve_old_cursor(self):
        emitter, journal = small_journal(max_bytes=1024)
        accepted(emitter, "task0001")
        turn = accepted(emitter, "task0002")
        del turn
        for index in range(20):
            turn_started(emitter, "task0001", "sess-0001", f"turn-long-{index}")
        self.assertLessEqual(journal.stats()["total_bytes"], 1024)
        # whatever was evicted, replay never silently starts at latest
        for after in range(0, journal.high_water("task0001")):
            result = journal.replay("task0001", after)
            if result["ok"]:
                self.assertEqual(
                    result["events"][0]["seq"],
                    max(after + 1, journal.retention_floor("task0001")))


class UiJournalSnapshotTests(unittest.TestCase):
    def seed_and_cut(self):
        emitter, journal = small_journal(page_max_records=1)
        seed_two_streams(emitter)
        cut = journal.new_cut()
        return emitter, journal, cut

    def test_cut_is_atomic_with_digest_and_streams(self):
        emitter, journal, cut = self.seed_and_cut()
        cursor = cut["snapshot_cursor"]
        self.assertEqual(cursor["v"], 1)
        self.assertEqual(cursor["hub_instance_id"], "hubtest01")
        self.assertEqual(cursor["journal_epoch"], 1)
        self.assertEqual(cursor["snapshot_id"],
                         journal.cut_entry(cursor)["cursor"]["snapshot_id"])
        self.assertEqual(len(cut["cut_digest"]), 64)
        entry = journal.cut_entry(cursor)
        self.assertEqual(entry["high_watermarks"],
                         {"hub": 1, "task0001": 1, "task0002": 3})

    def test_state_pages_same_cut_sequential_continuation_only(self):
        emitter, journal, cut = self.seed_and_cut()
        first = journal.snapshot(cut["snapshot_cursor"], client="c1")
        self.assertTrue(first["ok"])
        self.assertEqual(first["cut_digest"], cut["cut_digest"])
        self.assertFalse(first["done"])
        self.assertLessEqual(len(first["states"]), 2)
        second = journal.snapshot(
            cut["snapshot_cursor"], page_cursor=first["next_page_cursor"],
            client="c1")
        self.assertTrue(second["ok"])
        self.assertEqual(second["snapshot_cursor"], first["snapshot_cursor"])
        self.assertTrue(second["done"])
        self.assertIsNone(second["next_page_cursor"])
        # pages never mix later events into an old cut: a FRESH client pages
        # the same cut after a post-cut event landed — last_seq stays at the
        # cut's high-water
        turn_started(emitter, "task0001", "sess-0001", "turn-late")
        again_first = journal.snapshot(cut["snapshot_cursor"], client="c3")
        again_second = journal.snapshot(
            cut["snapshot_cursor"],
            page_cursor=again_first["next_page_cursor"], client="c3")
        self.assertTrue(again_second["done"])
        by_task = {s["task_id"]: s for s
                   in again_first["states"] + again_second["states"]}
        self.assertEqual(by_task["task0001"]["last_seq"], 1)
        # a finished pagination's continuation cannot be reused or fabricated
        bad = journal.snapshot(cut["snapshot_cursor"],
                               page_cursor=first["next_page_cursor"],
                               client="c1")
        self.assertFalse(bad["ok"])
        self.assertEqual(bad["error"], "invalid_request")
        fabricated = dict(first["next_page_cursor"])
        fabricated["after_stream"] = "zzz"
        bad2 = journal.snapshot(cut["snapshot_cursor"],
                                page_cursor=fabricated, client="c1")
        self.assertFalse(bad2["ok"])

    def test_state_pages_exclude_hub_stream_and_terminal_summary(self):
        emitter, journal, cut = self.seed_and_cut()
        completed(emitter, "task0002", outcome="ok")
        pages, cursor = [], None
        while True:
            page = journal.snapshot(cut["snapshot_cursor"],
                                    page_cursor=cursor, client="c1")
            self.assertTrue(page["ok"])
            pages.extend(page["states"])
            if page["done"]:
                break
            cursor = page["next_page_cursor"]
        self.assertEqual({s["task_id"] for s in pages},
                         {"task0001", "task0002"})
        by_task = {s["task_id"]: s for s in pages}
        # task0002 is still RUNNING at the cut (completed came later)
        self.assertEqual(by_task["task0002"]["status"], "running")

    def test_event_pages_replay_equals_state_golden(self):
        emitter, journal, cut = self.seed_and_cut()
        completed(emitter, "task0001", outcome="ok")
        # state golden across ALL pages of the ORIGINAL cut
        states, cursor = [], None
        while True:
            page = journal.snapshot(cut["snapshot_cursor"],
                                    page_cursor=cursor, client="c1")
            states.extend(page["states"])
            cursor = page["next_page_cursor"]
            if page["done"]:
                break
        # events replay of the SAME cut, from an empty reducer
        events, page_cursor = [], None
        while True:
            page = journal.snapshot_events(
                cut["snapshot_cursor"], page_cursor=page_cursor,
                client="c1")
            self.assertTrue(page["ok"])
            self.assertEqual(page["cut_digest"], cut["cut_digest"])
            events.extend(page["events"])
            if page["done"]:
                break
            page_cursor = page["next_page_cursor"]
        grouped = {}
        for event in events:
            grouped.setdefault(event["stream_id"], []).append(event)
        golden = {task_id: reduce_stream(stream_events)
                  for task_id, stream_events in grouped.items()
                  if task_id != HUB_STREAM_ID}
        self.assertEqual({s["task_id"]: s for s in states}, golden)
        # every stream in the cut map reached exactly its high-water
        entry = journal.cut_entry(cut["snapshot_cursor"])
        for stream_id, seqs in grouped.items():
            self.assertEqual(seqs[-1]["seq"],
                             entry["high_watermarks"][stream_id])

    def test_new_stream_during_paging_not_in_old_pages(self):
        emitter, journal, cut = self.seed_and_cut()
        first = journal.snapshot(cut["snapshot_cursor"], client="c1")
        # a THIRD task stream appears mid-pagination
        third = accepted(emitter, "task0003")
        announced(emitter, "task0003", third["event_id"])
        cursor = first["next_page_cursor"]
        while cursor is not None:
            page = journal.snapshot(cut["snapshot_cursor"],
                                    page_cursor=cursor, client="c1")
            self.assertTrue(page["ok"])
            self.assertNotIn("task0003",
                             {s["task_id"] for s in page["states"]})
            cursor = page["next_page_cursor"]
        events_cursor = None
        while True:
            page = journal.snapshot_events(
                cut["snapshot_cursor"], page_cursor=events_cursor,
                client="c1")
            self.assertNotIn("task0003",
                             {e["stream_id"] for e in page["events"]})
            if page["done"]:
                break
            events_cursor = page["next_page_cursor"]
        # and a NEW cut sees it, with a different digest
        later = journal.new_cut()
        self.assertNotEqual(later["cut_digest"], cut["cut_digest"])
        self.assertEqual(
            journal.cut_entry(later["snapshot_cursor"])["high_watermarks"][
                "task0003"], 1)

    def _small_cut_journal(self):
        emitter, journal = small_journal(page_max_records=1)
        seed_two_streams(emitter)
        return emitter, journal, journal.new_cut()

    def test_page_limits_per_client(self):
        _emitter, journal, cut = self._small_cut_journal()
        other = journal.new_cut()
        # ONE unfinished pagination slot per client, shared across state AND
        # events pages: a second kind or a second cut is an explicit refusal
        first = journal.snapshot(cut["snapshot_cursor"], client="c1")
        self.assertTrue(first["ok"])
        self.assertFalse(first["done"])
        conflict = journal.snapshot(other["snapshot_cursor"], client="c1")
        self.assertFalse(conflict["ok"])
        self.assertEqual(conflict["error"], "page_limit")
        clash = journal.snapshot_events(cut["snapshot_cursor"], client="c1")
        self.assertFalse(clash["ok"])
        self.assertEqual(clash["error"], "page_limit")
        # a different client paginates in parallel
        parallel = journal.snapshot(cut["snapshot_cursor"], client="c2")
        self.assertTrue(parallel["ok"])
        while not parallel["done"]:
            parallel = journal.snapshot(cut["snapshot_cursor"],
                                        page_cursor=parallel[
                                            "next_page_cursor"],
                                        client="c2")
        # once c1's state pagination finishes, its events pages work
        page = first
        while not page["done"]:
            page = journal.snapshot(cut["snapshot_cursor"],
                                    page_cursor=page["next_page_cursor"],
                                    client="c1")
        events = journal.snapshot_events(cut["snapshot_cursor"], client="c1")
        self.assertTrue(events["ok"])


class UiJournalRegistryTests(unittest.TestCase):
    def test_ttl_expiry_is_cursor_expired(self):
        emitter, journal = small_journal(registry_ttl_s=0.0)
        seed_two_streams(emitter)
        cut = journal.new_cut()
        page = journal.snapshot(cut["snapshot_cursor"], client="c1")
        self.assertFalse(page["ok"])
        self.assertEqual(page["error"], "cursor_expired")

    def test_lru_eviction_is_cursor_superseded(self):
        emitter, journal = small_journal(registry_max_cursors=2)
        seed_two_streams(emitter)
        first = journal.new_cut()
        journal.new_cut()
        journal.new_cut()  # evicts `first` (capacity 2)
        page = journal.snapshot(first["snapshot_cursor"], client="c1")
        self.assertFalse(page["ok"])
        self.assertEqual(page["error"], "cursor_superseded")

    def test_unknown_or_tampered_cursor_refused(self):
        emitter, journal = small_journal()
        seed_two_streams(emitter)
        cut = journal.new_cut()
        unknown = dict(cut["snapshot_cursor"])
        unknown["snapshot_id"] = "deadbeefdeadbeef"
        result = journal.snapshot(unknown, client="c1")
        self.assertFalse(result["ok"])
        self.assertEqual(result["error"], "invalid_request")
        tampered = dict(cut["snapshot_cursor"])
        tampered["journal_epoch"] = 99
        result2 = journal.snapshot(tampered, client="c1")
        self.assertFalse(result2["ok"])


class UiJournalSubscribeAllTests(unittest.TestCase):
    def test_streams_in_cut_continue_new_streams_from_seq1(self):
        emitter, journal = small_journal()
        first = accepted(emitter, "task0001")
        announced(emitter, "task0001", first["event_id"])
        cut = journal.new_cut()
        subscription = journal.subscribe_all(cut["snapshot_cursor"])
        # pre-cut events are NOT delivered for streams in the map...
        self.assertEqual(subscription.drain(100), [])
        # ...but post-cut ones are
        turn_started(emitter, "task0001", "sess-0001", "turn-0001")
        frames = subscription.drain(100)
        self.assertEqual([f["event"]["seq"] for f in frames], [2])
        # a NEW stream (created after the cut) replays from seq 1 — including
        # events emitted between the cut and the subscribe registration
        third = accepted(emitter, "task0003")
        announced(emitter, "task0003", third["event_id"])
        frames = subscription.drain(100)
        self.assertEqual([f["event"]["stream_id"] for f in frames],
                         ["task0003", "hub"])
        self.assertEqual([f["event"]["seq"] for f in frames], [1, 2])
        self.assertEqual(frames[-1]["event"]["kind"], "stream.announced")

    def test_stream_created_between_cut_and_subscribe_replays_from_seq1(self):
        # the contract case: snapshot cut FIRST, the task appears SECOND,
        # subscribe-all THIRD — registration must replay the new stream from
        # seq 1 up to the current high-water, then continue live
        emitter, journal = small_journal()
        first = accepted(emitter, "task0001")
        announced(emitter, "task0001", first["event_id"])
        cut = journal.new_cut()
        # the task stream is created AND progressed AFTER the cut, BEFORE
        # any subscription exists
        accepted(emitter, "task0007")
        turn_started(emitter, "task0007", "sess-0007", "turn-0007")
        turn_completed(emitter, "task0007", "sess-0007", "turn-0007")
        subscription = journal.subscribe_all(cut["snapshot_cursor"])
        backfill = subscription.drain(100)
        self.assertEqual(
            [(f["event"]["stream_id"], f["event"]["seq"])
             for f in backfill],
            [("task0007", 1), ("task0007", 2), ("task0007", 3)])
        # live continuation after the backfill
        emitter.emit("task.completed", "task0007", task_id="task0007",
                     worker="codex", mode="app-server", status="completed",
                     outcome="ok", delivery_ok=True, duration_ms=2.0)
        live = subscription.drain(100)
        self.assertEqual([f["event"]["seq"] for f in live], [4])

    def test_hello_only_and_after_subscribe_timepoints(self):
        emitter, journal = small_journal()
        cut = journal.hello()
        subscription = journal.subscribe_all(cut["initial_snapshot_cursor"])
        first = accepted(emitter, "task0009")
        announced(emitter, "task0009", first["event_id"])
        turn_started(emitter, "task0009", "sess-0009", "turn-0009")
        frames = subscription.drain(100)
        self.assertEqual([f["event"]["kind"] for f in frames],
                         ["task.accepted", "stream.announced", "turn.started"])
        self.assertEqual(frames[0]["event"]["seq"], 1)

    def test_subscribe_all_reset_when_new_stream_seq1_evicted(self):
        emitter, journal = small_journal(max_events=4)
        first = accepted(emitter, "task0001")
        announced(emitter, "task0001", first["event_id"])
        cut = journal.new_cut()
        subscription = journal.subscribe_all(cut["snapshot_cursor"])
        # a post-cut stream whose seq 1..k get evicted before subscribe-all
        # can replay them must reset, never start mid-stream
        third = accepted(emitter, "task0003")
        announced(emitter, "task0003", third["event_id"])
        for index in range(6):
            turn_started(emitter, "task0003", "sess-0003", f"t{index}")
        self.assertTrue(subscription.reset_pending)
        self.assertTrue(subscription.closed)

    def test_slow_subscription_bounded_queue_and_reset(self):
        emitter, journal = small_journal(queue_max_events=4)
        first = accepted(emitter, "task0001")
        announced(emitter, "task0001", first["event_id"])
        cut = journal.new_cut()
        subscription = journal.subscribe_all(cut["snapshot_cursor"])
        for index in range(30):  # burst far past the queue bound
            turn_started(emitter, "task0001", "sess-0001", f"burst-{index}")
        self.assertLessEqual(subscription.pending_events, 5)  # bound + reset
        self.assertLessEqual(subscription.pending_bytes,
                             journal.queue_max_bytes + 4096)
        self.assertTrue(subscription.reset_pending)
        self.assertTrue(subscription.closed)
        # the journal itself stays healthy for a fresh subscriber
        fresh = journal.subscribe_all(journal.new_cut()["snapshot_cursor"])
        self.assertFalse(fresh.closed)

    def test_slow_subscription_byte_bound_and_reset(self):
        emitter, journal = small_journal(queue_max_bytes=2048)
        first = accepted(emitter, "task0001")
        announced(emitter, "task0001", first["event_id"])
        cut = journal.new_cut()
        subscription = journal.subscribe_all(cut["snapshot_cursor"])
        # events sized to trip the BYTE ceiling well before the event count
        for index in range(40):
            emitter.emit("turn.started", "task0001", task_id="task0001",
                         worker="codex", mode="app-server",
                         session_id="sess-0001", turn_id=f"bytey-{index}")
        self.assertTrue(subscription.reset_pending)
        self.assertTrue(subscription.closed)
        self.assertLessEqual(subscription.pending_bytes, 2048 + 512)
        self.assertLessEqual(subscription.pending_events,
                             journal.queue_max_events + 1)

    def test_oversized_event_dropped_with_marker_not_payload(self):
        emitter, journal = small_journal(max_event_bytes=256)
        first = accepted(emitter, "task0001")
        del first
        cut = journal.new_cut()
        subscription = journal.subscribe_all(cut["snapshot_cursor"])
        # a fully valid envelope that simply exceeds the journal's event
        # byte ceiling: stored nowhere, forwarded nowhere, lost EXPLICITLY
        huge = accepted(emitter, "task0002")
        self.assertIsNotNone(huge)
        replay = journal.replay("task0002", 0)
        self.assertFalse(replay["ok"])
        self.assertEqual(replay["error"], "gap")
        self.assertEqual(replay["gap"],
                         {"stream_id": "task0002", "from_seq": 1,
                          "to_seq": 1, "count": 1})
        frames = subscription.drain(100)
        # the oversized payload never reaches any queue...
        self.assertFalse(any(huge["event_id"] in json.dumps(f)
                             for f in frames))
        # ...but a bounded reset marker does
        self.assertTrue(subscription.reset_pending)


class ReduceStreamTests(unittest.TestCase):
    def test_reduce_orders_state_without_rollback(self):
        emitter, journal = small_journal()
        accepted(emitter, "task0001")
        turn_started(emitter, "task0001", "sess-0001", "turn-0001")
        completed(emitter, "task0001", outcome="ok")
        state = reduce_stream(
            [e for e in journal.replay("task0001", 0)["events"]])
        self.assertEqual(state["task_id"], "task0001")
        self.assertEqual(state["status"], "completed")
        self.assertEqual(state["outcome"], "ok")
        self.assertEqual(state["session_id"], "sess-0001")
        # re-applying an earlier event must not roll the state back
        replayed = journal.replay("task0001", 0)["events"]
        stable = reduce_stream(replayed + replayed[:1])
        self.assertEqual(stable["status"], "completed")

    def test_reduce_empty_is_none(self):
        self.assertIsNone(reduce_stream([]))


class UiJournalLoopTests(unittest.TestCase):
    def test_ingest_is_non_blocking_under_burst(self):
        emitter, journal = small_journal()
        accepted(emitter, "task0001")

        async def go():
            loop = asyncio.get_running_loop()
            started = loop.time()
            for index in range(2000):
                turn_started(emitter, "task0001", "sess-0001", f"t{index}")
                if index % 50 == 0:
                    await asyncio.sleep(0)  # the loop stays responsive
            return loop.time() - started

        elapsed = asyncio.run(go())
        self.assertLess(elapsed, 5.0)
        self.assertLessEqual(journal.stats()["total_events"], 50)


class UiJournalRepair1Tests(unittest.TestCase):
    """repair1 (2026-10-27): forged continuations, replay→live handoff,
    live filters, oversize lost streams before/after a cut, and cut-range
    resets — each pinned by the independent spec probes that rejected the
    first candidate."""

    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)

    @staticmethod
    def make_journal(**options):
        emitter, journal = small_journal(**options)
        return emitter, journal

    def test_forged_state_continuation_cannot_skip_streams(self):
        emitter, journal = self.make_journal(page_max_records=1)
        for task in ("taskA", "taskB", "taskC"):
            accepted(emitter, task)
        cut = journal.new_cut()
        first = journal.snapshot(cut["snapshot_cursor"], client="forger")
        self.assertEqual([s["task_id"] for s in first["states"]], ["taskA"])
        forged = dict(first["next_page_cursor"])
        forged["after_stream"] = "taskB"  # taskB was never delivered
        response = journal.snapshot(cut["snapshot_cursor"], forged,
                                    client="forger")
        self.assertFalse(response["ok"])
        self.assertEqual(response["error"], "invalid_request")
        # the honest cursor still works afterwards
        honest = journal.snapshot(
            cut["snapshot_cursor"], first["next_page_cursor"],
            client="forger")
        self.assertTrue(honest["ok"])

    def test_forged_events_continuation_cannot_skip_seq(self):
        emitter, journal = self.make_journal(page_max_records=1)
        accepted(emitter, "taskA")
        turn_started(emitter, "taskA", "sess-A", "turn-A1")
        turn_started(emitter, "taskA", "sess-A", "turn-A2")
        cut = journal.new_cut()
        first = journal.snapshot_events(cut["snapshot_cursor"],
                                        client="forger")
        self.assertEqual([e["seq"] for e in first["events"]], [1])
        forged = dict(first["next_page_cursor"])
        forged["after_seq"] = 2  # seq 2 was never delivered
        response = journal.snapshot_events(cut["snapshot_cursor"], forged,
                                           client="forger")
        self.assertFalse(response["ok"])
        self.assertEqual(response["error"], "invalid_request")

    def test_single_stream_replays_then_follows_live(self):
        emitter, journal = self.make_journal()
        accepted(emitter, "taskA")
        subscription = journal.subscribe_stream("taskA", 0)
        self.assertEqual([f["event"]["seq"] for f in subscription.drain()],
                         [1])  # replay
        turn_started(emitter, "taskA", "sess-A", "turn-A1")
        live = subscription.drain()
        self.assertEqual([f["event"]["seq"] for f in live], [2])  # live
        self.assertFalse(subscription.closed)

    def test_filters_apply_to_replay_and_live_without_renumbering(self):
        emitter, journal = self.make_journal()
        accepted(emitter, "taskA")
        turn_started(emitter, "taskA", "sess-A", "turn-A1")
        subscription = journal.subscribe_stream(
            "taskA", 0, filters={"kinds": ("task.accepted", "task.completed")})
        replay = subscription.drain(100)
        self.assertEqual([f["event"]["kind"] for f in replay],
                         ["task.accepted"])  # replay filtered
        turn_started(emitter, "taskA", "sess-A", "turn-A2")
        emitter.emit("task.completed", "taskA", task_id="taskA",
                     worker="codex", mode="app-server", status="completed",
                     outcome="ok", delivery_ok=True, duration_ms=1.0)
        live = subscription.drain(100)
        self.assertEqual([f["event"]["kind"] for f in live],
                         ["task.completed"])  # live filtered, seq untouched
        self.assertEqual(live[0]["event"]["seq"], 4)

    def test_oversize_only_stream_loss_observable_and_first_cut_resets(self):
        # repair3 semantics: an oversize-only stream is NOT pinned by cuts.
        # An EMPTY cut leaves the loss unacknowledged — the subscriber sees
        # a bounded gap FRAME (never a silent seq-2 join). A NON-EMPTY cut
        # over an unacknowledged oversize-only loss is TAINTED: born-reset,
        # and acknowledged so the NEXT fresh cut is a clean baseline.
        emitter, journal = self.make_journal(max_event_bytes=2048)

        def emit_oversized(stream, **fields):
            envelope = dict(emitter.stream.build(
                "turn.started", stream, **fields))
            envelope["data"] = {"padding": "x" * 4096}
            emitter.publish(envelope)  # seq allocated, journal drops it

        emit_oversized("taskHuge", task_id="taskHuge", worker="codex",
                       mode="app-server", session_id="s", turn_id="t")
        self.assertEqual(journal.dropped_events, 1)
        # (a) empty-journal cut: loss observable via a gap frame; the
        # subscription itself stays usable for live events
        cut = journal.new_cut()
        self.assertNotIn("taskHuge", journal.cut_entry(
            cut["snapshot_cursor"])["high_watermarks"])
        subscription = journal.subscribe_all(cut["snapshot_cursor"])
        frames = subscription.drain(100)
        gap_frames = [f for f in frames if f.get("type") == "gap"]
        self.assertEqual(len(gap_frames), 1)
        self.assertEqual(gap_frames[0]["gap"]["stream_id"], "taskHuge")
        self.assertFalse(subscription.reset_pending)
        # (b) a live stream plus a NEW oversize-only loss: the first
        # non-empty cut is tainted (born-reset)…
        accepted(emitter, "taskAlive")
        emit_oversized("taskHuge2", task_id="taskHuge2", worker="codex",
                       mode="app-server", session_id="s", turn_id="t")
        cut2 = journal.new_cut()
        subscription2 = journal.subscribe_all(cut2["snapshot_cursor"])
        self.assertTrue(subscription2.reset_pending)
        self.assertEqual(subscription2.drain(100),
                         [{"type": "reset_required"}])
        # …and a later seq can never silently look like a complete stream
        turn_started(emitter, "taskHuge2", "sess-H2", "turn-H2")
        self.assertEqual(subscription2.drain(100), [])
        # (c) the tainted cut stays tainted forever (old-cut integrity);
        # the follow-up loss is carried by the now-live holey stream
        state2 = journal.snapshot(cut2["snapshot_cursor"], client="s2")
        self.assertEqual((state2["ok"], state2.get("error")),
                         (False, "reset_required"))

    def test_oversize_after_cut_marks_existing_subscribers(self):
        emitter, journal = self.make_journal(max_event_bytes=256)
        cut = journal.new_cut()
        subscription = journal.subscribe_all(cut["snapshot_cursor"])
        huge = accepted(emitter, "taskHuge")
        self.assertIsNotNone(huge)
        self.assertEqual(journal.high_water("taskHuge"), 1)
        self.assertEqual(journal.dropped_events, 1)
        frames = subscription.drain(100)
        self.assertEqual(frames, [{"type": "reset_required"}])
        self.assertNotIn("contract-taskHuge", json.dumps(frames))

    def test_fully_evicted_cut_stream_resets_both_page_kinds(self):
        emitter, journal = self.make_journal(max_events=2)
        accepted(emitter, "taskA")
        cut = journal.new_cut()  # promises taskA:1
        accepted(emitter, "taskB")
        turn_started(emitter, "taskB", "sess-B", "turn-B1")
        # global retention (2 events) evicted all of taskA
        state = journal.snapshot(cut["snapshot_cursor"], client="s")
        events = journal.snapshot_events(cut["snapshot_cursor"], client="e")
        self.assertFalse(state["ok"])
        self.assertEqual(state["error"], "reset_required")
        self.assertFalse(events["ok"])
        self.assertEqual(events["error"], "reset_required")

    def test_normal_new_stream_after_cut_still_autodiscovered(self):
        emitter, journal = self.make_journal()
        first = accepted(emitter, "taskOld")
        announced(emitter, "taskOld", first["event_id"])
        cut = journal.new_cut()
        subscription = journal.subscribe_all(cut["snapshot_cursor"])
        third = accepted(emitter, "taskNew")
        announced(emitter, "taskNew", third["event_id"])
        frames = subscription.drain(100)
        self.assertEqual([f["event"]["stream_id"] for f in frames],
                         ["taskNew", "hub"])
        self.assertEqual(frames[0]["event"]["seq"], 1)
        self.assertFalse(subscription.reset_pending)


class UiJournalRepair2Tests(unittest.TestCase):
    """repair2 (2026-10-27): every reviewer failure pinned — interior and
    oversize-only loss on all read paths, fresh-cut recovery, TTL/LRU slot
    release, byte accounting."""

    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)

    def test_interior_oversize_hole_gaps_replay_and_resets_reads(self):
        emitter, journal = small_journal(max_event_bytes=2048)

        def emit_oversized(stream, kind="turn.started", **fields):
            envelope = dict(emitter.stream.build(kind, stream, **fields))
            envelope["data"] = {"padding": "x" * 4096}
            emitter.publish(envelope)  # seq allocated, journal drops it

        accepted(emitter, "taskA")
        emit_oversized("taskA", task_id="taskA", worker="codex",
                       mode="app-server", session_id="s", turn_id="t")
        completed(emitter, "taskA", outcome="ok")
        self.assertEqual(journal.high_water("taskA"), 3)
        self.assertEqual(journal.retention_floor("taskA"), 1)
        self.assertEqual(journal.dropped_events, 1)
        # single-stream replay: explicit gap naming the lost seq
        replay = journal.replay("taskA", 0)
        self.assertFalse(replay["ok"])
        self.assertEqual(replay["error"], "gap")
        self.assertEqual(replay["gap"]["from_seq"], 2)
        self.assertEqual(replay["gap"]["count"], 1)
        # state/events pages and subscribe-all on a cut pinning the stream
        cut = journal.new_cut()
        state = journal.snapshot(cut["snapshot_cursor"], client="s")
        events = journal.snapshot_events(cut["snapshot_cursor"], client="e")
        self.assertEqual((state["ok"], state.get("error")),
                         (False, "reset_required"))
        self.assertEqual((events["ok"], events.get("error")),
                         (False, "reset_required"))
        subscription = journal.subscribe_all(cut["snapshot_cursor"])
        self.assertTrue(subscription.reset_pending)
        # a new stream created after the cut with the same hole resets too
        cut2 = journal.new_cut()
        accepted(emitter, "taskB")
        emit_oversized("taskB", task_id="taskB", worker="codex",
                       mode="app-server", session_id="s", turn_id="t")
        completed(emitter, "taskB", outcome="ok")
        subscription2 = journal.subscribe_all(cut2["snapshot_cursor"])
        self.assertTrue(subscription2.reset_pending)

    def test_partial_prefix_eviction_after_cut_resets_pages(self):
        emitter, journal = small_journal(max_events=3)
        accepted(emitter, "taskA")
        turn_started(emitter, "taskA", "s", "t1")
        turn_completed(emitter, "taskA", "s", "t1")
        cut = journal.new_cut()  # promises taskA 1..3
        accepted(emitter, "taskB")  # evicts only taskA seq 1
        self.assertEqual(journal.retention_floor("taskA"), 2)
        state = journal.snapshot(cut["snapshot_cursor"], client="s")
        events = journal.snapshot_events(cut["snapshot_cursor"], client="e")
        self.assertEqual((state["ok"], state.get("error")),
                         (False, "reset_required"))
        self.assertEqual((events["ok"], events.get("error")),
                         (False, "reset_required"))

    def test_consecutive_fresh_cuts_recover_after_full_eviction(self):
        emitter, journal = small_journal(max_events=1)
        accepted(emitter, "taskOld")
        accepted(emitter, "taskNew")  # evicts all of taskOld
        for index in range(2):
            cut = journal.new_cut()
            state = journal.snapshot(cut["snapshot_cursor"],
                                     client=f"s{index}")
            events = journal.snapshot_events(cut["snapshot_cursor"],
                                             client=f"e{index}")
            subscription = journal.subscribe_all(cut["snapshot_cursor"])
            self.assertTrue(state["ok"], state)
            self.assertTrue(events["ok"], events)
            self.assertFalse(subscription.reset_pending)
            self.assertNotIn("taskOld", journal.cut_entry(
                cut["snapshot_cursor"])["high_watermarks"])

    def test_ttl_or_lru_failure_releases_slot_for_fresh_recut(self):
        emitter, journal = small_journal(page_max_records=1)
        accepted(emitter, "taskA")
        accepted(emitter, "taskB")
        with unittest.mock.patch(
                "zcode_supervisor.ui_journal.time.monotonic",
                return_value=100.0):
            cut = journal.new_cut()
            first = journal.snapshot(cut["snapshot_cursor"],
                                     client="ttl")
            self.assertFalse(first["done"])
        with unittest.mock.patch(
                "zcode_supervisor.ui_journal.time.monotonic",
                return_value=500.0):
            expired = journal.snapshot(
                cut["snapshot_cursor"], first["next_page_cursor"],
                client="ttl")
            self.assertEqual(expired["error"], "cursor_expired")
            fresh = journal.new_cut()["snapshot_cursor"]
            recovered = journal.snapshot(fresh, client="ttl")
        self.assertTrue(recovered["ok"], recovered)

    def test_reset_marker_byte_accounting_never_negative(self):
        emitter, journal = small_journal(queue_max_events=2)
        cut = journal.new_cut()
        subscription = journal.subscribe_all(cut["snapshot_cursor"])
        for index in range(3):
            turn_started(emitter, "taskA", "s", f"t{index}")
        self.assertTrue(subscription.closed)
        frames = subscription.drain(20)
        self.assertEqual(frames[-1], {"type": "reset_required"})
        self.assertEqual(subscription.pending_events, 0)
        self.assertEqual(subscription.pending_bytes, 0)
        self.assertGreaterEqual(subscription.pending_bytes, 0)


class UiJournalRepair3Tests(unittest.TestCase):
    """repair3 (2026-10-27): per-cut loss evidence, fresh-cut recovery
    state machine, exact first-run gaps, late single-stream loss and
    anonymous slot release — the six dispatch-required regressions."""

    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)

    def test_first_tainted_cut_resets_then_fresh_cut_recovers(self):
        emitter, journal = small_journal(max_events=3)
        accepted(emitter, "taskA")
        accepted(emitter, "taskB")
        # partial prefix retention: taskA seq1 evicted after the first cut
        cut1 = journal.new_cut()
        accepted(emitter, "taskC")
        turn_started(emitter, "taskC", "s", "t")  # 4 events: evicts taskA:1
        self.assertEqual(journal.retention_floor("taskA"), 2)
        first = journal.snapshot(cut1["snapshot_cursor"], client="c")
        self.assertEqual((first["ok"], first.get("error")),
                         (False, "reset_required"))  # old cut keeps resetting
        again = journal.snapshot(cut1["snapshot_cursor"], client="c2")
        self.assertEqual((again["ok"], again.get("error")),
                         (False, "reset_required"))
        # the FRESH cut rebases on the current floors and serves cleanly
        cut2 = journal.new_cut()
        state = journal.snapshot(cut2["snapshot_cursor"], client="c")
        self.assertTrue(state["ok"], state)
        events = journal.snapshot_events(cut2["snapshot_cursor"],
                                         client="e")
        self.assertTrue(events["ok"], events)
        subscription = journal.subscribe_all(cut2["snapshot_cursor"])
        self.assertFalse(subscription.reset_pending)

    def test_old_cut_survives_lost_cap_trim(self):
        emitter, journal = small_journal(max_event_bytes=2048)
        accepted(emitter, "taskA")

        def emit_oversized(stream, **fields):
            envelope = dict(emitter.stream.build(
                "turn.started", stream, **fields))
            envelope["data"] = {"padding": "x" * 4096}
            emitter.publish(envelope)

        emit_oversized("taskA", task_id="taskA", worker="codex",
                       mode="app-server", session_id="s", turn_id="t")
        cut = journal.new_cut()  # pins taskA with the lost seq-2 snapshot
        for index in range(3, 1027):  # cap-trim seq 2 out of _lost
            emit_oversized("taskA", task_id="taskA", worker="codex",
                           mode="app-server", session_id="s",
                           turn_id=f"t{index}")
        self.assertNotIn(2, journal._lost.get("taskA", ()))
        # the OLD cut keeps its own loss evidence: reset forever
        state = journal.snapshot(cut["snapshot_cursor"], client="s")
        events = journal.snapshot_events(cut["snapshot_cursor"], client="e")
        self.assertEqual((state["ok"], state.get("error")),
                         (False, "reset_required"))
        self.assertEqual((events["ok"], events.get("error")),
                         (False, "reset_required"))
        # replay below the trim watermark degrades to an honest reset
        replay = journal.replay("taskA", 0)
        self.assertFalse(replay["ok"])
        self.assertIn(replay["error"], ("gap", "reset_required"))
        if replay["error"] == "gap":
            self.assertGreaterEqual(replay["gap"]["from_seq"], 2)

    def test_late_single_stream_interior_loss_direct(self):
        emitter, journal = small_journal(max_event_bytes=2048)
        accepted(emitter, "taskA")

        def emit_oversized(stream, **fields):
            envelope = dict(emitter.stream.build(
                "turn.started", stream, **fields))
            envelope["data"] = {"padding": "x" * 4096}
            emitter.publish(envelope)

        emit_oversized("taskA", task_id="taskA", worker="codex",
                       mode="app-server", session_id="s", turn_id="t")
        completed(emitter, "taskA", outcome="ok")
        subscription = journal.subscribe_stream("taskA", 0)
        self.assertTrue(subscription.reset_pending)
        self.assertEqual(subscription.drain(100),
                         [{"type": "reset_required"}])
        # past the hole, delivery resumes without reset
        resumed = journal.subscribe_stream("taskA", 2)
        self.assertFalse(resumed.reset_pending)
        self.assertEqual([f["event"]["seq"] for f in resumed.drain()], [3])
        # filters never change loss detection
        filtered = journal.subscribe_stream(
            "taskA", 0, filters={"kinds": ("task.completed",)})
        self.assertTrue(filtered.reset_pending)

    def test_noncontiguous_gap_reports_first_run_only(self):
        emitter, journal = small_journal(max_event_bytes=2048)
        accepted(emitter, "taskA")

        def emit_oversized(stream, **fields):
            envelope = dict(emitter.stream.build(
                "turn.started", stream, **fields))
            envelope["data"] = {"padding": "x" * 4096}
            emitter.publish(envelope)

        emit_oversized("taskA", task_id="taskA", worker="codex",
                       mode="app-server", session_id="s", turn_id="t1")
        turn_completed(emitter, "taskA", "s", "t1")  # seq 3 retained
        emit_oversized("taskA", task_id="taskA", worker="codex",
                       mode="app-server", session_id="s", turn_id="t2")
        replay = journal.replay("taskA", 0)
        self.assertFalse(replay["ok"])
        self.assertEqual(replay["gap"],
                         {"stream_id": "taskA", "from_seq": 2,
                          "to_seq": 2, "count": 1})

    def test_anonymous_ttl_error_releases_slot(self):
        emitter, journal = small_journal(page_max_records=1,
                                         registry_ttl_s=300)
        accepted(emitter, "taskA")
        accepted(emitter, "taskB")
        with unittest.mock.patch(
                "zcode_supervisor.ui_journal.time.monotonic",
                return_value=100.0):
            cut = journal.new_cut()
            first = journal.snapshot(cut["snapshot_cursor"])  # anonymous
            self.assertFalse(first["done"])
        with unittest.mock.patch(
                "zcode_supervisor.ui_journal.time.monotonic",
                return_value=500.0):
            expired = journal.snapshot(
                cut["snapshot_cursor"], first["next_page_cursor"])
            self.assertEqual(expired["error"], "cursor_expired")
            fresh = journal.new_cut()["snapshot_cursor"]
            recovered = journal.snapshot(fresh)  # anonymous again
        self.assertTrue(recovered["ok"], recovered)

    def test_anonymous_lru_error_releases_slot(self):
        emitter, journal = small_journal(page_max_records=1,
                                         registry_max_cursors=2)
        accepted(emitter, "taskA")
        accepted(emitter, "taskB")
        cut = journal.new_cut()
        first = journal.snapshot(cut["snapshot_cursor"])
        self.assertFalse(first["done"])
        journal.new_cut()
        journal.new_cut()
        superseded = journal.snapshot(
            cut["snapshot_cursor"], first["next_page_cursor"])
        self.assertEqual(superseded["error"], "cursor_superseded")
        fresh = journal.new_cut()["snapshot_cursor"]
        recovered = journal.snapshot(fresh)
        self.assertTrue(recovered["ok"], recovered)


class UiJournalRepair4Tests(unittest.TestCase):
    """repair4 (2026-10-27): per-cut isolation, fresh-cut recovery past
    acknowledged losses, post-cut retired-stream discovery and bounded
    lifecycle metadata — one regression per combined reviewer case."""

    HEX = "3" * 64
    RFC = "2070-01-01T00:00:00.000000Z"

    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)

    @staticmethod
    def make_journal(**options):
        emitter, journal = small_journal(**options)
        return emitter, journal

    def test_old_empty_cut_gap_isolated_from_later_acknowledgment(self):
        emitter, journal = self.make_journal(max_event_bytes=2048)

        def emit_oversized(stream, **fields):
            envelope = dict(emitter.stream.build(
                "turn.started", stream, **fields))
            envelope["data"] = {"padding": "x" * 4096}
            emitter.publish(envelope)

        old_empty = journal.new_cut()["snapshot_cursor"]
        emit_oversized("taskZ", task_id="taskZ", worker="codex",
                       mode="app-server", session_id="s", turn_id="t")
        control = journal.subscribe_all(old_empty)
        control_frames = control.drain(100)
        self.assertEqual([f["type"] for f in control_frames], ["gap"])
        self.assertEqual(control_frames[0]["gap"],
                         {"stream_id": "taskZ", "from_seq": 1,
                          "to_seq": 1, "count": 1})
        journal.close_subscription(control)
        # a separate NON-EMPTY cut acknowledges the zombie globally…
        accepted(emitter, "taskLive")
        tainted = journal.new_cut()
        self.assertFalse(journal.snapshot(
            tainted["snapshot_cursor"], client="s")["ok"])
        # …but the old empty cut keeps its own gap evidence
        again = journal.subscribe_all(old_empty)
        frames = again.drain(100)
        self.assertTrue(any(f.get("type") == "gap" and
                            f["gap"]["stream_id"] == "taskZ"
                            for f in frames), frames)
        # a second empty cut taken BEFORE any acknowledgment sees the same
        # gap (deferred, not consumed)

    def test_fresh_cut_rebases_past_acknowledged_interior_hole(self):
        emitter, journal = self.make_journal(max_event_bytes=2048)
        accepted(emitter, "taskA")

        def emit_oversized(stream, **fields):
            envelope = dict(emitter.stream.build(
                "turn.started", stream, **fields))
            envelope["data"] = {"padding": "x" * 4096}
            emitter.publish(envelope)

        emit_oversized("taskA", task_id="taskA", worker="codex",
                       mode="app-server", session_id="s", turn_id="t")
        completed(emitter, "taskA", outcome="ok")
        first = journal.new_cut()  # pins the unacknowledged hole -> reset
        self.assertEqual(journal.snapshot(
            first["snapshot_cursor"], client="s").get("error"),
            "reset_required")
        fresh = journal.new_cut()  # baseline advanced past the acked loss
        self.assertTrue(journal.snapshot(
            fresh["snapshot_cursor"], client="s")["ok"])
        self.assertTrue(journal.snapshot_events(
            fresh["snapshot_cursor"], client="e")["ok"])
        self.assertFalse(journal.subscribe_all(
            fresh["snapshot_cursor"]).reset_pending)
        # the exposing cut stays reset forever (immutable snapshot)
        self.assertEqual(journal.snapshot(
            first["snapshot_cursor"], client="s2").get("error"),
            "reset_required")

    def test_post_cut_retired_stream_and_its_new_loss_stay_observable(self):
        emitter, journal = self.make_journal(max_events=1)
        old_empty = journal.new_cut()["snapshot_cursor"]  # empty baseline
        accepted(emitter, "taskRetired")  # born AFTER the cut
        accepted(emitter, "taskOther")  # evicts taskRetired:1 -> retired
        subscription = journal.subscribe_all(old_empty)
        frames = subscription.drain(100)
        retired_gaps = [f["gap"] for f in frames
                        if f.get("type") == "gap"
                        and f["gap"]["stream_id"] == "taskRetired"]
        self.assertTrue(retired_gaps, frames)
        self.assertEqual(retired_gaps[0]["from_seq"], 1)

    def test_retired_stream_post_cut_oversize_observable_on_fresh_cut(self):
        emitter, journal = self.make_journal(max_events=1,
                                             max_event_bytes=2048)
        accepted(emitter, "taskRetired")
        accepted(emitter, "taskLive")
        fresh = journal.new_cut()["snapshot_cursor"]  # excludes retired
        subscription = journal.subscribe_all(fresh)
        self.assertEqual(subscription.drain(100), [])  # nothing owed yet
        # a NEW loss on the known-but-retired stream must stay observable
        envelope = dict(emitter.stream.build(
            "turn.started", "taskRetired", task_id="taskRetired",
            worker="codex", mode="app-server", session_id="s",
            turn_id="t2"))
        envelope["data"] = {"padding": "x" * 4096}
        emitter.publish(envelope)
        subscription2 = journal.subscribe_all(fresh)
        frames = subscription2.drain(100)
        self.assertTrue(any(f.get("type") == "gap" and
                            f["gap"]["stream_id"] == "taskRetired"
                            and f["gap"]["from_seq"] == 2
                            for f in frames), frames)

    def test_zombie_gap_starts_at_seq1_despite_lost_cap_trim(self):
        emitter, journal = self.make_journal(max_event_bytes=2048)

        def emit_oversized(stream, seq_hint, **fields):
            envelope = dict(emitter.stream.build(
                "turn.started", stream, **fields))
            envelope["data"] = {"padding": "x" * 4096}
            emitter.publish(envelope)
            return envelope

        for index in range(1025):  # cap-trims seq 1 out of _lost
            emit_oversized("taskZ", index, task_id="taskZ", worker="codex",
                           mode="app-server", session_id="s",
                           turn_id=f"t{index}")
        self.assertNotIn(1, journal._lost.get("taskZ", ()))
        empty = journal.new_cut()["snapshot_cursor"]
        subscription = journal.subscribe_all(empty)
        frames = subscription.drain(100)
        gaps = [f["gap"] for f in frames if f.get("type") == "gap"]
        self.assertTrue(gaps, frames)
        self.assertEqual(gaps[0]["from_seq"], 1)
        self.assertEqual(gaps[0]["stream_id"], "taskZ")

    def test_pruned_stream_reuse_continues_seq_and_old_cuts_track_it(self):
        # 回收（无存活 cut 引用）→ 同 stream 续写 → 新 cut/订阅：
        # high-water tombstone 保证续写从旧 high 之后开始，旧 cut 把它当作
        # 已知流的续区间（不误判新流、不重放、不回退 seq）
        emitter, journal = self.make_journal(max_events=1,
                                             registry_max_cursors=1)
        accepted(emitter, "taskA")
        journal.new_cut()  # cut1 pins taskA (registry cap 1)
        accepted(emitter, "taskB")                 # evicts A1
        turn_started(emitter, "taskB", "sess-B", "turn-B1")  # evicts B1
        journal.new_cut()  # registry cap evicts cut1 -> prune reclaims the
        self.assertNotIn("taskA", journal._streams)  # retired streams
        # repair5: the high-water map itself is reclaimed with the stream —
        # the bounded ``_recent_high`` store carries the monotonicity bound
        self.assertNotIn("taskA", journal._high_water)
        self.assertEqual(journal._known_high("taskA"), 1)  # recent bound
        # a rollback/duplicate seq must not re-create history (raw envelope:
        # no emitter seq allocation — the journal's own guard is under test)
        emitter.publish({"v": 1, "event_id": "stale/taskA/1",
                         "stream_id": "taskA", "seq": 1,
                         "kind": "task.accepted", "observed_at": self.RFC,
                         "task_id": "taskA", "worker": "codex",
                         "mode": "app-server",
                         "data": {"contract_id": "cA",
                                  "contract_digest": self.HEX,
                                  "created_at": self.RFC,
                                  "initiator_verified": False}})
        self.assertNotIn("taskA", journal._streams)
        # the same stream id CONTINUES past its recent high…
        turn_started(emitter, "taskA", "sess-A", "turn-A2")  # seq 2
        self.assertEqual(journal.high_water("taskA"), 2)
        # …and a NEW cut/subscription pins the continued stream at its true
        # high-water with a consistent floor: seq 2, never a seq-1 restart
        # (an old cut whose pinned stream slid is legitimately reset — the
        # reuse boundary here is monotonicity + no misjudgment as new)
        fresh = journal.new_cut()
        entry = journal.cut_entry(fresh["snapshot_cursor"])
        self.assertEqual(entry["high_watermarks"].get("taskA"), 2)
        self.assertEqual(entry["floors"].get("taskA"), 2)
        state = journal.snapshot(fresh["snapshot_cursor"], client="s")
        self.assertTrue(state["ok"], state)
        events = journal.snapshot_events(fresh["snapshot_cursor"],
                                         client="e")
        self.assertTrue(events["ok"], events)
        seqs_served = [row["seq"] for row in events["events"]
                       if row["stream_id"] == "taskA"]
        self.assertEqual(seqs_served, [2])

    def test_lifecycle_metadata_bounded_by_live_references(self):
        emitter, journal = self.make_journal(max_events=1,
                                             max_event_bytes=2048)

        def emit_oversized(stream, **fields):
            envelope = dict(emitter.stream.build(
                "turn.started", stream, **fields))
            envelope["data"] = {"padding": "x" * 4096}
            emitter.publish(envelope)

        for index in range(128):
            emit_oversized(f"z{index}", task_id=f"z{index}",
                           worker="codex", mode="app-server",
                           session_id="s", turn_id="t")
            accepted(emitter, f"r{index}")  # evicts the previous retained
            journal.new_cut()  # non-empty cut acknowledges each zombie
        self.assertEqual(journal.stats()["total_events"], 1)
        self.assertLessEqual(len(journal._ever_retained), 1)
        self.assertLessEqual(len(journal._zombie_acked), 1)


class UiJournalRepair5Tests(unittest.TestCase):
    """repair5 (2026-10-28): bounded monotonic seq guard, loss ack on
    fresh cuts, bounded tombstones/known_high and capacity-eviction
    tainting — the dispatch resource matrix as product regressions."""

    HEX = "3" * 64
    RFC = "2070-01-01T00:00:00.000000Z"

    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)

    @staticmethod
    def make_journal(**options):
        emitter, journal = small_journal(**options)
        return emitter, journal

    def raw_event(self, stream, seq, *, oversize=False):
        payload = "x" * 4096 if oversize else ""
        return {"v": 1, "event_id": f"raw/{stream}/{seq}",
                "stream_id": stream, "seq": seq, "kind": "task.accepted",
                "observed_at": self.RFC, "task_id": stream,
                "worker": "codex", "mode": "app-server",
                "data": {"padding": payload} if payload else {}}

    def test_strict_guard_rejects_duplicates_and_stale_oversize(self):
        for count in (1, 2):
            with self.subTest(window=count):
                emitter, journal = self.make_journal(max_event_bytes=2048)
                for seq in range(1, count + 1):
                    turn_started(emitter, "taskA", "s", f"t{seq}")
                before = journal.dropped_events
                # duplicate replay of the window [1..count]
                for seq in range(1, count + 1):
                    emitter.publish(self.raw_event("taskA", seq))
                # stale oversize at a retained seq changes NOTHING
                emitter.publish(self.raw_event("taskA", 1, oversize=True))
                self.assertEqual(journal.dropped_events, before + count + 1)
                self.assertNotIn("taskA", journal._lost)
                self.assertEqual(journal.high_water("taskA"), count)
                # a legal continuation is still accepted
                turn_started(emitter, "taskA", "s", "next")
                self.assertEqual(journal.high_water("taskA"), count + 1)

    def test_lost_cap_never_widens_gap_over_retained_seq(self):
        emitter, journal = self.make_journal(max_event_bytes=2048)
        accepted(emitter, "taskA")                       # seq 1 retained
        emitter.publish(self.raw_event("taskA", 2, oversize=True))  # lost 2
        turn_completed(emitter, "taskA", "s", "t1")      # seq 3 retained
        emitter.publish(self.raw_event("taskA", 4, oversize=True))  # lost 4
        turn_completed(emitter, "taskA", "s", "t2")      # seq 5 retained
        replay = journal.replay("taskA", 0)
        self.assertFalse(replay["ok"])
        self.assertEqual(replay["gap"],
                         {"stream_id": "taskA", "from_seq": 2,
                          "to_seq": 2, "count": 1})  # first run only

    def test_fresh_cut_ack_allows_reclaim_without_precut_loss(self):
        emitter, journal = self.make_journal(max_events=1,
                                             max_event_bytes=2048,
                                             registry_max_cursors=1)
        accepted(emitter, "taskS")                # retired by the next line
        accepted(emitter, "taskOther")            # evicts taskS:1
        emitter.publish(self.raw_event("taskS", 2, oversize=True))
        cut = journal.new_cut()                   # the only live cut: its
        # baseline (known copy bound 2) covers the retired loss -> ack+GC
        self.assertNotIn("taskS", journal._lost)
        self.assertNotIn("taskS", journal._streams)
        self.assertTrue(journal.snapshot(
            cut["snapshot_cursor"], client="s")["ok"])

    def test_resource_matrix_32_64_and_128_256_bounded(self):
        for unique, cuts in ((32, 0), (64, 0), (32, 8), (64, 8),
                             (128, 8), (256, 8)):
            with self.subTest(unique=unique, cuts=cuts):
                emitter, journal = self.make_journal(max_events=1,
                                                     registry_max_cursors=8)
                for index in range(unique):
                    accepted(emitter, f"s{index}")
                    if cuts:
                        journal.new_cut()
                if not cuts:
                    # no cut ever: the hard cap keeps the maps bounded and
                    # equal across scales (32 vs 64 / 128 vs 256)
                    pass
                self.assertLessEqual(len(journal._high_water),
                                     max(1, len(journal._ever_retained))
                                     + journal._metadata_cap)
                known_sizes = [len(entry["known_high"])
                               for entry in journal._registry.values()]
                self.assertLessEqual(
                    sum(known_sizes),
                    8 * max(1, len(journal._ever_retained)) + 8)
                self.assertEqual(len(journal._streams) <=
                                 journal._metadata_cap + 1, True)

    def test_capacity_eviction_taints_owed_old_cuts_before_reclaim(self):
        # force the hard cap tiny so unpinned metadata exceeds it
        emitter, journal = self.make_journal(max_events=1)
        journal._metadata_cap = 1
        old_empty = journal.new_cut()["snapshot_cursor"]  # predates streams
        accepted(emitter, "s0")   # retired by:
        accepted(emitter, "s1")   # retired by:
        accepted(emitter, "s2")   # live
        # cap 1 exceeded by s0/s1 (unpinned) -> oldest evicted; the cut
        # that predates them must be TAINTED, not silently lossy
        entry = journal._registry[old_empty["snapshot_id"]]
        self.assertTrue(entry["tainted"])
        result = journal.subscribe_all(old_empty)
        self.assertTrue(result.reset_pending)

    def test_stale_after_reclaim_rejected_via_recent_high(self):
        emitter, journal = self.make_journal(max_events=1,
                                             max_event_bytes=2048,
                                             registry_max_cursors=1)
        accepted(emitter, "taskA")
        accepted(emitter, "taskB")
        journal.new_cut()  # evicts prior cut; prune reclaims taskA/taskB
        self.assertNotIn("taskA", journal._high_water)  # retired + unreferenced
        self.assertIn("taskB", journal._streams)  # still live-retained
        before = journal.dropped_events
        emitter.publish(self.raw_event("taskA", 1))  # stale normal
        emitter.publish(self.raw_event("taskA", 1, oversize=True))
        self.assertEqual(journal.dropped_events, before + 2)
        self.assertNotIn("taskA", journal._lost)
        # continuation past the recent bound still accepted
        emitter.publish(self.raw_event("taskA", 2))
        self.assertEqual(journal.high_water("taskA"), 2)


class UiJournalRepair6Tests(unittest.TestCase):
    """repair6 (2026-10-28): every truncation/reclaim point that still owes
    an old cut degrades observably — one regression per dispatch item."""

    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)

    @staticmethod
    def make_journal(**options):
        emitter, journal = small_journal(**options)
        return emitter, journal

    def raw_event(self, stream, seq, *, oversize=False):
        payload = "x" * 4096 if oversize else ""
        return {"v": 1, "event_id": f"raw6/{stream}/{seq}",
                "stream_id": stream, "seq": seq, "kind": "task.accepted",
                "observed_at": "2070-01-01T00:00:00.000000Z",
                "task_id": stream, "worker": "codex", "mode": "app-server",
                "data": {"padding": payload} if payload else {}}

    def test_reclaimed_pinned_stream_never_serves_ok_state(self):
        # R5-01: reclaiming the pinned stream must degrade state pages and
        # subscribe_all observably, never a silent ok/done omission
        emitter, journal = self.make_journal(max_events=2)
        accepted(emitter, "taskA")
        turn_started(emitter, "taskA", "s", "t1")
        cut = journal.new_cut()["snapshot_cursor"]  # pins taskA:1..2
        accepted(emitter, "taskB")                  # evicts taskA:1
        turn_started(emitter, "taskB", "s", "tB1")  # evicts taskA:2 fully
        before = journal.snapshot(cut, client="ctl")
        self.assertEqual((before["ok"], before.get("error")),
                         (False, "reset_required"))  # floor evidence live
        journal.new_cut()  # prune reclaims taskA (no predating cut)
        self.assertNotIn("taskA", journal._high_water)
        after = journal.snapshot(cut, client="after")
        self.assertFalse(after["ok"] and after.get("done"), after)
        subscription = journal.subscribe_all(cut)
        self.assertTrue(subscription.reset_pending)

    def test_known_copy_excluded_unacked_loss_taints_cut_then_acks(self):
        # R5-02: a capped-out retired stream holding an UNACKED loss taints
        # the cut at creation (observable reset) and the taint IS the ack —
        # the NEXT fresh cut recovers clean
        emitter, journal = self.make_journal(max_events=2,
                                             max_event_bytes=2048)
        accepted(emitter, "lossy")                       # retired below
        accepted(emitter, "x1")
        accepted(emitter, "x2")   # evicts lossy:1
        emitter.publish(self.raw_event("lossy", 2, oversize=True))
        accepted(emitter, "live1")  # evict x1
        accepted(emitter, "live2")  # evict x2; live={live1, live2} cap 2
        first = journal.new_cut()
        entry = journal.cut_entry(first["snapshot_cursor"])
        self.assertNotIn("lossy", entry["known_high"])  # recency excluded
        self.assertTrue(entry["tainted"])                # observable
        self.assertEqual(journal.snapshot(
            first["snapshot_cursor"], client="s").get("error"),
            "reset_required")
        self.assertTrue(journal.subscribe_all(
            first["snapshot_cursor"]).reset_pending)
        # exposure-ack: the follow-up fresh cut is clean
        fresh = journal.new_cut()
        self.assertFalse(journal.cut_entry(
            fresh["snapshot_cursor"])["tainted"])
        self.assertTrue(journal.snapshot(
            fresh["snapshot_cursor"], client="s2")["ok"])

    def test_tainted_cut_events_page_and_empty_cut_reset(self):
        # R5-03: the taint check precedes EVERY early return — even an
        # empty (no pinned streams) tainted cut must never serve a
        # completed events page
        emitter, journal = self.make_journal(max_events=1)
        journal._metadata_cap = 1
        old_empty = journal.new_cut()["snapshot_cursor"]
        accepted(emitter, "s0")
        accepted(emitter, "s1")   # capacity eviction taints old_empty
        accepted(emitter, "s2")
        self.assertTrue(journal._registry[
            old_empty["snapshot_id"]]["tainted"])
        events = journal.snapshot_events(old_empty, client="e")
        self.assertEqual((events["ok"], events.get("error")),
                         (False, "reset_required"))
        self.assertTrue(journal.subscribe_all(old_empty).reset_pending)

    def test_zero_seq_and_post_reclaim_duplicates_allocate_no_epoch(self):
        # Q2: epochs exist only after the strict guard passes; duplicates
        # of a stream still inside the recent window reallocate nothing
        emitter, journal = self.make_journal(max_events=8)
        for index in range(64):
            emitter.publish(self.raw_event(f"ghost{index}", 0))
        self.assertEqual(journal._stream_epoch, {})
        accepted(emitter, "victim")
        for index in range(8):   # evict victim:1 (max_events=8)
            accepted(emitter, f"c{index}")
        journal._prune_metadata()
        self.assertNotIn("victim", journal._high_water)
        self.assertIn("victim", journal._recent_high)  # window still covers
        before = dict(journal._stream_epoch)
        for _ in range(32):
            emitter.publish(self.raw_event("victim", 1))  # rejected dups
        self.assertEqual(journal._stream_epoch, before)

    def test_oversize_only_storm_bounded_without_cuts(self):
        # Q3: the cap holds on the oversize ingest path with zero cuts
        emitter, journal = self.make_journal(max_events=16,
                                             max_event_bytes=2048)
        for index in range(64):
            emitter.publish(self.raw_event(f"z{index}", 1, oversize=True))
        unpinned = [s for s in journal._high_water
                    if not journal._streams.get(s)]
        self.assertLessEqual(len(unpinned), journal._metadata_cap)
        self.assertLessEqual(len(journal._stream_epoch),
                             journal._metadata_cap + len(journal._streams))


class UiJournalRepair7Tests(unittest.TestCase):
    """repair7 (2026-10-28): unified reclaimed-reset tier on every read
    path, and the (3b) excluded-loss obligation frozen as per-cut evidence
    immune to later cuts' taint/ack."""

    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)

    @staticmethod
    def make_journal(**options):
        emitter, journal = small_journal(**options)
        return emitter, journal

    def test_events_page_reset_on_reclaim_full_mid_and_consistency(self):
        emitter, journal = self.make_journal(max_events=8,
                                             page_max_records=10,
                                             page_max_bytes=64 * 1024)
        for task in ("taskA", "taskB", "taskC"):
            accepted(emitter, task)
            turn_started(emitter, task, "s", f"t-{task}")
        cut = journal.new_cut()["snapshot_cursor"]
        reference = journal.snapshot_events(cut, client="ref")
        self.assertEqual(len(reference["events"]), 6)
        # full reclamation of every pinned stream
        for index in range(8):
            accepted(emitter, f"churn{index}")
        journal.new_cut()
        for client in ("whole1", "whole2"):
            page = journal.snapshot_events(cut, client=client)
            self.assertEqual((page["ok"], page.get("error")),
                             (False, "reset_required"))
        # mid-pagination reclamation: first page served, then reset
        emitter, journal = self.make_journal(max_events=8,
                                             page_max_records=1)
        for task in ("taskA", "taskB", "taskC"):
            accepted(emitter, task)
            turn_started(emitter, task, "s", f"t-{task}")
        cut = journal.new_cut()["snapshot_cursor"]
        first = journal.snapshot_events(cut, client="pg")
        self.assertEqual(first["events"][0]["stream_id"], "taskA")
        for index in range(8):
            accepted(emitter, f"churn{index}")
        journal.new_cut()
        second = journal.snapshot_events(
            cut, page_cursor=first["next_page_cursor"], client="pg")
        self.assertEqual((second["ok"], second.get("error")),
                         (False, "reset_required"))

    def test_excluded_loss_gap_frozen_against_later_taint_ack(self):
        # (3b) per-cut evidence: reader A sees the gap; a later cut's
        # taint+ack of the same loss changes nothing for reader B on the
        # SAME cut — identical observable output
        emitter, journal = self.make_journal(max_events=2,
                                             max_event_bytes=2048)
        journal.new_cut()  # predating anchor cut keeps r alive (probe shape)
        accepted(emitter, "taskAnchor")
        accepted(emitter, "r")
        accepted(emitter, "x1")
        accepted(emitter, "x2")  # evicts r:1 -> r retired
        accepted(emitter, "live1")
        accepted(emitter, "live2")  # live={live1, live2}; copy cap 2
        old = journal.new_cut()
        self.assertNotIn("r", journal.cut_entry(
            old["snapshot_cursor"])["known_high"])
        self.assertIn("r", journal.cut_entry(
            old["snapshot_cursor"])["excluded_high"])
        emitter.publish({"v": 1, "event_id": "r7/r/5",
                         "stream_id": "r", "seq": 5, "kind": "task.accepted",
                         "observed_at": "2070-01-01T00:00:00.000000Z",
                         "task_id": "r", "worker": "codex",
                         "mode": "app-server",
                         "data": {"padding": "x" * 4096}})
        reader_a = journal.subscribe_all(old["snapshot_cursor"])
        gaps_a = [f["gap"] for f in reader_a.drain(100)
                  if f.get("type") == "gap" and f["gap"]["stream_id"] == "r"]
        self.assertEqual(gaps_a, [{"stream_id": "r", "from_seq": 1,
                                   "to_seq": 5, "count": 5}])
        journal.close_subscription(reader_a)
        later = journal.new_cut()  # excludes r -> taint + ack r:5 (then the
        self.assertTrue(journal.cut_entry(later["snapshot_cursor"])["tainted"])
        # global record of r may be reclaimed once acked — the frozen gap in
        # the OLD entry is what every reader of the old cut observes
        self.assertFalse(journal.cut_entry(old["snapshot_cursor"])["tainted"])
        reader_b = journal.subscribe_all(old["snapshot_cursor"])
        gaps_b = [f["gap"] for f in reader_b.drain(100)
                  if f.get("type") == "gap" and f["gap"]["stream_id"] == "r"]
        self.assertEqual(gaps_b, gaps_a)
        self.assertFalse(reader_b.reset_pending)
        # fresh-cut recovery is untouched
        fresh = journal.new_cut()
        self.assertFalse(journal.cut_entry(
            fresh["snapshot_cursor"])["tainted"])


class UiJournalRepair8Tests(unittest.TestCase):
    """repair8 (2026-10-28): (3b) obligations materialize AT LOSS-INGEST
    into every owing live cut — the first reader sees the gap even after
    later cuts' ack, pin expiry and cap eviction destroyed the globals."""

    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)

    @staticmethod
    def make_journal(**options):
        emitter, journal = small_journal(**options)
        return emitter, journal

    def raw_oversize(self, emitter, stream, seq):
        emitter.publish({"v": 1, "event_id": f"r8/{stream}/{seq}",
                         "stream_id": stream, "seq": seq,
                         "kind": "task.accepted",
                         "observed_at": "2070-01-01T00:00:00.000000Z",
                         "task_id": stream, "worker": "codex",
                         "mode": "app-server",
                         "data": {"padding": "x" * 4096}})

    def orphan_journal(self):
        # old cut's copy cap-excludes taskR (frozen high 1); a predating
        # cut keeps taskR's records alive until it expires
        emitter, journal = self.make_journal(max_events=1,
                                             registry_max_cursors=3)
        journal.new_cut()                      # predating pin
        accepted(emitter, "taskR")
        accepted(emitter, "taskKeep")          # retires taskR
        accepted(emitter, "taskLive")
        old = journal.new_cut()
        entry = journal.cut_entry(old["snapshot_cursor"])
        self.assertIn("taskR", entry["excluded_high"])
        return emitter, journal, old

    def test_loss_ingest_freezes_gap_before_any_reader(self):
        emitter, journal, old = self.orphan_journal()
        self.raw_oversize(emitter, "taskR", 2)     # the loss itself
        entry = journal.cut_entry(old["snapshot_cursor"])
        self.assertEqual(entry.get("excluded_gaps", {}).get("taskR"),
                         (1, 2))                    # frozen immediately
        self.assertNotIn("taskR", entry["excluded_high"])
        # later cut acks the loss; pin expiry reclaims the global records
        journal.new_cut()
        journal.new_cut()                           # LRU evicts the pin
        self.assertNotIn("taskR", journal._high_water)
        self.assertNotIn("taskR", journal._lost)
        # the FIRST reader still sees the frozen gap — no orphaning
        subscription = journal.subscribe_all(old["snapshot_cursor"])
        gaps = [f["gap"] for f in subscription.drain(100)
                if f.get("type") == "gap"
                and f["gap"]["stream_id"] == "taskR"]
        self.assertEqual(gaps, [{"stream_id": "taskR", "from_seq": 1,
                                 "to_seq": 2, "count": 2}])
        self.assertFalse(subscription.reset_pending)

    def test_reader_timing_independence_after_freeze(self):
        emitter, journal, old = self.orphan_journal()
        self.raw_oversize(emitter, "taskR", 2)
        early = journal.subscribe_all(old["snapshot_cursor"])
        early_gaps = [f["gap"] for f in early.drain(100)
                      if f.get("type") == "gap"]
        journal.close_subscription(early)
        # a later cut taint-acks the loss, then LRU expiry of the pin
        # reclaims taskR's global records — no cap pressure on old itself
        journal.new_cut()
        journal.new_cut()
        self.assertNotIn("taskR", journal._high_water)
        late = journal.subscribe_all(old["snapshot_cursor"])
        late_gaps = [f["gap"] for f in late.drain(100)
                     if f.get("type") == "gap"]
        self.assertEqual(late_gaps, early_gaps)     # timing-identical
        self.assertFalse(late.reset_pending)

    def test_cap_victim_with_owing_reference_degrades_observably(self):
        # ownership counting: a cap victim referenced by a cut's frozen
        # gap (post-creation loss) or its creation-time taint (pre-creation
        # loss) never leaves that cut silently empty — the frozen frames
        # keep serving verbatim, and the taint keeps resetting
        emitter, journal = self.make_journal(max_events=1,
                                             max_event_bytes=2048)
        journal._metadata_cap = 2
        anchor = journal.new_cut()["snapshot_cursor"]  # predates everything
        accepted(emitter, "taskR")
        accepted(emitter, "taskKeep")           # taskR retires
        accepted(emitter, "taskLive")
        cut = journal.new_cut()["snapshot_cursor"]
        self.assertIn("taskR", journal.cut_entry(cut)["excluded_high"])
        # post-creation loss: frozen instantly into every owing cut
        self.raw_oversize(emitter, "taskR", 2)
        # cap pressure evicts globals (zombies + retired), old refs included
        for index in range(6):
            self.raw_oversize(emitter, f"p{index}", 1)
        subscriber = journal.subscribe_all(cut)
        gaps = [f["gap"] for f in subscriber.drain(100)
                if f.get("type") == "gap"]
        observed = {
            "tainted": journal.cut_entry(cut)["tainted"],
            "reset": subscriber.reset_pending, "gaps": gaps}
        self.assertTrue(
            any(g["stream_id"] == "taskR" for g in gaps)
            or observed["tainted"] or observed["reset"], observed)
        # the predating anchor's discovery obligation degrades loudly too
        self.assertTrue(journal.cut_entry(anchor)["tainted"])


    def test_second_loss_above_frozen_run_served_from_live_map(self):
        # repair9/10 R9-03: a later, higher loss on an already-frozen
        # excluded stream is PERSISTENTLY appended at ingest as a new
        # singleton run — the reader's total view survives ack+GC and cap
        # eviction of the globals verbatim, for first and late readers
        emitter, journal, old = self.orphan_journal()
        self.raw_oversize(emitter, "taskR", 2)   # first loss: freezes (1,2)
        entry = journal.cut_entry(old["snapshot_cursor"])
        self.assertEqual(entry["excluded_gaps"].get("taskR"), (1, 2))
        self.assertNotIn("taskR", entry["excluded_high"])
        self.raw_oversize(emitter, "taskR", 3)   # non-contiguous growth
        self.raw_oversize(emitter, "taskR", 5)
        self.assertEqual(journal._lost.get("taskR"), [2, 3, 5])
        full_view = [
            {"stream_id": "taskR", "from_seq": 1, "to_seq": 2, "count": 2},
            {"stream_id": "taskR", "from_seq": 3, "to_seq": 3, "count": 1},
            {"stream_id": "taskR", "from_seq": 5, "to_seq": 5, "count": 1}]
        # globals live: reader A sees the full appended view
        subscription = journal.subscribe_all(old["snapshot_cursor"])
        gaps = [f["gap"] for f in subscription.drain(100)
                if f.get("type") == "gap"
                and f["gap"]["stream_id"] == "taskR"]
        self.assertEqual(gaps, full_view)
        self.assertFalse(subscription.reset_pending)
        # the frozen first run is immutable; the appended evidence persists
        journal.new_cut()
        journal.new_cut()                          # LRU evicts the pin
        self.assertNotIn("taskR", journal._high_water)
        self.assertNotIn("taskR", journal._lost)
        post = journal.subscribe_all(old["snapshot_cursor"])
        post_gaps = [f["gap"] for f in post.drain(100)
                     if f.get("type") == "gap"
                     and f["gap"]["stream_id"] == "taskR"]
        self.assertEqual(post_gaps, full_view)    # post-GC view unchanged
        self.assertEqual(
            journal.cut_entry(old["snapshot_cursor"])["excluded_gaps"]
            .get("taskR"), (1, 2))                # never rewritten
        fresh = journal.new_cut()
        self.assertFalse(journal.cut_entry(
            fresh["snapshot_cursor"])["tainted"])  # fresh baseline clean

    def test_unacked_growth_survives_cap_eviction_without_ack(self):
        # repair10 R9-03 strong variant: capacity eviction with NO ack
        # transition anywhere — the persisted appended run still serves
        emitter, journal = self.make_journal(max_events=1,
                                             max_event_bytes=2048,
                                             registry_max_cursors=8)
        journal.new_cut()                      # predating pin
        accepted(emitter, "taskR")
        accepted(emitter, "taskKeep")          # taskR retires
        accepted(emitter, "taskLive")
        for name in ("z0", "z1", "z2"):
            self.raw_oversize(emitter, name, 1)  # zombies, then acked
        journal.new_cut()                      # acking cut for zombies
        cut = journal.new_cut()["snapshot_cursor"]
        self.assertIn("taskR", journal.cut_entry(cut)["excluded_high"])
        self.raw_oversize(emitter, "taskR", 2)  # freeze (1,2)
        self.raw_oversize(emitter, "taskR", 5)  # growth, UNACKED
        self.assertEqual(journal._loss_ack_upto.get("taskR", 0), 0)
        journal._metadata_cap = 2
        self.raw_oversize(emitter, "z0", 2)     # trigger cap enforcement
        self.assertNotIn("taskR", journal._high_water)
        self.assertFalse(journal.cut_entry(cut)["tainted"])
        subscriber = journal.subscribe_all(cut)
        gaps = [f["gap"] for f in subscriber.drain(100)
                if f.get("type") == "gap"
                and f["gap"]["stream_id"] == "taskR"]
        self.assertTrue(any(g["to_seq"] >= 5 for g in gaps), gaps)
        self.assertFalse(subscriber.reset_pending)

    def test_multi_stream_persistent_appends_reader_determinism(self):
        # concurrent growth on several excluded streams: two readers at
        # the same global state see identical run sets; first runs stable
        emitter, journal = self.make_journal(max_events=4,
                                             max_event_bytes=2048,
                                             queue_max_events=64,
                                             registry_max_cursors=16)
        journal.new_cut()
        for index in range(6):
            accepted(emitter, f"junk{index}")
        targets = [f"r{index}" for index in range(3)]
        for name in targets:
            accepted(emitter, name)
        accepted(emitter, "keep1")
        accepted(emitter, "keep2")
        cut = journal.new_cut()["snapshot_cursor"]
        excluded = dict(journal.cut_entry(cut)["excluded_high"])
        self.assertTrue(excluded, "fixture: some retired stream excluded")
        # growth on every excluded target (seq 2+ leaves creation-high 1)
        for name in excluded:
            self.raw_oversize(emitter, name, 2)
        for name in excluded:
            self.raw_oversize(emitter, name, 4)
        def view():
            sub = journal.subscribe_all(cut)
            frames = sub.drain(256)
            journal.close_subscription(sub)
            return (sub.reset_pending,
                    tuple(sorted((f["gap"]["stream_id"],
                                  f["gap"]["from_seq"],
                                  f["gap"]["to_seq"])
                                 for f in frames
                                 if f.get("type") == "gap")))
        first, second = view(), view()
        self.assertEqual(first, second)        # reader determinism
        self.assertFalse(first[0])
        gaps = journal.cut_entry(cut)["excluded_gaps"]
        more = journal.cut_entry(cut).get("excluded_gaps_more", {})
        self.assertTrue(gaps, "nothing froze")
        for name, frozen in gaps.items():
            runs = tuple(r for r in first[1] if r[0] == name)
            self.assertTrue(runs, f"frozen obligation {name} missing")
            self.assertEqual(runs[0], (name, 1, frozen[1]))  # stable
            appended = more.get(name, ())
            self.assertEqual(
                tuple(runs[1:]),
                tuple((name, lo, hi) for lo, hi in appended))


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
