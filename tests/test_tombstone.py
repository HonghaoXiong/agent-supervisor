"""P0.2 tombstone contracts: known-late responses never fail the shared client.

Behavioral RED first: on the CURRENT ``protocol.ProtocolClient`` a response
frame that arrives after its request was locally timed out falls into the
unmatched branch, so ONE late frame fails the whole shared client and every
still-waiting request with it (PLAN §7 P0.2 / COMM-02). Those tests use only
the existing client API and must FAIL before the production change.

Tests that need the new symbols (``PendingTombstones``, the ``tombstones=``
constructor argument, ``client.tombstones``) import/construct them lazily:
on the unchanged tree they surface as ImportError/TypeError/AttributeError and
are listed separately from the behavioral failures — they are interface
evidence, not defect evidence.

No live CLI, node process, network, or model call. The harness drives the
REAL ``request``/``send``/``read_loop`` code against a recorded fake stdio
transport: ``stdin.write`` + ``drain`` are the real wire-send boundary and
responses are fed into a real ``StreamReader`` consumed by the real reader.
"""

import asyncio
import io
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from zcode_supervisor import protocol


class FakeClock:
    """Deterministic monotonic clock for tombstone TTL matrices."""

    def __init__(self, start=0.0):
        self.now = float(start)

    def __call__(self):
        return self.now

    def advance(self, delta):
        self.now += float(delta)


class ProtocolClientFixture(unittest.IsolatedAsyncioTestCase):
    """Real ProtocolClient on a recorded fake transport with a live reader."""

    def make_client(self, tombstones=None):
        # tombstones=None keeps the plain constructor so RED-phase behavioral
        # tests exercise the unchanged public surface.
        if tombstones is None:
            client = protocol.ProtocolClient(Path('/tmp/frozen'),
                                             Path('/tmp/not-written'), lambda _: {})
        else:
            client = protocol.ProtocolClient(Path('/tmp/frozen'),
                                             Path('/tmp/not-written'), lambda _: {},
                                             tombstones=tombstones)
        client.wire = io.StringIO()
        client.process = mock.Mock()
        client.process.stdout = asyncio.StreamReader()
        self.sent = []
        self.tracked = []
        self.addAsyncCleanup(self.reap_tracked_tasks)

        def record_write(data):
            self.sent.append(json.loads(data.decode()))

        client.process.stdin = mock.Mock()
        client.process.stdin.write = record_write
        client.process.stdin.drain = mock.AsyncMock(return_value=None)
        self.client = client
        self.reader = asyncio.create_task(client.read_loop())
        self.addAsyncCleanup(self.stop_reader)
        return client

    def track(self, task):
        """Keep a background request task from leaking an unretrieved result
        when an earlier assertion fails (RED noise hygiene only)."""
        self.tracked.append(task)
        return task

    async def reap_tracked_tasks(self):
        for task in self.tracked:
            if not task.done():
                task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass

    def feed(self, *frames):
        for frame in frames:
            self.client.process.stdout.feed_data(
                (json.dumps(frame) + '\n').encode())

    async def settle(self, seconds=0.02):
        await asyncio.sleep(seconds)

    async def stop_reader(self):
        self.client.closing = True
        self.client.process.stdout.feed_eof()
        await asyncio.wait_for(self.reader, 2)


class LateResponseBehaviorTests(ProtocolClientFixture):
    """RED defects: a late frame for a locally timed-out request fails all."""

    async def test_late_result_after_timeout_does_not_fail_client_or_pending_b(self):
        client = self.make_client()
        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(client.request('method/a', {}), 0.02)
        b_task = self.track(asyncio.create_task(client.request('method/b', {})))
        await self.settle()
        self.feed({'id': 'host-1', 'result': {'which': 'a-late'}})
        await self.settle()
        self.assertFalse(client.failure, client.failure)
        self.feed({'id': 'host-2', 'result': {'which': 'b'}})
        self.assertEqual(await asyncio.wait_for(b_task, 2), {'which': 'b'})

    async def test_late_error_after_timeout_does_not_fail_client_or_pending_b(self):
        client = self.make_client()
        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(client.request('method/a', {}), 0.02)
        b_task = self.track(asyncio.create_task(client.request('method/b', {})))
        await self.settle()
        self.feed({'id': 'host-1', 'error': {'code': -1, 'message': 'late'}})
        await self.settle()
        self.assertFalse(client.failure, client.failure)
        self.feed({'id': 'host-2', 'result': {'which': 'b'}})
        self.assertEqual(await asyncio.wait_for(b_task, 2), {'which': 'b'})

    async def test_duplicate_late_frames_stay_consumed_without_failing(self):
        client = self.make_client()
        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(client.request('method/a', {}), 0.02)
        b_task = self.track(asyncio.create_task(client.request('method/b', {})))
        await self.settle()
        self.feed({'id': 'host-1', 'result': {'which': 'a-late'}},
                  {'id': 'host-1', 'result': {'which': 'a-late-again'}})
        await self.settle()
        self.assertFalse(client.failure, client.failure)
        self.feed({'id': 'host-2', 'result': {'which': 'b'}})
        self.assertEqual(await asyncio.wait_for(b_task, 2), {'which': 'b'})

    async def test_late_result_is_never_replayed(self):
        client = self.make_client()
        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(client.request('method/a', {}), 0.02)
        b_task = self.track(asyncio.create_task(client.request('method/b', {})))
        await self.settle()
        self.feed({'id': 'host-1', 'result': {'which': 'a-late'}})
        await self.settle()
        self.feed({'id': 'host-2', 'result': {'which': 'b'}})
        self.assertEqual(await asyncio.wait_for(b_task, 2), {'which': 'b'})
        # No automatic replay: exactly one frame per request ever hit the wire.
        self.assertEqual([frame['method'] for frame in self.sent],
                         ['method/a', 'method/b'])

    async def test_external_cancel_reraises_and_late_frame_is_consumed(self):
        client = self.make_client()
        task = asyncio.create_task(client.request('method/a', {}))
        await self.settle()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        b_task = self.track(asyncio.create_task(client.request('method/b', {})))
        await self.settle()
        self.feed({'id': 'host-1', 'result': {'which': 'a-late'}})
        await self.settle()
        self.assertFalse(client.failure, client.failure)
        self.feed({'id': 'host-2', 'result': {'which': 'b'}})
        self.assertEqual(await asyncio.wait_for(b_task, 2), {'which': 'b'})


class ResponseRoutingGuards(ProtocolClientFixture):
    """Existing fail-closed / routing behavior that P0.2 must not weaken."""

    async def test_normal_responses_resolve_in_order(self):
        client = self.make_client()
        first = self.track(asyncio.create_task(client.request('method/a', {})))
        await self.settle()
        self.feed({'id': 'host-1', 'result': {'which': 'a'}})
        self.assertEqual(await asyncio.wait_for(first, 2), {'which': 'a'})
        second = self.track(asyncio.create_task(client.request('method/b', {})))
        await self.settle()
        self.feed({'id': 'host-2', 'result': {'which': 'b'}})
        self.assertEqual(await asyncio.wait_for(second, 2), {'which': 'b'})
        self.assertFalse(client.failure)

    async def test_out_of_order_responses_route_to_their_own_request(self):
        client = self.make_client()
        a_task = self.track(asyncio.create_task(client.request('method/a', {})))
        b_task = self.track(asyncio.create_task(client.request('method/b', {})))
        await self.settle()
        self.feed({'id': 'host-2', 'result': {'which': 'b'}},
                  {'id': 'host-1', 'result': {'which': 'a'}})
        self.assertEqual(await asyncio.wait_for(b_task, 2), {'which': 'b'})
        self.assertEqual(await asyncio.wait_for(a_task, 2), {'which': 'a'})
        self.assertFalse(client.failure)

    async def test_unknown_id_still_fails_closed(self):
        client = self.make_client()
        waiter = self.track(asyncio.create_task(client.request('method/a', {})))
        await self.settle()
        self.feed({'id': 'host-999', 'result': {}})
        await self.settle()
        self.assertIn('Unmatched protocol response', client.failure)
        with self.assertRaisesRegex(ValueError, 'Unmatched protocol response'):
            await waiter

    async def test_unsent_request_late_frame_still_fails_closed(self):
        client = self.make_client()
        transport = client.process
        client.process = None  # send() refuses before anything hits the wire
        with self.assertRaises(EOFError):
            await client.request('method/a', {})
        client.process = transport
        self.feed({'id': 'host-1', 'result': {'which': 'never-sent'}})
        await self.settle()
        self.assertIn('Unmatched protocol response', client.failure)

    async def test_cancel_during_send_drain_stays_fail_closed_on_late_frame(self):
        # A cancel inside drain() leaves the send ambiguous (write may or may
        # not have reached the wire) — no tombstone may claim it was sent.
        client = self.make_client()
        blocked = asyncio.Event()

        async def slow_drain():
            await blocked.wait()

        client.process.stdin.drain = slow_drain
        task = asyncio.create_task(client.request('method/a', {}))
        await self.settle()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        blocked.set()
        self.feed({'id': 'host-1', 'result': {'which': 'ambiguous-send'}})
        await self.settle()
        self.assertIn('Unmatched protocol response', client.failure)


class TombstoneIntegrationTests(ProtocolClientFixture):
    """New-interface contracts: needs PendingTombstones / tombstones= (lazy)."""

    def make_store(self, **kwargs):
        from zcode_supervisor.protocol import PendingTombstones
        return PendingTombstones(**kwargs)

    def wire_lines(self):
        return [json.loads(line) for line in
                self.client.wire.getvalue().splitlines() if line.strip()]

    async def test_timeout_records_one_tombstone_kept_after_consumption(self):
        client = self.make_client(tombstones=self.make_store())
        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(client.request('method/a', {}), 0.02)
        self.assertEqual(len(client.tombstones), 1)
        self.feed({'id': 'host-1', 'result': {'which': 'a-late'}})
        await self.settle()
        self.assertFalse(client.failure)
        # Kept until TTL: the consumed entry still holds the bounded state.
        self.assertEqual(len(client.tombstones), 1)
        self.assertEqual([line['late_response_to'] for line in self.wire_lines()],
                         ['host-1'])
        self.assertEqual(self.wire_lines()[0]['tombstone_reason'],
                         'local-timeout-or-cancel')

    async def test_duplicate_late_frames_are_counted_in_wire_evidence(self):
        client = self.make_client(tombstones=self.make_store())
        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(client.request('method/a', {}), 0.02)
        self.feed({'id': 'host-1', 'result': {'which': 'late-1'}},
                  {'id': 'host-1', 'error': {'code': -1, 'message': 'late-2'}})
        await self.settle()
        self.assertFalse(client.failure)
        lines = self.wire_lines()
        self.assertEqual([line['late_kind'] for line in lines],
                         ['result', 'error'])
        self.assertEqual([line['duplicate'] for line in lines], [False, True])
        self.assertEqual(len(client.tombstones), 1)

    async def test_normal_completion_and_close_record_no_tombstone(self):
        client = self.make_client(tombstones=self.make_store())
        request = self.track(asyncio.create_task(client.request('method/a', {})))
        await self.settle()
        self.feed({'id': 'host-1', 'result': {'which': 'a'}})
        self.assertEqual(await asyncio.wait_for(request, 2), {'which': 'a'})
        self.assertEqual(len(client.tombstones), 0)
        # A response for that id was never abandoned — still unmatched.
        self.feed({'id': 'host-1', 'result': {'which': 'again'}})
        await self.settle()
        self.assertIn('Unmatched protocol response', client.failure)

    async def test_cancel_after_send_records_tombstone_and_reraises(self):
        client = self.make_client(tombstones=self.make_store())
        task = asyncio.create_task(client.request('method/a', {}))
        await self.settle()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(len(client.tombstones), 1)

    async def test_cancel_while_closing_records_no_tombstone(self):
        client = self.make_client(tombstones=self.make_store())
        task = asyncio.create_task(client.request('method/a', {}))
        await self.settle()
        client.closing = True
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(len(client.tombstones), 0)

    async def test_ttl_expiry_at_late_frame_fails_closed_with_reason(self):
        clock = FakeClock()
        client = self.make_client(tombstones=self.make_store(ttl_s=10.0, clock=clock))
        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(client.request('method/a', {}), 0.02)
        waiter = self.track(asyncio.create_task(client.request('method/b', {})))
        await self.settle()
        clock.advance(10.001)
        self.feed({'id': 'host-1', 'result': {'which': 'too-late'}})
        await self.settle()
        self.assertIn('expired', client.failure)
        self.assertNotIn('Unmatched protocol response', client.failure)
        with self.assertRaisesRegex(ValueError, 'expired'):
            await waiter

    async def test_ttl_expiry_at_next_record_fails_closed(self):
        clock = FakeClock()
        client = self.make_client(tombstones=self.make_store(ttl_s=10.0, clock=clock))
        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(client.request('method/a', {}), 0.02)
        waiter = self.track(asyncio.create_task(client.request('method/b', {})))
        await self.settle()
        clock.advance(10.001)
        # The next local timeout would evict the expired entry: fail closed
        # now, with every pending request receiving an explainable terminal.
        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(client.request('method/c', {}), 0.02)
        self.assertIn('expired', client.failure)
        with self.assertRaisesRegex(ValueError, 'expired'):
            await waiter
        self.assertEqual(len(client.tombstones), 1)

    async def test_late_frame_inside_ttl_window_is_still_consumed(self):
        clock = FakeClock()
        client = self.make_client(tombstones=self.make_store(ttl_s=10.0, clock=clock))
        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(client.request('method/a', {}), 0.02)
        clock.advance(9.999)
        self.feed({'id': 'host-1', 'result': {'which': 'just-in-time'}})
        await self.settle()
        self.assertFalse(client.failure, client.failure)

    async def test_capacity_exhaustion_fails_closed_with_terminal_states(self):
        client = self.make_client(tombstones=self.make_store(capacity=2))
        for method in ('method/a', 'method/b'):
            with self.assertRaises(asyncio.TimeoutError):
                await asyncio.wait_for(client.request(method, {}), 0.02)
        self.assertEqual(len(client.tombstones), 2)
        waiter = self.track(asyncio.create_task(client.request('method/d', {})))
        await self.settle()
        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(client.request('method/c', {}), 0.02)
        self.assertIn('capacity', client.failure)
        with self.assertRaisesRegex(ValueError, 'capacity'):
            await waiter
        # Never past the configured bound, even mid-failure.
        self.assertEqual(len(client.tombstones), 2)


class PendingTombstonesUnitTests(unittest.TestCase):
    """Store-level matrix: deterministic fake clock, small capacity/TTL."""

    def make_store(self, **kwargs):
        from zcode_supervisor.protocol import PendingTombstones
        return PendingTombstones(**kwargs)

    def test_defaults_are_capacity_1024_and_ttl_300(self):
        from zcode_supervisor.protocol import PendingTombstones
        store = PendingTombstones()
        self.assertEqual(store.capacity, 1024)
        self.assertEqual(store.ttl_s, 300.0)
        store.record('host-1', 'local-timeout-or-cancel')
        entry = store.consume('host-1', 'result')
        self.assertEqual(entry['reason'], 'local-timeout-or-cancel')
        self.assertEqual(entry['late_count'], 1)
        self.assertEqual(entry['late_kind'], 'result')

    def test_invalid_capacity_or_ttl_are_rejected(self):
        for capacity in (0, -1, True, 1.5, '8', None):
            with self.subTest(capacity=capacity), self.assertRaises(ValueError):
                self.make_store(capacity=capacity)
        for ttl in (0, -1, True, float('nan'), float('inf'), 'x', None):
            with self.subTest(ttl=ttl), self.assertRaises(ValueError):
                self.make_store(ttl_s=ttl)

    def test_unknown_consume_returns_none_and_duplicates_are_counted(self):
        store = self.make_store(clock=FakeClock())
        self.assertIsNone(store.consume('host-1', 'result'))
        store.record('host-1', 'r')
        first = store.consume('host-1', 'result')
        second = store.consume('host-1', 'error')
        self.assertEqual(first['late_count'], 1)
        self.assertEqual(second['late_count'], 2)
        self.assertEqual(second['late_kind'], 'error')
        self.assertEqual(len(store), 1)

    def test_ttl_boundary_is_inclusive_then_expires_at_consume(self):
        from zcode_supervisor.protocol import TombstoneEvictionError
        clock = FakeClock()
        store = self.make_store(ttl_s=10.0, clock=clock)
        store.record('host-1', 'r')
        clock.advance(10.0)
        self.assertEqual(store.consume('host-1', 'result')['late_count'], 1)
        clock.advance(0.001)
        with self.assertRaisesRegex(TombstoneEvictionError, 'host-1'):
            store.consume('host-1', 'result')
        self.assertEqual(len(store), 0)

    def test_record_purging_an_expired_entry_fails_closed_without_recording(self):
        from zcode_supervisor.protocol import TombstoneEvictionError
        clock = FakeClock()
        store = self.make_store(ttl_s=10.0, clock=clock)
        store.record('host-1', 'r')
        clock.advance(10.001)
        with self.assertRaisesRegex(TombstoneEvictionError, 'host-1'):
            store.record('host-2', 'r')
        self.assertEqual(len(store), 1)
        self.assertIsNone(store.consume('host-2', 'result'))

    def test_capacity_overflow_raises_and_never_exceeds_the_bound(self):
        from zcode_supervisor.protocol import TombstoneEvictionError
        store = self.make_store(capacity=2, clock=FakeClock())
        store.record('host-1', 'r')
        store.record('host-2', 'r')
        with self.assertRaisesRegex(TombstoneEvictionError, 'capacity'):
            store.record('host-3', 'r')
        self.assertEqual(len(store), 2)
        store.consume('host-1', 'result')  # consumed entries still occupy slots
        with self.assertRaisesRegex(TombstoneEvictionError, 'capacity'):
            store.record('host-4', 'r')
        self.assertEqual(len(store), 2)

    def test_clock_is_injected_for_recorded_times(self):
        clock = FakeClock(start=100.0)
        store = self.make_store(clock=clock)
        store.record('host-1', 'r')
        entry = store.consume('host-1', 'result')
        self.assertEqual(entry['recorded_at'], 100.0)
        self.assertEqual(entry['last_late_at'], 100.0)


class ReviewFixRound2Tests(ProtocolClientFixture):
    """Round-2 review blockers, as deterministic fake-stdio interleavings.

    Scheduling note: ``feed_data`` and ``Task.cancel`` both schedule their
    wakeup callbacks synchronously inside the call, so a bare
    ``await asyncio.sleep(0)`` yields exactly once through the scheduled
    queue in creation order — reader-before-cleanup or cleanup-before-reader
    is chosen deterministically by the ORDER of feed/cancel, never by real
    time. No wall-clock sleep participates in any interleave here.
    """

    def make_store(self, **kwargs):
        from zcode_supervisor.protocol import PendingTombstones
        return PendingTombstones(**kwargs)

    def wire_late_lines(self):
        return [json.loads(line) for line in
                self.client.wire.getvalue().splitlines()
                if line.strip() and 'late_response_to' in line]

    async def test_response_consumed_then_cancel_records_no_tombstone(self):
        """Blocker 1: the response was already consumed by read_loop; a cancel
        landing before the request coroutine resumes must NOT tombstone — a
        later duplicate stays unknown and fails closed."""
        client = self.make_client(tombstones=self.make_store())
        task = self.track(asyncio.create_task(client.request('method/a', {})))
        await asyncio.sleep(0)   # send completed; task awaits its future
        self.feed({'id': 'host-1', 'result': {'which': 'a'}})
        # One yield runs ONLY the reader (scheduled first): the response is
        # consumed (future done with its result) while the request task has
        # not resumed yet.
        await asyncio.sleep(0)
        task.cancel()            # delivered-result discard race window
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(len(client.tombstones), 0)
        self.assertFalse(client.failure)
        self.feed({'id': 'host-1', 'result': {'which': 'a-duplicate'}})
        await self.settle()
        self.assertIn('Unmatched protocol response', client.failure)

    async def test_cancel_before_first_late_frame_audits_it_once_reader_first(self):
        """Blocker 2, reader-first interleave: cancel schedules the request
        wakeup AFTER the already-fed late frame's reader wakeup, so the FIRST
        late frame must be recorded-and-consumed by the reader (duplicate
        False) and the request cleanup must not double-record."""
        client = self.make_client(tombstones=self.make_store())
        task = self.track(asyncio.create_task(client.request('method/a', {})))
        await asyncio.sleep(0)
        b_task = self.track(asyncio.create_task(client.request('method/b', {})))
        await asyncio.sleep(0)
        self.feed({'id': 'host-1', 'result': {'which': 'late-1'}})  # reader queued
        task.cancel()            # request wakeup queued AFTER the reader's
        await asyncio.sleep(0)   # reader consumes late-1, then cleanup runs
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(len(client.tombstones), 1)
        self.assertFalse(client.failure, client.failure)
        self.feed({'id': 'host-1', 'result': {'which': 'late-2'}})
        await self.settle()
        lines = self.wire_late_lines()
        self.assertEqual([line['duplicate'] for line in lines], [False, True])
        self.assertEqual([line['late_response_to'] for line in lines],
                         ['host-1', 'host-1'])
        self.assertEqual(len(client.tombstones), 1)
        # B is untouched by any of A's late traffic, and nothing was replayed.
        self.feed({'id': 'host-2', 'result': {'which': 'b'}})
        self.assertEqual(await asyncio.wait_for(b_task, 2), {'which': 'b'})
        self.assertFalse(client.failure)
        self.assertEqual([frame['method'] for frame in self.sent],
                         ['method/a', 'method/b'])

    async def test_cancel_before_first_late_frame_audits_it_once_cleanup_first(self):
        """Blocker 2, cleanup-first interleave: cancel runs the request
        cleanup BEFORE the fed late frame reaches the reader — the very same
        false,true duplicate accounting must result (unified path)."""
        client = self.make_client(tombstones=self.make_store())
        task = self.track(asyncio.create_task(client.request('method/a', {})))
        await asyncio.sleep(0)
        task.cancel()            # request cleanup queued first
        self.feed({'id': 'host-1', 'result': {'which': 'late-1'}})  # reader second
        await asyncio.sleep(0)   # cleanup records, then reader consumes late-1
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(len(client.tombstones), 1)
        self.feed({'id': 'host-1', 'error': {'code': -1, 'message': 'late-2'}})
        await self.settle()
        lines = self.wire_late_lines()
        self.assertEqual([line['duplicate'] for line in lines], [False, True])
        self.assertEqual(len(client.tombstones), 1)
        self.assertFalse(client.failure, client.failure)

    async def test_first_terminal_failure_is_preserved(self):
        """Minor 3: a later EOF/protocol error must not overwrite the first
        tombstone-eviction terminal failure."""
        client = self.make_client(tombstones=self.make_store(capacity=1))
        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(client.request('method/a', {}), 0.02)
        self.assertEqual(len(client.tombstones), 1)
        with self.assertRaises(asyncio.TimeoutError):
            await asyncio.wait_for(client.request('method/c', {}), 0.02)
        self.assertIn('capacity', client.failure)
        # A subsequent stream error (here: an unmatched unknown frame) must
        # not replace the recorded first cause.
        self.feed({'id': 'host-999', 'result': {}})
        await self.settle()
        self.assertIn('capacity', client.failure)
        self.assertNotIn('Unmatched', client.failure)


if __name__ == '__main__':
    unittest.main()
