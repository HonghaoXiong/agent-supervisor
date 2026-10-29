"""P3.1 synchronous-blocking matrix (P3 dispatch item 3, PLAN §P3 line 568).

Characterization points on the REAL resident run path
(``ResidentZCode.run_packet``/``_run_locked``), each blocked with a
controllable ``threading.Event`` while a SHORT deadline is in force and
``task.cancel()`` fires mid-block:

 1. pre_first_inventory — the first boundary scan (runs on the bounded
    boundary-scan executor: the await is cancellable, the executor THREAD
    is not — the honest non-cooperative-executor evidence).
 2. check_frozen        — the post-success frozen check (synchronous on
    the event loop: non-preemptible).
 3. check_after         — the finally-side after-inventory (synchronous).
 4. finalize            — the registry release (synchronous file write).
 5. record_write        — the run.json evidence write (synchronous).

Every gate has a bounded release (hold + hard cap + cleanup), so the
suite can never hang. Real wall time, cancel-request time, block
enter/release and the local outcome are recorded per point in
:data:`SYNC_POINT_RESULTS` (read by REPORT).

Structural conclusion (PLAN: it may NOT turn green while any synchronous
section or non-cooperatively-cancellable executor remains):
``whole_call_wall_time_gate = 'not_satisfied'`` — pinned by the summary
test at the bottom.

All fake: no CLI, no model, no network; the shared client is a double.
"""

import asyncio
import json
import tempfile
import threading
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from zcode_supervisor import zcode_resident
from zcode_supervisor.authority import Authority, inventory as real_inventory
from zcode_supervisor.zcode_resident import ResidentZCode

WHOLE_CALL_WALL_TIME_GATE = 'not_satisfied'

HOLD_S = 1.2          # how long each characterization point blocks
CANCEL_AFTER_S = 0.3  # when task.cancel() fires mid-block
DEADLINE_S = 0.8      # the short run deadline (expires mid-block)
HARD_CAP_S = 3.0      # the fixture's absolute release bound

SYNC_POINT_RESULTS = []   # per-point records (wall / cleanup / local)


class SyncGate:
    """A bounded, releasable block for one characterization point."""

    def __init__(self, label, hold_s=HOLD_S, hard_cap_s=HARD_CAP_S):
        self.label = label
        self.event = threading.Event()
        self.hold_s = hold_s
        self.hard_cap_s = hard_cap_s
        self.entered_at = None
        self.released_at = None
        self.hard_released = False
        self.thread_released_at = None

    def block(self):
        self.entered_at = time.monotonic()
        released = self.event.wait(timeout=self.hold_s)
        if not released:                    # the hold elapsed: normal path
            pass
        self.released_at = time.monotonic()

    def block_hard_bounded(self):
        """Block up to the hard cap regardless of the hold (safety net)."""
        self.entered_at = time.monotonic()
        self.event.wait(timeout=self.hard_cap_s)
        self.hard_released = True
        self.released_at = time.monotonic()

    def release(self):
        self.event.set()

    def held_s(self):
        if self.entered_at is None or self.released_at is None:
            return None
        return self.released_at - self.entered_at


# ---------------------------------------------------------------------------
# Minimal resident fixture (compact FakeClient double)
# ---------------------------------------------------------------------------

class SyncFakeClient:
    def __init__(self, owner, workspace, output, permission, config=None):
        self.owner = owner
        self.permission = permission
        self.events = []
        self.failure = None
        self.reader = None
        self.process = SimpleNamespace(pid=77, returncode=None)
        self.session_count = 0

    async def start(self):
        pass

    async def close(self):
        self.process.returncode = 0

    async def request(self, method, params):
        if method == 'session/create':
            self.session_count += 1
            return {'session': {'sessionId': f'sess_{self.session_count}',
                                'status': 'idle'},
                    'runtime': {'pendingRequestIds': []}}
        if method == 'session/subscribe':
            return {'eventSeq': 0}
        if method == 'session/read':
            sid = f'sess_{self.session_count}'
            return {'session': {'sessionId': sid, 'status': 'idle'},
                    'runtime': {'pendingRequestIds': []}}
        if method == 'session/send':
            sid = f'sess_{self.session_count}'
            turn = f'turn_{sid}_1'
            self.events.append({'type': 'turn.started', 'sessionId': sid,
                                'turnId': turn, 'seq': 1,
                                'payload': {'inputId': params['inputId']}})
            self.events.append({'type': 'turn.completed', 'sessionId': sid,
                                'turnId': turn, 'seq': 2,
                                'payload': {'resultType': 'success'}})
            return {}
        return {}


class SyncBlockingFixture(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        for name in ('work', 'state', 'cache'):
            (self.root / name).mkdir()
        self.config = SimpleNamespace(work_root=self.root / 'work',
                                      state_dir=self.root / 'state',
                                      cache_dir=self.root / 'cache',
                                      notify=False)

        def client_factory(workspace, output, permission, config):
            return SyncFakeClient(self, workspace, output, permission,
                                  config)

        self.resident = ResidentZCode(self.config, self.root / 'work',
                                      client_factory=client_factory)
        self.workspace = self.root / 'work' / 'wsA'
        self.workspace.mkdir()
        self.packet_path = self.root / 'packet.json'
        self.packet_path.write_text(json.dumps(
            {'workspace': str(self.workspace), 'prompt': 'Fixture work',
             'allowed_files': []}))
        self.out_path = self.root / 'work' / 'run.json'

    async def asyncTearDown(self):
        await self.resident.stop()

    # -- the driving harness -------------------------------------------

    def arm_cancel(self, task, delay_s=CANCEL_AFTER_S):
        """Fire task.cancel() from a timer thread onto the live loop."""
        record = {'fired_at': None}
        loop = asyncio.get_running_loop()

        def fire():
            record['fired_at'] = time.monotonic()
            loop.call_soon_threadsafe(task.cancel)
        timer = threading.Timer(delay_s, fire)
        timer.daemon = True
        timer.start()
        self.addCleanup(timer.cancel)
        return record

    async def drive(self, gate):
        """Run one blocked characterization point; record and return."""
        started = time.monotonic()
        task = asyncio.create_task(self.resident.run_packet(
            self.packet_path, self.out_path, timeout_s=DEADLINE_S))
        cancel = self.arm_cancel(task)
        caller = {'saw': None}
        try:
            caller['result'] = await task
            caller['saw'] = 'returned'
        except asyncio.CancelledError:
            caller['saw'] = 'cancelled'
        except Exception as error:  # noqa: BLE001 - recorded honestly
            caller['saw'] = f'error:{type(error).__name__}'
            caller['error'] = str(error)
        wall = time.monotonic() - started
        run_json = None
        if self.out_path.is_file():
            run_json = json.loads(self.out_path.read_text())
        record = {
            'point': gate.label,
            'wall_s': round(wall, 3),
            'deadline_s': DEADLINE_S,
            'cancel_fired_after_s': (round(cancel['fired_at'] - started, 3)
                                     if cancel['fired_at'] else None),
            'block_entered_after_s': (round(gate.entered_at - started, 3)
                                      if gate.entered_at else None),
            'block_held_s': (round(gate.held_s(), 3)
                             if gate.held_s() is not None else None),
            'run_json_written': run_json is not None,
            'delivery_ok': bool(run_json and run_json.get('delivery_ok')),
            'caller_saw': caller['saw'],
            'deadline_exceeded_during_block': bool(
                gate.entered_at is not None
                and wall > DEADLINE_S),
        }
        SYNC_POINT_RESULTS.append(record)
        return record, caller, run_json

    def assert_loop_block_completed(self, record, caller, run_json):
        """The common structural facts for a LOOP-thread sync block."""
        # the sync section ran to completion despite the pending cancel
        self.assertIsNotNone(record['block_held_s'])
        self.assertGreaterEqual(record['block_held_s'], HOLD_S - 0.35,
                                f'{record["point"]}: the block did not hold')
        self.assertGreaterEqual(record['wall_s'], HOLD_S - 0.35,
                                f'{record["point"]}: wall {record["wall_s"]}s '
                                f'below the hold {HOLD_S}s — the sync section '
                                f'was preempted (gate would be satisfiable!)')
        # the cancel was requested DURING the block
        self.assertIsNotNone(record['cancel_fired_after_s'])
        self.assertLess(record['cancel_fired_after_s'], record['wall_s'])
        # the run deadline (0.8 s) elapsed while the block held (1.2 s)
        self.assertTrue(record['deadline_exceeded_during_block'],
                        f'{record["point"]}: deadline did not elapse in-block')
        # the synchronous work still completed: the evidence was written
        self.assertTrue(record['run_json_written'],
                        f'{record["point"]}: run.json missing')
        self.assertTrue(record['delivery_ok'])
        # the caller-visible surface (a pending cancel may surface either
        # way; both are recorded — the structural fact is above)
        self.assertIn(caller['saw'], ('cancelled', 'returned'))


# ---------------------------------------------------------------------------
# Point 1: before the first inventory (executor thread — cancellable await,
# non-cancellable worker)
# ---------------------------------------------------------------------------

class Point1PreFirstInventoryTests(SyncBlockingFixture):
    async def test_cancel_returns_early_but_executor_thread_holds(self):
        gate = SyncGate('pre_first_inventory')
        self.addCleanup(gate.release)
        calls = {'n': 0}

        def inventory_wrapper(workspace):
            calls['n'] += 1
            if calls['n'] == 1:
                # record the executor-thread release time separately
                gate.block()
                gate.thread_released_at = gate.released_at
            return real_inventory(workspace)

        patcher = mock.patch.object(zcode_resident, 'inventory',
                                    inventory_wrapper)
        patcher.start()
        self.addCleanup(patcher.stop)
        await self.resident.start()

        record, caller, run_json = await self.drive(gate)
        # wait (bounded) for the executor thread's own release
        deadline = time.monotonic() + HARD_CAP_S + 1.0
        while gate.thread_released_at is None and time.monotonic() < deadline:
            await asyncio.sleep(0.02)
        # the caller's await WAS cancellable: it ended long before the
        # blocked scan released (no whole-call bound)
        self.assertEqual(caller['saw'], 'cancelled')
        self.assertLess(record['wall_s'], HOLD_S - 0.35,
                        'the caller waited for the blocked executor scan')
        # nothing was dispatched and no evidence exists for this run
        self.assertFalse(record['run_json_written'])
        self.assertEqual(self.resident.client.session_count, 0)
        # the executor THREAD, however, could not be cancelled: it held
        # the full window (bounded release only)
        self.assertIsNotNone(gate.thread_released_at)
        held = gate.thread_released_at - gate.entered_at
        self.assertGreaterEqual(held, HOLD_S - 0.35,
                                'the executor thread was cooperatively '
                                'cancelled (the gate would be satisfiable!)')
        record['executor_thread_held_s'] = round(held, 3)
        record['structural'] = ('await cancellable; executor thread NOT '
                                'cooperatively cancellable')


# ---------------------------------------------------------------------------
# Point 2: the post-success check_frozen (synchronous on the loop)
# ---------------------------------------------------------------------------

class Point2CheckFrozenTests(SyncBlockingFixture):
    async def test_sync_check_frozen_blocks_past_deadline_and_cancel(self):
        gate = SyncGate('check_frozen')
        self.addCleanup(gate.release)
        calls = {'n': 0}
        original = Authority.check_frozen

        def frozen_wrapper(self):
            calls['n'] += 1
            if calls['n'] == 2:      # the post-success check
                gate.block()
            return original(self)

        patcher = mock.patch.object(zcode_resident.Authority, 'check_frozen',
                                    frozen_wrapper)
        patcher.start()
        self.addCleanup(patcher.stop)
        await self.resident.start()

        record, caller, run_json = await self.drive(gate)
        self.assertGreaterEqual(calls['n'], 2)  # the blocked call ran
        self.assert_loop_block_completed(record, caller, run_json)
        record['structural'] = 'synchronous check_frozen is non-preemptible'


# ---------------------------------------------------------------------------
# Point 3: the finally-side after-inventory (synchronous on the loop)
# ---------------------------------------------------------------------------

class Point3CheckAfterTests(SyncBlockingFixture):
    async def test_sync_after_inventory_blocks_past_deadline_and_cancel(self):
        gate = SyncGate('check_after')
        self.addCleanup(gate.release)
        calls = {'n': 0}

        def inventory_wrapper(workspace):
            calls['n'] += 1
            # calls 1-3 are executor-thread boundary scans (before-scan +
            # two provide_scans); call 4 is the finally's DIRECT
            # after-inventory — synchronous on the event loop
            if calls['n'] == 4:
                gate.block()
            return real_inventory(workspace)

        patcher = mock.patch.object(zcode_resident, 'inventory',
                                    inventory_wrapper)
        patcher.start()
        self.addCleanup(patcher.stop)
        await self.resident.start()

        record, caller, run_json = await self.drive(gate)
        self.assert_loop_block_completed(record, caller, run_json)
        self.assertGreaterEqual(calls['n'], 4)
        record['structural'] = ('synchronous finally-side after-inventory '
                                'is non-preemptible')


# ---------------------------------------------------------------------------
# Point 4: the registry release (session finalize; synchronous)
# ---------------------------------------------------------------------------

class Point4FinalizeTests(SyncBlockingFixture):
    async def test_sync_registry_release_blocks_past_deadline_and_cancel(self):
        gate = SyncGate('finalize')
        self.addCleanup(gate.release)
        original = zcode_resident.SessionRegistry.release

        def release_wrapper(self, session_id, last_seq):
            gate.block()
            return original(self, session_id, last_seq)

        patcher = mock.patch.object(zcode_resident.SessionRegistry,
                                    'release', release_wrapper)
        patcher.start()
        self.addCleanup(patcher.stop)
        await self.resident.start()

        record, caller, run_json = await self.drive(gate)
        self.assert_loop_block_completed(record, caller, run_json)
        record['structural'] = ('synchronous registry finalize is '
                                'non-preemptible')


# ---------------------------------------------------------------------------
# Point 5: the run.json record write (synchronous)
# ---------------------------------------------------------------------------

class Point5RecordWriteTests(SyncBlockingFixture):
    async def test_sync_record_write_blocks_past_deadline_and_cancel(self):
        gate = SyncGate('record_write')
        self.addCleanup(gate.release)
        original_write = zcode_resident.write_json

        def write_wrapper(path, value):
            if Path(path).resolve() == self.out_path.resolve():
                gate.block()
            return original_write(path, value)

        patcher = mock.patch.object(zcode_resident, 'write_json',
                                    write_wrapper)
        patcher.start()
        self.addCleanup(patcher.stop)
        await self.resident.start()

        record, caller, run_json = await self.drive(gate)
        self.assert_loop_block_completed(record, caller, run_json)
        record['structural'] = 'synchronous record write is non-preemptible'


# ---------------------------------------------------------------------------
# Summary: the structural conclusion the acceptance must keep
# ---------------------------------------------------------------------------

class WholeCallWallTimeGateTests(unittest.TestCase):
    def test_gate_stays_not_satisfied_while_sync_sections_exist(self):
        """The five characterization points ran earlier in this process
        (alphabetical class order) and each demonstrated a synchronous
        section (or a non-cooperatively-cancellable executor thread) that
        a short deadline + task.cancel() could not preempt: run wall
        exceeded the deadline with the cancel pending, and the blocked
        work still completed. While any such section remains, the
        whole-call hard wall-time gate MUST stay not_satisfied — passing
        async-RPC subtests elsewhere can never flip this conclusion."""
        self.assertEqual(len(SYNC_POINT_RESULTS), 5,
                         f'expected all five points recorded, got '
                         f'{[record["point"] for record in SYNC_POINT_RESULTS]}')
        loop_points = [record for record in SYNC_POINT_RESULTS
                       if record['point'] != 'pre_first_inventory']
        self.assertEqual(len(loop_points), 4)
        for record in loop_points:
            self.assertGreater(record['wall_s'], record['deadline_s'],
                               f'{record["point"]}: wall did not exceed the '
                               f'deadline — the gate conclusion would not '
                               f'be evidenced')
            # repair1 N4: file EXISTENCE alone is not evidence —
            # run_packet drops a delivery_ok=False placeholder at dispatch
            # time, so the summary must verify the FINAL synchronous
            # write actually completed (quality review M8: a preemptible
            # tail leaves the placeholder on disk with the point cells
            # red; this summary cell must go red on the same shape too)
            self.assertTrue(record['run_json_written'])
            self.assertTrue(record['delivery_ok'],
                            f'{record["point"]}: run.json exists but is the '
                            f'dispatch-time placeholder (delivery_ok false) '
                            f'— the synchronous evidence write did not '
                            f'complete')
        executor_point = next(record for record in SYNC_POINT_RESULTS
                              if record['point'] == 'pre_first_inventory')
        self.assertGreaterEqual(executor_point['executor_thread_held_s'],
                                HOLD_S - 0.35)
        # the structural conclusion itself
        self.assertEqual(WHOLE_CALL_WALL_TIME_GATE, 'not_satisfied')

    def test_every_gate_released_within_the_hard_bound(self):
        for record in SYNC_POINT_RESULTS:
            wall = record['wall_s']
            self.assertLess(wall, HARD_CAP_S + 2.0,
                            f'{record["point"]}: bounded release violated '
                            f'(wall {wall}s)')


if __name__ == '__main__':
    unittest.main()
