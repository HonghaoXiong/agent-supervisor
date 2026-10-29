"""P0.1 deadline/cancel-cleanup contracts for the two resident drivers.

Behavioral RED first: each defect-marked test reproduces a specific verified
defect (PLAN §1.4 COMM-01) on the CURRENT drivers — a response timeout that
does not cover ``stdin.drain()``, a ``turn/start`` timeout that bypasses
``turn/interrupt``, an outer ``asyncio.CancelledError`` that bypasses
interrupt/stop cleanup, and a ZCode deadline that starts only after the
synchronous preflight inventory (budget burned by synchronous sections is
unaccounted). No test here is an import or syntax check of a new module.

The synchronous pre/post/finally sections are characterized as NOT
preemptible: each blocking probe records ``total_wall_ms`` and pins
``whole_call_wall_time_gate = "not_satisfied"``; no whole-call wall-time bound
is asserted anywhere (PLAN §Global Constraints / §9.7).

No live CLI, node process, network, or model call: the Codex side drives the
production driver against an in-process scripted client plus one local
subprocess double for the real stdio transport; the ZCode side drives the
production ResidentZCode against an in-process fake client factory.
"""

import asyncio
import json
import os
import signal
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from zcode_supervisor.codex_appserver import (CodexAppServerClient,
                                              CodexAppServerDriver,
                                              SimpleContractAuthority)
from zcode_supervisor.zcode_resident import ResidentZCode

GATE_NOT_SATISFIED = 'not_satisfied'
SYNC_BLOCK_S = 0.2          # review probe shape: 0.20 s synchronous block
RECORDED_WALLS = []         # total_wall_ms records printed into the run logs


def _record_wall(label, elapsed_s, **extra):
    record = {'probe': label, 'total_wall_ms': round(elapsed_s * 1000.0, 3),
              'whole_call_wall_time_gate': GATE_NOT_SATISFIED}
    record.update(extra)
    RECORDED_WALLS.append(record)
    print(json.dumps(record), flush=True)


# ---------------------------------------------------------------------------
# Codex side: in-process scripted client (production driver, fake transport)
# ---------------------------------------------------------------------------

class ScriptedCodexClient:
    """Duck-typed CodexAppServerClient double for CodexAppServerDriver.

    ``sent`` records the REAL request boundary (which RPCs the driver actually
    issued, in order); scripted delays surface to the driver as
    ``asyncio.TimeoutError`` through the same wait_for(timeout) contract the
    production client applies to the response exchange.
    """

    THREAD_ID = 'th_deadline_1'
    TURN_ID = 'turn_deadline_1'

    def __init__(self, root, script=None):
        self.script = dict(script or {})
        self.evidence_dir = Path(root) / 'evidence'
        self.evidence_dir.mkdir(parents=True, exist_ok=True)
        self.frames_path = self.evidence_dir / 'appserver-frames.jsonl'
        self.notifications = []
        self.failure = None
        self.closing = False
        self.sent = []
        self.interrupts = []
        self._request_handlers = []
        self._notification_handlers = []
        self._tasks = []

    # -- subscriber model (same surface the driver registers through) -------

    def add_request_handler(self, fn):
        self._request_handlers.append(fn)

        def unsubscribe():
            try:
                self._request_handlers.remove(fn)
            except ValueError:
                pass
        return unsubscribe

    def add_notification_handler(self, fn):
        self._notification_handlers.append(fn)

        def unsubscribe():
            try:
                self._notification_handlers.remove(fn)
            except ValueError:
                pass
        return unsubscribe

    # -- fake wire -----------------------------------------------------------

    def _notify(self, method, params):
        self.notifications.append({'method': method, 'params': params})
        for handler in list(self._notification_handlers):
            try:
                handler(method, params)
            except Exception:
                pass

    def _spawn_turn(self, turn_id):
        async def flow():
            await asyncio.sleep(0.01)
            if self.script.get('never_complete'):
                return
            late = self.script.get('late_completion_s')
            if late:
                # completion arrives only after the driver's budget is gone
                await asyncio.sleep(late)
            else:
                await asyncio.sleep(0.01)
            self._notify('item/completed', {
                'threadId': self.THREAD_ID, 'turnId': turn_id,
                'item': {'type': 'agentMessage', 'text': 'done'}})
            self._notify('turn/completed', {
                'threadId': self.THREAD_ID,
                'turn': {'id': turn_id, 'status': 'completed'}})
        self._tasks.append(asyncio.create_task(flow()))

    async def _handle(self, method, params, on_written=None):
        def written():
            # The TRUE wire boundary (review quality-4): the frame write was
            # accepted — not the request entry. A send failure below never
            # reaches this mark.
            self.sent.append((method, dict(params)))
            if on_written is not None:
                on_written()

        if method == 'thread/start' and self.script.get('fail_thread_send'):
            raise EOFError('Codex app-server client is not running')
        written()  # every accepted frame lands on the wire here
        if method == 'thread/start':
            delay = self.script.get('thread_start_delay_s')
            if delay:
                await asyncio.sleep(delay)
            return {'thread': {'id': self.THREAD_ID}}
        if method == 'turn/start':
            if self.script.get('turn_start_valueerror'):
                # The frame was accepted and the turn began server-side; its
                # live id arrives via a notification, then the exchange fails
                # with an ordinary exception (review spec-C2 shape).
                self._notify('turn/started', {'threadId': self.THREAD_ID,
                                              'turnId': self.TURN_ID})
                raise ValueError('fake turn/start business failure')
            self._spawn_turn(self.TURN_ID)
            delay = self.script.get('turn_start_delay_s')
            if delay:
                await asyncio.sleep(delay)
            return {'turn': {'id': self.TURN_ID, 'status': 'inProgress'}}
        if method == 'turn/interrupt':
            self.interrupts.append(dict(params))
            if self.script.get('fail_interrupt'):
                raise ValueError('interrupt rejected by fake')
            return {}
        if method == 'thread/read':
            return {'thread': {'id': self.THREAD_ID, 'turns': []},
                    'readState': {}}
        return {}

    async def request(self, method, params, timeout=None, on_written=None):
        return await asyncio.wait_for(
            self._handle(method, params, on_written), timeout)

    async def settle(self):
        # Give pending scripted flows (e.g. a LATE completion) their bounded
        # moment to land before cancelling whatever is still outstanding.
        if self._tasks:
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self._tasks, return_exceptions=True), 1.0)
            except asyncio.TimeoutError:
                pass
        for task in self._tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()


class CodexDriverFixture(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='czs deadline codex ')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()

    async def _drive(self, script, timeout_s):
        client = ScriptedCodexClient(self.root, script)
        authority = SimpleContractAuthority([], [])
        receipt = await CodexAppServerDriver(client).run_task(
            authority, self.root / 'ws', 'do the deadline work',
            timeout_s=timeout_s)
        return receipt, client

    async def _drive_cancellable(self, script, timeout_s, cancel_after_s,
                                 cancel_again_after_s=None):
        client = ScriptedCodexClient(self.root, script)
        authority = SimpleContractAuthority([], [])
        driver = CodexAppServerDriver(client)
        task = asyncio.ensure_future(driver.run_task(
            authority, self.root / 'ws', 'cancellable work',
            timeout_s=timeout_s))
        await asyncio.sleep(cancel_after_s)
        task.cancel()
        if cancel_again_after_s is not None:
            await asyncio.sleep(cancel_again_after_s - cancel_after_s)
            task.cancel()
        raised = None
        try:
            await task
        except asyncio.CancelledError:
            raised = True
        await client.settle()
        return raised, task, client


class CodexNormalPathTests(CodexDriverFixture):
    async def test_normal_drain_start_turn_completes_without_interrupt(self):
        receipt, client = await self._drive({}, timeout_s=5)
        self.assertEqual(receipt['outcome'], 'delivered')
        self.assertEqual(receipt['thread_id'], ScriptedCodexClient.THREAD_ID)
        self.assertEqual(receipt['turn_id'], ScriptedCodexClient.TURN_ID)
        self.assertEqual(receipt['final_message'], 'done')
        # Real send boundary: start -> turn -> the one evidence read; no
        # interrupt was ever issued on a clean path, and the driver's
        # subscriptions left the shared client.
        self.assertEqual([method for method, _ in client.sent],
                         ['thread/start', 'turn/start', 'thread/read'])
        self.assertEqual(client.interrupts, [])
        self.assertEqual((len(client._request_handlers),
                          len(client._notification_handlers)), (0, 0))


class CodexStartBoundaryTests(CodexDriverFixture):
    async def test_start_send_failure_is_failed_and_never_interrupts(self):
        # The transport is dead before thread/start: no frame write was ever
        # accepted (nothing on the wire), so no interrupt may be fabricated.
        receipt, client = await self._drive({'fail_thread_send': True},
                                            timeout_s=5)
        self.assertEqual(receipt['outcome'], 'failed')
        self.assertIsNone(receipt['thread_id'])
        self.assertEqual(client.sent, [])  # the write never happened
        self.assertEqual(client.interrupts, [])

    async def test_start_response_timeout_is_timeout_and_never_interrupts(self):
        # thread/start was written but its response is withheld: without a thread
        # id there is nothing to interrupt (late-response handling is P0.2).
        receipt, client = await self._drive({'thread_start_delay_s': 0.5},
                                            timeout_s=0.15)
        self.assertEqual(receipt['outcome'], 'timeout')
        self.assertIsNone(receipt['thread_id'])
        self.assertEqual([method for method, _ in client.sent],
                         ['thread/start'])
        self.assertEqual(client.interrupts, [])
        await client.settle()


class CodexTurnStartTimeoutTests(CodexDriverFixture):
    async def test_turn_start_written_without_id_never_fabricates_interrupt(self):
        # REVIEW spec-C1 (RED): the turn/start frame WAS accepted on the wire
        # but its response never brought a turn id — the old code still sent
        # turn/interrupt with turnId=None. No known id means NO interrupt;
        # the unclean state is recorded and left to P0.2 tombstones.
        receipt, client = await self._drive(
            {'turn_start_delay_s': 0.5, 'never_complete': True}, timeout_s=0.15)
        self.assertEqual(receipt['outcome'], 'timeout')
        self.assertEqual([method for method, _ in client.sent],
                         ['thread/start', 'turn/start'])
        self.assertEqual(client.interrupts, [])
        self.assertEqual(receipt['interrupt'], 'unclean-no-turn-id')
        await client.settle()

    async def test_plain_exception_after_established_turn_interrupts_once(self):
        # REVIEW spec-C2 (RED): the turn/start frame was accepted, the live
        # turn id is known, and then an ORDINARY ValueError fails the run —
        # a bounded best-effort turn/interrupt must still happen exactly
        # once, with the original failure preserved in the receipt.
        receipt, client = await self._drive(
            {'turn_start_valueerror': True}, timeout_s=2)
        self.assertEqual(receipt['outcome'], 'failed')
        self.assertIn('fake turn/start business failure', receipt['error'])
        # the budget-covered evidence read still runs after the cleanup
        self.assertEqual([method for method, _ in client.sent],
                         ['thread/start', 'turn/start', 'turn/interrupt',
                          'thread/read'])
        self.assertEqual(client.interrupts,
                         [{'threadId': ScriptedCodexClient.THREAD_ID,
                           'turnId': ScriptedCodexClient.TURN_ID}])
        self.assertEqual(receipt['interrupt'], 'sent')
        await client.settle()


class CodexCancelTests(CodexDriverFixture):
    async def test_cancel_during_poll_interrupts_and_reraises(self):
        # DEFECT (RED): an outer CancelledError is a BaseException — the old
        # except tuple never saw it, so turn/interrupt was bypassed entirely.
        raised, task, client = await self._drive_cancellable(
            {'never_complete': True}, timeout_s=3, cancel_after_s=0.08)
        self.assertTrue(raised)  # the cancellation re-raised to the caller
        self.assertEqual(client.interrupts,
                         [{'threadId': ScriptedCodexClient.THREAD_ID,
                           'turnId': ScriptedCodexClient.TURN_ID}])

    async def test_cancel_during_turn_start_await_never_fabricates_interrupt(self):
        # REVIEW spec-C1 (RED): cancelled while awaiting the turn/start
        # response — the frame was written, but without a turn id no
        # interrupt may be fabricated (turnId=None is not a cleanup).
        raised, task, client = await self._drive_cancellable(
            {'turn_start_delay_s': 5.0, 'never_complete': True},
            timeout_s=3, cancel_after_s=0.08)
        self.assertTrue(raised)
        self.assertEqual([method for method, _ in client.sent],
                         ['thread/start', 'turn/start'])
        self.assertEqual(client.interrupts, [])


class CodexCleanupBoundaryTests(CodexDriverFixture):
    async def test_interrupt_failure_is_bounded_and_stays_timeout(self):
        receipt, client = await self._drive(
            {'never_complete': True, 'fail_interrupt': True}, timeout_s=0.15)
        self.assertEqual(receipt['outcome'], 'timeout')
        self.assertEqual(len(client.interrupts), 1)  # exactly one attempt

    async def test_late_completion_is_not_promoted_to_success(self):
        started = time.monotonic()
        receipt, client = await self._drive({'late_completion_s': 0.4},
                                            timeout_s=0.12)
        elapsed = time.monotonic() - started
        self.assertLess(elapsed, 1.0)  # the driver returned within budget
        self.assertEqual(receipt['outcome'], 'timeout')
        self.assertIsNotNone(receipt.get('interrupt') or client.interrupts)
        await client.settle()
        # The late terminal was observed on the stream afterwards, but the
        # already-returned receipt stays a timeout — no fabricated success.
        self.assertIn('turn/completed',
                      [frame['method'] for frame in client.notifications])
        self.assertEqual(receipt['outcome'], 'timeout')


# ---------------------------------------------------------------------------
# Codex side: the REAL client transport against a local subprocess double
# ---------------------------------------------------------------------------

# Answers initialize, then deliberately stops reading stdin: the next large
# client frame genuinely blocks in StreamWriter.drain() on OS pipe
# backpressure (same mechanism as the P0.0 block-drain fixture), then the
# child exits so an unbounded implementation still terminates for the test.
DRAIN_CHILD = '''#!/usr/bin/env python3
import json, sys, time
def send(frame):
    sys.stdout.write(json.dumps(frame) + '\\n')
    sys.stdout.flush()
for line in sys.stdin:
    frame = json.loads(line)
    if frame.get('method') == 'initialize':
        send({'id': frame['id'],
              'result': {'codexHome': '/tmp/fake', 'platformFamily': 'unix',
                         'platformOs': 'macos', 'userAgent': 'drain-fake/1'}})
        break
time.sleep(2.0)
sys.exit(0)
'''


class RealTransportDeadlineTests(unittest.TestCase):
    def test_request_timeout_covers_blocked_drain(self):
        # DEFECT (RED): the response timeout did not cover stdin.drain(); a
        # blocked drain outlived the caller's deadline unbounded.
        temporary = tempfile.TemporaryDirectory(prefix='czs deadline drain ')
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name).resolve()
        fake = root / 'drain-fake'
        fake.write_text(DRAIN_CHILD)
        fake.chmod(0o755)

        async def scenario():
            client = CodexAppServerClient(str(fake), root / 'evidence')
            try:
                await client.start()
                big = {'cwd': 'x' * 400000, 'sandbox': 'read-only',
                       'approvalPolicy': 'on-request', 'ephemeral': False}
                started = time.monotonic()
                try:
                    await client.request('thread/start', big, timeout=0.5)
                    outcome = 'returned'
                except asyncio.TimeoutError:
                    outcome = 'timeout-raised'
                except (EOFError, OSError):
                    outcome = 'transport-error'
                return outcome, time.monotonic() - started
            finally:
                # Kill the session leader first so close() does not wait out
                # its full 5 s SIGTERM grace while the child sleeps.
                if client.process is not None:
                    try:
                        os.killpg(os.getpgid(client.process.pid),
                                  signal.SIGKILL)
                    except (ProcessLookupError, PermissionError, OSError):
                        pass
                await client.close()

        outcome, elapsed = asyncio.run(scenario())
        self.assertEqual(outcome, 'timeout-raised')
        self.assertLess(elapsed, 1.5)
        _record_wall('codex-blocked-drain-bounded', elapsed,
                     bound_s=0.5)


# A fake app-server child for the CLI `run` path: initialize is delayed long
# enough that ONLY a shared primary budget (initialize + run) times the turn
# out; with two separate full budgets the turn completes and delivers.
class CliSharedBudgetTests(unittest.TestCase):
    """CLI-level shared-budget contract — deterministic round-2 rewrite.

    The first version drove a real child process whose 1.00 s initialize
    sleep raced the 1.5 s total budget on the real wall clock: under load
    the spawn+handshake overhead ate the remaining 0.5 s INSIDE
    ``client.start(budget=budget)``, the run died as
    ``{'run_ok': False, 'error': 'TimeoutError'}`` before any 'outcome'
    existed, and ``assertIn('outcome', summary)`` flaked (independent
    quality review: 60 runs, 1 failure, traceback at test_deadline.py:477).
    That was a test-harness wall-clock race, not a P0.2 production
    regression. The causal contract — ONE budget object shared by
    initialize and run_task, initialize COMPLETES, the run phase then
    exceeds what is left — is now pinned with an in-process client double
    and an injected fake clock: no spawn jitter, no wall-clock sleep ever
    decides the outcome, and the same ScriptedCodexClient completion flow
    CAN deliver (control test) so the timeout is proven to come from the
    shared budget, not from a broken fake.
    """

    def _run_cli(self, *, initialize_consumes_s):
        import contextlib
        import io
        from zcode_supervisor import codex_appserver
        from zcode_supervisor.codex_appserver import main
        clock = FakeClock(start=0.0)
        real_budget_cls = codex_appserver.CallBudget
        created = []

        def budget_factory(timeout_s, **kwargs):
            budget = real_budget_cls(timeout_s, clock=clock)
            created.append(budget)
            return budget

        class SharedBudgetClient(ScriptedCodexClient):
            instances = []

            def __init__(self, codex, out):
                super().__init__(Path(out))
                self.start_budgets = []
                SharedBudgetClient.instances.append(self)

            async def start(self, budget=None, initialize_timeout=None):
                self.start_budgets.append(budget)
                if initialize_consumes_s:
                    # initialize COMPLETES, deterministically consuming part
                    # of the ONE shared budget (1.0 of the 1.5 s total).
                    clock.advance(initialize_consumes_s)
                return {'userAgent': 'shared-budget/2', 'codexHome': '/tmp/fake',
                        'platformFamily': 'unix', 'platformOs': 'macos'}

            async def request(self, method, params, timeout=None, on_written=None):
                if method == 'turn/start':
                    # Run-phase exchange; by the time the driver polls, the
                    # SHARED budget is past its deadline (0.0 + 1.0 + 0.6).
                    clock.advance(0.6)
                return await super().request(method, params, timeout, on_written)

            async def close(self):
                await self.settle()

        SharedBudgetClient.instances = []
        temporary = tempfile.TemporaryDirectory(prefix='czs cli budget ')
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name).resolve()
        (root / 'ws').mkdir()
        buffer = io.StringIO()
        with mock.patch.object(codex_appserver, 'CallBudget', budget_factory), \
                mock.patch.object(codex_appserver, 'CodexAppServerClient',
                                  SharedBudgetClient):
            with contextlib.redirect_stdout(buffer):
                code = main(['run', '--codex', 'unused-fake-cli', '--workspace',
                             str(root / 'ws'), '--prompt', 'budget probe',
                             '--out', str(root / 'evidence'),
                             '--timeout-s', '1.5'])
        summary = json.loads(buffer.getvalue().strip().splitlines()[-1])
        clients = SharedBudgetClient.instances
        return code, summary, created, clients

    def test_initialize_and_run_task_share_one_primary_budget(self):
        code, summary, created, clients = self._run_cli(initialize_consumes_s=1.0)
        # The run reached the driver (not a boot error) and timed out INSIDE
        # the run phase — initialize completed first.
        self.assertIn('outcome', summary)
        self.assertEqual(code, 1)
        self.assertEqual(summary['outcome'], 'timeout')
        self.assertEqual(summary['thread_id'], 'th_deadline_1')
        self.assertEqual(summary['turn_id'], 'turn_deadline_1')
        # ONE budget object for the whole CLI run, and it is THE SAME object
        # handed to client.start (lazy initialize) — a fresh run-phase budget
        # would expire at fake-time 2.5 and deliver instead (control below).
        self.assertEqual(len(created), 1)
        self.assertEqual(len(clients), 1)
        self.assertEqual(clients[0].start_budgets, [created[0]])

    def test_control_same_fake_delivers_when_budget_is_not_consumed(self):
        """The fake's turn CAN complete: without the initialize consumption
        the same clock/exchange delivers, proving the shared-budget timeout
        above is causal, not a fixture artifact."""
        code, summary, created, _ = self._run_cli(initialize_consumes_s=0.0)
        self.assertEqual(code, 0)
        self.assertEqual(summary['outcome'], 'delivered')
        self.assertEqual(summary['thread_id'], 'th_deadline_1')
        self.assertEqual(len(created), 1)


# ---------------------------------------------------------------------------
# ZCode side: production ResidentZCode against an in-process fake client
# ---------------------------------------------------------------------------

class FakeResidentServer:
    """Scripted app-server behaviour shared by the resident fake clients."""

    def __init__(self):
        self.calls = []
        self.stops = []
        self.stop_delay_s = 0.0
        self.fail_stop = False
        self.fail_create = False
        self.create_delay_s = 0.0
        self.subscribe_delay_s = 0.0
        self.never_complete = set()
        self.fail_turn = set()
        self.resume_returns = None   # observed sessionId for session/resume
        self.created = []
        self.seq = 0
        self.script_tasks = []

    def new_session(self):
        sid = f'sess_{chr(ord("A") + len(self.created))}'
        self.created.append(sid)
        return sid

    def snapshot(self, sid):
        return {'session': {'sessionId': sid, 'status': 'idle'},
                'runtime': {'pendingRequestIds': []}}

    def emit(self, client, sid, event_type, payload, turn):
        self.seq += 1
        client.events.append({'type': event_type, 'sessionId': sid,
                              'turnId': turn, 'seq': self.seq,
                              'payload': payload})

    async def script(self, client, sid, input_id):
        turn = f'turn_{sid}_1'
        self.emit(client, sid, 'turn.started', {'inputId': input_id}, turn)
        if sid in self.fail_turn:
            self.emit(client, sid, 'turn.failed', {'error': 'fixture failure'},
                      turn)
            return
        if sid in self.never_complete:
            return
        await asyncio.sleep(0.03)
        self.emit(client, sid, 'turn.completed', {'resultType': 'success'},
                  turn)


class FakeResidentClient:
    def __init__(self, owner, workspace, output, permission, config=None):
        self.owner = owner
        self.events = []
        self.permission = permission
        self.failure = None
        self.process = SimpleNamespace(pid=7777, returncode=None)

    async def start(self):
        pass

    async def close(self):
        self.process.returncode = 0

    async def request(self, method, params):
        owner = self.owner
        owner.calls.append((method, dict(params)))
        if method == 'session/create':
            if owner.fail_create:
                raise ValueError('fake create failure')
            if owner.create_delay_s:
                await asyncio.sleep(owner.create_delay_s)
            return owner.snapshot(owner.new_session())
        if method == 'session/subscribe':
            if owner.subscribe_delay_s:
                await asyncio.sleep(owner.subscribe_delay_s)
            return {'eventSeq': 0}
        if method == 'session/resume':
            sid = params.get('sessionId')
            if owner.resume_returns:
                sid = owner.resume_returns
            return owner.snapshot(sid)
        if method == 'session/send':
            owner.script_tasks.append(asyncio.create_task(
                owner.script(self, params['sessionId'], params['inputId'])))
            return {}
        if method == 'session/stop':
            owner.stops.append(params['sessionId'])
            if owner.stop_delay_s:
                await asyncio.sleep(owner.stop_delay_s)
            if owner.fail_stop:
                raise ValueError('fake stop failure')
            return {}
        if method in ('session/read', 'session/setMode', 'session/resume',
                      'session/setModel'):
            return owner.snapshot(params.get('sessionId', 'sess_A'))
        return {}


class ZFixture(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='czs deadline resident ')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        for name in ('work', 'state', 'cache', 'packets'):
            (self.root / name).mkdir()
        self.config = SimpleNamespace(work_root=self.root / 'work',
                                      state_dir=self.root / 'state',
                                      cache_dir=self.root / 'cache', notify=False)
        self.server = FakeResidentServer()

        def factory(workspace, output, permission, config):
            return FakeResidentClient(self.server, workspace, output,
                                      permission, config)

        self.resident = ResidentZCode(self.config, self.root / 'work',
                                      client_factory=factory)

    async def asyncTearDown(self):
        for task in self.server.script_tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*self.server.script_tasks, return_exceptions=True)
        await self.resident.stop()

    def workspace(self, name):
        ws = self.root / 'work' / name
        ws.mkdir()
        return ws

    def packet(self, name, ws):
        path = self.root / 'packets' / f'{name}.json'
        path.write_text(json.dumps({'workspace': str(ws),
                                    'prompt': 'Fixture work',
                                    'allowed_files': []}))
        return path

    def registry_records(self):
        return json.loads(
            (self.root / 'state' / 'hub' / 'resident-sessions.json').read_text())

    async def run_soon(self, packet, out, **kwargs):
        task = asyncio.ensure_future(
            self.resident.run_packet(packet, out, **kwargs))
        await asyncio.sleep(0)
        return task


class ZNormalPathTests(ZFixture):
    async def test_normal_run_creates_sends_reads_and_never_stops(self):
        ws = self.workspace('wsA')
        await self.resident.start()
        result = await self.resident.run_packet(
            self.packet('p1', ws), self.root / 'work' / 'runA.json', timeout_s=10)
        self.assertTrue(result['delivery_ok'])
        self.assertEqual(result['outcome'], 'delivered')
        self.assertEqual(result['session_id'], 'sess_A')
        # Real send boundary on the resident path: create -> subscribe ->
        # send -> post-turn read; a clean turn never issues session/stop.
        self.assertEqual([method for method, _ in self.server.calls],
                         ['session/create', 'session/subscribe',
                          'session/send', 'session/read'])
        self.assertEqual(self.server.stops, [])
        self.assertEqual(self.registry_records()['sess_A']['state'], 'exited')


class ZStartBoundaryTests(ZFixture):
    async def test_create_failure_without_id_never_stops(self):
        # ZCode "no id": session/create itself fails — no session exists, no
        # session/stop may be fabricated.
        self.server.fail_create = True
        ws = self.workspace('wsA')
        await self.resident.start()
        result = await self.resident.run_packet(
            self.packet('p1', ws), self.root / 'work' / 'runA.json', timeout_s=10)
        self.assertFalse(result['delivery_ok'])
        self.assertEqual(result['outcome'], 'preflight-rejected')
        self.assertNotIn('session_id', result)
        self.assertEqual(self.server.stops, [])

    async def test_create_response_timeout_without_id_never_stops(self):
        self.server.create_delay_s = 0.5
        ws = self.workspace('wsA')
        await self.resident.start()
        result = await self.resident.run_packet(
            self.packet('p1', ws), self.root / 'work' / 'runA.json',
            timeout_s=0.15)
        self.assertEqual(result['outcome'], 'timeout')
        self.assertNotIn('session_id', result)
        self.assertEqual(self.server.stops, [])

    async def test_turn_failure_with_id_stops_exactly_once(self):
        # ZCode "with id": the session is bound at session/create, so a
        # failing turn must stop that session — exactly once.
        self.server.fail_turn = {'sess_A'}
        ws = self.workspace('wsA')
        await self.resident.start()
        result = await self.resident.run_packet(
            self.packet('p1', ws), self.root / 'work' / 'runA.json', timeout_s=10)
        self.assertEqual(result['outcome'], 'worker-failed')
        self.assertEqual(self.server.stops, ['sess_A'])
        self.assertEqual(self.registry_records()['sess_A']['state'], 'lost')

    async def test_claim_failure_stops_the_bound_session(self):
        # REVIEW quality-3 (RED): session/create returned sess_A but the
        # registry claim refuses (the id already 'running'). The id must be
        # bound BEFORE the claim, so this failure still stops sess_A once.
        ws = self.workspace('wsA')
        await self.resident.start()
        self.resident.registry.claim('sess_A', 'other-task', str(ws), 4242)
        result = await self.resident.run_packet(
            self.packet('p1', ws), self.root / 'work' / 'runA.json', timeout_s=10)
        self.assertEqual(result['outcome'], 'preflight-rejected')
        self.assertIn('already running', result['error'])
        self.assertEqual(self.server.stops, ['sess_A'])
        self.assertEqual(result['cleanup_stop'], 'sent')

    async def test_resume_mismatch_stops_the_observed_session(self):
        # REVIEW quality-3 (RED): session/resume observed a DIFFERENT id
        # (sess_Z). The observed id is what runs server-side, so it must be
        # bound before the mismatch check and stopped on the failure.
        ws = self.workspace('wsA')
        await self.resident.start()
        self.resident.registry.claim('sess_A', 'task-1', str(ws), 4242)
        self.resident.registry.release('sess_A', 3)
        self.server.resume_returns = 'sess_Z'
        result = await self.resident.run_packet(
            self.packet('p1', ws), self.root / 'work' / 'runB.json',
            timeout_s=10, resume='sess_A')
        self.assertEqual(result['outcome'], 'preflight-rejected')
        self.assertIn('different session', result['error'])
        self.assertEqual(self.server.stops, ['sess_Z'])
        self.assertEqual(result['cleanup_stop'], 'sent')


class ZCancelCleanupTests(ZFixture):
    async def test_cancel_mid_poll_stops_session_and_reraises(self):
        # DEFECT (RED): outer CancelledError is a BaseException; the old
        # except tuple never saw it, so session/stop was bypassed entirely.
        self.server.never_complete = {'sess_A'}
        ws = self.workspace('wsA')
        await self.resident.start()
        out = self.root / 'work' / 'runA.json'
        task = await self.run_soon(self.packet('p1', ws), out, timeout_s=5)
        await asyncio.sleep(0.08)  # inside the poll loop now
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(self.server.stops, ['sess_A'])
        self.assertEqual(self.registry_records()['sess_A']['state'], 'lost')

    async def test_cancel_during_setup_rpc_stops_bound_session(self):
        # DEFECT (RED): cancelled while awaiting session/subscribe — the id
        # was already obtained from session/create, so the bound session must
        # receive a bounded session/stop.
        self.server.subscribe_delay_s = 5.0
        ws = self.workspace('wsA')
        await self.resident.start()
        out = self.root / 'work' / 'runA.json'
        task = await self.run_soon(self.packet('p1', ws), out, timeout_s=5)
        await asyncio.sleep(0.05)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual(self.server.stops, ['sess_A'])
        self.assertEqual(self.registry_records()['sess_A']['state'], 'lost')

    async def test_repeated_cancel_still_runs_cleanup_once_to_completion(self):
        # DEFECT (RED) + idempotency: two cancels while the stop RPC is slow —
        # the cleanup survives both, is issued exactly once, and completes.
        self.server.never_complete = {'sess_A'}
        self.server.stop_delay_s = 0.25
        ws = self.workspace('wsA')
        await self.resident.start()
        out = self.root / 'work' / 'runA.json'
        task = await self.run_soon(self.packet('p1', ws), out, timeout_s=5)
        await asyncio.sleep(0.08)
        cancel_started = time.monotonic()
        task.cancel()
        await asyncio.sleep(0.05)
        task.cancel()  # repeated cancel lands mid-cleanup
        with self.assertRaises(asyncio.CancelledError):
            await task
        elapsed = time.monotonic() - cancel_started
        self.assertEqual(self.server.stops, ['sess_A'])
        self.assertGreaterEqual(elapsed, 0.25)  # the stop itself finished
        _record_wall('zcode-repeated-cancel-cleanup', elapsed,
                     stop_delay_s=0.25)

    async def test_early_cancel_cleanup_window_is_capped_to_cleanup_budget(self):
        # REVIEW quality-2 (RED): the cleanup window must be
        # min(cleanup_budget_s, cleanup_remaining_s()) — the old code passed
        # the full remaining time, so an early cancel let a 0.25 s stop ride
        # a ~0.33 s window under a 0.05 s cleanup budget.
        from zcode_supervisor import deadline as deadline_module
        self.server.never_complete = {'sess_A'}
        self.server.stop_delay_s = 0.25
        ws = self.workspace('wsA')
        await self.resident.start()
        out = self.root / 'work' / 'runA.json'
        with mock.patch.object(deadline_module, 'CLEANUP_BUDGET_S', 0.05):
            task = await self.run_soon(self.packet('p1', ws), out, timeout_s=0.30)
            await asyncio.sleep(0.02)
            cancel_started = time.monotonic()
            task.cancel()
            with self.assertRaises(asyncio.CancelledError):
                await task
        elapsed = time.monotonic() - cancel_started
        self.assertLess(elapsed, 0.20)             # window cap, not +0.25 ride
        self.assertEqual(self.server.stops, ['sess_A'])  # one attempt made
        written = json.loads(out.read_text())
        self.assertEqual(written['cleanup_stop'], 'failed')  # cut at window
        _record_wall('zcode-early-cancel-capped-window', elapsed,
                     cleanup_budget_s=0.05)

    async def test_cleanup_failure_keeps_finally_exactly_once(self):
        # The stop RPC fails; the synchronous finally must still settle the
        # registry and write the run evidence exactly once.
        self.server.never_complete = {'sess_A'}
        self.server.fail_stop = True
        ws = self.workspace('wsA')
        await self.resident.start()
        out = self.root / 'work' / 'runA.json'
        writes = []
        from zcode_supervisor import zcode_resident as resident_module
        real_write = resident_module.write_json

        def counting_write(path, data):
            if Path(path) == out:
                writes.append(time.monotonic())
            return real_write(path, data)

        with mock.patch.object(resident_module, 'write_json', counting_write):
            result = await self.resident.run_packet(self.packet('p1', ws), out,
                                                    timeout_s=0.15)
        self.assertEqual(result['outcome'], 'timeout')
        self.assertEqual(self.server.stops, ['sess_A'])
        self.assertEqual(len(writes), 1)
        self.assertEqual(self.registry_records()['sess_A']['state'], 'lost')


class ZBudgetAccountingTests(ZFixture):
    async def test_preflight_inventory_consumes_the_call_budget(self):
        # DEFECT (RED): the deadline used to start only after the synchronous
        # preflight inventory; a blocked preflight therefore bought the run a
        # fresh budget afterwards. The budget must start at the call start,
        # so the first RPC boundary fails fast with no new send.
        from zcode_supervisor import zcode_resident as resident_module
        real_inventory = resident_module.inventory

        def blocked_inventory(workspace):
            time.sleep(0.3)
            return real_inventory(workspace)

        ws = self.workspace('wsA')
        await self.resident.start()
        with mock.patch.object(resident_module, 'inventory', blocked_inventory):
            result = await self.resident.run_packet(
                self.packet('p1', ws), self.root / 'work' / 'runA.json',
                timeout_s=0.05)
        self.assertEqual(result['outcome'], 'timeout')
        self.assertNotIn('session_id', result)
        self.assertEqual(self.server.calls, [])  # no new wait, no new send
        self.assertEqual(self.server.stops, [])

    async def test_packet_validation_consumes_the_call_budget(self):
        # REVIEW spec-I3 / quality-1 (RED): the budget must be created right
        # after the timeout validation — BEFORE file/packet/preflight work.
        # A synchronous validate_packet block under a smaller timeout must
        # leave NO budget for any RPC.
        from zcode_supervisor import protocol as protocol_module
        real_validate = protocol_module.validate_packet

        def blocked_validate(packet):
            time.sleep(0.20)
            return real_validate(packet)

        ws = self.workspace('wsA')
        await self.resident.start()
        with mock.patch.object(protocol_module, 'validate_packet',
                               blocked_validate):
            result = await self.resident.run_packet(
                self.packet('p1', ws), self.root / 'work' / 'runA.json',
                timeout_s=0.10)
        self.assertEqual(result['outcome'], 'timeout')
        self.assertNotIn('session_id', result)
        self.assertEqual(self.server.calls, [])
        self.assertEqual(self.server.stops, [])


class ZSyncSectionProbes(ZFixture):
    """Three synchronous non-preemptible characterizations (PLAN §7 P0.1).

    Each probe blocks a synchronous section for 0.20 s against a 0.05 s-class
    budget: the section is NOT preempted, the honest total wall time is
    recorded, and whole_call_wall_time_gate stays not_satisfied.
    """

    async def test_first_inventory_block_is_not_preempted(self):
        from zcode_supervisor import zcode_resident as resident_module
        real_inventory = resident_module.inventory

        def blocked_inventory(workspace):
            time.sleep(SYNC_BLOCK_S)
            return real_inventory(workspace)

        ws = self.workspace('wsA')
        await self.resident.start()
        started = time.monotonic()
        with mock.patch.object(resident_module, 'inventory', blocked_inventory):
            result = await self.resident.run_packet(
                self.packet('p1', ws), self.root / 'work' / 'runA.json',
                timeout_s=0.05)
        elapsed = time.monotonic() - started
        self.assertGreaterEqual(elapsed, SYNC_BLOCK_S)
        self.assertIn(result['outcome'], ('timeout', 'cancelled'))
        _record_wall('sync-first-inventory', elapsed, budget_s=0.05)

    async def test_post_success_check_frozen_block_is_not_preempted(self):
        from zcode_supervisor.authority import Authority
        original = Authority.check_frozen

        def slow_check_frozen(self, *args, **kwargs):
            time.sleep(SYNC_BLOCK_S)
            return original(self, *args, **kwargs)

        ws = self.workspace('wsA')
        await self.resident.start()
        started = time.monotonic()
        with mock.patch.object(Authority, 'check_frozen', slow_check_frozen):
            result = await self.resident.run_packet(
                self.packet('p1', ws), self.root / 'work' / 'runA.json',
                timeout_s=1.0)
        elapsed = time.monotonic() - started
        # The turn succeeded and the (three times blocked) synchronous audit
        # still ran to completion — a success receipt, not a preemption.
        self.assertTrue(result['delivery_ok'])
        self.assertGreaterEqual(elapsed, 3 * SYNC_BLOCK_S)
        _record_wall('sync-post-success-check-frozen', elapsed, budget_s=1.0)

    async def test_post_cancel_finally_write_is_not_preempted(self):
        from zcode_supervisor import zcode_resident as resident_module
        real_write = resident_module.write_json

        def blocked_final_write(path, data):
            if Path(path).name == 'runA.json':
                time.sleep(SYNC_BLOCK_S)
            return real_write(path, data)

        self.server.never_complete = {'sess_A'}
        ws = self.workspace('wsA')
        await self.resident.start()
        out = self.root / 'work' / 'runA.json'
        task = await self.run_soon(self.packet('p1', ws), out, timeout_s=5)
        await asyncio.sleep(0.08)
        task.cancel()
        started = time.monotonic()
        with mock.patch.object(resident_module, 'write_json', blocked_final_write):
            with self.assertRaises(asyncio.CancelledError):
                await task
        elapsed = time.monotonic() - started
        # The finally-side evidence write completed despite the pending
        # cancellation: synchronous sections are not preemptible.
        self.assertGreaterEqual(elapsed, SYNC_BLOCK_S)
        self.assertTrue(out.exists())
        written = json.loads(out.read_text())
        self.assertIn(written.get('outcome'), ('cancelled', None))
        _record_wall('sync-post-cancel-finally-write', elapsed)


# ---------------------------------------------------------------------------
# P0.1 unit contracts: zcode_supervisor.deadline (lazy import; RED predates it)
# ---------------------------------------------------------------------------

class FakeClock:
    def __init__(self, start=100.0):
        self.now = float(start)

    def __call__(self):
        return self.now

    def advance(self, seconds):
        self.now += seconds


class DeadlineBudgetUnitTests(unittest.TestCase):
    def _module(self):
        from zcode_supervisor import deadline
        return deadline

    def test_budget_uses_the_shared_monotonic_clock_by_default(self):
        import inspect
        import time as time_module
        deadline = self._module()
        default = inspect.signature(deadline.CallBudget).parameters['clock'].default
        self.assertIs(default, time_module.monotonic)

    def test_primary_and_hard_deadline_arithmetic_from_one_clock(self):
        deadline = self._module()
        clock = FakeClock(100.0)
        budget = deadline.CallBudget(10.0, cleanup_budget_s=2.0, clock=clock)
        self.assertEqual(budget.started_at, 100.0)
        self.assertEqual(budget.primary_deadline, 110.0)
        self.assertEqual(budget.hard_deadline, 112.0)
        self.assertEqual(budget.remaining_s(), 10.0)
        self.assertEqual(budget.cleanup_remaining_s(), 12.0)
        clock.advance(9.0)
        self.assertEqual(budget.remaining_s(), 1.0)
        clock.advance(2.0)
        self.assertEqual(budget.remaining_s(), 0.0)          # floored, no raise
        self.assertEqual(budget.cleanup_remaining_s(), 1.0)  # reserved window
        clock.advance(5.0)
        self.assertEqual(budget.cleanup_remaining_s(), 0.0)

    def test_check_raises_timeout_subclass_exactly_when_exhausted(self):
        deadline = self._module()
        clock = FakeClock()
        budget = deadline.CallBudget(1.0, clock=clock)
        self.assertEqual(budget.check(), 1.0)
        clock.advance(0.999)
        budget.check()
        clock.advance(0.001)
        with self.assertRaises(deadline.BudgetExhausted) as caught:
            budget.check()
        self.assertIsInstance(caught.exception, asyncio.TimeoutError)
        with self.assertRaises(deadline.BudgetExhausted):
            budget.wait_s()
        with self.assertRaises(deadline.BudgetExhausted):
            budget.wait_s(cap=120)

    def test_wait_s_caps_the_segment_without_adding_time(self):
        deadline = self._module()
        budget = deadline.CallBudget(10.0, clock=FakeClock())
        self.assertEqual(budget.wait_s(cap=120), 10.0)
        self.assertEqual(budget.wait_s(cap=3), 3.0)

    def test_invalid_budgets_are_rejected(self):
        deadline = self._module()
        for bad in (0, -1, float('inf'), float('nan'), True, None, 'x'):
            with self.assertRaises(ValueError):
                deadline.CallBudget(bad, clock=FakeClock())
        with self.assertRaises(ValueError):
            deadline.CallBudget(1.0, cleanup_budget_s=0, clock=FakeClock())
        with self.assertRaises(ValueError):
            deadline.CallBudget(1.0, cleanup_budget_s=-2.0, clock=FakeClock())


class DeadlineCleanupUnitTests(unittest.IsolatedAsyncioTestCase):
    def _run(self, awaitable_factory, timeout_s):
        from zcode_supervisor.deadline import run_best_effort_cleanup
        return run_best_effort_cleanup(awaitable_factory, timeout_s=timeout_s)

    async def test_completed_cleanup_returns_true(self):
        done = []

        async def cleanup():
            await asyncio.sleep(0.01)
            done.append(True)
        self.assertTrue(await self._run(cleanup, timeout_s=1.0))
        self.assertEqual(done, [True])

    async def test_raising_cleanup_returns_false(self):
        async def cleanup():
            raise RuntimeError('cleanup exploded')
        self.assertFalse(await self._run(cleanup, timeout_s=1.0))

    async def test_slow_cleanup_is_bounded_by_its_window(self):
        started = time.monotonic()

        async def cleanup():
            await asyncio.sleep(5.0)
        self.assertFalse(await self._run(cleanup, timeout_s=0.05))
        self.assertLess(time.monotonic() - started, 0.5)

    async def test_window_close_adds_no_extra_wait_for_resisting_cleanup(self):
        # REVIEW spec-I4 (RED): after the cleanup window closed, the old
        # finally still waited a fixed extra 0.5 s for the (cancelled) task —
        # a resisting cleanup made a 0.05 s window return after ~0.55 s. The
        # window is the bound: cancel, let a cooperative cleanup settle on
        # its own scheduling, and return without any added wait.
        started = time.monotonic()
        settled = []

        async def resisting():
            while True:
                try:
                    await asyncio.sleep(5.0)
                except asyncio.CancelledError:
                    await asyncio.sleep(5.0)  # resists once, then settles
                    settled.append('resisted')
                    raise

        async def caller():
            return await self._run(resisting, timeout_s=0.05)

        task = asyncio.ensure_future(caller())
        result = await task
        elapsed = time.monotonic() - started
        self.assertFalse(result)
        self.assertLess(elapsed, 0.20)  # 0.05 window + scheduling, no +0.5

    async def test_cleanup_survives_outer_cancel_and_finishes(self):
        # The caller's cancellation must NOT kill the independent cleanup
        # task: it keeps its bounded window and completes.
        done = []

        async def cleanup():
            await asyncio.sleep(0.12)
            done.append('finished')

        async def caller():
            return await self._run(cleanup, timeout_s=1.0)

        task = asyncio.ensure_future(caller())
        await asyncio.sleep(0.03)
        task.cancel()
        started = time.monotonic()
        result = await task  # returns normally after the bounded cleanup
        self.assertTrue(result)
        self.assertEqual(done, ['finished'])
        self.assertGreaterEqual(time.monotonic() - started, 0.08)

    async def test_repeated_cancel_runs_factory_exactly_once(self):
        calls = []

        async def cleanup():
            calls.append(time.monotonic())
            await asyncio.sleep(0.15)
            calls.append(time.monotonic())

        async def caller():
            return await self._run(cleanup, timeout_s=1.0)

        task = asyncio.ensure_future(caller())
        await asyncio.sleep(0.03)
        task.cancel()
        await asyncio.sleep(0.03)
        task.cancel()
        self.assertTrue(await task)
        self.assertEqual(len(calls), 2)  # one call, one completion mark

    async def test_factory_is_invoked_exactly_once(self):
        calls = []

        async def cleanup():
            calls.append(1)
        self.assertTrue(await self._run(cleanup, timeout_s=1.0))
        self.assertEqual(calls, [1])

    async def test_invalid_window_rejected(self):
        from zcode_supervisor.deadline import run_best_effort_cleanup
        for bad in (0, -1, float('inf'), None):
            with self.assertRaises(ValueError):
                await run_best_effort_cleanup(lambda: None, timeout_s=bad)


class OuterCancelPendingUnitTests(unittest.IsolatedAsyncioTestCase):
    async def test_outer_cancel_pending_reflects_unhandled_cancel(self):
        from zcode_supervisor.deadline import outer_cancel_pending
        self.assertFalse(outer_cancel_pending())

        async def absorbing():
            try:
                await asyncio.sleep(5.0)
            except asyncio.CancelledError:
                await asyncio.sleep(0)
                return outer_cancel_pending()
        task = asyncio.ensure_future(absorbing())
        await asyncio.sleep(0.02)
        task.cancel()
        self.assertTrue(await task)


if __name__ == '__main__':
    unittest.main()
