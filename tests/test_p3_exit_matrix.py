"""P3.1 cold exit parameterized matrix (P3 dispatch item 1, PLAN §P3 564-581).

Six combinations ``driver in {cold_codex, cold_zcode}`` x ``scenario in
{normal, timeout_terminate_wait, timeout_kill_wait}`` plus the two shared
rows (shared-resident, shared-app-server). Every cell is asserted per-cell
(one test per cell, no merged assertions): before the real wait/close
observes the exit, ``task.completed`` frames (both literal shapes,
duplicated), reader EOF, a bare returncode assignment and signal-only
sends must all leave the ``process.exited`` emission count at 0; the
actual awaited wait/close projects EXACTLY ONE emission with the correct
exit code and process generation; repeated close/cleanup and repeated
task.completed add nothing.

cold_zcode additionally drives the REAL ``protocol.execute()`` end to end
per scenario and proves ``process_exit_code`` only ever reads as an
integer AFTER ``await client.close()`` completed its wait gradient (the
fake child's returncode stays None until a wait() actually returns).
cold ZCode is never substituted by a Codex result.

No real CLI or model: the codex cells run a subprocess fake body through
``codex_driver.run_readonly`` (argv contract honored, SIGTERM trapped or
ignored per scenario); the zcode cells use controllable fake process
objects with a real StreamReader stdout, exactly like the established
``test_worker_health`` doubles. Only test code here — src is untouched.
"""

import asyncio
import contextlib
import io
import json
import os
import signal
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import AsyncMock, Mock, patch

import unittest.mock as mock

from zcode_supervisor import codex_driver, protocol
from zcode_supervisor.ui_projection import UiEmitter

# Generations observed by the three cold-codex cells (checked pairwise
# distinct by the cross-cell summary test at the bottom).
_CODEX_CELL_GENERATIONS = []


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

async def _settle(rounds=4):
    """Let the event loop drain pending callbacks."""
    for _ in range(rounds):
        await asyncio.sleep(0)


async def _feed_task_completed_frames(stream, count=2):
    """Feed BOTH literal completion shapes, ``count`` times each.

    (a) a bare ``task.completed`` notification (unknown method: wire
    evidence only) and (b) a ``session/event`` whose params type IS
    ``task.completed`` — the duplicated completion the matrix must prove
    can never project a process exit.
    """
    for _ in range(count):
        stream.feed_data(
            b'{"method":"task.completed","params":{"turnId":"t1"}}\n')
        stream.feed_data((json.dumps({
            'method': 'session/event',
            'params': {'type': 'task.completed', 'turnId': 't1', 'seq': 1,
                       'payload': {'resultType': 'success'}}}) + '\n').encode())


async def _refeed_task_completed_after_close(client, child, count=1):
    """Re-feed duplicated task.completed frames through the production
    read_loop AFTER close: a replacement stdout (the original hit EOF),
    an in-memory wire (close closed the file) and closing=True (no EOF
    failure). Frames genuinely traverse read_loop again — no emission."""
    client.wire = io.StringIO()
    fresh = asyncio.StreamReader()
    child.stdout = fresh
    await _feed_task_completed_frames(fresh, count=count)
    fresh.feed_eof()
    await client.read_loop()


class GatedFakeChild:
    """Controllable fake child process for the zcode-side gradient cells.

    ``wait()`` models a real reaped exit: it completes only when the
    scenario's gate opens (immediately for well-behaved children, on the
    patched killpg's SIGTERM / SIGKILL otherwise) and only then sets
    ``returncode``. Every read of ``returncode`` is logged with whether a
    wait had completed — the proof surface for "process_exit_code is read
    only after the awaited close". A bare assignment is recorded but never
    marks the child reaped.
    """

    def __init__(self, *, mode='normal', pid=4242, normal_code=0,
                 term_code=-signal.SIGTERM, kill_code=-signal.SIGKILL):
        self.mode = mode
        self.pid = pid
        self._reaped = False
        self._reaped_code = None
        self._assigned = None
        self.reads = []          # [(value, reaped)] — every property read
        self.assignments = []    # bare assignments (never a wait)
        self.signals = []        # signals the patched killpg delivered
        self.signal_probes = []  # (sig, emission_count_at_send_time)
        self._exit_gate = asyncio.Event()
        self._planned_code = {'normal': normal_code, 'terminate': term_code,
                              'kill': kill_code}[mode]
        self.stdout = asyncio.StreamReader()
        self.stdin = Mock()
        self.stdin.drain = AsyncMock(return_value=None)

    # -- returncode: logged, reaped-only integer --------------------------
    @property
    def returncode(self):
        value = self._reaped_code if self._reaped else self._assigned
        self.reads.append((value, self._reaped))
        return value

    @returncode.setter
    def returncode(self, value):
        self.assignments.append(value)
        self._assigned = value

    async def wait(self):
        if not self._reaped:
            if self.mode in ('terminate', 'kill'):
                await self._exit_gate.wait()
            self._reaped = True
            self._reaped_code = self._planned_code
        return self._reaped_code

    def deliver_signal(self, sig, emission_count):
        """The patched killpg route: record, probe, open gates per mode."""
        self.signals.append(sig)
        self.signal_probes.append((sig, emission_count))
        if sig == signal.SIGKILL:
            self._exit_gate.set()
        elif sig == signal.SIGTERM and self.mode != 'kill':
            self._exit_gate.set()

    def killpg_patcher(self, emission_count):
        """A replacement os.killpg recording signals via ``emission_count``
        (a callable returning the current process.exited emission count)."""
        def fake_killpg(pid, sig):
            self.deliver_signal(sig, emission_count())
            return None
        return fake_killpg


def make_spawn_config(command=('fixture-node', 'cli.cjs')):
    config = Mock()
    config.command.return_value = list(command)
    config.environment.return_value = {}
    return config


# ---------------------------------------------------------------------------
# Cold codex cells (real subprocess fake body, real run_readonly)
# ---------------------------------------------------------------------------

EXIT_FAKE_CODEX = '''#!/usr/bin/env python3
"""Cold-codex fake body for the P3.1 exit matrix (argv-contract honoring).

normal      emit events (turn.completed TWICE), write last-message, hold
            0.5 s with stdout open while touching a heartbeat file, write
            an exiting marker, exit 0.
terminate   emit thread.started, close stdout (reader EOF while the
            process lives), hang with heartbeat; a SIGTERM handler writes
            the term marker and exits 42 after a visible grace (the
            handler keeps heartbeating through the grace so the FINAL
            heartbeat mtime is a reliable pre-death landmark).
kill        same but SIGTERM is ignored outright -> only SIGKILL ends it
            (returncode -9).
repair1:    the child publishes its own pid at startup so the test-side
            exit sink can probe child liveness AT the emission moment.
"""
import json, os, signal, sys, time

MODE = os.environ.get("EXIT_CODEX_MODE", "normal")
MARKER = os.environ.get("EXIT_CODEX_MARKER", "")
HEARTBEAT = os.environ.get("EXIT_CODEX_HEARTBEAT", "")
EXITING = os.environ.get("EXIT_CODEX_EXITING", "")
PIDFILE = os.environ.get("EXIT_CODEX_PIDFILE", "")
argv = sys.argv[1:]
last_message = argv[argv.index("-o") + 1]

if PIDFILE:
    with open(PIDFILE, "w") as handle:
        handle.write(str(os.getpid()))

def emit(event):
    sys.stdout.write(json.dumps(event) + "\\n")
    sys.stdout.flush()

def touch(path, text="beat"):
    with open(path, "w") as handle:
        handle.write(text + " %f\\n" % time.time())

if MODE == "terminate":
    def on_term(signum, frame):
        touch(MARKER, "term-seen")
        deadline = time.time() + 0.8   # visible grace: signal sent, wait
        while time.time() < deadline:  # not yet back; child provably alive
            touch(HEARTBEAT)
            time.sleep(0.05)
        os._exit(42)      # 42 proves the code came from the reaped child
    signal.signal(signal.SIGTERM, on_term)
elif MODE == "kill":
    signal.signal(signal.SIGTERM, signal.SIG_IGN)

emit({"type": "thread.started", "thread_id": "th-exit-matrix"})
emit({"type": "turn.started", "model": "fake-exit-model"})
if MODE == "normal":
    emit({"type": "turn.completed"})
    emit({"type": "turn.completed"})   # duplicated completion event
    with open(last_message, "w") as handle:
        handle.write(json.dumps({"summary": "ok", "files": []}))
    for _ in range(10):    # in-flight hold: provably alive via heartbeat
        touch(HEARTBEAT)
        time.sleep(0.05)
    touch(EXITING, "exiting")
    sys.exit(0)

sys.stdout.flush()
os.close(1)            # reader EOF while the process still runs
while True:            # hang with heartbeat until signaled
    touch(HEARTBEAT)
    time.sleep(0.05)
'''


class ColdCodexExitMatrixTests(unittest.TestCase):
    """Three cells: codex_driver.run_readonly terminate->kill->wait."""

    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name)
        self.workspace = self.root / 'workspace'
        self.workspace.mkdir()
        self.fake = self.root / 'fake-codex'
        self.fake.write_text(EXIT_FAKE_CODEX)
        self.fake.chmod(0o755)
        self.schema = {'type': 'object',
                       'properties': {'summary': {'type': 'string'},
                                      'files': {'type': 'array'}},
                       'required': ['summary', 'files']}
        self.marker = self.root / 'term-marker.txt'
        self.heartbeat = self.root / 'heartbeat.txt'
        self.exiting = self.root / 'exiting.txt'
        self.pidfile = self.root / 'child.pid'

    def _heartbeat_fresh(self, budget=0.14):
        try:
            return time.time() - self.heartbeat.stat().st_mtime < budget
        except OSError:
            return False

    def _child_alive(self, pid_cache=None):
        """repair1 N2: child liveness probe for the emission moment.

        The child publishes its pid at startup. ``os.kill(pid, 0)`` fails
        with ProcessLookupError only once the child is DEAD AND REAPED —
        exactly the state the production exit projection requires (it
        fires strictly after the driver's final ``process.wait()``
        returned). A gradient-internal fabricated emission (quality
        review M9: before SIGKILL is even sent; M10: while the child sits
        in its SIGTERM handler grace) always happens with the child still
        alive, so the probe deterministically separates the two. Callers
        pass a shared cache dict; the pid file exists before any
        emission-relevant event.
        """
        if pid_cache is None:
            pid_cache = {}
        try:
            if 'pid' not in pid_cache:
                pid_cache['pid'] = int(self.pidfile.read_text().strip())
            os.kill(pid_cache['pid'], 0)
            return True                      # alive (or unreaped zombie)
        except (OSError, ValueError):
            return False

    def _drive(self, mode, timeout_s):
        """Run one codex cell with a race-free in-flight watcher.

        Premature-emission detection is ORDER-BASED, never staleness-based:
        a legal emission can only exist after the driver's own wait
        expired (timeout cells) / after the child's pre-exit marker
        (normal cell), so any emission observed before those monotone
        landmarks is a genuine violation; a legal emission can never
        false-positive against them. repair1 N2 closes the
        gradient-internal window (between the driver's timeout and the
        final awaited wait): the sink probes child liveness AT the
        emission moment (reaped-or-not), and the cells additionally
        order the record against the child's final heartbeat/marker
        mtime; the watcher also asserts exits==0 in real time inside the
        terminate marker window.
        """
        evidence = self.root / f'evidence-{mode}'
        exits = []
        liveness = []      # child-alive flag per emission (N2)
        pid_cache = {}

        def sink(record):
            exits.append(record)
            with contextlib.suppress(Exception):
                liveness.append(
                    self._child_alive(pid_cache=pid_cache))

        state = {'done': False, 'error': None, 'samples': 0,
                 'violations': [], 'samples_after_marker': 0}
        t0 = time.monotonic()

        def drive():
            try:
                codex_driver.run_readonly(
                    self.fake, self.workspace, 'matrix task', self.schema,
                    evidence, timeout_s=timeout_s,
                    env={'EXIT_CODEX_MODE': mode,
                         'EXIT_CODEX_MARKER': str(self.marker),
                         'EXIT_CODEX_HEARTBEAT': str(self.heartbeat),
                         'EXIT_CODEX_EXITING': str(self.exiting),
                         'EXIT_CODEX_PIDFILE': str(self.pidfile),
                         'CODEX_HOME': str(self.root / 'codex-home')},
                    exit_event_sink=sink)
            except BaseException as error:  # noqa: BLE001 - surfaced below
                state['error'] = f'{type(error).__name__}: {error}'
            finally:
                state['done'] = True

        worker = threading.Thread(target=drive, daemon=True)
        worker.start()
        while not state['done']:
            time.sleep(0.02)
            state['samples'] += 1
            elapsed = time.monotonic() - t0
            # the terminate marker window: SIGTERM received and the child
            # is still alive in its handler grace — the awaited wait has
            # NOT returned, so an emission here is a violation. The
            # violation uses the LIVE liveness probe (never a staleness
            # window): a legitimate emission can never coexist with an
            # alive child (it fires strictly post-reap)
            if (mode == 'terminate' and self.marker.exists()
                    and self._heartbeat_fresh()):
                state['samples_after_marker'] += 1
            if exits:
                if self._child_alive(pid_cache=pid_cache):
                    state['violations'].append('emission-while-child-alive')
                if mode != 'normal' and elapsed < timeout_s:
                    state['violations'].append('before-driver-timeout')
                if mode == 'normal' and not self.exiting.exists():
                    state['violations'].append('before-child-exit-marker')
                break
        worker.join(timeout=60)
        receipt_path = evidence / 'receipt.json'
        receipt = (json.loads(receipt_path.read_text())
                   if receipt_path.is_file() else None)
        return exits, liveness, state, receipt

    def test_cell_cold_codex_normal(self):
        exits, liveness, state, receipt = self._drive('normal', timeout_s=30)
        self.assertIsNone(state['error'], state['error'])
        self.assertGreater(state['samples'], 3)   # watcher really sampled
        # no emission before the child's own pre-exit marker (duplicated
        # completion events parsed and stdout open the whole time)
        self.assertEqual(state['violations'], [])
        # AFTER the real wait: exactly one emission, exit code 0 reaped,
        # and projected at/after the child's own exiting marker.
        self.assertEqual(len(exits), 1)
        record = exits[0]
        self.assertEqual(record['type'], 'process.exited')
        self.assertEqual(record['returncode'], 0)
        self.assertIsInstance(record['generation'], int)
        self.assertNotIsInstance(record['generation'], bool)
        self.assertGreaterEqual(record['generation'], 1)
        _CODEX_CELL_GENERATIONS.append(record['generation'])
        exiting_mtime = self.exiting.stat().st_mtime
        self.assertGreaterEqual(record['observed_at'] + 0.001, exiting_mtime)
        # N2: the single emission happened with the child already dead
        # AND reaped (the driver's final wait had returned)
        self.assertEqual(liveness, [False])
        self.assertEqual(receipt['outcome'], 'delivered')
        self.assertEqual(receipt['exit_code'], 0)
        # duplicated task.completed events parsed twice, still ONE exit.
        self.assertEqual(receipt['event_counts'].get('turn.completed'), 2)
        # repeated cleanup analogue: re-parsing the finalized evidence adds
        # no further emission.
        lines = Path(receipt['events_path']).read_text().splitlines()
        codex_driver._parse_events(lines)
        self.assertEqual(len(exits), 1)

    def test_cell_cold_codex_timeout_terminate_wait(self):
        exits, liveness, state, receipt = self._drive('terminate',
                                                      timeout_s=0.8)
        self.assertIsNone(state['error'], state['error'])
        # no emission before the driver's own wait expired AND none inside
        # the marker window (SIGTERM seen, child still alive in grace)
        self.assertEqual(state['violations'], [])
        # SIGTERM was received and the child stayed alive in its handler
        # grace: the watcher sampled that marker+alive window repeatedly.
        self.assertTrue(self.marker.exists())
        self.assertGreater(state['samples_after_marker'], 3)
        # the wait observed the reaped exit: exactly once, code 42 — a
        # returncode only the reaped child could produce.
        self.assertEqual(len(exits), 1)
        record = exits[0]
        self.assertEqual(record['returncode'], 42)
        self.assertIsInstance(record['generation'], int)
        self.assertGreaterEqual(record['generation'], 1)
        _CODEX_CELL_GENERATIONS.append(record['generation'])
        # ordering: the projection happened at/after the child saw SIGTERM
        # AND at/after the child's FINAL heartbeat (the pre-death landmark
        # — the handler heartbeats until os._exit, so any emission before
        # the reaped exit lands strictly below it; quality M10 shape)
        marker_mtime = self.marker.stat().st_mtime
        self.assertGreaterEqual(record['observed_at'] + 0.001, marker_mtime)
        heartbeat_mtime = self.heartbeat.stat().st_mtime
        self.assertGreaterEqual(record['observed_at'] + 0.001,
                                heartbeat_mtime)
        # N2: the emission moment found the child dead AND reaped (an
        # in-grace fabrication like M10 sees it alive)
        self.assertEqual(liveness, [False])
        self.assertEqual(receipt['outcome'], 'timeout')
        self.assertEqual(receipt['exit_code'], 42)

    def test_cell_cold_codex_timeout_kill_wait(self):
        # the terminate grace is injected small for wall control (test-only
        # constant injection; the src default is 5.0 s)
        with mock.patch.object(codex_driver, '_TERMINATE_GRACE_S', 0.5):
            exits, liveness, state, receipt = self._drive('kill',
                                                          timeout_s=0.8)
        self.assertIsNone(state['error'], state['error'])
        # no emission before the driver's own wait expired
        self.assertEqual(state['violations'], [])
        # SIGTERM was ignored; only SIGKILL ends the child. The single
        # emission carries -9 — a value only a REAPED wait can report (the
        # projection rejects the None returncode a live child would have,
        # a property unit-pinned by worker_health's I5 contract).
        self.assertEqual(len(exits), 1)
        record = exits[0]
        self.assertEqual(record['returncode'], -signal.SIGKILL)
        self.assertIsInstance(record['generation'], int)
        self.assertGreaterEqual(record['generation'], 1)
        _CODEX_CELL_GENERATIONS.append(record['generation'])
        # N2: the emission moment found the child dead AND reaped. This is
        # the DETERMINISTIC catcher for the quality review's M9 shape (a
        # fabrication at the TERM-grace timeout branch fires strictly
        # BEFORE Popen.kill() is even invoked — the child is provably
        # still alive at that instant); the final-heartbeat ordering below
        # adds the general pre-death landmark guard
        self.assertEqual(liveness, [False])
        heartbeat_mtime = self.heartbeat.stat().st_mtime
        self.assertGreaterEqual(record['observed_at'] + 0.001,
                                heartbeat_mtime)
        self.assertEqual(receipt['outcome'], 'timeout')
        self.assertEqual(receipt['exit_code'], -signal.SIGKILL)
        self.assertLess(receipt['duration_s'], 10.0)  # bounded gradient

    def test_codex_cells_used_distinct_generations(self):
        # Cross-cell: all three codex cells ran (unittest runs methods
        # alphabetically — the cell methods sort before this one) and each
        # stamped its own process_generation.
        self.assertEqual(len(_CODEX_CELL_GENERATIONS), 3,
                         f'expected 3 codex cells recorded, '
                         f'got {_CODEX_CELL_GENERATIONS}')
        self.assertEqual(len(set(_CODEX_CELL_GENERATIONS)), 3)


# ---------------------------------------------------------------------------
# Cold zcode cells: ProtocolClient.close() gradient, direct per-cell drive
# ---------------------------------------------------------------------------

class ColdZcodeCloseMatrixTests(unittest.IsolatedAsyncioTestCase):
    """Three cells driving ProtocolClient.close() itself (never via Codex).

    The CLOSE_* constants are injected small so the gradient's bounded
    windows do not sleep the suite (worker_health tests inject the same
    shape of budget constants).
    """

    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.workspace = self.root / 'ws'
        self.workspace.mkdir()
        self.config = make_spawn_config()

    async def _make_client(self, child, sink):
        client = protocol.ProtocolClient(
            self.workspace, self.root / 'run.json',
            lambda params: {'decision': 'deny', 'reason': 'fixture'},
            self.config, exit_event_sink=sink)
        with patch.object(protocol.asyncio, 'create_subprocess_exec',
                          new=AsyncMock(return_value=child)):
            await asyncio.wait_for(client.start(), 5)
        return client

    async def _drive_cell(self, mode, patched_constants):
        exits = []
        child = GatedFakeChild(mode=mode)
        client = await self._make_client(child, exits.append)
        self.addAsyncCleanup(client.close)
        patchers = [
            patch.object(protocol.os, 'killpg',
                         child.killpg_patcher(
                             lambda: len(client.exit_projection.emitted))),
        ] + [mock.patch.object(protocol, name, value)
             for name, value in patched_constants.items()]
        for patcher in patchers:
            patcher.start()
            self.addCleanup(patcher.stop)

        # --- pre-close probes: all must leave the count at 0 -------------
        await _feed_task_completed_frames(child.stdout)     # duplicates
        await _settle()
        self.assertEqual(client.events[-1]['type'], 'task.completed')
        self.assertEqual(len(client.exit_projection.emitted), 0)
        child.stdout.feed_eof()                            # reader EOF
        for _ in range(50):
            if client.failure:
                break
            await asyncio.sleep(0.01)
        self.assertIsNotNone(client.failure)
        self.assertEqual(len(client.exit_projection.emitted), 0)
        child.returncode = 77                              # bare assignment
        self.assertEqual(len(client.exit_projection.emitted), 0)

        # --- the real close: gradient per scenario ----------------------
        await client.close()
        self.assertEqual(len(exits), 1)
        record = exits[0]
        self.assertEqual(record['type'], 'process.exited')
        self.assertEqual(record['returncode'], child._planned_code)
        # process_generation: the cold sink wiring stamped the client
        self.assertIsInstance(client.generation, int)
        self.assertEqual(record['generation'], client.generation)

        # --- repeats: duplicate close + re-fed duplicated task.completed -
        await client.close()
        await _refeed_task_completed_after_close(client, child)
        self.assertEqual(len(exits), 1)

        # --- signal-only probes were 0 at send time ---------------------
        for sig, count in child.signal_probes:
            self.assertEqual(count, 0,
                             f'signal {sig} sent with {count} emissions')
        return child, client, record

    async def test_cell_cold_zcode_normal(self):
        child, client, record = await self._drive_cell(
            'normal', {'CLOSE_GRACE_S': 0.5})
        self.assertEqual(child.signals, [])      # well-behaved: no signals
        self.assertEqual(record['returncode'], 0)

    async def test_cold_zcode_normal_unpatched_gradient_constants(self):
        """The normal cell also passes with the production constants (the
        grace wait completes immediately; no real sleeping)."""
        child, client, record = await self._drive_cell('normal', {})
        self.assertEqual(child.signals, [])
        self.assertEqual(record['returncode'], 0)

    async def test_cell_cold_zcode_timeout_terminate_wait(self):
        child, client, record = await self._drive_cell(
            'terminate', {'CLOSE_GRACE_S': 0.05, 'CLOSE_TERM_S': 0.5,
                          'CLOSE_KILL_S': 0.5})
        self.assertEqual(child.signals, [signal.SIGTERM])  # TERM sufficed
        self.assertEqual(record['returncode'], -signal.SIGTERM)

    async def test_cell_cold_zcode_timeout_kill_wait(self):
        child, client, record = await self._drive_cell(
            'kill', {'CLOSE_GRACE_S': 0.05, 'CLOSE_TERM_S': 0.05,
                     'CLOSE_KILL_S': 0.5})
        self.assertEqual(child.signals,
                         [signal.SIGTERM, signal.SIGKILL])  # escalation
        self.assertEqual(record['returncode'], -signal.SIGKILL)


# ---------------------------------------------------------------------------
# Cold zcode: the REAL protocol.execute() — process_exit_code read order
# ---------------------------------------------------------------------------

class _ScriptedStdin:
    """Fake stdin that synchronously dispatches each written frame."""

    def __init__(self, responder):
        self._responder = responder
        self.written = []
        self.closed = False
        self.drain = AsyncMock(return_value=None)

    def write(self, data):
        self.written.append(data)
        try:
            frame = json.loads(data)
        except ValueError:
            return
        asyncio.get_running_loop().call_soon(self._responder, frame)

    def close(self):
        self.closed = True


class SimpleNamespaceConfig:
    """Duck-typed runtime config: real client spawn + execute() paths."""

    def __init__(self, root):
        self.work_root = root
        self.state_dir = root / 'state'
        self.cache_dir = root / 'cache'
        self.notify = False

    def command(self):
        return ['fixture-node', 'cli.cjs']

    def environment(self):
        return {'FIXTURE': '1'}


def SimpleNamespaceArgs(packet, output):
    from types import SimpleNamespace
    return SimpleNamespace(packet=str(packet), out=str(output), resume=None,
                           timeout=10, quiet=True)


class ColdZcodeExecuteOrderTests(unittest.IsolatedAsyncioTestCase):
    """execute() end to end per scenario: process_exit_code AFTER close."""

    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.workspace = self.root / 'ws'
        self.workspace.mkdir()
        self.packet = self.root / 'packet.json'
        self.packet.write_text(json.dumps(
            {'workspace': str(self.workspace), 'prompt': 'Fixture work',
             'allowed_files': []}))
        self.output = self.root / 'run.json'
        self.config = SimpleNamespaceConfig(self.root)

    def _make_child(self, mode):
        child = GatedFakeChild(mode=mode, pid=5150)

        def responder(frame):
            rid, method = frame.get('id'), frame.get('method')
            if method == 'session/create':
                reply = {'id': rid, 'result': {
                    'session': {'sessionId': 'sess_cold'}}}
            elif method == 'session/subscribe':
                reply = {'id': rid, 'result': {'eventSeq': 0}}
            elif method == 'session/send':
                base = 10 * len(child.stdin.written)
                input_id = frame['params']['inputId']
                session_events = [
                    ('turn.started', {'inputId': input_id}),
                    ('turn.completed', {'resultType': 'success'}),
                    # duplicated task.completed shapes: pure probes — the
                    # run already completed above; these must never project
                    # a process exit (matrix wait-before-zero contract).
                    ('task.completed', {'resultType': 'success'}),
                ]
                for offset, (event_type, payload) in enumerate(session_events):
                    child.stdout.feed_data((json.dumps({
                        'method': 'session/event',
                        'params': {'type': event_type,
                                   'sessionId': 'sess_cold',
                                   'turnId': 'turn_cold',
                                   'seq': base + 1 + offset,
                                   'payload': payload}}) + '\n').encode())
                child.stdout.feed_data(
                    b'{"method":"task.completed","params":{"turnId":"t"}}\n')
                reply = {'id': rid, 'result': {}}
            elif method == 'session/read':
                reply = {'id': rid, 'result': {
                    'session': {'status': 'idle'},
                    'runtime': {'pendingRequestIds': []}}}
            else:
                reply = {'id': rid, 'result': {}}
            child.stdout.feed_data((json.dumps(reply) + '\n').encode())

        child.stdin = _ScriptedStdin(responder)
        return child

    async def _drive_execute(self, mode):
        child = self._make_child(mode)
        emitter = UiEmitter(hub_instance_id='exitmatrix', journal_epoch=1,
                            enabled=True)
        exits = []
        emitter.subscribe(exits.append)
        patchers = [
            patch.object(protocol.os, 'killpg',
                         child.killpg_patcher(lambda: len(exits))),
            patch.object(protocol.asyncio, 'create_subprocess_exec',
                         new=AsyncMock(return_value=child)),
        ] + [mock.patch.object(protocol, name, value) for name, value in
             {'CLOSE_GRACE_S': 0.05, 'CLOSE_TERM_S': 0.05,
              'CLOSE_KILL_S': 0.5}.items()]
        for patcher in patchers:
            patcher.start()
            self.addCleanup(patcher.stop)
        args = SimpleNamespaceArgs(self.packet, self.output)
        await protocol.execute(args, self.config, ui_emitter=emitter)
        result = json.loads(self.output.read_text())
        return child, exits, result

    def _assert_integer_reads_all_reaped(self, child):
        integer_reads = [read for read in child.reads
                         if isinstance(read[0], int)
                         and not isinstance(read[0], bool)]
        self.assertTrue(integer_reads,
                        'process_exit_code was never read as an integer')
        for value, reaped in integer_reads:
            self.assertTrue(
                reaped,
                'returncode read as an integer before any wait completed: '
                f'{value}')

    async def test_execute_normal_exit_code_after_close(self):
        child, exits, result = await self._drive_execute('normal')
        self.assertEqual(result['process_exit_code'], 0)
        self.assertEqual(result['delivery_ok'], True)
        self.assertEqual(result['outcome'], 'delivered')
        self._assert_integer_reads_all_reaped(child)
        # the duplicated task.completed arrived twice; the UI exit sink
        # still saw exactly one process.exited — from the real close.
        self.assertEqual(len(exits), 1)
        self.assertEqual(exits[0]['kind'], 'process.exited')

    async def test_execute_terminate_gradient_exit_code_after_close(self):
        child, exits, result = await self._drive_execute('terminate')
        # the turn delivered, but the child ignored stdin close: the close
        # gradient SIGTERMed and reaped it — recorded in the result.
        self.assertEqual(child.signals, [signal.SIGTERM])
        self.assertEqual(result['process_exit_code'], -signal.SIGTERM)
        self.assertEqual(result['delivery_ok'], False)
        self.assertEqual(result['outcome'], 'protocol-error')
        self._assert_integer_reads_all_reaped(child)
        self.assertEqual(len(exits), 1)

    async def test_execute_kill_gradient_exit_code_after_close(self):
        child, exits, result = await self._drive_execute('kill')
        self.assertEqual(child.signals, [signal.SIGTERM, signal.SIGKILL])
        self.assertEqual(result['process_exit_code'], -signal.SIGKILL)
        self.assertEqual(result['delivery_ok'], False)
        self.assertEqual(result['outcome'], 'protocol-error')
        self._assert_integer_reads_all_reaped(child)
        self.assertEqual(len(exits), 1)


# ---------------------------------------------------------------------------
# Shared rows: shared-resident and shared-app-server
# ---------------------------------------------------------------------------

class SharedResidentExitRowTests(unittest.IsolatedAsyncioTestCase):
    """Shared resident: actual stop/close + awaited wait, exactly once."""

    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        for name in ('work', 'state'):
            (self.root / name).mkdir()
        self.config = SimpleNamespaceConfig(self.root)
        self.child = GatedFakeChild(mode='normal', pid=4343, normal_code=3)

    async def test_shared_resident_stop_close_wait_exactly_once(self):
        from zcode_supervisor.zcode_resident import ResidentZCode

        def client_factory(workspace, output, permission, config):
            return protocol.ProtocolClient(workspace, output, permission,
                                           config)

        resident = ResidentZCode(self.config, self.root / 'work',
                                 client_factory=client_factory)
        resident.generation = 7   # as the hub's WorkerGeneration stamps
        with patch.object(protocol.asyncio, 'create_subprocess_exec',
                          new=AsyncMock(return_value=self.child)):
            await resident.start()
        client = resident.client
        self.assertEqual(client.generation, 7)
        # pre-stop probes: duplicated task.completed, EOF, assignment
        await _feed_task_completed_frames(self.child.stdout)
        await _settle()
        self.assertEqual(len(client.exit_projection.emitted), 0)
        self.child.stdout.feed_eof()
        for _ in range(50):
            if client.failure:
                break
            await asyncio.sleep(0.01)
        self.assertIsNotNone(client.failure)
        self.assertEqual(len(client.exit_projection.emitted), 0)
        self.child.returncode = 9
        self.assertEqual(len(client.exit_projection.emitted), 0)
        # actual stop -> real close -> awaited wait -> exactly once
        await resident.stop()
        self.assertEqual(len(client.exit_projection.emitted), 1)
        record = client.exit_projection.emitted[0]
        self.assertEqual(record['returncode'], 3)
        self.assertEqual(record['generation'], 7)
        # repeats: duplicate close + re-fed duplicated task.completed
        await client.close()
        await _refeed_task_completed_after_close(client, self.child)
        self.assertEqual(len(client.exit_projection.emitted), 1)
        await resident.stop()          # wrapper-level repeat: no-op, no emit
        self.assertEqual(len(client.exit_projection.emitted), 1)


class SharedAppServerExitRowTests(unittest.IsolatedAsyncioTestCase):
    """Shared app-server: actual close + awaited wait, exactly once."""

    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.child = GatedFakeChild(mode='normal', pid=4545, normal_code=5)

    async def test_shared_appserver_close_wait_exactly_once(self):
        from zcode_supervisor.codex_appserver import CodexAppServerClient
        initialize = {'id': 'czs-1', 'result': {
            'codexHome': '/tmp/fake', 'platformFamily': 'unix',
            'platformOs': 'macos', 'userAgent': 'exit-matrix/1'}}
        self.child.stdout.feed_data((json.dumps(initialize) + '\n').encode())
        # duplicated completion-shaped notifications before EOF: the
        # app-server analog of the repeated task.completed probe
        for _ in range(2):
            self.child.stdout.feed_data((json.dumps({
                'method': 'turn/completed',
                'params': {'threadId': 'th_shared',
                           'turn': {'id': 'turn_shared',
                                    'status': 'completed'}}}) + '\n').encode())
        self.child.stdout.feed_eof()
        client = CodexAppServerClient('unused-fake-cli', self.root / 'ev')
        with patch.object(protocol.asyncio, 'create_subprocess_exec',
                          new=AsyncMock(return_value=self.child)):
            await asyncio.wait_for(client.start(), 5)
        self.addAsyncCleanup(client.close)
        client.generation = 5   # as the holder stamps it
        projection = client.exit_projection
        # pre-close probes: duplicated completions recorded, EOF failed the
        # client, bare assignment — none of it is an awaited exit.
        for _ in range(50):
            if client.failure:
                break
            await asyncio.sleep(0.01)
        self.assertIsNotNone(client.failure)   # EOF after the handshake
        self.assertEqual(projection.emitted, [])
        self.child.returncode = 11
        self.assertEqual(projection.emitted, [])
        await client.close()
        self.assertEqual(len(projection.emitted), 1)
        record = projection.emitted[0]
        self.assertEqual(record['returncode'], 5)
        self.assertEqual(record['generation'], 5)
        await client.close()                    # duplicate close: still one
        self.assertEqual(len(projection.emitted), 1)


if __name__ == '__main__':
    unittest.main()
