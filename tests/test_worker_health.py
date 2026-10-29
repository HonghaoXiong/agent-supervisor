"""P0.3 worker generation / health evidence / exit projection contracts.

Behavioral RED first: on the CURRENT hub, a shared worker that died during a
delegate (stdout EOF / nonzero exit / late client.failure) is never replaced
— the next delegate reuses the dead instance and fails the same way forever
(PLAN §7 P0.3: "后续 request 不再永久抛同一 EOF"). Those tests use only the
existing public surface and must FAIL before the production change.

Tests needing the new symbols (``zcode_supervisor.worker_health``,
``exit_projection``/``generation`` on the clients) import/construct them
lazily: on the unchanged tree they surface as ImportError/AttributeError and
are listed separately — interface evidence, not defect evidence.

No live CLI/model/network: fake children are mock processes with a real
StreamReader stdout; "wait completed" is modeled by the fake wait() actually
setting returncode, exactly like a real child.
"""

import asyncio
import contextlib
import json
import os
import signal
import sys
import tempfile
import time
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from zcode_supervisor import contracts, hub_server as H
from zcode_supervisor.runtime import Config


def done_reader():
    """A reader-task double that has ACTUALLY ended (post-EOF shape)."""
    return SimpleNamespace(done=lambda: True)


def live_reader():
    return SimpleNamespace(done=lambda: False)


async def wait_until(condition, timeout=5.0):
    """Deterministic polling helper: waits only for an explicit condition."""
    deadline = time.monotonic() + timeout
    while not condition() and time.monotonic() < deadline:
        await asyncio.sleep(0.01)
    return condition()


# ---------------------------------------------------------------------------
# Hub-shaped fixture (compact HubFixture equivalent)
# ---------------------------------------------------------------------------

class WorkerHealthHubFixture(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.work_root = self.root / 'work'
        self.state_dir = self.root / 'state'
        self.cache_dir = self.root / 'cache'
        for path in (self.work_root, self.state_dir, self.cache_dir):
            path.mkdir()
        self.workspace = self.work_root / 'task-ws'
        self.workspace.mkdir()
        self.fake_cli = self.root / 'zcode.cjs'
        self.fake_cli.write_text('#!/bin/sh\n', encoding='utf-8')
        self.fake_codex = self.root / 'fake-codex'
        self.fake_codex.write_text('#!/bin/sh\n', encoding='utf-8')
        self.fake_codex.chmod(0o755)
        self.evidence_root = self.root / 'hub-evidence'
        self.config = Config(work_root=self.work_root, state_dir=self.state_dir,
                             cache_dir=self.cache_dir, cli_path=self.fake_cli,
                             node='node')
        for target, value in (
            ('zcode_supervisor.tri_probe.discover_codex',
             {'installed': False, 'path': None, 'candidates': []}),
            ('zcode_supervisor.tri_probe.discover_zcode',
             {'installed': True, 'path': str(self.fake_cli),
              'candidates': [str(self.fake_cli)]}),
            ('zcode_supervisor.tri_probe.discover_dsh',
             {'installed': False, 'path': None, 'candidates': []}),
        ):
            patcher = patch(target, return_value=value)
            patcher.start()
            self.addCleanup(patcher.stop)

    def register(self, contract):
        source = self.root / 'incoming'
        source.mkdir(exist_ok=True)
        path = source / f"{contract['contract_id']}.json"
        path.write_text(json.dumps(contract), encoding='utf-8')
        return contracts.register_contract(self.state_dir, path)

    def codex_appserver_contract(self):
        contract = {
            'contract_id': 'hub-codex-app', 'worker': 'codex', 'mode': 'app-server',
            'sandbox': 'workspace-write',
            'workspace': str(self.workspace), 'prompt': 'Read TASK.md and answer.',
            'allowed_files': ['OUT.md'],
            'commands': [{'id': 'ls', 'command': '/bin/ls',
                          'max_uses': 1, 'max_timeout_ms': 120000}],
            'model': {'model': 'gpt-5-codex'}, 'max_timeout_ms': 60_000,
            'budget': {'max_tokens': None, 'max_cost_usd': 1.0, 'note': 'p0'},
        }
        return contract

    def zcode_resident_contract(self):
        return {
            'contract_id': 'hub-zcode-res', 'worker': 'zcode', 'mode': 'resident',
            'workspace': str(self.workspace), 'prompt': 'Read TASK.md and report.',
            'allowed_files': [], 'commands': [],
            'model': {'providerId': 'glm', 'modelId': 'glm-5.3'},
            'max_timeout_ms': 60_000,
            'budget': {'max_tokens': None, 'max_cost_usd': None, 'note': 'p0'},
        }

    def delegate(self, contract_id, **kwargs):
        return asyncio.run(H.delegate_impl(
            self.config, self.evidence_root, contract_id,
            {'agent': 'codex-control', 'session_id': 'sess_ctrl1'},
            codex_path=str(self.fake_codex), **kwargs))


# ---------------------------------------------------------------------------
# Behavioral RED: dead shared workers are never replaced on the current hub
# ---------------------------------------------------------------------------

class DeadWorkerReplacementTests(WorkerHealthHubFixture):
    """PLAN P0.3 first bullet: the CURRENT task fails, no automatic replay,
    and the NEXT delegate creates a fresh generation."""

    def make_dying_appserver_family(self):
        """Factory + driver doubles: the first client dies on its first run
        with one of the three evidence shapes; later clients are healthy."""
        created = []

        class FakeAppServerClient:
            def __init__(self, dies_on_run=None):
                self.dies_on_run = dies_on_run
                self.starts = 0
                self.closes = 0
                self.failure = None
                self.reader = None
                self.process = None
                self.runs = 0
                created.append(self)

            async def start(self, budget=None, initialize_timeout=None):
                self.starts += 1
                return {'userAgent': 'fake-appserver'}

            async def close(self):
                self.closes += 1

            def mark_death(self):
                mode = self.dies_on_run
                if mode == 'eof':          # read loop saw stdout EOF
                    self.failure = 'EOFError: stream closed'
                    self.reader = done_reader()
                elif mode == 'nonzero-exit':  # reaped child exit
                    self.process = SimpleNamespace(returncode=1)
                    self.reader = done_reader()
                elif mode == 'late-failure':   # failure recorded after return
                    self.failure = 'AppServerError: late failure'

        class FakeDriver:
            def __init__(self, client):
                self.client = client

            async def run_task(self, authority, workspace, prompt, *, model=None,
                               sandbox='read-only', approval_policy='on-request',
                               timeout_s=600, budget=None):
                self.client.runs += 1
                if self.client.dies_on_run:
                    # A dead instance fails EVERY later run the same way
                    # (the permanent-EOF symptom P0.3 removes).
                    self.client.mark_death()
                    return {'outcome': 'failed', 'thread_id': 'th_dead',
                            'turn_id': None, 'final_message': None,
                            'command_items': [], 'approvals': [],
                            'error': 'worker transport died'}
                return {'outcome': 'delivered', 'thread_id': 'th_ok',
                        'turn_id': 'turn_ok', 'final_message': 'done',
                        'command_items': [], 'approvals': []}

        return created, FakeAppServerClient, FakeDriver

    def test_dead_appserver_worker_is_replaced_on_next_delegate(self):
        self.register(self.codex_appserver_contract())
        for mode in ('eof', 'nonzero-exit', 'late-failure'):
            with self.subTest(mode=mode):
                created, client_cls, driver_cls = self.make_dying_appserver_family()
                firstborn = {'value': True}

                def factory():
                    dies = firstborn['value']
                    firstborn['value'] = False
                    return client_cls(dies_on_run=mode if dies else None)

                workers = H.ResidentWorkers(self.config, self.evidence_root,
                                            appserver_factory=factory,
                                            appserver_driver_factory=driver_cls)
                first = self.delegate('hub-codex-app', workers=workers)
                self.assertEqual(first['outcome'], 'failed')  # current task fails
                second = self.delegate('hub-codex-app', workers=workers)
                # The next delegate must run on a FRESH generation and succeed.
                self.assertEqual(second['outcome'], 'delivered', second.get('error'))
                self.assertEqual(len(created), 2)      # a new instance was built
                self.assertEqual([client.starts for client in created], [1, 1])
                self.assertNotEqual(first['thread_id'], second['thread_id'])
                self.assertEqual(created[0].runs, 1)   # no automatic replay
                self.assertGreaterEqual(created[0].closes, 1)  # old gen closed

    def test_dead_resident_worker_is_replaced_on_next_delegate(self):
        self.register(self.zcode_resident_contract())
        created = []
        firstborn = {'value': True}

        class FakeResident:
            def __init__(self, dies):
                self.dies = dies
                self.starts = 0
                self.stops = 0
                self.runs = 0
                self.failure = None
                self.reader = None
                self.process = None
                created.append(self)

            async def start(self):
                self.starts += 1

            async def stop(self):
                self.stops += 1

            async def run_packet(self, packet_path, out, *, timeout_s, resume=None):
                self.runs += 1
                if self.dies:
                    self.failure = 'EOFError: stream closed'
                    self.reader = done_reader()
                    return {'delivery_ok': False, 'outcome': 'protocol-error',
                            'error': 'worker transport died'}
                return {'delivery_ok': True, 'session_id': 'sess_new',
                        'outcome': 'delivered', 'changed_files': [],
                        'execution_problems': []}

        def factory():
            dies = firstborn['value']
            firstborn['value'] = False
            return FakeResident(dies)

        workers = H.ResidentWorkers(self.config, self.evidence_root,
                                    resident_factory=factory)
        first = self.delegate('hub-zcode-res', workers=workers)
        self.assertEqual(first['outcome'], 'protocol-error')
        second = self.delegate('hub-zcode-res', workers=workers)
        self.assertEqual(second['outcome'], 'delivered', second.get('error'))
        self.assertEqual(len(created), 2)
        self.assertEqual([resident.starts for resident in created], [1, 1])
        self.assertEqual(created[0].runs, 1)          # no automatic replay
        self.assertGreaterEqual(created[0].stops, 1)  # old generation stopped

    def test_failed_task_is_never_replayed_within_one_delegate(self):
        created, client_cls, driver_cls = self.make_dying_appserver_family()
        self.register(self.codex_appserver_contract())
        result = self.delegate('hub-codex-app',
                               appserver_factory=lambda: client_cls('eof'),
                               appserver_driver_factory=driver_cls)
        self.assertEqual(result['outcome'], 'failed')
        self.assertEqual(created[0].runs, 1)  # exactly one attempt, no retry


class ConcurrentStartGuards(WorkerHealthHubFixture):
    """Guards (already true today via the double-checked lock): one shared
    start for concurrent first delegates."""

    def test_concurrent_first_delegates_start_the_appserver_once(self):
        class CountingClient:
            def __init__(self):
                self.starts = 0
                self.failure = None
                self.reader = None
                self.process = None

            async def start(self, budget=None, initialize_timeout=None):
                self.starts += 1
                return {'userAgent': 'counting'}

            async def close(self):
                pass

        self.register(self.codex_appserver_contract())
        workers = H.ResidentWorkers(self.config, self.evidence_root,
                                    appserver_factory=CountingClient)

        async def concurrent_start():
            return await asyncio.gather(*[workers.appserver_instance()
                                          for _ in range(8)])

        instances = asyncio.run(concurrent_start())
        self.assertEqual(len({id(instance) for instance in instances}), 1)
        self.assertEqual(instances[0].starts, 1)


# ---------------------------------------------------------------------------
# New-symbol contracts: worker_health unit matrices (lazy import; RED predates)
# ---------------------------------------------------------------------------

class GenerationGateUnitTests(unittest.IsolatedAsyncioTestCase):
    def module(self):
        from zcode_supervisor import worker_health
        return worker_health

    def make_gate(self, **kwargs):
        return self.module().WorkerGeneration(**kwargs)

    async def test_concurrent_first_waiters_start_exactly_one_generation(self):
        created = []

        class Instance:
            def __init__(self):
                self.starts = 0
                created.append(self)

            async def start(self):
                self.starts += 1
                await asyncio.sleep(0)  # yield inside the critical section

        gate = self.make_gate(factory=Instance)
        results = await asyncio.gather(*[gate.get_or_start() for _ in range(8)])
        self.assertEqual(len(created), 1)
        self.assertEqual({id(result['instance']) for result in results},
                         {id(created[0])})
        self.assertEqual({result['generation'] for result in results}, {1})

    async def test_failed_start_is_not_cached_as_a_generation(self):
        attempts = []

        class Flaky:
            def __init__(self):
                attempts.append(self)
                self.started = False

            async def start(self):
                if len(attempts) == 1:
                    raise RuntimeError('boot failed')

        gate = self.make_gate(factory=Flaky)
        with self.assertRaisesRegex(RuntimeError, 'boot failed'):
            await gate.get_or_start()
        self.assertIsNone(gate.current())
        second = await gate.get_or_start()
        self.assertEqual(len(attempts), 2)  # the next delegate starts fresh
        # A failed start consumed its generation number (stamped on the dead
        # instance before start) — the published generation is the next one.
        self.assertEqual(second['generation'], 2)

    async def test_invalidate_only_evicts_the_matching_generation(self):
        class Instance:
            def __init__(self):
                self.stops = 0

            async def start(self):
                pass

            async def stop(self):
                self.stops += 1

        gate = self.make_gate(factory=Instance)
        first = await gate.get_or_start()
        self.assertTrue(await gate.invalidate(
            first['generation'], {'failure': 'EOFError: stream closed'}))
        self.assertIsNone(gate.current())
        second = await gate.get_or_start()
        self.assertEqual(second['generation'], 2)
        # A stale event from generation 1 must NOT evict generation 2.
        self.assertFalse(await gate.invalidate(first['generation'],
                                                {'failure': 'stale'}))
        self.assertEqual(gate.current()['generation'], 2)
        self.assertEqual(first['instance'].stops, 1)   # gen1 was closed once
        self.assertEqual(second['instance'].stops, 0)  # gen2 was never touched

    async def test_invalidate_close_of_old_generation_is_bounded(self):
        release = asyncio.Event()

        class Hanging:
            def __init__(self):
                self.close_calls = 0

            async def start(self):
                pass

            async def close(self):
                self.close_calls += 1
                await release.wait()  # a hung old worker never finishes

        gate = self.make_gate(factory=Hanging, close_budget_s=0.05)
        first = await gate.get_or_start()
        started = time.monotonic()
        self.assertTrue(await gate.invalidate(first['generation'], {}))
        elapsed = time.monotonic() - started
        self.assertLess(elapsed, 2.0)  # the window is the bound, not the hang
        self.assertEqual(first['instance'].close_calls, 1)
        # The retirement lock serializes new starts with the bounded window:
        # the next generation is served right after it — blocked at most the
        # window, never by the (already given-up) hung close itself.
        release.set()
        second = await gate.get_or_start()
        self.assertEqual(second['generation'], 2)

    async def test_generation_identity_is_stamped_on_instance_and_client(self):
        class Client:
            generation = None

        class Wrapper:
            def __init__(self):
                self.client = Client()
                self.generation = None

            async def start(self):
                pass

        gate = self.make_gate(factory=Wrapper)
        first = await gate.get_or_start()
        second = await gate.get_or_start()
        self.assertEqual(first['instance'].generation, 1)
        self.assertEqual(first['instance'].client.generation, 1)
        # Stable reuse: same instance, no new generations.
        self.assertIs(second['instance'], first['instance'])
        self.assertEqual(second['generation'], 1)


class HealthEvidenceUnitTests(unittest.TestCase):
    def health(self, instance):
        from zcode_supervisor.worker_health import instance_health
        return instance_health(instance)

    def test_live_instance_with_running_reader_and_none_returncode_is_healthy(self):
        instance = SimpleNamespace(
            failure=None, reader=live_reader(),
            process=SimpleNamespace(returncode=None), generation=4)
        snapshot = self.health(instance)
        self.assertTrue(snapshot['healthy'])
        self.assertIsNone(snapshot['failure'])
        self.assertIsNone(snapshot['returncode'])  # live is NOT death evidence
        self.assertEqual(snapshot['generation'], 4)
        self.assertIn('observed_at', snapshot)
        json.dumps(snapshot)  # JSON-safe evidence record

    def test_failure_reader_end_and_real_returncode_are_each_death_evidence(self):
        cases = {
            'client-failure': SimpleNamespace(
                failure='EOFError: stream closed', reader=live_reader(),
                process=SimpleNamespace(returncode=None)),
            'reader-ended': SimpleNamespace(
                failure=None, reader=done_reader(),
                process=SimpleNamespace(returncode=None)),
            'real-returncode': SimpleNamespace(
                failure=None, reader=live_reader(),
                process=SimpleNamespace(returncode=1)),
        }
        for label, instance in cases.items():
            with self.subTest(label=label):
                self.assertFalse(self.health(instance)['healthy'])

    def test_ordinary_completed_task_is_not_death_evidence(self):
        # A finished TASK leaves no instance-level death signal: no failure,
        # reader alive, live process. It must not be inferred as death.
        instance = SimpleNamespace(failure=None, reader=live_reader(),
                                   process=SimpleNamespace(returncode=None))
        self.assertTrue(self.health(instance)['healthy'])


class ExitProjectionUnitTests(unittest.TestCase):
    def projection(self, on_event=None):
        from zcode_supervisor.worker_health import ExitProjection
        return ExitProjection(on_event=on_event)

    def test_awaited_exit_is_emitted_exactly_once_with_json_safe_record(self):
        seen = []
        projection = self.projection(on_event=seen.append)
        self.assertTrue(projection.observe_awaited_exit(7, generation=2))
        self.assertFalse(projection.observe_awaited_exit(7, generation=2))
        self.assertFalse(projection.observe_awaited_exit(0, generation=2))
        record = projection.emitted[0]
        self.assertEqual(record['type'], 'process.exited')
        self.assertEqual(record['returncode'], 7)
        self.assertEqual(record['generation'], 2)
        json.dumps(record)
        self.assertEqual(seen, [record])  # callback saw exactly one event

    def test_no_emission_without_an_awaited_exit(self):
        projection = self.projection()
        # Constructing the projection, or observing nothing, emits nothing:
        # EOF/failure isolate a generation but never fake an awaited exit.
        self.assertEqual(projection.emitted, [])


# ---------------------------------------------------------------------------
# New-symbol contracts: real-client exit projection (resident + app-server)
# ---------------------------------------------------------------------------

def fake_child(returncode=0):
    """Mock subprocess double: real StreamReader stdout; wait() actually
    sets returncode exactly like a reaped child."""
    child = Mock()
    child.pid = 4242
    child.returncode = None
    child.stdout = asyncio.StreamReader()
    child.stdin = Mock()
    child.stdin.drain = AsyncMock(return_value=None)

    async def wait():
        child.returncode = returncode
        return returncode

    child.wait = wait
    return child


class ResidentExitProjectionTests(unittest.IsolatedAsyncioTestCase):
    """Shared resident: process.exited fires once, only after the real
    stop/close path awaited the child's wait."""

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        for name in ('work', 'state', 'cache'):
            (self.root / name).mkdir()
        self.config = SimpleNamespace(work_root=self.root / 'work',
                                      state_dir=self.root / 'state',
                                      cache_dir=self.root / 'cache',
                                      notify=False,
                                      command=lambda: ['fixture-node'],
                                      environment=lambda: {'FIXTURE': '1'})
        self.child = fake_child(returncode=3)

    def make_resident(self):
        from zcode_supervisor import protocol, zcode_resident
        client_holder = {}

        def client_factory(workspace, output, permission, config):
            client = protocol.ProtocolClient(workspace, output, permission, config)
            client_holder['client'] = client
            return client

        resident = zcode_resident.ResidentZCode(
            self.config, self.root / 'work', client_factory=client_factory)
        return resident, client_holder

    async def test_exit_projection_requires_real_close_and_awaits_wait(self):
        resident, holder = self.make_resident()
        resident.generation = 7   # as WorkerGeneration stamps before start
        with patch.object(asyncio, 'create_subprocess_exec',
                          new=AsyncMock(return_value=self.child)):
            await resident.start()
        client = holder['client']
        self.assertEqual(client.generation, 7)  # identity reached the client
        projection = client.exit_projection
        # Ordinary traffic, reader EOF and a bare returncode assignment are
        # NOT awaited exits: zero emissions so far.
        client.process.stdout.feed_data(
            b'{"method":"session/event","params":{"type":"session.updated"}}\n')
        await asyncio.sleep(0)
        self.assertEqual(projection.emitted, [])
        client.process.stdout.feed_eof()      # EOF isolates, never projects
        await asyncio.sleep(0)
        self.assertEqual(projection.emitted, [])
        self.assertIsNotNone(client.failure)  # EOF did fail the client
        client.process.returncode = 3         # assignment alone is not a wait
        self.assertEqual(projection.emitted, [])
        # The real stop path: close -> actually awaited wait -> exactly once.
        await resident.stop()
        self.assertEqual(len(projection.emitted), 1)
        record = projection.emitted[0]
        self.assertEqual(record['returncode'], 3)
        self.assertEqual(record['generation'], 7)
        await resident.stop()                 # duplicate close: no second emit
        self.assertEqual(len(projection.emitted), 1)


class AppServerExitProjectionTests(unittest.IsolatedAsyncioTestCase):
    """Shared app-server client: same exactly-once awaited-exit contract."""

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.child = fake_child(returncode=5)

    async def test_exit_projection_requires_real_close_and_awaits_wait(self):
        from zcode_supervisor import codex_appserver
        client = codex_appserver.CodexAppServerClient(
            'unused-fake-cli', self.root / 'evidence')
        initialize = {'id': 'czs-1', 'result': {
            'codexHome': '/tmp/fake', 'platformFamily': 'unix',
            'platformOs': 'macos', 'userAgent': 'exit-projection/1'}}
        self.child.stdout.feed_data((json.dumps(initialize) + '\n').encode())
        self.child.stdout.feed_eof()
        with patch.object(codex_appserver.asyncio, 'create_subprocess_exec',
                          new=AsyncMock(return_value=self.child)):
            await client.start()
        self.addAsyncCleanup(client.close)
        projection = client.exit_projection
        client.generation = 5   # as the holder stamps it
        self.assertEqual(projection.emitted, [])  # EOF alone: no projection
        client.process.returncode = 5             # assignment: no projection
        self.assertEqual(projection.emitted, [])
        await client.close()
        self.assertEqual(len(projection.emitted), 1)
        self.assertEqual(projection.emitted[0]['returncode'], 5)
        self.assertEqual(projection.emitted[0]['generation'], 5)
        await client.close()                      # duplicate close: still one
        self.assertEqual(len(projection.emitted), 1)


# ---------------------------------------------------------------------------
# Round-2 review fixes: C1 termination gradient, I1-I5, Minor items
# ---------------------------------------------------------------------------

CHILD_SCRIPT = '''
import json, sys, threading, time

IGNORE_TERM = '--ignore-term' in sys.argv
if IGNORE_TERM:
    import signal
    signal.signal(signal.SIGTERM, signal.SIG_IGN)

def serve():
    # The reader thread ends at stdin EOF; the MAIN loop below keeps the
    # process alive, so this child ignores stdin close and only a real
    # signal (SIGTERM, or SIGKILL when --ignore-term) can end it.
    for line in sys.stdin:
        try:
            frame = json.loads(line)
        except ValueError:
            continue
        method = frame.get('method')
        if method == 'session/create':
            reply = {'id': frame['id'],
                     'result': {'session': {'sessionId': 'sess_child'}}}
        elif method == 'initialize':
            reply = {'id': frame['id'],
                     'result': {'codexHome': '/tmp/fake', 'platformFamily': 'unix',
                                'platformOs': 'macos', 'userAgent': 'real-child/1'}}
        elif method == 'bench/counters':
            reply = {'id': frame['id'], 'result': {'model_requests': 0}}
        else:
            reply = {'id': frame['id'], 'result': {}}
        sys.stdout.write(json.dumps(reply) + '\\n')
        sys.stdout.flush()

threading.Thread(target=serve, daemon=True).start()
while True:
    time.sleep(0.05)
'''


def kill_leaked_child(client):
    """RED-phase safety net: if a production bug leaks a live child, the
    test still reaps it instead of leaking the process."""
    process = getattr(client, 'process', None)
    if process is not None and process.returncode is None:
        with contextlib.suppress(ProcessLookupError, PermissionError, OSError):
            import signal as signal_module
            os.killpg(process.pid, signal_module.SIGKILL)


class RealChildTerminationTests(unittest.IsolatedAsyncioTestCase):
    """C1: production invalidate must REALLY terminate a stdin-ignoring,
    signal-responsive child inside the bounded window, await the reaped
    exit, and project it — not merely count close() calls."""

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.script = self.root / 'fake_child.py'
        self.script.write_text(CHILD_SCRIPT)
        self.workspace = self.root / 'ws'
        self.workspace.mkdir()

    def zcode_config(self, script):
        config = Mock()
        config.command.return_value = [sys.executable, '-u', str(script)]
        config.environment.return_value = {}
        return config

    async def wait_until(self, condition, timeout=5.0):
        return await wait_until(condition, timeout)

    async def test_stdin_ignoring_child_is_sigtermed_reaped_and_replaced(self):
        from zcode_supervisor import protocol, worker_health
        counter = {'n': 0}

        def factory():
            counter['n'] += 1
            return protocol.ProtocolClient(
                self.workspace, self.root / f'gen-{counter["n"]}.json',
                lambda _: {'decision': 'deny', 'reason': 'fixture'}, 
                self.zcode_config(self.script))

        gate = worker_health.WorkerGeneration(factory=factory)
        first = await gate.get_or_start()
        client = first['instance']
        self.addCleanup(lambda: kill_leaked_child(client))
        snapshot = await asyncio.wait_for(client.request(
            'session/create', {'workspace': {'workspacePath': str(self.workspace)}}), 5)
        self.assertEqual(snapshot['session']['sessionId'], 'sess_child')
        # Death evidence: EOF ends the real reader and fails the client.
        client.process.stdout.feed_eof()
        self.assertTrue(await self.wait_until(lambda: client.failure is not None))
        health = worker_health.instance_health(client)
        self.assertFalse(health['healthy'])
        self.assertTrue(await gate.invalidate(first['generation'], health))
        # C1: the child was REALLY terminated by SIGTERM, awaited, reaped.
        self.assertIsInstance(client.process.returncode, int)
        self.assertNotIsInstance(client.process.returncode, bool)
        self.assertEqual(client.process.returncode, -signal.SIGTERM)
        self.assertEqual(len(client.exit_projection.emitted), 1)
        self.assertEqual(client.exit_projection.emitted[0]['returncode'],
                         -signal.SIGTERM)
        # The next generation serves on a fresh child.
        second = await gate.get_or_start()
        self.addCleanup(lambda: kill_leaked_child(second['instance']))
        self.assertEqual(second['generation'], 2)
        snapshot2 = await asyncio.wait_for(second['instance'].request(
            'session/create', {'workspace': {'workspacePath': str(self.workspace)}}), 5)
        self.assertEqual(snapshot2['session']['sessionId'], 'sess_child')
        # Gen-2 kill-only: the replacement generation is intentionally left
        # alive here, so its open evidence streams are closed by the owning
        # test after the assertions (the cleanup kill reaps the child but
        # does not touch the files).
        for stream in (getattr(second['instance'], 'stderr', None),
                       getattr(second['instance'], 'wire', None)):
            if stream is not None and not stream.closed:
                stream.close()

    async def test_stdin_and_sigterm_ignoring_child_dies_by_sigkill(self):
        from zcode_supervisor import protocol, worker_health
        counter = {'n': 0}

        def factory():
            counter['n'] += 1
            return protocol.ProtocolClient(
                self.workspace, self.root / f'genk-{counter["n"]}.json',
                lambda _: {'decision': 'deny', 'reason': 'fixture'},
                self.zcode_config_with_flag('--ignore-term'))

        gate = worker_health.WorkerGeneration(factory=factory)
        first = await gate.get_or_start()
        client = first['instance']
        self.addCleanup(lambda: kill_leaked_child(client))
        client.process.stdout.feed_eof()
        self.assertTrue(await self.wait_until(lambda: client.failure is not None))
        self.assertTrue(await gate.invalidate(
            first['generation'], worker_health.instance_health(client)))
        self.assertEqual(client.process.returncode, -signal.SIGKILL)
        self.assertEqual(len(client.exit_projection.emitted), 1)
        # This kill-only path intentionally never runs the normal close
        # gradient, so the client's open evidence streams are closed HERE by
        # their owning test (the normal close path is already clean — see the
        # SIGTERM case above). The P0.3 once semantics asserted above are
        # unchanged.
        for stream in (getattr(client, 'stderr', None),
                       getattr(client, 'wire', None)):
            if stream is not None and not stream.closed:
                stream.close()

    def zcode_config_with_flag(self, flag):
        config = Mock()
        config.command.return_value = [sys.executable, '-u', str(self.script), flag]
        config.environment.return_value = {}
        return config

    async def test_appserver_client_terminates_stdin_ignoring_child(self):
        from zcode_supervisor import codex_appserver, worker_health
        client = codex_appserver.CodexAppServerClient(
            sys.executable, self.root / 'codex-evidence',
            env={'CHILD_FLAG': 'x'})
        # The child script must run with the right interpreter: codex_path
        # is invoked as `<path> app-server --listen stdio://`, so point the
        # path at a tiny wrapper that execs python on the real script.
        wrapper = self.root / 'codex-wrapper.sh'
        wrapper.write_text(f'#!/bin/sh\nexec {sys.executable} -u "{self.script}" "$@"\n')
        wrapper.chmod(0o755)
        client.codex_path = str(wrapper)
        self.addCleanup(lambda: kill_leaked_child(client))
        await asyncio.wait_for(client.start(), 5)
        client.process.stdout.feed_eof()
        self.assertTrue(await wait_until(lambda: client.failure is not None))
        gate = worker_health.WorkerGeneration(factory=lambda: client)
        # The client is already started; adopt it as generation 1 directly
        # (no second start) so invalidate exercises the production close.
        gate._current = {'instance': client, 'generation': 1}
        self.assertTrue(await gate.invalidate(
            1, worker_health.instance_health(client)))
        self.assertEqual(client.process.returncode, -signal.SIGTERM)
        self.assertEqual(len(client.exit_projection.emitted), 1)


class ReviewFixRound2Tests(unittest.IsolatedAsyncioTestCase):
    """I1-I5 and Minor contracts as gate-level deterministic tests."""

    def module(self):
        from zcode_supervisor import worker_health
        return worker_health

    # ---- I1: settle during cancel --------------------------------------

    async def test_cancel_absorbed_by_invalidate_cleanup_is_restored(self):
        worker_health = self.module()
        release = asyncio.Event()

        class SlowClose:
            def __init__(self):
                self.failure = None
                self.reader = None
                self.process = None
                self.closes = 0

            async def start(self):
                pass

            async def close(self):
                self.closes += 1
                await release.wait()

        gate = worker_health.WorkerGeneration(factory=SlowClose,
                                              close_budget_s=0.5)
        first = await gate.get_or_start()
        task = asyncio.create_task(gate.invalidate(first['generation'], {}))
        await asyncio.sleep(0.05)   # inside the bounded close window
        task.cancel()               # outer cancel arrives mid-cleanup
        with self.assertRaises(asyncio.CancelledError):
            await task              # must NOT return a settled True
        self.assertEqual(first['instance'].closes, 1)
        release.set()

    # ---- I2: run-period cancel still settles ----------------------------

    async def test_idle_dead_generation_is_retired_before_reuse(self):
        worker_health = self.module()
        created = []

        class Fake:
            def __init__(self):
                self.failure = None
                self.reader = None
                self.process = None
                self.closes = 0
                created.append(self)

            async def start(self):
                pass

            async def close(self):
                self.closes += 1

        gate = worker_health.WorkerGeneration(factory=Fake)
        first = await gate.get_or_start()
        # Idle death AFTER the previous delegate settled (late failure).
        first['instance'].failure = 'EOFError: late stream close'
        second = await gate.get_or_start()
        self.assertIsNot(second['instance'], first['instance'])
        self.assertEqual(second['generation'], first['generation'] + 1)
        self.assertEqual(first['instance'].closes, 1)   # retired under lock
        self.assertEqual(len(created), 2)

    # ---- I4: failed/cancelled start releases the half-built instance ----

    async def test_failed_start_closes_the_half_built_instance(self):
        worker_health = self.module()
        events = []

        class BrokenStart:
            def __init__(self, fail):
                self.fail = fail
                self.failure = None
                self.reader = None
                self.process = None

            async def start(self):
                if self.fail:
                    events.append('start-raise')
                    raise RuntimeError('start exploded')

            async def close(self):
                events.append('close')

        supply = iter((BrokenStart(fail=True), BrokenStart(fail=False)))
        gate = worker_health.WorkerGeneration(factory=lambda: next(supply))
        with self.assertRaisesRegex(RuntimeError, 'start exploded'):
            await gate.get_or_start()
        self.assertEqual(events, ['start-raise', 'close'])  # bounded release
        self.assertIsNone(gate.current())                    # nothing cached
        second = await gate.get_or_start()
        self.assertEqual(second['generation'], 2)            # next is gen+1

    async def test_cancelled_start_closes_and_reraises(self):
        worker_health = self.module()
        entered = asyncio.Event()
        release = asyncio.Event()
        events = []

        class BlockingStart:
            def __init__(self):
                self.failure = None
                self.reader = None
                self.process = None

            async def start(self):
                events.append('start-entered')
                entered.set()
                await release.wait()

            async def close(self):
                events.append('close')

        gate = worker_health.WorkerGeneration(factory=BlockingStart)
        task = asyncio.create_task(gate.get_or_start())
        await entered.wait()
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertIn('close', events)          # bounded release happened
        self.assertIsNone(gate.current())       # nothing cached
        release.set()
        second = await gate.get_or_start()
        self.assertEqual(second['generation'], 2)

    # ---- I5: exit evidence accepts only a REAL integer returncode ------

    async def test_non_integer_returncode_never_emits_or_poisons_dedup(self):
        worker_health = self.module()
        projection = worker_health.ExitProjection()
        self.assertFalse(projection.observe_awaited_exit(None, generation=1))
        self.assertFalse(projection.observe_awaited_exit(True, generation=1))
        self.assertEqual(projection.emitted, [])
        # A later genuine integer exit still emits exactly once.
        self.assertTrue(projection.observe_awaited_exit(-9, generation=1))
        self.assertEqual(len(projection.emitted), 1)
        self.assertEqual(projection.emitted[0]['returncode'], -9)

    async def test_blocked_wait_released_later_emits_once(self):
        """A wait stuck past the bounded window: no emission, no poisoned
        dedup; once released, the NEXT real close completes the wait and
        projects exactly once."""
        from zcode_supervisor import protocol, worker_health
        release = asyncio.Event()
        child = Mock()
        child.pid = 999
        child.returncode = None
        child.stdout = asyncio.StreamReader()
        child.stdin = Mock()
        child.stdin.drain = AsyncMock(return_value=None)

        async def blocked_wait():
            await release.wait()
            child.returncode = 6
            return 6

        child.wait = blocked_wait
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name).resolve()
        (root / 'ws').mkdir()
        spawn_config = Mock()
        spawn_config.command.return_value = ['fixture-node', 'cli.cjs']
        spawn_config.environment.return_value = {}
        with patch.object(asyncio, 'create_subprocess_exec',
                          new=AsyncMock(return_value=child)):
            client = protocol.ProtocolClient(
                root / 'ws', root / 'blocked.json',
                lambda _: {'decision': 'deny', 'reason': 'fixture'},
                spawn_config)
            await asyncio.wait_for(client.start(), 5)
        client.process.stdout.feed_eof()
        self.assertTrue(await wait_until(lambda: client.failure is not None))
        gate = worker_health.WorkerGeneration(factory=lambda: client,
                                              close_budget_s=0.2)
        # The client is already started; adopt it as generation 1 directly
        # (no second start) so invalidate exercises the production close.
        gate._current = {'instance': client, 'generation': 1}
        started = time.monotonic()
        self.assertTrue(await gate.invalidate(
            1, worker_health.instance_health(client)))
        self.assertLess(time.monotonic() - started, 3.0)   # bounded return
        self.assertEqual(client.exit_projection.emitted, [])  # window cut it
        release.set()
        await client.close()   # released: the wait completes for real now
        self.assertEqual(len(client.exit_projection.emitted), 1)
        self.assertEqual(client.exit_projection.emitted[0]['returncode'], 6)

    # ---- Minor: bounded lock serialization is the honest contract -------

    async def test_hung_old_close_serializes_new_start_within_the_window(self):
        worker_health = self.module()
        release = asyncio.Event()

        class HangingClose:
            def __init__(self):
                self.failure = None
                self.reader = None
                self.process = None
                self.closes = 0

            async def start(self):
                pass

            async def close(self):
                self.closes += 1
                await release.wait()

        gate = worker_health.WorkerGeneration(factory=HangingClose,
                                              close_budget_s=0.4)
        first = await gate.get_or_start()
        invalidator = asyncio.create_task(
            gate.invalidate(first['generation'], {}))
        await asyncio.sleep(0.05)   # the retirement lock is held mid-window
        starter = asyncio.create_task(gate.get_or_start())
        started = time.monotonic()
        second = await asyncio.wait_for(starter, 3.0)
        elapsed = time.monotonic() - started
        # Honest contract: the new start WAITS for the bounded retirement
        # (serialized by the lock) and is served right after the window —
        # blocked, but only ever for as long as the window allows.
        self.assertGreaterEqual(elapsed, 0.05)
        self.assertLess(elapsed, 3.0)
        self.assertEqual(second['generation'], 2)
        release.set()
        self.assertTrue(await invalidator)

    # ---- Minor: resident wrapper health shape + literal task.completed ---

    async def test_resident_wrapper_shape_reads_client_evidence(self):
        worker_health = self.module()
        client = SimpleNamespace(failure='EOFError: stream closed',
                                 reader=done_reader(),
                                 process=SimpleNamespace(returncode=None))
        wrapper = SimpleNamespace(client=client, generation=3)
        health = worker_health.instance_health(wrapper)
        self.assertFalse(health['healthy'])
        self.assertEqual(health['failure'], 'EOFError: stream closed')
        self.assertEqual(health['generation'], 3)  # identity from wrapper

    async def test_task_completed_frames_never_project_process_exit(self):
        """BOTH literal completion shapes flow through the production
        read_loop — a bare `task.completed` notification (unknown method:
        wire evidence only) and a `session/event` carrying
        params.type='task.completed' — and neither, before or after EOF,
        ever projects a process exit."""
        from zcode_supervisor import protocol
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        root = Path(temporary.name).resolve()
        child = Mock()
        child.pid = 4343
        child.returncode = None
        child.stdout = asyncio.StreamReader()
        child.stdin = Mock()
        child.stdin.drain = AsyncMock(return_value=None)

        async def wait():
            child.returncode = 0
            return 0

        child.wait = wait
        spawn_config = Mock()
        spawn_config.command.return_value = ['fixture-node', 'cli.cjs']
        spawn_config.environment.return_value = {}
        client = protocol.ProtocolClient(
            root, root / 'task-completed.json',
            lambda _: {'decision': 'deny', 'reason': 'fixture'}, spawn_config)
        with patch.object(asyncio, 'create_subprocess_exec',
                          new=AsyncMock(return_value=child)):
            await asyncio.wait_for(client.start(), 5)
        self.addAsyncCleanup(client.close)
        wire_path = client.output.with_suffix('.events.jsonl')

        async def pump(frames):
            for frame in frames:
                child.stdout.feed_data((json.dumps(frame) + '\n').encode())
            await asyncio.sleep(0)
            await asyncio.sleep(0)

        # (a) literal task.completed NOTIFICATION (no id): the production
        # read_loop only records it on the wire — nothing else happens.
        await pump([{'method': 'task.completed', 'params': {'turnId': 't1'}}])
        self.assertFalse(client.failure)
        self.assertEqual(client.events, [])
        self.assertIn('"task.completed"', wire_path.read_text())
        self.assertEqual(client.exit_projection.emitted, [])
        # (b) session/event whose payload type IS task.completed: it lands
        # in the events list like any session event — still no exit.
        event = {'type': 'task.completed', 'turnId': 't1', 'seq': 1,
                 'payload': {'resultType': 'success'}}
        await pump([{'method': 'session/event', 'params': event}])
        self.assertEqual(client.events, [event])
        self.assertEqual(client.exit_projection.emitted, [])
        self.assertIsNone(child.returncode)
        # EOF afterwards: isolation-grade failure, still no fabricated exit.
        child.stdout.feed_eof()
        self.assertTrue(await wait_until(lambda: client.failure is not None))
        self.assertEqual(client.exit_projection.emitted, [])
        # Only the real close (which awaits the child's wait) projects once.
        await client.close()
        self.assertEqual(len(client.exit_projection.emitted), 1)
        self.assertEqual(client.exit_projection.emitted[0]['returncode'], 0)


class ReviewFixRound3Tests(unittest.IsolatedAsyncioTestCase):
    """A pending outer cancel absorbed during the start-failure cleanup
    window must be restored — the gate must end in CancelledError, not the
    original start exception."""

    async def test_cancel_during_failed_start_cleanup_is_restored(self):
        from zcode_supervisor import worker_health
        start_entered = asyncio.Event()
        close_entered = asyncio.Event()
        release_close = asyncio.Event()
        calls = {'starts': 0, 'closes': 0}

        class FailingStartBlockingClose:
            def __init__(self):
                self.failure = None
                self.reader = None
                self.process = None

            async def start(self):
                calls['starts'] += 1
                start_entered.set()
                raise RuntimeError('start exploded')

            async def close(self):
                calls['closes'] += 1
                close_entered.set()
                await release_close.wait()

        gate = worker_health.WorkerGeneration(factory=FailingStartBlockingClose,
                                              close_budget_s=1.0)
        task = asyncio.create_task(gate.get_or_start())
        await start_entered.wait()
        await close_entered.wait()   # the failure cleanup holds the window
        task.cancel()                # outer cancel arrives inside the window
        release_close.set()          # the bounded close completes now
        with self.assertRaises(asyncio.CancelledError):
            await task               # restored cancel wins over RuntimeError
        self.assertEqual(calls['closes'], 1)
        self.assertEqual(gate.current(), None)


class HubDelegateCancellationTests(WorkerHealthHubFixture):
    """I1/I2 at the delegate level: cancellation never returns a result,
    still settles the shared generation, and never loses the records."""

    def make_workers(self, mode):
        created = []
        entered = asyncio.Event()
        release = asyncio.Event()
        close_entered = asyncio.Event()
        release_close = asyncio.Event()
        state = {'mode': mode}

        class Client:
            def __init__(self):
                self.failure = None
                self.reader = live_reader()
                self.process = SimpleNamespace(returncode=None)
                self.starts = 0
                self.closes = 0
                created.append(self)

            async def start(self, budget=None, initialize_timeout=None):
                self.starts += 1

            async def close(self):
                self.closes += 1
                close_entered.set()
                await release_close.wait()

        class Driver:
            def __init__(self, client):
                self.client = client

            async def run_task(self, authority, workspace, prompt, *,
                               model=None, sandbox='read-only',
                               approval_policy='on-request', timeout_s=600,
                               budget=None):
                if state['mode'] == 'cancel-run':
                    self.client.failure = 'EOFError: died during run'
                    entered.set()
                    await release.wait()
                else:  # cancel-settle: run completes, death lands with it
                    self.client.failure = 'injected late client.failure'
                return {'outcome': 'delivered', 'thread_id': 'th_x',
                        'turn_id': 't_x', 'final_message': 'ok',
                        'command_items': [], 'approvals': []}

        workers = H.ResidentWorkers(
            self.config, self.evidence_root,
            appserver_factory=Client, appserver_driver_factory=Driver)
        # tests inject a small retirement window; the production default
        # (worker_health.OLD_CLOSE_BUDGET_S) is not under test here.
        workers._appserver_gate.close_budget_s = 0.2
        return workers, created, entered, release, close_entered, release_close

    def test_cancel_during_run_settles_generation_and_keeps_records(self):
        self.register(self.codex_appserver_contract())
        workers, created, entered, release, _, _ = self.make_workers('cancel-run')

        async def scenario():
            task = asyncio.create_task(self.delegate_async('hub-codex-app',
                                                           workers=workers))
            await entered.wait()
            task.cancel()
            try:
                await task
                return 'returned'
            except asyncio.CancelledError:
                return 'cancelled'

        outcome = asyncio.run(scenario())
        release.set()
        self.assertEqual(outcome, 'cancelled')      # never a delivered result
        self.assertIsNone(workers._appserver_gate.current())  # dead gen retired
        self.assertEqual(created[0].closes, 1)      # I2: settle ran on cancel
        records = sorted((self.state_dir / 'hub' / 'tasks').glob('*.json'))
        self.assertEqual(len(records), 1)           # terminal record written
        record = json.loads(records[0].read_text())
        self.assertEqual(record['status'], 'failed')
        # The next delegate is served by a fresh generation, no replay.
        second = self.delegate('hub-codex-app', workers=workers)
        self.assertEqual(second['outcome'], 'delivered')
        self.assertEqual(len(created), 2)
        self.assertEqual(created[0].starts, 1)

    def test_cancel_absorbed_by_settle_close_is_restored_not_delivered(self):
        self.register(self.codex_appserver_contract())
        workers, created, _, _, close_entered, release_close = \
            self.make_workers('cancel-settle')

        async def scenario():
            task = asyncio.create_task(self.delegate_async('hub-codex-app',
                                                           workers=workers))
            await close_entered.wait()   # settle is inside the bounded close
            task.cancel()
            release_close.set()
            try:
                result = await task
                return ('returned', result['outcome'])
            except asyncio.CancelledError:
                return ('cancelled', None)

        what, result_outcome = asyncio.run(scenario())
        self.assertEqual(what, 'cancelled')         # I1: no delivered return
        self.assertEqual(created[0].closes, 1)
        self.assertIsNone(workers._appserver_gate.current())
        records = sorted((self.state_dir / 'hub' / 'tasks').glob('*.json'))
        record = json.loads(records[0].read_text())
        # The completed run's evidence is preserved, not rewritten as failure.
        self.assertEqual(record['status'], 'completed')
        self.assertEqual(record['outcome'], 'delivered')


    def test_cancel_during_failed_start_cleanup_not_runner_error_result(self):
        """Delegate level: the same interleave — the shared worker's start
        fails and an outer cancel arrives inside the gate's failure-cleanup
        window — must propagate CancelledError, never return a runner-error
        result, never replay the task."""
        self.register(self.codex_appserver_contract())
        start_entered = asyncio.Event()
        close_entered = asyncio.Event()
        release_close = asyncio.Event()
        created = []

        class BrokenStartClient:
            def __init__(self):
                self.failure = None
                self.reader = None
                self.process = None
                self.starts = 0
                self.closes = 0
                self.runs = 0
                created.append(self)

            async def start(self, budget=None, initialize_timeout=None):
                self.starts += 1
                start_entered.set()
                raise RuntimeError('app-server boot exploded')

            async def close(self):
                self.closes += 1
                close_entered.set()
                await release_close.wait()

        class UnusedDriver:
            def __init__(self, client):
                self.client = client

            async def run_task(self, *args, **kwargs):
                self.client.runs += 1
                return {'outcome': 'delivered', 'thread_id': 'th_never'}

        workers = H.ResidentWorkers(
            self.config, self.evidence_root,
            appserver_factory=BrokenStartClient,
            appserver_driver_factory=UnusedDriver)
        workers._appserver_gate.close_budget_s = 1.0

        async def scenario():
            task = asyncio.create_task(self.delegate_async('hub-codex-app',
                                                           workers=workers))
            await start_entered.wait()
            await close_entered.wait()
            task.cancel()
            release_close.set()
            try:
                result = await task
                return ('returned', result['outcome'])
            except asyncio.CancelledError:
                return ('cancelled', None)

        what, result_outcome = asyncio.run(scenario())
        self.assertEqual(what, 'cancelled')   # not a runner-error result
        self.assertEqual(created[0].starts, 1)
        self.assertEqual(created[0].closes, 1)   # bounded release happened
        self.assertEqual(created[0].runs, 0)     # no replay, no run at all
        self.assertIsNone(workers._appserver_gate.current())
        records = sorted((self.state_dir / 'hub' / 'tasks').glob('*.json'))
        record = json.loads(records[0].read_text())
        self.assertEqual(record['status'], 'failed')  # terminal record kept

    def delegate_async(self, contract_id, **kwargs):
        return H.delegate_impl(
            self.config, self.evidence_root, contract_id,
            {'agent': 'codex-control', 'session_id': 'sess_ctrl1'},
            codex_path=str(self.fake_codex), **kwargs)


if __name__ == '__main__':
    unittest.main()
