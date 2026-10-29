"""P3.1 communication fault matrix (P3 dispatch item 2, PLAN §P3 line 567).

The 13 PLAN items, one test (or subTest group) each — all fake, zero
model requests, zero real CLIs:

 1. blocked drain            — a session whose event stream never yields a
                               completion: bounded timeout, evidence kept,
                               process reusable, no silent loss.
 2. delayed start            — a shared client whose start is slow: ONE
                               start, concurrent delegates wait and share
                               the generation.
 3. cancel during run        — outer cancel mid-turn: cancelled caller,
                               bounded session/stop cleanup, evidence
                               written, no replay, process reusable.
 4. known late response      — a locally cancelled request whose response
                               arrives late: tombstoned, consumed once on
                               the wire, client stays usable, no
                               cross-task resolution.
 5. unknown late response    — an unmatched response frame: fail-closed.
 6. EOF / nonzero exit / late failure — three death shapes of a shared
                               resident client: current task fails
                               explainably, next delegate gets a fresh
                               generation, no replay, no fabricated exit.
 7. dual workspace           — two concurrent workspaces on the ONE shared
                               client: isolated sessions/results/decisions.
 8. missing identity         — TWO CONCURRENT handlers on the shared
                               client; identity-less (and unknown-session)
                               permission requests reach NEITHER Authority
                               (both sides' call counts stay at the valid
                               baseline, routing denies + alerts) plus a
                               valid-identity positive control proving
                               routing is not shut off.
 9. external write           — a frozen workspace input mutated mid-run:
                               postflight-rejected, never silently accepted.
10. slow UI consumption      — a subscriber that never drains: bounded
                               queue, explicit reset, journal stays healthy.
11. queue overflow           — a bounded event-router backlog overflows:
                               the OWNING subscription fails loudly, its
                               peer keeps every frame, nothing unbounded.
12. journal generation       — a journal cut while consumers live: paged
                               continuation across the cut loses nothing,
                               forged continuations refused.
13. malformed / oversized frames — non-object JSON, invalid JSON,
                               non-dict params, and a line past the
                               StreamReader limit: every shape fails the
                               client closed; nothing is silently accepted.

Results are scoped to "the UI projection under test and the fake
production routing surface" — they are NOT extrapolated to every real
service version.
"""

import asyncio
import io
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock, patch

from zcode_supervisor import protocol, worker_health, zcode_resident
from zcode_supervisor.authority import Authority
from zcode_supervisor.ui_events import HUB_STREAM_ID
from zcode_supervisor.ui_projection import UiEmitter
from zcode_supervisor.ui_journal import UiJournal
from zcode_supervisor.zcode_resident import ResidentZCode

MODEL = {'providerId': 'fixture-provider', 'modelId': 'fixture-model'}


# ---------------------------------------------------------------------------
# Resident fixture: compact FakeClient/FakeServer (no real child)
# ---------------------------------------------------------------------------

class FakeServer:
    """Scripted shared-client behaviour for resident-mode matrix items."""

    def __init__(self):
        self.calls = []
        self.starts = 0
        self.created = []
        self.stops = []
        self.commands = {}          # sessionId -> reviewed command
        self.foreign = {}           # sessionId -> injected foreign request
        self.inject = {}            # sessionId -> list of raw permission frames
        self.fail_turn = set()
        self.never_complete = set()
        self.die_on_first_turn = None   # 'eof' | 'nonzero-exit' | 'late-failure'
        self.start_delay_s = 0.0
        self.script_tasks = []
        self.seq = 0
        self.turn_counts = {}
        self.on_started_hooks = []

    def new_session(self):
        sid = f'sess_{chr(ord("A") + len(self.created))}'
        self.created.append(sid)
        return sid

    def snapshot(self, sid):
        return {'session': {'sessionId': sid, 'status': 'idle'},
                'runtime': {'pendingRequestIds': []}}

    def emit(self, client, sid, turn, type_, payload):
        self.seq += 1
        client.events.append({'type': type_, 'sessionId': sid, 'turnId': turn,
                              'seq': self.seq, 'payload': payload})

    async def script(self, client, sid, input_id):
        """One dispatched turn: started, optional probes, completion."""
        self.turn_counts[sid] = self.turn_counts.get(sid, 0) + 1
        turn = f'turn_{sid}_{self.turn_counts[sid]}'
        firstborn = getattr(client, 'firstborn', False)
        if self.die_on_first_turn == 'late-failure' and firstborn:
            # the run completes cleanly, the failure lands right after
            self.emit(client, sid, turn, 'turn.started',
                      {'inputId': input_id})
            self.emit(client, sid, turn, 'turn.completed',
                      {'resultType': 'success'})
            await asyncio.sleep(0.02)
            client.failure = 'EOFError: late stream close'
            return
        self.emit(client, sid, turn, 'turn.started', {'inputId': input_id})
        for hook in self.on_started_hooks:
            hook(sid)
        if sid in self.fail_turn or (firstborn and self.die_on_first_turn):
            if self.die_on_first_turn == 'eof':
                client.failure = 'EOFError: stream closed'
                client.reader = SimpleNamespace(done=lambda: True)
            elif self.die_on_first_turn == 'nonzero-exit':
                # a dead child: EOF ends the reader AND the returncode is
                # reaped — the realistic combined death evidence
                client.failure = 'EOFError: stream closed'
                client.reader = SimpleNamespace(done=lambda: True)
                client.process = SimpleNamespace(pid=4242, returncode=1)
            else:
                self.emit(client, sid, turn, 'turn.failed', {'error': 'fx'})
            return
        command = self.commands.get(sid)
        if command is not None:
            call = f'call_{turn}'
            self.emit(client, sid, turn, 'model.streaming',
                      {'kind': 'tool_call', 'toolName': 'Bash',
                       'toolCallId': call,
                       'input': {'command': command, 'timeout': 1000}})
            self.emit(client, sid, turn, 'tool.updated',
                      {'kind': 'scheduled', 'toolName': 'Bash',
                       'toolCallId': call,
                       'input': {'command': command, 'timeout': 1000}})
            await asyncio.sleep(0.03)
            # the session's OWN valid command permission (routed to the
            # owning authority, decided on the reviewed contract)
            client.permission({'sessionId': sid, 'turnId': turn,
                               'requestId': f'req_{turn}',
                               'toolCallId': call, 'toolName': 'Bash',
                               'input': {'command': command,
                                         'timeout': 1000}})
            for raw in self.inject.get(sid, []):
                client.permission(dict(raw))
            foreign = self.foreign.get(sid)
            if foreign:
                client.permission({'sessionId': foreign,
                                   'turnId': f'turn_{foreign}_x',
                                   'requestId': f'req_foreign_{sid}',
                                   'toolCallId': f'call_foreign_{sid}',
                                   'toolName': 'Bash',
                                   'input': {'command': 'echo foreign',
                                             'timeout': 1000}})
            await asyncio.sleep(0.02)
            self.emit(client, sid, turn, 'tool.updated', {
                'kind': 'result', 'toolCallId': call,
                'result': {'success': True, 'truncated': False,
                           'content': json.dumps(
                               {'stdout': 'ok', 'stderr': '',
                                'status': 'completed', 'exitCode': 0,
                                'interrupted': False}),
                           'perf': {'detail': {'kind': 'command', 'command': {
                               'exitCode': 0, 'timedOut': False,
                               'status': 'completed', 'runMs': 5}}}}})
        else:
            await asyncio.sleep(0.03)
            for raw in self.inject.get(sid, []):
                client.permission(dict(raw))
        if sid in self.never_complete:
            return                      # blocked drain: no completion
        self.emit(client, sid, turn, 'turn.completed',
                  {'resultType': 'success'})


class FakeClient:
    """One shared long-lived client double (resident double)."""

    def __init__(self, owner, workspace, output, permission, config=None):
        self.owner = owner
        self.workspace = workspace
        self.output = output
        self.permission = permission
        self.config = config
        self.events = []
        self.on_event = None
        self.failure = None
        self.reader = None
        self.process = SimpleNamespace(pid=4242, returncode=None)
        self.generation = None
        self.exit_projection = worker_health.ExitProjection()
        self.packets = 0

    async def start(self):
        self.owner.starts += 1
        if self.owner.start_delay_s:
            await asyncio.sleep(self.owner.start_delay_s)

    async def close(self):
        # a real close path: the fake wait reaps the child -> the
        # projection observes the awaited exit exactly once
        self.process.returncode = 0
        self.exit_projection.observe_awaited_exit(
            self.process.returncode, generation=self.generation,
            source='close')

    async def request(self, method, params):
        owner = self.owner
        owner.calls.append((method, params))
        if method == 'session/create':
            return owner.snapshot(owner.new_session())
        if method == 'session/read':
            return owner.snapshot(params['sessionId'])
        if method == 'session/subscribe':
            return {'eventSeq': 0}
        if method == 'session/send':
            self.packets += 1
            owner.script_tasks.append(asyncio.create_task(
                owner.script(self, params['sessionId'], params['inputId'])))
            return {}
        if method == 'session/stop':
            owner.stops.append(params['sessionId'])
            return {}
        return {}


class ResidentMatrixFixture(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        for name in ('work', 'state', 'cache', 'packets'):
            (self.root / name).mkdir()
        self.config = SimpleNamespace(work_root=self.root / 'work',
                                      state_dir=self.root / 'state',
                                      cache_dir=self.root / 'cache',
                                      notify=False)
        self.server = FakeServer()
        self.clients = []

        def factory(workspace, output, permission, config):
            client = FakeClient(self.server, workspace, output, permission,
                                config)
            self.clients.append(client)
            return client

        self.resident = ResidentZCode(self.config, self.root / 'work',
                                      client_factory=factory)
        self.registry = self.resident.registry

    async def asyncTearDown(self):
        if self.server.script_tasks:
            await asyncio.gather(*self.server.script_tasks,
                                 return_exceptions=True)
        await self.resident.stop()

    def workspace(self, name):
        ws = self.root / 'work' / name
        ws.mkdir()
        return ws

    def packet(self, name, ws, *, command=None, prompt='Fixture work'):
        body = {'workspace': str(ws), 'prompt': prompt, 'allowed_files': []}
        if command is not None:
            body['commands'] = [{'id': f'cmd_{name}', 'command': command,
                                 'max_uses': 1, 'max_timeout_ms': 5000}]
        path = self.root / 'packets' / f'{name}.json'
        path.write_text(json.dumps(body))
        return path

    async def drive(self, name, ws, *, timeout_s=5, command=None, prompt=None):
        return await self.resident.run_packet(
            self.packet(name, ws, command=command,
                        prompt=prompt or 'Fixture work'),
            self.root / 'work' / f'{name}.run.json', timeout_s=timeout_s)

    def run_result(self, name):
        return json.loads(
            (self.root / 'work' / f'{name}.run.json').read_text())


# ---------------------------------------------------------------------------
# Item 1: blocked drain
# ---------------------------------------------------------------------------

class Fault01BlockedDrainTests(ResidentMatrixFixture):
    async def test_blocked_drain_times_out_with_evidence_and_reuse(self):
        await self.resident.start()
        ws = self.workspace('wsA')
        self.server.never_complete = {'sess_A'}
        result = await self.drive('blocked', ws, timeout_s=1)
        self.assertEqual(result['outcome'], 'timeout')
        self.assertEqual(result['cleanup_stop'], 'sent')
        self.assertIsNotNone(result['turn_id'])   # turn.started was consumed
        on_disk = self.run_result('blocked')      # evidence persisted
        self.assertEqual(on_disk['outcome'], 'timeout')
        self.assertIn('sess_A', self.registry._load())   # session recorded
        self.assertEqual(self.registry._load()['sess_A']['state'], 'lost')
        # the shared process stays usable for the next packet
        ws2 = self.workspace('wsB')
        second = await self.drive('after_block', ws2)
        self.assertEqual(second['outcome'], 'delivered')
        self.assertEqual(self.clients[0].packets, 2)   # same shared client


# ---------------------------------------------------------------------------
# Item 2: delayed start
# ---------------------------------------------------------------------------

class Fault02DelayedStartTests(ResidentMatrixFixture):
    async def test_delayed_shared_start_is_single_and_awaited(self):
        self.server.start_delay_s = 0.3
        gate = worker_health.WorkerGeneration(factory=lambda: self.resident)
        first, second = await asyncio.gather(gate.get_or_start(),
                                             gate.get_or_start())
        self.assertIs(first['instance'], second['instance'])
        self.assertEqual(first['generation'], second['generation'])
        self.assertEqual(self.server.starts, 1)   # exactly ONE start
        self.assertIs(self.resident.client, self.clients[0])
        self.assertEqual(self.clients[0].generation, first['generation'])
        # the delayed client actually serves work afterwards
        ws = self.workspace('wsA')
        result = await self.drive('delayed', ws)
        self.assertEqual(result['outcome'], 'delivered')


# ---------------------------------------------------------------------------
# Item 3: cancel during run
# ---------------------------------------------------------------------------

class Fault03CancelDuringRunTests(ResidentMatrixFixture):
    async def test_cancel_mid_turn_settles_and_keeps_evidence(self):
        await self.resident.start()
        ws = self.workspace('wsA')
        self.server.never_complete = {'sess_A'}
        task = asyncio.create_task(self.drive('cancelled', ws, timeout_s=5))
        await asyncio.sleep(0.3)              # mid-turn (started, no end)
        task.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await task
        on_disk = self.run_result('cancelled')
        self.assertEqual(on_disk['outcome'], 'cancelled')
        self.assertEqual(on_disk['cleanup_stop'], 'sent')
        self.assertEqual(self.registry._load()['sess_A']['state'], 'lost')
        self.assertEqual(self.clients[0].packets, 1)   # no replay
        # the shared process serves the next packet normally
        ws2 = self.workspace('wsB')
        second = await self.drive('after_cancel', ws2)
        self.assertEqual(second['outcome'], 'delivered')


# ---------------------------------------------------------------------------
# Items 4/5/13: ProtocolClient stream faults (unit-level fakes)
# ---------------------------------------------------------------------------

def bare_client(root, stdout_limit=2 ** 20):
    """A ProtocolClient with a hand-fed stdout and an in-memory wire."""
    client = protocol.ProtocolClient(
        Path(root) / 'ws', Path(root) / 'run.json',
        lambda params: {'decision': 'deny', 'reason': 'fixture'})
    client.wire = io.StringIO()
    stdin = Mock()
    stdin.drain = AsyncMock(return_value=None)
    client.process = SimpleNamespace(pid=99, returncode=None, stdin=stdin,
                                     stdout=asyncio.StreamReader(
                                         limit=stdout_limit))
    return client


class Fault04KnownLateResponseTests(unittest.IsolatedAsyncioTestCase):
    async def test_known_late_response_consumed_once_client_usable(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        client = bare_client(temporary.name)
        reader = asyncio.create_task(client.read_loop())

        first = asyncio.create_task(client.request('session/create', {}))
        second = asyncio.create_task(client.request('session/read', {}))
        for _ in range(20):
            await asyncio.sleep(0)
        self.assertEqual(sorted(client.pending), ['host-1', 'host-2'])
        first.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await first                        # local timeout/cancel path
        # exactly the abandoned id is tombstoned
        self.assertEqual(len(client.tombstones), 1)
        # the LATE response to host-1 arrives while host-2 still waits
        client.process.stdout.feed_data(
            b'{"id":"host-1","result":{"session":{"sessionId":"sess_late"}}}\n')
        for _ in range(20):
            await asyncio.sleep(0)
        # consumed exactly once, recorded on the wire
        wire = client.wire.getvalue()
        self.assertIn('"late_response_to": "host-1"', wire)
        self.assertFalse(second.done())        # no cross-task resolution
        self.assertIsNone(client.failure)      # known-late is not fatal
        # the client is still usable: host-2 gets its own answer
        client.process.stdout.feed_data(
            b'{"id":"host-2","result":{"session":{"sessionId":"sess_2"}}}\n')
        answer = await asyncio.wait_for(second, 2)
        self.assertEqual(answer['session']['sessionId'], 'sess_2')
        reader.cancel()
        await asyncio.gather(reader, return_exceptions=True)


class Fault05UnknownLateResponseTests(unittest.IsolatedAsyncioTestCase):
    async def test_unknown_response_fails_closed(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        client = bare_client(temporary.name)
        pending = asyncio.get_running_loop().create_future()
        client.pending['host-9'] = pending
        reader = asyncio.create_task(client.read_loop())
        client.process.stdout.feed_data(b'{"id":"host-404","result":{}}\n')
        for _ in range(50):
            if client.failure:
                break
            await asyncio.sleep(0.01)
        self.assertIsNotNone(client.failure)     # fail-closed, not dropped
        self.assertIn('Unmatched', client.failure)
        with self.assertRaises(Exception):
            await pending                        # waiters end explainably
        self.assertEqual(client.pending, {})
        await asyncio.gather(reader, return_exceptions=True)


class Fault13MalformedOversizedFrameTests(unittest.IsolatedAsyncioTestCase):
    """One method per shape: every malformed/oversized frame fails the
    client closed; nothing is silently accepted."""

    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = temporary.name

    async def _fail_closed(self, payload, limit=2 ** 20):
        client = bare_client(self.root, stdout_limit=limit)
        pending = asyncio.get_running_loop().create_future()
        client.pending['host-1'] = pending
        client.process.stdout.feed_data(payload)
        await client.read_loop()                 # runs to its failure
        self.assertIsNotNone(client.failure,
                             f'frame was silently accepted: {payload!r}')
        with self.assertRaises(Exception):
            await pending

    async def test_non_object_frame_fails_closed(self):
        await self._fail_closed(b'[1, 2]\n')

    async def test_invalid_json_line_fails_closed(self):
        await self._fail_closed(b'{not json\n')

    async def test_non_dict_notification_params_fail_closed(self):
        await self._fail_closed(
            b'{"method":"session/event","params":"not-a-dict"}\n')

    async def test_oversized_line_fails_closed_bounded(self):
        huge = b'{"id":"host-1","result":"' + b'x' * 400 + b'"}\n'
        await self._fail_closed(huge, limit=128)


# ---------------------------------------------------------------------------
# Item 6: EOF / nonzero exit / late failure (shared resident client deaths)
# ---------------------------------------------------------------------------

class Fault06DeathShapesTests(ResidentMatrixFixture):
    """Each death shape, through the REAL gate retirement path."""

    async def test_all_three_death_shapes_replace_on_next_delegate(self):
        for mode in ('eof', 'nonzero-exit', 'late-failure'):
            with self.subTest(mode=mode):
                temporary = tempfile.TemporaryDirectory()
                self.addCleanup(temporary.cleanup)
                root = Path(temporary.name).resolve()
                for name in ('work', 'state', 'cache'):
                    (root / name).mkdir()
                config = SimpleNamespace(work_root=root / 'work',
                                         state_dir=root / 'state',
                                         cache_dir=root / 'cache',
                                         notify=False)
                server = FakeServer()
                server.die_on_first_turn = mode
                created = []
                firstborn = {'value': True}

                def client_factory(workspace, output, permission, cfg):
                    client = FakeClient(server, workspace, output,
                                        permission, cfg)
                    client.firstborn = firstborn['value']
                    firstborn['value'] = False
                    created.append(client)
                    return client

                def resident_factory():
                    return ResidentZCode(config, root / 'work',
                                         client_factory=client_factory)

                gate = worker_health.WorkerGeneration(
                    factory=resident_factory)
                ws = root / 'work' / 'wsA'
                ws.mkdir()
                packet_path = root / 'packet.json'
                packet_path.write_text(json.dumps(
                    {'workspace': str(ws), 'prompt': 'Fixture work',
                     'allowed_files': []}))

                first = await gate.get_or_start()
                dying = first['instance']
                result_one = await dying.run_packet(
                    packet_path, root / 'work' / 'one.json', timeout_s=5)
                # the current task fails explainably (late-failure delivers
                # first, then the client dies idle)
                if mode == 'late-failure':
                    self.assertEqual(result_one['outcome'], 'delivered')
                    for _ in range(100):
                        if dying.client.failure:
                            break
                        await asyncio.sleep(0.01)
                else:
                    self.assertEqual(result_one['outcome'], 'protocol-error')
                dying_client = dying.client   # stop() nulls .client
                # death evidence alone NEVER fabricated an awaited exit
                self.assertEqual(dying_client.exit_projection.emitted, [])
                health = worker_health.instance_health(dying)
                self.assertFalse(health['healthy'], mode)
                # the production retire path: bounded close -> the real
                # (fake-)waited exit projected EXACTLY once
                self.assertTrue(
                    await gate.invalidate(first['generation'], health))
                self.assertEqual(
                    len(dying_client.exit_projection.emitted), 1)
                # the next delegate is a FRESH generation and delivers
                second = await gate.get_or_start()
                self.assertIsNot(second['instance'], dying)
                self.assertEqual(second['generation'],
                                 first['generation'] + 1)
                result_two = await second['instance'].run_packet(
                    packet_path, root / 'work' / 'two.json', timeout_s=5)
                self.assertEqual(result_two['outcome'], 'delivered')
                # no replay: the dying client saw exactly one packet
                self.assertEqual(created[0].packets, 1)
                self.assertEqual(created[1].packets, 1)
                await second['instance'].stop()


# ---------------------------------------------------------------------------
# Item 7: dual workspace
# ---------------------------------------------------------------------------

class Fault07DualWorkspaceTests(ResidentMatrixFixture):
    async def test_two_workspaces_isolated_on_one_shared_client(self):
        await self.resident.start()
        ws_a, ws_b = self.workspace('wsA'), self.workspace('wsB')
        self.server.commands = {'sess_A': '/bin/echo A',
                                'sess_B': '/bin/echo B'}
        # session A also receives a FOREIGN permission claiming B's session
        # with a foreign turn: B's authority is consulted (the session IS
        # registered) but its identity checks DENY it; the denial must
        # surface in B's records only — never in A's result.
        self.server.foreign = {'sess_A': 'sess_B'}
        result_a, result_b = await asyncio.gather(
            self.drive('dual_a', ws_a, command='/bin/echo A'),
            self.drive('dual_b', ws_b, command='/bin/echo B'))
        self.assertEqual(result_a['outcome'], 'delivered', result_a)
        self.assertEqual(result_b['outcome'], 'delivered', result_b)
        self.assertEqual(result_a['session_id'], 'sess_A')
        self.assertEqual(result_b['session_id'], 'sess_B')
        decisions_a = result_a['permission_decisions']
        decisions_b = result_b['permission_decisions']
        # A decided exactly its own valid command; the foreign claim landed
        # ONLY on B's authority (denied by identity checks)
        self.assertEqual(len(decisions_a), 1)
        self.assertEqual(decisions_a[0]['decision'], 'allow')
        self.assertEqual(decisions_a[0]['request_id'], 'req_turn_sess_A_1')
        foreign = [record for record in decisions_b
                   if record['request_id'] == 'req_foreign_sess_A']
        self.assertEqual(len(foreign), 1)
        self.assertEqual(foreign[0]['decision'], 'deny')
        self.assertEqual(len(decisions_b), 2)   # own allow + foreign deny
        self.assertEqual(len(self.clients), 1)  # ONE shared client


# ---------------------------------------------------------------------------
# Item 8: missing identity (two concurrent handlers, shared client)
# ---------------------------------------------------------------------------

class CountingAuthority(Authority):
    """Authority double counting decide() calls per instance."""

    instances = []

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.decide_calls = 0
        CountingAuthority.instances.append(self)

    def decide(self, params):
        self.decide_calls += 1
        return super().decide(params)


class Fault08MissingIdentityTests(ResidentMatrixFixture):
    async def test_missing_identity_reaches_neither_authority(self):
        CountingAuthority.instances = []
        with patch.object(zcode_resident, 'Authority', CountingAuthority):
            await self.resident.start()
            ws_a, ws_b = self.workspace('wsA'), self.workspace('wsB')
            self.server.commands = {'sess_A': '/bin/echo A',
                                    'sess_B': '/bin/echo B'}
            # BOTH handlers are mid-turn when the identity-less and
            # unknown-session requests hit the ONE shared client
            self.server.inject = {
                'sess_A': [
                    {'requestId': 'req_noid', 'toolCallId': 'call_noid',
                     'toolName': 'Bash',
                     'input': {'command': 'rm -rf /', 'timeout': 1000}},
                    {'sessionId': 'sess_unknown', 'turnId': 'turn_ghost',
                     'requestId': 'req_ghost', 'toolCallId': 'call_ghost',
                     'toolName': 'Bash',
                     'input': {'command': 'rm -rf /', 'timeout': 1000}},
                ],
            }
            results = await asyncio.gather(
                self.drive('noid_a', ws_a, command='/bin/echo A'),
                self.drive('noid_b', ws_b, command='/bin/echo B'))
        authorities = {id(authority): authority
                       for authority in CountingAuthority.instances}
        self.assertEqual(len(authorities), 2)   # two concurrent handlers
        # BOTH delivered: the shared client kept serving both handlers
        self.assertEqual(results[0]['outcome'], 'delivered', results[0])
        self.assertEqual(results[1]['outcome'], 'delivered', results[1])
        # POSITIVE CONTROL: every authority DID route its own valid command
        # permission exactly once — routing is NOT shut off
        for authority in authorities.values():
            self.assertEqual(authority.decide_calls, 1,
                             'positive control failed: valid identity did '
                             'not route exactly once')
        # THE ASSERTION: the missing-identity + unknown-session requests
        # reached NEITHER side — both counts stayed at the valid baseline
        # (0 additional Authority calls on either handler's side)
        for authority in authorities.values():
            self.assertEqual(authority.decide_calls, 1)
        # the routing denied both injected requests and alerted
        alerts = {alert['request_id'] for alert in self.resident.alerts}
        self.assertIn('req_noid', alerts)
        self.assertIn('req_ghost', alerts)
        # and no injected decision leaked into either receipt
        for result in results:
            for record in result['permission_decisions']:
                self.assertNotIn(record.get('request_id'),
                                 ('req_noid', 'req_ghost'))


# ---------------------------------------------------------------------------
# Item 9: external write to a frozen surface
# ---------------------------------------------------------------------------

class Fault09ExternalWriteTests(ResidentMatrixFixture):
    async def test_frozen_input_mutation_is_postflight_rejected(self):
        await self.resident.start()
        ws = self.workspace('wsE')
        frozen = ws / 'INPUT.md'
        frozen.write_text('original frozen content\n')
        # the scripted turn mutates the frozen file mid-run (an "external"
        # writer on the workspace the worker does not own)
        def mutate(sid):
            frozen.write_text('mutated by external writer\n')
        self.server.on_started_hooks = [mutate]
        result = await self.drive('external', ws)
        self.assertEqual(result['outcome'], 'postflight-rejected')
        self.assertFalse(result['delivery_ok'])
        self.assertTrue(result.get('scope_error'))
        on_disk = self.run_result('external')
        self.assertEqual(on_disk['outcome'], 'postflight-rejected')
        self.assertFalse(on_disk['delivery_ok'])


# ---------------------------------------------------------------------------
# Item 10: slow UI consumption
# ---------------------------------------------------------------------------

HEX64 = '3' * 64
RFC = '2070-01-01T00:00:00.000000Z'


def journal_emitter():
    return UiEmitter(hub_instance_id='faultmatrix', journal_epoch=1,
                     enabled=True)


def emit_accepted(emitter, task_id):
    return emitter.emit('task.accepted', task_id, task_id=task_id,
                        worker='codex', mode='app-server',
                        data={'contract_id': f'contract-{task_id}',
                              'contract_digest': HEX64, 'created_at': RFC,
                              'initiator_verified': False})


def emit_turn(emitter, task_id, session_id, turn_id):
    return emitter.emit('turn.started', task_id, task_id=task_id,
                        worker='codex', mode='app-server',
                        session_id=session_id, turn_id=turn_id)


class Fault10SlowUiConsumptionTests(unittest.TestCase):
    def test_slow_subscriber_is_bounded_and_reset_not_unbounded(self):
        emitter = journal_emitter()
        journal = UiJournal(emitter, max_events=200, max_bytes=1 << 20,
                            max_event_bytes=4096, page_max_records=3,
                            page_max_bytes=2048, registry_max_cursors=2,
                            registry_ttl_s=300.0,
                            queue_max_events=4, queue_max_bytes=8192)
        emit_accepted(emitter, 'task0001')
        emitter.emit('stream.announced', HUB_STREAM_ID, task_id='task0001',
                     worker='codex', mode='app-server',
                     data={'task_stream_id': 'task0001',
                           'accepted_event_id': 'x'})
        cut = journal.new_cut()
        slow = journal.subscribe_all(cut['snapshot_cursor'])
        for index in range(30):       # burst far past the queue bound
            emit_turn(emitter, 'task0001', 'sess-0001', f'burst-{index}')
        # the slow queue is bounded and the loss is EXPLICIT (reset), never
        # a silent gap and never unbounded growth
        self.assertTrue(slow.reset_pending)
        self.assertTrue(slow.closed)
        self.assertLessEqual(slow.pending_events, 5)
        self.assertLessEqual(slow.pending_bytes, 8192 + 4096)
        # the journal itself stays healthy for a fresh subscriber and a
        # NEW cut (generation) still serves everything retained
        fresh = journal.subscribe_all(journal.new_cut()['snapshot_cursor'])
        self.assertFalse(fresh.closed)
        emit_turn(emitter, 'task0001', 'sess-0001', 'after-burst')
        frames = fresh.drain(100)
        self.assertTrue(any(frame['event']['kind'] == 'turn.started'
                            for frame in frames))


# ---------------------------------------------------------------------------
# Item 11: queue overflow (bounded event router)
# ---------------------------------------------------------------------------

class Fault11QueueOverflowTests(unittest.TestCase):
    def test_overflow_fails_the_owning_subscription_only(self):
        async def go():
            router = protocol.EventRouter(
                key_of=lambda frame: frame.get('sessionId'),
                seq_of=lambda frame: frame.get('seq'), bound=2)
            owner = router.subscribe('sess_a', 0)
            peer = router.subscribe('sess_b', 0)
            for seq in (1, 2, 3, 4, 5):        # owner never drains
                router.publish({'sessionId': 'sess_a', 'seq': seq,
                                'type': 'owner-event'})
            for seq in (1, 2):                 # peer drains fine
                router.publish({'sessionId': 'sess_b', 'seq': seq,
                                'type': 'peer-event'})
            peer_frames = peer.drain()
            return owner, peer, peer_frames

        owner, peer, peer_frames = asyncio.run(go())
        self.assertTrue(owner.overflowed)          # loud, not silent
        self.assertFalse(peer.overflowed)
        self.assertEqual([frame['seq'] for _s, frame in peer_frames],
                         [1, 2])                   # the peer lost nothing
        self.assertLessEqual(len(owner.pending), 2)  # bounded backlog
        # the owner's overflow flag is the loud signal the resident wait
        # loop turns into RouterOverflow for the OWNING task only


# ---------------------------------------------------------------------------
# Item 12: journal generation (cut handover)
# ---------------------------------------------------------------------------

class Fault12JournalGenerationTests(unittest.TestCase):
    def test_cut_handover_loses_nothing_and_refuses_forged_cursor(self):
        emitter = journal_emitter()
        journal = UiJournal(emitter, max_events=200, max_bytes=1 << 20,
                            max_event_bytes=4096, page_max_records=1,
                            page_max_bytes=2048, registry_max_cursors=4,
                            registry_ttl_s=300.0,
                            queue_max_events=64, queue_max_bytes=1 << 20)
        emit_accepted(emitter, 'task0001')
        emit_turn(emitter, 'task0001', 'sess-0001', 'turn-1')
        emit_accepted(emitter, 'task0002')          # a second stream
        cut1 = journal.new_cut()                    # generation 1
        live = journal.subscribe_all(cut1['snapshot_cursor'])
        emit_turn(emitter, 'task0001', 'sess-0001', 'turn-2')   # post-cut
        emit_accepted(emitter, 'task0003')          # a NEW stream post-cut
        cut2 = journal.new_cut()                    # generation 2 (handover)
        # a live consumer followed across the cut without a gap
        followed = [frame['event']['seq']
                    for frame in live.drain(100)]
        self.assertEqual(followed, [3, 1])          # turn-2 + new stream seq1
        # paging generation 2 covers EVERY stream born before it
        states, cursor = [], None
        while True:
            page = journal.snapshot(cut2['snapshot_cursor'],
                                    page_cursor=cursor, client='c1')
            self.assertTrue(page['ok'])
            states.extend(page['states'])
            if page['done']:
                break
            cursor = page['next_page_cursor']
        self.assertEqual({state['task_id'] for state in states},
                         {'task0001', 'task0002', 'task0003'})
        by_task = {state['task_id']: state for state in states}
        self.assertGreaterEqual(by_task['task0001']['last_seq'], 3)
        # FORGED continuation, cross-cut shape: a generation-1 page cursor
        # presented against the generation-2 cut is refused (its
        # snapshot_id does not match the cut) — no skipping streams
        cut1_page = journal.snapshot(cut1['snapshot_cursor'], client='c2')
        self.assertFalse(cut1_page['done'])         # a continuation exists
        cross_cut = journal.snapshot(
            cut2['snapshot_cursor'],
            page_cursor=cut1_page['next_page_cursor'], client='c1')
        self.assertFalse(cross_cut['ok'])
        self.assertEqual(cross_cut['error'], 'invalid_request')
        # FORGED continuation, skip shape: mutating the open page's own
        # cursor to skip a stream that was never delivered is refused
        cut2_page = journal.snapshot(cut2['snapshot_cursor'], client='c3')
        self.assertFalse(cut2_page['done'])
        forged = dict(cut2_page['next_page_cursor'])
        forged['after_stream'] = 'taskZZZ'          # never delivered
        skipped = journal.snapshot(cut2['snapshot_cursor'],
                                   page_cursor=forged, client='c3')
        self.assertFalse(skipped['ok'])
        self.assertEqual(skipped['error'], 'invalid_request')


# ---------------------------------------------------------------------------
# Cross-item invariant: zero model requests in this whole module
# ---------------------------------------------------------------------------

#: Structural scoping gate (repair1 N3): the fault matrix's
#: "model_requests=0 / all fake" claim rests on the module having NO real
#: model/network/CLI surface at all. Denied top-level import roots and
#: denied ``os`` process-spawn calls — adding any of them to this module
#: is a scoping violation the scan reports loudly.
DENIED_IMPORT_ROOTS = frozenset((
    # network stacks
    'socket', 'ssl', 'http', 'urllib', 'urllib3', 'requests', 'httpx',
    'aiohttp', 'ftplib', 'telnetlib', 'smtplib', 'xmlrpc',
    # model / inference client SDKs
    'openai', 'anthropic', 'cohere', 'mistral', 'boto3', 'googleapiclient',
    # real-process surfaces
    'subprocess',
))
DENIED_OS_CALLS = frozenset((
    'system', 'popen', 'execv', 'execve', 'execvp', 'execvpe', 'execl',
    'execle', 'execlp', 'execlpe', 'spawnv', 'spawnve', 'spawnvp',
    'spawnvpe', 'spawnl', 'spawnle', 'spawnlp', 'spawnlpe', 'fork',
    'forkpty',
))


def scan_for_model_network_surface(path):
    """AST-scan one Python source for real model/network/CLI surfaces.

    Returns a list of human-readable findings (empty = clean). Used on
    THIS module's own source by :class:`FaultMatrixScopingTests` — a real
    gate, not a tautology: importing a network/model stack or calling an
    ``os`` process-spawn entry point here turns the scoping test red.
    """
    import ast
    findings = []
    tree = ast.parse(Path(path).read_text(encoding='utf-8'))
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name.split('.')[0] in DENIED_IMPORT_ROOTS:
                    findings.append(f'import {alias.name}')
        elif isinstance(node, ast.ImportFrom):
            root = (node.module or '').split('.')[0]
            if root in DENIED_IMPORT_ROOTS:
                findings.append(f'from {node.module} import ...')
        elif (isinstance(node, ast.Call)
              and isinstance(node.func, ast.Attribute)
              and isinstance(node.func.value, ast.Name)
              and node.func.value.id == 'os'
              and node.func.attr in DENIED_OS_CALLS):
            findings.append(f'os.{node.func.attr}() call')
    return findings


class FaultMatrixScopingTests(unittest.TestCase):
    def test_module_has_no_real_model_network_or_cli_surface(self):
        """repair1 N3: the constant-true ``hasattr`` self-check is
        replaced by a structural AST scan of this module's own source —
        the honest basis of the matrix's model_requests=0 scoping claim."""
        findings = scan_for_model_network_surface(Path(__file__))
        self.assertEqual(
            findings, [],
            'the fault matrix must stay free of real model/network/CLI '
            f'surfaces; found: {findings}')


if __name__ == '__main__':
    unittest.main()
