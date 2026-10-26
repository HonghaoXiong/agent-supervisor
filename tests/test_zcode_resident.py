"""P1-2 resident app-server contracts: one process, session-isolated packets.

No live CLI, node process, network, or model call: one shared FakeClient
instance doubles the resident app-server; scripted per-session event streams
are interleaved on purpose to prove sessionId filtering.
"""
import asyncio
import copy
import io
import json
from contextlib import redirect_stdout
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest

from zcode_supervisor import zcode_resident as resident_module
from zcode_supervisor.authority import Authority, inventory
from zcode_supervisor.zcode_resident import (ResidentError, ResidentZCode,
                                             SessionRegistry, main)

MODEL = {'providerId': 'fixture-provider', 'modelId': 'fixture-model',
         'options': {'reasoningLevel': 'max'}}


class FakeServer:
    """Scripted app-server behaviour shared by every session of one client."""

    def __init__(self):
        self.calls = []
        self.starts = 0
        self.created = []
        self.live = set()
        self.max_live = 0
        self.stops = []
        self.models = {}
        self.initial_model = None
        self.receipt_model = {'providerId': MODEL['providerId'], 'modelId': MODEL['modelId']}
        self.commands = {}          # sessionId -> exact reviewed command
        self.foreign = {}           # sessionId -> sessionId for the cross-session request
        self.fail_turn = set()
        self.never_complete = set()
        self.include_receipts = True
        self.script_tasks = []
        self.seq = 0
        self.turn_counts = {}

    def new_session(self):
        sid = f'sess_{chr(ord("A") + len(self.created))}'
        self.created.append(sid)
        self.live.add(sid)
        self.max_live = max(self.max_live, len(self.live))
        self.models.setdefault(sid, copy.deepcopy(self.initial_model))
        return sid

    def snapshot(self, sid):
        return {'session': {'sessionId': sid, 'status': 'idle'},
                'runtime': {'pendingRequestIds': []},
                'settings': {'model': {'current': copy.deepcopy(self.models.get(sid))}}}

    def emit(self, client, sid, turn, type_, payload):
        self.seq += 1
        client.events.append({'type': type_, 'sessionId': sid, 'turnId': turn,
                              'seq': self.seq, 'payload': payload})

    async def script(self, client, sid, input_id):
        """One dispatched turn: started, optional receipts/tool/permission, completed."""
        self.turn_counts[sid] = self.turn_counts.get(sid, 0) + 1
        turn = f'turn_{sid}_{self.turn_counts[sid]}'
        self.emit(client, sid, turn, 'turn.started', {'inputId': input_id})
        if sid in self.fail_turn:
            self.emit(client, sid, turn, 'turn.failed', {'error': 'fixture failure'})
            return
        if sid in self.never_complete:
            return
        if self.include_receipts:
            for kind in ('model_request_started', 'model_request_completed'):
                self.emit(client, sid, turn, 'session.updated', {
                    'type': kind, **self.receipt_model, 'sessionId': sid, 'turnId': turn,
                    'requestId': f'request_{sid}',
                    'modelCall': {'actorKind': 'main', 'operation': 'agent_step'}})
        command = self.commands.get(sid)
        if command is not None:
            call = f'call_{turn}'
            self.emit(client, sid, turn, 'model.streaming',
                      {'kind': 'tool_call', 'toolName': 'Bash', 'toolCallId': call,
                       'input': {'command': command, 'timeout': 1000}})
            self.emit(client, sid, turn, 'tool.updated',
                      {'kind': 'scheduled', 'toolName': 'Bash', 'toolCallId': call,
                       'input': {'command': command, 'timeout': 1000}})
            # The wait loop must have consumed turn.started before permission
            # callbacks fire (as on the real wire, where callbacks are separate
            # frames that arrive after the turn has started).
            await asyncio.sleep(0.03)
            client.permission({'sessionId': sid, 'turnId': turn, 'requestId': f'req_{turn}',
                               'toolCallId': call, 'toolName': 'Bash',
                               'input': {'command': command, 'timeout': 1000}})
            foreign = self.foreign.get(sid)
            if foreign:
                client.permission({'sessionId': foreign, 'turnId': f'turn_{foreign}_x',
                                   'requestId': f'req_foreign_{sid}',
                                   'toolCallId': f'call_foreign_{sid}', 'toolName': 'Bash',
                                   'input': {'command': 'echo foreign', 'timeout': 1000}})
            await asyncio.sleep(0.02)
            self.emit(client, sid, turn, 'tool.updated', {
                'kind': 'result', 'toolCallId': call,
                'result': {'success': True, 'truncated': False,
                           'content': json.dumps({'stdout': 'ok', 'stderr': '',
                                                  'status': 'completed', 'exitCode': 0,
                                                  'interrupted': False}),
                           'perf': {'detail': {'kind': 'command', 'command': {
                               'exitCode': 0, 'timedOut': False,
                               'status': 'completed', 'runMs': 5}}}}})
        else:
            await asyncio.sleep(0.03)
        self.emit(client, sid, turn, 'turn.completed', {'resultType': 'success'})


class FakeClient:
    """One long-lived client instance serving many sessions (resident double)."""

    def __init__(self, owner, workspace, output, permission, config=None):
        self.owner = owner
        self.workspace = workspace
        self.output = output
        self.permission = permission
        self.config = config
        self.events = []
        self.on_event = None
        self.failure = None
        self.process = SimpleNamespace(pid=4242, returncode=None)

    async def start(self):
        self.owner.starts += 1

    async def close(self):
        self.process.returncode = 0

    async def request(self, method, params):
        owner = self.owner
        owner.calls.append((method, copy.deepcopy(params)))
        if method == 'session/create':
            return owner.snapshot(owner.new_session())
        if method == 'session/resume':
            sid = params['sessionId']
            owner.models.setdefault(sid, copy.deepcopy(owner.initial_model))
            return owner.snapshot(sid)
        if method == 'session/setModel':
            owner.models[params['sessionId']] = copy.deepcopy(params['model'])
            return {}
        if method == 'session/read':
            if params.get('messageLimit') == 10:
                owner.live.discard(params['sessionId'])
            return owner.snapshot(params['sessionId'])
        if method == 'session/subscribe':
            return {'eventSeq': 0}
        if method == 'session/send':
            owner.script_tasks.append(asyncio.create_task(
                owner.script(self, params['sessionId'], params['inputId'])))
            return {}
        if method == 'session/stop':
            owner.stops.append(params['sessionId'])
            return {}
        return {}


class ResidentFixture(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='czs resident ')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        for name in ('work', 'state', 'cache', 'packets'):
            (self.root / name).mkdir()
        self.config = SimpleNamespace(work_root=self.root / 'work',
                                      state_dir=self.root / 'state',
                                      cache_dir=self.root / 'cache', notify=False)
        self.server = FakeServer()
        self.instances = []

        def factory(workspace, output, permission, config):
            client = FakeClient(self.server, workspace, output, permission, config)
            self.instances.append(client)
            return client

        self.resident = ResidentZCode(self.config, self.root / 'work',
                                      client_factory=factory)
        self.registry = self.resident.registry

    async def asyncTearDown(self):
        if self.server.script_tasks:
            await asyncio.gather(*self.server.script_tasks, return_exceptions=True)
        await self.resident.stop()

    def workspace(self, name):
        ws = self.root / 'work' / name
        ws.mkdir()
        return ws

    def packet(self, name, ws, *, command=None, model=None, prompt='Fixture work'):
        body = {'workspace': str(ws), 'prompt': prompt, 'allowed_files': []}
        if command is not None:
            body['commands'] = [{'id': f'cmd_{name}', 'command': command,
                                 'max_uses': 1, 'max_timeout_ms': 5000}]
        if model is not None:
            body['model'] = model
        path = self.root / 'packets' / f'{name}.json'
        path.write_text(json.dumps(body))
        return path

    def registry_records(self):
        return json.loads((self.root / 'state' / 'hub' / 'resident-sessions.json')
                          .read_text())


class SequentialRunTests(ResidentFixture):
    async def test_two_packets_share_one_process_with_full_run_fields(self):
        ws_a, ws_b = self.workspace('wsA'), self.workspace('wsB')
        self.server.commands = {'sess_A': '/bin/echo A', 'sess_B': '/bin/echo B'}
        self.server.include_receipts = False
        packet_a = self.packet('p1', ws_a, command='/bin/echo A')
        packet_b = self.packet('p2', ws_b, command='/bin/echo B')
        await self.resident.start()
        self.assertEqual(len(self.instances), 1)
        self.assertEqual(self.instances[0].workspace, self.root / 'work')
        out_a, out_b = self.root / 'work' / 'runA.json', self.root / 'work' / 'runB.json'
        first = await self.resident.run_packet(packet_a, out_a, timeout_s=10)
        second = await self.resident.run_packet(packet_b, out_b, timeout_s=10)
        # Still exactly one client instance and one process start for two tasks.
        self.assertEqual(len(self.instances), 1)
        self.assertEqual(self.server.starts, 1)
        self.assertTrue(first['delivery_ok'])
        self.assertTrue(second['delivery_ok'])
        self.assertEqual(first['session_id'], 'sess_A')
        self.assertEqual(second['session_id'], 'sess_B')
        for key in ('delivery_ok', 'workspace', 'input_id', 'generation',
                    'scientific_acceptance', 'session_id', 'start_event_seq', 'turn_id',
                    'completion', 'commands', 'execution_problems', 'outcome',
                    'permission_decisions', 'process_exit_code', 'scope_enforcement',
                    'changed_files'):
            self.assertIn(key, first)
        self.assertEqual([entry['command_id'] for entry in first['commands']], ['cmd_p1'])
        self.assertEqual(first['execution_problems'], [])
        self.assertIsNone(first['process_exit_code'])  # resident stays alive
        # The session declares the TASK workspace although the process cwd is work_root.
        create = next(params for method, params in self.server.calls
                      if method == 'session/create')
        self.assertEqual(create['workspace']['workspacePath'], str(ws_a))
        self.assertEqual(create['toolAllowlist'], ['Read', 'Write', 'Edit', 'Bash'])
        self.assertEqual(create['mode'], 'build')
        # run.json on disk equals the returned result; before-inventory retained.
        self.assertEqual(json.loads(out_a.read_text()), first)
        self.assertTrue(out_a.with_suffix('.before.json').is_file())
        records = self.registry_records()
        self.assertEqual(records['sess_A']['state'], 'exited')
        self.assertEqual(records['sess_B']['state'], 'exited')
        self.assertTrue(self.registry.can_resume('sess_A', str(ws_a)))
        self.assertEqual(len(self.resident.resident_latency_hook()['setup_ms']), 2)

    async def test_model_packet_sets_and_verifies_model_on_shared_process(self):
        ws = self.workspace('wsA')
        self.server.initial_model = copy.deepcopy(MODEL)
        self.server.commands = {'sess_A': '/bin/echo A'}
        packet = self.packet('p1', ws, command='/bin/echo A', model=copy.deepcopy(MODEL))
        await self.resident.start()
        result = await self.resident.run_packet(packet, self.root / 'work' / 'runA.json',
                                                timeout_s=10)
        self.assertTrue(result['delivery_ok'])
        self.assertEqual(result['requested_model'], MODEL)
        self.assertEqual(result['observed_model_initial'], MODEL)
        self.assertEqual(result['effective_model_before'], MODEL)
        self.assertEqual(result['effective_model_after'], MODEL)
        self.assertEqual(len(result['model_execution']), 2)
        set_model = next(params for method, params in self.server.calls
                         if method == 'session/setModel')
        self.assertEqual(set_model, {'sessionId': 'sess_A', 'model': MODEL,
                                     'persistAsWorkspaceLastUsed': False})


class ConcurrentRunTests(ResidentFixture):
    async def test_different_workspaces_run_concurrently_with_isolated_sessions(self):
        ws_a, ws_b = self.workspace('wsA'), self.workspace('wsB')
        self.server.commands = {'sess_A': '/bin/echo A', 'sess_B': '/bin/echo B'}
        # A's script files a request under B's sessionId (must be decided — denied —
        # by B's authority, never by A's); B's script names an unknown session.
        self.server.foreign = {'sess_A': 'sess_B', 'sess_B': 'sess_unknown'}
        packet_a = self.packet('p1', ws_a, command='/bin/echo A')
        packet_b = self.packet('p2', ws_b, command='/bin/echo B')
        await self.resident.start()
        results = await asyncio.wait_for(asyncio.gather(
            self.resident.run_packet(packet_a, self.root / 'work' / 'runA.json', timeout_s=15),
            self.resident.run_packet(packet_b, self.root / 'work' / 'runB.json', timeout_s=15)), 20)
        first, second = results
        self.assertTrue(first['delivery_ok'])
        self.assertTrue(second['delivery_ok'])
        self.assertEqual((first['session_id'], second['session_id']), ('sess_A', 'sess_B'))
        self.assertNotEqual(first['session_id'], second['session_id'])
        self.assertEqual((first['turn_id'], second['turn_id']),
                         ('turn_sess_A_1', 'turn_sess_B_1'))
        self.assertEqual(self.server.max_live, 2)  # sessions genuinely overlapped
        # Each run's evidence only contains its own command.
        self.assertEqual([entry['command_id'] for entry in first['commands']], ['cmd_p1'])
        self.assertEqual([entry['command_id'] for entry in second['commands']], ['cmd_p2'])
        # A decided only its own request; the cross-session request landed with B
        # (denied, command outside B's contract) — never accepted for A.
        self.assertEqual([entry['decision'] for entry in first['permission_decisions']],
                         ['allow'])
        self.assertEqual(sorted(entry['decision'] for entry
                                in second['permission_decisions']), ['allow', 'deny'])
        self.assertTrue(any(entry['decision'] == 'deny'
                            and entry['reason'] in ('Wrong or missing turn identity',
                                                    'Not in this task contract')
                            for entry in second['permission_decisions']))
        for run in (first, second):
            self.assertTrue(all(entry['session_id'] == run['session_id']
                                for entry in run['permission_decisions']))
        # The unknown-session request was denied at the router and alerted.
        self.assertEqual(len(self.resident.alerts), 1)
        self.assertEqual(self.resident.alerts[0]['session_id'], 'sess_unknown')
        records = self.registry_records()
        self.assertEqual({records['sess_A']['state'], records['sess_B']['state']},
                         {'exited'})

    async def test_same_workspace_packets_are_serialized(self):
        ws = self.workspace('wsA')
        self.server.commands = {'sess_A': '/bin/echo A', 'sess_B': '/bin/echo B'}
        packet_a = self.packet('p1', ws, command='/bin/echo A')
        packet_b = self.packet('p2', ws, command='/bin/echo B')
        await self.resident.start()
        first, second = await asyncio.wait_for(asyncio.gather(
            self.resident.run_packet(packet_a, self.root / 'work' / 'runA.json', timeout_s=15),
            self.resident.run_packet(packet_b, self.root / 'work' / 'runB.json', timeout_s=15)), 20)
        self.assertTrue(first['delivery_ok'])
        self.assertTrue(second['delivery_ok'])
        # The shared frozen workspace never hosted two sessions at once.
        self.assertEqual(self.server.max_live, 1)
        self.assertEqual(self.server.created, ['sess_A', 'sess_B'])


class PermissionRoutingTests(ResidentFixture):
    def test_routing_decides_only_for_the_owning_session(self):
        ws = self.workspace('wsA')
        command = '/bin/echo A'
        path = self.packet('p1', ws, command=command)
        packet = json.loads(path.read_text())
        authority = Authority(packet, path, inventory(ws))
        authority.session_id = 'sess_A'
        authority.turn_id = 'turn_A'
        self.resident._authorities['sess_A'] = authority

        def request(session_id, request_id, call_id, **changes):
            value = {'sessionId': session_id, 'turnId': 'turn_A',
                     'requestId': request_id, 'toolCallId': call_id,
                     'toolName': 'Bash', 'input': {'command': command, 'timeout': 1000}}
            value.update(changes)
            return value

        allowed = self.resident._route_permission(request('sess_A', 'r1', 'c1'))
        self.assertEqual(allowed['decision'], 'allow')
        # A request naming B's session is not A's to answer: the router denies
        # it (unknown here) and raises an alert instead of consulting A.
        denied = self.resident._route_permission(request('sess_B', 'r2', 'c2'))
        self.assertEqual(denied, {'decision': 'deny', 'reason': 'unknown session on resident'})
        self.assertEqual(len(self.resident.alerts), 1)
        self.assertEqual(self.resident.alerts[0]['session_id'], 'sess_B')
        self.assertEqual(len(authority.decisions), 1)
        # Double insurance: even handed to A directly, decide() itself rejects
        # the foreign session identity.
        direct = authority.decide(request('sess_B', 'r3', 'c3'))
        self.assertEqual(direct['decision'], 'deny')
        self.assertEqual(direct['reason'], 'Wrong session')


class RegistryTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='czs registry ')
        self.addCleanup(self.temp.cleanup)
        self.registry = SessionRegistry(Path(self.temp.name) / 'state')

    def test_claim_release_and_resume_rules(self):
        self.registry.claim('sess_A', 'task-1', '/work/ws', 4242)
        records = self.registry._load()
        self.assertEqual(records['sess_A']['state'], 'running')
        self.assertEqual(records['sess_A']['task_id'], 'task-1')
        self.assertEqual(records['sess_A']['pid'], 4242)
        self.assertFalse(self.registry.can_resume('sess_A', '/work/ws'))
        self.assertFalse(self.registry.can_resume('sess_A', '/work/other'))
        with self.assertRaises(ResidentError):
            self.registry.claim('sess_A', 'task-2', '/work/ws', 4242)  # no concurrent claim
        self.registry.release('sess_A', 7)
        self.assertTrue(self.registry.can_resume('sess_A', '/work/ws'))
        self.assertFalse(self.registry.can_resume('sess_A', '/work/other'))
        self.assertEqual(self.registry._load()['sess_A']['last_seq'], 7)
        self.registry.mark_lost('sess_A', 'fixture loss')
        self.assertFalse(self.registry.can_resume('sess_A', '/work/ws'))
        self.assertEqual(self.registry._load()['sess_A']['reason'], 'fixture loss')
        # A lost session can be re-claimed but starts from a clean record.
        self.registry.claim('sess_A', 'task-3', '/work/ws', 4243)
        self.assertEqual(self.registry._load()['sess_A']['state'], 'running')
        with self.assertRaises(ResidentError):
            self.registry.release('sess_unknown', 0)

    def test_registry_file_is_atomically_written_under_state_hub(self):
        self.registry.claim('sess_A', 'task', '/work/ws', 1)
        path = Path(self.temp.name) / 'state' / 'hub' / 'resident-sessions.json'
        self.assertTrue(path.is_file())
        self.assertEqual(json.loads(path.read_text())['sess_A']['workspace'], '/work/ws')


class ResumeTests(ResidentFixture):
    async def test_resume_reuses_exited_session_of_same_workspace(self):
        ws = self.workspace('wsA')
        self.server.commands = {'sess_A': '/bin/echo A'}
        packet = self.packet('p1', ws, command='/bin/echo A')
        await self.resident.start()
        first = await self.resident.run_packet(packet, self.root / 'work' / 'runA.json',
                                               timeout_s=10)
        self.assertTrue(first['delivery_ok'])
        second = await self.resident.run_packet(packet, self.root / 'work' / 'runB.json',
                                                timeout_s=10, resume='sess_A')
        self.assertTrue(second['delivery_ok'])
        self.assertEqual(second['session_id'], 'sess_A')
        self.assertEqual(self.server.created, ['sess_A'])  # no second session created
        resume = next(params for method, params in self.server.calls
                      if method == 'session/resume')
        self.assertEqual(resume['sessionId'], 'sess_A')
        self.assertEqual(resume['workspace']['workspacePath'], str(ws))
        self.assertEqual(resume['toolAllowlist'], ['Read', 'Write', 'Edit', 'Bash'])
        self.assertEqual(self.registry_records()['sess_A']['state'], 'exited')

    async def test_resume_rejects_running_session_without_writing_evidence(self):
        ws = self.workspace('wsA')
        packet = self.packet('p1', ws)
        await self.resident.start()
        self.registry.claim('sess_busy', 'other-task', str(ws), 4242)
        out = self.root / 'work' / 'runA.json'
        with self.assertRaises(ResidentError):
            await self.resident.run_packet(packet, out, timeout_s=10, resume='sess_busy')
        self.assertFalse(out.exists())
        self.assertEqual(self.registry_records()['sess_busy']['state'], 'running')


class FailureModeTests(ResidentFixture):
    async def test_timeout_stops_only_that_session_and_keeps_process_usable(self):
        ws = self.workspace('wsA')
        self.server.never_complete = {'sess_A'}
        self.server.commands = {'sess_A': '/bin/echo A', 'sess_B': '/bin/echo B'}
        packet_a = self.packet('p1', ws)
        packet_b = self.packet('p2', self.workspace('wsB'), command='/bin/echo B')
        await self.resident.start()
        result = await self.resident.run_packet(packet_a, self.root / 'work' / 'runA.json',
                                                timeout_s=0.3)
        self.assertFalse(result['delivery_ok'])
        self.assertEqual(result['outcome'], 'timeout')
        self.assertEqual(self.server.stops, ['sess_A'])
        self.assertEqual(self.registry_records()['sess_A']['state'], 'lost')
        self.assertFalse(self.registry.can_resume('sess_A', str(ws)))
        # The shared process keeps serving other sessions afterwards.
        followup = await self.resident.run_packet(packet_b, self.root / 'work' / 'runB.json',
                                                  timeout_s=10)
        self.assertTrue(followup['delivery_ok'])
        self.assertEqual(followup['session_id'], 'sess_B')
        self.assertEqual(self.server.stops, ['sess_A'])

    async def test_turn_failed_is_worker_failed_and_marks_session_lost(self):
        ws = self.workspace('wsA')
        self.server.fail_turn = {'sess_A'}
        packet = self.packet('p1', ws)
        await self.resident.start()
        result = await self.resident.run_packet(packet, self.root / 'work' / 'runA.json',
                                                timeout_s=10)
        self.assertFalse(result['delivery_ok'])
        self.assertEqual(result['outcome'], 'worker-failed')
        self.assertIn('fixture failure', result['error'])
        self.assertEqual(self.server.stops, ['sess_A'])
        self.assertEqual(self.registry_records()['sess_A']['state'], 'lost')


class CliTests(unittest.TestCase):
    def test_rejects_out_of_range_timeout_before_loading_config(self):
        buffer = io.StringIO()
        with redirect_stdout(buffer):
            code = main(['run', '--config', '/nonexistent.json',
                         '--out-root', '/nonexistent/root',
                         '--packet', '/nonexistent/packet.json', '--timeout', '0'])
        self.assertEqual(code, 1)
        self.assertIn('error', json.loads(buffer.getvalue()))

    def test_module_exposes_resident_symbols_without_touching_protocol_state(self):
        self.assertTrue(issubclass(ResidentError, Exception))
        self.assertIs(resident_module.main, main)
        self.assertIs(resident_module.SessionRegistry, SessionRegistry)


if __name__ == '__main__':
    unittest.main()
