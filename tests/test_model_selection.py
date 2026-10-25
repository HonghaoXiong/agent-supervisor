"""Explicit model selection is verified before dispatch and from provider receipts."""
import copy
import io
import json
from contextlib import redirect_stdout
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import patch

from zcode_supervisor import protocol


MODEL = {'providerId': 'fixture-provider', 'modelId': 'fixture-model'}


def snapshot(model=MODEL):
    return {'session': {'sessionId': 'sess_fixture', 'status': 'idle'},
            'runtime': {'pendingRequestIds': []},
            'settings': {'model': {'current': copy.deepcopy(model)}}}


def receipt(kind, model=MODEL, **changes):
    value = {'sessionId': 'sess_fixture', 'turnId': 'turn_fixture',
             'type': 'session.updated', 'seq': 2,
             'payload': {'type': kind, **model, 'sessionId': 'sess_fixture',
                         'turnId': 'turn_fixture', 'requestId': 'request_fixture',
                         'modelCall': {'actorKind': 'main', 'operation': 'agent_step'}}}
    value.update(changes)
    return value


class ModelValidationTests(unittest.TestCase):
    def test_model_requires_both_identifiers_and_rejects_unsupported_options(self):
        base = {'workspace': '/work/worker', 'prompt': 'Do work', 'allowed_files': []}
        protocol.validate_packet({**base, 'model': MODEL})
        invalid = [None, 'model', [], {}, {'modelId': 'only'},
                   {**MODEL, 'options': {}}, {**MODEL, 'providerId': ' '},
                   {**MODEL, 'modelId': ' model'}, {**MODEL, 'modelId': 'm\x00x'},
                   {**MODEL, 'modelId': True}]
        for model in invalid:
            with self.subTest(model=model), self.assertRaises(ValueError):
                protocol.validate_packet({**base, 'model': model})

    def test_model_snapshot_rejects_missing_or_changed_identifiers(self):
        self.assertEqual(protocol.check_model_snapshot(snapshot(), MODEL), MODEL)
        for value in ({}, {'settings': None}, snapshot(None),
                      snapshot({**MODEL, 'modelId': 'other'}),
                      snapshot({**MODEL, 'providerId': 'other'})):
            with self.subTest(value=value), self.assertRaises(ValueError):
                protocol.check_model_snapshot(value, MODEL)

    def test_only_reasoning_level_option_is_supported(self):
        base = {'workspace': '/work/worker', 'prompt': 'Do work', 'allowed_files': []}
        selected = {**MODEL, 'options': {'reasoningLevel': 'max'}}
        protocol.validate_packet({**base, 'model': selected})
        for options in (None, [], {}, {'effort': 'max'},
                        {'reasoningLevel': 'max', 'temperature': 0},
                        {'reasoningLevel': True}, {'reasoningLevel': ''},
                        {'reasoningLevel': ' max'}, {'reasoningLevel': 'm\x00x'}):
            with self.subTest(options=options), self.assertRaises(ValueError):
                protocol.validate_packet({**base, 'model': {**MODEL, 'options': options}})

    def test_snapshot_reasoning_selection_must_match_when_explicit(self):
        selected = {**MODEL, 'options': {'reasoningLevel': 'max'}}
        self.assertEqual(protocol.check_model_snapshot(snapshot(selected), selected), selected)
        for current in (MODEL, {**MODEL, 'options': {}},
                        {**MODEL, 'options': {'reasoningLevel': 'high'}},
                        {**selected, 'options': {'reasoningLevel': 'max', 'extra': True}}):
            with self.subTest(current=current), self.assertRaises(ValueError):
                protocol.check_model_snapshot(snapshot(current), selected)

    def test_provider_receipts_do_not_invent_effective_reasoning(self):
        selected = {**MODEL, 'options': {'reasoningLevel': 'max'}}
        events = [receipt('model_request_'+kind) for kind in ('started', 'completed')]
        for event in events:
            event['payload']['modelCall']['reasoning'] = {
                'requestedLevel': 'max', 'effectiveLevel': 'unknown'}
        result = protocol.model_execution_evidence(events, 'sess_fixture', 'turn_fixture', selected)
        self.assertEqual(result[0]['reasoning'], {
            'requestedLevel': 'max', 'effectiveLevel': 'unknown'})

    def test_observed_model_diagnostics_keep_only_safe_selection_fields(self):
        value = {**MODEL, 'apiKey': 'secret', 'options': {
            'reasoningLevel': 'max', 'authorization': 'secret'}}
        self.assertEqual(protocol.observed_model(snapshot(value)), {
            **MODEL, 'options': {'reasoningLevel': 'max'}})
        self.assertIsNone(protocol.observed_model({}))

    def test_provider_evidence_requires_actual_start_and_completion(self):
        start, end = (receipt('model_request_'+kind) for kind in ('started', 'completed'))
        valid = protocol.model_execution_evidence([start, end], 'sess_fixture', 'turn_fixture', MODEL)
        self.assertEqual(len(valid), 2)
        for events in ([], [start], [end],
                       [receipt('model_request_started', turnId='foreign'), end],
                       [receipt('model_request_started', sessionId='foreign'), end]):
            with self.subTest(events=events), self.assertRaises(ValueError):
                protocol.model_execution_evidence(events, 'sess_fixture', 'turn_fixture', MODEL)

    def test_provider_switch_or_identity_mismatch_is_rejected(self):
        start, end = (receipt('model_request_'+kind) for kind in ('started', 'completed'))
        for field in ('providerId', 'modelId', 'sessionId', 'turnId'):
            bad = copy.deepcopy(end)
            bad['payload'][field] = 'other'
            with self.subTest(field=field), self.assertRaises(ValueError):
                protocol.model_execution_evidence([start, bad], 'sess_fixture', 'turn_fixture', MODEL)

    def test_foreign_turn_and_subagent_requests_do_not_satisfy_main_evidence(self):
        events = [receipt('model_request_'+kind) for kind in ('started', 'completed')]
        for event in events:
            event['payload']['modelCall']['actorKind'] = 'subagent'
        with self.assertRaises(ValueError):
            protocol.model_execution_evidence(events, 'sess_fixture', 'turn_fixture', MODEL)


class ModelDispatchTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.workspace = self.root / 'workspace'
        self.workspace.mkdir()
        self.packet = self.root / 'packet.json'
        self.output = self.root / 'run.json'
        self.packet.write_text(json.dumps({'workspace': str(self.workspace),
            'prompt': 'Fixture work', 'allowed_files': [], 'model': MODEL}))
        self.config = SimpleNamespace(work_root=self.root, state_dir=self.root / 'state',
                                      cache_dir=self.root / 'cache')
        self.args = SimpleNamespace(packet=str(self.packet), out=str(self.output),
                                    resume=None, timeout=5)
        self.calls = []
        self.initial_model = MODEL
        self.model_after_set = None
        self.final_model = MODEL
        self.provider_model = MODEL
        self.include_provider_receipts = True
        owner = self

        class FakeClient:
            def __init__(self, *args):
                self.events = []
                self.failure = None
                self.on_event = None
                self.process = SimpleNamespace(pid=987654, returncode=None)
                self.sent = False
                self.selected = owner.initial_model

            async def start(self):
                pass

            async def close(self):
                self.process.returncode = 0

            async def request(self, method, params):
                owner.calls.append((method, copy.deepcopy(params)))
                if method in ('session/create', 'session/resume', 'session/read'):
                    return snapshot(owner.final_model if self.sent else self.selected)
                if method == 'session/setModel' and owner.model_after_set is not None:
                    self.selected = owner.model_after_set
                if method == 'session/subscribe':
                    return {'eventSeq': 0}
                if method == 'session/send':
                    self.sent = True
                    started = {'type': 'turn.started', 'sessionId': 'sess_fixture',
                        'turnId': 'turn_fixture', 'seq': 1, 'payload': {'inputId': params['inputId']}}
                    self.on_event(started)
                    self.events.append(started)
                    if owner.include_provider_receipts:
                        self.events.extend(receipt('model_request_'+kind, owner.provider_model)
                                           for kind in ('started', 'completed'))
                    self.events.append({'type': 'turn.completed', 'sessionId': 'sess_fixture',
                        'turnId': 'turn_fixture', 'seq': 3, 'payload': {'resultType': 'success'}})
                return {}

        self.fake_client = FakeClient

    async def run_fixture(self):
        with patch.object(protocol, 'ProtocolClient', self.fake_client), redirect_stdout(io.StringIO()):
            await protocol.execute(self.args, self.config)
        return json.loads(self.output.read_text())

    async def test_create_passes_explicit_model_and_preserves_verified_receipts(self):
        result = await self.run_fixture()
        self.assertTrue(result['delivery_ok'])
        self.assertEqual(dict(self.calls)['session/create']['model'], MODEL)
        self.assertEqual(result['effective_model_before'], MODEL)
        self.assertEqual(result['effective_model_after'], MODEL)
        self.assertEqual(len(result['model_execution']), 2)

    async def test_create_passes_reasoning_option_and_checks_it_in_snapshots(self):
        selected = {**MODEL, 'options': {'reasoningLevel': 'max'}}
        packet = json.loads(self.packet.read_text())
        packet['model'] = selected
        self.packet.write_text(json.dumps(packet))
        self.initial_model = self.final_model = selected
        result = await self.run_fixture()
        self.assertTrue(result['delivery_ok'])
        self.assertEqual(dict(self.calls)['session/create']['model'], selected)
        self.assertEqual(dict(self.calls)['session/setModel']['model'], selected)
        self.assertEqual(result['effective_model_before'], selected)
        self.assertEqual(result['effective_model_after'], selected)

    async def test_create_loses_options_but_explicit_object_setter_restores_them(self):
        selected = {**MODEL, 'options': {'reasoningLevel': 'max'}}
        packet = json.loads(self.packet.read_text())
        packet['model'] = selected
        self.packet.write_text(json.dumps(packet))
        self.initial_model = MODEL  # Reproduces the observed CLI 0.16.9 create result.
        self.model_after_set = self.final_model = selected
        result = await self.run_fixture()
        self.assertTrue(result['delivery_ok'])
        self.assertEqual(result['observed_model_initial'], MODEL)
        self.assertEqual(result['observed_model_before'], selected)

    async def test_mismatch_before_send_fails_without_a_model_call(self):
        self.initial_model = {**MODEL, 'modelId': 'wrong'}
        result = await self.run_fixture()
        self.assertFalse(result['delivery_ok'])
        self.assertNotIn('session/send', dict(self.calls))
        self.assertEqual(result['observed_model_before'], self.initial_model)

    async def test_changed_final_model_fails_delivery(self):
        self.final_model = {**MODEL, 'modelId': 'wrong'}
        self.assertFalse((await self.run_fixture())['delivery_ok'])

    async def test_wrong_provider_model_fails_even_when_snapshots_match(self):
        self.provider_model = {**MODEL, 'modelId': 'wrong'}
        self.assertFalse((await self.run_fixture())['delivery_ok'])

    async def test_absent_provider_receipts_fail_delivery(self):
        self.include_provider_receipts = False
        self.assertFalse((await self.run_fixture())['delivery_ok'])

    async def test_resume_sets_model_before_dispatch_without_workspace_default(self):
        self.args.resume = 'sess_fixture'
        self.config.state_dir.mkdir()
        (self.config.state_dir / 'sess_fixture.json').write_text(json.dumps({
            'workspace': str(self.workspace), 'state': 'exited', 'pid': 987655}))
        with patch.object(protocol, 'process_alive', return_value=False):
            result = await self.run_fixture()
        self.assertTrue(result['delivery_ok'])
        methods = [method for method, _ in self.calls]
        self.assertLess(methods.index('session/setModel'), methods.index('session/send'))
        self.assertEqual(dict(self.calls)['session/setModel'], {
            'sessionId': 'sess_fixture', 'model': MODEL, 'persistAsWorkspaceLastUsed': False})
        self.assertNotIn('model', dict(self.calls)['session/resume'])

    async def test_unspecified_model_preserves_backward_compatibility(self):
        packet = json.loads(self.packet.read_text())
        del packet['model']
        self.packet.write_text(json.dumps(packet))
        self.initial_model = self.final_model = None
        self.include_provider_receipts = False
        result = await self.run_fixture()
        self.assertTrue(result['delivery_ok'])
        self.assertNotIn('model', dict(self.calls)['session/create'])
        self.assertNotIn('requested_model', result)


if __name__ == '__main__':
    unittest.main()
