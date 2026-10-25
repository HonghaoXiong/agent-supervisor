"""execute() must bind notifications correctly through the real notify.send path.

The fake client mirrors the minimum ProtocolClient surface; notifications are
captured through a runner injected behind the genuine notify.send, so a
positional-argument mistake in protocol.py fails these tests instead of a
user's screen. No osascript call and no model request is made.
"""

import copy
import io
import json
from contextlib import redirect_stdout
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import patch

from zcode_supervisor import notify, protocol


class NotificationSpy:
    """Records protocol's exact call shape and the desktop posts that escape."""

    def __init__(self, order):
        self.order = order
        self.calls = []
        self.desktop = []

    def hook(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        self.order.append(('notify_binding', args, kwargs))
        return notify.send(*args, **kwargs, runner=self, platform='darwin')

    def __call__(self, argv, **kwargs):
        self.order.append(('desktop_post', argv))
        self.desktop.append(argv)


class NotificationLifecycleTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.workspace = self.root / 'workspace'
        self.workspace.mkdir()
        self.packet_path = self.root / 'packet.json'
        self.output = self.root / 'run-42.json'
        self.notify_enabled = False
        self.completion = 'success'
        self.complete = True
        self.model = None
        self.tamper_on_send = False
        self.order = []
        self.spy = NotificationSpy(self.order)
        owner = self

        class FakeClient:
            def __init__(self, *args):
                self.events = []
                self.failure = None
                self.on_event = None
                self.process = SimpleNamespace(pid=987654, returncode=None)

            async def start(self):
                pass

            async def close(self):
                self.process.returncode = 0

            async def request(self, method, params):
                owner.order.append(('rpc', method))
                if method in ('session/create', 'session/resume', 'session/read'):
                    snapshot = {'session': {'sessionId': 'sess_fixture', 'status': 'idle'},
                                'runtime': {'pendingRequestIds': []}, 'settings': {}}
                    if owner.model is not None:
                        snapshot['settings'] = {'model': {'current': dict(owner.model)}}
                    return snapshot
                if method == 'session/subscribe':
                    return {'eventSeq': 0}
                if method == 'session/send':
                    if owner.tamper_on_send:
                        (owner.workspace / 'rogue.txt').write_text('frozen drift', encoding='utf-8')
                    started = {'type': 'turn.started', 'sessionId': 'sess_fixture',
                               'turnId': 'turn_fixture', 'seq': 1,
                               'payload': {'inputId': params['inputId']}}
                    self.on_event(started)
                    self.events.append(started)
                    if owner.complete:
                        self.events.append({'type': 'turn.completed',
                            'sessionId': 'sess_fixture', 'turnId': 'turn_fixture',
                            'seq': 3, 'payload': {'resultType': owner.completion}})
                return {}

        self.fake_client = FakeClient
        self.write_packet({'workspace': str(self.workspace), 'prompt': 'SECRET-PROMPT-MARKER',
                           'allowed_files': []})
        self.config = SimpleNamespace(work_root=self.root, state_dir=self.root / 'state',
                                      cache_dir=self.root / 'cache', notify=self.notify_enabled)
        self.args = SimpleNamespace(packet=str(self.packet_path), out=str(self.output),
                                    resume=None, timeout=5)

    def write_packet(self, value):
        self.packet_path.write_text(json.dumps(value))

    def set_notify(self, enabled):
        self.config = SimpleNamespace(work_root=self.root, state_dir=self.root / 'state',
                                      cache_dir=self.root / 'cache', notify=enabled)

    async def run_fixture(self):
        with patch.object(protocol, 'ProtocolClient', self.fake_client), \
                patch.object(protocol, 'send_notification', self.spy.hook), \
                redirect_stdout(io.StringIO()):
            outcome = await protocol.execute(self.args, self.config)
        return outcome, json.loads(self.output.read_text())

    def posts(self):
        return self.spy.desktop

    async def test_disabled_never_posts_and_does_not_affect_delivery(self):
        self.set_notify(False)
        outcome, result = await self.run_fixture()
        self.assertTrue(outcome)
        self.assertTrue(result['delivery_ok'])
        self.assertEqual(self.posts(), [])
        self.assertEqual([kwargs.get('enabled') for _, kwargs in self.spy.calls],
                         [False, False])

    async def test_success_posts_dispatch_then_completion_without_prompt_content(self):
        self.set_notify(True)
        outcome, result = await self.run_fixture()
        self.assertTrue(outcome)
        self.assertEqual(len(self.posts()), 2)
        dispatch, completion = (''.join(argv) for argv in self.posts())
        self.assertIn('czs：任务已派发', dispatch)
        self.assertIn('czs：任务结束', completion)
        self.assertIn('run-42', completion)
        self.assertIn('sess_fixture', completion)
        self.assertIn('交付检查通过', completion)
        self.assertNotIn('Basso', completion)
        self.assertNotIn('SECRET-PROMPT-MARKER', dispatch + completion)
        send_index = self.order.index(('rpc', 'session/send'))
        first_post = next(i for i, entry in enumerate(self.order)
                          if entry[0] == 'desktop_post')
        self.assertGreater(first_post, send_index)

    async def test_turn_failure_posts_failure_completion_with_sound(self):
        self.set_notify(True)
        self.completion = 'error'
        outcome, result = await self.run_fixture()
        self.assertFalse(outcome)
        self.assertFalse(result['delivery_ok'])
        self.assertEqual(result['outcome'], 'worker-failed')
        self.assertEqual(len(self.posts()), 2)
        completion = ''.join(self.posts()[1])
        self.assertIn('worker 失败', completion)
        self.assertNotIn('SECRET-PROMPT-MARKER', completion)
        self.assertNotIn(str(result.get('error')), completion)
        self.assertIn('sound name "Basso"', self.posts()[1][2])

    async def test_timeout_posts_timeout_category(self):
        self.set_notify(True)
        self.complete = False
        self.args.timeout = 1
        outcome, result = await self.run_fixture()
        self.assertFalse(outcome)
        self.assertEqual(result['outcome'], 'timeout')
        completion = ''.join(self.posts()[1])
        self.assertIn('超时', completion)
        self.assertNotIn('SECRET-PROMPT-MARKER', completion)

    async def test_missing_required_command_posts_evidence_rejection(self):
        self.set_notify(True)
        self.write_packet({'workspace': str(self.workspace), 'prompt': 'SECRET-PROMPT-MARKER',
                           'allowed_files': [],
                           'commands': [{'id': 'must-run', 'command': '/bin/true',
                                         'max_uses': 1, 'max_timeout_ms': 5000,
                                         'required': True}]})
        outcome, result = await self.run_fixture()
        self.assertFalse(outcome)
        self.assertFalse(result['delivery_ok'])
        self.assertEqual(result['outcome'], 'evidence-rejected')
        completion = ''.join(self.posts()[1])
        self.assertIn('命令证据拒收（1 项）', completion)
        self.assertNotIn('SECRET-PROMPT-MARKER', completion)

    async def test_success_records_delivered_outcome(self):
        self.set_notify(True)
        outcome, result = await self.run_fixture()
        self.assertTrue(outcome)
        self.assertEqual(result['outcome'], 'delivered')

    async def test_successful_turn_missing_model_receipts_is_postflight_rejection(self):
        probe = {'workspace': '/w', 'prompt': 'p', 'allowed_files': [],
                 'model': {'providerId': 'p', 'modelId': 'm'}}
        try:
            protocol.validate_packet(probe)
        except ValueError:
            self.skipTest('model field is the parallel session feature; absent in this composition')
        self.set_notify(True)
        self.model = {'providerId': 'fixture-provider', 'modelId': 'fixture-model'}
        self.write_packet({'workspace': str(self.workspace), 'prompt': 'SECRET-PROMPT-MARKER',
                           'allowed_files': [], 'model': self.model})
        outcome, result = await self.run_fixture()
        self.assertFalse(outcome)
        self.assertFalse(result['delivery_ok'])
        self.assertEqual(result['outcome'], 'postflight-rejected')
        self.assertEqual(result['completion']['resultType'], 'success')
        self.assertIn('model execution evidence', result['error'])
        completion = ''.join(self.posts()[1])
        self.assertIn('终态后审计拒收', completion)
        self.assertNotIn(result['error'], completion)
        self.assertIn('sound name "Basso"', self.posts()[1][2])

    async def test_post_success_frozen_audit_failure_is_postflight_rejection(self):
        # 不依赖模型功能：成功终态后冻结哈希审计失败也必须归为终态后审计拒收。
        self.set_notify(True)
        self.tamper_on_send = True
        outcome, result = await self.run_fixture()
        self.assertFalse(outcome)
        self.assertFalse(result['delivery_ok'])
        self.assertEqual(result['completion']['resultType'], 'success')
        self.assertEqual(result['outcome'], 'postflight-rejected')
        completion = ''.join(self.posts()[1])
        self.assertIn('终态后审计拒收', completion)
        self.assertNotIn(result['error'], completion)

    async def test_packet_rejection_before_dispatch_posts_nothing(self):
        self.set_notify(True)
        self.write_packet({'workspace': str(self.workspace), 'prompt': 'p',
                           'allowed_files': [], 'surprise': True})
        with self.assertRaises(ValueError):
            await self.run_fixture()
        self.assertEqual(self.posts(), [])

    async def test_resume_session_still_dispatches_both_notifications(self):
        self.set_notify(True)
        state = self.root / 'state'
        state.mkdir()
        (state / 'sess_fixture.json').write_text(json.dumps(
            {'workspace': str(self.workspace), 'state': 'exited', 'pid': 4000000}))
        self.args.resume = 'sess_fixture'
        outcome, result = await self.run_fixture()
        self.assertTrue(outcome)
        self.assertEqual(len(self.posts()), 2)
        self.assertIn('session/resume', [entry[1] for entry in self.order
                                         if entry[0] == 'rpc'])

    async def test_missing_config_attribute_means_silent_not_enabled(self):
        self.config = SimpleNamespace(work_root=self.root, state_dir=self.root / 'state',
                                      cache_dir=self.root / 'cache')
        outcome, _ = await self.run_fixture()
        self.assertTrue(outcome)
        self.assertEqual(self.posts(), [])
