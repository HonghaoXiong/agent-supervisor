"""Configuration bootstrap never needs credentials or a live ZCode session."""
import json
import hashlib
import io
from contextlib import redirect_stdout
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
from unittest.mock import patch

from zcode_supervisor.cli import create_project, doctor, main


class InitTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='czs test ')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.cli = self.root / 'installed' / 'glm' / 'zcode.cjs'
        self.cli.parent.mkdir(parents=True)
        self.cli.write_text('fixture CLI, not executable')
        self.project = self.root / 'another project'

    @patch('zcode_supervisor.cli.shutil.which', return_value='/usr/bin/node')
    def test_init_generates_local_paths_packet_and_ignore_without_copying_cli(self, _):
        result = create_project(self.project, self.cli)
        config_path = Path(result['config'])
        config = json.loads(config_path.read_text())
        self.assertEqual(config['work_root'], 'work')
        self.assertEqual(config['cli_path'], str(self.cli.resolve()))
        self.assertNotIn('provider_config', config)
        self.assertEqual(config_path.stat().st_mode & 0o777, 0o600)
        packet = json.loads(Path(result['packet']).read_text())
        self.assertTrue(Path(packet['workspace']).is_dir())
        self.assertEqual(packet['allowed_files'], ['REPORT.md'])
        self.assertEqual(packet['commands'], [])
        self.assertEqual((self.project / '.czs/.gitignore').read_text(), '*\n')
        self.assertEqual(list(self.project.rglob('*.cjs')), [])

    @patch('zcode_supervisor.cli.shutil.which', return_value='/usr/bin/node')
    def test_provider_is_only_referenced_and_never_copied(self, _):
        provider = self.root / 'provider.json'
        provider.write_text('{"apiKey":"DO_NOT_COPY_TEST_SENTINEL"}')
        result = create_project(self.project, self.cli, provider)
        config = json.loads(Path(result['config']).read_text())
        self.assertEqual(config['provider_config'], str(provider.resolve()))
        for path in self.project.rglob('*'):
            if path.is_file():
                self.assertNotIn('DO_NOT_COPY_TEST_SENTINEL', path.read_text())

    @patch('zcode_supervisor.cli.shutil.which', return_value='/usr/bin/node')
    def test_reinitialization_cannot_overwrite_existing_state(self, _):
        result = create_project(self.project, self.cli)
        previous = Path(result['config']).read_bytes()
        with self.assertRaises(ValueError):
            create_project(self.project, self.cli)
        self.assertEqual(Path(result['config']).read_bytes(), previous)

    @patch('zcode_supervisor.cli.shutil.which', return_value='/usr/bin/node')
    def test_missing_cli_does_not_create_configuration(self, _):
        with self.assertRaises(ValueError):
            create_project(self.project, self.root / 'missing.cjs')
        self.assertFalse((self.project / '.czs').exists())

    @patch('zcode_supervisor.cli.shutil.which', return_value=None)
    def test_missing_node_fails_before_creating_configuration(self, _):
        with self.assertRaises(ValueError):
            create_project(self.project, self.cli)
        self.assertFalse((self.project / '.czs').exists())

    def test_symlink_project_is_rejected(self):
        self.project.symlink_to(self.root, target_is_directory=True)
        with self.assertRaises(ValueError):
            create_project(self.project, self.cli)

    def test_missing_configuration_is_json_failure(self):
        with redirect_stdout(io.StringIO()) as captured:
            self.assertEqual(main(['--config', str(self.root / 'missing.json'), 'doctor']), 1)
        self.assertIs(json.loads(captured.getvalue())['ok'], False)


class DoctorHabitProfileTests(unittest.TestCase):
    """doctor reports habit profile health as an informational field.

    A missing, healthy or drifted profile only shapes ``habit_profile``; it
    never makes doctor itself fail (version mismatches still raise as before).
    """

    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='czs doctor profile ')
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.cli = self.root / 'cli.cjs'
        self.cli.write_text('fixture bundle, not a real CLI')
        self.config = SimpleNamespace(
            cli_path=self.cli,
            command=lambda: ['/usr/bin/node', str(self.cli)],
            environment=lambda: {},
            work_root=self.root,
            notify=False,
            config_dir=self.root)

    def doctor_now(self):
        def fake_run(args, **kwargs):
            return SimpleNamespace(stdout='0.16.9\n', returncode=0)
        with patch('zcode_supervisor.cli.subprocess.run', side_effect=fake_run):
            return doctor(self.config)

    def write_profile(self, source_sha):
        source = self.root / 'source.md'
        profile_value = {'schema_version': 1,
                         'rules': [{'id': 'r1', 'text': '习惯正文',
                                    'sources': [{'path': str(source),
                                                 'sha256': source_sha}],
                                    'applies_to': ['*']}],
                         'tools': []}
        (self.root / 'profile.json').write_text(
            json.dumps(profile_value, ensure_ascii=False), encoding='utf-8')
        return self.root / 'profile.json'

    def test_doctor_without_profile_reports_missing_profile_only(self):
        report = self.doctor_now()
        self.assertIs(report['ok'], True)
        self.assertEqual(report['habit_profile'], {'profile': None})

    def test_doctor_reports_healthy_profile_rule_and_tool_counts(self):
        source = self.root / 'source.md'
        source.write_text('评审通过的习惯来源\n', encoding='utf-8')
        path = self.write_profile(hashlib.sha256(source.read_bytes()).hexdigest())
        report = self.doctor_now()
        self.assertEqual(report['habit_profile'],
                         {'profile': str(path), 'ok': True, 'rules': 1, 'tools': 0})

    def test_doctor_survives_drifted_profile_pins_and_reports_error(self):
        source = self.root / 'source.md'
        source.write_text('评审时的原始内容\n', encoding='utf-8')
        pinned = hashlib.sha256(source.read_bytes()).hexdigest()
        source.write_text('评审之后被改写\n', encoding='utf-8')  # 晋升后漂移
        path = self.write_profile(pinned)
        report = self.doctor_now()
        self.assertIs(report['ok'], True)  # doctor 本身不因 profile 问题 raise
        entry = report['habit_profile']
        self.assertIs(entry['ok'], False)
        self.assertEqual(entry['profile'], str(path))
        self.assertTrue(entry['error'])
        self.assertIn('changed since review', entry['error'])


class RunExitCodeContractTests(unittest.TestCase):
    """run must exit 0 only on delivery: execute()'s contract is a bool, and
    a preflight refusal (foreign closure / no worker started) must surface as
    CLI exit 1 — never swallowed as a truthy non-bool success."""

    def _run(self, mode):
        import asyncio
        import shutil
        from types import SimpleNamespace
        from zcode_supervisor import protocol as P, runtime as R
        # Resolve: czs refuses workspaces crossing a symlinked ancestor
        # (macOS /var -> /private/var), which mkdtemp returns unresolved.
        root = Path(tempfile.mkdtemp(prefix='czs-exit-contract-')).resolve()
        self.addCleanup(shutil.rmtree, root, True)
        app = root/'app'; sk = app/'skills'/'dynamic-workflows'; sk.mkdir(parents=True)
        for member, text in [(app/'README.md', 'root'), (sk/'SKILL.md', 'v'),
                             (sk/'patterns.md', 'p'), (sk/'examples.md', 'e')]:
            member.write_text(text)
        cfg = R.Config(work_root=root/'work', state_dir=root/'state',
                       cache_dir=root/'cache', cli_path=root/'cli')
        cfg.cache_dir.mkdir(); cfg.work_root.mkdir()
        ws = cfg.work_root/'workspace'; ws.mkdir()
        R.prepare_bundled_skills(cfg, source=app)
        if mode == 'foreign-closure':
            other = root/'other'
            osk = other/'app'/'skills'/'dynamic-workflows'; osk.mkdir(parents=True)
            (other/'app'/'README.md').write_text('other root')
            osk.joinpath('SKILL.md').write_text('other source')
            other_cfg = R.Config(work_root=other/'work', state_dir=other/'state',
                                 cache_dir=other/'cache', cli_path=other/'cli')
            other_cfg.cache_dir.mkdir()
            foreign = R.prepare_bundled_skills(other_cfg, source=other/'app').resolve()
            link = cfg.cache_dir/'bundled-skills'; link.unlink()
            link.symlink_to(foreign, target_is_directory=True)
        workflow = {'script': 'await world.done({});', 'max_concurrency': 1}
        packet = root/'packet.json'; out = cfg.work_root/'run.json'
        packet.write_text(json.dumps({'workspace': str(ws), 'prompt': 'offline',
                                      'allowed_files': [],
                                      'orchestration': {'workflow': workflow}}))
        calls = []

        class Client:
            def __init__(self, *a, orchestration=None):
                self.permission = a[2]; self.events = []; self.failure = None
                self.on_event = None
                self.process = SimpleNamespace(pid=987654, returncode=None)
                self.sent = False
            async def start(self): calls.append('start')
            async def close(self): calls.append('close'); self.process.returncode = 0
            def event(self, typ, payload, seq):
                event = {'type': typ, 'sessionId': 's', 'turnId': 't', 'seq': seq,
                         'payload': payload}
                self.events.append(event)
                if self.on_event:
                    self.on_event(event)
            async def request(self, method, params):
                calls.append(method)
                if method in ('session/create', 'session/read'):
                    return {'session': {'sessionId': 's', 'status': 'idle'},
                            'runtime': {'pendingRequestIds': []},
                            'projection': {}, 'settings': {}}
                if method == 'session/subscribe': return {'eventSeq': 0}
                if method == 'session/subagents': return {'running': [], 'ended': {'items': []}}
                if method == 'session/send':
                    self.sent = True
                    self.event('turn.started', {'inputId': params['inputId']}, 1)
                    self.event('turn.completed', {'resultType': 'success'}, 5)
                return {}
        original_sleep = asyncio.sleep

        async def zero(*a, **kw):
            await original_sleep(0)
        with patch('zcode_supervisor.cli.load_config', return_value=cfg), \
             patch.object(P, 'ProtocolClient', Client), \
             patch.object(P, 'send_notification', lambda **kw: None), \
             patch('asyncio.sleep', side_effect=zero), \
             redirect_stdout(io.StringIO()):
            code = main(['run', '--packet', str(packet), '--out', str(out), '--timeout', '5'])
        return code, calls, json.loads(out.read_text())

    def test_delivered_run_exits_zero(self):
        code, calls, receipt = self._run('control')
        self.assertEqual(code, 0)
        self.assertTrue(receipt['delivery_ok'])
        self.assertIn('start', calls)

    def test_preflight_refusal_exits_one_without_starting_worker(self):
        code, calls, receipt = self._run('foreign-closure')
        self.assertEqual(code, 1)
        self.assertNotIn('start', calls)
        self.assertIs(receipt['delivery_ok'], False)
        self.assertEqual(receipt['outcome'], 'preflight-rejected')
        self.assertIn('controller-approved', receipt['skill_closure_refused'])


if __name__ == '__main__':
    unittest.main()
