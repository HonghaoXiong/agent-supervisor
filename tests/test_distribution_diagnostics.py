"""Read-only diagnostics tests; fake executables never contact Codex or ZCode."""
import contextlib
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import stat
import subprocess
import sys
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch


REPO = Path(__file__).resolve().parents[1]
SOURCE = REPO / 'packaging/macos/diagnostics.py'


@unittest.skipUnless(SOURCE.exists(), 'Distribution diagnostics not in core export')
class DistributionDiagnosticsTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        spec = importlib.util.spec_from_file_location('czs_diagnostics_tests', SOURCE)
        cls.diagnostics = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cls.diagnostics)

    def setUp(self):
        self.temporary = TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.plugin = self.root / 'plugins/zcode-supervisor'
        runtime = self.plugin / 'lib/zcode_supervisor'
        runtime.mkdir(parents=True)
        (self.plugin / 'plugin.json').write_text('{"version":"0.1.1"}')
        (runtime / '__init__.py').write_text('__version__ = "0.1.0"\n')
        self.cli = self.root / 'zcode.cjs'
        self.cli.write_text('reviewed fake byte fixture; never executed')
        self.fingerprint = hashlib.sha256(self.cli.read_bytes()).hexdigest()
        (runtime / 'runtime.py').write_text('REVIEWED_BUNDLES = ' + repr({self.fingerprint: {}}))
        self.node = self.executable('node', '#!/bin/sh\nprintf "v22.4.1\\n"\n')
        self.codex = self.executable('codex', '#!/bin/sh\ncase "$*" in\n  --version) printf "codex-cli 0.163.0-alpha.10.2\\n" ;;\n  *--help) printf "Usage: fake command\\n" ;;\n  *) exit 99 ;;\nesac\n')

    def executable(self, name, body):
        path = self.root / name
        path.write_text(body)
        path.chmod(0o755)
        return str(path)

    def inspect(self, **kwargs):
        return self.diagnostics.inspect_environment(self.plugin, codex=kwargs.get('codex', self.codex),
                                                    node=kwargs.get('node', self.node),
                                                    cli=kwargs.get('cli', self.cli))

    def check(self, report, name):
        return next(c for c in report['checks'] if c['id'] == name)

    def test_healthy_report_has_stable_capabilities_and_no_paths(self):
        report = self.inspect()
        self.assertTrue(report['ok'])
        self.assertEqual(report['schema_version'], 1)
        self.assertEqual(report['model_requests'], 0)
        self.assertEqual(report['plugin_version'], '0.1.1')
        self.assertEqual(report['core_version'], '0.1.0')
        self.assertEqual(self.check(report, 'node')['version'], '22.4.1')
        self.assertEqual(report['capabilities']['authentication']['status'], 'unknown')
        self.assertEqual(report['capabilities']['strict_all_actor_prestart_request_cap']['status'], 'unsupported')
        self.assertEqual(report['capabilities']['fixed_model_selection']['status'], 'unsupported')
        self.assertNotIn(str(self.root), json.dumps(report))
        self.assertTrue(all({'id', 'status', 'error_code', 'remediation'} <= set(c) for c in report['checks']))

    def test_only_version_and_help_commands_are_invoked(self):
        with patch.object(self.diagnostics.subprocess, 'run', wraps=subprocess.run) as called:
            self.inspect()
        commands = [c.args[0] for c in called.call_args_list]
        self.assertEqual(len(commands), 8)
        self.assertTrue(all(c[-1] in ('--version', '--help') for c in commands))
        self.assertNotIn('exec', [part for c in commands for part in c])

    def test_missing_explicit_dependencies_do_not_silently_fallback(self):
        report = self.inspect(node='/does/not/exist', codex='/does/not/exist')
        self.assertFalse(report['ok'])
        self.assertEqual(self.check(report, 'node')['error_code'], 'NODE_NOT_FOUND')
        self.assertEqual(self.check(report, 'codex_cli')['error_code'], 'CODEX_NOT_FOUND')
        self.assertEqual(self.check(report, 'codex_plugin_add')['status'], 'unknown')

    def test_unknown_bundle_fails_closed_without_executing_zcode(self):
        self.cli.write_text('unreviewed')
        with patch.object(self.diagnostics.subprocess, 'run', wraps=subprocess.run) as called:
            report = self.inspect()
        self.assertFalse(report['ok'])
        self.assertEqual(self.check(report, 'zcode_cli')['error_code'], 'ZCODE_FINGERPRINT_UNREVIEWED')
        self.assertFalse(any(str(self.cli) in c.args[0] for c in called.call_args_list))

    def test_missing_plugin_command_is_diagnostic_without_raw_error(self):
        bad = self.executable('old-codex', '#!/bin/sh\nif [ "$1" = --version ]; then echo "codex-cli 0.1.0"; else echo "SECRET alice@example.com /Users/alice/private" >&2; exit 2; fi\n')
        report = self.inspect(codex=bad)
        self.assertFalse(report['ok'])
        self.assertEqual(self.check(report, 'codex_marketplace_remove')['error_code'], 'CODEX_PLUGIN_COMMAND_UNSUPPORTED')
        self.assertNotIn('SECRET', json.dumps(report))
        self.assertNotIn('alice', json.dumps(report))

    def test_plugin_remove_missing_is_an_explicit_preflight_failure(self):
        bad = self.executable('no-remove-codex', '#!/bin/sh\ncase "$*" in\n "plugin remove --help") exit 2 ;;\n --version) echo "codex-cli 0.163.0" ;;\n *) echo "Usage: fake command" ;;\nesac\n')
        report = self.inspect(codex=bad)
        self.assertFalse(report['ok'])
        self.assertEqual(self.check(report, 'codex_plugin_remove')['status'], 'unsupported')
        self.assertEqual(self.check(report, 'codex_plugin_remove')['error_code'], 'CODEX_PLUGIN_COMMAND_UNSUPPORTED')
        self.assertEqual(self.check(report, 'codex_plugin_add')['status'], 'supported')

    def test_homebrew_resolved_cellar_path_is_used_in_setup(self):
        cellar = self.root / 'Cellar/node/22.4.1/bin/node'
        cellar.parent.mkdir(parents=True)
        cellar.write_text('#!/bin/sh\necho v22.4.1\n')
        cellar.chmod(0o755)
        link = self.root / 'node-link'
        link.symlink_to(cellar)
        self.assertEqual(self.diagnostics.find_node(str(link)), str(cellar))
        with patch.object(self.diagnostics, 'find_node', return_value='/opt/homebrew/Cellar/node/22.4.1/bin/node'):
            setup = self.diagnostics.setup_commands(self.plugin, self.root / 'work')
        self.assertIn('--node /opt/homebrew/Cellar/node/22.4.1/bin/node', setup['commands'][0])

    def test_local_node_path_prints_only_selected_absolute_path(self):
        with patch.object(self.diagnostics, 'locate_plugin_root', side_effect=AssertionError('unneeded')):
            with patch.object(self.diagnostics.subprocess, 'run', side_effect=AssertionError('no subprocess required')):
                with contextlib.redirect_stdout(io.StringIO()) as stdout:
                    code = self.diagnostics.main(['--node-path', '--node', self.node])
        self.assertEqual(code, 0)
        self.assertEqual(stdout.getvalue(), self.node + '\n')

    def test_local_node_path_missing_or_report_conflict_fails_without_writes(self):
        with contextlib.redirect_stdout(io.StringIO()) as stdout:
            code = self.diagnostics.main(['--node-path', '--node', '/does/not/exist'])
        self.assertEqual(code, 2)
        self.assertEqual(json.loads(stdout.getvalue())['error_code'], 'NODE_NOT_FOUND')
        output = self.root / 'must-not-exist.json'
        with patch.object(self.diagnostics, 'find_node', side_effect=AssertionError('must reject before resolution')):
            with contextlib.redirect_stdout(io.StringIO()) as stdout:
                code = self.diagnostics.main(['--node-path', '--output', str(output)])
        self.assertEqual(code, 2)
        self.assertEqual(json.loads(stdout.getvalue())['error_code'], 'NODE_PATH_REPORT_CONFLICT')
        self.assertFalse(output.exists())

    def test_explicit_node_outside_path_and_shareable_setup(self):
        with patch.dict(os.environ, {'PATH': '/does/not/exist'}):
            report = self.inspect()
            commands = self.diagnostics.setup_commands(self.plugin, self.root / 'new-project', self.node)
        self.assertTrue(report['ok'])
        self.assertIn('--node "$CZS_NODE"', commands['commands'][0])
        self.assertNotIn(str(self.root), json.dumps(commands))
        self.assertFalse((self.root / 'new-project').exists())

    def test_standard_absolute_node_path_is_used_in_init_command(self):
        with patch.object(self.diagnostics, 'find_node', return_value='/opt/homebrew/bin/node'):
            commands = self.diagnostics.setup_commands(self.plugin, self.root / 'new-project')
        self.assertIn('--node /opt/homebrew/bin/node', commands['commands'][0])
        self.assertIn('doctor', commands['commands'][1])

    def test_default_inspection_does_not_write_files(self):
        before = sorted(p.relative_to(self.root).as_posix() for p in self.root.rglob('*'))
        self.inspect()
        after = sorted(p.relative_to(self.root).as_posix() for p in self.root.rglob('*'))
        self.assertEqual(before, after)

    def test_report_created_private_and_never_overwritten(self):
        target = self.root / 'report.json'
        self.diagnostics.write_report(target, self.inspect())
        original = target.read_bytes()
        self.assertEqual(stat.S_IMODE(target.stat().st_mode), 0o600)
        with self.assertRaisesRegex(ValueError, 'REPORT_EXISTS'):
            self.diagnostics.write_report(target, {'new': True})
        self.assertEqual(target.read_bytes(), original)
        target.unlink()
        target.symlink_to(self.cli)
        with self.assertRaisesRegex(ValueError, 'REPORT_SYMLINK_REFUSED'):
            self.diagnostics.write_report(target, {})
        link = self.root / 'linked-parent'
        link.symlink_to(self.plugin, target_is_directory=True)
        with self.assertRaisesRegex(ValueError, 'REPORT_SYMLINK_REFUSED'):
            self.diagnostics.write_report(link / 'report.json', {})

    def test_locates_both_release_and_installed_plugin(self):
        self.assertEqual(self.diagnostics.locate_plugin_root(self.root / 'scripts/diagnostics.py'), self.plugin)
        self.assertEqual(self.diagnostics.locate_plugin_root(self.plugin / 'scripts/diagnostics.py'), self.plugin)

    def test_timeout_and_unrecognized_version_do_not_leak_output(self):
        def fake(command, **kwargs):
            if command[0] == self.node:
                return subprocess.CompletedProcess(command, 0, 'sensitive /Users/alice/custom', '')
            raise subprocess.TimeoutExpired(command, 15, output='secret', stderr='token')
        with patch.object(self.diagnostics.subprocess, 'run', side_effect=fake):
            report = self.inspect()
        self.assertEqual(self.check(report, 'node')['error_code'], 'NODE_VERSION_UNRECOGNIZED')
        self.assertEqual(self.check(report, 'codex_cli')['error_code'], 'COMMAND_TIMEOUT')
        self.assertNotIn('alice', json.dumps(report))
        self.assertNotIn('secret', json.dumps(report))

    def test_cli_no_output_flag_is_read_only(self):
        with patch.object(self.diagnostics, 'locate_plugin_root', return_value=self.plugin):
            with contextlib.redirect_stdout(io.StringIO()) as stdout:
                code = self.diagnostics.main(['--node', self.node, '--codex', self.codex, '--cli', str(self.cli),
                                              '--setup-directory', str(self.root / 'not-created')])
        self.assertEqual(code, 0)
        self.assertTrue(json.loads(stdout.getvalue())['ok'])
        self.assertFalse((self.root / 'not-created').exists())


if __name__ == '__main__':
    unittest.main()
