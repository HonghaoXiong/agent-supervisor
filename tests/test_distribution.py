"""Distribution tests do not install into the user's Codex home or call models."""
import importlib.util
import contextlib
import io
import json
from pathlib import Path
import shutil
import subprocess
import sys
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch
import zipfile

from test_distribution_upgrade import FakeCodex


REPO = Path(__file__).resolve().parents[1]


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@unittest.skipUnless((REPO / "packaging/accepted-core.json").exists(), "Packaging source is not in baseline exports")
class DistributionTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temporary = TemporaryDirectory()
        cls.root = Path(cls.temporary.name)
        cls.builder = load("czs_distribution_builder", REPO / "tools/build_macos_plugin.py")
        cls.installer = load("czs_distribution_installer", REPO / "packaging/macos/install.py")
        cls.archive = cls.builder.build(REPO, cls.root / "first")
        cls.version = json.loads((REPO / "plugins/zcode-supervisor/plugin.json").read_text())["version"]
        cls.release = cls.root / ("first/zcode-supervisor-" + cls.version)

    @classmethod
    def tearDownClass(cls):
        cls.temporary.cleanup()

    def copy_release(self, name):
        target = self.root / name
        shutil.copytree(self.release, target)
        return target

    def test_deterministic_archive_uses_accepted_blobs_and_no_private_files(self):
        second = self.builder.build(REPO, self.root / "second")
        self.assertEqual(self.archive.read_bytes(), second.read_bytes())
        with zipfile.ZipFile(second) as archive:
            names = archive.namelist()
            self.assertTrue(any(n.endswith('/.codex-plugin/plugin.json') for n in names))
            self.assertTrue(any(n.endswith('/.agents/plugins/marketplace.json') for n in names))
            self.assertFalse(any('/.czs/' in n or n.endswith('.events.jsonl') or '/__pycache__/' in n for n in names))
        core = self.release / 'plugins/zcode-supervisor/lib/zcode_supervisor/protocol.py'
        self.assertIn('def observed_model(', core.read_text())  # fixed-model feature ships since 0.1.2
        pins = json.loads((REPO / 'packaging/accepted-core.json').read_text())
        actual = self.installer.verify_release(self.release)
        self.assertEqual(actual['accepted_core'], pins)
        for path in self.release.rglob('*'):
            if path.is_file():
                self.assertNotIn('/Users/', path.read_text())

    def test_relocation_runs_without_repo_virtualenv_or_pythonpath(self):
        relocated = self.copy_release('relocated path with spaces')
        script = relocated / 'plugins/zcode-supervisor/scripts/czs.py'
        result = subprocess.run([sys.executable, '-I', '-B', str(script), '--version'],
                                cwd='/', capture_output=True, text=True, check=True)
        self.assertEqual(result.stdout.strip(), '0.1.0')

    def test_checksum_tamper_and_unexpected_private_file_are_rejected(self):
        modified = self.copy_release('modified')
        (modified/'plugins/zcode-supervisor/scripts/czs.py').write_text('raise SystemExit(0)')
        with self.assertRaisesRegex(ValueError, 'checksum mismatch'):
            self.installer.verify_release(modified)
        extra = self.copy_release('extra')
        (extra/'provider.json').write_text('{}')
        with self.assertRaisesRegex(ValueError, 'unexpected'):
            self.installer.verify_release(extra)

    def test_symlink_is_rejected_before_hashing_member_bytes(self):
        linked = self.copy_release('linked')
        (linked/'scripts/python.sh').unlink()
        (linked/'scripts/python.sh').symlink_to('/etc/hosts')
        with self.assertRaisesRegex(ValueError, 'Linked release component'):
            self.installer.verify_release(linked)

    def test_install_is_versioned_idempotent_and_preserves_existing_files(self):
        prefix = self.root/'installation'
        prefix.mkdir()
        marker = prefix/'other-plugin.txt'
        marker.write_text('keep')
        native = FakeCodex(self.root/'idempotent-native')
        with patch.object(self.installer, 'run_codex', side_effect=native) as cli:
            first = self.installer.install(self.release, prefix, '/codex')
            second = self.installer.install(self.release, prefix, '/codex')
        self.assertEqual(first['release_directory'], second['release_directory'])
        self.assertEqual(marker.read_text(), 'keep')
        self.assertEqual(cli.call_count, 10)
        self.assertTrue(all('plugin' in c.args[0] and 'exec' not in c.args[0] for c in cli.call_args_list))

    def test_existing_damaged_installation_is_not_overwritten(self):
        prefix = self.root/'damaged-install'
        with patch.object(self.installer, 'run_codex', side_effect=FakeCodex(self.root/'damaged-native')):
            result = self.installer.install(self.release, prefix, '/codex')
        damaged = Path(result['release_directory'])/'scripts/python.sh'
        damaged.write_text('keep forensic bytes')
        with patch.object(self.installer, 'run_codex') as cli:
            with self.assertRaisesRegex(ValueError, 'checksum mismatch'):
                self.installer.install(self.release, prefix, '/codex')
            cli.assert_not_called()
        self.assertEqual(damaged.read_text(), 'keep forensic bytes')

    def test_failed_codex_install_returns_failure_and_retains_copy(self):
        prefix = self.root/'failed-install'
        with patch.object(self.installer, 'run_codex', side_effect=RuntimeError('CLI unavailable')):
            with self.assertRaisesRegex(RuntimeError, 'CLI unavailable'):
                self.installer.install(self.release, prefix, '/codex')
        self.assertTrue(any(p.is_dir() for p in prefix.iterdir()))

    def test_builder_refuses_an_unlisted_template_file(self):
        template = self.root/'private-template'
        shutil.copytree(REPO/'plugins/zcode-supervisor', template)
        (template/'provider.json').write_text('{}')
        with self.assertRaisesRegex(ValueError, 'allowlist'):
            self.builder.template_files(template)

    def test_upgrade_only_replaces_a_verified_owned_source(self):
        prefix = self.root/'upgrade-prefix'
        previous = prefix/'old-release'
        previous.parent.mkdir()
        shutil.copytree(self.release, previous)
        target = prefix/'new-release'
        with patch.object(self.installer, 'run_codex', side_effect=FakeCodex(self.root/'upgrade-native', previous)) as cli:
            result = self.installer.register_marketplace(target, prefix, '/codex')
        self.assertEqual(result['previousOwnedSource'], str(previous))
        self.assertTrue(previous.exists())
        self.assertEqual(cli.call_count, 4)

    def test_upgrade_does_not_remove_an_unrelated_source(self):
        with patch.object(self.installer, 'run_codex', side_effect=[
            {'marketplaces': [{'name': self.installer.MARKETPLACE, 'root': '/another/source'}]},
            {'installed': []},
        ]) as cli:
            with self.assertRaisesRegex(ValueError, 'another source'):
                self.installer.register_marketplace(self.root/'target', self.root/'prefix', '/codex')
            self.assertEqual(cli.call_count, 2)

    def test_stale_plugin_cache_is_not_reported_successful(self):
        stale = self.copy_release('stale-cache')/'plugins/zcode-supervisor'
        (stale/'scripts/czs.py').write_text('old runtime')
        with self.assertRaisesRegex(ValueError, 'differs from this release'):
            self.installer.verify_plugin_cache(self.release, {'installedPath': str(stale)})

    def test_finder_metadata_does_not_break_verification(self):
        finder = self.copy_release('finder-opened')
        (finder/'.DS_Store').write_bytes(b'Finder metadata')
        self.assertEqual(self.installer.verify_release(finder), self.installer.verify_release(self.release))

    def test_failed_source_switch_restores_previous_owned_marketplace(self):
        prefix = self.root/'rollback-prefix'
        previous = prefix/'old-release'
        previous.parent.mkdir()
        shutil.copytree(self.release, previous)
        native = FakeCodex(self.root/'rollback-native', previous)
        native.failure = 'source-add'
        with patch.object(self.installer, 'run_codex', side_effect=native) as cli:
            with self.assertRaisesRegex(RuntimeError, 'original new-source error'):
                self.installer.register_marketplace(prefix/'new', prefix, '/codex')
        self.assertIn(['/codex', 'plugin', 'marketplace', 'add', str(previous), '--json'], native.calls)
        self.assertEqual(native.source, previous)

    def test_install_cannot_recursively_copy_itself(self):
        prefix = self.release/'nested-install'
        with patch.object(self.installer, 'run_codex') as cli:
            with self.assertRaisesRegex(ValueError, 'outside the extracted release'):
                self.installer.install(self.release, prefix, '/codex')
            cli.assert_not_called()
        self.assertFalse(prefix.exists())

    def test_install_command_preflight_refuses_before_registration(self):
        report = {'schema_version': 1, 'ok': False, 'model_requests': 0, 'checks': [
            {'id': 'codex_cli', 'status': 'supported'},
            {'id': 'codex_plugin_remove', 'status': 'unsupported',
             'error_code': 'CODEX_PLUGIN_COMMAND_UNSUPPORTED', 'remediation': 'Update Codex.'}]}
        output = io.StringIO()
        with patch.object(self.installer, '__file__', str(self.release/'scripts/install.py')), \
                patch.object(self.installer, 'find_codex', return_value='/codex'), \
                patch.object(self.installer, 'environment_report', return_value=report), \
                patch.object(self.installer, 'install') as install, contextlib.redirect_stderr(output):
            result = self.installer.main([])
        self.assertEqual(result, 1)
        install.assert_not_called()
        self.assertEqual(json.loads(output.getvalue())['error_code'], 'CODEX_PLUGIN_COMMANDS_UNAVAILABLE')

    def test_installer_and_standalone_diagnostics_share_the_same_report(self):
        diagnostics = self.installer.diagnostics_module()
        expected = diagnostics.inspect_environment(self.release/'plugins/zcode-supervisor',
                                                   codex='/missing-codex', node='/missing-node', cli='/missing-cli')
        actual = self.installer.environment_report(self.release, '/missing-codex', '/missing-node', '/missing-cli')
        self.assertEqual(actual, expected)
        self.assertEqual(actual['plugin_version'], self.version)
        self.assertEqual(actual['core_version'], '0.1.0')
        self.assertEqual(actual['model_requests'], 0)


if __name__ == '__main__':
    unittest.main()
