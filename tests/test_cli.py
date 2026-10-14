"""Configuration bootstrap never needs credentials or a live ZCode session."""
import json
import io
from contextlib import redirect_stdout
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from zcode_supervisor.cli import create_project, main


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


if __name__ == '__main__':
    unittest.main()
