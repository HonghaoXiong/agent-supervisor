"""Portable configuration and pinned-copy tests; never launch a live ZCode CLI."""

from dataclasses import replace
import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from zcode_supervisor import runtime


def digest(value):
    return hashlib.sha256(value).hexdigest()


class RuntimeFixture(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)
        self.source = self.directory / "installed.cjs"
        self.original = b"prefix," + runtime.FRAGMENT + b"suffix;"
        self.expected = b"prefix,suffix;"
        self.source.write_bytes(self.original)
        self.config = runtime.Config(
            work_root=self.directory / "work", state_dir=self.directory / "state",
            cache_dir=self.directory / "cache", cli_path=self.source,
        )
        hashes = patch.multiple(runtime, SOURCE_SHA256=digest(self.original), PATCHED_SHA256=digest(self.expected))
        hashes.start()
        self.addCleanup(hashes.stop)


class PreparedCliTests(RuntimeFixture):
    def test_exact_patch_only_and_no_source_or_credentials_modified(self):
        before = self.source.stat().st_mtime_ns
        output = runtime.prepare_cli(self.config)
        self.assertEqual(output.read_bytes(), self.expected)
        self.assertEqual(self.source.read_bytes(), self.original)
        self.assertEqual(self.source.stat().st_mtime_ns, before)
        self.assertEqual(output.parent.name, "zcode-0.16.9-" + digest(self.original))
        manifest = json.loads(output.with_name("manifest.json").read_text())
        self.assertIs(manifest["installation_modified"], False)
        self.assertIs(manifest["credentials_copied"], False)
        self.assertEqual(manifest["source_diff_sha256"], digest(runtime._source_diff()))
        self.assertNotIn(str(self.directory), json.dumps(manifest))
        self.assertFalse(self.config.state_dir.exists())
        self.assertFalse(self.config.work_root.exists())

    def test_reuses_verified_cache_without_rewriting(self):
        output = runtime.prepare_cli(self.config)
        manifest = output.with_name("manifest.json")
        times = output.stat().st_mtime_ns, manifest.stat().st_mtime_ns
        self.assertEqual(runtime.prepare_cli(self.config), output)
        self.assertEqual((output.stat().st_mtime_ns, manifest.stat().st_mtime_ns), times)

    def test_installed_upgrade_is_rejected_even_after_cache_creation(self):
        output = runtime.prepare_cli(self.config)
        self.source.write_bytes(self.original + b"changed")
        with self.assertRaisesRegex(ValueError, "Installed ZCode CLI changed"):
            runtime.prepare_cli(self.config)
        self.assertEqual(output.read_bytes(), self.expected)

    def test_unknown_bundle_does_not_create_cache(self):
        self.source.write_bytes(b"unknown release")
        with self.assertRaisesRegex(ValueError, "unsupported"):
            runtime.prepare_cli(self.config)
        self.assertFalse(self.config.cache_dir.exists())

    def test_missing_or_repeated_fragment_cannot_patch(self):
        for source in (b"missing", self.original + runtime.FRAGMENT):
            with self.subTest(source=source), patch.object(runtime, "SOURCE_SHA256", digest(source)):
                self.source.write_bytes(source)
                with self.assertRaisesRegex(ValueError, "exactly once"):
                    runtime.prepare_cli(self.config)
                self.assertFalse(self.config.cache_dir.exists())

    def test_patched_digest_must_match(self):
        with patch.object(runtime, "PATCHED_SHA256", "0" * 64):
            with self.assertRaisesRegex(ValueError, "patched CLI digest"):
                runtime.prepare_cli(self.config)
        self.assertFalse(self.config.cache_dir.exists())

    def test_cache_tampering_is_not_repaired(self):
        output = runtime.prepare_cli(self.config)
        output.write_bytes(b"tampered")
        with self.assertRaisesRegex(ValueError, "was modified"):
            runtime.prepare_cli(self.config)
        self.assertEqual(output.read_bytes(), b"tampered")

    def test_manifest_tampering_is_not_repaired(self):
        output = runtime.prepare_cli(self.config)
        manifest = output.with_name("manifest.json")
        manifest.write_text("{}")
        with self.assertRaisesRegex(ValueError, "provenance changed"):
            runtime.prepare_cli(self.config)
        self.assertEqual(manifest.read_text(), "{}")

    def test_malformed_manifest_is_reported(self):
        output = runtime.prepare_cli(self.config)
        output.with_name("manifest.json").write_text("not json")
        with self.assertRaisesRegex(ValueError, "Invalid local.*provenance"):
            runtime.prepare_cli(self.config)

    def test_source_diff_mismatch_is_detected(self):
        runtime.prepare_cli(self.config)
        with patch.object(runtime, "_source_diff", return_value=b"different source patch"):
            with self.assertRaisesRegex(ValueError, "provenance changed"):
                runtime.prepare_cli(self.config)

    def test_source_symlink_or_hardlink_is_rejected(self):
        target = self.directory / "real.cjs"
        self.source.rename(target)
        self.source.symlink_to(target)
        with self.assertRaises(ValueError):
            runtime.prepare_cli(self.config)
        self.source.unlink()
        os.link(target, self.source)
        with self.assertRaisesRegex(ValueError, "single-link"):
            runtime.prepare_cli(self.config)

    def test_source_fifo_is_rejected_without_blocking(self):
        self.source.unlink()
        os.mkfifo(self.source)
        with self.assertRaisesRegex(ValueError, "single-link"):
            runtime.prepare_cli(self.config)

    def test_cache_or_output_symlinks_cannot_touch_target(self):
        target = self.directory / "outside"
        target.mkdir()
        self.config.cache_dir.symlink_to(target, target_is_directory=True)
        with self.assertRaisesRegex(ValueError, "symlink directory"):
            runtime.prepare_cli(self.config)
        self.assertEqual(list(target.iterdir()), [])
        self.config.cache_dir.unlink()
        output = runtime.prepare_cli(self.config)
        external = target / "safe"
        external.write_bytes(b"untouched")
        for path in (output, output.with_name("manifest.json")):
            with self.subTest(path=path):
                original = path.read_bytes()
                path.unlink()
                path.symlink_to(external)
                with self.assertRaises(ValueError):
                    runtime.prepare_cli(self.config)
                self.assertEqual(external.read_bytes(), b"untouched")
                path.unlink()
                path.write_bytes(original)

    def test_world_writable_cache_rejected(self):
        self.config.cache_dir.mkdir(mode=0o700)
        self.config.cache_dir.chmod(0o777)
        with self.assertRaisesRegex(ValueError, "group/world writable"):
            runtime.prepare_cli(self.config)

    def test_command_uses_configured_node_and_pinned_copy(self):
        config = replace(self.config, node="/custom/node")
        command = config.command()
        self.assertEqual(command[0], "/custom/node")
        self.assertEqual(Path(command[1]).read_bytes(), self.expected)


class ConfigurationTests(RuntimeFixture):
    def setUp(self):
        super().setUp()
        self.path = self.directory / "config.json"
        self.values = {
            "schema_version": 1, "work_root": "work", "state_dir": "state",
            "cache_dir": "cache", "cli_path": "installed.cjs", "node": "node",
            "compatibility": "pinned-0.16.9",
        }

    def load(self, value=None):
        self.path.write_text(json.dumps(self.values if value is None else value))
        return runtime.load_config(self.path)

    def test_config_relative_paths_use_config_parent(self):
        config = self.load()
        self.assertEqual(config, self.config)
        self.assertFalse(config.cache_dir.exists())
        self.values["provider_config"] = "provider.json"
        self.assertEqual(self.load().provider_config, self.directory / "provider.json")

    def test_unknown_missing_and_nonobject_configs_fail(self):
        with self.assertRaisesRegex(ValueError, "JSON object"):
            self.load([])
        self.values["approve_all"] = True
        with self.assertRaisesRegex(ValueError, "unknown=.*approve_all"):
            self.load()
        self.values.pop("approve_all")
        self.values.pop("compatibility")
        with self.assertRaisesRegex(ValueError, "missing=.*compatibility"):
            self.load()

    def test_invalid_versions_and_unsafe_compatibility_are_rejected(self):
        for value in (True, "1", 0, 2):
            with self.subTest(value=value):
                self.values["schema_version"] = value
                with self.assertRaisesRegex(ValueError, "schema_version"):
                    self.load()
        self.values["schema_version"] = 1
        for value in ("raw", "unsafe", "auto", None, True):
            with self.subTest(value=value):
                self.values["compatibility"] = value
                with self.assertRaisesRegex(ValueError, "Only compatibility"):
                    self.load()

    def test_empty_or_nonstring_paths_and_node_rejected(self):
        for key in ("work_root", "state_dir", "cache_dir", "cli_path", "node"):
            saved = self.values[key]
            for value in ("", "  ", 123, False, "\x00"):
                with self.subTest(key=key, value=value):
                    self.values[key] = value
                    with self.assertRaises(ValueError):
                        self.load()
            self.values[key] = saved

    def test_shared_or_nested_control_and_worker_directories_rejected(self):
        for work in (".", "state", "state/subdirectory", "cache/subdirectory"):
            with self.subTest(work=work):
                self.values["work_root"] = work
                with self.assertRaisesRegex(ValueError, "separate, non-nested"):
                    self.load()

    def test_symlink_configuration_rejected(self):
        self.load()
        alias = self.directory / "alias.json"
        alias.symlink_to(self.path)
        with self.assertRaises(ValueError):
            runtime.load_config(alias)

    def test_configured_directory_symlink_rejected(self):
        self.config.cache_dir.symlink_to(self.directory / "other", target_is_directory=True)
        with self.assertRaisesRegex(ValueError, "symlink configured path"):
            self.load()

    def test_environment_is_process_local_and_does_not_read_provider(self):
        provider = self.directory / "not-read.json"
        config = replace(self.config, provider_config=provider)
        with patch.dict(os.environ, {"EXISTING_TEST_VALUE": "kept", "OMP_NUM_THREADS": "12"}, clear=True):
            environment = config.environment()
            self.assertEqual(environment["EXISTING_TEST_VALUE"], "kept")
            self.assertEqual(environment["OMP_NUM_THREADS"], "1")
            self.assertEqual(environment["OPENBLAS_NUM_THREADS"], "1")
            self.assertEqual(environment["ZCODE_BUILTIN_PROVIDER_CONFIG_FILE"], str(provider))
            self.assertEqual(os.environ["OMP_NUM_THREADS"], "12")
            self.assertNotIn("ZCODE_BUILTIN_PROVIDER_CONFIG_FILE", os.environ)
        self.assertFalse(provider.exists())

    def test_packaged_source_patch_is_available(self):
        self.assertIn(b"-    permission.allowedTools", runtime._source_diff())


if __name__ == "__main__":
    unittest.main()
