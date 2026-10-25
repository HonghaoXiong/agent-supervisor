"""Portable configuration and pinned-copy tests; never launch a live ZCode CLI."""

from dataclasses import replace
import hashlib
import json
import os
from pathlib import Path
import sys
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
        registry = {digest(self.original): {
            "patched_sha256": digest(self.expected),
            "upstream_commit": "fixturecommit", "reviewed": "fixture",
            "provenance": "fixture pin"}}
        self.pins = patch.object(runtime, "REVIEWED_BUNDLES", registry)
        self.pins.start()
        self.addCleanup(self.pins.stop)


class CommittedRegistryTests(unittest.TestCase):
    """The committed registry must keep the old reviewed pin and record honest provenance."""

    def test_both_reviewed_bundles_remain_supported(self):
        pins = runtime.REVIEWED_BUNDLES
        self.assertIn("b816e38656e70fb585fe2ab1ea010da89cda928dac1ccb5d83255fd18796da6e", pins)
        self.assertIn("31965ce1d5d4a808ef156784f67996de42ccab335591d2386a61ea8f6f506a54", pins)
        original = pins["b816e38656e70fb585fe2ab1ea010da89cda928dac1ccb5d83255fd18796da6e"]
        self.assertEqual(original["upstream_commit"], "aac4755666d09fdcd70272fcf063c077a639015f")
        replacement = pins["31965ce1d5d4a808ef156784f67996de42ccab335591d2386a61ea8f6f506a54"]
        self.assertIsNone(replacement["upstream_commit"])
        self.assertIn("could NOT be confirmed", replacement["provenance"])
        for pin in pins.values():
            self.assertRegex(pin["patched_sha256"], r"[a-f0-9]{64}")
            self.assertTrue(pin["reviewed"] and pin["provenance"])

    def test_old_pins_keep_0169_semantics_unchanged(self):
        pins = runtime.REVIEWED_BUNDLES
        original = pins["b816e38656e70fb585fe2ab1ea010da89cda928dac1ccb5d83255fd18796da6e"]
        self.assertEqual(original, {
            "patched_sha256": "31ef7f33f40591303047db5135845bd39b47399372110483fe6cf6c2df4328d2",
            "upstream_commit": "aac4755666d09fdcd70272fcf063c077a639015f",
            "reviewed": "2026-10-13",
            "provenance": "official source commit verified; desktop 3.15.1 installation",
            "cli_version": "0.16.9"})
        replacement = pins["31965ce1d5d4a808ef156784f67996de42ccab335591d2386a61ea8f6f506a54"]
        self.assertEqual(replacement["patched_sha256"],
                         "21ef67a4152d9184009613c7edd187063283dbe92a9db583618b375f1f1e2900")
        self.assertEqual((replacement["upstream_commit"], replacement["reviewed"],
                          replacement["cli_version"]), (None, "2026-10-14", "0.16.9"))
        self.assertIn("could NOT be confirmed", replacement["provenance"])

    def test_new_0170_pin_is_recorded_with_honest_provenance(self):
        pins = runtime.REVIEWED_BUNDLES
        pin = pins["a055234a6ca3da0172616d3987cf3c617b6a26deb1332d69035300c2a0ae0de1"]
        self.assertEqual(pin["patched_sha256"],
                         "87e3e4bfb2292ea75b3fb7398ab7c1490ecbe0848785bf69e0d99b893e27ab4c")
        self.assertIsNone(pin["upstream_commit"])
        self.assertEqual(pin["cli_version"], "0.17.0")
        self.assertEqual(pin["reviewed"], "2026-10-20")
        self.assertIn("3.16.2.8769", pin["provenance"])
        self.assertIn("could NOT be confirmed", pin["provenance"])
        self.assertIn("0.17.0", pin["provenance"])
        # Same exact one-fragment deletion as the reviewed 0.16.9 pins.
        self.assertEqual(runtime.CLI_VERSION, "0.16.9")
        self.assertEqual(runtime.CACHE_LAYOUT, 2)
        self.assertEqual(runtime.COMPATIBILITY, "pinned-0.16.9")

class PreparedCliTests(RuntimeFixture):
    def test_exact_patch_only_and_no_source_or_credentials_modified(self):
        before = self.source.stat().st_mtime_ns
        output = runtime.prepare_cli(self.config)
        self.assertEqual(output.read_bytes(), self.expected)
        self.assertEqual(self.source.read_bytes(), self.original)
        self.assertEqual(self.source.stat().st_mtime_ns, before)
        self.assertEqual(output.parent.name,
                         f"zcode-0.16.9-{digest(self.original)}-v{runtime.CACHE_LAYOUT}")
        manifest = json.loads(output.with_name("manifest.json").read_text())
        self.assertIs(manifest["installation_modified"], False)
        self.assertIs(manifest["credentials_copied"], False)
        self.assertEqual(manifest["source_diff_sha256"], digest(runtime._source_diff()))
        self.assertEqual(manifest["official_source_commit"], "fixturecommit")
        self.assertEqual(manifest["source_revision_status"], "verified")
        self.assertNotIn(str(self.directory), json.dumps(manifest))
        self.assertFalse(self.config.state_dir.exists())
        self.assertFalse(self.config.work_root.exists())

    def test_legacy_pins_without_cli_version_default_to_0169(self):
        # Legacy fixture pins (no explicit cli_version) keep their old 0.16.9
        # cache-name/manifest semantics; explicit 0.17.0 pins use their own.
        output = runtime.prepare_cli(self.config)
        manifest = json.loads(output.with_name("manifest.json").read_text())
        self.assertEqual(manifest["cli_version"], "0.16.9")
        self.assertEqual(output.parent.name,
                         f"zcode-0.16.9-{digest(self.original)}-v{runtime.CACHE_LAYOUT}")
        registry = {digest(self.original): {
            "patched_sha256": digest(self.expected),
            "upstream_commit": None, "reviewed": "fixture",
            "provenance": "fixture pin", "cli_version": "0.17.0"}}
        with patch.object(runtime, "REVIEWED_BUNDLES", registry):
            other = runtime.prepare_cli(self.config)
            other_manifest = json.loads(other.with_name("manifest.json").read_text())
            self.assertEqual(other_manifest["cli_version"], "0.17.0")
            self.assertEqual(other.parent.name,
                             f"zcode-0.17.0-{digest(self.original)}-v{runtime.CACHE_LAYOUT}")
            # Old 0.16.9 cache is neither rewritten nor removed.
            self.assertEqual(output.read_bytes(), self.expected)
            self.assertTrue(output.with_name("manifest.json").exists())

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
            with self.subTest(source=source):
                patched = source.replace(runtime.FRAGMENT, b"", 1)
                registry = {digest(source): {
                    "patched_sha256": digest(patched),
                    "upstream_commit": None, "reviewed": "fixture",
                    "provenance": "fixture pin"}}
                with patch.object(runtime, "REVIEWED_BUNDLES", registry):
                    self.source.write_bytes(source)
                    with self.assertRaisesRegex(ValueError, "exactly once"):
                        runtime.prepare_cli(self.config)
                    self.assertFalse(self.config.cache_dir.exists())

    def test_patched_digest_must_match(self):
        registry = {digest(self.original): {
            "patched_sha256": "0" * 64, "upstream_commit": None,
            "reviewed": "fixture", "provenance": "fixture pin"}}
        with patch.object(runtime, "REVIEWED_BUNDLES", registry):
            with self.assertRaisesRegex(ValueError, "patched CLI digest"):
                runtime.prepare_cli(self.config)
        self.assertFalse(self.config.cache_dir.exists())

    def test_cache_tampering_is_not_repaired(self):
        output = runtime.prepare_cli(self.config)
        output.write_bytes(b"tampered")
        with self.assertRaisesRegex(ValueError, "was modified"):
            runtime.prepare_cli(self.config)
        self.assertEqual(output.read_bytes(), b"tampered")

    def test_v1_layout_cache_with_legacy_manifest_is_bypassed_not_rejected(self):
        # 迁移策略：旧单钉版本写的 v1 目录（manifest 缺 source_revision_status 等新字段）
        # 不被读取也不被改写；新版本自动准备并复用 v2 目录；无需手工删除旧缓存。
        output = runtime.prepare_cli(self.config)
        legacy_dir = self.config.cache_dir / f"zcode-0.16.9-{digest(self.original)}"
        legacy_dir.mkdir(parents=True)
        v2_manifest = json.loads(output.with_name("manifest.json").read_text())
        legacy_manifest = {key: value for key, value in v2_manifest.items()
                           if key not in ("source_revision_status", "reviewed", "provenance")}
        (legacy_dir / "manifest.json").write_text(json.dumps(legacy_manifest, indent=2))
        (legacy_dir / "zcode.cjs").write_bytes(self.expected)
        before = {p.name: p.read_bytes() for p in legacy_dir.iterdir()}
        again = runtime.prepare_cli(self.config)
        self.assertEqual(again, output)  # v2 目录照常复用
        self.assertEqual({p.name: p.read_bytes() for p in legacy_dir.iterdir()}, before)
        self.assertTrue(again.parent.name.endswith("-v2"))

    def test_v2_layout_tampering_is_still_rejected_after_migration(self):
        output = runtime.prepare_cli(self.config)
        output.write_bytes(b"tampered")
        with self.assertRaisesRegex(ValueError, "was modified"):
            runtime.prepare_cli(self.config)
        output.write_bytes(self.expected)
        output.with_name("manifest.json").write_text("{}")
        with self.assertRaisesRegex(ValueError, "provenance"):
            runtime.prepare_cli(self.config)

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
        self.assertEqual(config, replace(self.config, notify=sys.platform == "darwin",
                                         config_dir=self.directory))
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


class BundledSkillsFreezeTests(unittest.TestCase):
    def _config(self, tmp):
        from zcode_supervisor.runtime import Config
        return Config(work_root=tmp/"work", state_dir=tmp/"state", cache_dir=tmp/"cache",
                      cli_path=Path("/Applications/ZCode.app/Contents/Resources/glm/zcode.cjs"),
                      node="node")

    def test_closure_freeze_and_drift_rejection(self):
        import shutil
        from zcode_supervisor import runtime as R
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            app = tmp/"app"/"bundled-skills"; (app/"skills"/"dynamic-workflows").mkdir(parents=True)
            (app/"skills"/"dynamic-workflows"/"SKILL.md").write_text("v1 body")
            (app/"skills"/"dynamic-workflows"/"patterns.md").write_text("patterns")
            config = self._config(tmp)
            config.cache_dir.mkdir(parents=True)
            link = R.prepare_bundled_skills(config, source=app)
            frozen = link.resolve()  # capture the first closure directory
            self.assertTrue(frozen.is_dir())
            self.assertEqual((frozen/"skills"/"dynamic-workflows"/"SKILL.md").read_text(), "v1 body")
            # drift: change the app source -> new closure id, link retargets
            (app/"skills"/"dynamic-workflows"/"SKILL.md").write_text("v2 body")
            link2 = R.prepare_bundled_skills(config, source=app)
            self.assertNotEqual(link2.resolve(), frozen.resolve())
            self.assertIn("v2", (link2.resolve()/"skills"/"dynamic-workflows"/"SKILL.md").read_text())
            # tamper with the frozen copy -> refusal
            tampered = link2.resolve()
            (tampered/"skills"/"dynamic-workflows"/"SKILL.md").write_text("tampered")
            with self.assertRaises(ValueError):
                R.prepare_bundled_skills(config, source=app)

    def test_foreign_valid_closure_refused_and_approved_restored(self):
        # N4 (round6): a second, independently prepared and fully
        # self-consistent closure is NOT this config's approved closure.
        from zcode_supervisor import runtime as R
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            app = tmp/"app"/"bundled-skills"; (app/"skills"/"dynamic-workflows").mkdir(parents=True)
            (app/"skills"/"dynamic-workflows"/"SKILL.md").write_text("source A")
            (app/"skills"/"dynamic-workflows"/"patterns.md").write_text("patterns")
            config = self._config(tmp); config.cache_dir.mkdir(parents=True)
            link = R.prepare_bundled_skills(config, source=app)
            approved = link.resolve()
            # A DIFFERENT controller root legitimately freezes another source.
            other = tmp/"other"; (other/"app"/"skills"/"dynamic-workflows").mkdir(parents=True)
            (other/"app"/"skills"/"dynamic-workflows"/"SKILL.md").write_text("source B")
            (other/"app"/"skills"/"dynamic-workflows"/"patterns.md").write_text("patterns")
            other_cfg = self._config(other); other_cfg.cache_dir.mkdir(parents=True)
            foreign = R.prepare_bundled_skills(other_cfg, source=other/"app").resolve()
            self.assertNotEqual(foreign, approved)
            # Swap this config's cache link to the foreign (internally valid)
            # closure: ensure must refuse — hash self-consistency is not approval.
            link.unlink(); link.symlink_to(foreign, target_is_directory=True)
            with self.assertRaises(ValueError):
                R.ensure_bundled_skills(config)
            # No auto-retarget: restoring the approved link is the only way on.
            link.unlink(); link.symlink_to(approved, target_is_directory=True)
            self.assertEqual(R.ensure_bundled_skills(config).resolve(), approved)

    def test_unapproved_link_and_malformed_record_refused(self):
        # N4 (round6): a cache link with no controller approval record behind
        # it must not define its own approval; a malformed record is a hard
        # error rather than a silent fallback to the untrusted link.
        from zcode_supervisor import runtime as R
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            app = tmp/"app"/"bundled-skills"; (app/"skills"/"dynamic-workflows").mkdir(parents=True)
            (app/"skills"/"dynamic-workflows"/"SKILL.md").write_text("v1")
            config = self._config(tmp); config.cache_dir.mkdir(parents=True)
            R.prepare_bundled_skills(config, source=app)
            record = config.state_dir/"skill-closure-approval.json"
            record.unlink()
            with self.assertRaises(ValueError):
                R.ensure_bundled_skills(config)
            record.write_text("{not json")
            with self.assertRaises(ValueError):
                R.ensure_bundled_skills(config)

    def test_record_missing_with_no_link_prepares_first_use(self):
        # True first use (no record AND no link) still provisions from the
        # configured source and records the approval for later comparison.
        from zcode_supervisor import runtime as R
        with tempfile.TemporaryDirectory() as tmp:
            tmp = Path(tmp)
            app = tmp/"app"/"bundled-skills"; (app/"skills"/"dynamic-workflows").mkdir(parents=True)
            (app/"skills"/"dynamic-workflows"/"SKILL.md").write_text("v1")
            config = self._config(tmp); config.cache_dir.mkdir(parents=True)
            # Drive first-use provisioning through prepare (its reviewed
            # default source is the installed app, not present in tests);
            # ensure then accepts exactly the recorded approved closure.
            link = R.prepare_bundled_skills(config, source=app)
            self.assertTrue(link.is_symlink())
            self.assertTrue((config.state_dir/"skill-closure-approval.json").is_file())
            self.assertEqual(R.ensure_bundled_skills(config).resolve(), link.resolve())
