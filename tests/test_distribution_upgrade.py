"""Installer transactions use a stateful local CLI double, never a real Codex home."""
import hashlib
import importlib.util
import json
from pathlib import Path
import shutil
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch


REPO = Path(__file__).resolve().parents[1]


def load_installer():
    spec = importlib.util.spec_from_file_location("czs_upgrade_installer", REPO / "packaging/macos/install.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def make_release(root, version="0.1.1"):
    files = {
        "plugins/zcode-supervisor/plugin.json": json.dumps({"name": "zcode-supervisor", "version": version}),
        "plugins/zcode-supervisor/skills/zcode-supervisor/SKILL.md": "---\nname: zcode-supervisor\n---\n" + version,
        "plugins/zcode-supervisor/scripts/czs.py": "# synthetic package fixture " + version,
        "plugins/zcode-supervisor/lib/zcode_supervisor/__init__.py": version,
    }
    for name, text in files.items():
        path = root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
    manifest = {"schema_version": 1, "name": "zcode-supervisor", "version": version,
                "files": {name: hashlib.sha256((root / name).read_bytes()).hexdigest() for name in files}}
    (root / "release-manifest.json").write_text(json.dumps(manifest))
    return root


class FakeCodex:
    """Only the reviewed plugin CLI shapes, with separately tracked source/cache state."""

    def __init__(self, root, source=None, installed=False, enabled=True):
        self.root = root
        self.source = source
        self.original = source
        self.plugin = self.record(source, enabled) if installed else None
        self.calls = []
        self.failure = None
        self.corrupt_new_cache = None
        self.restoration_error = None
        self.other_sources = {"another-plugin": "/unrelated/source"}

    def record(self, source, enabled=True):
        version = json.loads((source / "release-manifest.json").read_text())["version"]
        return {"pluginId": "zcode-supervisor@zcode-supervisor-local", "name": "zcode-supervisor",
                "marketplaceName": "zcode-supervisor-local", "version": version,
                "installed": True, "enabled": enabled,
                "source": {"source": "local", "path": str(source / "plugins/zcode-supervisor")}}

    def __call__(self, command):
        self.calls.append(command)
        op = command[2:]
        if op == ["marketplace", "list", "--json"]:
            records = [{"name": name, "root": root} for name, root in self.other_sources.items()]
            if self.source:
                records.append({"name": "zcode-supervisor-local", "root": str(self.source)})
            return {"marketplaces": records}
        if op == ["list", "--marketplace", "zcode-supervisor-local", "--json"]:
            return {"installed": [dict(self.plugin)] if self.plugin else [], "available": []}
        if op == ["marketplace", "remove", "zcode-supervisor-local", "--json"]:
            self.source = None
            return {}
        if op[:2] == ["marketplace", "add"]:
            target = Path(op[2])
            if self.failure == "source-add" and target != self.original:
                self.failure = None
                raise RuntimeError("original new-source error")
            self.source = target
            return {"marketplaceName": "zcode-supervisor-local", "installedRoot": str(target)}
        if op == ["add", "zcode-supervisor@zcode-supervisor-local", "--json"]:
            if self.source == self.original and self.restoration_error:
                raise RuntimeError(self.restoration_error)
            self.plugin = self.record(self.source)
            cache = self.root / "cache" / self.plugin["version"]
            if cache.exists():
                shutil.rmtree(cache)
            shutil.copytree(self.source / "plugins/zcode-supervisor", cache)
            if self.source != self.original:
                if self.corrupt_new_cache:
                    self.corrupt_new_cache(cache)
                if self.failure == "plugin-add":
                    self.failure = None
                    raise RuntimeError("original plugin-add error after partial installation")
            return {"installedPath": str(cache), "pluginId": self.plugin["pluginId"]}
        if op == ["remove", "zcode-supervisor@zcode-supervisor-local", "--json"]:
            self.plugin = None
            return {}
        raise AssertionError(f"Unexpected command: {command}")


class DistributionUpgradeTests(unittest.TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.installer = load_installer()
        self.release = make_release(self.root / "new")
        self.prefix = self.root / "installation"
        self.prefix.mkdir()
        self.previous = make_release(self.prefix / "old", "0.1.0")
        self.cli = FakeCodex(self.root, self.previous, installed=True)
        self.native = patch.object(self.installer, "run_codex", side_effect=self.cli)
        self.native.start()
        self.addCleanup(self.native.stop)

    def install(self):
        return self.installer.install(self.release, self.prefix, "/codex")

    def failure(self):
        with self.assertRaises(self.installer.InstallTransactionError) as raised:
            self.install()
        receipt = raised.exception.receipt
        self.assertEqual(json.loads(Path(receipt["failure_receipt"]).read_text()), receipt)
        self.assertFalse(receipt["installed"])
        self.assertEqual(receipt["model_requests"], 0)
        self.assertEqual(self.cli.other_sources, {"another-plugin": "/unrelated/source"})
        return receipt

    def assert_old_restored(self, receipt):
        self.assertEqual(self.cli.source, self.previous)
        self.assertEqual(self.cli.plugin["version"], "0.1.0")
        self.assertTrue(self.cli.plugin["enabled"])
        self.assertTrue(receipt["rollback"]["source_restored"])
        self.assertTrue(receipt["rollback"]["plugin_restored"])
        self.assertEqual(receipt["rollback"]["state"], "previous_installation_restored_verified")

    def test_successful_upgrade_keeps_previous_release_and_verifies_enabled_cache(self):
        result = self.install()
        self.assertTrue(result["installed_cache_verified"])
        self.assertTrue(result["installed_enabled_verified"])
        self.assertEqual(self.cli.plugin["version"], "0.1.1")
        self.assertTrue(self.previous.is_dir())
        self.assertEqual(result["marketplace_result"]["previousOwnedSource"], str(self.previous))
        self.assertTrue(all(c[1] == "plugin" for c in self.cli.calls))

    def test_plugin_add_failure_restores_previous_source_plugin_and_original_error(self):
        self.cli.failure = "plugin-add"
        receipt = self.failure()
        self.assertEqual(receipt["failed_stage"], "plugin-add")
        self.assertIn("original plugin-add error", receipt["error"])
        self.assert_old_restored(receipt)

    def test_cache_extra_skill_is_rejected_and_upgrade_is_recovered(self):
        def add_skill(cache):
            path = cache / "skills/extra/SKILL.md"
            path.parent.mkdir()
            path.write_text("unexpected fixture")
        self.cli.corrupt_new_cache = add_skill
        receipt = self.failure()
        self.assertEqual(receipt["failed_stage"], "plugin-cache-verification")
        self.assertIn("unexpected=", receipt["error"])
        self.assert_old_restored(receipt)

    def test_missing_and_changed_cache_bytes_are_rejected_and_recovered(self):
        for mutation in (lambda p: (p / "scripts/czs.py").unlink(),
                         lambda p: (p / "scripts/czs.py").write_text("wrong bytes")):
            with self.subTest(mutation=mutation):
                self.cli.corrupt_new_cache = mutation
                receipt = self.failure()
                self.assertIn("differs from this release", receipt["error"])
                self.assert_old_restored(receipt)

    def test_rollback_error_is_reported_without_hiding_original_failure(self):
        self.cli.failure = "plugin-add"
        self.cli.restoration_error = "old plugin could not be reinstalled"
        receipt = self.failure()
        self.assertIn("original plugin-add error", receipt["error"])
        self.assertEqual(receipt["rollback"]["state"], "recovery_failed")
        self.assertTrue(receipt["rollback"]["source_restored"])
        self.assertFalse(receipt["rollback"]["plugin_restored"])
        self.assertEqual(receipt["rollback"]["errors"][0]["stage"], "restore-previous-plugin")
        self.assertIn("old plugin could not", receipt["rollback"]["errors"][0]["error"])

    def test_cleanup_failure_does_not_prevent_restoring_the_owned_source(self):
        self.cli.plugin = None
        self.cli.failure = "plugin-add"
        native = self.cli.__call__
        def fail_cleanup(command):
            if command[2:] == ["remove", self.installer.SELECTOR, "--json"]:
                raise RuntimeError("partial new plugin uninstall failed")
            return native(command)
        with patch.object(self.installer, "run_codex", side_effect=fail_cleanup):
            receipt = self.failure()
        self.assertEqual(self.cli.source, self.previous)
        self.assertTrue(receipt["rollback"]["source_restored"])
        self.assertEqual(receipt["rollback"]["state"], "recovery_failed")
        self.assertIn("original plugin-add error", receipt["error"])
        self.assertIn("uninstall failed", receipt["rollback"]["errors"][0]["error"])

    def test_first_install_failure_removes_only_this_new_registration(self):
        self.cli.source = self.cli.original = None
        self.cli.plugin = None
        self.cli.failure = "plugin-add"
        receipt = self.failure()
        self.assertIsNone(self.cli.source)
        self.assertIsNone(self.cli.plugin)
        self.assertFalse(receipt["rollback"]["previous_plugin_installed"])
        self.assertEqual(receipt["rollback"]["state"], "new_installation_removed_no_previous_plugin")

    def test_previous_source_without_plugin_does_not_install_old_plugin_on_rollback(self):
        self.cli.plugin = None
        self.cli.failure = "plugin-add"
        receipt = self.failure()
        self.assertEqual(self.cli.source, self.previous)
        self.assertIsNone(self.cli.plugin)
        self.assertEqual(receipt["rollback"]["state"], "previous_source_restored_no_previous_plugin")

    def test_disabled_or_unknown_previous_enabled_state_refuses_before_mutation(self):
        for state in (False, None):
            with self.subTest(enabled=state):
                self.cli.plugin["enabled"] = state
                self.cli.calls.clear()
                with self.assertRaisesRegex(ValueError, "disabled|unknown"):
                    self.install()
                self.assertEqual(len(self.cli.calls), 2)
                self.assertEqual(self.cli.source, self.previous)

    def test_foreign_source_is_preserved_without_native_mutation(self):
        self.cli.source = make_release(self.root / "another-owner", "0.1.0")
        self.cli.plugin = self.cli.record(self.cli.source)
        with self.assertRaisesRegex(ValueError, "another source"):
            self.install()
        self.assertEqual(len(self.cli.calls), 2)

    def test_direct_registration_failure_restores_previous_plugin(self):
        self.cli.failure = "source-add"
        target = make_release(self.prefix / "direct-target")
        with self.assertRaises(self.installer.InstallTransactionError) as raised:
            self.installer.register_marketplace(target, self.prefix, "/codex")
        receipt = raised.exception.receipt
        self.assertEqual(receipt["failed_stage"], "marketplace-register")
        self.assertIn("original new-source error", receipt["error"])
        self.assert_old_restored(receipt)

    def test_receipt_write_failure_also_restores_previous_installation(self):
        original = self.installer.write_install_receipt
        def write(destination, receipt):
            if receipt["installed"]:
                raise OSError("original receipt write error")
            return original(destination, receipt)
        with patch.object(self.installer, "write_install_receipt", side_effect=write):
            receipt = self.failure()
        self.assertEqual(receipt["failed_stage"], "success-receipt-write")
        self.assertIn("original receipt write error", receipt["error"])
        self.assert_old_restored(receipt)

    def test_failure_receipt_write_error_cannot_mask_the_install_error(self):
        self.cli.failure = "plugin-add"
        with patch.object(self.installer, "write_install_receipt", side_effect=OSError("receipt path read-only")):
            with self.assertRaises(self.installer.InstallTransactionError) as raised:
                self.install()
        receipt = raised.exception.receipt
        self.assertIn("original plugin-add error", receipt["error"])
        self.assertEqual(receipt["failure_receipt_error"], "receipt path read-only")
        self.assertNotIn("failure_receipt", receipt)
        self.assert_old_restored(receipt)

    def test_cache_rejects_file_directory_and_root_symlinks(self):
        for kind in ("file", "directory", "root"):
            with self.subTest(kind=kind):
                cache = self.root / ("symlink-cache-" + kind)
                shutil.copytree(self.release / "plugins/zcode-supervisor", cache)
                if kind == "root":
                    location = self.root / "linked-root"
                    location.symlink_to(cache, target_is_directory=True)
                elif kind == "directory":
                    shutil.rmtree(cache / "skills")
                    (cache / "skills").symlink_to(self.release / "plugins/zcode-supervisor/skills", target_is_directory=True)
                    location = cache
                else:
                    (cache / "scripts/czs.py").unlink()
                    (cache / "scripts/czs.py").symlink_to(self.release / "plugins/zcode-supervisor/scripts/czs.py")
                    location = cache
                with self.assertRaisesRegex(ValueError, "[Ll]ink"):
                    self.installer.verify_plugin_cache(self.release, {"installedPath": str(location)})

    def test_finder_metadata_is_allowed_only_as_an_ordinary_file(self):
        cache = self.root / "metadata-cache"
        shutil.copytree(self.release / "plugins/zcode-supervisor", cache)
        metadata = cache / ".DS_Store"
        metadata.write_bytes(b"Finder")
        self.installer.verify_plugin_cache(self.release, {"installedPath": str(cache)})
        metadata.unlink()
        metadata.mkdir()
        with self.assertRaisesRegex(ValueError, "regular file"):
            self.installer.verify_plugin_cache(self.release, {"installedPath": str(cache)})
        metadata.rmdir()
        metadata.symlink_to(cache / "plugin.json")
        with self.assertRaisesRegex(ValueError, "Linked"):
            self.installer.verify_plugin_cache(self.release, {"installedPath": str(cache)})

    def test_extra_empty_cache_directory_also_fails_the_complete_tree_check(self):
        cache = self.root / "extra-directory-cache"
        shutil.copytree(self.release / "plugins/zcode-supervisor", cache)
        (cache / "unexpected-empty").mkdir()
        with self.assertRaisesRegex(ValueError, "unexpected-empty"):
            self.installer.verify_plugin_cache(self.release, {"installedPath": str(cache)})

    def test_failure_receipt_cannot_claim_full_restore_if_enabled_verification_fails(self):
        self.cli.failure = "plugin-add"
        original = self.cli.record
        def record(source, enabled=True):
            return original(source, False if source == self.previous else enabled)
        self.cli.record = record
        receipt = self.failure()
        self.assertIn("original plugin-add error", receipt["error"])
        self.assertEqual(receipt["rollback"]["state"], "recovery_failed")
        self.assertFalse(receipt["rollback"]["plugin_restored"])
        self.assertEqual(receipt["rollback"]["errors"][0]["stage"], "verify-restored-plugin-state")


if __name__ == "__main__":
    unittest.main()
