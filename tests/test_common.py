"""Filesystem publication and real POSIX advisory-lock regressions."""

import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from zcode_supervisor import common


class StateHelpersTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)

    def test_atomic_json_replaces_complete_file_and_uses_private_mode(self):
        output = self.directory / "state" / "result.json"
        common.atomicwrite_json(output, {"result": "first"})
        common.write_json(output, {"result": "second"})
        self.assertEqual(json.loads(output.read_text()), {"result": "second"})
        self.assertEqual(output.stat().st_mode & 0o777, 0o600)
        self.assertEqual(list(output.parent.iterdir()), [output])

    def test_invalid_json_does_not_create_output(self):
        path = self.directory / "invalid.json"
        with self.assertRaises(ValueError):
            common.write_json(path, {"number": float("nan")})
        self.assertFalse(path.exists())

    def test_atomic_publication_failure_preserves_old_file_and_cleans_temporary(self):
        path = self.directory / "result.json"
        common.write_json(path, {"old": True})
        with patch.object(common.os, "replace", side_effect=OSError("simulated filesystem failure")):
            with self.assertRaises(OSError):
                common.write_json(path, {"new": True})
        self.assertEqual(json.loads(path.read_text()), {"old": True})
        self.assertEqual(list(self.directory.iterdir()), [path])

    def test_output_symlink_or_hardlink_cannot_mutate_external_file(self):
        target, output = self.directory / "safe", self.directory / "result.json"
        target.write_text("untouched")
        output.symlink_to(target)
        with self.assertRaisesRegex(ValueError, "symlink output"):
            common.write_json(output, {})
        output.unlink()
        os.link(target, output)
        with self.assertRaisesRegex(ValueError, "single-link output"):
            common.write_json(output, {})
        self.assertEqual(target.read_text(), "untouched")

    def test_workspace_lock_is_exclusive_and_released_after_exception(self):
        work, state = self.directory / "work", self.directory / "state"
        work.mkdir()
        with self.assertRaisesRegex(RuntimeError, "worker failure"):
            with common.workspace_lock(work, state):
                with self.assertRaisesRegex(ValueError, "Another supervisor"):
                    with common.workspace_lock(work, state):
                        self.fail("second holder acquired same lock")
                raise RuntimeError("worker failure")
        with common.workspace_lock(work, state):
            lock = next(state.glob("*.lock"))
            self.assertEqual(json.loads(lock.read_text())["controller_pid"], os.getpid())

    def test_different_workspaces_can_be_locked_independently(self):
        state = self.directory / "state"
        with common.workspace_lock(self.directory / "a", state):
            with common.workspace_lock(self.directory / "b", state):
                self.assertEqual(len(list(state.glob("*.lock"))), 2)

    def test_lock_symlink_or_shared_inode_rejected_without_truncating_target(self):
        work, state = self.directory / "work", self.directory / "state"
        state.mkdir()
        key = hashlib.sha256(str(work.resolve()).encode()).hexdigest()
        lock = state / f"workspace-{key}.lock"
        target = self.directory / "safe"
        target.write_text("untouched")
        lock.symlink_to(target)
        with self.assertRaises(ValueError):
            with common.workspace_lock(work, state):
                self.fail("symlink lock acquired")
        lock.unlink()
        os.link(target, lock)
        with self.assertRaisesRegex(ValueError, "single-link file"):
            with common.workspace_lock(work, state):
                self.fail("hardlink lock acquired")
        self.assertEqual(target.read_text(), "untouched")

    def test_directory_link_and_world_writable_directory_rejected(self):
        target, link = self.directory / "target", self.directory / "link"
        target.mkdir()
        link.symlink_to(target, target_is_directory=True)
        with self.assertRaisesRegex(ValueError, "symlink directory"):
            common.ensure_directory(link)
        target.chmod(0o777)
        with self.assertRaisesRegex(ValueError, "group/world writable"):
            common.ensure_directory(target)

    def test_process_pid_requires_positive_nonboolean_integer(self):
        for pid in (None, False, True, 0, -1, "10", 1.5):
            with self.subTest(pid=pid), self.assertRaises(ValueError):
                common.process_alive(pid)
        self.assertTrue(common.process_alive(os.getpid()))

    def test_process_missing_or_permission_denied(self):
        with patch.object(common.os, "kill", side_effect=ProcessLookupError):
            self.assertFalse(common.process_alive(42))
        with patch.object(common.os, "kill", side_effect=PermissionError):
            self.assertTrue(common.process_alive(42))

    def test_unsupported_platform_fails_explicitly(self):
        with patch.object(common.os, "name", "nt"):
            with self.assertRaisesRegex(ValueError, "POSIX"):
                common.process_alive(42)


if __name__ == "__main__":
    unittest.main()
