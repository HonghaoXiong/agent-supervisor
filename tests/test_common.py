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


class StreamedHashTests(unittest.TestCase):
    """P2.1 chunked SHA-256: same admission/error contract as
    read_regular, bit-identical digests, bounded per-file buffer."""

    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.directory = Path(temporary.name)

    def patterned(self, size: int) -> bytes:
        return bytes(range(256)) * (size // 256) + bytes(size % 256)

    def test_digest_matches_read_regular_across_chunk_boundaries(self):
        chunk = common.HASH_CHUNK_BYTES
        for size in (0, 1, 255, 256, 1024, chunk - 1, chunk, chunk + 1,
                     2 * chunk, 3 * chunk + 7):
            with self.subTest(size=size):
                path = self.directory / f"s{size}.bin"
                payload = self.patterned(size)
                path.write_bytes(payload)
                self.assertEqual(common.sha256_regular(path),
                                 hashlib.sha256(payload).hexdigest())
                self.assertEqual(common.sha256_regular(path),
                                 hashlib.sha256(common.read_regular(path)).hexdigest())

    def test_custom_chunk_size_same_digest(self):
        path = self.directory / "custom.bin"
        payload = self.patterned(100_003)
        path.write_bytes(payload)
        self.assertEqual(common.sha256_regular(path, chunk_bytes=1024),
                         hashlib.sha256(payload).hexdigest())

    def test_error_types_and_messages_match_read_regular(self):
        real = self.directory / "real"
        real.write_text("content")
        link = self.directory / "link"
        link.symlink_to(real)
        shared = self.directory / "shared"
        os.link(real, shared)
        fifo = self.directory / "fifo"
        os.mkfifo(fifo)
        missing = self.directory / "missing"
        # a directory is refused with the identical message too (on macOS
        # the O_NONBLOCK open itself fails with EISDIR, so the OSError
        # wrapper fires before the fstat admission check)
        directory = self.directory / "dir"
        directory.mkdir()
        for path in (link, shared, fifo, missing, directory):
            with self.subTest(path=path.name):
                with self.assertRaises(ValueError) as read_error:
                    common.read_regular(path)
                with self.assertRaises(ValueError) as hash_error:
                    common.sha256_regular(path)
                self.assertEqual(str(hash_error.exception),
                                 str(read_error.exception))

    def test_directory_input_leaks_no_descriptors(self):
        """F-1 regression: on macOS os.open(dir, O_NONBLOCK) succeeds and
        os.fdopen then raises IsADirectoryError BEFORE the with-statement
        owns the fd — both readers must close it anyway and keep the
        ValueError contract."""
        def open_fds():
            return len(os.listdir("/dev/fd"))

        directory = self.directory / "adir"
        directory.mkdir()
        import gc
        for func in (common.read_regular, common.sha256_regular):
            with self.subTest(func=func.__name__):
                before = open_fds()
                messages = set()
                for _ in range(50):
                    with self.assertRaises(ValueError) as caught:
                        func(directory)
                    messages.add(str(caught.exception))
                gc.collect()
                self.assertEqual(open_fds() - before, 0)
                self.assertEqual(
                    messages,
                    {f"Cannot read ordinary file {directory}: Is a directory"})

    def test_unreadable_file_leaks_no_descriptors(self):
        """Permission-denied open must not leak either (the fdopen gap is
        unreachable there, but the error path is held to the same bar)."""
        if os.geteuid() == 0:
            self.skipTest("root ignores permission bits")
        def open_fds():
            return len(os.listdir("/dev/fd"))

        path = self.directory / "locked.bin"
        path.write_bytes(b"x")
        path.chmod(0o000)
        self.addCleanup(path.chmod, 0o600)
        import gc
        for func in (common.read_regular, common.sha256_regular):
            with self.subTest(func=func.__name__):
                before = open_fds()
                for _ in range(50):
                    with self.assertRaises(ValueError):
                        func(path)
                gc.collect()
                self.assertEqual(open_fds() - before, 0)

    def test_invalid_chunk_size_rejected(self):
        path = self.directory / "small.bin"
        path.write_bytes(b"x")
        for bad in (0, -1, True, 1.5, "1024",
                    common.HASH_CHUNK_BYTES + 1, common.HASH_CHUNK_BYTES * 1024):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                common.sha256_regular(path, chunk_bytes=bad)

    def test_peak_memory_stays_at_chunk_scale_not_file_scale(self):
        """A 8 MiB file must hash with well under 8 MiB of live allocations
        (read_regular's whole-file buffer would exceed the bound)."""
        import tracemalloc
        chunk = common.HASH_CHUNK_BYTES
        size = 8 * chunk
        path = self.directory / "big.bin"
        path.write_bytes(self.patterned(size))
        tracemalloc.start()
        try:
            common.sha256_regular(path)
            _, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
        self.assertLess(peak, 4 * chunk)


class BoundaryScanExecutorTests(unittest.TestCase):
    """P2.1: run-boundary scans ride one bounded off-loop executor."""

    def test_executor_is_shared_and_bounded(self):
        executor = common.boundary_scan_executor()
        self.assertIs(executor, common.boundary_scan_executor())
        self.assertEqual(executor._max_workers, common.BOUNDARY_SCAN_WORKERS)

    def test_scan_runs_off_the_event_loop_thread_and_returns_result(self):
        import asyncio
        import threading

        def probe(value):
            return value, threading.current_thread() is threading.main_thread()

        async def main():
            return await common.run_boundary_scan(probe, 41)

        value, ran_on_main = asyncio.run(main())
        self.assertEqual(value, 41)
        self.assertFalse(ran_on_main)

    def test_cancelling_the_await_never_kills_the_running_scan(self):
        import asyncio
        import threading

        started = threading.Event()
        finished = threading.Event()

        def slow_scan():
            started.set()
            for _ in range(200):
                if finished.wait(0.005):
                    return "scan-settled"
            return "scan-settled"

        async def main():
            task = asyncio.get_running_loop().create_task(
                common.run_boundary_scan(slow_scan))
            await asyncio.get_running_loop().run_in_executor(None, started.wait, 2)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass  # the await is gone; the scan thread keeps running
            for _ in range(400):
                if finished.is_set():
                    break
                await asyncio.sleep(0.005)
            return None

        async def settle():
            await asyncio.sleep(0.02)
            finished.set()

        async def both():
            await asyncio.gather(main(), settle())

        asyncio.run(both())
        self.assertTrue(started.is_set())


if __name__ == "__main__":
    unittest.main()
