"""Small POSIX state-management helpers; no ZCode or credential dependencies."""

from __future__ import annotations

import asyncio
import concurrent.futures
import hashlib
import json
import os
import stat
import tempfile
from contextlib import contextmanager
from pathlib import Path


def require_posix() -> None:
    if os.name != "posix":
        raise ValueError("This supervisor currently requires macOS or Linux (POSIX)")


def process_alive(pid: int) -> bool:
    """Probe one PID without signaling it; PID reuse is conservatively alive."""
    require_posix()
    if not isinstance(pid, int) or isinstance(pid, bool) or pid <= 0:
        raise ValueError("A positive OS process ID is required")
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def ensure_directory(path: Path) -> None:
    """Create a controller directory, refusing links in its missing path chain.

    Existing filesystem ancestors such as macOS /var may be system symlinks;
    the requested directory itself must be a real, privately owned directory.
    These checks are not protection against a hostile process with the same UID.
    """
    require_posix()
    path = Path(path)
    if path.is_symlink():
        raise ValueError(f"Refusing symlink directory: {path}")
    if not path.exists():
        ensure_directory(path.parent)
        try:
            path.mkdir(mode=0o700)
        except FileExistsError:
            pass
    info = path.lstat()
    if not stat.S_ISDIR(info.st_mode):
        raise ValueError(f"Expected an ordinary directory: {path}")
    if info.st_uid != os.getuid():
        raise ValueError(f"Controller directory is not owned by the current user: {path}")
    if stat.S_IMODE(info.st_mode) & 0o022:
        raise ValueError(f"Controller directory must not be group/world writable: {path}")


def read_regular(path: Path) -> bytes:
    """Read a single-link regular file, refusing a symlink or blocking FIFO."""
    require_posix()
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except OSError as error:
        raise ValueError(f"Cannot read ordinary file {path}: {error.strerror}") from error
    # F-1 (P2.2 pre-fix): os.fdopen itself can raise before the with
    # statement takes ownership (e.g. IsADirectoryError on macOS, where
    # opening a directory with O_NONBLOCK succeeds) — the raw fd must
    # still be closed, or every call leaks one descriptor.
    try:
        stream = os.fdopen(fd, "rb")
    except OSError as error:
        os.close(fd)
        raise ValueError(f"Cannot read ordinary file {path}: {error.strerror}") from error
    except BaseException:
        os.close(fd)
        raise
    try:
        with stream:
            info = os.fstat(stream.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                raise ValueError(f"Expected an ordinary single-link file: {path}")
            return stream.read()
    except OSError as error:
        raise ValueError(f"Cannot read ordinary file {path}: {error.strerror}") from error


# P2.1 (PLAN §P2.1): upper bound for one file's in-memory read buffer on
# the hashing path; the workspace-level 256 MiB / 5000-file inventory
# caps are enforced by the caller and are unchanged by streamed hashing.
HASH_CHUNK_BYTES = 1024 * 1024


def sha256_regular(path: Path, chunk_bytes: int = HASH_CHUNK_BYTES) -> str:
    """Streamed SHA-256 of one ordinary single-link file.

    Same admission checks and error contract as :func:`read_regular`
    (``O_NOFOLLOW`` open, ``fstat`` regular + single-link, OSError wrapped
    in ValueError with the identical message shape), but content is
    consumed in ``chunk_bytes`` pieces so peak per-file memory stays at
    one chunk instead of the whole file. The digest is bit-identical to
    ``hashlib.sha256(read_regular(path)).hexdigest()`` by construction.
    """
    require_posix()
    if isinstance(chunk_bytes, bool) or not isinstance(chunk_bytes, int) \
            or chunk_bytes <= 0:
        raise ValueError("chunk_bytes must be a positive integer")
    if chunk_bytes > HASH_CHUNK_BYTES:
        # F-7 (P2.2 pre-fix): BufferedReader.read(n) preallocates ~n, so
        # an unbounded chunk would break the <=1 MiB single-file read
        # buffer ceiling the P2.1 acceptance froze.
        raise ValueError("chunk_bytes must not exceed HASH_CHUNK_BYTES")
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    except OSError as error:
        raise ValueError(f"Cannot read ordinary file {path}: {error.strerror}") from error
    try:
        stream = os.fdopen(fd, "rb")
    except OSError as error:
        os.close(fd)  # F-1: same ownership gap as read_regular
        raise ValueError(f"Cannot read ordinary file {path}: {error.strerror}") from error
    except BaseException:
        os.close(fd)
        raise
    try:
        with stream:
            info = os.fstat(stream.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                raise ValueError(f"Expected an ordinary single-link file: {path}")
            digest = hashlib.sha256()
            while True:
                chunk = stream.read(chunk_bytes)
                if not chunk:
                    break
                digest.update(chunk)
            return digest.hexdigest()
    except OSError as error:
        raise ValueError(f"Cannot read ordinary file {path}: {error.strerror}") from error


def atomic_write(path: Path, data: bytes) -> None:
    """Publish bytes atomically without following an existing output link."""
    path = Path(path)
    ensure_directory(path.parent)
    if path.is_symlink():
        raise ValueError(f"Refusing symlink output: {path}")
    if path.exists():
        info = path.lstat()
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
            raise ValueError(f"Expected an ordinary single-link output: {path}")
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def atomicwrite_json(path: Path, value: object) -> None:
    """Atomically publish private JSON state, never accepting NaN/Infinity."""
    data = (json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False) + "\n").encode()
    atomic_write(Path(path), data)


# Keep the short historical name for the protocol client.
write_json = atomicwrite_json


# P2.1 (PLAN §P2.1): dedicated bounded executor for RUN-BOUNDARY scans.
# Only the run-boundary inventory/check_frozen scans of the async run
# paths go through this pool so the shared event loop keeps serving
# while a scan hashes; decide()-time frozen checks stay synchronous and
# fresh per call, and the approval path stays synchronous serial.
BOUNDARY_SCAN_WORKERS = 2

_boundary_scan_executor: concurrent.futures.ThreadPoolExecutor | None = None


def boundary_scan_executor() -> concurrent.futures.ThreadPoolExecutor:
    """The process-wide bounded executor for run-boundary scans.

    Lazily created (CLI paths that never scan spawn no threads). Bounds:
    at most ``BOUNDARY_SCAN_WORKERS`` scan threads exist at any time;
    further scans queue (their wait still counts into the caller's wall
    time — the pool only reduces event-loop blocking, it never shortens
    or greens the P0.1 whole-call hard gate).
    """
    global _boundary_scan_executor
    if _boundary_scan_executor is None:
        _boundary_scan_executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=BOUNDARY_SCAN_WORKERS,
            thread_name_prefix='czs-boundary-scan')
    return _boundary_scan_executor


async def run_boundary_scan(func, *args):
    """Await one pure scan function on the bounded boundary-scan executor.

    Cancellation of this await does NOT kill an already-running scan
    thread (the scan runs to completion exactly like the old synchronous
    call, which no cancellation could interrupt either) — the outer
    cancellation is simply re-delivered at the next await point.
    """
    future = boundary_scan_executor().submit(func, *args)
    return await asyncio.wrap_future(future)


@contextmanager
def workspace_lock(workspace: Path, state_dir: Path):
    """Advisory per-workspace lock; the OS releases it after process exit."""
    require_posix()
    import fcntl

    ensure_directory(state_dir)
    canonical_workspace = str(Path(workspace).resolve())
    key = hashlib.sha256(canonical_workspace.encode()).hexdigest()
    path = Path(state_dir) / f"workspace-{key}.lock"
    try:
        fd = os.open(path, os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
    except OSError as error:
        raise ValueError(f"Cannot open workspace lock: {path}") from error
    with os.fdopen(fd, "r+") as stream:
        info = os.fstat(stream.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
            raise ValueError("Workspace lock must be an ordinary single-link file")
        try:
            fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise ValueError("Another supervisor operation owns this workspace") from error
        try:
            stream.seek(0)
            stream.truncate()
            json.dump({"workspace": canonical_workspace, "controller_pid": os.getpid()}, stream)
            stream.flush()
            yield
        finally:
            fcntl.flock(stream, fcntl.LOCK_UN)
