"""Small POSIX state-management helpers; no ZCode or credential dependencies."""

from __future__ import annotations

from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import stat
import tempfile


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
        with os.fdopen(fd, "rb") as stream:
            info = os.fstat(stream.fileno())
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                raise ValueError(f"Expected an ordinary single-link file: {path}")
            return stream.read()
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
