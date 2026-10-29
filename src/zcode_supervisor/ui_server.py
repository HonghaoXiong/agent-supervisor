"""Read-only local UI event server (P1.1, PLAN §4.1/§4.5).

One owner-only Unix domain socket carrying newline-delimited JSON. The
protocol surface is exactly ``hello`` / ``snapshot`` / ``snapshot-events`` /
``subscribe`` plus heartbeat responses — nothing else: no delegate, no
approval replies, no tools, no shell, no path reads, no write-back channel.

Security posture (fail-closed, no fallback):

* the socket lives at ``<state_dir>/ui-events.sock``; the parent directory
  must be a real (non-symlink) directory, owned by the current user, mode
  0700 (no group/world access, no group/world write);
* over-long paths fail BEFORE bind; bind/listen failures surface as
  ``transport_unavailable`` — never a temp-directory socket, TCP listener,
  file polling or PATH fallback of any kind;
* the bound socket is chmod 0600; every accepted peer is verified as the
  same user via macOS ``getpeereid`` (a failed or mismatched check closes
  the connection immediately);
* a stale socket inode owned by us is replaced; anything else at the path
  (symlink, regular file, foreign socket) is refused.

Slow consumers are isolated per connection: the journal's bounded queue is
drained by a dedicated writer task; overflow sends one ``reset_required``
marker and closes that connection only. UI failures never touch the hub's
delegate/approval/evidence paths — the journal is just a subscriber of the
one shared projection emitter.
"""

from __future__ import annotations

import asyncio
import contextlib
import ctypes
import ctypes.util
import errno
import json
import os
import socket
import stat
import uuid
from pathlib import Path

from .ui_events import HUB_STREAM_ID  # noqa: F401 - protocol documentation
from .ui_journal import UiJournal, _SubscriptionError
from .ui_projection import _env_enabled

__all__ = ["UiEventServer", "UiTransportError", "start_ui_service",
           "SOCKET_NAME", "peer_euid"]

SOCKET_NAME = "ui-events.sock"
#: macOS ``struct sockaddr_un::sun_path`` limit — checked before bind.
UNIX_PATH_MAX = 104
#: One inbound NDJSON line may never exceed this (bounded parse).
MAX_LINE_BYTES = 256 * 1024
PROTOCOL_VERSION = 1
_ALLOWED_OPS = frozenset(
    ("hello", "snapshot", "snapshot-events", "subscribe"))
#: strict per-op request fields (PLAN §4.1: 越权字段 fail-closed)
_OP_FIELDS = {
    "hello": frozenset(("op", "v", "client")),
    "snapshot": frozenset(("op", "v", "snapshot_cursor", "page_cursor")),
    "snapshot-events": frozenset(("op", "v", "snapshot_cursor",
                                  "page_cursor")),
    "subscribe": frozenset(("op", "v", "stream_id", "snapshot_cursor",
                            "after_seq", "filters")),
}

_libc = None


def _euid() -> int:
    return os.geteuid()


def peer_euid(sock) -> int | None:
    """The verified peer uid on a connected AF_UNIX socket (macOS), or None.

    Uses ``getpeereid``; any failure is treated as unverifiable (fail
    closed by the caller).
    """
    global _libc
    if _libc is None:
        _libc = ctypes.CDLL(ctypes.util.find_library("c") or "libc.dylib",
                            use_errno=True)
    try:
        uid = ctypes.c_uint32()
        gid = ctypes.c_uint32()
        result = _libc.getpeereid(sock.fileno(), ctypes.byref(uid),
                                  ctypes.byref(gid))
        return uid.value if result == 0 else None
    except Exception:  # noqa: BLE001 - unverifiable peer fails closed
        return None


class UiTransportError(Exception):
    """The UDS transport cannot be established; UI shows unavailable."""

    def __init__(self, detail: str):
        super().__init__(detail)
        self.code = "transport_unavailable"
        self.detail = detail


class _Connection:
    """One accepted client: bounded line reader + bounded queue writer."""

    def __init__(self, server: "UiEventServer", reader, writer):
        self.server = server
        self.journal = server.journal
        self.reader = reader
        self.writer = writer
        self.client_id = uuid.uuid4().hex[:12]
        # optional diagnostic label from hello's "client" field (used by
        # tests/bench to build deterministic per-connection write gates)
        self.label = None
        self.subscription = None
        self.writer_task = None
        self.heartbeat_task = None
        self.closed = False
        self.frames_sent = 0
        self.frames_dropped = 0
        # ONE serialized wire writer per connection: request responses, the
        # subscription pump and heartbeats all funnel through _write_frames
        # (concurrent write/drain on a StreamWriter is illegal and used to
        # kill healthy connections via RuntimeError)
        self._wire_lock = asyncio.Lock()

    async def _write_frames(self, frames) -> bool:
        """One serialized, bounded write; False means close the connection."""
        if self.closed or not frames:
            return not self.closed
        blob = "".join(
            json.dumps(frame, ensure_ascii=False) + "\n" for frame in frames)
        try:
            async with self._wire_lock:
                self.writer.write(blob.encode())
                await asyncio.wait_for(self.writer.drain(), 5.0)
        except (asyncio.TimeoutError, ConnectionError, OSError, RuntimeError):
            return False  # bounded close: transport buffer must not grow
        self.frames_sent += len(frames)
        return True

    # -- lifecycle -----------------------------------------------------------
    async def run(self) -> None:
        sock = self.writer.get_extra_info("socket")
        if sock is None:
            # unverifiable peer: fail closed, not open
            self.writer.close()
            return
        uid = peer_euid(sock)
        if uid is None or uid != _euid():
            # unverifiable or foreign peer: close without a byte out
            self.writer.close()
            return
        try:
            while not self.closed:
                try:
                    line = await self.reader.readline()
                except (asyncio.LimitOverrunError, ValueError):
                    # a line past the bounded reader limit: explicit refusal
                    # and a bounded close (never an unbounded drain)
                    await self._send({"ok": False,
                                      "error": "invalid_request",
                                      "detail": "oversized frame"})
                    break
                if not line:
                    break
                if len(line) > MAX_LINE_BYTES:
                    await self._send({"ok": False,
                                      "error": "invalid_request",
                                      "detail": "oversized frame"})
                    continue
                text = line.strip()
                if not text:
                    continue  # blank keepalive line
                try:
                    payload = json.loads(text)
                except (json.JSONDecodeError, UnicodeDecodeError):
                    continue  # malformed line: skipped, connection lives
                if not isinstance(payload, dict):
                    continue
                await self._handle(payload)
        except (ConnectionError, asyncio.IncompleteReadError, OSError):
            pass  # peer disconnect: cleanup below
        finally:
            await self._cleanup()

    async def _cleanup(self) -> None:
        self.closed = True
        if self.subscription is not None:
            self.journal.close_subscription(self.subscription)
            self.subscription = None
        # repair2 B/F-06: the wire client id dies with this connection —
        # its unfinished pagination slot must not leak
        release = getattr(self.journal, "release_pages", None)
        if callable(release):
            release(self.client_id)
        for task in (self.writer_task, self.heartbeat_task):
            if task is not None and not task.done():
                task.cancel()
                with contextlib.suppress(asyncio.CancelledError):
                    await task
        self.writer.close()
        with contextlib.suppress(ConnectionError, OSError):
            await self.writer.wait_closed()

    # -- request dispatch ------------------------------------------------------
    async def _handle(self, payload: dict) -> None:
        op = payload.get("op")
        version = payload.get("v")
        if op not in _ALLOWED_OPS:
            reason = ("forbidden" if isinstance(op, str) and op
                      else "invalid_request")
            await self._send({"ok": False, "error": reason})
            return
        if version is not None and version != PROTOCOL_VERSION:
            # only an explicitly WRONG version is refused; hello carries the
            # negotiated version and later ops may omit it
            await self._send({"ok": False, "error": "unsupported_version"})
            return
        extra = set(payload) - _OP_FIELDS[op]
        if extra:
            await self._send({"ok": False, "error": "invalid_request",
                              "detail": f"unexpected fields: "
                                        f"{','.join(sorted(extra))}"})
            return
        if op == "hello":
            label = payload.get("client")
            if isinstance(label, str) and label:
                self.label = label[:64]
            hello = self.journal.hello()
            hello["ok"] = True
            hello["v"] = PROTOCOL_VERSION
            await self._send(hello)
        elif op == "snapshot":
            await self._send(self.journal.snapshot(
                payload.get("snapshot_cursor"),
                payload.get("page_cursor"),
                client=self.client_id))
        elif op == "snapshot-events":
            cursor = payload.get("snapshot_cursor")
            if cursor is None:
                await self._send({"ok": False,
                                  "error": "invalid_request",
                                  "detail": "snapshot_cursor required"})
                return
            await self._send(self.journal.snapshot_events(
                cursor, payload.get("page_cursor"),
                client=self.client_id))
        elif op == "subscribe":
            if self.subscription is not None:
                await self._send({"ok": False, "error": "invalid_request",
                                  "detail": "already subscribed"})
                return
            stream_id = payload.get("stream_id")
            if stream_id == "*":
                cursor = payload.get("snapshot_cursor")
                if not isinstance(cursor, dict):
                    # subscribe-all REQUIRES a cut-point cursor (hello's or a
                    # snapshot's); latest/no-cursor is never accepted
                    await self._send({"ok": False,
                                      "error": "invalid_request",
                                      "detail": "snapshot_cursor required"})
                    return
                try:
                    self.subscription = \
                        self.journal.subscribe_all(cursor)
                except _SubscriptionError as error:
                    await self._send(error.error)
                    return
                await self._send({"ok": True, "v": PROTOCOL_VERSION,
                                  "subscribed": "*",
                                  "snapshot_cursor": cursor})
                self.writer_task = asyncio.create_task(self._pump())
                self.heartbeat_task = asyncio.create_task(self._heartbeat())
            elif isinstance(stream_id, str) and stream_id:
                after_seq = payload.get("after_seq", 0)
                filters = payload.get("filters")
                if filters is not None and not isinstance(filters, dict):
                    await self._send({"ok": False,
                                      "error": "invalid_request",
                                      "detail": "filters must be an object"})
                    return
                try:
                    self.subscription = self.journal.subscribe_stream(
                        stream_id,
                        after_seq if isinstance(after_seq, int) else 0,
                        filters=filters)
                except _SubscriptionError as error:
                    await self._send(error.error)
                    return
                await self._send({"ok": True, "v": PROTOCOL_VERSION,
                                  "subscribed": stream_id})
                self.writer_task = asyncio.create_task(self._pump())
                self.heartbeat_task = asyncio.create_task(self._heartbeat())
            else:
                await self._send({"ok": False, "error": "invalid_request",
                                  "detail": "stream_id required"})

    # -- streaming side ---------------------------------------------------------
    async def _pump(self) -> None:
        """Drain the bounded subscription queue to the wire.

        Wakes on the queue's readiness event (no fixed-interval polling on
        the hot path); every write goes through the serialized bounded wire
        writer; on overflow the final bounded queue (reset marker included)
        is flushed before the controlled close.
        """
        subscription = self.subscription
        gate = getattr(self.server, "write_gate", None)
        try:
            while not self.closed and subscription is not None:
                frames = subscription.drain(64)
                if not frames:
                    await subscription.wait_ready(0.5)
                    continue
                if gate is not None:
                    # deterministic wire-backpressure hook (bench/tests;
                    # production default: no gate) — applies ONLY to the
                    # streaming pump, never to request responses
                    await gate(self)
                if not await self._write_frames(frames):
                    break
                if subscription.closed:
                    rest = subscription.drain(1 << 30)
                    if rest:
                        await self._write_frames(rest)
                    break
        finally:
            if not self.closed:
                self.closed = True
                self.writer.close()

    async def _heartbeat(self) -> None:
        """Protocol-liveness beats only (never process-exit evidence)."""
        interval = self.server.heartbeat_s
        try:
            while not self.closed and interval > 0:
                await asyncio.sleep(interval)
                if self.closed:
                    return
                await self._send({
                    "type": "heartbeat", "v": PROTOCOL_VERSION,
                    "snapshot_version": self.journal.hello_version(),
                    "high_watermarks": self.journal.hub_high_water()})
        except (ConnectionError, OSError):
            pass

    async def _send(self, payload: dict) -> None:
        await self._write_frames([payload])


class UiEventServer:
    """Owns the secure UDS listener and per-connection read loops."""

    def __init__(self, journal: UiJournal, *, socket_path=None,
                 state_dir=None, heartbeat_s: float = 15.0):
        self.journal = journal
        self.heartbeat_s = float(heartbeat_s)
        if socket_path is None:
            if state_dir is None:
                raise ValueError("socket_path or state_dir is required")
            socket_path = Path(state_dir) / SOCKET_NAME
        self.socket_path = Path(socket_path)
        self._server = None
        self._bound_ino = None
        self.connections: set[_Connection] = set()
        #: optional deterministic wire-backpressure hook for bench/tests
        #: (production default: None — never set outside fakes)
        self.write_gate = None

    # -- security checks (all fail-closed, no fallback) ----------------------
    def _validate_parent(self) -> Path:
        parent = self.socket_path.parent
        try:
            info = os.lstat(parent)
        except OSError as error:
            raise UiTransportError(f"state dir unavailable: {error}") \
                from error
        if stat.S_ISLNK(info.st_mode) or not stat.S_ISDIR(info.st_mode):
            raise UiTransportError("socket parent is not a real directory")
        if info.st_uid != _euid():
            raise UiTransportError("socket parent is not owned by this user")
        mode = stat.S_IMODE(info.st_mode)
        if mode != 0o700:
            raise UiTransportError(
                f"socket parent mode {oct(mode)} is not 0700")
        return parent

    def _validate_path_length(self) -> None:
        try:
            encoded = os.fsencode(self.socket_path)
        except (UnicodeEncodeError, ValueError) as error:
            raise UiTransportError(f"socket path unusable: {error}") \
                from error
        if len(encoded) + 1 > UNIX_PATH_MAX:
            raise UiTransportError("socket path too long for bind")

    def _validate_path_free(self) -> None:
        path = self.socket_path
        self._validate_path_length()
        try:
            info = os.lstat(path)
        except FileNotFoundError:
            return
        except OSError as error:
            raise UiTransportError(f"socket path unusable: {error}") \
                from error
        if stat.S_ISLNK(info.st_mode):
            raise UiTransportError("socket path is a symlink")
        if not stat.S_ISSOCK(info.st_mode):
            raise UiTransportError("socket path is not a socket")
        if info.st_uid != _euid():
            raise UiTransportError("socket path is owned by another user")
        # same-owner socket inode: only a CONFIRMED DEAD one (connect gets
        # ECONNREFUSED) may be replaced — a live listener must never be
        # stolen by a second server instance
        probe = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            probe.connect(str(path))
        except OSError as error:
            if error.errno != errno.ECONNREFUSED:
                raise UiTransportError(
                    f"existing socket not replaceable: {error}") from error
        else:
            probe.close()
            raise UiTransportError(
                "socket path already serves another listener")
        finally:
            probe.close()
        try:
            os.unlink(path)
        except OSError as error:
            raise UiTransportError(f"stale socket not removable: {error}") \
                from error

    # -- lifecycle -------------------------------------------------------------
    async def start(self) -> None:
        self._validate_path_length()  # fail before anything else on purpose
        self._validate_parent()
        self._validate_path_free()
        self._bound_ino = None

        async def connected(reader, writer):
            # bound the per-connection transport buffer: a peer that stops
            # reading must back up into the journal's bounded queue (reset
            # semantics), never into an unbounded kernel send buffer
            sock = writer.get_extra_info("socket")
            if sock is not None:
                with contextlib.suppress(OSError):
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF,
                                    16 * 1024)
            connection = _Connection(self, reader, writer)
            self.connections.add(connection)
            try:
                await connection.run()
            finally:
                self.connections.discard(connection)

        try:
            # the STREAMS server (reader/writer callback) with a bounded
            # per-connection line limit matching MAX_LINE_BYTES
            self._server = await asyncio.start_unix_server(
                connected, str(self.socket_path),
                limit=MAX_LINE_BYTES + 2)
        except OSError as error:
            raise UiTransportError(f"bind/listen failed: {error}") \
                from error
        try:
            os.chmod(self.socket_path, 0o600)
            self._bound_ino = os.stat(self.socket_path).st_ino
        except OSError as error:
            await self.close()
            raise UiTransportError(f"socket mode failed: {error}") from error

    async def close(self) -> None:
        if self._server is not None:
            self._server.close()
            await self._server.wait_closed()
            self._server = None
        for connection in list(self.connections):
            await connection._cleanup()  # noqa: SLF001 - owner shutdown
        self.connections.clear()
        # unlink ONLY the inode this server bound (a newer listener may
        # legitimately own the same path now)
        try:
            if self._bound_ino is not None \
                    and os.stat(self.socket_path).st_ino == self._bound_ino:
                os.unlink(self.socket_path)
        except OSError:
            pass

    # -- diagnostics -------------------------------------------------------------
    def stats(self) -> dict:
        return {"connections": len(self.connections),
                "socket": str(self.socket_path)}


async def start_ui_service(*, state_dir, emitter=None, journal=None,
                           heartbeat_s: float = 15.0):
    """Wire journal+server onto the ONE shared projection emitter.

    Enabled only by an explicit ``CZS_UI_EVENTS`` flag (default off). Any
    setup failure returns ``None`` — the UI surface stays unavailable and
    the hub's control plane is untouched. There is never a fallback
    transport.
    """
    if not _env_enabled():
        return None
    from . import ui_projection
    if emitter is None:
        emitter = ui_projection.default_emitter()
    attached_journal = journal is None
    if journal is None:
        journal = UiJournal(emitter)
    server = UiEventServer(journal, state_dir=state_dir,
                           heartbeat_s=heartbeat_s)
    try:
        await server.start()
    except UiTransportError:
        # repair2 D/F-08: a failed attempt leaves NOTHING behind — a
        # journal this attempt attached is detached, the emitter's enabled
        # state is untouched (it is only aligned on success), and repeated
        # failures cannot accumulate subscribers
        if attached_journal:
            journal.detach()
        return None
    if not emitter.enabled:
        # the explicit flag governs BOTH the projection emit and the UI
        # surface; the shared emitter is aligned only once the service is
        # actually up
        emitter.enabled = True
    return server
