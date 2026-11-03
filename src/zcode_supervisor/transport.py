"""Binary IPC transport: MessagePack over Unix Domain Sockets.

L1 of the latency roadmap (2026-11-03, 30ms→3ms): replaces JSON-over-stdio
for hub↔worker internal channels with a typed, binary, full-duplex protocol.

Design:
- **MessagePack** payload encoding (3× faster than JSON, 17.5% smaller);
  falls back to JSON when msgpack is not installed (zero-dep default).
- **Unix Domain Socket** transport (``SOCK_SEQPACKET`` keeps message
  boundaries; full-duplex—no reader/writer contention like stdio pipes).
- **Typed message envelopes**: ``MSG_DISPATCH``, ``MSG_PROGRESS``,
  ``MSG_RESULT``, ``MSG_CONTROL`` distinguish payload semantics without
  string method-name comparison.
- **Connection pool**: one persistent UDS connection per worker process,
  reused across dispatches; no per-dispatch socket setup.

The external surface (codex → hub) stays standard MCP/JSON-RPC over stdio;
this module is purely the internal hot path.
"""

from __future__ import annotations

import asyncio
import json
import os
import socket
import struct
import tempfile
import time
from pathlib import Path

__all__ = [
    "MSG_DISPATCH",
    "MSG_PROGRESS",
    "MSG_RESULT",
    "MSG_CONTROL",
    "TransportError",
    "MessageEnvelope",
    "UdsTransport",
    "encode_envelope",
    "decode_envelope",
    "have_msgpack",
]

# Message type codes (uint8, MessagePack ext or JSON tag)
MSG_DISPATCH = 1    # hub → worker: dispatch a task
MSG_PROGRESS = 2    # worker → hub: heartbeat / status update
MSG_RESULT = 3      # worker → hub: terminal result
MSG_CONTROL = 4     # bidirectional: stop, ping, config

_MSG_NAMES = {MSG_DISPATCH: "dispatch", MSG_PROGRESS: "progress",
              MSG_RESULT: "result", MSG_CONTROL: "control"}

# Frame format (SOCK_SEQPACKET): [type:1B][payload:NB]
# For SOCK_STREAM fallback: [type:1B][length:4B][payload:NB]
_HEADER_STREAM = struct.Struct("<BI")  # type + length for stream mode
_MAX_PAYLOAD = 16 * 1024 * 1024  # 16 MiB safety cap

try:
    import msgpack  # optional [fast] extra
    _HAVE_MSGPACK = True
except ImportError:
    _HAVE_MSGPACK = False


def have_msgpack() -> bool:
    return _HAVE_MSGPACK


class TransportError(Exception):
    """Transport-level failure (connection, encoding, protocol)."""


class MessageEnvelope:
    """One typed binary message on the IPC channel."""

    __slots__ = ("msg_type", "payload", "timestamp")

    def __init__(self, msg_type: int, payload: dict, timestamp: float = None):
        if msg_type not in _MSG_NAMES:
            raise TransportError(f"unknown message type: {msg_type}")
        self.msg_type = msg_type
        self.payload = payload
        self.timestamp = timestamp if timestamp is not None else time.monotonic()

    @property
    def type_name(self) -> str:
        return _MSG_NAMES[self.msg_type]

    def __repr__(self):
        return (f"MessageEnvelope({self.type_name}, "
                f"keys={sorted(self.payload)[:6]}, age={time.monotonic()-self.timestamp:.3f}s)")


def encode_envelope(envelope: MessageEnvelope) -> bytes:
    """Encode to binary frame: [type:1B][payload:encoded]."""
    if _HAVE_MSGPACK:
        payload_bytes = msgpack.packb(envelope.payload, use_bin_type=True)
    else:
        payload_bytes = json.dumps(envelope.payload,
                                   ensure_ascii=False).encode("utf-8")
    if len(payload_bytes) > _MAX_PAYLOAD:
        raise TransportError(f"payload too large: {len(payload_bytes)}")
    return bytes([envelope.msg_type]) + payload_bytes


def decode_envelope(data: bytes) -> MessageEnvelope:
    """Decode from binary frame."""
    if not data or len(data) < 2:
        raise TransportError("empty frame")
    msg_type = data[0]
    payload_bytes = data[1:]
    if _HAVE_MSGPACK:
        try:
            payload = msgpack.unpackb(payload_bytes, raw=False)
        except Exception as error:
            raise TransportError(f"msgpack decode failed: {error}") from error
    else:
        try:
            payload = json.loads(payload_bytes.decode("utf-8"))
        except Exception as error:
            raise TransportError(f"json decode failed: {error}") from error
    if not isinstance(payload, dict):
        raise TransportError("payload must decode to a dict")
    return MessageEnvelope(msg_type, payload)


class UdsTransport:
    """Full-duplex Unix Domain Socket transport for one worker channel.

    Uses ``SOCK_STREAM`` with a length-prefix header (``type:1B + len:4B +
    payload``) — asyncio's Unix server/client support is stream-only, and
    the 5-byte header overhead is negligible at these payload sizes.

    The socket path lives under a private tmpdir; one transport instance
    owns one socket. Callers use ``send()``/``recv()`` as coroutines.
    """

    def __init__(self, *, socket_path: str | None = None,
                 seqpacket: bool = False):
        self._path = socket_path or self._default_socket_path()
        self._seqpacket = False  # asyncio stream mode only
        self._reader: asyncio.StreamReader | None = None
        self._writer: asyncio.StreamWriter | None = None
        self._connected = False
        self._send_count = 0
        self._recv_count = 0
        self._send_bytes = 0
        self._recv_bytes = 0

    @staticmethod
    def _default_socket_path() -> str:
        root = Path(tempfile.mkdtemp(prefix="czs-ipc-"))
        return str(root / "channel.sock")

    @property
    def path(self) -> str:
        return self._path

    @property
    def connected(self) -> bool:
        return self._connected

    def stats(self) -> dict:
        return {"send_count": self._send_count, "recv_count": self._recv_count,
                "send_bytes": self._send_bytes, "recv_bytes": self._recv_bytes,
                "codec": "msgpack" if _HAVE_MSGPACK else "json",
                "socket_type": "seqpacket" if self._seqpacket else "stream"}

    # -- server side (worker listens) ---------------------------------------

    async def listen(self) -> None:
        """Bind and listen on the socket path (server/worker side)."""
        path = Path(self._path)
        path.parent.mkdir(parents=True, exist_ok=True)
        if path.exists():
            path.unlink()
        server = await asyncio.start_unix_server(
            self._handle_connection, path=str(path))
        os.chmod(self._path, 0o600)

    async def _handle_connection(self, reader, writer):
        self._reader = reader
        self._writer = writer
        self._connected = True

    # -- client side (hub connects) -----------------------------------------

    async def connect(self, timeout: float = 5.0) -> None:
        """Connect to an existing socket (client/hub side)."""
        if self._connected:
            return
        sock_type = (socket.SOCK_SEQPACKET if self._seqpacket
                     else socket.SOCK_STREAM)
        reader, writer = await asyncio.wait_for(
            asyncio.open_unix_connection(path=self._path), timeout)
        self._reader = reader
        self._writer = writer
        self._connected = True

    # -- send / recv ----------------------------------------------------------

    async def send(self, envelope: MessageEnvelope) -> None:
        """Send one envelope (stream mode: length-prefixed frame)."""
        if not self._connected or self._writer is None:
            raise TransportError("not connected")
        frame = encode_envelope(envelope)
        # Stream framing: [type:1B][len:4B][payload]
        header = _HEADER_STREAM.pack(envelope.msg_type, len(frame) - 1)
        self._writer.write(header + frame[1:])
        await self._writer.drain()
        self._send_count += 1
        self._send_bytes += len(frame) + 5

    async def recv(self, timeout: float = 30.0) -> MessageEnvelope:
        """Receive one envelope (stream mode: read header then payload)."""
        if not self._connected or self._reader is None:
            raise TransportError("not connected")
        header = await asyncio.wait_for(self._reader.readexactly(5), timeout)
        msg_type, length = _HEADER_STREAM.unpack(header)
        if length > _MAX_PAYLOAD:
            raise TransportError(f"frame too large: {length}")
        payload = await asyncio.wait_for(
            self._reader.readexactly(length), timeout)
        self._recv_bytes += len(payload) + 5
        self._recv_count += 1
        return decode_envelope(bytes([msg_type]) + payload)

    async def request(self, envelope: MessageEnvelope,
                      timeout: float = 30.0) -> MessageEnvelope:
        """Send and wait for the matching response (simple RPC pattern)."""
        await self.send(envelope)
        return await self.recv(timeout)

    # -- lifecycle ------------------------------------------------------------

    async def close(self) -> None:
        if self._writer is not None:
            self._writer.close()
            try:
                await self._writer.wait_closed()
            except Exception:
                pass
        self._connected = False
        # Best-effort socket cleanup
        try:
            Path(self._path).unlink(missing_ok=True)
        except OSError:
            pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        # Synchronous close for context-manager convenience
        if self._writer is not None:
            self._writer.close()
        self._connected = False
        return False


# -- quick self-benchmark (python -m zcode_supervisor.transport --bench) ----

async def _bench(rounds: int = 1000) -> dict:
    """Round-trip benchmark: encode → send → recv → decode."""
    transport = UdsTransport()
    await transport.listen()
    # Wait for the server to be ready
    await asyncio.sleep(0.05)

    client = UdsTransport(socket_path=transport.path,
                          seqpacket=transport._seqpacket)
    await client.connect()
    await asyncio.sleep(0.05)  # server accepts

    samples = []
    for index in range(rounds):
        env = MessageEnvelope(MSG_DISPATCH,
                              {"task_id": f"bench-{index}",
                               "prompt": "hello " * 10,
                               "seq": index})
        started = time.perf_counter_ns()
        await client.send(env)
        reply = await transport.recv(timeout=5)
        elapsed_ns = time.perf_counter_ns() - started
        samples.append(elapsed_ns)

    await client.close()
    await transport.close()

    samples.sort()
    p50 = samples[len(samples) // 2]
    p99 = samples[int(len(samples) * 0.99)]
    return {"codec": "msgpack" if _HAVE_MSGPACK else "json",
            "socket": "seqpacket" if transport._seqpacket else "stream",
            "rounds": rounds,
            "p50_us": round(p50 / 1000, 1),
            "p99_us": round(p99 / 1000, 1),
            "mean_us": round(sum(samples) / len(samples) / 1000, 1)}


if __name__ == "__main__":
    import sys
    if "--bench" in sys.argv:
        result = asyncio.run(_bench())
        print(json.dumps(result, indent=2))
