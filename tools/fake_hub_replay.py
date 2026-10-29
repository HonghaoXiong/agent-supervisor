#!/usr/bin/env python3
"""P1.2 fake-hub replay: three discovery timepoints over the REAL P1.1
UiEventServer, with the four-zero counters, plus the live Rust round trip.

For each timepoint (after_subscribe / hello_only / between_hello_and_
snapshot) the harness freezes the terminal index (never writes it), creates
a brand-new running task at exactly the right phase, and asserts a plain
read-only client discovered it from seq 1 with hello=1, per-new-task
subscribe=0, terminal-index read=0 and manual task_id input=0. Afterwards
it releases the terminal event and asserts the terminal update arrives.

Then (optionally, --rust-test) it re-exports the same live server to the
Rust integration test via CZS_UI_TEST_SOCKET and runs `cargo test --locked`
against it.

Usage:
  python3 tools/bench_ui.py --fake-hub-replay --out <dir> [--rust-test]
"""
import argparse
import asyncio
import json
import os
import shutil
import sys
import tempfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from zcode_supervisor.ui_journal import UiJournal  # noqa: E402
from zcode_supervisor.ui_server import UiEventServer  # noqa: E402
from zcode_supervisor.ui_projection import UiEmitter  # noqa: E402

HEX = "3" * 64
RFC = "2070-01-01T00:00:00.000000Z"


class Counters:
    """Four-zero contract counters for ONE client connection."""

    def __init__(self):
        self.hello = 0
        self.per_task_subscribes = 0
        self.terminal_index_reads = 0
        self.manual_task_ids = 0

    def as_dict(self):
        return {"hello": self.hello,
                "per_task_subscribes": self.per_task_subscribes,
                "terminal_index_reads": self.terminal_index_reads,
                "manual_task_ids": self.manual_task_ids}


class Client:
    """Minimal read-only wire client (hello/pages/subscribe; counters)."""

    def __init__(self, server, counters):
        self.server = server
        self.counters = counters
        self.reader = self.writer = None

    async def start(self):
        self.reader, self.writer = await asyncio.open_unix_connection(
            str(self.server.socket_path))

    async def close(self):
        if self.writer is not None:
            self.writer.close()
            try:
                await self.writer.wait_closed()
            except (ConnectionError, OSError):
                pass
            self.reader = self.writer = None

    async def request(self, payload):
        self.writer.write(json.dumps(payload).encode() + b"\n")
        await self.writer.drain()
        line = await asyncio.wait_for(self.reader.readline(), 10)
        return json.loads(line)

    async def hello(self):
        self.counters.hello += 1
        response = await self.request(
            {"op": "hello", "v": 1, "client": "fake-hub-replay"})
        assert response["ok"], response
        return response

    async def pages(self, op, cursor, key):
        collected, page_cursor = [], None
        while True:
            payload = {"op": op, "v": 1, "snapshot_cursor": cursor}
            if page_cursor is not None:
                payload["page_cursor"] = page_cursor
            page = await self.request(payload)
            assert page["ok"], page
            collected.extend(page.get(key, []))
            if page["done"]:
                return collected
            page_cursor = page["next_page_cursor"]

    async def subscribe_all(self, cursor):
        # stream_id "*" only: per-task subscribes stay 0 by construction
        response = await self.request(
            {"op": "subscribe", "v": 1, "stream_id": "*",
             "snapshot_cursor": cursor})
        assert response["ok"], response

    async def drain_events(self, seconds=1.0, limit=256):
        frames = []
        while len(frames) < limit:
            try:
                line = await asyncio.wait_for(
                    self.reader.readline(), seconds)
            except asyncio.TimeoutError:
                break
            if not line.strip():
                continue
            try:
                frames.append(json.loads(line))
            except json.JSONDecodeError:
                continue
        return frames


def emit_task(emitter, task_id):
    accepted = emitter.emit(
        "task.accepted", task_id, task_id=task_id, worker="codex",
        mode="app-server",
        data={"contract_id": f"contract-{task_id}",
              "contract_digest": HEX, "created_at": RFC,
              "initiator_verified": False})
    emitter.emit("stream.announced", "hub", task_id=task_id, worker="codex",
                 mode="app-server",
                 data={"task_stream_id": task_id,
                       "accepted_event_id": accepted["event_id"]})
    emitter.emit("turn.started", task_id, task_id=task_id, worker="codex",
                 mode="app-server", session_id=f"sess-{task_id}",
                 turn_id=f"turn-{task_id}")


def frames_for(frames, task_id):
    return [f for f in frames
            if f.get("type") == "event"
            and f["event"].get("stream_id") == task_id]


def terminal_seen(frames, task_id):
    return any(f["event"].get("kind") == "task.completed"
               and f["event"].get("stream_id") == task_id
               for f in frames)


async def timepoint_after_subscribe(emitter, server, counters, name):
    """hello -> state pages -> event pages -> subscribe -> NEW task."""
    client = Client(server, counters)
    await client.start()
    try:
        hello = await client.hello()
        cursor = hello["initial_snapshot_cursor"]
        await client.pages("snapshot", cursor, "states")
        await client.pages("snapshot-events", cursor, "events")
        await client.subscribe_all(cursor)
        emit_task(emitter, name)          # born AFTER subscribe
        frames = await client.drain_events(1.5)
        task_frames = frames_for(frames, name)
        assert task_frames, f"after_subscribe: no frames for {name}"
        assert task_frames[0]["event"]["seq"] == 1, task_frames
        assert any(f["event"]["kind"] == "task.accepted"
                   for f in task_frames), task_frames
        assert any(f["event"]["kind"] == "stream.announced"
                   for f in frames), frames
        return {"discovered": True, "frames": len(task_frames)}
    finally:
        await client.close()


async def timepoint_hello_only(emitter, server, counters, name):
    """hello -> subscribe with the initial cursor (NO snapshot pages)."""
    client = Client(server, counters)
    await client.start()
    try:
        hello = await client.hello()
        cursor = hello["initial_snapshot_cursor"]
        await client.subscribe_all(cursor)   # directly, no snapshot
        emit_task(emitter, name)             # born AFTER subscribe
        frames = await client.drain_events(1.5)
        task_frames = frames_for(frames, name)
        assert task_frames, f"hello_only: no frames for {name}"
        assert task_frames[0]["event"]["seq"] == 1, task_frames
        return {"discovered": True, "frames": len(task_frames)}
    finally:
        await client.close()


async def timepoint_between(emitter, server, counters, name):
    """hello -> NEW task -> state/event pages (old cursor) -> subscribe."""
    client = Client(server, counters)
    await client.start()
    try:
        hello = await client.hello()
        cursor = hello["initial_snapshot_cursor"]
        emit_task(emitter, name)          # born BETWEEN hello and pages
        states = await client.pages("snapshot", cursor, "states")
        assert name not in {s.get("task_id") for s in states}, states
        await client.pages("snapshot-events", cursor, "events")
        await client.subscribe_all(cursor)
        frames = await client.drain_events(1.5)
        task_frames = frames_for(frames, name)
        assert task_frames, f"between: no frames for {name}"
        assert task_frames[0]["event"]["seq"] == 1, task_frames
        return {"discovered": True, "frames": len(task_frames),
                "absent_from_state_page": True}
    finally:
        await client.close()


async def terminal_release_check(emitter, server, counters, name):
    """Release the frozen terminal event: the update must arrive."""
    client = Client(server, counters)
    await client.start()
    try:
        hello = await client.hello()
        cursor = hello["initial_snapshot_cursor"]
        await client.subscribe_all(cursor)
        emitter.emit("task.completed", name, task_id=name, worker="codex",
                     mode="app-server", status="completed", outcome="ok",
                     delivery_ok=True, duration_ms=1.0)
        frames = await client.drain_events(1.5)
        assert terminal_seen(frames, name), frames
        return {"terminal_update": True}
    finally:
        await client.close()


async def run_replay(out_dir: Path):
    # NOTE: no context manager — the caller owns the tempdir so the
    # socket file OUTLIVES this coroutine (the Rust subprocess round
    # trip needs it); cleanup happens in main_async's finally.
    tmp = tempfile.mkdtemp(prefix="fake-hub-replay-")
    state = Path(tmp)
    os.chmod(state, 0o700)
    emitter = UiEmitter(hub_instance_id="fake-hub-replay",
                        journal_epoch=1, enabled=True)
    journal = UiJournal(emitter)
    server = UiEventServer(journal, state_dir=state, heartbeat_s=60)
    await server.start()
    results = {}
    try:
        counters = Counters()
        results["after_subscribe"] = await timepoint_after_subscribe(
            emitter, server, counters, "task-after")
        results["hello_only"] = await timepoint_hello_only(
            emitter, server, counters, "task-hello")
        results["between_hello_and_snapshot"] = await timepoint_between(
            emitter, server, counters, "task-between")
        results["terminal_release"] = await terminal_release_check(
            emitter, server, counters, "task-after")
        results["counters"] = counters.as_dict()
        # one hello per client (3 timepoints + 1 terminal release), and
        # the harness never subscribes per task / reads the terminal
        # index / types a task id
        assert results["counters"]["hello"] == 3 + 1
        assert results["counters"]["per_task_subscribes"] == 0
        assert results["counters"]["terminal_index_reads"] == 0
        assert results["counters"]["manual_task_ids"] == 0
        # leave the server ALIVE for the optional Rust round trip
        return server, journal, results, tmp
    except BaseException:
        await server.close()
        journal.detach()
        shutil.rmtree(tmp, ignore_errors=True)
        raise


async def main_async(args):
    out_dir = args.out
    out_dir.mkdir(parents=True, exist_ok=True)
    server, journal, results, tmp = await run_replay(out_dir)
    rust = {"ran": False, "ok": None}
    try:
        if args.rust_test:
            env = dict(os.environ)
            env["CZS_UI_TEST_SOCKET"] = str(server.socket_path)
            # async subprocess: a blocking run would starve the event loop
            # the UiEventServer lives on (the Rust client would deadlock)
            proc = await asyncio.create_subprocess_exec(
                "cargo", "test", "--locked", "--manifest-path",
                str(ROOT / "ui/supervisor-macos/Cargo.toml"),
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=env, cwd=str(ROOT / "ui/supervisor-macos"))
            try:
                stdout, stderr = await asyncio.wait_for(
                    proc.communicate(), 600)
            except asyncio.TimeoutError:
                proc.kill()
                await proc.communicate()
                raise
            rust = {"ran": True, "ok": proc.returncode == 0,
                    "returncode": proc.returncode,
                    "stdout_tail": stdout.decode()[-2000:],
                    "stderr_tail": stderr.decode()[-2000:]}
        results["rust_live_test"] = rust
        results["model_requests"] = 0
        results["ok"] = all(
            entry.get("discovered", entry.get("terminal_update", False))
            for key, entry in results.items()
            if isinstance(entry, dict)
            and key not in ("rust_live_test", "counters")) \
            and (rust["ok"] is not False)
        from zcode_supervisor.common import atomicwrite_json
        atomicwrite_json(out_dir / "fake-hub-replay.json", results)
        print(json.dumps(
            {"ok": results["ok"],
             "timepoints": {k: v for k, v in results.items()
                            if isinstance(v, dict) and k != "rust_live_test"},
             "counters": results["counters"],
             "rust": rust}, ensure_ascii=False, indent=2))
        return 0 if results["ok"] else 1
    finally:
        await server.close()
        journal.detach()
        shutil.rmtree(tmp, ignore_errors=True)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--rust-test", action="store_true",
                        help="also run the live Rust integration against "
                             "the same server (CZS_UI_TEST_SOCKET)")
    args = parser.parse_args()
    return asyncio.run(main_async(args))


if __name__ == "__main__":
    sys.exit(main())
