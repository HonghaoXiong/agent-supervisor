#!/usr/bin/env python3
"""Generate the shared Python/Rust wire golden for the supervisor UI.

Starts the REAL P1.1 UiEventServer (frozen core, read-only use) on an
owner-only temp socket, drives one deterministic scenario (baseline task
+ post-cut discovery task + gap stream), and records the exact frames a
Rust client would see: hello, state pages, event pages and the live
subscribe stream. The Rust golden test replays the same JSON through the
reducer and asserts the identical projection.

Usage: python3 tools/gen_wire_golden.py --out <path>
"""
import argparse
import asyncio
import json
import os
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


def emit_task(emitter, task_id):
    accepted = emitter.emit(
        "task.accepted", task_id, task_id=task_id, worker="codex",
        mode="app-server",
        data={"contract_id": f"contract-{task_id}", "contract_digest": HEX,
              "created_at": RFC, "initiator_verified": False})
    emitter.emit("stream.announced", "hub", task_id=task_id, worker="codex",
                 mode="app-server",
                 data={"task_stream_id": task_id,
                       "accepted_event_id": accepted["event_id"]})
    emitter.emit("turn.started", task_id, task_id=task_id, worker="codex",
                 mode="app-server", session_id=f"sess-{task_id}",
                 turn_id=f"turn-{task_id}")


async def scenario_frames(state_dir):
    """Run the deterministic scenario; return the recorded wire frames."""
    emitter = UiEmitter(hub_instance_id="wire-golden", journal_epoch=1,
                        enabled=True)
    journal = UiJournal(emitter, max_event_bytes=2048)
    server = UiEventServer(journal, state_dir=state_dir, heartbeat_s=60)
    await server.start()
    frames = {"socket": str(server.socket_path), "hello": None,
              "state_pages": [], "event_pages": [], "live": []}
    try:
        # baseline task exists BEFORE the cut the client will take
        emit_task(emitter, "task-base")
        # gap stream: retired excluded stream with a real loss
        emitter.emit("task.accepted", "task-gap", task_id="task-gap",
                     worker="codex", mode="app-server",
                     data={"contract_id": "c-gap", "contract_digest": HEX,
                           "created_at": RFC, "initiator_verified": False})
        emitter.emit("task.accepted", "task-other", task_id="task-other",
                     worker="codex", mode="app-server",
                     data={"contract_id": "c-other",
                           "contract_digest": HEX, "created_at": RFC,
                           "initiator_verified": False})
        journal._metadata_cap = 1  # force task-gap's exclusion
        emitter.emit("task.accepted", "task-live", task_id="task-live",
                     worker="codex", mode="app-server",
                     data={"contract_id": "c-live",
                           "contract_digest": HEX, "created_at": RFC,
                           "initiator_verified": False})

        # -- the client protocol, recorded frame by frame ------------------
        reader, writer = await asyncio.open_unix_connection(
            str(server.socket_path))

        async def request(payload):
            writer.write(json.dumps(payload).encode() + b"\n")
            await writer.drain()
            line = await asyncio.wait_for(reader.readline(), 10)
            return json.loads(line)

        frames["hello"] = await request(
            {"op": "hello", "v": 1, "client": "wire-golden"})
        cursor = frames["hello"]["initial_snapshot_cursor"]

        state_page = await request(
            {"op": "snapshot", "v": 1, "snapshot_cursor": cursor})
        frames["state_pages"].append(state_page)
        while not state_page["done"]:
            state_page = await request(
                {"op": "snapshot", "v": 1, "snapshot_cursor": cursor,
                 "page_cursor": state_page["next_page_cursor"]})
            frames["state_pages"].append(state_page)

        events_page = await request(
            {"op": "snapshot-events", "v": 1, "snapshot_cursor": cursor})
        frames["event_pages"].append(events_page)
        while not events_page["done"]:
            events_page = await request(
                {"op": "snapshot-events", "v": 1,
                 "snapshot_cursor": cursor,
                 "page_cursor": events_page["next_page_cursor"]})
            frames["event_pages"].append(events_page)

        ack = await request(
            {"op": "subscribe", "v": 1, "stream_id": "*",
             "snapshot_cursor": cursor})
        assert ack["ok"], ack
        # live: post-cut discovery + gap loss + terminal update
        emit_task(emitter, "task-discovered")
        emitter.publish({
            "v": 1, "event_id": "golden/task-gap/3",
            "stream_id": "task-gap", "seq": 3, "kind": "task.accepted",
            "observed_at": RFC, "task_id": "task-gap", "worker": "codex",
            "mode": "app-server",
            "data": {"padding": "x" * 4096}})
        emitter.emit("task.completed", "task-base", task_id="task-base",
                     worker="codex", mode="app-server", status="completed",
                     outcome="ok", delivery_ok=True, duration_ms=1.0)
        await asyncio.sleep(0.05)
        # drain the live frames the same way the Rust client would
        for _ in range(64):
            try:
                line = await asyncio.wait_for(reader.readline(), 0.3)
            except asyncio.TimeoutError:
                break
            if not line.strip():
                continue
            try:
                frames["live"].append(json.loads(line))
            except json.JSONDecodeError:
                continue
        writer.close()
    finally:
        await server.close()
        journal.detach()
    return frames


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--out", type=Path, required=True)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(prefix="wire-golden-") as tmp:
        state = Path(tmp)
        os.chmod(state, 0o700)
        frames = asyncio.run(scenario_frames(state))
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(json.dumps(frames, indent=1, sort_keys=True) + "\n")
    counts = {
        "state_records": sum(len(p.get("states", []))
                             for p in frames["state_pages"]),
        "event_records": sum(len(p.get("events", []))
                             for p in frames["event_pages"]),
        "live_frames": len(frames["live"]),
    }
    print(json.dumps({"ok": True, "counts": counts,
                      "out": str(args.out)}))


if __name__ == "__main__":
    main()
