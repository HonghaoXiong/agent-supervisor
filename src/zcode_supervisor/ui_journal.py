"""Bounded UI event journal (P1.1, PLAN §4.4/§4.5).

The journal is a SUBSCRIBER of the one shared :class:`UiEmitter` projection:
sequence numbers stay allocated by the emitter's ``UiEventStream`` (the
single writer), and the journal only records what that pipeline produced —
no second task-state copy exists. Everything here is in-memory and bounded:

* retention — total events and total bytes caps with global FIFO eviction;
  every stream keeps a ``retention_floor``/``high_watermark`` pair;
* atomic cut points — ``new_cut()``/``hello()`` freeze a per-stream
  high-water map plus a SHA-256 ``cut_digest`` in a bounded TTL/LRU cursor
  registry;
* pull-based pages — ``snapshot()`` (state pages) and ``snapshot_events()``
  (event pages) are bounded by records AND bytes, continue only through the
  previous page's opaque cursor, and are per-client single-slot;
* replay — single-stream ``replay()`` inside the retention window, precise
  ``gap`` below it, never a silent jump to latest;
* subscribe-all — live subscriptions replay streams missing from the cut
  map from seq 1, continue mapped streams after their cut high-water, and
  keep only a bounded per-subscriber queue of undelivered frames.

All journal operations are synchronous, non-blocking and never raise on the
ingest path: the hub control flow calls ``ingest`` via the emitter, and a
UI-side failure can only produce explicit reset/gap markers.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import time
from collections import deque

from .ui_events import HUB_STREAM_ID

__all__ = [
    "JOURNAL_MAX_EVENTS", "JOURNAL_MAX_BYTES", "PAGE_MAX_RECORDS",
    "PAGE_MAX_BYTES", "MAX_EVENT_BYTES", "REGISTRY_MAX_CURSORS",
    "REGISTRY_TTL_S", "CLIENT_QUEUE_MAX_EVENTS", "CLIENT_QUEUE_MAX_BYTES",
    "UiJournal", "JournalSubscription", "reduce_stream",
]

#: §4.4 initial resource ceilings (configurable per instance for tests).
JOURNAL_MAX_EVENTS = 10_000
JOURNAL_MAX_BYTES = 16 * 1024 * 1024
PAGE_MAX_RECORDS = 100
PAGE_MAX_BYTES = 256 * 1024
MAX_EVENT_BYTES = 16 * 1024
REGISTRY_MAX_CURSORS = 64
REGISTRY_TTL_S = 300.0
CLIENT_QUEUE_MAX_EVENTS = 256
CLIENT_QUEUE_MAX_BYTES = 1024 * 1024

_RESET_ERRORS = frozenset(
    ("reset_required", "cursor_expired", "cursor_superseded", "page_limit",
     "invalid_request"))


def _error(error: str, **extra) -> dict:
    result = {"ok": False, "error": error}
    result.update(extra)
    return result


def _event_bytes(event: dict) -> int:
    return len(json.dumps(event, ensure_ascii=False,
                          separators=(",", ":")).encode("utf-8"))


def reduce_stream(events) -> dict | None:
    """Fold one stream's envelopes (ascending seq) into a state record.

    Used BOTH by snapshot state pages and by clients replaying the event
    pages from an empty reducer — the two views agree by construction.
    Re-applying older events never rolls the state back.
    """
    state = None
    for event in events:
        kind = event.get("kind")
        if state is None:
            state = {
                "task_id": event.get("task_id") or event.get("stream_id"),
                "worker": event.get("worker"),
                "mode": event.get("mode"),
                "status": "running", "outcome": None, "delivery_ok": None,
                "session_id": None, "turn_id": None, "turn_status": None,
                "accepted_event_id": None, "last_seq": None,
            }
        if kind == "task.accepted":
            # accepted never moves a state BACK: the initial record is
            # already "running", and re-applying it after task.completed
            # (duplicate/event-page overlap) must not roll the terminal
            # status back — later authoritative facts win.
            state["accepted_event_id"] = event.get("event_id")
        elif kind == "session.bound":
            state["session_id"] = event.get("session_id")
        elif kind == "turn.started":
            if event.get("session_id"):
                state["session_id"] = event.get("session_id")
            state["turn_id"] = event.get("turn_id")
            state["turn_status"] = "running"
        elif kind == "turn.completed":
            if event.get("session_id"):
                state["session_id"] = event.get("session_id")
            state["turn_id"] = event.get("turn_id")
            state["turn_status"] = event.get("status")
        elif kind == "task.completed":
            state["status"] = event.get("status")
            state["outcome"] = event.get("outcome")
            state["delivery_ok"] = event.get("delivery_ok")
        state["last_seq"] = event.get("seq")
    return state


class JournalSubscription:
    """One live bounded consumer queue fed by the journal single writer.

    ``target_stream`` fixes the subscription's SCOPE (repair1 / P3.2 F-01):
    ``None`` is a subscribe-all (cut-mapped streams continue after their
    high-water; streams missing from the cut map — including post-cut new
    streams — replay from seq 1); a stream id is a SINGLE-stream
    subscription whose queue only ever carries that stream's frames —
    other streams' events (live fanout) and their oversize-loss markers
    never enter it.

    ``drain`` removes delivered frames; ``pending_events``/``pending_bytes``
    never exceed the configured queue bounds (plus one bounded reset
    marker). On overflow the queue is closed after a ``reset_required``
    frame — a slow client can never grow an unbounded backlog.
    """

    def __init__(self, journal: "UiJournal", start_seq_by_stream: dict,
                 filters: dict | None = None, *,
                 target_stream: str | None = None):
        self._journal = journal
        self._start = dict(start_seq_by_stream)
        self.target_stream = target_stream
        self.filters = dict(filters) if isinstance(filters, dict) else None
        self._queue: deque[dict] = deque()
        self._bytes = 0
        self.reset_pending = False
        self.closed = False
        # loop-aware wakeup for streaming consumers (set on every enqueue;
        # never required for correctness — drain() works without a loop)
        self.ready = asyncio.Event()

    def _wake(self) -> None:
        try:
            self.ready.set()
        except RuntimeError:  # no running loop context: pollers still work
            pass

    async def wait_ready(self, timeout: float) -> None:
        """Block until new frames arrive (or the timeout, whichever first)."""
        try:
            await asyncio.wait_for(self.ready.wait(), timeout)
        except asyncio.TimeoutError:
            pass
        self.ready.clear()

    # -- diagnostics -------------------------------------------------------
    @property
    def pending_events(self) -> int:
        return len(self._queue)

    @property
    def pending_bytes(self) -> int:
        return self._bytes

    # -- writer side (journal single writer only) ---------------------------
    def _enqueue(self, frame: dict) -> None:
        if self.closed:
            return
        encoded = _event_bytes(frame)
        if len(self._queue) >= self._journal.queue_max_events \
                or self._bytes + encoded > self._journal.queue_max_bytes:
            self._overflow()
            return
        self._queue.append(frame)
        self._bytes += encoded
        self._wake()

    def _overflow(self) -> None:
        """Bounded slow-consumer close: one reset marker, then silence."""
        if self.reset_pending:
            return
        self.reset_pending = True
        marker = _event_bytes({"type": "reset_required"})
        while self._queue and self._bytes + marker > \
                self._journal.queue_max_bytes:
            self._bytes -= _event_bytes(self._queue.popleft())
        self._queue.append({"type": "reset_required"})
        self._bytes += marker  # repair2 C: accounted ONCE
        self.closed = True
        self._wake()

    # -- reader side ---------------------------------------------------------
    def drain(self, max_items: int = 64) -> list[dict]:
        """Remove and return up to ``max_items`` pending frames."""
        taken = []
        while self._queue and len(taken) < max_items:
            frame = self._queue.popleft()
            self._bytes -= _event_bytes(frame)
            taken.append(frame)
        return taken

    def close(self) -> None:
        self.closed = True
        self._queue.clear()
        self._bytes = 0


class UiJournal:
    """The bounded single-writer journal behind the UI read-only surface."""

    def __init__(self, emitter, *,
                 max_events: int = JOURNAL_MAX_EVENTS,
                 max_bytes: int = JOURNAL_MAX_BYTES,
                 max_event_bytes: int = MAX_EVENT_BYTES,
                 page_max_records: int = PAGE_MAX_RECORDS,
                 page_max_bytes: int = PAGE_MAX_BYTES,
                 registry_max_cursors: int = REGISTRY_MAX_CURSORS,
                 registry_ttl_s: float = REGISTRY_TTL_S,
                 queue_max_events: int = CLIENT_QUEUE_MAX_EVENTS,
                 queue_max_bytes: int = CLIENT_QUEUE_MAX_BYTES):
        self.emitter = emitter
        stream = getattr(emitter, "stream", None)
        self.hub_instance_id = getattr(stream, "hub_instance_id", "")
        self.journal_epoch = getattr(stream, "journal_epoch", 1)
        self.max_events = max(1, int(max_events))
        self.max_bytes = max(1, int(max_bytes))
        self.max_event_bytes = max(1, int(max_event_bytes))
        self.page_max_records = max(1, int(page_max_records))
        self.page_max_bytes = max(1, int(page_max_bytes))
        self.registry_max_cursors = max(1, int(registry_max_cursors))
        self.registry_ttl_s = float(registry_ttl_s)
        self.queue_max_events = max(1, int(queue_max_events))
        self.queue_max_bytes = max(1, int(queue_max_bytes))
        # retained events: stream -> deque of envelopes in seq order
        self._streams: dict[str, deque] = {}
        self._stream_bytes: dict[str, int] = {}
        self._global_order: deque[str] = deque()  # one entry per ingest
        self._total_events = 0
        self._total_bytes = 0
        self._high_water: dict[str, int] = {}
        # repair2 A / repair3-4: per-stream oversize-lost sequence numbers
        # (sorted, bounded at 1024). ``_lost_below`` records the highest
        # trimmed seq: at or below it completeness can never be PROVEN —
        # readers get a conservative reset instead of a fabricated gap.
        self._lost: dict[str, list[int]] = {}
        self._lost_below: dict[str, int] = {}
        # repair4: per-stream acknowledgment watermark — the highest lost
        # seq a PRIOR cut has already pinned (exposed) to consumers. A
        # fresh cut rebases its baseline floor above it; the exposing cut
        # itself keeps the loss in its own immutable snapshot forever.
        self._loss_ack_upto: dict[str, int] = {}
        # Diagnostic views ONLY (kept small; durable state lives in the
        # per-cut registry snapshots and the bounded structures above):
        # ``_ever_retained`` mirrors the CURRENTLY-live streams; a stream
        # that ever stored an event keeps its (possibly empty) deque KEY in
        # ``_streams`` — key absence is what classifies a never-retained
        # zombie. ``_zombie_acked`` mirrors what the LAST cut acknowledged.
        self._ever_retained: set[str] = set()
        self._zombie_acked: set[str] = set()
        # repair5 4: bounded lifecycle metadata. Streams get a monotone
        # EPOCH at first allocation; cuts record the epoch at creation, so
        # post-cut discovery is `stream_epoch > cut_epoch` (no full-history
        # known_high copies). Unpinned stream metadata is hard-capped at
        # ``_metadata_cap``; capacity eviction first taints the old cuts
        # that still need the victim (dispatch 5). ``_recent_high`` is a
        # bounded FIFO of recently-seen highs that survives reclamation so
        # the strict rollback/duplicate guard still rejects stale frames
        # for recently reclaimed streams.
        self._stream_epoch: dict[str, int] = {}
        self._epoch_counter = 0
        self._recent_high: dict[str, int] = {}
        self._metadata_cap = max(16, self.max_events)
        self._registry: dict[str, dict] = {}
        self._registry_order: deque[str] = deque()
        self._evicted_cursors: deque[str] = deque()  # recently superseded
        self._snapshot_version = 0
        self._subscriptions: list[JournalSubscription] = []
        # ONE unfinished pagination slot per client, shared across state and
        # events pages (PLAN §4.4). The slot binds the client's pagination
        # to the EXACT cursor of its last served page — a forged, replayed,
        # cross-cut or cross-kind continuation can never skip records.
        self._active_pages: dict[str, dict] = {}
        self.dropped_events = 0
        if emitter is not None:
            self._unsubscribe = emitter.subscribe(self.ingest)
        else:
            self._unsubscribe = None

    # -- ingest (the emitter's synchronous publish path) --------------------
    def _known_high(self, stream_id: str) -> int:
        """Highest seq the journal still knows about for the stream.

        Combines the live high-water map with the bounded ``_recent_high``
        store: after a stream's metadata is reclaimed, its recent high
        survives there for a bounded window so stale rollback/duplicate
        frames (normal OR oversize) are still rejected.
        """
        return max(self._high_water.get(stream_id, 0),
                   self._recent_high.get(stream_id, 0))

    def _note_high(self, stream_id: str, seq: int) -> None:
        self._high_water[stream_id] = max(
            self._high_water.get(stream_id, 0), seq)
        self._recent_high[stream_id] = max(
            self._recent_high.get(stream_id, 0), seq)
        while len(self._recent_high) > self._metadata_cap:
            oldest = next(iter(self._recent_high))
            del self._recent_high[oldest]

    def ingest(self, event) -> None:
        """Record one envelope; never raises, never blocks."""
        try:
            if not isinstance(event, dict) \
                    or not isinstance(event.get("stream_id"), str) \
                    or not isinstance(event.get("seq"), int) \
                    or isinstance(event.get("seq"), bool) \
                    or not isinstance(event.get("event_id"), str):
                return
            stream_id = event["stream_id"]
            known = self._known_high(stream_id)
            if event["seq"] <= known:
                # repair5 1 / repair6 Q2: ONE strict monotonic guard shared
                # by normal and oversize frames — a duplicate/rollback seq
                # at or below the known high changes NOTHING (no deque, no
                # bytes, no lost or loss-ack state, no epoch, no cuts, no
                # subscribers, no fanout; zero-seq storms allocate no state)
                self.dropped_events += 1
                return
            # repair6 Q2: epochs are allocated only AFTER the guard — a
            # name rejected by the guard never enters _stream_epoch
            if stream_id not in self._stream_epoch:
                self._epoch_counter += 1
                self._stream_epoch[stream_id] = self._epoch_counter
            encoded = _event_bytes(event)
            if encoded > self.max_event_bytes:
                # §4.4: oversized frames are never stored nor forwarded —
                # subscribers get a bounded marker instead of the payload.
                # The allocated seq is recorded as LOST so every later
                # reader sees an explicit gap/reset (repair2 A), never a
                # silently compacted history.
                self.dropped_events += 1
                self._note_high(stream_id, event["seq"])
                lost = self._lost.setdefault(stream_id, [])
                if event["seq"] not in lost:
                    lost.append(event["seq"])
                    lost.sort()
                    trimmed = lost[:-1024]  # bounded: oldest (=lowest) out
                    if trimmed:
                        self._lost_below[stream_id] = max(
                            self._lost_below.get(stream_id, 0), trimmed[-1])
                    del lost[:-1024]
                self._fanout_marker(stream_id)
                # repair8 1: IMMEDIATE materialization — the loss just
                # recorded is frozen as a full-range gap into EVERY live
                # cut whose excluded_high names this stream, before any
                # later cut's ack, pin expiry or cap eviction can reclaim
                # the global record (O(registry) per loss)
                self._freeze_excluded_gap(stream_id, event["seq"])
                # repair6 Q3: the cap must hold on EVERY growth path — an
                # oversize-only storm with zero cuts is bounded here too
                self._enforce_metadata_cap()
                return
            self._streams.setdefault(stream_id, deque())
            self._ever_retained.add(stream_id)  # diagnostic live-mirror
            events = self._streams[stream_id]
            events.append(event)
            self._stream_bytes[stream_id] = \
                self._stream_bytes.get(stream_id, 0) + encoded
            self._global_order.append(stream_id)
            self._total_events += 1
            self._total_bytes += encoded
            self._note_high(stream_id, event["seq"])
            self._evict()
            self._fanout(event)
        except Exception:  # noqa: BLE001 - journal never poisons the writer
            self.dropped_events += 1

    def detach(self) -> None:
        if self._unsubscribe is not None:
            self._unsubscribe()
            self._unsubscribe = None

    def _evict(self) -> None:
        while (self._total_events > self.max_events
                or self._total_bytes > self.max_bytes) \
                and self._global_order:
            stream_id = self._global_order[0]
            events = self._streams.get(stream_id)
            if not events:
                self._global_order.popleft()
                continue
            evicted = events.popleft()
            self._global_order.popleft()
            self._total_events -= 1
            encoded = _event_bytes(evicted)
            self._stream_bytes[stream_id] -= encoded
            self._total_bytes -= encoded
            if not events:
                # diagnostic live-mirror: nothing retained any more
                self._ever_retained.discard(stream_id)
                # repair5 4: keep unpinned stream metadata bounded even on
                # the no-cut-at-all path — the hard cap evicts the oldest
                # (tainting anything that still needs them)
                self._enforce_metadata_cap()

    def _fanout(self, event: dict) -> None:
        for subscription in list(self._subscriptions):
            if subscription.target_stream is not None \
                    and event["stream_id"] != subscription.target_stream:
                # repair1 / P3.2 F-01: a single-stream subscription only
                # ever receives its OWN target stream — the unmapped-stream
                # forward below exists for subscribe-all post-cut discovery
                continue
            start = subscription._start.get(event["stream_id"])
            if start is not None and event["seq"] <= start:
                continue  # mapped stream: only after its cut high-water
            if subscription.filters is not None \
                    and not self._matches(event, subscription.filters):
                continue  # filters apply to live exactly as to replay
            subscription._enqueue({"type": "event", "event": event})

    def _fanout_marker(self, stream_id: str) -> None:
        for subscription in list(self._subscriptions):
            if subscription.target_stream is not None \
                    and stream_id != subscription.target_stream:
                # repair1 / P3.2 F-01 adjudication: another stream's
                # oversize loss owes a single-stream subscriber nothing —
                # its view of its own target stream is unaffected, so a
                # reset here would be a spurious cross-stream signal (the
                # marker class of the F-01 leak). The target stream's own
                # loss still resets it (bounded, no silent seq hole), and
                # subscribe-all keeps resetting on ANY loss (a loss on any
                # of its covered streams breaks its no-silent-loss view).
                continue
            subscription._overflow()

    def _freeze_excluded_gap(self, stream_id: str, lost_seq: int) -> None:
        """repair8/10: freeze and PERSIST a cap-excluded stream's losses.

        First loss: the full-range run ``(1, high)`` is frozen at ingest
        into the immutable ``excluded_gaps`` tuple (removed from
        ``excluded_high``). A LATER, HIGHER loss is PERSISTENTLY appended
        at ingest as a singleton run to the append-only
        ``excluded_gaps_more`` list — existing runs are never rewritten
        (repair10 R9-03: the reader's total view survives ack+GC and
        cap eviction of the global records verbatim; no subscribe-time
        recomputation from live globals exists).
        """
        for entry in self._registry.values():
            frozen = entry.get("excluded_gaps", {}).get(stream_id)
            if frozen is not None:
                more = entry.setdefault("excluded_gaps_more", {})
                appended = more.get(stream_id, ())
                known_top = appended[-1][1] if appended else frozen[1]
                if lost_seq > known_top:
                    more.setdefault(stream_id, []).append(
                        (lost_seq, lost_seq))  # append-only, monotone
                continue  # lower/equal losses are already covered
            creation_high = entry.get("excluded_high", {}).get(stream_id)
            if creation_high is None or lost_seq <= creation_high:
                continue  # not excluded by this cut, or not a post-creation
            high = max(self._high_water.get(stream_id, 0), lost_seq)
            entry.setdefault("excluded_gaps", {})[stream_id] = (1, high)
            # the obligation has MOVED: frozen gaps are self-contained and
            # immune to capacity-eviction taint; the unfrozen exclusion is
            # what the cap treats as a live owing reference
            del entry["excluded_high"][stream_id]

    # -- diagnostics ---------------------------------------------------------
    def high_water(self, stream_id: str) -> int:
        return self._known_high(stream_id)

    def retention_floor(self, stream_id: str) -> int:
        events = self._streams.get(stream_id)
        if events:
            return events[0]["seq"]
        return self._high_water.get(stream_id, 0) + 1

    def stats(self) -> dict:
        return {"total_events": self._total_events,
                "total_bytes": self._total_bytes,
                "streams": len(self._streams),
                "cursors": len(self._registry),
                "subscriptions": len(self._subscriptions),
                "dropped_events": self.dropped_events}

    def hello_version(self) -> int:
        """The journal's current snapshot version (heartbeat diagnostic)."""
        return self._snapshot_version

    def hub_high_water(self) -> dict:
        return {HUB_STREAM_ID: self._high_water.get(HUB_STREAM_ID, 0)}

    def stream_ids(self) -> list[str]:
        return sorted(self._streams)

    # -- cut points ----------------------------------------------------------
    def _pending_zending(self) -> dict[str, int]:
        """Never-retained streams with still-unacknowledged oversize losses.

        A stream is NEVER-retained iff it has no key in ``_streams`` (the
        key survives total eviction, so a was-retained stream is never
        misread as a zombie). For such a stream every allocated seq was
        dropped: its full range [1..high] is honestly lost, derivable even
        after the bounded ``_lost`` list trims the prefix.
        """
        pending = {}
        for stream_id, lost in self._lost.items():
            if stream_id in self._streams:
                continue  # was retained at some point: not a zombie
            high = self._high_water.get(stream_id, 0)
            if self._loss_ack_upto.get(stream_id, 0) < high:
                pending[stream_id] = high
        return pending

    def _unacked_lost(self, stream_id: str, floor: int, high: int) -> list:
        """Lost seqs in [floor..high] not yet pinned by any prior cut."""
        ack = self._loss_ack_upto.get(stream_id, 0)
        return [seq for seq in self._lost.get(stream_id, ())
                if floor <= seq <= high and seq > ack]

    def _cut_streams(self) -> dict[str, int]:
        """Streams a FRESH cut pins: streams with retained events only.

        repair4: retired streams (empty deque, key present) and zombies
        (never stored, loss-only) are never pinned; their discovery/loss
        obligations live in each cut's immutable snapshot instead.
        """
        return {stream_id: self._high_water.get(stream_id, 0)
                for stream_id in sorted(self._streams)
                if self._streams[stream_id]}

    @staticmethod
    def _compress_runs(seqs: list[int]) -> list[tuple[int, int]]:
        """Contiguous inclusive runs of a sorted seq list (bounded, exact)."""
        runs = []
        for seq in seqs:
            if runs and seq == runs[-1][1] + 1:
                runs[-1] = (runs[-1][0], seq)
            else:
                runs.append((seq, seq))
        return runs

    def _lost_runs_upto(self, stream_id: str, floor: int, high: int) -> list:
        return self._compress_runs(
            self._unacked_lost(stream_id, floor, high))

    def _live_streams(self) -> set[str]:
        """Streams with retained events (subscribe-all backfill set)."""
        return {stream_id for stream_id, events in self._streams.items()
                if events}

    def _range_lost(self, stream_id: str, upto_seq: int) -> bool:
        """True when [1..upto_seq] is not fully replayable.

        Covers the evicted prefix (retention floor above 1), interior
        oversize holes, and the trim watermark (lost metadata was capped —
        completeness below it can no longer be proven, only reset).
        """
        if upto_seq < 1:
            return False
        if self.retention_floor(stream_id) > 1:
            return True
        if self._lost_below.get(stream_id, 0) >= 1:
            return True
        for seq in self._lost.get(stream_id, ()):
            if seq <= upto_seq:
                return True
            break  # sorted: the first entry above the range ends the scan
        return False

    def _entry_lost(self, entry: dict, stream_id: str) -> bool:
        """True when this CUT's own promised range is no longer complete.

        Per-cut immutable evidence: the entry snapshots each pinned
        stream's baseline floor (retention floor AND acknowledged-loss
        watermark at creation) and the unacknowledged holes it pins. Later
        global trims, further losses or acknowledgments can never turn an
        old cut back into "complete"; the retention floor sliding past the
        cut's baseline floor (prefix evicted AFTER the cut) also resets.
        repair6 R5-01 / repair7 1: a stream PINNED by this cut whose live
        records were reclaimed entirely (no deque key) resets on EVERY
        read path — state pages, events pages and subscribe_all share ONE
        strict tier (the repair6 dual tier is deleted).
        """
        if stream_id in entry["high_watermarks"] \
                and stream_id not in self._streams:
            return True  # pinned stream fully reclaimed: evidence gone
        if entry["lost_runs"].get(stream_id):
            return True  # this cut itself pinned an unacknowledged hole
        if entry["unprovable"].get(stream_id):
            return True  # completeness above the baseline was unprovable
        if self.retention_floor(stream_id) > entry["floors"].get(
                stream_id, 1):
            return True  # prefix evicted since the cut's baseline
        return False

    def new_cut(self) -> dict:
        """Atomically freeze a fresh baseline into one cut point.

        repair5 ownership model: every entry is an IMMUTABLE snapshot —
        pinned live streams with baseline floors + unacknowledged holes,
        the pending never-retained zombies (gap evidence), the creation
        EPOCH (post-cut discovery is `stream_epoch > cut_epoch`, no
        full-history copies) and a BOUNDED ``known_high`` copy of the
        live map at creation (capped at ``_metadata_cap`` most-recent
        streams) for known-unpinned continuation bounds.
        Acknowledgment advances journal-level watermarks only — for the
        streams this cut's baseline covers — and never rewrites an older
        entry, so old cuts keep their own loss/gap results.
        """
        self._snapshot_version += 1
        version = self._snapshot_version
        snapshot_id = hashlib.sha256(
            f"{self.hub_instance_id}/{self.journal_epoch}/"
            f"{version}/{time.monotonic_ns()}".encode()).hexdigest()[:16]
        high_watermarks = self._cut_streams()
        pending = self._pending_zending()
        # baseline floor = retention floor, advanced past acknowledged losses
        floors = {}
        lost_runs = {}
        unprovable = {}
        for stream_id, high in high_watermarks.items():
            floor = max(self.retention_floor(stream_id),
                        self._loss_ack_upto.get(stream_id, 0) + 1)
            floors[stream_id] = floor
            runs = self._lost_runs_upto(stream_id, floor, high)
            if runs:
                lost_runs[stream_id] = runs
            if self._lost_below.get(stream_id, 0) >= floor:
                unprovable[stream_id] = True
        # bounded known copy (repair5 4 / repair6 R5-02): per-cut
        # continuation bounds for UNPINNED retired streams only, capped at
        # the number of currently-live streams, most-recent epochs first.
        # If the cap EXCLUDES a retired stream that still holds an
        # UNACKNOWLEDGED loss, this cut is TAINTED at creation — the
        # truncation is observable (every read resets), never silent.
        def _has_unacked_loss(stream_id: str) -> bool:
            lost = self._lost.get(stream_id)
            if not lost:
                return False
            return max(lost) > self._loss_ack_upto.get(stream_id, 0)

        unpinned = [s for s in self._high_water
                    if s not in high_watermarks and s in self._streams]
        unpinned.sort(key=lambda s: -self._stream_epoch.get(s, 0))
        cap = max(1, len(self._ever_retained))
        known_high = {s: self._high_water[s] for s in unpinned[:cap]}
        # repair7 2: the cap-excluded set is FROZEN into the entry with each
        # stream's creation-time high. A post-creation loss on one of these
        # streams is owed by THIS cut forever — a full-range gap computed
        # from the frozen bound, immune to any later cut's taint/ack — so
        # every reader of this cut sees the same thing at any time.
        excluded_high = {s: self._high_water[s] for s in unpinned[cap:]}
        truncated_loss_owed = any(
            _has_unacked_loss(s) for s in excluded_high)
        # the first NON-EMPTY cut after unacknowledged zombies is tainted
        # (reset) and acknowledges them; empty cuts defer them (gap frames
        # from the entry's own snapshot, immune to later acknowledgment)
        tainted = (bool(pending) and bool(high_watermarks)) \
            or truncated_loss_owed
        digest = self._cut_digest(snapshot_id, version, high_watermarks)
        cursor = {"v": 1, "hub_instance_id": self.hub_instance_id,
                  "journal_epoch": self.journal_epoch,
                  "snapshot_version": version, "snapshot_id": snapshot_id}
        self._register(
            cursor, high_watermarks, digest, floors=floors,
            lost_runs=lost_runs, unprovable=unprovable, tainted=tainted,
            pending=dict(pending), known_high=known_high,
            excluded_high=excluded_high, epoch=self._epoch_counter)
        # acknowledgment watermarks (journal-level, never rewriting entries)
        acknowledged = set()
        if tainted:
            for stream_id, high in pending.items():
                self._loss_ack_upto[stream_id] = max(
                    self._loss_ack_upto.get(stream_id, 0), high)
                acknowledged.add(stream_id)
        for stream_id, runs in lost_runs.items():
            self._loss_ack_upto[stream_id] = max(
                self._loss_ack_upto.get(stream_id, 0), runs[-1][1])
        if truncated_loss_owed:
            # the taint IS the exposure: an excluded stream's unacknowledged
            # loss is now observable on this cut (as a permanent reset), so
            # it is acknowledged — the taint never sticks to LATER cuts
            for stream_id in unpinned[cap:]:
                if not _has_unacked_loss(stream_id):
                    continue
                loss_cap = 0
                for seq in self._lost.get(stream_id, ()):
                    if seq > self._high_water.get(stream_id, 0):
                        break
                    loss_cap = seq
                if loss_cap:
                    self._loss_ack_upto[stream_id] = max(
                        self._loss_ack_upto.get(stream_id, 0), loss_cap)
        # repair5 3: a cut whose baseline covers a KNOWN-RETIRED stream's
        # losses up to its bound has exposed them — they become reclaimable
        # without any further pin. The ack only advances to ACTUAL loss
        # seqs at or below the bound (a clean stream's bound acks NOTHING:
        # floors must never slide past its own high). Never-retained
        # zombies are excluded: only a TAINTED (non-empty) cut acks those.
        for stream_id, bound in known_high.items():
            if stream_id not in self._streams:
                continue
            loss_cap = 0
            for seq in self._lost.get(stream_id, ()):
                if seq > bound:
                    break
                loss_cap = seq
            below = self._lost_below.get(stream_id, 0)
            if 0 < below <= bound:
                loss_cap = max(loss_cap, below)
            if loss_cap:
                self._loss_ack_upto[stream_id] = max(
                    self._loss_ack_upto.get(stream_id, 0), loss_cap)
        self._zombie_acked = acknowledged  # diagnostic view (last cut only)
        self._prune_metadata()
        return {"snapshot_cursor": cursor, "cut_digest": digest}

    def _stream_pinned(self, stream_id: str) -> bool:
        """Any live reference that still needs this stream's metadata?

        repair5: a live cut that PREDATES the stream owes post-cut
        discovery (epoch comparison — no known_high copies needed here).
        Continuation obligations do NOT pin the stream: every cut's
        ``known_high`` snapshot is immutable and self-contained, and a
        reclaimed stream's later losses re-enter the live loss map with
        their seqs, so copy-based continuation keeps working.
        """
        events = self._streams.get(stream_id)
        if events:
            return True  # retained events
        epoch = self._stream_epoch.get(stream_id, 0)
        for entry in self._registry.values():
            if entry["epoch"] < epoch:
                return True  # a live cut PREDATES it: discovery owed
        # repair8 2: excluded_high/excluded_gaps references do NOT block
        # prune-time reclamation — the loss-ingest freeze (item 1) makes
        # materialized gaps self-contained in the entry, and an excluded
        # stream reclaimed BEFORE any loss owes nothing (a later loss
        # re-enters the live map with a NEW epoch and is observed through
        # post-cut discovery). They are honoured in _enforce_metadata_cap,
        # where capacity eviction taints the owing cuts before reclaim.
        for subscription in self._subscriptions:
            if stream_id in subscription._start:
                return True  # an active subscriber maps this stream
        return False

    def _drop_stream(self, stream_id: str) -> None:
        """Remove every global record of a fully reclaimed stream."""
        self._streams.pop(stream_id, None)
        self._stream_bytes.pop(stream_id, None)
        self._lost.pop(stream_id, None)
        self._lost_below.pop(stream_id, None)
        self._loss_ack_upto.pop(stream_id, None)
        self._high_water.pop(stream_id, None)
        self._stream_epoch.pop(stream_id, None)

    def _try_reclaim(self, stream_id: str) -> None:
        """Reclaim one stream's metadata when nothing references it.

        Conditions (dispatch 4/5): no retained events; no live cut predates
        it (post-cut discovery) nor owes it an unreported continuation; no
        active subscription maps it; all its loss evidence is acknowledged
        or subsumed by a baseline. The bounded ``_recent_high`` store keeps
        the rollback guard alive for recently reclaimed streams.
        """
        if self._streams.get(stream_id):
            return  # still live
        if stream_id in self._pending_zending():
            return  # unacknowledged never-retained loss: deferred evidence
        lost = self._lost.get(stream_id)
        if lost:
            ack = self._loss_ack_upto.get(stream_id, 0)
            if max(lost) > ack:
                return  # unacknowledged loss evidence still needed
            below = self._lost_below.get(stream_id, 0)
            if below > ack:
                return  # trimmed-loss uncertainty still needed
        if self._stream_pinned(stream_id):
            return
        self._drop_stream(stream_id)

    def _prune_metadata(self) -> None:
        """Reclaim unreferenced stream metadata; enforce the hard cap.

        repair5 4/5: unpinned stream entries are hard-capped at
        ``_metadata_cap`` (base ``max_events``). When the cap is exceeded,
        the oldest-epoch victims are reclaimed ONLY after every live cut
        or subscription that still needs them (predating cuts, unreported
        continuations, mapped subscribers) is explicitly tainted/reset —
        capacity pressure never silently deletes discovery evidence.
        """
        for stream_id in list(self._streams) + list(self._lost):
            self._try_reclaim(stream_id)
        # repair6 Q2: epoch records follow the same lifecycle — an epoch
        # entry whose stream is in neither the live high-water map nor the
        # bounded recent window is unreachable metadata
        for stream_id in list(self._stream_epoch):
            if stream_id not in self._high_water \
                    and stream_id not in self._recent_high:
                del self._stream_epoch[stream_id]
        self._enforce_metadata_cap()

    def _enforce_metadata_cap(self) -> None:
        unpinned = [stream_id for stream_id in self._high_water
                    if not self._streams.get(stream_id)]
        excess = len(unpinned) - self._metadata_cap
        if excess <= 0:
            return
        victims = sorted(unpinned,
                         key=lambda s: self._stream_epoch.get(s, 0))[:excess]
        for stream_id in victims:
            epoch = self._stream_epoch.get(stream_id, 0)
            high = self._known_high(stream_id)
            for entry in self._registry.values():
                if entry["epoch"] < epoch:
                    entry["tainted"] = True  # discovery obligation broken
                    continue
                bound = entry["known_high"].get(stream_id)
                if bound is not None and bound < high:
                    entry["tainted"] = True  # continuation obligation broken
                # repair8 2: an UNFROZEN exclusion on a stream that still
                # holds UNACKNOWLEDGED losses is a live owing obligation —
                # capacity eviction taints the owing cut before reclaiming.
                # A FROZEN gap (excluded_gaps) is self-contained in the
                # entry: eviction changes nothing observable, so no taint
                # is needed (or allowed — the frozen frames keep serving
                # verbatim). Clean excluded streams owe nothing.
                if stream_id in entry.get("excluded_high", {}) and any(
                        seq > self._loss_ack_upto.get(stream_id, 0)
                        for seq in self._lost.get(stream_id, ())):
                    entry["tainted"] = True
            for subscription in list(self._subscriptions):
                if stream_id in subscription._start:
                    subscription._overflow()  # bounded close, never silent
            self._drop_stream(stream_id)

    def hello(self) -> dict:
        """One atomic cut: initial cursor + the same-cut streams diagnostic."""
        cut = self.new_cut()
        entry = self._registry[cut["snapshot_cursor"]["snapshot_id"]]
        return {"initial_snapshot_cursor": cut["snapshot_cursor"],
                "initial_cut_digest": cut["cut_digest"],
                "snapshot_version":
                    cut["snapshot_cursor"]["snapshot_version"],
                "hub_instance_id": self.hub_instance_id,
                "journal_epoch": self.journal_epoch,
                "streams": [
                    {"stream_id": stream_id,
                     "retention_floor": self.retention_floor(stream_id),
                     "high_watermark": high}
                    for stream_id, high
                    in sorted(entry["high_watermarks"].items())],
                "cursor_registered": True}

    def _cut_digest(self, snapshot_id, version, high_watermarks) -> str:
        canonical = json.dumps(
            {"v": 1, "hub_instance_id": self.hub_instance_id,
             "journal_epoch": self.journal_epoch,
             "snapshot_version": version, "snapshot_id": snapshot_id,
             "high_watermarks": high_watermarks},
            sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(canonical.encode()).hexdigest()

    def cut_entry(self, cursor: dict) -> dict:
        """The registry entry for a validated cursor (raises ValueError)."""
        problem = self._cursor_problem(cursor)
        if problem:
            raise ValueError(problem)
        return self._registry[cursor["snapshot_id"]]

    def _cursor_problem(self, cursor) -> str | None:
        if not isinstance(cursor, dict) or cursor.get("v") != 1 \
                or not isinstance(cursor.get("snapshot_id"), str):
            return "malformed cursor"
        if cursor.get("hub_instance_id") != self.hub_instance_id \
                or cursor.get("journal_epoch") != self.journal_epoch:
            return "journal epoch mismatch"
        snapshot_id = cursor["snapshot_id"]
        if snapshot_id in self._registry:
            entry = self._registry[snapshot_id]
            if entry["cursor"]["snapshot_version"] \
                    != cursor.get("snapshot_version"):
                return "snapshot_version mismatch"
            if self.registry_ttl_s <= 0 or \
                    time.monotonic() - entry["last_used"] \
                    > self.registry_ttl_s:
                del self._registry[snapshot_id]
                self._registry_order.remove(snapshot_id)
                self._evicted_cursors.append(snapshot_id)
                return "cursor_expired"
            entry["last_used"] = time.monotonic()
            return None
        if snapshot_id in self._evicted_cursors:
            return "cursor_superseded"
        return "unknown"

    def _register(self, cursor, high_watermarks, digest, *, floors=None,
                  lost_runs=None, unprovable=None, tainted=False,
                  pending=None, known_high=None, excluded_high=None,
                  epoch=0) -> None:
        snapshot_id = cursor["snapshot_id"]
        now = time.monotonic()
        if snapshot_id in self._registry:
            self._registry_order.remove(snapshot_id)
        self._registry[snapshot_id] = {
            "cursor": cursor, "high_watermarks": dict(high_watermarks),
            "floors": dict(floors or {}), "lost_runs": dict(lost_runs or {}),
            "unprovable": dict(unprovable or {}), "tainted": tainted,
            "pending": dict(pending or {}),
            "known_high": dict(known_high or {}),
            "excluded_high": dict(excluded_high or {}),
            "epoch": epoch,
            "cut_digest": digest, "created": now, "last_used": now}
        self._registry_order.append(snapshot_id)
        while len(self._registry) > self.registry_max_cursors:
            oldest = self._registry_order.popleft()
            self._registry.pop(oldest, None)
            self._evicted_cursors.append(oldest)
        while len(self._evicted_cursors) > 4 * self.registry_max_cursors:
            self._evicted_cursors.popleft()

    def _resolve_cut(self, cursor, client=None, release_slot=True):
        """Validated registry entry for a request cursor, or an error dict.

        A cursor-lifecycle failure (expired/superseded/invalid) releases
        the client's unfinished page slot — including the DEFAULT
        anonymous client (repair3 5): the standard recovery is a FRESH
        cut, and it must not be blocked by the dead pagination. Paths that
        own no slot (subscribe-all) pass ``release_slot=False``.
        """
        if cursor is None:
            cut = self.new_cut()
            return self._registry[cut["snapshot_cursor"]["snapshot_id"]]
        problem = self._cursor_problem(cursor)
        if problem:
            if release_slot:
                self._active_pages.pop(self._client_key(client), None)
            if problem == "cursor_expired":
                return _error("cursor_expired")
            if problem == "cursor_superseded":
                return _error("cursor_superseded")
            return _error("invalid_request", detail=problem)
        return self._registry[cursor["snapshot_id"]]

    # -- paged snapshot: states ----------------------------------------------
    def snapshot(self, snapshot_cursor=None, page_cursor=None,
                 client=None) -> dict:
        entry = self._resolve_cut(snapshot_cursor, client=client)
        if isinstance(entry, dict) and entry.get("ok") is False:
            return entry
        snapshot_id = entry["cursor"]["snapshot_id"]
        opened, problem = self._claim_page_slot(client, "state",
                                                snapshot_id, page_cursor)
        if not opened:
            return problem
        if entry.get("tainted"):
            # the first non-empty cut after an unacknowledged oversize-only
            # stream: honest reset; the NEXT fresh cut is the clean baseline
            self._active_pages.pop(self._client_key(client), None)
            return _error("reset_required",
                          detail="cut was taken over an unacknowledged loss")
        after_stream = "" if page_cursor is None \
            else page_cursor["after_stream"]
        task_streams = [stream_id for stream_id
                        in sorted(entry["high_watermarks"])
                        if stream_id != HUB_STREAM_ID]
        states, after, truncated = [], after_stream, False
        for stream_id in task_streams:
            if stream_id <= after_stream:
                continue
            if self._entry_lost(entry, stream_id):
                # the cut's own promised range is no longer complete (prefix
                # evicted past the cut's baseline floor, a pinned hole, or a
                # loss since); the client must re-baseline, slot released
                self._active_pages.pop(self._client_key(client), None)
                return _error("reset_required",
                              detail=f"events for {stream_id} are no "
                                     "longer fully retained")
            replay = self._replay_upto(stream_id, entry)
            if isinstance(replay, dict) and replay.get("ok") is False:
                return replay
            state = reduce_stream(replay)
            if state is None:
                continue
            record = dict(state)
            record["task_id"] = stream_id
            if len(states) >= self.page_max_records or \
                    _event_bytes(states + [record]) > self.page_max_bytes:
                truncated = True
                break
            states.append(record)
            after = stream_id
        # anti-stall: a full pass that appended nothing (every remaining
        # stream yields an empty replay) terminates the pagination — an
        # unchanged next cursor must never be re-served forever
        done = not truncated and (
            not task_streams or after == task_streams[-1]
            or after == after_stream)
        result = {"ok": True,
                  "snapshot_cursor": entry["cursor"],
                  "cut_digest": entry["cut_digest"],
                  "states": states, "page": None,
                  "next_page_cursor": None, "done": done}
        if done:
            self._record_page_progress(client, None)
        else:
            next_cursor = {"snapshot_id": snapshot_id,
                           "after_stream": after}
            result["next_page_cursor"] = next_cursor
            self._record_page_progress(client, next_cursor)
        return result

    def _client_key(self, client) -> str:
        return client if isinstance(client, str) and client \
            else "anonymous"

    def _claim_page_slot(self, client, kind: str, snapshot_id: str,
                         page_cursor) -> tuple[bool, dict | None]:
        """Open or continue the client's ONE pagination slot.

        Continuation is bound to the EXACT cursor of the previously served
        page (repair1 gap 1): a forged after_stream/after_seq, a reused or
        out-of-order cursor, a cross-cut or cross-kind cursor can never
        skip undelivered records.
        """
        key = self._client_key(client)
        slot = self._active_pages.get(key)
        if page_cursor is None:
            if slot is not None and (slot["snapshot_id"] != snapshot_id
                                     or slot["kind"] != kind):
                return False, _error(
                    "page_limit",
                    detail="one unfinished pagination per client")
            self._active_pages[key] = {"snapshot_id": snapshot_id,
                                       "kind": kind, "next": None}
            return True, None
        if not isinstance(page_cursor, dict) \
                or page_cursor.get("snapshot_id") != snapshot_id:
            return False, _error("invalid_request",
                                 detail="continuation does not match the cut")
        if slot is None or slot["snapshot_id"] != snapshot_id \
                or slot["kind"] != kind or slot["next"] != page_cursor:
            return False, _error("invalid_request",
                                 detail="continuation is not the open page")
        return True, None

    def _record_page_progress(self, client, next_cursor) -> None:
        """Remember the exact cursor the client's NEXT page must present."""
        key = self._client_key(client)
        slot = self._active_pages.get(key)
        if slot is None:
            return
        if next_cursor is None:
            self._active_pages.pop(key, None)  # pagination finished
        else:
            slot["next"] = next_cursor

    # -- paged snapshot: events ------------------------------------------------
    def snapshot_events(self, snapshot_cursor, page_cursor=None,
                        client=None) -> dict:
        entry = self._resolve_cut(snapshot_cursor, client=client)
        if isinstance(entry, dict) and entry.get("ok") is False:
            return entry
        snapshot_id = entry["cursor"]["snapshot_id"]
        streams = sorted(entry["high_watermarks"])
        # repair6 R5-03: the taint check precedes EVERY early return — a
        # tainted cut (capacity eviction, unacknowledged zombie, broken
        # obligation) must never serve a completed empty page as intact
        if entry.get("tainted"):
            self._active_pages.pop(self._client_key(client), None)
            return _error("reset_required",
                          detail="cut was taken over an unacknowledged loss")
        if not streams:
            return self._events_page_done(entry)
        opened, problem = self._claim_page_slot(client, "events",
                                                snapshot_id, page_cursor)
        if not opened:
            return problem
        if page_cursor is None:
            position = 0
            after_seq = -1  # "from this stream's cut-baseline floor"
        else:
            if not isinstance(page_cursor.get("stream_id"), str) \
                    or not isinstance(page_cursor.get("after_seq"), int) \
                    or isinstance(page_cursor.get("after_seq"), bool):
                return _error("invalid_request",
                              detail="continuation does not match the cut")
            try:
                position = streams.index(page_cursor["stream_id"])
            except ValueError:
                return _error("invalid_request",
                              detail="continuation stream is not in the cut")
            after_seq = page_cursor["after_seq"]
        events, bytes_so_far, truncated = [], 0, False
        while position < len(streams):
            current = streams[position]
            limit = entry["high_watermarks"][current]
            floor = self.retention_floor(current)
            if self._entry_lost(entry, current):
                self._active_pages.pop(self._client_key(client), None)
                return _error("reset_required",
                              detail=f"events for {current} are no longer "
                                     "fully retained")
            if after_seq < 0:
                # fresh stream: from the cut's OWN baseline floor (which
                # may legitimately be above 1 for a fresh cut taken after
                # partial prefix retention)
                begin = max(entry["floors"].get(current, 1), floor)
            else:
                if after_seq + 1 < floor:
                    self._active_pages.pop(self._client_key(client), None)
                    return _error(
                        "reset_required",
                        detail=f"events for {current} no longer retained")
                begin = after_seq + 1
            if begin <= limit:
                replay = self._replay_range(current, begin, limit)
                if isinstance(replay, dict) and replay.get("ok") is False:
                    return replay
                for event in replay:
                    encoded = _event_bytes(event)
                    if len(events) >= self.page_max_records or \
                            bytes_so_far + encoded > self.page_max_bytes:
                        truncated = True
                        break
                    events.append(event)
                    bytes_so_far += encoded
                    after_seq = event["seq"]
                if truncated:
                    break
            after_seq = limit  # this stream is fully delivered
            position += 1
            if position < len(streams):
                after_seq = -1
        if position >= len(streams) and not truncated:
            self._record_page_progress(client, None)
            return self._events_page_done(entry, events)
        next_cursor = {"snapshot_id": snapshot_id,
                       "stream_id": streams[position],
                       "after_seq": after_seq}
        self._record_page_progress(client, next_cursor)
        return {"ok": True, "snapshot_cursor": entry["cursor"],
                "cut_digest": entry["cut_digest"], "events": events,
                "page": None, "done": False,
                "next_page_cursor": next_cursor}

    def _events_page_done(self, entry, events=()) -> dict:
        return {"ok": True, "snapshot_cursor": entry["cursor"],
                "cut_digest": entry["cut_digest"], "events": list(events),
                "page": None, "next_page_cursor": None, "done": True}

    def _replay_upto(self, stream_id: str, entry):
        return self._replay_range(
            stream_id, entry["floors"].get(stream_id, 1),
            entry["high_watermarks"].get(stream_id, 0))

    def _replay_range(self, stream_id: str, from_seq: int, to_seq: int):
        if to_seq <= 0 or from_seq > to_seq:
            return []
        floor = self.retention_floor(stream_id)
        if floor > from_seq:
            # an event the cut promised was evicted: whole-cursor reset
            return _error(
                "reset_required",
                detail=f"events for {stream_id} no longer retained")
        return [event for event in self._streams.get(stream_id, ())
                if from_seq <= event["seq"] <= to_seq]

    # -- single-stream replay ---------------------------------------------------
    def replay(self, stream_id: str, after_seq: int,
               limit: int = PAGE_MAX_RECORDS, filters: dict | None = None):
        if not isinstance(stream_id, str) \
                or not isinstance(after_seq, int) \
                or isinstance(after_seq, bool):
            return _error("invalid_request", detail="bad replay cursor")
        limit = max(1, min(int(limit), self.page_max_records))
        high = self.high_water(stream_id)
        floor = self.retention_floor(stream_id)
        if after_seq + 1 < floor and after_seq < high:
            return _error(
                "gap",
                gap={"stream_id": stream_id, "from_seq": after_seq + 1,
                     "to_seq": floor - 1, "count": floor - 1 - after_seq},
                retention_floor=floor, high_watermark=high,
                snapshot_version=self._snapshot_version)
        if after_seq < self._lost_below.get(stream_id, 0):
            # repair3 2/3: the bounded lost list was trimmed below this
            # request — completeness cannot be proven, so reset honestly
            # instead of fabricating a gap range
            return _error("reset_required",
                          detail=f"lost metadata for {stream_id} was trimmed",
                          retention_floor=floor, high_watermark=high,
                          snapshot_version=self._snapshot_version)
        holes = [seq for seq in self._lost.get(stream_id, ())
                 if after_seq < seq <= high]
        if holes:
            # repair3 3: report the FIRST CONTIGUOUS lost run only — a
            # wider span would cover retained events and lie about count
            run_end = holes[0]
            for seq in holes[1:]:
                if seq != run_end + 1:
                    break
                run_end = seq
            return _error(
                "gap",
                gap={"stream_id": stream_id, "from_seq": holes[0],
                     "to_seq": run_end, "count": run_end - holes[0] + 1},
                retention_floor=floor, high_watermark=high,
                snapshot_version=self._snapshot_version)
        events, bytes_so_far = [], 0
        for event in self._streams.get(stream_id, ()):
            if event["seq"] <= after_seq:
                continue
            if filters and not self._matches(event, filters):
                continue
            encoded = _event_bytes(event)
            if len(events) >= limit or \
                    bytes_so_far + encoded > self.page_max_bytes:
                break
            events.append(event)
            bytes_so_far += encoded
        more = bool(events) and events[-1]["seq"] < high
        return {"ok": True, "events": events,
                "next_after_seq": events[-1]["seq"] if events
                else after_seq,
                "retention_floor": floor, "high_watermark": high,
                "more": more, "snapshot_version": self._snapshot_version}

    @staticmethod
    def _matches(event: dict, filters: dict) -> bool:
        kinds = filters.get("kinds")
        if isinstance(kinds, (list, tuple)) and event.get("kind") \
                not in kinds:
            return False
        workers = filters.get("workers")
        if isinstance(workers, (list, tuple)) and event.get("worker") \
                not in workers:
            return False
        return True

    # -- live subscriptions -----------------------------------------------
    def subscribe_all(self, snapshot_cursor) -> JournalSubscription:
        """Register a subscribe-all against a validated cut point.

        Streams IN the cut map continue strictly after their high-water;
        streams missing from the map (created after the cut) replay from
        seq 1. Any stream whose cut-promised range — or a new stream's
        seq 1 — is no longer replayable resets the subscription instead
        of silently starting mid-stream.
        """
        entry = self._resolve_cut(snapshot_cursor, release_slot=False)
        if isinstance(entry, dict) and entry.get("ok") is False:
            raise _SubscriptionError(entry)
        if entry.get("tainted"):
            # the cut's own baseline was taken over an unacknowledged
            # never-retained loss: honest born-reset (the next fresh cut
            # carries the acknowledged clean baseline)
            subscription = JournalSubscription(self, {})
            subscription._overflow()
            self._subscriptions.append(subscription)
            return subscription
        start = {stream_id: high
                 for stream_id, high in entry["high_watermarks"].items()}
        subscription = JournalSubscription(self, start)
        for stream_id, bound in sorted(start.items()):
            # per-cut isolation: the cut's pinned history AND its live
            # continuation must both be hole-free above its baseline floor
            floor = entry["floors"].get(stream_id, 1)
            if self._entry_lost(entry, stream_id) or any(
                    seq >= floor for seq in self._lost.get(stream_id, ())):
                subscription._overflow()
                break
            if self.retention_floor(stream_id) > bound + 1:
                subscription._overflow()
                break
            for event in self._streams.get(stream_id, ()):
                if event["seq"] > bound:
                    subscription._enqueue({"type": "event", "event": event})
        if not subscription.closed:
            self._subscribe_pending_and_post_cut(entry, subscription, start)
        self._subscriptions.append(subscription)
        return subscription

    def _subscribe_pending_and_post_cut(self, entry, subscription,
                                        start: dict) -> None:
        """Loss/discovery obligations a cut owes beyond its pinned streams.

        repair5: post-cut discovery is epoch-based (``stream_epoch >
        cut epoch``) — no full-history copies. Known-unpinned continuation
        bounds come from the entry's BOUNDED ``known_high`` snapshot (a
        stream missing from the capped copy degrades to bound 0, i.e. a
        full-range gap — honest, never silent). A trimmed lost watermark
        (``_lost_below``) above the bound makes the exact continuation
        unknowable: an empty-deque stream still gets the exact full
        continuation gap (everything past the bound is unavailable), any
        other shape resets.
        """
        # (1) the cut's own pending-zombie snapshot (gap frames, no close)
        for stream_id, high in sorted(entry.get("pending", {}).items()):
            if high >= 1:
                subscription._enqueue({
                    "type": "gap",
                    "gap": {"stream_id": stream_id, "from_seq": 1,
                            "to_seq": high, "count": high}})
            if subscription.closed:
                return
        cut_epoch = entry.get("epoch", 0)
        known = entry.get("known_high", {})
        pending_snapshot = entry.get("pending", {})
        live_map = self._high_water
        # (2) streams born after the cut AND unknown to its copy: epoch
        # comparison (copy/pending members go to the continuation steps —
        # their obligations are immutable and copy-anchored; a re-ingested
        # name whose metadata was reclaimed keeps its old-cut membership)
        for stream_id in sorted(live_map):
            if stream_id in known or stream_id in pending_snapshot \
                    or self._stream_epoch.get(stream_id, 0) <= cut_epoch:
                continue
            events = self._streams.get(stream_id)
            if events:
                if self._range_lost(stream_id,
                                    self.high_water(stream_id)):
                    subscription._overflow()  # hole in a new live stream
                    return
                for event in events:
                    subscription._enqueue({"type": "event", "event": event})
            else:
                # empty (retired or never-retained): the whole allocated
                # range is unavailable — an exact full-range gap frame
                high = live_map[stream_id]
                if high >= 1:
                    subscription._enqueue({
                        "type": "gap",
                        "gap": {"stream_id": stream_id, "from_seq": 1,
                                "to_seq": high, "count": high}})
            if subscription.closed:
                return
        # (3) copy members (known streams, pinned ones skipped): the
        # continuation (bound, current high] — the copy's bound is
        # immutable; losses/trim-watermarks come from the live maps (a
        # reclaimed stream's later losses re-enter them with their seqs).
        # A stream in the cut's PENDING snapshot owes ONLY that snapshot:
        # its obligation was frozen at creation (never-retained streams
        # have no replayable continuation to extend).
        for stream_id, known_bound in sorted(known.items()):
            if stream_id in start or stream_id in pending_snapshot:
                continue
            high = live_map.get(stream_id, known_bound)
            lost_after = [seq for seq in self._lost.get(stream_id, ())
                          if seq > known_bound]
            below = self._lost_below.get(stream_id, 0)
            events = self._streams.get(stream_id)
            if not events:
                # nothing after the bound is replayable: any loss (known
                # or trimmed) makes the EXACT full continuation gap —
                # (bound, high] is precisely everything unavailable
                if (lost_after or below > known_bound) \
                        and high > known_bound:
                    subscription._enqueue({
                        "type": "gap",
                        "gap": {"stream_id": stream_id,
                                "from_seq": known_bound + 1,
                                "to_seq": high,
                                "count": high - known_bound}})
                continue
            if below > known_bound:
                subscription._overflow()  # uncertain live history: reset
                return
            if lost_after:
                runs = self._compress_runs(lost_after)
                subscription._enqueue({
                    "type": "gap",
                    "gap": {"stream_id": stream_id,
                            "from_seq": runs[0][0], "to_seq": runs[-1][1],
                            "count": sum(end - begin + 1
                                         for begin, end in runs)}})
                continue
            floor = self.retention_floor(stream_id)
            floor = self.retention_floor(stream_id)
            if floor > known_bound + 1:
                subscription._enqueue({
                    "type": "gap",
                    "gap": {"stream_id": stream_id,
                            "from_seq": known_bound + 1,
                            "to_seq": floor - 1,
                            "count": floor - 1 - known_bound}})
            else:
                for event in events:
                    if event["seq"] > known_bound:
                        subscription._enqueue({"type": "event",
                                               "event": event})
            if subscription.closed:
                return
        # (3b) repair8 1 / repair10: the cut's excluded obligations are
        # FULLY PERSISTED at loss-ingest time — the immutable frozen
        # first-run tuple plus the append-only ``excluded_gaps_more``
        # singleton runs for later, higher losses. There is NO
        # subscribe-time recomputation from the live global loss map: the
        # reader's total view is identical before/after ack+GC/cap
        # eviction of the globals, for first and late readers alike. The
        # safety net below only covers first-freezes that predate the
        # freeze mechanism's inputs.
        excluded = entry.get("excluded_high", {})
        frozen_gaps = entry.setdefault("excluded_gaps", {})
        frozen_more = entry.setdefault("excluded_gaps_more", {})
        owed = {stream_id: [frozen_gaps[stream_id]]
                + list(frozen_more.get(stream_id, ()))
                for stream_id in frozen_gaps}
        for stream_id, creation_high in excluded.items():
            lost_after = [seq for seq in self._lost.get(stream_id, ())
                          if seq > creation_high]
            if not lost_after or stream_id in owed:
                continue
            events = self._streams.get(stream_id)
            if events:
                subscription._overflow()  # replayable history: reset
                return
            high = live_map.get(stream_id, creation_high)
            owed[stream_id] = [(1, high)]
            frozen_gaps[stream_id] = (1, high)  # freeze on first sighting
        for stream_id, runs in sorted(owed.items()):
            for gap_from, gap_to in runs:  # each run -> one gap frame
                subscription._enqueue({
                    "type": "gap",
                    "gap": {"stream_id": stream_id,
                            "from_seq": gap_from, "to_seq": gap_to,
                            "count": gap_to - gap_from + 1}})
            if subscription.closed:
                return

    def subscribe_stream(self, stream_id: str, after_seq: int,
                         filters: dict | None = None) -> JournalSubscription:
        floor = self.retention_floor(stream_id)
        if after_seq + 1 < floor:
            # below the retention window: a structured gap error, never a
            # silently-late live-only stream
            raise _SubscriptionError(_error(
                "gap",
                gap={"stream_id": stream_id, "from_seq": after_seq + 1,
                     "to_seq": floor - 1, "count": floor - 1 - after_seq},
                retention_floor=floor,
                high_watermark=self.high_water(stream_id),
                snapshot_version=self._snapshot_version))
        subscription = JournalSubscription(
            self, {stream_id: after_seq}, filters=filters,
            target_stream=stream_id)
        if after_seq < self._lost_below.get(stream_id, 0):
            # trimmed lost metadata below the request: provably incomplete
            subscription._overflow()
        else:
            holes = [seq for seq in self._lost.get(stream_id, ())
                     if after_seq < seq <= self.high_water(stream_id)]
            if holes:
                # repair3 1: a LATE single-stream subscriber must see the
                # interior hole — born-reset (gap frame semantics stay with
                # replay); filters never influence loss detection
                subscription._overflow()
            else:
                for event in self._streams.get(stream_id, ()):
                    if event["seq"] > after_seq and (
                            not filters or self._matches(event, filters)):
                        subscription._enqueue({"type": "event",
                                               "event": event})
        self._subscriptions.append(subscription)
        return subscription

    def release_pages(self, client) -> None:
        """Drop a client's unfinished pagination slot (disconnect cleanup).

        Wire clients use random ids; without this their slots would leak
        and accumulate without bound (repair2 B / F-06).
        """
        self._active_pages.pop(self._client_key(client), None)

    def close_subscription(self, subscription: JournalSubscription) -> None:
        subscription.close()
        try:
            self._subscriptions.remove(subscription)
        except ValueError:
            pass
        # repair5 4: the reference disappeared — reclaim what it pinned
        self._prune_metadata()


class _SubscriptionError(Exception):
    """Raised by subscribe_* when the cursor itself is unusable."""

    def __init__(self, error: dict):
        super().__init__(error.get("error", "reset_required"))
        self.error = error
