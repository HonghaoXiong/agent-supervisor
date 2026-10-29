#!/usr/bin/env python3
"""Bounded ZCode app-server client. A permission broker, not an OS sandbox."""
import asyncio
import contextlib
import inspect
import itertools
import json
import math
import os
from pathlib import Path
import re
import signal
import stat
import time
import uuid

# P0-3: the worker-agnostic authority layer is extracted verbatim into
# authority.py; re-exported here so every protocol.* reference keeps working.
from .authority import (Authority, _edit_input_equivalent, _workflow_input_equivalent,
                        authorized_event, check_model_snapshot, execution_evidence,
                        goal_state_from_snapshot, inventory, model_execution_evidence,
                        observed_model, orchestration_tool_surface, own_event,
                        runtime_owned, workflow_jobs_from_snapshot)
from .common import (process_alive, read_regular, run_boundary_scan,
                     sha256_regular, workspace_lock, write_json)
from . import profile
from . import ui_projection
from .notify import send as send_notification
from .worker_health import ExitProjection

# P0.3 review C1: bounded close termination gradient. The worst case
# (grace + TERM + KILL) must fit WorkerGeneration's default retirement
# window (worker_health.OLD_CLOSE_BUDGET_S) so an evicted stdin-ignoring
# child really receives its signals and is reaped inside the window.
CLOSE_GRACE_S = 2.0   # stdin close -> first bounded wait (well-behaved child)

# Sequential per-process generations for cold clients wired to the UI exit
# sink (one cold child per execute run; PLAN §4.3 identity key).
_COLD_GENERATIONS = itertools.count(1)
CLOSE_TERM_S = 1.0    # SIGTERM -> second bounded wait
CLOSE_KILL_S = 1.0    # SIGKILL -> final bounded wait (then bounded give-up)


def redact_transport(value):
    """Keep execution evidence without persisting provider transport secrets."""
    if isinstance(value, dict):
        return {key: ('[redacted]' if re.sub(r'[^a-z]', '', key.lower()) in {
            'headers', 'requestheaders', 'responseheaders', 'authorization',
            'apikey', 'accesstoken', 'refreshtoken', 'cookie', 'setcookie',
            'clientsecret', 'password'} else redact_transport(item))
            for key, item in value.items()}
    if isinstance(value, list):
        return [redact_transport(item) for item in value]
    return value


# ---------------------------------------------------------------------------
# P2.2: keyed bounded event routing beside the evidence stores
# ---------------------------------------------------------------------------
# Per-subscription bound for the UNDELIVERED backlog. Delivered frames move
# into the subscription's task-owned history (its evidence slice) and are
# released when that task's evidence is finalized — retention grows only
# with active tasks, never with completed ones.
EVENT_ROUTER_BOUND = 1024


def event_router_mode():
    """CZS_EVENT_ROUTER=incremental opts every consumer loop of this
    process into the routed path; the default (and any other value) is
    the legacy scan-and-sleep behavior — the rollback switch."""
    return 'incremental' if os.environ.get('CZS_EVENT_ROUTER') == 'incremental' \
        else 'legacy'


class RouterOverflow(ValueError):
    """A bounded subscription's undelivered backlog overflowed (P2.2).

    The OWNING task fails with this error and runs its normal P0.1
    cleanup path (bounded stop, registry loss) — an internal control
    queue may never degrade into a UI-style gap."""
    pass


class BoundedSubscription:
    """One task's bounded subscription: wakeup-driven delivery plus a
    task-owned history that IS the evidence slice (released only after
    the task's evidence is finalized)."""

    def __init__(self, router, key, start_seq, bound,
                 deliver_unattributed=False):
        self._router = router
        self.key = key
        self.start_seq = start_seq
        self.bound = bound
        self.deliver_unattributed = deliver_unattributed
        self.pending = []            # [(seq, frame)] undelivered — bounded
        self.history = []            # delivered frames — the evidence slice
        self.overflowed = False
        self.released = False
        self.delivered_count = 0     # metrics
        self.wakeup_count = 0        # metrics: real wakeups consumed
        self._wakeup = asyncio.Event()
        # P2.2-repair1 (A/E): while the subscription sits on a PLACEHOLDER
        # key (None pre-thread-start, '' pre-session-create), keyed frames
        # that cannot match yet are stashed per future key and drained at
        # rekey — the rekey window can no longer lose them.
        self._stash = {}
        # P2.2-repair1 (D): the smallest seq this subscription ever saw on
        # the LIVE wire (delivered or stashed). Store-replay at rekey skips
        # seqs at/above it (already seen); below it lie only frames that
        # predate the subscription — exactly what replay must recover.
        self._live_floor = None

    def _is_placeholder(self):
        return self.key is None or self.key == ''

    # -- router side ------------------------------------------------------
    def _offer(self, seq, frame):
        if self.released:
            return
        if len(self.pending) >= self.bound:
            self.overflowed = True   # consumer lagged: the task will fail
            self._wakeup.set()
            return
        self.pending.append((seq, frame))
        self._wakeup.set()

    def _offer_live(self, seq, frame):
        if self._live_floor is None or seq < self._live_floor:
            self._live_floor = seq
        self._offer(seq, frame)

    def _stash_frame(self, future_key, seq, frame):
        if self._live_floor is None or seq < self._live_floor:
            self._live_floor = seq
        if sum(len(v) for v in self._stash.values()) >= self.bound:
            self.overflowed = True   # placeholder backlog is bounded too
            self._wakeup.set()
            return
        self._stash.setdefault(future_key, []).append((seq, frame))

    # -- consumer side ------------------------------------------------------
    def drain(self):
        """Take every undelivered frame into the task-owned history."""
        out, self.pending = self.pending, []
        self.history.extend(out)
        self.delivered_count += len(out)
        self._wakeup.clear()
        return out

    def narrow_start(self, start_seq):
        """Raise the delivery floor to a server-authoritative seq (the
        session/subscribe eventSeq). The subscription is created EARLY
        (start_seq 0, before session/create) so no frame of this session
        can slip past the read loop; once the subscribe response names
        the real baseline, everything at or below it is dropped — exactly
        the frames the legacy `seq <= start_event_seq` filter skipped."""
        self.start_seq = start_seq
        self.pending = [(seq, frame) for seq, frame in self.pending
                        if seq is None or seq > start_seq]
        self.history = [(seq, frame) for seq, frame in self.history
                        if seq is None or seq > start_seq]

    async def wait_new(self, timeout_s):
        """Wait for the next publish. The caller derives ``timeout_s``
        from its absolute budget (budget.wait_s/remaining_s), so the
        deadline semantics of the old polling loop are preserved; False
        means the timeout elapsed (the caller re-checks its budget)."""
        if self.pending or self.overflowed or self.released:
            return True
        self.wakeup_count += 1
        if self._router is not None:
            self._router.wakeup_total += 1
        try:
            await asyncio.wait_for(self._wakeup.wait(), timeout_s)
            return True
        except asyncio.TimeoutError:
            return False

    def release(self):
        """Drop the task-owned frames and leave the router. Called only
        after the task's evidence is finalized (raw wire evidence stays
        in the on-disk frame log — it is untouched by routing)."""
        self.released = True
        self.pending = []
        self.history = []
        if self._router is not None:
            self._router._release(self)

    def rekey(self, key, replay_pairs=()):
        """Switch the routing key (the authoritative id arrives only
        after session/create / thread/start).

        ``replay_pairs`` (P2.2-repair1 D): an explicit ``(seq, frame)``
        enumeration of the client's store, in seq order — the production
        callers pass it (codex: the notification store with its parallel
        seq record; frames predating this subscription are recovered
        here). Pairs at/above ``_live_floor`` were already seen on the
        live wire (delivered or stashed) and are skipped, so replay can
        never duplicate. The placeholder stash for the new key drains
        last; stashes for other keys are dropped (their frames remain in
        the store for their own subscribers' replay)."""
        self.key = key
        if not replay_pairs and self._router is not None:
            replay_pairs = self._router.replay(
                key, self.start_seq,
                include_unattributed=self.deliver_unattributed)
        for seq, frame in replay_pairs:
            if seq is None or seq <= self.start_seq:
                continue
            if self._live_floor is not None and seq >= self._live_floor:
                continue
            frame_key = self._router._key_of(frame) \
                if self._router is not None else None
            if frame_key == key or (frame_key is None
                                    and self.deliver_unattributed):
                self._offer(seq, frame)
        for seq, frame in self._stash.pop(key, []):
            self._offer(seq, frame)
        self._stash.clear()
        self._wakeup.set()


class EventRouter:
    """Keyed bounded fan-out beside a client's legacy evidence store.

    ``key_of``/``seq_of`` extract the routing identity (zcode: event
    ``sessionId``/``seq``; codex: notification ``threadId``/list index).
    ``store`` (optional) enables replay-at-subscribe for consumers whose
    legacy behavior drained the whole history (codex ``processed=0``).
    Unattributed frames (key None) are broadcast to subscriptions that
    asked for them — that is exactly what the legacy per-frame handlers
    processed."""

    def __init__(self, key_of, seq_of, store=None, store_seqs=None,
                 bound=None):
        self._key_of = key_of
        self._seq_of = seq_of       # live-wire seq extraction (zcode events)
        # P2.2-repair1 D: a store-carrying router OWNS its replay source —
        # production codex passes its notification store plus the aligned
        # seq record, so rekey's replay is a live production path (never
        # the old seq_of=None dead branch). ``store_seqs`` may be shorter
        # than the store for directly-constructed routers; the positional
        # fallback keeps replay usable there.
        self._store = store
        self._store_seqs = store_seqs
        # resolved at call time so tests can tighten the bound
        self.bound = bound if bound is not None else EVENT_ROUTER_BOUND
        self._subs = []
        self.publish_count = 0
        self.wakeup_total = 0   # consumer wakeups consumed (P2.2 metrics)

    def replay(self, key, start_seq, include_unattributed=False):
        """Matching (seq, frame) pairs of the store above ``start_seq``.

        Seq resolution: the aligned record's value — a None or missing
        record entry falls back to the frame's own seq via ``seq_of``,
        then to the store position (repair2 F4: the record NEVER wins
        with None). ``include_unattributed`` (repair2 F3) also returns
        key-None frames, so subscriptions that asked for the
        unattributed broadcast recover pre-subscription unattributed
        frames too — the legacy processed=0 drain delivered them."""
        if self._store is None:
            return []
        out = []
        for index, frame in enumerate(list(self._store)):
            seq = None
            if self._store_seqs is not None and index < len(self._store_seqs):
                seq = self._store_seqs[index]
            if seq is None:
                seq = self._seq_of(frame)
            if seq is None:
                seq = index + 1
            if seq <= start_seq:
                continue
            frame_key = self._key_of(frame)
            if frame_key == key or (frame_key is None
                                    and include_unattributed):
                out.append((seq, frame))
        return out

    def subscribe(self, key, start_seq, deliver_unattributed=False):
        sub = BoundedSubscription(self, key, start_seq, self.bound,
                                  deliver_unattributed=deliver_unattributed)
        self._subs.append(sub)
        return sub

    def publish(self, frame, seq=None):
        self.publish_count += 1
        key = self._key_of(frame)
        if seq is None:
            seq = self._seq_of(frame)
        for sub in list(self._subs):
            if sub.released or seq is None or seq <= sub.start_seq:
                continue
            if key == sub.key or (key is None and sub.deliver_unattributed):
                sub._offer_live(seq, frame)
            elif sub._is_placeholder() and key is not None:
                # P2.2-repair1 A/E: keyed frame under a placeholder key —
                # stash it for the future rekey instead of dropping it
                sub._stash_frame(key, seq, frame)

    def wake_all(self):
        """P2.2-repair1 B: wake every waiter (transport failure paths) so
        consumer loops re-check client.failure immediately — the legacy
        fail-fast classification is preserved."""
        for sub in list(self._subs):
            sub._wakeup.set()

    def has_subscriber_for(self, key):
        return any(not sub.released and sub.key == key for sub in self._subs)

    def has_placeholder(self):
        """True while any subscription still sits on a pre-authority
        placeholder key — the store must keep keyed frames so their
        receipts can scan them and their rekey can replay them."""
        return any(not sub.released and sub._is_placeholder()
                   for sub in self._subs)

    def _release(self, sub):
        with contextlib.suppress(ValueError):
            self._subs.remove(sub)

    def metrics(self):
        return {'active_subscriptions': len(self._subs),
                'retained_frames': sum(len(s.pending) + len(s.history)
                                       for s in self._subs)}


class TombstoneEvictionError(ValueError):
    """A pending-response tombstone had to be evicted (TTL or capacity).

    Subclasses :class:`ValueError` on purpose so the fail-closed path keeps
    flowing through the drivers' existing exception families unchanged.
    """


class PendingTombstones:
    """Bounded registry of request ids abandoned by a LOCAL timeout/cancel.

    Only ids whose frame was already sent on the wire and whose pending entry
    was then removed by the local caller (an ``asyncio`` timeout or
    cancellation) are recorded (PLAN §7 P0.2). A late response for such an id
    is consumed as a KNOWN-LATE frame instead of failing the shared client;
    responses for every other id keep the ``Unmatched protocol response``
    fail-closed handling.

    Capacity and TTL are resource contracts, not CLI compatibility
    measurements. Evicting an entry for either reason is never silent:
    ``record``/``consume`` raise :class:`TombstoneEvictionError` so the client
    can fail closed immediately with an explainable terminal state (an orderly
    transport reset belongs to the P0.3 worker-generation path). The registry
    never grows past ``capacity``, entries stay until their TTL so duplicates
    cannot degrade into ignorable unknowns, and ``clock`` is injectable for
    deterministic tests.
    """

    DEFAULT_CAPACITY = 1024
    DEFAULT_TTL_S = 300.0

    def __init__(self, capacity=DEFAULT_CAPACITY, ttl_s=DEFAULT_TTL_S,
                 clock=time.monotonic):
        if isinstance(capacity, bool) or not isinstance(capacity, int) or capacity <= 0:
            raise ValueError('Tombstone capacity must be a positive integer')
        if (isinstance(ttl_s, bool) or not isinstance(ttl_s, (int, float))
                or not math.isfinite(ttl_s) or ttl_s <= 0):
            raise ValueError('Tombstone TTL must be a positive finite number')
        self.capacity = capacity
        self.ttl_s = float(ttl_s)
        self._clock = clock
        self._entries = {}

    def __len__(self):
        return len(self._entries)

    def record(self, rid, reason):
        """Register one locally abandoned request id (its send had succeeded).

        Raises :class:`TombstoneEvictionError` — without recording ``rid`` —
        when any older entry is already past its TTL or the registry is at
        capacity: both are fail-closed resource events, never silent evictions.
        """
        now = self._clock()
        expired = sorted(old_rid for old_rid, entry in self._entries.items()
                         if now - entry['recorded_at'] > self.ttl_s)
        if expired:
            raise TombstoneEvictionError(
                'Pending tombstones expired before eviction: '
                + ', '.join(expired) + f' (TTL {self.ttl_s}s); fail-closed')
        if len(self._entries) >= self.capacity:
            raise TombstoneEvictionError(
                f'Pending tombstone capacity {self.capacity} exhausted; fail-closed')
        self._entries[rid] = {'reason': str(reason), 'recorded_at': now,
                              'late_kind': None, 'late_count': 0,
                              'last_late_at': None}

    def consume(self, rid, kind):
        """Mark one late response frame against its tombstone.

        Returns a copy of the recorded entry when ``rid`` is a live tombstone
        — the frame is a known-late response and must be consumed without
        resolving, replaying or failing anything — and ``None`` when ``rid``
        was never tombstoned (the caller keeps its unmatched fail-closed
        handling). Duplicate frames keep incrementing ``late_count``: the
        entry survives until its TTL, so a duplicate can never become an
        ignorable unknown. Raises :class:`TombstoneEvictionError` when this
        entry's TTL already elapsed.
        """
        entry = self._entries.get(rid)
        if entry is None:
            return None
        now = self._clock()
        if now - entry['recorded_at'] > self.ttl_s:
            del self._entries[rid]
            raise TombstoneEvictionError(
                f'Pending tombstone for {rid} expired (TTL {self.ttl_s}s) '
                'before its late response; fail-closed')
        entry['late_count'] += 1
        entry['late_kind'] = kind
        entry['last_late_at'] = now
        return dict(entry)


class ProtocolClient:
    def __init__(self, workspace, output, permission, config=None, orchestration=None,
                 tombstones=None, exit_event_sink=None):
        # orchestration: the validated packet block (or None) backing the
        # host-side subagent runtime config answers; never client input.
        self.orchestration = orchestration
        self.workspace = workspace
        self.output = output
        self.permission = permission
        self.config = config
        self.pending = {}
        # P0.2: known-late response bookkeeping for requests removed from
        # ``pending`` by a local timeout/cancel after a successful wire send.
        self.tombstones = tombstones if tombstones is not None else PendingTombstones()
        # P0.3: generation identity (stamped by the hub's WorkerGeneration
        # holder) and the exactly-once awaited-exit projection. P0.4 round 2:
        # an optional exit_event_sink links that projection to the UI
        # process.exited sideband (fresh construction only — existing custom
        # projections are never overwritten here; the resident path chains).
        self.generation = None
        self.exit_projection = (ExitProjection(on_event=exit_event_sink)
                                if exit_event_sink is not None
                                else ExitProjection())
        if exit_event_sink is not None and self.generation is None:
            # A cold client wired to the UI exit sink gets its own sequential
            # integer generation at creation (P0.3 per-process identity
            # semantics — {process_scope, generation} is the exit emission
            # key). Shared-resident clients are NOT wired here and keep
            # generation=None so the hub's WorkerGeneration stamp (or a test
            # double's own identity) is propagated unchanged.
            self.generation = next(_COLD_GENERATIONS)
        self.events = []
        self.responses = []
        self.counter = 0
        self.failure = None
        self.process = None
        self.tasks = []
        self.on_event = None
        self.stderr = None
        self.wire = None
        self.reader = None
        self.closing = False
        # P2.2: keyed bounded routing beside `events` (legacy consumers
        # never touch it; subscribe_events creates it on first use)
        self._event_router = None

    def subscribe_events(self, session_id, start_seq):
        """P2.2: one task's bounded subscription to this client's
        session/event stream, keyed by sessionId from ``start_seq`` on.
        Under CZS_EVENT_ROUTER=legacy nothing routes and this is unused."""
        if self._event_router is None:
            self._event_router = EventRouter(
                key_of=lambda event: event.get('sessionId')
                if isinstance(event, dict) else None,
                seq_of=lambda event: event.get('seq')
                if isinstance(event, dict) else None)
        return self._event_router.subscribe(session_id, start_seq)

    async def start(self):
        if self.config is None:
            raise ValueError('A runtime configuration is required to start ZCode')
        if self.process is not None or self.closing:
            raise ValueError('Protocol client cannot be started twice')
        try:
            command = self.config.command()
            self.stderr = self.output.with_suffix('.stderr.log').open('x')
            self.wire = self.output.with_suffix('.events.jsonl').open('x')
            self.process = await asyncio.create_subprocess_exec(
                *command, 'app-server', '--stdio', '--cwd', str(self.workspace),
                stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
                stderr=self.stderr, env=self.config.environment(), start_new_session=True,
                limit=8*1024**2)
            self.reader = asyncio.create_task(self.read_loop())
        except BaseException:
            await self.close()
            raise

    async def send(self, frame):
        if self.process is None or self.process.stdin is None or self.closing:
            raise EOFError('ZCode protocol client is not running')
        self.process.stdin.write((json.dumps(frame, ensure_ascii=False)+'\n').encode())
        await self.process.stdin.drain()

    async def request(self, method, params):
        if self.failure:
            raise EOFError(self.failure)
        self.counter += 1
        rid = f'host-{self.counter}'
        future = asyncio.get_running_loop().create_future()
        self.pending[rid] = future
        sent = False
        try:
            await self.send({'id':rid, 'method':method, 'params':params})
            sent = True
            return await future
        finally:
            # P0.2 sole-owner terminal cleanup. ``pending`` is the identity
            # registry: whoever removes the entry first owns the tombstone
            # decision exactly once. The cleanup records only when IT removed
            # its own still-cancelled future — i.e. the local caller (asyncio
            # timeout or cancel) ended the request with no response consumed
            # by read_loop (round-2 blocker 1: a response already delivered
            # leaves the entry popped and the future done-with-result, so no
            # tombstone and any duplicate stays unknown fail-closed). A cancel
            # inside drain leaves the future uncancelled (ambiguous send), and
            # teardown / already-failed clients are gated out as well.
            removed = self.pending.pop(rid, None)
            if (removed is future and sent and future.cancelled()
                    and not self.closing and self.failure is None):
                try:
                    self.tombstones.record(rid, 'local-timeout-or-cancel')
                except TombstoneEvictionError as error:
                    # Capacity/TTL eviction is never silent: fail closed now.
                    # The cancellation itself still propagates unchanged.
                    self.fail(error)

    async def reverse(self, frame):
        method, params = frame['method'], frame.get('params', {})
        if not isinstance(params, dict):
            raise ValueError('Host request parameters must be an object')
        if method == 'interaction/requestPermission':
            response = self.permission(params)
        elif method == 'session/requestRuntimePreferences':
            response = {'nativeSearchEnhancementsEnabled':False,
                        'subagentRuntimeConfigEnabled':bool(
                            self.orchestration and 'agents' in self.orchestration),
                        'memoryEnabled':False,
                        'askUserQuestionAutoResolutionEnabled':False,
                        'modelContextBudgetStrategy':'preflight-v1'}
        elif method == 'subagents/readRuntimeConfig':
            from .orchestration import runtime_config_answer
            answer = runtime_config_answer(self.orchestration)
            if answer is None:
                await self.send({'id':frame['id'], 'error':{
                    'code':-32601,
                    'message':'Subagent runtime config was not enabled by this task contract'}})
                return
            response = answer
        else:
            await self.send({'id':frame['id'], 'error':{
                'code':-32601, 'message':f'Unsupported host request: {method}'}})
            return
        self.wire.write(json.dumps({'host_response_to':frame['id'], 'method':method,
                                    'result':response}, ensure_ascii=False)+'\n')
        self.wire.flush()
        await self.send({'id':frame['id'], 'result':response})

    def fail(self, error):
        # P0.2 (round-2 minor 3): the FIRST terminal failure is the
        # explainable cause and is never overwritten — a later stream error
        # (e.g. the EOF that often follows an eviction fail-close) keeps it.
        # Still idempotent for pending futures: any straggler receives a
        # terminal exception so every waiter ends explainably.
        if self.failure is None:
            self.failure = str(error) or type(error).__name__
        # P2.2-repair1 B: routed waiters must re-check client.failure now,
        # not after their full budget — legacy fail-fast preserved.
        if self._event_router is not None:
            self._event_router.wake_all()
        for future in self.pending.values():
            if not future.done():
                future.set_exception(error)
        self.pending.clear()

    def reverse_done(self, task):
        # P2.2: the exception is read FIRST (an unobserved one would be
        # lost at gc time); the completed task then leaves the active
        # registry so finished work never accumulates.
        failed = None
        if not task.cancelled():
            failed = task.exception()
        with contextlib.suppress(ValueError):
            self.tasks.remove(task)
        if failed is not None:
            self.fail(failed)

    async def read_loop(self):
        try:
            while line := await self.process.stdout.readline():
                if not line.strip():
                    continue
                frame = json.loads(line)
                if not isinstance(frame, dict):
                    raise ValueError('Protocol frame is not an object')
                if 'method' in frame:
                    # Unknown host callbacks can contain account transport data.
                    known = frame['method'] in ('interaction/requestPermission',
                        'session/requestRuntimePreferences', 'subagents/readRuntimeConfig',
                        'session/event', 'state.updated')
                    self.wire.write(json.dumps(redact_transport(frame) if known else {
                        'method':frame['method'], 'id':frame.get('id')}, ensure_ascii=False)+'\n')
                    self.wire.flush()
                    if 'id' in frame:
                        task = asyncio.create_task(self.reverse(frame))
                        self.tasks.append(task)
                        task.add_done_callback(self.reverse_done)
                    elif frame['method'] == 'session/event':
                        if not isinstance(frame.get('params'), dict):
                            raise ValueError('Session event parameters must be an object')
                        event = frame['params']
                        if self._event_router is not None:
                            # P2.2 routed mode: subscriptions own their
                            # frames (bounded, released after evidence
                            # finalization); the raw wire log above keeps
                            # the persistent record untouched
                            self._event_router.publish(event)
                        else:
                            self.events.append(event)
                        if self.on_event:
                            self.on_event(event)
                elif frame.get('id') in self.pending:
                    future = self.pending.pop(frame['id'])
                    if future.cancelled() and not self.closing \
                            and self.failure is None:
                        # P0.2 (round-2 blocker 2): the request was locally
                        # removed (timeout/cancel) but its cleanup task has
                        # not necessarily run yet — THIS frame is the first
                        # late response. The reader took the pending entry,
                        # so it owns the record: atomically tombstone and
                        # consume the frame in one loop turn (the later
                        # cleanup finds its entry gone and cannot record a
                        # second time). The frame is audited exactly once,
                        # through the same consume path as every other late
                        # frame.
                        self.tombstones.record(frame['id'],
                                               'local-timeout-or-cancel')
                        self.consume_late(frame)
                        continue
                    if future.done():
                        continue
                    if 'error' in frame:
                        future.set_exception(ValueError(str(frame['error'])))
                    elif 'result' in frame:
                        future.set_result(frame['result'])
                    else:
                        raise ValueError('Response has no result or error')
                else:
                    self.consume_late(frame)
            if not self.closing:
                raise EOFError('ZCode protocol stream closed')
        except asyncio.CancelledError:
            return
        except Exception as error:
            self.fail(error)

    def consume_late(self, frame):
        """Consume one response frame whose request was locally abandoned.

        A known-late frame (its id is in the tombstone registry) is recorded
        to the evidence wire and dropped: nothing is resolved, replayed or
        failed. Everything else keeps the fail-closed unmatched behavior; a
        tombstone whose TTL already elapsed fails closed with the eviction
        reason instead of an anonymous unmatched error.
        """
        rid = frame.get('id')
        kind = ('error' if 'error' in frame
                else 'result' if 'result' in frame else None)
        entry = self.tombstones.consume(rid, kind)
        if entry is None:
            raise ValueError('Unmatched protocol response')
        if kind is None:
            raise ValueError('Response has no result or error')
        self.wire.write(json.dumps({'late_response_to': rid,
                                    'tombstone_reason': entry['reason'],
                                    'late_kind': kind,
                                    'duplicate': entry['late_count'] > 1},
                                   ensure_ascii=False)+'\n')
        self.wire.flush()

    async def close(self):
        self.closing = True
        if self.process is not None:
            if self.process.stdin is not None:
                self.process.stdin.close()
            # P0.3 review C1: a BOUNDED termination gradient whose worst
            # case (grace + TERM + KILL = 4 s) fits the worker gate's
            # default retirement window, so evicting a stdin-ignoring but
            # signal-responsive child actually delivers SIGTERM/SIGKILL and
            # awaits the reaped exit inside the cleanup budget. The previous
            # shape (5 s first wait, unbounded final wait) could overrun any
            # bounded window and leak an un-signalled child. An unkillable
            # process only ever costs the last bounded wait — no unbounded
            # wait and no fabricated exit evidence.
            exited = False
            try:
                await asyncio.wait_for(self.process.wait(), CLOSE_GRACE_S)
                exited = True
            except asyncio.TimeoutError:
                pass
            if not exited:
                try:
                    os.killpg(self.process.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
                try:
                    await asyncio.wait_for(self.process.wait(), CLOSE_TERM_S)
                    exited = True
                except asyncio.TimeoutError:
                    pass
            if not exited:
                try:
                    os.killpg(self.process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
                try:
                    await asyncio.wait_for(self.process.wait(), CLOSE_KILL_S)
                    exited = True
                except asyncio.TimeoutError:
                    pass  # bounded best-effort ends without exit evidence
            if exited:
                # P0.3: a wait in the gradient above actually completed — the
                # only place process.exited may be projected, exactly once
                # per client (the projection itself rejects non-integer
                # codes; duplicate close emits nothing).
                self.exit_projection.observe_awaited_exit(
                    self.process.returncode, generation=self.generation)
        tasks = ([self.reader] if self.reader is not None else []) + self.tasks
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        for future in self.pending.values():
            if not future.done():
                future.cancel()
        self.pending.clear()
        for stream in (self.wire, self.stderr):
            if stream is not None and not stream.closed:
                stream.close()


def normalized_absolute(value, label):
    if not isinstance(value, str) or not value or '\x00' in value:
        raise ValueError(label + ' must be a nonempty absolute path')
    path = Path(value)
    if not path.is_absolute() or '..' in path.parts or str(path) != value:
        raise ValueError(label + ' must be a normalized absolute path')
    return path


def normalized_relative(value):
    if not isinstance(value, str) or not value or '\x00' in value:
        raise ValueError('Allowed paths must be normalized workspace-relative files')
    path = Path(value)
    if (path.is_absolute() or value == '.' or '..' in path.parts
            or path.as_posix() != value):
        raise ValueError('Allowed paths must be normalized workspace-relative files')
    return value


def checked_locations(raw_workspace, output, config, packet_path=None):
    """Check controller/worker separation before creating any evidence files."""
    raw_workspace = Path(raw_workspace)
    work_root = Path(config.work_root).resolve()
    workspace = raw_workspace.resolve()
    if raw_workspace.is_symlink() or not workspace.is_dir():
        raise ValueError('Workspace must be a real directory')
    if workspace == work_root or not workspace.is_relative_to(work_root):
        raise ValueError('Workspace must be a child directory of work_root')
    # A lexical path below work_root must not cross a symlink on the way down.
    current = raw_workspace
    while current != current.parent:
        if current.is_symlink():
            raise ValueError('Workspace path crosses a symlink')
        if current == work_root:
            break
        current = current.parent
    output = Path(output).resolve()
    if output.suffix != '.json':
        raise ValueError('Evidence output must use a .json suffix')
    if not output.is_relative_to(work_root):
        raise ValueError('Controller evidence must be under work_root')
    protected = [output, Path(config.state_dir).resolve(), Path(config.cache_dir).resolve()]
    if packet_path is not None:
        protected.append(Path(packet_path).resolve())
    if any(path.is_relative_to(workspace) for path in protected):
        raise ValueError('Packet, evidence, state and cache must be outside worker workspace')
    for location in (Path(config.state_dir).resolve(), Path(config.cache_dir).resolve()):
        if workspace.is_relative_to(location):
            raise ValueError('Workspace must not be inside controller state or cache')
    for ext in ('.json', '.stderr.log', '.events.jsonl', '.before.json'):
        path = output.with_suffix(ext)
        if path.exists() or path.is_symlink():
            raise ValueError('Use a new output basename')
    return workspace, output


def validate_packet(packet):
    """Validate authority before it can reach a model or a permission callback."""
    if not isinstance(packet, dict):
        raise ValueError('Task packet must be an object')
    if set(packet) - {'workspace', 'prompt', 'allowed_files', 'commands', 'trusted_hashes',
                      'model', 'orchestration'}:
        raise ValueError('Unknown task packet field')
    if 'orchestration' in packet:
        from .orchestration import validate_orchestration
        validate_orchestration(packet['orchestration'])
    if 'model' in packet:
        model = packet['model']
        if (not isinstance(model, dict) or not {'providerId', 'modelId'} <= set(model)
                or set(model) - {'providerId', 'modelId', 'options'}
                or any(not isinstance(model[key], str) or not model[key].strip()
                       or model[key] != model[key].strip() or '\x00' in model[key]
                       for key in ('providerId', 'modelId'))):
            raise ValueError('Model must contain explicit providerId and modelId strings')
        if 'options' in model:
            options = model['options']
            if (not isinstance(options, dict) or set(options) != {'reasoningLevel'}
                    or not isinstance(options['reasoningLevel'], str)
                    or not options['reasoningLevel'].strip()
                    or options['reasoningLevel'] != options['reasoningLevel'].strip()
                    or '\x00' in options['reasoningLevel']):
                raise ValueError('Model options must contain only a nonempty reasoningLevel string')
    normalized_absolute(packet.get('workspace'), 'Workspace')
    if not isinstance(packet.get('prompt'), str) or not packet['prompt'].strip():
        raise ValueError('Task prompt must be a nonempty string')
    allowed = packet.get('allowed_files')
    if not isinstance(allowed, list):
        raise ValueError('Explicit allowed_files required')
    for value in allowed:
        normalized_relative(value)
    if len(set(allowed)) != len(allowed):
        raise ValueError('Duplicate allowed file')
    commands = packet.get('commands', [])
    if not isinstance(commands, list):
        raise ValueError('Commands must be an array')
    ids, texts = set(), set()
    for command in commands:
        if not isinstance(command, dict) or set(command) - {
                'id', 'command', 'max_uses', 'max_timeout_ms', 'required'}:
            raise ValueError('Invalid command specification')
        name, text = command.get('id'), command.get('command')
        if not isinstance(name, str) or not re.fullmatch(r'[A-Za-z0-9][A-Za-z0-9_.-]*', name):
            raise ValueError('Command ID must be a nonempty identifier')
        if not isinstance(text, str) or not text.strip() or '\x00' in text:
            raise ValueError('Command must be a nonempty string')
        if name in ids or text in texts:
            raise ValueError('Command IDs and command strings must be unique')
        ids.add(name)
        texts.add(text)
        uses = command.get('max_uses', 1)
        timeout = command.get('max_timeout_ms', 120000)
        if type(uses) is not int or uses <= 0:
            raise ValueError('Command max_uses must be a positive integer')
        if (isinstance(timeout, bool) or not isinstance(timeout, (int, float))
                or not math.isfinite(timeout) or timeout <= 0):
            raise ValueError('Command timeout must be positive and finite')
        if type(command.get('required', True)) is not bool:
            raise ValueError('Command required must be a boolean')
    trusted = packet.get('trusted_hashes', {})
    if not isinstance(trusted, dict):
        raise ValueError('Trusted hashes must be an object')
    for name, digest in trusted.items():
        normalized_absolute(name, 'Trusted input')
        if not isinstance(digest, str) or not re.fullmatch(r'[a-f0-9]{64}', digest):
            raise ValueError('Trusted inputs require lowercase SHA-256 hashes')


async def probe(args, config):
    workspace, output = checked_locations(args.workspace, args.out, config)
    inventory(workspace)
    output.parent.mkdir(parents=True, exist_ok=True)
    state_dir = Path(config.state_dir)
    state_dir.mkdir(parents=True, exist_ok=True)
    with workspace_lock(workspace, state_dir):
        client = ProtocolClient(workspace, output, lambda p: {
            'decision':'deny', 'reason':'probe only'}, config)
        try:
            await client.start()
            snapshot = await asyncio.wait_for(client.request('session/create', {
                'workspace':{'workspacePath':str(workspace), 'workspaceKey':str(workspace)},
                'mode':'build', 'titleGenerationEnabled':False,
                'mcpServers':[], 'toolAllowlist':['Read','Write','Edit','Bash'],
                'dynamicWorkflowEnabled':False}), 90)
            write_json(output, redact_transport(snapshot))
            print(json.dumps({'probe_ok':True, 'keys':list(snapshot), 'result_file':str(output)}))
            return True
        finally:
            await client.close()


def short_session(session_id):
    """Compact, non-sensitive session marker for operator notifications."""
    text = str(session_id) if session_id else ''
    if not text:
        return '未建立'
    return text[:8] + '…' + text[-4:] if len(text) > 16 else text


# Fixed, notification-safe outcome kinds; labels never embed raw error text.
OUTCOME_LABELS = {
    'delivered': '交付检查通过',
    'evidence-rejected': '命令证据拒收',
    'postflight-rejected': '终态后审计拒收',
    'timeout': '超时',
    'worker-failed': 'worker 失败',
    'preflight-rejected': '派发前拒收',
    'protocol-error': '协议错误',
    'scope-error': '范围检查失败',
}


def outcome_label(kind, problems=None):
    text = OUTCOME_LABELS.get(kind, kind)
    if kind == 'evidence-rejected' and problems:
        text += f'（{len(problems)} 项）'
    return text


async def execute(args, config, ui_emitter=None):
    if args.resume and not re.fullmatch(r'sess_[A-Za-z0-9_-]+', args.resume):
        raise ValueError('Invalid resume session ID')
    if (isinstance(args.timeout, bool) or not isinstance(args.timeout, (int, float))
            or not math.isfinite(args.timeout) or args.timeout <= 0):
        raise ValueError('Run timeout must be positive and finite')
    raw_packet = Path(args.packet)
    packet_info = raw_packet.lstat()
    if not stat.S_ISREG(packet_info.st_mode) or packet_info.st_nlink != 1:
        raise ValueError('Packet must be an ordinary, unshared file')
    packet_path = raw_packet.resolve()
    packet = json.loads(read_regular(packet_path))
    validate_packet(packet)
    raw_workspace = Path(packet['workspace'])
    workspace, output = checked_locations(raw_workspace, args.out, config, packet_path)
    for name in packet['allowed_files']:
        if (workspace/name).resolve().relative_to(workspace).as_posix() != name:
            raise ValueError('Allowed paths must be normalized workspace-relative files')
        if (workspace/name).is_dir():
            raise ValueError('Allowed paths must identify files, not directories')
    output.parent.mkdir(parents=True, exist_ok=True)
    state_dir = Path(config.state_dir).resolve()
    state_dir.mkdir(parents=True, exist_ok=True)
    # P0.4 round 2: UI projection stream label for this cold run (the hub
    # names the output file after its task id).
    ui_task_id = Path(args.out).stem
    with workspace_lock(workspace, state_dir):
        before = await run_boundary_scan(inventory, workspace)
        habit_profile = profile.resolve_profile_path(config)
        habits = None
        if habit_profile is not None:
            if Path(habit_profile).resolve().is_relative_to(Path(config.work_root).resolve()):
                raise ValueError('Habit profile must live outside work_root')
            loaded_profile = profile.load_profile(habit_profile)
            planned = sorted(set(packet.get('allowed_files', [])))
            packet, habits = profile.apply_profile(
                loaded_profile, packet, workspace, sorted(before) + planned)
            habits['applicability_paths'] = {'inventory': len(before), 'planned': planned}
            habits['profile'] = {'path': str(habit_profile),
                                 'sha256': profile.digest_file(habit_profile)}
            validate_packet(packet)
        authority = Authority(
            packet, packet_path, before,
            ui_emit=(ui_projection.authority_hook(
                ui_emitter, ui_task_id, 'zcode', 'cold')
                if ui_emitter is not None else None))
        authority.provide_scan(await run_boundary_scan(inventory, workspace))
        authority.check_frozen()
        # Reserve the basename atomically so another worker cannot overwrite it.
        with output.open('x') as stream:
            json.dump({'delivery_ok': False, 'state': 'starting'}, stream)
        write_json(output.with_suffix('.before.json'), before)
        # Test doubles from parallel work may predate the orchestration and
        # P0.4 parameters; pass each only when the constructor accepts it.
        client_params = {}
        constructor = inspect.signature(ProtocolClient.__init__).parameters
        if 'orchestration' in constructor:
            client_params['orchestration'] = authority.orchestration
        if ui_emitter is not None and ui_emitter.enabled \
                and 'exit_event_sink' in constructor:
            client_params['exit_event_sink'] = ui_projection.make_exit_sink(
                ui_emitter, 'cold', 'zcode')
        client = ProtocolClient(workspace, output, authority.decide, config,
                                **client_params)
        input_id = 'input_'+uuid.uuid4().hex
        def receive_event(event):
            # Native orchestration bookkeeping: every parent turn observed in
            # this session becomes part of the authorized turn set, and
            # subagent lifecycle events populate the child registry.
            authority.note_event(event)
            if (event.get('type') == 'turn.started'
                    and event.get('sessionId') == authority.session_id):
                authority.note_turn(event.get('turnId'))
            if (event.get('sessionId') == authority.session_id
                    and event.get('type') == 'turn.started'
                    and event.get('payload',{}).get('inputId') == input_id):
                authority.turn_id = event['turnId']
        client.on_event = receive_event
        # P2.2 routed mode: subscribe on the constructed client BEFORE any
        # session traffic (the read loop starts routing from the first
        # frame); the floor is narrowed once session/subscribe answers.
        # Duck-typed: clients/doubles without the API keep the legacy path.
        router_sub = None  # declared here: the finally releases it
        if (event_router_mode() == 'incremental'
                and hasattr(client, 'subscribe_events')):
            router_sub = client.subscribe_events(args.resume or '', 0)
        result = {'delivery_ok':False,'workspace':str(workspace),'input_id':input_id,
                  'generation':uuid.uuid4().hex, 'scientific_acceptance':'pending_codex_review'}
        if habits is not None:
            result['habits'] = habits
        requested_model = packet.get('model')
        if requested_model is not None:
            result['requested_model'] = requested_model
        deadline = time.monotonic()+args.timeout
        started = time.monotonic()
        # Test doubles build minimal duck-typed configs; missing means silent.
        notify_enabled = getattr(config, 'notify', False)
        run_label = Path(args.out).stem
        dispatched = False
        turn_succeeded = False
        def remaining():
            return max(0.001, deadline-time.monotonic())
        async def rpc(method, params):
            return await asyncio.wait_for(client.request(method, params), remaining())
        # N4c: the worker process discovers/reads skills at startup, so the
        # closure must be bound BEFORE client.start(), not between start and
        # session/create. Workflow packets launch script actors and skills
        # packets expose the Skill tool; both can read bundled skills, so both
        # bind the controller-approved closure.
        skill_binding = None
        if authority.orchestration and ('workflow' in authority.orchestration
                                        or 'skills' in authority.orchestration):
            from .runtime import ensure_bundled_skills, closure_digest
            try:
                bound_target = Path(ensure_bundled_skills(config)).resolve()
            except ValueError as error:
                # Pre-dispatch closure refusal: no session is created, the
                # failure and its evidence are recorded, delivery stays false.
                # The return contract is the SAME bool as the normal exit
                # (delivery_ok): returning a truthy non-bool here made
                # cli.main treat a refusal as success (CLI exit 0).
                result['skill_closure_refused'] = str(error)
                result['delivery_ok'] = False
                result['outcome'] = 'preflight-rejected'
                write_json(output, redact_transport(result))
                print(json.dumps({'ok': False, 'error': str(error)[:300]}))
                return False
            skill_binding = {'target': str(bound_target),
                             'digest': closure_digest(bound_target)}
            result['skill_closure_binding'] = skill_binding
        try:
            await asyncio.wait_for(client.start(), remaining())
            from .orchestration import session_options
            options = session_options({'workspace':{'workspacePath':str(workspace),'workspaceKey':str(workspace)},
                                       'mcpServers':[], 'toolAllowlist':['Read','Write','Edit','Bash'],
                                       'dynamicWorkflowEnabled':False}, authority.orchestration)
            if args.resume:
                reg = json.loads(read_regular(state_dir/(args.resume+'.json')))
                if reg.get('workspace') != str(workspace) or reg.get('state') != 'exited':
                    raise ValueError('Unknown, busy, or wrong-workspace session')
                if process_alive(reg['pid']):
                    raise ValueError('Previous app-server process is still alive')
                write_json(state_dir/(args.resume+'.json'),{**reg,'state':'running'})
                snapshot = await rpc('session/resume', {'sessionId':args.resume, **options})
                await rpc('session/setMode',{'sessionId':args.resume,'mode':'build'})
            else:
                snapshot = await rpc('session/create', {**options,'mode':'build','titleGenerationEnabled':False,
                    **({'model':requested_model} if requested_model is not None else {})})
            sid = snapshot['session']['sessionId']
            authority.session_id = sid
            result['session_id'] = sid
            if args.resume and sid != args.resume:
                raise ValueError('Resume returned a different session')
            if requested_model is not None:
                result['observed_model_initial'] = observed_model(snapshot)
                # CLI 0.16.9 create converts the initial selection to a string,
                # dropping reasoning options. The object setter preserves them.
                await rpc('session/setModel', {'sessionId':sid, 'model':requested_model,
                                               'persistAsWorkspaceLastUsed':False})
                snapshot = await rpc('session/read', {'sessionId':sid,
                    'deliveryKind':'desktop-continuous', 'messageLimit':1})
                result['observed_model_before'] = observed_model(snapshot)
                result['effective_model_before'] = check_model_snapshot(snapshot, requested_model)
            subscription = await rpc('session/subscribe', {
                'sessionId':sid,'deliveryKind':'desktop-continuous','includeSnapshot':False})
            result['start_event_seq'] = subscription['eventSeq']
            if router_sub is not None:
                if router_sub.key != sid:
                    router_sub.rekey(sid)
                router_sub.narrow_start(result['start_event_seq'])
            orchestration_result = {'enabled': bool(authority.orchestration)}
            await rpc('session/send', {'sessionId':sid,'inputId':input_id,'content':packet['prompt']})
            dispatched = True
            send_notification(title='czs：任务已派发',
                              message=f'{run_label} · 会话 {short_session(sid)}',
                              enabled=notify_enabled)
            completed = None
            processed = 0
            while time.monotonic() < deadline:
                if client.failure:
                    raise ValueError(client.failure)
                if router_sub is not None:
                    if router_sub.overflowed:
                        # internal control overflow: the OWNING task fails
                        # through its normal cleanup — never a UI-style gap
                        raise RouterOverflow(
                            'event subscription backlog overflowed for '
                            + str(sid))
                    for _seq, event in router_sub.drain():
                        payload = event.get('payload',{})
                        if event.get('type') == 'turn.started' and payload.get('inputId') == input_id:
                            authority.turn_id = event['turnId']
                        if own_event(event,sid,authority.turn_id):
                            if event.get('type') == 'turn.failed':
                                raise ValueError('Worker turn failed: '+str(payload.get('error')))
                            if event.get('type') == 'turn.completed':
                                completed = event
                else:
                    while processed < len(client.events):
                        event = client.events[processed]
                        processed += 1
                        if event.get('sessionId') != sid or event.get('seq', -1) <= result['start_event_seq']:
                            continue
                        payload = event.get('payload',{})
                        if event.get('type') == 'turn.started' and payload.get('inputId') == input_id:
                            authority.turn_id = event['turnId']
                        if own_event(event,sid,authority.turn_id):
                            if event.get('type') == 'turn.failed':
                                raise ValueError('Worker turn failed: '+str(payload.get('error')))
                            if event.get('type') == 'turn.completed':
                                completed = event
                if completed:
                    break
                if router_sub is not None:
                    await router_sub.wait_new(max(0.0, deadline - time.monotonic()))
                else:
                    await asyncio.sleep(0.02)
            if completed is None:
                raise asyncio.TimeoutError('Worker turn deadline exceeded')
            result['turn_id'] = authority.turn_id
            result['completion'] = completed['payload']
            if completed['payload'].get('resultType') != 'success':
                raise ValueError('Turn completion was not success')
            turn_succeeded = True
            if authority.orchestration:
                # Native orchestration outlives the dispatch turn: children
                # and workflow runs are waited for, polled bounded, and any
                # child failure / lost child / non-completed run fails the
                # delivery verdict. A goal's own "verified" state is recorded
                # as state only — Codex acceptance stays external.
                problems_orch = []
                if skill_binding is not None:
                    # Post-dispatch closure verification (N4): the SAME
                    # immutable reference must still be what the cache link
                    # resolves to, with unchanged bytes. Verification only —
                    # never re-prepare or retarget; drift is reported.
                    from .runtime import verify_bundled_skills, closure_digest
                    try:
                        final_target = Path(verify_bundled_skills(config)).resolve()
                        if str(final_target) != skill_binding['target'] \
                                or closure_digest(final_target) != skill_binding['digest']:
                            problems_orch.append('skill closure changed during the run: bound '
                                                 f'{skill_binding["target"]}, now {final_target}')
                    except ValueError as error:
                        problems_orch.append(f'skill closure verification failed: {error}')
                while time.monotonic() < deadline:
                    if client.failure:
                        raise ValueError(client.failure)
                    while processed < len(client.events):
                        event = client.events[processed]
                        processed += 1
                        authority.note_event(event)
                    try:
                        subagents = await rpc('session/subagents', {'sessionId': sid})
                        # The poll is the authoritative child registry; every
                        # poll merges running -> terminal transitions (N3).
                        authority.note_children_poll(
                            subagents.get('running') or [],
                            (subagents.get('ended') or {}).get('items') or [])
                    except (ValueError, asyncio.TimeoutError):
                        subagents = {'running': [], 'ended': {'items': []}}
                    snapshot_orch = await rpc('session/read', {
                        'sessionId': sid, 'deliveryKind': 'desktop-continuous',
                        'messageLimit': 1})
                    # Real schema (frozen RPC replay): running workflows appear
                    # as projection.backgroundJobs, not session.workflowRuns.
                    projection_orch = snapshot_orch.get('projection') \
                        if isinstance(snapshot_orch, dict) else None
                    run_states = [(job.get('taskId'), job.get('status'))
                                  for job in workflow_jobs_from_snapshot(
                                      {'projection': projection_orch} if isinstance(projection_orch, dict) else {})]
                    # N2 provenance: binding happens ONLY at the
                    # launch-result instant (note_event). This loop
                    # deliberately does NOT capture bytes later: a file that
                    # appears after the launch result is untrusted regardless
                    # of job status.
                    still_running = list(subagents.get('running', []))
                    active_runs = [rid for rid, status in run_states
                                   if status in ('pending', 'running')]
                    if not still_running and not active_runs:
                        break
                    await asyncio.sleep(2)
                orchestration_result['children'] = authority.children
                # Final boundary only (N3): mid-wait running states were never
                # failures; judge terminal completeness after the last poll.
                try:
                    subagents = await rpc('session/subagents', {'sessionId': sid})
                    authority.note_children_poll(
                        subagents.get('running') or [],
                        (subagents.get('ended') or {}).get('items') or [])
                    orchestration_result['subagents_poll'] = redact_transport(subagents)
                    for item in (subagents.get('ended', {}) or {}).get('items', []):
                        status = item.get('status')
                        if status in ('failed', 'cancelled', 'lost'):
                            problems_orch.append('Subagent ended with status '
                                                 f'{status}: {item.get("childSessionId")}')
                        running = subagents.get('running') or []
                        if running:
                            problems_orch.append(f'{len(running)} subagent(s) still running at deadline')
                    allowed_types = set((authority.orchestration or {})
                                         .get('agents', {}).get('allowed_types', []))
                    expected_children = ((authority.orchestration or {})
                                         .get('agents', {}).get('expected_children'))
                    for child_id, child in authority.children.items():
                        child_type = child.get('subagentType')
                        if allowed_types and child_type not in allowed_types:
                            problems_orch.append(f'Subagent of undeclared type observed: {child_type}')
                        status = child.get('status')
                        if status is None or status in ('running', 'ended-unknown'):
                            problems_orch.append(f'Subagent lacks a terminal state at audit: {child_id}')
                        elif status not in ('success', 'completed'):
                            problems_orch.append(f'Subagent terminal state is not success: '
                                                 f'{child_id} -> {status}')
                    if expected_children is not None and len(authority.children) < expected_children:
                        problems_orch.append(f'Expected {expected_children} registered children, '
                                             f'observed {len(authority.children)}')
                except (ValueError, asyncio.TimeoutError) as error:
                    problems_orch.append(f'session/subagents final poll failed: {error}')
                final_snapshot = await rpc('session/read', {'sessionId': sid,
                                              'deliveryKind': 'desktop-continuous',
                                              'messageLimit': 1})
                run_records = []
                for job in workflow_jobs_from_snapshot(final_snapshot):
                    run_records.append({'taskId': job.get('taskId'), 'status': job.get('status'),
                                        'taskKind': job.get('taskKind')})
                    status = job.get('status')
                    if status in ('running', 'pending'):
                        problems_orch.append(f'Workflow job {job.get("taskId")} still {status} '
                                             'at the audit boundary')
                    elif status not in ('completed', 'succeeded', 'done', 'terminal'):
                        problems_orch.append(f'Workflow job {job.get("taskId")} ended in '
                                             f'unknown state {status!r}')
                launched = set(authority.workflow_launches.values())
                observed = {job.get('taskId') for job in run_records}
                for run_id in launched - observed:
                    problems_orch.append(f'Launched workflow {run_id} has no background-job '
                                         'terminal evidence')
                orchestration_result['workflow_runs'] = run_records
                # N2 honesty record: runs whose approved launch result was
                # observed but whose workspace entry was already missing at
                # that first (final) observation — e.g. a tmpdir fallback —
                # never gain a workspace producer record afterwards.
                orchestration_result['workflow_entry_binding'] = {
                    'bound': sorted(authority.runtime_entry_bytes),
                    'missing_at_first_result': sorted(
                        set(authority.launch_results_observed)
                        - set(authority.runtime_entry_bytes)),
                }
                if authority.orchestration.get('goal'):
                    # The goal is armed after the dispatch turn so its verifier
                    # checks the finished deliverable and settles on its own;
                    # arming before the dispatch races the prompt (-32010).
                    try:
                        goal_set = await rpc('session/goal', {'sessionId': sid, 'action':'set',
                            'objective': authority.orchestration['goal']['objective']})
                        orchestration_result['goal_set'] = redact_transport(goal_set)
                        goal_gate = time.monotonic()+180
                        idle_streak = 0
                        while time.monotonic() < min(deadline, goal_gate):
                            gate_snapshot = await rpc('session/read', {'sessionId': sid,
                                'deliveryKind': 'desktop-continuous', 'messageLimit': 1})
                            if (gate_snapshot.get('session') or {}).get('status') == 'idle':
                                idle_streak += 1
                                if idle_streak >= 2:
                                    break
                            else:
                                idle_streak = 0
                            await asyncio.sleep(3)
                    except (ValueError, asyncio.TimeoutError) as error:
                        problems_orch.append(f'goal set at settle failed: {error}')
                    # Goal management is refused while a goal-driven prompt is
                    # running (-32010); retry to a bounded idle boundary.
                    goal_final = None
                    goal_deadline = min(deadline, time.monotonic()+30)
                    while goal_final is None and time.monotonic() < goal_deadline:
                        try:
                            goal_final = await rpc('session/goal', {'sessionId': sid,
                                                                    'action': 'show'})
                        except (ValueError, asyncio.TimeoutError) as error:
                            if 'prompt is running' not in str(error):
                                problems_orch.append(f'goal final show failed: {error}')
                                break
                            await asyncio.sleep(2)
                    if goal_final is not None:
                        orchestration_result['goal_final'] = redact_transport(goal_final)
                        snapshot = goal_final.get('snapshot') \
                            if isinstance(goal_final.get('snapshot'), dict) else {}
                        target = goal_state_from_snapshot(snapshot)
                        if target is None:
                            problems_orch.append('goal snapshot has no projection.target at audit')
                        else:
                            status = target.get('status')
                            orchestration_result['goal_state'] = status
                            if status in ('notSatisfied', 'failed'):
                                problems_orch.append(f'goal ended {status}')
                            elif status not in ('verified', 'complete'):
                                problems_orch.append(f'goal is not verified at audit: {status!r}')
                    elif not problems_orch or 'goal final show failed' not in str(problems_orch):
                        problems_orch.append('goal still busy at the audit boundary')
                # P2.2-repair1 C/F-4: orchestration evidence scans read the
                # SAME evidence slice as the later audits — the routed
                # history when incremental, the shared store when legacy.
                evidence_source = ([frame for _seq, frame in router_sub.history]
                                   if router_sub is not None else client.events)
                orchestration_tool_results = []
                for event in evidence_source:
                    payload = event.get('payload')
                    if not isinstance(payload, dict):
                        continue
                    if event.get('type') == 'tool.updated' and payload.get('kind') == 'result' \
                            and payload.get('toolName') in ('CreateWorkflow', 'TaskOutput',
                                                            'GetWorkflowRun', 'ListWorkflowRuns'):
                        content = ((payload.get('result') or {}).get('content')
                                   if isinstance(payload.get('result'), dict) else None)
                        orchestration_tool_results.append({
                            'toolCallId': payload.get('toolCallId'),
                            'tool': payload.get('toolName'),
                            'content': str(content)[:1500] if content is not None else None})
                orchestration_result['workflow_tool_results'] = orchestration_tool_results
                skill_calls = []
                streamed_skill_inputs = {}
                for event in evidence_source:
                    payload = event.get('payload')
                    if not isinstance(payload, dict):
                        continue
                    # The concrete skill name rides on the streamed tool_call
                    # event; tool.updated carries the lifecycle kinds.
                    if event.get('type') == 'model.streaming' and payload.get('kind') == 'tool_call' \
                            and payload.get('toolName') == 'Skill':
                        streamed_skill_inputs[payload.get('toolCallId')] = payload.get('input')
                    elif event.get('type') == 'tool.updated' and payload.get('toolName') == 'Skill':
                        skill_calls.append({'toolCallId': payload.get('toolCallId'),
                                            'kind': payload.get('kind'),
                                            'input': streamed_skill_inputs.get(payload.get('toolCallId'))})
                allowed_skills = (authority.orchestration.get('skills') or {}).get('allowed', [])
                for call in skill_calls:
                    name = ((call.get('input') or {}).get('skill'))
                    if allowed_skills and name not in allowed_skills:
                        problems_orch.append(f'Skill outside contract invoked: {name}')
                orchestration_result['skill_invocations'] = skill_calls
                if authority.orchestration.get('agents'):
                    orchestration_result['runtime_config_profiles'] = (
                        __import__('zcode_supervisor.orchestration', fromlist=['agent_profiles'])
                        .agent_profiles(authority.orchestration))
                orchestration_result['problems'] = problems_orch
                result['orchestration'] = orchestration_result
            snapshot = await rpc('session/read', {'sessionId':sid,'deliveryKind':'desktop-continuous','messageLimit':10})
            if snapshot['session'].get('status') != 'idle' or snapshot['runtime'].get('pendingRequestIds'):
                raise ValueError('Worker is not idle or still has pending interactions')
            evidence_source = ([frame for _seq, frame in router_sub.history]
                               if router_sub is not None else client.events)
            if requested_model is not None:
                result['observed_model_after'] = observed_model(snapshot)
                result['effective_model_after'] = check_model_snapshot(snapshot, requested_model)
                result['model_execution'] = model_execution_evidence(
                    evidence_source, sid, authority.turn_id, requested_model,
                    authority=authority)
            authority.provide_scan(await run_boundary_scan(inventory, workspace))
            authority.check_frozen()
            commands, problems = execution_evidence(evidence_source, authority)
            result['commands'] = commands
            seen = {entry['command_id'] for entry in commands}
            required = {entry['id'] for entry in packet.get('commands',[]) if entry.get('required',True)}
            if required-seen:
                problems.append('Required commands were not executed: '+str(sorted(required-seen)))
            problems.extend(result.get('orchestration', {}).get('problems', []))
            result['execution_problems'] = problems
            result['delivery_ok'] = not problems
            result['outcome'] = 'delivered' if not problems else 'evidence-rejected'
            if habits is not None:
                executed_ids = {entry['command_id'] for entry in commands}
                for tool in habits['tools']:
                    if tool.get('command_id'):
                        tool['executed'] = tool['command_id'] in executed_ids
        except (ValueError, OSError, EOFError, asyncio.TimeoutError, KeyError, TypeError) as error:
            result['error'] = str(error) or type(error).__name__
            if isinstance(error, asyncio.TimeoutError):
                result['outcome'] = 'timeout'
            elif not dispatched:
                result['outcome'] = 'preflight-rejected'
            elif client.failure:
                result['outcome'] = 'protocol-error'
            elif turn_succeeded:
                # The turn already reached a successful terminal state; this is a
                # post-flight identity/receipt/hash audit rejection, not a worker failure.
                result['outcome'] = 'postflight-rejected'
            else:
                result['outcome'] = 'worker-failed'
            if authority.session_id and client.process and client.process.returncode is None:
                try:
                    await asyncio.wait_for(client.request('session/stop',{'sessionId':authority.session_id}), 2)
                except Exception:
                    pass
        finally:
            # N4b: final lifecycle boundary — the same closure reference must
            # still hold after ALL children/workflow/goal settling and on
            # failure/timeout paths alike; drift evidence is recorded, never
            # repaired. (The pre-settle verify above stays as an early signal.)
            if skill_binding is not None:
                from .runtime import verify_bundled_skills, closure_digest
                try:
                    final_target = Path(verify_bundled_skills(config)).resolve()
                    if str(final_target) != skill_binding['target'] \
                            or closure_digest(final_target) != skill_binding['digest']:
                        result['skill_closure_drift'] = {
                            'bound': skill_binding['target'],
                            'found': str(final_target)}
                        result['delivery_ok'] = False
                        if result.get('outcome') == 'delivered':
                            result['outcome'] = 'postflight-rejected'
                except ValueError as error:
                    result['skill_closure_drift'] = {'bound': skill_binding['target'],
                                                     'error': str(error)}
                    result['delivery_ok'] = False
                    if result.get('outcome') == 'delivered':
                        result['outcome'] = 'postflight-rejected'
            await client.close()
            result['turn_id'] = result.get('turn_id') or authority.turn_id
            result['permission_decisions'] = authority.decisions
            result['process_exit_code'] = client.process.returncode if client.process else None
            result['scope_enforcement'] = 'permission broker and post-run hashes; not an OS sandbox'
            try:
                authority.check_frozen()
                after = inventory(workspace)
                changed_all = sorted(name for name in before.keys()|after.keys() if before.get(name)!=after.get(name))
                # Same producer-record standard as check_frozen: a
                # runtime-prefixed file is an artifact only when its bytes
                # match the trusted per-run content record (or it is the
                # exact-content .gitignore of a launched run).
                def _final_artifact(name):
                    if name in before or not runtime_owned(name):
                        return False
                    if name == '.zcode/workflow-runs/.gitignore' \
                            and authority.workflow_launches \
                            and (workspace / name).read_text(errors='replace') == '*\n':
                        return True
                    parts = Path(name).parts
                    if (len(parts) == 3 and parts[0] == '.zcode'
                            and parts[1] == 'workflow-runs' and parts[2].endswith('.mjs')):
                        run_id = 'dwfrun-' + parts[2][7:-4]
                        entry = workspace / name
                        return (entry.is_file() and not entry.is_symlink()
                                and sha256_regular(entry)
                                == authority.runtime_entry_bytes.get(run_id))
                    return False
                result['changed_files'] = [name for name in changed_all if not _final_artifact(name)]
                runtime_artifacts = [name for name in changed_all if _final_artifact(name)]
                if runtime_artifacts:
                    result['runtime_artifacts'] = {
                        name: sha256_regular(workspace / name)
                        for name in runtime_artifacts if (workspace / name).is_file()}
            except (ValueError,OSError) as error:
                result.update(delivery_ok=False,scope_error=str(error))
            if result['process_exit_code'] != 0:
                result['delivery_ok'] = False
                if result.get('outcome') == 'delivered':
                    result['outcome'] = 'protocol-error'
            if result.get('scope_error') and result.get('outcome') != 'postflight-rejected':
                # finally-only scope discoveries stay scope errors; an in-try
                # post-success audit verdict keeps its more specific category.
                result['outcome'] = 'scope-error'
            write_json(output,redact_transport(result))
            # P2.2: task-owned routed frames release only after the result
            # (with all its evidence) is finalized and persisted
            if router_sub is not None:
                router_sub.release()
            if authority.session_id and result.get('completion'):
                write_json(state_dir/(authority.session_id+'.json'), {
                    'workspace':str(workspace),'state':'exited','pid':client.process.pid,
                    'result_file':str(output)})
            send_notification(
                title='czs：任务结束', enabled=notify_enabled,
                message=(f'{run_label} · 会话 {short_session(authority.session_id)} · '
                         + outcome_label(result.get('outcome', 'worker-failed'),
                                         result.get('execution_problems'))
                         + f' · 用时 {time.monotonic()-started:.0f}s'),
                sound=None if result['delivery_ok'] else 'Basso')
        # quiet: in-process callers (hub zcode runner) set args.quiet so this
        # run summary cannot leak into their stdout (an MCP stdio wire);
        # CLI paths have no quiet attribute and keep the summary on stdout.
        if not getattr(args, 'quiet', False):
            print(json.dumps({k:result.get(k) for k in ['delivery_ok','session_id','turn_id','changed_files','execution_problems','error']}))
        return result['delivery_ok']
