"""Singleton worker generation gate, evidence-based health, exit projection.

PLAN §7 P0.3 scope, shared by both worker kinds (zcode resident and codex
app-server):

- :class:`WorkerGeneration` keeps ONE lock-protected current generation.
  ``get_or_start`` does lookup AND start inside the same critical section, so
  N concurrent first delegates observe exactly one start, and a failed start
  is never cached as a healthy generation. ``invalidate(expected_generation,
  evidence)`` evicts ONLY the matching old generation — a stale event from an
  already-replaced generation can never delete the newer replacement — and
  closes the evicted instance inside a BOUNDED best-effort window so a hung
  old worker cannot block new-generation work indefinitely.
- :func:`instance_health` limits death evidence to three observable facts:
  the client's recorded ``failure``, the reader task having ACTUALLY ended,
  and a REAL observed ``process.returncode``. ``returncode is None`` means
  live and is never death evidence; an ordinary completed task is not an
  instance signal at all.
- :class:`ExitProjection` reports ``process.exited`` exactly once per
  instance, and only from a close path that actually awaited the child's
  ``wait()``. Reader EOF or a late failure may ISOLATE a generation
  (invalidate) but can never impersonate an awaited process exit; a bare
  ``returncode`` assignment or a duplicate close emits nothing.

No OS signal magic, no global process scans, no PID-reuse guesses, no
unconditional reconnect loop, and no automatic task replay live here.
"""

from __future__ import annotations

import asyncio
import contextlib
import inspect
import time

from .deadline import outer_cancel_pending, run_best_effort_cleanup

#: Bounded window for closing an evicted/shut-down old generation. The
#: DEFAULT covers the clients' full bounded termination gradient
#: (grace 2.0 s + SIGTERM 1.0 s + SIGKILL 1.0 s) plus headroom, so the
#: production retirement path finishes its signals, awaits the real exit and
#: projects it INSIDE the window (P0.3 review C1); a fully cancel-resistant
#: or unkillable process still only costs this window. Tests inject smaller
#: values. The retirement lock serializes new starts with this window —
#: blocked, but only ever for as long as the window allows.
OLD_CLOSE_BUDGET_S = 5.0


def _now_epoch():
    """JSON-safe observation timestamp (epoch seconds, millisecond cut)."""
    return round(time.time(), 3)


def instance_health(instance):
    """Evidence-based health snapshot of one shared worker instance.

    ``instance`` is either the communication client itself (app-server) or a
    driver wrapper exposing ``.client`` (resident). Only the three P0.3
    observations can mark it unhealthy; everything else (a normally finished
    task, a live ``returncode is None`` process) keeps it healthy. The
    returned record is JSON-safe and carries ``generation`` so terminal
    evidence stays attributable to the generation that produced it.
    """
    client = getattr(instance, 'client', instance)
    failure = getattr(client, 'failure', None)
    reader = getattr(client, 'reader', None)
    reader_ended = reader is not None and reader.done()
    process = getattr(client, 'process', None)
    returncode = getattr(process, 'returncode', None) if process is not None else None
    return {
        'generation': getattr(instance, 'generation', None),
        'failure': failure if isinstance(failure, str) else None,
        'returncode': returncode,
        'reader_ended': reader_ended,
        'healthy': not (failure or reader_ended or returncode is not None),
        'observed_at': _now_epoch(),
    }


class ExitProjection:
    """``process.exited`` projection: exactly once, from an awaited wait.

    The close path of a real client calls :meth:`observe_awaited_exit` after
    its wait gradient actually completed. Duplicate observations are rejected
    (repeated close/wait never emits twice); anything that is not an awaited
    exit — reader EOF, a late failure, a bare ``returncode`` assignment, an
    ordinary completed task — simply never calls this.
    """

    def __init__(self, on_event=None):
        self.emitted = []
        self._on_event = on_event

    def observe_awaited_exit(self, returncode, generation=None, source='close'):
        """Record one awaited process exit; True only on the first call.

        Only a REAL integer exit code is exit evidence (negative signal
        codes are legal; ``None``, bools and anything else are not). A
        non-integer observation records nothing AND does not consume the
        exactly-once slot: a later genuine integer exit still emits once
        (P0.3 review I5).
        """
        if self.emitted:
            return False
        if isinstance(returncode, bool) or not isinstance(returncode, int):
            return False
        record = {'type': 'process.exited',
                  'returncode': returncode,
                  'generation': generation,
                  'source': source,
                  'observed_at': _now_epoch()}
        self.emitted.append(record)
        if self._on_event is not None:
            with contextlib.suppress(Exception):
                self._on_event(record)
        return True


async def _default_start(instance, **start_kwargs):
    """Start one instance; await coroutine starters. Raises on failure."""
    starter = getattr(instance, 'start', None)
    if starter is None:
        return None
    result = starter()
    if inspect.isawaitable(result):
        await result


def _bounded_close_factory(instance):
    """One never-raising close awaitable for an evicted old generation."""
    async def close_old():
        try:
            for name in ('stop', 'close'):
                closer = getattr(instance, name, None)
                if callable(closer):
                    result = closer()
                    if inspect.isawaitable(result):
                        await result
                    return True
            return False
        except Exception:
            return False
    return close_old


class WorkerGeneration:
    """One lock-protected current generation of a shared worker instance.

    ``factory()`` builds a fresh instance; ``starter(instance, **kwargs)``
    starts it (the default awaits ``instance.start()``); both run inside the
    single critical section of :meth:`get_or_start`, so concurrent first
    delegates share one start and a failed start leaves nothing cached.
    """

    def __init__(self, factory, starter=None, *,
                 close_budget_s=OLD_CLOSE_BUDGET_S):
        self._factory = factory
        self._starter = starter or _default_start
        self.close_budget_s = float(close_budget_s)
        self._lock = asyncio.Lock()
        self._current = None            # {'instance', 'generation'}
        self.generation = 0             # last assigned generation number
        self.invalidation = None        # last eviction evidence record

    def current(self):
        """The live ``{'instance', 'generation'}`` or None (no lock taken)."""
        return self._current

    async def get_or_start(self, **start_kwargs):
        """Return ``{'instance', 'generation'}`` from one critical section.

        P0.3 review I3: before reusing the current generation, its evidence
        health is checked under the SAME lock. A generation that died while
        idle (late failure, ended reader, reaped exit — after the previous
        delegate settled) is retired right here — bounded close, exactly one
        new generation — and the caller is handed the fresh replacement, so
        no delegate ever sends to a known-dead generation first.
        """
        async with self._lock:
            if self._current is None:
                return await self._start_locked(**start_kwargs)
            health = instance_health(self._current['instance'])
            if health['healthy']:
                return dict(self._current)
            old = self._current
            await self._retire_locked(old['instance'], old['generation'], health)
            return await self._start_locked(**start_kwargs)

    async def _start_locked(self, **start_kwargs):
        """Build, stamp and start one generation inside the held lock."""
        instance = self._factory()
        self.generation += 1
        number = self.generation
        instance.generation = number
        # Wrapper-style holders (resident) carry the communication client
        # one level down; stamp it too so exit projections and health
        # evidence stay attributable to this generation.
        client = getattr(instance, 'client', None)
        if client is not None and getattr(client, 'generation', None) is None:
            client.generation = number
        try:
            await self._starter(instance, **start_kwargs)
        except BaseException:
            # P0.3 review I4: a failed or cancelled start AFTER the factory
            # returned the instance: release the half-built generation with
            # the same bounded best-effort close, then re-raise (a cancelled
            # start stays cancelled). Nothing is cached — the next
            # get_or_start builds generation n+1. A factory that raises
            # before returning an instance has nothing to release.
            with contextlib.suppress(Exception):
                await run_best_effort_cleanup(
                    _bounded_close_factory(instance),
                    timeout_s=self.close_budget_s)
            # Round-3 Important (deadline.py:134-138 contract): an outer
            # cancel ABSORBED by the bounded cleanup above must be restored
            # BEFORE the original exception — a caller whose cancellation
            # already arrived must see CancelledError, not the start error.
            # The close/exit resource semantics and the recorded first cause
            # are unchanged; only the propagation order is corrected.
            if outer_cancel_pending():
                raise asyncio.CancelledError()
            raise
        self._current = {'instance': instance, 'generation': number}
        return dict(self._current)

    async def _retire_locked(self, instance, number, evidence):
        """Evict the current generation and close it, lock held (internal).

        Records the eviction evidence, then runs the bounded best-effort
        close of the old instance. An outer cancel that arrived while the
        bounded close ran (absorbed by the cleanup task, per the P0.1
        contract) is RESTORED here as a CancelledError — a retirement that
        swallowed a pending cancellation must never look settled to its
        caller (P0.3 review I1).
        """
        self._current = None
        self.invalidation = {
            'generation': number,
            'evidence': evidence if isinstance(evidence, dict) else
                {'note': str(evidence)},
            'observed_at': _now_epoch(),
        }
        await run_best_effort_cleanup(_bounded_close_factory(instance),
                                      timeout_s=self.close_budget_s)
        if outer_cancel_pending():
            raise asyncio.CancelledError()

    async def invalidate(self, expected_generation, evidence):
        """Evict ONLY the current generation when it matches ``expected``.

        Returns True when the eviction happened. A stale call (the expected
        generation was already replaced) changes nothing — an old reader,
        failure or close event can never delete a newer generation. The
        evicted instance is closed inside a bounded best-effort window; the
        window IS the bound, and the retirement lock serializes new starts
        with it — a new generation waits at most the window, never forever.
        """
        async with self._lock:
            current = self._current
            if current is None or current['generation'] != expected_generation:
                return False
            await self._retire_locked(current['instance'],
                                      expected_generation, evidence)
            return True
