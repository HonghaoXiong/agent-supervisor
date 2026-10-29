"""One monotonic call budget and outer-cancel-resistant cleanup (P0.1).

Scope (PLAN §7 P0.1 and §Global Constraints): these helpers bound only the
CANCELLABLE ASYNCHRONOUS COMMUNICATION SEGMENTS of one delegate call — the
send/drain + response exchange of each RPC and the event poll loop. They do
NOT preempt synchronous Python sections (the first inventory, the post-success
``check_frozen``, the finally-side after-inventory/evidence write): those run
to completion, their cost is measured as total wall time by the callers, and
no whole-call hard wall-time guarantee is claimed anywhere in this module.

``CLEANUP_BUDGET_S`` is the initial resource bound for one best-effort
cleanup, not a measured SLO; tests and callers may inject smaller values.
"""

from __future__ import annotations

import asyncio
import contextlib
import math
import time

CLEANUP_BUDGET_S = 2.0


class BudgetExhausted(asyncio.TimeoutError):
    """The primary budget of a :class:`CallBudget` has no time left.

    Deliberately subclasses :class:`asyncio.TimeoutError` so the existing
    outcome mapping (the ``timeout`` category) keeps working unchanged.
    """


def _positive_finite(value, label):
    if (isinstance(value, bool) or not isinstance(value, (int, float))
            or not math.isfinite(value) or value <= 0):
        raise ValueError(f'{label} must be a positive finite number')


class CallBudget:
    """Primary + hard deadlines counted from ONE monotonic clock.

    ``primary_deadline`` bounds the normal send/start/poll communication
    segments; ``hard_deadline = primary_deadline + cleanup_budget_s`` reserves
    a bounded window so timeout/cancel cleanup still has room. The budget is
    anchored at the CALL START, so synchronous pre/post sections that consume
    it are accounted honestly: the next asynchronous boundary fails fast via
    :meth:`check` / :meth:`wait_s` instead of adding new waits, while the
    already-elapsed synchronous time stays visible in the caller's total wall
    time. Preemption of those synchronous sections is out of scope.
    """

    def __init__(self, timeout_s, *, cleanup_budget_s=None, clock=time.monotonic):
        _positive_finite(timeout_s, 'timeout_s')
        if cleanup_budget_s is None:
            cleanup_budget_s = CLEANUP_BUDGET_S
        _positive_finite(cleanup_budget_s, 'cleanup_budget_s')
        self._clock = clock
        self.timeout_s = float(timeout_s)
        self.cleanup_budget_s = float(cleanup_budget_s)
        self.started_at = self._clock()
        self.primary_deadline = self.started_at + self.timeout_s
        self.hard_deadline = self.primary_deadline + self.cleanup_budget_s

    def remaining_s(self) -> float:
        """Primary budget left, floored at 0 (never raises)."""
        return max(0.0, self.primary_deadline - self._clock())

    def cleanup_remaining_s(self) -> float:
        """Reserved cleanup window before the hard deadline, floored at 0."""
        return max(0.0, self.hard_deadline - self._clock())

    def cleanup_window_s(self) -> float:
        """The window a cleanup call may actually use (review quality-2).

        Capped at ``cleanup_budget_s``: an early failure must not ride the
        still-unused primary time — the cleanup resource bound is the cap,
        the hard deadline only cuts it shorter.
        """
        return min(self.cleanup_budget_s, self.cleanup_remaining_s())

    def check(self) -> float:
        """Return the remaining primary budget, or fail the boundary now.

        Raises :class:`BudgetExhausted` when nothing is left, so the caller
        never STARTS a new asynchronous wait (or a new send) past its budget.
        """
        remaining = self.remaining_s()
        if remaining <= 0.0:
            raise BudgetExhausted(
                'call budget exhausted before the next asynchronous '
                'communication segment')
        return remaining

    def wait_s(self, cap=None) -> float:
        """One segment's wait bound: remaining budget, optionally capped."""
        remaining = self.check()
        return remaining if cap is None else min(float(cap), remaining)


def outer_cancel_pending() -> bool:
    """True when the current task still has an unhandled cancellation.

    Callers that absorbed an outer cancellation while running a bounded
    cleanup (via :func:`run_best_effort_cleanup`) use this to re-raise the
    cancellation afterwards instead of silently converting it into a normal
    result. Python 3.11+ (``Task.cancelling``).
    """
    task = asyncio.current_task()
    return task is not None and task.cancelling() > 0


def _swallow_task_result(task):
    """Retrieve or discard a finished cleanup task's exception (hygiene)."""
    if not task.cancelled():
        with contextlib.suppress(BaseException):
            task.exception()


async def run_best_effort_cleanup(awaitable_factory, timeout_s=CLEANUP_BUDGET_S) -> bool:
    """Run one cleanup awaitable in an independent, bounded, never-raising task.

    The cleanup coroutine runs as its OWN task, so an outer cancellation of
    the calling task cannot kill it; repeated outer cancels only renew the
    bounded wait until the window closes. The function never propagates the
    cleanup's own exceptions and returns ``True`` only when the cleanup
    completed cleanly inside its window.

    When the window closes the cleanup task is CANCELLED and the function
    returns without any added wait: a cooperative cleanup settles on its own
    scheduling at the cancellation, and no hard-kill of cancellation-resistant
    (or synchronous) code is claimed — a resistant cleanup may keep running
    after this returns, recorded as a best-effort failure.

    The bounded wait deliberately completes (rather than re-raising) even
    when an outer cancel arrived: the caller learns about the cancellation
    via :func:`outer_cancel_pending` (or its own ``except CancelledError``
    handler) and must re-raise it itself — waiting here does not cancel the
    caller's obligation to propagate.
    """
    _positive_finite(timeout_s, 'timeout_s')
    task = asyncio.ensure_future(awaitable_factory())
    task.add_done_callback(_swallow_task_result)
    deadline = time.monotonic() + float(timeout_s)
    try:
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0.0:
                break  # the bounded cleanup window is closed
            try:
                await asyncio.wait_for(asyncio.shield(task), remaining)
                break  # finished inside the window
            except asyncio.TimeoutError:
                break  # window closed mid-cleanup
            except asyncio.CancelledError:
                if task.done():
                    break
                continue  # outer cancel: the independent task keeps its window
            except Exception:
                break  # the cleanup itself failed: bounded, never re-raised
        return task.done() and not task.cancelled() and task.exception() is None
    finally:
        if not task.done():
            task.cancel()
            # No extra wait past the window (review spec-I4): the window IS
            # the bound; settle-or-abandon as documented above.
