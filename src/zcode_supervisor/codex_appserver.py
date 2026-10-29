"""Resident Codex app-server driver (P1-1): one long-lived
``codex app-server --listen stdio://`` process speaking JSON-RPC 2.0 over
JSONL, with approval callbacks bridged into the shared Authority policy.

Every method name, parameter field, and response shape below was read from
the exported schemas in ``out/tri-agent-p1-20261026/codex-schema`` (each is
cited at its use site); none come from memory. The wire frames carry no
``"jsonrpc"`` member — JSONRPCRequest.json/JSONRPCResponse.json/
JSONRPCNotification.json define ``{id, method, params}`` / ``{id, result}`` /
``{id, error}`` / ``{method, params}`` only, and the live initialize
handshake against codex-cli 0.164 confirmed it.

Public surface: :class:`AppServerError`, :class:`CodexAppServerClient`,
:class:`ApprovalBridge`, :class:`CodexAppServerDriver`,
:class:`SimpleContractAuthority`, :func:`server_argv`,
:func:`normalize_command`, :func:`redact`, :func:`main`.
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
import inspect
import json
import os
import re
import shlex
import signal
import time
from pathlib import Path

from . import __version__, ui_projection
from .authority import worker_response
from .deadline import (
    BudgetExhausted,
    CallBudget,
    outer_cancel_pending,
    run_best_effort_cleanup,
)
from .protocol import EVENT_ROUTER_BOUND, EventRouter, RouterOverflow, event_router_mode
from .worker_health import ExitProjection

# P0.3 review C1: bounded close termination gradient mirroring
# protocol.ProtocolClient — the worst case fits WorkerGeneration's default
# retirement window so an evicted child really receives its signals.
CLOSE_GRACE_S = 2.0
CLOSE_TERM_S = 1.0
CLOSE_KILL_S = 1.0

FRAMES_FILENAME = "appserver-frames.jsonl"
STDERR_FILENAME = "appserver.stderr.log"
THREAD_READ_FILENAME = "thread-read.json"
RECEIPT_FILENAME = "appserver-receipt.json"

# Same transport-secret key-name list as protocol.redact_transport, normalized
# by stripping every non-lowercase-letter character from the key.
_REDACT_KEYS = frozenset({
    'headers', 'requestheaders', 'responseheaders', 'authorization',
    'apikey', 'accesstoken', 'refreshtoken', 'cookie', 'setcookie',
    'clientsecret', 'password'})

# ServerRequest.json: the 11 server->client request methods.
V1_EXEC_APPROVAL = 'execCommandApproval'
V1_PATCH_APPROVAL = 'applyPatchApproval'
V2_COMMAND_APPROVAL = 'item/commandExecution/requestApproval'
V2_FILE_APPROVAL = 'item/fileChange/requestApproval'
# Bounded wait for the published live turn id before deciding an approval
# that omits its own; see ApprovalBridge.needs_turn_identity.
_TURN_IDENTITY_WAIT_S = 5.0
V2_PERMISSIONS_APPROVAL = 'item/permissions/requestApproval'
V2_TOOL_INPUT = 'item/tool/requestUserInput'
MCP_ELICITATION = 'mcpServer/elicitation/request'
DYNAMIC_TOOL_CALL = 'item/tool/call'

# P1 grants nothing beyond per-request command/file approvals. These are the
# schema-valid "empty refusal" responses for the remaining server requests:
# PermissionsRequestApprovalResponse.json requires {permissions};
# ToolRequestUserInputResponse.json requires {answers};
# McpServerElicitationRequestResponse.json requires {action: accept|decline|cancel};
# DynamicToolCallResponse.json requires {contentItems, success}.
_DENY_RESPONSES = {
    V2_PERMISSIONS_APPROVAL: {'permissions': {}},
    V2_TOOL_INPUT: {'answers': {}},
    MCP_ELICITATION: {'action': 'decline'},
    DYNAMIC_TOOL_CALL: {'contentItems': [], 'success': False},
}

#: The approval/permission server-request methods whose routing REQUIRES a
#: trusted thread/conversation identity (P0.5 fail-closed contract): such a
#: request may only reach ONE ApprovalBridge/Authority through the client's
#: unique-candidate resolution, never by registration order.
_IDENTITY_REQUIRED_METHODS = frozenset((
    V1_EXEC_APPROVAL, V1_PATCH_APPROVAL, V2_COMMAND_APPROVAL,
    V2_FILE_APPROVAL, V2_PERMISSIONS_APPROVAL,
))


def request_requires_task_identity(method):
    """True for the five approval/permission methods with identity routing.

    Everything else (unknown methods and the other server-request classes)
    keeps its existing dispatch path and fail-closed behaviour unchanged.
    """
    return method in _IDENTITY_REQUIRED_METHODS


#: The native envelope's thread/conversation identity field names.
_IDENTITY_FIELDS = ('threadId', 'thread_id', 'conversationId')


def request_thread_identity(params):
    """The single trusted thread/conversation identity of a request params.

    Fail-closed (returns ``None``) unless EVERY identity field PRESENT in
    ``params`` is the SAME non-empty string: absent fields are fine, but any
    present field that is empty, not a string (null/number/list), or a
    conflicting value rejects the request. One shared helper keeps the
    client resolver and the driver-side defence on the same contract.
    """
    if not isinstance(params, dict):
        return None
    identity = None
    for field in _IDENTITY_FIELDS:
        if field not in params:
            continue
        value = params[field]
        if not isinstance(value, str) or not value:
            return None
        if identity is None:
            identity = value
        elif value != identity:
            return None
    return identity


# Fixed supervisor-side classification of the denied request classes (UI
# tool_name tokens; never taken from request payloads).
_DENY_TOOL_NAMES = {
    V2_PERMISSIONS_APPROVAL: 'Permissions',
    V2_TOOL_INPUT: 'UserInput',
    MCP_ELICITATION: 'Elicitation',
    DYNAMIC_TOOL_CALL: 'DynamicToolCall',
}


class AppServerError(Exception):
    """Client misuse or transport failure; never a task outcome."""


def redact(value):
    """Keep frame evidence without persisting transport secrets (protocol.redact_transport key list)."""
    if isinstance(value, dict):
        return {key: ('[redacted]' if re.sub(r'[^a-z]', '', key.lower()) in _REDACT_KEYS
                      else redact(item))
                for key, item in value.items()}
    if isinstance(value, list):
        return [redact(item) for item in value]
    return value


def server_argv(codex_path, transport='stdio'):
    """Assemble the app-server launch argv for one transport mode."""
    if transport == 'stdio':
        # `--listen stdio://` confirmed in `codex app-server --help` (0.164).
        return [str(codex_path), 'app-server', '--listen', 'stdio://']
    if transport == 'proxy':
        return [str(codex_path), 'app-server', 'proxy']
    raise AppServerError(f'Unknown app-server transport: {transport!r}')


def normalize_command(value):
    """Reduce an approval command to the Bash-shaped command string.

    ``execCommandApproval`` carries ``command: [argv]``
    (ExecCommandApprovalParams.json); ``item/commandExecution/requestApproval``
    carries ``command: string`` (CommandExecutionRequestApprovalParams.json).
    Both are tolerated here: a ``[shell, "-lc"|"-c", payload]`` shape (list or
    shlex-parsable string) yields the payload; any other argv list is
    shlex-joined back into one string.
    """
    if isinstance(value, list):
        argv = [str(token) for token in value if isinstance(token, (str, int))]
        if not argv:
            return ''
        if len(argv) >= 3 and argv[1] in ('-c', '-lc'):
            return argv[2]
        return shlex.join(argv)
    if isinstance(value, str):
        try:
            parts = shlex.split(value)
        except ValueError:
            return value
        if len(parts) >= 3 and parts[1] in ('-c', '-lc'):
            return parts[2]
        return value
    return ''


def _handler_revoked(handler) -> bool:
    """True when a run_task has revoked this registered identity handler.

    The driver stamps ``czs_revoked`` (a ``{'revoked': bool}`` cell) on its
    request handler and sets it in the CancelledError handler AND the task
    finally — the earliest and the latest invalidation point.
    """
    cell = getattr(handler, 'czs_revoked', None)
    return isinstance(cell, dict) and bool(cell.get('revoked'))


class CodexAppServerClient:
    """One resident app-server process plus its JSON-RPC client.

    Structured after protocol.ProtocolClient: a read loop, a pending-future
    table keyed by request id, and reverse (server->client) request dispatch.
    All frames, both directions, are appended (redacted) to
    ``evidence_dir/appserver-frames.jsonl``.

    Server requests and notifications dispatch to a multi-subscriber model
    (2026-10-26 P3): concurrent run_task calls each register their own
    thread-filtered handlers via :meth:`add_request_handler` /
    :meth:`add_notification_handler` instead of overwriting one another's
    single-slot callback. The historical ``on_server_request`` /
    ``on_notification`` attributes remain as exclusive compat wrappers
    (assignment clears every other subscription, preserving the single-user
    P2 semantics).
    """

    def __init__(self, codex_path, evidence_dir: Path, *, transport='stdio',
                 env=None, exit_event_sink=None):
        self.codex_path = str(codex_path)
        self.evidence_dir = Path(evidence_dir)
        self.transport = transport
        self.env = env
        self.pending = {}
        self.notifications = []
        self.counter = 0
        # P2.2: keyed bounded notification routing beside the store; the
        # monotonically increasing frame seq keeps replay/subscriptions
        # consistent while released frames drop out of the store.
        # repair1 D: _store_seqs is the seq record ALIGNED with the store
        # (index-parallel) — rekey replay enumerates real (seq, frame)
        # pairs instead of the dead seq_of=None path.
        self._notification_router = None
        self._notification_seq = 0
        self._store_seqs = []
        self.failure = None
        self.process = None
        self.reader = None
        self.tasks = []
        self._request_handlers = []       # fn(method, params) -> response | None
        self._notification_handlers = []  # fn(method, params) -> None
        self._exclusive_request = None    # last value assigned via the property
        self._exclusive_notification = None
        self.frames_path = self.evidence_dir / FRAMES_FILENAME
        self.stderr_path = self.evidence_dir / STDERR_FILENAME
        self.frames = None
        self.stderr = None
        self.closing = False
        self.initialize_result = None
        # P0.4 round 2: the INDEPENDENT run-level unattributed view (PLAN
        # §4.2/222) — identity-less/conflicting notification frames merged
        # across every task served by this shared client. Never part of any
        # task receipt or timeline.
        self.notification_unattributed = {}
        # Per-client cursor so every PHYSICAL frame is folded into that
        # aggregate exactly once, no matter how many tasks (or how many
        # receipt scans) the shared client serves.
        self._unattributed_cursor = 0
        # Per-client ownership of identity-request ids (Q3): insertion-
        # ordered, FIFO-evicted at _IDENTITY_REQUEST_ID_LIMIT so a replaying
        # server cannot grow memory without bound. String "1" and int 1 stay
        # DISTINCT owners (type-tagged keys).
        self._identity_request_ids = {}
        # P0.3: generation identity (stamped by the hub's WorkerGeneration
        # holder) and the exactly-once awaited-exit projection. P0.4: the hub
        # may wire ``exit_event_sink`` so that (only) a real awaited exit is
        # also projected as a UI process.exited event (sideband).
        self.generation = None
        self.exit_projection = ExitProjection(on_event=exit_event_sink)

    # -- subscriber registration (P3) ----------------------------------------

    def add_request_handler(self, fn):
        """Subscribe ``fn(method, params) -> response dict | None``.

        Server requests are offered to handlers in registration order and the
        first non-None dict becomes the wire response; all-None falls through
        to the -32601 refusal. Handlers may be coroutine functions (or return
        any awaitable): each answer is awaited before the next subscriber is
        consulted. A raising handler counts as None — a broken subscriber
        never kills the read loop or starves later subscribers.
        Returns an idempotent unsubscribe callable.
        """
        self._request_handlers.append(fn)

        def unsubscribe():
            try:
                self._request_handlers.remove(fn)
            except ValueError:
                pass
        return unsubscribe

    def add_notification_handler(self, fn):
        """Subscribe ``fn(method, params) -> None``; notifications broadcast.

        Every notification reaches every handler; handler exceptions are
        swallowed by the dispatch loop. Returns an idempotent unsubscribe
        callable.
        """
        self._notification_handlers.append(fn)

        def unsubscribe():
            try:
                self._notification_handlers.remove(fn)
            except ValueError:
                pass
        return unsubscribe

    #: Ownership bound for identity-request ids (Q3): FIFO eviction keeps
    #: the registry finite over the client's lifetime.
    _IDENTITY_REQUEST_ID_LIMIT = 1024

    def _claim_identity_request_id(self, request_id) -> bool:
        """Claim one identity-request id for this client; False on replay.

        The FIRST frame with an id owns it for the rest of the client's
        lifetime (bounded FIFO); any later frame with the same id — valid or
        invalid — is dropped before a reverse task exists. Id types are kept
        distinct via type-tagged keys (string "1" != int 1). An id that
        cannot be owned at all (unhashable: list/object from a malformed
        server) is DROPPED — an id we cannot deduplicate must never be
        dispatched twice, so it is never dispatched. Non-identity routing is
        untouched.
        """
        key = (type(request_id).__name__, request_id)
        try:
            if key in self._identity_request_ids:
                return False
        except TypeError:
            return False  # unhashable id: fail-closed drop, never dispatch
        self._identity_request_ids[key] = True
        if len(self._identity_request_ids) > self._IDENTITY_REQUEST_ID_LIMIT:
            oldest = next(iter(self._identity_request_ids))
            del self._identity_request_ids[oldest]
        return True

    def subscribe_notifications(self, key, start_seq,
                                 deliver_unattributed=False):
        """P2.2: one task's bounded subscription to this client's
        notification stream. ``key`` is the threadId routing identity
        (None pre-thread/start; rekey once thread/start returns).
        Unattributed frames (no threadId) broadcast to every
        subscription that asked for them — exactly the frames the legacy
        per-frame handlers processed."""
        if self._notification_router is None:
            self._notification_router = EventRouter(
                key_of=lambda frame: (
                    frame.get('params', {}).get('threadId')
                    if isinstance(frame, dict)
                    and isinstance(frame.get('params'), dict)
                    and isinstance(frame.get('params').get('threadId'), str)
                    else None),
                seq_of=lambda frame: None,  # explicit seq at publish
                store=self.notifications,
                store_seqs=self._store_seqs,
                bound=EVENT_ROUTER_BOUND)
        return self._notification_router.subscribe(
            key, start_seq, deliver_unattributed=deliver_unattributed)

    def release_thread_frames(self, thread_id):
        """P2.2 bounded retention: after a task's receipt is finalized,
        its own threadId frames leave the shared store (the on-disk
        frame log keeps every raw frame). The unattributed fold cursor
        is index-based, so removals below it shift it back exactly."""
        if not thread_id:
            return
        kept, kept_seqs = [], []
        dropped_below_cursor = 0
        for index, frame in enumerate(self.notifications):
            params = frame.get('params') if isinstance(frame, dict) else None
            if (isinstance(params, dict)
                    and params.get('threadId') == thread_id):
                if index < self._unattributed_cursor:
                    dropped_below_cursor += 1
                continue
            kept.append(frame)
            if index < len(self._store_seqs):
                kept_seqs.append(self._store_seqs[index])
        self.notifications[:] = kept
        self._store_seqs[:] = kept_seqs
        self._unattributed_cursor -= dropped_below_cursor

    def sweep_ownerless_frames(self):
        """repair2 F2: attributed frames whose thread has NO active
        subscriber leave the store once no placeholder window is open.

        Safe by construction: an active task's own frames always have a
        keyed subscriber from its rekey until its release; a foreign
        thread's frames are never counted by any receipt (each receipt
        scans only its own thread + unattributed). Only run while no
        placeholder exists so another task's in-flight window keeps its
        store-kept frames."""
        router = self._notification_router
        if router is None or router.has_placeholder():
            return
        kept, kept_seqs = [], []
        for index, frame in enumerate(self.notifications):
            params = frame.get('params') if isinstance(frame, dict) else None
            identity = (params.get('threadId')
                        if isinstance(params, dict)
                        and isinstance(params.get('threadId'), str) else None)
            if identity is not None and not router.has_subscriber_for(identity):
                if index < self._unattributed_cursor:
                    self._unattributed_cursor -= 1
                continue
            kept.append(frame)
            if index < len(self._store_seqs):
                kept_seqs.append(self._store_seqs[index])
        self.notifications[:] = kept
        self._store_seqs[:] = kept_seqs

    def merge_unattributed_notifications(self):
        """Fold NEW physical notification frames into the client aggregate.

        The aggregate is a cross-task cumulative view; each frame is counted
        exactly once via the per-client cursor (sequential and concurrent
        run_tasks share it; the merge is one synchronous step, so the
        single-threaded event loop serializes callers). Own-receipt counting
        and the thread/read cutoff barrier are unchanged.
        """
        frames = self.notifications
        start = min(self._unattributed_cursor, len(frames))
        for frame in frames[start:]:
            if not isinstance(frame, dict):
                continue
            method = frame.get('method')
            if not isinstance(method, str) or not method:
                continue
            if len(ui_projection.notification_identity_set(
                    frame.get('params'))) != 1:
                self.notification_unattributed[method] = \
                    self.notification_unattributed.get(method, 0) + 1
        self._unattributed_cursor = len(frames)
        # P2.2-repair1 F-3: routed mode bounded retention — unattributed
        # frames below the fold cursor are already counted in the
        # aggregate and retrievable from the on-disk frame log; drop them
        # (attributed frames stay until their thread's release). Legacy
        # mode keeps the store byte-identical to the old behavior.
        if self._notification_router is not None and self._unattributed_cursor:
            kept, kept_seqs = [], []
            for index, frame in enumerate(self.notifications):
                if index < self._unattributed_cursor and (
                        len(ui_projection.notification_identity_set(
                            frame.get('params')
                            if isinstance(frame, dict) else None)) != 1):
                    continue
                kept.append(frame)
                if index < len(self._store_seqs):
                    kept_seqs.append(self._store_seqs[index])
            self.notifications[:] = kept
            self._store_seqs[:] = kept_seqs
            # surviving below-cursor frames are all attributed — never
            # counted by a merge — so the cursor may safely restart at 0
            self._unattributed_cursor = 0

    # -- compat single-slot wrappers (P2 semantics) ---------------------------

    @property
    def on_server_request(self):
        """Last callable assigned through this property (None when never set).

        Assignment registers ONE exclusive subscription and clears every
        other request handler, so legacy single-user code keeps its
        owns-everything behavior.
        """
        return self._exclusive_request

    @on_server_request.setter
    def on_server_request(self, fn):
        if fn is not None and not callable(fn):
            raise TypeError('on_server_request must be callable or None')
        self._request_handlers.clear()
        self._exclusive_request = fn
        if fn is not None:
            self._request_handlers.append(fn)

    @property
    def on_notification(self):
        """Last callable assigned through this property (None when never set).

        Assignment registers one exclusive subscription and clears every other
        notification handler; see :attr:`on_server_request`.
        """
        return self._exclusive_notification

    @on_notification.setter
    def on_notification(self, fn):
        if fn is not None and not callable(fn):
            raise TypeError('on_notification must be callable or None')
        self._notification_handlers.clear()
        self._exclusive_notification = fn
        if fn is not None:
            self._notification_handlers.append(fn)

    async def start(self, budget=None, initialize_timeout=None) -> dict:
        """Spawn the process, start the read loop, and run one initialize.

        Returns the initialize result (v1/InitializeResponse.json:
        {codexHome, platformFamily, platformOs, userAgent}). P0.1 (review
        spec-I5): a caller that owns a :class:`CallBudget` passes the OBJECT
        — the initialize exchange then runs on the SAME primary window
        (``budget.wait_s``) the later run_task uses; ``initialize_timeout``
        stays as a numeric compat bound, and the default remains unbounded
        for callers that own no budget (worker-lifecycle bounds are P0.3's
        generation work).
        """
        if self.process is not None or self.closing:
            raise AppServerError('App-server client cannot be started twice')
        if budget is not None:
            initialize_timeout = budget.wait_s(cap=120)
        argv = server_argv(self.codex_path, self.transport)
        try:
            self.evidence_dir.mkdir(parents=True, exist_ok=True)
            merged_env = dict(os.environ)
            if self.env:
                merged_env.update({str(key): str(value) for key, value in self.env.items()})
            self.stderr = self.stderr_path.open('x')
            self.frames = self.frames_path.open('x')
            self.process = await asyncio.create_subprocess_exec(
                *argv, stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
                stderr=self.stderr, env=merged_env, start_new_session=True,
                limit=8 * 1024 ** 2)
            self.reader = asyncio.create_task(self.read_loop())
            # v1/InitializeParams.json: {clientInfo{name,version,title}, capabilities?}
            self.initialize_result = await self.request('initialize', {
                'clientInfo': {'name': 'czs-hub', 'version': __version__,
                               'title': 'czs-hub supervisor'},
                'capabilities': {'experimentalApi': False}},
                timeout=initialize_timeout)
            return self.initialize_result
        except BaseException:
            await self.close()
            raise

    async def send(self, frame, on_written=None):
        """Write one frame; ``on_written`` fires once the writer accepted the
        bytes, BEFORE ``drain()`` completes (P0.1 review spec-C1: that mark —
        not the request entry — is the real send boundary callers gate on).
        """
        if self.process is None or self.process.stdin is None or self.closing:
            raise EOFError('Codex app-server client is not running')
        self._log_frame('send', frame)
        self.process.stdin.write((json.dumps(frame, ensure_ascii=False) + '\n').encode())
        if on_written is not None:
            on_written()
        await self.process.stdin.drain()

    async def request(self, method, params, timeout=None, on_written=None) -> dict:
        """One client->server request; returns its result or raises.

        P0.1 (COMM-01): the caller's timeout bounds the WHOLE exchange — the
        ``stdin.drain()`` inside :meth:`send` too, not just the response
        future — so a blocked drain can no longer outlive the deadline.
        ``on_written`` (optional) is invoked at the real send boundary above.
        """
        if self.failure:
            raise EOFError(self.failure)
        self.counter += 1
        rid = f'czs-{self.counter}'
        future = asyncio.get_running_loop().create_future()
        self.pending[rid] = future

        async def _exchange():
            await self.send({'id': rid, 'method': method, 'params': params},
                            on_written=on_written)
            return await future

        try:
            return await asyncio.wait_for(_exchange(), timeout)
        finally:
            self.pending.pop(rid, None)

    def _log_frame(self, direction, frame):
        if self.frames is not None:
            self.frames.write(json.dumps({'dir': direction, 'frame': redact(frame)},
                                         ensure_ascii=False) + '\n')
            self.frames.flush()

    def fail(self, error):
        self.failure = str(error) or type(error).__name__
        # P2.2-repair1 B: routed waiters re-check failure immediately
        if self._notification_router is not None:
            self._notification_router.wake_all()
        for future in self.pending.values():
            if not future.done():
                future.set_exception(EOFError(self.failure))
        self.pending.clear()

    async def _reverse(self, frame):
        rid, method = frame.get('id'), frame.get('method')
        raw_params = frame.get('params')
        params = raw_params if isinstance(raw_params, dict) else {}
        try:
            if request_requires_task_identity(method):
                response = await self._resolve_identity_request(method,
                                                                raw_params)
            else:
                response = None
                for handler in list(self._request_handlers):
                    try:
                        response = handler(method, params)
                        if inspect.isawaitable(response):
                            response = await response
                    except Exception:
                        response = None  # a broken subscriber never kills the loop
                    if response is not None:
                        break  # first non-None answer wins
            if response is None:
                await self.send({'id': rid, 'error': {
                    'code': -32601, 'message': f'Unsupported server request: {method}'}})
            else:
                await self.send({'id': rid, 'result': response})
        except (OSError, EOFError) as error:
            self.fail(error)

    async def _resolve_identity_request(self, method, raw_params):
        """Unique-candidate routing for identity-required approval requests.

        P0.5 fail-closed contract: the request may reach exactly ONE
        ApprovalBridge/Authority. It must carry dict params with EXACTLY ONE
        distinct thread/conversation identity (threadId/thread_id/
        conversationId; equal values count as one), and exactly ONE
        currently-registered handler must expose that exact trusted identity
        via its ``czs_request_identity`` matcher. Non-dict params, missing
        or conflicting identity, zero matches, or two matching handlers all
        return ``None`` — the caller then sends the single unsupported
        error; no Authority call, no task attribution, no raw params in the
        reply. Handlers without a matcher (observers) are never candidates.
        """
        identity = request_thread_identity(raw_params)
        if identity is None:
            return None
        candidates = []
        for handler in list(self._request_handlers):
            try:
                # Q2: the attribute access AND the matcher call are both
                # inside the failure boundary — a raising property/callable
                # poisons the whole request (single -32601, Authority=0)
                # and must NEVER silently fall back to another candidate.
                matcher = getattr(handler, 'czs_request_identity', None)
                trusted = matcher() if callable(matcher) else None
            except Exception:
                return None
            if isinstance(trusted, str) and trusted == identity:
                candidates.append(handler)
        if len(candidates) != 1:
            return None
        handler = candidates[0]
        # repair2 (2026-10-27): pre-Authority gate — the selected candidate
        # must STILL be registered and its revocation probe healthy BEFORE
        # any handler/Authority code may run. The registration check, the
        # attribute access and the probe call are all inside the failure
        # boundary: anything but a clean, unrevoked probe returns the
        # original single -32601 path with Authority/Bridge = 0 and no
        # params leak. A pre-broken probe must be refused here, not have its
        # response suppressed after the Authority already decided.
        try:
            if handler not in self._request_handlers:
                return None
            check = getattr(handler, 'czs_check_revoked', None)
            revoked = bool(check()) if callable(check) \
                else _handler_revoked(handler)
        except Exception:
            return None  # a broken probe refuses the request, never routes it
        if revoked:
            return None
        try:
            response = handler(method, raw_params)
            if inspect.isawaitable(response):
                response = await response
        except Exception:
            return None  # a broken handler never widens routing
        # C1: give a pending cancellation of the owning run_task one tick to
        # land, then verify the handler is STILL an active, unrevoked
        # candidate — a revoked handler's answer (even an allow that was
        # legitimately decided while active) must never reach the wire.
        await asyncio.sleep(0)
        try:
            # the attribute access AND the probe call are both inside the
            # failure boundary — a raising revocation property fails closed
            # (single -32601) instead of escaping and poisoning the client
            check = getattr(handler, 'czs_check_revoked', None)
            revoked = bool(check()) if callable(check) \
                else _handler_revoked(handler)
        except Exception:
            revoked = True
        if handler not in self._request_handlers or revoked:
            return None
        return response

    def _reverse_done(self, task):
        # P2.2: the exception is read FIRST (an unobserved one would die
        # at gc); the completed task then leaves the active registry.
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
                try:
                    frame = json.loads(line)
                except json.JSONDecodeError:
                    continue  # malformed line: skip, never crash
                if not isinstance(frame, dict):
                    continue
                if request_requires_task_identity(frame.get('method')):
                    # Q1: identity-required approval params are sensitive by
                    # default — the INBOUND frame log keeps only the method
                    # and id diagnostics; the routing frame above is never
                    # mutated and raw params never reach the evidence file.
                    logged = dict(frame)
                    logged['params'] = '[identity params redacted]'
                    self._log_frame('recv', logged)
                else:
                    self._log_frame('recv', frame)
                if 'method' in frame and 'id' in frame:
                    if request_requires_task_identity(frame.get('method')) \
                            and not self._claim_identity_request_id(
                                frame['id']):
                        continue  # duplicate id: dropped — no second
                        # Authority/bridge call and no second response
                    task = asyncio.create_task(self._reverse(frame))
                    self.tasks.append(task)
                    task.add_done_callback(self._reverse_done)
                elif 'method' in frame:
                    if self._notification_router is not None:
                        # P2.2 routed mode: publish with the frame seq;
                        # the store keeps only frames a future receipt
                        # can still count (unattributed, or attributed to
                        # a thread with an active subscription). repair1 A:
                        # frames arriving while a PLACEHOLDER subscription
                        # exists are kept too — they are stashed for the
                        # future rekey and their receipts must still scan
                        # them. Handlers dispatch for EVERY frame —
                        # unchanged.
                        self._notification_seq += 1
                        self._notification_router.publish(
                            frame, seq=self._notification_seq)
                        identity = None
                        params = frame.get('params')
                        if isinstance(params, dict) \
                                and isinstance(params.get('threadId'), str):
                            identity = params['threadId']
                        if (identity is None
                                or self._notification_router
                                .has_subscriber_for(identity)
                                or self._notification_router
                                .has_placeholder()):
                            self.notifications.append(frame)
                            self._store_seqs.append(self._notification_seq)
                    else:
                        # repair2 F1: the seq record is index-parallel to
                        # the store in EVERY mode — a pre-router frame
                        # consumes a seq here too, so a later router
                        # handover never pairs misaligned lists
                        self.notifications.append(frame)
                        self._notification_seq += 1
                        self._store_seqs.append(self._notification_seq)
                    for handler in list(self._notification_handlers):
                        try:
                            handler(frame.get('method'), frame.get('params'))
                        except Exception:
                            pass
                elif frame.get('id') in self.pending:
                    future = self.pending.pop(frame['id'])
                    if future.done():
                        continue
                    if 'error' in frame:
                        future.set_exception(AppServerError(str(frame['error'])))
                    elif 'result' in frame:
                        future.set_result(frame['result'])
                    else:
                        future.set_exception(AppServerError('Response has no result or error'))
                # Unmatched frames (stale ids, malformed shapes) are dropped.
            if not self.closing:
                self.fail(EOFError('Codex app-server stream closed'))
        except asyncio.CancelledError:
            return
        except Exception as error:
            self.fail(error)

    async def close(self):
        """Bounded SIGTERM -> SIGKILL gradient, mirroring ProtocolClient.close."""
        self.closing = True
        if self.process is not None:
            if self.process.stdin is not None:
                self.process.stdin.close()
            # P0.3 review C1: see protocol.CLOSE_GRACE_S — the gradient's
            # worst case fits the worker gate's retirement window; an
            # unkillable process only ever costs the last bounded wait.
            exited = False
            try:
                await asyncio.wait_for(self.process.wait(), CLOSE_GRACE_S)
                exited = True
            except asyncio.TimeoutError:
                pass
            for sig, window in ((signal.SIGTERM, CLOSE_TERM_S),
                                (signal.SIGKILL, CLOSE_KILL_S)):
                if exited:
                    break
                try:
                    os.killpg(self.process.pid, sig)
                except (ProcessLookupError, PermissionError):
                    pass
                try:
                    await asyncio.wait_for(self.process.wait(), window)
                    exited = True
                except asyncio.TimeoutError:
                    continue
            if exited:
                # P0.3: a wait actually completed — the only place
                # process.exited may be projected (projection rejects
                # non-integer codes; duplicate close emits nothing).
                self.exit_projection.observe_awaited_exit(
                    self.process.returncode, generation=self.generation)
        tasks = ([self.reader] if self.reader is not None else []) + self.tasks
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        # The read loop is joined — no further frames can arrive. One last
        # idempotent merge so frames retained after a cancelled task's
        # cleanup (or any tail the tasks never scanned) still land in the
        # client aggregate exactly once; repeated close is a no-op via the
        # cursor. Best-effort: close must never raise.
        try:
            self.merge_unattributed_notifications()
        except Exception:
            pass
        self.fail(EOFError('Codex app-server client closed'))
        for stream in (self.frames, self.stderr):
            if stream is not None and not stream.closed:
                stream.close()


class ApprovalBridge:
    """Translate Codex approval requests into Authority-shaped decisions.

    The authority duck-types Authority.decide(params) -> {'decision', 'reason'}
    with ZCode-shaped params ({sessionId, turnId, requestId, toolCallId,
    toolName, input}); the wire response is then shaped by
    authority.worker_response (persistent variants auto-downgrade to
    deny + alert). Every decision lands in ``self.decisions`` with a params
    summary.
    """

    def __init__(self, authority, thread_id, ui=None):
        self.authority = authority
        self.thread_id = thread_id
        # Live turn id, published by the driver after turn/start (and from
        # turn/* notifications): approval requests do not always carry a
        # turnId, and the real Authority checks turn identity on decide().
        self.turn_id = None
        self.decisions = []
        self._seq = 0
        # P0.4 round 2: paired approval projection with the APPLIED decision
        # (post worker_response, persistent variants are deny). Sideband
        # only — a raising or absent hook never changes a wire response.
        self.ui = ui

    def _ui(self, record):
        if self.ui is None:
            return
        try:
            self.ui(record)
        except Exception:
            pass  # sideband isolation: never break the approval path

    # -- plumbing -----------------------------------------------------------

    def _ids(self, method, params):
        """requestId/toolCallId from schema id fields (v2 itemId/approvalId, v1 callId).

        The supervisor-synthesized fallback is a slash/hash-free opaque token
        (method name reduced to [A-Za-z0-9_.-] + ':seq') so the UI id gates
        accept it verbatim; wire-provided ids are passed through unchanged.
        """
        candidates = [params.get('itemId'), params.get('callId'),
                      params.get('approvalId'), params.get('id')]
        strings = [value for value in candidates if isinstance(value, str) and value]
        self._seq += 1
        fallback = f"{re.sub(r'[^A-Za-z0-9_.-]', '', method)}:{self._seq}"
        request_id = strings[0] if strings else fallback
        tool_call_id = strings[1] if len(strings) > 1 else fallback
        return request_id, tool_call_id

    def _turn_id(self, params):
        """The turnId sent to authority.decide.

        Wire turnId first (when the approval request carries one), else the
        driver-recorded live turn id; the historical 'codex-turn' constant
        survives only as a last-resort placeholder for authorities that never
        check turn identity — the real Authority would deny it (fail-closed).
        """
        for candidate in (params.get('turnId'), params.get('turn_id')):
            if isinstance(candidate, str) and candidate:
                return candidate
        return self.turn_id or 'codex-turn'

    _APPROVAL_METHODS = frozenset((V1_EXEC_APPROVAL, V2_COMMAND_APPROVAL,
                                   V1_PATCH_APPROVAL, V2_FILE_APPROVAL))

    @classmethod
    def needs_turn_identity(cls, method, params):
        """True when handle() would fall back to the live turn id here.

        The driver waits for the published turn id before dispatching such a
        request: without the wait, an approval that omits turnId can be
        decided before the turn/start response is processed — Python 3.11's
        wait_for resolves through an extra done-callback hop, so a same-stream
        approval dispatched by create_task can overtake the awaiting driver —
        which hands the authority the 'codex-turn' placeholder and fails
        closed wrongly.
        """
        if method not in cls._APPROVAL_METHODS or not isinstance(params, dict):
            return False
        for candidate in (params.get('turnId'), params.get('turn_id')):
            if isinstance(candidate, str) and candidate:
                return False
        return True

    def _emit_requested(self, request_id, tool_call_id, tool_name, variant):
        """The requested half of the projection pair (ids owned by caller)."""
        self._ui({'phase': 'requested',
                  'request_id': request_id, 'tool_call_id': tool_call_id,
                  'tool_name': tool_name, 'session_id': self.thread_id,
                  'turn_id': self.turn_id, 'requested_variant': variant,
                  'command_id': None})

    def _ask(self, method, worker, params, summary, tool_name, tool_input, hint=None):
        """One authority.decide round trip plus worker_response shaping."""
        request_id, tool_call_id = self._ids(method, params)
        self._emit_requested(request_id, tool_call_id, tool_name, hint)
        authority_params = {
            'sessionId': self.thread_id,
            'turnId': self._turn_id(params),
            'requestId': request_id,
            'toolCallId': tool_call_id,
            'toolName': tool_name,
            'input': tool_input,
        }
        decided = self.authority.decide(authority_params) or {}
        decision = {'decision': decided.get('decision'),
                    'reason': decided.get('reason'),
                    'requested_variant': hint}
        response = worker_response(worker, decision)
        alert = response.pop('alert', None)  # wire stays strictly *Response.json-shaped
        record = {'method': method, 'worker': worker,
                  # record the APPLIED decision: a persistent-variant refusal
                  # downgrades an underlying allow to deny + alert.
                  'decision': ('deny' if alert else decision['decision']),
                  'reason': decision['reason'],
                  'requested_variant': hint, 'response': response,
                  'params': summary}
        if alert:
            record['alert'] = alert
        self.decisions.append(record)
        self._ui({'phase': 'decided',
                  'request_id': request_id, 'tool_call_id': tool_call_id,
                  'tool_name': tool_name, 'session_id': self.thread_id,
                  'turn_id': self.turn_id,
                  # the APPLIED decision, consistent with record/wire above
                  'decision': record['decision'],
                  'command_id': self._last_command_id(),
                  'reason_code': ('denied_persistent_variant' if alert
                                  else ('allowed_by_contract'
                                        if record['decision'] == 'allow'
                                        else 'denied_by_contract')),
                  'requested_variant': hint})
        return response

    def _last_command_id(self):
        """The packet command id of the latest authority decision, if any."""
        decisions = getattr(self.authority, 'decisions', None)
        if isinstance(decisions, list) and decisions \
                and isinstance(decisions[-1], dict):
            return decisions[-1].get('command_id')
        return None

    def _record_deny(self, method, params, summary, response, reason,
                     tool_name=None, request_id=None, tool_call_id=None):
        """Record an early/synthetic deny WITHOUT a new Authority decision.

        The caller owns the requested half of the projection pair (passing
        the ids it already emitted so the pair identity matches); this method
        projects only the decided half, with command_id=None — there is no
        CURRENT Authority decision to inherit from, and a stale command id
        from an earlier request must never leak in.
        """
        record = {'method': method, 'decision': 'deny', 'reason': reason,
                  'response': response, 'params': summary,
                  'alert': f'P1 grants no {method} requests'}
        self.decisions.append(record)
        if request_id is None or tool_call_id is None:
            request_id, tool_call_id = self._ids(method, params)
        self._ui({'phase': 'decided',
                  'request_id': request_id, 'tool_call_id': tool_call_id,
                  'tool_name': tool_name, 'session_id': self.thread_id,
                  'turn_id': self.turn_id, 'decision': 'deny',
                  'command_id': None, 'reason_code': 'denied_early',
                  'requested_variant': None})
        return response

    # -- dispatch ------------------------------------------------------------

    def handle(self, method, params):
        """Server-request callback: return the response dict, or None (-> -32601)."""
        if not isinstance(params, dict):
            params = {}
        if method == V1_EXEC_APPROVAL:
            return self._command(method, 'codex-v1', params)
        if method == V2_COMMAND_APPROVAL:
            return self._command(method, 'codex-v2', params)
        if method == V1_PATCH_APPROVAL:
            return self._files(method, 'codex-v1', params)
        if method == V2_FILE_APPROVAL:
            return self._files(method, 'codex-v2', params)
        if method in _DENY_RESPONSES:
            request_id, tool_call_id = self._ids(method, params)
            self._emit_requested(request_id, tool_call_id,
                                 _DENY_TOOL_NAMES.get(method), None)
            return self._record_deny(
                method, params, _params_summary(params),
                dict(_DENY_RESPONSES[method]),
                'P1 does not authorize this request class',
                tool_name=_DENY_TOOL_NAMES.get(method),
                request_id=request_id, tool_call_id=tool_call_id)
        return None

    # -- command approvals ---------------------------------------------------

    @staticmethod
    def _persistent_hint(worker, params):
        """Persistent-grant hints (auto-downgraded by worker_response).

        A persistent variant is only "requested" when the server offers NO
        per-request option: a proposedExecpolicyAmendment alongside a plain
        ``accept`` (verified live, 2026-10-26 run3) is an alternative response,
        not a persistent-grant demand — answer per-request accept in that case.
        """
        grant_root = isinstance(params.get('grantRoot'), str) and params['grantRoot']
        offered = params.get('availableDecisions')
        has_per_request = (worker == 'codex-v2' and isinstance(offered, list)
                           and 'accept' in offered)
        if worker == 'codex-v2':
            if has_per_request:
                return None
            if (isinstance(offered, list)
                    and any(isinstance(o, dict) or str(o).startswith('accept')
                            for o in offered)):
                return 'acceptWithExecpolicyAmendment'
            if (params.get('proposedExecpolicyAmendment')
                    or params.get('proposedNetworkPolicyAmendments')):
                return 'acceptWithExecpolicyAmendment'
            if grant_root:
                return 'acceptForSession'
        elif grant_root:
            return 'approved_for_session'
        return None

    @staticmethod
    def _command_candidates(params):
        """Command strings in precedence order (verified live, run3): the
        model's own command text (commandActions[*].command), the wrapper
        command's payload, then the amendment argv joined last — an amendment
        is an execpolicy pattern (e.g. "cat TASK.md*"), not the literal
        command, so it must not outrank the actual command text."""
        candidates = []
        actions = params.get('commandActions')
        if isinstance(actions, list):
            for action in actions:
                if isinstance(action, dict) and isinstance(action.get('command'), str) \
                        and action['command']:
                    candidates.append(action['command'])
        command = normalize_command(params.get('command'))
        if command:
            candidates.append(command)
        amendment = params.get('proposedExecpolicyAmendment')
        if (isinstance(amendment, list) and amendment
                and all(isinstance(part, str) for part in amendment)):
            candidates.append(shlex.join(amendment))
        return list(dict.fromkeys(candidates))

    def _command(self, method, worker, params):
        candidates = self._command_candidates(params)
        summary = _params_summary(params, command=candidates[0] if candidates else None)
        if not candidates:
            request_id, tool_call_id = self._ids(method, params)
            self._emit_requested(request_id, tool_call_id, 'Bash', None)
            return self._record_deny(method, params, summary,
                                     worker_response(worker, {'decision': 'deny',
                                                              'reason': 'no command text'}),
                                     'approval request carries no command',
                                     tool_name='Bash',
                                     request_id=request_id,
                                     tool_call_id=tool_call_id)
        return self._ask(method, worker, params, summary, 'Bash',
                         {'command': candidates[0]},
                         self._persistent_hint(worker, params))

    # -- file-change approvals -------------------------------------------------

    @staticmethod
    def _files_from(params):
        """File paths from v1 ``fileChanges`` (ApplyPatchApprovalParams.json,
        a path -> FileChange map); v2 FileChangeRequestApprovalParams.json
        declares no per-file field, so tolerant extras (fileChanges/changes)
        are honored and an empty extraction stays a refusal."""
        files = []
        for key in ('fileChanges', 'changes'):
            value = params.get(key)
            if isinstance(value, dict):
                files.extend(str(name) for name in value if isinstance(name, str))
            elif isinstance(value, list):
                for item in value:
                    if isinstance(item, str):
                        files.append(item)
                    elif isinstance(item, dict):
                        for field in ('path', 'file', 'name', 'key'):
                            if isinstance(item.get(field), str):
                                files.append(item[field])
                                break
        return files

    def _files(self, method, worker, params):
        files = self._files_from(params)
        summary = _params_summary(params, files=files)
        hint = self._persistent_hint(worker, params)
        request_id, tool_call_id = self._ids(method, params)
        self._emit_requested(request_id, tool_call_id, 'Write', hint)
        if not files:
            # The requested half is already emitted above with THIS pair's
            # ids — the deny below must not emit a second requested
            return self._record_deny(
                method, params, summary,
                worker_response(worker, {'decision': 'deny',
                                         'reason': 'no declared file paths to verify'}),
                'unverifiable file-change approval', tool_name='Write',
                request_id=request_id, tool_call_id=tool_call_id)
        decision, reason = 'allow', 'Every changed file is in the task contract'
        for path in files:
            one = self.authority.decide({
                'sessionId': self.thread_id,
                'turnId': self._turn_id(params),
                'requestId': f'{method}:files',
                'toolCallId': f'{method}:files:{path}:{self._seq}',
                'toolName': 'Write',
                'input': {'file_path': path, 'content': ''},
            }) or {}
            self._seq += 1
            if one.get('decision') != 'allow':
                decision, reason = 'deny', one.get('reason') or f'file not allowed: {path}'
                break
        response = worker_response(worker, {'decision': decision, 'reason': reason,
                                            'requested_variant': hint})
        alert = response.pop('alert', None)
        record = {'method': method, 'worker': worker,
                  'decision': ('deny' if alert else decision),
                  'reason': reason, 'requested_variant': hint, 'response': response,
                  'params': summary}
        if alert:
            record['alert'] = alert
        self.decisions.append(record)
        self._ui({'phase': 'decided',
                  'request_id': request_id, 'tool_call_id': tool_call_id,
                  'tool_name': 'Write', 'session_id': self.thread_id,
                  'turn_id': self.turn_id, 'decision': record['decision'],
                  'command_id': self._last_command_id(),
                  'reason_code': ('denied_persistent_variant' if alert
                                  else ('allowed_by_contract'
                                        if record['decision'] == 'allow'
                                        else 'denied_by_contract')),
                  'requested_variant': hint})
        return response


def _params_summary(params, **extra):
    """Small non-secret summary of an approval request for the decision log."""
    summary = {key: params.get(key) for key in ('threadId', 'turnId', 'conversationId',
                                                'itemId', 'callId', 'approvalId',
                                                'cwd', 'grantRoot', 'kind')
               if params.get(key) is not None}
    summary.update(extra)
    return summary


class CodexAppServerDriver:
    """Drive one task: thread/start -> turn/start -> notifications -> receipt.

    P0.4: an optional ``ui_emitter`` + ``task_id`` pair turns the driver's
    authoritative bindings (thread id, first live turn id, terminal turn
    status) into sideband UI events on the task's stream; the receipt, wire
    behaviour and cleanup are identical with or without it.
    """

    def __init__(self, client: CodexAppServerClient, *, ui_emitter=None,
                 task_id=None):
        self.client = client
        self.ui_emitter = ui_emitter
        self.ui_task_id = task_id

    async def run_task(self, authority, workspace: Path, prompt: str, *, model=None,
                       sandbox='read-only', approval_policy='on-request',
                       timeout_s=600, budget=None) -> dict:
        # P0.1 (review spec-I3/I5): the budget originates at the CALL START,
        # before any setup — and a caller that already owns one (CLI run with
        # its initialize, hub delegate with the lazy app-server start) passes
        # THE SAME budget in, so initialize and run never get two full
        # primary windows. It bounds only the cancellable asynchronous
        # communication segments; synchronous sections stay non-preemptible
        # and are measured as total wall time instead.
        if budget is None:
            budget = CallBudget(timeout_s)
        started = time.monotonic()
        workspace = Path(workspace)
        evidence_dir = self.client.evidence_dir
        receipt = {'outcome': 'failed', 'thread_id': None, 'turn_id': None,
                   'final_message': None, 'command_items': [], 'approvals': [],
                   'notification_counts': {}, 'duration_s': 0.0,
                   'evidence': {'frames': str(self.client.frames_path),
                                'thread_read': str(evidence_dir / THREAD_READ_FILENAME)}}
        bridge = ApprovalBridge(
            authority, None,
            ui=(ui_projection.approval_hook(
                self.ui_emitter, self.ui_task_id, 'codex', 'app-server')
                if self.ui_emitter is not None and self.ui_task_id is not None
                else None))
        handler_errors = []  # subscriber exceptions: recorded, never propagated
        terminal = {}
        turn_known = asyncio.Event()

        def publish_turn(turn_id):
            """Propagate the live turn id to the bridge and the authority.

            The real Authority re-checks turn identity on every decide() call;
            approval requests do not always carry a turnId, so the recorded
            live id is the fallback the bridge consults. note_turn() also
            admits the turn for orchestrated authorities (a no-op for plain
            ones). hasattr keeps SimpleContractAuthority (no identity
            checking) working unchanged.
            """
            if not isinstance(turn_id, str) or not turn_id:
                return
            first_binding = not receipt['turn_id']
            receipt['turn_id'] = turn_id
            bridge.turn_id = turn_id
            if hasattr(authority, 'turn_id'):
                authority.turn_id = turn_id
            note_turn = getattr(authority, 'note_turn', None)
            if callable(note_turn):
                note_turn(turn_id)
            turn_known.set()
            if first_binding:
                # P0.4 sideband: the first authoritative live-turn binding
                # (codex notifications carry no native seq -> None, never a
                # fabricated cursor).
                ui_projection.turn_event(
                    self.ui_emitter, self.ui_task_id, 'codex', 'app-server',
                    receipt['thread_id'], turn_id)

        def on_notification(method, params):
            if not isinstance(method, str) or not isinstance(params, dict):
                return
            if method.startswith('turn/'):
                # A notification may announce a turn the turn/start response
                # did not carry (or a later turn): bind it before deciding.
                candidate = params.get('turnId')
                if not (isinstance(candidate, str) and candidate):
                    turn_obj = params.get('turn')
                    candidate = turn_obj.get('id') if isinstance(turn_obj, dict) else None
                if (candidate and candidate != receipt['turn_id']
                        and (not params.get('threadId')
                             or params.get('threadId') == receipt['thread_id'])):
                    publish_turn(candidate)
            if (method == 'turn/completed'
                    and receipt['thread_id'] is not None
                    and params.get('threadId') == receipt['thread_id']
                    and terminal.get('turn') is None):
                turn = params.get('turn')
                terminal['turn'] = turn if isinstance(turn, dict) else {}
                # P0.4 sideband: the authoritative terminal capture above.
                ui_projection.turn_event(
                    self.ui_emitter, self.ui_task_id, 'codex', 'app-server',
                    receipt['thread_id'], receipt['turn_id'], completed=True,
                    status=turn.get('status') if isinstance(turn, dict) else None)

        def owns_request(params):
            """Does this server request belong to THIS task's thread?

            P3 concurrency filter on the shared client: params carrying a
            ``threadId`` are claimed only by the task owning that thread;
            v1-shaped approval requests carry ``conversationId`` (the same
            identity) instead, and requests carrying no identity field at all
            cannot be attributed — they are handled (single-task behavior is
            unchanged: an own threadId always matches once thread/start
            returned, and threadId-bearing requests cannot arrive before it).
            """
            if not isinstance(params, dict):
                return True
            mine = receipt['thread_id']
            for key in ('threadId', 'thread_id'):
                thread = params.get(key)
                if isinstance(thread, str) and thread:
                    return thread == mine
            conversation = params.get('conversationId')
            if isinstance(conversation, str) and conversation:
                return mine is None or conversation == mine
            return True

        async def request_handler(method, params):
            """Thread-filtered bridge dispatch for the subscriber model.

            Async so the dispatch can first wait for the live turn id when
            the approval omits its own (needs_turn_identity): deciding
            immediately can race the turn/start continuation and leak the
            placeholder identity. On timeout the decision proceeds exactly
            as before — fail closed, never hang the wire request.

            P0.5: identity-required approval methods reach this handler ONLY
            through the client's unique-candidate resolution; the strict
            exact-identity check below is defence in depth on top of it.
            """
            if request_requires_task_identity(method):
                if request_thread_identity(params) != receipt['thread_id'] \
                        or receipt['thread_id'] is None:
                    return None  # missing/illegal/foreign identity never
                    # reaches a bridge from here either
                if _revoked():
                    return None  # run_task cancelled: never decide
            elif not owns_request(params):
                return None  # another concurrent task's thread owns it
            if (bridge.turn_id is None
                    and ApprovalBridge.needs_turn_identity(method, params)):
                try:
                    await asyncio.wait_for(turn_known.wait(),
                                           _TURN_IDENTITY_WAIT_S)
                except asyncio.TimeoutError:
                    pass  # no live turn id in bounds: fail closed as before
                if _revoked():
                    return None  # cancelled while waiting for the turn id
            if _revoked():
                return None  # final pre-Authority check
            try:
                return bridge.handle(method, params)
            except Exception as error:
                handler_errors.append(
                    f'{method}: {type(error).__name__}: {error}'[:200])
                return None

        # P0.5: the client's unique-candidate routing reads this handler's
        # trusted thread identity (live: None until thread/start returns).
        request_handler.czs_request_identity = lambda: receipt['thread_id']

        def notification_handler(method, params):
            try:
                on_notification(method, params)
            except Exception as error:
                handler_errors.append(
                    f'notification {method}: {type(error).__name__}: {error}'[:200])

        # C1: revocation for the in-flight identity handler. Three signals:
        # the cell (set in the CancelledError handler and the task finally),
        # the owning task's cancellation STATE — Task.cancelling() turns
        # True the instant cancel() is requested, so a cancellation that is
        # still in flight (not yet delivered) already invalidates the
        # handler before Authority and before its response returns — and,
        # since repair2 (2026-10-27), an EXPLICIT unsubscribe: the moment
        # the handler leaves the client's registry it is as dead as a
        # cancelled run_task, and no later Authority call may start on this
        # task's behalf (re-checked after the missing-turn wait returns).
        request_state = {'revoked': False}
        own_task = asyncio.current_task()

        def _revoked():
            if request_state['revoked']:
                return True
            if own_task is not None and (
                    own_task.cancelled() or own_task.cancelling() > 0):
                return True
            return request_handler not in self.client._request_handlers

        request_handler.czs_revoked = request_state
        request_handler.czs_check_revoked = _revoked
        unsubscribe_request = self.client.add_request_handler(request_handler)
        unsubscribe_notification = \
            self.client.add_notification_handler(notification_handler)
        processed = 0
        # P2.2 routed mode: one bounded subscription replaces the shared-list
        # scan + 20 ms polling. Keyed None until thread/start returns the
        # thread id, then rekeyed (with replay) to it. Duck-typed: clients
        # and doubles without the API keep the exact legacy drain.
        router_sub = None
        if (event_router_mode() == 'incremental'
                and hasattr(self.client, 'subscribe_notifications')):
            router_sub = self.client.subscribe_notifications(
                None, 0, deliver_unattributed=True)

        # P0.1 (COMM-01 + review spec-C1): turn cleanup is armed ONLY at the
        # REAL send boundary — the client's writer accepting the turn/start
        # frame (on_written below), never the request entry — so a budget
        # exhausted (or a cancel arriving) before the send can never
        # fabricate an interrupt for work that was not issued.
        turn_wire = {'written': False}
        interrupt_state = {'issued': False}
        try:
            accepts_on_written = 'on_written' in inspect.signature(
                self.client.request).parameters
        except (TypeError, ValueError):
            accepts_on_written = False

        async def interrupt_turn():
            """One bounded, outer-cancel-resistant turn/interrupt, once ever.

            Only server-side work that can actually be addressed is
            interrupted: a thread that was established, a turn/start frame
            that was truly WRITTEN, and a KNOWN live turn id. Without a turn
            id there is no valid interrupt (turnId=None would fabricate a
            cleanup) — the unclean state is recorded and left to P0.2's
            tombstones; a dead transport or a failed/late thread/start never
            fabricates one either.
            """
            if interrupt_state['issued']:
                return 'already-issued'
            interrupt_state['issued'] = True
            if not receipt['thread_id']:
                return 'skipped-no-thread'
            if self.client.closing or self.client.failure:
                return 'skipped-transport-dead'
            if not turn_wire['written']:
                return 'skipped-turn-not-issued'
            if not receipt['turn_id']:
                return 'unclean-no-turn-id'  # written, id unknown: P0.2
            window = budget.cleanup_window_s()
            if window <= 0.0:
                return 'skipped-no-window'  # no new waits past the hard bound
            completed = await run_best_effort_cleanup(
                lambda: self.client.request('turn/interrupt', {
                    'threadId': receipt['thread_id'],
                    'turnId': receipt['turn_id']}),
                timeout_s=window)
            return 'sent' if completed else 'failed'

        def drain_terminal():
            nonlocal processed
            while processed < len(self.client.notifications):
                frame = self.client.notifications[processed]
                processed += 1
                on_notification(frame.get('method'), frame.get('params'))

        try:
            # v2/ThreadStartParams.json: cwd, sandbox, approvalPolicy,
            # approvalsReviewer (user|auto_review|guardian_subagent), ephemeral, model.
            thread_params = {'cwd': str(workspace), 'sandbox': sandbox,
                             'approvalPolicy': approval_policy,
                             'approvalsReviewer': 'user', 'ephemeral': False}
            if model is not None:
                thread_params['model'] = str(model)
            result = await self.client.request('thread/start', thread_params,
                                               timeout=budget.wait_s(cap=120))
            thread = result.get('thread') if isinstance(result, dict) else None
            receipt['thread_id'] = (thread or {}).get('id') if isinstance(thread, dict) else None
            if not receipt['thread_id']:
                raise AppServerError('thread/start returned no thread id')
            # Authority identity binding: the hub-built Authority re-checks
            # sessionId on every decide(), so it must learn the thread id
            # before any approval callback can fire (hasattr keeps
            # SimpleContractAuthority working unchanged).
            if hasattr(authority, 'session_id'):
                authority.session_id = receipt['thread_id']
            bridge.thread_id = receipt['thread_id']
            if router_sub is not None:
                # P2.2-repair1 A/D: the routing key arrives only now; the
                # placeholder stash drains AND the router replays its
                # live store (real (seq, frame) pairs via the aligned seq
                # record; at/above the live floor were already seen on
                # the wire, so replay cannot duplicate).
                router_sub.rekey(receipt['thread_id'])
            # P0.4 sideband: session binding established (thread id from the
            # authoritative thread/start result).
            ui_projection.session_bound(
                self.ui_emitter, self.ui_task_id, 'codex', 'app-server',
                receipt['thread_id'], 'app-server-thread')

            # v2/TurnStartParams.json: {threadId, input: [UserInput...]}; the
            # text variant is {type: "text", text: str}. The on_written hook
            # marks the REAL wire send (accepted by the writer, pre-drain);
            # a duck-typed client without it stays fail-safe unissued.
            result = await self.client.request(
                'turn/start',
                {'threadId': receipt['thread_id'],
                 'input': [{'type': 'text', 'text': prompt}]},
                timeout=budget.wait_s(cap=120),
                **({'on_written': lambda: turn_wire.__setitem__('written', True)}
                   if accepts_on_written else {}))
            turn = result.get('turn') if isinstance(result, dict) else None
            publish_turn((turn or {}).get('id') if isinstance(turn, dict) else None)

            while budget.remaining_s() > 0.0:
                if self.client.failure:
                    raise EOFError(self.client.failure)
                if router_sub is not None:
                    if router_sub.overflowed:
                        # internal control overflow: the OWNING task fails
                        # through its normal cleanup — never a UI-style gap
                        raise RouterOverflow(
                            'notification subscription backlog overflowed '
                            'for ' + str(receipt['thread_id']))
                    for _seq, frame in router_sub.drain():
                        on_notification(frame.get('method'),
                                        frame.get('params'))
                else:
                    drain_terminal()
                if terminal:
                    break
                if router_sub is not None:
                    # wakeup-driven wait on the absolute budget deadline
                    await router_sub.wait_new(budget.remaining_s())
                else:
                    await asyncio.sleep(0.02)
            if not terminal:
                receipt['outcome'] = 'timeout'
                receipt['interrupt'] = await interrupt_turn()
                if outer_cancel_pending():
                    # An outer cancel arrived while the bounded interrupt ran:
                    # it must still reach the caller, not become a receipt.
                    raise asyncio.CancelledError()
            else:
                # TurnStatus enum (v2/ThreadStartResponse.json): completed |
                # interrupted | failed | inProgress.
                status = terminal.get('turn', {}).get('status')
                receipt['outcome'] = 'delivered' if status == 'completed' else 'failed'
                error = terminal.get('turn', {}).get('error')
                if isinstance(error, dict) and error.get('message'):
                    receipt['error'] = str(error['message'])[:300]
        except asyncio.CancelledError:
            # P0.1 (COMM-01): an outer cancel is a BaseException — the old
            # except tuple never saw it, so interrupt was bypassed entirely.
            # Bounded interrupt first, then the cancellation re-raises (the
            # half-built receipt is never returned or written on this path).
            # C1: invalidate the in-flight identity handler IMMEDIATELY (the
            # earliest possible point) so it can neither call Authority nor
            # return an accept once it resumes.
            request_state['revoked'] = True
            receipt['outcome'] = 'cancelled'
            receipt['interrupt'] = await interrupt_turn()
            raise
        except (AppServerError, EOFError, OSError, asyncio.TimeoutError,
                ValueError, KeyError, TypeError) as error:
            # P0.1: a budget/timeout failure is a timeout, honestly labelled —
            # the receipt's initial 'failed' must not mask it (no raising path
            # sets a different outcome first).
            receipt['outcome'] = (
                'timeout' if isinstance(error, (asyncio.TimeoutError,
                                                BudgetExhausted)) else 'failed')
            receipt['error'] = str(error) or type(error).__name__
            # P0.1 review spec-C2: EVERY failure after established work gets
            # one bounded best-effort interrupt — an ordinary ValueError used
            # to skip cleanup entirely. interrupt_turn's own gates keep this
            # honest (nothing issued / dead transport / unknown turn id /
            # no window => no fabricated wire attempt), and the original
            # failure stays recorded above.
            receipt['interrupt'] = await interrupt_turn()
            if outer_cancel_pending():
                raise asyncio.CancelledError()
        finally:
            # C1 (latest point): the revocation outlives the unsubscribe so
            # an already-selected handler is dead even if it still runs.
            request_state['revoked'] = True
            # P3 subscriber hygiene: this task's handlers leave the shared
            # client the moment its turn ends (or fails), so no later task —
            # and no reuse of this driver — inherits a stale thread filter.
            unsubscribe_request()
            unsubscribe_notification()
            # Physical unattributed frames that already arrived must be
            # folded on cancel/exception exits too (idempotent cursor; no
            # new waits on these paths).
            self._merge_unattributed()

        # The post-terminal evidence/receipt tail runs OUTSIDE the main-stage
        # protection above: one outer try/finally gives every exit from this
        # tail — normal return, cancellation during the evidence read, any
        # exception — the same synchronous, cursor-idempotent unattributed
        # merge, so a retained physical frame is folded the moment the task
        # unwinds (never only at close). No new waits, no cutoff/identity
        # change, no whole-history rescan.
        try:
            if handler_errors:
                # Subscriber exceptions were contained above; they are
                # evidence, never a crash of the read loop or of a sibling.
                receipt['handler_errors'] = handler_errors
                receipt.setdefault('error', '; '.join(handler_errors)[:300])
            receipt['command_items'], agent_texts, receipt['token_usage'] = \
                self._collect(receipt['thread_id'])
            receipt['final_message'] = agent_texts[-1] if agent_texts else None
            receipt['approvals'] = bridge.decisions
            # P0.1: the closing evidence RPC is a normal-completion segment —
            # it runs only inside the SAME budget (bounded by what is left of
            # it); an exhausted budget adds no new wait, and the receipt
            # records the skipped evidence instead of fabricating it.
            try:
                receipt['evidence']['thread_read'] = \
                    await self._dump_thread_read(
                        receipt['thread_id'],
                        evidence_dir / THREAD_READ_FILENAME,
                        timeout=budget.wait_s(cap=30))
            except BudgetExhausted:
                receipt['evidence']['thread_read'] = {
                    'error': 'call budget exhausted before the thread-read '
                             'evidence'}
            if receipt['final_message'] is None:
                receipt['final_message'] = self._final_from_thread_read(
                    evidence_dir / THREAD_READ_FILENAME)
        # P0.4 round 2: count notifications ONLY after the thread/read
        # barrier — the server's thread/read RESPONSE arrives on the same
        # ordered stdio stream AFTER every frame it sent earlier, and the
        # client's read loop appends those frames before resolving this
        # coroutine, so the snapshot is deterministic (no scheduler race on
        # trailing frames). Attribution: the receipt counts ONLY frames whose
        # identity set is exactly this task's thread; identity-less and
        # conflicting frames merge into the shared client's INDEPENDENT
        # unattributed view (never a task receipt/timeline); single-identity
        # frames of other threads belong to their own receipts.
            receipt['notification_counts'], _unattributed = \
                ui_projection.split_notification_counts(
                    self.client.notifications, receipt['thread_id'])
            self._merge_unattributed()
            receipt['duration_s'] = round(time.monotonic() - started, 3)
            (evidence_dir / RECEIPT_FILENAME).write_text(
                json.dumps(redact(receipt), indent=2, sort_keys=True,
                           ensure_ascii=False) + '\n',
                encoding='utf-8')
            return receipt
        finally:
            self._merge_unattributed()
            # P2.2: evidence is finalized (receipt persisted or the task
            # failed) — release the task-owned frames, then drop this
            # thread's frames from the shared store. Raw frames stay in
            # the on-disk appserver-frames.jsonl log.
            if router_sub is not None:
                router_sub.release()
                release = getattr(self.client, 'release_thread_frames', None)
                if callable(release) and receipt.get('thread_id'):
                    release(receipt['thread_id'])
                sweep = getattr(self.client, 'sweep_ownerless_frames', None)
                if callable(sweep):
                    sweep()  # repair2 F2: bounded cross-window retention

    def _merge_unattributed(self):
        """Fold NEW physical notification frames into the client aggregate.

        Purely synchronous and idempotent (the client's per-frame cursor
        makes every call incremental), so the task finally (cancel/exception
        exits), the normal post-barrier tail and close() can all call it
        without ever double counting — and the cancel/exception paths add NO
        new waits (no thread/read barrier is fabricated there).
        """
        merge = getattr(self.client, 'merge_unattributed_notifications', None)
        if callable(merge):
            merge()

    def _collect(self, thread_id):
        """command items, agent message texts, latest token usage from notifications."""
        command_items, agent_texts, token_usage = [], [], None
        for frame in self.client.notifications:
            method, params = frame.get('method'), frame.get('params')
            if not isinstance(params, dict) or params.get('threadId') != thread_id:
                continue
            if method == 'item/completed':
                item = params.get('item')
                if not isinstance(item, dict):
                    continue
                if item.get('type') == 'commandExecution':
                    command_items.append({
                        'command': item.get('command'),
                        'exit_code': item.get('exitCode', item.get('exit_code')),
                        'status': item.get('status')})
                elif item.get('type') == 'agentMessage' and isinstance(item.get('text'), str):
                    agent_texts.append(item['text'])
            elif method == 'thread/tokenUsage/updated':
                token_usage = params.get('tokenUsage')
        return command_items, agent_texts, token_usage

    async def _dump_thread_read(self, thread_id, path, timeout=30):
        """One thread/read (v2/ThreadReadParams.json: {threadId, includeTurns}) as evidence."""
        if not thread_id or self.client.failure or self.client.closing:
            return {'error': 'no live thread to read'}
        try:
            result = await asyncio.wait_for(self.client.request(
                'thread/read', {'threadId': thread_id, 'includeTurns': True}),
                timeout)
        except Exception as error:
            return {'error': str(error) or type(error).__name__}
        path.write_text(json.dumps(redact(result), indent=2, sort_keys=True,
                                   ensure_ascii=False) + '\n', encoding='utf-8')
        return str(path)

    @staticmethod
    def _final_from_thread_read(path):
        """Last agentMessage text from the thread-read turns (notification fallback)."""
        try:
            payload = json.loads(path.read_text(encoding='utf-8'))
        except (OSError, json.JSONDecodeError):
            return None
        texts = []
        turns = (payload.get('thread') or {}).get('turns', []) \
            if isinstance(payload, dict) else []
        for turn in turns if isinstance(turns, list) else []:
            for item in (turn.get('items') or []) if isinstance(turn, dict) else []:
                if (isinstance(item, dict) and item.get('type') == 'agentMessage'
                        and isinstance(item.get('text'), str)):
                    texts.append(item['text'])
        return texts[-1] if texts else None


class SimpleContractAuthority:
    """Minimal Authority stand-in for the CLI `run` path (main-controller use).

    decide() matches the Authority signature: an exact approved-command set
    plus a file whitelist (relative name or suffix match) decide allow/deny.
    """

    def __init__(self, approved_commands=(), allowed_files=()):
        self.approved_commands = set(approved_commands)
        self.allowed_files = tuple(allowed_files)

    def _file_allowed(self, path):
        if not isinstance(path, str) or not path:
            return False
        return any(path == allowed or path.endswith('/' + allowed)
                   for allowed in self.allowed_files)

    def decide(self, params):
        tool = params.get('toolName')
        data = params.get('input') if isinstance(params.get('input'), dict) else {}
        if tool == 'Bash' and data.get('command') in self.approved_commands:
            return {'decision': 'allow', 'reason': 'command is in the approved set'}
        if tool in ('Write', 'Edit') and self._file_allowed(data.get('file_path')):
            return {'decision': 'allow', 'reason': 'file is in the whitelist'}
        return {'decision': 'deny', 'reason': 'not in this run contract'}


def _json_array(text, label):
    try:
        value = json.loads(text)
    except json.JSONDecodeError as error:
        raise AppServerError(f'{label} must be a JSON array: {error}')
    if not isinstance(value, list) or any(not isinstance(item, str) for item in value):
        raise AppServerError(f'{label} must be a JSON array of strings')
    return value


async def _handshake(args) -> int:
    client = CodexAppServerClient(args.codex, Path(args.out))
    try:
        result = await client.start()
        print(json.dumps({
            'handshake_ok': True,
            'user_agent': result.get('userAgent'),
            'codex_home': result.get('codexHome'),
            'platform_family': result.get('platformFamily'),
            'platform_os': result.get('platformOs'),
            'result_keys': sorted(result),
            'frames': str(client.frames_path)}, ensure_ascii=False))
        return 0
    except Exception as error:
        print(json.dumps({'handshake_ok': False,
                          'error': str(error) or type(error).__name__}))
        return 1
    finally:
        await client.close()


async def _run(args) -> int:
    workspace = Path(args.workspace)
    if not workspace.is_dir():
        print(json.dumps({'run_ok': False, 'error': 'workspace must be an existing directory'}))
        return 1
    authority = SimpleContractAuthority(
        _json_array(args.approve_commands, '--approve-commands'),
        _json_array(args.allowed_files, '--allowed-files'))
    client = CodexAppServerClient(args.codex, Path(args.out))
    try:
        # P0.1 review spec-I5: ONE budget for the whole CLI run — the lazy
        # initialize exchange and run_task share the SAME budget object
        # (previously they each got a full timeout). The shared hub singleton
        # gets its budget injected per delegate by the hub (P0.3 generation
        # work still owns eviction/replay semantics).
        budget = CallBudget(args.timeout_s)
        await client.start(budget=budget)
    except Exception as error:
        print(json.dumps({'run_ok': False,
                          'error': str(error) or type(error).__name__}))
        await client.close()
        return 1
    try:
        receipt = await CodexAppServerDriver(client).run_task(
            authority, workspace, args.prompt, model=args.model,
            sandbox=args.sandbox,
            approval_policy=getattr(args, 'approval_policy', 'on-request'),
            timeout_s=args.timeout_s, budget=budget)
    finally:
        await client.close()
    print(json.dumps({key: receipt.get(key) for key in
                      ('outcome', 'thread_id', 'turn_id', 'final_message', 'error')},
                     ensure_ascii=False))
    return 0 if receipt.get('outcome') == 'delivered' else 1


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        prog='python -m zcode_supervisor.codex_appserver',
        description='Resident Codex app-server client (experimental protocol).')
    sub = parser.add_subparsers(dest='command', required=True)

    handshake = sub.add_parser('handshake', help='initialize + close; zero model consumption')
    handshake.add_argument('--codex', required=True)
    handshake.add_argument('--out', required=True, help='evidence directory')

    run = sub.add_parser('run', help='one supervised task through the app-server')
    run.add_argument('--codex', required=True)
    run.add_argument('--workspace', required=True)
    run.add_argument('--prompt', required=True)
    run.add_argument('--out', required=True, help='evidence directory')
    run.add_argument('--sandbox', default='read-only',
                     choices=('read-only', 'workspace-write', 'danger-full-access'))
    run.add_argument('--approval-policy', default='on-request',
                     choices=('untrusted', 'on-request', 'never'),
                     help='untrusted asks approval for every exec; workspace-write '
                          'sandbox auto-runs sandbox-contained ops under on-request')
    run.add_argument('--approve-commands', default='[]',
                     help='JSON array of exactly approved command strings')
    run.add_argument('--allowed-files', default='[]',
                     help='JSON array of whitelist file paths')
    run.add_argument('--model', default=None)
    run.add_argument('--timeout-s', type=float, default=600)

    args = parser.parse_args(argv)
    if args.command == 'handshake':
        return asyncio.run(_handshake(args))
    return asyncio.run(_run(args))


if __name__ == '__main__':
    raise SystemExit(main())
