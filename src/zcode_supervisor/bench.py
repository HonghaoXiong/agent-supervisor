"""Control-plane latency bench for the P1 "resident dispatch" hypothesis.

Everything here measures dispatch overhead only: process spawn, protocol
handshakes and zero-model RPCs (``session/create``/``session/subscribe``,
``initialize``, ``thread/start``). No section ever sends a prompt unless the
caller explicitly opts in with ``--with-model`` (real model quota; intended
for the operator's real-machine run only). Every section degrades instead of
crashing: a missing parallel module (``zcode_resident``/``codex_appserver``)
falls back to bench-owned equivalents and the report says so. Zero-model RPCs
still persist session bookkeeping on the agents' side (ZCode session storage,
Codex rollouts for non-ephemeral threads); each section's notes state exactly
how many records it can leave. Benches read the existing configuration and
write only ``out_dir`` (scratch protocol evidence lives under
``out_dir/scratch`` and is retained).
"""

from __future__ import annotations

import argparse
import asyncio
import contextlib
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
import inspect
import json
import math
import os
from pathlib import Path
import platform
import shutil
import sys
from tempfile import TemporaryDirectory
import time
import uuid

from . import protocol  # ProtocolClient; tests patch protocol.ProtocolClient
from .common import atomic_write, atomicwrite_json
from .runtime import load_config

DEFAULT_CODEX = Path('/Applications/ChatGPT.app/Contents/Resources/codex-cli/'
                     'CodexCLI.app/Contents/MacOS/codex')
BENCH_CLIENT_INFO = {'name': 'czs-bench', 'version': '0'}
ZCODE_COLD = 'zcode-cold'
ZCODE_RESIDENT = 'zcode-resident'
CODEX_SPAWN = 'codex-spawn'
CODEX_APPSERVER_INIT = 'codex-appserver-init'
CODEX_APPSERVER_THREAD = 'codex-appserver-thread'
ZCODE_WITH_MODEL = 'zcode-with-model'
CODEX_EXEC_WITH_MODEL = 'codex-exec-with-model'
CODEX_SECTIONS = (CODEX_SPAWN, CODEX_APPSERVER_INIT, CODEX_APPSERVER_THREAD,
                  CODEX_EXEC_WITH_MODEL)
SECTION_ORDER = (ZCODE_COLD, ZCODE_RESIDENT, CODEX_SPAWN, CODEX_APPSERVER_INIT,
                 CODEX_APPSERVER_THREAD, ZCODE_WITH_MODEL, CODEX_EXEC_WITH_MODEL)
STATS_KEYS = ('min', 'p50', 'p90', 'p99', 'max')
MODEL_TRIALS_CAP = 3
# Zero-model RPCs can still block on a cold CLI; bound every step so a hung
# process surfaces as one failed trial instead of a hung bench.
REQUEST_TIMEOUT_S = 60
MODEL_TURN_TIMEOUT_S = 300


def percentile(samples, p):
    """Linear-interpolation percentile of ``samples``; ``p`` is in percent."""
    if isinstance(p, bool) or not isinstance(p, (int, float)) \
            or not math.isfinite(p) or not 0 <= p <= 100:
        raise ValueError('percentile point must be a finite number in [0, 100]')
    values = [float(sample) for sample in samples]
    if not values:
        raise ValueError('percentile requires at least one sample')
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    rank = (len(ordered) - 1) * (p / 100.0)
    lower, upper = math.floor(rank), math.ceil(rank)
    if lower == upper:
        return ordered[lower]
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (rank - lower)


def stats(samples):
    """Millisecond summary ``{"n","min","p50","p90","p99","max","mean"}``."""
    values = [float(sample) for sample in samples]
    if not values:
        raise ValueError('stats requires at least one sample')
    return {'n': len(values), 'min': round(min(values), 1),
            'p50': round(percentile(values, 50), 1),
            'p90': round(percentile(values, 90), 1),
            'p99': round(percentile(values, 99), 1),
            'max': round(max(values), 1),
            'mean': round(sum(values) / len(values), 1)}


async def measure(fn, trials, *, warmup=1):
    """Time ``await fn()`` per trial in milliseconds via time.perf_counter.

    ``warmup`` calls run first and are discarded. A trial that raises is
    recorded as ``None`` (and still consumes its slot): callers filter the
    ``None`` entries and count them for the report. A trial may return its own
    number of milliseconds to define the timing endpoint inside the trial
    (used when cleanup must stay outside the measured window); ``None``/other
    return values fall back to the outer wall-clock delta.
    """
    if isinstance(trials, bool) or not isinstance(trials, int) or trials < 0 \
            or isinstance(warmup, bool) or not isinstance(warmup, int) or warmup < 0:
        raise ValueError('trials and warmup must be nonnegative integers')
    samples = []
    for index in range(warmup + trials):
        started = time.perf_counter()
        failed, value = False, None
        try:
            value = await fn()
        except asyncio.CancelledError:
            raise
        except Exception:
            failed = True
        elapsed = (time.perf_counter() - started) * 1000.0
        if index < warmup:
            continue
        if failed:
            samples.append(None)
        elif isinstance(value, (int, float)) and not isinstance(value, bool):
            samples.append(float(value))
        else:
            samples.append(elapsed)
    return samples


def _section_result(samples, notes):
    """Assemble one section entry: stats over the good trials plus notes."""
    values = [sample for sample in samples if sample is not None]
    failures = len(samples) - len(values)
    entry = {'status': 'ok'}
    if failures:
        entry['errors'] = failures
    if values:
        entry.update(stats(values))
    else:
        entry['status'] = 'error'
        entry['error'] = f'all {failures} trial(s) failed'
    text = [note for note in notes if note]
    if failures:
        text.append(f'{failures} failed trial(s) excluded from stats')
    entry['notes'] = '; '.join(text)
    return entry


def _bench_permission(_params):
    return {'decision': 'deny', 'reason': 'czs bench sends no model turn'}


def _create_params(workspace):
    return {'workspace': {'workspacePath': str(workspace),
                          'workspaceKey': str(workspace)},
            'mode': 'build', 'titleGenerationEnabled': False, 'mcpServers': [],
            'toolAllowlist': ['Read', 'Write', 'Edit', 'Bash'],
            'dynamicWorkflowEnabled': False}


def _subscribe_params(session_id):
    return {'sessionId': session_id, 'deliveryKind': 'desktop-continuous',
            'includeSnapshot': False}


@contextmanager
def _bench_workspace(scratch_dir, name):
    """Scratch workspace (and protocol-evidence parent) for one section."""
    if scratch_dir is None:
        with TemporaryDirectory(prefix='czs-bench-') as temporary:
            yield Path(temporary)
        return
    root = Path(scratch_dir) / name
    root.mkdir(parents=True, exist_ok=True)
    yield root


async def _request(client, method, params, timeout=REQUEST_TIMEOUT_S):
    return await asyncio.wait_for(client.request(method, params), timeout)


async def _zcode_handshake(client, workspace, set_model=None):
    """create [+ optional setModel] + subscribe; returns (session id, event seq)."""
    snapshot = await _request(client, 'session/create', _create_params(workspace))
    session_id = snapshot['session']['sessionId']
    if set_model is not None:
        await _request(client, 'session/setModel',
                       {'sessionId': session_id, 'model': set_model,
                        'persistAsWorkspaceLastUsed': False})
    subscription = await _request(client, 'session/subscribe',
                                  _subscribe_params(session_id))
    return session_id, subscription.get('eventSeq')


async def bench_zcode_cold(config, trials, *, set_model=None, scratch_dir=None):
    """Cold path: per trial a fresh ProtocolClient start + create/subscribe.

    Timing covers start through the subscribe response; close happens after
    the timing endpoint. warmup=0 on purpose: warming up would measure a warm
    OS cache and defeat the cold-start question being asked.
    """
    notes = ['per trial: fresh ProtocolClient start + session/create'
             + (' + session/setModel' if set_model is not None else '')
             + ' + session/subscribe, then close (close outside the timed window);'
               ' no prompt, zero model calls; warmup=0 (cold semantics)']
    with _bench_workspace(scratch_dir, ZCODE_COLD) as root:
        counter = {'trial': 0}

        async def trial():
            counter['trial'] += 1
            output = root / f"cold-{counter['trial']}.json"
            client = protocol.ProtocolClient(root, output, _bench_permission, config)
            started = time.perf_counter()
            try:
                await asyncio.wait_for(client.start(), REQUEST_TIMEOUT_S)
                await _zcode_handshake(client, root, set_model)
                return (time.perf_counter() - started) * 1000.0
            finally:
                await client.close()

        samples = await measure(trial, trials, warmup=0)
    notes.append('each successful trial leaves one empty session record in the '
                 f'local ZCode session store ({trials} attempt(s) made)')
    return _section_result(samples, notes)


def _resident_class():
    from zcode_supervisor import zcode_resident  # parallel agent's module
    holder_class = getattr(zcode_resident, 'ResidentZCode', None)
    if holder_class is None:
        raise TypeError('zcode_resident module has no ResidentZCode')
    return holder_class


async def _maybe_await(value):
    if inspect.isawaitable(value):
        await value


async def _close_holder(holder):
    """Close a resident holder through whichever lifecycle method it has."""
    for name in ('close', 'stop', 'shutdown'):
        method = getattr(holder, name, None)
        if callable(method):
            await _maybe_await(method())
            return


def _find_protocol_client(holder):
    """Locate the ProtocolClient a ResidentZCode wraps, without guessing names."""
    candidates = list(vars(holder).values()) if hasattr(holder, '__dict__') else []
    for name in dir(holder):
        if not name.startswith('_'):
            try:
                candidates.append(getattr(holder, name))
            except Exception:
                continue
    seen = set()
    for value in candidates:
        if isinstance(value, protocol.ProtocolClient) and id(value) not in seen:
            return value
    raise TypeError('ResidentZCode exposes no ProtocolClient to time against')


async def _start_resident(config, evidence_root):
    """Start ResidentZCode and return (holder, client); degrade by raising."""
    holder_class = _resident_class()
    holder = None
    for args in ((config, evidence_root), (config,), ()):
        try:
            holder = holder_class(*args)
            break
        except TypeError:
            continue
    if holder is None:
        raise TypeError('ResidentZCode constructor rejected every attempted shape')
    await _maybe_await(holder.start())
    return holder, _find_protocol_client(holder)


async def bench_zcode_resident(config, trials, *, scratch_dir=None):
    """Resident path: one long-lived client, N create/subscribe round trips.

    Prefers the parallel agent's ResidentZCode (timing rides ITS client, which
    its own start() already launched); anything missing or unusable degrades
    to a bench-owned long-lived ProtocolClient, which measures the same
    control-plane round trip.
    """
    notes = []
    with _bench_workspace(scratch_dir, ZCODE_RESIDENT) as root:
        holder, client, owned = None, None, True
        try:
            holder, client = await _start_resident(config, root)
            owned = False
            notes.append('resident-module: timing create/subscribe on '
                         'zcode_resident.ResidentZCode own client')
        except Exception as error:
            if holder is not None:
                try:
                    await _close_holder(holder)
                except Exception:
                    pass
            notes.append(f'degraded: zcode_resident unusable '
                         f'({type(error).__name__}: {str(error)[:160]}); using a '
                         'bench-owned long-lived ProtocolClient instead')
        if client is None:
            client = protocol.ProtocolClient(root, root / 'resident.json',
                                             _bench_permission, config)
            await asyncio.wait_for(client.start(), REQUEST_TIMEOUT_S)
        try:

            async def trial():
                started = time.perf_counter()
                await _zcode_handshake(client, root)
                return (time.perf_counter() - started) * 1000.0

            # warmup=1 (default): resident dispatch is a steady-state question.
            samples = await measure(trial, trials)
        finally:
            if owned:
                await client.close()
            else:
                try:
                    await _close_holder(holder)
                except Exception:
                    pass
                finally:
                    await client.close()
    attempts = trials + 1  # warmup creates one extra session
    notes.append('one long-lived client; every create (warmup included) leaves '
                 'an empty session record in the local ZCode session store '
                 f'({attempts} attempt(s) made)')
    return _section_result(samples, notes)


async def _spawn_version(codex_path):
    """Time one full ``codex --version`` process lifecycle (spawn to exit)."""
    started = time.perf_counter()
    process = await asyncio.create_subprocess_exec(
        str(codex_path), '--version', stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.DEVNULL)
    try:
        returncode = await asyncio.wait_for(process.wait(), 30)
    except asyncio.CancelledError:
        process.kill()
        await process.wait()
        raise
    elapsed = (time.perf_counter() - started) * 1000.0
    if returncode != 0:
        raise ValueError(f'codex --version exited {returncode}')
    return elapsed


async def bench_codex_spawn(codex_path, trials):
    """Cold binary spawn baseline: full ``codex --version`` process lifetime."""
    path = Path(codex_path)
    if not path.is_file():
        raise ValueError(f'codex binary not found: {path}')
    samples = await measure(lambda: _spawn_version(path), trials, warmup=0)
    return _section_result(samples, [
        'codex --version spawn-to-exit; warmup=0 (cold-process semantics); '
        'leaves no session records'])


class _AppServerJsonl:
    """Minimal codex app-server stdio JSONL client (sequential requests).

    Only what the bench needs: spawn, request/response correlation by id,
    skip notifications, bounded close. Server requests that expect replies are
    ignored; for the short-lived zero-model probes here the server tolerates
    that, and a real handshake conversation is the parallel module's job.
    """

    def __init__(self, codex_path):
        self._argv = [str(codex_path), 'app-server']
        self.process = None
        self._counter = 0

    async def start(self):
        self.process = await asyncio.create_subprocess_exec(
            *self._argv, stdin=asyncio.subprocess.PIPE,
            stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL,
            limit=8 * 1024 ** 2)

    async def request(self, method, params, timeout=REQUEST_TIMEOUT_S):
        if self.process is None or self.process.stdin is None:
            raise EOFError('codex app-server client is not running')
        self._counter += 1
        request_id = self._counter
        frame = {'id': request_id, 'method': method, 'params': params}
        wire = (json.dumps(frame, ensure_ascii=False) + '\n').encode()
        self.process.stdin.write(wire)
        await self.process.stdin.drain()
        deadline = time.monotonic() + timeout
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise asyncio.TimeoutError(f'{method} exceeded {timeout}s')
            line = await asyncio.wait_for(self.process.stdout.readline(), remaining)
            if not line:
                raise EOFError('codex app-server stream closed')
            if not line.strip():
                continue
            try:
                message = json.loads(line)
            except ValueError:
                continue
            if not isinstance(message, dict) or message.get('id') != request_id:
                continue
            if 'error' in message:
                raise ValueError(f'{method} failed: {str(message["error"])[:200]}')
            if 'result' in message:
                return message['result']

    async def close(self):
        if self.process is None:
            return
        if self.process.stdin is not None:
            self.process.stdin.close()
        try:
            await asyncio.wait_for(self.process.wait(), 5)
        except asyncio.TimeoutError:
            self.process.terminate()
            try:
                await asyncio.wait_for(self.process.wait(), 3)
            except asyncio.TimeoutError:
                self.process.kill()
                await self.process.wait()


def _appserver_client_maker(codex_path):
    """Return ``make(evidence_dir) -> client`` from codex_appserver or a reason.

    The parallel module is preferred only when it quacks like the bench
    client (async start/request/close) and its constructor accepts either
    ``(codex_path)`` or ``(codex_path, evidence_dir)``; otherwise the bench
    degrades to its own minimal JSONL client. The landed module takes the
    evidence directory and its ``start()`` already performs initialize.
    """
    try:
        from zcode_supervisor import codex_appserver  # parallel agent's module
    except Exception as error:
        return None, f'codex_appserver unavailable ({type(error).__name__})'
    client_class = getattr(codex_appserver, 'CodexAppServerClient', None)
    if client_class is None:
        return None, 'codex_appserver has no CodexAppServerClient'
    if not all(callable(getattr(client_class, name, None))
               for name in ('start', 'request', 'close')):
        return None, 'CodexAppServerClient lacks the start/request/close surface'
    try:
        positional = len(inspect.signature(client_class.__init__).parameters)
    except (TypeError, ValueError):
        positional = 2
    if positional >= 3:
        return (lambda evidence_dir: client_class(codex_path, evidence_dir)), None
    return (lambda evidence_dir: client_class(codex_path)), None


def _resolve_codex(codex_path):
    """Explicit path first, then the known install location, then PATH."""
    if codex_path is not None:
        return Path(codex_path)
    if DEFAULT_CODEX.is_file():
        return DEFAULT_CODEX
    which = shutil.which('codex')
    return Path(which) if which else None


def _require_codex(codex_path):
    path = Path(codex_path)
    if not path.is_file():
        raise ValueError(f'codex binary not found: {path}')
    return path


async def _initialize_client(client):
    """Spawn + initialize handshake, tolerating clients that self-initialize."""
    result = client.start()
    if inspect.isawaitable(result):
        result = await result
    if isinstance(result, dict) \
            or getattr(client, 'initialize_result', None) is not None:
        return  # the client's own start() already completed the handshake
    await client.request('initialize',
                         {'clientInfo': BENCH_CLIENT_INFO, 'capabilities': {}},
                         REQUEST_TIMEOUT_S)


async def bench_codex_appserver(codex_path, trials, out_dir):
    """Per trial: spawn app-server stdio + initialize handshake, then close.

    Timing runs from spawn to the initialize response; close stays outside
    the timed window. warmup=0 keeps the cold-process semantics.
    """
    path = _require_codex(codex_path)
    notes = []
    maker, reason = _appserver_client_maker(path)
    notes.append('using codex_appserver.CodexAppServerClient' if maker is not None
                 else f'degraded: {reason}; using the bench minimal JSONL client')
    scratch = Path(out_dir) / 'scratch' / CODEX_APPSERVER_INIT
    counter = {'trial': 0}

    async def trial():
        counter['trial'] += 1
        evidence = scratch / f"evidence-{counter['trial']}"
        if maker is not None:
            evidence.mkdir(parents=True, exist_ok=True)
            client = maker(evidence)
        else:
            client = _AppServerJsonl(path)
        started = time.perf_counter()
        try:
            await _initialize_client(client)
            return (time.perf_counter() - started) * 1000.0
        finally:
            await client.close()

    samples = await measure(trial, trials, warmup=0)
    notes.append('per trial: spawn + initialize response (no thread, no turn, '
                 'zero model calls); leaves no session or rollout records')
    return _section_result(samples, notes)


async def bench_codex_appserver_thread(codex_path, trials, out_dir):
    """One resident app-server: initialize once, then N thread/start round trips.

    Threads are read-only sandboxed; ephemeral threads (no rollout on disk)
    are tried first and the fallback to persisted threads is recorded in the
    notes with its rollout-record count. No turn is ever started.
    """
    path = _require_codex(codex_path)
    notes = []
    maker, reason = _appserver_client_maker(path)
    notes.append('using codex_appserver.CodexAppServerClient' if maker is not None
                 else f'degraded: {reason}; using the bench minimal JSONL client')
    with _bench_workspace(Path(out_dir) / 'scratch', CODEX_APPSERVER_THREAD) as root:
        if maker is not None:
            evidence = root / 'evidence'
            evidence.mkdir(parents=True, exist_ok=True)
            client = maker(evidence)
        else:
            client = _AppServerJsonl(path)
        try:
            await _initialize_client(client)
            thread_params = {'cwd': str(root), 'sandbox': 'read-only'}
            try:
                await client.request('thread/start',
                                     {**thread_params, 'ephemeral': True},
                                     REQUEST_TIMEOUT_S)
                ephemeral = True
                notes.append('thread/start accepts ephemeral=true: no rollout '
                             'records are written for probe/warmup/trials')
            except Exception as error:
                ephemeral = False
                notes.append(f'ephemeral thread/start rejected ({str(error)[:160]}); '
                             'fallback: persisted threads')
            params = ({**thread_params, 'ephemeral': True} if ephemeral
                      else thread_params)

            async def trial():
                started = time.perf_counter()
                await client.request('thread/start', params, REQUEST_TIMEOUT_S)
                return (time.perf_counter() - started) * 1000.0

            # warmup=1 (default): resident thread dispatch is steady-state.
            samples = await measure(trial, trials)
        finally:
            await client.close()
    attempts = trials + 2  # one ephemeral probe + one warmup + trials
    if not ephemeral:
        notes.append('persisted thread/start writes rollout records under '
                     f'$CODEX_HOME/sessions: about {attempts} for this section')
    notes.append('read-only sandbox, no turn is ever started (zero model calls)')
    return _section_result(samples, notes)


async def _await_turn(client, session_id, start_seq, timeout_s):
    """Wait for the session's first post-subscribe turn terminal event."""
    deadline = time.monotonic() + timeout_s
    processed = 0
    while time.monotonic() < deadline:
        while processed < len(client.events):
            event = client.events[processed]
            processed += 1
            if event.get('sessionId') != session_id \
                    or event.get('seq', -1) <= start_seq:
                continue
            if event.get('type') == 'turn.completed':
                return event
            if event.get('type') == 'turn.failed':
                payload = event.get('payload', {})
                raise ValueError(f"turn failed: {payload.get('error')}")
        if client.failure:
            raise EOFError(client.failure)
        await asyncio.sleep(0.02)
    raise asyncio.TimeoutError(f'model turn exceeded {timeout_s}s')


async def bench_zcode_with_model(config, trials, *, scratch_dir=None,
                                 prompt='只回复 OK',
                                 timeout_s=MODEL_TURN_TIMEOUT_S):
    """Opt-in end-to-end: cold client + one tiny model turn, timed as a whole.

    This CONSUMES MODEL QUOTA (one minimal turn per trial) and leaves one
    session record per trial. Intended for the operator's real-machine run.
    """
    notes = [f'end-to-end incl. one tiny model turn ({prompt!r}); warmup=0; '
             'CONSUMES MODEL QUOTA; leaves one session record per successful trial']
    with _bench_workspace(scratch_dir, 'zcode-with-model') as root:
        counter = {'trial': 0}

        async def trial():
            counter['trial'] += 1
            output = root / f"model-{counter['trial']}.json"
            client = protocol.ProtocolClient(root, output, _bench_permission, config)
            started = time.perf_counter()
            try:
                await asyncio.wait_for(client.start(), REQUEST_TIMEOUT_S)
                session_id, start_seq = await _zcode_handshake(client, root)
                input_id = 'bench_' + uuid.uuid4().hex
                await _request(client, 'session/send',
                               {'sessionId': session_id, 'inputId': input_id,
                                'content': prompt})
                completed = await _await_turn(client, session_id, start_seq, timeout_s)
                if completed['payload'].get('resultType') != 'success':
                    raise ValueError('model turn did not complete successfully')
                return (time.perf_counter() - started) * 1000.0
            finally:
                await client.close()

        samples = await measure(trial, trials, warmup=0)
    return _section_result(samples, notes)


async def bench_codex_exec_with_model(codex_path, trials, out_dir,
                                      prompt='只回复 OK。不要执行任何命令。',
                                      timeout_s=MODEL_TURN_TIMEOUT_S):
    """Opt-in end-to-end: one tiny ``codex exec`` run per trial (model quota)."""
    from .codex_driver import run_readonly
    path = _require_codex(codex_path)
    root = Path(out_dir) / 'scratch' / 'codex-with-model'
    root.mkdir(parents=True, exist_ok=True)
    schema = {'type': 'object', 'properties': {'reply': {'type': 'string'}},
              'required': ['reply']}
    counter = {'trial': 0}
    loop = asyncio.get_running_loop()

    async def trial():
        counter['trial'] += 1
        evidence = root / f"exec-{counter['trial']}"
        receipt = await loop.run_in_executor(
            None, lambda: run_readonly(path, root, prompt, schema, evidence,
                                       timeout_s=timeout_s))
        if receipt.outcome != 'delivered':
            raise ValueError(f'codex exec outcome {receipt.outcome}: {receipt.error}')
        return receipt.duration_s * 1000.0

    samples = await measure(trial, trials, warmup=0)
    return _section_result(samples, [
        f'end-to-end codex exec incl. one tiny model turn ({prompt!r}); '
        'CONSUMES MODEL QUOTA; each trial leaves a rollout record under '
        '$CODEX_HOME/sessions plus evidence under out_dir/scratch'])


# ---- P0.0: fake/replay baseline fixtures and measurement (PLAN §7 P0.0) ----
# Everything below drives the fixture child tests/fixtures/ui/fake_server.py
# through the REAL ProtocolClient / CodexAppServerClient / ResidentZCode. No
# real CLI, model call, or remote task is ever involved: model_requests stays
# 0 and each section proves it from the child's own counters.

FAKE_COLD = 'fake-cold-lifecycle'
FAKE_APPSERVER = 'fake-appserver'
FAKE_RESIDENT = 'fake-resident-lifecycle'
FAKE_CLOCK = 'fake-clock-sidecar'
FAKE_READ_LIST = 'fake-read-list'
FAKE_GENERATION_EXIT = 'fake-generation-exit'
FAKE_UI_JOURNAL = 'fake-ui-journal-server'
# Mirrors tests/fixtures/ui/fake_server.py FAULT_MODES; kept in sync by
# tools/bench_ui.py's --list-faults check.
FAKE_CHILD_FAULT_MODES = ('delay-reply', 'delay-first', 'delay-turn',
                          'never-complete', 'block-drain', 'silent-initialize')
WALL_GATE_BASELINE_ONLY = 'baseline-only'
UI_FIXTURE_KINDS = ('task.accepted', 'session.updated', 'turn.started',
                    'turn.completed', 'approval.requested', 'stream.gap')
UI_PROBE_ORDER = ('source', 'journal_enqueued', 'ui_received',
                  'reducer_committed', 'frame_submitted')


def monotonic_ns():
    """Same-source CLOCK_MONOTONIC reading; the only clock used for timing."""
    return time.clock_gettime_ns(time.CLOCK_MONOTONIC)


class FakeRuntimeConfig:
    """Config stand-in whose ``command()`` spawns the fixture fake child.

    Deliberately not ``runtime.Config``: no CLI bundle is prepared or
    validated. ``environment()`` returns a scrubbed environment so no
    credentials or provider configuration ever reach the child.
    """

    def __init__(self, server_path, work_root, state_dir, cache_dir):
        self.server_path = Path(server_path)
        self.work_root = Path(work_root)
        self.state_dir = Path(state_dir)
        self.cache_dir = Path(cache_dir)

    def command(self):
        return [sys.executable, str(self.server_path), '--role', 'zcode']

    def environment(self):
        return {'CZS_FAKE_CHILD': '1'}


def _fake_layout(server_path, root):
    """Create the work/state/cache/packets layout under ``root``."""
    root = Path(root)
    for name in ('work', 'state', 'cache', 'packets'):
        (root / name).mkdir(parents=True, exist_ok=True)
    return FakeRuntimeConfig(server_path, root / 'work', root / 'state',
                             root / 'cache')


def phase_stats(samples):
    """Median/p95 summary plus the UNROUNDED raw samples themselves.

    Review contract: every measured phase keeps its per-trial samples in the
    JSON document so median/p95 can be recomputed independently; empty input
    stays explicitly baseline-pending instead of a fabricated summary.
    """
    values = [float(sample) for sample in samples if sample is not None]
    if not values:
        return {'n': 0, 'status': 'baseline-pending'}
    return {'n': len(values), 'median': round(percentile(values, 50), 3),
            'p95': round(percentile(values, 95), 3),
            'min': round(min(values), 3), 'max': round(max(values), 3),
            'mean': round(sum(values) / len(values), 3),
            'samples': values}


def build_ui_fixture(spec):
    """Deterministic PLAN §5.3.2 UI fixture: 100 nodes/300 edges/10k events.

    v1 is pinned to the exact §5.3 shape; any other spec is rejected so the
    digest in tests/fixtures/ui/ui-fixture-spec.json stays meaningful. All
    content is synthetic: no credentials, no real session identifiers.
    """
    shape = {'nodes': 100, 'edges': 300, 'events': 10000, 'replay_events_per_s': 100}
    if isinstance(spec, dict) and isinstance(spec.get('spec'), dict):
        spec = spec['spec']  # accept the whole pinned spec document
    if not isinstance(spec, dict) or any(spec.get(key) != value
                                         for key, value in shape.items()):
        raise ValueError('ui fixture v1 is pinned to the PLAN 5.3 shape: '
                         + json.dumps(shape, sort_keys=True))
    nodes = ['node-%03d' % index for index in range(spec['nodes'])]
    edges = []
    for index in range(spec['edges']):
        left = index % spec['nodes']
        right = (left + 1 + index // spec['nodes']) % spec['nodes']
        edges.append([nodes[left], nodes[right]])
    base = datetime(2070, 1, 1, tzinfo=timezone.utc)
    step_ns = 1_000_000_000 // spec['replay_events_per_s']
    events = [{
        'event_id': 'evt-%05d' % index, 'seq': index,
        'kind': UI_FIXTURE_KINDS[index % len(UI_FIXTURE_KINDS)],
        'task_id': 'task-fake-%02d' % (index % 7),
        'node_id': nodes[index % spec['nodes']],
        'monotonic_ns': 1_000_000_000_000 + index * step_ns,
        'observed_at': (base + timedelta(seconds=index)).isoformat(),
    } for index in range(spec['events'])]
    return {'fixture_version': 'ui-fixture-v1', 'spec': dict(spec),
            'nodes': nodes, 'edges': edges, 'events': events,
            'replay': {'events_per_s': spec['replay_events_per_s'],
                       'start_event_id': 'evt-00000',
                       'marker_running': 'evt-05000',
                       'marker_terminal': 'evt-09999'}}


def canonical_fixture_bytes(fixture):
    """Canonical serialization used for the pinned fixture digest."""
    return json.dumps(fixture, sort_keys=True, separators=(',', ':'),
                      ensure_ascii=True).encode()


def ui_event_latency(probes):
    """Monotonic-only latency segments; ``observed_at`` never participates."""
    order = UI_PROBE_ORDER
    marks = {key: probes[key] for key in order
             if isinstance(probes.get(key), int) and not isinstance(probes[key], bool)}
    keys = [key for key in order if key in marks]
    segments = {f'{left}__to__{right}': marks[right] - marks[left]
                for left, right in zip(keys, keys[1:])}
    result = {'segments_ns': segments, 'observed_at_used': False}
    if 'source' in marks and 'frame_submitted' in marks:
        result['source_to_present_ns'] = marks['frame_submitted'] - marks['source']
    else:
        result['end_to_end_latency'] = 'unavailable'
    return result


def _fixture_workspace(work_root, name='ws-a', files=8, size=1024):
    """Deterministic frozen-input workspace for the resident rounds.

    Resolved on purpose: checked_locations walks the packet workspace
    lexically and compares against the resolved work_root, so an unresolved
    tempdir prefix (/var vs /private/var) would be misread as a symlink.
    """
    workspace = (Path(work_root) / name).resolve()
    workspace.mkdir(parents=True, exist_ok=True)
    for index in range(files):
        payload = (('fake-input %02d ' % index) * (size // 13 + 1))[:size]
        (workspace / ('input-%02d.txt' % index)).write_text(payload)
    return workspace


def _fixture_packet(packets_dir, name, workspace):
    body = {'workspace': str(workspace),
            'prompt': 'P0.0 fake/replay baseline fixture prompt',
            'allowed_files': [], 'commands': []}
    path = Path(packets_dir) / f'{name}.json'
    path.write_text(json.dumps(body))
    return path


class _AuthorityCollector:
    """Append-only log of instrumented inventory/check_frozen/write/rpc calls."""

    def __init__(self):
        self.entries = []

    def record(self, kind, duration_ms, **extra):
        entry = {'kind': kind, 'duration_ms': float(duration_ms),
                 'at': time.monotonic()}
        entry.update(extra)
        self.entries.append(entry)


def _hashed_bytes(workspace, inventory_result):
    total = 0
    base = Path(workspace)
    for name in inventory_result:
        try:
            total += (base / name).stat().st_size
        except OSError:
            continue
    return total


@contextmanager
def _instrumented_resident(collector):
    """Runtime-only timing wrappers around the production authority path.

    Wraps (never replaces the behavior of) ``inventory``, ``Authority.check_frozen``,
    ``write_json`` and ``ProtocolClient.request``; restores every original on
    exit. Internal ``check_frozen`` inventories resolve through the patched
    module global, so all five scans of a clean round are counted.
    """
    from . import authority as authority_module
    from . import zcode_resident as resident_module
    real_inventory = authority_module.inventory
    real_check_frozen = authority_module.Authority.check_frozen
    real_authority_class = resident_module.Authority
    real_write = resident_module.write_json
    real_request = protocol.ProtocolClient.request
    real_own_event = resident_module.own_event
    real_evidence = resident_module.execution_evidence

    def timed_inventory(workspace):
        started = time.perf_counter()
        result = real_inventory(workspace)
        collector.record('inventory', (time.perf_counter() - started) * 1000.0,
                         workspace=str(workspace),
                         hashed_bytes=_hashed_bytes(workspace, result))
        return result

    class TimedAuthority(real_authority_class):
        def check_frozen(self):
            started = time.perf_counter()
            try:
                return real_check_frozen(self)
            finally:
                collector.record('check_frozen',
                                 (time.perf_counter() - started) * 1000.0)

    def timed_write(path, data):
        started = time.perf_counter()
        try:
            return real_write(path, data)
        finally:
            collector.record('write_json', (time.perf_counter() - started) * 1000.0,
                             path=str(path))

    async def timed_request(self, method, params):
        started = time.perf_counter()
        try:
            return await real_request(self, method, params)
        finally:
            collector.record('rpc', (time.perf_counter() - started) * 1000.0,
                             method=method)

    def timed_own_event(event, session_id, turn_id):
        # Called by the production drain loop exactly once per event that
        # passed the session/seq gate: counting these yields the events the
        # drain ACTUALLY processed (not the shared list length).
        collector.record('own_event', 0.0)
        return real_own_event(event, session_id, turn_id)

    def timed_execution_evidence(events, authority):
        started = time.perf_counter()
        try:
            return real_evidence(events, authority)
        finally:
            collector.record('evidence_scan', (time.perf_counter() - started) * 1000.0,
                             events=len(events))

    authority_module.inventory = timed_inventory
    resident_module.inventory = timed_inventory
    resident_module.Authority = TimedAuthority
    resident_module.write_json = timed_write
    protocol.ProtocolClient.request = timed_request
    resident_module.own_event = timed_own_event
    resident_module.execution_evidence = timed_execution_evidence
    try:
        yield
    finally:
        authority_module.inventory = real_inventory
        resident_module.inventory = real_inventory
        resident_module.Authority = real_authority_class
        resident_module.write_json = real_write
        protocol.ProtocolClient.request = real_request
        resident_module.own_event = real_own_event
        resident_module.execution_evidence = real_evidence


async def _heartbeat(interval_s, samples, stop):
    """Event-loop lag probe: expected/actual monotonic gap per beat, in ms."""
    next_at = time.monotonic() + interval_s
    while not stop.is_set():
        delay = next_at - time.monotonic()
        if delay > 0:
            await asyncio.sleep(delay)
        samples.append(max(0.0, (time.monotonic() - next_at) * 1000.0))
        next_at += interval_s


def _split_round(entries, wall_ms):
    """Classify one round's instrumented entries into the P0.0 marks.

    All values stay UNROUNDED (the raw-sample contract); display rounding
    happens only inside phase_stats summaries.
    """
    rpcs = [entry for entry in entries if entry['kind'] == 'rpc']
    inventories = [entry for entry in entries if entry['kind'] == 'inventory']
    checks = [entry for entry in entries if entry['kind'] == 'check_frozen']
    writes = [entry for entry in entries if entry['kind'] == 'write_json']
    scans = [entry for entry in entries if entry['kind'] == 'evidence_scan']
    by_method = {}
    for entry in rpcs:
        by_method.setdefault(entry['method'], []).append(entry['duration_ms'])
    return {
        'total_wall_ms': float(wall_ms),
        'comm_rpc_ms': float(sum(entry['duration_ms'] for entry in rpcs)),
        'rpc_by_method_ms': {method: [float(value) for value in values]
                             for method, values in sorted(by_method.items())},
        'session_read_ms': [float(value) for value in
                            by_method.get('session/read', [])],
        'inventory_calls': len(inventories),
        'inventory_ms': [float(entry['duration_ms']) for entry in inventories],
        'hashed_bytes': sum(entry['hashed_bytes'] for entry in inventories),
        'check_frozen_ms': [float(entry['duration_ms']) for entry in checks],
        'write_json_ms': [float(entry['duration_ms']) for entry in writes],
        'drain_events_evaluated': sum(1 for entry in entries
                                      if entry['kind'] == 'own_event'),
        'evidence_scan_events': int(scans[-1]['events']) if scans else None,
        'sync_segments_ms': {
            'first_inventory_ms':
                inventories[0]['duration_ms'] if inventories else None,
            'pre_check_frozen_ms':
                checks[0]['duration_ms'] if checks else None,
            'post_success_check_frozen_ms':
                checks[1]['duration_ms'] if len(checks) > 1 else None,
            'finally_check_frozen_ms':
                checks[2]['duration_ms'] if len(checks) > 2 else None,
            'finally_after_inventory_ms':
                inventories[-1]['duration_ms'] if len(inventories) > 1 else None,
            'finally_write_ms':
                writes[-1]['duration_ms'] if writes else None,
        },
    }


async def bench_fake_cold(server_path, trials, *, scratch_dir=None):
    """Cold fake/replay section: spawn / create / subscribe / close split.

    Per trial a fresh real ProtocolClient starts the fixture child and runs
    the zero-model handshake; every phase is timed separately (warmup=0).
    """
    notes = ['fake child (tests/fixtures/ui/fake_server.py): real '
             'ProtocolClient spawn + session/create + session/subscribe + '
             'close, phases timed separately; warmup=0 cold semantics; zero '
             'model calls; leaves no session records (fake child only)']
    with _bench_workspace(scratch_dir, FAKE_COLD) as root:
        config = _fake_layout(server_path, root)
        samples = {'spawn': [], 'session_create': [], 'session_subscribe': [],
                   'close': []}
        aggregated = {}
        for index in range(trials):
            output = root / f'cold-{index + 1}.json'
            client = protocol.ProtocolClient(root, output, _bench_permission,
                                             config)
            try:
                started = time.perf_counter()
                await asyncio.wait_for(client.start(), REQUEST_TIMEOUT_S)
                samples['spawn'].append((time.perf_counter() - started) * 1000.0)
                started = time.perf_counter()
                await _request(client, 'session/create',
                               _create_params(root))
                samples['session_create'].append(
                    (time.perf_counter() - started) * 1000.0)
                started = time.perf_counter()
                await _request(client, 'session/subscribe',
                               _subscribe_params('sess_fake_1'))
                samples['session_subscribe'].append(
                    (time.perf_counter() - started) * 1000.0)
                _merge_counters(aggregated,
                                await _request(client, 'bench/counters', {}))
                started = time.perf_counter()
                await client.close()
                samples['close'].append((time.perf_counter() - started) * 1000.0)
            finally:
                if client.process is not None and client.process.returncode is None:
                    await client.close()
    phases = {name: phase_stats(values) for name, values in samples.items()}
    return _fake_section(phases, aggregated, notes)


def _merge_counters(target, source):
    """Sum per-child counters into one aggregate (in place)."""
    for key, value in (source or {}).items():
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            continue
        target[key] = target.get(key, 0) + value


async def bench_fake_generation_exit(server_path, *, scratch_dir=None):
    """P0.3 correctness section: generation replacement + exit projection.

    One ``worker_health.WorkerGeneration`` over the REAL ProtocolClient
    against the fixture child. Generation 1 starts and answers a
    session/create, then dies by EOF evidence; the health gate invalidates
    it (the bounded close actually awaits the child exit, projecting exactly
    one ``process.exited``), and the next ``get_or_start`` builds generation
    2 which serves another session/create. Checks: exactly two starts, one
    live generation at a time, one projected exit per generation AFTER its
    real close awaited the child, zero replays. This is a correctness probe
    only — no performance claim is made from it. Zero model calls.
    """
    from .worker_health import WorkerGeneration, instance_health
    notes = ['P0.3 correctness probe (not a timing measurement): real '
             'ProtocolClient behind a real WorkerGeneration against the fake '
             'child; EOF kills generation 1 by evidence, invalidate closes it '
             '(actually awaited child exit -> exactly one process.exited), '
             'the next get_or_start serves generation 2; zero model calls']
    with _bench_workspace(scratch_dir, FAKE_GENERATION_EXIT) as root:
        config = _fake_layout(server_path, root)
        starts = {'count': 0}

        def factory():
            starts['count'] += 1
            return protocol.ProtocolClient(root, root / f'gen-{starts["count"]}.json',
                                           _bench_permission, config)

        gate = WorkerGeneration(factory=factory)
        checks = {}
        aggregated = {}
        clients = []
        try:
            first = await gate.get_or_start()
            clients.append(first['instance'])
            await _request(first['instance'], 'session/create',
                           _create_params(root))
            # EOF evidence: the real reader ends and the client records its
            # failure — unhealthy, but nothing is projected yet.
            first['instance'].process.stdout.feed_eof()
            deadline = time.monotonic() + REQUEST_TIMEOUT_S
            while first['instance'].failure is None and time.monotonic() < deadline:
                await asyncio.sleep(0.005)
            health = instance_health(first['instance'])
            checks['gen1_unhealthy_after_eof'] = not health['healthy']
            checks['gen1_no_exit_before_close'] = \
                not first['instance'].exit_projection.emitted
            checks['gen1_invalidated'] = await gate.invalidate(
                first['generation'], health)
            deadline = time.monotonic() + REQUEST_TIMEOUT_S
            while not first['instance'].exit_projection.emitted \
                    and time.monotonic() < deadline:
                await asyncio.sleep(0.005)
            emitted = first['instance'].exit_projection.emitted
            checks['gen1_exit_projected_once'] = len(emitted) == 1
            checks['gen1_exit_generation'] = bool(emitted) \
                and emitted[0]['generation'] == first['generation']
            # The replacement generation serves requests on a fresh child.
            second = await gate.get_or_start()
            clients.append(second['instance'])
            await _request(second['instance'], 'session/create',
                           _create_params(root))
            checks['gen2_serves_requests'] = True
            checks['starts_total_two'] = starts['count'] == 2
            _merge_counters(aggregated, await _request(second['instance'],
                                                       'bench/counters', {}))
        except Exception as error:  # a broken probe is a section error
            checks['section_error'] = f'{type(error).__name__}: {error}'[:200]
        finally:
            for client in clients:
                if client.process is not None and client.process.returncode is None:
                    with contextlib.suppress(Exception):
                        await client.close()
        if len(clients) > 1:
            emitted2 = clients[1].exit_projection.emitted
            checks['gen2_exit_projected_once'] = len(emitted2) == 1
            if emitted2:
                checks['gen2_exit_generation'] = \
                    emitted2[0]['generation'] == 2
        model_requests = aggregated.get('model_requests')
        entry = {'status': 'ok', 'checks': checks,
                 'model_requests': model_requests
                 if isinstance(model_requests, int) else None,
                 'server_counters': aggregated,
                 'notes': '; '.join(notes)}
        if not all(value is True for value in checks.values()) \
                or model_requests != 0:
            entry['status'] = 'error'
        return entry


async def bench_fake_appserver(server_path, trials, *, rounds=3, scratch_dir=None,
                               start_timeout=REQUEST_TIMEOUT_S):
    """Fake codex app-server section: spawn+initialize, thread rounds, close.

    Uses the real ``codex_appserver.CodexAppServerClient`` against the fixture
    child (production ``result.thread.id`` shapes); records notification/
    reverse-task retention after the thread rounds and aggregates per-child
    counters. ``start()`` is bounded by ``start_timeout``: a child that never
    answers initialize becomes one failed trial (or a section error when every
    trial fails), never a hung bench.
    """
    from .codex_appserver import CodexAppServerClient
    notes = ['fake child via CodexAppServerClient: spawn+initialize per trial '
             '(cold, bounded by start_timeout), then one resident client for '
             'thread/start rounds; the fake emits production-shaped '
             'thread/turn/approval frames plus one notification and one '
             'reverse server request per round so unbounded retention is '
             'observable; zero model calls']
    with _bench_workspace(scratch_dir, FAKE_APPSERVER) as root:
        spawn_samples, close_samples, thread_samples = [], [], []
        aggregated, failures, last_error = {}, 0, None
        for index in range(trials):
            evidence = root / f'evidence-{index + 1}'
            evidence.mkdir(parents=True, exist_ok=True)
            client = CodexAppServerClient(server_path, evidence)
            try:
                started = time.perf_counter()
                await asyncio.wait_for(client.start(), start_timeout)
            except asyncio.TimeoutError:
                failures += 1
                last_error = (f'initialize did not answer within '
                              f'{start_timeout}s (trial {index + 1})')
                await client.close()
                continue
            spawn_samples.append((time.perf_counter() - started) * 1000.0)
            try:
                _merge_counters(aggregated, await client.request(
                    'bench/counters', {}, REQUEST_TIMEOUT_S))
            finally:
                started = time.perf_counter()
                await client.close()
                close_samples.append((time.perf_counter() - started) * 1000.0)
        retention = {}
        if failures < trials:
            evidence = root / 'evidence-resident'
            evidence.mkdir(parents=True, exist_ok=True)
            client = CodexAppServerClient(server_path, evidence)
            try:
                await asyncio.wait_for(client.start(), start_timeout)
                for index in range(rounds):
                    started = time.perf_counter()
                    thread = await client.request('thread/start',
                                                  {'cwd': str(root),
                                                   'sandbox': 'read-only',
                                                   'ephemeral': True},
                                                  REQUEST_TIMEOUT_S)
                    thread_samples.append(
                        (time.perf_counter() - started) * 1000.0)
                    thread_id = (thread.get('thread') or {}).get('id')
                    # A production-shaped turn drives the notification and
                    # reverse-request traffic the retention metric observes.
                    await client.request(
                        'turn/start',
                        {'threadId': thread_id,
                         'input': [{'type': 'text', 'text': 'bench fixture'}]},
                        REQUEST_TIMEOUT_S)
                    await asyncio.sleep(0.06)  # items + approval land
                await asyncio.sleep(0.05)
                retention = {'notifications': len(client.notifications),
                             'reverse_tasks': len(client.tasks),
                             'pending': len(client.pending)}
                _merge_counters(aggregated, await client.request(
                    'bench/counters', {}, REQUEST_TIMEOUT_S))
            finally:
                started = time.perf_counter()
                await client.close()
                close_samples.append((time.perf_counter() - started) * 1000.0)
    phases = {'spawn_initialize': phase_stats(spawn_samples),
              'thread_start': phase_stats(thread_samples),
              'close': phase_stats(close_samples)}
    entry = _fake_section(phases, aggregated, notes)
    if failures:
        entry['errors'] = failures
        if failures == trials:
            entry['status'] = 'error'
            entry['error'] = last_error or 'every initialize trial timed out'
    entry['retention_after_rounds'] = retention
    entry['rounds'] = rounds
    return entry


async def _resident_round(resident, packet, out, resume, collector, client, kind):
    events_before = len(client.events)
    index = len(collector.entries)
    started = time.monotonic()
    result = await resident.run_packet(packet, out, timeout_s=30, resume=resume)
    wall_ms = (time.monotonic() - started) * 1000.0
    round_ = _split_round(collector.entries[index:], wall_ms)
    round_['kind'] = kind
    round_['delivery_ok'] = result.get('delivery_ok')
    round_['session_id'] = result.get('session_id')
    round_['round_events_before'] = events_before
    round_['round_events_emitted'] = len(client.events) - events_before
    round_['events_retained_after'] = len(client.events)
    return round_


async def _cancel_round(resident, packets_dir, workspace, collector, client,
                        work_root, reset_reply_delay_s=0.0):
    """Arm never-complete, cancel mid-poll, time the synchronous finally.

    ``cancel_to_finish_ms`` is taken from cancel() to the moment the task
    join returns — the fault-reset RPC happens strictly AFTER that timestamp
    and is metered separately (``reset_rpc_ms``), so a slow reset cannot
    pollute the cancellation measurement. The fake keeps streaming
    intermediate events; the bench settles before reading retention so the
    emitted count is deterministic.
    """
    await _request(client, 'bench/set-fault', {'mode': 'never-complete'})
    packet = _fixture_packet(packets_dir, 'cancel', workspace)
    out = Path(work_root) / 'run-cancel.json'
    events_before = len(client.events)
    index = len(collector.entries)
    started = time.monotonic()
    task = asyncio.create_task(resident.run_packet(packet, out, timeout_s=30))
    await asyncio.sleep(0.25)  # setup + send dispatch; completion is withheld
    cancel_started = time.monotonic()
    task.cancel()
    cancelled = False
    try:
        await task
    except asyncio.CancelledError:
        cancelled = True
    finished_at = time.monotonic()  # immediately after the join, before reset
    wall_ms = (time.monotonic() - started) * 1000.0
    reset_started = time.perf_counter()
    await _request(client, 'bench/set-fault',
                   {'mode': 'none', 'reply_delay_s': float(reset_reply_delay_s)})
    reset_rpc_ms = (time.perf_counter() - reset_started) * 1000.0
    await asyncio.sleep(0.6)  # let the withheld stream finish emitting
    entries = collector.entries[index:]
    post = [entry for entry in entries if entry['at'] >= cancel_started]
    post_sync = [entry for entry in post
                 if entry['kind'] in ('inventory', 'check_frozen', 'write_json')]
    round_ = _split_round(entries, wall_ms)
    round_.update({
        'kind': 'cancel-never-complete', 'cancelled': cancelled,
        'cancel_to_finish_ms': (finished_at - cancel_started) * 1000.0,
        'reset_rpc_ms': reset_rpc_ms,
        'post_cancel_finally_ms': sum(entry['duration_ms'] for entry in post_sync),
        'post_cancel_check_frozen_ms': sum(
            entry['duration_ms'] for entry in post
            if entry['kind'] == 'check_frozen'),
        'post_cancel_after_inventory_ms': sum(
            entry['duration_ms'] for entry in post
            if entry['kind'] == 'inventory'),
        'post_cancel_write_ms': sum(
            entry['duration_ms'] for entry in post
            if entry['kind'] == 'write_json'),
        'round_events_before': events_before,
        'round_events_emitted': len(client.events) - events_before,
        'events_retained_after': len(client.events),
        'note': 'the finally check_frozen/after-inventory/write segments are '
                'synchronous and not preemptible; recorded as the P0.1 '
                'sync-segment baseline, not as a satisfied wall-time gate; the '
                'drain stopped mid-stream (see drain_events_evaluated vs '
                'round_events_emitted) while retention kept every event',
    })
    return round_


@contextmanager
def _pinned_event_router_mode(mode):
    """Bench-only env pin: force one CZS_EVENT_ROUTER mode (restored)."""
    prior = os.environ.get('CZS_EVENT_ROUTER')
    os.environ['CZS_EVENT_ROUTER'] = mode
    try:
        yield
    finally:
        if prior is None:
            os.environ.pop('CZS_EVENT_ROUTER', None)
        else:
            os.environ['CZS_EVENT_ROUTER'] = prior


async def bench_fake_resident(server_path, steady_rounds, *, scratch_dir=None,
                               cancel_reset_reply_delay_s=0.0):
    """Fake resident section: first create, steady resume/send, cancel, close.

    One real ``ResidentZCode`` over the fixture child. Runtime instrumentation
    (wrappers restored afterwards) counts every inventory call (including the
    three inside check_frozen), hashes, RPC segments per method, event-loop
    lag, event retention/rescans, and the post-cancel synchronous finally.
    """
    import resource
    from .zcode_resident import ResidentZCode
    notes = ['fake child through the real ResidentZCode: first session/create '
             '+ subscribe round, steady session/resume + subscribe + send '
             'rounds, one cancelled never-complete round, then stop; '
             'instrumentation wraps inventory/check_frozen/write_json/request '
             'at runtime (originals restored); zero model calls']
    # WP-B default flip (2026-10-30): this section's retention and
    # rescan metrics (round_events_emitted / events_retained /
    # evidence_scan_events) are DEFINED on the legacy shared event
    # store (client.events) — the explicit CZS_EVENT_ROUTER=legacy
    # rollback keeps this P0-era baseline measuring the same store;
    # the routed mode's bounded-retention story is measured by the
    # P2.2 soak tooling (tasks/P2.2/ab soak, WP-B codex soak).
    with _bench_workspace(scratch_dir, FAKE_RESIDENT) as root, \
            _pinned_event_router_mode('legacy'):
            config = _fake_layout(server_path, root)
            workspace = _fixture_workspace(root / 'work')
            collector = _AuthorityCollector()
            rounds, lag_samples = [], []
            stop = asyncio.Event()
            resident = ResidentZCode(config, root / 'work')
            with _instrumented_resident(collector):
                started = time.perf_counter()
                await resident.start()
                start_ms = (time.perf_counter() - started) * 1000.0
                client = resident.client
                heartbeat = asyncio.create_task(_heartbeat(0.02, lag_samples, stop))
                try:
                    packet = _fixture_packet(root / 'packets', 'first', workspace)
                    first = await _resident_round(
                        resident, packet, root / 'work' / 'run-first.json',
                        None, collector, client, 'first-create')
                    rounds.append(first)
                    for index in range(steady_rounds):
                        packet = _fixture_packet(root / 'packets',
                                                 f'steady-{index}', workspace)
                        rounds.append(await _resident_round(
                            resident, packet,
                            root / 'work' / f'run-steady-{index}.json',
                            first['session_id'], collector, client, 'steady-resume'))
                    rounds.append(await _cancel_round(
                        resident, root / 'packets', workspace, collector, client,
                        root / 'work',
                        reset_reply_delay_s=cancel_reset_reply_delay_s))
                    counters = await _request(client, 'bench/counters', {})
                    retention = {'events_retained': len(client.events),
                                 'reverse_tasks': len(client.tasks)}
                finally:
                    stop.set()
                    heartbeat.cancel()
                    try:
                        await heartbeat
                    except asyncio.CancelledError:
                        pass
                    started = time.perf_counter()
                    await resident.stop()
                    stop_ms = (time.perf_counter() - started) * 1000.0
            peak_rss = {
                'self_maxrss': resource.getrusage(resource.RUSAGE_SELF).ru_maxrss,
                'children_maxrss': resource.getrusage(resource.RUSAGE_CHILDREN).ru_maxrss,
                'unit': 'bytes on macOS, KiB on Linux',
            }
    phases = {
        'resident_start': phase_stats([start_ms]),
        'first_create': phase_stats([round_['total_wall_ms']
                                     for round_ in rounds
                                     if round_['kind'] == 'first-create']),
        'steady_resume': phase_stats([round_['total_wall_ms']
                                      for round_ in rounds
                                      if round_['kind'] == 'steady-resume']),
        'cancel_to_finish': phase_stats([round_['cancel_to_finish_ms']
                                         for round_ in rounds
                                         if round_['kind'] == 'cancel-never-complete']),
        'resident_stop': phase_stats([stop_ms]),
        'event_loop_lag_ms': phase_stats(lag_samples),
        'session_read_ms': phase_stats(
            [value for round_ in rounds for value in round_['session_read_ms']]),
    }
    entry = _fake_section(phases, counters, notes)
    entry.update({'rounds': rounds, 'peak_rss': peak_rss,
                  'whole_call_wall_time_gate': WALL_GATE_BASELINE_ONLY,
                  'retention': retention})
    return entry


async def bench_fake_clock_sidecar(server_path, trials, *, scratch_dir=None):
    """Same-source CLOCK_MONOTONIC probes + the blocked-drain gate smoke.

    This section measures only the same-source monotonic clock (the fixture
    child reads the same CLOCK_MONOTONIC: child mark t1 satisfies
    t0 <= t1 <= t2 for every probe) and the blocked-drain gate. A real frame
    presentation probe is deferred to P1.2; none is claimed here.
    """
    notes = ['same-source CLOCK_MONOTONIC sidecar probes against the fake '
             'child; latency math uses monotonic marks only (observed_at is '
             'display-only, raw probes retained for recomputation); the '
             'presentation probe is NOT_RUN in P0.0 — no synthetic '
             'frame-submitted stand-in is claimed; includes the blocked-drain '
             'gate smoke (a large write blocks in drain() on real OS pipe '
             'backpressure while the child withholds reading); zero model '
             'calls']
    with _bench_workspace(scratch_dir, FAKE_CLOCK) as root:
        config = _fake_layout(server_path, root)
        client = protocol.ProtocolClient(root, root / 'clock.json',
                                         _bench_permission, config)
        round_trips, probes_raw, blocked_drain = [], [], {}
        same_source = True
        armed = []
        try:
            await asyncio.wait_for(client.start(), REQUEST_TIMEOUT_S)
            for index in range(trials):
                t0 = monotonic_ns()
                response = await _request(client, 'bench/clock-probe', {})
                t2 = monotonic_ns()
                t1 = response.get('monotonic_ns')
                if not (isinstance(t1, int) and t0 <= t1 <= t2):
                    same_source = False
                round_trips.append((t2 - t0) / 1e6)
                probes_raw.append({'t0_ns': t0, 'child_ns': t1,
                                   't2_ns': t2,
                                   'observed_wall_ns': response.get('wall_ns')})
            blocked_drain = await _blocked_drain_probe(client)
            for mode in FAKE_CHILD_FAULT_MODES:  # arming smoke only (tiny delay)
                if mode == 'block-drain':
                    continue  # exercised for real above
                await _request(client, 'bench/set-fault',
                               {'mode': mode, 'delay_s': 0.02})
                armed.append(mode)
            await _request(client, 'bench/set-fault', {'mode': 'none'})
            counters = await _request(client, 'bench/counters', {})
        finally:
            await client.close()
    phases = {'round_trip_ms': phase_stats(round_trips)}
    entry = _fake_section(phases, counters, notes)
    entry.update({
        'same_source': same_source,
        'probes_raw': probes_raw,
        'presentation_probe': ('NOT_RUN in P0.0: requires a real UI '
                               'frame-submitted marker; no synthetic '
                               'stand-in is claimed'),
        'source_to_present': 'baseline-pending',
        'fault_fixture_modes_armed': armed,
        'blocked_drain': blocked_drain,
    })
    if not same_source:
        entry['status'] = 'error'
        entry['error'] = 'clock probes from the child violated monotonic ordering'
    return entry


async def _blocked_drain_probe(client):
    """Prove the blocked-drain fixture genuinely gates the client's drain.

    The child stops reading stdin for 2 s after the fault is armed; the bench
    then writes a ~256 KiB frame. With the OS pipe full and no reader, the
    client's ``await drain()`` blocks — a 0.5 s bound times out (that IS the
    block, not a guess) — and once the child resumes, framing is intact and a
    follow-up request completes (recovery). The big frame is a
    bench/drain-probe request the child deliberately never answers, so no
    unmatched response ever reaches the read loop.
    """
    await _request(client, 'bench/set-fault',
                   {'mode': 'block-drain', 'delay_s': 2.0})
    frame = {'id': 'bench-drain-probe', 'method': 'bench/drain-probe',
             'params': {'padding': 'x' * 262144}}
    gate_started = time.perf_counter()
    blocked = False
    try:
        await asyncio.wait_for(client.send(frame), 0.5)
    except asyncio.TimeoutError:
        blocked = True
    gate_ms = (time.perf_counter() - gate_started) * 1000.0
    recovery_started = time.perf_counter()
    recovered = True
    try:
        await asyncio.wait_for(_request(client, 'bench/counters', {}), 8.0)
    except Exception:
        recovered = False
    recovery_ms = (time.perf_counter() - recovery_started) * 1000.0
    result = {'armed': True, 'blocked': blocked, 'recovered': recovered,
              'gate_ms': gate_ms, 'recovery_ms': recovery_ms}
    if not blocked:
        result['status'] = 'not-reproduced'
    return result


def _synthetic_session_db(db_path, session_id, messages):
    """Synthetic ZCode-shaped sqlite store (no secrets, no real session)."""
    import sqlite3
    db_path.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(db_path)
    try:
        connection.executescript(
            'CREATE TABLE session (id TEXT PRIMARY KEY, directory TEXT,'
            ' title TEXT, time_created INTEGER, time_updated INTEGER);'
            'CREATE TABLE message (id TEXT PRIMARY KEY, session_id TEXT,'
            ' time_created INTEGER, data TEXT, sequence INTEGER);'
            'CREATE TABLE part (id TEXT PRIMARY KEY, message_id TEXT,'
            ' session_id TEXT, time_created INTEGER, time_updated INTEGER,'
            ' data TEXT, sequence INTEGER);'
            'CREATE TABLE tool_usage (session_id TEXT, tool_name TEXT,'
            ' read_only INTEGER, destructive INTEGER, approval_status TEXT,'
            ' status TEXT, exit_code INTEGER, duration_ms INTEGER);')
        connection.execute('INSERT INTO session VALUES (?,?,?,?,?)',
                           (session_id, '/fake/project', 'bench fixture',
                            1000, 2000))
        for index in range(messages):
            connection.execute('INSERT INTO message VALUES (?,?,?,?,?)',
                               (f'm{index}', session_id, 1000 + index,
                                json.dumps({'role': 'user' if index % 2 == 0
                                            else 'assistant'}), index))
            connection.execute('INSERT INTO part VALUES (?,?,?,?,?,?,?)',
                               (f'p{index}', f'm{index}', session_id,
                                1000 + index, 1000 + index,
                                json.dumps({'type': 'text',
                                            'text': f'fake body {index}'}), 0))
        connection.commit()
    finally:
        connection.close()


async def bench_fake_read_list(scratch_dir, *, tasks=2, runs_per_task=60,
                               messages=40, trials=5):
    """Production read/list latency over synthetic records (zero models).

    Times the REAL hub paths — ``mcp_readonly.list_runs_impl`` scanning
    run-<N>.json records under an evidence root and
    ``session_reader.read_zcode_context`` paging a synthetic sqlite session
    store — instead of any bench-invented endpoint. Inputs are fully
    synthetic: no real runs, sessions, or credentials are read.
    """
    from .mcp_readonly import list_runs_impl
    from .session_reader import read_zcode_context
    notes = ['production read/list latency on synthetic data: '
             'mcp_readonly.list_runs_impl over run-<N>.json records and '
             'session_reader.read_zcode_context paging a synthetic sqlite '
             'session store (the real hub read paths, fake data only); '
             'zero model calls']
    with _bench_workspace(scratch_dir, FAKE_READ_LIST) as root:
        evidence_root = root / 'evidence'
        for task_index in range(tasks):
            task_dir = evidence_root / f'task-fake-{task_index:02d}'
            task_dir.mkdir(parents=True)
            for run_index in range(runs_per_task):
                record = {'outcome': 'delivered', 'delivery_ok': True,
                          'session_id': (f'sess_fake_{task_index}_{run_index}'),
                          'turn_id': f'turn_fake_{run_index}'}
                (task_dir / f'run-{run_index:03d}.json').write_text(
                    json.dumps(record))
        db_path = root / 'fake-sessions.db'
        _synthetic_session_db(db_path, 'sess_fake_bench', messages)
        list_ms, read_ms = [], []
        listed = messages_read = None
        for _ in range(trials):
            started = time.perf_counter()
            listing = list_runs_impl(evidence_root)
            list_ms.append((time.perf_counter() - started) * 1000.0)
            listed = listing['total_runs']
            started = time.perf_counter()
            context = read_zcode_context('sess_fake_bench', db_path,
                                         max_items=50)
            read_ms.append((time.perf_counter() - started) * 1000.0)
            messages_read = len(context.get('messages', []))
    phases = {'list_runs_ms': phase_stats(list_ms),
              'read_context_ms': phase_stats(read_ms)}
    entry = _fake_section(phases, {'model_requests': 0}, notes)
    entry.update({'records_created': tasks * runs_per_task,
                  'records_listed': listed, 'messages_read': messages_read,
                  'production_paths': [
                      'zcode_supervisor.mcp_readonly.list_runs_impl',
                      'zcode_supervisor.session_reader.read_zcode_context']})
    return entry


async def bench_fake_ui_journal(scratch_dir, *, clients=50, burst=300,
                                cadence_every=50, cadence_s=0.01):
    """P1.1 fake section: bounded journal + UDS read-only server.

    Local sockets and the in-process emitter only — model_requests stays 0.
    Measures the §4.4 review asks under the fixed bounds: hub event-loop
    lag during the burst cadence, the per-client queued events/bytes peaks,
    and the per-publish enqueue (queue wait) cost with 50 subscribers.
    """
    from tempfile import TemporaryDirectory as _TempDir

    from .ui_journal import UiJournal
    from .ui_projection import UiEmitter
    from .ui_server import UiEventServer

    # a SHORT system-temp dir: the deep scratch tree can exceed the UDS
    # sun_path limit, and the section owns its throwaway state dir anyway
    hex64 = 'b' * 64
    rfc = '2070-01-01T00:00:00.000000Z'
    emitter = UiEmitter(hub_instance_id='czsbench', journal_epoch=1,
                        enabled=True)
    journal = UiJournal(emitter)
    scratch = _TempDir(prefix='czsui')
    state = Path(scratch.name)
    os.chmod(state, 0o700)
    server = UiEventServer(journal, socket_path=state / 'ui-events.sock')
    await server.start()

    def emit_task(task_id):
        accepted = emitter.emit(
            'task.accepted', task_id, task_id=task_id, worker='codex',
            mode='app-server',
            data={'contract_id': f'contract-{task_id}',
                  'contract_digest': hex64, 'created_at': rfc,
                  'initiator_verified': False})
        emitter.emit('stream.announced', 'hub', task_id=task_id,
                     worker='codex', mode='app-server',
                     data={'task_stream_id': task_id,
                           'accepted_event_id': accepted['event_id']})

    connections = []
    try:
        for task_id in ('benchtask01', 'benchtask02'):
            emit_task(task_id)
        hello_cursor = None
        for index in range(clients):
            reader, writer = await asyncio.open_unix_connection(
                server.socket_path)
            connections.append((reader, writer))
            writer.write((json.dumps(
                {'op': 'hello', 'v': 1, 'client': f'bench-{index}'})
                + '\n').encode())
            await writer.drain()
            hello = json.loads(await asyncio.wait_for(reader.readline(), 10))
            hello_cursor = hello['initial_snapshot_cursor']
            writer.write((json.dumps(
                {'op': 'subscribe', 'stream_id': '*',
                 'snapshot_cursor': hello_cursor}) + '\n').encode())
            await writer.drain()
            ack = json.loads(await asyncio.wait_for(reader.readline(), 10))
            if not ack.get('ok'):
                raise AssertionError(f'subscribe failed: {ack}')

        received = [0] * clients
        resets = [0] * clients

        async def reader_loop(index):
            reader, _writer = connections[index]
            while True:
                line = await reader.readline()
                if not line:
                    return
                frame = json.loads(line)
                if frame.get('type') == 'reset_required':
                    resets[index] += 1
                elif frame.get('type') == 'event' \
                        and frame['event'].get('kind') == 'task.completed':
                    received[index] += 1
                    return  # terminal marker seen: this client is done

        readers = [asyncio.create_task(reader_loop(index))
                   for index in range(clients)]
        publish_waits = []
        loop_lag = []
        for index in range(burst):
            task_id = 'benchtask01' if index % 2 else 'benchtask02'
            started = time.monotonic()
            emitter.emit('turn.started', task_id, task_id=task_id,
                         worker='codex', mode='app-server',
                         session_id='sess-bench', turn_id=f'bench-{index}')
            publish_waits.append(
                (time.monotonic() - started) * 1000.0)
            if index % cadence_every == cadence_every - 1:
                step = time.monotonic()
                await asyncio.sleep(cadence_s)
                loop_lag.append((time.monotonic() - step) * 1000.0)
        pending_peak_events = max(
            (sub.pending_events for sub in journal._subscriptions), default=0)
        pending_peak_bytes = max(
            (sub.pending_bytes for sub in journal._subscriptions), default=0)
        for task_id in ('benchtask01', 'benchtask02'):
            emitter.emit('task.completed', task_id, task_id=task_id,
                         worker='codex', mode='app-server',
                         status='completed', outcome='ok', delivery_ok=True,
                         duration_ms=1.0)
        await asyncio.wait_for(
            asyncio.gather(*readers), 30)
        queue_events = journal.queue_max_events
        queue_bytes = journal.queue_max_bytes
        ok = (sum(resets) == 0 and all(count == 1 for count in received)
              and pending_peak_events <= queue_events + 1
              and pending_peak_bytes <= queue_bytes + 4096)
        return {
            'status': 'ok' if ok else 'error',
            'model_requests': 0,
            'error': None if ok else
                f'resets={sum(resets)} completed={received} '
                f'pending_peak={pending_peak_events}/{pending_peak_bytes}',
            'metrics': {
                'clients': clients,
                'burst_events': burst,
                'hub_event_loop_lag_ms': stats(loop_lag),
                'publish_wait_ms': stats(publish_waits),
                'queued_events_peak': pending_peak_events,
                'queued_bytes_peak': pending_peak_bytes,
                'queue_bounds_events': queue_events,
                'queue_bounds_bytes': queue_bytes,
                'journal_total_events': journal.stats()['total_events'],
                'journal_total_bytes': journal.stats()['total_bytes'],
            },
            'phases': {},
        }
    finally:
        for _reader, writer in connections:
            writer.close()
        await server.close()
        scratch.cleanup()  # explicit: no GC-time ResourceWarning evidence


def _fake_section(phases, counters, notes):
    """Assemble a fake/replay section entry from per-phase sample lists."""
    counters = counters or {}
    model_requests = counters.get('model_requests')
    entry = {'status': 'ok', 'phases': phases}
    entry['model_requests'] = model_requests if isinstance(model_requests, int) else None
    entry['server_counters'] = counters
    if model_requests != 0:
        entry['status'] = 'error'
        entry['error'] = f'fake child reported model_requests={model_requests}'
    entry['notes'] = '; '.join(note for note in notes if note)
    return entry


async def run_fake_baseline(server_path, out_dir, *, steady_rounds=3,
                            init_trials=3, thread_rounds=3, clock_trials=3):
    """Run every P0.0 fake/replay section; write the baseline JSON.

    The document carries raw per-phase samples (unrounded, with
    n/median/p95 summaries), the environment, the exact child command,
    exit_code, and the total model_requests (asserted 0). Reruns into the
    SAME output directory are supported: each run gets its own unique
    scratch child and its baseline lands in a NEW file — an existing good
    fake-baseline.json is never overwritten (fake-baseline-2.json, -3, ...).
    """
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    scratch = out_dir / f'scratch-{uuid.uuid4().hex[:8]}'
    sections = {}
    runners = [
        (FAKE_COLD, lambda: bench_fake_cold(server_path, init_trials,
                                            scratch_dir=scratch)),
        (FAKE_APPSERVER, lambda: bench_fake_appserver(
            server_path, init_trials, rounds=thread_rounds,
            scratch_dir=scratch)),
        (FAKE_RESIDENT, lambda: bench_fake_resident(
            server_path, steady_rounds, scratch_dir=scratch)),
        (FAKE_GENERATION_EXIT, lambda: bench_fake_generation_exit(
            server_path, scratch_dir=scratch)),
        (FAKE_CLOCK, lambda: bench_fake_clock_sidecar(
            server_path, clock_trials, scratch_dir=scratch)),
        (FAKE_READ_LIST, lambda: bench_fake_read_list(
            scratch, tasks=2, runs_per_task=60, messages=40, trials=3)),
        (FAKE_UI_JOURNAL, lambda: bench_fake_ui_journal(scratch)),
    ]
    for name, runner in runners:
        try:
            sections[name] = await runner()
        except Exception as error:  # one section never sinks the others
            sections[name] = {'status': 'error',
                              'error': f'{type(error).__name__}: {error}'[:300]}
    model_requests = 0
    for entry in sections.values():
        if isinstance(entry, dict) and isinstance(entry.get('model_requests'), int):
            model_requests += entry['model_requests']
    ok = model_requests == 0 and all(entry.get('status') == 'ok'
                                     for entry in sections.values())
    baseline_path = out_dir / 'fake-baseline.json'
    suffix = 2
    while baseline_path.exists():  # never replace a good baseline
        baseline_path = out_dir / f'fake-baseline-{suffix}.json'
        suffix += 1
    document = {
        'generated': datetime.now(timezone.utc).isoformat(timespec='seconds'),
        'trials': {'cold': init_trials, 'appserver_init': init_trials,
                   'appserver_threads': thread_rounds,
                   'resident_steady': steady_rounds, 'clock_probes': clock_trials},
        'command': [sys.executable, str(server_path)],
        'environment': {'python': sys.version,
                        'platform': platform.platform(),
                        'machine': platform.machine(),
                        'system': platform.system(),
                        'release': platform.release()},
        'exit_code': 0 if ok else 1,
        'model_requests': model_requests,
        'sections': sections,
        'output': {'json': str(baseline_path), 'scratch': str(scratch)},
        'notes': ['fake/replay baseline only: fixture child, zero model '
                  'calls, no real CLI, no remote tasks',
                  'raw baseline before any threshold freeze; no improvement '
                  'percentages are claimed',
                  'metrics without reliable samples are marked '
                  'baseline-pending inside their section entries',
                  'reruns never overwrite: this run wrote '
                  + baseline_path.name],
    }
    atomicwrite_json(baseline_path, document)
    return document


def _table_cell(value):
    return '-' if value is None else str(value)


def _markdown(result):
    lines = ['# czs bench', '',
             f"generated: {result['generated']}",
             f"trials: {result['trials']}"
             + (' (with-model sections capped at 3)' if result['with_model'] else ''),
             f"codex: {_table_cell(result.get('codex'))}",
             'all timings in milliseconds; control-plane only unless the section '
             'name says with-model', '',
             '| section | n | min | p50 | p90 | p99 | max | notes |',
             '|---|---|---|---|---|---|---|---|']
    for name, entry in result['sections'].items():
        if 'n' in entry:
            cells = [name, str(entry['n'])] \
                + [_table_cell(entry.get(key)) for key in STATS_KEYS] \
                + [entry.get('notes', '')]
        else:
            cells = [name, '-', '-', '-', '-', '-', '-',
                     f"{entry.get('status', 'error')}: {entry.get('error', '')}"]
        row = [str(cell).replace('|', '/').replace('\n', ' ') for cell in cells]
        lines.append('| ' + ' | '.join(row) + ' |')
    return '\n'.join(lines) + '\n'


async def run_bench(config, out_dir, *, trials=10, sections=None,
                    with_model=False, codex_path=None):
    """Run the selected sections; write out_dir/bench.json and bench.md.

    Any section failure becomes ``{"status": "error", ...}`` for that section
    only. Codex sections are skipped (not errors) when no binary resolves.
    With-model section names are only known while ``with_model`` is set, so
    naming one without the flag fails closed as an unknown section.
    """
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    resolved_codex = _resolve_codex(codex_path)
    runners = {
        ZCODE_COLD: lambda: bench_zcode_cold(config, trials,
                                             scratch_dir=out_dir / 'scratch'),
        ZCODE_RESIDENT: lambda: bench_zcode_resident(config, trials,
                                                     scratch_dir=out_dir / 'scratch'),
        CODEX_SPAWN: lambda: bench_codex_spawn(resolved_codex, trials),
        CODEX_APPSERVER_INIT: lambda: bench_codex_appserver(resolved_codex, trials,
                                                            out_dir),
        CODEX_APPSERVER_THREAD: lambda: bench_codex_appserver_thread(
            resolved_codex, trials, out_dir),
    }
    if with_model:
        model_trials = min(trials, MODEL_TRIALS_CAP)
        runners[ZCODE_WITH_MODEL] = lambda: bench_zcode_with_model(
            config, model_trials, scratch_dir=out_dir / 'scratch')
        runners[CODEX_EXEC_WITH_MODEL] = lambda: bench_codex_exec_with_model(
            resolved_codex, model_trials, out_dir)
    if sections is None:
        selected = [name for name in SECTION_ORDER if name in runners]
        unknown = []
    else:
        wanted = [str(name).strip() for name in sections if str(name).strip()]
        selected = [name for name in SECTION_ORDER
                    if name in runners and name in wanted]
        unknown = [name for name in wanted if name not in runners]
    result = {'generated': datetime.now(timezone.utc).isoformat(timespec='seconds'),
              'trials': trials, 'with_model': with_model,
              'codex': str(resolved_codex) if resolved_codex else None,
              'sections': {}, 'output': {'json': str(out_dir / 'bench.json'),
                                         'markdown': str(out_dir / 'bench.md')}}
    for name in selected:
        if resolved_codex is None and name in CODEX_SECTIONS:
            result['sections'][name] = {
                'status': 'skipped',
                'error': 'codex binary not found; pass --codex <path>'}
            continue
        try:
            result['sections'][name] = await runners[name]()
        except Exception as error:  # one section never sinks the others
            result['sections'][name] = {
                'status': 'error',
                'error': f'{type(error).__name__}: {error}'[:300]}
    for name in unknown:
        result['sections'][name] = {'status': 'error',
                                    'error': f'unknown section: {name}'}
    atomicwrite_json(out_dir / 'bench.json', result)
    atomic_write(out_dir / 'bench.md', _markdown(result).encode())
    return result


def main(argv=None):
    parser = argparse.ArgumentParser(
        description='Control-plane latency bench; zero model calls unless --with-model')
    parser.add_argument('--config', type=Path, default=Path('.czs/config.json'),
                        help='Local JSON configuration (default: .czs/config.json)')
    parser.add_argument('--out', type=Path, required=True,
                        help='Output directory for bench.json and bench.md')
    parser.add_argument('--trials', type=int, default=10,
                        help='Trials per section (with-model sections cap at 3)')
    parser.add_argument('--sections',
                        help='Comma-separated subset, e.g. zcode-cold,codex-spawn')
    parser.add_argument('--with-model', action='store_true',
                        help='ALSO send one tiny model turn per mode (real quota)')
    parser.add_argument('--codex', default=None,
                        help=f'codex binary path (default: {DEFAULT_CODEX}, then PATH)')
    args = parser.parse_args(argv)
    sections = None
    if args.sections:
        sections = [name.strip() for name in args.sections.split(',') if name.strip()]
    try:
        config = load_config(args.config)
        result = asyncio.run(run_bench(config, args.out, trials=args.trials,
                                       sections=sections,
                                       with_model=args.with_model,
                                       codex_path=args.codex))
    except (ValueError, OSError, KeyError, TypeError, asyncio.TimeoutError) as error:
        print(json.dumps({'ok': False, 'error': str(error) or type(error).__name__},
                         ensure_ascii=False))
        return 1
    summary = {}
    for name, entry in result['sections'].items():
        if 'n' in entry:
            summary[name] = {'status': entry['status'], 'n': entry['n'],
                             'p50': entry['p50'], 'p99': entry['p99']}
            if entry.get('errors'):
                summary[name]['errors'] = entry['errors']
        else:
            summary[name] = {'status': entry.get('status', 'error'),
                             'error': entry.get('error', '')}
    print(json.dumps({'ok': True, 'trials': result['trials'],
                      'bench_json': result['output']['json'],
                      'bench_md': result['output']['markdown'],
                      'sections': summary}, ensure_ascii=False, indent=2))
    entries = list(result['sections'].values())
    return 1 if entries and all(entry.get('status') == 'error'
                                for entry in entries) else 0


if __name__ == '__main__':  # pragma: no cover
    sys.exit(main())
