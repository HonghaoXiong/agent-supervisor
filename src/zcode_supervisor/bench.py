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
from contextlib import contextmanager
from datetime import datetime, timezone
import inspect
import json
import math
from pathlib import Path
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
