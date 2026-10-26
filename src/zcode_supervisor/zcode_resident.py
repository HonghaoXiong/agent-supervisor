#!/usr/bin/env python3
"""P1-2 resident ZCode app-server: one long-lived process, many isolated sessions.

The cold path (``protocol.execute``) starts one node app-server per task
(~0.5 s fixed cost, node cold start dominant). This module keeps ONE resident
process for the whole hub: every task gets its own session on that process
(``session/create`` with the task workspace), and a hub-side
:class:`SessionRegistry` — not ``process_alive`` — decides whether a session
may be resumed. Session isolation is enforced twice: permission requests are
routed by sessionId to the owning task's :class:`~zcode_supervisor.authority.Authority`
(which re-checks session/turn identity itself), and the event wait loop only
ever consumes events whose sessionId matches the run's own session.

Two points are deliberately left to real-hardware experiments by the operator:
same-process concurrent-session isolation, and whether the process cwd
(= work_root here) is respected when a session declares another workspacePath.

This is still a permission broker, not an OS sandbox.
"""
import argparse
import asyncio
import json
import math
import re
import stat
import time
import uuid
from pathlib import Path

from . import protocol
from . import runtime
from .authority import (Authority, check_model_snapshot, execution_evidence,
                        inventory, model_execution_evidence, observed_model,
                        own_event)
from .common import read_regular, write_json


class ResidentError(Exception):
    """Resident lifecycle violation (session ownership, double start, ...)."""


class SessionRegistry:
    """Hub-side session ownership registry, replacing process_alive probes.

    ``state_dir/hub/resident-sessions.json`` (atomic writes). A session is
    resumable only when the registry — the ownership truth the hub itself
    wrote — says ``exited`` for the very same workspace. Whether the previous
    OS process is alive is irrelevant on the resident path: the process is
    shared and long-lived, ownership comes from this registry alone.
    """

    def __init__(self, state_dir):
        self.path = Path(state_dir) / 'hub' / 'resident-sessions.json'

    def _load(self):
        if not self.path.exists():
            return {}
        try:
            records = json.loads(read_regular(self.path))
        except (ValueError, OSError) as error:
            raise ResidentError(f'Resident session registry is unreadable: {error}') from error
        if not isinstance(records, dict):
            raise ResidentError('Resident session registry is malformed')
        return records

    def _save(self, records):
        write_json(self.path, records)

    def claim(self, session_id, task_id, workspace, pid):
        """Atomically take a session to running; a running session never re-claims."""
        records = self._load()
        record = records.get(session_id)
        if record is not None and record.get('state') == 'running':
            raise ResidentError(f'Session {session_id} is already running on the resident')
        now = time.time()
        records[session_id] = {
            'task_id': task_id, 'workspace': str(workspace), 'state': 'running',
            'created': (record or {}).get('created', now), 'updated': now,
            'last_seq': (record or {}).get('last_seq'), 'pid': pid, 'reason': None}
        self._save(records)

    def release(self, session_id, last_seq):
        """Settle a cleanly finished turn: exited (resumable) with its last event seq."""
        records = self._load()
        record = records.get(session_id)
        if record is None:
            raise ResidentError(f'Unknown session {session_id}')
        record.update(state='exited', last_seq=last_seq, updated=time.time(), reason=None)
        self._save(records)

    def mark_lost(self, session_id, reason):
        """Record a session that ended without a clean completion (not resumable)."""
        records = self._load()
        record = records.get(session_id)
        if record is None:
            record = records[session_id] = {'task_id': None, 'workspace': None,
                                            'created': time.time(), 'last_seq': None, 'pid': None}
        record.update(state='lost', updated=time.time(), reason=str(reason))
        self._save(records)

    def can_resume(self, session_id, workspace):
        """state exited AND the very same workspace (registry-owned ownership)."""
        record = self._load().get(session_id)
        return bool(record and record.get('state') == 'exited'
                    and record.get('workspace') == str(workspace))


class ResidentZCode:
    """One resident app-server process executing many session-isolated packets."""

    def __init__(self, config, evidence_root: Path, *, client_factory=None):
        self.config = config
        self.evidence_root = Path(evidence_root)
        # client_factory(workspace, output, permission, config); tests inject a fake.
        self._client_factory = client_factory or protocol.ProtocolClient
        self.client = None
        self.registry = SessionRegistry(Path(config.state_dir))
        # sessionId -> the one Authority allowed to answer that session's requests.
        self._authorities = {}
        self._workspace_locks = {}
        self._setup_latencies_ms = []
        self.alerts = []

    async def start(self):
        if self.client is not None:
            raise ResidentError('Resident app-server is already started')
        self.evidence_root.mkdir(parents=True, exist_ok=True)
        output = self.evidence_root / f'resident-app-server-{uuid.uuid4().hex}.json'
        # Process cwd is work_root — NOT a task workspace; each session declares
        # its own workspacePath at session/create time.
        client = self._client_factory(Path(self.config.work_root), output,
                                      self._route_permission, self.config)
        try:
            await client.start()
        except BaseException:
            await client.close()
            raise
        self.client = client

    async def stop(self):
        client, self.client = self.client, None
        if client is not None:
            await client.close()

    def _route_permission(self, params):
        """Route one interaction/requestPermission to the owning task's authority."""
        if not isinstance(params, dict):
            return {'decision': 'deny', 'reason': 'unknown session on resident'}
        session_id = params.get('sessionId')
        authority = self._authorities.get(session_id) if isinstance(session_id, str) else None
        if authority is None:
            self.alerts.append({'session_id': session_id,
                                'request_id': params.get('requestId'),
                                'tool': params.get('toolName')})
            return {'decision': 'deny', 'reason': 'unknown session on resident'}
        # Double insurance: Authority.decide re-checks sessionId/turnId itself,
        # so task A's authority can never accept a request naming task B.
        return authority.decide(params)

    def _lock_for(self, workspace_key):
        lock = self._workspace_locks.get(workspace_key)
        if lock is None:
            lock = self._workspace_locks[workspace_key] = asyncio.Lock()
        return lock

    def resident_latency_hook(self):
        """Per-run session/setup latency (create-or-resume -> subscribe done), ms."""
        return {'setup_ms': list(self._setup_latencies_ms)}

    async def run_packet(self, packet_path: Path, out: Path, *, timeout_s: int,
                         resume: str | None = None) -> dict:
        # ---- preflight: nothing written yet; caller errors raise (as in execute) ----
        if resume and not re.fullmatch(r'sess_[A-Za-z0-9_-]+', resume):
            raise ValueError('Invalid resume session ID')
        if (isinstance(timeout_s, bool) or not isinstance(timeout_s, (int, float))
                or not math.isfinite(timeout_s) or timeout_s <= 0):
            raise ValueError('Run timeout must be positive and finite')
        raw_packet = Path(packet_path)
        packet_info = raw_packet.lstat()
        if not stat.S_ISREG(packet_info.st_mode) or packet_info.st_nlink != 1:
            raise ValueError('Packet must be an ordinary, unshared file')
        packet_path = raw_packet.resolve()
        packet = json.loads(read_regular(packet_path))
        protocol.validate_packet(packet)
        if packet.get('orchestration'):
            raise ValueError('Orchestration packets are not supported on the resident path')
        workspace, output = protocol.checked_locations(Path(packet['workspace']), out,
                                                       self.config, packet_path)
        for name in packet['allowed_files']:
            if (workspace / name).resolve().relative_to(workspace).as_posix() != name:
                raise ValueError('Allowed paths must be normalized workspace-relative files')
            if (workspace / name).is_dir():
                raise ValueError('Allowed paths must identify files, not directories')
        if self.client is None:
            raise ResidentError('Resident app-server is not started')
        if resume and not self.registry.can_resume(resume, str(workspace)):
            raise ResidentError('Session is unknown, busy, lost, or bound to another workspace')
        # Same workspace serializes (shared frozen inputs); different workspaces
        # may run concurrently on their own sessions.
        async with self._lock_for(str(workspace)):
            return await self._run_locked(packet, packet_path, workspace, output,
                                          timeout_s, resume)

    async def _run_locked(self, packet, packet_path, workspace, output, timeout_s, resume):
        client = self.client
        registry = self.registry
        output.parent.mkdir(parents=True, exist_ok=True)
        before = inventory(workspace)
        authority = Authority(packet, packet_path, before)
        authority.check_frozen()
        task_id = Path(output).stem
        with output.open('x') as stream:
            json.dump({'delivery_ok': False, 'state': 'starting'}, stream)
        write_json(output.with_suffix('.before.json'), before)
        input_id = 'input_' + uuid.uuid4().hex
        result = {'delivery_ok': False, 'workspace': str(workspace), 'input_id': input_id,
                  'generation': uuid.uuid4().hex, 'scientific_acceptance': 'pending_codex_review',
                  'resident_pid': client.process.pid if client.process else None}
        requested_model = packet.get('model')
        if requested_model is not None:
            result['requested_model'] = requested_model
        deadline = time.monotonic() + timeout_s
        dispatched = False
        turn_succeeded = False
        last_seq = 0

        def remaining():
            return max(0.001, deadline - time.monotonic())

        async def rpc(method, params):
            return await asyncio.wait_for(client.request(method, params), remaining())

        try:
            setup_started = time.monotonic()
            options = {'workspace': {'workspacePath': str(workspace), 'workspaceKey': str(workspace)},
                       'mcpServers': [], 'toolAllowlist': ['Read', 'Write', 'Edit', 'Bash'],
                       'dynamicWorkflowEnabled': False}
            pid = client.process.pid if client.process else None
            if resume:
                # Registry ownership replaces the old process_alive guard: the
                # claim itself is the atomic running transition.
                registry.claim(resume, task_id, str(workspace), pid)
                try:
                    snapshot = await rpc('session/resume', {'sessionId': resume, **options})
                    await rpc('session/setMode', {'sessionId': resume, 'mode': 'build'})
                except BaseException:
                    registry.mark_lost(resume, 'session/resume failed')
                    raise
                sid = snapshot['session']['sessionId']
                if sid != resume:
                    raise ValueError('Resume returned a different session')
            else:
                snapshot = await rpc('session/create', {
                    **options, 'mode': 'build', 'titleGenerationEnabled': False,
                    **({'model': requested_model} if requested_model is not None else {})})
                sid = snapshot['session']['sessionId']
                registry.claim(sid, task_id, str(workspace), pid)
            authority.session_id = sid
            self._authorities[sid] = authority
            result['session_id'] = sid
            if requested_model is not None:
                result['observed_model_initial'] = observed_model(snapshot)
                await rpc('session/setModel', {'sessionId': sid, 'model': requested_model,
                                               'persistAsWorkspaceLastUsed': False})
                snapshot = await rpc('session/read', {'sessionId': sid,
                                                      'deliveryKind': 'desktop-continuous',
                                                      'messageLimit': 1})
                result['observed_model_before'] = observed_model(snapshot)
                result['effective_model_before'] = check_model_snapshot(snapshot, requested_model)
            subscription = await rpc('session/subscribe', {
                'sessionId': sid, 'deliveryKind': 'desktop-continuous', 'includeSnapshot': False})
            result['start_event_seq'] = subscription['eventSeq']
            last_seq = subscription['eventSeq']
            self._setup_latencies_ms.append(round((time.monotonic() - setup_started) * 1000, 3))
            await rpc('session/send', {'sessionId': sid, 'inputId': input_id,
                                       'content': packet['prompt']})
            dispatched = True
            completed = None
            processed = 0
            while time.monotonic() < deadline:
                if client.failure:
                    raise ValueError(client.failure)
                # The events list is shared process-wide; each run keeps its own
                # cursor and only ever consumes its OWN session's events.
                while processed < len(client.events):
                    event = client.events[processed]
                    processed += 1
                    if (event.get('sessionId') != sid
                            or event.get('seq', -1) <= result['start_event_seq']):
                        continue
                    payload = event.get('payload', {})
                    seq = event.get('seq')
                    if isinstance(seq, int) and seq > last_seq:
                        last_seq = seq
                    if (event.get('type') == 'turn.started'
                            and payload.get('inputId') == input_id):
                        authority.turn_id = event['turnId']
                    if own_event(event, sid, authority.turn_id):
                        if event.get('type') == 'turn.failed':
                            raise ValueError('Worker turn failed: ' + str(payload.get('error')))
                        if event.get('type') == 'turn.completed':
                            completed = event
                if completed:
                    break
                await asyncio.sleep(0.02)
            if completed is None:
                raise asyncio.TimeoutError('Worker turn deadline exceeded')
            result['turn_id'] = authority.turn_id
            result['completion'] = completed['payload']
            if completed['payload'].get('resultType') != 'success':
                raise ValueError('Turn completion was not success')
            turn_succeeded = True
            snapshot = await rpc('session/read', {'sessionId': sid,
                                                  'deliveryKind': 'desktop-continuous',
                                                  'messageLimit': 10})
            if (snapshot['session'].get('status') != 'idle'
                    or snapshot['runtime'].get('pendingRequestIds')):
                raise ValueError('Worker is not idle or still has pending interactions')
            if requested_model is not None:
                result['observed_model_after'] = observed_model(snapshot)
                result['effective_model_after'] = check_model_snapshot(snapshot, requested_model)
                result['model_execution'] = model_execution_evidence(
                    client.events, sid, authority.turn_id, requested_model)
            authority.check_frozen()
            commands, problems = execution_evidence(client.events, authority)
            result['commands'] = commands
            seen = {entry['command_id'] for entry in commands}
            required = {entry['id'] for entry in packet.get('commands', [])
                        if entry.get('required', True)}
            if required - seen:
                problems.append('Required commands were not executed: '
                                + str(sorted(required - seen)))
            result['execution_problems'] = problems
            result['delivery_ok'] = not problems
            result['outcome'] = 'delivered' if not problems else 'evidence-rejected'
        except (ResidentError, ValueError, OSError, EOFError, asyncio.TimeoutError,
                KeyError, TypeError) as error:
            result['error'] = (f'{type(error).__name__}: {error}' if isinstance(error, ResidentError)
                               else (str(error) or type(error).__name__))
            if isinstance(error, asyncio.TimeoutError):
                result['outcome'] = 'timeout'
            elif not dispatched:
                result['outcome'] = 'preflight-rejected'
            elif client.failure:
                result['outcome'] = 'protocol-error'
            elif turn_succeeded:
                result['outcome'] = 'postflight-rejected'
            else:
                result['outcome'] = 'worker-failed'
            # Stop only THIS session; the resident process keeps serving others.
            if authority.session_id and client.process and client.process.returncode is None:
                try:
                    await asyncio.wait_for(
                        client.request('session/stop', {'sessionId': authority.session_id}), 2)
                except Exception:
                    pass
        finally:
            self._authorities.pop(authority.session_id, None)
            result['turn_id'] = result.get('turn_id') or authority.turn_id
            result['permission_decisions'] = authority.decisions
            result['process_exit_code'] = client.process.returncode if client.process else None
            result['scope_enforcement'] = 'permission broker and post-run hashes; not an OS sandbox'
            if authority.session_id:
                # Same registry boundary as execute's per-session state file:
                # only a session whose turn completed cleanly becomes resumable.
                if result.get('completion'):
                    registry.release(authority.session_id, last_seq)
                else:
                    registry.mark_lost(authority.session_id,
                                       result.get('error') or result.get('outcome') or 'lost')
            try:
                authority.check_frozen()
                after = inventory(workspace)
                result['changed_files'] = sorted(
                    name for name in before.keys() | after.keys()
                    if before.get(name) != after.get(name))
            except (ValueError, OSError) as error:
                result.update(delivery_ok=False, scope_error=str(error))
            exit_code = result['process_exit_code']
            if exit_code is not None and exit_code != 0:
                # On the resident path a live process is returncode None (fine);
                # only an actually observed nonzero exit is a protocol failure.
                result['delivery_ok'] = False
                if result.get('outcome') == 'delivered':
                    result['outcome'] = 'protocol-error'
            if result.get('scope_error') and result.get('outcome') != 'postflight-rejected':
                result['outcome'] = 'scope-error'
            write_json(output, protocol.redact_transport(result))
        return result


def build_parser():
    parser = argparse.ArgumentParser(
        prog='python -m zcode_supervisor.zcode_resident',
        description='Run reviewed packets on one resident ZCode app-server process')
    sub = parser.add_subparsers(dest='action', required=True)
    run = sub.add_parser('run', help='Execute packets sequentially or concurrently on the resident')
    run.add_argument('--config', required=True, help='czs configuration JSON')
    run.add_argument('--out-root', type=Path, required=True,
                     help='evidence directory for run.json files (must be under work_root)')
    run.add_argument('--packet', type=Path, action='append', required=True,
                     help='task packet; repeat for multiple packets')
    run.add_argument('--concurrent', action='store_true',
                     help='run all packets concurrently instead of sequentially')
    run.add_argument('--timeout', type=int, default=300)
    return parser


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    if not 1 <= args.timeout <= 86400:
        print(json.dumps({'ok': False, 'error': '--timeout must be 1-86400 seconds'}))
        return 1
    holder = {}

    async def drive():
        config = runtime.load_config(args.config)
        resident = ResidentZCode(config, args.out_root)
        holder['resident'] = resident
        await resident.start()
        try:
            jobs = [resident.run_packet(packet,
                                        args.out_root / f'{packet.stem}-{index:02d}.json',
                                        timeout_s=args.timeout)
                    for index, packet in enumerate(getattr(args, "packets", None) or args.packet)]
            if args.concurrent:
                return list(await asyncio.gather(*jobs))
            return [await job for job in jobs]
        finally:
            await resident.stop()

    try:
        results = asyncio.run(drive())
    except (ValueError, OSError, ResidentError) as error:
        print(json.dumps({'ok': False, 'error': str(error) or type(error).__name__},
                         ensure_ascii=False))
        return 1
    for packet, result in zip(getattr(args, "packets", None) or args.packet, results):
        print(json.dumps({'packet': str(packet), 'delivery_ok': result.get('delivery_ok'),
                          'session_id': result.get('session_id'),
                          'outcome': result.get('outcome'), 'error': result.get('error')},
                         ensure_ascii=False))
    resident = holder.get('resident')
    if resident is not None:
        print(json.dumps({'setup_latency_ms': resident.resident_latency_hook()['setup_ms']}))
    return 0 if results and all(result.get('delivery_ok') for result in results) else 1


if __name__ == '__main__':
    raise SystemExit(main())
