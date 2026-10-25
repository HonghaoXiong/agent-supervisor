#!/usr/bin/env python3
"""Bounded ZCode app-server client. A permission broker, not an OS sandbox."""
import asyncio
import hashlib
import inspect
import json
import math
import os
from pathlib import Path
import re
import signal
import stat
import time
import uuid

from .common import process_alive, read_regular, workspace_lock, write_json
from . import profile
from .notify import send as send_notification


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


def inventory(workspace):
    if not workspace.is_dir() or workspace.is_symlink():
        raise ValueError('Workspace must be a real directory')
    result = {}
    total = 0
    for path in [workspace, *sorted(workspace.rglob('*'))]:
        info = path.lstat()
        if stat.S_ISLNK(info.st_mode):
            raise ValueError(f'Symlink in input: {path}')
        if stat.S_ISDIR(info.st_mode):
            continue
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
            raise ValueError(f'Only ordinary, unshared files are allowed: {path}')
        total += info.st_size
        if total > 256 * 1024**2 or len(result) >= 5000:
            raise ValueError('Input exceeds 256 MiB / 5000 files')
        result[path.relative_to(workspace).as_posix()] = {
            'sha256': hashlib.sha256(read_regular(path)).hexdigest(),
            'mode': stat.S_IMODE(info.st_mode)}
    return result


class ProtocolClient:
    def __init__(self, workspace, output, permission, config=None, orchestration=None):
        # orchestration: the validated packet block (or None) backing the
        # host-side subagent runtime config answers; never client input.
        self.orchestration = orchestration
        self.workspace = workspace
        self.output = output
        self.permission = permission
        self.config = config
        self.pending = {}
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
        try:
            await self.send({'id':rid, 'method':method, 'params':params})
            return await future
        finally:
            self.pending.pop(rid, None)

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
        self.failure = str(error) or type(error).__name__
        for future in self.pending.values():
            if not future.done():
                future.set_exception(error)
        self.pending.clear()

    def reverse_done(self, task):
        if not task.cancelled() and task.exception() is not None:
            self.fail(task.exception())

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
                        self.events.append(frame['params'])
                        if self.on_event:
                            self.on_event(frame['params'])
                elif frame.get('id') in self.pending:
                    future = self.pending.pop(frame['id'])
                    if future.done():
                        continue
                    if 'error' in frame:
                        future.set_exception(ValueError(str(frame['error'])))
                    elif 'result' in frame:
                        future.set_result(frame['result'])
                    else:
                        raise ValueError('Response has no result or error')
                else:
                    raise ValueError('Unmatched protocol response')
            if not self.closing:
                raise EOFError('ZCode protocol stream closed')
        except asyncio.CancelledError:
            return
        except Exception as error:
            self.fail(error)

    async def close(self):
        self.closing = True
        if self.process is not None:
            if self.process.stdin is not None:
                self.process.stdin.close()
            try:
                await asyncio.wait_for(self.process.wait(), 5)
            except asyncio.TimeoutError:
                try:
                    os.killpg(self.process.pid, signal.SIGTERM)
                except ProcessLookupError:
                    pass
                try:
                    await asyncio.wait_for(self.process.wait(), 3)
                except asyncio.TimeoutError:
                    try:
                        os.killpg(self.process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                    await self.process.wait()
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


def goal_state_from_snapshot(snapshot):
    """Goal facts from the real session/goal snapshot (frozen RPC replay
    verified): target state lives at projection.target; missing means no goal."""
    if not isinstance(snapshot, dict):
        return None
    projection = snapshot.get('projection')
    target = projection.get('target') if isinstance(projection, dict) else None
    return target if isinstance(target, dict) else None


def workflow_jobs_from_snapshot(snapshot):
    """Background workflow jobs from the real snapshot projection."""
    if not isinstance(snapshot, dict):
        return []
    projection = snapshot.get('projection')
    jobs = projection.get('backgroundJobs') if isinstance(projection, dict) else None
    if not isinstance(jobs, list):
        return []
    return [job for job in jobs if isinstance(job, dict)]


def runtime_owned(name):
    """Paths owned by the native workflow runtime inside the workspace."""
    return (isinstance(name, str)
            and (name.startswith('.zcode/workflow-runs/')
                 or name.startswith('.zcode/workflow-drafts/')))


class Authority:
    """One-turn authority loaded from a supervisor-owned immutable packet."""
    def __init__(self, packet, packet_path, before):
        self.packet = packet
        self.packet_path = packet_path
        self.packet_hash = hashlib.sha256(read_regular(packet_path)).hexdigest()
        self.workspace = Path(packet['workspace']).resolve()
        self.before = before
        self.allowed = set(packet['allowed_files'])
        self.session_id = None
        self.turn_id = None
        self.seen = set()
        self.seen_tools = set()
        self.decisions = []
        self.uses = {}
        # Native orchestration (optional packet block): multi-turn binding for
        # goal/workflow-driven turns, and a child registry keyed by the
        # childSessionId values observed in this parent's subagent_spawned
        # events. Children never self-declare: a request whose origin names an
        # unobserved child session is denied.
        self.orchestration = packet.get('orchestration')
        self.parent_turns = {None} if not self.orchestration else set()
        self.children = {}
        self.runtime_artifacts = []
        # Agent dispatch toolCallIds observed in this parent's own stream: a
        # bubbled child request is only honored when its parentToolCallId is
        # one of these actual dispatches (or the child was registered from a
        # subagent_spawned event, when the server emits them).
        self.agent_dispatches = set()
        # Approved CreateWorkflow launches: toolCallId -> launched run id
        # (parsed from the tool result). Workflow actors embed the run id in
        # their child session ids, which is the parent-observable fact that
        # associates them with a reviewed script launch.
        self.workflow_launches = {}
        self._approved_workflow_tids = set()
        # N2 provenance: first-trusted content record per launched run — the
        # .mjs bytes observed while the run's background job was completed.
        # A matching filename alone proves nothing.
        self.runtime_entry_bytes = {}
        # N2 idempotence: run ids whose approved launch result has been
        # observed ONCE. The first observation fixes the outcome — bytes
        # captured, or the entry factually missing — and identical result
        # replays (subscription callback AND the event-drain loop both feed
        # note_event) never sample a second time to adopt later bytes.
        self.launch_results_observed = set()

    def note_turn(self, turn_id):
        """Register a parent-session turn as authorized (orchestration only)."""
        if self.orchestration and isinstance(turn_id, str) and turn_id:
            self.parent_turns.add(turn_id)

    def note_children_poll(self, running, ended_items):
        """Merge the authoritative session/subagents poll into the registry.

        This is the ONLY path that introduces new children (besides observed
        subagent_spawned events): the poll is a server fact, not a request
        claim. Repeated polls update statuses running -> terminal.
        """
        for item in list(running or []) + list(ended_items or []):
            if not isinstance(item, dict):
                continue
            child = item.get('childSessionId')
            if not isinstance(child, str) or not child:
                continue
            dispatch = item.get('toolCallId')
            is_actor_entry = 'dwf-' in child
            entry = {
                'agentId': item.get('agentId'),
                'parentToolCallId': (f'workflow:{child.split("dwf-")[1].split("-actor")[0]}'
                                     if is_actor_entry and 'dwf-' in child
                                     else dispatch),
                'subagentType': item.get('subagentType'),
                'status': item.get('status') or 'running',
            }
            existing = self.children.get(child)
            if existing is None:
                self.children[child] = entry
            else:
                existing.update({k: v for k, v in entry.items()
                                 if existing.get(k) in (None, 'running') or v is not None})

    def note_event(self, event):
        """Feed parent-session lifecycle events into the child registry."""
        payload = event.get('payload') if isinstance(event, dict) else None
        if not isinstance(payload, dict):
            return
        if event.get('type') == 'tool.updated' and payload.get('kind') == 'result':
            content = ((payload.get('result') or {}).get('content')
                       if isinstance(payload.get('result'), dict) else None)
            tid = payload.get('toolCallId')
            # tool.updated results carry no toolName (verified live), but run
            # ids are registered ONLY for toolCallIds the broker approved as
            # CreateWorkflow launches — an ordinary tool result quoting a
            # dwfrun id is text, never a registry credential.
            if content and isinstance(tid, str) and tid in self._approved_workflow_tids:
                match = re.search(r'dwfrun-[0-9a-f-]{36}', str(content))
                if match:
                    run_id = match.group(0)
                    # First registration wins: a replayed (or contradicting)
                    # later result never redefines which run this launch is.
                    if tid not in self.workflow_launches:
                        self.workflow_launches[tid] = run_id
                    # N2 provenance: the FIRST observation of the launch
                    # result fixes the content record — at that instant the
                    # reviewed runtime has just written the run entry
                    # (write-then-report), so capture THOSE bytes, or record
                    # the miss when no entry exists yet. Either way the
                    # outcome is final: a later identical result replay must
                    # never adopt bytes that appeared after the first look.
                    if run_id not in self.launch_results_observed:
                        self.launch_results_observed.add(run_id)
                        entry = self.workspace / '.zcode' / 'workflow-runs' / f'{run_id}.mjs'
                        if entry.is_file() and not entry.is_symlink():
                            self.runtime_entry_bytes[run_id] = (
                                hashlib.sha256(read_regular(entry)).hexdigest())
        if event.get('type') == 'tool.updated' and payload.get('kind') == 'scheduled' \
                and payload.get('toolName') == 'Agent':
            dispatch_id = payload.get('toolCallId')
            if isinstance(dispatch_id, str) and dispatch_id:
                self.agent_dispatches.add(dispatch_id)
        if event.get('type') == 'subagent_spawned':
            child = payload.get('childSessionId')
            if isinstance(child, str) and child and child not in self.children:
                self.children[child] = {
                    'agentId': payload.get('agentId'),
                    'parentToolCallId': payload.get('parentToolCallId'),
                    'subagentType': payload.get('subagentType'),
                    'status': 'running',
                }
        elif event.get('type') == 'subagent_stopped':
            child = payload.get('childSessionId')
            if isinstance(child, str) and child in self.children:
                status = payload.get('status')
                self.children[child]['status'] = status if isinstance(status, str) else 'ended-unknown'

    def check_frozen(self):
        if hashlib.sha256(read_regular(self.packet_path)).hexdigest() != self.packet_hash:
            raise ValueError('Supervisor packet changed')
        current = inventory(self.workspace)
        changed = {name for name in self.before.keys() | current.keys()
                   if self.before.get(name) != current.get(name)}
        # The workflow runtime journals its runs into .zcode/workflow-runs
        # (and drafts into .zcode/workflow-drafts). Inputs that existed in the
        # before-inventory are ALWAYS frozen — a path prefix is never a
        # license to modify or delete existing evidence. Only files that did
        # not exist before the run are classified as new runtime artifacts
        # (recorded with hashes; evidence, not an origin guarantee under this
        # server version's broker-less child boundary).
        if self.orchestration:
            launched_ids = {run_id.split('dwfrun-', 1)[-1]
                            for run_id in self.workflow_launches.values()}
            new_runtime = set()
            for name in changed:
                if name in self.before or not runtime_owned(name):
                    continue
                # Producer accounting (N2), real native layout (verified in
                # the pinned CLI, writeChildEntryFile ~offset 14173530): the
                # runtime writes FLAT .zcode/workflow-runs/<runId>.mjs plus a
                # first-write .gitignore containing exactly '*\n'. Only those
                # exact shapes for a run THIS task launched are artifacts;
                # anything else — named run directories, extra files — has no
                # producer binding and stays a violation.
                parts = Path(name).parts
                if (len(parts) == 3 and parts[0] == '.zcode'
                        and parts[1] == 'workflow-runs'
                        and parts[2].endswith('.mjs')
                        and parts[2][:-4].startswith('dwfrun-')
                        and parts[2][7:-4] in {rid.split('dwfrun-', 1)[-1]
                                                for rid in self.runtime_entry_bytes}):
                    run_id = 'dwfrun-' + parts[2][7:-4]
                    entry = self.workspace / name
                    current = (hashlib.sha256(read_regular(entry)).hexdigest()
                               if entry.is_file() and not entry.is_symlink() else None)
                    if current is not None and current == self.runtime_entry_bytes.get(run_id):
                        new_runtime.add(name)
                elif (name == '.zcode/workflow-runs/.gitignore'
                        and launched_ids
                        and self.workspace.joinpath(name).read_text(errors='replace') == '*\n'):
                    new_runtime.add(name)
        else:
            new_runtime = set()
        self.runtime_artifacts = sorted(new_runtime)
        violating = changed - self.allowed - new_runtime
        if violating:
            raise ValueError('Frozen workspace inputs changed: '+str(sorted(violating)))
        for name, digest in self.packet.get('trusted_hashes', {}).items():
            path = Path(name)
            info = path.lstat()
            if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
                    or hashlib.sha256(read_regular(path)).hexdigest() != digest):
                raise ValueError('Trusted executable/input changed: '+name)

    def decide(self, params):
        decision, reason, command_id = 'deny', 'Not in this task contract', None
        try:
            origin = params.get('origin') if isinstance(params.get('origin'), dict) else None
            if origin and origin.get('kind') == 'subagent':
                # Bubbled child request: identity comes from the parent's own
                # subagent_spawned events plus the origin metadata, never from
                # the child's self-declared session alone.
                if params.get('sessionId') != self.session_id or not self.session_id:
                    raise ValueError('Wrong session')
                child = origin.get('childSessionId')
                if not isinstance(child, str) or not child:
                    raise ValueError('Child origin lacks its own session identity')
                record = self.children.get(child)
                # Every identity field the origin carries must be complete and
                # must match what this parent observed; a request never fills
                # missing identity from its own claims (guidance §7).
                # Workflow actors: their session id embeds the run id of a
                # CreateWorkflow launch THIS parent got approved and observed
                # in a tool result, and their dispatch id is that launch's
                # toolCallId. Background actors legitimately outlive turns, so
                # the binding is the reviewed launch, not a live parent turn.
                actor_match = re.fullmatch(r'sess_dwf-(dwfrun-[0-9a-f-]{36})-actor_[0-9]+_[0-9]+',
                                           child if isinstance(child, str) else '')
                is_actor = bool(actor_match) and origin.get('agentType') == 'zcode-workflow' \
                    and actor_match.group(1) in self.workflow_launches.values()
                parent_turn = origin.get('parentTurnId')
                if not is_actor:
                    if not isinstance(parent_turn, str) or not parent_turn \
                            or parent_turn not in self.parent_turns:
                        raise ValueError('Child origin lacks a registered parent turn identity')
                elif parent_turn is not None and not isinstance(parent_turn, str):
                    raise ValueError('Child origin parent turn identity is malformed')
                child_turn = origin.get('childTurnId')
                if not isinstance(child_turn, str) or not child_turn:
                    raise ValueError('Child origin lacks its own turn identity')
                agent_id = origin.get('agentId')
                if not isinstance(agent_id, str) or not agent_id:
                    raise ValueError('Child origin lacks an agent identity')
                dispatch_id = origin.get('parentToolCallId')
                if record is not None:
                    if record.get('agentId') not in (None, agent_id):
                        raise ValueError('Child origin agent identity differs from the registered child')
                    # Workflow-actor origins carry no dispatch id (verified
                    # live); absence is compatible only when the registry
                    # record came from a workflow launch.
                    record_from_launch = str(record.get('parentToolCallId', '')).startswith('workflow:')
                    if dispatch_id is not None and record.get('parentToolCallId') not in (None, dispatch_id):
                        raise ValueError('Child origin dispatch identity differs from the registered child')
                    if dispatch_id is None and record.get('parentToolCallId') is not None \
                            and not record_from_launch:
                        raise ValueError('Child origin dispatch identity differs from the registered child')
                elif is_actor and record is None:
                    # An approved launch proves the run exists, not that every
                    # actor identity in a request exists. Actors enter the
                    # registry only from trusted facts (subagents poll or
                    # subagent_spawned events); is_actor merely waives the
                    # parent-turn binding for a record that is already there.
                    record = None
                # Dispatch-observed registration is no longer sufficient on
                # its own: an origin can quote a scheduled parent dispatch id
                # while inventing the child identity (verified counterexample).
                # Children enter the registry ONLY from server facts — the
                # subagents poll or subagent_spawned events — and the origin
                # must then match that record field by field.
                elif isinstance(dispatch_id, str) and dispatch_id in self.agent_dispatches:
                    record = None
                if record is None:
                    raise ValueError('Unknown child session; children must trace to an Agent dispatch of this run')
                if record.get('status') not in (None, 'running'):
                    raise ValueError('Child session is no longer running')
                if not is_actor:
                    allowed_types = (self.orchestration or {}).get('agents', {}).get('allowed_types', [])
                    agent_type = origin.get('agentType') if isinstance(origin.get('agentType'), str) else record['subagentType']
                    if agent_type not in allowed_types or record['subagentType'] not in allowed_types:
                        raise ValueError('Subagent type is not in the task contract')
            elif params.get('sessionId') != self.session_id or not self.session_id:
                raise ValueError('Wrong session')
            elif self.orchestration:
                # Orchestrated runs are legitimately multi-turn (goal/workflow
                # driven): every parent turn observed through turn.started is
                # authorized; nothing else is.
                if params.get('turnId') not in self.parent_turns:
                    raise ValueError('Turn is not part of this orchestrated run')
            elif not self.turn_id or params.get('turnId') != self.turn_id:
                raise ValueError('Wrong or missing turn identity')
            rid, tid = params.get('requestId'), params.get('toolCallId')
            if (not isinstance(rid, str) or not rid or not isinstance(tid, str)
                    or not tid or rid in self.seen or tid in self.seen_tools):
                raise ValueError('Missing identity or repeated permission request')
            self.seen.add(rid)
            self.seen_tools.add(tid)
            self.check_frozen()
            tool = params.get('toolName')
            data = params.get('input')
            if not isinstance(data, dict):
                raise ValueError('Tool input must be an object')
            if tool == 'Bash':
                if set(data) - {'command','description','timeout','run_in_background','dangerouslyDisableSandbox'}:
                    raise ValueError('Unexpected Bash input field')
                for flag in ('run_in_background', 'dangerouslyDisableSandbox'):
                    if flag in data and data[flag] is not False:
                        raise ValueError('Only foreground execution with normal sandbox settings')
                if 'description' in data and not isinstance(data['description'], str):
                    raise ValueError('Command description must be a string')
                for item in self.packet.get('commands', []):
                    if data.get('command') == item['command']:
                        command_id = item['id']
                        if self.uses.get(command_id, 0) >= item.get('max_uses', 1):
                            raise ValueError('Command invocation budget exhausted')
                        timeout = data.get('timeout', 120000)
                        if (isinstance(timeout, bool) or not isinstance(timeout, (int, float))
                                or not math.isfinite(timeout) or timeout <= 0
                                or timeout > item.get('max_timeout_ms', 120000)):
                            raise ValueError('Command timeout exceeds task budget')
                        self.uses[command_id] = self.uses.get(command_id, 0) + 1
                        decision, reason = 'allow', 'Exact supervisor-reviewed command and frozen hashes match'
                        break
            elif tool in ('Write','Edit'):
                fields = ({'file_path', 'content'} if tool == 'Write' else
                          {'file_path', 'old_string', 'new_string', 'replace_all'})
                if set(data) - fields:
                    raise ValueError('Unexpected file tool input field')
                required = ('content',) if tool == 'Write' else ('old_string', 'new_string')
                if any(not isinstance(data.get(field), str) for field in required):
                    raise ValueError('File tool text fields must be strings')
                if 'replace_all' in data and type(data['replace_all']) is not bool:
                    raise ValueError('Edit replace_all must be a boolean')
                name = data.get('file_path')
                if not isinstance(name, str) or not name or '\x00' in name:
                    raise ValueError('File tool has no file_path')
                path = Path(name)
                if '..' in path.parts:
                    raise ValueError('File path traversal is not allowed')
                if not path.is_absolute():
                    path = self.workspace/path
                path.relative_to(self.workspace)
                relative = path.resolve().relative_to(self.workspace).as_posix()
                if relative in self.allowed and not path.is_symlink():
                    decision, reason = 'allow', 'Explicit output file in task contract'
            elif tool == 'CreateWorkflow' and self.orchestration and 'workflow' in self.orchestration:
                workflow = self.orchestration['workflow']
                # adjustable_settings is appended by the runtime variant of
                # the tool schema (verified live); everything else is fixed.
                allowed_fields = {'name', 'script', 'path', 'args', 'max_concurrency',
                                  'subagent_model', 'adjustable_settings'}
                if set(data) - allowed_fields:
                    raise ValueError('Unexpected CreateWorkflow input field')
                adjustable = data.get('adjustable_settings', {})
                # Runtime-rebuilt approval-UI capability descriptor (verified
                # semantics): {subagent_model: bool, concurrency_ceiling?:
                # positive int}. The ceiling is a UI ceiling, NOT the effective
                # concurrency and is never compared to max_concurrency.
                if not isinstance(adjustable, dict):
                    raise ValueError('adjustable_settings must be an object')
                if set(adjustable) - {'subagent_model', 'concurrency_ceiling'}:
                    raise ValueError('adjustable_settings has unknown fields')
                if 'subagent_model' in adjustable and type(adjustable['subagent_model']) is not bool:
                    raise ValueError('adjustable_settings.subagent_model must be a boolean')
                if 'concurrency_ceiling' in adjustable and (
                        isinstance(adjustable['concurrency_ceiling'], bool)
                        or not isinstance(adjustable['concurrency_ceiling'], int)
                        or adjustable['concurrency_ceiling'] <= 0):
                    raise ValueError('adjustable_settings.concurrency_ceiling must be a positive integer')
                if 'args' in data:
                    # Reviewer finding 4: no packet field pins workflow args,
                    # so the channel stays closed until the schema declares it.
                    raise ValueError('Workflow args are not part of this contract')
                if 'script' in workflow and ('path' in data or 'args' in data):
                    raise ValueError('Inline-script contract cannot be launched by path or args')
                if 'path' in workflow and 'script' in data:
                    # Launching by path, the runtime re-attaches the file
                    # content as an inline script field; accept it only when it
                    # is byte-identical to the frozen reviewed file.
                    if data['script'] != read_regular(self.workspace / workflow['path']).decode('utf-8'):
                        raise ValueError('Runtime script differs from the reviewed file bytes')
                if 'script' in workflow:
                    if data.get('script') != workflow['script']:
                        raise ValueError('Workflow script does not match the reviewed packet bytes')
                else:
                    declared = (self.workspace / workflow['path']).resolve()
                    submitted = data.get('path')
                    if not isinstance(submitted, str) or not submitted:
                        raise ValueError('Workflow must be launched by its reviewed path')
                    candidate = Path(submitted)
                    if not candidate.is_absolute():
                        candidate = self.workspace / candidate
                    if candidate.resolve() != declared:
                        raise ValueError('Workflow path does not match the reviewed script file')
                if data.get('max_concurrency') != workflow['max_concurrency']:
                    raise ValueError('max_concurrency must equal the packet-declared value')
                if workflow.get('subagent_model') is not None:
                    if data.get('subagent_model') != workflow['subagent_model']:
                        raise ValueError('subagent_model must equal the packet-declared value')
                if 'name' in data and workflow.get('name') and data['name'] != workflow['name']:
                    raise ValueError('Workflow name does not match the packet')
                decision, reason = 'allow', 'Workflow script matches the reviewed packet contract'
                self._approved_workflow_tids.add(tid)
        except (ValueError, OSError) as error:
            reason = str(error)
        self.decisions.append({'request_id':params.get('requestId'),
                              'session_id':params.get('sessionId'), 'turn_id':params.get('turnId'),
                              'tool_call_id':params.get('toolCallId'), 'tool':params.get('toolName'),
                              'input':params.get('input'), 'decision':decision,
                              'reason':reason, 'command_id':command_id,
                              'origin':params.get('origin')})
        return {'decision':decision, 'reason':reason}


def own_event(event, session_id, turn_id):
    return (isinstance(event, dict) and event.get('sessionId') == session_id
            and event.get('turnId') == turn_id)


def authorized_event(event, authority):
    """Event filter that admits orchestrated turns and registered children.

    Baseline runs stay exactly two-field bound (parent session + dispatch
    turn). With an orchestration block, parent-session turns observed through
    turn.started and child sessions registered from subagent_spawned events
    are also admitted; nothing else is.
    """
    if not authority.orchestration:
        return own_event(event, authority.session_id, authority.turn_id)
    if not isinstance(event, dict):
        return False
    session, turn = event.get('sessionId'), event.get('turnId')
    if session == authority.session_id:
        return turn in authority.parent_turns or turn == authority.turn_id
    return session in authority.children


def orchestration_tool_surface(orchestration):
    from .orchestration import AGENT_TOOLS, WORKFLOW_TOOLS
    tools = {'Read', 'Write', 'Edit', 'Bash'}
    if orchestration:
        if 'agents' in orchestration:
            tools.update(AGENT_TOOLS)
            # Children with pinned profile.skills legitimately invoke the
            # Skill tool inside their own sessions (verified live); those
            # inner calls surface in the parent stream.
            for profile in (orchestration['agents'].get('profiles') or []):
                if isinstance(profile, dict) and profile.get('skills'):
                    tools.add('Skill')
        if 'workflow' in orchestration:
            tools.update(WORKFLOW_TOOLS)
        if 'skills' in orchestration:
            tools.add('Skill')
    return tools


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


def observed_model(snapshot):
    """Keep only safe model selection fields, including on a failed preflight."""
    settings = snapshot.get('settings') if isinstance(snapshot, dict) else None
    model = settings.get('model') if isinstance(settings, dict) else None
    current = model.get('current') if isinstance(model, dict) else None
    if not isinstance(current, dict):
        return None
    safe = {key:current.get(key) if isinstance(current.get(key), str) else None
            for key in ('providerId', 'modelId')}
    if 'options' in current:
        options = current['options']
        value = options.get('reasoningLevel') if isinstance(options, dict) else None
        safe['options'] = {'reasoningLevel':value if isinstance(value, str) else None}
    return safe


def check_model_snapshot(snapshot, requested):
    """Require the runtime selection before sending a task and after completion."""
    settings = snapshot.get('settings') if isinstance(snapshot, dict) else None
    model = settings.get('model') if isinstance(settings, dict) else None
    current = model.get('current') if isinstance(model, dict) else None
    selected = ({key:current.get(key) for key in ('providerId', 'modelId')}
                if isinstance(current, dict) else None)
    if isinstance(selected, dict) and 'options' in requested:
        selected['options'] = current.get('options')
    if selected != requested:
        raise ValueError('Runtime model does not match the task packet')
    return selected


def model_execution_evidence(events, session_id, turn_id, requested, authority=None):
    """Audit main-agent provider requests, not the worker's own model claim."""
    receipts = []
    for event in events:
        if authority is not None:
            if not authorized_event(event, authority) or event.get('type') != 'session.updated':
                continue
        elif not own_event(event, session_id, turn_id) or event.get('type') != 'session.updated':
            continue
        payload = event.get('payload')
        if not isinstance(payload, dict) or payload.get('type') not in (
                'model_request_started', 'model_request_completed', 'model_request_failed'):
            continue
        call = payload.get('modelCall')
        if not isinstance(call, dict) or call.get('actorKind') != 'main':
            continue
        selected = {key:payload.get(key) for key in ('providerId', 'modelId')}
        identity = {key:requested[key] for key in ('providerId', 'modelId')}
        if authority is not None and event.get('sessionId') != session_id:
            # Child-session model requests are receipted with their own
            # identity; the parent identity check does not apply to them.
            receipts.append({'event_seq':event.get('seq'), 'type':payload['type'],
                             'session_id':event.get('sessionId'),
                             'session_kind':'subagent-child',
                             **selected, 'request_id':payload.get('requestId'),
                             'operation':call.get('operation')})
            continue
        if authority is not None and payload.get('sessionId') == session_id:
            # Orchestrated parent sessions run goal/workflow-driven turns and
            # workflow actors with their own turn ids; the model identity pin
            # stays, the single-turn equality does not.
            if selected != identity:
                raise ValueError('Provider request model or identity differs from the task packet')
        elif (selected != identity or payload.get('sessionId') != session_id
                or payload.get('turnId') != turn_id):
            raise ValueError('Provider request model or identity differs from the task packet')
        receipts.append({'event_seq':event.get('seq'), 'type':payload['type'],
                         **selected, 'request_id':payload.get('requestId'),
                         'operation':call.get('operation'),
                         'reasoning':call.get('reasoning')})
    kinds = {receipt['type'] for receipt in receipts}
    if not {'model_request_started', 'model_request_completed'} <= kinds:
        raise ValueError('Missing actual provider model execution evidence')
    return receipts


def _workflow_input_equivalent(tool, actual, approved, workspace):
    """CreateWorkflow inputs are runtime-expanded before the broker sees
    them (path absolutized, script re-attached, adjustable_settings added);
    the streamed model input stays raw. Compare the pinned fields only —
    script bytes were already verified at approval time."""
    if tool != 'CreateWorkflow' or not isinstance(actual, dict) or not isinstance(approved, dict):
        return False
    if actual.get('name') != approved.get('name'):
        return False
    if actual.get('max_concurrency') != approved.get('max_concurrency'):
        return False
    actual_path = actual.get('path')
    approved_path = approved.get('path')
    if isinstance(actual_path, str) and isinstance(approved_path, str):
        from pathlib import Path as _P
        from pathlib import Path as _P2
        root = _P2(workspace)
        left = root / actual_path if not _P(actual_path).is_absolute() else _P(actual_path)
        right = root / approved_path if not _P(approved_path).is_absolute() else _P(approved_path)
        return left.resolve() == right.resolve()
    return actual_path == approved_path


def _edit_input_equivalent(tool, actual, approved):
    """Edit permission requests carry the schema-default replace_all=false;
    the streamed model input legitimately omits the field (verified live,
    2026-10-24 resume run). Only that documented default may differ — any
    other field mismatch stays a rejection."""
    if tool != 'Edit' or not isinstance(actual, dict) or not isinstance(approved, dict):
        return False
    normalized_actual = {**actual, 'replace_all': actual.get('replace_all', False)}
    normalized_approved = {**approved, 'replace_all': approved.get('replace_all', False)}
    return normalized_actual == normalized_approved


def execution_evidence(events, authority):
    """Correlate actual terminal metadata with a one-use broker decision."""
    scheduled, results, streamed = {}, {}, {}
    problems = []
    orchestration = authority.orchestration
    tool_surface = orchestration_tool_surface(orchestration)
    strict_pairing_tools = ({'Bash', 'Write', 'Edit', 'CreateWorkflow'} if orchestration
                            else {'Bash', 'Write', 'Edit'})
    for event in events:
        if not authorized_event(event, authority):
            continue
        if event.get('type') not in ('model.streaming', 'tool.updated'):
            continue
        payload = event.get('payload')
        if not isinstance(payload, dict):
            problems.append('Malformed tool event payload')
            continue
        kind = payload.get('kind')
        if event['type'] == 'model.streaming' and kind != 'tool_call':
            continue
        if event['type'] == 'tool.updated' and kind not in ('scheduled', 'result', 'error'):
            continue
        tid = payload.get('toolCallId')
        if not isinstance(tid, str) or not tid:
            problems.append('Tool event lacks a valid call identity')
            continue
        if event['type'] == 'model.streaming':
            if tid in streamed and streamed[tid] != payload:
                problems.append('Conflicting streamed tool input: '+tid)
            streamed[tid] = payload
        elif kind == 'scheduled':
            if tid in scheduled:
                problems.append('Duplicate scheduled tool: '+tid)
            scheduled[tid] = payload
        else:
            if tid in results:
                problems.append('Duplicate terminal tool result: '+tid)
            results[tid] = payload
    approvals = {item['tool_call_id']:item for item in authority.decisions
                 if item['decision']=='allow' and item['tool'] in strict_pairing_tools}
    commands = []
    for tid in approvals:
        if tid not in scheduled or tid not in results:
            problems.append('Approved tool lacks scheduling or terminal evidence: '+str(tid))
    for tid in results:
        if tid not in scheduled:
            problems.append('Terminal tool result has no scheduling identity: '+str(tid))
    for tid, scheduled_tool in scheduled.items():
        tool = scheduled_tool.get('toolName')
        if tool not in tool_surface:
            problems.append('Tool is outside the exposed tool allowlist: '+tid)
            continue
        terminal = results.get(tid)
        if not terminal or terminal.get('kind') != 'result':
            problems.append('Tool did not return a successful result: '+tid)
            continue
        if orchestration and tool not in strict_pairing_tools:
            # Orchestration tools (Agent/Skill/GetWorkflowRun/...) are
            # receipted as observed schedules with terminal results; their
            # outcome truth comes from the child registry and workflow run
            # state, not from this generic success field.
            commands.append({'command_id': f'tool:{tool}:{tid}', 'tool_call_id': tid,
                             'tool': tool, 'kind': 'orchestration-tool',
                             'terminal_kind': terminal.get('kind')})
            continue
        if 'toolName' in terminal and terminal['toolName'] != tool:
            problems.append('Terminal tool name differs from scheduled tool: '+tid)
            continue
        data = terminal.get('result')
        if not isinstance(data, dict) or data.get('success') is not True:
            problems.append('Tool result reports failure or is malformed: '+tid)
            continue
        actual_input = scheduled_tool.get('input')
        if tid in streamed:
            actual_input = streamed[tid].get('input')
            if streamed[tid].get('toolName') != tool:
                problems.append('Streamed tool name differs from scheduled tool: '+tid)
                continue
        if tool == 'Read':
            continue
        approved = approvals.get(tid)
        if approved is None or approved['tool'] != tool:
            problems.append(tool+' execution has no matching broker approval: '+tid)
            continue
        if ((tid in streamed or actual_input is not None)
                and actual_input != approved['input']
                and not _workflow_input_equivalent(tool, actual_input, approved['input'],
                                                   authority.workspace)
                and not _edit_input_equivalent(tool, actual_input, approved['input'])):
            problems.append('Scheduled tool differs from approved input: '+tid)
            continue
        if tool in ('Write', 'Edit'):
            continue
        if tool == 'CreateWorkflow':
            # The launch itself is receipted here; run-state evidence comes
            # from the orchestration tool results block, not from a shell
            # exit record.
            commands.append({'command_id': f'workflow-launch:{tid}', 'tool_call_id': tid,
                             'tool': tool, 'kind': 'workflow-launch',
                             'terminal_kind': terminal.get('kind')})
            continue
        perf = data.get('perf')
        detail = perf.get('detail') if isinstance(perf, dict) else None
        command = detail.get('command') if isinstance(detail, dict) else None
        if (not isinstance(command, dict) or detail.get('kind') != 'command'
                or type(command.get('exitCode')) is not int or command['exitCode'] != 0
                or command.get('timedOut') is not False
                or command.get('status') != 'completed'):
            problems.append('Command lacks successful foreground exit evidence: '+tid)
            continue
        commands.append({'command_id':approved['command_id'],
                         'tool_call_id':tid, 'result':data})
    return commands, problems


async def execute(args, config):
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
    with workspace_lock(workspace, state_dir):
        before = inventory(workspace)
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
        authority = Authority(packet, packet_path, before)
        authority.check_frozen()
        # Reserve the basename atomically so another worker cannot overwrite it.
        with output.open('x') as stream:
            json.dump({'delivery_ok': False, 'state': 'starting'}, stream)
        write_json(output.with_suffix('.before.json'), before)
        # Test doubles from parallel work may predate the orchestration
        # parameter; pass it only when the constructor accepts it.
        if 'orchestration' in inspect.signature(ProtocolClient.__init__).parameters:
            client = ProtocolClient(workspace, output, authority.decide, config,
                                    orchestration=authority.orchestration)
        else:
            client = ProtocolClient(workspace, output, authority.decide, config)
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
                orchestration_tool_results = []
                for event in client.events:
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
                for event in client.events:
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
            if requested_model is not None:
                result['observed_model_after'] = observed_model(snapshot)
                result['effective_model_after'] = check_model_snapshot(snapshot, requested_model)
                result['model_execution'] = model_execution_evidence(
                    client.events, sid, authority.turn_id, requested_model,
                    authority=authority)
            authority.check_frozen()
            commands, problems = execution_evidence(client.events, authority)
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
                                and hashlib.sha256(read_regular(entry)).hexdigest()
                                == authority.runtime_entry_bytes.get(run_id))
                    return False
                result['changed_files'] = [name for name in changed_all if not _final_artifact(name)]
                runtime_artifacts = [name for name in changed_all if _final_artifact(name)]
                if runtime_artifacts:
                    result['runtime_artifacts'] = {
                        name: hashlib.sha256(read_regular(workspace / name)).hexdigest()
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
        print(json.dumps({k:result.get(k) for k in ['delivery_ok','session_id','turn_id','changed_files','execution_problems','error']}))
        return result['delivery_ok']
