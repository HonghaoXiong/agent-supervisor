#!/usr/bin/env python3
"""Bounded ZCode app-server client. A permission broker, not an OS sandbox."""
import asyncio
import hashlib
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
    def __init__(self, workspace, output, permission, config=None):
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
                        'subagentRuntimeConfigEnabled':False, 'memoryEnabled':False,
                        'askUserQuestionAutoResolutionEnabled':False,
                        'modelContextBudgetStrategy':'preflight-v1'}
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
                        'session/requestRuntimePreferences', 'session/event', 'state.updated')
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
    if set(packet) - {'workspace', 'prompt', 'allowed_files', 'commands', 'trusted_hashes'}:
        raise ValueError('Unknown task packet field')
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

    def check_frozen(self):
        if hashlib.sha256(read_regular(self.packet_path)).hexdigest() != self.packet_hash:
            raise ValueError('Supervisor packet changed')
        current = inventory(self.workspace)
        changed = {name for name in self.before.keys() | current.keys()
                   if self.before.get(name) != current.get(name)}
        if changed - self.allowed:
            raise ValueError('Frozen workspace inputs changed: '+str(sorted(changed-self.allowed)))
        for name, digest in self.packet.get('trusted_hashes', {}).items():
            path = Path(name)
            info = path.lstat()
            if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
                    or hashlib.sha256(read_regular(path)).hexdigest() != digest):
                raise ValueError('Trusted executable/input changed: '+name)

    def decide(self, params):
        decision, reason, command_id = 'deny', 'Not in this task contract', None
        try:
            if params.get('sessionId') != self.session_id or not self.session_id:
                raise ValueError('Wrong session')
            if not self.turn_id or params.get('turnId') != self.turn_id:
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
        except (ValueError, OSError) as error:
            reason = str(error)
        self.decisions.append({'request_id':params.get('requestId'),
                              'session_id':params.get('sessionId'), 'turn_id':params.get('turnId'),
                              'tool_call_id':params.get('toolCallId'), 'tool':params.get('toolName'),
                              'input':params.get('input'), 'decision':decision,
                              'reason':reason, 'command_id':command_id})
        return {'decision':decision, 'reason':reason}


def own_event(event, session_id, turn_id):
    return (isinstance(event, dict) and event.get('sessionId') == session_id
            and event.get('turnId') == turn_id)


def execution_evidence(events, authority):
    """Correlate actual terminal metadata with a one-use broker decision."""
    scheduled, results, streamed = {}, {}, {}
    problems = []
    for event in events:
        if not own_event(event, authority.session_id, authority.turn_id):
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
                 if item['decision']=='allow' and item['tool'] in ('Bash', 'Write', 'Edit')}
    commands = []
    for tid in approvals:
        if tid not in scheduled or tid not in results:
            problems.append('Approved tool lacks scheduling or terminal evidence: '+str(tid))
    for tid in results:
        if tid not in scheduled:
            problems.append('Terminal tool result has no scheduling identity: '+str(tid))
    for tid, scheduled_tool in scheduled.items():
        tool = scheduled_tool.get('toolName')
        if tool not in ('Read', 'Write', 'Edit', 'Bash'):
            problems.append('Tool is outside the exposed tool allowlist: '+tid)
            continue
        terminal = results.get(tid)
        if not terminal or terminal.get('kind') != 'result':
            problems.append('Tool did not return a successful result: '+tid)
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
                and actual_input != approved['input']):
            problems.append('Scheduled tool differs from approved input: '+tid)
            continue
        if tool in ('Write', 'Edit'):
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
        authority = Authority(packet, packet_path, before)
        authority.check_frozen()
        # Reserve the basename atomically so another worker cannot overwrite it.
        with output.open('x') as stream:
            json.dump({'delivery_ok': False, 'state': 'starting'}, stream)
        write_json(output.with_suffix('.before.json'), before)
        client = ProtocolClient(workspace, output, authority.decide, config)
        input_id = 'input_'+uuid.uuid4().hex
        def receive_event(event):
            if (event.get('sessionId') == authority.session_id
                    and event.get('type') == 'turn.started'
                    and event.get('payload',{}).get('inputId') == input_id):
                authority.turn_id = event['turnId']
        client.on_event = receive_event
        result = {'delivery_ok':False,'workspace':str(workspace),'input_id':input_id,
                  'generation':uuid.uuid4().hex, 'scientific_acceptance':'pending_codex_review'}
        deadline = time.monotonic()+args.timeout
        def remaining():
            return max(0.001, deadline-time.monotonic())
        async def rpc(method, params):
            return await asyncio.wait_for(client.request(method, params), remaining())
        try:
            await asyncio.wait_for(client.start(), remaining())
            options = {'workspace':{'workspacePath':str(workspace),'workspaceKey':str(workspace)},
                       'mcpServers':[], 'toolAllowlist':['Read','Write','Edit','Bash'],
                       'dynamicWorkflowEnabled':False}
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
                snapshot = await rpc('session/create', {**options,'mode':'build','titleGenerationEnabled':False})
            sid = snapshot['session']['sessionId']
            authority.session_id = sid
            result['session_id'] = sid
            if args.resume and sid != args.resume:
                raise ValueError('Resume returned a different session')
            subscription = await rpc('session/subscribe', {
                'sessionId':sid,'deliveryKind':'desktop-continuous','includeSnapshot':False})
            result['start_event_seq'] = subscription['eventSeq']
            await rpc('session/send', {'sessionId':sid,'inputId':input_id,'content':packet['prompt']})
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
            snapshot = await rpc('session/read', {'sessionId':sid,'deliveryKind':'desktop-continuous','messageLimit':10})
            if snapshot['session'].get('status') != 'idle' or snapshot['runtime'].get('pendingRequestIds'):
                raise ValueError('Worker is not idle or still has pending interactions')
            authority.check_frozen()
            commands, problems = execution_evidence(client.events, authority)
            result['commands'] = commands
            seen = {entry['command_id'] for entry in commands}
            required = {entry['id'] for entry in packet.get('commands',[]) if entry.get('required',True)}
            if required-seen:
                problems.append('Required commands were not executed: '+str(sorted(required-seen)))
            result['execution_problems'] = problems
            result['delivery_ok'] = not problems
        except (ValueError, OSError, EOFError, asyncio.TimeoutError, KeyError, TypeError) as error:
            result['error'] = str(error) or type(error).__name__
            if authority.session_id and client.process and client.process.returncode is None:
                try:
                    await asyncio.wait_for(client.request('session/stop',{'sessionId':authority.session_id}), 2)
                except Exception:
                    pass
        finally:
            await client.close()
            result['permission_decisions'] = authority.decisions
            result['process_exit_code'] = client.process.returncode if client.process else None
            result['scope_enforcement'] = 'permission broker and post-run hashes; not an OS sandbox'
            try:
                authority.check_frozen()
                after = inventory(workspace)
                result['changed_files'] = sorted(name for name in before.keys()|after.keys() if before.get(name)!=after.get(name))
            except (ValueError,OSError) as error:
                result.update(delivery_ok=False,scope_error=str(error))
            if result['process_exit_code'] != 0:
                result['delivery_ok'] = False
            write_json(output,redact_transport(result))
            if authority.session_id and result.get('completion'):
                write_json(state_dir/(authority.session_id+'.json'), {
                    'workspace':str(workspace),'state':'exited','pid':client.process.pid,
                    'result_file':str(output)})
        print(json.dumps({k:result.get(k) for k in ['delivery_ok','session_id','turn_id','changed_files','execution_problems','error']}))
        return result['delivery_ok']
