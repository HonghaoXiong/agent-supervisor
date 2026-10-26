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
import json
import os
import re
import shlex
import signal
import time
from collections import Counter
from pathlib import Path

from . import __version__
from .authority import worker_response

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


class CodexAppServerClient:
    """One resident app-server process plus its JSON-RPC client.

    Structured after protocol.ProtocolClient: a read loop, a pending-future
    table keyed by request id, and reverse (server->client) request dispatch.
    All frames, both directions, are appended (redacted) to
    ``evidence_dir/appserver-frames.jsonl``.
    """

    def __init__(self, codex_path, evidence_dir: Path, *, transport='stdio', env=None):
        self.codex_path = str(codex_path)
        self.evidence_dir = Path(evidence_dir)
        self.transport = transport
        self.env = env
        self.pending = {}
        self.notifications = []
        self.counter = 0
        self.failure = None
        self.process = None
        self.reader = None
        self.tasks = []
        self.on_server_request = None  # (method, params) -> response dict | None
        self.on_notification = None    # (method, params) -> None
        self.frames_path = self.evidence_dir / FRAMES_FILENAME
        self.stderr_path = self.evidence_dir / STDERR_FILENAME
        self.frames = None
        self.stderr = None
        self.closing = False
        self.initialize_result = None

    async def start(self) -> dict:
        """Spawn the process, start the read loop, and run one initialize.

        Returns the initialize result (v1/InitializeResponse.json:
        {codexHome, platformFamily, platformOs, userAgent}).
        """
        if self.process is not None or self.closing:
            raise AppServerError('App-server client cannot be started twice')
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
                'capabilities': {'experimentalApi': False}})
            return self.initialize_result
        except BaseException:
            await self.close()
            raise

    async def send(self, frame):
        if self.process is None or self.process.stdin is None or self.closing:
            raise EOFError('Codex app-server client is not running')
        self._log_frame('send', frame)
        self.process.stdin.write((json.dumps(frame, ensure_ascii=False) + '\n').encode())
        await self.process.stdin.drain()

    async def request(self, method, params, timeout=None) -> dict:
        """One client->server request; returns its result or raises."""
        if self.failure:
            raise EOFError(self.failure)
        self.counter += 1
        rid = f'czs-{self.counter}'
        future = asyncio.get_running_loop().create_future()
        self.pending[rid] = future
        try:
            await self.send({'id': rid, 'method': method, 'params': params})
            return await asyncio.wait_for(future, timeout)
        finally:
            self.pending.pop(rid, None)

    def _log_frame(self, direction, frame):
        if self.frames is not None:
            self.frames.write(json.dumps({'dir': direction, 'frame': redact(frame)},
                                         ensure_ascii=False) + '\n')
            self.frames.flush()

    def fail(self, error):
        self.failure = str(error) or type(error).__name__
        for future in self.pending.values():
            if not future.done():
                future.set_exception(EOFError(self.failure))
        self.pending.clear()

    async def _reverse(self, frame):
        rid, method = frame.get('id'), frame.get('method')
        params = frame.get('params')
        if not isinstance(params, dict):
            params = {}
        try:
            response = None
            if callable(self.on_server_request):
                try:
                    response = self.on_server_request(method, params)
                except Exception:
                    response = None  # an unknown request shape never kills the loop
            if response is None:
                await self.send({'id': rid, 'error': {
                    'code': -32601, 'message': f'Unsupported server request: {method}'}})
            else:
                await self.send({'id': rid, 'result': response})
        except (OSError, EOFError) as error:
            self.fail(error)

    def _reverse_done(self, task):
        if not task.cancelled() and task.exception() is not None:
            self.fail(task.exception())

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
                self._log_frame('recv', frame)
                if 'method' in frame and 'id' in frame:
                    task = asyncio.create_task(self._reverse(frame))
                    self.tasks.append(task)
                    task.add_done_callback(self._reverse_done)
                elif 'method' in frame:
                    self.notifications.append(frame)
                    if callable(self.on_notification):
                        try:
                            self.on_notification(frame.get('method'), frame.get('params'))
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
        """SIGTERM -> SIGKILL gradient, mirroring ProtocolClient.close."""
        self.closing = True
        if self.process is not None:
            if self.process.stdin is not None:
                self.process.stdin.close()
            try:
                await asyncio.wait_for(self.process.wait(), 5)
            except asyncio.TimeoutError:
                for sig in (signal.SIGTERM, signal.SIGKILL):
                    try:
                        os.killpg(self.process.pid, sig)
                    except (ProcessLookupError, PermissionError):
                        pass
                    try:
                        await asyncio.wait_for(self.process.wait(), 3)
                        break
                    except asyncio.TimeoutError:
                        continue
                else:
                    await self.process.wait()
        tasks = ([self.reader] if self.reader is not None else []) + self.tasks
        for task in tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
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

    def __init__(self, authority, thread_id):
        self.authority = authority
        self.thread_id = thread_id
        self.decisions = []
        self._seq = 0

    # -- plumbing -----------------------------------------------------------

    def _ids(self, method, params):
        """requestId/toolCallId from schema id fields (v2 itemId/approvalId, v1 callId)."""
        candidates = [params.get('itemId'), params.get('callId'),
                      params.get('approvalId'), params.get('id')]
        strings = [value for value in candidates if isinstance(value, str) and value]
        self._seq += 1
        request_id = strings[0] if strings else f'{method}#{self._seq}'
        tool_call_id = (strings[1] if len(strings) > 1 else f'{method}#{self._seq}')
        return request_id, tool_call_id

    def _ask(self, method, worker, params, summary, tool_name, tool_input, hint=None):
        """One authority.decide round trip plus worker_response shaping."""
        request_id, tool_call_id = self._ids(method, params)
        authority_params = {
            'sessionId': self.thread_id,
            'turnId': (params.get('turnId') if isinstance(params.get('turnId'), str) and params['turnId']
                       else params.get('turn_id') if isinstance(params.get('turn_id'), str)
                       else 'codex-turn'),
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
        return response

    def _record_deny(self, method, params, summary, response, reason):
        record = {'method': method, 'decision': 'deny', 'reason': reason,
                  'response': response, 'params': summary,
                  'alert': f'P1 grants no {method} requests'}
        self.decisions.append(record)
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
            return self._record_deny(method, params, _params_summary(params),
                                     dict(_DENY_RESPONSES[method]),
                                     'P1 does not authorize this request class')
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
            return self._record_deny(method, params, summary,
                                     worker_response(worker, {'decision': 'deny',
                                                              'reason': 'no command text'}),
                                     'approval request carries no command')
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
        if not files:
            return self._record_deny(
                method, params, summary,
                worker_response(worker, {'decision': 'deny',
                                         'reason': 'no declared file paths to verify'}),
                'unverifiable file-change approval')
        decision, reason = 'allow', 'Every changed file is in the task contract'
        for path in files:
            one = self.authority.decide({
                'sessionId': self.thread_id,
                'turnId': params.get('turnId') if isinstance(params.get('turnId'), str) else 'codex-turn',
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
    """Drive one task: thread/start -> turn/start -> notifications -> receipt."""

    def __init__(self, client: CodexAppServerClient):
        self.client = client

    async def run_task(self, authority, workspace: Path, prompt: str, *, model=None,
                       sandbox='read-only', approval_policy='on-request',
                       timeout_s=600) -> dict:
        started = time.monotonic()
        workspace = Path(workspace)
        evidence_dir = self.client.evidence_dir
        receipt = {'outcome': 'failed', 'thread_id': None, 'turn_id': None,
                   'final_message': None, 'command_items': [], 'approvals': [],
                   'notification_counts': {}, 'duration_s': 0.0,
                   'evidence': {'frames': str(self.client.frames_path),
                                'thread_read': str(evidence_dir / THREAD_READ_FILENAME)}}
        bridge = ApprovalBridge(authority, None)
        self.client.on_server_request = bridge.handle
        terminal = {}

        def on_notification(method, params):
            if (method == 'turn/completed' and isinstance(params, dict)
                    and receipt['thread_id'] is not None
                    and params.get('threadId') == receipt['thread_id']
                    and terminal.get('turn') is None):
                turn = params.get('turn')
                terminal['turn'] = turn if isinstance(turn, dict) else {}

        self.client.on_notification = on_notification
        processed = 0

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
                                               timeout=min(timeout_s, 120))
            thread = result.get('thread') if isinstance(result, dict) else None
            receipt['thread_id'] = (thread or {}).get('id') if isinstance(thread, dict) else None
            bridge.thread_id = receipt['thread_id']
            if not receipt['thread_id']:
                raise AppServerError('thread/start returned no thread id')

            # v2/TurnStartParams.json: {threadId, input: [UserInput...]}; the
            # text variant is {type: "text", text: str}.
            result = await self.client.request(
                'turn/start',
                {'threadId': receipt['thread_id'],
                 'input': [{'type': 'text', 'text': prompt}]},
                timeout=min(timeout_s, 120))
            turn = result.get('turn') if isinstance(result, dict) else None
            receipt['turn_id'] = (turn or {}).get('id') if isinstance(turn, dict) else None

            deadline = time.monotonic() + timeout_s
            while time.monotonic() < deadline:
                if self.client.failure:
                    raise EOFError(self.client.failure)
                drain_terminal()
                if terminal:
                    break
                await asyncio.sleep(0.02)
            if not terminal:
                receipt['outcome'] = 'timeout'
                try:
                    # v2/TurnInterruptParams.json: {threadId, turnId}.
                    await asyncio.wait_for(self.client.request(
                        'turn/interrupt', {'threadId': receipt['thread_id'],
                                           'turnId': receipt['turn_id']}), 5)
                except Exception:
                    pass
            else:
                # TurnStatus enum (v2/ThreadStartResponse.json): completed |
                # interrupted | failed | inProgress.
                status = terminal.get('turn', {}).get('status')
                receipt['outcome'] = 'delivered' if status == 'completed' else 'failed'
                error = terminal.get('turn', {}).get('error')
                if isinstance(error, dict) and error.get('message'):
                    receipt['error'] = str(error['message'])[:300]
        except (AppServerError, EOFError, OSError, asyncio.TimeoutError,
                ValueError, KeyError, TypeError) as error:
            receipt['outcome'] = receipt.get('outcome') or 'failed'
            receipt['error'] = str(error) or type(error).__name__

        receipt['command_items'], agent_texts, receipt['token_usage'] = \
            self._collect(receipt['thread_id'])
        receipt['final_message'] = agent_texts[-1] if agent_texts else None
        receipt['notification_counts'] = dict(Counter(
            frame.get('method') for frame in self.client.notifications
            if isinstance(frame.get('method'), str)))
        receipt['approvals'] = bridge.decisions
        receipt['evidence']['thread_read'] = await self._dump_thread_read(
            receipt['thread_id'], evidence_dir / THREAD_READ_FILENAME)
        if receipt['final_message'] is None:
            receipt['final_message'] = self._final_from_thread_read(
                evidence_dir / THREAD_READ_FILENAME)
        receipt['duration_s'] = round(time.monotonic() - started, 3)
        (evidence_dir / RECEIPT_FILENAME).write_text(
            json.dumps(redact(receipt), indent=2, sort_keys=True, ensure_ascii=False) + '\n',
            encoding='utf-8')
        return receipt

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

    async def _dump_thread_read(self, thread_id, path):
        """One thread/read (v2/ThreadReadParams.json: {threadId, includeTurns}) as evidence."""
        if not thread_id or self.client.failure or self.client.closing:
            return {'error': 'no live thread to read'}
        try:
            result = await asyncio.wait_for(self.client.request(
                'thread/read', {'threadId': thread_id, 'includeTurns': True}), 30)
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
        await client.start()
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
            timeout_s=args.timeout_s)
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
