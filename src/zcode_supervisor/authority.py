#!/usr/bin/env python3
"""Worker-agnostic authority layer: packet-brokered permissions and evidence.

P0-3 extraction: the Authority class and its module-level helpers moved here
verbatim from protocol.py (no logic, string, or signature changes) so worker
drivers — the ZCode app-server today, Codex approval callbacks in P1 — share
one policy core. protocol.py re-exports every moved name; new code must
import from this module, not protocol.
"""
import contextlib
import hashlib
import math
import re
import stat
from pathlib import Path
from typing import Protocol, runtime_checkable

from .common import read_regular, sha256_regular


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
            'sha256': sha256_regular(path),
            'mode': stat.S_IMODE(info.st_mode)}
    return result


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
    def __init__(self, packet, packet_path, before, ui_emit=None):
        self.packet = packet
        self.packet_path = packet_path
        self.packet_hash = sha256_regular(packet_path)
        self.workspace = Path(packet['workspace']).resolve()
        self.before = before
        self.allowed = set(packet['allowed_files'])
        # P0.4 read-only decision projection hook (sideband): called at the
        # decision point with a minimal safe record (ids, bindings, decision)
        # — never with tool input or reason text. The callable must never
        # raise (ui_projection.authority_hook guarantees it); a None hook (the
        # default, e.g. the frozen cold protocol path) changes nothing.
        self.ui_emit = ui_emit
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
        # P2.1 one-shot boundary-scan hand-over slot (see provide_scan)
        self._handed_scan = None

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
                                sha256_regular(entry))
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

    def provide_scan(self, current):
        """Hand a JUST-awaited boundary inventory to the next
        [`check_frozen`](Self.check_frozen) call (P2.1).

        The async run paths compute the workspace scan on the bounded
        boundary-scan executor and hand it over immediately before the
        (synchronous) check — no await sits between hand-over and
        consumption, so no other code path can pick it up. The hand-over
        is one-shot: consumed by the very next check_frozen, or dropped
        if that call fails its packet re-hash. ``decide`` never hands
        anything over and always gets a fresh synchronous scan.
        """
        self._handed_scan = current

    def check_frozen(self):
        """Verify packet, workspace inputs and trusted executables.

        Signature unchanged (external instrumentation subclasses this
        exact surface). When the async run path handed over a scan, its
        result is this check's own inventory — merely computed off-loop;
        the packet/trusted-executable re-hashes and every bookkeeping
        check below stay fresh in this call.
        """
        if sha256_regular(self.packet_path) != self.packet_hash:
            self._handed_scan = None  # a handed scan never outlives this
            raise ValueError('Supervisor packet changed')
        current = self._handed_scan
        self._handed_scan = None
        if current is None:
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
                    current = (sha256_regular(entry)
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
                    or sha256_regular(path) != digest):
                raise ValueError('Trusted executable/input changed: '+name)

    def decide(self, params):
        decision, reason, command_id = 'deny', 'Not in this task contract', None
        if self.ui_emit is not None:
            # P0.4 sideband: the request arrival observation (read-only; the
            # authoritative transition below is untouched whatever happens).
            # Session/turn identity is the authority's OWN binding, never the
            # request's claim of it.
            with contextlib.suppress(Exception):
                self.ui_emit({'phase': 'requested',
                              'request_id': params.get('requestId')
                              if isinstance(params, dict) else None,
                              'tool_call_id': params.get('toolCallId')
                              if isinstance(params, dict) else None,
                              'tool_name': params.get('toolName')
                              if isinstance(params, dict) else None,
                              'session_id': self.session_id,
                              'turn_id': self.turn_id})
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
        if self.ui_emit is not None:
            # P0.4 sideband: the decision record above is the authoritative
            # state; this projection copies only its safe ids and outcome
            # (session/turn identity comes from the authority's OWN bindings,
            # not from the request's claims).
            with contextlib.suppress(Exception):
                self.ui_emit({'phase': 'decided',
                              'request_id': params.get('requestId')
                              if isinstance(params, dict) else None,
                              'tool_call_id': params.get('toolCallId')
                              if isinstance(params, dict) else None,
                              'tool_name': params.get('toolName')
                              if isinstance(params, dict) else None,
                              'session_id': self.session_id,
                              'turn_id': self.turn_id,
                              'command_id': command_id,
                              'decision': decision})
        return {'decision':decision, 'reason':reason}

    def audit(self, events):
        """Broker-facing evidence hook; delegates to execution_evidence."""
        return execution_evidence(events, self)

    def verify_model(self, snapshot, requested):
        """Broker-facing model check; delegates to check_model_snapshot."""
        return check_model_snapshot(snapshot, requested)


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


@runtime_checkable
class PermissionBroker(Protocol):
    """Duck-typed broker contract shared by every worker driver (P0-3).

    decide() answers one permission request; audit() and verify_model() are
    the post-run evidence hooks. Implementations stay worker-agnostic:
    shaping a decision for a specific wire protocol belongs to the
    to_*_response mappers below, never to the broker itself.
    """

    def decide(self, params: dict) -> dict:
        """Answer one request: {'decision': 'allow'|'deny', 'reason': str}."""
        ...

    def audit(self, events) -> tuple[list, list]:
        """Post-run execution evidence: (commands, problems)."""
        ...

    def verify_model(self, snapshot, requested: dict) -> dict:
        """Return the observed model selection, or raise on any mismatch."""
        ...


# Worker approval-callback response shapes, kept as a commented constant
# table. Sources: exported app-server schemas — the ZCode
# interaction/requestPermission answer, the Codex v1 ReviewDecision enum
# behind execCommandApproval/applyPatchApproval (8 variants), and the Codex
# v2 item/<name>/requestApproval decision enum — all pending P1 live
# validation. Policy: persistent authorization is disabled, so only
# per-request allow/deny forms are ever produced; every persistent or
# policy-amendment variant in PERSISTENT_VARIANTS is refused, never
# negotiated and never emitted as a decision value.
PERSISTENT_VARIANTS = frozenset({
    'approved_for_session',           # Codex v1: session-scoped command approval
    'approved_mcp_policy_amendment',  # Codex v1: persistent MCP policy rewrite
    'approved_execpolicy_amendment',  # Codex v1: persistent exec policy rewrite
    'network_policy_amendment',       # Codex v1: persistent network policy rewrite
    'acceptForSession',               # Codex v2: session-scoped approval
    'acceptWithExecpolicyAmendment',  # Codex v2: persistent exec policy rewrite
    'applyNetworkPolicyAmendment',    # Codex v2: persistent network policy rewrite
    'escalate',                       # ZCode callback escalation variant
    'modify',                         # ZCode callback modify variant
    'permissionUpdates',              # ZCode callback persistent-rule push
    'addRules',                       # ZCode permissionUpdates rule payload
})


def reject_persistent_request(requested_variant, decision):
    """Persistent authorization is disabled policy: a request that names (or
    implies) a persistent/policy-amendment variant is downgraded to a
    per-request deny carrying an explicit alert, whatever the underlying
    evaluation said. Anything else passes through untouched."""
    if requested_variant in PERSISTENT_VARIANTS:
        return {'decision': 'deny',
                'reason': decision.get('reason'),
                'alert': 'persistent authorization refused: ' + str(requested_variant)}
    return decision


def to_zcode_response(decision):
    """ZCode interaction/requestPermission: pass decision and reason through."""
    decision = reject_persistent_request(decision.get('requested_variant'), decision)
    response = {'decision': decision.get('decision'), 'reason': decision.get('reason')}
    if response['decision'] not in ('allow', 'deny'):
        response['decision'] = 'deny'
    if 'alert' in decision:
        response['alert'] = decision['alert']
    return response


def to_codex_v1_response(decision):
    """Codex v1 approval: allow -> "approved"; deny -> {"denied": {...}}."""
    decision = reject_persistent_request(decision.get('requested_variant'), decision)
    if decision.get('decision') == 'allow':
        response = {'decision': 'approved'}
    else:
        response = {'decision': {'denied': {'rejection': decision.get('reason')}}}
    if 'alert' in decision:
        response['alert'] = decision['alert']
    return response


def to_codex_v2_response(decision):
    """Codex v2 requestApproval: allow -> "accept"; deny -> "decline"."""
    decision = reject_persistent_request(decision.get('requested_variant'), decision)
    response = {'decision': 'accept' if decision.get('decision') == 'allow' else 'decline'}
    if 'alert' in decision:
        response['alert'] = decision['alert']
    return response


def worker_response(worker, decision):
    """Shape one broker decision for a specific worker's approval callback."""
    if worker == 'zcode':
        return to_zcode_response(decision)
    if worker == 'codex-v1':
        return to_codex_v1_response(decision)
    if worker == 'codex-v2':
        return to_codex_v2_response(decision)
    raise ValueError('Unknown worker: ' + str(worker))
