#!/usr/bin/env python3
"""P0.0 fake app-server child: the fixed fake/replay fixture endpoint.

Speaks the NDJSON control plane used by both the ZCode client
(``protocol.ProtocolClient``: ``session/create``/``subscribe``/``send``/...) and
the Codex client (``codex_appserver.CodexAppServerClient`` /
``CodexAppServerDriver``: ``initialize``, ``thread/start`` with the production
``result.thread.id`` shape, ``turn/start`` -> ``result.turn.id``,
``turn/completed`` / ``item/completed`` / ``thread/tokenUsage/updated``
notifications, one ``item/commandExecution/requestApproval`` reverse request,
``thread/read``). It NEVER calls a model, opens the network, or reads any real
session store: ``model_requests`` counts calls into ``_invoke_model`` (there
are none) and is reported through ``bench/counters``. Fault modes for the
communication-fault fixtures (tests/fixtures/ui/comm-faults-v1.json):
``delay-reply`` (every reply delayed), ``delay-first`` (first reply delayed),
``delay-turn`` (turn events delayed), ``never-complete`` (turn started but
completion withheld; the ZCode variant streams intermediate session/event
frames so a mid-cancel leaves a partially processed stream),
``block-drain`` (the child stops reading stdin for ``delay_s`` seconds, so a
large client write genuinely blocks in ``drain()`` on OS pipe backpressure),
``silent-initialize`` (initialize is never answered, to prove caller-side
bounds). ``bench/set-fault`` also accepts ``reply_delay_s`` to delay that one
reply. Every reply carries a ``monotonic_ns`` mark read from CLOCK_MONOTONIC
so the bench can prove same-source clock probes across processes;
``wall_ns`` values are display-only stand-ins for ``observed_at``.
"""
import argparse
import json
import sys
import threading
import time

FAULT_MODES = ('delay-reply', 'delay-first', 'delay-turn', 'never-complete',
               'block-drain', 'silent-initialize')

_state = {
    'fault': 'none',
    'delay': 0.0,
    'seq': 0,
    'sessions': 0,
    'threads': 0,
    'server_requests': 0,
    'replies': 0,
}
_counters = {
    'model_requests': 0, 'session_creates': 0, 'session_resumes': 0,
    'session_subscribes': 0, 'session_sends': 0, 'session_reads': 0,
    'session_stops': 0, 'set_models': 0, 'set_modes': 0, 'initializes': 0,
    'thread_starts': 0, 'turn_starts': 0, 'turn_interrupts': 0,
    'thread_reads': 0, 'clock_probes': 0, 'faults_armed': 0,
    'turns_started': 0, 'turns_completed': 0, 'approvals_requested': 0,
    'notifications_emitted': 0, 'reverse_requests_sent': 0,
}
_threads = {}  # threadId -> {'turns': [turnId, ...], 'items': [...]}
_out_lock = threading.Lock()


def _monotonic_ns():
    return time.clock_gettime_ns(time.CLOCK_MONOTONIC)


def _invoke_model(prompt):
    """The only place a model call could start; nothing ever calls this."""
    _counters['model_requests'] += 1
    raise AssertionError('the fake fixture never invokes a model')


def _write(frame):
    line = json.dumps(frame, ensure_ascii=True) + '\n'
    with _out_lock:
        sys.stdout.write(line)
        sys.stdout.flush()


def _reply(request_id, result):
    _state['replies'] += 1
    result = dict(result)
    result['monotonic_ns'] = _monotonic_ns()
    _write({'id': request_id, 'result': result})


def _next_seq():
    _state['seq'] += 1
    return _state['seq']


def _emit_event(session_id, event_type, payload):
    _write({'method': 'session/event', 'params': {
        'type': event_type, 'sessionId': session_id,
        'turnId': payload.get('turnId', ''), 'seq': _next_seq(),
        'payload': payload}})


def _notify(method, params):
    _counters['notifications_emitted'] += 1
    _write({'method': method, 'params': params})


def _server_request(method, params):
    _state['server_requests'] += 1
    _counters['reverse_requests_sent'] += 1
    _write({'id': 'srvreq-%d' % _state['server_requests'],
            'method': method, 'params': params})


def _snapshot(session_id):
    return {'session': {'sessionId': session_id, 'status': 'idle'},
            'runtime': {'pendingRequestIds': []},
            'settings': {'model': {'current': None}}}


def _zcode_turn(session_id, input_id):
    mode = _state['fault']
    turn = 'turn_%s_%d' % (session_id, _counters['turns_started'] + 1)
    _counters['turns_started'] += 1
    _emit_event(session_id, 'turn.started',
                {'inputId': input_id, 'turnId': turn})
    if mode == 'delay-turn':
        time.sleep(_state['delay'])
    if mode == 'never-complete':
        # Intermediate stream frames so a mid-cancel leaves a partially
        # processed drain; completion is withheld entirely.
        for index in range(10):
            time.sleep(0.04)
            _emit_event(session_id, 'session.updated',
                        {'turnId': turn, 'update': 'fake-progress-%02d' % index})
        return
    time.sleep(0.005)
    _counters['turns_completed'] += 1
    _emit_event(session_id, 'turn.completed',
                {'turnId': turn, 'resultType': 'success'})


def _codex_turn(thread_id):
    mode = _state['fault']
    turn = 'turn_%s_%d' % (thread_id, _counters['turns_started'] + 1)
    _counters['turns_started'] += 1
    if mode == 'delay-turn':
        time.sleep(_state['delay'])
    # One production-shaped approval round trip (command approval, v2).
    _counters['approvals_requested'] += 1
    _server_request('item/commandExecution/requestApproval', {
        'threadId': thread_id, 'turnId': turn,
        'itemId': 'item-%s' % turn, 'callId': 'call-%s' % turn,
        'cwd': '/fake/workspace', 'command': 'printf fake',
        'availableDecisions': ['accept', 'deny']})
    time.sleep(0.02)
    record = {'threadId': thread_id, 'item': {
        'type': 'commandExecution', 'command': 'printf fake',
        'exitCode': 0, 'status': 'completed'}}
    _threads[thread_id]['items'].append(record['item'])
    _notify('item/completed', record)
    agent = {'threadId': thread_id, 'item': {
        'type': 'agentMessage', 'text': 'fake reply'}}
    _threads[thread_id]['items'].append(agent['item'])
    _notify('item/completed', agent)
    _notify('thread/tokenUsage/updated', {
        'threadId': thread_id,
        'tokenUsage': {'inputTokens': 10, 'outputTokens': 5, 'totalTokens': 15}})
    if mode == 'never-complete':
        return  # items delivered, terminal turn/completed withheld
    time.sleep(0.005)
    _counters['turns_completed'] += 1
    _notify('turn/completed', {'threadId': thread_id,
                               'turn': {'id': turn, 'status': 'completed'}})


def _handle(frame):
    method = frame.get('method')
    request_id = frame.get('id')
    params = frame.get('params') or {}
    if not isinstance(method, str):
        return
    if _state['fault'] == 'delay-reply':
        time.sleep(_state['delay'])
    elif _state['fault'] == 'delay-first' and _state['replies'] == 0:
        time.sleep(_state['delay'])
    if method == 'bench/counters':
        _reply(request_id, _counters)
    elif method == 'bench/set-fault':
        mode = params.get('mode', 'none')
        if mode != 'none' and mode not in FAULT_MODES:
            _write({'id': request_id, 'error': {
                'code': -32602, 'message': 'unknown fault mode: %s' % mode}})
            return
        _state['fault'] = mode
        _state['delay'] = float(params.get('delay_s', 0.0) or 0.0)
        _counters['faults_armed'] += 1
        reply_delay = float(params.get('reply_delay_s', 0.0) or 0.0)
        if reply_delay > 0:
            time.sleep(reply_delay)  # delays only THIS reply (reset-meter test)
        _reply(request_id, {'mode': mode, 'delay_s': _state['delay']})
    elif method == 'bench/drain-probe':
        return  # consumed, never answered: the blocked-drain gate fixture
    elif method == 'bench/clock-probe':
        _counters['clock_probes'] += 1
        _reply(request_id, {'monotonic_ns': _monotonic_ns(),
                            'wall_ns': time.time_ns()})
    elif method == 'initialize':
        if _state['fault'] == 'silent-initialize':
            return  # accepted, never answered: proves the caller-side bound
        _counters['initializes'] += 1
        _reply(request_id, {'codexHome': '/fake/codex-home',
                            'platformFamily': 'fake',
                            'platformOs': 'fake',
                            'userAgent': 'czs-fake-server/1'})
    elif method == 'session/create':
        _counters['session_creates'] += 1
        _state['sessions'] += 1
        _reply(request_id, _snapshot('sess_fake_%d' % _state['sessions']))
    elif method == 'session/resume':
        _counters['session_resumes'] += 1
        _reply(request_id, _snapshot(str(params.get('sessionId'))))
    elif method == 'session/setMode':
        _counters['set_modes'] += 1
        _reply(request_id, {})
    elif method == 'session/setModel':
        _counters['set_models'] += 1
        _reply(request_id, {})
    elif method == 'session/subscribe':
        _counters['session_subscribes'] += 1
        _reply(request_id, {'eventSeq': _state['seq']})
    elif method == 'session/read':
        _counters['session_reads'] += 1
        _reply(request_id, _snapshot(str(params.get('sessionId'))))
    elif method == 'session/send':
        _counters['session_sends'] += 1
        thread = threading.Thread(
            target=_zcode_turn, args=(str(params.get('sessionId')),
                                      str(params.get('inputId'))), daemon=True)
        thread.start()
        _reply(request_id, {})
    elif method == 'session/stop':
        _counters['session_stops'] += 1
        _reply(request_id, {})
    elif method == 'thread/start':
        _counters['thread_starts'] += 1
        _state['threads'] += 1
        thread_id = 'th_fake_%d' % _state['threads']
        _threads[thread_id] = {'turns': [], 'items': []}
        # v2/ThreadStartResponse.json shape: {thread: {id, ...}}.
        _reply(request_id, {'thread': {'id': thread_id, 'status': 'idle'}})
    elif method == 'turn/start':
        _counters['turn_starts'] += 1
        thread_id = str(params.get('threadId'))
        turn = 'turn_%s_%d' % (thread_id, len(_threads.get(thread_id,
                                                           {'turns': []})['turns']) + 1)
        _threads.setdefault(thread_id, {'turns': [], 'items': []})
        _threads[thread_id]['turns'].append(turn)
        _reply(request_id, {'turn': {'id': turn, 'status': 'inProgress'}})
        threading.Thread(target=_codex_turn, args=(thread_id,),
                         daemon=True).start()
    elif method == 'turn/interrupt':
        _counters['turn_interrupts'] += 1
        _reply(request_id, {})
    elif method == 'thread/read':
        _counters['thread_reads'] += 1
        thread_id = str(params.get('threadId'))
        record = _threads.get(thread_id, {'turns': [], 'items': []})
        _reply(request_id, {'thread': {
            'id': thread_id, 'status': 'idle',
            'turns': [{'id': turn, 'status': 'completed', 'items': record['items']}
                      for turn in record['turns']]}})
    else:
        _write({'id': request_id, 'error': {
            'code': -32601,
            'message': 'fake server: unsupported method %s' % method}})


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument('--role', default='zcode', choices=('zcode', 'codex'))
    parser.add_argument('--fault', default='none',
                        choices=('none',) + FAULT_MODES)
    parser.add_argument('--fault-delay', type=float, default=0.0)
    parser.add_argument('--list-faults', action='store_true')
    parser.add_argument('rest', nargs='*',
                        help='ignored app-server positional arguments')
    args, _ignored = parser.parse_known_args()
    if args.list_faults:
        print(json.dumps({'fault_modes': list(FAULT_MODES)}))
        return 0
    _state['fault'] = args.fault if args.fault in FAULT_MODES else 'none'
    _state['delay'] = args.fault_delay
    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            frame = json.loads(line)
        except ValueError:
            continue
        if not isinstance(frame, dict) or 'method' not in frame:
            continue  # response to a server-initiated request: nothing to do
        try:
            _handle(frame)
        except BrokenPipeError:
            return 0
        if _state['fault'] == 'block-drain':
            # Real drain gate: stop consuming stdin for delay_s seconds. The
            # OS pipe fills and a large client write blocks inside drain().
            _state['fault'] = 'none'
            time.sleep(_state['delay'])
    return 0


if __name__ == '__main__':
    try:
        sys.exit(main())
    except BrokenPipeError:
        sys.exit(0)
