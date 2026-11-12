"""Private, append-only registration of independently owned native channels."""
from __future__ import annotations

from contextlib import contextmanager
import fcntl
import json
import os
from pathlib import Path
import re
import secrets
import stat
import tempfile
import time
import uuid

from .work_models import WorkError

SCHEMA = 'zsc.native-channels/v1'
DEFAULT_ROOT = Path.home()/'.czs/native-gui'


def private_directory(path):
    path = Path(path).absolute()
    path.mkdir(parents=True, mode=0o700, exist_ok=True)
    info = path.lstat()
    if (not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid()
            or info.st_mode & 0o077):
        raise WorkError('channel-config-unsafe', 'Channel directory must be private and owned')
    return path


def read_private_json(path):
    path = Path(path)
    info = path.lstat()
    if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
            or info.st_nlink != 1 or info.st_mode & 0o077):
        raise WorkError('channel-config-unsafe', 'Channel files must be private regular files')
    try:
        result = json.loads(path.read_text())
    except (ValueError, UnicodeError) as exc:
        raise WorkError('channel-config-invalid', 'Invalid channel JSON') from exc
    if not isinstance(result, dict):
        raise WorkError('channel-config-invalid', 'Channel JSON must be an object')
    return result


def atomic_json(path, document, *, replace_existing=True):
    path = Path(path)
    descriptor, temporary = tempfile.mkstemp(prefix='.'+path.name+'-', dir=path.parent)
    try:
        with os.fdopen(descriptor, 'w') as handle:
            json.dump(document, handle, ensure_ascii=False, indent=2)
            handle.write('\n');handle.flush();os.fsync(handle.fileno())
        if replace_existing:
            os.replace(temporary, path)
        else:
            os.link(temporary,path)
            os.unlink(temporary)
        directory = os.open(path.parent, os.O_RDONLY)
        try:os.fsync(directory)
        finally:os.close(directory)
    finally:
        if os.path.exists(temporary):os.unlink(temporary)


@contextmanager
def _lock(path, timeout_s=10):
    descriptor = os.open(path, os.O_CREAT|os.O_RDWR|os.O_NOFOLLOW, 0o600)
    try:
        info = os.fstat(descriptor)
        if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                or info.st_nlink != 1 or info.st_mode & 0o077):
            raise WorkError('channel-config-unsafe', 'Unsafe channel lock')
        deadline = time.monotonic()+timeout_s
        while True:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX|fcntl.LOCK_NB);break
            except BlockingIOError:
                if time.monotonic() >= deadline:
                    raise WorkError('channel-lock-busy', 'Another channel mutation holds the lock')
                time.sleep(min(0.02,max(0.001,deadline-time.monotonic())))
        try:yield
        finally:fcntl.flock(descriptor,fcntl.LOCK_UN)
    finally:os.close(descriptor)


def _id(value, allow_legacy=False):
    if (not isinstance(value, str) or not re.fullmatch(r'[a-z0-9][a-z0-9_-]{0,47}', value)
            or value == 'legacy' and not allow_legacy):
        raise WorkError('channel-id-invalid', 'Use a unique lowercase channel id, not legacy')
    return value


def _owner(value):
    try:
        if str(uuid.UUID(value)) != value:raise ValueError()
    except (ValueError, TypeError, AttributeError) as exc:
        raise WorkError('channel-owner-invalid', 'A channel needs its Codex owner thread id') from exc
    return value


def _registry(root):
    path = Path(root)/'channels.json'
    if not path.exists():return {'schema_version':SCHEMA,'revision':0,'channels':{}}
    data = read_private_json(path)
    if (set(data) != {'schema_version','revision','channels'} or data['schema_version'] != SCHEMA
            or type(data['revision']) is not int or data['revision'] < 0
            or not isinstance(data['channels'], dict)):
        raise WorkError('channel-registry-invalid', 'Unsupported or malformed channel registry')
    return data


def _config(path, channel_id=None):
    data = read_private_json(path)
    required = ('token','sessionId','workspace','socket','stateDir')
    if any(not isinstance(data.get(key),str) or not data[key] for key in required):
        raise WorkError('channel-config-invalid', 'Incomplete channel binding')
    if channel_id is not None:
        if data.get('channelId') != channel_id:
            raise WorkError('channel-config-invalid', 'Channel name does not match its config')
        _owner(data.get('ownerThreadId'))
        if not isinstance(data.get('receiptDir'),str):
            raise WorkError('channel-config-invalid', 'A channel needs its receipt directory')
    for key in ('workspace','socket','stateDir','receiptDir'):
        if key in data and not Path(data[key]).is_absolute():
            raise WorkError('channel-config-invalid', 'Channel paths must be absolute')
    return data


def resolve_channel(root, channel_id):
    root = Path(root)
    _id(channel_id, allow_legacy=True)
    if channel_id == 'legacy':
        _config(root/'bridge.json');return root/'bridge.json'
    registry = _registry(root)
    value = registry['channels'].get(channel_id)
    if not isinstance(value,str) or not Path(value).is_absolute():
        raise WorkError('channel-not-found', 'No registered channel with this name')
    path = Path(value)
    expected = root/'channels'/channel_id/'bridge.json'
    if path != expected:
        raise WorkError('channel-config-invalid', 'Channel config is outside its registered directory')
    _config(path, channel_id)
    return path


def _overlaps(left,right):
    left,right = Path(left).resolve(),Path(right).resolve()
    return left == right or left in right.parents or right in left.parents


def _check_conflicts(candidate, bindings):
    routes = [candidate[k] for k in ('socket','stateDir','receiptDir')]
    for i,left in enumerate(routes):
        if any(_overlaps(left,right) for right in routes[i+1:]):
            raise WorkError('channel-path-conflict', 'Channel output roots must be independent')
    for old in bindings:
        if candidate['sessionId'] == old['sessionId']:
            raise WorkError('channel-session-conflict', 'This ZCode session already has a channel')
        for left in routes:
            if any(_overlaps(left,old[key]) for key in ('socket','stateDir','receiptDir') if old.get(key)):
                raise WorkError('channel-path-conflict', 'Channel paths overlap an existing channel')


def register_channel(root, channel_id, *, session_id, workspace, owner_thread_id,
                     socket_path=None, receipt_dir=None):
    _id(channel_id);_owner(owner_thread_id)
    if not isinstance(session_id,str) or not re.fullmatch(r'sess_[A-Za-z0-9_-]+',session_id):
        raise WorkError('channel-session-invalid','A channel needs a ZCode session id')
    workspace = Path(workspace).resolve(strict=True)
    if not workspace.is_dir():raise WorkError('channel-workspace-invalid','Workspace must be a directory')
    root = private_directory(root)
    channel_root = root/'channels'/channel_id
    endpoint = (Path(socket_path).absolute() if socket_path is not None else
                Path('/private/tmp')/f'zsc-native-gui-{os.getuid()}'/(channel_id+'.sock'))
    if len(os.fsencode(str(endpoint))) >= 104:
        raise WorkError('channel-socket-invalid','Use a shorter Unix socket path')
    desired = {'channelId':channel_id,'ownerThreadId':owner_thread_id,'sessionId':session_id,
        'workspace':str(workspace),'socket':str(endpoint),'stateDir':str(channel_root/'deliveries'),
        'receiptDir':str(Path(receipt_dir).absolute() if receipt_dir is not None else channel_root/'receipts')}
    with _lock(root/'registry.lock'):
        state = _registry(root)
        if channel_id in state['channels']:
            existing = _config(resolve_channel(root,channel_id),channel_id)
            if any(existing.get(k) != v for k,v in desired.items()):
                raise WorkError('channel-binding-conflict','An existing channel cannot be repointed')
            return existing
        existing=[]
        if (root/'bridge.json').exists():existing.append(_config(root/'bridge.json'))
        for name in state['channels']:
            existing.append(_config(resolve_channel(root,name),name))
        _check_conflicts(desired,existing)
        private_directory(root/'channels');private_directory(channel_root)
        private_directory(endpoint.parent);private_directory(desired['stateDir']);private_directory(desired['receiptDir'])
        config_path=channel_root/'bridge.json'
        if config_path.exists():
            config=_config(config_path,channel_id)
            if any(config.get(k)!=v for k,v in desired.items()):
                raise WorkError('channel-binding-conflict','A prior incomplete registration has different terms')
        else:
            config={**desired,'token':secrets.token_hex(32)}
            atomic_json(config_path,config)
        state['channels'][channel_id]=str(config_path);state['revision']+=1
        atomic_json(root/'channels.json',state)
        return config


def list_channels(root=DEFAULT_ROOT):
    root=Path(root);result=[]
    entries=list(_registry(root)['channels'].items())
    if (root/'bridge.json').exists():entries.insert(0,('legacy',str(root/'bridge.json')))
    for name,_ in entries:
        try:
            path=resolve_channel(root,name);config=_config(path,None if name=='legacy' else name)
            result.append({'channel_id':name,'session_id':config['sessionId'],
                'owner_thread_id':config.get('ownerThreadId'),'workspace':config['workspace'],
                'socket':config['socket'],'receipt_dir':config.get('receiptDir'),
                'state_dir':config['stateDir'],'control_config':str(path)})
        except (WorkError,OSError):result.append({'channel_id':name,'status':'invalid'})
    return result


@contextmanager
def installation_guard(root, owner_thread_id, timeout_s=10):
    root=private_directory(root);_owner(owner_thread_id)
    with _lock(root/'plugin-install.lock',timeout_s):
        path=root/'maintainer.json'
        if path.exists():
            if read_private_json(path).get('ownerThreadId')!=owner_thread_id:
                raise WorkError('plugin-maintainer-mismatch','Another Codex chat owns plugin installation')
        else:atomic_json(path,{'ownerThreadId':owner_thread_id,'scope':'native-plugin-installation'})
        yield
