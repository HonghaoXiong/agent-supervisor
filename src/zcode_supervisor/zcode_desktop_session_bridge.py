"""Contract-bound delivery through the ZCode GUI's native extension API.

The extension uses the desktop's existing permission client. This bridge
does not resolve approvals or change permission modes. Admission is not a
delivery receipt: the persisted message and its terminal turn prove that.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import socket
import stat
import time

from .contract_compile import verify_against_authorization
from .session_binding import session_status
from .work_contracts import canonical_sha256
from .work_models import WorkError
from .zcode_cli_bridge import ZCodeCliBridge
from .native_channels import DEFAULT_ROOT, resolve_channel, private_directory, atomic_json, read_private_json


class ZCodeDesktopSessionBridge(ZCodeCliBridge):
    def __init__(self, *, control_config=None, db_path=None, timeout_s=900,
                 poll_s=0.5, channel_id=None, channels_root=None, owner_thread_id=None):
        super().__init__(db_path=db_path, timeout_s=timeout_s)
        if channel_id is not None and control_config is not None:
            raise WorkError('channel-config-conflict','Select a channel or an explicit control config, not both')
        self.channel_id = channel_id
        self.owner_thread_id = owner_thread_id or os.environ.get('CODEX_THREAD_ID')
        self.control_config = (resolve_channel(Path(channels_root or DEFAULT_ROOT), channel_id)
            if channel_id is not None else Path(control_config or DEFAULT_ROOT/'bridge.json').expanduser())
        self.poll_s = poll_s

    def _config(self):
        try:
            info = self.control_config.lstat()
            if (not stat.S_ISREG(info.st_mode) or info.st_uid != os.getuid()
                    or info.st_mode & 0o077 or info.st_nlink != 1):
                raise WorkError('native-control-unsafe',
                                'The control config must be a private regular file')
            config = json.loads(self.control_config.read_text())
            if not isinstance(config, dict) or any(not isinstance(config.get(k), str)
                    or not config[k] for k in ('token', 'socket', 'sessionId', 'workspace')):
                raise WorkError('native-control-unsafe', 'Incomplete native control config')
            if not Path(config['socket']).is_absolute():
                raise WorkError('native-control-unsafe', 'Socket path must be absolute')
            if config.get('channelId'):
                if config.get('ownerThreadId') != self.owner_thread_id or not self.owner_thread_id:
                    raise WorkError('channel-owner-mismatch','This channel belongs to a different Codex chat')
                if self.channel_id is not None and config['channelId'] != self.channel_id:
                    raise WorkError('channel-config-conflict','Selected channel does not match its control config')
                if not isinstance(config.get('receiptDir'),str) or not Path(config['receiptDir']).is_absolute():
                    raise WorkError('native-control-unsafe','Named channels need their own receipt directory')
            return config
        except WorkError:
            raise
        except (OSError, ValueError) as exc:
            raise WorkError('native-desktop-unavailable',
                            'The native desktop extension is not configured') from exc

    def _request(self, config, request, timeout):
        try:
            info = Path(config['socket']).lstat()
            parent = Path(config['socket']).parent.lstat()
            if (not stat.S_ISSOCK(info.st_mode) or info.st_uid != os.getuid()
                    or info.st_mode & 0o077 or not stat.S_ISDIR(parent.st_mode)
                    or parent.st_uid != os.getuid() or parent.st_mode & 0o077):
                raise WorkError('native-control-unsafe', 'The native socket is not private')
            with socket.socket(socket.AF_UNIX) as client:
                client.settimeout(min(timeout, 30))
                client.connect(config['socket'])
                client.sendall((json.dumps({**request, 'token': config['token']},
                                          ensure_ascii=False)+'\n').encode())
                data = b''
                while b'\n' not in data:
                    chunk = client.recv(65536)
                    if not chunk:
                        raise WorkError('native-delivery-unconfirmed',
                                        'The native channel closed without admission evidence')
                    data += chunk
                    if len(data) > 2*1024*1024:
                        raise WorkError('native-delivery-unconfirmed',
                                        'The native admission response exceeds its bound')
                result = json.loads(data.split(b'\n', 1)[0])
                if not isinstance(result, dict):
                    raise ValueError('invalid response')
                return result
        except WorkError:
            raise
        except (OSError, ValueError) as exc:
            raise WorkError('native-desktop-unavailable',
                            'The native desktop extension is unavailable; reload extensions') from exc

    @staticmethod
    def _check_identity(workspace, expected_identity):
        current = os.stat(workspace)
        actual = (current.st_dev, current.st_ino)
        if isinstance(expected_identity, dict):
            expected_identity = (expected_identity.get('dev'), expected_identity.get('ino'))
        if expected_identity is not None and tuple(expected_identity) != actual:
            raise WorkError('workspace-replaced', 'The approved workspace identity changed')
        return actual

    def _claim_intent(self, session_id, delivery_id, claimed, prompt, workspace, binding=None):
        directory = self.control_config.parent/'client-intents'
        directory.mkdir(mode=0o700, exist_ok=True)
        info = directory.lstat()
        if not stat.S_ISDIR(info.st_mode) or info.st_uid != os.getuid() or info.st_mode & 0o077:
            raise WorkError('native-control-unsafe', 'The client intent directory must be private')
        key = hashlib.sha256((session_id+'\n'+delivery_id).encode()).hexdigest()
        target = directory/(key+'.json')
        intent = {'session_id':session_id,'delivery_id':delivery_id,
                  'contract_sha256':claimed,'body_sha256':hashlib.sha256(prompt.encode()).hexdigest(),
                  'workspace':workspace}
        if binding and binding.get('channelId'):
            intent.update(channel_id=binding['channelId'],owner_thread_id=binding['ownerThreadId'])
        try:
            with target.open('x') as handle:
                os.chmod(target, 0o600)
                json.dump(intent, handle)
                handle.flush()
                os.fsync(handle.fileno())
            descriptor = os.open(directory, os.O_RDONLY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
            return True
        except FileExistsError:
            info = target.lstat()
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_uid != os.getuid():
                raise WorkError('native-control-unsafe', 'Unsafe prior native intent')
            try:
                previous = json.loads(target.read_text())
            except (OSError, ValueError) as exc:
                raise WorkError('native-delivery-unconfirmed', 'The prior native intent is incomplete') from exc
            if previous != intent:
                raise WorkError('native-contract-mismatch', 'This delivery id already names a different contract')
            return False

    @staticmethod
    def _publish_receipt(config, result):
        if not config.get('channelId'):return result
        result.update(channel_id=config['channelId'],owner_thread_id=config['ownerThreadId'])
        directory = private_directory(config['receiptDir'])
        delivery_id = result['delivery_id']
        import re
        if not re.fullmatch(r'[A-Za-z0-9_-]{1,100}',delivery_id):
            raise WorkError('schema-invalid','Use a bounded delivery id without path separators')
        target = directory/(delivery_id+'.json')
        result['receipt_path'] = str(target)
        try:
            atomic_json(target,result,replace_existing=False)
        except FileExistsError:
            # Completion and recovery can publish concurrently. Preserve the first
            # full receipt rather than replacing its original admission evidence.
            for attempt in range(10):
                try:
                    old=read_private_json(target);break
                except WorkError:
                    if target.lstat().st_nlink!=2 or attempt==9:raise
                    time.sleep(0.01)
            for name in ('channel_id','owner_thread_id','session_id','delivery_id','turn_id','contract_sha256'):
                if old.get(name)!=result.get(name):
                    raise WorkError('channel-receipt-conflict','An existing channel receipt names a different delivery')
        return result

    def deliver(self, session_id, text, *, delivery_id, code_root,
                contract=None, authorization=None, timeout_s=None,
                expected_identity=None, expected_model=None, attempt_id=None):
        if not isinstance(delivery_id, str) or not delivery_id:
            raise WorkError('schema-invalid', 'A native delivery needs an id')
        if not isinstance(contract, dict) or not isinstance(authorization, dict):
            raise WorkError('native-contract-missing', 'A native delivery needs its authorization and contract')
        body = {k:v for k,v in contract.items() if k != 'contract_sha256'}
        claimed = contract.get('contract_sha256')
        if not claimed or claimed != canonical_sha256(body):
            raise WorkError('native-contract-mismatch', 'Contract content does not match its digest')
        verify_against_authorization(authorization, contract)
        if contract.get('require_command'):
            raise WorkError('native-command-unenforceable',
                'The native GUI applies its own permission policy; a mandatory brokered command needs the reviewed RPC runner')
        prompt = contract.get('prompt')
        if not isinstance(prompt, str) or not prompt.strip():
            raise WorkError('native-contract-missing', 'The approved prompt is empty')
        workspace = str(Path(contract.get('workspace') or code_root).resolve())
        identity = self._check_identity(workspace, expected_identity)
        config = self._config()
        if config['sessionId'] != session_id or Path(config['workspace']).resolve() != Path(workspace):
            raise WorkError('scope-conflict', 'The desktop control binding does not match this session and workspace')
        status = session_status(session_id, self.db_path)
        if status.get('status') != 'located' or Path(status['session']['directory']).resolve() != Path(workspace):
            raise WorkError('scope-conflict', 'The native session directory does not match the approved workspace')
        # A recorded, completed attempt is recoverable without a new send.
        existing = self.find(session_id, delivery_id, expected_text=prompt)
        if existing is not None:
            key = hashlib.sha256((session_id+'\n'+delivery_id).encode()).hexdigest()
            if not (self.control_config.parent/'client-intents'/(key+'.json')).is_file():
                raise WorkError('native-delivery-unconfirmed', 'The completed message lacks its original client intent')
            self._claim_intent(session_id, delivery_id, claimed, prompt, workspace,binding=config)
            if expected_model is not None and existing.get('model') != expected_model:
                raise WorkError('native-receipt-mismatch', 'The recovered turn ran a different model')
            existing.update(contract_sha256=claimed, attempt_id=attempt_id,
                            channel='zcode-native-extension')
            return self._publish_receipt(config,existing)
        if not status.get('idle'):
            raise WorkError('session-busy', 'Wait for the current native turn to finish')
        if expected_model is not None and self._session_model(session_id).get('model') != expected_model:
            raise WorkError('native-contract-unenforceable', 'The fixed session model differs from the approved model')
        new_intent = self._claim_intent(session_id, delivery_id, claimed, prompt, workspace,binding=config)
        if not new_intent:
            raise WorkError('native-delivery-unconfirmed',
                            'A prior attempt has no proven terminal receipt; reconcile it before another send')
        limit = float(contract.get('timeout_sec') or timeout_s or self.timeout_s)
        started = time.monotonic()
        wire_prompt = prompt+'\n\n[zsc-native-delivery '+delivery_id+']'
        self._check_identity(workspace, identity)
        scope = ({'channel_id':config['channelId'],'owner_thread_id':config['ownerThreadId']}
                 if config.get('channelId') else {})
        admission = self._request(config, {'op':'send', 'session_id':session_id,
            'delivery_id':delivery_id, 'prompt':wire_prompt,
            'body_sha256':hashlib.sha256(wire_prompt.encode()).hexdigest(),
            'contract_sha256':claimed,**scope}, limit)
        if not admission.get('ok'):
            raise WorkError('native-delivery-unconfirmed',
                            'The native API did not confirm admission: '+str(admission.get('error') or admission.get('admission')))
        if any(admission.get(key)!=value for key,value in scope.items()):
            raise WorkError('channel-receipt-mismatch','Native admission belongs to another channel or owner')
        while time.monotonic()-started < limit:
            self._check_identity(workspace, identity)
            found = self._find_delivery(session_id, delivery_id, expected_text=prompt)
            if found is not None:
                state = self.turn_state(session_id, found.get('turn_id'))
                if state.get('failed'):
                    raise WorkError('native-delivery-unconfirmed', 'The native turn ended with an error')
                if state.get('completed'):
                    if expected_model is not None and state.get('model') != expected_model:
                        raise WorkError('native-receipt-mismatch', 'The native turn ran a different model')
                    receipt = self._receipt(found, session_id, delivery_id,
                        time.monotonic()-started, 'delivered',
                        contract_sha256=claimed, model=state.get('model'), attempt_id=attempt_id)
                    receipt.update(channel='zcode-native-extension', native_admission=admission)
                    return self._publish_receipt(config,receipt)
            time.sleep(min(self.poll_s, max(0.001, limit-(time.monotonic()-started))))
        raise WorkError('native-delivery-unconfirmed',
                        'No successful terminal native turn was proven before the contract deadline; do not resend blindly')

    def find(self, session_id, delivery_id, **kwargs):
        result = super().find(session_id, delivery_id, **kwargs)
        if result is not None:
            result['channel'] = 'zcode-native-extension'
        return result
