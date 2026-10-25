#!/usr/bin/env python3
"""Read-only, shareable prerequisite diagnostics; never starts a model or worker."""
from __future__ import annotations

import argparse
import ast
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shlex
import shutil
import subprocess
import sys


DEFAULT_CLI = Path('/Applications/ZCode.app/Contents/Resources/glm/zcode.cjs')
CODEX_CANDIDATES = (
    '/Applications/Codex.app/Contents/Resources/codex',
    '/Applications/Codex.app/Contents/Resources/codex-cli/CodexCLI.app/Contents/MacOS/codex',
    '/Applications/ChatGPT.app/Contents/Resources/codex-cli/CodexCLI.app/Contents/MacOS/codex',
)
HELP_COMMANDS = (
    ('codex_plugin_add', ('plugin', 'add')),
    ('codex_plugin_list', ('plugin', 'list')),
    ('codex_plugin_remove', ('plugin', 'remove')),
    ('codex_marketplace_add', ('plugin', 'marketplace', 'add')),
    ('codex_marketplace_list', ('plugin', 'marketplace', 'list')),
    ('codex_marketplace_remove', ('plugin', 'marketplace', 'remove')),
)


def find_executable(explicit=None, name=None, fallbacks=()):
    """Resolve a runnable executable. An invalid explicit override never falls back."""
    candidates = [explicit] if explicit else [shutil.which(name or ''), *fallbacks]
    for candidate in candidates:
        if not candidate:
            continue
        path = Path(candidate).expanduser()
        if not path.is_absolute():
            resolved = shutil.which(str(path))
            if not resolved:
                continue
            path = Path(resolved)
        if path.is_file() and os.access(path, os.X_OK):
            return str(path.resolve())
    return None


def find_codex(explicit=None):
    return find_executable(explicit, 'codex', CODEX_CANDIDATES)


def find_node(explicit=None):
    return find_executable(explicit, 'node', ('/opt/homebrew/bin/node', '/usr/local/bin/node'))


def locate_plugin_root(script=None):
    """Recognize both the extracted release and Codex's installed plugin layout."""
    directory = Path(script or __file__).resolve().parent.parent
    candidates = (directory, directory / 'plugins/zcode-supervisor')
    for candidate in candidates:
        if (candidate / 'plugin.json').is_file() and (candidate / 'lib/zcode_supervisor').is_dir():
            return candidate
    raise ValueError('PLUGIN_RUNTIME_MISSING')


def _literal(path, name):
    """Read a packaged Python constant without importing or running the runtime."""
    tree = ast.parse(path.read_text(encoding='utf-8'))
    for node in tree.body:
        if isinstance(node, ast.Assign) and any(isinstance(t, ast.Name) and t.id == name for t in node.targets):
            return ast.literal_eval(node.value)
    raise ValueError('PACKAGED_METADATA_MISSING')


def _check(identifier, status, code='', remediation='', **details):
    return {'id': identifier, 'status': status, 'error_code': code,
            'remediation': remediation, **details}


def _probe(command):
    """Return only an exit classification and bounded version; never raw stderr."""
    try:
        result = subprocess.run(command, capture_output=True, text=True, timeout=15)
    except subprocess.TimeoutExpired:
        return 'unknown', 'COMMAND_TIMEOUT', None
    except (OSError, subprocess.SubprocessError, UnicodeError):
        return 'unsupported', 'COMMAND_UNAVAILABLE', None
    if result.returncode:
        return 'unsupported', 'COMMAND_REJECTED', None
    # A strict version token avoids copying arbitrary stdout, accounts or paths.
    match = re.fullmatch(r'(?:v|codex-cli\s+)?([0-9]+\.[0-9]+\.[0-9]+(?:[-+][0-9A-Za-z.-]+)?)', result.stdout.strip())
    return 'supported', '', match.group(1) if match else None


def inspect_environment(plugin_root: Path, codex=None, node=None, cli=None) -> dict:
    """Inspect prerequisites only. Returned JSON contains no local paths or logs."""
    root = Path(plugin_root)
    checks = []
    python_ok = sys.version_info >= (3, 11)
    checks.append(_check('python', 'supported' if python_ok else 'unsupported',
                         '' if python_ok else 'PYTHON_TOO_OLD',
                         '' if python_ok else 'Use an installed Python 3.11+ interpreter with scripts/python.sh.',
                         version='.'.join(str(v) for v in sys.version_info[:3])))
    plugin_version = core_version = 'unknown'
    reviewed = set()
    try:
        manifest = json.loads((root / 'plugin.json').read_text(encoding='utf-8'))
        plugin_version = manifest['version']
        core_version = _literal(root / 'lib/zcode_supervisor/__init__.py', '__version__')
        pins = _literal(root / 'lib/zcode_supervisor/runtime.py', 'REVIEWED_BUNDLES')
        if not isinstance(plugin_version, str) or not re.fullmatch(r'[0-9]+\.[0-9]+\.[0-9]+', plugin_version):
            raise ValueError('invalid version')
        if not isinstance(core_version, str) or not re.fullmatch(r'[0-9]+\.[0-9]+\.[0-9]+', core_version):
            raise ValueError('invalid core version')
        if not isinstance(pins, dict) or not pins or not all(re.fullmatch(r'[0-9a-f]{64}', p) for p in pins):
            raise ValueError('invalid pins')
        reviewed = set(pins)
        checks.append(_check('plugin_runtime', 'supported'))
    except (OSError, ValueError, KeyError, TypeError, SyntaxError, UnicodeError):
        plugin_version = core_version = 'unknown'
        checks.append(_check('plugin_runtime', 'unsupported', 'PLUGIN_RUNTIME_INVALID',
                             'Use a verified built release; do not run the source template.'))
    node_path = find_node(node)
    if node_path:
        status, code, version = _probe([node_path, '--version'])
        if status == 'supported' and version is None:
            status, code = 'unknown', 'NODE_VERSION_UNRECOGNIZED'
        checks.append(_check('node', status, code,
                             '' if status == 'supported' else 'Pass --node with an installed Node.js executable.',
                             version=version or 'unknown'))
    else:
        checks.append(_check('node', 'unsupported', 'NODE_NOT_FOUND',
                             'Install Node.js separately, or pass --node with its absolute executable path.', version='unknown'))
    codex_path = find_codex(codex)
    if codex_path:
        status, code, version = _probe([codex_path, '--version'])
        checks.append(_check('codex_cli', status, code,
                             '' if status == 'supported' else 'Pass --codex with a compatible local Codex CLI.',
                             version=version or 'unknown'))
        for identifier, command in HELP_COMMANDS:
            status, code, _ = _probe([codex_path, *command, '--help'])
            checks.append(_check(identifier, status,
                                 'CODEX_PLUGIN_COMMAND_UNSUPPORTED' if code == 'COMMAND_REJECTED' else code,
                                 '' if status == 'supported' else 'Use a Codex CLI supporting the local plugin marketplace commands.'))
    else:
        checks.append(_check('codex_cli', 'unsupported', 'CODEX_NOT_FOUND',
                             'Install Codex separately, or pass --codex with its absolute executable path.', version='unknown'))
        for identifier, _ in HELP_COMMANDS:
            checks.append(_check(identifier, 'unknown', 'CODEX_NOT_CHECKED', 'Resolve CODEX_NOT_FOUND first.'))
    fingerprint = None
    try:
        candidate = Path(cli).expanduser() if cli else DEFAULT_CLI
        if not candidate.is_file():
            raise FileNotFoundError()
        fingerprint = hashlib.sha256(candidate.read_bytes()).hexdigest()
        if not reviewed:
            checks.append(_check('zcode_cli', 'unknown', 'REVIEWED_REGISTRY_UNAVAILABLE',
                                 'Restore the verified plugin runtime before checking ZCode.'))
        elif fingerprint not in reviewed:
            checks.append(_check('zcode_cli', 'unsupported', 'ZCODE_FINGERPRINT_UNREVIEWED',
                                 'Use a reviewed ZCode build or request compatibility review; do not bypass fingerprint checks.',
                                 sha256=fingerprint))
        else:
            checks.append(_check('zcode_cli', 'supported', sha256=fingerprint))
    except (OSError, ValueError):
        checks.append(_check('zcode_cli', 'unsupported', 'ZCODE_CLI_NOT_READABLE',
                             'Install ZCode separately, or pass --cli with the reviewed zcode.cjs path.'))
    ok = all(check['status'] == 'supported' for check in checks)
    return {'schema_version': 1, 'ok': ok, 'platform': platform.system(),
            'architecture': platform.machine(), 'plugin_version': plugin_version,
            'core_version': core_version, 'checks': checks,
            'capabilities': {
                'bounded_delegation': {'status': 'supported' if ok else 'unsupported',
                                       'reason': 'Prerequisites checked; no task execution or authentication tested.' if ok else 'Resolve prerequisite checks first.'},
                'authentication': {'status': 'unknown', 'reason': 'Not inspected; sign in to ZCode separately.'},
                'fixed_model_selection': {'status': 'unsupported', 'reason': 'Excluded from this accepted release baseline.'},
                'strict_all_actor_prestart_request_cap': {'status': 'unsupported', 'reason': 'The reviewed interface cannot enforce an actual provider-request gate before every actor starts.'},
                'actual_codex_cost': {'status': 'unknown', 'reason': 'No model requests or cost experiment performed.'},
                'os_sandbox': {'status': 'unsupported', 'reason': 'Permission brokerage and evidence checks are not an operating-system sandbox.'},
            }, 'model_requests': 0}


def setup_commands(plugin_root: Path, directory: Path, node=None) -> dict:
    """Prepare commands only, retaining a discovered absolute Node executable."""
    selected = find_node(node)
    if selected is None:
        return {'status': 'unsupported', 'error_code': 'NODE_NOT_FOUND', 'commands': []}
    home = str(Path.home())
    # Shareable reports deliberately omit arbitrary local directory names.
    known = ('/opt/homebrew/', '/usr/local/', '/usr/bin/', '/bin/')
    if selected.startswith(known):
        node_token = shlex.quote(selected)
        note = 'The Node executable is the absolute path discovered on this Mac.'
    elif selected.startswith(home + '/'):
        node_token = '"$HOME"/' + shlex.quote(selected[len(home) + 1:])
        note = 'The Node executable uses the discovered home-relative path; set CZS_PLUGIN_ROOT and CZS_SETUP_DIRECTORY locally.'
    else:
        node_token = '"$CZS_NODE"'
        note = 'Set CZS_NODE locally to the absolute Node executable passed to diagnostics; its private path is omitted from this report.'
    launcher = '"$CZS_PLUGIN_ROOT/scripts/python.sh" "$CZS_PLUGIN_ROOT/scripts/czs.py"'
    return {'status': 'supported', 'error_code': '', 'variables': {
        'CZS_PLUGIN_ROOT': 'Absolute installed plugin directory on this Mac (not included in this shareable report).',
        'CZS_SETUP_DIRECTORY': 'Absolute task root chosen on this Mac (not included in this shareable report).'},
        'note': note, 'commands': [
            f'{launcher} init --directory "$CZS_SETUP_DIRECTORY" --node {node_token}',
            f'{launcher} --config "$CZS_SETUP_DIRECTORY/.czs/config.json" doctor',
        ]}


def write_report(path: Path, report: dict) -> None:
    """Create a private report without overwriting or following linked parents."""
    path = Path(path).expanduser().absolute()
    for parent in (path.parent, *path.parents):
        if parent.is_symlink():
            raise ValueError('REPORT_SYMLINK_REFUSED')
    if path.is_symlink():
        raise ValueError('REPORT_SYMLINK_REFUSED')
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, 'O_NOFOLLOW'):
        flags |= os.O_NOFOLLOW
    try:
        descriptor = os.open(path, flags, 0o600)
    except FileExistsError:
        raise ValueError('REPORT_EXISTS') from None
    with os.fdopen(descriptor, 'w', encoding='utf-8') as stream:
        stream.write(json.dumps(report, ensure_ascii=False, indent=2) + '\n')


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--codex')
    parser.add_argument('--node')
    parser.add_argument('--cli')
    parser.add_argument('--output', type=Path)
    parser.add_argument('--node-path', action='store_true',
                        help='Local use only: print the resolved Node executable path, without creating a shareable report')
    parser.add_argument('--setup-directory', type=Path,
                        help='Compute setup commands only; never initializes or runs doctor')
    args = parser.parse_args(argv)
    try:
        if args.node_path:
            if args.output is not None or args.setup_directory is not None:
                raise ValueError('NODE_PATH_REPORT_CONFLICT')
            selected = find_node(args.node)
            if selected is None:
                raise ValueError('NODE_NOT_FOUND')
            print(selected)
            return 0
        plugin = locate_plugin_root()
        report = inspect_environment(plugin, args.codex, args.node, args.cli)
        if args.setup_directory is not None:
            report['setup'] = setup_commands(plugin, args.setup_directory, args.node)
        if args.output is not None:
            write_report(args.output, report)
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 0 if report['ok'] else 2
    except (OSError, ValueError) as error:
        code = str(error) if str(error) in {'PLUGIN_RUNTIME_MISSING', 'REPORT_SYMLINK_REFUSED', 'REPORT_EXISTS', 'NODE_PATH_REPORT_CONFLICT', 'NODE_NOT_FOUND'} else 'DIAGNOSTICS_FAILED'
        print(json.dumps({'schema_version': 1, 'ok': False, 'error_code': code,
                          'remediation': 'Use a verified release and a new report path in an existing private directory.',
                          'model_requests': 0}))
        return 2


if __name__ == '__main__':
    raise SystemExit(main())
