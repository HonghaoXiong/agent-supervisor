"""Small, reproducible command line; never edit global application settings."""
import argparse
import asyncio
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys

from . import __version__
from .profile import load_profile, resolve_profile_path
from .runtime import load_config


DEFAULT_CLI = Path('/Applications/ZCode.app/Contents/Resources/glm/zcode.cjs')


def create_project(directory, cli_path=None, provider_config=None, node='node'):
    """Generate local configuration and a harmless, reviewable first task."""
    if os.name != 'posix':
        raise ValueError('This release supports macOS/Linux; native Windows is not verified')
    raw = Path(directory).expanduser()
    if raw.is_symlink():
        raise ValueError('Project directory must not be a symlink')
    directory = raw.resolve()
    directory.mkdir(parents=True, exist_ok=True)
    local = directory / '.czs'
    if local.exists() or local.is_symlink():
        raise ValueError('.czs already exists; inspect the existing configuration instead of overwriting it')
    cli = Path(cli_path).expanduser().resolve() if cli_path else DEFAULT_CLI
    if not cli.is_file():
        raise ValueError('ZCode CLI not found; pass --cli /absolute/path/to/zcode.cjs')
    if not shutil.which(node):
        raise ValueError('Node.js was not found; install Node or pass --node /absolute/path/to/node')
    if provider_config:
        provider = Path(provider_config).expanduser().resolve()
        if not provider.is_file():
            raise ValueError('Provider configuration path does not exist')
    else:
        candidate = cli.parent.parent / 'config/provider/zcode-builtin.json'
        provider = candidate if candidate.is_file() else None
    config = {'schema_version': 1, 'cli_path': str(cli), 'node': node,
              'work_root': 'work', 'state_dir': 'state', 'cache_dir': 'cache',
              'compatibility': 'pinned-0.16.9'}
    if provider:
        config['provider_config'] = str(provider)
    # No provider content is opened, copied, or written by initialization.
    local.mkdir(mode=0o700)
    (local / '.gitignore').write_text('*\n')
    config_path = local / 'config.json'
    config_path.write_text(json.dumps(config, indent=2) + '\n')
    config_path.chmod(0o600)
    for name in ('state', 'cache'):
        (local / name).mkdir(mode=0o700)
    example = local / 'work/example'
    workspace = example / 'workspace'
    workspace.mkdir(parents=True)
    (workspace / 'AGENTS.md').write_text(
        '# Bounded integration task\n\nRead TASK.md. Only create REPORT.md. '
        'No shell commands, network access, subagents, or changes outside this workspace.\n')
    (workspace / 'TASK.md').write_text(
        'Write a short REPORT.md confirming that you read this file. '
        'Explain that protocol delivery does not prove domain correctness. '
        'Do not claim that any tests were executed.\n')
    packet = {'workspace': str(workspace), 'allowed_files': ['REPORT.md'],
              'commands': [],
              'prompt': 'Read TASK.md and create REPORT.md as requested. Only use Read and Write/Edit. '
                        'Do not execute Bash or change other files. Report permission failures honestly.'}
    (example / 'packet.json').write_text(json.dumps(packet, indent=2) + '\n')
    return {'initialized': True, 'config': str(config_path),
            'packet': str(example / 'packet.json'),
            'next': 'Review configuration, run czs doctor, then run the example task.'}


def doctor(config):
    command = config.command()  # verifies source compatibility and local copy
    from .runtime import REVIEWED_BUNDLES, CLI_VERSION
    installed = hashlib.sha256(Path(config.cli_path).read_bytes()).hexdigest()
    pin = REVIEWED_BUNDLES.get(installed) or {}
    node_version = subprocess.run([command[0], '--version'], capture_output=True,
                                  text=True, timeout=15, check=True).stdout.strip()
    result = subprocess.run([*command, '--version'], capture_output=True,
                            text=True, env=config.environment(), timeout=30, check=True)
    cli_version = result.stdout.strip()
    # Accept only the ACTUAL version of the matched pin; the version string
    # alone never grants compatibility (unknown hashes already failed closed
    # inside config.command()). Legacy pins without an explicit cli_version
    # keep their reviewed 0.16.9 semantics.
    expected_version = pin.get('cli_version', CLI_VERSION)
    if cli_version != expected_version:
        raise ValueError('Prepared CLI version differs from the reviewed compatibility target')
    result = {'ok': True, 'supervisor_version': __version__, 'python': sys.version.split()[0],
              'platform': sys.platform, 'node_version': node_version, 'zcode_cli_version': cli_version,
              'cli_sha256': hashlib.sha256(Path(command[1]).read_bytes()).hexdigest(),
              'bundle_sha256': installed,
              'bundle_source_revision': pin.get('upstream_commit') or 'unknown',
              'bundle_reviewed': pin.get('reviewed') or 'unreviewed',
              'work_root': str(config.work_root),
              'notifications': 'enabled' if config.notify else 'disabled',
              'authentication': 'not tested; use a real bounded task',
              'scope_enforcement': 'permission broker and post-run evidence audit; not an OS sandbox'}
    # Habit profile health is informational here: a drifted or broken profile
    # must never take the whole doctor report down with it.
    path = resolve_profile_path(config)
    if path is None:
        result['habit_profile'] = {'profile': None}
    else:
        try:
            loaded = load_profile(path)
            result['habit_profile'] = {'profile': str(path), 'ok': True,
                                       'rules': len(loaded['rules']), 'tools': len(loaded['tools'])}
        except ValueError as error:
            result['habit_profile'] = {'profile': str(path), 'ok': False, 'error': str(error)[:300]}
    return result


def parser():
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument('--version', action='version', version=__version__)
    result.add_argument('--config', type=Path, default=Path('.czs/config.json'),
                        help='Local JSON configuration (default: .czs/config.json)')
    sub = result.add_subparsers(dest='action', required=True)
    init = sub.add_parser('init', help='Create local config and a report-only example; no model calls')
    init.add_argument('--directory', type=Path, default=Path('.'))
    init.add_argument('--cli', type=Path)
    init.add_argument('--provider-config', type=Path)
    init.add_argument('--node', default='node')
    sub.add_parser('doctor', help='Check paths, pinned compatibility and versions; no model calls')
    probe = sub.add_parser('probe', help='Open and close a protocol session without sending a model task')
    probe.add_argument('--workspace', type=Path, required=True)
    probe.add_argument('--out', type=Path, required=True)
    run = sub.add_parser('run', help='Execute a reviewed packet once, with evidence checks')
    run.add_argument('--packet', type=Path, required=True)
    run.add_argument('--out', type=Path, required=True)
    run.add_argument('--resume', help='A completed worker session registered by this installation')
    run.add_argument('--timeout', type=int, default=300)
    habits = sub.add_parser('habits', help='Import or inspect user ZCode habits; no model calls')
    habits_sub = habits.add_subparsers(dest='habit_action', required=True)
    habits_import = habits_sub.add_parser(
        'import', help='Scan real ZCode stores and write a reviewable draft profile')
    habits_import.add_argument('--zcode-home', type=Path, default=Path.home() / '.zcode')
    habits_import.add_argument('--repo', type=Path, default=Path.cwd(),
                               help='Project repo for its AGENTS.md and pixi interpreter')
    habits_import.add_argument('--python', type=Path,
                               help='Extra interpreter for Python module probes')
    habits_sub.add_parser('status', help='Live profile summary and source drift')
    probe_all = sub.add_parser(
        'probe-all', help='Read-only Codex/ZCode/dsh install probe, contracts and '
                          'baseline diff plus an MCP handshake smoke; no model calls')
    probe_all.add_argument('--out', type=Path, required=True,
                           help='Output directory for contracts and tri-probe.json')
    probe_all.add_argument('--write-baseline', action='store_true',
                           help='Overwrite the stored baseline after comparing '
                                '(explicit action; by default the baseline is only created)')
    probe_all.add_argument('--revalidate', action='store_true',
                           help='Publish the probe result as hub health.json and print '
                                'the routing view; exit 0 only when every installed '
                                'worker stays enabled (fail-closed)')
    probe_all.add_argument('--hub-server-argv',
                           help='JSON array overriding the MCP smoke-test server command')
    hub = sub.add_parser('hub', help='Hub-side operations: contracts, health view, MCP server')
    hub_sub = hub.add_subparsers(dest='hub_action', required=True)
    hub_register = hub_sub.add_parser(
        'register-contract', help='Validate a human-supplied contract file and freeze '
                                  'it under the hub state directory')
    hub_register.add_argument('file', type=Path, help='Contract JSON file to register')
    hub_sub.add_parser('health', help='Print the published hub health.json '
                                      '(written by probe-all --revalidate)')
    hub_serve = hub_sub.add_parser('serve', help='Run the hub MCP server on stdio')
    hub_serve.add_argument('--evidence-root', type=Path, default=None,
                           help='Evidence root for hub dispatches '
                                '(default <work_root>/hub-evidence)')
    return result


def _hub_action(args, config) -> int:
    """The three hub subcommands; contract rejection prints its problem list."""
    from . import contracts, health, hub_server
    if args.hub_action == 'register-contract':
        try:
            result = contracts.register_contract(config.state_dir, args.file)
        except contracts.ContractError as error:
            print(json.dumps({'ok': False, 'error': str(error)}, ensure_ascii=False))
            return 1
        print(json.dumps(result, ensure_ascii=False, indent=2))
        return 0
    if args.hub_action == 'health':
        document = health.read_health(config.state_dir)
        if document is None:
            print(json.dumps({'ok': False,
                              'error': 'no health.json; run probe-all --revalidate'},
                             ensure_ascii=False))
            return 1
        print(json.dumps(document, ensure_ascii=False, indent=2))
        return 0
    # serve: hand the flags to hub_server's own entry point so its defaults
    # (config resolution, evidence-root under work_root) stay identical.
    hub_argv = ['--config', str(args.config)]
    if args.evidence_root is not None:
        hub_argv += ['--evidence-root', str(args.evidence_root)]
    return hub_server.main(hub_argv)


def main(argv=None):
    args = parser().parse_args(argv)
    try:
        if args.action == 'init':
            print(json.dumps(create_project(args.directory, args.cli, args.provider_config, args.node),
                             ensure_ascii=False, indent=2))
            return 0
        config = load_config(args.config)
        if args.action == 'doctor':
            print(json.dumps(doctor(config), ensure_ascii=False, indent=2))
            return 0
        if args.action == 'probe-all':
            from .tri_probe import revalidate, run_probe_all
            hub_argv = None
            if args.hub_server_argv:
                hub_argv = json.loads(args.hub_server_argv)
                if (not isinstance(hub_argv, list) or not hub_argv
                        or not all(isinstance(item, str) and item for item in hub_argv)):
                    raise ValueError('--hub-server-argv must be a JSON array of command strings')
            if args.revalidate:
                result = revalidate(config, args.out, hub_server_argv=hub_argv,
                                    write_baseline=args.write_baseline)
                document = result['health']
                print(json.dumps({
                    'overall': result['probe']['overall'],
                    'routing': document['routing'],
                    'workers': {name: {'status': entry['status'],
                                       'enabled': entry['enabled'],
                                       'reason': entry['reason']}
                                for name, entry in document['workers'].items()},
                }, ensure_ascii=False, indent=2))
                return 0 if result['all_installed_enabled'] else 1
            result = run_probe_all(config, args.out, hub_server_argv=hub_argv,
                                   write_baseline=args.write_baseline)
            print(json.dumps(result, ensure_ascii=False, indent=2))
            return 0 if result['overall'] == 'ok' else 1
        if args.action == 'hub':
            return _hub_action(args, config)
        if args.action == 'habits':
            from . import habits
            if args.habit_action == 'status':
                print(json.dumps(habits.habits_status(config), ensure_ascii=False, indent=2))
                return 0
            summary = habits.import_habits(
                config, zcode_home=args.zcode_home, repo=args.repo,
                extra_python=args.python)
            print(json.dumps(summary, ensure_ascii=False, indent=2))
            return 0
        from .protocol import execute, probe
        if args.action == 'probe':
            asyncio.run(probe(args, config))
            return 0
        if not 1 <= args.timeout <= 86400:
            raise ValueError('--timeout must be 1–86400 seconds')
        return 0 if asyncio.run(execute(args, config)) else 1
    except (ValueError, OSError, KeyError, TypeError, asyncio.TimeoutError,
            subprocess.SubprocessError) as error:
        print(json.dumps({'ok': False, 'error': str(error) or type(error).__name__}, ensure_ascii=False))
        return 1
    except KeyboardInterrupt:
        print(json.dumps({'ok': False, 'error': 'Interrupted; inspect retained evidence before retrying'}))
        return 130
