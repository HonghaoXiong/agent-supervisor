#!/usr/bin/env python3
"""P0.0 bench entry: verify the fixed fixtures, then run the fake/replay baseline.

Verifies the pinned UI fixture digest and the communication fault manifest
against the fixture child, then runs the fake/replay baseline sections
(cold spawn/handshake, app-server initialize/thread rounds, resident
first/steady/cancel rounds, clock sidecar). Writes raw JSON samples to
``--out`` (ui-fixture-check.json, fake-baseline.json) and exits 0 only when
every section ran and ``model_requests == 0``. Never calls a model, the real
CLI, or remote tasks.
"""
import argparse
import asyncio
import hashlib
import json
from pathlib import Path
import subprocess
import sys

REPO_ROOT = Path(__file__).resolve().parents[1]
FIXTURE_DIR = REPO_ROOT / 'tests' / 'fixtures' / 'ui'
PINNED_COUNTS = {'nodes': 100, 'edges': 300, 'events': 10000}


def _load_bench():
    source_root = str(REPO_ROOT / 'src')
    if source_root not in sys.path:
        sys.path.insert(0, source_root)
    from zcode_supervisor import bench
    return bench


def verify_fixtures(repo_root=None):
    """Check the pinned UI fixture digest and the comm-fault manifest modes."""
    repo_root = Path(repo_root) if repo_root is not None else REPO_ROOT
    fixture_dir = repo_root / 'tests' / 'fixtures' / 'ui'
    bench = _load_bench()
    spec = json.loads((fixture_dir / 'ui-fixture-spec.json').read_text())
    fixture = bench.build_ui_fixture(spec['spec'])
    blob = bench.canonical_fixture_bytes(fixture)
    report = {
        'fixture_sha256_match':
            hashlib.sha256(blob).hexdigest() == spec['fixture_sha256'],
        'regeneration_deterministic':
            blob == bench.canonical_fixture_bytes(bench.build_ui_fixture(spec['spec'])),
        'counts': {'nodes': len(fixture['nodes']), 'edges': len(fixture['edges']),
                   'events': len(fixture['events'])},
    }
    listing = subprocess.run(
        [sys.executable, str(fixture_dir / 'fake_server.py'), '--list-faults'],
        capture_output=True, text=True, timeout=120)
    supported = set(json.loads(listing.stdout)['fault_modes']) \
        if listing.returncode == 0 else set()
    manifest = json.loads((fixture_dir / 'comm-faults-v1.json').read_text())
    modes = {scenario['fake_server_fault']['mode']
             for scenario in manifest['scenarios']}
    report['fault_modes_supported'] = listing.returncode == 0 and modes <= supported
    report['ok'] = all((report['fixture_sha256_match'],
                        report['regeneration_deterministic'],
                        report['counts'] == PINNED_COUNTS,
                        report['fault_modes_supported']))
    return report


def _run_replay_module(module, argv):
    """Run fake_hub_replay.main() with a fresh argv (P1.2 mode)."""
    old = sys.argv
    try:
        sys.argv = ['fake_hub_replay'] + list(argv)
        return module.main()
    finally:
        sys.argv = old


def main(argv=None):
    parser = argparse.ArgumentParser(
        description='P0.0 fake/replay baseline runner (fixtures check + smoke)')
    parser.add_argument('--out', type=Path, required=True,
                        help='Output directory for the raw JSON evidence')
    parser.add_argument('--fake-hub-replay', action='store_true',
                        help='P1.2: three discovery timepoints over the '
                             'real UiEventServer + four-zero counters '
                             '(delegates to tools/fake_hub_replay.py; '
                             'implies --rust-test unless --no-rust-test)')
    parser.add_argument('--rust-test', action='store_true', default=None,
                        dest='rust_test',
                        help='with --fake-hub-replay: also run the live '
                             'Rust integration test against the same server '
                             '(default ON in that mode)')
    parser.add_argument('--no-rust-test', action='store_false',
                        dest='rust_test',
                        help='skip the Rust round trip')
    parser.add_argument('--steady-rounds', type=int, default=3,
                        help='Steady resume/subscribe/send rounds in the resident section')
    parser.add_argument('--init-trials', type=int, default=3,
                        help='Cold spawn/initialize trials')
    args = parser.parse_args(argv)
    if getattr(args, 'fake_hub_replay', False):
        # P1.2 fake-hub replay mode: no model requests, delegates to the
        # dedicated harness. --rust-test defaults ON here (None = unset
        # -> on); --no-rust-test is the only way to skip it (repair1
        # flag-logic fix: the old store_true default False silently
        # disabled the Rust round trip unless the flag was repeated).
        sys.path.insert(0, str(Path(__file__).resolve().parent))
        import fake_hub_replay
        replay_args = ['--out', str(args.out)]
        if args.rust_test is not False:
            replay_args.append('--rust-test')
        return _run_replay_module(fake_hub_replay, replay_args)
    bench = _load_bench()
    from zcode_supervisor.common import atomicwrite_json
    check = verify_fixtures()
    out_dir = args.out
    out_dir.mkdir(parents=True, exist_ok=True)
    atomicwrite_json(out_dir / 'ui-fixture-check.json', check)
    if not check['ok']:
        print(json.dumps({'ok': False, 'fixture_check': check},
                         ensure_ascii=False, indent=2))
        return 2
    document = asyncio.run(bench.run_fake_baseline(
        FIXTURE_DIR / 'fake_server.py', out_dir,
        steady_rounds=args.steady_rounds, init_trials=args.init_trials))
    summary = {'ok': document['exit_code'] == 0,
               'model_requests': document['model_requests'],
               'exit_code': document['exit_code'],
               'sections': {name: entry.get('status')
                            for name, entry in document['sections'].items()},
               'fixture_check': check,
               # The actual baseline file this run wrote (reruns never
               # overwrite: fake-baseline.json, then -2, -3, ...).
               'fake_baseline_json': document['output']['json'],
               'fixture_check_json': str(out_dir / 'ui-fixture-check.json')}
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return document['exit_code']


if __name__ == '__main__':
    sys.exit(main())
