# Local setup and task packets

The package bundles the accepted supervisor Python sources in `lib/`. It uses
only the Python standard library for CLI execution; no pip install is required.
Find the plugin root from the installed skill path. Do not depend on a global
`czs` command or the repository's development virtual environment.

Prerequisites: macOS, Python 3.11+, Node.js, and a separately installed ZCode
that is logged in on this computer. Doctor accepts only the exact reviewed
bundle fingerprints recorded in `docs/COMPATIBILITY.md` (currently 0.16.9 and
0.17.0 pins). Equal version text is insufficient.
An unsupported build blocks execution; do not edit the registry to bypass it.

`init` creates a private local `.czs/config.json`, cache/state directories and a
report-only example. It detects `/Applications/ZCode.app/Contents/Resources/glm/zcode.cjs`;
use `--cli`, `--node` and `--provider-config` for other local paths. It records
the provider file path, never copies its contents. Existing `.czs` is not replaced.
`doctor` validates compatibility without sending a model task. Authentication is
not proven by doctor; the first authorized task provides that evidence.
Run `scripts/diagnostics.py` through the bundled Python launcher first. It checks
the required host plugin commands and reports stable check codes and remedies,
with authentication and unavailable guarantees kept unknown/unsupported.
Use the detected absolute Node path in `init --node`, including when launched
from Finder with a different PATH. `diagnostics.py --node-path` returns it locally
without adding it to a shareable report. Plugin 0.1.4 retained core runtime
0.1.0 and added fixed-model packet support (`model` field with explicit
providerId/modelId, optional reasoningLevel) plus the tri-agent orchestration
hub (contract-gated MCP control plane, resident workers, per-command
approvals; see `docs/USAGE.md`). Plugin 0.2.1 adds the stall-timeout progress
ledger (`--stall-timeout`, `czs progress`, `czs codex-status`) and registers
`dsh` as the third hub worker (headless mode).

The generated example is at `<directory>/.czs/work/example/packet.json`:

```json
{
  "workspace": "/absolute/directory/.czs/work/example/workspace",
  "prompt": "Read TASK.md and create REPORT.md. Do not execute shell commands.",
  "allowed_files": ["REPORT.md"],
  "commands": []
}
```

Run from any cwd by passing `--config` before the subcommand. For a test command:

```json
{
  "workspace": "/absolute/directory/.czs/work/task/workspace",
  "prompt": "Fix the specified bug and run the exact approved test command.",
  "allowed_files": ["module.py", "tests/test_module.py"],
  "commands": [
    {"id": "tests", "command": "python3 -m unittest discover -s tests -v", "max_uses": 1, "max_timeout_ms": 60000, "required": true}
  ]
}
```

Check the exact command schema in `docs/USAGE.md` before executing a new packet.
Do not treat examples as permission to run unrelated tests.

Time limits (2026-11-03 rule — do not regress to short absolute walls):

* For CODE-WRITING tasks dispatch with `--stall-timeout` (600–900) and NO
  `--timeout`: the run then terminates only after N seconds WITHOUT a
  progress heartbeat, and the wall fuse rises to 86400. A worker that keeps
  making progress is never cut off mid-implementation.
* Bare `--timeout N` (default 300) keeps the legacy absolute semantics —
  use it only for genuinely bounded probes, never for code writing.
* Never assume a run is stuck from silence alone: peek first with
  `czs progress <task-dir-or-run.json> --last 3` (one bounded line per
  heartbeat, zero context cost, works from any side including a ZCode
  session) and only then decide to resume or rework.
* `czs codex-status` (any cwd, no config needed) shows live Codex sessions,
  dsh workspaces and in-flight czs runs for cross-agent monitoring.

Use `--out <task>/run-1.json`, then run-2, etc. The bridge retains matching
`.events.jsonl`, `.progress.jsonl` heartbeat and before-state evidence. Never
overwrite or remove failed attempts to make a later result appear clean. A
registered, settled worker can be continued with `--resume <session_id>` and
a newly reviewed packet/output.

Only merge accepted changes into a working repository under the user's existing
authorization. Preserve other sessions' unstaged and staged hunks. No automatic
commit, push, branch change, remote job or publication follows from delegation.

The bridge is a permission broker plus evidence audit, not an OS sandbox. Use
trusted, bounded copies. Worker hooks and already-approved processes still run
as the local user. Do not copy credentials or share private run logs in the plugin.

The evidence MCP module is optional and needs its separately pinned SDK; the
standard installation does not register it. Execution stays through `czs.py run`.
The GLM surrogate benchmark does not establish actual Codex token savings.
