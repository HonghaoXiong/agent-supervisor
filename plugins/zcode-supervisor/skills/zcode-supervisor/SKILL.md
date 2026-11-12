---
name: zcode-supervisor
description: Delegate a bounded coding or investigation task from Codex to a local ZCode worker, resume a registered worker, or inspect its delivery evidence. Use when the user asks to use ZCode, zcode-supervisor, czs, or Codex–ZCode collaboration. Does not control an active ZCode GUI conversation or provide an OS sandbox.
---

Use the bundled supervisor; Codex owns the task contract and independent acceptance.

The plugin root is two directories above this skill directory. Its launcher is
`scripts/python.sh`; pass `scripts/czs.py` as its first argument. Use absolute,
quoted paths derived from this installed skill, never a developer-machine path.
The launcher finds Python 3.11+; set `CZS_PYTHON` to an explicit interpreter if needed.
The plugin is usable without pip or a separately installed `czs` command.

## Establish the local installation

Read [the setup and packet reference](references/delegation.md) when setting up
the bridge or composing a packet. First run the bundled read-only diagnostics:

```sh
"<plugin-root>/scripts/python.sh" "<plugin-root>/scripts/diagnostics.py"
"<plugin-root>/scripts/python.sh" "<plugin-root>/scripts/diagnostics.py" --node-path
```

Inspect its stable check codes, remedies and supported/unsupported/unknown
capabilities. It sends no model requests and does not test authentication.
Use the local `--node-path` result explicitly for initialization. It is separate
from the shareable report, which omits private paths. Optional setup commands
contain local placeholders to resolve before use.
Reuse the user's existing bridge configuration
if given; otherwise initialize a dedicated directory, normally `~/zcode-work`:

```sh
"<plugin-root>/scripts/python.sh" "<plugin-root>/scripts/czs.py" init --directory "$HOME/zcode-work" --node "<detected-absolute-node>"
"<plugin-root>/scripts/python.sh" "<plugin-root>/scripts/czs.py" --config "$HOME/zcode-work/.czs/config.json" doctor
```

Initialization and doctor do not send model tasks. Doctor checks the exact CLI
fingerprint and makes a private compatibility copy; it never patches the app.
If Node is installed outside PATH, pass `init --node /absolute/path/to/node`
(common macOS locations are `/opt/homebrew/bin/node` and `/usr/local/bin/node`).
If it refuses an unknown build, report its fingerprint and stop delegation.
Do not auto-approve a new fingerprint, change provider/auth settings, or turn on
ZCode yolo. The target computer must have its own working ZCode login.

## Delegate and review

For an authorized task, put a small working copy under the configured work_root.
Copy the target project's `AGENTS.md` into that workspace when its rules should
bind the worker (worker sessions load the workspace AGENTS.md; a habit profile
carries only its reviewed snapshot). The worker has no MCP tools and no native
web search; scope prompts accordingly. Keep packet and output evidence outside
that workspace. Choose explicit output
files and exact commands with invocation/time limits; do not delegate an entire
unrelated repository by default. Record the baseline and relevant input hashes.
Use a fresh `run-<N>.json` output name for every attempt, including failures.

```sh
"<plugin-root>/scripts/python.sh" "<plugin-root>/scripts/czs.py" --config "<config>" run --packet "<packet.json>" --out "<task>/run-1.json" --stall-timeout 600
```

Time-limit rule (2026-11-03): dispatch CODE-WRITING tasks with
`--stall-timeout` (600–900) and no `--timeout` — the run then dies only after
N seconds WITHOUT a progress heartbeat (the wall fuse rises to 86400), so a
worker that keeps making progress is never cut off mid-implementation. Bare
`--timeout N` (default 300, absolute) is for bounded probes only. Never
declare a run stuck from silence alone: peek
`czs progress <task-dir-or-run.json> --last 3` first (one bounded line per
heartbeat, zero context cost), and monitor any side with `czs codex-status`.

Capability + latency conventions (2026-11-03):

* **Know the worker's skills before dispatching**: `czs skills <zcode|codex|dsh>`
  (or the hub `list_skills` tool) lists name + description; NAME the relevant
  skills explicitly in the task prompt so the worker uses them instead of
  improvising. Delegate receipts also carry a bounded `worker_skills` list.
* **Prefer the warm topology for repeated work**: keep the hub connected as a
  persistent MCP server (`czs hub serve`) and register zcode contracts with an
  explicit `"mode": "resident"` — the shared warm process sets up in 94/68ms
  versus 604ms cold. Old contracts without a mode keep their cold default.
  `hub serve` needs the `[hub]` extra (mcp + anyio); the plugin's stdlib-only
  czs CLI cannot serve — run it from the hub venv launcher (`czs-hub-mcp.sh`)
  or an environment with `pip install 'codex-zcode-supervisor[hub]'`.
* **Status never queues**: the hub `task_status` tool (or `czs progress`)
  reads running tasks' heartbeat ledgers directly — check it before assuming
  a worker is stuck.
* **Follow-ups attach, they don't restart**: to continue a prior task's
  worker conversation (rework, next step), register the follow-up contract
  and call `delegate` with `action=attach` and `attach_to=<prior task id>`
  — same session, same workspace (zcode/dsh). `action=stop` +
  `attach_to=<running task id>` signals an in-flight dsh dispatch to stop.

Orientation and durable workflow conventions (0.3):

* **Orient before investigation**: build a TaskFrame/v1 (goal, task_kinds,
  domains, features, evidence_refs) and run `czs work prepare --request <json>`
  (or hub `prepare_work`). It is deterministic and sends zero model requests.
  Optional `theme_ids` select the bundled mechanics, seismic, flow/transport,
  thermal, stratigraphy/geomorphology or reaction/geochemistry question sets;
  they are investigation aids, never scientific verdicts.
* **Use one explicit owner for the full lifecycle**: start
  `czs work serve --state-root <private-directory>` in the foreground. CLI and
  hub MCP share that owner through `WorkService`; adapter exit does not kill
  workers. Create work, run recon, submit and validate the brief, accept a
  versioned design, implement, deliver, verify independently, then record
  acceptance. Mutations require `expected_revision` and `idempotency_key`.
  Start returns an accepted attempt/task, not a completed worker result.
* **Native and packaged workflows are capability-gated**: the ZCode cold path
  supports declared CreateWorkflow orchestration, including the packaged
  `templates/recon/architecture.dwf.ts` with host-pinned bytes and args. Load
  `dynamic-workflows` explicitly and use the exact declaration; never replace
  it with a direct Agent or modified script. Resident orchestration remains
  unsupported and must select cold. Unknown skills and missing host evidence
  remain missing rather than being silently satisfied.
* **Acceptance stays independent**: a valid ArchitectureBrief, completed
  worker, or `delivery_ok=true` is not design/code acceptance. Keep candidate
  and design hashes, dependency checks and verifier identity bound; rework
  uses a new candidate. The workflow never merges or publishes automatically.
* **Provider limits fail closed**: observed usage is retained, but public RPCs
  do not provide strict provider token/cost caps or monetary cost. A routing
  policy that requires those caps is rejected before dispatch as
  `provider-budget-unsupported`; missing measurements stay unknown.
* Read the packaged `docs/WORKFLOW.md` and `docs/CAPABILITY-MATRIX.md` for the
  lifecycle, recovery, read-only UI negotiation and scoped live boundaries.

Capability conventions (2026-11-04):

* **Pick the model per task type**: query `czs models` (or the hub
  `list_models` tool) once per session — it lists the local ZCode's vendor
  models as `providerId/modelId` pairs (personally-added models flagged;
  credentials are never exposed), and every zcode delegate receipt
  re-advertises a bounded `worker_models` list. Pin the choice explicitly
  in the contract's `model` field — zcode wants
  `{"providerId": ..., "modelId": ..., "options": ...}` (`options`
  optional), codex only `{"model": "<name>"}`, dsh is profile-driven (null).
  Route heavy code generation and architecture work to the strongest tier
  (e.g. a `*-pro` model), quick scans/reviews/formatting to the flash tier;
  omitting `model` means the worker's default — say so when that is
  intentional.
* **Skills-first kickoff for software work**: when starting development on,
  optimizing, or adding a module to an existing codebase, don't rebuild the
  understanding yourself — first delegate an analysis pass: name the skills
  to run explicitly (pick them from `czs skills zcode` / hub `list_skills`,
  e.g. workflow- and achievement-style reporting skills), scope the target
  repository by ABSOLUTE path in the task prompt, and state exactly which
  report you want back. Then research, plan and optimize on top of the
  returned report, using `action=attach` follow-ups on the same worker
  session. One repository per task: a skills prompt must never be ambiguous
  about which repo it operates on.

This sends real ZCode model requests and consumes that computer's quota. Existing
user authorization for the task carries through; installing the plugin alone
does not authorize arbitrary work or an unbounded experiment.

Read the run JSON, actual file diff, command results and raw event evidence.
Check refusal, identity, exit status, timeouts, drift and unknowns. `delivery_ok`
is a bridge delivery check, not your independent acceptance. Return concrete
findings; request only useful, scoped rework. For continuation use `--resume`
only with the session_id registered by this installation and retained evidence.
Never assume a GUI conversation is that worker or take over the user's GUI.

Codex chooses relevant per-task skills/plugins; ZCode reports missing or changed
bindings instead of substituting them. Installation, discovery, actual Skill
loading and consumption are separate claims. Fixed-model delegation accepts the
optional packet `model` field (explicit `providerId` and `modelId`, optional
`reasoningLevel`): the bridge sets it before dispatch, verifies the runtime
selection before send and after completion, and checks real provider request
receipts; any mismatch or missing receipt rejects delivery. Discover this
machine's identifiers from a read-only probe snapshot instead of guessing.

Advanced native workflow, goal and Agent contracts are described in the packaged
`docs/USAGE.md`; read them only if needed. They do not provide a strict all-actor
model-request admission cap. If that pre-start guarantee is required, report
UNSUPPORTED rather than launch and kill after observing extra requests. Child
cost or exact Skill consumption that is absent from evidence remains unknown.
The optional evidence MCP is read-only and does not execute ZCode tasks.

The tri-agent orchestration hub (`czs-hub`, also in `docs/USAGE.md`) exposes a
contract-gated MCP control plane: any installed agent can delegate read or
write tasks to the other installed workers (zcode / codex / dsh) through
human-registered contracts, with per-command approvals and conservative
session readback.

Keep a minimal external handoff for long-running coordination. At phase changes
or material context pressure, use the host's supported context maintenance or a
new coordinator with explicit handoff. Do not interrupt a busy user GUI, silently
recreate workers, replay completed runs or assume permissions/skills survived.

- Session loop (fixed-session auto round-trip): live-verified for the managed-reviewer background mode (`czs work bind-session` family). The owner pump advances terminal turns on its own; control bindings need a proven hub task record; the owner dispatches its own verification and records acceptance. `claim_native_session` handles a user-created session: it requires the paired Codex reviewer thread, keeps the session directory and the authorised code root separate, derives the cursor from the anchored turn and grants observe+review; the rework goes back through `ZCodeCliBridge` (`zcode --resume <same sess>`), with the persisted user message + turn as the receipt. Native GUI takeover is still unverified end-to-end; a running GUI session answers `session-busy` and nothing writes its database. A `ZCodeGuiBridge` adapter (System Events typing, fail-closed on drafts/ambiguous or title-colliding windows/busy sessions, verifiable session identity via `ZSC_GUI_SESSION_ID_ATTRIBUTE`, persisted intent plus message-marker/turn receipt) is implemented as the fallback when the CLI cannot resume, but this host denies macOS Accessibility to osascript (`-25211`) and exposes no configured identity attribute, so the GUI leg stays unsupported until the user grants both. Codex desktop chat delivery is live-verified on this host through the app's own queue+wake channel (`DesktopBridge`, `thread/queue/add` + `codex://threads/wake/<thread>`, receipt from the new turn), with `CZS_DESKTOP_WAKE_CMD` as the fallback.
