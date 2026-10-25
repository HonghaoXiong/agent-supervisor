# Read-only MCP evidence interface (pilot)

Status: pilot, independently reviewable; nothing registered anywhere. This
document is the in-repo pointer; the full pilot evidence (benchmark plan,
results, stdio proof, ACP fidelity matrix, SDK lock) lives in the workspace
evidence directory `out/czs-mcp-pilot-20261014/` of the UW2 repository.

## What shipped

`src/zcode_supervisor/mcp_readonly.py` — a read-only MCP server over stdio
(`python -m zcode_supervisor.mcp_readonly --evidence-root DIR`), built on the
pinned official SDK `mcp==2.3.0` (MIT, modelcontextprotocol/python-sdk;
installed in the project `.venv`, see the pilot's `mcp-sdk-lock.json`).

Three tools, all confined to one evidence root:

- `list_runs(task?)` — capped listing with explicit truncation and resume hints
- `get_digest(run_id)` — the accepted digest contract, served honestly:
  historical runs are full-review with `bindings: "unknown"`; serving never
  backfills current code hashes as execution identity
- `get_receipt(run_id, command_id, offset, limit)` — raw receipt paging;
  pages concatenate back to the original; exit/timeout/status facts come from
  the digest module's single guarded extraction

Read-only and confinement are enforced by code (see `tests/test_mcp_readonly.py`):
run_id alphabet + resolved-path containment (traversal, absolute ids and
symlink escapes rejected; the root itself is resolved once), malformed JSON
and ill-typed fields surface as explicit `is_error` results, every serialized
payload respects an 8 KB budget with visible truncation flags, and there are
no write, execute or arbitrary-path tools. No global MCP registration, no user
Codex/zcode configuration, authentication or installed-application change.

## Measured result (bounded pilot, 6 sessions, equal review quality)

Control-side reviewer sessions over the same frozen evidence with identical
task text and model; arm A = current CLI short JSON + on-demand file reads,
arm B = MCP digest (fetched through the real stdio server) + same on-demand
reads. Per-sample input tokens (including cache reads): −25% / −49% / −35%;
overall 786,405 → 479,496 (−39%), model requests 21 → 16, output tokens
similar. Full numbers and the disclosure that arm B is a conservative
estimate of the production MCP diet: `results/analysis.json`.

## ACP replay verdict

Offline field-by-field replay through pinned `agent-client-protocol==0.12.1`
(schema v1.19.0): 4 facts preserved natively, 7 only via `field_meta` /
`raw_input` / `raw_output` extension dicts, 2 degraded (fixed permission-option
kinds cannot express exact-command one-shot policy; denial reasons have no
native field), 2 stay external to ACP. Verdict: do not migrate this round; the
bridge plus extension registry would be permanent maintenance surface.
See `acp-fidelity-matrix.json`.

## Not shipped / future wiring

- No execution-time binding capture: digests served for historical runs stay
  full-review. Wiring bindings into new runs must capture real
  code/packet/profile/runtime association at execution time and verify it
  independently (registered as future work, per the accepted digest contract).
- Reviewer sessions in the benchmark received the digest via prompt because
  the zcode CLI has no transient MCP config and user config must not change;
  production Codex would call the tools directly.
