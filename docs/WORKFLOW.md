# Durable collaboration workflow

The work API separates **request acceptance**, **worker execution**, and
**independent acceptance**. It does not treat a returned task id as a completed
worker result, and it never merges a target branch automatically.

## One private owner

Start the foreground owner explicitly. CLI and MCP adapters do not install a
system service or daemonize themselves.

```bash
czs work serve --state-root /absolute/private/work-owner
```

The owner owns SQLite, artifact objects, dispatch intents and worker attempts.
Its Unix control socket is mode `0600` and accepts only same-uid local peers.
Keep the state root private and outside the candidate workspace. A short-lived
CLI or stdio MCP adapter may exit without stopping the owner or its workers.

With no owner, public calls fail with `service-unavailable` and print the
explicit start command. The default foreground command has no production model
runner configured; offline tests inject a deterministic fixture runner. Live
model dispatch is a separate verification dimension.

## Common request rules

Mutation requests contain:

```json
{
  "work_id": "...",
  "expected_revision": 0,
  "idempotency_key": "a-unique-client-key",
  "payload": {}
}
```

`expected_revision` detects stale writers. Reusing an idempotency key with
identical canonical bytes returns the original result; changing the bytes is
`idempotency-conflict`. A replayed start may re-read its durable dispatch
binding, but it does not create a second task.

Start requests additionally carry a durable intent:

```json
{
  "schema_version": "zsc.dispatch-intent/v1",
  "work_id": "...",
  "attempt_id": "recon-a",
  "contract": {"contract_id": "...", "worker": "zcode"},
  "contract_sha256": "sha256-of-canonical-contract"
}
```

The returned `request_accepted: true` and `execution_status: accepted` mean the
state transition and intent were committed. Read `work_status` for the actual
worker result.

## CLI lifecycle

Each request is a bounded JSON document. Paths below are examples, not
executed commands.

```bash
czs work create --request create.json --state-root /absolute/private/work-owner
czs work recon --request recon.json --state-root /absolute/private/work-owner
czs work submit-brief --request brief.json --state-root /absolute/private/work-owner
czs work accept-design --request design.json --state-root /absolute/private/work-owner
czs work implement --request implementation.json --state-root /absolute/private/work-owner
czs work submit-delivery --request delivery.json --state-root /absolute/private/work-owner
czs work verify --request verification.json --state-root /absolute/private/work-owner
czs work accept --request acceptance.json --state-root /absolute/private/work-owner
```

Useful controls and reads:

```bash
czs work status --work-id WORK_ID --state-root /absolute/private/work-owner
czs work cancel --request cancel.json --state-root /absolute/private/work-owner
czs work reconcile --request reconcile.json --state-root /absolute/private/work-owner
czs work artifact --request artifact.json --state-root /absolute/private/work-owner
czs work capabilities --request capabilities.json --state-root /absolute/private/work-owner
```

`authorize` is a host-side CLI operation; worker-facing MCP does not expose a
self-registration tool. Knowledge search/proposal/review/revalidation use the
same `--request --state-root` convention under `czs work knowledge ...`.

## MCP lifecycle

The hub exposes `work_capabilities`, `create_work`, `start_recon`,
`submit_brief`, `accept_design`, `start_implementation`, `submit_delivery`,
`start_verification`, `record_acceptance`, `cancel_work`, `reconcile_work`,
`work_status`, `read_artifact`, and the knowledge tools. They call the same
independent owner as the CLI. Business errors are MCP tool results with
`is_error=true` and a stable `error` code, not JSON-RPC transport failures.

The hub resolves its owner at `<config.state_dir>/work-owner`. It does not keep
a second work database or duplicate lifecycle rules in the adapter.

## Evidence and acceptance gates

- A brief must pass schema and current-source checks.
- An accepted design binds its exact hash and unresolved-conflict state.
- Implementation requires an isolated candidate child, B0 and an allowed write
  set. Unexpected writes or target drift fail delivery.
- Delivery requires candidate/B1 and cumulative contributor evidence.
- Verification binds the same candidate hash. No author, reviewer, patch
  contributor, or contributor session may verify its own contribution.
- Acceptance requires the bound independent verifier, all required checks and
  dependencies, and code evidence. Numerical, physics, observational and live
  runtime dimensions are not upgraded implicitly.
- `completed` does not merge or replace the installed release.

After an owner crash, `pending` intents may be recovered once. An attempt that
was already `dispatching` or `running` becomes `needs_reconciliation`; its send
status is unknown and it is not automatically replayed. This is deliberately
not an external exactly-once claim.

## Read-only desktop projection

The same foreground command also serves `<state-root>/ui-events.sock`. It is a
separate, same-uid, read-only UI socket backed by the same SQLite owner, not a
second event journal or a worker control channel. Use `--no-ui` only to retain
the legacy control-only process, or `--ui-socket` to choose another private
socket path.

Legacy v1 task clients keep their existing wire behavior. A collaboration
client must opt in during `hello` with version `1` and explicit capabilities;
only then may it call `work-snapshot`, `work-events`, `work-evidence`, or
`work-subscribe`. Unnegotiated work operations fail with
`capability-not-negotiated`.

The server sends sanitized full-work upserts. Work revision is the durable
business revision; UI event sequence is only a replay cursor. Duplicate and
stale events do not roll back state, retention loss produces an explicit event
gap, and evidence is paged independently. The UI must display dependency
status, checker and candidate bindings as received rather than infer closure.
Raw prompts, project/workspace paths, actor evidence and artifact bodies are
not part of the work projection.

Offline fixture rendering and headless egui audits are not live-app visual
acceptance. The checked-in shared fixture is
`tests/fixtures/ui/work-wire-golden.json`; `tools/render_ui_fixture.py` writes
360/720/1280 PNG resize evidence without launching a model or a real desktop.

## Session loop (0.4.0-dev)

Bind a fixed ZCode session once; the owner's pump notices each completed turn by itself, the pinned-thread Codex reviewer decides, and the next instruction returns to the same session via attach. Control bindings need a matching hub task record; a dispatch that lands in another session is marked needs_reconciliation. See USAGE.md 'Session loop'. Offline-tested only; live desktop wakeup unsupported without CZS_DESKTOP_WAKE_CMD.
