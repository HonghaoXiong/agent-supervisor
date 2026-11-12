"""Real-executor wiring for the persistent work owner (session-loop Task 1).

Bridges the durable :class:`~zcode_supervisor.work_dispatch.WorkDispatcher`
runner slot to the hub delegate path:

* a derived stage contract (``zsc.derived-contract/v1`` from
  ``contract_compile``) is compiled into a hub-registered EXECUTABLE contract
  using an explicit host worker profile (worker / mode / model mapping /
  skills) — derived fields never reach the hub directly;
* both hashes (derived, executable) are recorded on the attempt result and
  the executable is bound to the durable dispatch intent;
* attach re-dispatch (``attach_to`` dispatch metadata on the derived
  contract) continues the ORIGINAL worker session through delegate_impl's
  attach checks — a raw ``resume_session`` field is refused.

Offline tests inject fakes exactly at the process/model boundary
(``zcode_runner`` / ``codex_runner`` / ``dsh_runner`` / factories).
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import json
import shlex
import threading
from pathlib import Path

from . import contracts
from .hub_server import HubError, ResidentWorkers, delegate_impl
from .contract_compile import verify_against_authorization
from .work_authorization import validate_authorization
from .work_contracts import canonical_sha256
from .work_models import WorkError

DERIVED_SCHEMA = "zsc.derived-contract/v1"

# The compiler's own output shape (contract_compile.compile_stage_contract).
_DERIVED_KEYS = {
    "schema_version", "stage", "workspace", "prompt", "model", "allowed_files",
    "timeout_sec", "concurrency", "command", "authorization_sha256",
    "bindings_sha256", "compiler_version", "execution", "contract_id",
    "template_version", "require_command",
}
# Dispatch metadata the session loop may attach ON TOP of the compiled
# contract (hashed into the intent, never copied into the executable).
_DISPATCH_METADATA_KEYS = {"attach_to"}
# Session identity may only be reached through the attach_to record checks;
# raw injection of these fields is a bypass attempt.
_FORBIDDEN_METADATA_KEYS = {"resume_session", "session_id", "thread_id"}


def resolve_profile(profile) -> dict:
    """Validate the host worker profile; explicit, never guessed."""
    if profile is None:
        raise WorkError(
            "runner-profile-missing",
            "the persistent owner needs an explicit host worker profile "
            "(worker, mode, models) to dispatch real workers; start it with "
            "a runner profile or keep the offline runner-unconfigured posture")
    if not isinstance(profile, dict) or profile.get("worker") not in contracts.WORKERS:
        raise WorkError("runner-profile-invalid",
                        "profile must name one of " + ", ".join(contracts.WORKERS))
    if not isinstance(profile.get("models"), dict) or not profile["models"]:
        raise WorkError("runner-profile-invalid",
                        "profile.models must map derived model tokens to "
                        "executable worker model values")
    return profile


def compile_executable_contract(state_dir, derived, profile) -> dict:
    """Derived stage contract → hub-registered executable contract.

    Returns the executable contract plus both content hashes and the
    ``attach_to`` dispatch metadata (which is NOT part of the executable —
    it only selects the delegate action).
    """
    if not isinstance(derived, dict) or derived.get("schema_version") != DERIVED_SCHEMA:
        raise WorkError("derived-contract-invalid",
                        "the durable runner accepts only "
                        f"{DERIVED_SCHEMA} stage contracts")
    unknown = set(derived) - _DERIVED_KEYS - _DISPATCH_METADATA_KEYS
    bypass = set(derived) & _FORBIDDEN_METADATA_KEYS
    if bypass:
        raise WorkError(
            "derived-contract-invalid",
            f"raw session fields {sorted(bypass)} are refused: session "
            "continuation must go through attach_to so the record checks "
            "(worker, workspace, prior session) stay in force")
    if unknown:
        raise WorkError("derived-contract-invalid",
                        f"unknown derived fields: {sorted(unknown)}")
    missing = {"workspace", "prompt", "model", "allowed_files", "timeout_sec",
               "command", "contract_id", "stage"} - set(derived)
    if missing:
        raise WorkError("derived-contract-invalid",
                        f"derived contract is missing fields: {sorted(missing)}")
    attach_to = derived.get("attach_to")
    if attach_to is not None and (not isinstance(attach_to, str) or not attach_to.strip()):
        raise WorkError("derived-contract-invalid",
                        "attach_to must be a non-empty prior hub task id")

    token = derived["model"]
    model = profile["models"].get(token)
    if model is None:
        raise WorkError("model-unresolved",
                        f"derived model token {token!r} has no executable "
                        "mapping in the host worker profile")

    timeout_ms = derived["timeout_sec"] * 1000
    executable = {
        "contract_id": "",  # content-addressed below
        "worker": profile["worker"],
        "workspace": derived["workspace"],
        "prompt": derived["prompt"],
        "allowed_files": list(derived["allowed_files"]),
        # The stage command is capability, not obligation, for ordinary
        # stages: the argv template was already path-checked by the compiler.
        # A verification stage sets require_command on its binding: there the
        # command IS the check, and a zero-tool turn must fail delivery
        # instead of being accepted on the model's word.
        "commands": [{
            "id": f"stage-{derived['stage']}",
            "command": shlex.join(derived["command"]["argv"]),
            "max_uses": 1,
            "max_timeout_ms": timeout_ms,
            "required": bool(derived.get("require_command")),
        }],
        "model": model,
        # The owner re-verifies this hash against the artifact store before
        # any dispatch intent reaches the hub; an executable that dropped it
        # would be unverifiable the moment it left the compiler.
        "authorization_sha256": derived["authorization_sha256"],
        "max_timeout_ms": timeout_ms,
        "budget": profile.get("budget")
        or {"note": "compiled from zsc.derived-contract/v1 by the persistent owner"},
    }
    for optional in ("mode", "skills", "sandbox", "output_schema"):
        if profile.get(optional) is not None:
            executable[optional] = profile[optional]
    executable["contract_id"] = "exec-" + canonical_sha256(executable)[:28]

    state_dir = Path(state_dir)
    contracts_dir = contracts.contracts_dir(state_dir)
    contracts_dir.mkdir(parents=True, exist_ok=True)
    staging = contracts_dir / f".staging-{executable['contract_id']}.json"
    try:
        staging.write_text(json.dumps(executable, ensure_ascii=False))
        contracts.register_contract(state_dir, staging)
    finally:
        with contextlib.suppress(FileNotFoundError):
            staging.unlink()
    return {
        "contract": executable,
        "derived_sha256": canonical_sha256(derived),
        "executable_sha256": contracts.contract_digest(executable),
        "attach_to": attach_to,
    }


def plan_dispatch(state_dir, derived, profile) -> dict:
    """Enqueue-time companion to the runner.

    Compiles and registers the executable contract (deterministic and
    idempotent) and returns the hash the durable intent must BIND:
    the executable hash (design §7 — derived and executable hashes are
    both recorded, the latter rides on the dispatch intent).
    """
    adaptation = compile_executable_contract(state_dir, derived, profile)
    return {
        "contract_sha256": adaptation["executable_sha256"],
        "derived_sha256": adaptation["derived_sha256"],
        "executable_contract_id": adaptation["contract"]["contract_id"],
    }


class WorkRunner:
    """Dispatcher runner driving the hub delegate under durable identity.

    The shared :class:`ResidentWorkers` holder lives for the OWNER's
    lifetime (created once, settled on :meth:`close` — not per dispatch).
    """

    def __init__(self, config, evidence_root, profile, *, codex_path=None,
                 zcode_runner=None, codex_runner=None, dsh_runner=None,
                 resident_factory=None, appserver_factory=None,
                 appserver_driver_factory=None, appserver_concurrency=0,
                 ui_emitter=None, artifact_root=None):
        self._config = config
        self._evidence_root = Path(evidence_root)
        self._profile = resolve_profile(profile)
        # Where host-registered authorizations live (the owner's artifact
        # store). Dispatch is REFUSED until the executable contract is
        # traced back to one of these documents.
        self._artifact_root = Path(artifact_root) if artifact_root is not None \
            else None
        self._event_loop = None
        self._loop_thread = None
        self._loop_lock = threading.Lock()
        if ui_emitter is None:
            from . import ui_projection
            ui_emitter = ui_projection.default_emitter()
        self._injectables = {
            name: value for name, value in {
                "codex_path": codex_path,
                "zcode_runner": zcode_runner,
                "codex_runner": codex_runner,
                "dsh_runner": dsh_runner,
                "resident_factory": resident_factory,
                "appserver_factory": appserver_factory,
                "appserver_driver_factory": appserver_driver_factory,
                "appserver_concurrency": appserver_concurrency,
            }.items() if value is not None
        }
        self._workers = ResidentWorkers(
            config, self._evidence_root, codex_path=codex_path,
            resident_factory=resident_factory,
            appserver_factory=appserver_factory,
            appserver_driver_factory=appserver_driver_factory,
            appserver_concurrency=appserver_concurrency,
            ui_emitter=ui_emitter)

    # ------------------------------------------------------ authorization

    def _load_authorization(self, digest) -> dict:
        """Load the REGISTERED authorization this contract claims to follow.

        A missing document, a hash mismatch or an invalid document is a
        refusal: a self-declared hash is not an authorization.
        """
        if self._artifact_root is None:
            raise WorkError(
                "authorization-unverified",
                "the runner has no artifact root, so no registered work "
                "authorization can be loaded; refusing to execute")
        if not isinstance(digest, str) or len(digest) != 64:
            raise WorkError("authorization-unverified",
                            "the contract carries no authorization hash")
        path = self._artifact_root / "sha256" / digest[:2] / digest
        try:
            data = path.read_bytes()
        except OSError as error:
            raise WorkError(
                "authorization-unverified",
                f"no registered work authorization for {digest[:12]}…; the "
                "contract must be compiled from a host-registered "
                "authorization") from error
        if hashlib.sha256(data).hexdigest() != digest:
            raise WorkError("authorization-unverified",
                            "the authorization object's bytes do not match its "
                            "content hash")
        try:
            document = json.loads(data)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise WorkError("authorization-unverified",
                            "the authorization object is not valid JSON") from error
        return validate_authorization(document)

    def _verify_authorization(self, derived, contract) -> dict:
        """Cross-check the executable contract against its authorization."""
        authorization = self._load_authorization(derived.get("authorization_sha256"))
        # the same re-derivation the owner applies before queueing: the
        # runner must not trust that the owner was the only writer
        verify_against_authorization(authorization, derived)
        policy = authorization.get("workspace_policy", "isolated_candidate")
        root = Path(authorization["bound_workspace"] if policy == "bound_existing"
                    else authorization["candidate_parent"]).resolve(strict=False)
        workspace = Path(contract["workspace"]).resolve(strict=False)
        try:
            workspace.relative_to(root)
        except ValueError as error:
            raise WorkError("authorization-unverified",
                            "the executable workspace escapes the authorized "
                            "root") from error
        if policy == "bound_existing" and workspace != root:
            raise WorkError("authorization-unverified",
                            "a bound_existing authorization runs in exactly its "
                            "bound workspace")
        if policy != "bound_existing" and workspace == root:
            raise WorkError("authorization-unverified",
                            "the workspace must be a child of the candidate parent")
        allowed = set(contract.get("allowed_files") or [])
        writable = set(authorization["writable_relative_paths"])
        # An empty set is a READ-ONLY stage (recon/verification): it expands
        # nothing. Only names outside the authorization are refused.
        if not allowed <= writable:
            raise WorkError("authorization-unverified",
                            "allowed_files expand the authorized write scope")
        for item in contract.get("commands") or []:
            argv = shlex.split(item.get("command") or "")
            if not argv:
                raise WorkError("authorization-unverified", "empty stage command")
            for argument in argv:
                if not argument.startswith("/"):
                    continue
                try:
                    Path(argument).resolve(strict=False).relative_to(root)
                except ValueError as error:
                    raise WorkError(
                        "authorization-unverified",
                        f"stage command path {argument!r} escapes the "
                        "authorized root") from error
        if derived.get("model") not in authorization["model_allowlist"]:
            raise WorkError("authorization-unverified",
                            "the derived model token is outside the authorized "
                            "allowlist")
        limits = authorization["limits"]
        for field in ("timeout_sec", "max_timeout_ms", "concurrency"):
            if field in limits and derived.get(field) is not None:
                if derived[field] > limits[field]:
                    raise WorkError("authorization-unverified",
                                    f"{field} exceeds the authorized limit")
        return authorization

    # ------------------------------------------------------------ event loop

    def _loop(self):
        with self._loop_lock:
            if self._event_loop is None or self._event_loop.is_closed():
                loop = asyncio.new_event_loop()
                thread = threading.Thread(target=loop.run_forever,
                                          name="zsc-work-runner", daemon=True)
                thread.start()
                self._event_loop, self._loop_thread = loop, thread
            return self._event_loop

    def _run(self, coroutine):
        """Run on the runner's OWN long-lived loop.

        A per-call ``asyncio.run`` would destroy the resident reader's loop
        between rounds (dropping shared app-server/zcode subscriptions), so
        the loop outlives every dispatch and is stopped only by ``close``.
        """
        return asyncio.run_coroutine_threadsafe(coroutine, self._loop()).result()

    def __call__(self, binding, contract):
        adaptation = compile_executable_contract(
            Path(self._config.state_dir), contract, self._profile)
        if binding.contract_sha256 != adaptation["executable_sha256"]:
            # The durable intent must bind the EXECUTABLE hash; anything
            # else means the enqueue side skipped plan_dispatch (or the
            # derived content drifted after enqueue) — refuse loudly.
            raise WorkError(
                "dispatch-contract-mismatch",
                "the durable intent was not bound to the executable "
                "contract compiled from this derived contract; enqueue "
                "through plan_dispatch")
        self._verify_authorization(contract, adaptation["contract"])
        action = "attach" if adaptation["attach_to"] else "start"
        try:
            result = self._run(delegate_impl(
                self._config, self._evidence_root,
                adaptation["contract"]["contract_id"],
                {"agent": "zsc-work-owner", "session_id": binding.intent_id},
                action=action, attach_to=adaptation["attach_to"],
                dispatch_binding=binding, workers=self._workers,
                **self._injectables))
        except HubError as error:
            # Refusals keep their hub code (worker-disabled, unknown-task,
            # workspace mismatch, ...) — the attempt fails loudly, never as
            # a phantom completed run.
            raise WorkError(error.code, str(error)) from error
        status = "completed" if result.get("outcome") in ("delivered", "completed") \
            else "failed"
        outcome = {
            "status": status,
            "task_id": result.get("task_id"),
            "outcome": result.get("outcome"),
            "session_locator": result.get("session_locator"),
            "evidence_dir": result.get("evidence_dir"),
            "derived_contract_sha256": adaptation["derived_sha256"],
            "executable_contract_sha256": adaptation["executable_sha256"],
        }
        for key in ("delivery_ok", "changed_files", "final_message", "error"):
            if result.get(key) is not None:
                outcome[key] = result[key]
        return outcome

    def close(self):
        """Settle the shared worker holder, then stop the runner's loop."""
        with contextlib.suppress(Exception):
            self._run(self._workers.close())
        with self._loop_lock:
            loop, thread = self._event_loop, self._loop_thread
            self._event_loop = self._loop_thread = None
        if loop is not None:
            loop.call_soon_threadsafe(loop.stop)
        if thread is not None:
            thread.join(timeout=5)


def build_work_runner(config, evidence_root, *, profile=None, **injectables) -> WorkRunner:
    """Build the production runner for a persistent owner.

    ``profile`` is REQUIRED for real dispatch (explicit host worker
    configuration — worker, mode, model mapping); ``None`` fails at build
    time so an owner never starts with a half-configured executor.
    """
    return WorkRunner(config, evidence_root, profile, **injectables)
