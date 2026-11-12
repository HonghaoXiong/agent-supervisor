"""Independent local work owner and bounded local control protocol."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import secrets
import socket
import sys
import threading
import uuid
from pathlib import Path

from .artifact_store import ArtifactStore
from .capability_registry import discover_capabilities
from .collaboration import CollaborationService
from .contract_compile import verify_against_authorization
from .knowledge_store import KnowledgeStore
from .work_authorization import register_work_authorization, validate_authorization
from .work_dispatch import WorkDispatcher
from .work_models import ActorContext, WorkError
from .work_policy import choose_route
from .work_store import WorkStore
from .work_contracts import canonical_sha256


FRAME_LIMIT = 1024 * 1024

# Session-loop control surface. Control identity is TOKEN-bound at the
# connection-message level for v1: bind/pause/resume/unbind/advance/adopt
# demand the host control token; decision submission additionally accepts
# the reviewer token (which can NEVER bind, pause or adopt). Request-body
# "actor" fields are ignored — possession of the token IS the identity.
_LOOP_METHODS = frozenset({
    "bind_session", "claim_native_session", "session_status",
    "pause_session", "resume_session", "unbind_session",
    "advance_session_loop", "submit_review_decision", "adopt_existing_work",
})
_LOOP_CONTROL_METHODS = frozenset({
    "bind_session", "claim_native_session", "pause_session",
    "resume_session", "unbind_session", "advance_session_loop",
    "adopt_existing_work",
})

# Every method that mutates work state, registers an authorization or can
# cause an execution requires the host control token — the same credential
# for the whole surface, checked before any handler runs. Read-only
# projections (ping/status/work_status/read_artifact/knowledge search) stay
# open to the owner's read-only peers.
_MUTATING_METHODS = frozenset({
    "create_work", "authorize", "start_recon", "start_implementation",
    "start_verification", "submit_brief", "accept_design", "submit_delivery",
    "record_acceptance", "rework_work", "cancel_work", "reconcile_work",
    "enqueue", "cancel", "reconcile", "propose_knowledge", "review_knowledge",
    "revalidate_knowledge",
})


class WorkOwner:
    METHODS = frozenset({
        "create_work", "enqueue", "status", "cancel", "reconcile", "ping", "shutdown",
        "authorize", "work_capabilities", "start_recon", "start_implementation",
        "start_verification", "submit_brief", "accept_design", "submit_delivery",
        "record_acceptance", "rework_work", "cancel_work", "reconcile_work", "work_status",
        "read_artifact", "search_knowledge", "revalidate_knowledge",
        "propose_knowledge", "review_knowledge",
    }) | _LOOP_METHODS

    def __init__(self, state_root, *, store=None, runner=None, max_workers=4,
                 capability_config=None, routing_policy=None, routing_history=None,
                 work_journal=None, session_loop_factory=None,
                 control_token=None, review_token=None, hub_state_dir=None,
                 pump_interval_s=1.0):
        self.root = Path(state_root).expanduser().resolve()
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.root.chmod(0o700)
        self.socket_path = self.root / "control.sock"
        self.hub_state_dir = Path(hub_state_dir) if hub_state_dir is not None else None
        self._pump_interval_s = pump_interval_s
        self._pump = None
        self.store = store or WorkStore.open(self.root)
        self._session_loop_factory = session_loop_factory
        self._session_loop = None
        self._control_token = control_token
        self._review_token = review_token
        self.actor = ActorContext("local-work-owner", {
            "work:create", "work:action", "work:authorize", "design:accept",
            "verification:start", "acceptance:record", "knowledge:review"}, host_bound=True)
        self.knowledge_producer = ActorContext("local-knowledge-producer", {"knowledge:propose"},
                                                host_bound=True, session_id="owner-knowledge-producer")
        self.knowledge = KnowledgeStore(self.root / "knowledge")
        self.collaboration = CollaborationService(self.store)
        self.dispatcher = WorkDispatcher(self.store, runner or self._missing_runner,
                                         max_workers=max_workers)
        self.capability_config = capability_config or {}
        self.routing_policy = routing_policy
        self.routing_history = routing_history or {}
        self.routing_decisions = {}
        self.artifacts = ArtifactStore(self.root / "objects")
        self._server = None
        self._stopped = threading.Event()
        self._connection_lock = threading.Lock()
        self.work_journal = work_journal
        self.work_projection_errors = {}
        if self.work_journal is not None:
            for work_id in self.store.list_work_ids():
                self._project_work(work_id)

    @staticmethod
    def _missing_runner(binding, contract):
        raise WorkError("runner-unconfigured",
                        "no production runner is configured for this independent owner")

    # ------------------------------------------------------------ credentials

    def _control_token_value(self) -> str:
        """The host control token: configured, or minted once per state root.

        When the host does not configure one, the owner mints a random token
        and persists it with mode 0600; every mutating request must present
        it. Possession of the file is the host's own credential — this
        defends against other processes, not against the same UID.
        """
        if isinstance(self._control_token, str) and self._control_token:
            return self._control_token
        path = self.root / "control.token"
        if not path.exists():
            fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                handle.write(secrets.token_hex(32))
        # cache it: every control path must compare against the SAME value,
        # including the one this owner just minted
        self._control_token = path.read_text(encoding="utf-8").strip()
        return self._control_token

    def _adopt_native_claim(self, binding, params):
        """Let a claimed native session adopt an existing work.

        The claim alone proves only the session identity: it is observe +
        review. A work becomes reviewable only when the host supplies its
        design and code_root carries a complete candidate fingerprint. Both
        are recorded as adoption — never as a fabricated hub task.
        """
        design = params.get("design")
        work_id = binding.get("work_id")
        if not isinstance(design, dict) or not design.get("sha256"):
            return binding
        from .session_binding import build_candidate
        context = self.collaboration.context(work_id)
        if context.get("state") == "created":
            self.collaboration.apply_action(
                work_id, "start_adoption",
                {"source": {"binding_id": binding["binding_id"],
                            "session_id": binding["session_id"]}},
                expected_revision=context["revision"],
                idempotency_key=f"native-adopt-{binding['binding_id']}",
                actor=self.actor)
            context = self.collaboration.context(work_id)
        if context.get("state") != "adoption_pending":
            raise WorkError(
                "state-conflict",
                f"native adoption cannot start from {context.get('state')!r}")
        candidate = build_candidate(binding["workspace"])
        if candidate.get("coverage") != "complete" \
                or not candidate.get("sha256"):
            raise WorkError(
                "candidate-coverage-incomplete",
                "the claimed code_root has no complete candidate fingerprint")
        contributors = params.get("contributors") or [
            {"actor_id": "zcode", "session_id": binding["session_id"]}]
        self.collaboration.apply_action(
            work_id, "adopt_candidate", {
                "design": design,
                "candidate": {"sha256": candidate["sha256"]},
                "candidate_manifest": candidate.get("files") or {},
                "source": {"binding_id": binding["binding_id"],
                           "session_id": binding["session_id"]},
                "contributors": contributors},
            expected_revision=context["revision"],
            idempotency_key=f"native-adopt-candidate-{binding['binding_id']}",
            actor=self.actor)
        return binding

    def _require_control(self, params):
        import hmac as _hmac
        token = params.get("control_token")
        expected = self._control_token_value()
        if not (isinstance(token, str) and _hmac.compare_digest(token, expected)):
            raise WorkError("actor-untrusted",
                            "this method changes work state and requires the "
                            "owner's control token")
        params.pop("control_token", None)
        params.pop("actor", None)

    def _verify_intent_authorization(self, intent):
        """Refuse to enqueue an intent that no registered authorization backs."""
        contract = intent.get("contract") if isinstance(intent, dict) else None
        if not isinstance(contract, dict):
            raise WorkError("authorization-unverified", "the intent carries no contract")
        digest = contract.get("authorization_sha256")
        if not isinstance(digest, str) or len(digest) != 64:
            raise WorkError("authorization-unverified",
                            "the contract carries no authorization hash")
        try:
            data = (self.artifacts.root / "sha256" / digest[:2] / digest).read_bytes()
        except OSError as error:
            raise WorkError(
                "authorization-unverified",
                f"no registered work authorization for {digest[:12]}…") from error
        if hashlib.sha256(data).hexdigest() != digest:
            raise WorkError("authorization-unverified",
                            "the authorization object does not match its hash")
        document = validate_authorization(json.loads(data))
        # The hash proves this authorization EXISTS. Re-derive the contract
        # from it so a caller cannot keep the hash and swap the stage, the
        # program or the write scope.
        verify_against_authorization(document, contract)
        return document

    def start(self):
        if self.socket_path.exists() or self.socket_path.is_symlink():
            if self.socket_path.is_symlink():
                raise WorkError("service-already-running", "refusing to replace a control-socket symlink")
            try:
                ControlClient(self.socket_path).request("ping", {}, timeout=0.2)
            except WorkError as error:
                if error.code != "service-unavailable":
                    raise WorkError("service-already-running", "inspect the existing owner/control socket") from error
                # WorkStore's exclusive lock proves no live owner remains; remove only its stale socket.
                self.socket_path.unlink()
            else:
                raise WorkError("service-already-running", "inspect the existing owner/control socket")
        self._server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self._server.bind(str(self.socket_path))
        os.chmod(self.socket_path, 0o600)
        self._server.listen(32)
        self._server.settimeout(0.2)
        self._control_token_value()  # mint/persist the credential before serving
        threading.Thread(target=self._accept_loop, name="zsc-work-owner", daemon=True).start()
        self._start_pump()
        return self

    def _session_loop_instance(self):
        if self._session_loop is None:
            if self._session_loop_factory is None:
                raise WorkError("loop-unconfigured",
                                "the owner was started without a session-loop "
                                "configuration (runner-config session_loop section)")
            self._session_loop = self._session_loop_factory(self)
        return self._session_loop

    def _start_pump(self):
        """Watch every bound session from the owner itself (no external calls)."""
        if self._session_loop_factory is None or self.hub_state_dir is None:
            return
        from .session_loop import SessionPump
        self._pump = SessionPump(
            self._session_loop_instance, self.store,
            interval_s=self._pump_interval_s,
            logger=lambda message: print(f"session-pump: {message}",
                                         file=sys.stderr, flush=True))
        self._pump.start()

    def _accept_loop(self):
        while not self._stopped.is_set():
            try:
                connection, _ = self._server.accept()
            except socket.timeout:
                continue
            except OSError:
                break
            threading.Thread(target=self._serve_connection, args=(connection,), daemon=True).start()

    def _serve_connection(self, connection):
        with connection:
            try:
                if hasattr(connection, "getpeereid"):
                    uid, gid = connection.getpeereid()
                    if uid != os.getuid():
                        raise WorkError("peer-untrusted", "local peer uid differs from owner")
                elif hasattr(socket, "SO_PEERCRED"):
                    import struct
                    credentials = connection.getsockopt(socket.SOL_SOCKET, socket.SO_PEERCRED, 12)
                    pid, uid, gid = struct.unpack("3i", credentials)
                    if uid != os.getuid():
                        raise WorkError("peer-untrusted", "local peer uid differs from owner")
                connection.settimeout(30)
                stream = connection.makefile("rwb")
                with stream:
                    for raw in stream:
                        if len(raw) > FRAME_LIMIT:
                            raise WorkError("frame-too-large", "control frame exceeds 1 MiB")
                        try:
                            request = json.loads(raw)
                            if not isinstance(request, dict) or request.get("v") != 1 \
                                    or not isinstance(request.get("request_id"), str) \
                                    or not isinstance(request.get("params"), dict):
                                raise WorkError("invalid-argument", "invalid or unadvertised control request")
                            if request.get("method") not in self.METHODS:
                                raise WorkError("unknown-method", "control method is not advertised")
                            result = self._invoke(request["method"], request["params"])
                            response = {"v": 1, "request_id": request["request_id"], "ok": True,
                                        "result": result}
                        except Exception as error:  # noqa: BLE001 - a handler fault
                            # must surface as an error frame on THIS request,
                            # never as a silently dropped connection
                            code = getattr(error, "code", None)
                            if code is None:
                                code = ("invalid-argument"
                                        if isinstance(error, (ValueError, OSError))
                                        else "owner-internal-error")
                            response = {"v": 1, "request_id": locals().get("request", {}).get("request_id"),
                                        "ok": False,
                                        "error": {"code": code,
                                                  "message": f"{type(error).__name__}: {error}"[:500]}}
                        payload = (json.dumps(response, ensure_ascii=False) + "\n").encode("utf-8")
                        if len(payload) > FRAME_LIMIT:
                            payload = b'{"v":1,"error":{"code":"response-too-large"}}\n'
                        stream.write(payload)
                        stream.flush()
                        if isinstance(locals().get("request"), dict) \
                                and request.get("method") == "shutdown" and "result" in response:
                            self._stopped.set()
                            return
            except (WorkError, OSError, ValueError):
                return

    def _invoke(self, method, params):
        result = self._invoke_inner(method, params)
        if self.work_journal is not None and method in {
                "create_work", "start_recon", "start_implementation", "start_verification",
                "submit_brief", "accept_design", "submit_delivery", "record_acceptance",
                "rework_work", "cancel_work", "reconcile_work"}:
            work_id = params.get("work_id") or result.get("work_id")
            self._project_work(work_id)
        return result

    def _invoke_loop(self, method, params):
        """Token-gated session-loop control surface.

        The control token authorizes bind/pause/resume/unbind/advance/adopt;
        decision submission additionally accepts the reviewer token, which
        can NEVER reach the control methods. A request-body "actor" field
        changes nothing — possession of the token is the identity, and a
        missing token configuration fails closed for every loop method.
        """
        import hmac as _hmac
        token = params.get("control_token")
        tokens_ok = isinstance(self._control_token, str) and isinstance(token, str) \
            and _hmac.compare_digest(token, self._control_token)
        review_ok = isinstance(self._review_token, str) and isinstance(token, str) \
            and _hmac.compare_digest(token, self._review_token) \
            and method == "submit_review_decision"
        if method == "session_status":
            pass  # read-only projection of binding/round state
        elif not (tokens_ok or review_ok):
            raise WorkError("actor-untrusted",
                            "session-loop control requires the host control "
                            "token (or the reviewer token for decisions)")
        params.pop("control_token", None)
        params.pop("actor", None)  # body actor fields are never trusted

        if method in ("bind_session", "claim_native_session", "pause_session",
                      "resume_session", "unbind_session"):
            from . import session_binding
            if method == "bind_session":
                return session_binding.bind_session(
                    self.store, params, self.actor,
                    hub_state_dir=self.hub_state_dir)
            if method == "claim_native_session":
                binding = session_binding.claim_native_session(
                    self.store, params, self.actor,
                    hub_state_dir=self.hub_state_dir)
                return self._adopt_native_claim(binding, params)
            handler = {"pause_session": session_binding.pause_session,
                       "resume_session": session_binding.resume_session,
                       "unbind_session": session_binding.unbind_session}[method]
            return handler(self.store, params, self.actor)
        if method == "session_status":
            binding_id = params.get("binding_id")
            binding = self.store.get_session_binding(binding_id) if binding_id else None
            if binding is None:
                return {"bindings": [
                    {"binding_id": item["binding_id"], "session_id": item["session_id"],
                     "state": item["state"], "mode": item["mode"]}
                    for item in self.store.session_bindings(params.get("session_id") or "")
                    or []] if params.get("session_id") else []}
            return {"binding": {key: binding[key] for key in
                                ("binding_id", "session_id", "state", "mode",
                                 "workspace", "workspace_policy", "cursor",
                                 "capabilities")},
                    "rounds": [{"round_id": item["round_id"], "seq": item["seq"],
                                "state": item["state"],
                                "candidate_sha256": item["candidate_sha256"],
                                "decision": item.get("decision"),
                                "instruction": item.get("instruction"),
                                "verification": item.get("verification")}
                               for item in self.store.session_rounds(binding_id)]}
        # loop-consuming methods need the configured SessionLoop
        session_loop = self._session_loop_instance()
        if method == "advance_session_loop":
            return session_loop.advance(params.get("binding_id"),
                                        params.get("source") or None)
        if method == "submit_review_decision":
            reviewer_actor = ActorContext(
                "session-loop-reviewer", {"review:submit"},
                host_bound=True, session_id="reviewer-credential")
            return session_loop.submit_decision(
                params.get("decision"), reviewer_actor)
        if method == "adopt_existing_work":
            return session_loop.adopt_existing_work(params, self.actor)
        raise WorkError("unknown-method", "unreachable loop method")

    def _invoke_inner(self, method, params):
        if method == "ping":
            return {"pid": os.getpid(), "protocol_version": 1}
        if method == "shutdown":
            return {"stopping": True}
        if method in _LOOP_METHODS:
            return self._invoke_loop(method, params)
        if method in _MUTATING_METHODS:
            self._require_control(params)
        if method == "create_work":
            created = self.collaboration.create_work(params.get("spec"),
                idempotency_key=params.get("idempotency_key"), actor=self.actor)
            decision = self._plan("plan", params)
            if decision is not None:
                created["planning"] = decision
                self.routing_decisions[created["work_id"]] = {"plan": decision}
            return created
        if method == "authorize":
            return register_work_authorization(params.get("document"), self.actor,
                                               artifact_store=self.artifacts)
        if method == "work_capabilities":
            return discover_capabilities(self.capability_config, params.get("request", params))
        if method in ("start_recon", "start_implementation", "start_verification"):
            return self._start(method, params)
        if method in ("submit_brief", "accept_design", "submit_delivery"):
            return self._apply(method, params)
        if method == "record_acceptance":
            accepted = self._apply("record_acceptance", params)
            completion = self.collaboration.apply_action(params.get("work_id"), "complete", {
                "candidate_sha256": params.get("payload", {}).get("candidate_sha256")},
                expected_revision=accepted["revision"],
                idempotency_key=str(params.get("idempotency_key")) + ":complete", actor=self.actor)
            return {"acceptance": accepted, "completion": completion,
                    "work_id": completion["work_id"], "revision": completion["revision"],
                    "state": completion["state"]}
        if method == "rework_work":
            return self._apply("rework", params)
        if method == "cancel_work":
            result = self._apply("cancel", params)
            attempt_id = params.get("payload", {}).get("attempt_id")
            result["execution"] = self.dispatcher.cancel(attempt_id)
            return result
        if method == "reconcile_work":
            work_id = params.get("work_id")
            recovery = self.dispatcher.reconcile(work_id)
            if recovery["status"] == "needs_reconciliation" and "expected_revision" in params:
                result = self._apply("reconcile", params)
                result["execution"] = recovery
                return result
            return {"work_id": work_id, "state": recovery["status"], "execution": recovery}
        if method in ("work_status", "status"):
            if method == "status":
                return self.dispatcher.status(params.get("attempt_id"))
            return self._work_status(params)
        if method == "read_artifact":
            detail = params.get("detail", "full")
            default_limit = {"summary": 4096, "index": 16384, "full": 65536}.get(detail)
            if default_limit is None:
                raise WorkError("invalid-argument", "artifact detail must be summary/index/full")
            return self.artifacts.read(params.get("ref"), cursor=params.get("cursor"),
                max_bytes=params.get("max_bytes", default_limit))
        if method == "search_knowledge":
            query = params.get("query", {"text": params.get("text", "")})
            scope = params.get("scope") or {"project_id": params.get("project_id")}
            return self.knowledge.search(query, scope, params.get("cursor"))
        if method == "propose_knowledge":
            document = params.get("document") or params.get("claim")
            return self.knowledge.propose_claim(document, self.knowledge_producer)
        if method == "review_knowledge":
            ref = params.get("ref") or {"claim_id": params.get("claim_id")}
            decision = params.get("decision")
            return self.knowledge.review_claim(ref, decision, self.actor)
        if method == "revalidate_knowledge":
            ref = params.get("ref") or {"claim_id": params.get("claim_id")}
            return self.knowledge.revalidate(ref, params.get("current_manifest"))
        if method == "enqueue":
            self._verify_intent_authorization(params.get("intent"))
            handle = self.dispatcher.enqueue(params.get("intent"))
            return {"attempt_id": handle.attempt_id, "task_id": handle.task_id,
                    "intent_id": handle.intent_id}
        if method == "cancel":
            return self.dispatcher.cancel(params.get("attempt_id"))
        if method == "reconcile":
            return self.dispatcher.reconcile(params.get("work_id"))
        raise WorkError("invalid-argument", f"unadvertised method {method}")

    def _apply(self, action, params):
        return self.collaboration.apply_action(params.get("work_id"), action,
            params.get("payload"), expected_revision=params.get("expected_revision"),
            idempotency_key=params.get("idempotency_key"), actor=self.actor)

    def _plan(self, stage, params):
        if self.routing_policy is None:
            return None
        intent = params.get("intent") if isinstance(params.get("intent"), dict) else {}
        contract = intent.get("contract") if isinstance(intent.get("contract"), dict) else {}
        worker = contract.get("worker", "zcode")
        capabilities = discover_capabilities(self.capability_config, {"worker": worker})
        for key in ("models", "roster"):
            if key in self.capability_config:
                capabilities[key] = self.capability_config[key]
        payload = params.get("payload") if isinstance(params.get("payload"), dict) else {}
        supplied = params.get("task") if isinstance(params.get("task"), dict) else {}
        task = {**supplied, "stage": stage,
                "session_id": supplied.get("session_id", payload.get("session_id")),
                "brief": supplied.get("brief", payload.get("brief"))}
        decision = choose_route(task, capabilities, self.routing_policy, self.routing_history)
        if decision["status"] != "planned":
            raise WorkError("model-unqualified", "no roster model satisfies the routing policy",
                            decision["upgrade_conditions"])
        return decision

    def _start(self, method, params):
        action = {"start_recon": "start_recon", "start_implementation": "start_implementation",
                  "start_verification": "start_verification"}[method]
        lifecycle = params.get("payload")
        intent = params.get("intent")
        if not isinstance(lifecycle, dict) or not isinstance(intent, dict) \
                or intent.get("work_id") != params.get("work_id") \
                or intent.get("attempt_id") != lifecycle.get("attempt_id"):
            raise WorkError("invalid-argument", "lifecycle and durable intent bindings differ")
        decision = self._plan(action.replace("start_", ""), params)
        if decision is not None:
            lifecycle = {**lifecycle, "routing_decision": decision}
            params = {**params, "payload": lifecycle}
            contract = dict(intent["contract"])
            selected = decision["selected"]
            worker = contract.get("worker")
            model_value = selected["model"] if worker == "zcode" else selected["model_id"]
            if worker not in ("zcode", "codex"):
                raise WorkError("policy-conflict", "routing currently compiles only zcode/codex model fields")
            if "model" in contract and contract["model"] != model_value:
                raise WorkError("policy-conflict", "contract model differs from the routing decision")
            contract["model"] = model_value
            intent = {**intent, "contract": contract, "contract_sha256": canonical_sha256(contract)}
            params["intent"] = intent
        accepted = self._apply(action, params)
        self._verify_intent_authorization(intent)
        handle = self.dispatcher.enqueue(intent)
        current = accepted
        if method in ("start_recon", "start_implementation"):
            running_action = "recon_running" if method == "start_recon" else "implementation_running"
            current = self.collaboration.apply_action(params.get("work_id"), running_action,
                {"attempt_id": handle.attempt_id}, expected_revision=accepted["revision"],
                idempotency_key=str(params.get("idempotency_key")) + ":running", actor=self.actor)
        return {**current, "accepted_revision": accepted["revision"],
                "request_accepted": True, "execution_status": "accepted",
                "planning": decision,
                "attempt_id": handle.attempt_id, "task_id": handle.task_id,
                "intent_id": handle.intent_id}

    def _work_status(self, params):
        result = self.collaboration.context(params.get("work_id"))
        work_id = params.get("work_id")
        result["work_id"] = work_id
        if work_id in self.routing_decisions:
            result["planning"] = self.routing_decisions[work_id]
        attempt_id = params.get("attempt_id") or result.get("attempt_id")
        if attempt_id:
            try:
                result["execution"] = self.dispatcher.status(attempt_id)
            except WorkError as error:
                if error.code != "attempt-missing":
                    raise
                result["execution"] = {"attempt_id": attempt_id, "status": "not_enqueued"}
        result["next_actions"] = {
            "created": ["start_recon", "start_adoption"], "recon_queued": ["recon_running", "work_status"],
            "recon_running": ["submit_brief", "work_status"], "brief_ready": ["accept_design"],
            "design_ready": ["start_implementation"],
            "adoption_pending": ["adopt_candidate", "work_status"],
            "implementation_queued": ["implementation_running", "work_status"],
            "implementation_running": ["submit_delivery", "work_status"],
            "candidate_ready": ["start_verification"],
            "verification_running": ["record_acceptance", "work_status"],
            "rework_required": ["start_implementation", "start_recon"],
            "completed": ["work_status"],
            "cancelled": ["work_status"], "needs_reconciliation": ["reconcile_work"],
        }.get(result["state"], [])
        return result

    def _project_work(self, work_id):
        if self.work_journal is None or not work_id:
            return
        try:
            self.work_journal.ingest(self._work_status({"work_id": work_id}))
            self.work_projection_errors.pop(work_id, None)
        except (ValueError, TypeError) as error:
            # A committed business mutation must not be rolled back by a display failure.
            self.work_projection_errors[work_id] = getattr(error, "code", "projection-invalid")[:80]

    def wait(self):
        self._stopped.wait()

    def stop(self):
        self._stopped.set()
        if self._pump is not None:
            self._pump.stop()
        if self._server is not None:
            self._server.close()
        runner = getattr(self.dispatcher, "runner", None)
        if hasattr(runner, "close"):
            # Real-executor runners own shared worker holders; settle them
            # with the owner (never per client connection).
            try:
                runner.close()
            except Exception:  # noqa: BLE001 - shutdown must complete anyway
                pass
        self.dispatcher.close()
        self.knowledge.close()
        self.store.close()
        try:
            self.socket_path.unlink()
        except FileNotFoundError:
            pass


class WorkUiHost:
    """Production composition of the private owner and its read-only UI socket."""

    def __init__(self, state_root, *, socket_path=None, owner=None, owner_options=None):
        from .ui_projection import UiEmitter
        from .ui_journal import UiJournal
        from .ui_server import UiEventServer
        from .ui_work import UiWorkJournal
        self.root = Path(state_root).resolve(strict=False)
        self.work_journal = UiWorkJournal()
        self.owner = owner or WorkOwner(self.root, work_journal=self.work_journal,
                                        **dict(owner_options or {}))
        identity = hashlib.sha256(str(self.root).encode("utf-8")).hexdigest()[:16]
        emitter = UiEmitter(hub_instance_id=f"work-{identity}", journal_epoch=1, enabled=True)
        self.legacy_journal = UiJournal(emitter)
        self.server = UiEventServer(self.legacy_journal,
            socket_path=Path(socket_path) if socket_path else self.root / "ui-events.sock",
            work_journal=self.work_journal)

    async def start(self):
        self.owner.start()
        try:
            await self.server.start()
        except BaseException:
            self.owner.stop()
            raise
        return self

    async def wait(self):
        while not self.owner._stopped.is_set():
            await asyncio.sleep(0.1)

    async def close(self):
        await self.server.close()
        self.owner.stop()


class ControlClient:
    """One request-scoped control connection.

    ``host=True`` is for the user's own trusted CLI: it may pick the control
    token up from the environment or the 0600 token file next to the socket.
    A model-facing connection (the work MCP service) is never host by
    default: seeing the socket path must not upgrade it to the host role.
    """

    def __init__(self, socket_path, *, token=None, host=False):
        self.socket_path = Path(socket_path)
        self.token = token
        self.host = bool(host)

    def _token(self):
        if isinstance(self.token, str) and self.token:
            return self.token
        if not self.host:
            return None
        environment = os.environ.get("CZS_WORK_CONTROL_TOKEN")
        if environment:
            return environment
        path = self.socket_path.parent / "control.token"
        try:
            if path.stat().st_mode & 0o077:
                return None
            return path.read_text(encoding="utf-8").strip() or None
        except OSError:
            return None

    def request(self, method, params=None, *, timeout=10):
        body = dict(params or {})
        token = self._token()
        if token is not None and "control_token" not in body:
            body["control_token"] = token
        payload = json.dumps({"v": 1, "request_id": uuid.uuid4().hex, "method": method,
                              "params": body}, ensure_ascii=False).encode("utf-8") + b"\n"
        return self.raw(payload, timeout=timeout)

    def raw(self, payload, *, timeout=10):
        if len(payload) > FRAME_LIMIT:
            raise WorkError("frame-too-large", "control-frame-too-large")
        try:
            connection = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        except OSError as error:
            raise WorkError("service-unavailable", str(error)) from error
        with connection:
            try:
                connection.settimeout(timeout)
                connection.connect(str(self.socket_path))
                stream = connection.makefile("rwb")
                with stream:
                    stream.write(payload)
                    stream.flush()
                    raw = stream.readline(FRAME_LIMIT + 1)
            except OSError as error:
                raise WorkError("service-unavailable",
                    f"work owner unavailable at {self.socket_path}; start it with "
                    f"`czs work serve --state-root {self.socket_path.parent}`") from error
        if not raw or len(raw) > FRAME_LIMIT:
            raise WorkError("protocol-error", "missing or oversized control response")
        response = json.loads(raw)
        if response.get("error"):
            raise WorkError(response["error"].get("code", "protocol-error"),
                            response["error"].get("message", "work owner error"))
        if response.get("v") != 1:
            raise WorkError("protocol-error", "control protocol version mismatch")
        return response

    def call(self, method, params=None, *, timeout=10):
        response = self.request(method, params, timeout=timeout)
        if "result" not in response:
            raise WorkError("protocol-error", "control response has no result")
        return response["result"]

    def close(self):
        # Connections are request-scoped; closing an adapter must not stop the durable owner.
        return None
