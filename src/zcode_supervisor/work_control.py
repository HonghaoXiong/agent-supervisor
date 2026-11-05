"""Independent local work owner and bounded local control protocol."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import socket
import threading
import uuid
from pathlib import Path

from .artifact_store import ArtifactStore
from .capability_registry import discover_capabilities
from .collaboration import CollaborationService
from .knowledge_store import KnowledgeStore
from .work_authorization import register_work_authorization
from .work_dispatch import WorkDispatcher
from .work_models import ActorContext, WorkError
from .work_policy import choose_route
from .work_store import WorkStore
from .work_contracts import canonical_sha256


FRAME_LIMIT = 1024 * 1024


class WorkOwner:
    METHODS = frozenset({
        "create_work", "enqueue", "status", "cancel", "reconcile", "ping", "shutdown",
        "authorize", "work_capabilities", "start_recon", "start_implementation",
        "start_verification", "submit_brief", "accept_design", "submit_delivery",
        "record_acceptance", "rework_work", "cancel_work", "reconcile_work", "work_status",
        "read_artifact", "search_knowledge", "revalidate_knowledge",
        "propose_knowledge", "review_knowledge",
    })

    def __init__(self, state_root, *, store=None, runner=None, max_workers=4,
                 capability_config=None, routing_policy=None, routing_history=None,
                 work_journal=None):
        self.root = Path(state_root).expanduser().resolve()
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        self.root.chmod(0o700)
        self.socket_path = self.root / "control.sock"
        self.store = store or WorkStore.open(self.root)
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
        threading.Thread(target=self._accept_loop, name="zsc-work-owner", daemon=True).start()
        return self

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
                        except (WorkError, ValueError, OSError) as error:
                            response = {"v": 1, "request_id": locals().get("request", {}).get("request_id"),
                                        "ok": False,
                                        "error": {"code": getattr(error, "code", "invalid-argument"),
                                                  "message": str(error)[:500]}}
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

    def _invoke_inner(self, method, params):
        if method == "ping":
            return {"pid": os.getpid(), "protocol_version": 1}
        if method == "shutdown":
            return {"stopping": True}
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
            "created": ["start_recon"], "recon_queued": ["recon_running", "work_status"],
            "recon_running": ["submit_brief", "work_status"], "brief_ready": ["accept_design"],
            "design_ready": ["start_implementation"],
            "implementation_queued": ["implementation_running", "work_status"],
            "implementation_running": ["submit_delivery", "work_status"],
            "candidate_ready": ["start_verification"],
            "verification_running": ["record_acceptance", "work_status"],
            "rework_required": ["start_recon"], "completed": ["work_status"],
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
        if self._server is not None:
            self._server.close()
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
    def __init__(self, socket_path):
        self.socket_path = Path(socket_path)

    def request(self, method, params=None, *, timeout=10):
        payload = json.dumps({"v": 1, "request_id": uuid.uuid4().hex, "method": method,
                              "params": params or {}}, ensure_ascii=False).encode("utf-8") + b"\n"
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
