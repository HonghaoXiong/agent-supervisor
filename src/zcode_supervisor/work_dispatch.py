"""Durable dispatch intents, one-time claim and conservative recovery."""

from __future__ import annotations

import concurrent.futures
import json
import re
import threading
from dataclasses import dataclass, field

from .contracts import canonical_json
from .work_contracts import canonical_sha256
from .work_models import WorkError


@dataclass
class DispatchBinding:
    reserved_task_id: str
    contract_sha256: str
    intent_id: str
    attempt_id: str
    cancel_event: threading.Event = field(default_factory=threading.Event)


class TaskHandle:
    def __init__(self, dispatcher, intent_id, attempt_id, task_id):
        self.dispatcher = dispatcher
        self.intent_id = intent_id
        self.attempt_id = attempt_id
        self.task_id = task_id

    def wait(self, timeout=None):
        future = self.dispatcher._futures.get(self.intent_id)
        if future is not None:
            future.result(timeout=timeout)
        return self.dispatcher.status(self.attempt_id)


class WorkDispatcher:
    def __init__(self, store, runner, *, start_workers=True, max_workers=4):
        self.store = store
        self.runner = runner
        self._futures = {}
        self._bindings = {}
        self._executor = concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) \
            if start_workers else None
        if self._executor is not None:
            for row in self.store._connection.execute(
                    "SELECT intent_id FROM dispatch_intents WHERE state='pending'"):
                self._submit(row["intent_id"])

    @staticmethod
    def _validate(intent):
        if not isinstance(intent, dict) or set(intent) != {
                "schema_version", "work_id", "attempt_id", "contract", "contract_sha256"} \
                or intent["schema_version"] != "zsc.dispatch-intent/v1" \
                or not all(isinstance(intent[key], str) and intent[key]
                           for key in ("work_id", "attempt_id", "contract_sha256")) \
                or not re.fullmatch(r"[0-9a-f]{64}", intent["contract_sha256"]) \
                or not isinstance(intent["contract"], dict):
            raise WorkError("schema-invalid", "invalid durable dispatch intent")
        return intent

    def enqueue(self, intent):
        self._validate(intent)
        digest = canonical_sha256(intent)
        intent_id = "intent-" + digest[:24]
        task_id = canonical_sha256({"reserved_for": intent_id})[:32]
        with self.store._lock:
            self.store._connection.execute("BEGIN IMMEDIATE")
            try:
                if self.store._connection.execute("SELECT 1 FROM works WHERE work_id=?",
                                                  (intent["work_id"],)).fetchone() is None:
                    raise WorkError("work-missing", "dispatch requires an existing work")
                old = self.store._connection.execute(
                    "SELECT * FROM dispatch_intents WHERE intent_id=?", (intent_id,)).fetchone()
                if old is not None:
                    if old["request_sha256"] != digest:
                        raise WorkError("idempotency-conflict", "dispatch identity has different bytes")
                    self.store._connection.execute("COMMIT")
                    return self.handle(intent["attempt_id"])
                self.store._connection.execute("INSERT INTO attempts VALUES(?,?,?,?,?,NULL)",
                    (intent["attempt_id"], intent["work_id"], task_id, "pending", intent["contract_sha256"]))
                self.store._connection.execute("INSERT INTO dispatch_intents VALUES(?,?,?,?,?)",
                    (intent_id, intent["attempt_id"], "pending", digest, task_id))
                self.store._connection.execute("INSERT INTO dispatch_payloads VALUES(?,?)",
                    (intent_id, canonical_json(intent)))
                self.store._connection.execute("COMMIT")
            except WorkError:
                self.store._connection.execute("ROLLBACK")
                raise
            except Exception as error:
                self.store._connection.execute("ROLLBACK")
                raise WorkError("store-write-failed", str(error)[:300]) from error
        if self._executor is not None:
            self._submit(intent_id)
        return self.handle(intent["attempt_id"])

    def _submit(self, intent_id):
        if intent_id in self._futures:
            return
        self._futures[intent_id] = self._executor.submit(self._execute, intent_id, None)

    def _execute(self, intent_id, override):
        with self.store._lock:
            row = self.store._connection.execute(
                "SELECT * FROM dispatch_intents WHERE intent_id=?", (intent_id,)).fetchone()
            if row is None:
                raise WorkError("intent-missing", "unknown intent")
            intent = json.loads(canonical_json(override)) if override is not None else None
            if intent is None:
                # The durable request is reconstructed from identity-bound rows; production
                # enqueue stores the full contract in the private payload table below.
                payload = self.store._connection.execute(
                    "SELECT payload_json FROM dispatch_payloads WHERE intent_id=?", (intent_id,)).fetchone()
                intent = json.loads(payload["payload_json"])
            if canonical_sha256(intent) != row["request_sha256"]:
                raise WorkError("dispatch-contract-mismatch", "contract digest differs from durable intent")
            if row["state"] == "completed":
                return self.status(row["attempt_id"])
            if row["state"] != "pending":
                return self.status(row["attempt_id"])
            claimed = self.store._connection.execute(
                "UPDATE dispatch_intents SET state='dispatching' WHERE intent_id=? AND state='pending'",
                (intent_id,)).rowcount
            if claimed != 1:
                return self.status(row["attempt_id"])
            self.store._connection.execute("UPDATE attempts SET status='running' WHERE attempt_id=?",
                                           (row["attempt_id"],))
            binding = DispatchBinding(row["task_id"], intent["contract_sha256"], intent_id, row["attempt_id"])
            self._bindings[intent_id] = binding
        try:
            result = self.runner(binding, intent["contract"])
            if not isinstance(result, dict):
                raise WorkError("runner-result-invalid", "runner must return an object")
            status = result.get("status", "completed")
            if status not in ("completed", "failed", "cancelled"):
                raise WorkError("runner-result-invalid", "invalid runner status")
        except Exception as error:
            result = {"status": "failed", "error": f"{type(error).__name__}: {error}"[:500]}
            status = "failed"
        # BaseException deliberately leaves a dispatching record for recovery: sent is unknown.
        # Cancellation/reconciliation may have changed the durable state while the
        # external runner was in flight. A late result must never overwrite it.
        with self.store._lock:
            self.store._connection.execute(
                "UPDATE dispatch_intents SET state=? WHERE intent_id=? "
                "AND state IN ('dispatching','running')", (status, intent_id))
            self.store._connection.execute(
                "UPDATE attempts SET status=?,result_json=? WHERE attempt_id=? AND status='running'",
                (status, canonical_json(result), row["attempt_id"]))
        return result

    def handle(self, attempt_id):
        row = self.store._connection.execute(
            "SELECT intent_id,task_id FROM dispatch_intents WHERE attempt_id=?", (attempt_id,)).fetchone()
        if row is None:
            raise WorkError("attempt-missing", "unknown attempt")
        return TaskHandle(self, row["intent_id"], attempt_id, row["task_id"])

    def status(self, attempt_id):
        row = self.store._connection.execute("SELECT * FROM attempts WHERE attempt_id=?", (attempt_id,)).fetchone()
        if row is None:
            raise WorkError("attempt-missing", "unknown attempt")
        return {"attempt_id": attempt_id, "task_id": row["task_id"], "status": row["status"],
                "result": json.loads(row["result_json"]) if row["result_json"] else None}

    def cancel(self, attempt_id):
        handle = self.handle(attempt_id)
        binding = self._bindings.get(handle.intent_id)
        if binding:
            binding.cancel_event.set()
        future = self._futures.get(handle.intent_id)
        if future:
            future.cancel()
        with self.store._lock:
            self.store._connection.execute(
                "UPDATE dispatch_intents SET state='cancelled' WHERE intent_id=? AND state NOT IN ('completed')",
                (handle.intent_id,))
            self.store._connection.execute(
                "UPDATE attempts SET status='cancelled' WHERE attempt_id=? AND status!='completed'", (attempt_id,))
        return self.status(attempt_id)

    def reconcile(self, work_id):
        rows = self.store._connection.execute(
            "SELECT i.* FROM dispatch_intents i JOIN attempts a USING(attempt_id) WHERE a.work_id=?",
            (work_id,)).fetchall()
        if not rows:
            raise WorkError("work-missing", "no dispatch intent for work")
        statuses = []
        for row in rows:
            if row["state"] in ("dispatching", "running"):
                with self.store._lock:
                    self.store._connection.execute(
                        "UPDATE dispatch_intents SET state='needs_reconciliation' WHERE intent_id=?", (row["intent_id"],))
                    self.store._connection.execute(
                        "UPDATE attempts SET status='needs_reconciliation' WHERE attempt_id=?", (row["attempt_id"],))
                statuses.append("needs_reconciliation")
            else:
                statuses.append(row["state"])
        status = "needs_reconciliation" if "needs_reconciliation" in statuses else statuses[-1]
        return {"work_id": work_id, "status": "queued" if status == "pending" else status,
                "attempts": [row["attempt_id"] for row in rows]}

    def close(self):
        if self._executor is not None:
            self._executor.shutdown(wait=True, cancel_futures=False)
