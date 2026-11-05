"""SQLite-backed revisioned work state with transactional idempotency."""

from __future__ import annotations

import fcntl
import json
import os
import sqlite3
import threading
from pathlib import Path

from .contracts import canonical_json
from .work_contracts import canonical_sha256
from .work_models import ActorContext, WorkError, validate_work_spec


SCHEMA = """
CREATE TABLE IF NOT EXISTS works(
 work_id TEXT PRIMARY KEY, revision INTEGER NOT NULL, state TEXT NOT NULL,
 spec_json TEXT NOT NULL, created_by TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS events(
 event_id INTEGER PRIMARY KEY AUTOINCREMENT, work_id TEXT NOT NULL,
 revision INTEGER NOT NULL, action TEXT NOT NULL, payload_json TEXT NOT NULL,
 actor_json TEXT NOT NULL, FOREIGN KEY(work_id) REFERENCES works(work_id));
CREATE TABLE IF NOT EXISTS actions(
 scope TEXT NOT NULL, work_id TEXT NOT NULL, idempotency_key TEXT NOT NULL,
 action TEXT NOT NULL, request_sha256 TEXT NOT NULL, response_json TEXT NOT NULL,
 PRIMARY KEY(scope,work_id,idempotency_key));
CREATE TABLE IF NOT EXISTS attempts(
 attempt_id TEXT PRIMARY KEY, work_id TEXT NOT NULL, task_id TEXT,
 status TEXT NOT NULL, contract_sha256 TEXT, result_json TEXT,
 FOREIGN KEY(work_id) REFERENCES works(work_id));
CREATE TABLE IF NOT EXISTS dispatch_intents(
 intent_id TEXT PRIMARY KEY, attempt_id TEXT NOT NULL, state TEXT NOT NULL,
 request_sha256 TEXT NOT NULL, task_id TEXT,
 FOREIGN KEY(attempt_id) REFERENCES attempts(attempt_id));
CREATE TABLE IF NOT EXISTS dispatch_payloads(
 intent_id TEXT PRIMARY KEY, payload_json TEXT NOT NULL,
 FOREIGN KEY(intent_id) REFERENCES dispatch_intents(intent_id));
CREATE TABLE IF NOT EXISTS artifact_refs(
 work_id TEXT NOT NULL, ref_json TEXT NOT NULL, object_root TEXT NOT NULL,
 PRIMARY KEY(work_id,ref_json), FOREIGN KEY(work_id) REFERENCES works(work_id));
"""


class WorkStore:
    def __init__(self, root: Path, connection, lock_fd: int):
        self.root = root
        self._connection = connection
        self._lock_fd = lock_fd
        self._lock = threading.RLock()
        self._closed = False

    @classmethod
    def open(cls, state_root) -> "WorkStore":
        root = Path(state_root)
        root.mkdir(parents=True, exist_ok=True, mode=0o700)
        lock_fd = os.open(root / "service.lock", os.O_RDWR | os.O_CREAT, 0o600)
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            os.close(lock_fd)
            raise WorkError("store-locked", "another work write service owns this state") from error
        try:
            connection = sqlite3.connect(root / "work.sqlite3", isolation_level=None,
                                         check_same_thread=False, timeout=5)
            connection.row_factory = sqlite3.Row
            connection.execute("PRAGMA foreign_keys=ON")
            connection.execute("PRAGMA journal_mode=WAL")
            version = connection.execute("PRAGMA user_version").fetchone()[0]
            if version not in (0, 1, 2):
                raise WorkError("migration-unsupported", f"database version {version} is newer than supported")
            connection.executescript(SCHEMA)
            attempt_columns = {row[1] for row in connection.execute("PRAGMA table_info(attempts)")}
            if "result_json" not in attempt_columns:
                connection.execute("ALTER TABLE attempts ADD COLUMN result_json TEXT")
            connection.execute("PRAGMA user_version=2")
            store = cls(root, connection, lock_fd)
            store._verify_artifacts()
            return store
        except Exception:
            try:
                fcntl.flock(lock_fd, fcntl.LOCK_UN)
            finally:
                os.close(lock_fd)
            raise

    def close(self):
        with self._lock:
            if self._closed:
                return
            self._connection.close()
            fcntl.flock(self._lock_fd, fcntl.LOCK_UN)
            os.close(self._lock_fd)
            self._closed = True

    @staticmethod
    def _key(key):
        if not isinstance(key, str) or not key or len(key) > 256:
            raise WorkError("invalid-argument", "idempotency key must be 1..256 characters")
        return key

    def _idempotent(self, scope, work_id, key, request_sha):
        row = self._connection.execute(
            "SELECT request_sha256,response_json FROM actions WHERE scope=? AND work_id=? AND idempotency_key=?",
            (scope, work_id, key)).fetchone()
        if row is None:
            return None
        if row["request_sha256"] != request_sha:
            raise WorkError("idempotency-conflict", "same idempotency key has a different request")
        return json.loads(row["response_json"])

    @staticmethod
    def _record_action(connection, scope, work_id, key, action, request_sha, response):
        connection.execute("INSERT INTO actions VALUES(?,?,?,?,?,?)",
                           (scope, work_id, key, action, request_sha, canonical_json(response)))

    def create_work(self, spec, *, idempotency_key, actor: ActorContext):
        actor.require("work:create")
        spec = validate_work_spec(spec)
        key = self._key(idempotency_key)
        request_sha = canonical_sha256(spec)
        work_id = "work-" + request_sha[:24]
        with self._lock:
            self._connection.execute("BEGIN IMMEDIATE")
            try:
                old = self._idempotent("create", "", key, request_sha)
                if old is not None:
                    self._connection.execute("COMMIT")
                    return old
                row = self._connection.execute("SELECT revision,state FROM works WHERE work_id=?", (work_id,)).fetchone()
                if row is None:
                    self._connection.execute("INSERT INTO works VALUES(?,?,?,?,?)",
                        (work_id, 0, "created", canonical_json(spec), actor.actor_id))
                    self._connection.execute(
                        "INSERT INTO events(work_id,revision,action,payload_json,actor_json) VALUES(?,?,?,?,?)",
                        (work_id, 0, "create", canonical_json(spec), canonical_json(actor.evidence())))
                result = {"work_id": work_id, "revision": row["revision"] if row else 0,
                          "state": row["state"] if row else "created"}
                self._record_action(self._connection, "create", "", key, "create", request_sha, result)
                self._connection.execute("COMMIT")
                return result
            except WorkError:
                self._connection.execute("ROLLBACK")
                raise
            except (sqlite3.Error, OSError) as error:
                self._connection.execute("ROLLBACK")
                raise WorkError("store-write-failed", str(error)[:300]) from error

    def apply_action(self, work_id, action, payload, *, expected_revision, idempotency_key,
                     actor: ActorContext):
        actor.require("work:action")
        if not isinstance(work_id, str) or not isinstance(action, str) or not action \
                or not isinstance(payload, dict) or isinstance(expected_revision, bool) \
                or not isinstance(expected_revision, int):
            raise WorkError("invalid-argument", "invalid action envelope")
        key = self._key(idempotency_key)
        request_sha = canonical_sha256({"work_id": work_id, "action": action, "payload": payload})
        with self._lock:
            self._connection.execute("BEGIN IMMEDIATE")
            try:
                # Replay wins over revision checks, as required after client timeout.
                old = self._idempotent("action", work_id, key, request_sha)
                if old is not None:
                    self._connection.execute("COMMIT")
                    return old
                row = self._connection.execute("SELECT revision,state FROM works WHERE work_id=?", (work_id,)).fetchone()
                if row is None:
                    raise WorkError("work-missing", "unknown work")
                if row["revision"] != expected_revision:
                    raise WorkError("revision-conflict",
                                    f"expected {expected_revision}, current {row['revision']}")
                revision = row["revision"] + 1
                state = payload.get("state", row["state"])
                if not isinstance(state, str) or not state:
                    raise WorkError("schema-invalid", "state must be non-empty text")
                self._connection.execute("UPDATE works SET revision=?,state=? WHERE work_id=? AND revision=?",
                                         (revision, state, work_id, row["revision"]))
                self._connection.execute(
                    "INSERT INTO events(work_id,revision,action,payload_json,actor_json) VALUES(?,?,?,?,?)",
                    (work_id, revision, action, canonical_json(payload), canonical_json(actor.evidence())))
                result = {"work_id": work_id, "revision": revision, "state": state}
                self._record_action(self._connection, "action", work_id, key, action, request_sha, result)
                self._connection.execute("COMMIT")
                return result
            except WorkError:
                self._connection.execute("ROLLBACK")
                raise
            except (sqlite3.Error, OSError) as error:
                self._connection.execute("ROLLBACK")
                raise WorkError("store-write-failed", str(error)[:300]) from error

    def list_work_ids(self):
        """Read-only ids for startup projection replay; business state stays in SQLite."""
        with self._lock:
            return [row["work_id"] for row in self._connection.execute(
                "SELECT work_id FROM works ORDER BY rowid")]

    def register_artifact(self, work_id, ref, artifact_store):
        from .artifact_store import ArtifactStore
        if not isinstance(artifact_store, ArtifactStore):
            raise WorkError("invalid-argument", "host ArtifactStore required")
        artifact_store._read_bytes(ref)  # validate before durable reference
        with self._lock:
            self._connection.execute("INSERT OR IGNORE INTO artifact_refs VALUES(?,?,?)",
                (work_id, canonical_json(ref), str(artifact_store.root)))

    def _verify_artifacts(self):
        from .artifact_store import ArtifactStore, ArtifactError
        for row in self._connection.execute("SELECT ref_json,object_root FROM artifact_refs"):
            try:
                ArtifactStore(row["object_root"])._read_bytes(json.loads(row["ref_json"]))
            except (ArtifactError, ValueError) as error:
                raise WorkError("artifact-integrity", "durable artifact reference failed startup verification") from error
