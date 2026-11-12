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
CREATE TABLE IF NOT EXISTS session_bindings(
 binding_id TEXT PRIMARY KEY, session_id TEXT NOT NULL, db_path TEXT,
 mode TEXT NOT NULL, state TEXT NOT NULL, capabilities_json TEXT NOT NULL,
 workspace TEXT, workspace_policy TEXT NOT NULL, authorization_ref TEXT,
 initial_review TEXT NOT NULL, cursor_json TEXT, revision INTEGER NOT NULL,
 evidence_json TEXT, created TEXT NOT NULL, updated TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS session_rounds(
 round_id TEXT PRIMARY KEY, binding_id TEXT NOT NULL, seq INTEGER NOT NULL,
 state TEXT NOT NULL, source_json TEXT NOT NULL, candidate_sha256 TEXT,
 design_revision INTEGER, reviewer_json TEXT, packet_json TEXT,
 decision_json TEXT, instruction_json TEXT, error TEXT,
 created TEXT NOT NULL, updated TEXT NOT NULL, verification_json TEXT,
 review_json TEXT, dispatch_json TEXT);
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
            round_columns = {row[1] for row in connection.execute(
                "PRAGMA table_info(session_rounds)")}
            if "verification_json" not in round_columns:
                connection.execute(
                    "ALTER TABLE session_rounds ADD COLUMN verification_json TEXT")
            if "review_json" not in round_columns:
                connection.execute(
                    "ALTER TABLE session_rounds ADD COLUMN review_json TEXT")
            if "dispatch_json" not in round_columns:
                connection.execute(
                    "ALTER TABLE session_rounds ADD COLUMN dispatch_json TEXT")
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

    # ------------------------------------------------- session bindings

    _BINDING_COLUMNS = ("binding_id", "session_id", "db_path", "mode", "state",
                        "capabilities_json", "workspace", "workspace_policy",
                        "authorization_ref", "initial_review", "cursor_json",
                        "revision", "evidence_json", "created", "updated")

    @staticmethod
    def _binding_row(row) -> dict:
        record = {key: row[key] for key in WorkStore._BINDING_COLUMNS}
        record["capabilities"] = json.loads(record.pop("capabilities_json"))
        record["cursor"] = json.loads(record.pop("cursor_json")) if record["cursor_json"] else None
        record["evidence"] = json.loads(record.pop("evidence_json")) if record["evidence_json"] else {}
        return record

    def save_session_binding(self, record: dict) -> None:
        with self._lock:
            self._connection.execute(
                "INSERT OR REPLACE INTO session_bindings VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                (record["binding_id"], record["session_id"], record.get("db_path"),
                 record["mode"], record["state"],
                 canonical_json(record.get("capabilities", [])),
                 record.get("workspace"), record["workspace_policy"],
                 record.get("authorization_ref"), record["initial_review"],
                 canonical_json(record.get("cursor") or {}), record.get("revision", 1),
                 canonical_json(record.get("evidence") or {}),
                 record["created"], record["updated"]))

    def get_session_binding(self, binding_id: str):
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM session_bindings WHERE binding_id=?", (binding_id,)).fetchone()
        return self._binding_row(row) if row is not None else None

    def session_bindings(self, session_id: str) -> list:
        with self._lock:
            rows = self._connection.execute(
                "SELECT * FROM session_bindings WHERE session_id=? ORDER BY created",
                (session_id,)).fetchall()
        return [self._binding_row(row) for row in rows]

    # ------------------------------------------------- session rounds

    _ROUND_COLUMNS = ("round_id", "binding_id", "seq", "state", "source_json",
                      "candidate_sha256", "design_revision", "reviewer_json",
                      "packet_json", "decision_json", "instruction_json",
                      "error", "created", "updated", "verification_json",
                      "review_json", "dispatch_json")

    def claim_dispatch(self, round_id: str, dispatch: dict):
        """Register the FIRST native dispatch for a round atomically.

        Returns ``(stored, created)``. Only the creator may send: two
        concurrent decisions can reach the send path, and the second must
        reuse the first identity instead of typing the instruction again.
        """
        with self._lock:
            row = self._connection.execute(
                "SELECT dispatch_json FROM session_rounds WHERE round_id=?",
                (round_id,)).fetchone()
            if row is not None and row[0]:
                existing = json.loads(row[0])
                if existing:
                    return existing, False
            self._connection.execute(
                "UPDATE session_rounds SET dispatch_json=? WHERE round_id=?",
                (canonical_json(dispatch), round_id))
            self._connection.commit()
            return dispatch, True

    def update_dispatch(self, round_id: str, **fields) -> dict:
        """Merge fields into the stored dispatch; a receipt is never cleared."""
        with self._lock:
            row = self._connection.execute(
                "SELECT dispatch_json FROM session_rounds WHERE round_id=?",
                (round_id,)).fetchone()
            current = json.loads(row[0]) if row is not None and row[0] else {}
            merged = {**current, **fields}
            if current.get("completed_at") and "completed_at" not in fields:
                merged["completed_at"] = current["completed_at"]
            self._connection.execute(
                "UPDATE session_rounds SET dispatch_json=? WHERE round_id=?",
                (canonical_json(merged), round_id))
            self._connection.commit()
            return merged

    def save_session_round(self, record: dict) -> None:
        with self._lock:
            row = dict(record)
            for key, inner in (("source", "source_json"), ("reviewer", "reviewer_json"),
                               ("packet", "packet_json"), ("decision", "decision_json"),
                               ("instruction", "instruction_json"),
                               ("verification", "verification_json"),
                               ("review", "review_json"),
                               ("dispatch", "dispatch_json")):
                value = row.pop(key, None)
                row[inner] = canonical_json(value) if value is not None else None
            self._connection.execute(
                "INSERT OR REPLACE INTO session_rounds VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                tuple(row.get(key) for key in self._ROUND_COLUMNS))

    def get_session_round(self, round_id: str):
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM session_rounds WHERE round_id=?", (round_id,)).fetchone()
        if row is None:
            return None
        record = {key: row[key] for key in self._ROUND_COLUMNS}
        for key, inner in (("source", "source_json"), ("reviewer", "reviewer_json"),
                           ("packet", "packet_json"), ("decision", "decision_json"),
                           ("instruction", "instruction_json"),
                           ("verification", "verification_json"),
                           ("review", "review_json"),
                           ("dispatch", "dispatch_json")):
            text = record.pop(inner)
            record[key] = json.loads(text) if text else None
        return record

    def session_rounds(self, binding_id: str) -> list:
        with self._lock:
            rows = self._connection.execute(
                "SELECT round_id FROM session_rounds WHERE binding_id=? ORDER BY seq",
                (binding_id,)).fetchall()
        return [self.get_session_round(row["round_id"]) for row in rows]

    def session_rounds_all(self) -> list:
        """Every round in the store, for cross-binding turn ownership."""
        with self._lock:
            rows = self._connection.execute(
                "SELECT round_id FROM session_rounds ORDER BY seq").fetchall()
        return [self.get_session_round(row["round_id"]) for row in rows]

    def next_round_seq(self, binding_id: str) -> int:
        with self._lock:
            row = self._connection.execute(
                "SELECT MAX(seq) FROM session_rounds WHERE binding_id=?",
                (binding_id,)).fetchone()
        return (row[0] or 0) + 1

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
