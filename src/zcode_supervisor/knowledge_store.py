"""Persistent, project-scoped knowledge claims with independent review and revalidation."""

from __future__ import annotations

import base64
import json
import re
import sqlite3
import threading
from pathlib import Path

from .contracts import canonical_json
from .project_profiles import register_profile as build_profile_ref
from .work_contracts import canonical_sha256
from .work_models import WorkError


CLAIM_KINDS = {"fact", "inference", "decision", "failed_attempt", "rejected_option", "practice_candidate"}
EVIDENCE_CLASSES = {"structure", "history", "experience"}


class KnowledgeStore:
    def __init__(self, root):
        self.root = Path(root).expanduser().resolve()
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        self._lock = threading.RLock()
        self._connection = sqlite3.connect(self.root / "knowledge.sqlite3", check_same_thread=False)
        self._connection.row_factory = sqlite3.Row
        self._connection.execute("PRAGMA journal_mode=WAL")
        self._connection.execute("PRAGMA foreign_keys=ON")
        self._connection.executescript("""
            CREATE TABLE IF NOT EXISTS claims(
                claim_id TEXT PRIMARY KEY, project_id TEXT NOT NULL, kind TEXT NOT NULL,
                visibility TEXT NOT NULL, status TEXT NOT NULL, document_json TEXT NOT NULL,
                created_seq INTEGER NOT NULL);
            CREATE TABLE IF NOT EXISTS profiles(
                project_id TEXT NOT NULL, profile_sha256 TEXT NOT NULL,
                document_json TEXT NOT NULL, PRIMARY KEY(project_id,profile_sha256));
        """)
        self._connection.commit()

    def close(self):
        with self._lock:
            self._connection.close()

    def register_profile(self, profile, root_bindings, actor):
        ref = build_profile_ref(profile, root_bindings, actor)
        with self._lock:
            self._connection.execute("INSERT OR REPLACE INTO profiles VALUES(?,?,?)",
                (ref["project_id"], ref["profile_sha256"], canonical_json(ref)))
            self._connection.commit()
        return ref

    @staticmethod
    def _validate_document(document):
        if not isinstance(document, dict) or document.get("schema_version") != "zsc.knowledge-claim/v1" \
                or document.get("status") != "candidate" or document.get("kind") not in CLAIM_KINDS \
                or document.get("evidence_class") not in EVIDENCE_CLASSES:
            raise WorkError("schema-invalid", "knowledge input must be a candidate KnowledgeClaim/v1")
        for field in ("project_id", "statement"):
            if not isinstance(document.get(field), str) or not document[field].strip():
                raise WorkError("schema-invalid", f"knowledge {field} is required")
        if len(document["statement"]) > 16384:
            raise WorkError("schema-invalid", "knowledge statement exceeds 16 KiB")
        for field in ("source_refs", "dependency_refs", "counterexamples", "tags"):
            if not isinstance(document.get(field), list):
                raise WorkError("schema-invalid", f"knowledge {field} must be an array")
        scope = document.get("scope")
        if not isinstance(scope, dict) or scope.get("visibility") not in ("project", "shared_abstract"):
            raise WorkError("schema-invalid", "knowledge scope must be project or shared_abstract")
        if scope["visibility"] == "shared_abstract":
            if document["kind"] != "practice_candidate" or document["evidence_class"] != "experience":
                raise WorkError("scope-conflict", "only abstract practices may cross projects")
            serialized = canonical_json({"statement": document["statement"], "scope": scope,
                                         "source_refs": document["source_refs"]})
            if re.search(r'"(?:path|root|workspace)"\s*:|(?:^|[\s"=])/(?:Users|home|var|private|tmp)/', serialized):
                raise WorkError("scope-conflict", "shared abstract knowledge contains private paths")
        if document.get("supersedes") is not None and not isinstance(document["supersedes"], str):
            raise WorkError("schema-invalid", "supersedes must be a claim id or null")

    def propose_claim(self, document, actor):
        actor.require("knowledge:propose")
        self._validate_document(document)
        stored = dict(document)
        stored["producer"] = {"actor_id": actor.actor_id, "session_id": actor.session_id,
                              "worker": actor.worker}
        stored["review_ref"] = None
        claim_id = "claim-" + canonical_sha256(stored)[:24]
        with self._lock:
            self._connection.execute("BEGIN IMMEDIATE")
            existing = self._connection.execute(
                "SELECT document_json,status FROM claims WHERE claim_id=?", (claim_id,)).fetchone()
            if existing is not None:
                old = json.loads(existing["document_json"])
                if canonical_json(old) != canonical_json(stored):
                    self._connection.execute("ROLLBACK")
                    raise WorkError("idempotency-conflict", "knowledge claim identity has different bytes")
                self._connection.execute("COMMIT")
                return {"claim_id": claim_id, "claim_sha256": canonical_sha256(old),
                        "status": existing["status"]}
            seq = self._connection.execute("SELECT COALESCE(MAX(created_seq),0)+1 FROM claims").fetchone()[0]
            self._connection.execute("INSERT INTO claims VALUES(?,?,?,?,?,?,?)",
                (claim_id, stored["project_id"], stored["kind"], stored["scope"]["visibility"],
                 "candidate", canonical_json(stored), seq))
            self._connection.execute("COMMIT")
        return {"claim_id": claim_id, "claim_sha256": canonical_sha256(stored), "status": "candidate"}

    def _row(self, ref):
        claim_id = ref.get("claim_id") if isinstance(ref, dict) else ref
        row = self._connection.execute("SELECT * FROM claims WHERE claim_id=?", (claim_id,)).fetchone()
        if row is None:
            raise WorkError("knowledge-missing", "unknown knowledge claim")
        return row

    def review_claim(self, ref, decision, actor):
        row = self._row(ref)
        document = json.loads(row["document_json"])
        producer = document["producer"]
        if actor.actor_id == producer.get("actor_id") \
                or (actor.session_id and actor.session_id == producer.get("session_id")):
            raise WorkError("independence-conflict", "knowledge producer cannot review its own claim")
        actor.require("knowledge:review")
        if not isinstance(decision, dict) or decision.get("decision") not in ("confirm", "reject") \
                or not isinstance(decision.get("reason"), str) or not decision["reason"].strip():
            raise WorkError("schema-invalid", "review requires confirm/reject and a reason")
        if decision["decision"] == "confirm" and not document["source_refs"]:
            raise WorkError("acceptance-blocked", "confirmed knowledge requires reviewable sources")
        review = {"decision": decision["decision"], "reason": decision["reason"],
                  "reviewer": {"actor_id": actor.actor_id, "session_id": actor.session_id},
                  "conditions": decision.get("conditions", []),
                  "counterexamples": decision.get("counterexamples", [])}
        document["review_ref"] = canonical_sha256(review)
        document["counterexamples"] = list(document["counterexamples"])
        for example in review["counterexamples"]:
            if example not in document["counterexamples"]:
                document["counterexamples"].append(example)
        if decision.get("conditions"):
            document["scope"] = dict(document["scope"], conditions=list(decision["conditions"]))
        document["status"] = "confirmed" if decision["decision"] == "confirm" else "rejected"
        with self._lock:
            self._connection.execute("BEGIN IMMEDIATE")
            self._connection.execute("UPDATE claims SET status=?,document_json=? WHERE claim_id=?",
                (document["status"], canonical_json(document), row["claim_id"]))
            if decision["decision"] == "confirm" and document.get("supersedes"):
                previous = self._row(document["supersedes"])
                if previous["project_id"] != row["project_id"]:
                    self._connection.execute("ROLLBACK")
                    raise WorkError("scope-conflict", "cannot supersede another project's claim")
                old = json.loads(previous["document_json"])
                old["status"] = "superseded"
                old["superseded_by"] = row["claim_id"]
                self._connection.execute("UPDATE claims SET status='superseded',document_json=? WHERE claim_id=?",
                    (canonical_json(old), previous["claim_id"]))
            self._connection.execute("COMMIT")
        return dict(document, claim_id=row["claim_id"], claim_sha256=canonical_sha256(document))

    def search(self, query, scope, cursor=None) -> dict:
        if not isinstance(scope, dict) or not isinstance(scope.get("project_id"), str):
            raise WorkError("schema-invalid", "search requires a project scope")
        query = {"text": query} if isinstance(query, str) else query
        if not isinstance(query, dict):
            raise WorkError("schema-invalid", "knowledge query must be text or an object")
        limit = query.get("limit", 10)
        if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 100:
            raise WorkError("invalid-argument", "knowledge page limit must be between 1 and 100")
        project = scope["project_id"]
        known = self._connection.execute("SELECT 1 FROM claims WHERE project_id=? UNION SELECT 1 FROM profiles WHERE project_id=? LIMIT 1",
                                         (project, project)).fetchone()
        if known is None:
            raise WorkError("scope-conflict", "project is not registered in this knowledge store")
        # Privacy filtering happens in SQL before statement text is read or ranked.
        rows = self._connection.execute(
            "SELECT * FROM claims WHERE project_id=? OR (visibility='shared_abstract' "
            "AND kind='practice_candidate' AND json_extract(document_json,'$.evidence_class')='experience' "
            "AND status='confirmed')",
            (project,)).fetchall()
        offset = self._cursor_offset(query, scope, cursor)
        items = []
        terms = re.findall(r"[\w.-]+", str(query.get("text", "")).lower())
        for row in rows:
            document = json.loads(row["document_json"])
            if document["project_id"] != project and not (
                    document["status"] == "confirmed" and document["kind"] == "practice_candidate"
                    and document["evidence_class"] == "experience"
                    and document["scope"]["visibility"] == "shared_abstract"):
                continue
            if not query.get("include_history") and document["status"] in ("rejected", "superseded"):
                continue
            if query.get("kind") and document["kind"] != query["kind"]:
                continue
            if query.get("status") and document["status"] != query["status"]:
                continue
            haystack = " ".join([document["statement"], document["kind"], *document["tags"]]).lower()
            if terms and not all(term in haystack for term in terms):
                continue
            score = sum(haystack.count(term) for term in terms)
            if document["project_id"] != project:
                document = dict(document, project_id="shared",
                                scope={"visibility": "shared_abstract"}, producer=None)
            items.append((score, row["claim_id"], document))
        items.sort(key=lambda item: (-item[0], item[1]))
        total = len(items)
        selected = items[offset:offset + limit]
        end = offset + len(selected)
        page_items = [dict(item[2], claim_id=item[1], claim_sha256=canonical_sha256(item[2]),
                           relevance_score=item[0]) for item in selected]
        return {"schema_version": "zsc.knowledge-page/v1", "items": page_items,
                "remaining": max(0, total - end),
                "next_cursor": self._make_cursor(query, scope, end) if end < total else None}

    @staticmethod
    def _binding(query, scope):
        normalized = dict(query)
        normalized.pop("limit", None)
        return canonical_sha256({"query": normalized, "scope": scope})

    def _make_cursor(self, query, scope, offset):
        body = {"offset": offset, "binding": self._binding(query, scope)}
        body["signature"] = canonical_sha256(body)[:24]
        return base64.urlsafe_b64encode(canonical_json(body).encode("utf-8")).decode("ascii").rstrip("=")

    def _cursor_offset(self, query, scope, cursor):
        if cursor is None:
            return 0
        try:
            raw = base64.urlsafe_b64decode(cursor + "=" * (-len(cursor) % 4))
            body = json.loads(raw)
            if body["binding"] != self._binding(query, scope) \
                    or body["signature"] != canonical_sha256({"offset": body["offset"], "binding": body["binding"]})[:24] \
                    or not isinstance(body["offset"], int) or body["offset"] < 0:
                raise ValueError("cursor binding")
            return body["offset"]
        except (ValueError, KeyError, TypeError, json.JSONDecodeError) as error:
            raise WorkError("cursor-conflict", "knowledge cursor is invalid or belongs to another query") from error

    def revalidate(self, ref, current_manifest):
        row = self._row(ref)
        document = json.loads(row["document_json"])
        if not isinstance(current_manifest, dict):
            raise WorkError("schema-invalid", "current source manifest is required")
        files = {(item.get("root"), item.get("path")): item for item in current_manifest.get("files", [])}
        directories = {(item.get("root"), item.get("path")): item
                       for item in current_manifest.get("directory_members", [])}
        environment = current_manifest.get("environment", {})
        reasons = []
        for dependency in document["dependency_refs"]:
            if not isinstance(dependency, dict):
                reasons.append("invalid-dependency")
                continue
            kind = dependency.get("type")
            if kind == "file":
                current = files.get((dependency.get("root"), dependency.get("path")))
                if current is None or current.get("sha256") != dependency.get("sha256"):
                    reasons.append(f"file:{dependency.get('root')}:{dependency.get('path')}")
            elif kind == "directory":
                current = directories.get((dependency.get("root"), dependency.get("path")))
                if current is None or current.get("sha256") != dependency.get("sha256"):
                    reasons.append(f"directory:{dependency.get('root')}:{dependency.get('path')}")
            elif kind == "environment":
                current = environment.get(dependency.get("key"))
                if not isinstance(current, dict) or current.get("sha256") != dependency.get("sha256"):
                    reasons.append(f"environment:{dependency.get('key')}")
            else:
                reasons.append(f"unsupported:{kind}")
        if reasons:
            document["status"] = "stale"
            document["stale_reasons"] = sorted(set(reasons))
            with self._lock:
                self._connection.execute("UPDATE claims SET status='stale',document_json=? WHERE claim_id=?",
                    (canonical_json(document), row["claim_id"]))
                self._connection.execute("COMMIT")
        return {"claim_id": row["claim_id"], "status": document["status"],
                "reasons": sorted(set(reasons)), "auto_confirmed": False,
                "claim_sha256": canonical_sha256(document)}
