"""Separately negotiated, read-only collaboration UI projection.

This is not the legacy task-event schema and never consumes raw work payloads as UI data.
The WorkStore revision remains the business authority; event seq is only a UI transport cursor.
"""

from __future__ import annotations

import copy
import hashlib
import hmac
import json
import os

from .ui_events import safe_hex64, safe_opaque, safe_token
from .work_contracts import canonical_sha256

WORK_SCHEMA = "zsc.ui.work/v1"
WORK_EVENT_SCHEMA = "zsc.ui.work-event/v1"
WORK_CAPABILITY_VERSION = 1
WORK_CAPABILITIES = frozenset(("work-snapshot", "work-events", "evidence-summaries"))


class UiWorkError(ValueError):
    def __init__(self, code, message):
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message


def negotiate_collaboration(declaration):
    if declaration is None:
        return None
    if not isinstance(declaration, dict) or set(declaration) - {"v", "capabilities"} \
            or not isinstance(declaration.get("capabilities"), list) \
            or any(not isinstance(item, str) for item in declaration["capabilities"]):
        return {"v": None, "capabilities": [], "reason": "invalid-declaration"}
    if declaration.get("v") != WORK_CAPABILITY_VERSION:
        return {"v": None, "capabilities": [], "reason": "unsupported-version"}
    requested = set(declaration["capabilities"])
    accepted = sorted(requested & WORK_CAPABILITIES)
    result = {"v": WORK_CAPABILITY_VERSION, "capabilities": accepted}
    rejected = sorted(requested - WORK_CAPABILITIES)
    if rejected:
        result["rejected"] = rejected
    return result


def _token_list(values, limit=32):
    if not isinstance(values, list):
        return []
    result = []
    for value in values[:limit]:
        token = safe_token(value)
        if token is not None:
            result.append(token)
    return result


def project_work(status: dict) -> dict:
    if not isinstance(status, dict) or safe_opaque(status.get("work_id")) is None:
        raise UiWorkError("invalid-work-status", "host work_id and status object are required")
    design = status.get("design") if isinstance(status.get("design"), dict) else {}
    candidate = status.get("candidate") if isinstance(status.get("candidate"), dict) else {}
    identities = {
        "design_sha256": safe_hex64(status.get("design_sha256") or design.get("sha256")),
        "base_manifest_sha256": safe_hex64(status.get("base_manifest_sha256")) or
            (canonical_sha256(status["base_manifest"]) if isinstance(status.get("base_manifest"), dict) else None),
        "candidate_manifest_sha256": safe_hex64(status.get("candidate_manifest_sha256")) or
            (canonical_sha256(status["candidate_manifest"]) if isinstance(status.get("candidate_manifest"), dict) else None),
        "candidate_sha256": safe_hex64(status.get("candidate_sha256") or candidate.get("sha256")),
    }
    dependencies = []
    checker_by_interface = {}
    for contract in design.get("interface_contracts", [])[:64] \
            if isinstance(design.get("interface_contracts"), list) else []:
        if isinstance(contract, dict) and isinstance(contract.get("checker_ids"), list) \
                and contract["checker_ids"]:
            checker_by_interface[contract.get("interface_id")] = contract["checker_ids"][0]
    report = status.get("dependency_results") if isinstance(status.get("dependency_results"), dict) else {}
    for edge in report.get("edges", [])[:64] if isinstance(report.get("edges"), list) else []:
        if not isinstance(edge, dict):
            continue
        edge_id = safe_opaque(edge.get("edge_id"))
        interface_id = edge_id.split(":", 1)[0] if edge_id else None
        dependencies.append({
            "edge_id": edge_id,
            "required": edge.get("required") is True,
            "status": safe_token(edge.get("result")) or "unknown",
            "checker_id": safe_opaque(edge.get("checker_id")) or safe_opaque(
                checker_by_interface.get(interface_id)),
            "oracle_ref": safe_hex64(edge.get("oracle_ref")),
            "candidate_sha256": safe_hex64(edge.get("candidate_ref")),
            "evidence_refs": [ref for ref in (safe_opaque(item) for item in
                edge.get("evidence_refs", [])[:16]) if ref is not None]
                if isinstance(edge.get("evidence_refs"), list) else [],
        })
    knowledge_raw = status.get("knowledge") if isinstance(status.get("knowledge"), dict) else {}
    knowledge = {"status": safe_token(knowledge_raw.get("status")) or "unknown",
                 "claim_ids": [item for item in (safe_opaque(value) for value in
                    knowledge_raw.get("claim_ids", [])[:32]) if item is not None]
                    if isinstance(knowledge_raw.get("claim_ids"), list) else [],
                 "stale_reason": safe_token(knowledge_raw.get("stale_reason"))}
    cost_raw = status.get("cost") if isinstance(status.get("cost"), dict) else {}
    cost = {"status": safe_token(cost_raw.get("status")) or "unknown", "total": None}
    if cost["status"] == "measured" and isinstance(cost_raw.get("total"), (int, float)) \
            and not isinstance(cost_raw.get("total"), bool):
        cost["total"] = cost_raw["total"]
    evidence = []
    for item in status.get("evidence", [])[:64] if isinstance(status.get("evidence"), list) else []:
        if not isinstance(item, dict):
            continue
        evidence.append({"digest": safe_hex64(item.get("digest")),
                         "kind": safe_token(item.get("kind")) or "unknown",
                         "status": safe_token(item.get("status")) or "unknown",
                         "summary_code": safe_token(item.get("summary_code"))})
    if not evidence:
        checks = status.get("checks") if isinstance(status.get("checks"), list) else []
        required_checks = [check for check in checks if isinstance(check, dict)
                           and check.get("required") is True]
        if required_checks and all(check.get("status") == "passed" for check in required_checks):
            evidence.append({"digest": None, "kind": "checks", "status": "passed",
                             "summary_code": "checks_passed"})
        if report.get("status") == "closed":
            evidence.append({"digest": identities["candidate_sha256"], "kind": "dependency",
                             "status": "passed", "summary_code": "dependency_closed"})
        acceptance = status.get("acceptance") if isinstance(status.get("acceptance"), dict) else {}
        if acceptance.get("verdict") == "accepted":
            evidence.append({"digest": identities["candidate_sha256"], "kind": "acceptance",
                             "status": "passed", "summary_code": "independent_verification"})
    blocked = status.get("blocked_reason") or status.get("blocker")
    revision = status.get("revision")
    if not isinstance(revision, int) or isinstance(revision, bool) or revision < 0:
        raise UiWorkError("invalid-work-status", "revision must be a non-negative integer")
    return {"schema": WORK_SCHEMA, "work_id": safe_opaque(status["work_id"]),
            "phase": safe_token(status.get("state")) or "unknown", "revision": revision,
            "identities": identities, "dependencies": dependencies,
            "knowledge": knowledge, "cost": cost,
            "last_activity": safe_token(status.get("last_action")),
            "blocked_reason": safe_token(blocked),
            "next_actions": _token_list(status.get("next_actions")), "evidence": evidence}


class UiWorkJournal:
    def __init__(self, *, retention=1000):
        if not isinstance(retention, int) or not 2 <= retention <= 10000:
            raise ValueError("retention must be 2..10000")
        self.retention = retention
        self._secret = os.urandom(32)
        self._works = {}
        self._order = []
        self._events = []
        self._seq = 0
        self._listeners = []

    def add_listener(self, callback):
        """Register an internal read-only transport listener (not a business writer)."""
        self._listeners.append(callback)
        return lambda: self._listeners.remove(callback)

    def _notify(self, event):
        for callback in tuple(self._listeners):
            callback(copy.deepcopy(event))

    def _cursor(self, kind, offset, binding):
        body = {"v": 1, "kind": kind, "offset": offset, "binding": binding}
        body["sig"] = hmac.new(self._secret, json.dumps(body, sort_keys=True,
            separators=(",", ":")).encode(), hashlib.sha256).hexdigest()[:24]
        return body

    def _offset(self, cursor, kind, binding):
        if cursor is None:
            return 0
        if not isinstance(cursor, dict):
            raise UiWorkError("invalid-cursor", "cursor must be an object")
        sig = cursor.get("sig")
        unsigned = {key: value for key, value in cursor.items() if key != "sig"}
        expected = hmac.new(self._secret, json.dumps(unsigned, sort_keys=True,
            separators=(",", ":")).encode(), hashlib.sha256).hexdigest()[:24]
        if cursor.get("v") != 1 or cursor.get("kind") != kind \
                or cursor.get("binding") != binding or not isinstance(sig, str) \
                or not hmac.compare_digest(sig, expected) \
                or not isinstance(cursor.get("offset"), int) or cursor["offset"] < 0:
            raise UiWorkError("invalid-cursor", "cursor is stale, forged, or belongs to another query")
        return cursor["offset"]

    def ingest(self, status):
        projection = project_work(status)
        work_id = projection["work_id"]
        previous = self._works.get(work_id)
        if previous is not None:
            if projection["revision"] < previous["revision"] or projection == previous:
                return None
            if projection["revision"] == previous["revision"]:
                raise UiWorkError("revision-conflict", "same revision cannot change authoritative content")
        if previous is None:
            self._order.append(work_id)
        self._works[work_id] = projection
        self._seq += 1
        event = {"schema": WORK_EVENT_SCHEMA, "event_id": f"work/{self._seq}",
                 "seq": self._seq, "work_id": work_id, "revision": projection["revision"],
                 "kind": "work.upsert", "work": copy.deepcopy(projection)}
        encoded = json.dumps(event, ensure_ascii=False, allow_nan=False).encode()
        if len(encoded) > 65536:
            self._works[work_id] = previous
            self._seq -= 1
            if previous is None:
                self._order.remove(work_id)
            raise UiWorkError("projection-too-large", "sanitized work projection exceeds 64 KiB")
        self._events.append(event)
        if len(self._events) > self.retention:
            del self._events[:len(self._events) - self.retention]
        self._notify(event)
        return copy.deepcopy(event)

    def snapshot(self, *, cursor=None, limit=20):
        if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 100:
            raise UiWorkError("invalid-argument", "limit must be 1..100")
        binding = canonical_sha256(self._order)[:24]
        offset = self._offset(cursor, "work-snapshot", binding)
        works = [self._works[work_id] for work_id in self._order[offset:offset + limit]]
        next_offset = offset + len(works)
        return {"ok": True, "schema": WORK_SCHEMA, "works": copy.deepcopy(works),
                "next_cursor": self._cursor("work-snapshot", next_offset, binding)
                    if next_offset < len(self._order) else None,
                "high_watermark": self._seq}

    def events(self, *, after_seq=0):
        if not isinstance(after_seq, int) or isinstance(after_seq, bool) or after_seq < 0:
            raise UiWorkError("invalid-argument", "after_seq must be non-negative")
        floor = self._events[0]["seq"] if self._events else self._seq + 1
        if after_seq + 1 < floor:
            raise UiWorkError("event-gap", json.dumps({"from_seq": after_seq + 1,
                "to_seq": floor - 1, "retention_floor": floor, "high_watermark": self._seq}))
        return {"ok": True, "schema": WORK_EVENT_SCHEMA,
                "events": copy.deepcopy([event for event in self._events if event["seq"] > after_seq]),
                "high_watermark": self._seq}

    def evidence(self, work_id, *, cursor=None, limit=10):
        if not isinstance(limit, int) or isinstance(limit, bool) or not 1 <= limit <= 50:
            raise UiWorkError("invalid-argument", "limit must be 1..50")
        work = self._works.get(work_id)
        if work is None:
            raise UiWorkError("work-missing", "unknown work projection")
        binding = canonical_sha256([work_id, work["revision"]])[:24]
        offset = self._offset(cursor, "work-evidence", binding)
        evidence = work["evidence"][offset:offset + limit]
        next_offset = offset + len(evidence)
        return {"ok": True, "work_id": work_id, "revision": work["revision"],
                "evidence": copy.deepcopy(evidence),
                "next_cursor": self._cursor("work-evidence", next_offset, binding)
                    if next_offset < len(work["evidence"]) else None}
