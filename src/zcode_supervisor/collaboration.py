"""Persistent design→implementation→delivery→independent-acceptance workflow."""

from __future__ import annotations

import json
from pathlib import Path

from .work_models import WorkError
from .work_store import WorkStore
from .dependency_checks import evaluate_dependency_results


class CollaborationService:
    def __init__(self, store: WorkStore):
        self.store = store

    def create_work(self, spec, *, idempotency_key, actor):
        return self.store.create_work(spec, idempotency_key=idempotency_key, actor=actor)

    def context(self, work_id):
        row = self.store._connection.execute("SELECT * FROM works WHERE work_id=?", (work_id,)).fetchone()
        if row is None:
            raise WorkError("work-missing", "unknown work")
        result = {"spec": json.loads(row["spec_json"]), "state": row["state"],
                  "revision": row["revision"], "contributors": []}
        contributors = []
        for event in self.store._connection.execute(
                "SELECT action,payload_json FROM events WHERE work_id=? ORDER BY event_id", (work_id,)):
            payload = json.loads(event["payload_json"])
            if event["action"] == "create":
                continue
            for key, value in payload.items():
                if key not in ("state", "contributors"):
                    result[key] = value
            if "contributors" in payload:
                for contributor in payload["contributors"]:
                    if contributor not in contributors:
                        contributors.append(contributor)
            result["last_action"] = event["action"]
        result["contributors"] = contributors
        return result

    def apply_action(self, work_id, action, payload, *, expected_revision, idempotency_key, actor):
        if not isinstance(payload, dict):
            raise WorkError("schema-invalid", "action payload must be an object")
        context = self.context(work_id)
        transitions = {
            "start_recon": "recon_queued", "recon_running": "recon_running",
            "submit_brief": "brief_ready", "accept_design": "design_ready",
            "start_implementation": "implementation_queued",
            "implementation_running": "implementation_running",
            "submit_delivery": "candidate_ready", "start_verification": "verification_running",
            "record_acceptance": "verification_running", "complete": "completed",
            "rework": "rework_required", "cancel": "cancelled",
            "reconcile": "needs_reconciliation",
        }
        if action not in transitions:
            raise WorkError("invalid-argument", f"unknown collaboration action {action}")
        outgoing = dict(payload)
        outgoing["state"] = transitions[action]
        if action == "record_acceptance":
            outgoing["acceptance"] = {"verdict": "accepted", "candidate_sha256": payload["candidate_sha256"],
                                      "dimensions": dict(payload["dimensions"])}
            if context.get("design", {}).get("interface_contracts"):
                outgoing["dependency_results"] = evaluate_dependency_results(
                    context["design"], payload.get("dependency_receipts", []))
        if context["revision"] != expected_revision:
            # Replay hashes the exact enriched bytes used on the first attempt.
            return self.store.apply_action(work_id, action, outgoing,
                expected_revision=expected_revision, idempotency_key=idempotency_key, actor=actor)
        self._validate(work_id, action, payload, context, actor)
        return self.store.apply_action(work_id, action, outgoing,
            expected_revision=expected_revision, idempotency_key=idempotency_key, actor=actor)

    def _validate(self, work_id, action, payload, context, actor):
        expected = {
            "start_recon": {"created", "rework_required"},
            "recon_running": {"recon_queued"}, "submit_brief": {"recon_running"},
            "accept_design": {"brief_ready"}, "start_implementation": {"design_ready"},
            "implementation_running": {"implementation_queued"},
            "submit_delivery": {"implementation_running"},
            "start_verification": {"candidate_ready"},
            "record_acceptance": {"verification_running"},
            "complete": {"verification_running"},
            "rework": {"brief_ready", "design_ready", "candidate_ready", "verification_running"},
            "cancel": {"created", "recon_queued", "recon_running", "brief_ready", "design_ready",
                       "implementation_queued", "implementation_running", "candidate_ready",
                       "verification_running", "rework_required"},
            "reconcile": {"created", "recon_queued", "recon_running", "brief_ready", "design_ready",
                          "implementation_queued", "implementation_running", "candidate_ready",
                          "verification_running", "rework_required"},
        }
        if action not in expected:
            raise WorkError("invalid-argument", f"unknown collaboration action {action}")
        if context["state"] not in expected[action]:
            raise WorkError("state-conflict", f"{action} is invalid from {context['state']}")
        if action == "start_recon":
            if not payload.get("attempt_id"):
                raise WorkError("schema-invalid", "recon attempt required")
        elif action == "recon_running":
            self._attempt(payload, context)
        elif action == "submit_brief":
            self._attempt(payload.get("envelope", {}), context)
            validation = payload.get("envelope", {}).get("validation", {})
            if validation.get("schema_valid") is not True or validation.get("sources_valid") is not True:
                raise WorkError("brief-invalid", "brief must pass schema and current-source checks")
        elif action == "accept_design":
            actor.require("design:accept")
            if payload.get("unresolved_conflicts") or not isinstance(payload.get("design"), dict) \
                    or not payload["design"].get("sha256") or not payload.get("decision_ref"):
                raise WorkError("dependency-open", "design has unresolved conflicts or missing bindings")
        elif action == "start_implementation":
            if not payload.get("attempt_id"):
                raise WorkError("schema-invalid", "implementation attempt required")
            if payload.get("design_sha256") != context.get("design", {}).get("sha256"):
                raise WorkError("source-drift", "implementation must use the current accepted design")
            parent = Path(context["spec"]["candidate_parent"]).resolve(strict=False)
            workspace = Path(payload.get("workspace", "")).resolve(strict=False)
            if workspace == parent or parent not in workspace.parents:
                raise WorkError("scope-conflict", "implementation requires an isolated candidate child")
            if not set(payload.get("allowed_files", [])) <= set(context["design"].get("allowed_files", [])):
                raise WorkError("scope-conflict", "write set exceeds accepted design")
            if not isinstance(payload.get("base_manifest"), dict):
                raise WorkError("schema-invalid", "B0 manifest is required")
        elif action == "implementation_running":
            self._attempt(payload, context)
        elif action == "submit_delivery":
            if payload.get("status") != "delivered" or not payload.get("candidate", {}).get("sha256") \
                    or not isinstance(payload.get("candidate_manifest"), dict) \
                    or not isinstance(payload.get("contributors"), list):
                raise WorkError("delivery-invalid", "a failed channel/result cannot become candidate_ready")
            if payload.get("unexpected_writes") or payload.get("target_drift"):
                raise WorkError("source-drift", "unexpected writes or target-tree drift")
            if "design_sha256" in payload and payload["design_sha256"] != context.get("design_sha256"):
                raise WorkError("source-drift", "B1 changed the design without a new decision")
        elif action == "start_verification":
            actor.require("verification:start")
            if payload.get("candidate_sha256") != context.get("candidate", {}).get("sha256"):
                raise WorkError("source-drift", "verification candidate differs from delivery")
            verifier = payload.get("verifier", {})
            for contributor in context["contributors"]:
                if verifier.get("actor_id") == contributor.get("actor_id") \
                        or verifier.get("session_id") == contributor.get("session_id"):
                    raise WorkError("independence-conflict", "contributor or contributor session cannot verify")
        elif action == "record_acceptance":
            if payload.get("verifier") != context.get("verifier") \
                    or payload.get("candidate_sha256") != context.get("candidate_sha256"):
                raise WorkError("independence-conflict", "acceptance verifier/candidate binding mismatch")
            checks = payload.get("checks", [])
            required = set(context.get("required_checks", []))
            passed = {check.get("id") for check in checks
                      if check.get("status") == "passed" and check.get("required") is True}
            dependencies = payload.get("dependencies", [])
            if context.get("design", {}).get("interface_contracts"):
                dependency_report = evaluate_dependency_results(
                    context["design"], payload.get("dependency_receipts", []))
                if dependency_report["status"] != "closed" \
                        or dependency_report.get("candidate_ref") != payload["candidate_sha256"]:
                    raise WorkError("acceptance-blocked", "required interface dependencies are not closed on this candidate")
            elif not required <= passed or any(item.get("status") != "passed" for item in dependencies):
                raise WorkError("acceptance-blocked", "required checks or dependencies have not passed")
            if not required <= passed:
                raise WorkError("acceptance-blocked", "required checks have not passed")
            dimensions = payload.get("dimensions", {})
            if dimensions.get("code") != "passed":
                raise WorkError("acceptance-blocked", "code evidence is required; other dimensions are not implied")
        elif action == "complete":
            actor.require("acceptance:record")
            if context.get("acceptance", {}).get("verdict") != "accepted" \
                    or payload.get("candidate_sha256") != context.get("acceptance", {}).get("candidate_sha256"):
                raise WorkError("acceptance-blocked", "only the accepted candidate can complete")
        elif action in ("cancel", "reconcile"):
            if payload.get("attempt_id") and payload.get("attempt_id") != context.get("attempt_id"):
                raise WorkError("state-conflict", "attempt is not the current attempt")
        elif action == "rework":
            if not payload.get("reason") or payload.get("baseline") not in ("B0", "B1") \
                    or not payload.get("resume_phase"):
                raise WorkError("schema-invalid", "rework requires reason, resume phase and explicit baseline")

    @staticmethod
    def _attempt(payload, context):
        if payload.get("attempt_id") != context.get("attempt_id"):
            raise WorkError("state-conflict", "attempt is not the current attempt")
