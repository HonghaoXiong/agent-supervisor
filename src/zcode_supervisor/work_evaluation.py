"""Offline, event-derived collaboration and trial evaluation.

Business events prove acquisition, references, tool receipts, ordering and actor
identity. They do not prove that a model understood an artifact. Missing evidence
stays unknown and missing measurements stay null.
"""

from __future__ import annotations

import json
import math
from typing import Iterable

from .bench import phase_stats
from .work_contracts import canonical_sha256


class EvaluationError(ValueError):
    def __init__(self, code, message):
        super().__init__(message)
        self.code = code
        self.message = message


def _events(events):
    if not isinstance(events, list):
        raise EvaluationError("schema-invalid", "events must be an array")
    seen = set()
    result = []
    for event in events:
        if not isinstance(event, dict) or not isinstance(event.get("event_id"), str) \
                or not event["event_id"] or isinstance(event.get("seq"), bool) \
                or not isinstance(event.get("seq"), int) or event["seq"] < 0 \
                or not isinstance(event.get("kind"), str):
            raise EvaluationError("schema-invalid", "each event needs event_id, nonnegative seq and kind")
        if event["event_id"] in seen or event["seq"] in {item["seq"] for item in result}:
            raise EvaluationError("event-conflict", "event IDs and sequence numbers must be unique")
        seen.add(event["event_id"])
        result.append(event)
    return sorted(result, key=lambda item: (item["seq"], item["event_id"]))


def _measure(status, rationale, event_ids=(), **extra):
    return {"status": status, "rationale": rationale,
            "evidence_event_ids": list(event_ids), **extra}


def evaluate_adherence(events, design) -> dict:
    events = _events(events)
    if not isinstance(design, dict) or not isinstance(design.get("sha256"), str):
        raise EvaluationError("schema-invalid", "design requires a stable sha256")
    measures = {}
    for key, kinds, rationale in (
        ("report_acquired", {"artifact_read"}, "proves artifact acquisition only"),
        ("report_referenced", {"decision_reference", "artifact_reference"},
         "proves an explicit reference, not comprehension"),
    ):
        expected = design.get("brief_ref")
        matching = [event for event in events if event["kind"] in kinds
                    and expected is not None and event.get("artifact_ref") == expected]
        measures[key] = _measure("passed" if matching else "unknown",
                                 rationale if matching else "no matching business event",
                                 [event["event_id"] for event in matching])
    skills = {}
    for skill in design.get("required_skills", []):
        matching = [event for event in events if event["kind"] == "skill_loaded"
                    and event.get("skill") == skill and event.get("tool") == "Skill"
                    and event.get("host_verified") is True and event.get("status") == "succeeded"]
        skills[skill] = _measure("passed" if matching else "unknown",
                                 "host Skill receipt" if matching else "loading was not observed; prompt claims do not count",
                                 [event["event_id"] for event in matching])
    measures["skills"] = skills
    workflows = [event for event in events if event["kind"] == "tool_result"
                 and event.get("tool") == "CreateWorkflow" and event.get("call_id")
                 and event.get("result_ref") and event.get("status") == "succeeded"]
    if design.get("native_workflow_required") is True:
        measures["native_workflow"] = _measure(
            "passed" if workflows else "unknown",
            "real CreateWorkflow receipt" if workflows else "Skill loading alone cannot satisfy native workflow",
            [event["event_id"] for event in workflows])
    else:
        measures["native_workflow"] = _measure("not_applicable", "native workflow was not required")

    writes = [event for event in events if event["kind"] == "code_write"]
    decisions = [event for event in events if event["kind"] == "design_accepted"
                 and event.get("design_sha256") == design["sha256"]]
    if writes and decisions:
        decision = min(decisions, key=lambda event: event["seq"])
        first_write = min(writes, key=lambda event: event["seq"])
        passed = decision["seq"] < first_write["seq"]
        measures["design_before_code"] = _measure(
            "passed" if passed else "failed",
            "accepted design precedes code" if passed else "code was written before this design was accepted",
            [decision["event_id"], first_write["event_id"]], evidence_event_seq=decision["seq"])
    elif writes:
        measures["design_before_code"] = _measure("failed", "code exists without an accepted current design")
    else:
        measures["design_before_code"] = _measure("unknown", "no code-write event to order")

    acceptances = [event for event in events if event["kind"] == "record_acceptance"
                   and event.get("status") == "accepted"]
    contributors = {(event.get("actor_id"), event.get("session_id")) for event in events
                    if event["kind"] in ("code_write", "submit_delivery")}
    contributor_ids = {actor for actor, _ in contributors if actor}
    contributor_sessions = {session for _, session in contributors if session}
    if acceptances and contributors:
        independent = [event for event in acceptances
                       if event.get("actor_id") not in contributor_ids
                       and event.get("session_id") not in contributor_sessions]
        measures["independent_verification"] = _measure(
            "passed" if independent else "failed",
            "verifier differs from all contributors" if independent else "author or contributor session self-accepted",
            [event["event_id"] for event in (independent or acceptances)])
    else:
        measures["independent_verification"] = _measure(
            "unknown", "both contributor lineage and an acceptance event are required")
    statuses = [value["status"] for value in measures.values() if isinstance(value, dict) and "status" in value]
    statuses += [value["status"] for value in skills.values()]
    overall = "failed" if "failed" in statuses else ("unknown" if "unknown" in statuses else "passed")
    return {"schema_version": "zsc.adherence-report/v1", "overall": overall,
            "design_sha256": design["sha256"], "measures": measures,
            "claims_content_comprehension": False,
            "method": "versioned-business-events-only", "event_count": len(events)}


def evaluate_dependencies(events, checks) -> dict:
    events = _events(events)
    if not isinstance(checks, list):
        raise EvaluationError("schema-invalid", "checks must be an array")
    required = [check for check in checks if check.get("required") is True]
    results = []
    closing = []
    for check in required:
        if not all(isinstance(check.get(key), str) and check[key] for key in
                   ("edge_id", "checker_id", "candidate_ref", "oracle_ref")):
            raise EvaluationError("schema-invalid", "dependency checks require edge/checker/candidate/oracle")
        matching = [event for event in events if event["kind"] == "checker_result"
                    and all(event.get(key) == check[key] for key in
                            ("edge_id", "checker_id", "candidate_ref", "oracle_ref"))
                    and event.get("author_self") is False]
        latest = matching[-1] if matching else None
        status = latest.get("status") if latest else "unknown"
        if status not in ("passed", "failed", "unknown"):
            status = "unknown"
        results.append({"edge_id": check["edge_id"], "checker_id": check["checker_id"],
                        "candidate_ref": check["candidate_ref"], "oracle_ref": check["oracle_ref"],
                        "status": status, "event_id": latest["event_id"] if latest else None,
                        "seq": latest["seq"] if latest else None})
        if status == "passed":
            closing.append(latest)
    passed = sum(item["status"] == "passed" for item in results)
    closed = bool(results) and passed == len(results)
    local = [event for event in events if event["kind"] == "local_test"]
    local_rate = (sum(event.get("status") == "passed" for event in local) / len(local)) if local else None
    return {"schema_version": "zsc.dependency-evaluation/v1",
            "results": results, "required_count": len(results), "passed_count": passed,
            "pass_rate": (passed / len(results)) if results else None,
            "first_closed_seq": max((event["seq"] for event in closing), default=None) if closed else None,
            "closing_event_ids": sorted((event["event_id"] for event in closing)) if closed else [],
            "local_test_pass_rate": local_rate,
            "author_claims_used": False,
            "status": "closed" if closed else "not-closed"}


def _number(value):
    return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)


def compare_trials(manifest, results) -> dict:
    if not isinstance(manifest, dict) or not isinstance(manifest.get("tasks"), list) \
            or not isinstance(manifest.get("conditions"), list) or not isinstance(results, list):
        raise EvaluationError("schema-invalid", "manifest tasks/conditions and results array are required")
    expected_conditions = ["solo", "legacy", "no-practices", "full"]
    if manifest["conditions"] != expected_conditions:
        raise EvaluationError("schema-invalid", "all four ordered conditions are required")
    tasks = {task.get("task_id"): task for task in manifest["tasks"]
             if isinstance(task, dict) and isinstance(task.get("task_id"), str)}
    if len(tasks) != len(manifest["tasks"]):
        raise EvaluationError("schema-invalid", "task IDs must be unique objects")
    normalized = []
    warnings = []
    for result in results:
        if not isinstance(result, dict) or result.get("condition") not in expected_conditions \
                or result.get("task_id") not in tasks or not isinstance(result.get("trial_id"), str):
            raise EvaluationError("schema-invalid", "trial has unknown task/condition or missing trial_id")
        task = tasks[result["task_id"]]
        if result.get("oracle_ref") != task.get("oracle_ref") or result.get("oracle_independent") is not True:
            raise EvaluationError("oracle-invalid", "trial is not bound to the manifest's independent oracle")
        materials = result.get("worker_materials", [])
        sensitive_keys = {"oracle", "oracle_ref", "expected", "answers", "expected_output"}
        leaked = bool(sensitive_keys.intersection(materials)) or task.get("oracle_ref") in materials
        isolation = "failed" if leaked else "passed"
        if leaked:
            warnings.append("oracle-isolation")
        item = dict(result)
        item["oracle_isolation"] = isolation
        normalized.append(item)
    normalized.sort(key=lambda item: (item["task_id"], item["condition"], item["trial_id"]))
    conditions = {}
    for condition in expected_conditions:
        trials = [item for item in normalized if item["condition"] == condition]
        successful = [item for item in trials if item.get("status") == "passed"]
        missing = sorted({key for item in trials for key in ("tokens", "cost", "wall_time_ms")
                          if not _number(item.get(key))})
        def total(key):
            values = [item.get(key) for item in trials]
            return sum(values) if trials and all(_number(value) for value in values) else None
        wall_samples = [float(item["wall_time_ms"]) for item in successful if _number(item.get("wall_time_ms"))]
        conditions[condition] = {
            "trials": len(trials), "successful": len(successful),
            "failures": sum(item.get("status") != "passed" for item in trials),
            "retries": sum(item.get("retries", 0) for item in trials if isinstance(item.get("retries"), int)),
            "human_interventions": sum(item.get("human_interventions", 0) for item in trials
                                       if isinstance(item.get("human_interventions"), int)),
            "team_tokens": {"total": total("tokens"), "status": "measured" if total("tokens") is not None else "unknown"},
            "team_cost": {"total": total("cost"), "status": "measured" if total("cost") is not None else "unknown"},
            "wall_time_ms": phase_stats(wall_samples),
            "missing_measurements": missing,
            "versions": [item.get("versions") for item in trials],
        }
    comparisons = {}
    for task_id in sorted(tasks):
        per_task = {}
        for baseline in expected_conditions[:-1]:
            full = next((item for item in normalized if item["task_id"] == task_id
                         and item["condition"] == "full"), None)
            other = next((item for item in normalized if item["task_id"] == task_id
                          and item["condition"] == baseline), None)
            key = f"full_vs_{baseline}"
            if not full or not other or full.get("status") != "passed" or other.get("status") != "passed" \
                    or not _number(full.get("wall_time_ms")) or not _number(other.get("wall_time_ms")):
                per_task[key] = {"status": "insufficient-data", "wall_time_reduction_percent": None}
            elif other["wall_time_ms"] == 0:
                per_task[key] = {"status": "not-comparable", "wall_time_reduction_percent": None}
            else:
                per_task[key] = {"status": "measured", "wall_time_reduction_percent":
                    (other["wall_time_ms"] - full["wall_time_ms"]) / other["wall_time_ms"] * 100.0,
                    "baseline_trial_id": other["trial_id"], "full_trial_id": full["trial_id"]}
        comparisons[task_id] = per_task
    return {"schema_version": "zsc.trial-comparison/v1", "mode": "replay", "model_requests": 0,
            "manifest_sha256": canonical_sha256(manifest), "results_sha256": canonical_sha256(results),
            "conditions": conditions, "comparisons": comparisons, "trials": normalized,
            "warnings": sorted(set(warnings)),
            "notes": ["Failed trials and missing measurements remain visible; no percentage is imputed.",
                      "Oracle material must never be included in worker materials."]}


def dump_report(report) -> bytes:
    return (json.dumps(report, ensure_ascii=True, sort_keys=True, separators=(",", ":")) + "\n").encode("ascii")
