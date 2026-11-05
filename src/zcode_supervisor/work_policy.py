"""Deterministic planning-first model and resource routing (zero model calls)."""

from __future__ import annotations

import math


class PolicyError(ValueError):
    def __init__(self, code, message):
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message


def _model_entries(capabilities):
    entries = []
    if isinstance(capabilities.get("models"), list):
        for item in capabilities["models"]:
            if not isinstance(item, dict) or not isinstance(item.get("provider_id"), str) \
                    or not isinstance(item.get("model_id"), str):
                raise PolicyError("schema-invalid", "model entries require provider_id/model_id")
            if item.get("enabled", True) is True:
                entries.append((item["provider_id"], item["model_id"]))
    roster = capabilities.get("roster")
    if isinstance(roster, dict):
        for provider in roster.get("providers", []):
            for model in provider.get("models", []):
                entries.append((provider.get("providerId"), model.get("modelId")))
    result = []
    for provider, model in entries:
        if not isinstance(provider, str) or not provider or not isinstance(model, str) or not model:
            raise PolicyError("schema-invalid", "roster contains an invalid explicit model identity")
        if (provider, model) not in result:
            result.append((provider, model))
    return result


def _metadata(policy, model_id):
    value = policy.get("model_metadata", {}).get(model_id)
    if not isinstance(value, dict) or isinstance(value.get("quality"), bool) \
            or not isinstance(value.get("quality"), (int, float)) \
            or not math.isfinite(value["quality"]) or not isinstance(value.get("permissions"), list):
        return None
    return value


def choose_execution_mode(task: dict, support: dict, evidence: dict) -> dict:
    """Select a mode only from equivalent host evidence, never a warm-process alias."""
    if not all(isinstance(value, dict) for value in (task, support, evidence)):
        raise PolicyError("invalid-argument", "task, mode support and evidence must be objects")
    active = task.get("active_session")
    requested = task.get("requested_mode")
    if isinstance(active, dict) and active.get("session_id") and active.get("mode") \
            and requested and requested != active["mode"]:
        raise PolicyError("active-session-mode-immutable",
                          "an existing session cannot silently cross execution modes")
    needs_orchestration = any(task.get(name) for name in
                              ("needs_skills", "needs_workflow", "needs_orchestration"))
    rejected = {}
    missing = []
    resident = support.get("resident", {})
    if resident.get("supported") is not True:
        rejected["resident"] = ["mode-unsupported"]
    elif needs_orchestration and resident.get("orchestration") is not True:
        rejected["resident"] = ["orchestration-unsupported"]
    elif evidence.get("resident", {}).get("qualified") is not True:
        rejected["resident"] = ["equivalent-qualification-missing"]
        missing.append("resident-equivalent-qualification")
    if support.get("cold", {}).get("supported") is not True:
        rejected["cold"] = ["mode-unsupported"]
    selected = None
    basis = None
    if "resident" not in rejected and (requested == "resident" or not needs_orchestration):
        warm = evidence.get("resident", {})
        cold = evidence.get("cold", {})
        if requested == "resident":
            selected, basis = "resident", "explicit-request-with-equivalent-qualification"
        elif warm.get("measured") is True and cold.get("measured") is True \
                and isinstance(warm.get("whole_task_ms"), (int, float)) \
                and isinstance(cold.get("whole_task_ms"), (int, float)) \
                and warm["whole_task_ms"] < cold["whole_task_ms"]:
            selected, basis = "resident", "equivalent-qualified-whole-task-measurement"
        else:
            missing.append("whole-task-comparison-missing")
    if selected is None and "cold" not in rejected:
        selected, basis = "cold", basis or "supported-default-without-resident-equivalence"
    if selected is None:
        raise PolicyError("mode-unqualified", "no execution mode has the required support and evidence")
    return {"schema_version": "zsc.execution-mode-decision/v1", "selected_mode": selected,
            "selection_basis": basis, "rejected_modes": rejected,
            "missing_evidence": sorted(set(missing)),
            "qualification_schema": "zsc.qualification/v1", "mode_is_mutable_alias": False,
            "execution_gate": "qualification-required-before-dispatch",
            "observed_timings": {mode: {key: value.get(key) for key in
                ("startup_ms", "whole_task_ms", "measured")}
                for mode, value in evidence.items() if isinstance(value, dict)},
            "model_requests": 0}


def choose_route(task, capabilities, policy, history) -> dict:
    """Choose an explicit roster model after quality/permission admission.

    Model names never imply quality. Cost/time comparisons require measured
    history for every admitted candidate; otherwise an explicit default or
    roster order is retained and the missing evidence is reported.
    """
    if not all(isinstance(value, dict) for value in (task, capabilities, policy, history)):
        raise PolicyError("invalid-argument", "task, capabilities, policy and history must be objects")
    stage = task.get("stage", "recon")
    if stage not in ("plan", "recon", "implementation", "verification"):
        raise PolicyError("invalid-argument", f"unsupported planning stage {stage!r}")
    if policy.get("schema_version") != "zsc.routing-policy/v1":
        raise PolicyError("schema-invalid", "routing policy must be zsc.routing-policy/v1")
    budgets = policy.get("budgets", {})
    if budgets.get("require_strict_provider_budget") is True \
            or budgets.get("max_provider_tokens") is not None \
            or budgets.get("max_provider_cost_usd") is not None:
        raise PolicyError("provider-budget-unsupported",
            "the current provider adapter exposes no strict token/cost cap; refusing unbounded execution")
    requirements = policy.get("stage_requirements", {}).get(stage,
        policy.get("stage_requirements", {}).get("recon", {"min_quality": 0, "permissions": []}))
    min_quality = requirements.get("min_quality", 0)
    required_permissions = set(requirements.get("permissions", []))
    entries = _model_entries(capabilities)
    admitted = []
    rejected = {}
    for provider, model in entries:
        model_id = f"{provider}/{model}"
        metadata = _metadata(policy, model_id)
        reasons = []
        if metadata is None:
            reasons.append("quality-or-permission-metadata-missing")
        else:
            if metadata["quality"] < min_quality:
                reasons.append("quality-below-requirement")
            missing = sorted(required_permissions - set(metadata["permissions"]))
            if missing:
                reasons.append("permissions-missing:" + ",".join(missing))
        if reasons:
            rejected[model_id] = reasons
        else:
            admitted.append({"id": model_id, "provider_id": provider, "model_id": model,
                             "quality": metadata["quality"],
                             "permissions": sorted(set(metadata["permissions"]))})
    measurements = history.get("models", {})
    complete_history = bool(admitted) and all(
        isinstance(measurements.get(item["id"]), dict)
        and measurements[item["id"]].get("measured") is True
        and isinstance(measurements[item["id"]].get("cost"), (int, float))
        and isinstance(measurements[item["id"]].get("wall_time_ms"), (int, float))
        for item in admitted)
    default_id = policy.get("defaults", {}).get(stage)
    selected = None
    basis = "unqualified"
    missing_evidence = []
    if admitted:
        if complete_history:
            selected = min(admitted, key=lambda item: (
                measurements[item["id"]]["cost"], measurements[item["id"]]["wall_time_ms"], item["id"]))
            basis = "measured-cost-then-time"
        else:
            missing_evidence.append("no-measured-history" if not measurements else "incomplete-measured-history")
            selected = next((item for item in admitted if item["id"] == default_id), admitted[0])
            basis = "default-or-metadata-not-name"
    alternatives = []
    for item in admitted:
        if selected is not None and item["id"] == selected["id"]:
            continue
        observed = measurements.get(item["id"], {})
        alternatives.append({"id": item["id"], "quality": item["quality"],
            "observed_cost": observed.get("cost") if observed.get("measured") is True else None,
            "observed_wall_time_ms": observed.get("wall_time_ms") if observed.get("measured") is True else None})
    alternatives.sort(key=lambda item: (
        item["observed_cost"] is None, item["observed_cost"] if item["observed_cost"] is not None else 0,
        item["id"]))
    selected_doc = None
    if selected is not None:
        selected_doc = {"id": selected["id"],
            "model": {"providerId": selected["provider_id"], "modelId": selected["model_id"]},
            "quality": selected["quality"], "permissions": selected["permissions"]}
    brief = task.get("brief")
    reusable = isinstance(brief, dict) and brief.get("trusted") is True and brief.get("valid") is True \
        and isinstance(brief.get("sha256"), str) and brief.get("session_id") == task.get("session_id")
    mode = "delta" if reusable else ("full_replay" if isinstance(brief, dict) else "full")
    upgrade = []
    if selected is None:
        upgrade.append("register-qualified-model-or-adjust-explicit-policy")
    if isinstance(brief, dict) and not reusable:
        upgrade.append("refresh-brief-and-replay-in-current-session")
    dispatch_status = task.get("dispatch_status")
    max_attempts = policy.get("max_new_attempts", 1)
    if isinstance(max_attempts, bool) or not isinstance(max_attempts, int) or not 0 <= max_attempts <= 1:
        raise PolicyError("schema-invalid", "max_new_attempts defaults to and may not exceed one")
    if dispatch_status == "sent-unknown":
        retry = {"allowed": False, "reason": "sent-unknown-requires-reconciliation",
                 "max_new_attempts": max_attempts, "sent_unknown_auto_retry": False}
    else:
        used = task.get("new_attempts_used", 0)
        retry = {"allowed": dispatch_status == "failed-before-send" and used < max_attempts,
                 "reason": "new-attempt-budget" if used < max_attempts else "new-attempt-budget-exhausted",
                 "max_new_attempts": max_attempts, "sent_unknown_auto_retry": False}
    observed = history.get("observed_usage", {})
    return {"schema_version": "zsc.routing-decision/v1", "status": "planned" if selected else "unqualified",
            "stage": stage, "selected": selected_doc, "selection_basis": basis,
            "reasons": ["quality-and-permissions-before-cost", "one-recon-agent-by-default"],
            "alternatives": alternatives, "rejected_candidates": rejected,
            "missing_evidence": missing_evidence,
            "team": {"recon_agents": 1, "specialists": [],
                     "reason": "no specialist is inferred from absent domain features"},
            "investigation_mode": mode,
            "reuse": {"brief_sha256": brief.get("sha256") if reusable else None,
                      "same_session": reusable},
            "enforceable_budget": {"max_wall_time_s": budgets.get("max_wall_time_s"),
                "max_concurrency": budgets.get("max_concurrency", 1),
                "allowed_commands": list(budgets.get("allowed_commands", [])),
                "enforcement": "host-command-time-concurrency"},
            "provider_budget": {
                "status": "unsupported", "token_cap_enforced": False,
                "cost_cap_enforced": False,
                "reason": "current-provider-adapter-has-no-strict-token-or-cost-cap",
                "strict_request_behavior": "fail-closed-before-dispatch",
                "observed_usage_recording": "unchanged"},
            "observed_only_budget": {
                "team_tokens": observed.get("team_tokens") if isinstance(observed.get("team_tokens"), (int, float)) else None,
                "team_cost": observed.get("team_cost") if isinstance(observed.get("team_cost"), (int, float)) else None,
                "team_cost_status": "measured" if isinstance(observed.get("team_cost"), (int, float)) else "unknown",
                "enforcement": "observed-only-not-a-provider-wide-cap"},
            "retry": retry, "upgrade_conditions": upgrade, "model_requests": 0}
