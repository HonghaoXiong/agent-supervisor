"""Zero-model capability discovery and receipt-derived qualification."""

from __future__ import annotations

import hashlib
from pathlib import Path

from .work_contracts import canonical_sha256


CAPABILITIES_SCHEMA = "zsc.capabilities/v1"
QUALIFICATION_SCHEMA = "zsc.qualification/v1"


class CapabilityError(ValueError):
    def __init__(self, code: str, message: str, details=None):
        super().__init__(f"{code}: {message}")
        self.code = code
        self.details = details or []


def _skill_records(config: dict, worker: str) -> dict:
    records = {}
    roots = config.get("skill_roots", {}).get(worker, [])
    for root_value in roots:
        root = Path(root_value)
        try:
            markers = sorted(root.glob("*/SKILL.md"))
        except OSError as error:
            raise CapabilityError("skill-discovery-failed", str(error)[:300]) from error
        for marker in markers:
            try:
                data = marker.read_bytes()
            except OSError as error:
                raise CapabilityError("skill-discovery-failed", str(error)[:300]) from error
            records.setdefault(marker.parent.name, []).append({
                "root": str(root), "path": str(marker),
                "sha256": hashlib.sha256(data).hexdigest(),
            })
    return records


def discover_capabilities(config: dict, request: dict) -> dict:
    """Discover presence and admission state without calling any model/tool."""
    if not isinstance(config, dict) or not isinstance(request, dict):
        raise CapabilityError("invalid-argument", "config and request must be objects")
    worker = request.get("worker", "zcode")
    if worker not in ("zcode", "codex", "dsh"):
        raise CapabilityError("invalid-argument", "unknown worker")
    required = request.get("required", [])
    preferred = request.get("preferred", [])
    if not isinstance(required, list) or not isinstance(preferred, list) \
            or any(not isinstance(x, str) or not x for x in required + preferred):
        raise CapabilityError("invalid-argument", "required/preferred must be name lists")
    records = _skill_records(config, worker)
    advertised = set(config.get("advertised_skills", {}).get(worker, []))
    enabled = set(config.get("enabled_skills", {}).get(worker, []))
    capabilities = []
    unknowns = []
    seen = set()
    for name in required + preferred:
        if name in seen:
            continue
        seen.add(name)
        sources = records.get(name, [])
        if not sources:
            status = "missing"
            unknowns.append(name)
            entry = {"name": name, "requested_name": name, "mechanism": "skill",
                     "status": status, "qualified": False, "sources": []}
        else:
            is_advertised = name in advertised
            is_enabled = name in enabled
            status = ("available_unqualified" if is_advertised and is_enabled
                      else "present_not_advertised" if not is_advertised
                      else "present_not_enabled")
            entry = {"name": name, "requested_name": name, "mechanism": "skill",
                     "status": status, "qualified": False, "sources": sources,
                     "sha256": sources[0]["sha256"],
                     "ambiguous": len({source["sha256"] for source in sources}) > 1,
                     "usage": "investigation template only; content does not grant tools"}
        capabilities.append(entry)
    if request.get("needs_workflow"):
        tools = config.get("tools", {}).get(worker, {})
        advertised_tools = set(tools.get("advertised", []))
        enabled_tools = set(tools.get("enabled", []))
        native = config.get("native", {}).get("workflow", {})
        available = "CreateWorkflow" in advertised_tools and "CreateWorkflow" in enabled_tools \
            and native.get("supported") is True
        capabilities.append({
            "name": "CreateWorkflow", "requested_name": "workflow",
            "mechanism": "native", "status": "advertised_uninvoked" if available else "missing",
            "qualified": False, "advertised": "CreateWorkflow" in advertised_tools,
            "enabled": "CreateWorkflow" in enabled_tools,
            "native_supported": native.get("supported") is True,
            "usage": "native workflow execution, distinct from a skill template",
        })
        if not available:
            unknowns.append("workflow")
    return {"schema_version": CAPABILITIES_SCHEMA, "worker": worker,
            "bundle_sha256": config.get("bundle_sha256"),
            "capabilities": capabilities, "unknowns": sorted(set(unknowns)),
            "discovery": {"model_requests": 0, "tool_invocations": 0,
                          "qualification": "not_evaluated"}}


def validate_qualification(receipt: dict, bindings: dict) -> dict:
    """Validate host-observed identity/tool/terminal evidence, never worker prose."""
    reasons = []
    if not isinstance(receipt, dict) or not isinstance(bindings, dict):
        return {"schema_version": QUALIFICATION_SCHEMA, "qualified": False,
                "status": "unqualified", "reasons": ["invalid-argument"]}
    identity = receipt.get("identity")
    required_identity = {"worker", "mode", "session_id", "invocation_id",
                         "host_verified", "bundle_sha256"}
    if not isinstance(identity, dict) or not required_identity <= set(identity):
        reasons.append("identity-missing")
        identity = {}
    else:
        if identity.get("host_verified") is not True:
            reasons.append("identity-untrusted")
        for field in ("worker", "mode", "bundle_sha256"):
            if field in bindings and identity.get(field) != bindings[field]:
                reasons.append("identity-binding-drift")
    tool = receipt.get("tool")
    if not isinstance(tool, dict) or not {"name", "call_id", "invoked", "result_ref"} <= set(tool):
        reasons.append("tool-incomplete")
        tool = {}
    else:
        if tool.get("invoked") is not True:
            reasons.append("tool-not-invoked")
        required_tool = bindings.get("required_tool")
        if required_tool and tool.get("name") != required_tool:
            reasons.append("wrong-tool")
    terminal = receipt.get("terminal")
    terminal_fields = {"status", "root_matches", "exact_commands", "declared_children_only"}
    if not isinstance(terminal, dict) or not terminal_fields <= set(terminal):
        reasons.append("terminal-incomplete")
        terminal = {}
    else:
        if terminal.get("status") not in ("completed", "needs_human"):
            reasons.append("terminal-not-settled")
        for field in ("root_matches", "exact_commands", "declared_children_only"):
            if terminal.get(field) is not True:
                reasons.append(f"terminal-{field.replace('_', '-')}")
    receipt_bindings = receipt.get("bindings")
    if not isinstance(receipt_bindings, dict):
        reasons.append("bindings-missing")
    else:
        for field in ("skill_sha256", "template_sha256", "bundle_sha256", "root"):
            if field in bindings and receipt_bindings.get(field) != bindings[field]:
                reasons.append("binding-drift")
    reasons = sorted(set(reasons))
    result = {
        "schema_version": QUALIFICATION_SCHEMA,
        "capability": receipt.get("capability"),
        "qualified": not reasons,
        "status": "qualified" if not reasons else "unqualified",
        "reasons": reasons,
        "identity_evidence": {key: identity.get(key) for key in sorted(required_identity)
                              if key in identity},
        "tool_evidence": dict(tool), "terminal_evidence": dict(terminal),
        "bindings_sha256": canonical_sha256(bindings),
        "worker_claims_consumed": False,
    }
    result["receipt_sha256"] = canonical_sha256(receipt)
    return result


def validate_mode_qualification(receipt: dict, bindings: dict) -> dict:
    """Apply the same qualification schema to cold and resident host evidence."""
    result = validate_qualification(receipt, bindings)
    reasons = list(result["reasons"])
    evidence = receipt.get("mode_evidence") if isinstance(receipt, dict) else None
    if not isinstance(evidence, dict):
        evidence = {}
    identity = receipt.get("identity", {}) if isinstance(receipt, dict) else {}
    for feature in bindings.get("required_features", []):
        item = evidence.get(feature)
        if not isinstance(item, dict) or item.get("host_verified") is not True:
            reasons.append(f"mode-evidence-missing:{feature}")
            continue
        if feature == "session_turn_ownership":
            if item.get("session_id") != identity.get("session_id") or not item.get("turn_id"):
                reasons.append("session-turn-ownership-mismatch")
        elif feature == "skill":
            if item.get("loaded") is not True or not item.get("receipt_ref"):
                reasons.append("skill-load-receipt-missing")
            if item.get("sha256") != bindings.get("skill_sha256"):
                reasons.append("skill-binding-drift")
        elif feature == "model":
            if not item.get("requested") or item.get("observed") != item.get("requested"):
                reasons.append("model-identity-mismatch")
        elif feature == "cancel_restart_attach":
            if any(item.get(name) is not True for name in ("cancel", "restart", "attach")):
                reasons.append("lifecycle-equivalence-missing")
        elif feature == "workspace_children":
            if item.get("candidate_isolated") is not True or item.get("child_read_only") is not True:
                reasons.append("workspace-child-permissions-unqualified")
    result["reasons"] = sorted(set(reasons))
    result["qualified"] = not result["reasons"]
    result["status"] = "qualified" if result["qualified"] else "unqualified"
    result["mode"] = identity.get("mode")
    result["mode_evidence_sha256"] = canonical_sha256(evidence)
    return result


def select_recon_profile(request: dict, capabilities: dict) -> dict:
    if capabilities.get("schema_version") != CAPABILITIES_SCHEMA:
        raise CapabilityError("invalid-argument", "unknown capabilities schema")
    entries = {entry["name"]: entry for entry in capabilities.get("capabilities", [])}
    missing = []
    for name in request.get("required", []):
        entry = entries.get(name)
        if entry is None or entry.get("status") not in ("available_unqualified", "qualified"):
            missing.append(name)
        elif entry.get("ambiguous") and not request.get("selected_root"):
            missing.append(f"{name}(ambiguous-root)")
    degradations = []
    for name in request.get("preferred", []):
        entry = entries.get(name)
        if entry is None or entry.get("status") not in ("available_unqualified", "qualified"):
            degradations.append({"name": name, "reason": "preferred capability is not currently admitted"})
    if request.get("needs_workflow"):
        workflow = entries.get("CreateWorkflow")
        if workflow is None or workflow.get("status") == "missing":
            missing.append("workflow")
    if missing:
        raise CapabilityError("capability-missing", "required capabilities are unavailable",
                              [{"name": name, "status": "missing_or_unadmitted"} for name in missing])
    selected = [name for name in request.get("required", []) + request.get("preferred", [])
                if name in entries and not any(x["name"] == name for x in degradations)]
    return {"schema_version": "zsc.recon-profile/v1",
            "worker": capabilities.get("worker"), "selected_skills": selected,
            "native_tools": ["CreateWorkflow"] if request.get("needs_workflow") else [],
            "degradations": degradations,
            "execution_gate": "qualification_required_before_execution",
            "capabilities_sha256": canonical_sha256(capabilities)}
