"""Deterministic, non-executing stage contract compiler."""

from __future__ import annotations

import re
from pathlib import Path

from .contracts import canonical_json
from .work_contracts import canonical_sha256
from .work_authorization import validate_authorization
from .work_models import WorkError


_SLOT = re.compile(r"^\{([A-Za-z_][A-Za-z0-9_]*)\}$")


def _inside(path, parent):
    resolved = Path(path).resolve(strict=False)
    try:
        resolved.relative_to(Path(parent).resolve(strict=True))
    except (ValueError, OSError) as error:
        raise WorkError("scope-conflict", f"path escapes authorized parent: {path}") from error
    if resolved == Path(parent).resolve():
        raise WorkError("scope-conflict", "workspace must be a child, not the candidate parent")
    return resolved


def _command(template, slots, parent):
    if not isinstance(template, dict) or set(template) != {"argv"} \
            or not isinstance(template["argv"], list) or not template["argv"] \
            or any(not isinstance(x, str) or not x for x in template["argv"]):
        raise WorkError("schema-invalid", "command template must be a fixed argv")
    result = []
    for argument in template["argv"]:
        match = _SLOT.fullmatch(argument)
        if match:
            name = match.group(1)
            if name not in slots or not isinstance(slots[name], str) or "\x00" in slots[name]:
                raise WorkError("invalid-argument", f"missing or invalid path slot {name}")
            result.append(str(_inside(slots[name], parent)))
        elif "{" in argument or "}" in argument:
            raise WorkError("invalid-argument", "only whole-argv typed path slots are allowed")
        else:
            result.append(argument)
    return {"argv": result}


def compile_stage_contract(authorization, stage, bindings) -> dict:
    validate_authorization(authorization)
    if not isinstance(stage, str) or stage not in authorization["stages"] \
            or not isinstance(bindings, dict):
        raise WorkError("invalid-argument", "unknown stage or invalid bindings")
    definition = authorization["stages"][stage]
    required = {"workspace", "model", "template_version", "materials", "allowed_files",
                "timeout_sec", "concurrency", "argv_slots"}
    if not required <= set(bindings):
        raise WorkError("invalid-argument", "stage bindings are incomplete")
    workspace = _inside(bindings["workspace"], authorization["candidate_parent"])
    model = bindings["model"]
    if model not in authorization["model_allowlist"] or model not in definition.get("models", []):
        raise WorkError("role-denied", "model is outside both authorization and stage allowlists")
    if bindings["template_version"] != definition.get("template_version"):
        raise WorkError("source-drift", "prompt template version does not match authorization")
    allowed = bindings["allowed_files"]
    if not isinstance(allowed, list) or any(not isinstance(x, str) for x in allowed) \
            or not set(allowed) <= set(definition.get("allowed_files", [])) \
            or not set(allowed) <= set(authorization["writable_relative_paths"]):
        raise WorkError("scope-conflict", "allowed_files expands stage or authorization scope")
    limits = authorization["limits"]
    for field, limit in (("timeout_sec", "timeout_sec"), ("concurrency", "concurrency")):
        value = bindings[field]
        if isinstance(value, bool) or not isinstance(value, int) or value < 1 \
                or value > limits.get(limit, 0):
            raise WorkError("scope-conflict", f"{field} exceeds authorization")
    template = definition.get("prompt_template")
    if not isinstance(template, str) or template.count("{{materials}}") != 1 \
            or template.replace("{{materials}}", "").find("{{") >= 0:
        raise WorkError("schema-invalid", "stage prompt must contain only the materials slot")
    prompt = template.replace("{{materials}}", canonical_json(bindings["materials"]))
    command = _command(definition.get("command"), bindings["argv_slots"],
                       authorization["candidate_parent"])
    contract = {
        "schema_version": "zsc.derived-contract/v1", "stage": stage,
        "workspace": str(workspace), "prompt": prompt, "model": model,
        "allowed_files": list(allowed), "timeout_sec": bindings["timeout_sec"],
        "concurrency": bindings["concurrency"], "command": command,
        "authorization_sha256": canonical_sha256(authorization),
        "bindings_sha256": canonical_sha256({k: v for k, v in bindings.items() if k != "registered_contracts"}),
        "compiler_version": "contract-compile/v1", "execution": "requires-separate-durable-intent",
    }
    contract["contract_id"] = "contract-" + canonical_sha256(contract)[:32]
    for old in bindings.get("registered_contracts", []):
        if isinstance(old, dict) and all(old.get(key) == value for key, value in contract.items()
                                         if key != "contract_id"):
            reused = dict(contract)
            reused["contract_id"] = old.get("contract_id")
            reused["reused_registered_contract"] = True
            return reused
    return contract
