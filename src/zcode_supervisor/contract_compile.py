"""Deterministic, non-executing stage contract compiler."""

from __future__ import annotations

import re
from pathlib import Path

from .contracts import canonical_json
from .work_contracts import canonical_sha256
from .work_authorization import validate_authorization
from .work_models import WorkError


_SLOT = re.compile(r"^\{([A-Za-z_][A-Za-z0-9_]*)\}$")


def _inside(path, parent, *, inclusive=False):
    resolved = Path(path).resolve(strict=False)
    parent = Path(parent).resolve(strict=True)
    try:
        resolved.relative_to(parent)
    except (ValueError, OSError) as error:
        raise WorkError("scope-conflict", f"path escapes authorized parent: {path}") from error
    if resolved == parent and not inclusive:
        raise WorkError("scope-conflict", "workspace must be a child, not the candidate parent")
    return resolved


def _command(template, slots, parent, *, inclusive=False):
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
            result.append(str(_inside(slots[name], parent, inclusive=inclusive)))
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
    policy = authorization.get("workspace_policy", "isolated_candidate")
    if policy == "bound_existing":
        # The bound session keeps working in ITS OWN directory: the workspace
        # IS the authorized root (inclusive), and command slots resolve
        # inside it — an older candidate_parent grants nothing here.
        workspace = _inside(bindings["workspace"], authorization["bound_workspace"],
                            inclusive=True)
        command_root = Path(authorization["bound_workspace"])
    else:
        workspace = _inside(bindings["workspace"], authorization["candidate_parent"])
        command_root = Path(authorization["candidate_parent"])
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
                       command_root, inclusive=(policy == "bound_existing"))
    contract = {
        "schema_version": "zsc.derived-contract/v1", "stage": stage,
        "workspace": str(workspace), "prompt": prompt, "model": model,
        "allowed_files": list(allowed), "timeout_sec": bindings["timeout_sec"],
        "concurrency": bindings["concurrency"], "command": command,
        "authorization_sha256": canonical_sha256(authorization),
        "template_version": bindings["template_version"],
        "bindings_sha256": canonical_sha256({k: v for k, v in bindings.items() if k != "registered_contracts"}),
        "compiler_version": "contract-compile/v1", "execution": "requires-separate-durable-intent",
    }
    # A verification binding marks its command as an obligation: the runner
    # turns this into commands[].required=true, so a zero-tool turn cannot be
    # delivered and then accepted on the model's own word.
    if bindings.get("require_command"):
        contract["require_command"] = True
    contract["contract_id"] = "contract-" + canonical_sha256(contract)[:32]
    for old in bindings.get("registered_contracts", []):
        if isinstance(old, dict) and all(old.get(key) == value for key, value in contract.items()
                                         if key != "contract_id"):
            reused = dict(contract)
            reused["contract_id"] = old.get("contract_id")
            reused["reused_registered_contract"] = True
            return reused
    return contract


def _verify_command(template, compiled, root, policy) -> None:
    """The compiler owns the program; a slot only fills a value.

    A caller cannot swap the program, add arguments, or point a slot outside
    the authorized root — the content hash of the authorization says the
    authorization exists, not that this argv came from it.
    """
    if not isinstance(template, dict) or not isinstance(compiled, dict):
        raise WorkError("authorization-unverified",
                        "the stage has no registered command template")
    expected = template.get("argv")
    actual = compiled.get("argv")
    if not isinstance(expected, list) or not isinstance(actual, list) \
            or len(expected) != len(actual):
        raise WorkError("authorization-unverified",
                        "the contract command does not match the registered argv template")
    root_path = Path(root).resolve(strict=False)
    for slot, value in zip(expected, actual):
        if _SLOT.fullmatch(slot):
            if not isinstance(value, str) or not value or "\x00" in value:
                raise WorkError("authorization-unverified",
                                "a command slot is empty or invalid")
            try:
                Path(value).resolve(strict=False).relative_to(root_path)
            except ValueError as error:
                raise WorkError("authorization-unverified",
                                "a command slot resolves outside the authorized root") from error
            continue
        if value != slot:
            raise WorkError("authorization-unverified",
                            "the contract replaced a fixed argv element")


def verify_against_authorization(authorization, contract) -> dict:
    """Re-check a derived contract against the registered authorization.

    Called by the owner before an intent is queued and by the runner before a
    worker is invoked: stage, write scope, model, limits, prompt template and
    the argv template must all be the ones the authorization declares.
    """
    validate_authorization(authorization)
    if not isinstance(contract, dict):
        raise WorkError("authorization-unverified", "the intent carries no contract")
    stage = contract.get("stage")
    stages = authorization["stages"]
    if not isinstance(stage, str) or not stage or stage not in stages:
        # Every contract this compiler produces names a registered stage. A
        # missing/null stage is not "unconstrained": it would bypass the
        # stage's command template, write set and model allowlist entirely.
        raise WorkError("authorization-unverified",
                        f"stage {stage!r} is not registered in this authorization")
    definition = stages[stage]
    policy = authorization.get("workspace_policy", "isolated_candidate")
    root = (authorization["bound_workspace"] if policy == "bound_existing"
            else authorization["candidate_parent"])
    root_path = Path(root).resolve(strict=False)
    workspace = Path(str(contract.get("workspace", ""))).resolve(strict=False)
    try:
        workspace.relative_to(root_path)
    except ValueError as error:
        raise WorkError("authorization-unverified",
                        "the contract workspace escapes the authorization") from error
    allowed = contract.get("allowed_files")
    if not isinstance(allowed, list) or any(not isinstance(x, str) for x in allowed):
        raise WorkError("authorization-unverified",
                        "allowed_files must be a list of strings")
    if not set(allowed) <= set(authorization["writable_relative_paths"]):
        raise WorkError("authorization-unverified",
                        "allowed_files expands the registered write scope")
    stage_scope = definition.get("allowed_files")
    # An EMPTY stage write set is a read-only stage (verification): it still
    # constrains the contract — only an empty request may pass. A stage that
    # does not declare allowed_files (synthetic fixture stages) checks only
    # the authorization-wide scope above.
    if isinstance(stage_scope, list) and not set(allowed) <= set(stage_scope):
        raise WorkError("authorization-unverified",
                        "allowed_files expands the registered stage scope")
    model = contract.get("model")
    candidates = []
    if isinstance(model, dict):
        provider = model.get("providerId") or model.get("provider_id")
        ident = model.get("modelId") or model.get("model_id")
        if ident:
            candidates.append(ident)  # authorizations name bare model ids
        if provider and ident:
            candidates.append(f"{provider}/{ident}")
        model = candidates[0] if candidates else None
    elif isinstance(model, str):
        candidates.append(model)
    if model is not None:
        allowlist = authorization["model_allowlist"]
        if not any(candidate in allowlist for candidate in candidates):
            raise WorkError("authorization-unverified",
                            "model is outside the registered authorization allowlist")
        stage_models = definition.get("models")
        if isinstance(stage_models, list) and stage_models \
                and not any(c in stage_models for c in candidates):
            raise WorkError("authorization-unverified",
                            "model is outside the registered stage allowlist")
    limits = authorization["limits"]
    for field, limit in (("timeout_sec", "timeout_sec"),
                         ("concurrency", "concurrency")):
        value = contract.get(field)
        if value is None:
            continue  # the contract does not request this limit at all
        if isinstance(value, bool) or not isinstance(value, int) or value < 1 \
                or value > limits.get(limit, 0):
            raise WorkError("authorization-unverified",
                            f"{field} exceeds the registered authorization")
    # Check only what the stage actually pins: a stage that declares a
    # prompt template or an argv template is held to it; a stage that
    # declares neither (synthetic fixtures, host-owned stages) pins only the
    # scope/model/limits above.
    # Only a template that carries the materials slot can have produced a
    # prompt; a synthetic stage template (no slot) pins nothing about text.
    declared = contract.get("template_version")
    pinned = definition.get("template_version")
    if declared is not None and pinned is not None and declared != pinned:
        raise WorkError("authorization-unverified",
                        "the contract template version is not the registered one")
    if definition.get("command") is not None:
        _verify_command(definition.get("command"), contract.get("command"),
                        root, policy)
    return contract
