"""Executable recon plans and host-bound ArchitectureBrief envelopes."""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

from .architecture_brief import validate_brief
from .contracts import canonical_json
from .work_contracts import canonical_sha256
from .work_orientation import frame_digest, normalize_frame


PROFILES = ("native-workflow", "skill-assisted", "plain-recon")
_TEMPLATE_PATH = Path(__file__).with_name("templates") / "recon" / "architecture.dwf.ts"


class ReconError(ValueError):
    def __init__(self, code: str, message: str, details=None):
        super().__init__(f"{code}: {message}")
        self.code = code
        self.details = details or []


def build_recon_plan(frame, orientation, manifest, capabilities, profile_plan,
                     *, profile=None) -> dict:
    normalized = normalize_frame(frame)
    if not all(isinstance(x, dict) for x in (orientation, manifest, capabilities, profile_plan)):
        raise ReconError("invalid-argument", "orientation/manifest/capabilities/profile must be objects")
    selected = profile or ("native-workflow" if profile_plan.get("native_tools")
                           else "skill-assisted" if profile_plan.get("selected_skills")
                           else "plain-recon")
    if selected not in PROFILES:
        raise ReconError("invalid-argument", "unknown recon profile")
    if selected == "native-workflow" and "CreateWorkflow" not in profile_plan.get("native_tools", []):
        raise ReconError("capability-missing", "native workflow was not selected")
    if selected == "skill-assisted" and not profile_plan.get("selected_skills"):
        raise ReconError("capability-missing", "skill-assisted profile has no admitted skill")
    digest = frame_digest(normalized)
    frozen = {"frame": normalized, "task_frame_sha256": digest,
              "orientation": orientation, "manifest": manifest,
              "capabilities_sha256": canonical_sha256(capabilities)}
    script = _TEMPLATE_PATH.read_bytes()
    result = {
        "schema_version": "zsc.recon-plan/v1", "profile": selected,
        "task_frame_sha256": digest,
        "orientation_sha256": canonical_sha256(orientation),
        "manifest_sha256": canonical_sha256(manifest),
        "capabilities_sha256": canonical_sha256(capabilities),
        "template_sha256": hashlib.sha256(script).hexdigest(),
        "degradations": list(profile_plan.get("degradations", [])),
        "output_schema": "zsc.architecture-brief/v1",
        "allowed_files": ["report.json"],
        "execution_gate": "host_qualification_required",
    }
    if selected == "native-workflow":
        result["script"] = script.decode("utf-8")
        result["workflow"] = {"path": ".zsc-input/recon.dwf.ts", "max_concurrency": 1,
                              "args": {"requestJson": canonical_json(frozen)}}
    else:
        result["skills"] = list(profile_plan.get("selected_skills", []))
        result["input_json"] = canonical_json(frozen)
    return result


def _read_all(store, ref):
    pages = []
    cursor = None
    while True:
        page = store.read(ref, cursor=cursor, max_bytes=65536)
        pages.append(page["data"])
        if not page["has_more"]:
            return "".join(pages)
        cursor = page["next_cursor"]


def _load_json(store, ref, code):
    try:
        value = json.loads(_read_all(store, ref))
    except (ValueError, UnicodeError) as error:
        raise ReconError(code, "artifact is not valid JSON") from error
    if not isinstance(value, dict):
        raise ReconError(code, "JSON artifact must be an object")
    return value


def collect_brief(task_binding: dict, body_ref: dict, receipt_refs: list) -> dict:
    """Validate and persist a report using only host-supplied identity/bindings."""
    required = {"artifact_store", "workspace", "expected_frame_sha256", "task_id",
                "work_id", "attempt_id", "profile", "output_path", "allowed_output", "producer"}
    if not isinstance(task_binding, dict) or not required <= set(task_binding):
        raise ReconError("task-binding-invalid", "host task binding is incomplete")
    if task_binding["profile"] not in PROFILES or not isinstance(task_binding["producer"], dict):
        raise ReconError("task-binding-invalid", "invalid profile or producer")
    store = task_binding["artifact_store"]
    output = Path(task_binding["output_path"])
    allowed = Path(task_binding["allowed_output"])
    if output != allowed or output.is_symlink():
        raise ReconError("output-scope-denied", "report output is not the exact host-authorized file")
    receipts = [_load_json(store, ref, "receipt-invalid") for ref in receipt_refs]
    if not receipts:
        raise ReconError("capability-receipt-missing", "at least one execution receipt is required")
    for receipt in receipts:
        for field in ("task_id", "work_id", "attempt_id"):
            if receipt.get(field) != task_binding[field]:
                raise ReconError("receipt-binding-mismatch", f"receipt {field} does not match task")
        if receipt.get("status") != "completed":
            raise ReconError("recon-not-completed", f"receipt status is {receipt.get('status')}")
    required_tool = {"native-workflow": "CreateWorkflow", "skill-assisted": "Skill",
                     "plain-recon": None}[task_binding["profile"]]
    if required_tool and not any(isinstance(tool, dict) and tool.get("name") == required_tool
                                 and tool.get("call_id") and tool.get("result_ref")
                                 for receipt in receipts for tool in receipt.get("tools", [])):
        raise ReconError("capability-receipt-missing", f"{required_tool} receipt evidence is required")
    try:
        body = _load_json(store, body_ref, "brief-invalid")
        validation = validate_brief(body, workspace=task_binding["workspace"],
                                    expected_frame_sha256=task_binding["expected_frame_sha256"])
    except ReconError:
        raise
    except Exception as error:
        raise ReconError("brief-invalid", str(error)[:300]) from error
    if not validation["schema_valid"]:
        raise ReconError("brief-invalid", "ArchitectureBrief schema validation failed", validation["issues"])
    if not validation["sources_valid"]:
        raise ReconError("source-drift", "brief sources do not match the current workspace",
                         validation["issues"])
    envelope = {
        "schema_version": "zsc.brief-envelope/v1",
        "task_id": task_binding["task_id"], "work_id": task_binding["work_id"],
        "attempt_id": task_binding["attempt_id"], "profile": task_binding["profile"],
        "producer": dict(task_binding["producer"]), "body_ref": dict(body_ref),
        "receipt_refs": [dict(ref) for ref in receipt_refs], "validation": validation,
        "acceptance": "not_evaluated",
    }
    try:
        output.parent.mkdir(parents=True, exist_ok=True)
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
        fd = os.open(output, flags, 0o600)
        with os.fdopen(fd, "wb") as stream:
            stream.write(canonical_json(envelope).encode("utf-8"))
            stream.flush()
            os.fsync(stream.fileno())
    except FileExistsError as error:
        raise ReconError("output-already-exists", "refusing to replace an existing report") from error
    except OSError as error:
        raise ReconError("output-unwritable", str(error)[:300]) from error
    return envelope
