"""Shared cold/resident conversion from registered contracts to czs packets."""

from __future__ import annotations

import hashlib
import json
import os
import tempfile
from pathlib import Path

from .orchestration import validate_orchestration


class PacketError(ValueError):
    def __init__(self, code: str, message: str):
        super().__init__(f"{code}: {message}")
        self.code = code


def contract_to_zcode_packet(contract: dict, *, trusted_inputs: dict) -> dict:
    if not isinstance(contract, dict) or not isinstance(trusted_inputs, dict):
        raise PacketError("invalid-argument", "contract and trusted_inputs must be objects")
    if contract.get("worker", "zcode") != "zcode":
        raise PacketError("invalid-argument", "only zcode contracts use this packet converter")
    if contract.get("skills") and contract.get("orchestration"):
        raise PacketError("orchestration-conflict",
                          "legacy skills and orchestration are mutually exclusive")
    packet = {
        "workspace": contract["workspace"],
        "prompt": contract["prompt"],
        "allowed_files": list(contract.get("allowed_files", [])),
        "commands": list(contract.get("commands", [])),
        "trusted_hashes": {},
    }
    if isinstance(contract.get("model"), dict):
        packet["model"] = contract["model"]
    orch = contract.get("orchestration")
    if orch is None and contract.get("skills"):
        orch = {"skills": {"allowed": list(contract["skills"])}}
    if orch is not None:
        try:
            orch = validate_orchestration(orch)
        except (ValueError, KeyError) as error:
            raise PacketError("orchestration-invalid", str(error)) from error
        packet["orchestration"] = orch
        # The host needs the script for validation, but the worker also needs to
        # see the declaration it is being asked to invoke. Keep this projection
        # canonical and host-derived instead of relying on each caller to splice
        # a possibly different script into free-form prompt text.
        packet["prompt"] += (
            "\n\nHost orchestration contract (authoritative JSON):\n"
            + json.dumps(orch, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
            + "\nFollow this declaration exactly; load allowed skills required by gated tools."
        )
        if orch.get("workflow"):
            packet["prompt"] += (
                " Use CreateWorkflow with the exact declared inline script or path; "
                "do not substitute a direct Agent run."
            )
        workflow = orch.get("workflow")
        if workflow and workflow.get("path"):
            data = trusted_inputs.get("workflow_script_bytes")
            if not isinstance(data, (bytes, bytearray)):
                raise PacketError("trusted-input-missing",
                                  "host must supply frozen workflow script bytes")
            data = bytes(data)
            target = Path(contract["workspace"]).joinpath(*Path(workflow["path"]).parts)
            packet["trusted_hashes"][str(target)] = hashlib.sha256(data).hexdigest()
    return packet


def materialize_trusted_inputs(packet: dict, trusted_inputs: dict) -> None:
    """Freeze host-supplied workflow bytes before dispatch; never follow links."""
    workflow = packet.get("orchestration", {}).get("workflow")
    if not workflow or not workflow.get("path"):
        return
    data = trusted_inputs.get("workflow_script_bytes")
    if not isinstance(data, (bytes, bytearray)):
        raise PacketError("trusted-input-missing", "frozen workflow bytes are required")
    data = bytes(data)
    target = Path(next(iter(packet["trusted_hashes"])))
    expected = packet["trusted_hashes"][str(target)]
    if hashlib.sha256(data).hexdigest() != expected:
        raise PacketError("trusted-input-drift", "workflow bytes do not match packet hash")
    if target.exists() or target.is_symlink():
        if target.is_symlink() or not target.is_file():
            raise PacketError("trusted-input-drift", "workflow target is not a regular file")
        if target.read_bytes() != data:
            raise PacketError("trusted-input-drift", "existing workflow file differs")
        return
    target.parent.mkdir(parents=True, exist_ok=True)
    fd, temporary = tempfile.mkstemp(prefix=".workflow-", dir=target.parent)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        os.link(temporary, target)
    finally:
        Path(temporary).unlink(missing_ok=True)
