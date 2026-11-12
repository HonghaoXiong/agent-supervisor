"""CLI/MCP shared services for P0-A work orientation.

Both entries are deterministic and read-only: prepare_work_impl matches a
TaskFrame against the bundled catalog (no config, no network, no model);
validate_brief_impl binds the workspace to a REGISTERED contract — the
caller cannot supply one — and never delegates, executes or registers
anything. This module must not import hub_server.
"""

from __future__ import annotations

import re
from pathlib import Path

from . import architecture_brief, practice_catalog, work_orientation
from .contracts import load_contract
from .work_contracts import WorkInputError, issue


_HEX64 = re.compile(r"^[0-9a-f]{64}$")


def prepare_work_impl(frame: dict, *, limit: int = 5, theme_ids=()) -> dict:
    """Deterministic orientation over the bundled catalog and explicit themes."""
    return work_orientation.prepare_orientation(
        frame, practice_catalog.load_practice_set(theme_ids), limit=limit)


def validate_brief_impl(config, contract_id: str, brief: dict,
                        expected_frame_sha256: str) -> dict:
    """Validate a brief against a registered contract's workspace only."""
    if not isinstance(expected_frame_sha256, str) \
            or not _HEX64.match(expected_frame_sha256):
        raise WorkInputError(
            "work-input-invalid",
            [issue("bad-hash", "$.expected_frame_sha256",
                   "expected_frame_sha256 must be a lowercase 64-hex "
                   "sha256 of the accepted task frame")])
    contract = load_contract(Path(config.state_dir), contract_id)
    return architecture_brief.validate_brief(
        brief, workspace=Path(contract["workspace"]),
        expected_frame_sha256=expected_frame_sha256)


class WorkService:
    """Thin shared CLI/MCP client; all business rules remain in the owner."""

    def __init__(self, client):
        self.client = client

    @classmethod
    def for_state_root(cls, state_root, *, host=False, token=None):
        """A client for one owner socket.

        ``host=True`` is only for the user's own CLI: it lets the client load
        the host control token. The work MCP service keeps the default, so a
        model connection cannot inherit the host role from the socket path.
        """
        from .work_control import ControlClient
        return cls(ControlClient(Path(state_root) / "control.sock",
                                 host=host, token=token))

    def invoke(self, action: str, request: dict) -> dict:
        if not isinstance(request, dict):
            from .work_models import WorkError
            raise WorkError("invalid-argument", "work request must be an object")
        method = {
            "authorize": "authorize", "capabilities": "work_capabilities",
            "create": "create_work", "recon": "start_recon",
            "implement": "start_implementation", "verify": "start_verification",
            "submit-brief": "submit_brief", "accept-design": "accept_design",
            "submit-delivery": "submit_delivery", "accept": "record_acceptance",
            "rework": "rework_work", "cancel": "cancel_work", "reconcile": "reconcile_work",
            "status": "work_status", "artifact": "read_artifact",
            "knowledge-search": "search_knowledge", "knowledge-revalidate": "revalidate_knowledge",
            "knowledge-propose": "propose_knowledge", "knowledge-review": "review_knowledge",
            "bind-session": "bind_session",
            "claim-session": "claim_native_session",
            "session-status": "session_status",
            "pause-session": "pause_session", "resume-session": "resume_session",
            "unbind-session": "unbind_session", "advance-session": "advance_session_loop",
            "submit-decision": "submit_review_decision", "adopt": "adopt_existing_work",
        }.get(action)
        if method is None:
            from .work_models import WorkError
            raise WorkError("invalid-argument", f"unknown work action {action}")
        return self.client.call(method, request)


MCP_ACTIONS = {
    "work_capabilities": "capabilities", "create_work": "create",
    "start_recon": "recon", "submit_brief": "submit-brief",
    "accept_design": "accept-design", "start_implementation": "implement",
    "submit_delivery": "submit-delivery", "start_verification": "verify",
    "record_acceptance": "accept", "rework_work": "rework", "cancel_work": "cancel",
    "reconcile_work": "reconcile", "work_status": "status",
    "read_artifact": "artifact", "search_knowledge": "knowledge-search",
    "revalidate_knowledge": "knowledge-revalidate", "propose_knowledge": "knowledge-propose",
    "review_knowledge": "knowledge-review",
    "bind_session": "bind-session",
    "claim_native_session": "claim-session", "session_status": "session-status",
    "pause_session": "pause-session", "resume_session": "resume-session",
    "unbind_session": "unbind-session", "advance_session": "advance-session",
    "submit_session_decision": "submit-decision", "adopt_work": "adopt",
}
