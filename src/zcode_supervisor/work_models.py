"""Host-authenticated actors and strict persistent work models."""

from __future__ import annotations

from dataclasses import dataclass


SPEC_FIELDS = {"schema_version", "title", "goal", "project_root", "candidate_parent"}


class WorkError(ValueError):
    def __init__(self, code: str, message: str, details=None):
        super().__init__(f"{code}: {message}")
        self.code = code
        self.details = details or []


@dataclass(frozen=True)
class ActorContext:
    actor_id: str
    roles: frozenset
    host_bound: bool
    worker: str | None = None
    session_id: str | None = None

    def require(self, role: str) -> None:
        if self.host_bound is not True or not self.actor_id:
            raise WorkError("actor-untrusted", "actor context was not constructed by the host")
        if role not in self.roles:
            raise WorkError("role-denied", f"actor lacks {role}")

    def evidence(self) -> dict:
        # Identity labels and worker/session binding are separate evidence fields.
        return {"actor_id": self.actor_id, "roles": sorted(self.roles),
                "worker": self.worker, "session_id": self.session_id}


def validate_work_spec(spec: dict) -> dict:
    if not isinstance(spec, dict) or set(spec) != SPEC_FIELDS:
        raise WorkError("schema-invalid", "work spec has unknown or missing fields")
    if spec["schema_version"] != "zsc.work-spec/v1":
        raise WorkError("schema-invalid", "unknown work spec schema")
    for field in ("title", "goal", "project_root", "candidate_parent"):
        if not isinstance(spec[field], str) or not spec[field].strip() or "\x00" in spec[field]:
            raise WorkError("schema-invalid", f"invalid {field}")
    return dict(spec)
