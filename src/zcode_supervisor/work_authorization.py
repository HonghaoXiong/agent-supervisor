"""Host-only immutable work authorization registration."""

from .artifact_store import ArtifactStore
from .work_models import ActorContext, WorkError


FIELDS = {"schema_version", "project_root", "candidate_parent", "source_read_scope",
          "writable_relative_paths", "model_allowlist", "limits", "stages"}


def validate_authorization(document):
    if not isinstance(document, dict) or set(document) != FIELDS \
            or document.get("schema_version") != "zsc.work-authorization/v1":
        raise WorkError("schema-invalid", "invalid work authorization fields")
    for field in ("project_root", "candidate_parent"):
        if not isinstance(document[field], str) or not document[field].startswith("/"):
            raise WorkError("schema-invalid", f"{field} must be absolute")
    for field in ("source_read_scope", "writable_relative_paths", "model_allowlist"):
        if not isinstance(document[field], list) or not document[field] \
                or any(not isinstance(x, str) or not x for x in document[field]):
            raise WorkError("schema-invalid", f"invalid {field}")
    if not isinstance(document["stages"], dict) or not document["stages"] \
            or not isinstance(document["limits"], dict):
        raise WorkError("schema-invalid", "stages and limits are required")
    return document


def register_work_authorization(document, actor: ActorContext, *, artifact_store: ArtifactStore):
    actor.require("work:authorize")
    validate_authorization(document)
    if not isinstance(artifact_store, ArtifactStore):
        raise WorkError("invalid-argument", "host artifact store is required")
    # This entry is deliberately not exposed as a worker MCP tool.
    return artifact_store.put_json("work-authorization", document)
