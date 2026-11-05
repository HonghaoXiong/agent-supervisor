"""Bounded, explicit context deltas for new investigations and new sessions."""

from __future__ import annotations

from .work_contracts import canonical_sha256
from .work_models import WorkError


SECTIONS = ("interfaces", "assumptions", "sources")


def _mapping(context, key):
    value = context.get(key, {})
    if not isinstance(value, dict):
        raise WorkError("schema-invalid", f"context {key} must be an object")
    return value


def _changes(previous, current):
    changes = []
    for name in sorted(set(previous) | set(current)):
        before = previous.get(name)
        after = current.get(name)
        if before == after:
            continue
        kind = "added" if before is None else "removed" if after is None else "modified"
        changes.append({"id": name, "kind": kind, "before": before, "after": after,
                        "before_sha256": canonical_sha256(before) if before is not None else None,
                        "after_sha256": canonical_sha256(after) if after is not None else None})
    return changes


def _valid_artifacts(context):
    artifacts = context.get("artifacts", [])
    if not isinstance(artifacts, list):
        raise WorkError("schema-invalid", "context artifacts must be an array")
    return [item for item in artifacts if isinstance(item, dict)
            and isinstance(item.get("ref"), str) and item.get("status") == "accepted"
            and isinstance(item.get("sha256"), str)]


def _cost_bytes(records):
    total = 0
    for record in records:
        size = record.get("bytes")
        if not isinstance(size, int) or isinstance(size, bool) or size < 0:
            return None
        total += size
    return total


def build_context_delta(previous, current, query) -> dict:
    """Return only changed evidence plus still-valid references.

    A different or invalidated session always triggers explicit replay of accepted
    artifacts; the result never claims to attach to the old worker session.
    """
    if not isinstance(previous, dict) or not isinstance(current, dict) or not isinstance(query, dict):
        raise WorkError("schema-invalid", "previous/current contexts and query must be objects")
    if set(query) - {"sections"}:
        raise WorkError("invalid-argument", "context delta query only accepts sections")
    sections = query.get("sections", list(SECTIONS))
    if not isinstance(sections, list) or not sections or any(section not in SECTIONS for section in sections) \
            or len(set(sections)) != len(sections):
        raise WorkError("invalid-argument", f"sections must be unique values from {SECTIONS}")
    old_session = previous.get("session_id")
    current_session = current.get("session_id")
    continuous = (isinstance(old_session, str) and old_session == current_session
                  and previous.get("session_valid") is True and current.get("session_valid") is True)
    changed = {section: _changes(_mapping(previous, section), _mapping(current, section))
               for section in sections}
    old_artifacts = {item["ref"]: item for item in _valid_artifacts(previous)}
    current_artifacts = {item["ref"]: item for item in _valid_artifacts(current)}
    still_valid = sorted(ref for ref, item in current_artifacts.items()
                         if old_artifacts.get(ref, {}).get("sha256") == item["sha256"])
    replay = [] if continuous else [current_artifacts[ref] for ref in sorted(current_artifacts)]
    current_sources = list(_mapping(current, "sources").values())
    all_current_records = current_sources + list(current_artifacts.values())
    cold = _cost_bytes(all_current_records)
    if continuous:
        changed_ids = {change["id"] for change in changed.get("sources", []) if change["kind"] != "removed"}
        warm_records = [record for name, record in _mapping(current, "sources").items() if name in changed_ids]
        warm_records += [item for ref, item in current_artifacts.items()
                         if old_artifacts.get(ref, {}).get("sha256") != item["sha256"]]
        warm = _cost_bytes(warm_records)
    else:
        warm = cold
    return {
        "schema_version": "zsc.context-delta/v1",
        "mode": "delta" if continuous else "full_replay",
        "continuity": "same-valid-session" if continuous else "new-session-replay",
        "new_session": not continuous,
        "previous_session_id": old_session,
        "current_session_id": current_session,
        "changed": changed,
        "still_valid_refs": still_valid,
        "replay_artifacts": replay,
        "read_cost": {"accounting": "declared-source-bytes", "cold_build_bytes": cold,
                      "warm_reuse_bytes": warm,
                      "saved_bytes": cold - warm if cold is not None and warm is not None else None},
        "notice": "A new session replays accepted artifacts and does not claim to resume the old session."
                  if not continuous else "Only changed sections and still-valid references are transmitted.",
        "delta_sha256": canonical_sha256({"previous": previous, "current": current, "sections": sections}),
    }
