"""Worker health evaluation for the hub (companion to probe-all --revalidate).

Turns one tri-probe result (``run_probe_all`` output shape:
``{"agents": {...}, "comparison": {...}}``) into the ``state_dir/hub/
health.json`` document the hub server's ``health`` tool reads. Evaluation is
fail-closed: a worker is enabled only on an explicit ``ok`` probe status with
an explicit ``changed: False`` baseline comparison — missing pieces (no agent
entry, unrecognized status, no comparison verdict) all disable the worker as
``unknown``. The routing block records which workers remain dispatchable and
carries the standing note that degraded routing does not guarantee worker
equivalence.

Public surface: :func:`evaluate`, :func:`write_health`, :func:`read_health`.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from . import common

__all__ = [
    "HEALTH_SCHEMA_VERSION",
    "ROUTING_NOTE",
    "evaluate",
    "read_health",
    "write_health",
]

HEALTH_SCHEMA_VERSION = 1
ROUTING_NOTE = ("degraded routing does not guarantee worker equivalence "
                "(model/skills/workflows differ)")

HEALTH_FILENAME = "health.json"
_RECOGNIZED_PROBE_STATUSES = ("ok", "changed", "absent", "error")
_DETAIL_LIMIT = 300
# comparison.diff keys summarized into the disable reason (tri_probe's
# _agent_diff shape: scalar drift as {"was", "now"}, method drift as lists).
_SCALAR_DRIFT_KEYS = ("version", "fingerprint", "pinned", "installed")


def _diff_summary(diff) -> str:
    """Human-readable one-line summary of a tri-probe comparison diff."""
    if not isinstance(diff, dict) or not diff:
        return "probe reported drift without diff details"
    pieces: list[str] = []
    methods = diff.get("methods")
    if isinstance(methods, dict):
        for side in ("added", "removed"):
            names = methods.get(side)
            if isinstance(names, list) and names:
                pieces.append(f"{len(names)} {side} method(s)")
    for key in _SCALAR_DRIFT_KEYS:
        change = diff.get(key)
        if isinstance(change, dict):
            pieces.append(f"{key} {change.get('was')!r} -> {change.get('now')!r}")
    for key in sorted(diff):
        if key == "methods" or key in _SCALAR_DRIFT_KEYS:
            continue
        pieces.append(f"{key} changed")
    return "; ".join(pieces) or "probe reported drift without diff details"


def _worker_entry(agent, comparison, probe_ref: str) -> dict:
    """Evaluate one worker; every unrecognized input disables it."""
    entry = {"enabled": False, "status": "unknown", "reason": "",
             "probe_ref": probe_ref}
    if not isinstance(agent, dict):
        entry["reason"] = "probe result missing for this worker"
        return entry
    status = agent.get("status")
    if status == "error":
        error_text = agent.get("error")
        entry["status"] = "error"
        entry["reason"] = (error_text[:_DETAIL_LIMIT]
                           if isinstance(error_text, str)
                           else "probe reported error")
        return entry
    if agent.get("installed") is False or status == "absent":
        entry["status"] = "absent"
        entry["reason"] = "worker not installed"
        return entry
    comparison = comparison if isinstance(comparison, dict) else {}
    diff = comparison.get("diff")
    if status == "changed" or comparison.get("changed") is True:
        entry["status"] = "changed"
        entry["reason"] = _diff_summary(diff)[:_DETAIL_LIMIT]
        return entry
    if status == "ok":
        if comparison.get("changed") is False:
            entry["enabled"] = True
            entry["status"] = "ok"
        else:
            entry["reason"] = "probe status ok but baseline comparison missing"
        return entry
    entry["reason"] = f"probe status {status!r} not recognized"
    return entry


def evaluate(probe_result: dict | None, probe_ref: str = "") -> dict:
    """Evaluate a tri-probe result into a health.json document (fail-closed).

    Inputs are lenient — ``probe_result`` itself, ``agents`` and
    ``comparison`` may be missing or malformed, and every gap evaluates to a
    disabled ``unknown`` worker.
    """
    result = probe_result if isinstance(probe_result, dict) else {}
    agents_value = result.get("agents")
    agents = agents_value if isinstance(agents_value, dict) else {}
    comparison_value = result.get("comparison")
    comparisons = comparison_value if isinstance(comparison_value, dict) else {}
    workers = {name: _worker_entry(agents.get(name), comparisons.get(name),
                                   probe_ref)
               for name in sorted(agents)}
    enabled = sorted(name for name, entry in workers.items()
                     if entry["enabled"])
    disabled = sorted(name for name, entry in workers.items()
                      if not entry["enabled"])
    return {
        "schema_version": HEALTH_SCHEMA_VERSION,
        "generated": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "workers": workers,
        "routing": {
            "enabled_workers": enabled,
            "disabled_workers": disabled,
            "note": ROUTING_NOTE,
        },
    }


def health_path(state_dir) -> Path:
    return Path(state_dir) / "hub" / HEALTH_FILENAME


def write_health(state_dir, health: dict) -> Path:
    """Atomically publish the health document at ``state_dir/hub/health.json``."""
    path = health_path(state_dir)
    common.write_json(path, health)
    return path


def read_health(state_dir) -> dict | None:
    """Read the health document; missing/unreadable/invalid yields None."""
    try:
        document = json.loads(common.read_regular(health_path(state_dir)))
    except (OSError, ValueError):
        return None
    return document if isinstance(document, dict) else None
