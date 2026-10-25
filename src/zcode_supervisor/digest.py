"""Compact evidence digests bound to accepted baselines (prototype, offline).

Contract (per controller reviews 2026-10-14):
- A digest LOCATES and SUMMARISES evidence; it never judges delivery. Verdict
  fields are copied verbatim; a structural delta of two digests never implies
  acceptance, and null vs absent are distinct in deltas.
- Bindings separate three sources: candidate INDEX blob, candidate WORKTREE
  file (what this machine actually imports), and the HISTORICAL run's own
  execution identity. Current bindings are never attached to a historical run
  to claim verified provenance — runs without a trusted binding association
  digest as bindings="unknown" and mode="full-review". No model rerun needed.
- Baseline gate: REQUIRED binding keys must all be present, non-unknown, and
  equal to the accepted baseline; any extra unreviewed binding key is unknown
  execution identity. Missing / unknown / not captured / changed / extra /
  no-baselines-passed all force mode="full-review". Unrelated metadata may be
  stored separately but never counts as reviewed identity.
- Completeness gate: missing/ill-typed important execution fields (outcome,
  delivery_ok, session/turn identity for turn-bearing outcomes, per-command
  exitCode/timedOut/status) force full-review. Errors are preserved as a
  bounded excerpt with an explicit truncation flag; every digest-side
  truncation is flagged.
- Raw locators: run_id → run.json + events.jsonl paths, byte sizes and
  SHA-256, so any digest claim can be checked against original receipts.
Pure stdlib; no model calls; no behaviour change to existing commands.
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
import re

TRACKED_BLOBS = ("src/zcode_supervisor/protocol.py",
                 "src/zcode_supervisor/profile.py",
                 "src/zcode_supervisor/runtime.py",
                 "src/zcode_supervisor/habits.py",
                 "src/zcode_supervisor/notify.py",
                 "src/zcode_supervisor/cli.py",
                 "src/zcode_supervisor/common.py",
                 "src/zcode_supervisor/digest.py",
                 "src/zcode_supervisor/patches/zcode-permission-allowlist.patch")
# Every tracked binding is execution identity: missing/unknown/extra ⇒ review.
REQUIRED_BINDINGS = (tuple(name.rsplit("/", 1)[-1] for name in TRACKED_BLOBS)
                     + ("cli_bundle", "profile.json"))
TURN_BEARING_OUTCOMES = ("delivered", "evidence-rejected", "postflight-rejected",
                         "worker-failed", "protocol-error", "timeout")
# 与 protocol.OUTCOME_LABELS 键保持一致（本地副本，避免循环导入）
KNOWN_OUTCOMES = TURN_BEARING_OUTCOMES + ("preflight-rejected", "scope-error")
_SHA256 = re.compile(r"[0-9a-f]{64}")
ERROR_EXCERPT_CHARS = 200
DENY_REASON_CHARS = 120

_MISSING = object()


def _sha_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _sha_file(path: Path) -> str:
    return _sha_bytes(Path(path).read_bytes())


def _bundle_sha(repo_root: Path) -> str:
    """Identity of the EXPLICITLY configured CLI, hashed from its actual bytes.

    Cache manifests are never scanned: a stale cache cannot prove which entry
    point runs. No resolvable configured cli_path ⇒ "unknown".
    """
    config = repo_root / ".czs" / "config.json"
    try:
        value = json.loads(config.read_text())
        cli = Path(value["cli_path"])
    except (OSError, KeyError, TypeError, json.JSONDecodeError):
        return "unknown"
    return _sha_file(cli) if cli.is_file() else "unknown"


def current_bindings(repo_root: Path, profile_path: Path | None = None,
                     source: str = "worktree") -> dict:
    """Candidate identity snapshot. source='worktree' (what actually imports)
    or 'index' (staged candidate blob). Historical runs must NOT reuse this."""
    import subprocess
    root = Path(repo_root)
    bindings = {}
    for rel in TRACKED_BLOBS:
        if source == "index":
            blob = subprocess.run(["git", "show", f":{rel}"], cwd=root,
                                  capture_output=True)
            bindings[rel.rsplit("/", 1)[-1]] = (_sha_bytes(blob.stdout)
                                                if blob.returncode == 0
                                                else "unknown")
        else:
            path = root / rel
            bindings[rel.rsplit("/", 1)[-1]] = (_sha_file(path)
                                                if path.is_file() else "unknown")
    bindings["cli_bundle"] = _bundle_sha(root)
    if profile_path is not None and Path(profile_path).is_file():
        bindings["profile.json"] = _sha_file(profile_path)
    return bindings


def _excerpt(text: str, limit: int) -> tuple[str, bool]:
    text = " ".join(str(text).split())
    return text[:limit], len(text) > limit


def _error_class(run: dict) -> str:
    error = run.get("error")
    if not isinstance(error, str) or not error:
        return ""
    first = error.split(":", 1)[0]
    return re.sub(r"[^\w.-]", "", first)[:60] or "error"


def _command_detail(entry: dict) -> dict:
    """Single guarded entry point for the result→perf→detail→command chain.

    Any non-dict container anywhere (result=None included) yields an empty
    detail so facts degrade to "unknown" instead of raising.
    """
    node = entry.get("result")
    for key in ("perf", "detail", "command"):
        if not isinstance(node, dict):
            return {}
        node = node.get(key)
    return node if isinstance(node, dict) else {}


def _command_facts(run: dict) -> list:
    facts = []
    for entry in run.get("commands") or []:
        if not isinstance(entry, dict):
            entry = {}
        detail = _command_detail(entry)
        facts.append({
            "command_id": entry.get("command_id", "unknown"),
            "exit_code": detail.get("exitCode", "unknown"),
            "timed_out": detail.get("timedOut", "unknown"),
            "status": detail.get("status", "unknown"),
        })
    return facts


def _failures(run: dict) -> dict:
    decisions = run.get("permission_decisions") or []
    denies = []
    for d in decisions:
        if d.get("decision") == "deny":
            reason, truncated = _excerpt(d.get("reason") or "unknown",
                                         DENY_REASON_CHARS)
            denies.append({"tool": d.get("toolName", "unknown"),
                           "reason": reason, "reason_truncated": truncated})
    error_excerpt, error_truncated = _excerpt(run.get("error") or "",
                                              ERROR_EXCERPT_CHARS)
    truncated_outputs = []
    for entry in run.get("commands") or []:
        if not isinstance(entry, dict):
            continue
        result = entry.get("result")
        if not isinstance(result, dict):
            continue
        if result.get("truncated") or (result.get("originalBytes") is not None
                                       and result.get("originalBytes")
                                       != result.get("returnedBytes")):
            truncated_outputs.append(entry.get("command_id", "unknown"))
    return {
        "denials": denies,
        "execution_problems": list(run.get("execution_problems") or []),
        "non_zero_exits": [c for c in _command_facts(run)
                           if c["exit_code"] not in (0, "unknown")],
        "timeouts": [run.get("outcome") == "timeout"],
        "input_drift": {"changed_files": run.get("changed_files") or [],
                        "scope_error": run.get("scope_error", "unknown")},
        "identity": {"requested_model": run.get("requested_model"),
                     "effective_before": run.get("effective_model_before"),
                     "effective_after": run.get("effective_model_after"),
                     "process_exit_code": run.get("process_exit_code",
                                                  "unknown")},
        "error_class": _error_class(run) or "unknown",
        "error_excerpt": error_excerpt,
        "error_excerpt_truncated": error_truncated,
        "truncated_outputs": truncated_outputs,
    }


def _completeness_issues(result: dict, run: dict) -> list:
    issues = []
    outcome, delivered = run.get("outcome"), run.get("delivery_ok")
    if outcome is None or outcome == "unknown":
        issues.append("outcome")
    elif not isinstance(outcome, str) or outcome not in KNOWN_OUTCOMES:
        issues.append("outcome:type-or-value")
    if not isinstance(delivered, bool):
        issues.append("delivery_ok:type" if delivered is not None
                      else "delivery_ok")
    turn_id, session_id = run.get("turn_id"), run.get("session_id")
    if outcome in TURN_BEARING_OUTCOMES:
        if not isinstance(turn_id, str) or not turn_id:
            issues.append("turn_id:type-or-missing")
        if not isinstance(session_id, str) or not session_id:
            issues.append("session_id:type-or-missing")
    elif (isinstance(outcome, str) and outcome not in ("preflight-rejected",)
          and (session_id is not None and not isinstance(session_id, str))):
        issues.append("session_id:type")
    for fact in result["commands"]:
        cid = fact["command_id"]
        if fact["exit_code"] == "unknown":
            issues.append(f"command-fields:{cid}")
        elif isinstance(fact["exit_code"], bool) or not isinstance(fact["exit_code"], int):
            issues.append(f"command-fields:{cid}:exit_code:type")
        if fact["timed_out"] == "unknown":
            issues.append(f"command-fields:{cid}")
        elif not isinstance(fact["timed_out"], bool):
            issues.append(f"command-fields:{cid}:timed_out:type")
        if fact["status"] == "unknown" or not isinstance(fact["status"], str) \
                or not fact["status"]:
            issues.append(f"command-fields:{cid}:status:unknown-type-or-missing")
    exit_code = run.get("process_exit_code")
    if isinstance(exit_code, bool) or not isinstance(exit_code, int):
        issues.append("process_exit_code:type-or-missing")
    return issues


def digest(run_path: Path, events_path: Path | None = None,
          baselines: dict | None = None,
          bindings: dict | None = None,
          bindings_note: str = "") -> dict:
    """Build one digest; binding/completeness gates decide digest vs review."""
    run_path = Path(run_path)
    run_bytes = run_path.read_bytes()
    run = json.loads(run_bytes)
    events_path = (Path(events_path) if events_path is not None
                   else run_path.with_suffix(".events.jsonl"))
    result = {
        "run_file": {"path": str(run_path), "bytes": len(run_bytes),
                     "sha256": _sha_bytes(run_bytes)},
        "outcome": run.get("outcome", "unknown"),
        "delivery_ok": run.get("delivery_ok", "unknown"),
        "session_id": run.get("session_id"),
        "turn_id": run.get("turn_id"),
        "commands": _command_facts(run),
        "failures": _failures(run),
        "bindings": bindings if isinstance(bindings, dict) else "unknown",
        "bindings_note": bindings_note,
        "mode": "digest",
    }
    if events_path.is_file():
        data = events_path.read_bytes()
        result["events_file"] = {"path": str(events_path), "bytes": len(data),
                                 "sha256": _sha_bytes(data)}
    else:
        result["events_file"] = "unknown"
    review_reasons = []
    if not isinstance(bindings, dict) or not isinstance(baselines, dict):
        review_reasons.append("bindings-or-baselines-not-captured")
    else:
        required = sorted(set(REQUIRED_BINDINGS) | set(baselines))
        missing = [k for k in required
                   if not isinstance(bindings.get(k), str) or not bindings[k]]
        invalid = [k for k in required
                   if isinstance(bindings.get(k), str) and bindings[k]
                   and not _SHA256.fullmatch(bindings[k])]
        changed = [k for k in required
                   if isinstance(bindings.get(k), str) and bindings[k]
                   and k in baselines and bindings[k] != baselines[k]]
        extras = [k for k in bindings if k not in baselines]
        if missing:
            result["baseline_missing"] = missing
        if invalid:
            result["baseline_invalid_format"] = invalid
        if changed:
            result["baseline_changed"] = changed
        if extras:
            result["baseline_unreviewed_extras"] = extras
        if missing or invalid or changed or extras:
            review_reasons.append("baseline-mismatch")
    issues = _completeness_issues(result, run)
    if issues:
        result["completeness_issues"] = issues
        review_reasons.append("incomplete-execution-fields")
    if review_reasons:
        result["mode"] = "full-review"
        result["review_reasons"] = review_reasons
    return result


def delta(first: dict, second: dict, prefix: str = "") -> list:
    """Explicit leaf-level structural delta; null and absent are DISTINCT,
    and equality never means acceptance."""
    changes = []
    for key in sorted(set(first) | set(second)):
        path = f"{prefix}.{key}" if prefix else key
        a = first.get(key, _MISSING)
        b = second.get(key, _MISSING)
        a_gone, b_gone = a is _MISSING, b is _MISSING
        if isinstance(a, dict) and isinstance(b, dict):
            changes.extend(delta(a, b, path))
            continue
        if a_gone or b_gone or a != b:
            changes.append({"path": path,
                            "from": "<absent>" if a_gone else a,
                            "to": "<absent>" if b_gone else b})
    return changes
