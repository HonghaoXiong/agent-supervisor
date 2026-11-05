"""Design-bound interface contracts and dependency-closure evidence."""

from __future__ import annotations

import hashlib
import json
import re
from typing import Any

_HASH = re.compile(r"^[0-9a-f]{64}$")


class DependencyError(ValueError):
    pass


def _canonical(value: Any) -> bytes:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
                      allow_nan=False).encode("utf-8")


def _digest(value: Any) -> str:
    return hashlib.sha256(_canonical(value)).hexdigest()


def _checks_by_id(design: dict) -> dict[str, dict]:
    result = {}
    for check in design.get("acceptance_checks", []):
        check_id = check.get("id")
        if not isinstance(check_id, str) or not check_id or check_id in result:
            raise DependencyError("design-invalid: acceptance check IDs must be unique")
        if not check.get("registered_command_id") or not check.get("oracle_ref") \
                or not isinstance(check.get("required"), bool):
            raise DependencyError("design-invalid: checker must have oracle/command/required")
        result[check_id] = check
    return result


def _validate_dag(design: dict) -> None:
    tasks = design.get("task_dag", [])
    ids = [task.get("task_id") for task in tasks]
    if len(ids) != len(set(ids)) or any(not isinstance(value, str) or not value for value in ids):
        raise DependencyError("design-invalid: task DAG IDs must be unique")
    graph = {task["task_id"]: list(task.get("depends_on", [])) for task in tasks}
    if any(dep not in graph for deps in graph.values() for dep in deps):
        raise DependencyError("design-invalid: task DAG dependency is missing")
    visiting, visited = set(), set()

    def visit(node):
        if node in visiting:
            raise DependencyError("design-invalid: task DAG contains a cycle")
        if node not in visited:
            visiting.add(node)
            for dep in graph[node]:
                visit(dep)
            visiting.remove(node)
            visited.add(node)
    for node in graph:
        visit(node)


def _edge_id(contract: dict, consumer: dict) -> str:
    return f"{contract['interface_id']}:{contract['producer']['task_id']}:{consumer['task_id']}"


def _plan_items(design: dict, candidate: dict) -> list[dict]:
    if not _HASH.fullmatch(candidate.get("candidate_sha256", "")):
        raise DependencyError("candidate-invalid: integrated candidate hash required")
    _validate_dag(design)
    registered = _checks_by_id(design)
    items = []
    for contract in design.get("interface_contracts", []):
        try:
            if contract["schema_version"] != "zsc.interface-contract/v1" \
                    or contract["owner_decision_id"] != design["decision_id"] \
                    or contract["owner_revision"] != design["revision"]:
                raise DependencyError("design-binding-mismatch: interface owner must be the logical decision ID/revision")
            if contract["producer"]["version"] != contract["version"] \
                    or any(c["expected_version"] != contract["version"] for c in contract["consumers"]):
                raise DependencyError("interface-version-conflict: producer/consumer interface versions differ")
            if not contract.get("checker_ids"):
                raise DependencyError("checker-unregistered: interface has no checker")
            contract_hash = _digest(contract)
            for consumer in contract["consumers"]:
                for checker_id in contract["checker_ids"]:
                    check = registered.get(checker_id)
                    if check is None:
                        raise DependencyError(f"checker-unregistered: {checker_id}")
                    items.append({"edge_id": _edge_id(contract, consumer),
                        "interface_id": contract["interface_id"], "interface_version": contract["version"],
                        "interface_sha256": contract_hash, "producer": contract["producer"]["task_id"],
                        "consumer": consumer["task_id"], "checker_id": checker_id,
                        "command_id": check["registered_command_id"], "oracle_ref": check["oracle_ref"],
                        "required": check["required"], "candidate_ref": candidate["candidate_sha256"]})
        except KeyError as error:
            raise DependencyError(f"design-invalid: missing interface field {error}") from error
    return items


def plan_dependency_checks(design: dict, candidate: dict) -> dict:
    """Return executable checker identities, never commands supplied by a report."""
    items = _plan_items(design, candidate)
    return {"schema_version": "zsc.dependency-check-plan/v1",
            "decision_id": design.get("decision_id"), "design_revision": design.get("revision"),
            "candidate_ref": candidate.get("candidate_sha256"), "checks": items}


def bind_design_interfaces(design: dict) -> dict:
    """Bind already published interface bytes to D without a circular D back-hash."""
    contracts = design.get("interface_contracts", [])
    hashes = {}
    for contract in contracts:
        if contract.get("owner_decision_id") != design.get("decision_id") \
                or contract.get("owner_revision") != design.get("revision"):
            raise DependencyError("design-binding-mismatch: interface owner mismatch")
        interface_id = contract.get("interface_id")
        if interface_id in hashes:
            raise DependencyError("design-invalid: duplicate interface ID")
        hashes[interface_id] = _digest(contract)
    return {"schema_version": "zsc.design-interface-binding/v1",
            "decision_id": design.get("decision_id"), "revision": design.get("revision"),
            "design_sha256": _digest(design), "interface_hashes": hashes}


def validate_design_binding(design: dict, binding: dict) -> bool:
    if bind_design_interfaces(design) != binding:
        raise DependencyError("design-binding-mismatch: published interface/design bytes changed")
    return True


def _valid_receipt(receipt: dict, item: dict) -> bool:
    event = receipt.get("checker_event", {})
    return receipt.get("schema_version") == "zsc.check-receipt/v1" \
        and receipt.get("edge_id") == item["edge_id"] \
        and receipt.get("checker_id") == item["checker_id"] \
        and receipt.get("interface_sha256") == item["interface_sha256"] \
        and receipt.get("candidate_ref") == item["candidate_ref"] \
        and receipt.get("oracle_ref") == item["oracle_ref"] \
        and event.get("command_id") == item["command_id"] \
        and event.get("candidate_ref") == item["candidate_ref"] \
        and isinstance(event.get("seq"), int) and not isinstance(event.get("seq"), bool) \
        and event.get("seq") >= 1 and isinstance(event.get("event_id"), str) \
        and receipt.get("result") in ("passed", "failed", "unknown", "not_applicable")


def evaluate_dependency_results(design: dict, receipts: list[dict]) -> dict:
    """Evaluate the latest valid checker event per edge/checker on one candidate."""
    candidate_events = [receipt for receipt in receipts
        if _HASH.fullmatch(receipt.get("candidate_ref", ""))
        and isinstance(receipt.get("checker_event", {}).get("seq"), int)
        and not isinstance(receipt.get("checker_event", {}).get("seq"), bool)]
    candidate_ref = "0" * 64
    if candidate_events:
        newest = max(receipt["checker_event"]["seq"] for receipt in candidate_events)
        newest_candidates = {receipt["candidate_ref"] for receipt in candidate_events
                             if receipt["checker_event"]["seq"] == newest}
        if len(newest_candidates) != 1:
            return {"schema_version": "zsc.dependency-report/v1", "status": "blocked",
                    "dependency_pass_rate": None, "first_closed_seq": None, "edges": [],
                    "reason": "candidate-conflict: equal latest checker events name different candidates"}
        candidate_ref = newest_candidates.pop()
    try:
        items = _plan_items(design, {"candidate_sha256": candidate_ref})
    except DependencyError as error:
        return {"schema_version": "zsc.dependency-report/v1", "status": "blocked",
                "dependency_pass_rate": None, "first_closed_seq": None,
                "edges": [], "reason": str(error)}
    grouped = {}
    for item in items:
        grouped.setdefault(item["edge_id"], []).append(item)
    latest = {}
    stale_seen = set()
    for receipt in receipts:
        for item in items:
            if receipt.get("edge_id") == item["edge_id"] and receipt.get("checker_id") == item["checker_id"]:
                key = (item["edge_id"], item["checker_id"])
                if not _valid_receipt(receipt, item):
                    stale_seen.add(key)
                    continue
                if key not in latest or receipt["checker_event"]["seq"] > latest[key]["checker_event"]["seq"]:
                    latest[key] = receipt
    rationales = design.get("dependency_rationales", {})
    edges = []
    for edge_id, edge_items in grouped.items():
        outcomes = []
        seqs = []
        evidence = []
        oracle = edge_items[0]["oracle_ref"]
        for item in edge_items:
            key = (edge_id, item["checker_id"])
            receipt = latest.get(key)
            if receipt is None:
                outcomes.append("stale" if key in stale_seen else "missing")
                continue
            outcomes.append(receipt["result"])
            seqs.append(receipt["checker_event"]["seq"])
            for ref in receipt.get("evidence_refs", []):
                if ref not in evidence:
                    evidence.append(ref)
            if receipt["result"] == "passed" and not receipt.get("evidence_refs"):
                outcomes[-1] = "missing-evidence"
        priority = ("failed", "missing-evidence", "unknown", "stale", "missing", "not_applicable", "passed")
        result = next(value for value in priority if value in outcomes)
        rationale = rationales.get(edge_id)
        named_na = isinstance(rationale, dict) and rationale.get("status") == "not_applicable" \
            and rationale.get("decision_id") == design.get("decision_id") and bool(rationale.get("reason"))
        required = any(item["required"] for item in edge_items)
        applicable = required and not (result == "not_applicable" and named_na)
        edges.append({"edge_id": edge_id, "result": result, "oracle_ref": oracle,
            "candidate_ref": edge_items[0]["candidate_ref"], "evidence_refs": evidence,
            "first_closed_seq": max(seqs) if result == "passed" and len(seqs) == len(edge_items) else None,
            "required": required, "applicable": applicable,
            "rationale": rationale if result in ("unknown", "not_applicable") else None})
    applicable = [edge for edge in edges if edge["applicable"]]
    passed = [edge for edge in applicable if edge["result"] == "passed"]
    blocked = any(edge["applicable"] and edge["result"] != "passed" for edge in edges)
    status = "blocked" if blocked else "closed"
    rate = len(passed) / len(applicable) if applicable else None
    first_closed = max(edge["first_closed_seq"] for edge in applicable) if applicable and len(passed) == len(applicable) else None
    return {"schema_version": "zsc.dependency-report/v1", "status": status,
            "decision_id": design.get("decision_id"), "design_revision": design.get("revision"),
            "candidate_ref": candidate_ref, "edges": edges, "dependency_pass_rate": rate,
            "first_closed_seq": first_closed,
            "reason": None if applicable else "no_required_dependencies"}
