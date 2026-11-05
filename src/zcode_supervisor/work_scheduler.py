"""Conservative DAG scheduling and single-versioned conflict resolution."""

from __future__ import annotations


class SchedulerError(ValueError):
    def __init__(self, code, message, details=None):
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message
        self.details = details or {}


def _cycle_path(nodes, dependencies):
    visiting = []
    visited = set()

    def visit(node):
        if node in visiting:
            return visiting[visiting.index(node):] + [node]
        if node in visited:
            return None
        visiting.append(node)
        for parent in sorted(dependencies[node]):
            result = visit(parent)
            if result:
                return result
        visiting.pop()
        visited.add(node)
        return None

    for node in sorted(nodes):
        result = visit(node)
        if result:
            return result
    return None


def _task_order(nodes, dependencies):
    remaining = set(nodes)
    completed = set()
    result = []
    while remaining:
        ready = sorted(node for node in remaining if dependencies[node] <= completed)
        if not ready:
            raise SchedulerError("dependency-cycle", "task dependencies contain a cycle",
                                 {"path": _cycle_path(nodes, dependencies)})
        result.extend(ready)
        completed.update(ready)
        remaining.difference_update(ready)
    return result


def plan_schedule(design, leases, policy) -> dict:
    if not all(isinstance(value, dict) for value in (design, policy)) or not isinstance(leases, list):
        raise SchedulerError("invalid-argument", "design/policy must be objects and leases a list")
    tasks = design.get("task_dag")
    if not isinstance(tasks, list):
        raise SchedulerError("schema-invalid", "design task_dag must be a list")
    dependencies = {}
    for task in tasks:
        task_id = task.get("task_id")
        parents = task.get("depends_on")
        if not isinstance(task_id, str) or not task_id or task_id in dependencies \
                or not isinstance(parents, list) or any(not isinstance(item, str) for item in parents):
            raise SchedulerError("schema-invalid", "task IDs must be unique and dependencies string lists")
        dependencies[task_id] = set(parents)
    for task_id, parents in dependencies.items():
        if not parents <= set(dependencies):
            raise SchedulerError("dependency-missing", f"{task_id} depends on an unknown task")
    edges = {f"dag:{parent}:{task}" for task, parents in dependencies.items() for parent in parents}
    for contract in design.get("interface_contracts", []):
        producer = contract.get("producer", {})
        producer_id = producer.get("task_id")
        if producer_id not in dependencies:
            raise SchedulerError("dependency-missing", "interface producer is not a design task")
        for consumer in contract.get("consumers", []):
            consumer_id = consumer.get("task_id")
            if consumer_id not in dependencies:
                raise SchedulerError("dependency-missing", "interface consumer is not a design task")
            if producer.get("version") != consumer.get("expected_version"):
                raise SchedulerError("interface-version-conflict",
                    "disjoint writes cannot bypass interface version semantics",
                    {"interface_id": contract.get("interface_id"), "producer": producer_id,
                     "consumer": consumer_id})
            dependencies[consumer_id].add(producer_id)
            edges.add(f"interface:{contract.get('interface_id')}:{producer_id}:{consumer_id}")
    cycle = _cycle_path(dependencies, dependencies)
    if cycle:
        raise SchedulerError("dependency-cycle", "task/interface dependencies contain a cycle", {"path": cycle})
    order = _task_order(dependencies, dependencies)
    requested_limit = policy.get("max_parallel", 2)
    if isinstance(requested_limit, bool) or not isinstance(requested_limit, int) or requested_limit < 1:
        raise SchedulerError("schema-invalid", "max_parallel must be a positive integer")
    limit = min(2, requested_limit)
    task_io = policy.get("task_io", {})
    integration_ids = sorted(check["id"] for check in design.get("acceptance_checks", [])
        if check.get("required") is True and check.get("qualification") == "integration")
    conflicts = []
    for conflict in policy.get("conflicts", []):
        if conflict.get("status") != "resolved":
            conflicts.append({"kind": conflict.get("kind", "unresolved-conflict"),
                              "conflict_id": conflict.get("conflict_id"),
                              "resolution": "single-versioned-design-decision-required"})
    for lease in leases:
        if lease.get("mode") != "write":
            continue
        for task_id, io in task_io.items():
            if lease.get("task_id") == task_id or lease.get("candidate_id") != io.get("candidate_id"):
                continue
            if lease.get("resource") in set(io.get("writes", [])) | {"*"}:
                conflicts.append({"kind": "resource-lease-conflict", "task_id": task_id,
                                  "lease_task_id": lease.get("task_id"),
                                  "candidate_id": lease.get("candidate_id"),
                                  "resource": lease.get("resource")})
    upgrade = []
    serial_reasons = {}

    def compatible(left, right):
        a = task_io.get(left)
        b = task_io.get(right)
        if not isinstance(a, dict) or not isinstance(b, dict) \
                or a.get("scope_known") is not True or b.get("scope_known") is not True:
            serial_reasons[f"{left}:{right}"] = "read-write-scope-unknown"
            return False
        if not integration_ids:
            serial_reasons[f"{left}:{right}"] = "integration-check-missing"
            return False
        if a.get("candidate_id") == b.get("candidate_id"):
            serial_reasons[f"{left}:{right}"] = "isolated-candidate-required"
            return False
        a_reads, a_writes = set(a.get("reads", [])), set(a.get("writes", []))
        b_reads, b_writes = set(b.get("reads", [])), set(b.get("writes", []))
        if a_writes & (b_reads | b_writes) or b_writes & a_reads:
            serial_reasons[f"{left}:{right}"] = "read-write-overlap"
            return False
        return True

    waves = []
    if not conflicts:
        remaining = set(order)
        completed = set()
        while remaining:
            ready = [task for task in order if task in remaining and dependencies[task] <= completed]
            wave = []
            for task in ready:
                if len(wave) >= limit:
                    break
                if all(compatible(task, other) for other in wave):
                    wave.append(task)
            if not wave:
                # The first topologically ready task can always run serially.
                wave = [ready[0]]
            waves.append(wave)
            completed.update(wave)
            remaining.difference_update(wave)
    if any(not isinstance(task_io.get(task), dict) or task_io[task].get("scope_known") is not True
           for task in order):
        upgrade.append("declare-read-write-scope-and-isolated-candidate")
    if not integration_ids and len(order) > 1:
        upgrade.append("register-integration-checker-before-parallel-writes")
    if any(reason == "isolated-candidate-required" for reason in serial_reasons.values()):
        upgrade.append("allocate-separate-candidate-copies")
    return {"schema_version": "zsc.schedule/v1", "status": "blocked" if conflicts else "planned",
            "decision_id": design.get("decision_id"), "design_revision": design.get("revision"),
            "max_parallel": limit, "waves": waves, "edges": sorted(edges),
            "conflicts": conflicts, "serial_reasons": serial_reasons,
            "integration": {"candidate": "single-integrated-candidate",
                            "checker_ids": integration_ids, "rerun_all_affected": True,
                            "local_results_skip_integration": False},
            "upgrade_conditions": sorted(set(upgrade))}


def resolve_conflict(conflict, decision_ref, actor) -> dict:
    actor.require("design:accept")
    if not isinstance(conflict, dict) or not isinstance(decision_ref, dict) \
            or not conflict.get("conflict_id"):
        raise SchedulerError("invalid-argument", "conflict and decision_ref must be objects with an ID")
    required = {"decision_id", "revision", "selected_option", "rationale"}
    if not required <= set(decision_ref) or not decision_ref["rationale"]:
        raise SchedulerError("decision-invalid", "a unified design decision is required")
    participants = sorted(set(conflict.get("participants", [])))
    receipts = decision_ref.get("delta_receipts", [])
    delivered = {receipt.get("task_id") for receipt in receipts
                 if receipt.get("delivered") is True and receipt.get("artifact_sha256")}
    dispatch_status = "recorded" if participants and participants <= list(delivered) else "pending"
    return {"schema_version": "zsc.conflict-resolution/v1", "conflict_id": conflict["conflict_id"],
            "status": "resolved", "strategy": "single-versioned-design-decision",
            "resolution_revision": conflict.get("resolution_revision", 0) + 1,
            "supersedes": conflict.get("decision_ref"),
            "decision_ref": dict(decision_ref), "actor": actor.evidence(),
            "delta_dispatch": {"status": dispatch_status, "recipients": participants,
                "receipt_count": len(receipts), "note": "pending is not a delivery claim"},
            "integration_recheck_required": True}
