#!/usr/bin/env python3
"""Export a privacy-filtered Mac validation report without requesting model work."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import platform
import select
import stat
import subprocess
import tempfile
import time
import uuid


SKILL = "zcode-supervisor:zcode-supervisor"
PLUGIN_ID = "zcode-supervisor@zcode-supervisor-local"
MAX_CATALOG_BYTES = 8 * 1024 * 1024
MAX_RECEIPT_BYTES = 16 * 1024 * 1024
OUTCOMES = {
    "delivered", "evidence-rejected", "timeout", "preflight-rejected",
    "protocol-error", "postflight-rejected", "worker-failed", "scope-error",
}


class DiscoveryError(Exception):
    """The fixed code is safe for a report shared with another computer."""


def load_diagnostics():
    path = Path(__file__).resolve().with_name("diagnostics.py")
    spec = importlib.util.spec_from_file_location("czs_validation_diagnostics", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def query_installed_skill(codex: str, timeout: float = 20) -> dict:
    """Use only read-only app-server discovery; do not start a thread or turn."""
    deadline = time.monotonic() + timeout
    process = None
    pending = bytearray()
    received = 0
    sent_methods = []

    def send(message):
        method = message["method"]
        if method not in {"initialize", "initialized", "skills/list"}:
            raise DiscoveryError("unexpected_method")
        sent_methods.append(method)
        process.stdin.write((json.dumps(message) + "\n").encode())
        process.stdin.flush()

    def response(request_id):
        nonlocal received
        while True:
            while b"\n" in pending:
                line, _, remainder = pending.partition(b"\n")
                pending[:] = remainder
                if not line.strip():
                    continue
                try:
                    frame = json.loads(line)
                except (ValueError, UnicodeError):
                    raise DiscoveryError("invalid_app_server_json") from None
                if isinstance(frame, dict) and frame.get("id") == request_id:
                    if "error" in frame:
                        raise DiscoveryError("app_server_rpc_error")
                    if not isinstance(frame.get("result"), dict):
                        raise DiscoveryError("invalid_app_server_result")
                    return frame["result"]
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise DiscoveryError("app_server_timeout")
            if not select.select([process.stdout], [], [], remaining)[0]:
                raise DiscoveryError("app_server_timeout")
            data = os.read(process.stdout.fileno(), 65536)
            if not data:
                raise DiscoveryError("app_server_closed")
            received += len(data)
            if received > MAX_CATALOG_BYTES:
                raise DiscoveryError("app_server_response_too_large")
            pending.extend(data)

    try:
        # A fresh cwd avoids scanning a user's current project. The existing
        # Codex home is queried so this checks the plugin the user installed.
        with tempfile.TemporaryDirectory(prefix="czs-validation-") as temporary:
            process = subprocess.Popen(
                [str(codex), "app-server"], cwd=temporary,
                stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
            )
            send({"id": 1, "method": "initialize", "params": {
                "clientInfo": {"name": "czs-mac-validation", "version": "1"},
                "capabilities": {"experimentalApi": True},
            }})
            response(1)
            send({"method": "initialized", "params": {}})
            send({"id": 2, "method": "skills/list", "params": {
                "cwds": [temporary], "forceReload": True,
            }})
            result = response(2)
            data = result.get("data")
            if not isinstance(data, list) or len(data) != 1 or not isinstance(data[0], dict):
                raise DiscoveryError("invalid_skills_catalog")
            catalog = data[0]
            skills = catalog.get("skills")
            errors = catalog.get("errors")
            if not isinstance(skills, list) or not isinstance(errors, list):
                raise DiscoveryError("invalid_skills_catalog")
            target = [item for item in skills if isinstance(item, dict)
                      and item.get("name") == SKILL]
            enabled = len(target) == 1 and target[0].get("enabled") is True
            installed = len(target) == 1 and target[0].get("pluginId") == PLUGIN_ID
            ok = enabled and installed and not errors
            return {
                "status": "passed" if ok else "failed",
                "skill": SKILL, "plugin_id": PLUGIN_ID,
                "found": bool(target), "matches": len(target),
                "enabled": enabled, "expected_plugin_identity": installed,
                "catalog_error_count": len(errors),
                "methods_sent": sent_methods, "model_requests": 0,
            }
    except DiscoveryError as error:
        return {"status": "failed", "error_code": str(error),
                "skill": SKILL, "methods_sent": sent_methods, "model_requests": 0}
    except (OSError, ValueError, subprocess.SubprocessError):
        # Raw app-server output or exception strings may contain local paths.
        return {"status": "failed", "error_code": "app_server_unavailable",
                "skill": SKILL, "methods_sent": sent_methods, "model_requests": 0}
    finally:
        if process is not None:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=2)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=2)
            for stream in (process.stdin, process.stdout):
                if stream is not None:
                    stream.close()


def regular_bytes(path: Path, limit: int) -> bytes:
    # O_NONBLOCK prevents a supplied FIFO from hanging before fstat rejects it.
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
    fd = os.open(path, flags)
    try:
        info = os.fstat(fd)
        if not stat.S_ISREG(info.st_mode) or info.st_size > limit:
            raise ValueError("invalid_file")
        with os.fdopen(fd, "rb", closefd=False) as stream:
            data = stream.read(limit + 1)
        if len(data) > limit:
            raise ValueError("oversized_file")
        return data
    finally:
        os.close(fd)


def inspect_receipt(path: Path | None) -> dict:
    """Summarize an existing receipt, preserving unknowns without accepting it."""
    if path is None:
        return {"status": "not_run", "validator_started_task": False,
                "acceptance": "not_assessed"}
    result = {"status": "receipt_supplied", "validator_started_task": False,
              "inspection_only": True, "acceptance": "pending_independent_review"}
    try:
        if not path.is_absolute():
            raise ValueError("relative_receipt")
        raw = regular_bytes(path, MAX_RECEIPT_BYTES)
        receipt = json.loads(raw)
        if not isinstance(receipt, dict):
            raise ValueError("invalid_receipt")
        result["receipt_sha256"] = hashlib.sha256(raw).hexdigest()
        result["delivery_ok"] = receipt.get("delivery_ok") \
            if type(receipt.get("delivery_ok")) is bool else None
        exit_code = receipt.get("process_exit_code")
        result["process_exit_code"] = exit_code if type(exit_code) is int else None
        outcome = receipt.get("outcome")
        result["outcome"] = outcome if isinstance(outcome, str) and outcome in OUTCOMES else "unknown"
        identities = {}
        for key in ("session_id", "turn_id", "input_id"):
            value = receipt.get(key)
            known = isinstance(value, str) and bool(value.strip())
            identities[key] = {"status": "known" if known else "unknown"}
            if known:
                identities[key]["sha256"] = hashlib.sha256(value.encode()).hexdigest()
        result["identity"] = identities
        evidence = {}
        for label, suffix in (("events", ".events.jsonl"), ("stderr", ".stderr.log"),
                              ("before", ".before.json")):
            member = path.with_suffix(suffix)
            try:
                info = member.lstat()
                present = stat.S_ISREG(info.st_mode)
                evidence[label] = {"present": present,
                                   "bytes": info.st_size if present else None}
            except OSError:
                evidence[label] = {"present": False, "bytes": None}
        result["raw_evidence"] = evidence
        problems = receipt.get("execution_problems")
        result["execution_problem_count"] = len(problems) if isinstance(problems, list) else None
        # This describes the supplied receipt's completeness, never native
        # compiled-bridge proof, task output correctness, or model cost.
        result["reported_delivery_complete"] = (
            result["delivery_ok"] is True and result["process_exit_code"] == 0
            and result["outcome"] == "delivered"
            and all(item["status"] == "known" for item in identities.values())
            and all(item["present"] for item in evidence.values())
            and evidence["events"]["bytes"] > 0
            and result["execution_problem_count"] == 0
        )
        return result
    except (OSError, ValueError, UnicodeError, TypeError):
        result.update(status="invalid_receipt", error_code="receipt_unreadable_or_invalid",
                      reported_delivery_complete=False)
        return result


def build_report(plugin_root: Path, *, codex=None, node=None, cli=None,
                 receipt: Path | None = None, timeout: float = 20) -> dict:
    report = {
        "schema_version": 1, "report_type": "zcode-supervisor-mac-validation",
        "created_at_utc": datetime.now(timezone.utc).isoformat(),
        "platform": platform.system(), "architecture": platform.machine(),
        "model_requests": 0, "zcode_started": False,
        "actual_task": inspect_receipt(receipt),
    }
    try:
        diagnostics = load_diagnostics()
        report["environment"] = diagnostics.inspect_environment(
            plugin_root, codex=codex, node=node, cli=cli)
        executable = diagnostics.find_codex(codex)
        report["skill_discovery"] = query_installed_skill(str(executable), timeout) \
            if executable else {"status": "failed", "error_code": "codex_cli_missing", "model_requests": 0}
    except (OSError, ValueError, ImportError, AttributeError, TypeError):
        report["environment"] = {"ok": False, "error_code": "diagnostics_unavailable", "model_requests": 0}
        report["skill_discovery"] = {"status": "not_run", "error_code": "diagnostics_unavailable", "model_requests": 0}
    base_ok = report["environment"].get("ok") is True \
        and report["skill_discovery"].get("status") == "passed"
    task_ok = receipt is None or report["actual_task"].get("reported_delivery_complete") is True
    report["ok"] = base_ok and task_ok
    report["scope"] = "Environment and installed-skill discovery; supplied receipts require independent task review."
    return report


def save_report(path: Path, report: dict) -> None:
    if not path.is_absolute():
        raise ValueError("Report output must be an absolute path")
    data = (json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode()
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    with os.fdopen(fd, "wb") as stream:
        stream.write(data)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, help="New absolute report path; never overwrites a report")
    parser.add_argument("--receipt", type=Path, help="Existing absolute czs result JSON; inspect only")
    parser.add_argument("--codex", help="Existing Codex CLI executable")
    parser.add_argument("--node", help="Existing Node executable")
    parser.add_argument("--cli", help="Existing ZCode CLI entry; never started by validation")
    parser.add_argument("--timeout", type=float, default=20, help="Discovery deadline in seconds (1–60)")
    args = parser.parse_args(argv)
    if not 1 <= args.timeout <= 60:
        parser.error("--timeout must be 1–60 seconds")
    output = args.output
    if output is None:
        desktop = Path.home() / "Desktop"
        desktop.mkdir(mode=0o700, exist_ok=True)
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        output = desktop / ("zcode-supervisor-validation-" + timestamp + "-" + uuid.uuid4().hex[:8] + ".json")
    root = Path(__file__).resolve().parent.parent / "plugins" / "zcode-supervisor"
    report = build_report(root, codex=args.codex, node=args.node, cli=args.cli,
                          receipt=args.receipt, timeout=args.timeout)
    try:
        save_report(output, report)
    except (OSError, ValueError):
        print(json.dumps({"ok": False, "error": "Cannot create a new report at the output path; choose an existing writable directory and unused filename."}))
        return 2
    print(json.dumps({"ok": report["ok"], "report_file": str(output), "model_requests": 0}, ensure_ascii=False))
    print("验证报告已保存；请回传这个 JSON。此工具未启动 ZCode，也未发起模型任务。")
    return 0 if report["ok"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
