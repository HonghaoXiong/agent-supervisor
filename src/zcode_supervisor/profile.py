"""Reviewed local habit profile: auto-loaded user rules and bounded tool executors.

A profile is controller-side JSON discovered next to the configuration. It carries
two layers that must not be conflated:

- ``rules``: habit text auto-appended to the task prompt. Every rule pins the
  SHA-256 of the file it was distilled from at review time; drift fails closed.
- ``tools``: bounded executors injected as additional exact-match Bash commands
  with hash-frozen executor scripts, or explicitly reported ``unavailable``
  entries (for example MCP-only tools) so every run states their status.

Injection only extends the packet. Duplicate ids, duplicate command strings, or
trusted-hash disagreements with the packet's own contract are hard errors: a
profile must never shadow or relax Codex's authority. Whether an injected tool
actually executed is decided solely by the run's command receipts; a prompt that
mentions a tool is never evidence of execution.
"""

from __future__ import annotations

import fnmatch
import hashlib
import json
from pathlib import Path
import re
import shlex

from .common import read_regular

SCHEMA_VERSION = 1
RULE_KEYS = {"id", "text", "sources", "applies_to"}
TOOL_KEYS = {"id", "description", "kind", "command", "executor", "applies_to",
             "max_uses", "max_timeout_ms", "required", "unavailable_reason"}
COMMAND_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]*")
SHA256 = re.compile(r"[a-f0-9]{64}")
# Bounded prompt-appendix budget. Sized to hold complete skill operational
# sections (section-aware excerpts, never cut mid-sentence) alongside habit
# rules and the approved-command disclosure; still a hard reviewed bound.
MAX_PROMPT_APPENDIX = 6144


def digest_file(path):
    return hashlib.sha256(read_regular(path)).hexdigest()


def resolve_profile_path(config):
    """Explicit ``profile`` wins and must exist; otherwise auto-discover beside config."""
    explicit = getattr(config, "profile", None)
    if explicit is not None:
        if not explicit.is_file():
            raise ValueError(f"Configured habit profile is missing: {explicit}")
        return explicit
    base = getattr(config, "config_dir", None)
    if base is not None and (base / "profile.json").is_file():
        return base / "profile.json"
    return None


def _checked_patterns(value, label):
    if (not isinstance(value, list) or not value
            or any(not isinstance(item, str) or not item.strip()
                   or item != item.strip() or "\x00" in item for item in value)):
        raise ValueError(f"{label} applies_to must be a nonempty list of clean patterns")
    return value


def _checked_sources(value, label):
    if not isinstance(value, list) or not value:
        raise ValueError(f"{label} sources must be a nonempty list")
    for item in value:
        if (not isinstance(item, dict) or set(item) != {"path", "sha256"}
                or not isinstance(item["path"], str) or not item["path"].startswith("/")
                or not SHA256.fullmatch(item["sha256"] or "")):
            raise ValueError(f"{label} source entries need absolute path and sha256")
        resolved = Path(item["path"])
        if ".." in resolved.parts:
            raise ValueError(f"{label} source path must not traverse upwards")
        if not resolved.is_file():
            raise ValueError(f"{label} source file is missing: {resolved}")
        if digest_file(resolved) != item["sha256"]:
            raise ValueError(f"{label} source changed since review; re-review the profile: {resolved}")




def _checked_command_template(tool):
    command = tool.get("command")
    if not isinstance(command, str) or not command.strip() or "\x00" in command:
        raise ValueError("Tool command must be a nonempty string")
    if "{" in command or "}" in command:
        remainder = command.replace("{workspace}", "")
        if "{" in remainder or "}" in remainder:
            raise ValueError("Tool command may only use the {workspace} placeholder")
        for token in command.split():
            if "{workspace}" in token and token != "{workspace}":
                raise ValueError(
                    "{workspace} must be a standalone unquoted token; pre-quoted or "
                    "embedded forms would double-quote or splice and are rejected "
                    "before dispatch")
    return command


def load_profile(path):
    """Read and fully verify a profile file; any drift from review fails closed."""
    raw = read_regular(path)
    try:
        value = json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError) as error:
        raise ValueError("Habit profile must be valid UTF-8 JSON") from error
    if not isinstance(value, dict) or set(value) != {"schema_version", "rules", "tools"}:
        raise ValueError("Habit profile must contain exactly schema_version, rules and tools")
    if type(value["schema_version"]) is not int or value["schema_version"] != SCHEMA_VERSION:
        raise ValueError("Only habit profile schema_version=1 is supported")
    if not isinstance(value["rules"], list) or not isinstance(value["tools"], list):
        raise ValueError("Habit profile rules and tools must be arrays")

    ids = set()
    for rule in value["rules"]:
        if not isinstance(rule, dict) or set(rule) != RULE_KEYS:
            raise ValueError("Rule entries must contain exactly " + ", ".join(sorted(RULE_KEYS)))
        if (not isinstance(rule["id"], str) or not COMMAND_ID.fullmatch(rule["id"])):
            raise ValueError("Rule id must be a simple identifier")
        if rule["id"] in ids:
            raise ValueError("Duplicate rule id: " + rule["id"])
        ids.add(rule["id"])
        if (not isinstance(rule["text"], str) or not rule["text"].strip()
                or len(rule["text"]) > MAX_PROMPT_APPENDIX):
            raise ValueError("Rule text must be nonempty and bounded")
        _checked_patterns(rule["applies_to"], f"Rule {rule['id']}")
        _checked_sources(rule["sources"], f"Rule {rule['id']}")

    ids = set()
    for tool in value["tools"]:
        if not isinstance(tool, dict) or set(tool) != TOOL_KEYS:
            raise ValueError("Tool entries must contain exactly " + ", ".join(sorted(TOOL_KEYS)))
        if (not isinstance(tool["id"], str) or not COMMAND_ID.fullmatch(tool["id"])):
            raise ValueError("Tool id must be a simple identifier")
        if tool["id"] in ids:
            raise ValueError("Duplicate tool id: " + tool["id"])
        ids.add(tool["id"])
        if not isinstance(tool["description"], str) or not tool["description"].strip():
            raise ValueError("Tool description must be a nonempty string")
        if tool["kind"] not in ("executor", "unavailable"):
            raise ValueError("Tool kind must be executor or unavailable")
        uses = tool.get("max_uses", 1)
        timeout = tool.get("max_timeout_ms", 120000)
        if type(uses) is not int or uses <= 0:
            raise ValueError("Tool max_uses must be a positive integer")
        if not isinstance(timeout, (int, float)) or isinstance(timeout, bool) or timeout <= 0:
            raise ValueError("Tool max_timeout_ms must be positive")
        if type(tool.get("required", False)) is not bool:
            raise ValueError("Tool required must be a boolean")
        _checked_patterns(tool["applies_to"], f"Tool {tool['id']}")
        if tool["kind"] == "unavailable":
            if (tool.get("command") is not None or tool.get("executor") is not None
                    or not isinstance(tool.get("unavailable_reason"), str)
                    or not tool["unavailable_reason"].strip()):
                raise ValueError("Unavailable tools need a reason and no command or executor")
            continue
        if not isinstance(tool.get("unavailable_reason"), type(None)):
            raise ValueError("Executor tools must leave unavailable_reason null")
        _checked_command_template(tool)
        executor = tool.get("executor")
        if (not isinstance(executor, dict) or set(executor) != {"path", "sha256"}
                or not isinstance(executor["path"], str)
                or not executor["path"].startswith("/") or ".." in executor["path"].split("/")):
            raise ValueError("Tool executor needs an absolute normalized path and sha256")
        script = Path(executor["path"])
        if not script.is_file():
            raise ValueError(f"Tool executor is missing: {script}")
        if not SHA256.fullmatch(executor["sha256"] or ""):
            raise ValueError("Tool executor sha256 must be a lowercase digest")
        if digest_file(script) != executor["sha256"]:
            raise ValueError(f"Tool executor changed since review; re-review: {script}")
    return value


def _applies(patterns, relative_paths):
    return any(fnmatch.fnmatchcase(name, pattern)
               for name in relative_paths for pattern in patterns)


def apply_profile(profile, packet, workspace, relative_paths):
    """Extend a validated packet with applicable habits; return (packet, report).

    ``relative_paths`` should combine the current workspace inventory with the
    packet's ``allowed_files`` so habits also trigger on planned new files.
    The ``{workspace}`` placeholder is substituted shell-quoted; callers must
    not pre-quote it in the template.
    """
    paths = list(relative_paths)
    report = {"rules": [], "tools": [], "prompt_appended_chars": 0}

    applicable_rules = [rule for rule in profile["rules"] if _applies(rule["applies_to"], paths)]
    lines = [f"- [habit:{rule['id']}] {rule['text'].strip()}" for rule in applicable_rules]
    tool_lines = []
    for rule in profile["rules"]:
        matched = sorted(name for name in paths if _applies(rule["applies_to"], [name]))
        report["rules"].append({"id": rule["id"], "applies": rule in applicable_rules,
                                "matched": matched[:5]})

    commands = [dict(entry) for entry in packet.get("commands", [])]
    trusted = dict(packet.get("trusted_hashes", {}))
    existing_ids = {entry["id"] for entry in commands}
    existing_commands = {entry["command"] for entry in commands}
    for tool in profile["tools"]:
        applies = _applies(tool["applies_to"], paths)
        entry = {"id": tool["id"], "kind": tool["kind"], "applies": applies,
                 "description": tool["description"]}
        if tool["kind"] == "unavailable":
            entry["unavailable_reason"] = tool["unavailable_reason"]
            entry["executed"] = False
            report["tools"].append(entry)
            continue
        if not applies:
            report["tools"].append(entry)
            continue
        command_id = "habit-" + tool["id"]
        # Contract enforced again at dispatch: the placeholder must be one bare
        # token; shlex.quote() then renders exactly one safe shell argument.
        for token in tool["command"].split():
            if "{workspace}" in token and token != "{workspace}":
                raise ValueError("{workspace} must be a standalone unquoted token")
        rendered = tool["command"].replace("{workspace}", shlex.quote(str(workspace)))
        if command_id in existing_ids:
            raise ValueError(f"Packet already defines command id {command_id}; refusing to shadow")
        if rendered in existing_commands:
            raise ValueError(f"Packet already contains the exact habit command of {tool['id']}")
        script = tool["executor"]["path"]
        pinned = trusted.get(script)
        if pinned is not None and pinned != tool["executor"]["sha256"]:
            raise ValueError(f"Packet and profile disagree on trusted hash for {script}")
        trusted[script] = tool["executor"]["sha256"]
        commands.append({"id": command_id, "command": rendered,
                         "max_uses": tool.get("max_uses", 1),
                         "max_timeout_ms": tool.get("max_timeout_ms", 120000),
                         "required": tool.get("required", False)})
        existing_ids.add(command_id)
        existing_commands.add(rendered)
        entry["command_id"] = command_id
        entry["executed"] = False
        report["tools"].append(entry)
        budget = tool.get("max_timeout_ms", 120000)
        tool_lines.append(f"- {command_id}（逐字复制，不得改路径或加前缀；timeout 不得超过 {budget}ms）: {rendered}")

    appendix = ""
    if lines or tool_lines:
        parts = []
        if lines:
            parts.append("[用户习惯规则 · 自动注入 · 来源哈希已在结果中登记]\n" + "\n".join(lines))
        if tool_lines:
            parts.append(
                "[已批准的习惯工具命令 · 逐字复制执行 · 合同范围：Bash 仅限逐字执行下列命令，"
                "读取已知工作区文件用 Read；原生自动许可不是合同许可，信息不足时如实报告而非自行探测]\n"
                + "\n".join(tool_lines))
        appendix = "\n\n" + "\n\n".join(parts)
        if len(appendix) > MAX_PROMPT_APPENDIX:
            raise ValueError("Habit rule appendix exceeds the bounded prompt budget")

    extended = dict(packet)
    extended["commands"] = commands
    extended["trusted_hashes"] = trusted
    # Task inputs under .habit-params/ feed injected executors (formula scripts,
    # graph parameters). Their immutability is the dispatch contract: allowing
    # the worker to rewrite them would let unreviewed code run under an
    # approved exact command, so any overlap with allowed_files is refused.
    if tool_lines:
        conflicting = sorted(name for name in packet.get("allowed_files", [])
                             if name == ".habit-params"
                             or name.startswith(".habit-params/"))
        if conflicting:
            raise ValueError(
                "task parameter files must stay immutable for injected habit "
                f"tools; remove them from allowed_files: {conflicting}")
    extended["prompt"] = packet["prompt"] + appendix
    report["prompt_appended_chars"] = len(appendix)
    return extended, report
