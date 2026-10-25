"""Reviewed native-orchestration contract for czs worker packets.

A packet may declare one ``orchestration`` block enabling, under explicit
contracts, the native mechanisms verified against the installed bundle
(capability-inventory.json in the pilot evidence):

* ``goal``      – ``session/goal`` lifecycle (set/show; pause/resume/clear are
                  validated in small-scale probes only; the RPC carries no
                  budget field and none is invented);
* ``agents``    – the Agent tool plus host-side subagent runtime config: czs
                  answers ``subagents/readRuntimeConfig`` with profiles pinned
                  by the immutable packet (``permissionMode: "default"`` so
                  child tool calls still reach the broker; Explore is
                  forbidden because the bundle gives it a yolo substitute);
* ``workflow``  – the CreateWorkflow family with the whole script pinned in
                  the packet (strict input: exactly one source, no shell in
                  ``world.run``), ``max_concurrency`` capped by the packet;
* ``skills``    – the Skill tool for explicitly enumerated skill names only.

Without the block nothing changes: the accepted four-tool baseline behavior
is byte-identical. Enforcement boundaries are honest: broker-checked are
exact commands, allowed files, workflow script bytes and child identity;
workflow-subagent behaviour beyond profiles is instruction-level and is
never presented as a sandbox.
"""
from __future__ import annotations

import re
from typing import Any

AGENT_TOOLS = ("Agent", "TaskOutput", "TaskStop")
WORKFLOW_TOOLS = ("CreateWorkflow", "GetWorkflowRun", "ListWorkflowRuns", "TaskOutput")

_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")
# Verified live (small-scale run B, 2026-10-14): broker-dependent tools held
# by Agent children auto-execute in the child runtime WITHOUT a host broker
# round-trip on this server version; czs only catches them post-flight. The
# preventive contract therefore admits read-only child tools only — every
# write path stays parent-side under the accepted broker.
SAFE_CHILD_TOOLS = {"Read"}
_MAX_CONCURRENCY = 64
_FORBIDDEN_TYPES = {"Explore"}  # bundle: explore agents run with a yolo permission substitute


def _plain_str(value: Any, label: str, max_len: int = 4096) -> str:
    if (not isinstance(value, str) or not value.strip() or value != value.strip()
            or "\x00" in value or len(value) > max_len):
        raise ValueError(f"{label} must be a short nonempty trimmed string")
    return value


def _tools_list(value: Any, label: str) -> list[str]:
    if not isinstance(value, list) or not value:
        raise ValueError(f"{label} must be a nonempty list of tool names")
    for name in value:
        _plain_str(name, f"{label} entry", 64)
    if len(set(value)) != len(value):
        raise ValueError(f"{label} entries must be unique")
    return list(value)


def validate_orchestration(value: Any) -> dict:
    """Strictly validate the packet orchestration block; raises ValueError."""
    if not isinstance(value, dict):
        raise ValueError("orchestration must be an object")
    if set(value) - {"goal", "agents", "workflow", "skills"}:
        raise ValueError("Unknown orchestration field")
    result: dict[str, Any] = {}
    if "goal" in value:
        goal = value["goal"]
        if not isinstance(goal, dict) or set(goal) - {"objective"}:
            raise ValueError("goal must be an object with an objective")
        objective = _plain_str(goal.get("objective"), "goal objective", 8192)
        result["goal"] = {"objective": objective}
    if "agents" in value:
        agents = value["agents"]
        if not isinstance(agents, dict) or set(agents) - {"profiles", "allowed_types",
                                                          "neutralize_builtins", "expected_children"}:
            raise ValueError("agents must be an object with profiles and allowed_types")
        profiles = agents.get("profiles")
        allowed_types = agents.get("allowed_types")
        neutralize = agents.get("neutralize_builtins", True)
        if not isinstance(neutralize, bool):
            raise ValueError("neutralize_builtins must be a boolean")
        if not isinstance(profiles, list) or not profiles:
            raise ValueError("agent profiles must be a nonempty list")
        names: set[str] = set()
        for profile in profiles:
            if not isinstance(profile, dict) or set(profile) - {"name", "description", "systemPrompt",
                                                                 "tools", "max_turns", "skills"}:
                raise ValueError("Invalid agent profile specification")
            name = _plain_str(profile.get("name"), "profile name", 64)
            if not _NAME.match(name):
                raise ValueError("profile name must be an identifier")
            if name in names:
                raise ValueError("profile names must be unique")
            names.add(name)
            _plain_str(profile.get("description"), "profile description", 512)
            # The server-side runtime-config schema requires systemPrompt as a
            # string at the wire boundary (verified live: Zod rejects its
            # absence), so the packet must always provide one.
            _plain_str(profile.get("systemPrompt"), "profile systemPrompt", 8192)
            tools = _tools_list(profile.get("tools"), "profile tools")
            skills = profile.get("skills")
            if skills is not None:
                if not isinstance(skills, list) or not skills:
                    # An empty list converts to undefined in the runtime filter
                    # and means UNLIMITED skills — rejected outright.
                    raise ValueError("profile skills must be a nonempty list of exact "
                                     "qualified skill names (empty means unlimited)")
                for name in skills:
                    _plain_str(name, "profile skill entry", 160)
                if len(set(skills)) != len(skills):
                    raise ValueError("profile skills entries must be unique")
            allowed_tools = set(SAFE_CHILD_TOOLS) | ({"Skill"} if skills else set())
            unsafe = set(tools) - allowed_tools
            if unsafe:
                raise ValueError(f"child profile tools must be read-only on this server "
                                 f"version (broker-less auto-execution verified live; Skill "
                                 f"only with pinned skills): {sorted(unsafe)}")
            if "Skill" in tools and not skills:
                raise ValueError("Skill tool requires pinned profile skills")
            max_turns = profile.get("max_turns")
            if isinstance(max_turns, bool) or not isinstance(max_turns, int) or not 1 <= max_turns <= 64:
                raise ValueError("profile max_turns must be an integer in 1..64")
        if not isinstance(allowed_types, list) or not allowed_types:
            raise ValueError("allowed_types must be a nonempty list")
        for type_name in allowed_types:
            _plain_str(type_name, "allowed type", 64)
            if type_name in _FORBIDDEN_TYPES and not (
                    neutralize and type_name in NEUTRALIZED_BUILTINS):
                # With neutralize_builtins the name refers to czs's read-only
                # shadow profile, not the built-in yolo-substituted agent.
                raise ValueError(f"{type_name} subagents run with an unrestricted permission substitute "
                                 "and are not allowed in constrained packets")
        if len(set(allowed_types)) != len(allowed_types):
            raise ValueError("allowed_types entries must be unique")
        # Reviewer finding 1: a built-in general-purpose child runs with the
        # server's default toolset, and broker-dependent child tools
        # auto-execute without a host round-trip on this server version —
        # so every allowed type must be a declared read-only profile.
        undeclared = {t for t in allowed_types if t not in names}
        if undeclared:
            raise ValueError(f"allowed types must be declared read-only profiles: {sorted(undeclared)}")
        result["agents"] = {"profiles": profiles, "allowed_types": list(allowed_types),
                            "neutralize_builtins": neutralize}
        expected = agents.get("expected_children")
        if expected is not None and (isinstance(expected, bool) or not isinstance(expected, int)
                                     or expected < 1):
            raise ValueError("expected_children must be a positive integer")
        if expected is not None:
            result["agents"]["expected_children"] = expected
    if "workflow" in value:
        workflow = value["workflow"]
        if not isinstance(workflow, dict) or set(workflow) - {
                "name", "script", "path", "max_concurrency", "subagent_model"}:
            raise ValueError("Invalid workflow specification")
        if "name" in workflow:
            _plain_str(workflow["name"], "workflow name", 64)
        script, script_path = workflow.get("script"), workflow.get("path")
        if bool(script) == bool(script_path):
            raise ValueError("workflow must declare exactly one script source: "
                             "inline script or a workspace-relative path")
        if script is not None and (not isinstance(script, str) or not script.strip()
                                   or "\x00" in script or len(script) > 256_000):
            raise ValueError("workflow script must be a nonempty string within the 256000-byte launch cap")
        if script_path is not None:
            # Path mode pins bytes exactly: the supervisor writes the script
            # file before dispatch (frozen input) and the worker passes only
            # the path — no model retyping of the script.
            from pathlib import Path as _Path
            if not isinstance(script_path, str) or "\x00" in script_path \
                    or not 1 <= len(script_path) <= 256:
                raise ValueError("workflow path must be a short workspace-relative file name")
            candidate = _Path(script_path)
            if candidate.is_absolute() or ".." in candidate.parts \
                    or candidate.as_posix() != script_path or candidate.suffix != ".ts":
                raise ValueError("workflow path must be a normalized relative .ts file")
        concurrency = workflow.get("max_concurrency")
        if isinstance(concurrency, bool) or not isinstance(concurrency, int) \
                or not 1 <= concurrency <= _MAX_CONCURRENCY:
            raise ValueError(f"workflow max_concurrency must be an integer in 1..{_MAX_CONCURRENCY}")
        if "subagent_model" in workflow:
            _plain_str(workflow["subagent_model"], "workflow subagent_model", 128)
        result["workflow"] = workflow
    if "skills" in value:
        skills = value["skills"]
        if not isinstance(skills, dict) or set(skills) - {"allowed"}:
            raise ValueError("skills must be an object with an allowed list")
        allowed = _tools_list(skills["allowed"], "allowed skills")
        result["skills"] = {"allowed": allowed}
    if not result:
        raise ValueError("orchestration must enable at least one mechanism")
    return result


def session_options(base: dict, orch: dict | None) -> dict:
    """Extend the session create/resume options for an orchestration block."""
    if not orch:
        return dict(base)
    options = dict(base)
    tools = {"Read", "Write", "Edit", "Bash"}
    if "agents" in orch:
        tools.update(AGENT_TOOLS)
    if "workflow" in orch:
        tools.update(WORKFLOW_TOOLS)
        options["dynamicWorkflowEnabled"] = True
    if "skills" in orch:
        tools.add("Skill")
    options["toolAllowlist"] = sorted(tools)
    return options


NEUTRALIZED_BUILTINS = ("Explore",)


def agent_profiles(orch: dict) -> list[dict]:
    """Host-side subagent profiles pinned by the packet (readRuntimeConfig).

    normalizeAgentProfiles inserts the built-in general-purpose and Explore
    profiles FIRST and merges host profiles afterwards (Map last-write-wins,
    bundle offset 12712006): a host profile named Explore therefore replaces
    the built-in yolo-substituted one BEFORE any dispatch. When
    neutralize_builtins is set (default), czs synthesizes read-only shadow
    profiles for those built-ins so the forbidden types cannot execute with
    their default unrestricted surfaces.
    """
    profiles = []
    names = set()
    for profile in orch.get("agents", {}).get("profiles", []):
        entry = {
            "name": profile["name"],
            "description": profile["description"],
            "systemPrompt": profile["systemPrompt"],
            "source": "user",
            "tools": list(profile["tools"]),
            "permissionMode": "default",
            "maxTurns": profile["max_turns"],
        }
        if profile.get("skills"):
            entry["skills"] = list(profile["skills"])
        profiles.append(entry)
        names.add(profile["name"])
    if orch.get("agents", {}).get("neutralize_builtins", True):
        for builtin in NEUTRALIZED_BUILTINS:
            if builtin not in names:
                profiles.append({
                    "name": builtin,
                    "description": "Built-in neutralized by the task contract: read-only shadow.",
                    "systemPrompt": "You are a read-only probe agent. Use only the Read tool. "
                                    "Report any requested write or shell action as not performed.",
                    "source": "user",
                    "tools": ["Read"],
                    "permissionMode": "default",
                    "maxTurns": 4,
                })
    return profiles


def runtime_config_answer(orch: dict | None) -> dict | None:
    """The subagents/readRuntimeConfig response, or None when agents are off.

    The discriminated 'ready' variant requires the two override maps; both
    stay empty so no built-in or plugin agent gets a hidden model override.
    """
    if not orch or "agents" not in orch:
        return None
    return {
        "kind": "ready",
        "profiles": agent_profiles(orch),
        "builtInModelSelectionOverrides": {},
        "pluginAgentModelSelectionOverrides": {},
    }
