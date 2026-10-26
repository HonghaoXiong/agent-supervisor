"""Tri-agent (Codex / ZCode / DeepSeek Harness) read-only install probing.

P0-1 of the tri-hub compatibility strategy: one command answers "who changed
and what changed" after an upstream auto-update. Every probe is read-only —
versions come from ``--version`` style invocations that never start a model
task, contracts come from Codex's JSON-schema export and a static grep of the
ZCode bundle, and nothing writes outside ``out_dir`` plus
``<state_dir>/tri-probe``. A missing agent (dsh today) is the honest state
``absent``, never an error; a failing single probe degrades to
``status: "error"`` with a bounded message instead of taking the run down.
"""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys

from . import health
from .common import atomicwrite_json, ensure_directory
from .runtime import REVIEWED_BUNDLES

AGENTS = ("codex", "zcode", "dsh")

# Known install locations first (they are the reviewed facts), PATH lookups
# second; candidates keep every hit so a report can show both.
CODEX_KNOWN_PATHS = (
    "/Applications/ChatGPT.app/Contents/Resources/codex-cli/bin/codex",
)
DSH_HOME = Path.home() / ".dsh"

# Mach-O magic numbers (32/64-bit, either endianness) and fat/universal files.
MACHO_MAGICS = (b"\xfe\xed\xfa\xce", b"\xce\xfa\xed\xfe",
                b"\xfe\xed\xfa\xcf", b"\xcf\xfa\xed\xfe", b"\xca\xfe\xba\xbe")

# Static, read-only protocol-method grep over the ZCode bundle (never executed).
ZCODE_METHOD_RE = re.compile(rb'"((?:session|interaction|subagents|state|skills|'
                             rb'workflow)/[A-Za-z/]+)"')

CODEX_CONTRACT_FILES = (("client_methods", "ClientRequest"),
                        ("server_requests", "ServerRequest"),
                        ("notifications", "ServerNotification"))

BASELINE_RELPATH = ("tri-probe", "baseline.json")


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as stream:
        for chunk in iter(lambda: stream.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _brief(error: BaseException) -> str:
    return f"{type(error).__name__}: {error}"[:300]


def discover_codex() -> dict:
    """Locate Codex: known app paths first, then PATH; never executes it."""
    candidates: list[str] = []
    for known in CODEX_KNOWN_PATHS:
        if Path(known).is_file() and known not in candidates:
            candidates.append(known)
    which = shutil.which("codex")
    if which and which not in candidates:
        candidates.append(which)
    return {"installed": bool(candidates),
            "path": candidates[0] if candidates else None,
            "candidates": candidates}


def discover_zcode(config) -> dict:
    """The configured ZCode bundle plus the configured node runtime."""
    cli = Path(config.cli_path)
    installed = cli.is_file()
    node_found = shutil.which(config.node) is not None or Path(config.node).is_file()
    return {"installed": installed, "path": str(cli) if installed else None,
            "node": config.node, "node_found": bool(node_found),
            "candidates": [str(cli)] if installed else []}


def discover_dsh() -> dict:
    which = shutil.which("dsh")
    return {"installed": which is not None, "path": which,
            "home_dir_present": DSH_HOME.is_dir(),
            "candidates": [which] if which else []}


def _codex_binary_target(path: Path) -> tuple[Path, str]:
    """Resolve a wrapper script to the binary it execs, or hash the wrapper.

    Real installs ship a tiny ``sh`` wrapper whose ``exec`` line names a path
    relative to the wrapper directory (``$bin_dir/../CodexCLI.../codex``). We
    resolve that statically: an absolute exec target is taken verbatim, a
    ``$variable/...`` target is resolved against the wrapper's directory. A
    Mach-O file is the binary itself; anything unresolvable falls back to
    hashing the wrapper with an honest note.
    """
    try:
        with open(path, "rb") as stream:
            head = stream.read(4)
    except OSError:
        return path, "unreadable"
    if head in MACHO_MAGICS:
        return path, "mach-o"
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return path, "unreadable"
    for line in text.splitlines():
        stripped = line.strip()
        if not stripped.startswith("exec"):
            continue
        for token in re.findall(r'"([^"]+)"|(\S+)', stripped):
            value = token[0] or token[1]
            if value in ("exec",) or value.startswith(("$@", "-")):
                continue
            if value.startswith("$"):
                _, _, relative = value.partition("/")
                if not relative:
                    continue
                candidate = Path(os.path.normpath(path.parent / relative))
            elif value.startswith("/"):
                candidate = Path(value)
            else:
                continue
            if candidate.is_file():
                return candidate, "resolved-from-wrapper"
    return path, "wrapper"


def _schema_methods(schema_dir: Path, name: str) -> list[str]:
    """Method names from one exported ``<name>.json`` oneOf contract."""
    path = schema_dir / f"{name}.json"
    if not path.is_file():
        return []
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError, OSError):
        return []
    variants = data.get("oneOf") if isinstance(data, dict) else None
    methods = []
    for variant in variants if isinstance(variants, list) else []:
        if not isinstance(variant, dict):
            continue
        try:
            value = variant["properties"]["method"]["enum"][0]
        except (KeyError, IndexError, TypeError):
            continue
        if isinstance(value, str):
            methods.append(value)
    return sorted(set(methods))


def probe_codex(path, contract_dir: Path, timeout: int = 60) -> dict:
    """Version + real-binary fingerprint + exported app-server contract."""
    path = Path(path)
    contract_dir = Path(contract_dir)
    result: dict = {"agent": "codex", "installed": True, "path": str(path)}
    errors: list[str] = []
    try:
        completed = subprocess.run([str(path), "--version"], capture_output=True,
                                   text=True, timeout=timeout)
        if completed.returncode == 0:
            result["version"] = completed.stdout.strip()
        else:
            errors.append(f"--version exit {completed.returncode}: "
                          f"{(completed.stderr or completed.stdout).strip()[:200]}")
    except (subprocess.SubprocessError, OSError) as error:
        errors.append(f"--version failed: {_brief(error)}")
    try:
        target, note = _codex_binary_target(path)
        result["fingerprint"] = _sha256_file(target)
        result["fingerprint_source"] = f"{note}:{target}"
    except OSError as error:
        errors.append(f"fingerprint failed: {_brief(error)}")
    try:
        schema_dir = contract_dir / "schema"
        if schema_dir.exists():
            shutil.rmtree(schema_dir)
        schema_dir.mkdir(parents=True)
        completed = subprocess.run(
            [str(path), "app-server", "generate-json-schema", "--experimental",
             "--out", str(schema_dir)],
            capture_output=True, text=True, timeout=timeout)
        if completed.returncode != 0:
            errors.append(f"generate-json-schema exit {completed.returncode}: "
                          f"{(completed.stderr or completed.stdout).strip()[:200]}")
        else:
            contract = {"schema_dir": str(schema_dir),
                        "exported_files": len(list(schema_dir.iterdir()))}
            for key, filename in CODEX_CONTRACT_FILES:
                methods = _schema_methods(schema_dir, filename)
                contract[key] = len(methods)
                if key == "client_methods":
                    contract["methods"] = methods
            result["contract"] = contract
            try:
                contract_dir.mkdir(parents=True, exist_ok=True)
                (contract_dir / "contract.json").write_text(
                    json.dumps(contract, indent=2, ensure_ascii=False) + "\n",
                    encoding="utf-8")
            except OSError as error:
                errors.append(f"contract write failed: {_brief(error)}")
    except (subprocess.SubprocessError, OSError) as error:
        errors.append(f"generate-json-schema failed: {_brief(error)}")
    if errors:
        result["status"] = "error"
        result["error"] = "; ".join(errors)
    else:
        result["status"] = "ok"
    return result


def probe_zcode(config, contract_dir: Path, timeout: int = 60) -> dict:
    """Bundle fingerprint + pin lookup + static protocol-method contract."""
    cli = Path(config.cli_path)
    contract_dir = Path(contract_dir)
    result: dict = {"agent": "zcode", "installed": True, "path": str(cli)}
    errors: list[str] = []
    data = b""
    try:
        data = cli.read_bytes()
    except OSError as error:
        errors.append(f"bundle read failed: {_brief(error)}")
    if data:
        result["fingerprint"] = hashlib.sha256(data).hexdigest()
        pin = REVIEWED_BUNDLES.get(result["fingerprint"])
        result["pinned"] = pin is not None
        result["pin"] = ({"reviewed": pin.get("reviewed"),
                          "cli_version": pin.get("cli_version")} if pin else None)
        methods = sorted({match.decode("ascii", "replace")
                          for match in ZCODE_METHOD_RE.findall(data)})
        result["contract"] = {"methods": methods, "count": len(methods)}
        try:
            contract_dir.mkdir(parents=True, exist_ok=True)
            (contract_dir / "contract.json").write_text(
                json.dumps(result["contract"], indent=2, ensure_ascii=False) + "\n",
                encoding="utf-8")
        except OSError as error:
            errors.append(f"contract write failed: {_brief(error)}")
    try:
        completed = subprocess.run([config.node, str(cli), "--version"],
                                   capture_output=True, text=True, timeout=timeout)
        if completed.returncode == 0:
            result["version"] = completed.stdout.strip()
        else:
            errors.append(f"--version exit {completed.returncode}: "
                          f"{(completed.stderr or completed.stdout).strip()[:200]}")
    except (subprocess.SubprocessError, OSError) as error:
        errors.append(f"--version failed: {_brief(error)}")
    if errors:
        result["status"] = "error"
        result["error"] = "; ".join(errors)
    else:
        result["status"] = "ok"
    return result


def probe_dsh(path) -> dict:
    """A missing dsh is the honest absent state, never an error."""
    if path is None:
        return {"agent": "dsh", "installed": False, "status": "absent"}
    path = Path(path)
    result: dict = {"agent": "dsh", "installed": True, "path": str(path)}
    try:
        result["fingerprint"] = _sha256_file(path)
    except OSError as error:
        result["fingerprint_error"] = _brief(error)
    try:
        completed = subprocess.run([str(path), "--version"], capture_output=True,
                                   text=True, timeout=30)
        result["version"] = (completed.stdout or completed.stderr).strip()[:200]
        if completed.returncode == 0:
            result["status"] = "ok"
        else:
            result["status"] = "error"
            result["error"] = f"--version exit {completed.returncode}"
    except (subprocess.SubprocessError, OSError) as error:
        result["status"] = "error"
        result["error"] = _brief(error)
    return result


async def _hub_handshake(server_argv: list[str]) -> dict:
    from mcp import ClientSession, StdioServerParameters
    from mcp.client.stdio import stdio_client

    # Belt and braces for running from a source tree: make this package
    # importable in the spawned server even without an editable install.
    package_root = str(Path(__file__).resolve().parents[1])
    existing = os.environ.get("PYTHONPATH")
    env = {"PYTHONPATH": f"{package_root}{os.pathsep}{existing}" if existing else package_root}
    parameters = StdioServerParameters(command=server_argv[0], args=list(server_argv[1:]),
                                       env=env)
    async with stdio_client(parameters) as (read, write):
        async with ClientSession(read, write) as session:
            initialized = await session.initialize()
            tools = await session.list_tools()
            return {"status": "ok",
                    "server_name": initialized.server_info.name,
                    "server_version": getattr(initialized.server_info, "version", None),
                    "protocol_version": initialized.protocol_version,
                    "tools": sorted(tool.name for tool in tools.tools)}


def probe_hub_mcp(server_argv: list[str], timeout: int = 30) -> dict:
    """initialize + tools/list over stdio against one MCP server (smoke)."""
    if not isinstance(server_argv, (list, tuple)) or not server_argv \
            or not all(isinstance(item, str) and item for item in server_argv):
        return {"status": "error", "error": "server_argv must be a nonempty list of strings"}
    try:
        detail = asyncio.run(asyncio.wait_for(_hub_handshake(list(server_argv)),
                                              timeout=timeout))
    except asyncio.TimeoutError:
        return {"status": "error",
                "error": f"hub MCP handshake exceeded {timeout}s",
                "server_argv": list(server_argv)}
    except Exception as error:  # spawn failures, protocol errors, ImportError
        return {"status": "error", "error": _brief(error),
                "server_argv": list(server_argv)}
    detail["server_argv"] = list(server_argv)
    return detail


def _agents_of(value: dict | None) -> dict:
    if not isinstance(value, dict):
        return {}
    agents = value.get("agents")
    return agents if isinstance(agents, dict) else value


def _agent_diff(current: dict, baseline: dict | None) -> dict:
    """One agent's drift: version, fingerprint, pin, contract method set.

    An errored probe is reported as itself, never as phantom drift: its
    missing fields would otherwise masquerade as removed methods/versions
    while the error status already flags the run for attention.
    """
    if baseline is None or (not current.get("installed")
                            and not (baseline or {}).get("installed")):
        return {"changed": False, "diff": {}}
    if current.get("status") == "error":
        return {"changed": False, "diff": {"probe_error": True}}
    diff: dict = {}
    for key in ("installed", "version", "fingerprint", "pinned"):
        now, was = current.get(key), (baseline or {}).get(key)
        if now != was:
            diff[key] = {"was": was, "now": now}
    current_methods = set((current.get("contract") or {}).get("methods") or [])
    baseline_methods = set(((baseline or {}).get("contract") or {}).get("methods") or [])
    if current_methods != baseline_methods:
        diff["methods"] = {"added": sorted(current_methods - baseline_methods),
                           "removed": sorted(baseline_methods - current_methods)}
    return {"changed": bool(diff), "diff": diff}


def compare_with_baseline(current: dict, baseline: dict | None) -> dict:
    """Per-agent comparison; absent-on-both is not a change."""
    current_agents = _agents_of(current)
    baseline_agents = _agents_of(baseline)
    return {agent: _agent_diff(current_agents.get(agent) or {"installed": False},
                               baseline_agents.get(agent))
            for agent in AGENTS}


def _default_hub_argv(out_dir: Path) -> list[str]:
    return [sys.executable, "-m", "zcode_supervisor.mcp_readonly",
            "--evidence-root", str(out_dir)]


def run_probe_all(config, out_dir: Path, hub_server_argv=None,
                  write_baseline: bool = False) -> dict:
    """Probe all three agents, export contracts, compare the stored baseline."""
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    generated = _utc_now()

    def absent(name: str) -> dict:
        return {"agent": name, "installed": False, "status": "absent"}

    agents: dict[str, dict] = {}

    codex_found = discover_codex()
    if codex_found["path"] is None:
        agents["codex"] = absent("codex")
    else:
        try:
            agents["codex"] = probe_codex(codex_found["path"],
                                          out_dir / "contracts" / "codex")
        except Exception as error:  # a single probe never sinks the run
            agents["codex"] = {"agent": "codex", "installed": True,
                               "status": "error", "error": _brief(error)}

    zcode_found = discover_zcode(config)
    if not zcode_found["installed"]:
        agents["zcode"] = absent("zcode")
    else:
        try:
            agents["zcode"] = probe_zcode(config, out_dir / "contracts" / "zcode")
        except Exception as error:
            agents["zcode"] = {"agent": "zcode", "installed": True,
                               "status": "error", "error": _brief(error)}

    dsh_found = discover_dsh()
    try:
        agents["dsh"] = probe_dsh(dsh_found["path"])
    except Exception as error:
        agents["dsh"] = {"agent": "dsh", "installed": dsh_found["installed"],
                         "status": "error", "error": _brief(error)}

    baseline_path = Path(config.state_dir).joinpath(*BASELINE_RELPATH)
    baseline_created = baseline_updated = False
    stored: dict | None = None
    try:
        if baseline_path.is_file():
            stored = json.loads(baseline_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, UnicodeDecodeError, OSError):
        stored = None  # an unreadable baseline reports as absent, never crashes
    if stored is None:
        ensure_directory(baseline_path.parent)
        atomicwrite_json(baseline_path, {"generated": generated, "agents": agents})
        baseline_created = True
        comparison = compare_with_baseline(agents, None)
    else:
        comparison = compare_with_baseline(agents, stored)
        if write_baseline:
            ensure_directory(baseline_path.parent)
            atomicwrite_json(baseline_path, {"generated": generated, "agents": agents})
            baseline_updated = True

    hub_argv = list(hub_server_argv) if hub_server_argv else _default_hub_argv(out_dir)
    hub = probe_hub_mcp(hub_argv)

    result = {
        "generated": generated,
        "agents": agents,
        "comparison": comparison,
        "baseline": {"path": str(baseline_path), "created": baseline_created,
                     "updated": baseline_updated},
        "hub_mcp": hub,
        "overall": "ok" if (not any(entry.get("changed") for entry in comparison.values())
                            and not any(probe.get("status") == "error"
                                        for probe in agents.values())
                            and hub.get("status") == "ok") else "attention",
        "output": str(out_dir / "tri-probe.json"),
    }
    (out_dir / "tri-probe.json").write_text(
        json.dumps(result, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return result


def revalidate(config, out_dir: Path, hub_server_argv=None,
               write_baseline: bool = False) -> dict:
    """One probe pass published as ``<state_dir>/hub/health.json``.

    Companion of ``czs probe-all --revalidate``: run the full read-only probe
    (baseline handling identical to :func:`run_probe_all`; ``write_baseline``
    is the human's explicit acceptance of a drifted baseline), evaluate the
    fresh result into a health document, and atomically publish it where the
    hub server's ``health``/``delegate`` tools read it.

    Returns ``{"probe": <run_probe_all result>, "health": <health document>,
    "all_installed_enabled": bool}`` where the flag is True only when every
    non-absent worker entry is enabled (fail-closed: changed/error/unknown all
    count as disabled; an absent worker such as dsh is not counted either way).
    """
    probe = run_probe_all(config, out_dir, hub_server_argv=hub_server_argv,
                          write_baseline=write_baseline)
    document = health.evaluate(probe, probe_ref=str(Path(out_dir) / "tri-probe.json"))
    health.write_health(config.state_dir, document)
    installed_enabled = all(entry.get("enabled") is True
                            for entry in document["workers"].values()
                            if entry.get("status") != "absent")
    return {"probe": probe, "health": document,
            "all_installed_enabled": installed_enabled}
