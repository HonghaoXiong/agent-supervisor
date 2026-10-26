"""Human-registered delegation contracts for the three-way orchestration hub.

Core principle: MCP exposure is not approval. A delegate call is only executed
against a contract that a human previously registered on this host (see
:func:`register_contract`); the hub never invents or widens authority itself.

The contract freezes the delegation shape shared by both configured workers.
Each worker has two modes (optional ``mode``, defaulting to the first):

* ``zcode``: ``"cold"`` (one app-server process per task via the czs protocol
  executor) or ``"resident"`` (sessions on one shared resident process);
* ``codex``: ``"exec"`` (``codex exec -s read-only`` single shot) or
  ``"app-server"`` (resident app-server with approval callbacks brokered
  through the contract Authority).

Write paths (``allowed_files`` / ``commands``) follow live-verified worker
semantics (evidence ``out/tri-agent-p1-20261026``):

* codex ``exec`` has **no approval callback** — under a workspace-write
  sandbox, sandbox-contained operations execute without any approval — so
  ``allowed_files`` and ``commands`` must stay empty there;
* zcode brokers ``allowed_files`` and exact ``commands`` in both modes;
* codex ``app-server`` + ``approvalPolicy="untrusted"`` sends per-command
  approval callbacks, and file approvals carry no per-file schema field
  (the bridge declines them whole), so writes travel through approved exact
  commands under ``sandbox: "workspace-write"`` — a write task in that mode
  must therefore declare ``"workspace-write"``;
* ``output_schema`` is the exec entry point's ``--output-schema`` mechanism:
  required for codex ``exec``, refused for codex ``app-server`` (which
  returns the final_message text), and unconsumed by zcode;
* credentials are never part of a contract: the hub holds no auth.json, no API
  key, and reads none — each worker keeps its own credentials.

Validation collects every problem before failing (one call, the full list).
"""

from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path

from .common import read_regular, write_json

__all__ = [
    "CODEX_MODES",
    "OPTIONAL",
    "REQUIRED",
    "SANDBOX_MODES",
    "WORKERS",
    "ZCODE_MODES",
    "ContractError",
    "contract_digest",
    "load_contract",
    "register_contract",
    "validate_contract",
]


class ContractError(ValueError):
    """Contract rejection; the message lists every problem found at once."""


REQUIRED = (
    "contract_id",
    "worker",
    "workspace",
    "prompt",
    "allowed_files",
    "commands",
    "model",
    "max_timeout_ms",
    "budget",
)
WORKERS = ("zcode", "codex")
OPTIONAL = ("snapshots", "output_schema", "mode", "sandbox")

ZCODE_MODES = ("cold", "resident")
CODEX_MODES = ("exec", "app-server")
SANDBOX_MODES = ("read-only", "workspace-write")

MAX_TIMEOUT_MS_CEILING = 3_600_000

_CONTRACT_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")
_COMMAND_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]*$")
_COMMAND_KEYS = {"id", "command", "max_uses", "max_timeout_ms", "required"}

# Live-verified fact (out/tri-agent-p1-20261026): the codex exec entry point
# has no approval callback — under a workspace-write sandbox, sandbox-contained
# operations execute without any approval. Write authority therefore cannot
# exist in exec mode; app-server mode brokers every command through the
# contract Authority instead.
_CODEX_EXEC_NO_BROKER = (
    "codex exec mode has no approval callback: under a workspace-write sandbox "
    "sandbox-contained operations execute without any approval (live-verified "
    '2026-10-26), so {field} must be empty; register write tasks as codex mode '
    '"app-server"')


def canonical_json(value) -> str:
    """Deterministic JSON text used for digests and identity comparisons."""
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False)


def contract_digest(contract) -> str:
    """sha256 over the canonical JSON form of the contract."""
    return hashlib.sha256(canonical_json(contract).encode("utf-8")).hexdigest()


def _schema_object_node_problems(node, label: str) -> list[str]:
    """Strict-mode problems for one schema node and everything nested below it.

    Every schema node that declares ``"type": "object"`` (at any depth, also
    inside ``items`` / ``anyOf`` / ``oneOf`` / ``allOf``) must set
    ``additionalProperties: false`` and list every property of its
    ``properties`` in ``required``. This mirrors what the codex service itself
    enforces (OpenAI strict mode): a schema missing it is rejected server-side
    with ``invalid_json_schema`` and the turn fails, so the contract layer
    reports it up front with a precise, fixable message.
    """
    problems: list[str] = []
    if not isinstance(node, dict):
        return [f"{label}: schema fragments must be objects"]

    if node.get("type") == "object":
        properties = node.get("properties")
        if not isinstance(properties, dict) or not properties:
            problems.append(f"{label}: object schema must define a non-empty 'properties' object")
            properties = {}
        else:
            if node.get("additionalProperties") is not False:
                problems.append(
                    f"{label}: must set \"additionalProperties\": false "
                    "(codex strict mode rejects the schema otherwise)")
            required = node.get("required")
            names = {r for r in required if isinstance(r, str)} if isinstance(required, list) else set()
            uncovered = sorted(set(properties) - names)
            if uncovered:
                problems.append(
                    f"{label}: 'required' must cover every property "
                    f"(missing: {', '.join(uncovered)})")
        for name, sub in sorted(properties.items()):
            problems.extend(_schema_object_node_problems(sub, f"{label}.{name}"))

    items = node.get("items")
    if isinstance(items, dict):
        problems.extend(_schema_object_node_problems(items, f"{label}[]"))
    elif isinstance(items, list):
        for index, sub in enumerate(items):
            problems.extend(_schema_object_node_problems(sub, f"{label}[{index}]"))
    for combinator in ("anyOf", "oneOf", "allOf"):
        variants = node.get(combinator)
        if isinstance(variants, list):
            for index, sub in enumerate(variants):
                problems.extend(_schema_object_node_problems(
                    sub, f"{label}.{combinator}[{index}]"))
    return problems


def _output_schema_problems(schema, label: str) -> list[str]:
    """Strict-mode checks: the top level itself must be an object schema."""
    if not isinstance(schema, dict):
        return [f"{label} must be a JSON Schema object"]
    problems: list[str] = []
    if schema.get("type") != "object":
        problems.append(f"{label} must declare \"type\": \"object\" at the top level")
    problems.extend(_schema_object_node_problems(schema, label))
    return problems


def _positive_int(value) -> bool:
    return type(value) is int and value > 0


def validate_contract(contract: dict) -> dict:
    """Validate one delegation contract; raise ContractError listing everything.

    Filesystem facts (the workspace directory exists and is not a symlink) are
    checked here too, so a contract can never be registered against a path
    that only materializes later.
    """
    if not isinstance(contract, dict):
        raise ContractError("contract must be a JSON object")

    problems: list[str] = []
    missing = [key for key in REQUIRED if key not in contract]
    problems.extend(f"missing required field: {key}" for key in missing)
    unknown = sorted(set(contract) - set(REQUIRED) - set(OPTIONAL))
    if unknown:
        problems.append(f"unknown field(s) rejected: {', '.join(unknown)}")

    # --- contract_id ---------------------------------------------------
    value = contract.get("contract_id")
    if isinstance(value, str) and not _CONTRACT_ID.match(value):
        problems.append("contract_id must match ^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$")

    # --- worker --------------------------------------------------------
    value = contract.get("worker")
    if value == "dsh":
        problems.append("worker 'dsh' is absent on this host")
    elif value not in WORKERS:
        problems.append(f"worker must be one of {WORKERS} (dsh is not installed on this host)")
    worker = contract.get("worker")

    # --- mode (worker execution mode; default keeps old-contract meaning) ---
    if worker == "zcode":
        modes, default_mode = ZCODE_MODES, "cold"
    else:
        modes, default_mode = CODEX_MODES, "exec"
    value = contract.get("mode")
    if "mode" in contract and (not isinstance(value, str) or value not in modes):
        problems.append(
            f"mode must be one of {modes} for worker {worker!r} "
            f"(default {default_mode!r})")
    effective_mode = (value if isinstance(value, str) and value in modes
                      else default_mode)

    # --- sandbox (codex only; the czs packet path has no sandbox dial) ----
    value = contract.get("sandbox")
    if "sandbox" in contract:
        if worker != "codex":
            problems.append(
                "sandbox applies to codex workers only; zcode brokering is "
                "governed by the czs packet itself")
        if not isinstance(value, str) or value not in SANDBOX_MODES:
            problems.append(f"sandbox must be one of {SANDBOX_MODES}")
    effective_sandbox = (value if isinstance(value, str) and value in SANDBOX_MODES
                         else "read-only")

    # --- workspace -----------------------------------------------------
    value = contract.get("workspace")
    if isinstance(value, str) and value:
        path = Path(value)
        if not path.is_absolute():
            problems.append("workspace must be an absolute path")
        elif path.is_symlink():
            problems.append("workspace must not be a symlink")
        elif not path.is_dir():
            problems.append(f"workspace must be a real directory: {value}")
    elif "workspace" in contract:
        problems.append("workspace must be a non-empty absolute path string")

    # --- prompt --------------------------------------------------------
    value = contract.get("prompt")
    if "prompt" in contract and (not isinstance(value, str) or not value.strip()):
        problems.append("prompt must be a non-empty string")

    # --- allowed_files (write path) -------------------------------------
    value = contract.get("allowed_files")
    write_task = False
    if "allowed_files" in contract:
        if not isinstance(value, list):
            problems.append("allowed_files must be a list")
        elif value:
            write_task = True
            for name in value:
                if not isinstance(name, str) or not name.strip():
                    problems.append(
                        "allowed_files entries must be non-empty strings")
                elif (name.startswith("/") or ".." in Path(name).parts
                        or Path(name).as_posix() != name):
                    problems.append(
                        f"allowed_files entries must be normalized "
                        f"workspace-relative paths: {name!r}")
            if worker == "codex" and effective_mode == "exec":
                problems.append(_CODEX_EXEC_NO_BROKER.format(field="allowed_files"))
            elif worker not in WORKERS:
                # Unknown/uninstalled workers get no write path at all.
                problems.append(
                    f"allowed_files must be empty for worker {worker!r} "
                    "(no write path exists for this worker)")

    # --- commands ------------------------------------------------------
    value = contract.get("commands")
    if "commands" in contract:
        if not isinstance(value, list):
            problems.append("commands must be a list of command specifications")
        else:
            write_task = write_task or bool(value)
            for position, command in enumerate(value):
                problems.extend(
                    _command_problems(command, f"commands[{position}]"))
            if worker == "codex" and effective_mode == "exec" and value:
                problems.append(_CODEX_EXEC_NO_BROKER.format(field="commands"))
            elif worker not in WORKERS and value:
                problems.append(
                    f"commands must be empty for worker {worker!r} "
                    "(no approval broker exists for this worker)")

    # --- codex app-server write tasks need the workspace-write sandbox ----
    if (worker == "codex" and effective_mode == "app-server" and write_task
            and effective_sandbox != "workspace-write"):
        problems.append(
            'codex app-server write tasks (allowed_files or commands non-empty) '
            'must declare sandbox "workspace-write": under "read-only" the '
            'sandbox itself blocks the writes the contract expects to broker '
            "(read-only app-server tasks keep the default read-only sandbox)")

    # --- model ---------------------------------------------------------
    value = contract.get("model")
    if "model" in contract:
        if worker == "zcode":
            problems.extend(_zcode_model_problems(value))
        elif worker == "codex":
            if value is not None:
                if (not isinstance(value, dict) or set(value) != {"model"}
                        or not isinstance(value.get("model"), str)
                        or not value.get("model", "").strip()):
                    problems.append('codex model must be {"model": "<name>"} or null')
        elif not isinstance(value, dict):
            problems.append("model must be an object for the chosen worker")

    # --- max_timeout_ms --------------------------------------------------
    value = contract.get("max_timeout_ms")
    if "max_timeout_ms" in contract:
        if isinstance(value, bool) or not isinstance(value, int):
            problems.append("max_timeout_ms must be an integer")
        elif not 1 <= value <= MAX_TIMEOUT_MS_CEILING:
            problems.append(
                f"max_timeout_ms must be within 1..{MAX_TIMEOUT_MS_CEILING}")

    # --- budget (recorded, not enforced, in P0) -------------------------
    value = contract.get("budget")
    if "budget" in contract:
        if not isinstance(value, dict):
            problems.append("budget must be an object")
        else:
            extra = sorted(set(value) - {"max_tokens", "max_cost_usd", "note"})
            if extra:
                problems.append(f"budget has unknown key(s): {', '.join(extra)}")
            tokens = value.get("max_tokens")
            if tokens is not None and (isinstance(tokens, bool)
                                       or not isinstance(tokens, int) or tokens < 0):
                problems.append("budget.max_tokens must be a non-negative integer or null")
            cost = value.get("max_cost_usd")
            if cost is not None and (isinstance(cost, bool)
                                     or not isinstance(cost, (int, float)) or cost < 0):
                problems.append("budget.max_cost_usd must be a non-negative number or null")
            note = value.get("note")
            if note is not None and not isinstance(note, str):
                problems.append("budget.note must be a string")

    # --- optional: snapshots --------------------------------------------
    value = contract.get("snapshots")
    if "snapshots" in contract:
        if not isinstance(value, list) or not all(
                isinstance(item, str) and item.strip() for item in value):
            problems.append("snapshots must be a list of non-empty strings")

    # --- optional / codex-exec-required: output_schema -------------------
    value = contract.get("output_schema")
    if "output_schema" in contract:
        if worker == "codex" and effective_mode == "app-server":
            problems.append(
                "output_schema is exec-only (codex exec recaptures the final "
                "answer through --output-schema); app-server mode returns the "
                "final_message text and must not carry output_schema")
        elif value is None:
            problems.append("output_schema must be a JSON Schema object, not null")
        else:
            problems.extend(_output_schema_problems(value, "output_schema"))
    elif worker == "codex" and effective_mode == "exec":
        problems.append(
            "codex contract requires output_schema (strict object schema; "
            "the final answer is recaptured through --output-schema)")

    if problems:
        raise ContractError("; ".join(problems))
    return contract


def _command_problems(command, label: str) -> list[str]:
    """One czs command specification (same shape as the task packet)."""
    if not isinstance(command, dict):
        return [f"{label}: must be an object"]
    problems: list[str] = []
    extra = sorted(set(command) - _COMMAND_KEYS)
    if extra:
        problems.append(f"{label}: unknown key(s): {', '.join(extra)}")
    identifier = command.get("id")
    if not isinstance(identifier, str) or not _COMMAND_ID.match(identifier):
        problems.append(f"{label}: id must match ^[A-Za-z0-9][A-Za-z0-9_.-]*$")
    text = command.get("command")
    if not isinstance(text, str) or not text.strip() or "\x00" in text:
        problems.append(f"{label}: command must be a non-empty string without NUL")
    uses = command.get("max_uses", 1)
    if isinstance(uses, bool) or not isinstance(uses, int) or uses <= 0:
        problems.append(f"{label}: max_uses must be a positive integer")
    timeout = command.get("max_timeout_ms", 120000)
    if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) \
            or not timeout > 0:
        problems.append(f"{label}: max_timeout_ms must be positive and finite")
    required = command.get("required", True)
    if type(required) is not bool:
        problems.append(f"{label}: required must be a boolean")
    return problems


def _zcode_model_problems(value) -> list[str]:
    if not isinstance(value, dict):
        return ["zcode model must be {\"providerId\", \"modelId\", optional \"options\"}"]
    problems: list[str] = []
    extra = sorted(set(value) - {"providerId", "modelId", "options"})
    if extra:
        problems.append(f"zcode model has unknown key(s): {', '.join(extra)}")
    for key in ("providerId", "modelId"):
        text = value.get(key)
        if not isinstance(text, str) or not text.strip() or text != text.strip():
            problems.append(f"zcode model.{key} must be a non-empty trimmed string")
    options = value.get("options")
    if options is not None:
        if (not isinstance(options, dict) or set(options) != {"reasoningLevel"}
                or not isinstance(options.get("reasoningLevel"), str)
                or not options["reasoningLevel"].strip()
                or options["reasoningLevel"] != options["reasoningLevel"].strip()):
            problems.append(
                "zcode model.options must contain only a non-empty reasoningLevel string")
    return problems


def contracts_dir(state_dir: Path) -> Path:
    return Path(state_dir) / "hub" / "contracts"


def register_contract(state_dir: Path, contract_path: Path) -> dict:
    """Validate a human-supplied contract file and freeze it into the hub.

    The stored copy is written atomically under ``state_dir/hub/contracts``.
    Registering the exact same contract again is idempotent; registering a
    different contract under an already-used id is refused (never overwritten).
    """
    contract_path = Path(contract_path)
    try:
        contract = json.loads(read_regular(contract_path))
    except (OSError, ValueError) as error:
        raise ContractError(f"contract file unreadable: {error}") from error
    validate_contract(contract)

    digest = contract_digest(contract)
    destination = contracts_dir(state_dir) / f"{contract['contract_id']}.json"
    if destination.exists():
        try:
            stored = json.loads(read_regular(destination))
            same = contract_digest(stored) == digest
        except (OSError, ValueError) as error:
            raise ContractError(
                f"stored contract is unreadable ({error}); "
                "resolve the file under the hub state directory before re-registering"
            ) from error
        if not same:
            raise ContractError(
                f"a different contract is already registered as "
                f"{contract['contract_id']!r}; refusing to overwrite "
                "(choose a new contract_id or remove the old registration by hand)")
        return {"contract_id": contract["contract_id"], "sha256": digest,
                "path": str(destination), "idempotent": True}

    write_json(destination, contract)
    return {"contract_id": contract["contract_id"], "sha256": digest,
            "path": str(destination), "idempotent": False}


def load_contract(state_dir: Path, contract_id) -> dict:
    """Load and re-validate a registered contract by id."""
    if not isinstance(contract_id, str) or not _CONTRACT_ID.match(contract_id):
        raise ContractError("unknown contract")
    path = contracts_dir(state_dir) / f"{contract_id}.json"
    if not path.is_file():
        raise ContractError(
            f"unknown contract: {contract_id!r} is not registered under the hub "
            "state directory; MCP exposure is not approval — a human must "
            "register the full contract first via register_contract")
    try:
        contract = json.loads(read_regular(path))
    except (OSError, ValueError) as error:
        raise ContractError(f"registered contract unreadable: {error}") from error
    return validate_contract(contract)
