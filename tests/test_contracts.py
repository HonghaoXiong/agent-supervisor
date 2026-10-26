"""Contract validation/registration tests: one-shot full error lists, P0 scope,
strict codex output schemas, and frozen registration semantics."""

import json
import tempfile
import unittest
from pathlib import Path

from zcode_supervisor import contracts
from zcode_supervisor.contracts import ContractError


def strict_schema():
    return {
        "type": "object",
        "properties": {
            "answer": {"type": "string"},
            "confidence": {"type": "number"},
        },
        "required": ["answer", "confidence"],
        "additionalProperties": False,
    }


def codex_contract(workspace: str, **overrides):
    contract = {
        "contract_id": "demo-codex-1",
        "worker": "codex",
        "workspace": workspace,
        "prompt": "Read TASK.md and answer strictly.",
        "allowed_files": [],
        "commands": [],
        "model": {"model": "gpt-5-codex"},
        "max_timeout_ms": 120_000,
        "budget": {"max_tokens": None, "max_cost_usd": 1.0, "note": "P0"},
        "output_schema": strict_schema(),
    }
    contract.update(overrides)
    return contract


def zcode_contract(workspace: str, **overrides):
    contract = {
        "contract_id": "demo-zcode-1",
        "worker": "zcode",
        "workspace": workspace,
        "prompt": "Read TASK.md and report.",
        "allowed_files": [],
        "commands": [],
        "model": {"providerId": "glm", "modelId": "glm-5.3",
                  "options": {"reasoningLevel": "high"}},
        "max_timeout_ms": 300_000,
        "budget": {"max_tokens": 200000, "max_cost_usd": None, "note": "P0"},
    }
    contract.update(overrides)
    return contract


class ValidateContractTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.workspace = Path(self.temporary.name) / "workspace"
        self.workspace.mkdir()
        self.ws = str(self.workspace)

    def test_valid_codex_and_zcode_contracts_pass(self):
        contracts.validate_contract(codex_contract(self.ws))
        contracts.validate_contract(zcode_contract(self.ws))

    def test_missing_fields_are_all_reported_at_once(self):
        with self.assertRaises(ContractError) as caught:
            contracts.validate_contract({"contract_id": "x1"})
        message = str(caught.exception)
        for field in ("worker", "workspace", "prompt", "allowed_files",
                      "commands", "model", "max_timeout_ms", "budget"):
            self.assertIn(field, message)
        # contract_id is present, so it must not be reported as missing
        self.assertNotIn("missing required field: contract_id", message)

    def test_unknown_fields_are_rejected_and_listed(self):
        contract = codex_contract(self.ws, extra_thing=1)
        with self.assertRaises(ContractError) as caught:
            contracts.validate_contract(contract)
        self.assertIn("extra_thing", str(caught.exception))

    def test_codex_exec_rejects_allowed_files_no_approval_callback(self):
        contract = codex_contract(self.ws, allowed_files=["REPORT.md"])
        with self.assertRaises(ContractError) as caught:
            contracts.validate_contract(contract)
        message = str(caught.exception)
        self.assertIn("codex exec mode has no approval callback", message)
        self.assertIn("allowed_files must be empty", message)
        self.assertIn('"app-server"', message)

    def test_dsh_worker_is_rejected_as_absent(self):
        contract = codex_contract(self.ws, worker="dsh")
        with self.assertRaises(ContractError) as caught:
            contracts.validate_contract(contract)
        self.assertIn("worker 'dsh' is absent on this host", str(caught.exception))

    def test_unknown_worker_is_rejected(self):
        with self.assertRaises(ContractError):
            contracts.validate_contract(codex_contract(self.ws, worker="claude"))

    def test_workspace_must_be_absolute_real_directory(self):
        with self.assertRaises(ContractError) as caught:
            contracts.validate_contract(codex_contract("relative/ws"))
        self.assertIn("absolute", str(caught.exception))
        with self.assertRaises(ContractError) as caught:
            contracts.validate_contract(
                codex_contract(str(Path(self.temporary.name) / "missing")))
        self.assertIn("real directory", str(caught.exception))
        link = self.workspace.parent / "ws-link"
        link.symlink_to(self.workspace)
        self.addCleanup(link.unlink)
        with self.assertRaises(ContractError) as caught:
            contracts.validate_contract(codex_contract(str(link)))
        self.assertIn("symlink", str(caught.exception))

    def test_codex_exec_rejects_commands_no_approval_callback(self):
        command = {"id": "run-tests", "command": "/bin/true",
                   "max_uses": 1, "max_timeout_ms": 120000, "required": True}
        contract = codex_contract(self.ws, commands=[command])
        with self.assertRaises(ContractError) as caught:
            contracts.validate_contract(contract)
        message = str(caught.exception)
        self.assertIn("codex exec mode has no approval callback", message)
        self.assertIn("commands must be empty", message)
        # the same command list is fine for a zcode worker (any mode)
        contracts.validate_contract(zcode_contract(self.ws, commands=[command]))
        contracts.validate_contract(
            zcode_contract(self.ws, mode="resident", commands=[command]))
        # and for codex app-server mode under the workspace-write sandbox
        appserver = codex_contract(self.ws, commands=[command],
                                   mode="app-server", sandbox="workspace-write")
        del appserver["output_schema"]
        contracts.validate_contract(appserver)

    def test_command_specification_shape_is_validated(self):
        bad = [{"id": "bad id!", "command": "/bin/true"}]
        with self.assertRaises(ContractError) as caught:
            contracts.validate_contract(zcode_contract(self.ws, commands=bad))
        self.assertIn("commands[0]", str(caught.exception))
        with self.assertRaises(ContractError):
            contracts.validate_contract(
                zcode_contract(self.ws, commands=[{"command": "/bin/true"}]))

    def test_model_shapes_per_worker(self):
        with self.assertRaises(ContractError):
            contracts.validate_contract(
                zcode_contract(self.ws, model={"providerId": "glm"}))
        with self.assertRaises(ContractError):
            contracts.validate_contract(
                zcode_contract(self.ws, model={"model": "gpt-5-codex"}))
        with self.assertRaises(ContractError):
            contracts.validate_contract(
                zcode_contract(self.ws,
                               model={"providerId": "glm", "modelId": "m",
                                      "options": {"depth": 3}}))
        with self.assertRaises(ContractError):
            contracts.validate_contract(
                codex_contract(self.ws, model={"model": "  "}))
        # codex may leave the model unset; zcode may omit options
        contracts.validate_contract(codex_contract(self.ws, model=None))
        contracts.validate_contract(
            zcode_contract(self.ws,
                           model={"providerId": "glm", "modelId": "m"}))

    def test_max_timeout_ms_bounds(self):
        with self.assertRaises(ContractError):
            contracts.validate_contract(codex_contract(self.ws, max_timeout_ms=0))
        with self.assertRaises(ContractError):
            contracts.validate_contract(
                codex_contract(self.ws, max_timeout_ms=3_600_001))
        with self.assertRaises(ContractError):
            contracts.validate_contract(codex_contract(self.ws, max_timeout_ms=True))
        contracts.validate_contract(
            codex_contract(self.ws, max_timeout_ms=3_600_000))

    def test_budget_shape(self):
        with self.assertRaises(ContractError):
            contracts.validate_contract(codex_contract(self.ws, budget=[]))
        with self.assertRaises(ContractError) as caught:
            contracts.validate_contract(
                codex_contract(self.ws,
                               budget={"max_tokens": -1, "currency": "USD"}))
        message = str(caught.exception)
        self.assertIn("max_tokens", message)
        self.assertIn("currency", message)

    def test_contract_id_pattern(self):
        with self.assertRaises(ContractError):
            contracts.validate_contract(codex_contract(self.ws, contract_id="-bad"))
        with self.assertRaises(ContractError):
            contracts.validate_contract(codex_contract(self.ws, contract_id="a" * 65))
        contracts.validate_contract(codex_contract(self.ws, contract_id="a" * 64))


class StrictOutputSchemaTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        workspace = Path(self.temporary.name) / "ws"
        workspace.mkdir()
        self.ws = str(workspace)

    def test_codex_requires_output_schema(self):
        contract = codex_contract(self.ws)
        del contract["output_schema"]
        with self.assertRaises(ContractError) as caught:
            contracts.validate_contract(contract)
        self.assertIn("requires output_schema", str(caught.exception))

    def test_missing_additional_properties_false_is_rejected(self):
        schema = strict_schema()
        del schema["additionalProperties"]
        with self.assertRaises(ContractError) as caught:
            contracts.validate_contract(codex_contract(self.ws, output_schema=schema))
        self.assertIn("additionalProperties", str(caught.exception))

    def test_top_level_must_be_object(self):
        schema = {"type": "array", "items": {"type": "string"}}
        with self.assertRaises(ContractError) as caught:
            contracts.validate_contract(codex_contract(self.ws, output_schema=schema))
        self.assertIn("object", str(caught.exception))

    def test_required_must_cover_every_property(self):
        schema = strict_schema()
        schema["required"] = ["answer"]
        with self.assertRaises(ContractError) as caught:
            contracts.validate_contract(codex_contract(self.ws, output_schema=schema))
        self.assertIn("confidence", str(caught.exception))

    def test_nested_objects_are_checked_recursively(self):
        schema = strict_schema()
        schema["properties"]["details"] = {
            "type": "object",
            "properties": {"path": {"type": "string"}},
            "required": ["path"],
            "additionalProperties": False,
        }
        schema["required"] = ["answer", "confidence", "details"]
        contracts.validate_contract(codex_contract(self.ws, output_schema=schema))
        # nested object without additionalProperties: false must be rejected
        broken = json.loads(json.dumps(schema))
        broken["properties"]["details"]["additionalProperties"] = True
        with self.assertRaises(ContractError) as caught:
            contracts.validate_contract(codex_contract(self.ws, output_schema=broken))
        self.assertIn("details", str(caught.exception))
        # nested object whose required misses a property must be rejected
        missing = json.loads(json.dumps(schema))
        del missing["properties"]["details"]["required"]
        with self.assertRaises(ContractError) as caught:
            contracts.validate_contract(codex_contract(self.ws, output_schema=missing))
        self.assertIn("details", str(caught.exception))

    def test_objects_inside_arrays_are_checked_too(self):
        schema = strict_schema()
        schema["properties"]["steps"] = {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {"name": {"type": "string"}},
                "required": ["name"],
                "additionalProperties": False,
            },
        }
        schema["required"] = ["answer", "confidence", "steps"]
        contracts.validate_contract(codex_contract(self.ws, output_schema=schema))
        broken = json.loads(json.dumps(schema))
        del broken["properties"]["steps"]["items"]["additionalProperties"]
        with self.assertRaises(ContractError) as caught:
            contracts.validate_contract(codex_contract(self.ws, output_schema=broken))
        self.assertIn("steps[]", str(caught.exception))

    def test_zcode_output_schema_is_optional_but_checked_when_present(self):
        contract = zcode_contract(self.ws)
        contracts.validate_contract(contract)  # absent is fine
        with self.assertRaises(ContractError):
            contracts.validate_contract(
                zcode_contract(self.ws, output_schema={"type": "object"}))


class ModeAndSandboxTests(unittest.TestCase):
    """Optional worker mode + codex sandbox: legal matrix and every rule."""

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        workspace = Path(self.temporary.name) / "ws"
        workspace.mkdir()
        self.ws = str(workspace)
        self.command = {"id": "run-tests", "command": "/bin/true",
                        "max_uses": 1, "max_timeout_ms": 120000, "required": True}

    def appserver_contract(self, **overrides):
        contract = codex_contract(self.ws, mode="app-server", **overrides)
        del contract["output_schema"]  # exec-only mechanism
        return contract

    def test_legal_mode_matrix(self):
        contracts.validate_contract(zcode_contract(self.ws))                       # cold default
        contracts.validate_contract(zcode_contract(self.ws, mode="cold"))
        contracts.validate_contract(zcode_contract(self.ws, mode="resident"))
        contracts.validate_contract(codex_contract(self.ws))                       # exec default
        contracts.validate_contract(codex_contract(self.ws, mode="exec"))
        contracts.validate_contract(self.appserver_contract())

    def test_mode_for_wrong_worker_or_bad_value_is_rejected(self):
        for bad in ("exec", "app-server", "banana", 1, None):
            contract = zcode_contract(self.ws)
            if bad is None:
                contract["mode"] = None
            else:
                contract["mode"] = bad
            with self.assertRaises(ContractError) as caught:
                contracts.validate_contract(contract)
            self.assertIn("mode must be one of", str(caught.exception))
        for bad in ("cold", "resident", "banana"):
            with self.assertRaises(ContractError) as caught:
                contracts.validate_contract(codex_contract(self.ws, mode=bad))
            self.assertIn("mode must be one of", str(caught.exception))

    def test_zcode_write_paths_legal_in_both_modes(self):
        for mode in (None, "cold", "resident"):
            overrides = {"allowed_files": ["REPORT.md"], "commands": [self.command]}
            if mode:
                overrides["mode"] = mode
            contracts.validate_contract(zcode_contract(self.ws, **overrides))

    def test_appserver_write_task_requires_workspace_write(self):
        # default sandbox (read-only) and explicit read-only both refuse a
        # write task; explicit workspace-write accepts it
        for overrides in ({"allowed_files": ["OUT.md"]},
                          {"commands": [self.command]},
                          {"allowed_files": ["OUT.md"], "sandbox": "read-only"}):
            with self.assertRaises(ContractError) as caught:
                contracts.validate_contract(self.appserver_contract(**overrides))
            self.assertIn('must declare sandbox "workspace-write"',
                          str(caught.exception))
        contracts.validate_contract(
            self.appserver_contract(allowed_files=["OUT.md"],
                                    sandbox="workspace-write"))
        # read-only tasks (nothing to broker) keep the default read-only sandbox
        contracts.validate_contract(self.appserver_contract(sandbox="read-only"))

    def test_appserver_rejects_output_schema(self):
        with self.assertRaises(ContractError) as caught:
            contracts.validate_contract(
                codex_contract(self.ws, mode="app-server",
                               sandbox="workspace-write"))
        self.assertIn("output_schema is exec-only", str(caught.exception))
        # exec still requires it
        with self.assertRaises(ContractError) as caught:
            contract = codex_contract(self.ws)
            del contract["output_schema"]
            contracts.validate_contract(contract)
        self.assertIn("requires output_schema", str(caught.exception))

    def test_old_contracts_without_mode_keep_their_semantics(self):
        # zcode without mode: no output_schema requirement, write paths legal
        contracts.validate_contract(zcode_contract(self.ws, allowed_files=["A.md"]))
        # codex without mode: exec semantics, output_schema still required
        with self.assertRaises(ContractError) as caught:
            contract = codex_contract(self.ws)
            del contract["output_schema"]
            contracts.validate_contract(contract)
        self.assertIn("requires output_schema", str(caught.exception))

    def test_sandbox_value_and_worker_rules(self):
        with self.assertRaises(ContractError) as caught:
            contracts.validate_contract(codex_contract(self.ws, sandbox="danger"))
        self.assertIn("sandbox must be one of", str(caught.exception))
        with self.assertRaises(ContractError) as caught:
            contracts.validate_contract(zcode_contract(self.ws, sandbox="read-only"))
        self.assertIn("sandbox applies to codex workers only",
                      str(caught.exception))
        contracts.validate_contract(
            self.appserver_contract(sandbox="workspace-write"))

    def test_allowed_files_entries_must_be_normalized_relative_paths(self):
        for bad in ("/abs/OUT.md", "../OUT.md", "./OUT.md", "a//b.md"):
            contract = zcode_contract(self.ws, allowed_files=[bad])
            with self.assertRaises(ContractError) as caught:
                contracts.validate_contract(contract)
            self.assertIn("normalized workspace-relative",
                          str(caught.exception))
        with self.assertRaises(ContractError) as caught:
            contracts.validate_contract(zcode_contract(self.ws, allowed_files=[" "]))
        self.assertIn("non-empty strings", str(caught.exception))


class DigestTests(unittest.TestCase):
    def test_digest_is_canonical_and_key_order_insensitive(self):
        one = codex_contract("/tmp/x")
        two = {key: one[key] for key in reversed(list(one))}
        self.assertEqual(contracts.contract_digest(one),
                         contracts.contract_digest(two))
        self.assertNotEqual(contracts.contract_digest(one),
                            contracts.contract_digest(
                                codex_contract("/tmp/x", prompt="different")))


class RegisterAndLoadTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.state_dir = self.root / "state"
        self.workspace = self.root / "workspace"
        self.workspace.mkdir()
        self.source = self.root / "incoming"
        self.source.mkdir()

    def write_contract(self, contract) -> Path:
        path = self.source / f"{contract['contract_id']}.json"
        path.write_text(json.dumps(contract, indent=2), encoding="utf-8")
        return path

    def test_register_then_load_round_trip(self):
        contract = codex_contract(str(self.workspace))
        result = contracts.register_contract(self.state_dir, self.write_contract(contract))
        self.assertEqual(result["contract_id"], "demo-codex-1")
        self.assertEqual(result["idempotent"], False)
        stored = self.state_dir / "hub" / "contracts" / "demo-codex-1.json"
        self.assertEqual(result["path"], str(stored))
        self.assertTrue(stored.is_file())
        self.assertEqual(result["sha256"], contracts.contract_digest(contract))
        loaded = contracts.load_contract(self.state_dir, "demo-codex-1")
        self.assertEqual(contracts.contract_digest(loaded), result["sha256"])

    def test_register_is_idempotent_for_identical_content(self):
        contract = codex_contract(str(self.workspace))
        path = self.write_contract(contract)
        first = contracts.register_contract(self.state_dir, path)
        # re-register from a differently formatted copy of the same contract
        other = self.source / "formatted.json"
        other.write_text(json.dumps(contract, indent=4, sort_keys=True),
                         encoding="utf-8")
        second = contracts.register_contract(self.state_dir, other)
        self.assertEqual(second["idempotent"], True)
        self.assertEqual(first["sha256"], second["sha256"])
        self.assertEqual(first["path"], second["path"])

    def test_register_refuses_to_overwrite_different_content(self):
        contracts.register_contract(
            self.state_dir, self.write_contract(codex_contract(str(self.workspace))))
        other = codex_contract(str(self.workspace), prompt="changed prompt")
        other_path = self.source / "other.json"
        other_path.write_text(json.dumps(other), encoding="utf-8")
        with self.assertRaises(ContractError) as caught:
            contracts.register_contract(self.state_dir, other_path)
        self.assertIn("refusing to overwrite", str(caught.exception))
        # the stored contract is unchanged
        loaded = contracts.load_contract(self.state_dir, "demo-codex-1")
        self.assertEqual(loaded["prompt"], codex_contract(str(self.workspace))["prompt"])

    def test_register_rejects_invalid_contract_without_writing(self):
        path = self.write_contract(
            codex_contract(str(self.workspace), allowed_files=["REPORT.md"]))
        with self.assertRaises(ContractError):
            contracts.register_contract(self.state_dir, path)
        self.assertFalse((self.state_dir / "hub" / "contracts").exists()
                         or (self.state_dir / "hub" / "contracts" / "demo-codex-1.json").is_file())

    def test_load_unknown_contract_is_rejected(self):
        with self.assertRaises(ContractError) as caught:
            contracts.load_contract(self.state_dir, "never-registered")
        self.assertIn("unknown contract", str(caught.exception))
        # path traversal cannot escape the contracts directory
        with self.assertRaises(ContractError) as caught:
            contracts.load_contract(self.state_dir, "../../secrets")
        self.assertIn("unknown contract", str(caught.exception))

    def test_load_revalidates_against_the_current_filesystem(self):
        contracts.register_contract(
            self.state_dir, self.write_contract(codex_contract(str(self.workspace))))
        self.workspace.rmdir()
        with self.assertRaises(ContractError) as caught:
            contracts.load_contract(self.state_dir, "demo-codex-1")
        self.assertIn("workspace", str(caught.exception))


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
