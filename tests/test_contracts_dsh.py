"""dsh worker contracts: registration rules for the headless channel (0.2.1)."""

import tempfile
import unittest
from pathlib import Path

from zcode_supervisor import contracts
from zcode_supervisor.contracts import ContractError


def dsh_contract(**overrides):
    contract = {
        "contract_id": "dsh-write-1",
        "worker": "dsh",
        "workspace": None,  # filled per-test (must be a real directory)
        "prompt": "Implement TASK.md in the workspace.",
        "allowed_files": ["REPORT.md"],
        "commands": [],
        "model": None,
        "max_timeout_ms": 1_800_000,
        "budget": {},
    }
    contract.update(overrides)
    return contract


class DshContractValidation(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.workspace = Path(self.tmp.name) / "workspace"
        self.workspace.mkdir()

    def tearDown(self):
        self.tmp.cleanup()

    def _validate(self, **overrides):
        return contracts.validate_contract(
            dsh_contract(workspace=str(self.workspace), **overrides))

    def test_valid_contract_registers(self):
        contract = self._validate()
        self.assertEqual(contract["worker"], "dsh")

    def test_default_and_explicit_mode(self):
        self._validate()  # default headless
        self._validate(mode="headless")
        with self.assertRaises(ContractError) as caught:
            self._validate(mode="exec")
        self.assertIn("mode must be one of", str(caught.exception))

    def test_commands_must_stay_empty(self):
        with self.assertRaises(ContractError) as caught:
            self._validate(commands=[{"id": "t", "command": "make test",
                                      "max_uses": 1, "max_timeout_ms": 60000,
                                      "required": True}])
        self.assertIn("commands must be empty for worker 'dsh'",
                      str(caught.exception))

    def test_model_must_be_null(self):
        with self.assertRaises(ContractError) as caught:
            self._validate(model={"model": "deepseek-v4.1-flash"})
        self.assertIn("dsh model selection is profile-driven",
                      str(caught.exception))

    def test_output_schema_refused(self):
        with self.assertRaises(ContractError) as caught:
            self._validate(output_schema={"type": "object"})
        self.assertIn("output_schema is not consumed by dsh",
                      str(caught.exception))

    def test_stall_timeout_ms_bounds(self):
        self._validate(stall_timeout_ms=900_000)
        for bad in (999, 3_600_001, True, "900000"):
            with self.assertRaises(ContractError) as caught:
                self._validate(stall_timeout_ms=bad)
            self.assertIn("stall_timeout_ms", str(caught.exception))

    def test_sandbox_does_not_apply(self):
        with self.assertRaises(ContractError) as caught:
            self._validate(sandbox="workspace-write")
        self.assertIn("sandbox applies to codex workers only",
                      str(caught.exception))


if __name__ == "__main__":
    unittest.main()
