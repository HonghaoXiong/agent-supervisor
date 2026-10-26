"""Tri-probe tests: faked agents on tempdirs, baseline flow, real MCP smoke.

No real codex/zcode/dsh is ever executed: probe targets are fake scripts and
bundles under a TemporaryDirectory, and run_probe_all's discovery layer is
patched. The MCP smoke test spawns the real read-only evidence server, which
never touches a model.
"""
import asyncio
import hashlib
import importlib.util
import io
import json
from contextlib import redirect_stdout
from pathlib import Path
import sys
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from zcode_supervisor import health as H  # noqa: E402
from zcode_supervisor import runtime as R  # noqa: E402
from zcode_supervisor import tri_probe as T  # noqa: E402

HAS_MCP = importlib.util.find_spec("mcp") is not None

FAKE_CODEX = '''#!%PYTHON%
import json
import sys
from pathlib import Path

here = Path(__file__).resolve().parent
args = sys.argv[1:]
if args == ["--version"]:
    print((here / "codex-version.txt").read_text().strip())
    raise SystemExit(0)
if args[:2] == ["app-server", "generate-json-schema"]:
    if (here / "codex-broken").exists():
        print("simulated schema export failure", file=sys.stderr)
        raise SystemExit(3)
    out = Path(args[args.index("--out") + 1])
    out.mkdir(parents=True, exist_ok=True)
    schemas = json.loads((here / "codex-schemas.json").read_text())
    for name, methods in schemas.items():
        variants = [{"properties": {"method": {"enum": [method]}}} for method in methods]
        (out / (name + ".json")).write_text(json.dumps({"oneOf": variants}))
    raise SystemExit(0)
print("unexpected arguments: " + " ".join(args), file=sys.stderr)
raise SystemExit(2)
'''

FAKE_NODE = '''#!%PYTHON%
import sys
from pathlib import Path

here = Path(__file__).resolve().parent
if "--version" in sys.argv[1:]:
    print((here / "zcode-version.txt").read_text().strip())
    raise SystemExit(0)
print("fake node: only --version is supported", file=sys.stderr)
raise SystemExit(2)
'''


def write_bundle(path: Path, methods, extra="") -> None:
    body = "".join('  send("%s", handler);\n' % method for method in methods)
    path.write_text("// fake zcode bundle\n" + body + extra, encoding="utf-8")


class Harness:
    """Fake codex CLI, fake node and fake zcode bundle under one temp root."""

    def __init__(self, base: Path):
        self.base = base
        base.mkdir(parents=True, exist_ok=True)
        self.codex_version = base / "codex-version.txt"
        self.codex_version.write_text("codex-cli 0.1.2-fake\n")
        self.schemas = base / "codex-schemas.json"
        self.schemas.write_text(json.dumps({
            "ClientRequest": ["client/one", "client/two", "client/three"],
            "ServerRequest": ["server/one"],
            "ServerNotification": ["note/one", "note/two"],
        }))
        self.codex = base / "codex"
        self.codex.write_text(FAKE_CODEX.replace("%PYTHON%", sys.executable))
        self.codex.chmod(0o755)
        self.node = base / "node"
        self.node.write_text(FAKE_NODE.replace("%PYTHON%", sys.executable))
        self.node.chmod(0o755)
        self.zcode_version = base / "zcode-version.txt"
        self.zcode_version.write_text("0.17.0-fake\n")
        self.bundle = base / "zcode.cjs"
        write_bundle(self.bundle, ["session/create", "session/read",
                                   "interaction/requestPermission"])

    def config(self, root: Path) -> R.Config:
        return R.Config(work_root=root / "work", state_dir=root / "state",
                        cache_dir=root / "cache", cli_path=self.bundle,
                        node=str(self.node))

    def patch_discovery(self, test: unittest.TestCase, dsh_path=None) -> None:
        for name, value in (
            ("discover_codex", {"installed": True, "path": str(self.codex),
                                "candidates": [str(self.codex)]}),
            ("discover_zcode", {"installed": True, "path": str(self.bundle),
                                "node": str(self.node), "node_found": True,
                                "candidates": [str(self.bundle)]}),
            ("discover_dsh", {"installed": dsh_path is not None, "path": dsh_path,
                              "home_dir_present": False, "candidates": []}),
        ):
            patcher = patch.object(T, name, return_value=value)
            patcher.start()
            test.addCleanup(patcher.stop)


class ProbeTests(unittest.TestCase):
    """Requirement 1: per-agent probes and contract counts on fakes."""

    def setUp(self):
        self.temp = TemporaryDirectory(prefix="czs tri-probe ")
        self.addCleanup(self.temp.cleanup)
        # Resolve once: macOS mkdtemp lives under the /var symlink and Config
        # paths must stay real, absolute and link-free.
        self.root = Path(self.temp.name).resolve()
        self.harness = Harness(self.root / "fakes")
        self.out = self.root / "out"

    def test_codex_probe_counts_contract_and_fingerprints_wrapper(self):
        result = T.probe_codex(self.harness.codex, self.out / "contracts" / "codex")
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["version"], "codex-cli 0.1.2-fake")
        contract = result["contract"]
        self.assertEqual(contract["client_methods"], 3)
        self.assertEqual(contract["server_requests"], 1)
        self.assertEqual(contract["notifications"], 2)
        self.assertEqual(contract["methods"],
                         ["client/one", "client/three", "client/two"])
        self.assertEqual(contract["exported_files"], 3)
        self.assertTrue((self.out / "contracts" / "codex" / "contract.json").is_file())
        # A text script with no resolvable exec line falls back to the wrapper
        # itself, stated honestly.
        self.assertEqual(result["fingerprint"],
                         hashlib.sha256(self.harness.codex.read_bytes()).hexdigest())
        self.assertIn("wrapper", result["fingerprint_source"])

    def test_codex_wrapper_resolves_to_real_binary_fingerprint(self):
        app = self.root / "wrapped" / "bin"
        app.mkdir(parents=True)
        (self.root / "wrapped" / "CodexCLI.app").mkdir()
        real = self.root / "wrapped" / "real-codex"
        real.write_bytes(b"\xcf\xfa\xed\xfefake mach-o payload")
        wrapper = app / "codex"
        wrapper.write_text('#!/bin/sh\nexec "$bin_dir/../real-codex" "$@"\n')
        wrapper.chmod(0o755)
        target, note = T._codex_binary_target(wrapper)
        self.assertEqual(target, real)
        self.assertEqual(note, "resolved-from-wrapper")

    def test_codex_probe_reports_schema_export_failure_without_crashing(self):
        (self.harness.base / "codex-broken").write_text("")
        result = T.probe_codex(self.harness.codex, self.out / "c")
        self.assertEqual(result["status"], "error")
        self.assertIn("simulated schema export failure", result["error"])
        self.assertNotIn("contract", result)

    def test_zcode_probe_contract_pin_and_version(self):
        config = self.harness.config(self.root)
        result = T.probe_zcode(config, self.out / "contracts" / "zcode")
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["version"], "0.17.0-fake")
        self.assertEqual(result["contract"]["methods"],
                         ["interaction/requestPermission", "session/create",
                          "session/read"])
        self.assertEqual(result["contract"]["count"], 3)
        self.assertIs(result["pinned"], False)
        self.assertIsNone(result["pin"])
        digest = hashlib.sha256(self.harness.bundle.read_bytes()).hexdigest()
        self.assertEqual(result["fingerprint"], digest)
        self.assertTrue((self.out / "contracts" / "zcode" / "contract.json").is_file())

    def test_zcode_probe_pins_known_bundle(self):
        config = self.harness.config(self.root)
        digest = hashlib.sha256(self.harness.bundle.read_bytes()).hexdigest()
        pin = {"reviewed": "2026-10-24", "cli_version": "0.17.0"}
        with patch.object(T, "REVIEWED_BUNDLES", {digest: pin}):
            result = T.probe_zcode(config, self.out / "c")
        self.assertEqual(result["status"], "ok")
        self.assertIs(result["pinned"], True)
        self.assertEqual(result["pin"], pin)

    def test_dsh_absent_is_honest(self):
        self.assertEqual(T.probe_dsh(None),
                         {"agent": "dsh", "installed": False, "status": "absent"})

    def test_discovery_without_installs_reports_absent(self):
        with patch("shutil.which", return_value=None), \
             patch.object(T, "CODEX_KNOWN_PATHS", ()), \
             patch.object(T, "DSH_HOME", self.root / "no-dsh-home"):
            codex = T.discover_codex()
            dsh = T.discover_dsh()
        self.assertEqual(codex, {"installed": False, "path": None, "candidates": []})
        self.assertIs(dsh["installed"], False)
        self.assertIsNone(dsh["path"])
        self.assertIs(dsh["home_dir_present"], False)
        config = self.harness.config(self.root)
        zcode = T.discover_zcode(config)
        self.assertIs(zcode["installed"], True)
        self.assertEqual(zcode["path"], str(self.harness.bundle))
        self.assertIs(zcode["node_found"], True)


@unittest.skipUnless(HAS_MCP, "the MCP client SDK backs the hub smoke test")
class BaselineFlowTests(unittest.TestCase):
    """Requirements 2-5 through run_probe_all with patched discovery."""

    def setUp(self):
        self.temp = TemporaryDirectory(prefix="czs tri-probe flow ")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.harness = Harness(self.root / "fakes")
        self.config = self.harness.config(self.root)
        self.harness.patch_discovery(self)
        self.out = self.root / "out"

    def run_probe(self, **kwargs):
        return T.run_probe_all(self.config, self.out, **kwargs)

    def test_first_run_creates_baseline_and_second_run_is_ok(self):
        first = self.run_probe()
        self.assertIs(first["baseline"]["created"], True)
        self.assertEqual(first["overall"], "ok")
        self.assertEqual(first["agents"]["dsh"]["status"], "absent")
        self.assertEqual(first["agents"]["codex"]["status"], "ok")
        self.assertEqual(first["agents"]["zcode"]["status"], "ok")
        self.assertEqual(first["hub_mcp"]["status"], "ok")
        baseline = json.loads(Path(first["baseline"]["path"]).read_text())
        self.assertEqual(set(baseline["agents"]), set(T.AGENTS))
        second = self.run_probe()
        self.assertIs(second["baseline"]["created"], False)
        self.assertIs(second["baseline"]["updated"], False)
        self.assertEqual(second["overall"], "ok")
        self.assertTrue(all(not entry["changed"]
                            for entry in second["comparison"].values()))

    def test_upstream_drift_reports_added_removed_and_overall_attention(self):
        self.run_probe()
        # codex: version bump plus one method replaced in the exported schema
        self.harness.codex_version.write_text("codex-cli 0.2.0-fake\n")
        schemas = json.loads(self.harness.schemas.read_text())
        schemas["ClientRequest"] = ["client/one", "client/three", "client/four"]
        self.harness.schemas.write_text(json.dumps(schemas))
        # zcode: new protocol method plus changed bundle bytes
        write_bundle(self.harness.bundle,
                     ["session/create", "session/read",
                      "interaction/requestPermission", "workflow/submit"],
                     extra="// byte drift\n")
        drifted = self.run_probe()
        self.assertEqual(drifted["overall"], "attention")
        codex = drifted["comparison"]["codex"]
        self.assertIs(codex["changed"], True)
        self.assertEqual(codex["diff"]["methods"],
                         {"added": ["client/four"], "removed": ["client/two"]})
        self.assertEqual(codex["diff"]["version"],
                         {"was": "codex-cli 0.1.2-fake", "now": "codex-cli 0.2.0-fake"})
        self.assertNotIn("fingerprint", codex["diff"])
        zcode = drifted["comparison"]["zcode"]
        self.assertIs(zcode["changed"], True)
        self.assertEqual(zcode["diff"]["methods"],
                         {"added": ["workflow/submit"], "removed": []})
        self.assertIn("fingerprint", zcode["diff"])
        dsh = drifted["comparison"]["dsh"]
        self.assertIs(dsh["changed"], False)
        # explicit refresh stores the drifted state; the refresh run still
        # reports the drift it observed (compare happens before the overwrite),
        # and only the next run against the refreshed baseline is clean again
        refreshed = self.run_probe(write_baseline=True)
        self.assertIs(refreshed["baseline"]["updated"], True)
        self.assertEqual(refreshed["overall"], "attention")
        self.assertEqual(self.run_probe()["overall"], "ok")

    def test_single_probe_failure_is_error_not_crash(self):
        self.run_probe()
        (self.harness.base / "codex-broken").write_text("")
        result = self.run_probe()
        self.assertEqual(result["agents"]["codex"]["status"], "error")
        self.assertEqual(result["agents"]["zcode"]["status"], "ok")
        self.assertEqual(result["overall"], "attention")
        # a failing probe is not silently absorbed into the baseline diff
        self.assertIs(result["comparison"]["codex"]["changed"], False)

    def test_compare_treats_absent_on_both_sides_as_unchanged(self):
        comparison = T.compare_with_baseline(
            {"agents": {"dsh": {"agent": "dsh", "installed": False, "status": "absent"}}},
            {"agents": {"dsh": {"agent": "dsh", "installed": False, "status": "absent"}}})
        self.assertIs(comparison["dsh"]["changed"], False)
        disappearance = T.compare_with_baseline(
            {"agents": {"codex": {"agent": "codex", "installed": False}}},
            {"agents": {"codex": {"agent": "codex", "installed": True,
                                  "version": "1", "fingerprint": "f"}}})
        self.assertIs(disappearance["codex"]["changed"], True)
        self.assertEqual(disappearance["codex"]["diff"]["installed"],
                         {"was": True, "now": False})


@unittest.skipUnless(HAS_MCP, "the MCP client SDK backs the hub smoke test")
class RevalidateFlowTests(unittest.TestCase):
    """P0-5 acceptance: revalidate publishes health.json and fails closed.

    The scenario mirrors one upstream auto-update round: probe and publish,
    watch a changed worker get disabled (probe, health.json and the hub's
    health/delegate tools all agree), then accept the new baseline and
    recover.
    """

    def setUp(self):
        self.temp = TemporaryDirectory(prefix="czs revalidate ")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.harness = Harness(self.root / "fakes")
        self.config = self.harness.config(self.root)
        self.harness.patch_discovery(self)
        self.out = self.root / "out"

    def drift_zcode_bundle(self, marker: str) -> None:
        # same protocol methods, different bytes: pure fingerprint drift
        write_bundle(self.harness.bundle,
                     ["session/create", "session/read",
                      "interaction/requestPermission"],
                     extra=marker)

    def test_first_revalidate_enables_installed_workers_and_publishes(self):
        result = T.revalidate(self.config, self.out)
        self.assertIs(result["all_installed_enabled"], True)
        workers = result["health"]["workers"]
        self.assertEqual(workers["codex"]["status"], "ok")
        self.assertIs(workers["codex"]["enabled"], True)
        self.assertEqual(workers["zcode"]["status"], "ok")
        self.assertIs(workers["zcode"]["enabled"], True)
        self.assertEqual(workers["dsh"]["status"], "absent")
        self.assertIs(workers["dsh"]["enabled"], False)  # absent, not counted
        routing = result["health"]["routing"]
        self.assertIn("codex", routing["enabled_workers"])
        self.assertIn("zcode", routing["enabled_workers"])
        self.assertIn("dsh", routing["disabled_workers"])
        self.assertEqual(routing["note"], H.ROUTING_NOTE)
        # the document is on disk at the path the hub reads, byte-identical
        self.assertTrue((self.config.state_dir / "hub" / "health.json").is_file())
        self.assertEqual(H.read_health(self.config.state_dir), result["health"])

    def test_upstream_update_disables_changed_workers_and_persists(self):
        T.revalidate(self.config, self.out)
        # zcode: bundle bytes change (new fingerprint) plus one new method
        write_bundle(self.harness.bundle,
                     ["session/create", "session/read",
                      "interaction/requestPermission", "workflow/submit"],
                     extra="// byte drift\n")
        # codex: exported schema swaps client/two for client/four
        schemas = json.loads(self.harness.schemas.read_text())
        schemas["ClientRequest"] = ["client/one", "client/three", "client/four"]
        self.harness.schemas.write_text(json.dumps(schemas))

        result = T.revalidate(self.config, self.out)
        self.assertIs(result["all_installed_enabled"], False)
        workers = result["health"]["workers"]
        zcode = workers["zcode"]
        self.assertIs(zcode["enabled"], False)
        self.assertEqual(zcode["status"], "changed")
        self.assertIn("fingerprint", zcode["reason"])
        self.assertIn("added method", zcode["reason"])
        codex = workers["codex"]
        self.assertIs(codex["enabled"], False)
        self.assertEqual(codex["status"], "changed")
        self.assertIn("added method", codex["reason"])
        self.assertIn("removed method", codex["reason"])
        self.assertEqual(workers["dsh"]["status"], "absent")
        routing = result["health"]["routing"]
        self.assertEqual(routing["enabled_workers"], [])  # both workers drifted
        self.assertIn("zcode", routing["disabled_workers"])
        self.assertIn("codex", routing["disabled_workers"])
        self.assertEqual(routing["note"], H.ROUTING_NOTE)
        self.assertEqual(H.read_health(self.config.state_dir), result["health"])

    def test_hub_health_and_delegate_fail_closed_after_drift(self):
        from zcode_supervisor import contracts, hub_server

        T.revalidate(self.config, self.out)
        self.drift_zcode_bundle("// upstream drift\n")
        T.revalidate(self.config, self.out)

        # hub view: health.json is authoritative for the disabled worker
        view = hub_server.health_impl(self.config)
        zcode = view["workers"]["zcode"]
        self.assertIs(zcode["enabled"], False)
        self.assertEqual(zcode["status"], "changed")
        self.assertIs(zcode["declared"], True)
        self.assertEqual(view["health_source"], "health.json")
        self.assertEqual(view["routing_note"], H.ROUTING_NOTE)
        self.assertIn("zcode", view["routing"]["disabled_workers"])
        self.assertIn("codex", view["routing"]["enabled_workers"])

        # a registered zcode contract is refused while the worker is disabled
        workspace = self.config.work_root / "ws"
        workspace.mkdir(parents=True)
        contract = {
            "contract_id": "revalidate-drift-check",
            "worker": "zcode",
            "workspace": str(workspace),
            "prompt": "Read-only drift check; expect no dispatch.",
            "allowed_files": [],
            "commands": [],
            "model": {"providerId": "builtin", "modelId": "probe",
                      "options": {"reasoningLevel": "low"}},
            "max_timeout_ms": 1000,
            "budget": {"max_tokens": 100, "max_cost_usd": 0.1},
        }
        contract_path = self.root / "contract.json"
        contract_path.write_text(json.dumps(contract))
        registered = contracts.register_contract(self.config.state_dir, contract_path)
        self.assertIs(registered["idempotent"], False)

        async def dispatch():
            return await hub_server.delegate_impl(
                self.config, self.root / "evidence", contract["contract_id"],
                {"agent": "codex", "session_id": "session-1"})

        with self.assertRaises(hub_server.HubError) as caught:
            asyncio.run(dispatch())
        self.assertEqual(caught.exception.code, "worker-disabled")
        self.assertIn("remaining_workers: ['codex']", caught.exception.detail)
        self.assertIn("equivalence", caught.exception.detail)  # routing note

    def test_write_baseline_then_revalidate_restores_enabled(self):
        T.revalidate(self.config, self.out)
        self.drift_zcode_bundle("// accepted upstream update\n")
        drifted = T.revalidate(self.config, self.out)
        self.assertIs(drifted["all_installed_enabled"], False)
        self.assertIs(drifted["health"]["workers"]["zcode"]["enabled"], False)
        # the human accepts the drifted state as the new baseline...
        T.run_probe_all(self.config, self.out, write_baseline=True)
        accepted = T.revalidate(self.config, self.out)
        self.assertIs(accepted["all_installed_enabled"], True)
        zcode = accepted["health"]["workers"]["zcode"]
        self.assertIs(zcode["enabled"], True)
        self.assertEqual(zcode["status"], "ok")


@unittest.skipUnless(HAS_MCP, "the MCP client SDK backs the hub smoke test")
class HubMcpSmokeTests(unittest.TestCase):
    """Requirement 6: real stdio MCP handshake against mcp_readonly."""

    def test_handshake_lists_readonly_tools(self):
        with TemporaryDirectory(prefix="czs tri-probe hub ") as tmp:
            root = Path(tmp).resolve()
            argv = [sys.executable, "-m", "zcode_supervisor.mcp_readonly",
                    "--evidence-root", str(root)]
            result = T.probe_hub_mcp(argv, timeout=60)
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["server_name"], "czs-readonly-evidence")
        self.assertIn("list_runs", result["tools"])
        self.assertIn("get_digest", result["tools"])
        self.assertTrue(result["tools"])

    def test_spawn_failure_is_error_not_exception(self):
        result = T.probe_hub_mcp([str(Path("/nonexistent/server-command-xyz"))])
        self.assertEqual(result["status"], "error")
        self.assertIn("error", result)


@unittest.skipUnless(HAS_MCP, "the MCP client SDK backs the hub smoke test")
class CliProbeAllTests(unittest.TestCase):
    """The probe-all subcommand wires config, output and exit codes."""

    def setUp(self):
        self.temp = TemporaryDirectory(prefix="czs tri-probe cli ")
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        self.harness = Harness(self.root / "fakes")
        self.config = self.harness.config(self.root)
        self.harness.patch_discovery(self)
        self.out = self.root / "out"

    def _main(self):
        from zcode_supervisor.cli import main
        buffer = io.StringIO()
        with patch("zcode_supervisor.cli.load_config", return_value=self.config), \
                redirect_stdout(buffer):
            code = main(["probe-all", "--out", str(self.out)])
        return code, json.loads(buffer.getvalue())

    def test_exit_zero_when_ok_and_one_on_attention(self):
        code, result = self._main()
        self.assertEqual(code, 0)
        self.assertEqual(result["overall"], "ok")
        self.harness.codex_version.write_text("codex-cli 9.9.9-fake\n")
        code, drifted = self._main()
        self.assertEqual(code, 1)
        self.assertEqual(drifted["overall"], "attention")

    def test_hub_server_argv_must_be_a_json_string_array(self):
        from zcode_supervisor.cli import main
        for bad in ('{"command": "x"}', '[]', '["a", 3]'):
            buffer = io.StringIO()
            with patch("zcode_supervisor.cli.load_config",
                       return_value=self.config), redirect_stdout(buffer):
                code = main(["probe-all", "--out", str(self.out),
                             "--hub-server-argv", bad])
            self.assertEqual(code, 1)
            self.assertIn("error", json.loads(buffer.getvalue()))


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
