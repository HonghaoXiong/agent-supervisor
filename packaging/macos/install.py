#!/usr/bin/env python3
"""Install the local plugin through Codex's plugin CLI, without sending model tasks."""
from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
from pathlib import Path, PurePosixPath
import re
import shutil
import stat
import subprocess
import sys
import tempfile


MARKETPLACE = "zcode-supervisor-local"
SELECTOR = "zcode-supervisor@" + MARKETPLACE


class InstallTransactionError(RuntimeError):
    """A failed installation with its original error and recovery evidence."""

    def __init__(self, receipt: dict):
        self.receipt = receipt
        super().__init__(f"Installation failed at {receipt['failed_stage']}: {receipt['error']}")


def plugin_inventory(root: Path) -> dict[str, tuple[str, str]]:
    """Inventory the complete tree without following links or special files."""
    root = Path(root)
    if root.is_symlink() or not root.is_dir():
        raise ValueError(f"Plugin tree must be an unlinked directory: {root}")
    entries = {}
    for path in root.rglob("*"):
        relative = path.relative_to(root).as_posix()
        mode = path.lstat().st_mode
        if stat.S_ISLNK(mode):
            raise ValueError(f"Linked plugin component: {relative}")
        if path.name == ".DS_Store":
            if not stat.S_ISREG(mode):
                raise ValueError(f"Finder metadata must be a regular file: {relative}")
            continue
        if stat.S_ISDIR(mode):
            entries[relative] = ("directory", "")
        elif stat.S_ISREG(mode):
            entries[relative] = ("file", hashlib.sha256(path.read_bytes()).hexdigest())
        else:
            raise ValueError(f"Special plugin component: {relative}")
    return entries


def verify_release(root: Path) -> dict:
    """Verify every distributed file and reject unexpected files or symlinks."""
    root = Path(root)
    if root.is_symlink():
        raise ValueError("Release directory must not be a symlink")
    manifest_path = root / "release-manifest.json"
    if manifest_path.is_symlink():
        raise ValueError("Manifest must not be a symlink")
    manifest = json.loads(manifest_path.read_text())
    if manifest.get("schema_version") != 1 or not isinstance(manifest.get("files"), dict):
        raise ValueError("Invalid release manifest")
    actual = set()
    for path in root.rglob("*"):
        if path.is_symlink():
            raise ValueError(f"Linked release component: {path.relative_to(root)}")
        if path.name == ".DS_Store" and not stat.S_ISREG(path.lstat().st_mode):
            raise ValueError("Finder metadata must be a regular file")
        if path.is_file() and path != manifest_path and path.name != ".DS_Store":
            actual.add(path.relative_to(root).as_posix())
    if actual != set(manifest["files"]):
        raise ValueError("Release has unexpected or missing files")
    for name, expected in manifest["files"].items():
        rel = PurePosixPath(name)
        if rel.is_absolute() or ".." in rel.parts or str(rel) != name:
            raise ValueError("Invalid manifest path")
        path = root / name
        if not path.is_file() or path.is_symlink():
            raise ValueError(f"Missing or linked release file: {name}")
        if hashlib.sha256(path.read_bytes()).hexdigest() != expected:
            raise ValueError(f"Release checksum mismatch: {name}")
    version = manifest.get("version", "")
    if not isinstance(version, str) or not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", version):
        raise ValueError("Invalid release version")
    return manifest


def find_codex(explicit: str | None = None) -> str | None:
    return diagnostics_module().find_codex(explicit)


def diagnostics_module():
    path = Path(__file__).with_name("diagnostics.py")
    spec = importlib.util.spec_from_file_location("czs_install_diagnostics", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def environment_report(root: Path, codex: str | None, node=None, cli=None) -> dict:
    return diagnostics_module().inspect_environment(
        root / "plugins/zcode-supervisor", codex=codex, node=node, cli=cli)


def run_codex(command: list[str]) -> dict:
    result = subprocess.run(command, capture_output=True, text=True, timeout=120)
    if result.returncode:
        raise RuntimeError(f"Codex plugin command failed ({result.returncode}): "
                           f"{result.stderr.strip() or result.stdout.strip()}")
    try:
        return json.loads(result.stdout)
    except ValueError:
        return {"output": result.stdout.strip()}


def marketplace_source(codex: str) -> Path | None:
    listing = run_codex([codex, "plugin", "marketplace", "list", "--json"])
    if not isinstance(listing, dict) or not isinstance(listing.get("marketplaces"), list):
        raise ValueError("Cannot read existing marketplace state; leaving it unchanged")
    matches = [m for m in listing["marketplaces"]
               if isinstance(m, dict) and m.get("name") == MARKETPLACE]
    if not matches:
        return None
    if len(matches) != 1 or not isinstance(matches[0].get("root"), str) or not matches[0]["root"]:
        raise ValueError("Cannot identify the existing marketplace; leaving it unchanged")
    previous = Path(matches[0]["root"])
    if not previous.is_absolute():
        raise ValueError("Marketplace root must be absolute; leaving it unchanged")
    return previous


def installed_plugin(codex: str) -> dict | None:
    # Restrict the query to this local marketplace; do not enumerate remote catalogs.
    listing = run_codex([codex, "plugin", "list", "--marketplace", MARKETPLACE, "--json"])
    if not isinstance(listing, dict) or not isinstance(listing.get("installed"), list):
        raise ValueError("Cannot read installed plugin state; leaving it unchanged")
    matches = [p for p in listing["installed"]
               if isinstance(p, dict) and p.get("pluginId") == SELECTOR]
    if len(matches) > 1:
        raise ValueError("Ambiguous installed plugin state; leaving it unchanged")
    if not matches:
        return None
    record = matches[0]
    if record.get("installed") is not True or type(record.get("enabled")) is not bool:
        raise ValueError("Installed/enabled plugin state is unknown; leaving it unchanged")
    return record


def verify_plugin_state(record: dict | None, root: Path, manifest: dict) -> None:
    source = record.get("source") if record else None
    if not record or record.get("enabled") is not True:
        raise ValueError("Expected plugin is not installed and enabled")
    if record.get("version") != manifest["version"] or not isinstance(source, dict) \
            or source.get("source") != "local" or not isinstance(source.get("path"), str) \
            or Path(source["path"]).resolve() != (root / "plugins/zcode-supervisor").resolve():
        raise ValueError("Installed plugin identity, version or source differs from this release")


def installation_snapshot(prefix: Path, codex: str) -> dict:
    previous = marketplace_source(codex)
    plugin = installed_plugin(codex)
    manifest = None
    if previous is not None:
        if not previous.resolve().is_relative_to(prefix.resolve()):
            raise ValueError("The marketplace belongs to another source outside this install prefix; leaving it unchanged")
        manifest = verify_release(previous)
        if manifest.get("name") != "zcode-supervisor":
            raise ValueError("Existing marketplace is not an owned ZCode Supervisor release")
    if plugin:
        if previous is None:
            raise ValueError("Installed plugin has no verified owned marketplace; leaving it unchanged")
        if plugin["enabled"] is not True:
            # The reviewed CLI has no plugin enable/disable command. Do not silently
            # enable a disabled plugin or claim that its state can be restored.
            raise ValueError("Existing plugin is disabled; this CLI cannot restore disabled state. Enable it explicitly before upgrading, or leave this installation unchanged")
        verify_plugin_state(plugin, previous, manifest)
    return {"source": previous, "manifest": manifest, "plugin": plugin}


def switch_marketplace(target: Path, codex: str, previous: dict, transaction: dict) -> dict:
    source = previous["source"]
    transaction["stage"] = "marketplace-register"
    if source is not None and source.resolve() != target.resolve():
        transaction["source_changed"] = True
        transaction["stage"] = "marketplace-remove-previous"
        run_codex([codex, "plugin", "marketplace", "remove", MARKETPLACE, "--json"])
    transaction["stage"] = "marketplace-register"
    if source is None or source.resolve() != target.resolve():
        # Include commands that fail after making a partial native state change.
        transaction["source_changed"] = True
    added = run_codex([codex, "plugin", "marketplace", "add", str(target), "--json"])
    if not isinstance(added, dict):
        raise ValueError("Codex returned an invalid marketplace result")
    if source is not None and source.resolve() != target.resolve():
        added["previousOwnedSource"] = str(source)
    return added


def restore_installation(target: Path, codex: str, previous: dict, transaction: dict) -> dict:
    recovery = {"attempted": bool(transaction["source_changed"] or transaction["plugin_attempted"]),
                "previous_source": str(previous["source"]) if previous["source"] else None,
                "previous_plugin_installed": previous["plugin"] is not None,
                "source_restored": False, "plugin_restored": False,
                "errors": []}
    if not recovery["attempted"]:
        recovery["state"] = "not_needed"
        return recovery
    stage = "inspect-current-source"
    try:
        current = marketplace_source(codex)
        source = previous["source"]
        permitted = {target.resolve()}
        if source is not None:
            permitted.add(source.resolve())
        if current is not None and current.resolve() not in permitted:
            raise ValueError("Marketplace changed to another source; refusing to alter it during recovery")
        if previous["plugin"] is None and transaction["plugin_attempted"]:
            stage = "remove-new-plugin"
            try:
                if installed_plugin(codex) is not None:
                    run_codex([codex, "plugin", "remove", SELECTOR, "--json"])
                if installed_plugin(codex) is not None:
                    raise ValueError("New plugin remains installed after cleanup")
                recovery["plugin_absence_verified"] = True
            except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as cleanup_error:
                # Source restoration remains useful even when native uninstall
                # failed; retain both outcomes rather than abandoning recovery.
                recovery["errors"].append({"stage": stage, "error": str(cleanup_error),
                                           "error_type": type(cleanup_error).__name__})
        if source is not None:
            stage = "verify-previous-release"
            if verify_release(source) != previous["manifest"]:
                raise ValueError("Previous release changed; refusing to restore unverified bytes")
        if current is not None and (source is None or current.resolve() != source.resolve()):
            stage = "remove-new-source"
            run_codex([codex, "plugin", "marketplace", "remove", MARKETPLACE, "--json"])
            current = None
        if source is not None and current is None:
            stage = "restore-previous-source"
            run_codex([codex, "plugin", "marketplace", "add", str(source), "--json"])
        stage = "verify-restored-source"
        actual = marketplace_source(codex)
        if (actual.resolve() if actual else None) != (source.resolve() if source else None):
            raise ValueError("Marketplace source recovery was not verified")
        recovery["source_restored"] = True
        if previous["plugin"] is not None:
            stage = "restore-previous-plugin"
            restored = run_codex([codex, "plugin", "add", SELECTOR, "--json"])
            stage = "verify-restored-plugin-cache"
            verify_plugin_cache(source, restored)
            stage = "verify-restored-plugin-state"
            verify_plugin_state(installed_plugin(codex), source, previous["manifest"])
            recovery["plugin_restored"] = True
            recovery["state"] = "previous_installation_restored_verified"
        else:
            recovery["state"] = "previous_source_restored_no_previous_plugin" if source else "new_installation_removed_no_previous_plugin"
        if recovery["errors"]:
            recovery["state"] = "recovery_failed"
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        recovery["state"] = "recovery_failed"
        recovery["errors"].append({"stage": stage, "error": str(error), "error_type": type(error).__name__})
    return recovery


def transaction_failure(error: Exception, target: Path, codex: str,
                        previous: dict, transaction: dict) -> InstallTransactionError:
    failed_stage = transaction["stage"]
    recovery = restore_installation(target, codex, previous, transaction)
    return InstallTransactionError({"schema_version": 1, "installed": False,
                                    "error_code": "INSTALLATION_TRANSACTION_FAILED", "plugin": SELECTOR,
                                    "release_directory": str(target),
                                    "failed_stage": failed_stage, "error": str(error),
                                    "error_type": type(error).__name__,
                                    "rollback": recovery, "model_requests": 0})


def register_marketplace(target: Path, prefix: Path, codex: str) -> dict:
    """Register a source, retaining recovery when called without install()."""
    previous = installation_snapshot(prefix, codex)
    transaction = {"stage": "marketplace-register", "source_changed": False, "plugin_attempted": False}
    try:
        return switch_marketplace(target, codex, previous, transaction)
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        raise transaction_failure(error, target, codex, previous, transaction) from error


def verify_plugin_cache(root: Path, installed: dict) -> None:
    if not isinstance(installed, dict):
        raise ValueError("Codex returned an invalid plugin installation result")
    location = installed.get("installedPath")
    if not isinstance(location, str) or not Path(location).is_dir():
        raise ValueError("Codex returned no usable installedPath; plugin installation is unverified")
    expected = plugin_inventory(root / "plugins/zcode-supervisor")
    actual = plugin_inventory(Path(location))
    if set(expected) != set(actual):
        raise ValueError(f"Installed plugin differs from this release: unexpected={sorted(set(actual) - set(expected))}, missing={sorted(set(expected) - set(actual))}")
    for relative, entry in expected.items():
        if actual[relative] != entry:
            raise ValueError(f"Installed plugin differs from this release: {relative}")


def install(root: Path, prefix: Path, codex: str) -> dict:
    manifest = verify_release(root)
    if prefix.is_symlink():
        raise ValueError("Install prefix must not be a symlink")
    if prefix.resolve().is_relative_to(root.resolve()):
        raise ValueError("Install prefix must be outside the extracted release")
    prefix.mkdir(parents=True, exist_ok=True)
    prefix = prefix.resolve()
    identity = hashlib.sha256((root / "release-manifest.json").read_bytes()).hexdigest()[:12]
    target = prefix / (manifest["version"] + "-" + identity)
    if target.exists() or target.is_symlink():
        if verify_release(target) != manifest:
            raise ValueError("Existing installation differs; refusing to overwrite it")
    else:
        # Verify the staged copy too; never replace another version or user files.
        with tempfile.TemporaryDirectory(prefix=".install-", dir=prefix) as temporary:
            staged = Path(temporary) / "release"
            shutil.copytree(root, staged, ignore=shutil.ignore_patterns(".DS_Store"))
            if verify_release(staged) != manifest:
                raise ValueError("Copied release differs")
            staged.rename(target)
    previous = installation_snapshot(prefix, codex)
    transaction = {"stage": "marketplace-register", "source_changed": False, "plugin_attempted": False}
    try:
        added = switch_marketplace(target, codex, previous, transaction)
        transaction["stage"] = "plugin-add"
        transaction["plugin_attempted"] = True
        installed = run_codex([codex, "plugin", "add", SELECTOR, "--json"])
        transaction["stage"] = "plugin-cache-verification"
        verify_plugin_cache(target, installed)
        transaction["stage"] = "plugin-state-verification"
        verify_plugin_state(installed_plugin(codex), target, manifest)
        receipt = {"installed": True, "plugin": SELECTOR, "release_directory": str(target),
                   "marketplace_result": added, "plugin_result": installed,
                   "model_requests": 0, "installed_cache_verified": True,
                   "installed_enabled_verified": True,
                   "next": "Restart Codex and open a new chat. Say: 使用 ZCode Supervisor 检查环境，然后委派我的任务。"}
        transaction["stage"] = "success-receipt-write"
        write_install_receipt(prefix / ("install-" + identity + ".json"), receipt)
        return receipt
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        failure = transaction_failure(error, target, codex, previous, transaction)
        try:
            destination = prefix / ("install-" + identity + "-failed.json")
            failure.receipt["failure_receipt"] = str(destination)
            write_install_receipt(destination, failure.receipt)
        except (OSError, ValueError) as receipt_error:
            failure.receipt.pop("failure_receipt", None)
            failure.receipt["failure_receipt_error"] = str(receipt_error)
        raise failure from error


def write_install_receipt(destination: Path, receipt: dict) -> None:
    """Leave an existing successful receipt intact if a replacement write fails."""
    with tempfile.TemporaryDirectory(prefix=".receipt-", dir=destination.parent) as temporary:
        staged = Path(temporary) / "receipt.json"
        staged.write_text(json.dumps(receipt, ensure_ascii=False, indent=2) + "\n")
        staged.chmod(0o600)
        staged.replace(destination)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true", help="Verify release and prerequisites without installing")
    parser.add_argument("--prefix", type=Path, default=Path.home() / ".local/share/zcode-supervisor")
    parser.add_argument("--codex", help="Explicit Codex CLI executable")
    parser.add_argument("--node", help="Explicit Node executable for prerequisite checks")
    parser.add_argument("--cli", help="Explicit ZCode CLI path for fingerprint checks")
    args = parser.parse_args(argv)
    root = Path(__file__).resolve().parent.parent
    try:
        verify_release(root)
        codex = find_codex(args.codex)
        report = environment_report(root, args.codex if args.codex is not None else codex,
                                    args.node, args.cli)
        if args.check:
            print(json.dumps(report, ensure_ascii=False, indent=2))
            return 0 if report["ok"] else 2
        host_checks = [check for check in report["checks"] if check["id"].startswith("codex_")]
        if not codex or not host_checks or any(check["status"] != "supported" for check in host_checks):
            report.update(installed=False, error_code="CODEX_PLUGIN_COMMANDS_UNAVAILABLE",
                          remediation="Resolve the Codex checks before installing; no plugin registration was attempted.")
            print(json.dumps(report, ensure_ascii=False, indent=2), file=sys.stderr)
            return 1
        # Installation is allowed before ZCode setup; actual task execution remains doctor-gated.
        report.update(install(root, args.prefix.expanduser().absolute(), codex))
        if not report["ok"]:
            report["setup_required"] = "Plugin installed; resolve the remaining checks, then initialize and run doctor before delegation. Never bypass fingerprint checks."
        print(json.dumps(report, ensure_ascii=False, indent=2))
        return 0
    except InstallTransactionError as error:
        print(json.dumps(error.receipt, ensure_ascii=False), file=sys.stderr)
        return 1
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        print(json.dumps({"installed": False, "error_code": "INSTALLATION_PRECHECK_FAILED",
                          "error": str(error), "model_requests": 0}, ensure_ascii=False), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
