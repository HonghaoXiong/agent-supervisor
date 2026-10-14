"""Portable configuration and a hash-pinned local ZCode CLI compatibility copy.

Tool visibility must not grant permission to execute tools. The reviewed source
patch removes that conversion in the known 0.16.9 bundle. Unknown bundles fail
closed. Installed applications and authentication settings are never modified.
This adapter is a permission broker prerequisite, not an operating-system sandbox.
"""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
from importlib.resources import files
import json
import os
from pathlib import Path

from .common import atomic_write, ensure_directory, read_regular, require_posix


CLI_VERSION = "0.16.9"
UPSTREAM_SHA = "aac4755666d09fdcd70272fcf063c077a639015f"
SOURCE_SHA256 = "b816e38656e70fb585fe2ab1ea010da89cda928dac1ccb5d83255fd18796da6e"
PATCHED_SHA256 = "31ef7f33f40591303047db5135845bd39b47399372110483fe6cf6c2df4328d2"
FRAGMENT = b"e.runtimeConfig?.toolAllowlist&&(n.allowedTools=[...e.runtimeConfig.toolAllowlist]),"
COMPATIBILITY = "pinned-0.16.9"


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _source_diff() -> bytes:
    return files("zcode_supervisor").joinpath("patches/zcode-permission-allowlist.patch").read_bytes()


def _path(value: object, base: Path, key: str) -> Path:
    if not isinstance(value, str) or not value.strip() or "\x00" in value:
        raise ValueError(f"{key} must be a nonempty filesystem path string")
    path = Path(value).expanduser()
    if not path.is_absolute():
        path = base / path
    # Preserve the final component so file/directory link checks remain possible.
    return Path(os.path.abspath(path))


@dataclass(frozen=True)
class Config:
    work_root: Path
    state_dir: Path
    cache_dir: Path
    cli_path: Path
    provider_config: Path | None = None
    node: str = "node"
    compatibility: str = COMPATIBILITY

    def __post_init__(self) -> None:
        require_posix()
        if self.compatibility != COMPATIBILITY:
            raise ValueError(f"Only compatibility={COMPATIBILITY!r} is supported")
        if not isinstance(self.node, str) or not self.node.strip() or "\x00" in self.node:
            raise ValueError("node must be a nonempty executable name or path")
        for name in ("work_root", "state_dir", "cache_dir", "cli_path", "provider_config"):
            path = getattr(self, name)
            if name == "provider_config" and path is None:
                continue
            if not isinstance(path, Path) or not path.is_absolute():
                raise ValueError(f"{name} must be an absolute pathlib.Path")
            if path.is_symlink():
                raise ValueError(f"Refusing symlink configured path: {name}")
        roots = [self.work_root.resolve(), self.state_dir.resolve(), self.cache_dir.resolve()]
        for index, left in enumerate(roots):
            for right in roots[index + 1:]:
                if left.is_relative_to(right) or right.is_relative_to(left):
                    raise ValueError("work_root, state_dir and cache_dir must be separate, non-nested directories")

    def command(self) -> list[str]:
        """Validate/prepare the pinned bundle before any ZCode invocation."""
        return [self.node, str(prepare_cli(self))]

    def environment(self) -> dict[str, str]:
        """Return a process-local environment without loading credential files."""
        environment = os.environ.copy()
        environment["OPENBLAS_NUM_THREADS"] = "1"
        environment["OMP_NUM_THREADS"] = "1"
        if self.provider_config is not None:
            environment["ZCODE_BUILTIN_PROVIDER_CONFIG_FILE"] = str(self.provider_config)
        return environment


def load_config(path: Path | str) -> Config:
    """Read explicit schema v1 JSON; relative paths use the config directory."""
    path = Path(path).expanduser().absolute()
    try:
        value = json.loads(read_regular(path))
    except (json.JSONDecodeError, UnicodeDecodeError) as error:
        raise ValueError("Configuration must be valid UTF-8 JSON") from error
    required = {"schema_version", "work_root", "state_dir", "cache_dir", "cli_path", "node", "compatibility"}
    allowed = required | {"provider_config"}
    if not isinstance(value, dict):
        raise ValueError("Configuration must be a JSON object")
    unknown, missing = set(value) - allowed, required - set(value)
    if unknown or missing:
        raise ValueError(f"Invalid configuration keys: unknown={sorted(unknown)}, missing={sorted(missing)}")
    if type(value["schema_version"]) is not int or value["schema_version"] != 1:
        raise ValueError("Only configuration schema_version=1 is supported")
    provider = value.get("provider_config")
    return Config(
        work_root=_path(value["work_root"], path.parent, "work_root"),
        state_dir=_path(value["state_dir"], path.parent, "state_dir"),
        cache_dir=_path(value["cache_dir"], path.parent, "cache_dir"),
        cli_path=_path(value["cli_path"], path.parent, "cli_path"),
        provider_config=None if provider is None else _path(provider, path.parent, "provider_config"),
        node=value["node"], compatibility=value["compatibility"],
    )


def prepare_cli(config: Config) -> Path:
    """Return the verified cache copy, refusing source/cache upgrades or tampering."""
    if config.compatibility != COMPATIBILITY:
        raise ValueError(f"Only compatibility={COMPATIBILITY!r} is supported")
    original = read_regular(config.cli_path)
    actual = _sha256(original)
    if actual != SOURCE_SHA256:
        raise ValueError(
            "Installed ZCode CLI changed or is unsupported; review compatibility "
            f"before use. Expected {SOURCE_SHA256}, got {actual}."
        )
    if original.count(FRAGMENT) != 1:
        raise ValueError("Pinned approval-mapping fragment must occur exactly once")
    patched = original.replace(FRAGMENT, b"", 1)
    if _sha256(patched) != PATCHED_SHA256:
        raise ValueError("Unexpected patched CLI digest")
    source_diff = _source_diff()
    manifest = {
        "schema_version": 1, "cli_version": CLI_VERSION,
        "official_source_commit": UPSTREAM_SHA,
        "official_source_path": "apps/zcode-cli/packages/bootstrap/src/app/app-config-options.ts",
        "source_sha256": SOURCE_SHA256, "patched_sha256": PATCHED_SHA256,
        "source_diff_sha256": _sha256(source_diff),
        "removed_fragment_sha256": _sha256(FRAGMENT), "removed_fragment_matches": 1,
        "patch": "Remove toolAllowlist to permission.allowedTools conversion only",
        "installation_modified": False, "credentials_copied": False,
    }
    ensure_directory(config.cache_dir)
    runtime = config.cache_dir / f"zcode-{CLI_VERSION}-{SOURCE_SHA256}"
    ensure_directory(runtime)
    output, manifest_path = runtime / "zcode.cjs", runtime / "manifest.json"
    if output.exists() or output.is_symlink():
        if _sha256(read_regular(output)) != PATCHED_SHA256:
            raise ValueError(f"Local protocol CLI was modified: {output}")
    else:
        atomic_write(output, patched)
    if manifest_path.exists() or manifest_path.is_symlink():
        try:
            existing = json.loads(read_regular(manifest_path))
        except (json.JSONDecodeError, UnicodeDecodeError) as error:
            raise ValueError(f"Invalid local protocol CLI provenance: {manifest_path}") from error
        if existing != manifest:
            raise ValueError(f"Local protocol CLI provenance changed: {manifest_path}")
    else:
        atomic_write(manifest_path, (json.dumps(manifest, indent=2) + "\n").encode())
    if _sha256(read_regular(output)) != PATCHED_SHA256:
        raise ValueError("Local protocol CLI digest failed after preparation")
    return output
