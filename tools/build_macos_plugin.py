#!/usr/bin/env python3
"""Build a deterministic, allowlisted local Codex plugin from accepted Git blobs."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import tempfile
import zipfile


REPOSITORY = Path(__file__).resolve().parent.parent
PLUGIN = "zcode-supervisor"
MARKETPLACE = "zcode-supervisor-local"
TEMPLATE_FILES = {
    "plugin.json", ".codex-plugin/plugin.json", "scripts/czs.py",
    "skills/zcode-supervisor/SKILL.md", "skills/zcode-supervisor/agents/openai.yaml",
    "skills/zcode-supervisor/references/delegation.md",
}


def template_files(template: Path) -> list[Path]:
    files = []
    for path in sorted(template.rglob("*")):
        if path.is_symlink():
            raise ValueError("Plugin source contains a symlink")
        if path.is_file():
            files.append(path)
    if {p.relative_to(template).as_posix() for p in files} != TEMPLATE_FILES:
        raise ValueError("Plugin template has unexpected or missing files; update the explicit release allowlist")
    return files


def git_blob(repo: Path, path: str, expected: str) -> bytes:
    record = subprocess.run(["git", "ls-files", "--stage", "--", path], cwd=repo,
                            capture_output=True, text=True, check=True).stdout.strip()
    metadata, actual_path = record.split("\t", 1)
    mode, blob, stage = metadata.split()
    if stage != "0" or actual_path != path or blob != expected or mode != "100644":
        raise ValueError(f"Accepted index blob changed: {path}; review before packaging")
    return subprocess.run(["git", "cat-file", "blob", blob], cwd=repo,
                          capture_output=True, check=True).stdout


def write(root: Path, path: str, data: bytes, executable=False) -> None:
    target = root / path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(data)
    target.chmod(0o755 if executable else 0o644)


def build(repo: Path, output: Path) -> Path:
    pins = json.loads((repo / "packaging/accepted-core.json").read_text())
    template = repo / "plugins" / PLUGIN
    portable = json.loads((template / "plugin.json").read_text())
    legacy = json.loads((template / ".codex-plugin/plugin.json").read_text())
    version = portable["version"]
    if portable["name"] != PLUGIN or legacy["name"] != PLUGIN or legacy["version"] != version:
        raise ValueError("Plugin manifests disagree")
    output = output.expanduser().absolute()
    output.mkdir(parents=True, exist_ok=True)
    release_name = PLUGIN + "-" + version
    archive = output / (PLUGIN + "-macos-" + version + ".zip")
    if archive.exists() or (output / release_name).exists():
        raise ValueError("Output already exists; choose a fresh output directory")
    with tempfile.TemporaryDirectory(prefix=".czs-build-", dir=output) as temporary:
        release = Path(temporary) / release_name
        release.mkdir()
        for path in template_files(template):
            rel = path.relative_to(template)
            write(release, f"plugins/{PLUGIN}/{rel.as_posix()}", path.read_bytes())
        for path, record in pins["files"].items():
            data = git_blob(repo, path, record["blob"])
            if hashlib.sha256(data).hexdigest() != record["sha256"]:
                raise ValueError("Git blob SHA256 mismatch")
            relative = path.removeprefix("src/")
            write(release, f"plugins/{PLUGIN}/lib/{relative}", data)
        for name in ("LICENSE", "THIRD_PARTY_NOTICES.md"):
            data = subprocess.run(["git", "show", ":" + name], cwd=repo,
                                  capture_output=True, check=True).stdout
            write(release, name, data)
            write(release, f"plugins/{PLUGIN}/{name}", data)
        # USAGE entry-point section is rewritten for plugin users; the accepted
        # index (including the packet.model feature) is the content authority.
        for name in ("USAGE.md", "COMPATIBILITY.md"):
            data = subprocess.run(["git", "show", ":docs/" + name], cwd=repo,
                                  capture_output=True, check=True).stdout
            if name == "USAGE.md":
                text = data.decode()
                start = text.index("本机经验证入口是")
                end = text.index("配置中的", start)
                text = (text[:start] + '插件安装用户使用包内入口，无需开发机 venv 或全局 PATH：\n\n'
                        '```bash\n"<plugin-root>/scripts/python.sh" "<plugin-root>/scripts/czs.py" \\\n'
                        '  --config /absolute/local-project/.czs/config.json <subcommand>\n```\n\n'
                        + text[end:])
                data = text.encode()
            write(release, f"plugins/{PLUGIN}/docs/{name}", data)
        write(release, f"plugins/{PLUGIN}/docs/MCP-READONLY-PILOT.md",
              (repo / "docs/MCP-READONLY-PILOT.md").read_bytes())
        for source, destination in [
            ("Install.command", "Install.command"),
            ("Check.command", "Check.command"),
            ("Validate.command", "Validate.command"),
            ("python.sh", "scripts/python.sh"),
            ("install.py", "scripts/install.py"),
            ("diagnostics.py", "scripts/diagnostics.py"),
            ("validate.py", "scripts/validate.py"),
            ("INSTALL.zh-CN.md", "INSTALL.zh-CN.md"),
            ("python.sh", f"plugins/{PLUGIN}/scripts/python.sh"),
            ("diagnostics.py", f"plugins/{PLUGIN}/scripts/diagnostics.py"),
        ]:
            write(release, destination, (repo / "packaging/macos" / source).read_bytes(),
                  executable=destination.endswith((".command", ".sh")))
        marketplace = {"name": MARKETPLACE, "interface": {"displayName": "ZCode Supervisor"},
                       "plugins": [{"name": PLUGIN, "source": {"source": "local", "path": "./plugins/" + PLUGIN},
                                    "policy": {"installation": "AVAILABLE", "authentication": "ON_INSTALL"},
                                    "category": "Developer tools"}]}
        write(release, ".agents/plugins/marketplace.json",
              (json.dumps(marketplace, ensure_ascii=False, indent=2) + "\n").encode())
        manifest = {"schema_version": 1, "name": PLUGIN, "version": version,
                    "core_version": "0.1.0",
                    "accepted_core": pins, "runtime_dependencies": ["Python>=3.11", "Node.js", "supported signed-in ZCode", "Codex plugin CLI"],
                    "model_requests_during_install": 0,
                    "scope": "Baseline CLI delegation and discoverable skill; fixed-model packet field included (verified selection plus provider receipts); no live strict budget or Codex cost claims.",
                    "files": {p.relative_to(release).as_posix(): hashlib.sha256(p.read_bytes()).hexdigest()
                              for p in sorted(release.rglob("*")) if p.is_file()}}
        write(release, "release-manifest.json", (json.dumps(manifest, ensure_ascii=False, indent=2) + "\n").encode())
        destination = output / release_name
        release.rename(destination)
        with zipfile.ZipFile(archive, "w", compression=zipfile.ZIP_DEFLATED) as bundle:
            for path in sorted(destination.rglob("*")):
                if not path.is_file():
                    continue
                entry = zipfile.ZipInfo(release_name + "/" + path.relative_to(destination).as_posix(), (1980, 1, 1, 0, 0, 0))
                entry.create_system = 3
                entry.external_attr = ((0o100755 if path.suffix in (".command", ".sh") else 0o100644) << 16)
                bundle.writestr(entry, path.read_bytes(), compress_type=zipfile.ZIP_DEFLATED)
        checksum = hashlib.sha256(archive.read_bytes()).hexdigest()
        (output / (archive.name + ".sha256")).write_text(checksum + "  " + archive.name + "\n")
    return archive


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=REPOSITORY / "dist")
    args = parser.parse_args()
    print(build(REPOSITORY, args.output))


if __name__ == "__main__":
    main()
