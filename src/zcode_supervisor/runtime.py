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
import sys

from .common import atomic_write, ensure_directory, read_regular, require_posix


CLI_VERSION = "0.16.9"
FRAGMENT = b"e.runtimeConfig?.toolAllowlist&&(n.allowedTools=[...e.runtimeConfig.toolAllowlist]),"
COMPATIBILITY = "pinned-0.16.9"
# Cache layout version. Layout v1 dirs (written by the single-pin releases, whose
# manifests lack source_revision_status/reviewed/provenance) are never read or
# modified by this version: a fresh v2 directory is prepared from the verified
# installed bundle instead. Old caches stay on disk as retained evidence; a
# tampered v2 manifest or patched copy is still rejected, never repaired.
CACHE_LAYOUT = 2

# Reviewed bundle registry: installed-bundle SHA256 -> pin record. Each pin was
# accepted only after the real-hardware steps in docs/COMPATIBILITY.md; any
# other fingerprint fails closed. ``upstream_commit`` is the verified source
# revision or None when it could NOT be confirmed — recorded as unknown, never
# described as a mere repack of a previously reviewed build. ``cli_version`` is
# the ACTUAL version string of that bundle and drives cache/manifest naming;
# legacy fixture pins without the key keep their old 0.16.9 semantics. The
# COMPATIBILITY constant below is a legacy configuration-policy identifier
# carried from the single-pin releases, NOT a claim that every pinned CLI is
# 0.16.9 — fingerprints remain the authority.
REVIEWED_BUNDLES = {
    "b816e38656e70fb585fe2ab1ea010da89cda928dac1ccb5d83255fd18796da6e": {
        "patched_sha256": "31ef7f33f40591303047db5135845bd39b47399372110483fe6cf6c2df4328d2",
        "upstream_commit": "aac4755666d09fdcd70272fcf063c077a639015f",
        "reviewed": "2026-10-13",
        "provenance": "official source commit verified; desktop 3.15.1 installation",
        "cli_version": "0.16.9",
    },
    "31965ce1d5d4a808ef156784f67996de42ccab335591d2386a61ea8f6f506a54": {
        "patched_sha256": "21ef67a4152d9184009613c7edd187063283dbe92a9db583618b375f1f1e2900",
        "upstream_commit": None,
        "reviewed": "2026-10-14",
        "provenance": ("installed-bundle fingerprint replaced the 2026-10-13 review; CLI "
                       "version string 0.16.9 and the single allowlist-conversion fragment "
                       "verified; the underlying source revision could NOT be confirmed "
                       "(unknown). Accepted after the real create/resume/approve/deny/"
                       "tamper/receipt checks recorded in docs/COMPATIBILITY.md"),
        "cli_version": "0.16.9",
    },
    "a055234a6ca3da0172616d3987cf3c617b6a26deb1332d69035300c2a0ae0de1": {
        "patched_sha256": "87e3e4bfb2292ea75b3fb7398ab7c1490ecbe0848785bf69e0d99b893e27ab4c",
        "upstream_commit": None,
        "reviewed": "2026-10-20",
        "provenance": ("installed-bundle fingerprint from Desktop 3.16.2 / build 3.16.2.8769; CLI "
                       "version string 0.17.0 and the single allowlist-conversion fragment "
                       "verified (same one-fragment deletion as the 0.16.9 pins); the "
                       "underlying source revision could NOT be confirmed (unknown), so this "
                       "is not described as a repack or a verified commit. Accepted after the "
                       "static review, offline baseline and bounded live allow/resume/deny/"
                       "model-timeout evidence recorded under "
                       "the underworld2-2.17.3 repository's out/zcode-compatibility-20261020"),
        "cli_version": "0.17.0",
    },
    "97490d0f06a7a225d54d47817acf4b4859cdb268b59f2490caaeda982bef063a": {
        "patched_sha256": "e27dcb3cb77d21a49fef9eb4b3541a25cac7a0f829ea99e6ba45356456fd42b8",
        "upstream_commit": None,
        "reviewed": "2026-10-24",
        "provenance": ("installed-bundle fingerprint from Desktop 3.16.3 / build 3.16.3.8992; CLI "
                       "version string 0.17.0 and the single allowlist-conversion fragment "
                       "verified (same one-fragment deletion as the earlier pins); the "
                       "underlying source revision could NOT be confirmed (unknown), so this "
                       "is not described as a repack or a verified commit. Accepted after the "
                       "static review, offline tamper check and bounded live doctor/probe/"
                       "allow+deny/model-selection run evidence recorded under "
                       "the underworld2-2.17.3 repository's out/zcode-compatibility-20261024"),
        "cli_version": "0.17.0",
    },
}
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
    notify: bool = False
    profile: Path | None = None
    config_dir: Path | None = None

    def __post_init__(self) -> None:
        require_posix()
        if self.compatibility != COMPATIBILITY:
            raise ValueError(f"Only compatibility={COMPATIBILITY!r} is supported")
        if type(self.notify) is not bool:
            raise ValueError("notify must be a boolean")
        if not isinstance(self.node, str) or not self.node.strip() or "\x00" in self.node:
            raise ValueError("node must be a nonempty executable name or path")
        for name in ("work_root", "state_dir", "cache_dir", "cli_path", "provider_config",
                     "profile", "config_dir"):
            path = getattr(self, name)
            if name == "provider_config" and path is None:
                continue
            if name in ("profile", "config_dir") and path is None:
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
    allowed = required | {"provider_config", "notify", "profile"}
    if not isinstance(value, dict):
        raise ValueError("Configuration must be a JSON object")
    unknown, missing = set(value) - allowed, required - set(value)
    if unknown or missing:
        raise ValueError(f"Invalid configuration keys: unknown={sorted(unknown)}, missing={sorted(missing)}")
    if type(value["schema_version"]) is not int or value["schema_version"] != 1:
        raise ValueError("Only configuration schema_version=1 is supported")
    provider = value.get("provider_config")
    notify = value.get("notify", sys.platform == "darwin")
    if type(notify) is not bool:
        raise ValueError("notify must be a boolean when present")
    profile = value.get("profile")
    return Config(
        work_root=_path(value["work_root"], path.parent, "work_root"),
        state_dir=_path(value["state_dir"], path.parent, "state_dir"),
        cache_dir=_path(value["cache_dir"], path.parent, "cache_dir"),
        cli_path=_path(value["cli_path"], path.parent, "cli_path"),
        provider_config=None if provider is None else _path(provider, path.parent, "provider_config"),
        node=value["node"], compatibility=value["compatibility"], notify=notify,
        profile=None if profile is None else _path(profile, path.parent, "profile"),
        config_dir=path.parent,
    )


BUNDLED_SKILL_FILES = ("SKILL.md", "patterns.md", "examples.md", "manifest.json")


def _verify_closure_manifest(target: Path) -> None:
    """Verify a frozen closure against its freeze-time manifest (read-only)."""
    import stat as stat_module
    manifest_path = target / "czs-closure-manifest.json"
    if not manifest_path.is_file():
        raise ValueError("Frozen closure lacks its trusted manifest; refusing")
    manifest_bytes = manifest_path.read_bytes()
    # Anchor check (N4): the content-addressed directory name must reproduce
    # from the CURRENT tree — recompute the member digest over the actual
    # files and fold in the CURRENT manifest bytes; a member+manifest rewrite
    # or a deleted member+entry no longer matches the anchored name.
    # Enumerate in the SAME order freeze used: root entries first (sorted),
    # then per-skill members in BUNDLED_SKILL_FILES order — otherwise the
    # recomputed digest diverges on ordering alone.
    present = []
    for entry in sorted(target.iterdir()):
        if entry.is_file() and entry.name != "czs-closure-manifest.json":
            present.append(entry)
    for skill_dir in sorted((target / "skills").iterdir()) if (target / "skills").is_dir() else []:
        for member in BUNDLED_SKILL_FILES:
            candidate = skill_dir / member
            if candidate.is_file() and candidate.name != "czs-closure-manifest.json":
                present.append(candidate)
    recomputed = hashlib.sha256()
    manifest_present = json.loads(manifest_bytes)
    members_present = manifest_present.get("members", {})
    for path in present:
        rel = str(path.relative_to(target))
        if rel not in members_present:
            raise ValueError(f"Closure contains an unlisted member: {rel}")
        data = path.read_bytes()
        if '/' not in rel:
            recomputed.update(b"<root>" + rel.encode())
        else:
            skill_name, member = rel.split('/')[1], rel.split('/')[2]
            recomputed.update(skill_name.encode()); recomputed.update(member.encode())
        recomputed.update(str(len(data)).encode()); recomputed.update(data)
    # Extra files the freeze never saw (e.g. dropped into the frozen tree)
    # break the member-set equality below, but catch them explicitly here.
    listed = {str(p.relative_to(target)) for p in present}
    extra = {str(p.relative_to(target)) for p in target.rglob("*")
             if p.is_file() and p.name != "czs-closure-manifest.json"} - listed
    if extra:
        raise ValueError(f"Closure contains unexpected files: {sorted(extra)}")
    anchored = hashlib.sha256(recomputed.digest()
                              + hashlib.sha256(manifest_bytes).hexdigest().encode()).hexdigest()
    expected_name = f"skills-{anchored[:16]}"
    if target.name != expected_name:
        raise ValueError(f"Closure anchor mismatch: directory is {target.name}, content "
                         f"re-hashes to {expected_name}; refusing")
    if sorted(members_present) != sorted(str(p.relative_to(target)) for p in present):
        raise ValueError("Closure member set differs from the manifest map")
    try:
        manifest = json.loads(manifest_bytes)
    except ValueError as error:
        raise ValueError("Closure manifest unreadable") from error
    members = manifest.get("members") if isinstance(manifest, dict) else None
    if not isinstance(members, dict) or not members:
        raise ValueError("Closure manifest has no member map")
    for rel, expected in members.items():
        member = target / rel
        try:
            info = member.lstat()
        except OSError as error:
            raise ValueError(f"Closure member missing: {rel}") from error
        if not stat_module.S_ISREG(info.st_mode):
            raise ValueError(f"Closure member {rel} is not a regular file (symlink refused)")
        if not isinstance(expected, str) \
                or hashlib.sha256(member.read_bytes()).hexdigest() != expected:
            raise ValueError(f"Closure member {rel} drifted from its frozen digest")


def verify_bundled_skills(config: Config) -> Path:
    """Verify the cache-level bundled-skills binding WITHOUT mutating it.

    Returns the current link target. Unlike prepare_bundled_skills this never
    re-freezes, never retargets the link and never switches sources: if the
    link is missing, swapped or its closure drifted, that is a hard error the
    caller reports — auto-repair here would mask exactly the drift the
    lifecycle contract exists to catch (N4).
    """
    import stat as stat_module
    link = Path(config.cache_dir) / "bundled-skills"
    if not link.is_symlink():
        raise ValueError("cache bundled-skills link is missing or not a symlink")
    target = link.resolve()
    if not target.is_dir() or not target.name.startswith("skills-"):
        raise ValueError("cache bundled-skills link does not point at a frozen closure")
    _verify_closure_manifest(target)
    return target


def closure_digest(target: Path) -> str:
    """Content digest of a frozen closure directory (for binding receipts)."""
    digest = hashlib.sha256()
    for path in sorted(target.rglob("*")):
        if path.is_file():
            digest.update(str(path.relative_to(target)).encode())
            data = path.read_bytes()
            digest.update(str(len(data)).encode()); digest.update(data)
    return digest.hexdigest()


def _closure_approval_path(config: Config) -> Path:
    return Path(config.state_dir) / "skill-closure-approval.json"


def read_closure_approval(config: Config) -> dict | None:
    """Read the controller-side approved closure reference, or None if absent.

    The record lives in the trusted state directory (controller state, never
    the task workspace or the swappable cache). A PRESENT but malformed
    record is a hard error: treating corruption as "no reference" would let
    the current cache link redefine approval — exactly what N4 forbids.
    """
    path = _closure_approval_path(config)
    if not path.exists():
        return None
    try:
        value = json.loads(read_regular(path))
    except (json.JSONDecodeError, UnicodeDecodeError) as error:
        raise ValueError("Skill-closure approval record is unreadable; refusing "
                         "to fall back to the untrusted cache link") from error
    if not isinstance(value, dict) or value.get("schema_version") != 1 \
            or not isinstance(value.get("target"), str) or not value["target"].startswith("/") \
            or not isinstance(value.get("digest"), str) or len(value.get("digest", "")) != 64:
        raise ValueError("Skill-closure approval record is malformed; refusing "
                         "to fall back to the untrusted cache link")
    return value


def _write_closure_approval(config: Config, target: Path) -> None:
    """Anchor WHICH closure this controller approved (trusted state write).

    Called only from prepare_bundled_skills — the controller provisioning
    path — after the frozen tree verified against its own manifest. ensure
    never rewrites it: task-time code compares against this reference and
    refuses on any mismatch instead of redefining approval.
    """
    record = {"schema_version": 1, "anchor": target.name,
              "target": str(target.resolve()), "digest": closure_digest(target)}
    ensure_directory(Path(config.state_dir))
    atomic_write(_closure_approval_path(config),
                 (json.dumps(record, sort_keys=True, indent=2) + "\n").encode())


def ensure_bundled_skills(config: Config) -> Path | None:
    """Bind the task to the CONTROLLER-APPROVED closure, verifying only.

    N4: a self-consistent content-addressed closure proves nothing about
    approval for this task. The trusted reference is the controller-side
    approval record written at prepare time; the current cache link must
    resolve to exactly that target with exactly those bytes before the
    worker can read a single skill. A link with no approval record behind
    it, a retargeted link, or drifted content is a hard error — never an
    adoption of whatever the link currently points at, and never an
    auto-repair. First use (no record AND no link) prepares from the
    configured reviewed source and records the approval atomically.
    """
    record = read_closure_approval(config)
    link = Path(config.cache_dir) / "bundled-skills"
    if record is None:
        if link.is_symlink() or link.exists():
            raise ValueError("cache bundled-skills link exists without a controller "
                             "approval record; refusing to let the current link define "
                             "what is approved")
        return prepare_bundled_skills(config)
    if not link.is_symlink():
        raise ValueError("cache bundled-skills link is missing or not a symlink")
    target = link.resolve()
    if str(target) != record["target"]:
        raise ValueError(f"cache bundled-skills link no longer points at the "
                         f"controller-approved closure: approved {record['target']}, "
                         f"found {target}; refusing")
    _verify_closure_manifest(target)
    if closure_digest(target) != record["digest"]:
        raise ValueError("frozen closure content differs from the controller-approved "
                         "digest; refusing")
    return link


def prepare_bundled_skills(config: Config, source: Path | None = None) -> Path:
    """Materialize a content-addressed private copy of the bundled skills.

    The installed app's bundled-skills tree is a live dependency: the runtime
    only pins CLI bytes, so a skill-content change or a retargeted symlink
    would silently change what workers load. This freezes the four-file
    closure per skill into ``<cache>/skills-<sha>/`` (the sha covers the
    concatenated closure), points the cache-level ``bundled-skills`` entry at
    it, and refuses drift: a changed closure or a non-regular-file link
    raises instead of serving unknown content. The installed application and
    the user global config are never touched. Successful provisioning also
    records the approved closure reference (target + digest) in the trusted
    state directory, which task-time ensure_bundled_skills compares the
    cache link against — the link never defines its own approval (N4).
    """
    import hashlib
    import shutil as _shutil
    app_root = source or Path("/Applications/ZCode.app/Contents/Resources/glm/"
                              "packages/bundled-skills")
    skills_root = app_root / "skills"
    if not skills_root.is_dir():
        raise ValueError("Installed bundled-skills tree not found; refusing to guess")
    import stat as stat_module
    def _regular(path: Path) -> bytes | None:
        # both the live source and the frozen copy must be real files; a
        # symlink with identical content is NOT accepted (N4).
        try:
            info = path.lstat()
        except OSError:
            return None
        if not stat_module.S_ISREG(info.st_mode):
            return None
        return path.read_bytes()

    digest = hashlib.sha256()
    names = []
    closure_entries = []
    for entry in sorted(skills_root.parent.iterdir()):
        if entry.is_dir() and entry.name == "skills":
            continue
        if entry.is_file() or entry.is_symlink():
            data = _regular(entry)
            if data is None:
                raise ValueError(f"bundled-skills closure root member {entry.name} is not a "
                                 "regular file; refusing to freeze")
            digest.update(b"<root>" + entry.name.encode())
            digest.update(str(len(data)).encode()); digest.update(data)
            closure_entries.append((entry, data, 'root'))
    for skill_dir in sorted(p for p in skills_root.iterdir() if p.is_dir()):
        for member in BUNDLED_SKILL_FILES:
            path = skill_dir / member
            data = _regular(path)
            if data is None:
                continue
            digest.update(skill_dir.name.encode()); digest.update(member.encode())
            digest.update(str(len(data)).encode()); digest.update(data)
            names.append(f"{skill_dir.name}/{member}")
            closure_entries.append((path, data, 'skill'))
    # The manifest is part of the identity: finish the member digest, build
    # the manifest from the SAME entries, then fold the manifest sha into the
    # final content-addressed name.
    member_digest = digest.digest()
    manifest = {"members": {}}
    for entry, data, kind in closure_entries:
        rel = entry.name if kind == 'root' else f"skills/{entry.parent.name}/{entry.name}"
        manifest["members"][rel] = hashlib.sha256(data).hexdigest()
    manifest_bytes = (json.dumps(manifest, sort_keys=True, indent=1) + "\n").encode()
    manifest_sha = hashlib.sha256(manifest_bytes).hexdigest()
    closure_sha = hashlib.sha256(member_digest + manifest_sha.encode()).hexdigest()
    target = Path(config.cache_dir) / f"skills-{closure_sha[:16]}"
    if not target.exists():
        staged = target.with_name(target.name + ".partial")
        if staged.exists():
            _shutil.rmtree(staged)
        staged.mkdir(parents=True)
        for entry, data, kind in closure_entries:
            if data is None:
                continue
            if kind == 'root':
                out = staged / entry.name
            else:
                out = staged / "skills" / entry.parent.name / entry.name
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_bytes(data)
        (staged / "czs-closure-manifest.json").write_bytes(manifest_bytes)
        staged.rename(target)
    # verify the frozen copy against the trusted manifest: member set must
    # match exactly, every member a regular file (symlinks refused) and bytes
    # re-hashing to the freeze-time digests. Tampering before any task binds
    # is caught here because the manifest itself is part of the closure sha
    # directory name — a re-frozen tampered tree lands in a DIFFERENT dir and
    # the link no longer matches the task binding.
    _verify_closure_manifest(target)
    link = Path(config.cache_dir) / "bundled-skills"
    if link.is_symlink() and link.resolve() != target.resolve():
        link.unlink()
    if link.exists() and not link.is_symlink():
        _shutil.rmtree(link)
    if not link.is_symlink():
        link.symlink_to(target, target_is_directory=True)
    # N4: anchor WHICH closure this controller provisioned, in trusted
    # controller state. Task-time ensure compares the cache link against
    # this reference; the link itself never redefines approval.
    _write_closure_approval(config, target)
    return link


def prepare_cli(config: Config) -> Path:
    """Return the verified cache copy, refusing source/cache upgrades or tampering."""
    if config.compatibility != COMPATIBILITY:
        raise ValueError(f"Only compatibility={COMPATIBILITY!r} is supported")
    original = read_regular(config.cli_path)
    actual = _sha256(original)
    pin = REVIEWED_BUNDLES.get(actual)
    if pin is None:
        raise ValueError(
            "Installed ZCode CLI changed or is unsupported; review compatibility "
            f"before use. Reviewed bundles: {sorted(REVIEWED_BUNDLES)}. Got {actual}."
        )
    # Legacy fixture pins recorded no explicit cli_version; keep their old
    # 0.16.9 semantics. Everything else uses the matched pin's actual version.
    pin_cli_version = pin.get("cli_version", CLI_VERSION)
    if original.count(FRAGMENT) != 1:
        raise ValueError("Pinned approval-mapping fragment must occur exactly once")
    patched = original.replace(FRAGMENT, b"", 1)
    if _sha256(patched) != pin["patched_sha256"]:
        raise ValueError("Unexpected patched CLI digest")
    source_diff = _source_diff()
    manifest = {
        "schema_version": 1, "cli_version": pin_cli_version,
        "official_source_commit": pin["upstream_commit"],
        "source_revision_status": "verified" if pin["upstream_commit"] else "unknown",
        "official_source_path": "apps/zcode-cli/packages/bootstrap/src/app/app-config-options.ts",
        "source_sha256": actual, "patched_sha256": pin["patched_sha256"],
        "source_diff_sha256": _sha256(source_diff),
        "removed_fragment_sha256": _sha256(FRAGMENT), "removed_fragment_matches": 1,
        "patch": "Remove toolAllowlist to permission.allowedTools conversion only",
        "reviewed": pin["reviewed"], "provenance": pin["provenance"],
        "installation_modified": False, "credentials_copied": False,
    }
    ensure_directory(config.cache_dir)
    runtime = config.cache_dir / f"zcode-{pin_cli_version}-{actual}-v{CACHE_LAYOUT}"
    ensure_directory(runtime)
    output, manifest_path = runtime / "zcode.cjs", runtime / "manifest.json"
    if output.exists() or output.is_symlink():
        if _sha256(read_regular(output)) != pin["patched_sha256"]:
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
    if _sha256(read_regular(output)) != pin["patched_sha256"]:
        raise ValueError("Local protocol CLI digest failed after preparation")
    return output
