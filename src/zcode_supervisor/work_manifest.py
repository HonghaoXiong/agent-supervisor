"""Bounded source manifests with frozen bytes and explicit drift reporting."""

from __future__ import annotations

import base64
import hashlib
import json
import os
import stat
import subprocess
from pathlib import Path, PurePosixPath

from .contracts import canonical_json


MANIFEST_SCHEMA = "zsc.source-manifest/v1"
DRIFT_SCHEMA = "zsc.drift-report/v1"
DEFAULT_FILE_LIMIT = 16 * 1024 * 1024
DEFAULT_TOTAL_LIMIT = 64 * 1024 * 1024


class ManifestError(ValueError):
    def __init__(self, code: str, message: str):
        super().__init__(f"{code}: {message}")
        self.code = code


def _relative(value) -> str:
    if not isinstance(value, str) or "\x00" in value:
        raise ManifestError("source-path-invalid", "path must be NUL-free text")
    path = PurePosixPath(value)
    if path.is_absolute() or ".." in path.parts:
        raise ManifestError("source-path-invalid", f"unsafe relative path {value!r}")
    return str(path)


def _read_fd(fd: int, cap: int) -> bytes:
    chunks = []
    remaining = cap + 1
    while remaining:
        chunk = os.read(fd, min(remaining, 1024 * 1024))
        if not chunk:
            break
        chunks.append(chunk)
        remaining -= len(chunk)
    data = b"".join(chunks)
    if len(data) > cap:
        raise ManifestError("source-limit", "file exceeds byte limit")
    return data


def _git_inventory(root: Path):
    try:
        head = subprocess.run(["git", "-C", str(root), "rev-parse", "HEAD"],
                              capture_output=True, text=True, timeout=5, check=False)
        if head.returncode:
            return None, {}
        status = subprocess.run(["git", "-C", str(root), "status", "--porcelain=v1",
                                 "-z", "--untracked-files=all"],
                                capture_output=True, timeout=10, check=False)
        states = {}
        if status.returncode == 0:
            for record in status.stdout.split(b"\0"):
                if len(record) < 4:
                    continue
                code = record[:2].decode("ascii", "replace")
                path = record[3:].decode("utf-8", "surrogateescape")
                states[path] = "untracked" if code == "??" else "dirty"
        return head.stdout.strip(), states
    except (OSError, subprocess.TimeoutExpired):
        return None, {}


def capture_manifest(roots: dict, scope: dict) -> dict:
    if not isinstance(roots, dict) or not roots or not isinstance(scope, dict):
        raise ManifestError("source-path-invalid", "registered roots and scope are required")
    includes = scope.get("include")
    ignores = scope.get("ignore", [])
    if not isinstance(includes, list) or not includes or not isinstance(ignores, list):
        raise ManifestError("source-path-invalid", "include/ignore must be lists")
    max_file = scope.get("max_file_bytes", DEFAULT_FILE_LIMIT)
    max_total = scope.get("max_total_bytes", DEFAULT_TOTAL_LIMIT)
    if any(isinstance(v, bool) or not isinstance(v, int) or v < 1
           for v in (max_file, max_total)):
        raise ManifestError("source-limit", "invalid byte limits")
    normalized_roots = {}
    for root_id, root in roots.items():
        if not isinstance(root_id, str) or not root_id or "/" in root_id:
            raise ManifestError("source-path-invalid", "invalid root id")
        normalized_roots[root_id] = Path(root)
    include_specs = []
    for item in includes:
        if not isinstance(item, dict) or set(item) != {"root", "path"} \
                or item["root"] not in normalized_roots:
            raise ManifestError("source-path-invalid", "include must name a registered root")
        include_specs.append((item["root"], _relative(item["path"])))
    ignore_specs = []
    for item in ignores:
        if not isinstance(item, dict) or set(item) != {"root", "path"} \
                or item["root"] not in normalized_roots:
            raise ManifestError("source-path-invalid", "ignore must name a registered root")
        ignore_specs.append((item["root"], _relative(item["path"])))
    ignored_names = [f"{root}:{path}" for root, path in sorted(ignore_specs)]

    files = {}
    directories = {}
    root_heads = {}
    total = 0

    def is_ignored(root_id: str, rel: str) -> bool:
        return any(root == root_id and (rel == ignored or rel.startswith(ignored.rstrip("/") + "/"))
                   for root, ignored in ignore_specs)

    def add_file(root_id: str, rel: str, path: Path, git_states: dict):
        nonlocal total
        if is_ignored(root_id, rel):
            return
        try:
            info = path.lstat()
        except OSError as error:
            raise ManifestError("source-missing", str(error)[:300]) from error
        mode = stat.S_IMODE(info.st_mode)
        key = f"{root_id}:{rel}"
        if stat.S_ISLNK(info.st_mode):
            try:
                target = os.readlink(path)
            except OSError as error:
                raise ManifestError("source-unreadable", str(error)[:300]) from error
            raw = target.encode("utf-8", "surrogateescape")
            files[key] = {"root": root_id, "path": rel, "type": "symlink",
                          "mode": mode, "size": len(raw),
                          "sha256": hashlib.sha256(raw).hexdigest(),
                          "target": target,
                          "state": git_states.get(rel, "clean")}
            return
        if not stat.S_ISREG(info.st_mode):
            raise ManifestError("source-path-invalid", f"unsupported source type: {key}")
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_NONBLOCK", 0)
        try:
            fd = os.open(path, flags)
            try:
                before = os.fstat(fd)
                if not stat.S_ISREG(before.st_mode) or before.st_dev != info.st_dev \
                        or before.st_ino != info.st_ino:
                    raise ManifestError("source-path-invalid", f"source changed before open: {key}")
                raw = _read_fd(fd, max_file)
                after = os.fstat(fd)
                if (before.st_dev, before.st_ino, before.st_size,
                        before.st_mtime_ns) != (after.st_dev, after.st_ino,
                                                after.st_size, after.st_mtime_ns):
                    raise ManifestError("source-race", f"source changed during read: {key}")
            finally:
                os.close(fd)
        except ManifestError:
            raise
        except OSError as error:
            raise ManifestError("source-unreadable", str(error)[:300]) from error
        total += len(raw)
        if total > max_total:
            raise ManifestError("source-limit", "total source byte limit exceeded")
        entry = {"root": root_id, "path": rel, "type": "file", "mode": mode,
                 "size": len(raw), "sha256": hashlib.sha256(raw).hexdigest(),
                 "content_base64": base64.b64encode(raw).decode("ascii"),
                 "state": git_states.get(rel, "clean" if root_heads[root_id] else "untracked")}
        if info.st_nlink > 1:
            entry["hardlink_group"] = hashlib.sha256(
                f"{info.st_dev}:{info.st_ino}".encode()).hexdigest()[:16]
        files[key] = entry

    for root_id, rel in include_specs:
        root = normalized_roots[root_id]
        if root_id not in root_heads:
            root_heads[root_id], states = _git_inventory(root)
        else:
            states = {}
        target = root if rel == "." else root.joinpath(*PurePosixPath(rel).parts)
        try:
            info = target.lstat()
        except OSError as error:
            raise ManifestError("source-missing", str(error)[:300]) from error
        if stat.S_ISLNK(info.st_mode) and rel == ".":
            raise ManifestError("source-path-invalid", "root must not be a symlink")
        if stat.S_ISDIR(info.st_mode):
            for current, dirnames, filenames in os.walk(target, topdown=True,
                                                        followlinks=False):
                current_path = Path(current)
                current_rel = "." if current_path == target else current_path.relative_to(root).as_posix()
                kept = []
                for name in sorted(dirnames):
                    child_rel = name if current_rel == "." else f"{current_rel}/{name}"
                    if not is_ignored(root_id, child_rel):
                        kept.append(name)
                dirnames[:] = kept
                members = sorted(kept + filenames)
                directories[f"{root_id}:{current_rel}"] = {
                    "root": root_id, "path": current_rel, "members": members,
                    "sha256": hashlib.sha256(canonical_json(members).encode()).hexdigest(),
                }
                for name in sorted(filenames):
                    child = current_path / name
                    child_rel = name if current_rel == "." else f"{current_rel}/{name}"
                    add_file(root_id, child_rel, child, states)
        elif rel == "." or stat.S_ISLNK(info.st_mode):
            # Direct includes name authoritative regular files, never a link to one.
            raise ManifestError("source-path-invalid", f"direct include is not a regular file: {rel}")
        else:
            add_file(root_id, rel, target, states)

    manifest = {
        "schema_version": MANIFEST_SCHEMA,
        "roots": [{"id": root_id, "head": root_heads.get(root_id)}
                  for root_id in sorted(normalized_roots)],
        "scope": {"include": [{"root": root, "path": path} for root, path in include_specs],
                  "ignore": [{"root": root, "path": path} for root, path in ignore_specs]},
        "files": [files[key] for key in sorted(files)],
        "directory_members": [directories[key] for key in sorted(directories)],
        "environment": scope.get("environment", {}),
        "skill_fingerprints": scope.get("skill_fingerprints", {}),
        "checker_fingerprints": scope.get("checker_fingerprints", {}),
        "coverage": {"complete": True, "unscanned": [], "ignored": ignored_names},
        "limits": {"max_file_bytes": max_file, "max_total_bytes": max_total,
                   "frozen_file_bytes": total},
    }
    return manifest


def _file_map(manifest: dict) -> dict:
    return {f"{item['root']}:{item['path']}": item for item in manifest.get("files", [])}


def compare_manifests(base: dict, current: dict) -> dict:
    if base.get("schema_version") != MANIFEST_SCHEMA \
            or current.get("schema_version") != MANIFEST_SCHEMA:
        raise ManifestError("manifest-invalid", "both inputs must be source manifests")
    before = _file_map(base)
    after = _file_map(current)
    added = sorted(set(after) - set(before))
    removed = sorted(set(before) - set(after))
    changed = sorted(key for key in set(before) & set(after)
                     if any(before[key].get(field) != after[key].get(field)
                            for field in ("sha256", "type", "mode", "state")))
    unchanged = sorted(set(before) & set(after) - set(changed))
    base_dirs = {f"{x['root']}:{x['path']}": x["sha256"]
                 for x in base.get("directory_members", [])}
    current_dirs = {f"{x['root']}:{x['path']}": x["sha256"]
                    for x in current.get("directory_members", [])}
    coverage_fields = ("environment", "skill_fingerprints", "checker_fingerprints",
                       "scope", "coverage")
    coverage_changes = {field: {"before": base.get(field), "current": current.get(field)}
                        for field in coverage_fields if base.get(field) != current.get(field)}
    return {
        "schema_version": DRIFT_SCHEMA,
        "base_sha256": hashlib.sha256(canonical_json(base).encode()).hexdigest(),
        "current_sha256": hashlib.sha256(canonical_json(current).encode()).hexdigest(),
        "added": added, "removed": removed, "changed": changed,
        "unchanged": unchanged,
        "directory_membership_changed": base_dirs != current_dirs,
        "coverage_changes": coverage_changes,
        "equivalent": not (added or removed or changed or base_dirs != current_dirs
                           or coverage_changes),
    }
