"""Host-bound project profile indexing; profiles point at authority, never replace it."""

from __future__ import annotations

import hashlib
import os
from pathlib import Path, PurePosixPath
import stat

from .work_contracts import canonical_sha256
from .work_models import WorkError


def _relative(value):
    if not isinstance(value, str) or not value or "\x00" in value:
        raise WorkError("schema-invalid", "profile paths must be non-empty relative strings")
    path = PurePosixPath(value)
    if path.is_absolute() or ".." in path.parts:
        raise WorkError("scope-conflict", "profile path escapes its registered root")
    return path


def _hash_file(path):
    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(path, flags)
        try:
            info = os.fstat(fd)
            if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                raise WorkError("source-path-invalid", "profile document must be a single-link regular file")
            digest = hashlib.sha256()
            total = 0
            while True:
                chunk = os.read(fd, 1024 * 1024)
                if not chunk:
                    break
                total += len(chunk)
                if total > 16 * 1024 * 1024:
                    raise WorkError("source-limit", "profile document exceeds 16 MiB")
                digest.update(chunk)
            after = os.fstat(fd)
            if (info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns) != (
                    after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns):
                raise WorkError("source-drift", "profile document changed while being indexed")
            return digest.hexdigest(), total
        finally:
            os.close(fd)
    except WorkError:
        raise
    except OSError as error:
        raise WorkError("source-unreadable", f"cannot index profile document: {error}") from error


def register_profile(profile, root_bindings, actor) -> dict:
    """Return a deterministic, path-free ProfileRef using host-registered roots only."""
    actor.require("profile:register")
    if not isinstance(profile, dict) or profile.get("schema_version") != "zsc.project-profile/v1" \
            or not isinstance(profile.get("project_id"), str) or not profile["project_id"] \
            or not isinstance(profile.get("revision"), int) or profile["revision"] < 1:
        raise WorkError("schema-invalid", "invalid project profile identity")
    roots = profile.get("root_refs")
    documents = profile.get("documents")
    if not isinstance(roots, list) or not roots or not isinstance(documents, list) \
            or not isinstance(root_bindings, dict):
        raise WorkError("schema-invalid", "profile root bindings and documents are required")
    normalized_roots = {}
    for ref in roots:
        if not isinstance(ref, dict) or not isinstance(ref.get("root_id"), str):
            raise WorkError("schema-invalid", "invalid root_ref")
        root_id = ref["root_id"]
        relative = _relative(ref.get("path"))
        if root_id in normalized_roots or root_id not in root_bindings:
            raise WorkError("scope-conflict", f"unregistered or duplicate root {root_id}")
        base = Path(root_bindings[root_id]).expanduser().resolve(strict=True)
        if not base.is_dir():
            raise WorkError("scope-conflict", "registered profile root is not a directory")
        target = (base / Path(*relative.parts)).resolve(strict=False)
        if target != base and base not in target.parents:
            raise WorkError("scope-conflict", "profile root leaves its host binding")
        normalized_roots[root_id] = (ref, base, target)
    indexed = []
    for document in documents:
        if not isinstance(document, dict) or not isinstance(document.get("role"), str):
            raise WorkError("schema-invalid", "invalid profile document")
        root_id = document.get("root_id", next(iter(normalized_roots)) if len(normalized_roots) == 1 else None)
        if root_id not in normalized_roots:
            raise WorkError("scope-conflict", "document root is not registered")
        relative = _relative(document.get("path"))
        _, _, root_path = normalized_roots[root_id]
        target = (root_path / Path(*relative.parts)).resolve(strict=False)
        if root_path != target and root_path not in target.parents:
            raise WorkError("scope-conflict", "profile document escapes its root")
        digest, size = _hash_file(target)
        indexed.append({"root_id": root_id, "path": relative.as_posix(),
                        "role": document["role"], "sha256": digest, "size": size})
    for field in ("decision_refs", "domains"):
        if not isinstance(profile.get(field), list) \
                or any(not isinstance(item, str) or not item for item in profile[field]):
            raise WorkError("schema-invalid", f"profile {field} must be strings")
    checks = profile.get("checks")
    if not isinstance(checks, list) or any(not isinstance(item, dict)
            or not isinstance(item.get("registered_command_id"), str) for item in checks):
        raise WorkError("schema-invalid", "profile checks must reference registered commands")
    exported = {
        "schema_version": "zsc.profile-ref/v1", "project_id": profile["project_id"],
        "revision": profile["revision"],
        "root_refs": [normalized_roots[key][0] for key in sorted(normalized_roots)],
        "documents": sorted(indexed, key=lambda item: (item["root_id"], item["path"])),
        "checks": checks, "decision_refs": list(profile["decision_refs"]),
        "domains": sorted(set(profile["domains"])), "authority": "source_documents_remain_authoritative",
    }
    exported["profile_sha256"] = canonical_sha256(exported)
    return exported
