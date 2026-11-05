"""Shared strict-contract primitives for the P0-A work-orientation services.

WorkInputError carries a stable code plus a list of issues, each issue being
``{"code": str, "path": str, "message": str}``. Path anchors: ``$`` is the
document root; ``practices[i].field``-style paths point into collections.

parse_json_document performs byte-level admission only (size, UTF-8,
duplicate object keys, non-finite numbers, top-level object); it never opens
files — callers read to their own byte budgets and pass the bytes in.
canonical_sha256 reuses contracts.canonical_json so every digest in this
layer is byte-identical to the existing contract digests.

This module has no directory or runtime dependencies and must not import
the catalog/orientation/brief modules.
"""

from __future__ import annotations

import hashlib
import json

from .contracts import canonical_json


TASK_KINDS = ("new_method", "feature", "debug", "performance",
              "verification", "calibration", "operations")
FEATURE_KEYS = ("moving_state", "coupled_processes", "distributed",
                "observations", "compiled_backend")


class _DuplicateKey(ValueError):
    def __init__(self, key):
        super().__init__(f"duplicate object key: {key}")
        self.key = key


class _NonFinite(ValueError):
    def __init__(self, token):
        super().__init__(f"non-finite number: {token}")
        self.token = token


class WorkInputError(ValueError):
    """Validation failure with a stable code and located issues."""

    def __init__(self, code: str, issues: list[dict]):
        super().__init__(f"{code}: {issues}")
        self.code = code
        self.issues = issues


def issue(code: str, path: str, message: str) -> dict:
    return {"code": code, "path": path, "message": message}


def _reject_constant(token: str):
    raise _NonFinite(token)


def _reject_overflow(token: str):
    number = float(token)
    if number != number or number in (float("inf"), float("-inf")):
        raise _NonFinite(token)
    return number


def _object_pairs(pairs):
    mapping = {}
    for key, value in pairs:
        if key in mapping:
            raise _DuplicateKey(key)
        mapping[key] = value
    return mapping


def parse_json_document(data: bytes, *, max_bytes: int) -> dict:
    """Strict JSON admission: size, UTF-8, duplicate keys, non-finite."""
    if not isinstance(data, (bytes, bytearray)):
        raise WorkInputError("request-invalid",
                             [issue("not-bytes", "$",
                                    "input must be bytes")])
    if len(data) > max_bytes:
        raise WorkInputError("request-too-large",
                             [issue("too-large", "$",
                                    f"{len(data)} bytes exceeds the "
                                    f"{max_bytes}-byte limit")])
    try:
        text = data.decode("utf-8")
    except UnicodeDecodeError as error:
        raise WorkInputError("request-invalid",
                             [issue("not-utf8", "$",
                                    f"invalid UTF-8: {error}"[:300])]) from error
    try:
        document = json.loads(text, object_pairs_hook=_object_pairs,
                              parse_constant=_reject_constant,
                              parse_float=_reject_overflow)
    except _DuplicateKey as error:
        raise WorkInputError("request-invalid",
                             [issue("duplicate-key", "$", str(error))]) from error
    except _NonFinite as error:
        raise WorkInputError("request-invalid",
                             [issue("non-finite", "$", str(error))]) from error
    except json.JSONDecodeError as error:
        raise WorkInputError("request-invalid",
                             [issue("not-json", "$",
                                    f"invalid JSON: {error}"[:300])]) from error
    except RecursionError as error:
        raise WorkInputError("request-invalid",
                             [issue("too-deep", "$",
                                    "JSON nesting exceeds the supported "
                                    "depth")]) from error
    if not isinstance(document, dict):
        raise WorkInputError("request-invalid",
                             [issue("not-an-object", "$",
                                    "top-level JSON value must be an object")])
    try:
        canonical_json(document).encode("utf-8")
    except (UnicodeEncodeError, ValueError) as error:
        raise WorkInputError("request-invalid",
                             [issue("not-encodable", "$",
                                    f"document is not UTF-8 encodable: "
                                    f"{error}"[:300])]) from error
    return document


def canonical_sha256(document: dict) -> str:
    """sha256 over the canonical JSON form (contracts.canonical_json)."""
    return hashlib.sha256(canonical_json(document).encode("utf-8")).hexdigest()


def _read_capped(path, cap: int) -> bytes:
    """Read at most ``cap`` bytes (admission seam; keep budgets enforced
    here instead of slurping whole files first)."""
    with open(path, "rb") as stream:
        return stream.read(cap)


def read_json_input(path, *, max_bytes: int) -> dict:
    """Admission-controlled JSON file read: regular-file check, capped
    read (limit + 1 byte for overrun detection), strict parse."""
    import os
    from pathlib import Path

    path = Path(path)
    try:
        path.stat()
    except OSError as error:
        raise WorkInputError(
            "request-invalid",
            [issue("not-readable", "$",
                   f"cannot access input file: "
                   f"{type(error).__name__}: {error}"[:300])]) from error
    if not os.path.isfile(path) or path.is_symlink():
        raise WorkInputError(
            "request-invalid",
            [issue("not-a-regular-file", "$",
                   "input must be a regular, non-symlinked file")])
    try:
        data = _read_capped(path, max_bytes + 1)
    except OSError as error:
        raise WorkInputError(
            "request-invalid",
            [issue("not-readable", "$",
                   f"cannot read input file: "
                   f"{type(error).__name__}: {error}"[:300])]) from error
    if len(data) > max_bytes:
        raise WorkInputError(
            "request-too-large",
            [issue("too-large", "$",
                   f"input exceeds the {max_bytes}-byte limit")])
    return parse_json_document(data, max_bytes=max_bytes)
