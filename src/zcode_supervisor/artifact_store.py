"""Content-addressed, tamper-evident artifact storage with bounded paging."""

from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import stat
import tempfile
from pathlib import Path

from .contracts import canonical_json


REF_SCHEMA = "zsc.artifact-ref/v1"
PAGE_SCHEMA = "zsc.artifact-page/v1"
MAX_READ_BYTES = 65536
DEFAULT_READ_BYTES = 16384
ARTIFACT_KINDS = frozenset({
    "json", "text", "manifest", "brief", "design", "delivery",
    "acceptance", "knowledge", "interface", "report", "work-authorization",
    "work-spec", "work-event", "recon-report", "routing-policy",
    "capability", "qualification", "packet", "evidence", "evaluation",
})
_LARGE_KINDS = frozenset({"manifest", "delivery", "knowledge", "design",
                          "interface", "acceptance"})
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


class ArtifactError(ValueError):
    def __init__(self, code: str, message: str):
        super().__init__(f"{code}: {message}")
        self.code = code


def _limit_for(kind: str) -> int:
    return 16 * 1024 * 1024 if kind in _LARGE_KINDS else 1024 * 1024


class ArtifactStore:
    """Store immutable bytes below ``root/sha256/aa/<digest>``.

    Existing objects are never replaced. Publication uses a same-directory
    temporary file followed by an atomic hard-link operation, so concurrent
    publishers cannot overwrite an accepted object.
    """

    def __init__(self, root):
        self.root = Path(root)
        self.objects = self.root / "sha256"
        self.objects.mkdir(parents=True, exist_ok=True)

    def put_json(self, kind: str, document: dict) -> dict:
        if not isinstance(document, dict):
            raise ArtifactError("artifact-invalid", "JSON artifact must be an object")
        try:
            data = canonical_json(document).encode("utf-8")
        except (UnicodeEncodeError, ValueError, TypeError) as error:
            raise ArtifactError("artifact-encoding", str(error)[:300]) from error
        return self._put(kind, data)

    def put_text(self, kind: str, data) -> dict:
        if isinstance(data, str):
            try:
                data = data.encode("utf-8")
            except UnicodeEncodeError as error:
                raise ArtifactError("artifact-encoding", str(error)[:300]) from error
        if not isinstance(data, (bytes, bytearray)):
            raise ArtifactError("artifact-invalid", "text data must be str or bytes")
        data = bytes(data)
        try:
            data.decode("utf-8")
        except UnicodeDecodeError as error:
            raise ArtifactError("artifact-encoding", "text must be valid UTF-8") from error
        return self._put(kind, data)

    def _put(self, kind: str, data: bytes) -> dict:
        if kind not in ARTIFACT_KINDS:
            raise ArtifactError("artifact-kind-invalid", f"unknown kind {kind!r}")
        if len(data) > _limit_for(kind):
            raise ArtifactError("artifact-too-large", f"{len(data)} bytes exceeds kind limit")
        digest = hashlib.sha256(data).hexdigest()
        ref = {"schema_version": REF_SCHEMA, "sha256": digest, "kind": kind,
               "byte_count": len(data)}
        target = self.path_for(ref)
        if target.exists():
            self._read_bytes(ref)  # never trust a pre-existing path blindly
            return ref
        target.parent.mkdir(parents=True, exist_ok=True)
        fd, temporary = tempfile.mkstemp(prefix=f".{digest}.tmp-",
                                         dir=target.parent)
        temporary_path = Path(temporary)
        try:
            with os.fdopen(fd, "wb") as stream:
                stream.write(data)
                stream.flush()
                os.fsync(stream.fileno())
            os.chmod(temporary_path, 0o444)
            try:
                os.link(temporary_path, target)
            except FileExistsError:
                pass
            self._fsync_directory(target.parent)
        finally:
            temporary_path.unlink(missing_ok=True)
        self._read_bytes(ref)
        return ref

    @staticmethod
    def _fsync_directory(path: Path) -> None:
        fd = os.open(path, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    def path_for(self, ref: dict) -> Path:
        self._validate_ref(ref)
        return self.objects / ref["sha256"][:2] / ref["sha256"]

    @staticmethod
    def _validate_ref(ref: dict) -> None:
        if not isinstance(ref, dict) or set(ref) != {
                "schema_version", "sha256", "kind", "byte_count"}:
            raise ArtifactError("artifact-ref-invalid", "invalid reference fields")
        if ref["schema_version"] != REF_SCHEMA or ref["kind"] not in ARTIFACT_KINDS:
            raise ArtifactError("artifact-ref-invalid", "unknown schema or kind")
        if not isinstance(ref["sha256"], str) or not _SHA256.fullmatch(ref["sha256"]):
            raise ArtifactError("artifact-ref-invalid", "invalid sha256")
        if isinstance(ref["byte_count"], bool) or not isinstance(ref["byte_count"], int) \
                or ref["byte_count"] < 0 or ref["byte_count"] > _limit_for(ref["kind"]):
            raise ArtifactError("artifact-ref-invalid", "invalid byte count")

    def _read_bytes(self, ref: dict) -> bytes:
        path = self.path_for(ref)
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        try:
            fd = os.open(path, flags)
            try:
                info = os.fstat(fd)
                if not stat.S_ISREG(info.st_mode) or info.st_nlink < 1:
                    raise ArtifactError("artifact-integrity", "object is not a regular file")
                chunks = []
                remaining = ref["byte_count"] + 1
                while remaining:
                    chunk = os.read(fd, min(remaining, 1024 * 1024))
                    if not chunk:
                        break
                    chunks.append(chunk)
                    remaining -= len(chunk)
                data = b"".join(chunks)
            finally:
                os.close(fd)
        except ArtifactError:
            raise
        except FileNotFoundError as error:
            raise ArtifactError("artifact-missing", str(error)) from error
        except OSError as error:
            raise ArtifactError("artifact-unreadable", str(error)[:300]) from error
        if len(data) != ref["byte_count"] or hashlib.sha256(data).hexdigest() != ref["sha256"]:
            raise ArtifactError("artifact-integrity", "size or digest mismatch")
        return data

    def read(self, ref: dict, *, cursor: str | None = None,
             max_bytes: int = DEFAULT_READ_BYTES) -> dict:
        if isinstance(max_bytes, bool) or not isinstance(max_bytes, int) \
                or not 1 <= max_bytes <= MAX_READ_BYTES:
            raise ArtifactError("read-limit-invalid", "max_bytes must be 1..65536")
        data = self._read_bytes(ref)
        binding = hashlib.sha256(canonical_json({
            "sha256": ref["sha256"], "kind": ref["kind"],
            "byte_count": ref["byte_count"], "query": "read",
        }).encode("utf-8")).hexdigest()
        start = 0
        if cursor is not None:
            start = self._decode_cursor(cursor, binding)
        if start > len(data):
            raise ArtifactError("cursor-mismatch", "cursor position exceeds object")
        end = min(start + max_bytes, len(data))
        while end < len(data) and end > start and (data[end] & 0xC0) == 0x80:
            end -= 1
        if end == start and end < len(data):
            raise ArtifactError("read-limit-invalid", "max_bytes splits the first codepoint")
        try:
            text = data[start:end].decode("utf-8")
        except UnicodeDecodeError as error:
            raise ArtifactError("artifact-encoding", "invalid UTF-8 artifact") from error
        has_more = end < len(data)
        next_cursor = self._encode_cursor(binding, end) if has_more else None
        return {
            "schema_version": PAGE_SCHEMA,
            "artifact": dict(ref),
            "data": text,
            "returned_bytes": end - start,
            "total_bytes": len(data),
            "truncated": False,
            "has_more": has_more,
            "next_cursor": next_cursor,
        }

    @staticmethod
    def _encode_cursor(binding: str, position: int) -> str:
        payload = {"binding": binding, "position": position}
        raw = canonical_json(payload).encode("utf-8")
        signature = hashlib.sha256(raw).hexdigest()[:16]
        token = base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")
        return f"{token}.{signature}"

    @staticmethod
    def _decode_cursor(cursor: str, binding: str) -> int:
        try:
            token, signature = cursor.rsplit(".", 1)
            raw = base64.urlsafe_b64decode(token + "=" * (-len(token) % 4))
            if hashlib.sha256(raw).hexdigest()[:16] != signature:
                raise ValueError("bad cursor signature")
            payload = json.loads(raw.decode("utf-8"))
            if set(payload) != {"binding", "position"} \
                    or payload["binding"] != binding \
                    or isinstance(payload["position"], bool) \
                    or not isinstance(payload["position"], int) \
                    or payload["position"] < 0:
                raise ValueError("cursor binding mismatch")
            return payload["position"]
        except (ValueError, UnicodeError, json.JSONDecodeError) as error:
            raise ArtifactError("cursor-mismatch", "invalid or foreign cursor") from error
