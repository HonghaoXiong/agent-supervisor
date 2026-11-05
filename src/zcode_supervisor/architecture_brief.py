"""ArchitectureBrief/v1 validation: strict schema plus current-workspace
source verification.

Deterministic checks only. The verifier locates claims in the CURRENT
workspace bytes — it freezes nothing, executes nothing and confers no
scientific or design acceptance: ``acceptance`` stays ``not_evaluated`` and
capability self-reports stay ``unverified``. This is evidence admission,
not an OS sandbox against a hostile same-UID process.
"""

from __future__ import annotations

import hashlib
import os
from pathlib import Path, PurePosixPath

from .contracts import canonical_json
from .work_contracts import issue


BRIEF_SCHEMA = "zsc.architecture-brief/v1"
VALIDATION_SCHEMA = "zsc.brief-validation/v1"
SOURCE_SCOPE = "current_workspace_only"
MAX_BRIEF_BYTES = 256 * 1024
MAX_SUMMARY = 6000
MAX_TEXT = 2000
MAX_SYMBOL_TEXT = 512
MAX_CLAIMS = 64
MAX_OPTIONS = 8
MAX_SYMBOLS = 64
MAX_CHECKS = 32
MAX_UNKNOWNS = 32
MAX_CAPABILITIES = 16
MAX_SOURCES_PER_CLAIM = 8
MAX_UNIQUE_SOURCES = 64
SINGLE_SOURCE_LIMIT = 16 * 1024 * 1024
TOTAL_SOURCE_LIMIT = 64 * 1024 * 1024
MAX_ISSUES = 128

BRIEF_FIELDS = ("schema_version", "task_frame_sha256", "summary",
                "orientation", "claims", "options", "impacted_symbols",
                "checks", "unknowns", "capability_observations")
CLAIM_FIELDS = ("id", "statement", "status", "source_refs", "rationale")
SOURCE_FIELDS = ("path", "sha256", "start_line", "end_line")
OPTION_FIELDS = ("id", "approach", "benefits", "costs", "limits")
SYMBOL_FIELDS = ("path", "symbol", "reason")
CHECK_FIELDS = ("id", "question", "method", "expected_evidence",
                "qualification")
CAPABILITY_FIELDS = ("name", "requested", "enabled", "observed",
                     "receipt_ref")
CLAIM_STATUSES = ("observed", "inferred", "unknown")
QUALIFICATIONS = ("code", "numerical", "physical", "observation",
                  "operational")


class _Problems:
    def __init__(self):
        self.items: list[dict] = []

    def add(self, code: str, path: str, message: str) -> None:
        self.items.append(issue(code, path, message))

    def text(self, value, *, path, field, limit=MAX_TEXT):
        if not isinstance(value, str) or not value.strip():
            self.add("not-nonempty-text", path, f"{field} must be non-empty")
        elif len(value) > limit:
            self.add("too-long", path, f"{field} exceeds {limit} characters")

    @property
    def exceeded(self) -> bool:
        return len(self.items) >= MAX_ISSUES


def _is_positive_int(value) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and value >= 1


def _hex64(value) -> bool:
    return (isinstance(value, str) and len(value) == 64
            and all(character in "0123456789abcdef" for character in value))


def _valid_relative_path(value) -> bool:
    if not isinstance(value, str) or not value or "\x00" in value \
            or "\\" in value:
        return False
    if value.startswith("/") or _looks_windows(value):
        return False
    parts = value.split("/")
    return all(part not in ("", ".", "..") for part in parts)


def _looks_windows(value: str) -> bool:
    if len(value) >= 2 and value[1] == ":" and value[0].isalpha():
        return True
    return value.startswith("//")


def _schema_problems(brief, expected_frame_sha256: str, problems: _Problems):
    if not isinstance(brief, dict):
        problems.add("not-an-object", "$", "brief must be an object")
        return
    if set(brief) != set(BRIEF_FIELDS):
        problems.add("unknown-field", "$",
                     f"brief allows exactly {BRIEF_FIELDS}; got "
                     f"{sorted(brief)}")
    if brief.get("schema_version") != BRIEF_SCHEMA:
        problems.add("bad-schema", "$.schema_version",
                     f"must be {BRIEF_SCHEMA!r}")
    frame_hash = brief.get("task_frame_sha256")
    if not _hex64(frame_hash) or frame_hash != expected_frame_sha256:
        problems.add("frame-hash-mismatch", "$.task_frame_sha256",
                     "must be the 64-hex sha256 of the accepted task frame")
    try:
        size = len(canonical_json(brief).encode("utf-8"))
    except UnicodeEncodeError:  # subclass of ValueError: must come first
        size = 0
        problems.add("not-encodable", "$",
                     "brief contains strings that cannot be UTF-8 encoded "
                     "(lone surrogates)")
    except (TypeError, ValueError):
        size = 0
    if size > MAX_BRIEF_BYTES:
        problems.add("too-large", "$",
                     f"brief exceeds {MAX_BRIEF_BYTES} bytes")
    problems.text(brief.get("summary"), path="$.summary", field="summary",
                  limit=MAX_SUMMARY)
    problems.text(brief.get("orientation"), path="$.orientation",
                  field="orientation", limit=MAX_SUMMARY)

    claims = brief.get("claims")
    if not isinstance(claims, list) or not 1 <= len(claims) <= MAX_CLAIMS:
        problems.add("bad-array-size", "$.claims",
                     f"claims must be a list of 1..{MAX_CLAIMS}")
        claims = []
    seen_claim_ids: set[str] = set()
    unique_sources: list[dict] = []
    for index, claim in enumerate(claims):
        path = f"$.claims[{index}]"
        if not isinstance(claim, dict) or set(claim) != set(CLAIM_FIELDS):
            problems.add("unknown-field", path,
                         f"claim fields must be exactly {CLAIM_FIELDS}")
            continue
        claim_id = claim["id"]
        if not isinstance(claim_id, str) or not claim_id.strip():
            problems.add("not-nonempty-text", f"{path}.id", "id required")
        elif claim_id in seen_claim_ids:
            problems.add("duplicate", f"{path}.id", f"duplicate id {claim_id!r}")
        else:
            seen_claim_ids.add(claim_id)
        problems.text(claim["statement"], path=f"{path}.statement",
                      field="statement")
        # Contract: observed claims may carry an EMPTY rationale; only
        # inferred/unknown require a non-empty one (checked below).
        if not isinstance(claim["rationale"], str):
            problems.add("not-nonempty-text", f"{path}.rationale",
                         "rationale must be a string")
        elif len(claim["rationale"]) > MAX_TEXT:
            problems.add("too-long", f"{path}.rationale",
                         f"rationale exceeds {MAX_TEXT} characters")
        status = claim["status"]
        if status not in CLAIM_STATUSES:
            problems.add("bad-status", f"{path}.status",
                         f"must be one of {CLAIM_STATUSES}")
            status = None
        refs = claim["source_refs"]
        if not isinstance(refs, list) or len(refs) > MAX_SOURCES_PER_CLAIM:
            problems.add("bad-array-size", f"{path}.source_refs",
                         f"source_refs must be a list of at most "
                         f"{MAX_SOURCES_PER_CLAIM}")
            refs = []
        elif status in ("observed", "inferred") and not refs:
            problems.add("missing-source", f"{path}.source_refs",
                         f"{status} claims require at least one source_ref")
        if status == "inferred" and not (isinstance(claim["rationale"], str)
                                         and claim["rationale"].strip()):
            problems.add("missing-rationale", f"{path}.rationale",
                         "inferred claims require a non-empty rationale")
        if status == "unknown" and not (isinstance(claim["rationale"], str)
                                        and claim["rationale"].strip()):
            problems.add("missing-rationale", f"{path}.rationale",
                         "unknown claims must explain the missing evidence")
        for position, ref in enumerate(refs):
            ref_path = f"{path}.source_refs[{position}]"
            if not isinstance(ref, dict) or set(ref) != set(SOURCE_FIELDS):
                problems.add("unknown-field", ref_path,
                             f"source fields must be exactly {SOURCE_FIELDS}")
                continue
            if not _valid_relative_path(ref["path"]):
                problems.add("source-path-invalid", f"{ref_path}.path",
                             "paths must be normalized relative POSIX paths "
                             "inside the workspace")
            if not _hex64(ref.get("sha256")):
                problems.add("bad-sha256", f"{ref_path}.sha256",
                             "must be a lowercase 64-hex sha256")
            for line_field in ("start_line", "end_line"):
                if not _is_positive_int(ref[line_field]):
                    problems.add("bad-line", f"{ref_path}.{line_field}",
                                 "line numbers must be positive integers")
            if _is_positive_int(ref.get("start_line")) \
                    and _is_positive_int(ref.get("end_line")) \
                    and ref["end_line"] < ref["start_line"]:
                problems.add("bad-line", ref_path,
                             "end_line must be >= start_line")
            if ref not in unique_sources:
                unique_sources.append(ref)
    if len(unique_sources) > MAX_UNIQUE_SOURCES:
        problems.add("bad-array-size", "$.claims",
                     f"more than {MAX_UNIQUE_SOURCES} unique sources")

    options = brief.get("options")
    if not isinstance(options, list) or not 1 <= len(options) <= MAX_OPTIONS:
        problems.add("bad-array-size", "$.options",
                     f"options must be a list of 1..{MAX_OPTIONS}")
    else:
        for index, option in enumerate(options):
            if not isinstance(option, dict) or set(option) != set(OPTION_FIELDS):
                problems.add("unknown-field", f"$.options[{index}]",
                             f"option fields must be exactly {OPTION_FIELDS}")
                continue
            for field in OPTION_FIELDS:
                problems.text(option[field], path=f"$.options[{index}].{field}",
                              field=field)

    symbols = brief.get("impacted_symbols")
    if not isinstance(symbols, list) or len(symbols) > MAX_SYMBOLS:
        problems.add("bad-array-size", "$.impacted_symbols",
                     f"impacted_symbols must be a list of at most "
                     f"{MAX_SYMBOLS}")
    else:
        for index, symbol in enumerate(symbols):
            if not isinstance(symbol, dict) or set(symbol) != set(SYMBOL_FIELDS):
                problems.add("unknown-field", f"$.impacted_symbols[{index}]",
                             f"symbol fields must be exactly {SYMBOL_FIELDS}")
                continue
            for field in SYMBOL_FIELDS:
                problems.text(symbol[field],
                              path=f"$.impacted_symbols[{index}].{field}",
                              field=field, limit=MAX_SYMBOL_TEXT)
            if not _valid_relative_path(symbol["path"]):
                problems.add("source-path-invalid",
                             f"$.impacted_symbols[{index}].path",
                             "paths must be normalized relative POSIX paths")

    checks = brief.get("checks")
    if not isinstance(checks, list) or not 1 <= len(checks) <= MAX_CHECKS:
        problems.add("bad-array-size", "$.checks",
                     f"checks must be a list of 1..{MAX_CHECKS}")
    else:
        for index, check in enumerate(checks):
            if not isinstance(check, dict) or set(check) != set(CHECK_FIELDS):
                problems.add("unknown-field", f"$.checks[{index}]",
                             f"check fields must be exactly {CHECK_FIELDS}")
                continue
            for field in CHECK_FIELDS:
                problems.text(check[field], path=f"$.checks[{index}].{field}",
                              field=field)
            if check["qualification"] not in QUALIFICATIONS:
                problems.add("bad-qualification",
                             f"$.checks[{index}].qualification",
                             f"must be one of {QUALIFICATIONS}")

    unknowns = brief.get("unknowns")
    if not isinstance(unknowns, list) or len(unknowns) > MAX_UNKNOWNS:
        problems.add("bad-array-size", "$.unknowns",
                     f"unknowns must be a list of at most {MAX_UNKNOWNS}")
    else:
        for index, item in enumerate(unknowns):
            problems.text(item, path=f"$.unknowns[{index}]", field="unknown",
                          limit=1000)

    capabilities = brief.get("capability_observations")
    if not isinstance(capabilities, list) \
            or len(capabilities) > MAX_CAPABILITIES:
        problems.add("bad-array-size", "$.capability_observations",
                     f"capability_observations must be a list of at most "
                     f"{MAX_CAPABILITIES}")
    else:
        for index, capability in enumerate(capabilities):
            path = f"$.capability_observations[{index}]"
            if not isinstance(capability, dict) \
                    or set(capability) != set(CAPABILITY_FIELDS):
                problems.add("unknown-field", path,
                             f"capability fields must be exactly "
                             f"{CAPABILITY_FIELDS}")
                continue
            problems.text(capability["name"], path=f"{path}.name",
                          field="name", limit=160)
            for field in ("requested", "enabled", "observed"):
                if capability[field] is not None \
                        and not isinstance(capability[field], bool):
                    problems.add("not-a-bool-or-null", f"{path}.{field}",
                                 "status fields must be true/false/null")
            receipt = capability["receipt_ref"]
            if receipt is not None and (not isinstance(receipt, str)
                                        or len(receipt) > MAX_SYMBOL_TEXT):
                problems.add("bad-receipt", f"{path}.receipt_ref",
                             "receipt_ref must be null or a short string")


def _read_fd(fd: int, cap: int) -> bytes:
    """Read at most ``cap`` bytes from an OPEN descriptor (test seam)."""
    return os.read(fd, cap)


def _component_walk_clear(workspace: Path, relative: str) -> bool:
    """True when no path component below the workspace root is a symlink."""
    walked = workspace
    try:
        return not any((walked := walked / part).is_symlink()
                       for part in PurePosixPath(relative).parts)
    except OSError:
        return False


def _open_source_fd(candidate: Path) -> int:
    """Open the source with O_NOFOLLOW|O_NONBLOCK|O_RDONLY.

    O_NOFOLLOW closes the TOCTOU window between the component walk and
    the open: if the final component became a symlink after the walk,
    this open fails instead of following it. O_NONBLOCK keeps FIFO or
    special-file surprises from hanging the verifier.
    """
    flags = os.O_RDONLY
    flags |= getattr(os, "O_NOFOLLOW", 0)
    flags |= getattr(os, "O_NONBLOCK", 0)
    return os.open(candidate, flags)


def _source_issues(brief, workspace: Path, problems: _Problems) -> bool:
    """Verify every unique source against the current workspace bytes."""
    references: dict[tuple, dict] = {}
    for claim in brief.get("claims", []):
        if isinstance(claim, dict):
            for ref in claim.get("source_refs", []):
                if isinstance(ref, dict) and set(ref) == set(SOURCE_FIELDS):
                    references[(ref["path"], ref["sha256"],
                                ref["start_line"], ref["end_line"])] = ref
    valid = True
    budget = TOTAL_SOURCE_LIMIT
    for (relative, sha, start_line, end_line), ref in references.items():
        label = f"$.claims.source[{relative}]"
        if problems.exceeded:
            return False
        if not _valid_relative_path(relative) or not _hex64(sha) \
                or not _is_positive_int(start_line) \
                or not _is_positive_int(end_line) or end_line < start_line:
            valid = False  # already reported during schema validation
            continue
        candidate = workspace.joinpath(*PurePosixPath(relative).parts)
        # component walk: no symlinked component below the workspace root
        if not _component_walk_clear(workspace, relative):
            problems.add("source-path-invalid", label,
                         "symlinked path components are not accepted")
            valid = False
            continue
        # R3: FD-level admission (O_NOFOLLOW|O_NONBLOCK). If the final
        # component became a symlink after the walk, the open fails with
        # ELOOP instead of following it.
        try:
            fd = _open_source_fd(candidate)
        except OSError as error:
            import errno
            if error.errno == errno.ELOOP:
                problems.add("source-path-invalid", label,
                             "final component is a symlink")
            elif error.errno == errno.ENOENT:
                problems.add("source-missing", label, "file does not exist")
            else:
                problems.add("source-unreadable", label,
                             f"cannot open: {error}"[:200])
            valid = False
            continue
        try:
            stat_before = os.fstat(fd)
            if not stat_before.st_mode & 0o170000 == 0o100000 \
                    or stat_before.st_nlink > 1:
                problems.add("source-not-regular", label,
                             "only regular unlinked-again files are accepted")
                valid = False
                continue
            if stat_before.st_size > SINGLE_SOURCE_LIMIT or budget <= 0:
                problems.add("source-limit", label,
                             "source exceeds the per-file or total byte "
                             "budget")
                valid = False
                continue
            cap = min(SINGLE_SOURCE_LIMIT, budget) + 1  # +1 detects overrun
            try:
                data = _read_fd(fd, cap)
            except OSError as error:
                problems.add("source-unreadable", label,
                             f"read failed: {error}"[:200])
                valid = False
                continue
            # R3: consumed bytes are deducted BEFORE any later failure
            # branch — a failed read must still eat into the budget.
            budget -= len(data)
            if len(data) > cap - 1:
                problems.add("source-limit", label,
                             "source exceeded the per-file or remaining "
                             "total byte budget during read")
                valid = False
                continue
            stat_after = os.fstat(fd)
            if (stat_after.st_dev, stat_after.st_ino, stat_after.st_size,
                    stat_after.st_mtime_ns) != (
                    stat_before.st_dev, stat_before.st_ino,
                    stat_before.st_size, stat_before.st_mtime_ns):
                problems.add("source-raced", label,
                             "file changed during the verification read")
                valid = False
                continue
            if hashlib.sha256(data).hexdigest() != sha:
                problems.add("source-drift", label,
                             "current bytes do not match the claimed sha256")
                valid = False
                continue
            # R3: NUL bytes are binary for line-range purposes even when
            # they decode as UTF-8; reject them explicitly.
            if b"\x00" in data:
                problems.add("source-encoding-unsupported", label,
                             "sources must be text (no NUL bytes) in this "
                             "batch")
                valid = False
                continue
            try:
                text = data.decode("utf-8")
            except UnicodeDecodeError:
                problems.add("source-encoding-unsupported", label,
                             "sources must be UTF-8 text in this batch")
                valid = False
                continue
            line_count = len(text.splitlines())
            if start_line > line_count or end_line > line_count:
                problems.add("source-range-invalid", label,
                             f"line range {start_line}..{end_line} exceeds "
                             f"the {line_count} lines of the current file")
                valid = False
        finally:
            os.close(fd)
    return valid


def _capped(problems: _Problems) -> list[dict]:
    if len(problems.items) <= MAX_ISSUES:
        return problems.items
    return problems.items[:MAX_ISSUES - 1] + [
        issue("issues-truncated", "$",
              f"more than {MAX_ISSUES} issues; showing the first "
              f"{MAX_ISSUES - 1}")]


def validate_brief(brief: dict, *, workspace: Path,
                   expected_frame_sha256: str) -> dict:
    """Validate structure, then locate sources in the current workspace."""
    problems = _Problems()
    _schema_problems(brief, expected_frame_sha256, problems)
    if problems.items:
        return {"schema_version": VALIDATION_SCHEMA,
                "schema_valid": False, "sources_valid": None,
                "source_scope": SOURCE_SCOPE,
                "acceptance": "not_evaluated",
                "capability_consumption": "unverified",
                "issues": _capped(problems)}
    sources_valid = _source_issues(brief, Path(workspace), problems)
    return {"schema_version": VALIDATION_SCHEMA,
            "schema_valid": True, "sources_valid": sources_valid,
            "source_scope": SOURCE_SCOPE,
            "acceptance": "not_evaluated",
            "capability_consumption": "unverified",
            "issues": _capped(problems)}
