"""Portable practice catalog: load, validate, digest. Pure functions.

The bundled catalog ships with the package (importlib.resources); nothing
here reads user history, skills directories or the developer machine. The
catalog file is data, not authority: entries describe investigation
APPROACHES distilled from historical cases, never project-specific values.
"""

from __future__ import annotations

import re
from importlib.resources import files

from .work_contracts import (FEATURE_KEYS, TASK_KINDS, WorkInputError,
                             canonical_sha256, issue, parse_json_document)


CATALOG_RESOURCE = "practices/scientific-development-v1.json"
CATALOG_SCHEMA = "zsc.practice-catalog/v1"
MAX_ENTRIES = 64
MAX_CATALOG_BYTES = 256 * 1024
THEME_IDS = ("mechanics", "seismic", "flow_transport", "thermal",
             "stratigraphy_geomorphology", "reaction_geochemistry")

STATUSES = ("active", "candidate", "retired")
ENTRY_FIELDS = ("id", "revision", "status", "title", "task_kinds", "domains",
                "requires_features", "questions", "first_actions",
                "evidence_required", "not_applicable", "stop_conditions",
                "source_ids", "validated_scope")
TEXT_ARRAY_FIELDS = ("questions", "first_actions", "evidence_required",
                     "not_applicable", "stop_conditions")

_DOMAIN = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
_ID = re.compile(r"^[a-z][a-z0-9-]{1,62}$")


def _valid_text(value, *, limit, path, field, problems):
    if not isinstance(value, str) or not value.strip():
        problems.append(issue("not-nonempty-text", path,
                              f"{field} must be a non-empty string"))
        return False
    if len(value) > limit:
        problems.append(issue("too-long", path,
                              f"{field} exceeds {limit} characters"))
        return False
    return True


def _valid_text_array(values, *, path, field, problems):
    if not isinstance(values, list) or not 1 <= len(values) <= 12:
        problems.append(issue("bad-array-size", path,
                              f"{field} must be a list of 1..12 items"))
        return False
    for index, value in enumerate(values):
        _valid_text(value, limit=512, path=f"{path}[{index}]",
                    field=field, problems=problems)


def _valid_tag_list(values, *, allowed, path, field, problems, allow_empty):
    if not isinstance(values, list) or len(values) > 8:
        problems.append(issue("bad-array-size", path,
                              f"{field} must be a list of at most 8 items"))
        return False
    if not allow_empty and not values:
        problems.append(issue("empty", path, f"{field} must not be empty"))
        return False
    # Element typing FIRST (unhashable items must not crash set()).
    for index, value in enumerate(values):
        if not isinstance(value, str):
            problems.append(issue("bad-tag", f"{path}[{index}]",
                                  f"{field} items must be strings"))
            return False
    if len(set(values)) != len(values):
        problems.append(issue("duplicate", path, f"{field} has duplicates"))
    for index, value in enumerate(values):
        if allowed is not None:
            if value not in allowed:
                problems.append(issue("unknown-tag", f"{path}[{index}]",
                                      f"{value!r} is not an allowed "
                                      f"{field} value"))
        elif not _DOMAIN.match(value):
            problems.append(issue("bad-tag", f"{path}[{index}]",
                                  f"{field} items must be lowercase "
                                  "snake_case tags of 1..64 characters"))
    return True


def validate_catalog(document: dict) -> dict:
    """Strict PracticeCatalog/v1 validation; returns the validated document."""
    problems: list[dict] = []
    if not isinstance(document, dict):
        raise WorkInputError("catalog-invalid",
                             [issue("not-an-object", "$",
                                    "catalog must be an object")])
    if set(document) != {"schema_version", "practices"}:
        problems.append(issue("unknown-field", "$",
                              "catalog allows exactly schema_version and "
                              "practices"))
    if document.get("schema_version") != CATALOG_SCHEMA:
        problems.append(issue("bad-schema", "$.schema_version",
                              f"must be {CATALOG_SCHEMA!r}"))
    entries = document.get("practices")
    if not isinstance(entries, list) or not 1 <= len(entries) <= MAX_ENTRIES:
        raise WorkInputError("catalog-invalid", problems or [
            issue("bad-array-size", "$.practices",
                  f"practices must be a list of 1..{MAX_ENTRIES} entries")])

    seen_ids: set[str] = set()
    for index, entry in enumerate(entries):
        path = f"practices[{index}]"
        if not isinstance(entry, dict):
            problems.append(issue("not-an-object", path, "entry must be an object"))
            continue
        if set(entry) != set(ENTRY_FIELDS):
            unknown = sorted(set(entry) - set(ENTRY_FIELDS))
            missing = sorted(set(ENTRY_FIELDS) - set(entry))
            problems.append(issue("unknown-field", path,
                                  f"entry fields differ; unknown={unknown}, "
                                  f"missing={missing}"))
            continue
        entry_id = entry["id"]
        if not isinstance(entry_id, str) or not _ID.match(entry_id):
            problems.append(issue("bad-id", f"{path}.id",
                                  "id must be a lowercase hyphen identifier "
                                  "of 2..63 characters"))
        elif entry_id in seen_ids:
            problems.append(issue("duplicate", f"{path}.id",
                                  f"duplicate id {entry_id!r}"))
        else:
            seen_ids.add(entry_id)
        revision = entry["revision"]
        if isinstance(revision, bool) or not isinstance(revision, int) \
                or revision < 1:
            problems.append(issue("bad-revision", f"{path}.revision",
                                  "revision must be a positive integer"))
        if entry["status"] not in STATUSES:
            problems.append(issue("bad-status", f"{path}.status",
                                  f"status must be one of {STATUSES}"))
        _valid_text(entry["title"], limit=160, path=f"{path}.title",
                    field="title", problems=problems)
        _valid_tag_list(entry["task_kinds"], allowed=TASK_KINDS,
                        path=f"{path}.task_kinds", field="task_kinds",
                        problems=problems, allow_empty=False)
        _valid_tag_list(entry["domains"], allowed=None,
                        path=f"{path}.domains", field="domains",
                        problems=problems, allow_empty=True)
        features = entry["requires_features"]
        if not isinstance(features, dict):
            problems.append(issue("not-an-object", f"{path}.requires_features",
                                  "requires_features must be an object"))
        else:
            unknown = sorted(set(features) - set(FEATURE_KEYS))
            if unknown:
                problems.append(issue("unknown-field",
                                      f"{path}.requires_features",
                                      f"unknown feature keys: {unknown}"))
            for key, value in features.items():
                if not isinstance(value, bool):
                    problems.append(
                        issue("not-a-bool",
                              f"{path}.requires_features.{key}",
                              "feature values must be true/false, never "
                              "integers or strings"))
        for field in TEXT_ARRAY_FIELDS:
            _valid_text_array(entry[field], path=f"{path}.{field}",
                              field=field, problems=problems)
        sources = entry["source_ids"]
        if not isinstance(sources, list) or not sources \
                or len(sources) > 12:
            problems.append(issue("bad-array-size", f"{path}.source_ids",
                                  "source_ids must be a non-empty list"))
        else:
            for position, value in enumerate(sources):
                _valid_text(value, limit=256,
                            path=f"{path}.source_ids[{position}]",
                            field="source_ids", problems=problems)
        _valid_text(entry["validated_scope"], limit=1024,
                    path=f"{path}.validated_scope", field="validated_scope",
                    problems=problems)
    if problems:
        raise WorkInputError("catalog-invalid", problems)
    return document


def load_catalog() -> dict:
    """Load and validate the bundled catalog resource."""
    resource = files("zcode_supervisor").joinpath(CATALOG_RESOURCE)
    document = parse_json_document(resource.read_bytes(),
                                   max_bytes=MAX_CATALOG_BYTES)
    return validate_catalog(document)


def load_practice_set(theme_ids=()) -> dict:
    """Compose the unchanged v1 catalog with explicitly selected reviewable themes."""
    if not isinstance(theme_ids, (list, tuple)) or any(not isinstance(item, str) for item in theme_ids):
        raise WorkInputError("catalog-invalid", [issue("bad-themes", "$.theme_ids",
                                                      "theme_ids must be an array of strings")])
    selected = []
    for theme_id in theme_ids:
        if theme_id not in THEME_IDS:
            raise WorkInputError("catalog-invalid", [issue("unknown-theme", "$.theme_ids",
                f"unknown practice theme {theme_id!r}; available={list(THEME_IDS)}")])
        if theme_id not in selected:
            selected.append(theme_id)
    base = load_catalog()
    if not selected:
        return base
    practices = list(base["practices"])
    for theme_id in selected:
        resource = files("zcode_supervisor").joinpath(f"practices/themes/{theme_id}.json")
        document = parse_json_document(resource.read_bytes(), max_bytes=MAX_CATALOG_BYTES)
        validate_catalog(document)
        if len(document["practices"]) != 3:
            raise WorkInputError("catalog-invalid", [issue("bad-theme-size", theme_id,
                                                          "each bundled theme must contain exactly three entries")])
        for entry in document["practices"]:
            if entry["domains"] != [theme_id] or entry["status"] != "active" \
                    or not entry["source_ids"][0].startswith(f"design-derived:{theme_id}:") \
                    or "调查候选" not in entry["validated_scope"]:
                raise WorkInputError("catalog-invalid", [issue("theme-binding", entry["id"],
                    "theme domain/status/source/validated-scope binding is invalid")])
        practices.extend(document["practices"])
    # validate_catalog enforces the shared upper bound and duplicate-ID checks;
    # no overflow is silently truncated.
    return validate_catalog({"schema_version": CATALOG_SCHEMA, "practices": practices})


def catalog_digest(catalog: dict) -> str:
    """Deterministic digest over the catalog document."""
    return canonical_sha256(catalog)
