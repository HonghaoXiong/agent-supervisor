"""One-time deterministic import of existing ZCode habits, skills and tools.

Scans the user's real ZCode stores on this machine:

- global ``<zcode-home>/AGENTS.md`` and project ``<repo>/AGENTS.md`` habit text
- per-project memory index ``<zcode-home>/cli/memories/projects/<repo>-<hash>/memory/MEMORY.md`` (bullet index lines, budget-capped)
- user skills under ``<zcode-home>/skills/*/SKILL.md`` (metadata only)
- MCP servers configured in ``<zcode-home>/cli/config.json``
- local CLI tools and Python module backends by direct probe

and writes, next to the configuration:

- ``habits-source.md``   the pinned rule carrier; a marked auto section is
  regenerated in place while everything above it is preserved verbatim
- ``habits-import.json`` full provenance manifest: origin hashes, probes and
  drift against the previous manifest
- ``profile.draft.json`` a complete, loadable draft profile for review

Import never edits a live ``profile.json``: promotion is an explicit reviewed
copy. Origin hashes are provenance for humans, not live pins — upstream edits
do not break runs; rerun the import to detect drift and refresh the draft.
Runtime applicability selection stays entirely in ``profile.apply_profile``.
"""

from __future__ import annotations

import datetime
import hashlib
import json
from pathlib import Path
import re
import shlex
import shutil
import subprocess

MODULE_RECEIPT = '''"""Python module availability receipt; prints one JSON line."""
import json
import sys
from importlib import metadata

print(json.dumps({name: metadata.version(name) for name in sys.argv[1:]}))
'''

BULLET = re.compile(r"(?:[-*]|\d{1,2}[.,、])\s+(.+)")
SKILL_ID = re.compile(r"[^A-Za-z0-9_.-]+")


def _sha256_file(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


def _slug(name):
    return SKILL_ID.sub("-", str(name)).strip("-.") or "unnamed"


def extract_bullets(text, limit=40, max_len=400):
    """Verbatim bullet/numbered habit lines; tables, code and headings skipped."""
    items, in_code = [], False
    for raw in text.splitlines():
        line = raw.strip()
        if line.startswith("```"):
            in_code = not in_code
            continue
        if in_code or line.startswith("|") or line.startswith("#"):
            continue
        match = BULLET.match(line)
        if match:
            item = match.group(1).strip()
            if 8 <= len(item) <= max_len:
                items.append(item)
        if len(items) >= limit:
            break
    return items


FRONTMATTER = re.compile(r"\A---\n.*?\n---\n", re.DOTALL)
CODE_FENCE = re.compile(r"```.*?```", re.DOTALL)
SECTION_SPLIT = re.compile(r"\n(?=#{1,3} )")
SKILL_EXCERPT_CHARS = 2000


def _skill_excerpt(text):
    """Reviewed-excerpt candidate built from COMPLETE markdown sections.

    Sections (frontmatter and code fences stripped) accumulate in order until
    the budget is reached; a section that does not fit is skipped whole, so an
    excerpt never ends mid-sentence. `truncated` reports omitted sections.
    The SKILL.md itself stays the hash-pinned dependency, so later skill edits
    fail closed until the profile is re-reviewed.
    """
    body = FRONTMATTER.sub("", text).strip()
    body = CODE_FENCE.sub("", body)
    kept, total, truncated = [], 0, False
    for section in SECTION_SPLIT.split(body):
        block = " ".join(section.split())
        if not block:
            continue
        if total + len(block) + 3 > SKILL_EXCERPT_CHARS:
            truncated = True
            break
        kept.append(block)
        total += len(block) + 3
    return " | ".join(kept), truncated


def scan_skills(zcode_home):
    skills = []
    root = Path(zcode_home) / "skills"
    if not root.is_dir():
        return skills
    for skill_dir in sorted(p for p in root.iterdir() if p.is_dir()):
        doc = skill_dir / "SKILL.md"
        if not doc.is_file():
            continue
        try:
            text = doc.read_text(encoding="utf-8", errors="replace")
        except OSError:
            continue
        name, description = skill_dir.name, ""
        front = re.search(r"^name:\s*(.+)$", text, re.MULTILINE)
        if front:
            name = front.group(1).strip()
        desc = re.search(r"^description:\s*(.+)$", text, re.MULTILINE)
        if desc:
            description = desc.group(1).strip()[:200]
        excerpt, truncated = _skill_excerpt(text)
        skills.append({"name": name, "description": description,
                       "excerpt": excerpt, "excerpt_truncated": truncated,
                       "path": str(doc), "sha256": _sha256_file(doc)})
    return skills


def scan_mcp_servers(zcode_home):
    config = Path(zcode_home) / "cli" / "config.json"
    if not config.is_file():
        return []
    try:
        value = json.loads(config.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError, UnicodeDecodeError):
        return []
    servers = value.get("mcp", {}).get("servers", {}) if isinstance(value, dict) else {}
    return sorted(name for name, entry in servers.items()
                  if isinstance(entry, dict) or isinstance(entry, str))


def scan_memory_index(zcode_home, repo, budget=2560):
    """扫描项目记忆索引 MEMORY.md 的 bullet 行；预算截断并如实报省略数。

    项目键形如 ``<仓库名>-<16位hex>``；无键匹配时退试同名目录，仍无则记为
    未命中（matched=None）。返回 root/matched/index/exists/lines/omitted，
    其中 lines 为去掉 ``- `` 前缀后的索引文本；超预算截断时末尾附注记行。
    """
    root = Path(zcode_home) / "cli" / "memories" / "projects"
    result = {"root": str(root), "matched": None, "index": None,
              "exists": False, "lines": [], "omitted": 0}
    if not root.is_dir():
        return result
    key = re.compile(re.escape(repo.name) + r"-[0-9a-f]{16}")
    candidates = sorted(entry for entry in root.iterdir()
                        if entry.is_dir() and key.fullmatch(entry.name))
    if not candidates and (root / repo.name).is_dir():
        candidates = [root / repo.name]
    if not candidates:
        return result
    index = candidates[0] / "memory" / "MEMORY.md"
    result["matched"], result["index"] = candidates[0].name, str(index)
    if not index.is_file():
        return result
    entries = []
    for raw in index.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw.strip()
        if not line.startswith("- "):
            continue
        content = line[2:].strip()
        if content:
            entries.append(content)
    lines, used = [], 0
    for content in entries:
        if used + len(content) + 1 > budget:
            break
        lines.append(content)
        used += len(content) + 1
    omitted = len(entries) - len(lines)
    if omitted:
        lines.append(f"（记忆索引超预算截断：另有 {omitted} 条未注入）")
    result["exists"], result["lines"], result["omitted"] = True, lines, omitted
    return result


def probe_cli_tools(names, timeout=10):
    results = []
    for name in names:
        path = shutil.which(name)
        entry = {"name": name, "path": path, "probe": None, "exit": None}
        if path:
            for flag in ("--version", "--help"):
                try:
                    done = subprocess.run([path, flag], capture_output=True, timeout=timeout)
                except (OSError, subprocess.SubprocessError):
                    continue
                if done.returncode == 0:
                    entry["probe"], entry["exit"] = flag, 0
                    break
        results.append(entry)
    return results


def probe_python_modules(interpreters, modules, timeout=30):
    results = []
    code = ("import sys, json\n"
            "from importlib import metadata\n"
            "print(json.dumps({n: metadata.version(n) for n in sys.argv[1:]}))\n")
    for interpreter in interpreters:
        entry = {"python": str(interpreter), "modules": {}}
        try:
            done = subprocess.run([str(interpreter), "-I", "-c", code, *modules],
                                  capture_output=True, text=True, timeout=timeout)
            if done.returncode == 0:
                found = json.loads(done.stdout.strip().splitlines()[-1])
                if isinstance(found, dict):
                    entry["modules"] = {k: v for k, v in found.items()
                                        if isinstance(v, str)}
        except (OSError, subprocess.SubprocessError, json.JSONDecodeError,
                ValueError, IndexError):
            pass
        results.append(entry)
    return results


def _load_live(config_dir):
    live = config_dir / "profile.json"
    if live.is_file():
        try:
            return json.loads(live.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError, OSError):
            return None
    return None


def _immutable_source(config_dir, auto_lines):
    """Write (once) and return a content-addressed rule-carrier source file.

    Content is deterministic in the extracted habits only — no timestamps — so
    unchanged upstream stores address the same file and re-imports are stable.
    Existing files are never rewritten: the live profile and earlier drafts pin
    these paths, and any modification would invalidate them by design.
    """
    body = ["# Worker 习惯源（内容寻址；一经写入不可变，导入永不改写）", "",
            "<!-- manual：人工核定规则以在用 profile 为准，本文件只承载自动导入区 -->", "",
            "<!-- auto-import（czs habits import 生成） -->"]
    body += auto_lines if auto_lines else ["（本轮扫描未发现可导入条目）"]
    body.append("<!-- auto-import-end -->")
    content = ("\n".join(body) + "\n").encode("utf-8")
    digest = hashlib.sha256(content).hexdigest()
    directory = Path(config_dir) / "habits-sources"
    directory.mkdir(parents=True, exist_ok=True)
    target = directory / f"{digest}.md"
    if target.exists():
        if hashlib.sha256(target.read_bytes()).hexdigest() != digest:
            raise ValueError(f"Immutable habit source was modified: {target}")
    else:
        target.write_bytes(content)
    # Legacy single-file carrier from earlier releases is a possible live-profile
    # dependency; it is read for provenance at most and never written again.
    return target


def import_habits(config, zcode_home=None, repo=None, extra_python=None,
                  cli_tools=("graphify", "ocr"), python_modules=("sympy", "mpmath")):
    """Scan real stores and write the rule carrier, manifest and draft profile."""
    zcode_home = Path(zcode_home) if zcode_home else Path.home() / ".zcode"
    repo = Path(repo) if repo else Path.cwd()
    config_dir = Path(getattr(config, "config_dir", None) or Path.cwd())
    config_dir.mkdir(parents=True, exist_ok=True)
    executors = config_dir / "executors"
    live = _load_live(config_dir) or {"schema_version": 1, "rules": [], "tools": []}

    origins, auto_lines, auto_rules = [], [], []
    for key, path in (("global", zcode_home / "AGENTS.md"), ("project", repo / "AGENTS.md")):
        origin = {"kind": "agents", "name": key, "path": str(path), "exists": path.is_file()}
        if origin["exists"]:
            origin["sha256"] = _sha256_file(path)
            bullets = extract_bullets(path.read_text(encoding="utf-8", errors="replace"))
            origin["bullets"] = len(bullets)
            for number, bullet in enumerate(bullets, 1):
                # Content-addressed rule ids: identical text keeps one stable id
                # (idempotent re-imports); changed text yields a new id and is
                # surfaced for review instead of being silently skipped.
                rule_id = f"auto-{key}-{hashlib.sha256(bullet.encode('utf-8')).hexdigest()[:10]}"
                auto_lines.append(f"{number}. [{rule_id}] {bullet}")
                auto_rules.append({"id": rule_id, "text": bullet})
        origins.append(origin)

    # 记忆索引行与 AGENTS bullet 同机制导入：内容寻址 id + 不可变载体钉哈希，
    # 同文本同 id 天然幂等，与 AGENTS 来源撞 id（同文本）时跳过去重。
    memory_scan = scan_memory_index(zcode_home, repo)
    memory_origin = {"kind": "memory",
                     "name": memory_scan["matched"] or f"{repo.name}(no-match)",
                     "path": memory_scan["index"] or memory_scan["root"],
                     "exists": memory_scan["exists"]}
    memory_rules = 0
    if memory_scan["exists"]:
        memory_origin["sha256"] = _sha256_file(memory_scan["index"])
        memory_origin["lines"] = len(memory_scan["lines"])
        memory_origin["omitted"] = memory_scan["omitted"]
        seen_ids = {rule["id"] for rule in auto_rules}
        for number, line in enumerate(memory_scan["lines"], 1):
            rule_id = f"auto-mem-{hashlib.sha256(line.encode('utf-8')).hexdigest()[:10]}"
            if rule_id in seen_ids:
                continue
            seen_ids.add(rule_id)
            auto_lines.append(f"{number}. [{rule_id}] {line}")
            auto_rules.append({"id": rule_id, "text": line})
            memory_rules += 1
    origins.append(memory_origin)

    skills = scan_skills(zcode_home)
    mcp_servers = scan_mcp_servers(zcode_home)

    source = _immutable_source(config_dir, auto_lines)
    source_hash = _sha256_file(source)
    # Live rules keep their original (immutable) pins untouched; only genuinely
    # new auto ids are appended, so re-imports never duplicate or invalidate.
    live_rule_ids = {rule.get("id") for rule in live.get("rules", [])}
    new_rules = []
    for rule in auto_rules:
        if rule["id"] in live_rule_ids:
            continue
        rule["sources"] = [{"path": str(source), "sha256": source_hash}]
        rule["applies_to"] = ["*"]
        new_rules.append(rule)
    # Skill bodies become reviewable rule candidates pinned to their SKILL.md
    # (dependency hash), enabling reviewed auto-selection instead of mere
    # name-only unavailable entries. A skill whose pinned hash drifted gets an
    # explicit reviewable UPDATE in the draft (new text + new hash under the
    # stable id) instead of a silent skip that would pin a dead hash.
    live_rules_by_id = {rule.get("id"): rule for rule in live.get("rules", [])}
    drifted_skills = []
    for skill in skills:
        rule_id = f"skill-{_slug(skill['name'])}"
        existing = live_rules_by_id.get(rule_id)
        if existing is not None:
            pinned = existing["sources"][0]["sha256"] if existing.get("sources") else None
            if pinned == skill["sha256"]:
                continue  # unchanged → idempotent
            new_rules.append({**existing, "text": skill["excerpt"],
                              "sources": [{"path": skill["path"],
                                           "sha256": skill["sha256"]}]})
            drifted_skills.append({"name": skill["name"], "path": skill["path"],
                                   "pinned_sha256": pinned,
                                   "current_sha256": skill["sha256"]})
            continue
        if not skill["excerpt"]:
            continue
        new_rules.append({"id": rule_id,
                          "text": skill["excerpt"],
                          "sources": [{"path": skill["path"],
                                       "sha256": skill["sha256"]}],
                          "applies_to": ["*"]})

    cli_probes = probe_cli_tools(cli_tools)
    interpreters = []
    if extra_python:
        interpreters.append(Path(extra_python))
    pixi = repo / ".pixi/envs/default/bin/python"
    if pixi.is_file():
        interpreters.append(pixi)
    interpreters.append(Path(shutil.which("python3") or "python3"))
    python_probes = probe_python_modules(interpreters, list(python_modules))

    existing_ids = {tool.get("id") for tool in live.get("tools", [])}
    tools = [dict(entry) for entry in live.get("tools", [])]
    for probe in cli_probes:
        if probe["path"] and probe["exit"] == 0 and f"{probe['name']}-cli-receipt" not in existing_ids:
            wrapper = executors / f"{probe['name']}_receipt.sh"
            executors.mkdir(parents=True, exist_ok=True)
            wrapper.write_text(f"#!/bin/sh\nexec {shlex.quote(probe['path'])} {probe['probe']}\n",
                               encoding="utf-8")
            tools.append({"id": f"{probe['name']}-cli-receipt",
                          "description": f"{probe['name']} CLI 可执行回执（探测成功自动生成）",
                          "kind": "executor",
                          "command": f"/bin/sh {shlex.quote(str(wrapper))}",
                          "executor": {"path": str(wrapper), "sha256": _sha256_file(wrapper)},
                          "applies_to": ["*"], "max_uses": 1, "max_timeout_ms": 120000,
                          "required": False, "unavailable_reason": None})
    for entry in python_probes:
        if set(python_modules) <= set(entry["modules"]) and "sympy-formula-receipt" not in existing_ids:
            wrapper = executors / "module_receipt.py"
            executors.mkdir(parents=True, exist_ok=True)
            wrapper.write_text(MODULE_RECEIPT, encoding="utf-8")
            tools.append({"id": "sympy-formula-receipt",
                          "description": "公式合同后端模块可执行回执（探测成功自动生成）",
                          "kind": "executor",
                          "command": f"{shlex.quote(entry['python'])} -I -B {shlex.quote(str(wrapper))} "
                                     + " ".join(python_modules),
                          "executor": {"path": str(wrapper), "sha256": _sha256_file(wrapper)},
                          "applies_to": ["*.py"], "max_uses": 1, "max_timeout_ms": 120000,
                          "required": False, "unavailable_reason": None})
    covered = {tool.get("id", "") for tool in tools}
    for server in mcp_servers:
        tool_id = f"mcp-{_slug(server)}"
        if tool_id not in covered and server not in covered:
            tools.append({"id": tool_id,
                          "description": f"MCP 服务 {server}（~/.zcode/cli/config.json）",
                          "kind": "unavailable", "command": None, "executor": None,
                          "applies_to": ["*"], "max_uses": 1, "max_timeout_ms": 1000,
                          "required": False,
                          "unavailable_reason": "MCP 服务按沙箱设计不向 worker 暴露"
                          "（mcpServers=[]）；如需进入任务包须为它写独立审查的 CLI 包装执行器"})
    for skill in skills:
        tool_id = f"skill-{_slug(skill['name'])}"
        if tool_id not in covered:
            tools.append({"id": tool_id,
                          "description": f"ZCode 技能 {skill['name']}",
                          "kind": "unavailable", "command": None, "executor": None,
                          "applies_to": ["*"], "max_uses": 1, "max_timeout_ms": 1000,
                          "required": False,
                          "unavailable_reason": f"技能已登记（{skill['path']}）但 worker 侧未接线；"
                          "进入任务包须为该技能写独立审查的执行器并固定哈希"})

    # Draft carries live rules plus candidates; drifted skill updates REPLACE
    # the stale entry under the same stable id so the draft stays loadable
    # (it pins the CURRENT file hash) and the change is reviewable as a diff.
    rules_by_id = {rule["id"]: dict(rule) for rule in live.get("rules", [])}
    for rule in new_rules:
        rules_by_id[rule["id"]] = rule
    draft = {"schema_version": 1,
             "rules": list(rules_by_id.values()),
             "tools": tools}
    draft_path = config_dir / "profile.draft.json"
    draft_path.write_text(json.dumps(draft, ensure_ascii=False, indent=2) + "\n",
                          encoding="utf-8")

    manifest_path = config_dir / "habits-import.json"
    drift = []
    if manifest_path.is_file():
        try:
            previous = json.loads(manifest_path.read_text(encoding="utf-8"))
            before = {item["name"]: item.get("sha256")
                      for item in previous.get("origins", []) if item.get("kind") == "agents"}
            for origin in origins:
                if origin["name"] in before and before[origin["name"]] != origin.get("sha256"):
                    drift.append(origin["name"])
        except (json.JSONDecodeError, UnicodeDecodeError, OSError, KeyError, TypeError):
            drift.append("previous-manifest-unreadable")
    manifest = {
        "generated": datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds"),
        "origins": origins,
        "skills": skills,
        "mcp_servers": mcp_servers,
        "cli_probes": cli_probes,
        "python_probes": python_probes,
        "drift_vs_previous": drift,
        "skills_drifted": drifted_skills,
        "generated_files": {"source": str(source), "draft": str(draft_path)},
        "live_profile_touched": False,
        "promote": "审查 profile.draft.json（剪裁/调整 applies_to）后复制为 profile.json；"
                   "在用 profile 永不被导入自动改写",
    }
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
                             encoding="utf-8")
    return {"imported": True,
            "auto_rules": len(new_rules),
            "live_rules_kept": len(live.get("rules", [])),
            "tools_total": len(tools),
            "skills_found": len(skills),
            "mcp_servers": mcp_servers,
            "memory_matched": memory_scan["matched"],
            "memory_rules": memory_rules,
            "drift_vs_previous": drift,
            "skills_drifted": drifted_skills,
            "draft": str(draft_path),
            "manifest": str(manifest_path),
            "source": str(source)}


def habits_status(config):
    """Report the live profile plus source drift since the last import."""
    config_dir = Path(getattr(config, "config_dir", None) or Path.cwd())
    live = _load_live(config_dir)
    report = {"live_profile": None if live is None else {
        "rules": len(live.get("rules", [])), "tools": len(live.get("tools", []))}}
    manifest_path = config_dir / "habits-import.json"
    drift = []
    if manifest_path.is_file():
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError, OSError):
            manifest = None
        if manifest:
            for origin in manifest.get("origins", []):
                if origin.get("kind") not in ("agents", "memory"):
                    continue
                path = Path(origin["path"])
                current = _sha256_file(path) if path.is_file() else None
                if current != origin.get("sha256"):
                    drift.append(origin["name"])
            report["last_import"] = manifest.get("generated")
    report["drifted_since_import"] = drift
    report["next_step"] = "drift 为空且已有在用 profile 则无需动作；否则重跑 habits import 并审查晋升"
    return report
