"""Habit import determinism, provenance and drift tests; no model calls."""

import hashlib
import json
from pathlib import Path
import tempfile
import types
import unittest

from zcode_supervisor import habits, profile


def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()


AGENTS = """# 全局习惯

- 联网检索优先 exa 工具，内置检索仅在不可用时兜底。
- 写完代码必须过审查再交付。

| 表格行 | 应跳过 |
| --- | --- |

```bash
- 代码块里的行应跳过
```

1. 编号陷阱也应导入。
"""


class HabitImportTests(unittest.TestCase):
    def setUp(self):
        temporary = tempfile.TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        self.root = Path(temporary.name).resolve()
        self.zcode = self.root / "zcode-home"
        self.repo = self.root / "repo"
        self.repo.mkdir(parents=True)
        (self.zcode / "skills" / "graphify").mkdir(parents=True)
        (self.zcode / "skills" / "graphify" / "SKILL.md").write_text(
            "---\nname: graphify\ndescription: code graphs first\n---\n\n"
            "# /graphify\n\nTurn folders into a queryable knowledge graph.\n\n"
            "## What graphify is for\n\nArchitecture questions first.\n\n"
            "## What You Must Do When Invoked\n\nFor any codebase question with an "
            "existing graph, run graphify query immediately and answer only from "
            "graph output, citing source_location lines.\n\n"
            "### Step 9 - Never included\n\n" + "filler " * 600 + "\n", encoding="utf-8")
        (self.zcode / "cli").mkdir(parents=True)
        (self.zcode / "cli" / "config.json").write_text(json.dumps(
            {"mcp": {"servers": {"serena": {"command": "x"}, "exa": "https://e"}}}),
            encoding="utf-8")
        (self.zcode / "AGENTS.md").write_text(AGENTS, encoding="utf-8")
        (self.repo / "AGENTS.md").write_text(
            "# 项目\n- MPI 运行前置单线程环境变量。\n", encoding="utf-8")
        self.config_dir = self.root / "ctrl"
        self.config_dir.mkdir()
        self.config = types.SimpleNamespace(config_dir=self.config_dir)

    def import_now(self):
        return habits.import_habits(self.config, zcode_home=self.zcode, repo=self.repo,
                                    cli_tools=(), python_modules=(), extra_python=None)

    def memory_dir(self, key="repo-abcdef0123456789"):
        directory = self.zcode / "cli" / "memories" / "projects" / key / "memory"
        directory.mkdir(parents=True)
        return directory

    def test_import_extracts_bullets_skips_tables_and_code(self):
        summary = self.import_now()
        self.assertEqual(summary["auto_rules"], 5)  # 3 global + 1 project + 1 skill 摘录
        draft = json.loads((self.config_dir / "profile.draft.json").read_text())
        texts = [rule["text"] for rule in draft["rules"]]
        self.assertTrue(any("exa" in t for t in texts))
        self.assertTrue(any("编号陷阱" in t for t in texts))
        self.assertTrue(any("单线程" in t for t in texts))
        self.assertFalse(any("代码块" in t or "表格行" in t for t in texts))
        profile.load_profile(self.config_dir / "profile.draft.json")  # draft is loadable

    def test_skills_become_pinned_excerpt_rules_and_stay_reported_unavailable(self):
        # 技能层：SKILL.md 受审摘录进入草稿规则（钉定技能文件哈希），
        # 同时未晋升技能在 tools 层保持 unavailable 汇报——两层并存由晋升裁定。
        summary = self.import_now()
        draft = json.loads((self.config_dir / "profile.draft.json").read_text())
        rule = next(r for r in draft["rules"] if r["id"] == "skill-graphify")
        self.assertEqual(rule["sources"],
                         [{"path": str(self.zcode / "skills/graphify/SKILL.md"),
                           "sha256": sha(self.zcode / "skills/graphify/SKILL.md")}])
        self.assertNotIn("---", rule["text"])          # frontmatter 已剥
        self.assertNotIn("```", rule["text"])          # 代码栅栏已剥
        self.assertIn("What You Must Do When Invoked", rule["text"])
        self.assertTrue(rule["text"].endswith("citing source_location lines.")
                        or rule["text"].rstrip().endswith("lines."))
        self.assertNotIn("Step 9", rule["text"])       # 装不下的整段跳过，不半句截断
        manifest = json.loads((self.config_dir / "habits-import.json").read_text())
        entry = next(s for s in manifest["skills"] if s["name"] == "graphify")
        self.assertTrue(entry["excerpt_truncated"])    # 有整段被略，如实标记
        self.assertLessEqual(len(rule["text"]), 1000)
        self.assertEqual(rule["applies_to"], ["*"])    # 晋升前默认全域，审查时改定
        ids = {tool["id"] for tool in draft["tools"]}
        self.assertIn("skill-graphify", ids)           # 未晋升仍按 unavailable 登记
        unavailable = next(t for t in draft["tools"] if t["id"] == "skill-graphify")
        self.assertEqual(unavailable["kind"], "unavailable")
        profile.load_profile(self.config_dir / "profile.draft.json")  # 草稿可加载

    def test_drifted_skill_gets_reviewable_update_and_loadable_draft(self):
        # 缺陷2反例：晋升后修改 SKILL.md 再导入——旧逻辑静默跳过导致草稿钉死旧哈希不可加载；
        # 现在草稿在同一稳定 ID 下携带新正文+新哈希（可审查 diff），manifest 记录漂移。
        self.import_now()
        (self.config_dir / "profile.json").write_text(
            (self.config_dir / "profile.draft.json").read_text(), encoding="utf-8")
        live_bytes = (self.config_dir / "profile.json").read_bytes()
        skill_doc = self.zcode / "skills" / "graphify" / "SKILL.md"
        old_hash = sha(skill_doc)
        skill_doc.write_text(
            "---\nname: graphify\ndescription: updated\n---\n\n"
            "# /graphify\n\nUpdated operational guidance v2.\n", encoding="utf-8")
        second = self.import_now()
        self.assertEqual((self.config_dir / "profile.json").read_bytes(), live_bytes)
        drift = [d for d in second["skills_drifted"] if d["name"] == "graphify"]
        self.assertEqual(drift[0]["pinned_sha256"], old_hash)
        self.assertEqual(drift[0]["current_sha256"], sha(skill_doc))
        draft = json.loads((self.config_dir / "profile.draft.json").read_text())
        rule = next(r for r in draft["rules"] if r["id"] == "skill-graphify")
        self.assertIn("Updated operational guidance v2", rule["text"])
        self.assertEqual(rule["sources"][0]["sha256"], sha(skill_doc))
        ids = [r["id"] for r in draft["rules"]]
        self.assertEqual(len(ids), len(set(ids)))       # 同 ID 替换而非重复
        profile.load_profile(self.config_dir / "profile.draft.json")  # 草稿可加载

    def test_import_reports_skills_mcp_and_never_touches_live_profile(self):
        self.import_now()
        manifest = json.loads((self.config_dir / "habits-import.json").read_text())
        self.assertEqual([s["name"] for s in manifest["skills"]], ["graphify"])
        self.assertEqual(manifest["mcp_servers"], ["exa", "serena"])
        self.assertFalse(manifest["live_profile_touched"])
        draft = json.loads((self.config_dir / "profile.draft.json").read_text())
        ids = {tool["id"] for tool in draft["tools"]}
        self.assertIn("mcp-serena", ids)
        self.assertIn("mcp-exa", ids)
        self.assertIn("skill-graphify", ids)
        unavailable = {t["id"]: t for t in draft["tools"] if t["kind"] == "unavailable"}
        self.assertIn("MCP", unavailable["mcp-serena"]["unavailable_reason"])
        self.assertFalse((self.config_dir / "profile.json").exists())

    def test_import_never_touches_live_profile_dependencies(self):
        # 缺陷1反例：在用 profile 钉定的源文件在导入前后必须逐字节不变，live 可加载。
        legacy = self.config_dir / "habits-source.md"
        legacy.write_text("# legacy carrier\n- 人工规则 A。\n", encoding="utf-8")
        live = {"schema_version": 1,
                "rules": [{"id": "manual", "text": "人工规则 A。",
                           "sources": [{"path": str(legacy), "sha256": sha(legacy)}],
                           "applies_to": ["*"]}],
                "tools": []}
        (self.config_dir / "profile.json").write_text(
            json.dumps(live, ensure_ascii=False), encoding="utf-8")
        before = legacy.read_bytes()
        self.import_now()
        self.assertEqual(legacy.read_bytes(), before)
        profile.load_profile(self.config_dir / "profile.json")       # live 仍可加载
        profile.load_profile(self.config_dir / "profile.draft.json")  # 草稿亦可加载
        draft = json.loads((self.config_dir / "profile.draft.json").read_text())
        self.assertEqual([r["id"] for r in draft["rules"]][0], "manual")
        self.assertEqual(len(draft["rules"]), 6)  # manual + 3 global + 1 project + 1 skill
        self.assertEqual(draft["rules"][0]["sources"], live["rules"][0]["sources"])

    def test_repeat_import_is_idempotent_and_keeps_everything_loadable(self):
        # 缺陷2反例：不改上游重复导入 → 源文件集合不变、草稿逐字节恒等、可加载。
        self.import_now()
        draft_path = self.config_dir / "profile.draft.json"
        first_draft = draft_path.read_bytes()
        sources_dir = self.config_dir / "habits-sources"
        first_sources = {p.name: p.read_bytes() for p in sources_dir.iterdir()}
        self.import_now()
        self.assertEqual({p.name: p.read_bytes() for p in sources_dir.iterdir()},
                         first_sources)
        self.assertEqual(draft_path.read_bytes(), first_draft)
        draft = json.loads(draft_path.read_text())
        ids = [r["id"] for r in draft["rules"]]
        self.assertEqual(len(ids), len(set(ids)))
        profile.load_profile(draft_path)

    def test_reimport_after_promotion_dedups_and_stays_idempotent(self):
        # 晋升后重复导入（上游不变）：auto 去重为 0、live 与草稿均可加载。
        self.import_now()
        (self.config_dir / "profile.json").write_text(
            (self.config_dir / "profile.draft.json").read_text(), encoding="utf-8")
        second = self.import_now()
        self.assertEqual(second["auto_rules"], 0)
        profile.load_profile(self.config_dir / "profile.json")
        profile.load_profile(self.config_dir / "profile.draft.json")

    def test_reimport_after_promotion_and_upstream_drift_stays_consistent(self):
        self.import_now()
        live = json.loads((self.config_dir / "profile.draft.json").read_text())
        (self.config_dir / "profile.json").write_text(
            json.dumps(live, ensure_ascii=False), encoding="utf-8")
        live_bytes = (self.config_dir / "profile.json").read_bytes()
        old_sources = {p.name for p in (self.config_dir / "habits-sources").iterdir()}
        (self.zcode / "AGENTS.md").write_text("# 变了\n- 全新习惯内容条目一条。\n", encoding="utf-8")
        second = self.import_now()
        self.assertEqual(second["drift_vs_previous"], ["global"])
        self.assertEqual(second["live_rules_kept"], len(live["rules"]))
        self.assertGreater(len({p.name for p in (self.config_dir / "habits-sources").iterdir()}),
                           len(old_sources))  # 内容变了 → 新的内容寻址文件
        self.assertEqual((self.config_dir / "profile.json").read_bytes(), live_bytes)
        profile.load_profile(self.config_dir / "profile.json")
        draft = json.loads((self.config_dir / "profile.draft.json").read_text())
        ids = [r["id"] for r in draft["rules"]]
        self.assertEqual(len(ids), len(set(ids)))
        new_text = "全新习惯内容条目一条。"
        fresh_id = "auto-global-" + __import__("hashlib").sha256(
            new_text.encode()).hexdigest()[:10]
        self.assertIn(fresh_id, ids)  # 变更文本以新内容寻址 ID 进入草稿
        profile.load_profile(self.config_dir / "profile.draft.json")

    def test_status_reports_drift_and_live_counts(self):
        self.import_now()
        live = json.loads((self.config_dir / "profile.draft.json").read_text())
        live["rules"] = live["rules"][:2]
        (self.config_dir / "profile.json").write_text(
            json.dumps(live, ensure_ascii=False), encoding="utf-8")
        (self.zcode / "AGENTS.md").write_text("# 变了\n", encoding="utf-8")
        status = habits.habits_status(self.config)
        self.assertEqual(status["live_profile"], {"rules": 2, "tools": len(live["tools"])})
        self.assertEqual(status["drifted_since_import"], ["global"])

    def test_memory_index_imports_bullet_rules_and_reimport_is_idempotent(self):
        # 记忆索引行与 AGENTS bullet 同机制导入：内容寻址 auto-mem- id、钉不可变
        # 载体哈希；标题/表格/正文不导入；重复导入同文本同 id 天然幂等。
        memory = self.memory_dir()
        (memory / "MEMORY.md").write_text(
            "# 记忆索引\n\n"
            "- [提交纪律](commit.md) — 提交前必跑全部相关测试\n"
            "- [MPI 安全](mpi.md) — 控制回路变量先 allreduce 再用\n"
            "\n正文段落不是 bullet，不应导入。\n\n"
            "| 表格 | 行 |\n| --- | --- |\n", encoding="utf-8")
        summary = self.import_now()
        self.assertEqual(summary["memory_matched"], "repo-abcdef0123456789")
        self.assertEqual(summary["memory_rules"], 2)
        manifest = json.loads((self.config_dir / "habits-import.json").read_text())
        origin = next(o for o in manifest["origins"] if o["kind"] == "memory")
        self.assertEqual(origin["lines"], 2)
        self.assertEqual(origin["omitted"], 0)
        self.assertEqual(origin["sha256"], sha(memory / "MEMORY.md"))
        draft = json.loads((self.config_dir / "profile.draft.json").read_text())
        expected = ["[提交纪律](commit.md) — 提交前必跑全部相关测试",
                    "[MPI 安全](mpi.md) — 控制回路变量先 allreduce 再用"]
        mem_rules = [r for r in draft["rules"] if r["id"].startswith("auto-mem-")]
        self.assertEqual({r["text"] for r in mem_rules}, set(expected))
        self.assertEqual({r["id"] for r in mem_rules},
                         {"auto-mem-" + hashlib.sha256(t.encode("utf-8")).hexdigest()[:10]
                          for t in expected})
        profile.load_profile(self.config_dir / "profile.draft.json")  # 草稿可加载
        second = self.import_now()
        self.assertEqual(second["memory_rules"], 2)
        redraft = json.loads((self.config_dir / "profile.draft.json").read_text())
        ids = [r["id"] for r in redraft["rules"]]
        self.assertEqual(len(ids), len(set(ids)))  # 幂等：无重复 id
        self.assertEqual({r["id"] for r in redraft["rules"] if r["id"].startswith("auto-mem-")},
                         {r["id"] for r in mem_rules})

    def test_memory_index_truncates_over_budget_and_appends_note(self):
        # 40 条 120 字符长行超 2560 预算：21 条入预算即停，其余 19 条计入
        # omitted，末尾追加注记行；注记行同样成为一条 auto-mem- 规则。
        memory = self.memory_dir()
        rows = [f"- 记忆{i:02d}：" + "长" * (120 - len(f"记忆{i:02d}：")) for i in range(40)]
        (memory / "MEMORY.md").write_text("\n".join(rows) + "\n", encoding="utf-8")
        summary = self.import_now()
        self.assertEqual(summary["memory_rules"], 22)  # 21 条内容 + 1 条注记
        draft = json.loads((self.config_dir / "profile.draft.json").read_text())
        mem_texts = [r["text"] for r in draft["rules"] if r["id"].startswith("auto-mem-")]
        self.assertEqual(len(mem_texts), 22)
        self.assertEqual(mem_texts[0], "记忆00：" + "长" * 115)
        self.assertIn("（记忆索引超预算截断：另有 19 条未注入）", mem_texts)
        manifest = json.loads((self.config_dir / "habits-import.json").read_text())
        origin = next(o for o in manifest["origins"] if o["kind"] == "memory")
        self.assertEqual((origin["lines"], origin["omitted"]), (22, 19))

    def test_memory_index_without_project_key_match_records_absent_origin(self):
        # 无 `<repo>-<16hex>` 项目键也无同名目录：不产生 auto-mem- 规则，
        # origins 仍记录 memory 来源且 exists=False。
        self.memory_dir(key="unrelated-abcdef0123456789")
        summary = self.import_now()
        self.assertIsNone(summary["memory_matched"])
        self.assertEqual(summary["memory_rules"], 0)
        draft = json.loads((self.config_dir / "profile.draft.json").read_text())
        self.assertFalse([r for r in draft["rules"] if r["id"].startswith("auto-mem-")])
        manifest = json.loads((self.config_dir / "habits-import.json").read_text())
        origin = next(o for o in manifest["origins"] if o["kind"] == "memory")
        self.assertEqual(origin["name"], "repo(no-match)")
        self.assertFalse(origin["exists"])
        self.assertEqual(origin["path"], str(self.zcode / "cli" / "memories" / "projects"))

    def test_status_reports_memory_index_drift(self):
        # 导入后改写 MEMORY.md：habits_status 的漂移名单须含 memory 来源名。
        memory = self.memory_dir()
        index = memory / "MEMORY.md"
        index.write_text("- [旧记忆](old.md) — 旧钩子说明\n", encoding="utf-8")
        self.import_now()
        index.write_text("- [新记忆](new.md) — 新钩子说明\n", encoding="utf-8")
        status = habits.habits_status(self.config)
        self.assertEqual(status["drifted_since_import"], ["repo-abcdef0123456789"])


if __name__ == "__main__":
    unittest.main()
