# 工作定向与架构报告（P0-A）

本批交付**确定性工具**：经验匹配、工作方向摘要与架构报告来源核查。模型负责推理，工具负责检索与校验；任何入口都不隐式调用模型、不执行报告或经验中的命令、不扩大权限。原生 Workflow 接线、自动阶段门与持久知识库在 P0-B/P1，不在本批。

## 人工协调流程

1. **prepare（定向）**：协调者（通常是 Codex）把任务做成 TaskFrame/v1，调用
   `czs work prepare --request <json>` 或 hub `prepare_work`。返回匹配经验
   （candidate/conditional，均非科学结论）、未定字段、关键未知、建议调查步骤和
   一段可直接交给调查 worker 的提示词。此步零模型请求、无需 config。
2. **派发调查（沿用既有合同）**：协调者按现有权限用 `delegate` 派发只读调查任务，
   prompt 使用上一步的 worker_prompt（可裁剪），并在任务里明确目标仓库的绝对路径
   与要运行的技能名称。调查只产出报告，不改产品源码。
3. **validate（核查报告）**：worker 交回 ArchitectureBrief/v1 后，协调者调用
   `czs --config <config> work validate-brief --contract-id <id> --brief <json>
   --expected-frame-sha256 <hash>` 或 hub `validate_brief`。工具校验结构，并把
   每条 claim 定位到**该已注册合同 workspace 的当前字节**（sha256 + 行段）。
   结构通过且来源可定位 ≠ 科学或设计接受：`acceptance` 恒为 `not_evaluated`，
   `capability_consumption` 恒为 `unverified`（worker 自述不作数）。
4. **Codex 复核内容**：读短摘要与关键 claim 的证据索引，对高风险论据定向抽查，
   再决定采用/修改/拒绝。工具不替代这一步。

## 字段速览

- **TaskFrame/v1**（≤64 KiB）：`goal`、`task_kinds`（7 类枚举，可空=未定）、
  `domains`（snake_case 标签，未知标签保留）、`features`（5 个固定键，bool/null）、
  `evidence_refs`（≤32 条线索，仅作线索，不解引用不执行）。
- **Orientation/v1**：`matched_practices`（含 `applicability/score/reasons`）、
  `unmatched_domains`（目录无专业条目覆盖 ≠ 不受支持）、`unresolved_fields`、
  `missing_evidence`（全候选汇总后才截断）、`suggested_probes`、`worker_prompt`。
- **ArchitectureBrief/v1**（≤256 KiB）：`summary/orientation`（≤6000 字符）、
  `claims`（observed/inferred/unknown，observed/inferred 必须带 source_ref，
  inferred/unknown 必须有非空 rationale）、`options`（含真实备选代价，允许有据
  no-op）、`impacted_symbols`、`checks`（qualification 枚举）、`unknowns`、
  `capability_observations`（自述）。
- **source_ref**：`{path, sha256, start_line, end_line}`——workspace 内规范相对
  路径、当前字节哈希、UTF-8 文本行段；拒绝绝对路径/越界/反斜杠/symlink 组件/
  多链接文件；单源 ≤16 MiB、总 ≤64 MiB；读取前后 stat 不一致判 `source-raced`。

## 能力边界（如实）

- 来源核查范围是 `current_workspace_only`：不冻结 B0、不证明语义真实性、
  不是防恶意同 UID 进程的 OS 沙箱。
- 经验目录（`practices/scientific-development-v1.json`，10 条）随包分发、
  跨领域通用；不含任何项目路径、单位倍率或集群配置。`source_ids` 是
  "case-derived:<id>" 形式的稳定标识，人读解释见条目 `validated_scope`。
- `achivement` 在本机技能目录中**未解析**；相关能力请求会如实反映在
  capability_observations 里，不会被冒充满足。
- CLI 的 prepare/validate 与 MCP 两工具共用同一服务函数；prepare 在无
  mcp/jsonschema/msgpack 的纯标准库环境同样可用。
