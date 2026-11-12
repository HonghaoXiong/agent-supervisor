# 使用指南

## 配置和目录

`czs init --directory PROJECT` 生成 `PROJECT/.czs/`。重复初始化会拒绝覆盖已有目录。全局参数 `--config` 放在子命令前；默认读取当前目录的 `.czs/config.json`：

```bash
czs --config /absolute/project/.czs/config.json doctor
```

本机经验证入口是安装 venv 内的完整路径（shell PATH 中没有裸 `czs`，也无需改全局 PATH）：

```bash
~/zcode-supervisor/.venv/bin/czs \
  --config ~/zcode-supervisor/.czs/config.json <subcommand>
```

配置中的 `cli_path`、可选 `provider_config` 指向本机安装；`node` 是 Node.js 命令或路径。`work_root`、`state_dir`、`cache_dir` 相对于配置文件所在目录解析，缺省分别为 `work`、`state`、`cache`。不要把旧机器生成的配置、缓存或状态目录直接提交到 Git。

可选 `notify`（布尔）控制本机桌面通知：`czs run` 在任务包实际派发给 worker、以及运行结束（含失败）时各发一条 macOS 通知；失败带提示音。macOS 缺省开启，其他平台缺省关闭；`doctor` 输出中的 `notifications` 字段显示当前状态。通知只包含运行名、会话短标识与交付检查结论，不含提示词、凭据或原始异常文本；它只是给操作者的状态提示，不属于交付证据，发送失败也不影响任务结果与退出码。

## 习惯 profile：自动继承用户规则与适用工具

`czs run` 会自动发现配置目录旁的 `profile.json`（或在配置中显式指定 `profile` 路径）。profile 是经审查的控制端 JSON，把用户在 ZCode 中的既有习惯带入每个任务包，分三层且互不混淆：

1. **规则层（自动加载）**：`rules` 的习惯文本在适用时自动追加进任务 prompt（附有界预算），每条规则用 SHA-256 固定其蒸馏来源文件；来源漂移即拒绝运行，需重审 profile。
2. **工具层（适用性选择）**：`tools` 按工作区文件 glob 判定适用性。`executor` 类工具作为额外精确字符串 Bash 命令注入任务包（`habit-` 前缀、执行器脚本哈希进 `trusted_hashes`、命令原文披露进 prompt 供 worker 逐字复制），默认 `required=false`（建议性）；`unavailable` 类工具每次运行都如实报告不可用及原因。
3. **执行层（实际可执行）**：以运行结束时的命令回执为准。结果 JSON 的 `habits` 块记录 profile 指纹、各规则/工具的适用与执行状态；提示词提到工具不构成执行证据。

profile 只做扩展、绝不放宽权限：与任务包命令重号、命令串重合或受信哈希不一致都会硬错误拒收；profile 必须位于 work_root 之外。工具命令的 `{workspace}` 占位符必须独立成词且不得预加引号：预引号或嵌套形式在加载与派发前都会被拒收，替换时以 shell 安全引用渲染为单一参数。schema 与冲突判据见 `src/zcode_supervisor/profile.py` 与 `tests/test_profile.py`（后者位于不随仓库分发的开发机本地 tests/ 树）。

### 从现有 ZCode 习惯一次性导入

```bash
czs habits import --repo /absolute/project-repo   # 扫描真实存储，生成可审查草稿
czs habits status                                  # 在用 profile 摘要 + 来源漂移
```

`import` 确定性扫描本机真实来源：全局 `~/.zcode/AGENTS.md` 与 `--repo/AGENTS.md` 的逐条习惯（跳过表格/代码块/标题）、`~/.zcode/skills/*/SKILL.md` 的**段落感知操作摘录**、`~/.zcode/cli/config.json` 的 MCP 服务清单、graphify/ocr CLI 探测、pixi 解释器的 sympy/mpmath 模块探测。产物：`habits-sources/<sha256>.md`（**内容寻址、写后不可变**的规则载体——同内容同文件；导入与在用 profile 互不改写对方依赖的文件）、`habits-import.json`（全部发现 + 来源哈希 + 与上轮的漂移）、`profile.draft.json`（可直接通过 load_profile 校验的完整草稿：在用规则原钉定保留、新自动规则按内容寻址 ID 追加并与在用规则去重）。上游不变时重复导入完全幂等；上游变化以新 ID 进草稿待审，不静默跳过、不失效在用 profile。旧版单一 `habits-source.md` 若被在用 profile 钉定则只读保留、永不改写。

**技能摘录与两类哈希的区别**：技能摘录按 markdown 完整段落累积（frontmatter/代码栅栏剥离、装不下的段落整段跳过、`excerpt_truncated` 如实标记，不半句截断），作为规则候选进入草稿并可晋升为自动选用的注入规则；其 `sources` **运行时钉定 SKILL.md 本体**——技能文件被修改后，在用规则即 fail-closed，重跑 `import` 会在同一稳定 ID 下生成携带新正文+新哈希的可审查更新草稿（`skills_drifted` 记录新旧哈希），晋升后恢复可用。与之相对，AGENTS.md 习惯进入内容寻址源文件后才成为运行时钉子，manifest 里的 AGENTS 来源哈希只是给人看的溯源记录，上游变化不中断运行、由 `status` 报告漂移。

导入**永不改写**在用 `profile.json`；晋升 = 人工审查草稿（剪裁规则/技能条目、调整 applies_to、必要时刷新来源哈希钉定）后显式复制。未晋升的技能与 MCP 条目以 `unavailable` 形态登记（原因写明接线路径），CLI/模块探测成功则自动生成哈希固定的回执执行器。

**任务执行输入不可变**：注入执行器消费的 `.habit-params/` 命名空间（公式脚本、图谱参数等）是派发合同的一部分——packet 的 `allowed_files` 与之任何重合都会在派发前被拒绝，防止 worker 改写输入后借已批准的精确命令执行未复审代码。

典型布局：

```text
.czs/
  config.json
  cache/                 # 经校验的本机 CLI 副本，不上传
  state/                 # 本桥接登记的会话与锁，不上传
  work/
    task-001/
      packet.json        # Codex 控制的合同
      validator.py       # 可选的、审查后冻结的执行器
      run.json           # 控制端交付结果
      run.events.jsonl
      run.stderr.log
      run.before.json
      workspace/         # ZCode 的小型工作副本
        TASK.md
        REPORT.md
```

工作副本必须在 `work_root` 内，最多 256 MiB / 5000 个普通文件，不能含符号链接、硬链接或特殊文件。合同与控制端输出必须在工作副本外。不要复制 `.git`、环境、依赖缓存或大规模计算结果进去。

`doctor` 检查路径、版本和已知 bundle 指纹，并准备独立兼容副本，不发模型任务。它不证明登录有效。`probe` 可以开启并关闭协议会话，不发送模型任务；ZCode 可能保存自己的会话元数据：

```bash
czs probe \
  --workspace .czs/work/example/workspace \
  --out .czs/work/example/probe.json
```

真实验证仍需执行一个有界任务。

## 任务包

任务包是 Codex 审查后的 JSON。所有路径在提交前应展开为目标机器的实际路径；`allowed_files` 使用标准化的工作副本相对路径。

```json
{
  "workspace": "/absolute/project/.czs/work/task-001/workspace",
  "allowed_files": ["implementation.py", "REPORT.md"],
  "commands": [],
  "prompt": "实现 TASK.md 的要求。只修改 implementation.py 和 REPORT.md。不要运行命令。"
}
```

只有明确列出的文件可以获得 Write/Edit 批准。未列出的现有文件会被冻结，执行前后检查哈希与模式。worker 不应修改自己的合同或验收器。

需要固定执行模型时，任务包增加 `model`，使用本机 ZCode 实际配置中的两个标识，例如：

```json
{"model": {"providerId": "<本机提供方标识>", "modelId": "<精确模型标识>", "options": {"reasoningLevel": "<本机该模型支持的档位>"}}}
```

桥接在新建会话时显式选择该模型；新建和续接后都调用完整对象形式的 `session/setModel`，再读取快照。这是对 CLI 0.16.9 创建路径丢失 reasoning options 的兼容处理，保持严格模型检查。发送任务前和完成后都核对运行时选择，并检查当前 turn 的真实主 agent 提供方请求记录。标识不匹配或缺少请求证据时拒收；发送前不匹配则不发模型任务。结果的 `requested_model`、`effective_model_before`、`effective_model_after`、`model_execution` 是验收依据；`observed_model_initial/before/after` 保留各阶段观察到的安全选择字段，预检失败也可复核。`options` 可省略；提供时只接受 `reasoningLevel`，并在前后快照中精确核对。部分提供方要求此项，应通过只读 probe 的 `settings.model.available[].reasoning` 确认支持档位与默认值，不能凭显示名称猜测。提供方请求回执中的 reasoning 信息原样保存；`effectiveLevel=unknown` 只说明实际强度未被提供方证明，不能表述为已确认。省略 `model` 时保持原有选择行为，提示词中的模型名称不等于选择约束。不同机器应重新确认提供方标识，不能照搬另一台机器的 UUID。

工具仍限定 Read、Write、Edit、Bash，原生 MCP 服务未向 worker 暴露。需要 Serena、graphify 等工具时，可把限定操作写进独立、已审查且哈希冻结的执行器，经下面的精确 Bash 命令批准实际调用，并保存工具原始回执。只在提示词里提到工具、或只检查工具已安装，不能作为已经使用的证据。

允许执行命令时，另加：

```json
{
  "commands": [
    {
      "id": "local-tests",
      "command": "/absolute/venv/bin/python -I -B /absolute/project/.czs/work/task-001/validator.py",
      "max_uses": 1,
      "max_timeout_ms": 120000,
      "required": true
    }
  ],
  "trusted_hashes": {
    "/absolute/project/.czs/work/task-001/validator.py": "<审查后文件的完整 SHA256>"
  }
}
```

以上是需要填入真实路径和哈希的字段片段。命令按**完整字符串**匹配，不支持通配或任意 shell 权限。每条命令使用唯一 `id`；默认要求执行。Bash 必须前台执行，不能请求关闭沙箱，超时不能超过合同预算。一个已批准的脚本仍能在本机权限范围内做事，因此要审查其源码、参数与输入，再固定受信文件哈希。

**两个实测陷阱（2026-10-24，CLI 0.17.0 活体验证）**：
1. **低风险命令不经审批回调**：只读/无害命令（如裸 `echo`、`ls`、`grep`）会被服务端原生自动执行，不产生 `interaction/requestPermission` 回调；事后证据审计把这类未配对执行判为 `Bash execution has no matching broker approval` 并拒收交付。任务提示词必须写明"Bash 仅限逐字执行批准命令，查看文件用 Read"；拒收验证探针也要用会触发回调的形式（如 `/bin/sh -c '…'`）。
2. **`max_timeout_ms` 小于 120000 的超时陷阱**：worker 调用 Bash 不带 `timeout` 字段时，broker 按默认 120000 与包预算比较，`max_timeout_ms < 120000` 的批准命令会被 `Command timeout exceeds task budget` 拒绝。要么在提示词里要求 worker 显式设置 `timeout ≤ 预算`（习惯 profile 注入的工具披露行已含预算说明），要么把该命令预算设为 120000。

执行器应自行设置确定的工作目录和必要环境，核验运行前后输入哈希，保存完整测试结果，并传回真实退出码。测试数量和测试内容属于独立验收要求，桥接不会从一句“测试通过”推断正确性。

## 派发、验收与续接

```bash
czs run --packet .czs/work/task-001/packet.json \
  --out .czs/work/task-001/run.json --stall-timeout 600
```

**时限语义（2026-11-03 起）**：`--stall-timeout N` 把终止条件从“总时长”改为“N 秒无进展心跳”——监督者本来就在进程内消费 worker 的每个事件，每个事件向 `<out>.progress.jsonl` 追加一行紧凑心跳（白名单字段，不含提示词/代码正文；繁忙流按 2 秒窗口合并重复）。心跳不断就永不终止；带 stall 时 `--timeout` 缺省升为 86400 保险丝。写码任务一律用 stall 语义；裸 `--timeout N`（缺省 300，绝对墙钟）只用于有界探针。结果 JSON 增加有界 `progress` 块（心跳数/最近动作/停滞秒数）。

任意一侧（含 ZCode 会话）可用两个只读命令窥探，不进任何 agent 的上下文：

```bash
czs progress <task目录或run.json> --last 3   # 最近心跳行 + 存活判定
czs codex-status --fresh 600                 # 活跃 Codex 会话/dsh 工作区/在跑 czs 任务
```

退出码为 0 且 `delivery_ok=true` 才表示交付检查通过。核对 `permission_decisions`、`commands`、`execution_problems`、`changed_files`，并单独审查实际文件差异和测试证据。Bash 结果必须包含 `exitCode=0`、`timedOut=false`、`status=completed`；缺少审批或终态证据会拒收。

`run.events.jsonl` 保存协议证据，`run.stderr.log` 保存子进程诊断；部分 transport 凭据字段会脱敏，但任意提示词、代码和程序输出不能保证自动脱敏。保留在本地，不把真实日志作为仓库示例。

返工时建立新的合同、使用新的输出名，传回之前结果中的 `session_id`：

```bash
czs run --packet .czs/work/task-001/rework.json \
  --out .czs/work/task-001/rework-run.json \
  --resume sess_<之前结果中的ID> --stall-timeout 600
```

只能续接此配置状态目录已登记、进程已退出、工作副本一致的会话。不要随意选择 GUI 会话或迁移原生会话数据库。超时或失败后先检查保留证据及已发生的副作用，再决定返工；不要把重试当作撤销。

## 三方编排 hub（czs-hub）

hub 是决策层 agent 经 MCP stdio 驱动的统一派发点（`python -m zcode_supervisor.hub_server`），提供 health / list_workers / list_sessions / read_context / list_skills / task_status / delegate 七个工具。delegate 只执行人工经 `register_contract` 预先登记的合同（`state_dir/hub/contracts/`）；MCP 暴露不等于批准。各 worker 的执行模式（合同可选 `mode` 字段，缺省取各 worker 的第一档，旧合同语义不变；**新 zcode 合同建议显式 `"mode": "resident"`**——共享热进程 setup 94/68ms vs 冷启 604ms，且 hub 作为持久 MCP 连接时免每任务 python 冷启）：

**技能可见（2026-11-03）**：`list_skills`（或 CLI `czs skills <worker>`）列出 zcode/codex/dsh 三方已安装技能的名字+frontmatter 描述行（同名多副本折叠为一条、按 root 优先序取第一；mtime 缓存；**仍不读 SKILL.md 正文**）。派发回执带 `worker_skills` 名单；惯例=派发前查技能、在 prompt 里点名要用的技能。**`task_status`** 返回最近任务与在跑任务的心跳判定（直接读 `.progress.jsonl`，状态查询永不排在运行中的工作后面）。

**P-B/P-C（2026-11-03 第二轮架构闭环）**：
- 合同可选 `skills`（**zcode 专属**，≤8 个已安装技能名）：派发前校验本机可用性，cold 包自动注入 orchestration 白名单——回执广告的技能变成 worker 真正可加载的能力；resident 路径不支持（诚实拒绝）。codex/dsh 自带 harness 技能，prompt 点名即可，字段拒绝。
- `action=attach` 现支持 **codex**（`exec resume` 续既有 thread；该子命令无 `--output-schema`，结构化回执降级为实例侧校验的最终消息文本）；`action=stop` 支持 codex exec（在飞进程终止梯度）。
- `czs pin-candidate`：对已装 ZCode bundle 产**静态钉候选**（bundle/patched 哈希、fragment 计数、版本串）——只省去哈希算术，入册仍需活体仪式（doctor/probe/有界任务/篡改拒绝）+ 人审。
- config 可选 `events_cap_bytes`：`run.events.jsonl` 原始帧日志超限后写一条 `events_truncated` 标记并停写（审计读内存/路由事件切片，交付证据不受影响；缺省不限）。
- hub extra 增 `jsonschema>=4,<5`（可选）：装了则合同 schema 注册时做 Draft 2020-12 合法性检查、codex 最终消息做全量实例校验；未装保持手写权威，行为对合格负载一致。

**delegate 三态（2026-11-03 P2，super-agent 续发/停止语义）**：`action=start`（缺省，历史行为不变）；`action=attach` + `attach_to=<先前任务id>`——把一份**新的后续合同**发到先前任务的 worker 会话上续跑（dsh 走 `--session-id`、zcode 走 resume：cold 需会话已在 state 目录登记退出、resident 校验 workspace；要求同 worker，zcode 另要求同 workspace，不匹配即拒；codex 本批未实现、诚实拒绝）；`action=stop` + `attach_to=<运行中任务id>`——向在飞 dsh 派发发停止信号（登记表仅存在于派发生命期，回执 `stopped`；zcode/codex 任务按 stall/wall 边界自然结束，本批不提供强停）。attach 即 super-agent 的"重试/追问复用同一 session+目录"模式。

| worker | mode | 通道 | 写路径 |
| --- | --- | --- | --- |
| zcode | `cold`（默认） | 每任务一个 app-server 进程（czs protocol executor） | `allowed_files` + 精确命令，完整审批 |
| zcode | `resident` | 共享常驻进程上的隔离会话（`zcode_resident`） | 同 cold |
| codex | `exec`（默认） | `codex exec -s read-only` 单发 + `--output-schema` | 禁止：`allowed_files` 与 `commands` 必须 `[]` |
| codex | `app-server` | 常驻 app-server + 审批回调（`approvalPolicy=untrusted`） | 仅 `sandbox: "workspace-write"` 下经批准的精确命令 |
| dsh | `headless`（默认，2026-11-03） | `dsh --profile headless --json` 单发，任务走 stdin，事件流即心跳 | 无审批层：`commands`/`model`/`output_schema` 必须空/null；`allowed_files` 仅作验收比对（hub 记录前后清点证据），workspace 必须是一次性副本 |

写路径规则与理由（全部为真机验证事实，证据 `out/tri-agent-p1-20261026`；dsh 行为 2026-11-03 探针）：

1. codex `exec` 模式没有审批回调：workspace-write 沙箱内操作不经审批自动执行——所以 exec 模式永远只读，写任务必须注册为 `app-server` 模式。
2. codex `app-server` + `approvalPolicy=untrusted` 每条命令都发审批回调（批准→accept 执行、未批准→decline 拒绝）；但文件审批 `item/fileChange/requestApproval` 的 schema 无逐文件字段、桥只能整体 decline——Write 工具不可用，写只能经批准的精确命令，且（`allowed_files` 或 `commands` 非空的）写任务必须显式声明 `sandbox: "workspace-write"`（read-only 任务保持默认 `read-only`）。
3. ZCode 冷启与常驻两模式都完整支持 `allowed_files` + 精确命令（czs 生产语义），无额外限制。
4. dsh `headless` 的 `--json` 输出为 NDJSON 事件流（`session`/`status{phase}` 心跳、`turn_end{reason}`、`final{text}`），**进程退出码在错误时也是 0**（如 MISSING_CREDENTIAL）——成败只能从事件流判定，驱动已按此实现；合同可选 `stall_timeout_ms`（缺省 900 s，max 档思考单步可能静默数分钟）。凭据留在 dsh profile 自己的凭据仓，缺 key 以 `error` 结果原样透出。

`output_schema` 是 exec 入口的 `--output-schema` 专用机制：codex+exec 必填（严格模式递归校验不变），codex+app-server 拒收（该模式返回 `final_message` 文本），zcode 不消费该字段。

运行期形态：zcode `resident` 与 codex `app-server` 共享实例在首个相应 delegate 时懒加载启动、整个 server 生命周期复用、进程退出时 best-effort close——这是常驻模式的核心收益（实测 zcode setup 延迟 604ms 冷启 → 94ms 首次 / 68ms 稳态）。app-server delegate 默认**并发**（2026-10-26 P3 起客户端为多订阅者模型，按 threadId 路由审批与通知、任务结束即退订；如需回退串行设 `appserver_concurrency=1`），同一 workspace 仍受跨厂商 advisory lock 约束；resident 侧并发由常驻进程自身的会话隔离与 workspace 锁承担。事件消费路由（P2.2）默认 `incremental`：每任务有界订阅 + 事件唤醒替代共享列表扫描 + 20 ms 轮询（2026-10-30 WP-B 翻转，翻转前默认 legacy）；回滚开关 `CZS_EVENT_ROUTER=legacy`（唯一显式回退值，恢复旧扫描消费路径，goldens 两模式证据字段一致）。合同字段与校验规则详见 `src/zcode_supervisor/contracts.py` 模块文档。

## 远程任务：提交与回收分开

本仓库不内置 SSH 账户、集群地址或任意远程执行接口。针对目标环境准备两份经审查的固定执行器，分别加入两个任务包的精确命令列表。

1. **提交阶段**：冻结源码和参数清单；先写唯一提交意图，再上传到新建的限定目录；申请明确资源上限，保存原始返回和作业 ID，立即结束模型 turn。结果不明时查询已有意图和回执，不能盲目再次提交。
2. **回收阶段**：后续单独续接会话；只读取回执登记的作业与目录；收集调度器终态、退出码、源码哈希、测试数量和机器可读完成记录，保存本地证据。
3. **独立验收**：Codex 同时核对调度器和工作程序证据。队列消失、文件缺失或调度器记录过期应记为 pending/unknown，不得推断成功。测试退出 0 仍需满足任务的内容判据。

调度器状态可能有保留期限，生产执行器应持久化必要完成证据。所批准的资源、远程写入根目录、命令预算和失败处置都属于任务合同，不能靠自由文本报告替代。这种分阶段模式不需要让一个模型调用等候数小时，也不自动创建后台监控。


## Session loop: fixed-session auto round-trip (0.4.0-dev, offline-tested)

Bind an EXISTING native ZCode session once; after each completed turn the
owner builds a review packet, drives the managed Codex reviewer (ONE pinned
review thread, resumed every round), parses the structured decision and
dispatches the next instruction back into the SAME session (`attach_to`).

The running owner watches bound sessions itself. When `work serve` has both a
`session_loop` factory and a hub state dir, a pump polls each active binding's
latest turn, advances it once per turn, and records the dispatched attempt.
No external `advance-session` call is required; the verb stays as a manual
repair entry and derives its source from the recorded handoff or round.

A `control` binding needs proof: a hub task record whose
`session_locator.session_id` is this session. Without it the bind fails with
`control-unproven` and only observe-only binding is available. Dispatched
instructions must target that same session; after a dispatch the pump compares
the worker's real `session_locator` with the binding and marks the round
`needs_reconciliation` on a mismatch (never attaches to a different session).

`desktop-bound` uses the app's queue+wake channel when available (`DesktopBridge`: `thread/queue/add` with a unique `clientUserMessageId`, then `codex://threads/wake/<thread>`; the receipt is the thread's new turn and final agent message, not the opener's exit code). Without that channel the round is resolved before any model call: without a host-validated
`CZS_DESKTOP_WAKE_CMD` the round fails with `desktop-wakeup-unsupported` and
nothing is sent to the background reviewer. Every result names the reviewer
that actually ran (`requested_mode`, `kind`, `thread_id`, `turn_id`).
The reviewer thread is a PAIRED field (`evidence.reviewer.thread_id`, a
`01...` Codex thread with `host=local`); the binding's `sess_...` id is never
passed to the Codex bridge, and a binding without the pair is refused with
`reviewer-thread-missing`.

A user-created session (no hub task record) is claimed read-only with
`claim_native_session`: it verifies the session id, idle state, the session's
own directory and a separately authorised `code_root`, derives the cursor from
the anchored turn (`native-cursor-unresolved` instead of silently handing the
whole history to the reviewer), and grants `observe+review`. Its rework is
delivered through `ZCodeCliBridge`, which runs the shipped CLI with
`--resume <same sess>`; the delivery counts only after the persisted user
message carrying the unique delivery id (and the turn it started) is read back
— the CLI's exit code is not a receipt. A running GUI session answers
`session-busy`; the adapter never writes the chat database and never claims the
host's exclusive lock.

```
czs work serve --state-root .zsc/work-owner --runner-config runner.json
czs work bind-session   --request bind.json    # session_id, db_path, control,
                                               # workspace_policy=bound_existing
czs work claim-session  --request claim.json   # native claim: reviewer_thread,
                                               # code_root, workspace; observe+review
czs work advance-session --request advance.json# after a delivery lands
czs work session-status --request status.json
czs work pause-session / resume-session / unbind-session --request ...
czs work submit-decision --request decision.json  # reviewer token suffices
czs work adopt          --request adopt.json      # import an existing delivery
```

`runner.json` carries `hub_config`, `profile`, and a `session_loop` section
(`codex_path`, `review_workspace`, `review_model`, `authorization`,
`stage_bindings`, `control_token`, `review_token`, optional `max_rounds` /
`max_no_progress`, and the native channel switch `native_bridge` with
`zcode_cli_path` / `zcode_node` / `zcode_timeout_s` / `zcode_mode`). The
control token gates every loop verb
(`CZS_WORK_CONTROL_TOKEN` / `--control-token`); the reviewer token reaches
`submit-decision` ONLY. Request-body `actor` fields are ignored.

Limits, honestly: pause wins over queued dispatches; resume replays nothing;
at most 20 dispatched rounds (configurable) and 3 no-progress rounds; a
candidate drift or a user message after the review expires the decision.
A turn counts as delivered only when its assistant message carries a
completion time and a terminal `finish`; a turn-scoped running tool keeps it
`waiting_tool`. The full turn text is byte-paged (32 KiB, a single long
message continues at `text_offset`) and archived as an artifact the packet
references. The candidate digest is built from git changes (tracked and
untracked) or an explicit manifest; a scan that exhausts its file or byte
budget reports `coverage=incomplete` and refuses to judge drift. Every
dispatch must carry `authorization_sha256` of a registered work authorization;
an intent without one is refused (`authorization-unverified`), and every
mutating owner verb requires the control token.
Desktop chat delivery is live-verified on macOS through the app's own
thread queue (`DesktopBridge`: `thread/queue/add` + `codex://threads/wake/…`,
receipt from the new turn). Without that channel — or a host-validated
`CZS_DESKTOP_WAKE_CMD` — the round fails `desktop-wakeup-unsupported`; the
managed reviewer never silently replaces the desktop chat. The managed
round-trip (real ZCode session + real Codex review, two rounds incl. one
rework, owner restart and automatic verification/acceptance) was demonstrated
2026-11-11. The native leg back into a user-created session is implemented
through the shipped CLI (`zcode --resume`), which appends to the same session
store; it was exercised end-to-end on a native CLI-created session, while the
probe against a GUI-owned session (which this agent itself occupies) is
reported separately — the adapter refuses `session-busy` rather than writing
the database or grabbing the host's lock. When even the CLI cannot resume the
session, `ZCodeGuiBridge` types into the session's own window through System
Events and verifies the persisted user message before reporting delivery. It
refuses an ambiguous window title, a non-empty draft, a busy session and a
missing Automation/Accessibility grant; on this host macOS denies Accessibility
to `osascript` (`-25211`), so this leg reports `desktop-wakeup-unsupported`. The bridge also needs a verifiable session identity on the window (set `ZSC_GUI_SESSION_ID_ATTRIBUTE` to the accessibility attribute that carries the session id); without it the GUI channel is unsupported and the window title is only used to locate the window, never to prove identity
with the exact helper and permission until the user authorises it.
