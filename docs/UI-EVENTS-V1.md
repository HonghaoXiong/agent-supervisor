# UI Events V1 — 测量口径（P0.0，含 round-1 审查修订）

> 本文件本轮（P0.0）只固定**测量口径**；版本化线协议 schema、initial/snapshot
> cursor、subscribe-all、重连与权限边界由后续任务（P0.4/P1.x）在同一文件补写。
> 本口径对 Rust UI 与 Python hub 双侧生效；一切性能门槛遵循“先测基线再冻结阈值”，
> 本轮不冻结任何阈值，不声称任何提升百分比。

## 1. 时钟纪律（clock discipline）

- 计时唯一时钟源：主机 `CLOCK_MONOTONIC` 纳秒值。Python 侧统一入口
  `zcode_supervisor.bench.monotonic_ns()`（`time.clock_gettime_ns(time.CLOCK_MONOTONIC)`）；
  不得序列化 Rust opaque `Instant`，Rust 侧必须读取同源 `CLOCK_MONOTONIC`。
- **`observed_at` 只用于展示**（wall clock，display-only），绝不参与任何延迟计算。
  纯函数 `bench.ui_event_latency(probes)` 只读 `source / journal_enqueued /
  ui_received / reducer_committed / frame_submitted` 的单调纳秒标记，输出相邻段
  差与 `source_to_present_ns`；任一端点探针缺失时输出 `end_to_end_latency =
  "unavailable"`，不用 wall clock 或调度时间替代。
- 跨进程同源可行性：fixture 子进程（`tests/fixtures/ui/fake_server.py`）在每个
  响应中回带自己的 `CLOCK_MONOTONIC` 读数；`bench_fake_clock_sidecar` 断言每次
  探测满足 `t0 <= t1(child) <= t2`（`same_source`），并在文档中保留逐次
  `probes_raw`（`t0_ns/child_ns/t2_ns/observed_wall_ns`）供独立重算——延迟只从
  单调字段重算，`observed_wall_ns` 仅随档留存。
- **呈现探针（presentation probe）**定义：“包含该 event_id 的 reducer 状态完成
  一次实际 frame 绘制并提交窗口系统后的应用侧标记”。**本轮（P0.0）状态 =
  NOT_RUN**：没有真实 UI 就不制造合成替身自证；`presentation_probe` 字段固定
  为 `NOT_RUN in P0.0`，`source_to_present = "baseline-pending"`。本轮唯一被
  认可的呈现相关 smoke 是 clock sidecar 本身。

## 2. fake/replay 基线分段（P0.0 实测内容）

全部走 fixture 子进程 + 真实生产类（`ProtocolClient` / `CodexAppServerClient` /
`CodexAppServerDriver` 合同测试 / `ResidentZCode`；fake 说生产形状：
`thread/start → result.thread.id`、`turn/start → result.turn.id`、
`turn/completed`/`item/completed`/`thread/tokenUsage/updated` 通知、
`item/commandExecution/requestApproval` 反向请求、`thread/read`）。零模型调用
（`model_requests == 0`，由每个子进程自身计数器聚合并证明）、不触碰真实 CLI /
远程任务 / 真实会话数据。入口 `bench.run_fake_baseline` / `tools/bench_ui.py`。

| section | 分段 | 说明 |
| --- | --- | --- |
| `fake-cold-lifecycle` | spawn / session_create / session_subscribe / close | 每轮全新真实 `ProtocolClient`，warmup=0 冷语义；子进程计数器**跨 child 聚合** |
| `fake-appserver` | spawn_initialize / thread_start / close | 真实 `CodexAppServerClient`；`start()` 受 `start_timeout` 有界（无响应子进程记为失败试次，全部失败则 section error，不悬挂）；thread 轮附带生产形状 turn 流量以观测 notifications / reverse tasks / pending 无界保留 |
| `fake-resident-lifecycle` | resident_start / first_create / steady_resume / cancel_to_finish / reset_rpc / resident_stop / event_loop_lag_ms / session_read_ms | 真实 `ResidentZCode`：首轮 create/subscribe、steady resume/subscribe/send、never-complete 多事件流中途取消、关闭 |
| `fake-clock-sidecar` | round_trip_ms | 同源钟 smoke（`probes_raw` 留档）+ `blocked_drain` 真门探测 + 故障 arming smoke |
| `fake-read-list` | list_runs_ms / read_context_ms | **生产读/列表路径**：`mcp_readonly.list_runs_impl` 扫描合成 `run-<N>.json` 记录 + `session_reader.read_zcode_context` 分页合成 sqlite 会话库；不用任何 bench 自造端点替代 |

每轮（round）记录（均为**未舍入原始值**）：`total_wall_ms` 与**可取消通信段**
`comm_rpc_ms`（该轮全部 request/response RPC 时长之和；二者分开报告，总 wall
time 不是 P0.1 硬门）、逐方法 RPC 延迟、`inventory_calls` / `hashed_bytes`（含
`check_frozen` 内部扫描）、`check_frozen_ms`、`write_json_ms`、
`session_read_ms`、`drain_events_evaluated`（生产 drain 循环实际处理数——按
`own_event` 调用计，与保留量分开）、`evidence_scan_events`（成功轮 post-turn
`execution_evidence` 全历史重扫量；取消轮为 null）、`round_events_emitted` /
`round_events_before` / `events_retained_after`（保留量与处理量分离）、事件
循环 lag（20 ms heartbeat 的 expected/actual 单调差）、峰值 RSS（`getrusage`
self/children 历史高水，macOS 字节 / Linux KiB，平台标注）。

同步段单独标记（**不受 P0.1 deadline 抢占**，如实计入总时长）：
`first_inventory_ms`、`pre_check_frozen_ms`、`post_success_check_frozen_ms`、
`finally_check_frozen_ms`、`finally_after_inventory_ms`、`finally_write_ms`。
取消轮：`cancel_to_finish_ms` 从 `task.cancel()` 起到 **task join 返回即取时**；
故障复位 RPC 严格在其后单独计时（`reset_rpc_ms`，支持 `reply_delay_s` 注入证
明不污染）；另记 `post_cancel_{finally,check_frozen,after_inventory,write}_ms`。
本轮 resident 段输出 `whole_call_wall_time_gate = "baseline-only"`——P0.0 不满足
也不声称整调用硬 wall-time 门。

`blocked-drain` 故障夹具为**真实门**：子进程停读 stdin 期间，OS 管道写满使大帧
（~256 KiB）写方的 `await drain()` 真实阻塞（0.5 s 有界等待超时即证阻塞），
子进程恢复读后 framing 完整、后续请求完成（recovery 计时）。未复现时显式标
`not-reproduced`，不改名冒充。

## 3. 原始样本与缺测约定

- **每个测量 phase 保留逐次未舍入 `samples` 数组**；`n/median/p95/min/max/mean`
  为展示摘要，测试从 JSON 内 `samples` 用 `bench.percentile` 独立重算校验。
- 顶层文档（`fake-baseline.json`，重跑自动落 `fake-baseline-2.json`、`-3` …，
  **绝不覆盖既有好基线**；每次运行 scratch 目录唯一）：`environment`
  （python/platform/machine/system/release）、`command`（确切子进程命令）、
  `exit_code`、`model_requests`（各子进程计数器聚合，非零即失败）、每 section
  原始样本与轮明细。
- 没有可靠样本的指标一律写 `baseline-pending`（如真实 UI 呈现段）或 `NOT_RUN`
  （本轮未执行的探针本体），不得填预期值或提升百分比；`phase_stats([])` 同样
  返回 `baseline-pending`。
- 重复次数与任何 SLO 在首轮基线后由用户/审查者冻结；本轮无阈值。

## 4. 固定夹具（fixtures，可重复、无凭据/真实会话）

- `tests/fixtures/ui/fake_server.py`：fixture 子进程（可执行、纯标准库、无网络、
  无 subprocess import）：ZCode+codex 双协议 NDJSON（含生产 thread/turn/审批
  形状与 `thread/read`）、`bench/counters`（`model_requests` 恒 0——计数仅在
  `_invoke_model` 入口递增，而该函数永不被调用）、`bench/set-fault`（支持
  `reply_delay_s` 延迟该次回复，用于复位计时分离证明）、`--list-faults` 列出
  故障模式、每响应带 `monotonic_ns` 同源钟标记。
- `tests/fixtures/ui/ui-fixture-spec.json`：PLAN §5.3.2 UI 夹具（100 节点/300
  边/10000 事件、同起点 100 events/s 重放、running/terminal marker）；由
  `bench.build_ui_fixture` 确定性生成，规范序列化 sha256 钉死，重生成必须逐位
  一致。
- `tests/fixtures/ui/comm-faults-v1.json`：通信故障夹具清单（blocked-drain→
  `block-drain` 真门 / delayed-start / delayed-turn-start / cancel-mid-poll /
  cancel-after-send / initialize-no-reply→`silent-initialize`）；P0.0 钉清单、
  验证 arming，并对 blocked-drain 做真实门 smoke；完整故障矩阵测量归 P0.1。

## 5. P0.4 进程内事件投影合同（versioned v1，无 journal/UDS）

> 本节由 P0.4 落地（`src/zcode_supervisor/ui_events.py` + `ui_projection.py`）。
> 范围 = **进程内**投影/emit 接口：v1 信封 schema、allowlist、序号/时间纪律、
> 可信 initiator 边界、当前 task 通知归因、旁路 emit 透明性。journal/UDS/snapshot
> 游标仍是 P1.1，UI 客户端是 P1.2，本节不定义它们。

### 5.1 v1 信封（固定 20 字段，缺省 `null`，不伪造）

```
v, event_id, stream_id, seq, kind, observed_at, task_id, worker, mode,
session_id, turn_id, tool_call_id, request_id, source_event_seq, status,
outcome, delivery_ok, duration_ms, error, data
```

- `event_id = <hub_instance_id>/<journal_epoch>/<stream_id>/<seq>`；`seq` 由单一
  `UiEventStream` 按流从 1 单调连续分配（P0.4 = 每 hub 进程一份内存流水线，
  journal_epoch 恒 1，P1.1 接管真实换代）。`event_id` 只用于去重，不替代原生
  session/turn/tool/request 身份。
- `observed_at` 恒为 supervisor UTC RFC3339 微秒 `Z` 形（仅展示）；`duration_ms`
  只能来自 `time.monotonic()` 差值。
- `source_event_seq` 原样保留 ZCode 原生 seq；**Codex 无原生 seq 恒为 `null`**，
  不得冒充宿主游标。
- id 类字段（task/session/turn/tool_call/request）一律过 `safe_opaque` 形状门
  （单行、可打印、≤128、无路径分隔符）：不合格即置 `null`（省略而非截断泄漏）。
- `error` 只允许类型形代码（如 `ValueError`、`BudgetExhausted`）；自由文本、路径、
  消息体永不入事件（留在原生证据记录）。

### 5.2 事件目录与 data allowlist（每 kind 显式枚举）

| kind | data 允许键（值域受限） | 明确不含 |
| --- | --- | --- |
| `task.accepted` | contract_id、contract_digest(64hex)、created_at、initiator_agent、initiator_session_id、initiator_verified(bool)、reason_code(`initiator_unverified`)、parent_session_id | prompt、workspace、合同正文、grantRoot、原始 initiator 串、未验证父身份 |
| `stream.announced` | task_stream_id、accepted_event_id | 任何业务 payload（只复用 accepted 已批准字段） |
| `session.bound` / `session.state` | locator_type(`resident`/`app-server-thread`)；state 另有 last_source_seq、reason_code(`clean_exit`/`lost`) | session 目录、原生日志正文 |
| `turn.started` / `turn.completed` | （无 data；身份走信封） | agentMessage/final_message 全文 |
| `tool.scheduled` | tool_name、command_id(可空)、read_only(可空 bool) | 命令文本、tool input/output、文件列表 |
| `tool.completed` | tool_name、command_id(可空)、read_only(可空 bool)、exit_code(可空 int)、timed_out(可空 bool) | 命令文本、tool input/output、文件列表 |
| `approval.requested` | tool_name、command_id(可空=尚无决策)、requested_variant(固定枚举，可空) | Authority 原始 input/params、reason 文本 |
| `approval.decided` | tool_name、command_id(当前决策的，无决策为 null)、decision(allow/deny)、reason_code(`allowed_by_contract`/`denied_by_contract`/`denied_early`/`denied_persistent_variant`)、requested_variant(固定枚举，可空) | Authority 原始 input/params、reason 文本 |
| `task.completed` | reason_code(仅 `error_unclassified`，错误存在但不可分类时) | result payload、final_message、原始 error 文本 |
| `process.exited` | process_scope(cold/shared-resident/shared-app-server)、process_generation、exit_code、reason | resident 存活 `returncode=None` 不是退出证据 |

未枚举键一律 `UiEventError` 拒绝；**v1 无 `result_preview`**。

### 5.3 可信 initiator（fail-closed）

- `UiInitiatorResolver` 仅当 agent ∈ 固定枚举 {codex, zcode} 且**显式注入的可信
  registry** 对 `(agent, session_id)` 精确匹配（同 agent）时，才发射 canonical
  opaque `initiator_session_id`；否则两 initiator 字段为 null、
  `initiator_verified=false`、`data.reason_code=initiator_unverified`。
- 生产 v1 **没有可信跨 worker registry** → 一律 unverified；不存在“自称即可信”
  的注册路径（resolver 无注册 API）。
- parent edge 仅来自 registry 的 `verified_parent_of` 记录；initiator payload 里
  自报的 parent 一律忽略。伪造/未登记/agent 不匹配/畸形 id（路径、换行、超长、
  base64 串）全部 unverified 且原串不入事件。

### 5.4 顺序、归因与进程退出

- `task.accepted` 先落 running task record（可读）再发射，随后同流水线向 hub 流
  发 `stream.announced`（引用 accepted 的 event_id）——发现通知永不指向尚不存在
  的 accepted 事件。
- `notification_counts`（app-server receipt）只统计 thread/conversation 精确命中
  本 task 的帧；他 task 的帧留给其自身 receipt；无身份帧进
  `notification_unattributed`（独立桶，绝不猜给”唯一 active task”）。顺序与交错
  线程零串线；`attribute_notification` 的固定原因码：
  `identity_missing`/`identity_unknown`/`identity_ambiguous`。
- **WP-C receipt 元数据治理（PLAN §9.8 另立合同，默认开启、无环境开关，回滚=
  代码 revert）**：cold `codex exec` receipt 的 `event_counts`/`command_items`/
  `hard_errors` 与 app-server receipt 的 `command_items` 各有**条目上限**——
  256 / 64 / 32 / 64（常量 `codex_driver.EVENT_COUNTS_ENTRY_CAP` /
  `COMMAND_ITEMS_ENTRY_CAP` / `HARD_ERRORS_ENTRY_CAP`，app-server 复用
  command 上限；依据 P3.2 soak 实测分布 ≈4 条 event_counts/任务 × 安全余量）。
  超阈=**保序截断（keep-first）+ 显式溢出标记**：被截断 receipt 同字段旁出现
  `”<name>_truncated”: true` 与 `”<name>_dropped”: <int>`（未计入的源事件数；
  列表 1 丢弃条=1 事件；`event_counts` 为类型键不再保留的出现次数——精确的
  丢弃键数需无界 distinct 集合，与上限自相矛盾）；不静默、不报错、每条目消息
  仍按原 300 字符上限截断。未超阈 receipt 与治理前逐字节一致（标记键只在截断
  时出现）；上限在累加期生效（accumulator/收集扫描中即有界），原始证据
  （codex-events.jsonl / appserver-frames.jsonl / thread read）永远全量落盘，
  截断仅作用于 receipt 摘要字段。
- `process.exited` 只能来自 P0.3 `ExitProjection` 的真实 awaited wait 记录
  （每 `{process_scope, generation}` 恰一次）；EOF、task 完成、returncode 赋值、
  重复 close 均不发射。

### 5.5 旁路纪律与回滚

- 一切 emit 在权威状态迁移**之后**；`UiEmitter` build/publish/serialize/订阅者
  失败只记入自身 `failures`，永不改变 delegate 结果、审批决定、原生证据或进程
  回收。无法可靠观察的状态（如 codex tool 细粒度）宁可省略，不从终态倒造。
  **订阅者按不可信输入隔离**（round 2 Critical）：每个订阅者只拿独立深拷贝；
  accepted 的权威 event_id 在 publish 前保存，订阅者 clear/篡改/抛错后
  announced 链路与 delegate 结果不受影响。
- 回滚开关：`CZS_UI_EVENTS`（**默认开=投影 emit 生效**——2026-10-24 产品裁定翻转
  原"默认 0"；未设/空串=开，显式 `0`/`false`/`no`/`off`（strip+大小写不敏感）=关，
  任何其他非空值按显式开启计；PLAN §7 P0.4）；通知计数修复独立于开关恒生效。
  关闭态对既有 custom exit projection **零改动**；启用态将既有 callback 与 UI sink
  链式组合（各恰一次，保留 P0.3 once 语义）。

### 5.6 round 2 修订（双审查 + 控制器 round 1）

- **error 只认固定受信枚举**：`error` 必须是
  `ui_events.TRUSTED_ERROR_CODES`（本仓实际会抛出的异常类型名固定集）成员；
  native error.message、`str(error)`、base64、自由文本一律不进事件——未分类错
  误发 `error=null` + `data.reason_code=error_unclassified`。
- **Codex 审批成对投影在 ApprovalBridge**：requested/decided 每次恰一对，decided
  携带**最终 applied** decision（persistent variant 经 worker_response 降级后为
  deny，与 wire/Authority 证据一致）与 `requested_variant`（两事件成对携带）；
  early-deny（无命令文本/无可验证文件/拒绝类请求）同样成对。hub 不再给 codex
  Authority 挂钩（避免双发）；zcode（resident+cold）仍在 Authority 决策点成对投
  影（该路径 applied==authority decision）。
- **通知归因按身份集合**：收集 native envelope 全部候选身份
  （threadId/thread_id/conversationId）求不同值集合；恰好一个且等于本 task 才计
  own；0 个/多值进 **client 级独立 unattributed 聚合**
  （`client.notification_unattributed`，跨 run 合并），绝不进任何 task receipt/
  timeline；单一但不匹配的身份由其所属 task 的 receipt 计数。计数在
  **thread/read 响应屏障后**进行（同一有序 stdio：服务端先发的帧必先于该响应被
  读循环入账），trailing frame 归因确定、无调度竞态。
- **cold 生命周期**：cold Codex 在真实 child wait（含 terminate/kill 梯度后的
  wait）完成后经 P0.3 `ExitProjection` 恰一次投影 `process.exited`
  （process_scope=cold，每子进程独立 generation）；cold ZCode 由
  `protocol.execute(ui_emitter=...)` 同时接 cold Authority 审批投影与
  ProtocolClient exit sink（新增构造参数，不覆盖既有 projection）。
- **schema 严格化**：`v` 必须是精确整数 1（拒 bool/float）；event_id epoch 必须
  ASCII 数字；`observed_at` 必须是真实 UTC 日历时间（语义解析+零偏移）；
  `duration_ms` 必须 finite（拒 NaN/Infinity/超范围大数，不抛 OverflowError）；
  发布序列化 `allow_nan=False`；逐 kind 必备关联/状态
  （`_REQUIRED_ENVELOPE`/`_REQUIRED_DATA`）——缺失即整条省略，不倒造。
- **seq 提交后置**：候选事件全部校验通过才提交序号；build/校验失败不改变可见
  流状态（下一合法事件仍取下一连续 seq）。
- **resident `turn.failed`**：观察到权威 native failed 后、抛原异常前，投影
  `turn.completed`（固定安全 outcome=failed + native seq），失败语义不变。

### 5.7 P0.5：缺身份/歧义审批请求的生产路由 fail-closed

- 五个审批/权限方法（`execCommandApproval`、`applyPatchApproval`、
  `item/commandExecution/requestApproval`、`item/fileChange/requestApproval`、
  `item/permissions/requestApproval`）由 `request_requires_task_identity` 显式
  枚举，路由走 client 的**唯一候选**解析：params 必须是 dict，且每个出现的
  threadId/thread_id/conversationId 字段都必须是同一非空字符串（缺失字段可，
  空值/非字符串/冲突值一律拒绝）；当前 active handlers 中恰好一个 handler 的
  可信身份（`czs_request_identity`）精确匹配才被调用。
- 拒绝复用既有单次 unsupported/-32601 应答：不进任何 ApprovalBridge/Authority、
  不写审批决定、不归因任何 task；UI 侧不伪造 task 事件，至多安全 reason 的
  unattributed 视图；响应与日志不携带原始 params。
- 即使只有一个 active handler，缺身份/非法身份也不可认领；两个 handler 同身份
  匹配、零匹配、注册顺序均不决定归属。缺 turnId 但 threadId 精确匹配的既有
  合同、5 秒 turn 等待、有效身份的既有决定/响应逐字段不变；未知方法与另外
  三类 server request（tool 输入/elicitation/dynamic tool call）走原路径。
- **残余风险（unconfirmed）**：真实 app-server 是否会发送缺身份/歧义审批帧
  未在 live 验证；本节仅固定 fake/local 复现下的生产行为。P0.5 不改变 UI
  journal/UDS（P1.1）与只读 UI 客户端（P1.2）的范围。

## 6. P1.1：有界 journal、snapshot/subscribe-all 与本机只读 UDS（线协议）

默认开启（未设/空串 `CZS_UI_EVENTS` = 开）；journal 与
`<state_dir>/ui-events.sock` 随 UI 服务默认创建，显式 `CZS_UI_EVENTS=0`
（0/false/no/off）时关闭（2026-10-24 翻转前的旧默认为 0）。
journal 作为共享投影 emitter 的订阅者（seq 仍由同一 `UiEventStream` 单写者分配，
无第二份 task 状态）；emit/UI/journal/socket 失败只使 UI 不可用，不改变
delegate、审批、证据与进程回收。

### 6.1 传输与安全（fail-closed，无 fallback）

- NDJSON over Unix domain socket；父目录必须真实目录（拒 symlink）、当前用户
  所有且 mode 0700；socket 文件 0600。路径超长在 bind 前拒绝；同 owner 的
  残留 socket 仅在 connect 得到 ECONNREFUSED（确认已死）时替换，活动监听者
  不可被第二个实例抢占；close 只删除自己 bind 的 inode。
- 每个接受连接经 macOS `getpeereid` 校验同用户；校验失败即断开。
- 任何校验/bind/listen 失败只报 `transport_unavailable`：无临时目录 socket、
  TCP、文件轮询或 PATH 回退。

### 6.2 只读操作（越权即拒）

仅 `hello`、`snapshot`、`snapshot-events`、`subscribe`、心跳。逐 op 字段
allowlist（未知字段/`delegate`/审批回复/tool/shell/路径读取/未知 op/显式错误
版本均拒绝）；单行帧上限 256 KiB（超限显式 `invalid_request` 并有界断开）；
畸形/分片 NDJSON 行跳过不断连。

- `hello`：原子切点返回 `initial_snapshot_cursor`/`initial_cut_digest`/
  `snapshot_version` 与同一切点 streams 诊断（`retention_floor`/
  `high_watermark`）；`client` 标签可选（测试/基准确定性 gate 用）。
- `snapshot`/`snapshot-events`：有界 pull 分页（100 records / 256 KiB，
  每客户端同时仅一个未完成分页，跨 state/events 共用 slot）；页含
  `snapshot_cursor/cut_digest/page/next_page_cursor/done`；continuation 只能
  按上一页游标顺序推进；事件页在切点全部流到达各自 high-water 后才 done。
  事件页从空 reducer 重放与状态页 golden 投影一致；所需事件被淘汰即整体
  `reset_required`。
- `subscribe(stream_id="*", snapshot_cursor=...)`：subscribe-all 必须携带合法
  切点 cursor（无 cursor/latest/手拼 cursor 拒绝）；切点内流只发 high-water
  之后事件，切点后新流从 seq 1 重放（含 accepted 与 hub `stream.announced`）
  ——三时点（after_subscribe / hello_only / between_hello_and_snapshot）均在
  terminal index 之前自动发现 running task，无需二次 hello、按 task 订阅或
  terminal-index 读取。已知单流可 `{stream_id, after_seq}` 订阅（可选
  kinds/workers 过滤，不改服务器序号）。
- 心跳（默认 15 s）仅报告协议存活/snapshot_version/high-water，不作为进程
  退出证据。

### 6.3 有界资源（§4.4 初始上限）

journal 总保留 10,000 events / 16 MiB（全局 FIFO 淘汰）；单事件 16 KiB 上限
（超限不发送原文，订阅者收有界 reset 标记，重放显式 gap，不静默成空流）；
每客户端 outbound 队列 256 events / 1 MiB（溢出 best-effort `reset_required`
 后有界关闭该连接）；cursor registry 64 个 / TTL 300 s（过期
 `cursor_expired`、LRU 淘汰 `cursor_superseded`、未知/篡改 `invalid_request`）；
 超保留窗重放显式 `gap={stream_id,from_seq,to_seq,count}`，绝不静默跳 latest。

### 6.4 覆盖边界

以上均为本地 fake/in-memory + UDS 实测口径；不构成 live（真实 codex
app-server/远程任务）验收或性能 SLO。`fake-ui-journal-server` bench 段仅记录
50 客户端 burst 下的 event-loop lag / publish enqueue 耗时 / 队列峰值原始样本。
