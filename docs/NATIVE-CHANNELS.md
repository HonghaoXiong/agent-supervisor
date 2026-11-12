# 原生固定会话与多通道（0.4.1）

Codex 可通过 ZCode 官方扩展 API 向用户指定的桌面会话追加指令，再读取同一 session 的真实完成轮次。不同 Codex chat 可各自绑定固定会话、Unix socket、令牌、投递账本和收据目录，同时工作。逐轮发送不使用 GUI 复制、粘贴或截图。

本功能使用 `ctx.sessions.sendToSession`，不写聊天数据库，不自动批准工具，不改变会话的工具权限模式。CLI/RPC 的现有委派方式继续可用。原生投递收据证明消息与完成轮次；实现结果仍由 Codex 独立验收。

## 首次安装

1. 下载并解压 0.4.1 发行包。Codex 侧继续运行 `Install.command`。
2. 原生投递还需在 ZCode 侧安装同一份插件。发行包根目录含 `.claude-plugin/marketplace.json`，其中 `./plugins/zcode-supervisor` 指向完整插件；用 ZCode 的本地插件市场入口添加解压后的发行包目录并安装 `zcode-supervisor`。不要直接安装未构建的源码模板目录。
3. 在 ZCode 正常确认插件申请的 `sessions.write` 权限，执行一次“重新加载扩展”。包内扩展 manifest 声明 extension API `^1.4.0`；应用版本号与 extension API 版本号不同。
4. 在桌面打开要绑定的会话，完成至少一轮，并等待会话空闲。仅在独立 CLI 进程中新建的 session 可能不在应用的可信范围，不能当作已可桌面投递。

安装和注册本身不调用模型；用户用于建立会话锚点的对话会正常消耗模型额度。后续新增通道由扩展约每秒发现，无需再次重载或切换原通道。

## 在各自 Codex chat 注册通道

以下命令中的 `<plugin-root>` 是已安装插件根目录，不是仓库根目录：

```sh
bash "<plugin-root>/scripts/python.sh" "<plugin-root>/scripts/native_channels.py" \
  register --channel task-a --session-id <固定ZCode会话ID> --workspace <绝对工作目录>

bash "<plugin-root>/scripts/python.sh" "<plugin-root>/scripts/native_channels.py" list
```

owner 默认取当前 `CODEX_THREAD_ID`。另一个 chat 对自己的 session 用 `task-b` 注册即可；不要手工修改该环境变量来冒充另一个 chat。用户明确指定其他 chat 的绑定时，维护者可加 `--owner-thread-id <Codex-chat-ID>`。这只是路由与使用约束，不是同一系统账户内的 OS 安全边界。

每个名称接受小写字母、数字、`_`、`-`，最长 48 字符，首字符须为字母或数字；`legacy` 保留。注册检查真实 session 的目录。同名同绑定重复注册返回已有配置，其他更改会被拒绝。同一 session 不能重复注册；socket、state、receipt 路径不得与其他通道重叠。工作目录可以相同，共享文件的修改仍须由任务协调。

私有注册表为 `~/.czs/native-gui/channels.json`，通道配置位于 `channels/<name>/bridge.json`，目录与文件分别要求 0700/0600。命令只输出非秘密绑定信息，不能把配置中的 token 放进报告或 Git。

如需自选 socket 或收据目录，注册时加 `--socket <绝对路径>`、`--receipt-dir <绝对路径>`。配置与账本仍各自独立；不要事后手改已运行的绑定。改变或移除运行中的配置会在 `channels-ready.json` 中报错，原服务保留，交由维护者核对。

## 向固定会话投递一轮

沿用已有 authorization 与阶段合同流程，使用 `compile_stage_contract` 编译并以 `canonical_sha256` 固定合同。为本轮选择唯一 delivery ID：

```sh
bash "<plugin-root>/scripts/python.sh" "<plugin-root>/scripts/native_session.py" \
  --channel task-a --session-id <同一固定ZCode会话ID> \
  --delivery-id <本轮唯一ID> \
  --contract <contract.json> --authorization <authorization.json>
```

具名通道要求当前 chat 与 owner 一致。成功后返回的 `receipt_path` 默认为 `~/.czs/native-gui/channels/task-a/receipts/<delivery-id>.json`。`--out` 可省略；若指定，须留在该通道收据目录内。收据不覆盖，核对 session、turn、contract hash 及实际结果后再给下一轮指令。

相同通道内，原合同配原 delivery ID 可恢复证据而不重发；换合同会被拒绝。不同通道允许相同 delivery ID，各自恢复和落盘。未知通道、owner 不符或配置不符均报错，不回退默认会话。超时或未知接收状态须先核对原 intent、账本和实际消息，不换 ID 盲目重投。

常见拒绝：

| 状态 | 处理 |
|---|---|
| `session-busy` | 等待当前轮次和工具审批结束，再核对未发出的尝试。 |
| `proto.sessionNotFound` | 在桌面打开目标，确认运行时已加载；不要切换通道目标。 |
| `forbidden` / 目标不在可信范围 | 核对是否为桌面应用认识的会话，以及正常插件授权；不绕过应用检查。 |
| `channel-owner-mismatch` | 回到绑定的 Codex chat 调用。 |
| 未知接收或完成状态 | 保留 intent 和账本，先只读核对；不认定成功，不自动重放。 |

## 旧通道与 owner 服务

已有 `~/.czs/native-gui/bridge.json` 继续作为默认 legacy 路由，配置无需迁移。旧调用仍需显式新鲜 `--out`；原生 legacy 协议没有具名 owner 校验。若现有目标已占用该路由，不要为同一 session 再注册一条具名通道。

已有 owner 的 runner 可按任务分别设置：

```json
{
  "session_loop": {
    "native_transport": "desktop-extension",
    "native_channel": "task-a",
    "native_owner_thread": "<本任务Codex-chat-ID>"
  }
}
```

这只是配置片段，不能覆盖完整 runner 的其他字段。owner 没有继承 Codex chat 环境时需显式 `native_owner_thread`；自选私有根目录时可加 `native_channels_root`。每个循环仍须绑定自己的 reviewer/Codex thread 及既有状态根目录。注册通道不会创建或改绑自动回投 pump，也不应为一个业务会话另起竞争 owner。

选一个 chat 维护共享插件代码与安装，其余 chat 只注册和使用通道。库提供 `installation_guard` 供安装工具使用，以维护者记录和文件锁串行安装；这不限制用户或其他程序直接写文件。本机项目专用的维护者/业务 ID、收据与回投配置不随发行包携带。

## 验证范围

2026-11-12 在 macOS / ZCode 0.17.1、extension API 1.4.0 上，两条专用桌面会话原生并发完成，实际轮次重叠 2106 ms；相同 delivery ID 分别落盘，恢复不重发，GUI 分别显示插件消息。两个测试通道的 owner 是同一个维护 chat；不同 owner 的拒绝另有离线回归及真实配置拒绝检查，不冒称两个独立 Codex chat 的并行自动回投已实测。

桌面未加载目标、CLI-only 目标的首轮拒绝均保留在本机验收中。最终测试需先在 GUI 一次性创建/打开并取得锚点，随后投递与恢复全部走原生接口。工具 Write/Bash 的单会话联调已有证据，正常审批仍适用。Linux 桌面与其他 ZCode 版本未获本轮真机资格；私有会话和日志不随公开仓库分发。
