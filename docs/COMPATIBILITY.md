# 兼容性与权限补丁

## 已审查基线

| 项目 | 当前范围 |
| --- | --- |
| Python | 3.11+；CI 选择 3.11、3.13 |
| 平台 | macOS / Linux 的 POSIX 进程与文件机制；原生 Windows 未验证 |
| ZCode CLI | 仅 `REVIEWED_BUNDLES` 注册表内已审查的 0.16.9 / 0.17.0 精确 bundle 指纹 |
| 协议 | ZCode Protocol v1，NDJSON stdio |
| 真实联调基础 | macOS（b816 钉对应 Desktop 3.15.1；31965ce 钉对应 2026-10-14 起的本机安装；a05523 钉对应 Desktop 3.16.2 / build 3.16.2.8769；97490d 钉对应 Desktop 3.16.3 / build 3.16.3.8992，2026-10-24 起的本机安装） |
| Linux | 可配置本机路径并做离线回归；真实安装、登录和模型执行须在目标环境验证 |

协议使用官方 `session/create`、`resume`、`subscribe`、`send`、`read`、`stop` 及权限回调；它不是通用 JSON-RPC，也没有假定 `initialize` / `shutdown` 方法。

同样的版本字符串不一定表示相同构建产物。注册表只接受以下原始 bundle；源码出处无法确认时如实记 unknown，不称为重打包：

| 安装 bundle SHA256 | 兼容副本 SHA256 | 源码出处 | 审查 |
| --- | --- | --- | --- |
| `b816e38656e70fb585fe2ab1ea010da89cda928dac1ccb5d83255fd18796da6e` | `31ef7f33f40591303047db5135845bd39b47399372110483fe6cf6c2df4328d2` | `aac4755666d09fdcd70272fcf063c077a639015f`（已核实） | 2026-10-13 |
| `31965ce1d5d4a808ef156784f67996de42ccab335591d2386a61ea8f6f506a54` | `21ef67a4152d9184009613c7edd187063283dbe92a9db583618b375f1f1e2900` | unknown（版本串 0.16.9 与片段单次出现已验，源码修订未能确认） | 2026-10-14 |
| `a055234a6ca3da0172616d3987cf3c617b6a26deb1332d69035300c2a0ae0de1` | `87e3e4bfb2292ea75b3fb7398ab7c1490ecbe0848785bf69e0d99b893e27ab4c` | unknown（版本串 0.17.0 与片段单次出现已验；Desktop 3.16.2 / build 3.16.2.8769 本机安装，源码修订未能确认） | 2026-10-20 |
| `97490d0f06a7a225d54d47817acf4b4859cdb268b59f2490caaeda982bef063a` | `e27dcb3cb77d21a49fef9eb4b3541a25cac7a0f829ea99e6ba45356456fd42b8` | unknown（版本串 0.17.0 与片段单次出现已验；Desktop 3.16.3 / build 3.16.3.8992 本机安装，源码修订未能确认） | 2026-10-24 |

不同平台分发包或自行构建的同版本 CLI 也可能产生不同哈希；这时会拒绝运行，不能靠改配置跳过检查。替换或新增指纹必须先完成「新版本如何适配」一节的实测审查；旧钉除非明确记录废弃理由，否则保留可用。

**旧缓存升级路径（撤回先前"旧钉机器不受影响"的表述）**：保留旧钉只解决指纹接受问题；由旧单钉版本写下的 v1 缓存目录（manifest 缺 `source_revision_status/reviewed/provenance` 字段）不能被本版本读取——若直接比对会被误判为 provenance 变化而拒绝。本版本的明确策略是**缓存布局版本化**（`CACHE_LAYOUT=2`，目录名带 `-v2` 后缀）：v1 目录不读、不改、不删（原样保留作历史证据），首次运行自动从已验证的安装 bundle 准备 v2 目录；v2 内的补丁副本或 manifest 被篡改仍然拒绝且不自动修复。用户无需手工删除任何旧缓存或 manifest。

**版本策略**：默认仍是 `pinned-0.16.9` 的遗留配置策略（`COMPATIBILITY = "pinned-0.16.9"`）；实际接受的版本跟随所匹配的钉记录中的 `cli_version`（未写明的旧钉按默认 0.16.9 语义），版本字符串本身从不授予兼容性。已有的 v2 缓存不受本次 0.17.0 指纹新增的影响，保持不变。

**新指纹实测记录（2026-10-20）**：在实时允许路径上，初始 Write 已通过；第一条 Bash 因命令末尾多余的句点被正确拒收；随后计划内注册的 resume 以精确允许的命令完成执行并实际更新了目标文件。拒绝路径与模型阶段取消均通过。超时场景没有执行任何 Bash，也没有可续接的 resume 注册表条目，因此它只能证明模型侧取消，不能证明命令取消。以上状态接受仅适用于本地可编辑安装的 supervisor；未做发行插件（`发行插件`，而非"分布式"）验证。最终的演进状态以 最终独立验收（`underworld2-2.17.3/out/zcode-compatibility-20261020/ACCEPTANCE.md`） 为准，最终普通 doctor / 集成验收由该文件控制；此处不声称最终 doctor 已通过。证据根目录：`underworld2-2.17.3/out/zcode-compatibility-20261020`，原始探测证据见其 `LIVE-PROBES-REVIEW.md`。

**新指纹实测记录（2026-10-24，Desktop 3.16.3 / build 3.16.3.8992，CLI 0.17.0）**：背景是 ZCode.app 于 2026-10-23 自动升级，既有钉（独立版注册表三钉与仓内 `.codex/zcode_protocol_cli.py` 单钉）全部按设计拒绝。重钉证据：静态核对 bundle SHA256 `97490d0f…`、审批映射片段单次出现、补丁后 SHA256 `e27dcb3c…`；源码修订未能确认（unknown）。离线 doctor 通过；临时缓存副本篡改一字节被拒绝。活体验证：probe 建会话成功；run-5 纯允许路径 `delivery_ok=true`（精确批准命令前台执行、Write 在合同文件内、model max 档前后快照与 8 条提供方回执核对通过）；run-3/run-4 拒绝路径（`/bin/sh -c 'echo deny-probe'` 及变体均被 broker 拒绝）；run-7 注册 resume 以 Edit 完成续接并 `delivery_ok=true`（同会话、模型选择续接后重设核对）。复核了"只读/低风险命令原生自动执行、不经审批回调、由交付审计拒收"的既有已记录行为（run-1/run-2 中裸 `echo`、`ls`、`grep` 的未配对执行均被拒收），非本版本新增（`autoApproveHighRisk/allowMediumRiskInAuto` 权限设置面在 0.16.9/0.17.0 bundle 中逐字节一致）。本轮新发现并修复一处桥接缺陷：Edit 权限请求携带 schema 默认 `replace_all=false` 而模型流式输入省略该字段，严格相等比对误拒（run-6 证据）；修复后 run-7 交付通过。另登记两个任务包层陷阱（详见 USAGE.md）：`max_timeout_ms < 120000` 且 worker 不显式传 `timeout` 时按默认 120000 比较会被拒；指示 worker 主动尝试被拒命令的 run 按设计不可能 `delivery_ok=true`（被拒终态计入问题）。证据根目录：`underworld2-2.17.3/out/zcode-compatibility-20261024`（probe-1、run-1…run-7 回执与事件流、run5-REPORT.md）。仓内 `.codex/zcode_protocol_cli.py` 已同步重钉至同一安装指纹并启用 `out/zcode-protocol-runtime-v2/` 目录（v1 留存不读）。

## 为什么需要独立副本

在上述源码基线中，[app-config-options.ts](https://github.com/zai-org/ZCode/blob/aac4755666d09fdcd70272fcf063c077a639015f/apps/zcode-cli/packages/bootstrap/src/app/app-config-options.ts) 同时把 `toolAllowlist` 写入 `permission.allowedTools`。工具曝光清单因此成为预批准清单，即使选择 build 模式，Bash / Write 也可能不经过宿主权限回调。真实联调发现并拒收了这种执行。

适配器仅在本机缓存中创建 CLI 副本，删除这项转换：保留工具过滤，不把“模型可见”直接当作“已批准”。补丁前后哈希与原片段单次匹配均受检查，安装的 ZCode 应用保持不变。仓库只保存小型源代码差异及校验逻辑，不分发完整 CLI，也未声称修复已合入上游。

每次启动重新校验原文件和缓存副本。ZCode 升级、构建变化或缓存损坏时停止，明确报告不兼容；没有 yolo、自动允许或静默回退。只读命令仍可能被 ZCode 自身规则自动允许，但缺少匹配审批的 Bash 会被交付审计拒收；这不等于所有此类命令都在执行前阻断。

## 新版本如何适配

适配新版本需要重新完成源码与实测审查，不能只把新哈希加入列表：

1. 查清工具过滤、权限优先级、回调和会话恢复的实际实现。
2. 判断该版本是否仍需补丁；若需要，准备最小差异，记录对应源码与 bundle 指纹。
3. 跑离线回归和协议会话探测。
4. 跑实际允许路径与无害的拒绝路径，并核对工具输入、审批及命令退出证据。
5. 验证续接、超时/异常收尾和新旧输出不覆盖，再更新兼容性记录。

“能找到 CLI 路径”和“协议握手成功”都不足以证明执行控制正确。

## 数据和权限边界

工具面限于 Read、Write、Edit、Bash；不启用动态工作流或子 agent。桥接只为自己的子进程设置必要环境，不修改全局模型配置、不复制登录凭据。

ZCode 仍管理自己的原生会话、缓存和账户数据；初始化目录不是 ZCode 所有数据的完整收纳点。批准的程序、应用内部行为、已配置 hooks 和只读工具运行在真实账户权限下。需要处理不可信代码时，应另行使用合适的操作系统账号、容器或其他系统隔离；本工具没有提供该层保证。
