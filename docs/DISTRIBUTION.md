# macOS 插件发行

发行包把可自动发现的 Codex skill 与经过独立 Git blob 验收的 supervisor 核心放在一起。
不要求目标机器克隆私有仓库，也不依赖开发机的虚拟环境。标准安装为技能插件，通过包内 CLI
执行；可选只读 MCP 仍单独管理，不会偷偷注册执行工具、hook 或账户。

`plugins/zcode-supervisor/` 是插件模板，`lib/` 由构建器从已审 Git index 填充。
`packaging/accepted-core.json` 固定核心 blob 和 SHA256；构建器发现索引变化就拒绝，不能把
工作树内并行 model-selection 改动误打包。修改核心后应先完成新候选验收再更新 pin。

```sh
python3 tools/build_macos_plugin.py --output dist
```

产物为 `dist/zcode-supervisor-macos-<version>.zip`、SHA256 和同名版本解压树（版本号单一来源为 `src/zcode_supervisor/__init__.py` 的 `__version__`，构建器校验全部版本引用一致）。
只复制显式列出的源、许可证、模板和文档；不打包 `.czs`、out、模型/provider/认证配置、
CLI bundle、工作日志、原项目输入或个人 skills 库。运行证据属于每台机器的独立私有目录。

另一台 Mac 的使用说明见 [安装说明](../packaging/macos/INSTALL.zh-CN.md)。
安装器先校验完整文件清单，再复制到版本化目录，调用 `codex plugin marketplace add` 与
`codex plugin add`。没有直接覆写 Codex config.toml，也不覆盖已存在的其他 marketplace。
插件缓存由 Codex 自己管理；升级时保留旧版本目录和任务证据。
0.1.4（沿用既有升级事务机制）将来源切换、插件安装、完整缓存核验及回执作为一个升级事务处理；后续失败时恢复
已验证的旧来源与已启用插件，单独报告恢复失败。旧插件禁用或状态不明时前置拒绝切换，
不改变用户的禁用偏好。缓存拒绝多余、缺失、字节不同或符号链接成员，仅普通 Finder 元数据例外。

`Check.command` 和安装器共用标准库诊断，检查依赖及所需 Codex 插件子命令，输出稳定错误码、
修复建议与 supported/unsupported/unknown 能力表。已安装技能也可使用包内 `scripts/diagnostics.py`。
早期发行曾将插件版本与执行核心版本分列（0.1.4/0.1.0），自 0.2.0 起二者同步（单一版本源见上文）；核心仍来自同一组已验收 Git blobs（自 0.1.3 起随包携带三方编排 hub：tri-probe/常驻驱动/合同库/MCP 控制面）。

`Validate.command` 只初始化 Codex app-server 并查询技能目录，过滤报告为本插件信息，不发送
thread/turn 或 ZCode 模型请求。可用 `--receipt` 汇总目标机器另行授权的小任务，原始证据留在当地。
这为用户回传第二台 Mac 的结果提供入口；本机模拟与安装测试不替代另一台机器的实际验收。

本包不分发 Python/Node/ZCode，也不自动下载运行时。Python 3.11+ 和 Node 应预先安装；
ZCode 自行登录。Intel/Apple Silicon 使用同一纯 Python 包，但这不等于两种硬件都已完成
实际模型联调。未知 ZCode bundle 指纹保持拒绝。ZCode 升级后的兼容性需要独立复核。

0.3 的核心包括持久 WorkOwner、完整设计到独立验收生命周期、原生 CreateWorkflow
与随包 path template、冷/常驻 scoped 配对、知识复用和协商式只读桌面投影。
随包 `WORKFLOW.md` 与 `CAPABILITY-MATRIX.md` 是功能边界入口。公开 RPC 不支持严格
provider token/cost cap，严格预算请求在派发前 fail-closed；实际货币成本仍未知。
本轮完成了 4 任务×4 条件的真实对照：16 个 worker 均交付、独立 oracle 质量 14/16；
完整协议有无经验均为 4/4，单轮墙钟/token 差异不构成稳定收益证明。原始 live
回执属于各机器的私有验收证据，不随公开仓库或插件分发。

插件采用 [OpenAI 官方打包格式](https://developers.openai.com/plugins/build/plugins)，
包含 portable root manifest 和 Codex compatibility manifest；桌面运行声明参考
[desktop-only plugins](https://developers.openai.com/plugins/deploy/desktop-only)。
安装/发现测试使用隔离 Codex home，不改开发者现有插件、认证或账户。


## 0.4.1 原生扩展载荷

发行包同时包含 Codex 的 `.agents/plugins/marketplace.json` 与 ZCode 本地市场的
`.claude-plugin/marketplace.json`，两者指向同一完整插件载荷。ZCode manifest、
原生 extension 和注册/投递脚本纳入显式模板白名单，新增核心模块继续通过 Git blob pin 核验。
`NATIVE-CHANNELS.md` 随包提供首次授权、桌面加载、具名绑定与旧通道兼容说明。

`Install.command` 继续只管理 Codex 安装事务，不自动授予 ZCode 权限或改动已有通道。
ZCode 安装步骤与验证边界见 [原生多通道](NATIVE-CHANNELS.md)。不要直接使用未构建的
源码插件模板；其 `lib/` 仅在构建发行包时填充。
