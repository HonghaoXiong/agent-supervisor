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

产物为 `dist/zcode-supervisor-macos-0.1.2.zip`、SHA256 和同名版本解压树。
只复制显式列出的源、许可证、模板和文档；不打包 `.czs`、out、模型/provider/认证配置、
CLI bundle、工作日志、原项目输入或个人 skills 库。运行证据属于每台机器的独立私有目录。

另一台 Mac 的使用说明见 [安装说明](../packaging/macos/INSTALL.zh-CN.md)。
安装器先校验完整文件清单，再复制到版本化目录，调用 `codex plugin marketplace add` 与
`codex plugin add`。没有直接覆写 Codex config.toml，也不覆盖已存在的其他 marketplace。
插件缓存由 Codex 自己管理；升级时保留旧版本目录和任务证据。
0.1.2（沿用 0.1.1 的升级事务机制）将来源切换、插件安装、完整缓存核验及回执作为一个升级事务处理；后续失败时恢复
已验证的旧来源与已启用插件，单独报告恢复失败。旧插件禁用或状态不明时前置拒绝切换，
不改变用户的禁用偏好。缓存拒绝多余、缺失、字节不同或符号链接成员，仅普通 Finder 元数据例外。

`Check.command` 和安装器共用标准库诊断，检查依赖及所需 Codex 插件子命令，输出稳定错误码、
修复建议与 supported/unsupported/unknown 能力表。已安装技能也可使用包内 `scripts/diagnostics.py`。
插件版本 0.1.2 与执行核心版本 0.1.0 分列，核心仍来自同一组已验收 Git blobs（0.1.2 起纳入固定模型选择的 packet.model 字段）。

`Validate.command` 只初始化 Codex app-server 并查询技能目录，过滤报告为本插件信息，不发送
thread/turn 或 ZCode 模型请求。可用 `--receipt` 汇总目标机器另行授权的小任务，原始证据留在当地。
这为用户回传第二台 Mac 的结果提供入口；本机模拟与安装测试不替代另一台机器的实际验收。

本包不分发 Python/Node/ZCode，也不自动下载运行时。Python 3.11+ 和 Node 应预先安装；
ZCode 自行登录。Intel/Apple Silicon 使用同一纯 Python 包，但这不等于两种硬件都已完成
实际模型联调。未知 ZCode bundle 指纹保持拒绝。ZCode 升级后的兼容性需要独立复核。

已审 baseline 不含 packet.model 可选特性；强制固定模型、严格 all-actor 请求前置预算及
原八路逐 Skill 消费不能因安装成功而声称通过。实际 Codex 成本对照也未完成。

插件采用 [OpenAI 官方打包格式](https://developers.openai.com/plugins/build/plugins)，
包含 portable root manifest 和 Codex compatibility manifest；桌面运行声明参考
[desktop-only plugins](https://developers.openai.com/plugins/deploy/desktop-only)。
安装/发现测试使用隔离 Codex home，不改开发者现有插件、认证或账户。
