# 在另一台 Mac 安装 ZCode Supervisor

1. 解压整个 ZIP，双击 `Install.command`。安装器把完整插件复制到
   `~/.local/share/zcode-supervisor/<版本>-<校验标识>/`，通过 Codex 原生插件命令注册并安装。
   安装成功后可以移走下载的 ZIP 和解压目录。
2. 重启 Codex，开一个新聊天。插件列表应显示 **ZCode Supervisor**。
3. 对 Codex 说：**“使用 ZCode Supervisor 检查这台 Mac 的环境，然后把这个任务交给 ZCode。”**
   也可显式调用 `$zcode-supervisor:zcode-supervisor`（插件目录会加命名空间）。
   插件技能会告诉 Codex 使用包内入口，无需重新解释协作流程。

需要 Python 3.11+、Node.js、支持插件安装命令的 Codex，以及单独安装并已登录的 ZCode。
不需要克隆仓库、pip 安装或把 `czs` 加入 PATH。Apple Silicon 与 Intel 使用同一份纯 Python 包；
本机已验证安装流程，其他机器的实际 ZCode 执行仍需在当地完成环境检查和授权小任务验证。

如果双击未能启动，可在终端运行（路径换成解压目录）：

```sh
bash "/absolute/path/zcode-supervisor-0.1.3/Install.command"
```

安装器不需要 sudo，不下载解释器，不复制登录、provider 配置或历史任务，不发模型请求。
若 Python 未找到，安装 Python 3.11+，或用 `CZS_PYTHON=/absolute/path/python3` 指定现有解释器。
若 Codex CLI 未找到，可用 `--codex /absolute/path/to/codex`，也可在支持本地来源的插件界面
添加解压目录的 marketplace。旧版 Codex 若没有 `plugin add`，请更新 Codex 后重试。

只检查、不安装：

```sh
bash "/absolute/path/zcode-supervisor-0.1.3/Check.command"
```

安装完毕，Codex 会为这台机器初始化独立的 `~/zcode-work/.czs/` 并运行 `doctor`。
新版统一诊断会检查所需 Codex 插件命令、解释器、Node、ZCode 指纹和能力边界；每项失败有
稳定错误码与修复建议。首次初始化使用诊断找到的 Node 绝对路径，避免 Finder 与终端 PATH 不同。
当前只接受 **CLI 0.16.9 的两个已审 SHA256**，版本号相同也不能代替指纹。
如果这台 Mac 的 ZCode 已更新，doctor 可能拒绝执行；应提交新指纹供兼容性复核，不能直接加白名单。
ZCode 自己的登录与模型选择留在当地；本基线不保证强制固定 provider/model，也不接管正在使用的 GUI 聊天。

## 更新与卸载

新版包安装到新的版本目录，不覆盖旧包或已有任务。再次运行同一包可重试原生插件注册。
更新时仅切换安装器目录内校验通过的旧 marketplace 来源，并核对 Codex 缓存中的实际插件字节；
缓存文件集合也必须吻合，额外 Skill、脚本或库文件会被拒绝。若切换后的安装、核验或回执失败，
安装器尝试恢复旧来源和原有已启用插件，并报告失败阶段及恢复结果；恢复失败时保留两版文件供处理。
原有插件已禁用或启用状态无法确认时，安装器在切换前停止，由你在插件界面确认状态后再升级。
若发现同名来源在其他目录，会停下保留原配置，交给你确认来源归属。
卸载用 `codex plugin remove zcode-supervisor@zcode-supervisor-local`，之后按需用
`codex plugin marketplace remove zcode-supervisor-local` 移除来源。
这两步不删除 `~/zcode-work` 里的任务证据；旧安装目录可在确认不再需要时由你自行删除。

## 第二台 Mac 的验证与回传

1. 安装后重启 Codex；双击 `Check.command`，桌面会生成新的环境报告。
2. 双击 `Validate.command`，检查当前 Codex 是否发现并启用了本插件技能。它只查询技能目录，
   不创建模型任务，不启动 ZCode worker；桌面报告的真实任务状态此时为 `not_run`。
3. 在目标 Mac 的 Codex 新聊天中发送：

   > 使用 ZCode Supervisor 检查环境，然后只运行初始化生成的报告例子一次，超时 120 秒。
   > 只读 TASK.md 并生成 REPORT.md，不派子代理、不创建 goal、不执行 shell 命令、不自动重试。
   > 保留成功或失败的全部原始证据，核对实际报告内容，并告诉我 run JSON 的绝对路径。

   这一步发送真实 ZCode 请求并消耗当地额度。超时是墙钟限制，不代表严格模型请求数上限。
   若环境、指纹或所需能力不支持，应报告原因并停止，不绕过保护。
4. 用生成的实际 run JSON 再汇总一次（替换两个绝对路径）：

   ```sh
   bash "/absolute/path/zcode-supervisor-0.1.3/Validate.command" --receipt "/absolute/local/task/run-1.json"
   ```

把桌面的最新验证 JSON 发回当前 Codex 聊天。报告仅包含环境与本插件状态、任务结果摘要，
不附 provider、认证、其他技能目录、原始对话或工作文件；原始任务证据仍保留在目标 Mac。
报告不会把 `delivery_ok` 自动当成功能验收，实际 REPORT.md 内容需由当地 Codex 核对。
失败报告同样可回传，不必重复启动模型来凑成功结果。

## 本版本范围

这是本地技能插件，包含已审 supervisor 核心。执行使用有明确合同的 CLI，证据 MCP 保持可选且只读；
没有新建任意 shell MCP 工具，也没有 hook 在后台启动任务。任务执行会消耗 ZCode 模型额度，安装不会。

工程基线经过冻结 Git 索引测试与边界回放；严格全 actor 模型请求前置预算门仍不支持，子技能实际消费
与成本未被证明的部分仍记 unknown。实际 Codex 公平成本对照未进行，不能把 GLM 探索结果称为 Codex 收益。
交付检查不等于功能或科学方法验收，权限 broker 也不是 OS 沙箱。
插件发行版为 0.1.3，包内已验收执行核心仍为 0.1.0；二者分别显示，执行核心指纹没有因安装器更新改变。

这是独立维护的适配器，不是 ZCode 官方插件。插件格式与本地 marketplace 使用
[OpenAI 官方插件打包文档](https://developers.openai.com/plugins/build/plugins)；
桌面执行标记依据[桌面插件文档](https://developers.openai.com/plugins/deploy/desktop-only)。
