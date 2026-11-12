# agent-supervisor

通过 ZCode 官方 app-server 协议，让 Codex 派发有明确范围的任务，由 ZCode 实现，再由 Codex 根据实际文件、审批和执行证据验收。

Python 包名为 `codex-zcode-supervisor`（仓库名 agent-supervisor），命令为 `czs`；也可使用 `python -m zcode_supervisor`。这是独立的协作适配器，不是 ZCode 官方产品，也不是 [AkiGarage/ZCode-supervisor](https://github.com/AkiGarage/ZCode-supervisor) 的同版本发行。

当前能力包括：

- 独立 worker 会话及同会话续接；0.4.1 支持经用户绑定且空闲的桌面会话原生投递。
- 具名多通道分别绑定 Codex chat、ZCode session、socket、账本和收据目录，可并发工作且保留现有默认通道。
- 显式文件范围、冻结输入哈希、精确 Bash 命令与次数/时间预算。
- 审批与实际工具调用绑定，核验前台命令退出码、超时和文件变化。
- 保存成功与失败证据；提交服务器作业和回收结果可以拆成两个短任务。
- 0.3 增加持久 WorkOwner、版本化设计/候选、幂等生命周期、独立验收、
  依赖检查、知识复用与只读桌面投影；CLI 和 MCP 共用同一服务。
- ZCode cold 路径支持声明式 CreateWorkflow 及随包调研模板；resident
  对不支持的能力明确回退 cold，严格 provider 预算不可用时在派发前拒绝。
- 本机配置独立生成，不携带原开发电脑的路径、账户、模型凭据或任务记录。

**仅接受已审查指纹的 ZCode CLI bundle**（已审清单与版本见[兼容性与补丁](docs/COMPATIBILITY.md)，随审查滚动更新）。 配置可迁移不等于兼容所有 ZCode 版本。macOS 已有真实联调基础；Linux 面向 POSIX 运行与离线测试，仍须在目标机器完成真实联调。原生 Windows 尚未验证。

## 安装与首次使用

多台 Mac 使用 Codex 插件时，优先使用已构建的 `zcode-supervisor-macos-<version>.zip`（版本号以 [pyproject.toml](pyproject.toml) 为准）：
解压后运行 `Install.command`，重启 Codex 并开新聊天，对它说“使用 ZCode Supervisor 检查环境并委派任务”。
`Check.command` 生成无模型环境诊断；`Validate.command` 验证本机技能发现并生成可回传报告。
插件内含已审核心与自动发现技能，无需克隆本仓或单独 pip 安装。
构建与目标机器前提见[插件发行说明](docs/DISTRIBUTION.md)；安装不会发送模型任务。
0.4.1 的桌面原生扩展还需在 ZCode 安装并正常授权，见[原生多通道](docs/NATIVE-CHANNELS.md)与[版本说明](docs/RELEASE-0.4.1.md)。

需要 Python 3.11+、Node.js、已安装且能正常登录使用的 ZCode。本仓库为公开仓库，直接克隆即可；推送权限使用自己的 GitHub 登录或 SSH 配置，不要把 token 放进 URL。

```bash
gh repo clone HonghaoXiong/agent-supervisor
cd agent-supervisor
uv tool install .
```

不用 uv 时可在虚拟环境中安装：

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install .
```

在独立任务目录初始化。本机常规 macOS 安装可自动找到 CLI：

```bash
czs init --directory "$HOME/zcode-work"
cd "$HOME/zcode-work"
czs doctor
```

其他安装位置显式提供 `--cli /absolute/path/to/zcode.cjs`；需要时另加 `--node /absolute/path/to/node` 和 `--provider-config /absolute/path/to/provider.json`。初始化只记录配置文件位置，不复制其内容。每台机器应分别完成 ZCode 登录。

`init` 创建 `.czs/config.json` 与一个只写报告的示例任务。先查看 `.czs/work/example/packet.json`，再执行：

```bash
czs run \
  --packet .czs/work/example/packet.json \
  --out .czs/work/example/run.json \
  --timeout 300
```

这一步会调用实际模型并消耗相应额度。检查输出 JSON 的 `delivery_ok`、审批记录及 `workspace/REPORT.md`；示例不运行 Bash、不声称做过测试。第二次运行要换新的输出名，以保留原证据。

## 在 Codex 中使用

安装上述插件后，Codex 可发现 `zcode-supervisor` skill，按其中的合同与证据流程调用包内入口。
源码/普通 Python 包用户也可把以下约定放进目标项目的协作说明，不需要让两个 GUI 互相模拟点击：

> Codex 负责方案、任务合同和独立验收。每次把小型工作副本放入配置的 work_root，合同和证据放在副本外；通过 czs run 委派给 ZCode。按实际 diff、工具结果和测试输出验收。失败时给出具体返工合同，并续接本桥接登记的 session。通过前不自动合并或发布。

完整命令、任务包字段、续接及远程作业模式见[使用指南](docs/USAGE.md)。桥接本身不自动向 Codex 注册 MCP 工具，也不创建后台调度服务。

## 边界与验证

这是**权限 broker 加执行证据审计，不是操作系统沙箱**。ZCode、只读工具、hooks 和已批准的程序仍在本机账户权限下运行；提示词和哈希检查不能提供系统隔离。使用明确范围的可信小型副本，切勿直接把凭据目录或共享生产工作树交给 worker。

`delivery_ok=true` 表示本次交付检查通过，不代表软件需求、科学方法或长期数值稳定性已经验收。原始日志也可能包含私有代码和模型输出；`.czs/` 不应上传。

离线单元测试在持有本地 `tests/` 树的开发机上运行（`tests/` 与 CI 配置不随本仓库分发）：

```bash
python -m pip install -e .
python -m unittest discover -s tests -v
```

离线矩阵为 macOS/Linux、Python 3.11/3.13，不使用模型凭据、不提交远程作业。原适配器的真实审批、拒绝、同会话续接以及一次独立小型远程提交/回收曾通过验证；迁移后的适配器及目标机器仍须各自验收。见[验证方法](docs/VALIDATION.md)。

版权与第三方来源见 [LICENSE](LICENSE) 和 [THIRD_PARTY_NOTICES.md](THIRD_PARTY_NOTICES.md)。本仓库不分发 ZCode 应用、CLI bundle、登录凭据或原项目代码。
