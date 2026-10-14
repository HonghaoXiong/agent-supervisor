# 使用指南

## 配置和目录

`czs init --directory PROJECT` 生成 `PROJECT/.czs/`。重复初始化会拒绝覆盖已有目录。全局参数 `--config` 放在子命令前；默认读取当前目录的 `.czs/config.json`：

```bash
czs --config /absolute/project/.czs/config.json doctor
```

配置中的 `cli_path`、可选 `provider_config` 指向本机安装；`node` 是 Node.js 命令或路径。`work_root`、`state_dir`、`cache_dir` 相对于配置文件所在目录解析，缺省分别为 `work`、`state`、`cache`。不要把旧机器生成的配置、缓存或状态目录直接提交到 Git。

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

执行器应自行设置确定的工作目录和必要环境，核验运行前后输入哈希，保存完整测试结果，并传回真实退出码。测试数量和测试内容属于独立验收要求，桥接不会从一句“测试通过”推断正确性。

## 派发、验收与续接

```bash
czs run --packet .czs/work/task-001/packet.json \
  --out .czs/work/task-001/run.json --timeout 300
```

退出码为 0 且 `delivery_ok=true` 才表示交付检查通过。核对 `permission_decisions`、`commands`、`execution_problems`、`changed_files`，并单独审查实际文件差异和测试证据。Bash 结果必须包含 `exitCode=0`、`timedOut=false`、`status=completed`；缺少审批或终态证据会拒收。

`run.events.jsonl` 保存协议证据，`run.stderr.log` 保存子进程诊断；部分 transport 凭据字段会脱敏，但任意提示词、代码和程序输出不能保证自动脱敏。保留在本地，不把真实日志作为仓库示例。

返工时建立新的合同、使用新的输出名，传回之前结果中的 `session_id`：

```bash
czs run --packet .czs/work/task-001/rework.json \
  --out .czs/work/task-001/rework-run.json \
  --resume sess_<之前结果中的ID> --timeout 300
```

只能续接此配置状态目录已登记、进程已退出、工作副本一致的会话。不要随意选择 GUI 会话或迁移原生会话数据库。超时或失败后先检查保留证据及已发生的副作用，再决定返工；不要把重试当作撤销。

## 远程任务：提交与回收分开

本仓库不内置 SSH 账户、集群地址或任意远程执行接口。针对目标环境准备两份经审查的固定执行器，分别加入两个任务包的精确命令列表。

1. **提交阶段**：冻结源码和参数清单；先写唯一提交意图，再上传到新建的限定目录；申请明确资源上限，保存原始返回和作业 ID，立即结束模型 turn。结果不明时查询已有意图和回执，不能盲目再次提交。
2. **回收阶段**：后续单独续接会话；只读取回执登记的作业与目录；收集调度器终态、退出码、源码哈希、测试数量和机器可读完成记录，保存本地证据。
3. **独立验收**：Codex 同时核对调度器和工作程序证据。队列消失、文件缺失或调度器记录过期应记为 pending/unknown，不得推断成功。测试退出 0 仍需满足任务的内容判据。

调度器状态可能有保留期限，生产执行器应持久化必要完成证据。所批准的资源、远程写入根目录、命令预算和失败处置都属于任务合同，不能靠自由文本报告替代。这种分阶段模式不需要让一个模型调用等候数小时，也不自动创建后台监控。
