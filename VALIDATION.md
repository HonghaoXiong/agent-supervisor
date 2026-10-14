# v0.1.0 验证记录

日期：2026-10-14（Asia/Shanghai）。这份记录针对独立 Python 包，不沿用原项目适配器的测试数量。

## 本地与安装包

- Python 3.13.16、macOS、Node.js v26.11.1、已审查的 ZCode CLI 0.16.9 精确 bundle。
- 新建 Python 环境，editable 安装成功；配置初始化与 doctor 成功。
- **114 项离线测试通过**：协议 72、运行时 24、通用状态/锁 11、初始化 CLI 7。
- 构建 sdist 和 wheel 成功；另建干净环境仅安装 wheel，114 项测试再次通过。
- wheel 内包含运行必需的源补丁与 Apache 许可证/第三方声明，不含 CLI bundle、配置或任务日志。

## 真实 ZCode 联调

1. 新建 worker 会话，读取示例并经审批创建 REPORT.md：delivery_ok=true，只有批准文件发生变化。
2. 从已安装 wheel 续接该会话，执行一次精确批准的 Python 命令：实际退出码 0、未超时、前台完成，输出与请求一致；delivery_ok=true。
3. 同会话尝试一次合同外文件创建命令：broker 返回 deny，文件未生成，工作副本无变化；该轮 delivery_ok=false 是预期的拒收结果。

所有真实记录保存在开发电脑忽略的 `.czs/` 中，没有进入 Git 或发行包。这里不发布会话 ID、账户配置或模型对话内容。

## 持续验证与界限

GitHub Actions 对 macOS/Linux × Python 3.11/3.13 运行相同离线测试。远端实际状态以仓库 Actions 为准；不把配置了 CI 等同于 CI 已通过。

本轮未在 Linux 调用真实 ZCode，也未从独立版提交真实服务器作业。远程任务的通用模式是分别批准提交与回收命令；原机器的固定服务器执行器没有被搬进此包。目标机器必须拥有匹配的 CLI bundle、Node.js 和有效的 ZCode 登录。未知 bundle 拒绝执行，不能仅凭版本号假定兼容。

本项目提供审批与证据核验，不是操作系统沙箱，也不代替软件需求或科学方法验收。
