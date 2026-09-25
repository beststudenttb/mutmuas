# 节点基础配置标准 v2.1(2026-09-25)

原则:**基础部分所有节点完全一致;个人风格只放在下表"个人特化"那一栏划出的位置。**
由 A、B、C 三方按同一格式报告各自的实际情况,秘书(结构标准负责人)对比后定稿。改动需经 leader 批准。

## 基础(必须一致)

| # | 项 | 标准 | 自查命令 |
|---|---|---|---|
| 1 | 根目录 | 每台机器一个根:`~/mutmuas/`,下面是 `claude/`、`codex/`(该厂商在本机时才有)、`node/`、`work/`,NATS 主机另有 `server/`。不再有 `~/.mutmuas`,家目录下也不留其他 `mutmuas*` 目录 | `ls -d ~/mutmuas* ~/.mutmuas; ls ~/mutmuas` |
| 2 | node/ | 目录权限 700。只放 `<NODE>.env`(600)、`ca.crt`、`data/`、`node.yaml`(600),以及服务管理器的日志。不放任务产物,也不长期留 .bak | `ls -la ~/mutmuas/node` |
| 3 | node.yaml 路径 | `data_dir`、`credentials_file`、`tls_ca` 都写成相对 node/ 的路径(`./data`、`./<N>.env`、`./ca.crt`)。新节点由 `setup.sh` 自动生成,不手改 | `grep -nE 'data_dir\|credentials_file\|tls_ca\|workdir' ~/mutmuas/node/node.yaml` |
| 4 | 代码版本 | 所有节点的守护进程跑 **claude 分支的同一个提交**,即 `agentctl status` 里每个节点的 code 一致。main 只存原始源码,不部署 | `agentctl status` |
| 5 | Python | mutmuas 自身跑在 conda 环境 `mutmuas`(Python 3.12),`.venv` 由它建立。任务自己要用的 Python 另算 | `readlink -f ~/mutmuas/claude/.venv/bin/python` |
| 6 | 服务 | 守护进程随机器开机自启,掉线自动重启。Linux:`systemd --user` 下的 `mutmuas-agent-node`,并开 linger;macOS:launchd 的 `dev.mutmuas.mutmuas.<NODE>`。启动参数统一为 `agent-node start --config ~/mutmuas/node/node.yaml` | `systemctl --user cat mutmuas-agent-node` / `launchctl print gui/$(id -u)/dev.mutmuas.mutmuas.<N>` |
| 7 | worker 工作目录 | `~/mutmuas/work/<agent-id>`(node.yaml 里写 `../work/<agent-id>`),不和交互会话共用,也不放在代码 checkout 或 node/ 里。做 code 任务的 worker 另用 `repo:` 指向对应的 checkout | 同第 3 项 |
| 8 | 唤醒 | 每个交互 agent **必须能被自动唤醒,且实测过**:由另一节点发一个 query,确认会话被叫醒。标已读之前必须先看到内容,不许盲标 | 实测 |
| 9 | 记忆与工作日志 | 两样都**必须存在**(手册 R7):记忆放长期事实,工作日志按日期写"完成 / 进行中 / 下一步 / leader 原话" | 自报路径 |
| 10 | 命名 | `<厂商>` = 本机主交互会话;`<厂商>-worker` = worker;同厂商其他会话用 `<厂商>-<岗位>`。worker 的 notify 指向同机主会话 | `agentctl agents` |
| 11 | 重启恢复 | 守护进程随机器自动恢复(第 6 项);**交互会话和唤醒器由 leader 手动恢复**。a) 会话的**启动目录 = node.yaml 里该 agent 的 workdir**。Claude Code 按启动目录分项目记忆,换了目录记忆就"丢"了。b) 每个节点在工作日志里维护一份**重启后恢复清单**:开哪些会话、从哪个目录、resume 哪个会话。c) 会话恢复后的第一件事依次是:读工作日志 → 重挂唤醒器 → 处理积压的信 | 清单自报;`grep workdir node.yaml` 与实际启动目录对照 |

## 个人特化(允许不同)

- **唤醒的实现方式**:Claude 用会话后台 `agentctl inbox --wait --peek --only wake` 加游标,或者用脚本;Codex 用 launchd `agentctl watch` + `codex queue`。标准只要求"能被唤醒",不规定具体怎么实现。
- **收发方式**:用 MCP 还是 `agentctl` CLI 都可以。装 MCP 的话,命令格式统一为 `agentctl mcp --config ~/mutmuas/node/node.yaml --as <地址>`。
- **记忆和工作日志的位置**:跟着各自的工具走,比如 Claude Code 的记忆目录。
- **交互会话的工作目录**:放在会话实际运行的地方,可以在根目录之外(例如秘书的 `~/mutmuas-secretary`、研究会话的项目目录)。
- **会话在哪里跑**:用 tmux、Terminal.app 还是其他终端;用 `--resume` 恢复旧会话还是新开;要不要用 SessionStart hook 自动提示恢复步骤。
- **平台专属服务**:例如 Mac 的防睡眠服务 `dev.mutmuas.mutmuas.A.nosleep`。
- **实验用的 worktree**:放在 `~/mutmuas/worktrees/`,可选,用完清理。

## 已知待办(2026-09-25)

- A:把 codex-worker 的 workdir 从 `../codex` 改到 `../work/codex-worker`(`repo:` 保留 `../codex`),需 A:codex 同意;清理 `worktrees/` 里两个旧版守护进程残留的 `A-codex-worker-T-*` 目录,以及搬家前留下的两份旧记忆目录;A:claude 补上工作日志。
- 唤醒:给 src 加 `inbox --wait --new`,只等挂上之后才到的新消息,省掉各节点自己维护游标。由 A:claude 在 exp/ 分支做。
