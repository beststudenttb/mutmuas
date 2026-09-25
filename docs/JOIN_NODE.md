# 新节点手动接入 mutmuas(以笔记本 = 节点 C 为例)

适用:claude 分支,结构标准 v1(每台机器一个根 `<ROOT>/mutmuas/{claude,codex,node,server}`)。
一台机器 = 一个节点。节点名 A = Mac mini,B = 服务器(NATS 在这里),C = 笔记本。

**密钥规则**:`C.env` 是密码,**不进 git、不贴进聊天、不写进消息**。只能用 scp、U 盘这类方式直接拷贝。`ca.crt` 是公开证书,不算秘密。

---

## 第 0 步 前提(笔记本)

- **先读 [BEFORE_JOINING.md](BEFORE_JOINING.md)(入职前须知)**,读完再往下做。

- 系统:macOS 或 Linux。Windows 请在 WSL2(Ubuntu)里操作,本文按 Linux 走。
- 已装:`git`、conda(Miniconda/Anaconda 都行)、Claude Code CLI(`claude --version` 能出版本号)。
- 能访问服务器:`nc -vz 150.89.170.193 4222` 显示 succeeded/open。
- 能拉仓库:`git clone git@github.com:beststudenttb/mutmuas.git` 需要 GitHub 访问权限。

## 第 1 步 服务器签发凭据(由 leader 在服务器 B 上执行)

```bash
cd ~/mutmuas/server && b=backup-$(date +%Y%m%d-%H%M%S) && mkdir -m 700 $b && cp -p nats-server.conf *.env $b/   # 先备份(R4.2)
cd ~/mutmuas/claude
.venv/bin/agent-node server-config --project mutmuas --nodes A,B,C --out ~/mutmuas/server \
    --tls 150.89.170.193,127.0.0.1 --store-dir /home/tb/mutmuas/server/jetstream
systemctl --user reload mutmuas-nats-server
```

- `--tls` 的主机列表**必须和首次生成证书时一致**(现在是 `150.89.170.193,127.0.0.1`)。证书已存在时会直接复用,但哪天证书重签,少写的地址就连不上了:B 本机用 127.0.0.1 连接的进程会 TLS 失败。

- 重新运行时,已有节点(A、B、admin)的密码和 TLS 证书**保持不变**,只新增 `node_C`,已在 B 上用副本验证过。
- 产出两个文件:`~/mutmuas/server/C.env`(密码)和 `~/mutmuas/server/tls/ca.crt`(公开证书)。

## 第 2 步 把两个文件拷到笔记本

在笔记本上执行:

```bash
mkdir -p ~/mutmuas-join && chmod 700 ~/mutmuas-join
scp tb@<服务器地址>:~/mutmuas/server/C.env        ~/mutmuas-join/
scp tb@<服务器地址>:~/mutmuas/server/tls/ca.crt   ~/mutmuas-join/
chmod 600 ~/mutmuas-join/C.env
```

## 第 3 步 拉代码,准备 Python(笔记本)

mutmuas 自身统一用 conda 环境 `mutmuas`(Python 3.12)。任务自己要用的 Python 另算,互不影响。

```bash
conda create -y -n mutmuas python=3.12
mkdir -p ~/mutmuas
git clone -b claude git@github.com:beststudenttb/mutmuas.git ~/mutmuas/claude
cd ~/mutmuas/claude
PYTHON="$(conda run -n mutmuas which python)" scripts/install.sh
```

## 第 4 步 一键配置节点(笔记本)

```bash
~/mutmuas/claude/deploy/claude/setup.sh --node C \
    --server nats://150.89.170.193:4222 \
    --credentials ~/mutmuas-join/C.env --ca ~/mutmuas-join/ca.crt \
    --skip-install --service --mcp
```

- 这一步会生成 `~/mutmuas/node/node.yaml`,并把凭据和证书复制到 `~/mutmuas/node/`,权限为 700/600。
- 同时注册交互式 agent `C:claude`,并跑一遍 doctor 检查。
- `--service`:把守护进程装成开机服务。macOS 用 launchd,Linux 用 systemd --user。
- `--mcp`:在 Claude Code 里注册 mutmuas 工具,让笔记本上的 Claude 可以直接收发消息。
- `--workdir` 是交互会话运行的目录,默认 `~`。需要无人值守的 worker 时,再加上 `--worker`;worker 要用别的目录,就加 `--worker-workdir <目录>`,不加则和交互会话共用同一个目录。

完成后删掉临时副本:`rm -rf ~/mutmuas-join`

最后,在笔记本的交互式 Claude 会话里挂一个**后台**唤醒监视(结构标准第 7 项)。有消息来时它会退出,把会话叫醒;会话处理完消息后,再挂一次:

```bash
~/mutmuas/claude/.venv/bin/agentctl inbox --wait 3600 --peek --only wake \
    --config ~/mutmuas/node/node.yaml --as C:claude
```

## 第 5 步 验证(笔记本)

```bash
cd ~/mutmuas/claude
.venv/bin/agentctl status --config ~/mutmuas/node/node.yaml    # 应能看到 NODE A / B / C 都 ONLINE
.venv/bin/agentctl ask B:claude-secretary "C 节点接入测试:请回 pong" \
    --config ~/mutmuas/node/node.yaml --as C:claude --wait 600
```

收到秘书的回复,就说明接入成功。还要再验证一次**唤醒**:请任意另一个节点给 `C:claude` 发一个 query,确认你的会话被叫醒。只看 status 显示 ONLINE,测不出唤醒有没有问题。之后秘书会发入职说明,内容包括职责、手册和沟通规则。

## 常见问题

| 现象 | 原因 / 处理 |
|---|---|
| `Authorization Violation` | 第 1 步没做或没 reload;或者拷错了 env 文件 |
| TLS handshake error / certificate | 没带 `--ca`;或者地址不在证书里(证书只签了 `150.89.170.193` 和 `127.0.0.1`) |
| 连接超时 | 网络到不了 4222 端口,先用 `nc -vz` 检查 |
| setup.sh 说 node 不匹配 | `~/mutmuas/node/node.yaml` 已存在,而且是别的节点名。先确认再处理,不要直接删 |

## 撤销一个节点(服务器)

1. 用去掉该节点的列表重新运行 `server-config`,例如 `--nodes A,B`,然后 reload。
2. 删除 `~/mutmuas/server/<NODE>.env`。
3. 旧凭据会被拒绝,报 Authorization Violation。
