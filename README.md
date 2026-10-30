# RocketMQ-RS

> Apache RocketMQ 服务端的 Rust 复刻实现（单 crate，edition 2024，rust-version 1.89，Apache-2.0）。
>
> A Rust reimplementation of the Apache RocketMQ server — single crate, edition 2024, rust-version 1.89, Apache-2.0 licensed.

`rocketmq-rs` 用 Rust 重新实现 Java RocketMQ（`org.apache.rocketmq`）的**服务端组件**，目标是与官方 Java 5.x 服务端在协议与行为上对齐。当前聚焦服务端与路由/存储/消费模型，不实现 Java 客户端（`client` 模块）。

`rocketmq-rs` reimplements the **server-side components** of Java RocketMQ (`org.apache.rocketmq`) in Rust, aiming to align with the official Java 5.x server on both protocol and behavior. The current focus is the server itself together with routing, storage, and the consumption model; the Java client (`client` module) is not reimplemented.

- 包名：`rocketmq-rs` · 版本：`0.1.0` · 二进制：`rocketmq`（clap CLI）
- Package: `rocketmq-rs` · Version: `0.1.0` · Binary: `rocketmq` (clap CLI)
- 传输：RocketMQ Remoting（TCP 自定义线协议）+ v2 gRPC 代理（基于 `tonic` / `prost`，proto 见 `proto/`）
- Transport: RocketMQ Remoting (a custom TCP wire protocol) + a v2 gRPC proxy (built on `tonic` / `prost`; protos live in `proto/`)

---

## 与 Java 5.2.2 的客户端对齐 · Java 5.2.2 Client Parity

使用官方 `rocketmq-client-java` 5.2.2 客户端（gRPC v2）进行缺陷猎杀，**15/15 全场景对齐**（对照 zhaohai666-rocketmq Java 参考服务端）。详细场景与修复记录见 [`JAVA_CLIENT_PARITY_REPORT.md`](./JAVA_CLIENT_PARITY_REPORT.md)。

We ran defect-hunting against the official `rocketmq-client-java` 5.2.2 client (gRPC v2) and achieved **15/15 scenario parity** (against the `zhaohai666-rocketmq` Java reference server). Detailed scenarios and fix notes are in [`JAVA_CLIENT_PARITY_REPORT.md`](./JAVA_CLIENT_PARITY_REPORT.md).

| 被测服务端 Server under test | 结果 Result | 缺陷 Defects |
|---|---|---|
| Rust 服务端（commit `8ef9c11`）Rust server | **15/15 PASS** | 0 |
| Java 服务端（干净基线）Java server (clean baseline) | 14/15 PASS | 1（t08 客户端流 flake，非服务端缺陷；client-stream flake, not a server defect） |

覆盖场景 Covered scenarios：POP 不可见窗口与重投；ChangeInvisibleDuration 校验；TAG 过滤；消息类型与 topic 声明冲突拒绝；priority 回传；lite 订阅/退订；同组负载均衡；已过期 deliveryTimestamp 投递；空 body 拒绝；孤儿事务回查；长轮询等。

POP invisible window & redelivery; ChangeInvisibleDuration validation; TAG filtering; rejection of message-type vs. topic-declaration conflicts; priority echo-back; lite subscribe/unsubscribe; intra-group load balancing; delivery of expired `deliveryTimestamp`; empty-body rejection; orphan transaction check-back; long polling, and more.

---

## 功能模块 · Modules

`src/` 下按模块组织（与 Java 对应组件对齐）：

`src/` is organized by module, each aligned with its Java counterpart:

| 模块 Module | 对应 Java 组件 Java counterpart | 说明 Description |
|---|---|---|
| `namesrv` | NameServer | 路由注册表、broker 心跳与超时剔除 / Routing registry, broker heartbeats, and timeout eviction |
| `broker` | BrokerController | 消息收发、POP 消费、事务半消息巡检、消费过滤、topic/队列管理 / Send/receive, POP, half-message check, consume filter, topic/queue management |
| `store` | MessageStore | 提交日志式存储、消费队列、消息 key 索引、QUERY_MESSAGE / Commitlog storage, consume queues, key index, QUERY_MESSAGE |
| `proxy` | ProxyStartup | v2 gRPC（MessagingService + Admin）、客户端注册表、telemetry、lite topic、FIFO 队列钉定、Settings 合并回推、事务回查推送 / v2 gRPC, client registry, telemetry, lite topic, FIFO queue pinning, Settings merge-back, txn check-back push |
| `controller` | ControllerManager | HA 主选举、sync-state 管理 / HA master election and sync-state management |
| `container` | BrokerContainer | 单进程托管多个静态配置的内层 broker / One process hosting multiple statically-configured inner brokers |
| `remoting` | Remoting | TCP 线协议编解码、请求分发、broker 注册/注销 / TCP codec, request dispatch, broker register/unregister |
| `schedule` | 定时消息 | 延迟级别投递（timer wheel），`DEFAULT_DELAY_LEVELS` / Delayed-level delivery (timer wheel) |
| `filter` | 消费过滤 | TAG / SQL92 过滤 / TAG / SQL92 filtering |
| `raft` | Dledger | 控制器 Raft 状态机 / Controller Raft state machine |
| `tieredstore` | TieredStore | 分层存储（冷数据卸载）/ Tiered storage (cold-data offloading) |
| `auth` | ACL / TLS | 鉴权与 rustls TLS（见[安全说明](#安全说明--security)）/ Auth and rustls TLS (see [Security](#安全说明--security)) |
| `openmessaging` | OpenMessaging | OMS 接入层 / OMS access layer |
| `common` / `srvutil` | 公共 / 工具 | 错误类型、配置、通用工具 / Error types, config, utilities |

---

## 快速开始 · Quick Start

### 构建 · Build

```bash
cargo build            # 开发构建，0 error / 0 warning
cargo build --release  # 发布构建
```

```bash
cargo build            # dev build, 0 error / 0 warning
cargo build --release  # release build
```

### 启动一个本地开发集群 · Start a local dev cluster

```bash
# 1) NameServer（默认 127.0.0.1:9876）
cargo run -- namesrv

# 2) Broker（默认 127.0.0.1:10911，自动注册到 NameServer）
cargo run -- broker --namesrv 127.0.0.1:9876

# 3) 或一条命令拉起 NameServer + Broker（开发/测试用）
cargo run -- standalone

# 4) v2 gRPC 代理（默认 127.0.0.1:8081，前置 namesrv/broker）
cargo run -- proxy --namesrv 127.0.0.1:9876
```

```bash
# 1) NameServer (default 127.0.0.1:9876)
cargo run -- namesrv

# 2) Broker (default 127.0.0.1:10911, auto-registers to NameServer)
cargo run -- broker --namesrv 127.0.0.1:9876

# 3) or bring up NameServer + Broker in one command (dev/test)
cargo run -- standalone

# 4) v2 gRPC proxy (default 127.0.0.1:8081, fronts namesrv/broker)
cargo run -- proxy --namesrv 127.0.0.1:9876
```

### 管理命令（`admin` 子命令）· Admin commands (`admin` subcommand)

```bash
cargo run -- admin cluster-list --namesrv 127.0.0.1:9876
cargo run -- admin topic-route  --namesrv 127.0.0.1:9876 --topic TestTopic
cargo run -- admin topic-create --broker 127.0.0.1:10911 \
        --topic TestTopic --queues 4 --message-type NORMAL
cargo run -- admin runtime --broker 127.0.0.1:10911
```

---

## CLI 子命令 · CLI subcommands

| 子命令 Subcommand | 默认地址 Default addr | 说明 Description |
|---|---|---|
| `namesrv` | `127.0.0.1:9876` | 启动 NameServer / Start the NameServer |
| `broker` | `127.0.0.1:10911` | 单 broker（无 HA/复制）；支持 `--advertise`、`--namesrv`、`--broker-name`、`--cluster-name`、`--default-queues`、`--auto-create-topic`、`--enable-property-filter`、`--auto-create-group`、`--message-delay-level`、`--timer-wheel-enable`、`--recall-message-enable` 等 / Single broker (no HA/replication); supports `--advertise`, `--namesrv`, `--broker-name`, `--cluster-name`, `--default-queues`, `--auto-create-topic`, `--enable-property-filter`, `--auto-create-group`, `--message-delay-level`, `--timer-wheel-enable`, `--recall-message-enable`, etc. |
| `standalone` | namesrv `9876` / broker `10911` | 单进程 NameServer + Broker（非 Java broker-container 动态管理）/ Single-process NameServer + Broker (not the Java broker-container dynamic management) |
| `controller` | `127.0.0.1:9878` | HA 控制器（主选举 / sync-state）/ HA controller (master election / sync-state) |
| `proxy` | `127.0.0.1:8081` | v2 gRPC 代理；`--config` 可选 JSON `ProxyConfig`，CLI 覆盖配置值 / v2 gRPC proxy; `--config` takes an optional JSON `ProxyConfig`, CLI overrides config values |
| `container` | — | 单进程托管多个静态内层 broker（`--config` 指向 `BrokerContainerConfig` JSON）/ Single process hosting multiple static inner brokers (`--config` points to a `BrokerContainerConfig` JSON) |
| `admin` | — | 集群/路由/运行时查看与建 topic（`cluster-list` / `topic-route` / `topic-create` / `runtime`）/ Cluster/route/runtime inspection and topic creation |

> 通配符 broker 监听器（`0.0.0.0` / `[::]`）必须显式传 `--advertise` 具体 IP:port，否则启动被拒。
>
> Wildcard broker listeners (`0.0.0.0` / `[::]`) must pass an explicit `--advertise` IP:port, otherwise startup is rejected.

---

## 测试与验证 · Tests & Verification

```bash
cargo test             # 全量回归（lib + main + e2e + process + schedule + sql_filter_diff）
```

```bash
cargo test             # full regression (lib + main + e2e + process + schedule + sql_filter_diff)
```

- 集成/端到端 Integrated / end-to-end：`tests/e2e.rs`、`tests/process.rs`、`tests/schedule.rs`、`tests/sql_filter_diff.rs`
- Java 互操作 Java interop：`tests/JavaInterop.java` + `tests/java_interop.py`（配合 Java 客户端做真实协议联调 / real protocol integration with the Java client）
- 当前验证基线 Current verification baseline：`cargo build` 0 error / 0 warning；`cargo test` 全绿；Java 5.2.2 客户端缺陷猎杀 15/15 PASS / `cargo build` 0 error / 0 warning; `cargo test` all green; Java 5.2.2 client defect-hunting 15/15 PASS

---

## 安全说明 · Security

> ⚠️ **TLS 与鉴权尚未在监听路径上启用。** 当前 dev CLI 的 remoting / gRPC 监听器以明文运行，请勿将其直接暴露于公网。`auth`（ACL）与 TLS（rustls）模块已引入，但服务端尚未在监听流程中强制开启。
>
> ⚠️ **TLS and authentication are not yet enabled on the listener path.** The current dev CLI runs remoting / gRPC listeners in plaintext — do not expose them directly to the public internet. The `auth` (ACL) and TLS (rustls) modules are present but the server does not yet force-enable them on the listen path.

---

## 目录结构 · Directory Layout

```
rocketmq-rs/
├── Cargo.toml            # 单 crate 包定义 + 二进制 `rocketmq`
├── build.rs              # proto 编译（tonic-build / prost-build）
├── proto/                # gRPC v2 .proto 定义（3 个文件）
├── src/                  # 服务端实现（16 个模块 + main.rs + lib.rs）
├── tests/                # e2e / process / schedule / sql_filter_diff / Java 互操作
├── data/                 # 默认本地数据目录（namesrv / broker）
└── JAVA_CLIENT_PARITY_REPORT.md   # Java 5.2.2 客户端对齐报告
```

```
rocketmq-rs/
├── Cargo.toml            # single-crate package definition + `rocketmq` binary
├── build.rs              # proto compilation (tonic-build / prost-build)
├── proto/                # gRPC v2 .proto definitions (3 files)
├── src/                  # server implementation (16 modules + main.rs + lib.rs)
├── tests/                # e2e / process / schedule / sql_filter_diff / Java interop
├── data/                 # default local data dir (namesrv / broker)
└── JAVA_CLIENT_PARITY_REPORT.md   # Java 5.2.2 client parity report
```

---

## 许可 · License

Apache-2.0。详见 [`LICENSE`](./LICENSE)。

Apache-2.0. See [`LICENSE`](./LICENSE).
