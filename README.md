# RocketMQ-RS

> Apache RocketMQ 服务端的 Rust 复刻实现（单 crate，edition 2024，rust-version 1.89，Apache-2.0）。

`rocketmq-rs` 用 Rust 重新实现 Java RocketMQ（`org.apache.rocketmq`）的**服务端组件**，目标是与官方 Java 5.x 服务端在协议与行为上对齐。当前聚焦服务端与路由/存储/消费模型，不实现 Java 客户端（`client` 模块）。

- 包名：`rocketmq-rs` · 版本：`0.1.0` · 二进制：`rocketmq`（clap CLI）
- 传输：RocketMQ Remoting（TCP 自定义线协议）+ v2 gRPC 代理（基于 `tonic` / `prost`，proto 见 `proto/`）

---

## 与 Java 5.2.2 的客户端对齐

使用官方 `rocketmq-client-java` 5.2.2 客户端（gRPC v2）进行缺陷猎杀，**15/15 全场景对齐**（对照 zhaohai666-rocketmq Java 参考服务端）。详细场景与修复记录见 [`JAVA_CLIENT_PARITY_REPORT.md`](./JAVA_CLIENT_PARITY_REPORT.md)。

| 被测服务端 | 结果 | 缺陷 |
|---|---|---|
| Rust 服务端（commit `8ef9c11`） | **15/15 PASS** | 0 |
| Java 服务端（干净基线） | 14/15 PASS | 1（t08 客户端流 flake，非服务端缺陷） |

覆盖场景：POP 不可见窗口与重投、ChangeInvisibleDuration 校验、TAG 过滤、消息类型与 topic 声明冲突拒绝、priority 回传、lite 订阅/退订、同组负载均衡、已过期 deliveryTimestamp 投递、空 body 拒绝、**孤儿事务回查**、长轮询等。

---

## 功能模块

`src/` 下按模块组织（与 Java 对应组件对齐）：

| 模块 | 对应 Java 组件 | 说明 |
|---|---|---|
| `namesrv` | NameServer | 路由注册表、broker 心跳与超时剔除 |
| `broker` | BrokerController | 消息收发、POP 消费、事务半消息巡检、消费过滤、topic/队列管理 |
| `store` | MessageStore | 提交日志式存储、消费队列、消息 key 索引、QUERY_MESSAGE |
| `proxy` | ProxyStartup | v2 gRPC（MessagingService + Admin）、客户端注册表、telemetry、lite topic、FIFO 队列钉定、Settings 合并回推、事务回查推送 |
| `controller` | ControllerManager | HA 主选举、sync-state 管理 |
| `container` | BrokerContainer | 单进程托管多个静态配置的内层 broker |
| `remoting` | Remoting | TCP 线协议编解码、请求分发、broker 注册/注销 |
| `schedule` | 定时消息 | 延迟级别投递（timer wheel），`DEFAULT_DELAY_LEVELS` |
| `filter` | 消费过滤 | TAG / SQL92 过滤 |
| `raft` | Dledger | 控制器 Raft 状态机 |
| `tieredstore` | TieredStore | 分层存储（冷数据卸载） |
| `auth` | ACL / TLS | 鉴权与 rustls TLS（见[安全说明](#安全说明)） |
| `openmessaging` | OpenMessaging | OMS 接入层 |
| `common` / `srvutil` | 公共 / 工具 | 错误类型、配置、通用工具 |

---

## 快速开始

### 构建

```bash
cargo build            # 开发构建，0 error / 0 warning
cargo build --release  # 发布构建
```

### 启动一个本地开发集群

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

### 管理命令（`admin` 子命令）

```bash
cargo run -- admin cluster-list --namesrv 127.0.0.1:9876
cargo run -- admin topic-route  --namesrv 127.0.0.1:9876 --topic TestTopic
cargo run -- admin topic-create --broker 127.0.0.1:10911 \
        --topic TestTopic --queues 4 --message-type NORMAL
cargo run -- admin runtime --broker 127.0.0.1:10911
```

---

## CLI 子命令

| 子命令 | 默认地址 | 说明 |
|---|---|---|
| `namesrv` | `127.0.0.1:9876` | 启动 NameServer |
| `broker` | `127.0.0.1:10911` | 单 broker（无 HA/复制）；支持 `--advertise`、`--namesrv`、`--broker-name`、`--cluster-name`、`--default-queues`、`--auto-create-topic`、`--enable-property-filter`、`--auto-create-group`、`--message-delay-level`、`--timer-wheel-enable`、`--recall-message-enable` 等 |
| `standalone` | namesrv `9876` / broker `10911` | 单进程 NameServer + Broker（非 Java broker-container 动态管理） |
| `controller` | `127.0.0.1:9878` | HA 控制器（主选举 / sync-state） |
| `proxy` | `127.0.0.1:8081` | v2 gRPC 代理；`--config` 可选 JSON `ProxyConfig`，CLI 覆盖配置值 |
| `container` | — | 单进程托管多个静态内层 broker（`--config` 指向 `BrokerContainerConfig` JSON） |
| `admin` | — | 集群/路由/运行时查看与建 topic（`cluster-list` / `topic-route` / `topic-create` / `runtime`） |

> 通配符 broker 监听器（`0.0.0.0` / `[::]`）必须显式传 `--advertise` 具体 IP:port，否则启动被拒。

---

## 测试与验证

```bash
cargo test             # 全量回归（lib + main + e2e + process + schedule + sql_filter_diff）
```

- 集成/端到端：`tests/e2e.rs`、`tests/process.rs`、`tests/schedule.rs`、`tests/sql_filter_diff.rs`
- Java 互操作：`tests/JavaInterop.java` + `tests/java_interop.py`（配合 Java 客户端做真实协议联调）
- 当前验证基线：`cargo build` 0 error / 0 warning；`cargo test` 全绿；Java 5.2.2 客户端缺陷猎杀 15/15 PASS

---

## 安全说明

> ⚠️ **TLS 与鉴权尚未在监听路径上启用。** 当前 dev CLI 的 remoting / gRPC 监听器以明文运行，请勿将其直接暴露于公网。`auth`（ACL）与 TLS（rustls）模块已引入，但服务端尚未在监听流程中强制开启。

---

## 已知差距（低优先级，未修）

> 以下三项已在 2026-10-29 的提交中修复，此处保留为变更记录：

- **长轮询参数边界校验**：`ReceiveMessage` 路径现复用 `ProxyConfig` 的 `grpcClientConsumer{min,max}LongPollingTimeoutMillis`（默认 5000 / 20000）对 `awaitDuration` 做 clamp，并对 `invisibleDuration` 做 `[0, 12h]` 校验（`ILLEGAL_INVISIBLE_TIME`），不再只是统一 clamp `0..30s`。
- **事务回查 per-message 计数**：`run_transaction_check` 现按 `(born_ts, check_times)` 跟踪每条半消息，免疫时间到达后递增 `check_times`，超过 `transactionCheckMax`（默认 15）即停止复查（Java `PROPERTY_TRANSACTION_CHECK_TIMES` 语义；注：Java 5.2.2 实际为常量免疫时间 + 计数上限，并非旧版描述的翻倍退避）。
- **producer 注册粒度**：`ClientRegistry` 新增 per-channel receipt-handle 缓存（`cache_handle` / `lookup_handle`，TTL 12h）与空闲客户端回收（`reap_expired` + `run_registry_reaper`，每 5s 扫描、空闲 >120s 剔除并清理 `group_members` / `producer_topics`），对齐 Java `MessageReceiptHandleManager` + `GrpcChannelManager.scanNotActiveChannel`。

---

## 目录结构

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

---

## 许可

Apache-2.0。详见 [`LICENSE`](./LICENSE)。
