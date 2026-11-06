# Java 版 RocketMQ 功能覆盖差距报告（Rust 复刻）

> 审计日期：2026-11-06 ｜ 基线：`5bdde8a`（含 `8ef9c11` 的 15/15 对齐 + 后续 `c8f473e` 等 7 个提交）+ 本轮 `main.rs` 修正
> Java 参考：`/Users/haizai/project/jingsai/roocketmq/zhaohai666-rocketmq`（11 个模块、1,034 个源文件）
> Rust 复刻：`/Users/haizai/project/RocketMQ-RS/src`（16 个文件、32,594 行，单 crate）
> 方法：4 路并行源码审计（broker / proxy / store+tieredstore / 辅助模块），再对关键结论逐条复核源码
> 补齐进度：§7 按 §5 的顺序逐项记录落地情况；原表保留审计当时的现状，不回改结论。

---

## 0. 一句话结论

**Rust 版把「Java 5.x gRPC 客户端能观察到的行为」做全了，但把「服务端赖以生产运行的基础设施」基本没做。**

| 维度 | 状态 |
|---|---|
| gRPC v2 MessagingService | **17/17 方法全部实现**（QueryRoute … SyncLiteSubscription） |
| gRPC v2 AdminService | **16/16 方法全部实现**，但多数是语义空壳（见 P1） |
| Java 5.2.2 客户端缺陷猎杀 | **15/15 PASS**（`JAVA_CLIENT_PARITY_REPORT.md`） |
| 服务端基础设施 | **大面积缺失**：HA/多副本、mmap 刷盘、文件生命周期、Raft 日志持久化；**POP/计时器状态持久化已于 §7.1 补齐** |
| broker remoting 请求码 | 约 30 个 vs Java `AdminBrokerProcessor` 约 70 个 case |
| namesrv 请求码 | 13 个 vs Java `DefaultRequestProcessor` 数十个 |

> 客户端测试全绿只证明「单进程、不重启、无主从」这条路径对齐；一旦涉及崩溃恢复、主从切换、运维诊断、4.x 客户端，差距立即暴露。
>
> **基线校准**：上一轮 `JAVA_CLIENT_PARITY_REPORT.md` 列出的 3 项遗留差距，已由后续提交 `c8f473e` 全部补齐（长轮询 5s/20s 边界 + invisible `[0,12h]`、事务回查 `check_times` 上限、per-channel receipt-handle 缓存与空闲客户端回收），本报告不再重复计入，且差距清单以当前 HEAD 为准。

---

## 1. P0 — 数据可靠性与持久化（生产级硬缺口）

| # | 功能 | Java 证据 | Rust 现状（复核） | 后果 |
|---|---|---|---|---|
| 1 | **HA 主从复制 / 多副本切换** | `DefaultHAService`、`DefaultHAConnection`、`GroupTransferService`、`SlaveSynchronize.syncAll`、`ReplicasManager.changeBrokerRole`、`DLedgerRoleChangeHandler`、`EscapeBridge`、`AutoSwitchHAService`+`EpochFileCache` | `broker.rs:3188` 写死 `brokerId=0 / brokerRole=ASYNC_MASTER`；`broker.rs:772` 心跳 `haServerAddr=""`；`container.rs` 头注释自认 "does not perform HA log shipping"；`main.rs` 各 broker 独立监听 | **无副本、无 failover**，单点是唯一部署形态 |
| 2 | **mmap + 刷盘策略 + 组提交** | `DefaultMappedFile`(FileChannel.map)、`MappedFileQueue`、`AllocateMappedFileService`、`LibC.mlock`、`TransientStorePool`、`GroupCommitService`、`FlushManager`、`FlushDiskWatcher` | `store.rs:82` `log: std::fs::File` 单文件 + `read_exact`；`store.rs:725` 仅手动 `flush`；`sync_log` 只调 `sync_data()`；无 SYNC/ASYNC_FLUSH、无组提交、无文件滚动/预分配/预热 | 无顺序写吞吐优化；配置虽报 `SYNC_FLUSH` 但语义不符 |
| 3 | **文件生命周期与崩溃恢复** | `CleanCommitLogService.deleteExpiredFiles`、`isOSPageCacheBusy`、`abort` 文件 + `load(lastExitOK)` / `recoverAbnormally`、`StoreCheckpoint` 三时间戳 | 仅 `.store.lock` + `validate_partial_prefix` 截坏尾；状态全靠 `*.json`（offsets/delayOffsets/timercheck）；无过期删除、无磁盘水位、无 abort 标记 | **磁盘会无限增长**；异常退出无专门恢复分支 |
| 4 | **POP 状态持久化（revive）** | `PopReviveService` 把 `PopCheckPoint`/`AckMsg` 写 revive topic、`PopBufferMergeService.addCk/addAk`、重启复活 | **本轮实测确认**：`broker.rs:524` `pop_invisible: Mutex<BTreeMap<...>>` 纯内存，`pop_ck` 仅逗号串；`tests/process.rs` 的重启测试只校验 topic config 与 offset，不覆盖 POP 状态 | **进程重启丢失 invisible/ack → 消息重复投递**（比客户端测试能暴露的更严重） |
| 5 | **精确延迟消息落盘** | 磁盘 `TimerWheel`(putSlot/flush/backup)、`TimerLog`、`TimerMessageStore.recover/recoverAndRevise`、escape/roll | `broker.rs:420` `Wheel { slots: BTreeMap }` 内存轮；`broker.rs:415` 自认 "the `rmq_sys_wheel_timer` queue **is** the log"；靠 commitlog 队列重放 | 重启后定时状态依赖重放，无 slot 快照/容错 |
| 6 | **Raft / controller 持久化** | `DLedgerController.appendToDLedgerAndWait`、`JRaftController.setLogUri`、`onSnapshotSave/onSnapshotLoad`、`EventSerializer` | `raft.rs:8` 自认 "**In-memory state only**: term/vote/log are not persisted"；无快照、无日志压缩、leader 侧校验后其余节点盲 apply、偶数集群 quorum 放宽 | controller 重启丢共识状态 |
| 7 | **压缩 / 批量消息解码** | `MessageExtEncoder`、`MessageDecoder.decode(..., deCompressBody)`、`decodesBatch` | `broker.rs:854` 明确拒绝 "compressed batch envelope is not supported"；`common.rs:197` 只保留压缩字节不解开 | 4.x 客户端的压缩/批量消息不可用 |
| 8 | **顺序消费队列锁** | `RebalanceLockManager.tryLock/unlockBatch`、`LOCK_BATCH_MQ`、`ConsumerOrderInfoManager`/`QueueLevelConsumerManager`(POP orderly)、`QueryAssignmentProcessor`(broker 侧) | `broker.rs:1118` 自认 "arrives with the ordered-message lock manager"（未实现）；broker 分发无 `QUERY_ASSIGNMENT`；仅 `schedule.rs:683` 有配置位 | 顺序消费/FIFO 只有队列选择层，无锁保障 |

> 另有：`IndexFile` 哈希索引文件与 RocksDB 索引后端（`store.rs` 用内存 `Keys` BTreeMap 代替，按 key 查询能用但无索引文件/滚动/过期删除）、`BatchConsumeQueue`/CQ 纠错/ConsumeQueueExt、`PutMessageSpinLock`（全局 `Mutex<Inner>` 串行）。

---

## 2. P1 — 协议与运维能力（方法在、语义空）

| # | 功能 | Java 证据 | Rust 现状 |
|---|---|---|---|
| 9 | **4.x remoting 协议完整接入** | `ClientManagerActivity`、`PopMessageActivity`、`PullMessageActivity`、`SendMessageActivity`、`AckMessageActivity`、`ConsumerManagerActivity`、`TransactionActivity` | `proxy.rs:5337 serve_remoting_front` 仅按 `NAMESRV_FORWARD_CODES`/`BROKER_FORWARD_CODES` 白名单转发，其余 `REQUEST_CODE_NOT_SUPPORTED`；无同端口多协议协商、无 HTTP/2 代理、无 HAProxy PROXY protocol |
| 10 | **RPCHook / zone 就近路由** | `RPCHook.doBeforeRequest`、`ZoneRouteRPCHook.filterByZoneName`、`registerRPCHook` | `remoting.rs` 无 hook 机制；`namesrv.rs` `route()` 原样返回全量路由，无 `zoneName` 过滤 |
| 11 | **请求 future 表 / 异步回调 / 连接治理** | `responseTable`、`scanResponseTable`、`ResponseFuture`、`InvokeCallback`、`IdleStateHandler`、`WRITE_BUFFER_WATER_MARK`、`ChannelEventListener`、`FileRegionEncoder` 零拷贝 | 单次 `tokio::time::timeout`；仅 `MAX_IN_FLIGHT` 信号量近似；无空闲检测/水位/背压事件/零拷贝 |
| 12 | **AdminService 空壳语义** | `ProxyAdminGrpcService.changeLogLevel`、`printThreadStackTrace`、`verifyMessage`、`getConsumerRunningInfo`、`queryMessage(scroll_id)`、`queryAllBrokers`、`ProxyAdminForwarder.forwardIfRemote` | 16 个方法都在，但 `change_log_level` 只回 "log level is fixed at startup"；线程栈/消费验证无在线客户端即 typed failed；`get_consumer_running_info` 的 process-queue / consume-status 两表为空；`scroll_id` 恒空；`get_proxy_runtime_stats` 只有 name+version；无跨 proxy 转发 |
| 13 | **broker Admin 命令集** | `AdminBrokerProcessor` ~70 个 case：消费诊断/重置 offset/克隆 offset/直接消费/连接列表/broker 统计/HA 状态/epoch/ACL CRUD/配置动态更新/RocksDB 进度 | `broker.rs` 分发约 30 个 code，`_ => "request code {} is not implemented"`；`UPDATE_BROKER_CONFIG(25)` 常量存在但无处理；`controller.rs:1737` `GET/UPDATE_CONTROLLER_CONFIG` 返回**空 SUCCESS 桩** |
| 14 | **namesrv 请求码** | `GET_TOPICS_BY_CLUSTER`、`GET_UNIT_TOPIC_LIST`、`GET_SYSTEM_TOPIC_LIST_FROM_NS`、`REGISTER_TOPIC_IN_NAMESRV`、`QUERY_DATA_VERSION`、`UPDATE_NAMESRV_CONFIG`、`notifyMinBrokerIdChanged`、`BatchUnregistrationService` | `namesrv.rs:304-328` 仅 13 个 code；319 回显硬编码字符串；318 明确禁用；无路由变更主动推送、无批量注销、无 zone |
| 15 | **receipt handle 生命周期** | `DefaultReceiptHandleManager.scheduleRenewTask/renewMessage`、`ReceiptHandleGroup`、`DataScanner`、`clearGroup/returnHandleGroup`、通道关闭回收、`scanExpireResultFuture` | 已有 per-channel 缓存(12h TTL) + 5s 定时空闲回收（`c8f473e`）；仍缺**自动续期 renew、分组映射/锁、通道关闭事件驱动的即时回收、relay future 过期扫描** |
| 16 | **事务状态机（proxy 侧）** | `AbstractTransactionService`、`TransactionDataManager`、`ClusterTransactionService.scanProducerHeartBeat`、`endTransactionOneway` | `end_transaction` 直接转发；事务回查为 best-effort telemetry 推送（gRPC 链路已 PASS，见 §4）；无事务数据管理/心跳/跨集群订阅 |
| 17 | **批量 / 进阶收发** | `batchAckMessage`(enableBatchAck)、`lockBatchMQ`、`sendMessageBack`、`updateConsumerOffsetAsync`、`SendMessageActivity` 完整校验链（body size / key / message group / delay / transaction recovery / control chars / user properties）、`ReplyMessageProcessor`(323/324)、`NotificationProcessor`/`PeekMessageProcessor`、消息轨迹 `traceOn` | `ack_message` 逐 entry 循环；`send_message` 仅 topic/body 非空 + 类型校验；`trace_context` 恒 `None`；分发无 323/324、NOTIFICATION、PEEK_MESSAGE、BATCH_ACK |
| 18 | **4.x 事务主动回查** | `Broker2Client.checkProducerTransactionState` → `CHECK_TRANSACTION_STATE` | `broker.rs:2228` 自认 "no push channel, the sweep only reports"；**5.x gRPC 已由 proxy telemetry 覆盖（t14 PASS）**，故只缺 remoting 通道 |
| 19 | **负载均衡/路由进阶** | `TopicQueueMappingManager`(静态 topic)、`BroadcastOffsetManager`、`MQFaultStrategy`/`updateFaultItem`/`penalizer`、`MessageRequestModeManager`、路由惩罚与优先级 | 仅 `java_string_hash` 选写队列；无静态 topic 映射、广播 offset、故障惩罚、队列优先级；`namesrv.rs:55` 仅注册映射表 |

---

## 3. P2 — 可观测性、增强与生态（不影响单进程功能）

| 域 | Java 有 / Rust 缺 |
|---|---|
| **指标导出** | `ProxyMetricsManager`(Prometheus/OTLP-gRPC/LOG 三导出)、`BrokerMetricsManager`/`PopMetricsManager`/`DefaultStoreMetricsManager`(OTel)、`StoreStatsService`/`BrokerStatsManager`、`ControllerMetricsManager` → Rust 仅 `record_op` 原子计数 + `runtime_stats` + tracing 日志 |
| **消息轨迹** | `SendMessageHook`/`ConsumeMessageHook`/`SendMessageContext`/`HookUtils` → 无；`broker.rs:2963` 只写空 `TRACE_CONTEXT` 属性 |
| **限流/熔断** | `BrokerFastFailure.cleanExpiredRequestQueue`、`protectBroker()`(MomentStatsItem)、`ColdDataCgCtrService` 冷数据流控、`RemotingProtocolServer.cleanExpireRequest` → 全无 |
| **tieredstore** | S3/OSS `FileSegmentProvider`、分段 multipart 上传流、FlatCommitLog/CQ 分离、独立 `IndexStoreService` 冷读索引、rocksdb 元数据、过期删除、读缓存/预读 → `tieredstore.rs:12-21` 自认 "POSIX backend only / in-memory index / no executor pools / no read-ahead cache / backpressure rejects instead of suspending" |
| **filter** | `BloomFilter`/`BitsArray`、`FilterFactory` 编译缓存 + SqlFilter SPI、`ConsumerFilterManager`、JavaCC 完整 SQL92（NOW/数值分支） → `filter.rs` 手写解析器每次重解析、`:7` 自认数值分支缺失、`:718` 尾部 token 忽略 |
| **配置热加载** | `ConfigurationManager.initEnv/loadJsonConfig`(RMQ_PROXY_HOME)、ACL/TLS 证书 `FileWatchService` + `TlsContextReloadListener`、`AuthConfig` 独立 → 启动加载一次；`srvutil.rs` 有 FileWatchService 但未接入 auth |
| **生命周期/插件** | `BrokerController.shutdownBasicService()` 按序停 ~30 个服务、`BrokerPreOnlineService`、`BrokerAttachedPlugin`/`PullMessageResultHandler` → 仅 2 个后台任务 + 单回调 shutdown hook |
| **container** | `ADD_BROKER`/`REMOVE_BROKER` 运行时动态增删、共享 remoting/executor、`BrokerBootHook` → 启动期静态装载，`container.rs:526` "placeholder" |
| **openmessaging** | OMS `LocalTransactionExecutor` 事务发送、`ProducerInterceptor`/`ConsumerInterceptor` → 无（异步发送 `sendAsync` 已实现） |
| **认证进阶** | `AuthenticationFactory`/`AuthorizationFactory` 多策略反射装配、三级 pipeline（ContextInit/Authentication/Authorization）、动态用户/ACL CRUD、Admin 独立认证拦截器 → `AclGuard.check_access` 内联签名校验；`proxy.rs:35` 自认 ACL 签名为空字节串 |

---

## 4. 纠偏 — 代理报告中不成立 / 已过时的判断

| 说法 | 核验结论 |
|---|---|
| 「事务回查缺失，half 消息永不决议」(broker 域报为高缺陷) | **部分过时**：`broker.rs:2228` 的注释确实说无推送通道，但 5.x gRPC 链路已由 `proxy.rs run_transaction_recovery` 经 telemetry 实现，t14 两轮 PASS。仅 4.x remoting 的 `CHECK_TRANSACTION_STATE` 缺失（已降为 P1 #18） |
| 「AdminService RPC 未实现」 | **不成立**：16/16 方法齐备（`proxy.rs:4175-5116`），问题是方法体空壳，已改列为 P1 #12 |
| 「TLS / authentication 未实现」 | **不成立**：`remoting.rs:718 serve_tls`(rustls + mTLS `WebPkiClientVerifier`)、`proxy.rs:5205 ServerTlsConfig`、`main.rs --tls-* / --acl-*` 均已实现（2026-10-30 起） |
| `main.rs` CLI `SECURITY_NOTICE` 仍写着上述错误文案 | **属实且已在本轮修正**：改为说明 TLS/ACL 可用但默认关闭、部署前须启用 |
| audit log / 有序 topic / 压缩注册体 / 超级管理员 / 内部凭证 | 均已实现，不计入缺口 |

---

## 5. 建议补齐顺序（按投入产出）

1. ✅ **POP + Timer 状态落盘**（P0 #4/#5）：已完成，见 §7.1。
2. **文件生命周期**（P0 #3）：过期删除 + 磁盘水位 + checkpoint/abort，避免磁盘无限增长。
3. **broker Admin 命令集 + controller config 空桩**（P1 #13）：运维可诊断性，属纯增量。
4. **4.x remoting 协议 + RPCHook**（P1 #9/#10）：决定能否接入存量 4.x 客户端与 zone 路由。
5. **HA / 多副本与 mmap 刷盘**（P0 #1/#2）：架构级改造，需独立规划，不宜与前面几项混做。

---

## 6. 本轮代码改动与验证

```text
src/main.rs  SECURITY_NOTICE 文案修正（TLS/ACL 已实现但默认关闭，不再宣称"未实现"）
cargo build  -> 0 error / 0 warning
cargo test   -> 225 passed / 0 failed
```

> 本报告为只读审计结论（未改动各模块功能逻辑）；除上述一行文案外无代码变更。

---

## 7. 补齐进度（按 §5 顺序）

### 7.1 ✅ 第 1 项：POP + Timer 状态落盘（P0 #4/#5）

**目标**：消除进程重启后丢失 POP 的 ack / invisible 状态导致的重复投递，以及定时轮槽位依赖全量重放的问题。

| 改动 | 文件 | 说明 |
|---|---|---|
| typed runtime snapshot API | `src/store.rs` | 新增 `Store::put_runtime_state<T: Serialize>` / `Store::runtime_state<T: DeserializeOwned>`；`validate_state_name` 阻止路径穿越；`replace_json(..., durable: bool)` 让热路径保留原子 rename 但跳过 `sync_all()` / `sync_directory()`（进程级崩溃恢复不受影响，机器掉电可能丢最后一次快照，与 Java 异步刷盘同级） |
| POP 快照 | `src/broker.rs` | `pop_state.json` 保存 `(group,topic,queue,offset,invisible_until)` 与 `(group,topic,queue,next-scan-offset)`；`Broker::new` 启动时恢复 `pop_invisible` / `pop_offsets` |
| Timer 快照 | `src/broker.rs` | `timer_wheel.json` 保存 `checkpoint.scan_offset` 与全部待投递 slot；启动时一起恢复，避免「游标已前进但消息还停在轮里」的空洞 |
| 写入时机 | `src/broker.rs` | 每次 POP 请求（锁外、一次请求只写一次）、ack 命中、change-invisible 命中、scan_wheel 一批处理完、deliver_due 投递完；空长轮询与空 due 轮不写盘；`state_write: Mutex<()>` 串行化快照，防止旧快照覆盖新快照 |
| 顺带修正 | `src/broker.rs` | broker 三处 `invisibleTime` 从硬截断 1h 统一到 Java `GrpcValidator` 的 12h，避免 gRPC 层放行 2h 却被 remoting 层静默改成 1h |

**验证**

```text
cargo build                        -> 0 error / 0 warning
cargo test                         -> 233 passed / 0 failed（基线 230 + 2 单元 + 1 进程级）
cargo clippy --all-targets         -> 76 warnings（与基线持平，无新增）
新增 broker::tests::pop_buffer_state_survives_restart
新增 broker::tests::timer_wheel_slots_survive_restart
新增 tests/process.rs::pop_ack_and_invisible_state_survive_sigkill
     （真实进程 SIGKILL(9) → 重启 → 断言已 ack 与仍 invisible 的消息不再投递，仅未投递消息返回）
```

**并发 flake 修复**：初版在 `pop_queue` 内每队列写盘且持有队列锁，使 `invisibleTime=100ms` 的既有测试在并发负载下窗口过期、`second_batch.len()` 得到 2 而非 1。已把快照写入移到 `pop_queues` 末尾（锁外、每请求一次），并把该测试改为用 `change_invisible(invisibleTime=0)` 显式触发过期，不再依赖真实时钟的 100ms 窗口；`cargo test --lib` 连续 3 轮并发复跑均 188/188 通过。

**仍存的限制**（不属本项范围）：快照为全量 JSON、每次状态变更同步重写，未做 debounce/合并写；无 revive topic，也没有 check point 的过期清理，长期不 ack 的 invisible 条目会累积；`popTimeout>0` 的长轮询在循环内复用请求开始时的 `now`，等待期间到期的 invisible 条目要等下一次请求才会复活。这些留待文件生命周期（第 2 项）与 HA/mmap 架构改造（第 5 项）一并处理。

### 7.2 待办

| 顺序 | 项 | 状态 |
|---|---|---|
| 2 | 文件生命周期（过期删除 / 磁盘水位 / checkpoint / abort 异常恢复分支） | 未开始 |
| 3 | broker Admin 命令集 + controller config 空桩 | 未开始 |
| 4 | 4.x remoting 协议 + RPCHook / zone | 未开始 |
| 5 | HA / 多副本 / mmap 刷盘（架构级） | 未开始 |
