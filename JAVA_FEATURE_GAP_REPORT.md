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
| 服务端基础设施 | **大面积缺失**：HA/多副本、mmap 刷盘、commitlog 分段与过期回收、Raft 日志持久化；**POP/计时器状态持久化（§7.1）、abort/checkpoint/磁盘水位/孤儿文件清理（§7.2）、Admin 诊断命令与 controller 配置（§7.3）已补齐** |
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
2. ◐ **文件生命周期**（P0 #3）：abort/checkpoint/水位/孤儿文件清理已完成（§7.2）；物理分段与过期 commitlog 回收并入第 5 项。
3. ◐ **broker Admin 命令集 + controller config 空桩**（P1 #13）：已完成高频只读诊断命令与 config 空桩（§7.3）；写操作类 Admin 仍缺。
4. ◐ **4.x remoting 协议 + RPCHook**（P1 #9/#10）：转发分类/白名单、无 topic 命令路由、顺序消费锁 41/42 已落地（§7.4）；RPCHook、zone 就近路由、同端口多协议协商、多 broker fan-out 仍缺。
5. **HA / 多副本与 mmap 刷盘**（P0 #1/#2）：架构级改造，需独立规划，不宜与前面几项混做。

---

## 6. 审计当时的改动与验证

```text
src/main.rs  SECURITY_NOTICE 文案修正（TLS/ACL 已实现但默认关闭，不再宣称"未实现"）
cargo build  -> 0 error / 0 warning
cargo test   -> 225 passed / 0 failed
```

> 审计阶段本身是只读的（除上述一行文案外无代码变更）。**后续「按顺序补齐能力」的代码改动与验证见 §7 各小节**；当前基线为 `cargo test -> 245 passed / 0 failed`、`cargo clippy --all-targets -> 75 warnings`。

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

### 7.2 ◐ 第 2 项：文件生命周期管理（P0 #3，物理分段部分除外）

| 能力 | 落地位置 | 说明 |
|---|---|---|
| `abort` 标记 / 异常恢复分支 | `src/store.rs` `Store::open` / `Store::shutdown` / `impl Drop` | 启动时创建 `abort`；`main.rs` 的 `supervise` 正常关闭时显式 `flush()+shutdown()` 删除它，`Drop` 兜底；SIGKILL 不会执行两者，下次 open 检测残留 → `recoverAbnormally` 分支：记 `warn`、累加 `abnormalRecoveries`、强制校验 commitlog 尾部并 sync 目录 |
| `StoreCheckpoint` | `storecheckpoint.json` | 三时间戳（physic/logics/index）+ `commitlogLen` + `cleanExit` + `abnormalRecoveries` + `lastCleanTimestamp`；`Store::flush_checkpoint()` 由 `maintain_expiry` 每 10s 落盘 |
| 磁盘水位 | `StoreLimits::max_store_bytes`（默认 32 GiB）、`Store::open_with_limits`、`Store::disk_usage` | `append_records` 写盘前用增量维护的目录字节数做硬限制，超限返回 `store disk watermark exceeded` 且**不 fence**（可恢复，对齐 Java `SERVICE_NOT_AVAILABLE`）；状态文件覆盖写时按单文件 size delta 修正计数 |
| 过期文件清理 | `Store::clean_expired_files() -> CleanReport` | 删除崩溃残留的 `.<name>.<pid>.<seq>.tmp`；单写者文件锁保证无并发写者，`MIN_ORPHAN_AGE_MS = 1h` 保护正在写的临时文件；由 `maintain_expiry` 每 10s 调用，结果记入 checkpoint |

```text
cargo build                   -> 0 error / 0 warning
cargo test                    -> 238 passed / 0 failed（233 + 4 store 单测 + 1 进程级）
cargo clippy --all-targets    -> 76 warnings（基线持平）
新增 tests/process.rs::abnormal_shutdown_marker_survives_sigkill
     （真实 SIGKILL(9)：运行期 abort 存在 → kill 后仍存在 → 重启 abnormalRecoveries==1、消息完好）
```

**明确未做、归入第 5 项（#30）**：commitlog 的**物理分段滚动与过期段删除**。当前 `LOG_FILE = "commitlog"` 是单文件，`Location.physical` 是文件内绝对字节偏移，读路径 6 处直接 `seek(physical)`。要做 Java `MappedFileQueue` 式的滚动/删除，必须同时改 recover、append、全部读路径与索引语义，属于存储物理布局重构；与 #30 的 mmap/刷盘/文件预分配共用同一套设计更合理，单独塞进本项会破坏已验证的崩溃恢复语义。在此之前，磁盘增长由 `max_store_bytes` 硬上限兜住，但**旧空间不可回收**，这是与 Java 仍存在的实际差距。

### 7.3 ◐ 第 3 项：broker Admin 命令集 + controller config 空桩（P1 #12/#13）

**controller `GET/UPDATE_CONTROLLER_CONFIG`（原为返回空 SUCCESS 的桩）**

| 改动 | 说明 |
|---|---|
| 配置所有权 | `Controller.config` 改为 `Arc<RwLock<ControllerConfig>>`，`ControllerManager` 不再持有会与运行态脱节的启动副本；`metadata()` 读实时值 |
| GET (317) | 返回真实 `ControllerConfig` JSON，不再是空 SUCCESS |
| UPDATE (318) | 解析 Java `Properties` 文本 / JSON 对象，先在候选副本上校验全部键再提交（拒绝时无部分更新）；黑名单包含 `configBlackList` 用户配置 + `enableRaft`/`nodeId`/`peers` 三个需重建 Raft 组的键；非法值返回 `CONTROLLER_INVALID_REQUEST(20014)` 并带 remark |
| 动态生效 | 更新同步到 `ReplicasInfoManager.config`（选主逻辑读 `enableElectUncleanMaster`）与 `DefaultBrokerHeartbeatManager`（`timeout_ms` 改为 `AtomicU64`，心跳存活窗口立即变化）；`ControllerManager::config()` 返回实时快照 |

**broker `AdminBrokerProcessor` 高频只读命令**

| Code | 命令 | 落地 |
|---|---|---|
| 28 | GET_BROKER_RUNTIME_INFO | 原 6 项扩展为 12 项，新增 `storeCleanExit` / `storeAbnormalRecoveries` / `storeDiskUsedBytes` / `storeDiskLimitBytes` / `popInvisibleEntries` / `timerPendingSlots`，把 §7.1/§7.2 的状态暴露给运维 |
| 60 | GET_TIMER_CHECK_POINT | 返回真实 `wheel.checkpoint.scanOffset` |
| 202 | GET_TOPIC_STATS_INFO | `TopicStatsTable`：每队列 min/max offset + 最后一条的 store timestamp |
| 203 | GET_CONSUMER_CONNECTION_LIST | `ConsumerConnection`：真实 client id / 订阅表（来自 heartbeat 注册表） |
| 204 | GET_PRODUCER_CONNECTION_LIST | `ProducerConnection`：**新增 `Broker.producers` 注册表**（heartbeat 的 `producerDataSet` 注册、UNREGISTER_CLIENT(35) 清理），此前 broker 只记录 consumer 组 |
| 208 | GET_CONSUME_STATS | `ConsumeStats`：brokerOffset / consumerOffset / pullOffset / lastTimestamp（单 topic 形式） |
| 23 / 305 | GET_TOPIC_NAME_LIST / GET_SYSTEM_TOPIC_LIST_FROM_BROKER | 原来两者合并返回**全部 topic**，与 Java 不符；已拆开：23 返回全部，305 只返回系统 topic 集合 |
| 43 | GET_ALL_CONSUMER_OFFSET | 复核发现**已由 `Store::all_offsets()` 实现**，不再重复添加 |

响应体形状对齐 Java：`offsetTable` 使用 `MessageQueue.toString()` 形式的键 `MessageQueue [topic=…, brokerName=…, queueId=…]`，`topic@group -> queueId -> offset` 直接来自持久化 offset 表。

```text
cargo build                   -> 0 error / 0 warning
cargo test                    -> 240 passed / 0 failed（238 + 1 controller + 1 broker）
cargo clippy --all-targets    -> 75 warnings（比 76 基线少 1，顺手修掉老代码 let_and_return）
新增 controller::tests::controller_config_get_and_update_are_live
新增 broker::tests::admin_diagnostics_report_real_broker_state
```

**仍缺（写操作与更深的诊断）**：`UPDATE_BROKER_CONFIG(25)`（需把 `BrokerConfig` 从启动值改为共享可变配置，属配置模型重构）、`GET_CONSUMER_RUNNING_INFO(307)` 的 process-queue 明细、`RESET_CONSUMER_OFFSET_IN_BROKER(212)` / `CLONE_OFFSET` / `CONSUME_MESSAGE_DIRECTLY` / ACL CRUD / HA 状态 / epoch / RocksDB 进度，以及 AdminService 里仍为空表的 `get_consumer_running_info`、`scroll_id`、跨 proxy 转发。

### 7.4 ◐ 第 4 项：4.x remoting 协议接入（P1 #9，RPCHook/zone 除外）

**先纠正一处方向性误判**：本项最初按「把白名单扩到后端实现的全部 request code」推进，复核 Java `RemotingProtocolServer.registerRemotingServer` 后证明这是错的，而且有安全问题。

| 复核结论 | 依据 |
|---|---|
| Java proxy 的前端只注册**客户端消息面**的 code | 源码注册了 send(10/310/320)/send-back(36)/end-transaction(37)/recall(370)/heartbeat(34)/unregister(35)/check-client-config(46)/pull(11/361)/pop(200050)/offset(15/14/29/30/31)/ack(200051)/change-invisible(200053)/connection-list(203)/consumer-list(38)/lock-unlock(41/42)，namesrv 侧**只有 105**；Admin 走 gRPC `AdminService` |
| 把 namesrv 内部 code 放进公网前端是集群完整性漏洞 | 103/104/904 可让任意客户端注册/注销 broker、污染路由；100/102 改 KV；205/327 改写权限；216/318 删 topic/改配置 |
| 把 broker Admin code 放进前端是**权限提升** | `ProxyBackend::apply_inner_credentials` 会用 proxy 的内部高权限 AK/SK 重签转发命令，而前端 `AclGuard::check_command` **只验签名、不做授权**（Java 有 `AuthorizationPipeline`）。任何能签名的用户都会变成管理员 |

**落地内容**

| 改动 | 文件 | 说明 |
|---|---|---|
| 三段式转发分类 | `src/proxy.rs` | `ForwardTarget = Namesrv / Broker / Internal / Unsupported` + `forward_target(code)`；`NAMESRV_FORWARD_CODES` 收敛为只读元数据（101/105/106/206/219/224/319/901），新增 `NAMESRV_PRIVATE_CODES` 显式拒绝 100/102/103/104/205/216/318/327/904；`BROKER_FORWARD_CODES` 收敛为 Java 注册的消息面 code + 本 port 的只读查询/LMQ 扩展（12/200070/200071） |
| 无 topic 命令不再被拒 | `src/proxy.rs` | 原逻辑对所有 broker 命令强制 `topic` 字段，heartbeat(34)、unregister(35)、check-client-config(46)、consumer-list(38)、connection-list(203) 等全部被 `missing the topic field` 挡掉。新增 `ProxyBackend::invoke_broker_command_any()`，经 namesrv 106 `GET_BROKER_CLUSTER_INFO` 选一个 master（空 cluster 参数走全集群），由 `route_topic()` 决定走 topic 路由还是集群任选 |
| 41/42 按 body 路由 | `src/proxy.rs` | `LOCK_BATCH_MQ` / `UNLOCK_BATCH_MQ` 的队列在 JSON body 的 `mqSet` 里而不是 `topic` 头；`route_topic()` 从第一条 MessageQueue 取 topic，保证路由到该 topic 的 master |
| 顺序消费队列锁 | `src/broker.rs` | 新增 `RebalanceLockManager` 等价实现：`rebalance_locks: group -> LockQueue -> LockOwner(client_id, Instant)`；41 逐队列判定（空闲/自己持有→授予，他人持有→跳过，Java **不中断整批**），返回 `lockOKMQSet`；42 只有 owner 能释放；空 `mqSet` 回显 body + remark；`REBALANCE_LOCK_MAX_LIVE_TIME = 60s`，惰性过期（Java 定时扫描） |

```text
cargo build                   -> 0 error / 0 warning
cargo test                    -> 245 passed / 0 failed（240 + 3 proxy 单测 + 1 broker 单测 + 1 联调）
cargo clippy --all-targets    -> 75 warnings（基线持平，无新增）
新增 proxy::tests::remoting_front_classifies_4x_client_codes
新增 proxy::tests::remoting_front_routes_lock_batches_by_body_topic
新增 proxy::tests::remoting_front_rejects_cluster_private_and_admin_codes_locally
新增 broker::tests::rebalance_locks_follow_java_ownership_rules
新增 tests/e2e.rs::remoting_front_forwards_4x_commands_to_a_live_cluster
     （真实 namesrv + broker + remoting 前端：105 转发、46/34 无 topic 转发、41/42 按 body 路由，
       以及 103/104/904/100/102/205/216/327/318/17/28/999999 全部在本地以 code 3 拒绝；
       连续复跑 3 轮稳定）
```

**仍未做（P1 #9/#10 剩余）**：`RPCHook.doBeforeRequest` 钩子链、`ZoneRouteRPCHook` 就近路由（需 broker 注册携带 `zoneName`）、`MultiProtocolRemotingServer` 同端口协议协商 / HTTP/2 代理 / HAProxy PROXY protocol、TLS 证书热加载；多 broker 集群下无 topic 命令的 fan-out/merge（Java 的 heartbeat/unregister 由 proxy 自己的 `ClientManagerActivity` 本地处理，本 port 只转给一个 broker，`one_broker_addr` 取到的就是该 broker 的视图）；§7.3 记录的写操作类 Admin 仍缺。

### 7.5 待办

| 顺序 | 项 | 状态 |
|---|---|---|
| 2 | 文件生命周期（abort / checkpoint / 水位 / 孤儿文件清理） | **已落地**（§7.2）；物理分段滚动 + 过期 commitlog 段删除并入第 5 项 |
| 3 | broker Admin 命令集 + controller config 空桩 | **高频只读诊断 + config 已落地**（§7.3）；写操作类 Admin 仍缺 |
| 4 | 4.x remoting 协议 + RPCHook / zone | **转发分类 / 无 topic 路由 / 41/42 已落地**（§7.4）；RPCHook、zone、多协议协商、fan-out 未开始 |
| 5 | HA / 多副本 / mmap 刷盘 + commitlog 分段 | 未开始 |
