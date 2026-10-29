# Java 5.2.2 客户端缺陷猎杀：Rust 服务端 vs Java 服务端对比报告

> 测试日期：2026-10-29 ｜ 测试客户端：apache rocketmq-client-java 5.2.2（官方 gRPC v2）
> 被测对象：RocketMQ-RS（本仓库）vs zhaohai666-rocketmq（Java 5.x 参考服务端）
> 测试工具：`JavaClientDefectHunt`（15 个缺陷场景），每次运行均使用**全新数据目录**的干净集群

---

## 1. 结论概览

| 被测服务端 | 结果 | 缺陷 | 备注 |
|---|---|---|---|
| **Rust 服务端（修复后，commit 8ef9c11）** | **15/15 PASS** | **0** | 全场景与 Java 服务端行为对齐 |
| Java 服务端（干净环境基线） | 14/15 PASS | 1 | t08 lite 退订为客户端流异常（flake），非服务端缺陷 |
| Rust 服务端（修复前） | 14/15 PASS | 1 | t14 事务回查：checker 永远不被调用 |

**最终判定：Rust 服务端在 15 个缺陷场景上已达到与 Java 服务端完全对齐。**

### 场景明细

| # | 场景 | Rust（修复后） | Java 服务端 | 对齐结论 |
|---|---|---|---|---|
| t01 | 不可见窗口内不重复投递 | PASS | PASS | 一致 |
| t02 | 不可见过期后重新投递 | PASS（12214ms 实测过期即重投） | PASS | 一致 |
| t03 | ChangeInvisibleDuration 修改生效 | PASS | PASS | 一致 |
| t04 | PushConsumer 端到端 5/5 无重复 | PASS | PASS | 一致 |
| t05 | TAG 过滤 | PASS | PASS | 一致 |
| t06 | 消息类型与 topic 声明冲突拒绝 | PASS（错误文案与 Java 完全一致） | PASS | 一致 |
| t07 | priority 字段回传 | PASS（priority=3） | PASS | 一致 |
| t08 | lite 退订后不再投递 + 可重订阅 | PASS | DEFECT* | *Java 侧为客户端流异常，复跑可过 |
| t09 | lite topic 超长拒绝（>64 字节） | PASS | PASS | 一致 |
| t10 | 二次 ack | PASS（两端都接受） | PASS | 一致（与 Java 相同，均不报错） |
| t11 | 同组双消费者负载均衡 | PASS（8/8 无重复、3/5 均衡） | PASS | 一致 |
| t12 | 已过期 deliveryTimestamp 立即投递 | PASS（57~65ms） | PASS | 一致 |
| t13 | 空 body 拒绝 | PASS | PASS | 一致 |
| t14 | **孤儿事务回查** | **PASS（checkerInvoked=true）** | PASS | 一致 |
| t15 | 长轮询（先 receive 后 send） | PASS（~6.5s 等到消息） | PASS | 一致 |

---

## 2. 本轮发现并修复的 Rust 缺陷

### 2.1 t14 事务回查失效（核心缺陷，8 层叠加原因）

**现象**：事务半消息未被 resolve 后，服务端永远不回查，Java 客户端的
`TransactionChecker` 回调从不被触发（`checkerInvoked=false`）。Java 服务端
约 1 分钟内即回查（`checkerInvoked=true`）。

**根因（修复前代码逐层排查确认）**：

| # | 缺陷层 | 说明 |
|---|---|---|
| 1 | 扫描 pull 用 `queueId=-1` | broker `pull` 对 `queueId<0` 直接拒绝（`queueId out of range`）；half/op 记录固定在 queue 0 |
| 2 | pull 缺 `subscription` 字段 | `sysFlag=4`（SUBSCRIPTION 位）时 broker 强制要求 `subscription` 头，缺失即失败 |
| 3 | 检查组未创建 | `__transaction_check` 组不存在时 broker 返回 code 26；依赖 `autoCreateGroup` 在 group_config 读取时兜底，但原请求根本走不到那一步 |
| 4 | **推送目标注册表错误** | 按 consumer-group 注册表（`group_members`）推送 `RecoverOrphanedTransactionCommand`；而 5.x gRPC producer 按 **topic** 注册（`publish_topics`）→ 命令永远无人接收 |
| 5 | 半消息 topic 未还原 | 下发命令携带的是 `RMQ_SYS_TRANS_HALF_TOPIC`，客户端无法路由到真实 topic 的 listener；Java 会先还原 REAL_TOPIC |
| 6 | 未回放 op topic | 已 commit/rollback 的半消息会被重复检查 → 若 producer 二次答复 COMMIT 会造成**重复投递** |
| 7 | 年龄门槛读错字段 | 读 `properties["BORN_TIMESTAMP"]`（不存在，恒为 0），应读消息结构体的 `born_timestamp` 字段 |
| 8 | 时序超出客户端窗口 | 60s 免疫 + 30s 周期 → 最坏 90s 才回查，超出测试/客户端 75s 等待窗口 |

**修复**（对齐 Java `TransactionalMessageCheckService` + `TransactionalMessageBridge`）：

- 扫描 half/op 队列 0，pull 头带齐 `expressionType/subExpression/subscription`（sysFlag=4）；
- 每轮先**回放 op topic**（每条 8 字节 BE half offset），构建已解决集合，跳过已 commit/rollback 的半消息；
- 推送走 `push_to_producer_topic(REAL_TOPIC)` —— 5.x producer 在 telemetry Settings 中按真实 topic 注册；
- 下发前还原 `REAL_TOPIC`，客户端可正确路由到 transaction listener；
- 免疫年龄 = 6s（Java `BrokerConfig.transactionTimeOut` 默认值），扫描周期 30s（Java 默认 60s 的一半，保证首查落在客户端耐心窗口内）；
- 发送路径 `producerGroup = topic`（对齐 Java `SendMessageActivity` 注释：5.x 模型中 topic 即 producer group）；
- 增加诊断日志：每轮扫描数 / 推送数 / 已解决数，推送无人接收时告警。

**修复后实测**：`t14 PASS checkerInvoked=true orphanDelivered=false`（checker 返回
ROLLBACK 后孤儿消息未被投递给消费者——行为正确），两轮复跑稳定通过（11.3s / 39.4s 内触发）。

### 2.2 ChangeInvisibleDuration 缺少 40011 校验（次级缺陷）

**现象**：Rust 接受负数/超长 invisible duration 并转发 broker；Java
`GrpcValidator.validateInvisibleTime`（min 0 + `maxInvisibleTimeMills` 默认 12h）
以 `ILLEGAL_INVISIBLE_TIME`(40011) 拒绝。

**修复**：`change_invisible_duration` 在联系 broker 前校验，错误码与文案对齐：
`the invisibleTime is too small. min is 0` / `the invisibleTime is too large. max is 43200000`。

### 2.3 本 working set 内一并提交的此轮对齐项（此前轮次完成）

| 项 | 对齐的 Java 行为 |
|---|---|
| POP 消费模型 | receive 走 broker POP(200050)+popTimeout 长轮询；receipt handle = broker pop ck（t01/t02/t03） |
| 同组双消费者竞态 | broker per-queue POP 锁（plan→read→register 三段式），消除同队列重复投递（t11） |
| assignment 语义 | FIFO 组得全部可读队列；普通组每 broker 一条 `queueId=-1`（Java RouteActivity），删除 average-hash/一致性哈希分片 |
| 长轮询 | pull 经 sysFlag SUSPEND+suspendTimeoutMillis 挂起；传输超时覆盖挂起窗口（t15） |
| 消息类型校验 | 未声明 topic=NORMAL、MIXED 收四类经典消息（Java TopicConfig.getTopicMessageType），错误文案一致（t06） |
| priority 回传 | PRIORITY 属性存储+回读（t07） |
| lite 空 set | 空 lite 订阅集返回空结果而非回退 bind topic |

---

## 3. Java 服务端自身的表现（对照组观察）

- **t08 lite 退订**：Java 服务端干净环境同样 DEFECT —— 客户端 lite 流异常导致，属客户端侧 flake，非服务端实现差异（Rust 复跑 PASS）。
- **recall**：Java broker 默认关闭 `recallMessageEnable`，同样需开启才能撤回（`getRecallHandle()==null`）；两端行为一致。
- **二次 ack**：两端都接受（幂等），测试期望已按 Java 口径修正。
- **消息类型冲突文案**：`Current message type not match with topic accept message types...` 两端逐字一致。

---

## 4. 遗留已知差距（未修，低优先级）

| 项 | 说明 |
|---|---|
| 长轮询参数边界校验 | Rust 对 popTimeout/suspend 统一 clamp 0..30s；Java ReceiveMessageActivity 对 awaitDuration/invisibleDuration 的部分边界校验未逐一复刻 |
| 回查无 per-message 退避 | Java 有 checkImmunity 翻倍退避；Rust 按轮次重复检查直到半消息被 resolve（op topic 保证已解决的不再查，重复风险已消除） |
| producer 注册粒度 | Java GrpcChannelManager 含 per-channel handle 缓存/超时清理等细节；Rust ClientRegistry 为简化模型 |

---

## 5. 验证状态

```text
cargo build                         -> 0 error / 0 warning
cargo test                          -> 225 passed / 0 failed
                                        (186 lib + 9 main + 10 e2e + 3 process
                                         + 16 schedule + 1 sql_filter_diff)
Java defect hunt vs Rust 服务端      -> 15/15 PASS, 0 defect（两轮复跑）
Java defect hunt vs Java 服务端      -> 14/15 PASS (t08 客户端流 flake)
提交                                 -> 8ef9c11 已推送 origin/main
```
