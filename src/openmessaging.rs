//! OpenMessaging (OMS) <-> Apache RocketMQ bridge, ported from the Java module at
//! `openmessaging/src/main/java/io/openmessaging/rocketmq/`.
//!
//! This is a client-side bridge library. Message translation, the in-memory consumer cache,
//! the promise/future machinery, and the config/bean mapping are ported one-to-one from the
//! Java source. The producer and consumer halves talk the RocketMQ remoting protocol
//! ([`crate::remoting`], request codes 10/11/14/15) directly against the addresses listed in
//! `ClientConfig::access_points`. Like the Java OMS bridge — whose `DefaultMQProducer` /
//! `DefaultMQPushConsumer` are constructed with statically configured name-server or broker
//! addresses — no name-server route discovery is performed.
//!
//! Source-of-truth classes:
//! domain/{BytesMessageImpl,SendResultImpl,ConsumeRequest,NonStandardKeys,RocketMQConstants}.java,
//! producer/{AbstractOMSProducer,ProducerImpl}.java,
//! consumer/{PushConsumerImpl,PullConsumerImpl,LocalMessageCache}.java,
//! promise/{FutureState,DefaultPromise}.java, config/ClientConfig.java,
//! utils/{BeanUtils,OMSUtil}.java, MessagingAccessPointImpl.java.

use std::{
    collections::{BTreeMap, HashMap, HashSet, VecDeque},
    future::Future,
    pin::Pin,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context, Poll, Waker},
    time::Duration,
};

use crate::common::{
    Message as RocketMqMessage, Result, encode_properties, now_millis,
    Error,
};
use crate::remoting::{self, Command};
use serde::{Deserialize, Serialize};

/// Java: `io.openmessaging.Message.BuiltinKeys` — the standard OMS header keys.
pub mod oms_keys {
    pub const MESSAGE_ID: &str = "messageId";
    pub const DESTINATION: &str = "destination";
    pub const TOPIC: &str = "topic";
    pub const QUEUE: &str = "queue";
    pub const SHARDING_KEY: &str = "shardingKey";
    pub const TIMEOUT: &str = "timeout";
    pub const START_TIME: &str = "startTime";
    pub const PRODUCER_ID: &str = "producerId";
    pub const CONSUMER_ID: &str = "consumerId";
    pub const BORN_HOST: &str = "bornHost";
    pub const BORN_TIMESTAMP: &str = "bornTimestamp";
    pub const STORE_HOST: &str = "storeHost";
    pub const STORE_TIMESTAMP: &str = "storeTimestamp";
    pub const SEARCH_KEYS: &str = "searchKeys";
}

/// Java: `io.openmessaging.rocketmq.domain.RocketMQConstants`.
pub mod rmq_constants {
    /// Key of scheduled message delivery time.
    pub const START_DELIVER_TIME: &str = "__STARTDELIVERTIME";
}

/// RocketMQ `Message` property keys (brief: KEYS, TAGS, DELAY, TRANSIENT, MSG_REGION).
pub mod rmq_msg_keys {
    pub const KEYS: &str = "KEYS";
    pub const TAGS: &str = "TAGS";
    pub const DELAY: &str = "DELAY";
    pub const TRANSIENT: &str = "TRANSIENT";
    pub const MSG_REGION: &str = "MSG_REGION";
}

/// Java: `io.openmessaging.rocketmq.domain.NonStandardKeys` (RocketMQ-specific config keys).
pub mod non_standard_keys {
    pub const CONSUMER_GROUP: &str = "rmq.consumer.group";
    pub const PRODUCER_GROUP: &str = "rmq.producer.group";
    pub const MAX_REDELIVERY_TIMES: &str = "rmq.max.redelivery.times";
    pub const MESSAGE_CONSUME_TIMEOUT: &str = "rmq.message.consume.timeout";
    pub const MAX_CONSUME_THREAD_NUMS: &str = "rmq.max.consume.thread.nums";
    pub const MIN_CONSUME_THREAD_NUMS: &str = "rmq.min.consume.thread.nums";
    pub const MESSAGE_CONSUME_STATUS: &str = "rmq.message.consume.status";
    pub const MESSAGE_DESTINATION: &str = "rmq.message.destination";
    pub const PULL_MESSAGE_BATCH_NUMS: &str = "rmq.pull.message.batch.nums";
    pub const PULL_MESSAGE_CACHE_CAPACITY: &str = "rmq.pull.message.cache.capacity";
}

/// All keys that identify an OMS-builtin header, used by `is_oms_header`.
const OMS_HEADER_KEYS: &[&str] = &[
    oms_keys::MESSAGE_ID,
    oms_keys::DESTINATION,
    oms_keys::TOPIC,
    oms_keys::QUEUE,
    oms_keys::SHARDING_KEY,
    oms_keys::TIMEOUT,
    oms_keys::START_TIME,
    oms_keys::PRODUCER_ID,
    oms_keys::CONSUMER_ID,
    oms_keys::BORN_HOST,
    oms_keys::BORN_TIMESTAMP,
    oms_keys::STORE_HOST,
    oms_keys::STORE_TIMESTAMP,
    oms_keys::SEARCH_KEYS,
];

// =================================================================================================
// KeyValue — Java: io.openmessaging.KeyValue (stringly-typed property bag with typed accessors).
// =================================================================================================

/// Java: `io.openmessaging.KeyValue`. A stringly-typed property map shared by producer/consumer
/// properties, message headers, and `SendResult` properties.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub struct KeyValue {
    map: BTreeMap<String, String>,
}

impl KeyValue {
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert a key/value, returning `self` for chaining (Java `KeyValue.put` returns the bag).
    pub fn put(&mut self, key: impl Into<String>, value: impl Into<String>) -> &mut Self {
        self.map.insert(key.into(), value.into());
        self
    }

    pub fn get(&self, key: &str) -> Option<&str> {
        self.map.get(key).map(String::as_str)
    }

    pub fn get_string(&self, key: &str) -> Option<String> {
        self.map.get(key).cloned()
    }

    pub fn get_string_or(&self, key: &str, default: &str) -> String {
        self.map.get(key).cloned().unwrap_or_else(|| default.to_string())
    }

    pub fn get_i32(&self, key: &str) -> Option<i32> {
        self.map.get(key).and_then(|v| v.parse().ok())
    }

    pub fn get_i32_or(&self, key: &str, default: i32) -> i32 {
        self.get_i32(key).unwrap_or(default)
    }

    pub fn get_i64(&self, key: &str) -> Option<i64> {
        self.map.get(key).and_then(|v| v.parse().ok())
    }

    pub fn get_i64_or(&self, key: &str, default: i64) -> i64 {
        self.get_i64(key).unwrap_or(default)
    }

    pub fn get_f64(&self, key: &str) -> Option<f64> {
        self.map.get(key).and_then(|v| v.parse().ok())
    }

    pub fn contains_key(&self, key: &str) -> bool {
        self.map.contains_key(key)
    }

    pub fn remove(&mut self, key: &str) {
        self.map.remove(key);
    }

    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Iterate over the `(key, value)` entries (Java `KeyValue.keySet()` / iteration).
    pub fn iter(&self) -> impl Iterator<Item = (&String, &String)> {
        self.map.iter()
    }

    pub fn keys(&self) -> impl Iterator<Item = &String> {
        self.map.keys()
    }

    /// Merge another `KeyValue` into this one (later wins). Java: `OMSUtil.buildKeyValue`.
    pub fn extend_from(&mut self, other: &KeyValue) {
        for (k, v) in other.iter() {
            self.map.insert(k.clone(), v.clone());
        }
    }

    /// Build a fresh `KeyValue` merging all the given bags (Java: `OMSUtil.buildKeyValue`).
    pub fn merge(bags: &[&KeyValue]) -> KeyValue {
        let mut out = KeyValue::new();
        for bag in bags {
            out.extend_from(bag);
        }
        out
    }
}

// =================================================================================================
// Domain types
// =================================================================================================

/// Java: `io.openmessaging.rocketmq.domain.BytesMessageImpl` (implements `BytesMessage`).
///
/// Carries a message `body` plus system and user headers. System headers take priority over user
/// headers when flattened into RocketMQ message properties (see `OmsUtil::msg_convert`).
#[derive(Clone, Debug, Default, PartialEq)]
pub struct BytesMessage {
    sys_headers: KeyValue,
    user_headers: KeyValue,
    body: Vec<u8>,
}

impl BytesMessage {
    pub fn new() -> Self {
        Self::default()
    }

    /// Java: `setBody(byte[])` — returns `self` for chaining.
    pub fn set_body(&mut self, body: Vec<u8>) -> &mut Self {
        self.body = body;
        self
    }

    pub fn body(&self) -> &[u8] {
        &self.body
    }

    pub fn sys_headers(&self) -> &KeyValue {
        &self.sys_headers
    }

    pub fn sys_headers_mut(&mut self) -> &mut KeyValue {
        &mut self.sys_headers
    }

    pub fn user_headers(&self) -> &KeyValue {
        &self.user_headers
    }

    pub fn user_headers_mut(&mut self) -> &mut KeyValue {
        &mut self.user_headers
    }

    pub fn put_sys_headers<K: Into<String>, V: Into<String>>(
        &mut self,
        key: K,
        value: V,
    ) -> &mut Self {
        self.sys_headers.put(key, value);
        self
    }

    pub fn put_user_headers<K: Into<String>, V: Into<String>>(
        &mut self,
        key: K,
        value: V,
    ) -> &mut Self {
        self.user_headers.put(key, value);
        self
    }
}

/// Java: `io.openmessaging.producer.SendResult` / `SendResultImpl`.
///
/// The Java `SendResultImpl` only carries `messageId` + `properties`; the topic/queue/offset/
/// region/trace fields mirror the richer RocketMQ `SendResult` and are populated by
/// `OmsUtil::send_result_from_message` once a real broker send exists.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct SendResult {
    pub message_id: String,
    pub topic: String,
    pub queue_id: i32,
    pub queue_offset: i64,
    pub region_id: Option<String>,
    pub trace_id: Option<String>,
    pub properties: KeyValue,
}

impl SendResult {
    pub fn new(message_id: impl Into<String>) -> Self {
        Self {
            message_id: message_id.into(),
            ..Self::default()
        }
    }

    /// Build a `SendResult` from a stored RocketMQ message (used once a broker send returns).
    pub fn from_message(message: &RocketMqMessage) -> Self {
        Self {
            message_id: message.message_id(),
            topic: message.topic.clone(),
            queue_id: message.queue_id,
            queue_offset: message.queue_offset as i64,
            region_id: message.properties.get(rmq_msg_keys::MSG_REGION).map(|s| s.to_string()),
            trace_id: None,
            properties: KeyValue::new(),
        }
    }
}

/// Java: `MessageQueue` (RocketMQ `org.apache.rocketmq.common.message.MessageQueue`).
/// Identifies a single logical queue inside a topic on a broker.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct MessageQueue {
    pub topic: String,
    pub broker_name: String,
    pub queue_id: i32,
}

impl MessageQueue {
    pub fn new(topic: impl Into<String>, broker_name: impl Into<String>, queue_id: i32) -> Self {
        Self {
            topic: topic.into(),
            broker_name: broker_name.into(),
            queue_id,
        }
    }
}

/// Java: `io.openmessaging.rocketmq.domain.ConsumeRequest`
/// (`messageExt` + `messageQueue` + `processQueue` + `startConsumeTimeMillis`).
///
/// The RocketMQ `MessageExt`/`ProcessQueue` pair is collapsed into a single decoded
/// [`RocketMqMessage`] plus its originating [`MessageQueue`]; `start_consume_time_millis` is set
/// when the request leaves the cache.
#[derive(Clone, Debug, PartialEq)]
pub struct ConsumeRequest {
    pub message: RocketMqMessage,
    pub message_queue: MessageQueue,
    pub start_consume_time_millis: i64,
}

impl ConsumeRequest {
    pub fn new(message: RocketMqMessage, message_queue: MessageQueue) -> Self {
        Self {
            message,
            message_queue,
            start_consume_time_millis: 0,
        }
    }

    pub fn message_id(&self) -> String {
        self.message.message_id()
    }
}

/// Java: `io.openmessaging.rocketmq.promise.FutureState` (DOING/DONE/CANCELLED).
///
/// The brief specifies the richer lifecycle {INIT, RUNNING, SUCCESS, FAILURE, CANCEL}; that is
/// implemented here. Mapping to the Java source: DOING -> `Running`, DONE -> `Success`,
/// CANCELLED -> `Cancel`; the extra `Init`/`Failure` states round out a real promise.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FutureState {
    Init,
    Running,
    Success,
    Failure,
    Cancel,
}

impl FutureState {
    pub fn is_init(self) -> bool {
        self == FutureState::Init
    }
    pub fn is_running(self) -> bool {
        self == FutureState::Running
    }
    /// Java: `isDoneState` — a terminal state (success, failure, or cancel).
    pub fn is_done(self) -> bool {
        matches!(self, FutureState::Success | FutureState::Failure | FutureState::Cancel)
    }
    pub fn is_success(self) -> bool {
        self == FutureState::Success
    }
    pub fn is_failure(self) -> bool {
        self == FutureState::Failure
    }
    pub fn is_cancelled(self) -> bool {
        self == FutureState::Cancel
    }
}

/// Java: `io.openmessaging.rocketmq.promise.DefaultPromise<V>` (implements `Promise<V>`).
///
/// A single-shot promise usable both synchronously (`get`/`get_timeout` block on a `Condvar`) and
/// asynchronously (it implements [`Future`], waking waiters via stored [`Waker`]s). On completion
/// it notifies any registered listeners.
pub struct DefaultPromise<T> {
    inner: Arc<PromiseInner<T>>,
}

struct PromiseInner<T> {
    state: Mutex<PromiseState<T>>,
    cond: Condvar,
}

struct PromiseState<T> {
    future_state: FutureState,
    result: Option<T>,
    exception: Option<String>,
    wakers: Vec<Waker>,
    listeners: Vec<Box<dyn Fn(&DefaultPromise<T>) + Send + Sync>>,
}

impl<T> Clone for DefaultPromise<T> {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl<T> DefaultPromise<T> {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(PromiseInner {
                state: Mutex::new(PromiseState {
                    future_state: FutureState::Init,
                    result: None,
                    exception: None,
                    wakers: Vec::new(),
                    listeners: Vec::new(),
                }),
                cond: Condvar::new(),
            }),
        }
    }

    /// Transition `Init -> Running`. No-op once already past `Init` (matches Java semantics where
    /// completion checks guard the running state).
    pub fn start(&self) {
        let mut state = self.inner.state.lock().unwrap();
        if state.future_state == FutureState::Init {
            state.future_state = FutureState::Running;
        }
    }

    pub fn is_done(&self) -> bool {
        self.inner.state.lock().unwrap().future_state.is_done()
    }

    pub fn is_running(&self) -> bool {
        self.inner.state.lock().unwrap().future_state.is_running()
    }

    pub fn is_cancelled(&self) -> bool {
        self.inner.state.lock().unwrap().future_state.is_cancelled()
    }

    pub fn is_success(&self) -> bool {
        self.inner.state.lock().unwrap().future_state.is_success()
    }

    pub fn is_failure(&self) -> bool {
        self.inner.state.lock().unwrap().future_state.is_failure()
    }

    pub fn get_throwable(&self) -> Option<String> {
        self.inner.state.lock().unwrap().exception.clone()
    }

    /// Java: `set(V)` — complete successfully. Returns `false` if already completed.
    pub fn set(&self, value: T) -> bool {
        self.finish(FutureState::Success, Some(value), None)
    }

    /// Java: `setFailure(Throwable)` — complete with failure. Returns `false` if already completed.
    pub fn set_failure(&self, cause: impl Into<String>) -> bool {
        self.finish(FutureState::Failure, None, Some(cause.into()))
    }

    /// Java: `cancel(boolean)` — cancel the promise. Java always returns `false`; here we return
    /// whether the cancel took effect.
    pub fn cancel(&self) -> bool {
        self.finish(
            FutureState::Cancel,
            None,
            Some("Get request result is timeout or interrupted".to_string()),
        )
    }

    /// Java: `addListener(FutureListener<V>)` — invoke `f` on completion, or immediately if the
    /// promise has already completed.
    pub fn add_listener<F>(&self, f: F)
    where
        F: Fn(&DefaultPromise<T>) + Send + Sync + 'static,
    {
        let mut state = self.inner.state.lock().unwrap();
        if state.future_state.is_done() {
            drop(state);
            f(self);
        } else {
            state.listeners.push(Box::new(f));
        }
    }

    /// Java: `get()` — block until completion and return the value or the recorded failure.
    pub fn get(&self) -> std::result::Result<T, String>
    where
        T: Clone,
    {
        let mut state = self.inner.state.lock().unwrap();
        while !state.future_state.is_done() {
            state = self.inner.cond.wait(state).unwrap();
        }
        Self::take_value(&state)
    }

    /// Java: `get(long timeout)` — block up to `timeout`; on timeout the promise is cancelled.
    pub fn get_timeout(&self, timeout: Duration) -> std::result::Result<T, String>
    where
        T: Clone,
    {
        let mut state = self.inner.state.lock().unwrap();
        if !state.future_state.is_done() {
            let (guard, waited) = self.inner.cond.wait_timeout(state, timeout).unwrap();
            state = guard;
            if !state.future_state.is_done() && waited.timed_out() {
                drop(state);
                self.cancel();
                state = self.inner.state.lock().unwrap();
            }
        }
        Self::take_value(&state)
    }

    // --- internal helpers ---

    fn finish(
        &self,
        state: FutureState,
        result: Option<T>,
        exception: Option<String>,
    ) -> bool {
        let mut guard = self.inner.state.lock().unwrap();
        if guard.future_state.is_done() {
            return false;
        }
        guard.future_state = state;
        guard.result = result;
        guard.exception = exception;
        let wakers = std::mem::take(&mut guard.wakers);
        let listeners = std::mem::take(&mut guard.listeners);
        drop(guard);
        self.inner.cond.notify_all();
        for waker in wakers {
            waker.wake();
        }
        for listener in listeners {
            listener(self);
        }
        true
    }

    fn take_value(state: &PromiseState<T>) -> std::result::Result<T, String>
    where
        T: Clone,
    {
        if let Some(exception) = &state.exception {
            return Err(exception.clone());
        }
        state
            .result
            .clone()
            .ok_or_else(|| "promise completed without a result".to_string())
    }
}

impl<T> Default for DefaultPromise<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T: Clone + Unpin> Future for DefaultPromise<T> {
    type Output = std::result::Result<T, String>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut state = self.inner.state.lock().unwrap();
        if state.future_state.is_done() {
            return Poll::Ready(Self::take_value(&state));
        }
        // Re-register the waker, replacing any equivalent one.
        if let Some(idx) = state.wakers.iter().position(|w| w.will_wake(cx.waker())) {
            state.wakers.remove(idx);
        }
        state.wakers.push(cx.waker().clone());
        Poll::Pending
    }
}

// =================================================================================================
// ClientConfig — Java: io.openmessaging.rocketmq.config.ClientConfig
// =================================================================================================

/// Java: `io.openmessaging.rocketmq.config.ClientConfig`. Field defaults mirror the Java source.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(default)]
pub struct ClientConfig {
    pub driver_impl: Option<String>,
    pub access_points: Option<String>,
    pub namespace: Option<String>,
    pub producer_id: Option<String>,
    pub consumer_id: Option<String>,
    pub operation_timeout: i32,
    pub region: Option<String>,
    pub routing_source: Option<String>,
    pub routing_destination: Option<String>,
    pub routing_expression: Option<String>,
    pub rmq_consumer_group: Option<String>,
    pub rmq_producer_group: String,
    pub rmq_max_redelivery_times: i32,
    pub rmq_message_consume_timeout: i32,
    pub rmq_max_consume_thread_nums: i32,
    pub rmq_min_consume_thread_nums: i32,
    pub rmq_message_destination: Option<String>,
    pub rmq_pull_message_batch_nums: i32,
    pub rmq_pull_message_cache_capacity: i32,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            driver_impl: None,
            access_points: None,
            namespace: None,
            producer_id: None,
            consumer_id: None,
            operation_timeout: 5000,
            region: None,
            routing_source: None,
            routing_destination: None,
            routing_expression: None,
            rmq_consumer_group: None,
            rmq_producer_group: "__OMS_PRODUCER_DEFAULT_GROUP".to_string(),
            rmq_max_redelivery_times: 16,
            rmq_message_consume_timeout: 15,
            rmq_max_consume_thread_nums: 64,
            rmq_min_consume_thread_nums: 20,
            rmq_message_destination: None,
            rmq_pull_message_batch_nums: 32,
            rmq_pull_message_cache_capacity: 1000,
        }
    }
}

impl ClientConfig {
    /// Validate that a consumer group is present (RocketMQ requires it).
    pub fn check_consumer_group(&self) -> Result<()> {
        if self.consumer_id.as_ref().map(String::is_empty).unwrap_or(true) {
            return Err(Error::Invalid(
                "Consumer Group is necessary for RocketMQ, please set it.".into(),
            ));
        }
        Ok(())
    }

    /// Split `accessPoints` (comma/semicolon/space separated `host:port` list) into broker
    /// addresses. The bridge sends straight to these addresses; no name-server route
    /// discovery is performed (see the module docs).
    pub fn access_point_addrs(&self) -> Vec<String> {
        self.access_points
            .as_deref()
            .map(|points| {
                points
                    .split([',', ';', ' '])
                    .map(str::trim)
                    .filter(|addr| !addr.is_empty())
                    .map(str::to_owned)
                    .collect()
            })
            .unwrap_or_default()
    }
}

// =================================================================================================
// OmsUtil — Java: io.openmessaging.rocketmq.utils.OMSUtil
// =================================================================================================

/// Java: `io.openmessaging.rocketmq.utils.OMSUtil`. Property translation between OMS headers and
/// RocketMQ message properties, plus instance-name and `SendResult` helpers.
pub struct OmsUtil;

impl OmsUtil {
    /// Java: `buildInstanceName` — `pid%OpenMessaging%nanos`.
    pub fn build_instance_name() -> String {
        let pid = std::process::id();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        format!("{pid}%OpenMessaging%{nanos}")
    }

    /// Java: `msgConvert(BytesMessage)` — translate an OMS `BytesMessage` into a RocketMQ
    /// [`RocketMqMessage`]. All RocketMQ destinations are topics (DESTINATION -> topic); system
    /// headers override user headers; START_TIME -> `__STARTDELIVERTIME`.
    pub fn msg_convert(oms: &BytesMessage) -> Result<RocketMqMessage> {
        let mut message = RocketMqMessage::default();
        message.body = oms.body.clone();

        let destination = oms
            .sys_headers
            .get(oms_keys::DESTINATION)
            .ok_or_else(|| Error::Invalid("DESTINATION is required to send an OMS message".into()))?
            .to_string();
        message.topic = destination;

        if let Some(start_time) = oms.sys_headers.get_i64(oms_keys::START_TIME) {
            if start_time > 0 {
                message
                    .properties
                    .insert(rmq_constants::START_DELIVER_TIME.to_string(), start_time.to_string());
            }
        }

        // User headers first, then system headers (system headers win on collision).
        for (key, value) in oms.user_headers.iter() {
            message.properties.insert(key.clone(), value.clone());
        }
        for (key, value) in oms.sys_headers.iter() {
            message.properties.insert(key.clone(), value.clone());
        }

        Ok(message)
    }

    /// Java: `msgConvert(MessageExt)` (reverse) — rebuild an OMS `BytesMessage` from a stored
    /// RocketMQ [`RocketMqMessage`]. OMS-builtin keys go to sys-headers; the rest to user-headers.
    pub fn msg_convert_from_ext(message: &RocketMqMessage) -> BytesMessage {
        let mut oms = BytesMessage::new();
        oms.set_body(message.body.clone());

        for (key, value) in &message.properties {
            if is_oms_header(key) {
                oms.sys_headers.put(key.clone(), value.clone());
            } else {
                oms.user_headers.put(key.clone(), value.clone());
            }
        }

        oms.put_sys_headers(oms_keys::MESSAGE_ID, message.message_id());
        oms.put_sys_headers(oms_keys::DESTINATION, message.topic.clone());
        oms.put_sys_headers(oms_keys::TOPIC, message.topic.clone());
        oms.put_sys_headers(oms_keys::QUEUE, message.queue_id.to_string());
        oms.put_sys_headers(oms_keys::START_TIME, message.born_timestamp.to_string());
        oms
    }

    /// Java: `sendResultConvert` — build an OMS `SendResult` from a stored broker message.
    pub fn send_result_from_message(message: &RocketMqMessage) -> SendResult {
        SendResult::from_message(message)
    }
}

/// Java: `OMSUtil.isOMSHeader` — whether a property key is an OMS-builtin header.
pub fn is_oms_header(key: &str) -> bool {
    OMS_HEADER_KEYS.contains(&key)
}

// =================================================================================================
// BeanUtils — Java: io.openmessaging.rocketmq.utils.BeanUtils
// =================================================================================================

/// Java: `io.openmessaging.rocketmq.utils.BeanUtils.populate` — copy `KeyValue` entries onto a
/// `ClientConfig` by matching normalized property names (split on `.`/`_`, camel-cased) to the
/// config's setter/field names. Reflection is replaced with a direct, well-defined mapping
/// (the brief asks for the essential logic, kept simple).
pub struct BeanUtils;

impl BeanUtils {
    pub fn populate(properties: &KeyValue) -> ClientConfig {
        let mut config = ClientConfig::default();
        for (key, value) in properties.iter() {
            Self::apply(&mut config, key, value);
        }
        config
    }

    fn apply(config: &mut ClientConfig, key: &str, value: &str) {
        // Normalize: lowercase, drop every non-alphanumeric char (collapses `.`/`_`/camelCase).
        let normalized: String = key.chars().filter(|c| c.is_alphanumeric()).collect::<String>().to_lowercase();
        let set_str = |slot: &mut Option<String>| *slot = Some(value.to_string());
        match normalized.as_str() {
            "driverimpl" => set_str(&mut config.driver_impl),
            "accesspoints" => set_str(&mut config.access_points),
            "namespace" => set_str(&mut config.namespace),
            "producerid" => set_str(&mut config.producer_id),
            "consumerid" => set_str(&mut config.consumer_id),
            "operationtimeout" => {
                if let Ok(v) = value.parse() {
                    config.operation_timeout = v;
                }
            }
            "region" => set_str(&mut config.region),
            "routingsource" => set_str(&mut config.routing_source),
            "routingdestination" => set_str(&mut config.routing_destination),
            "routingexpression" => set_str(&mut config.routing_expression),
            "rmqconsumergroup" => set_str(&mut config.rmq_consumer_group),
            "rmqproducergroup" => config.rmq_producer_group = value.to_string(),
            "rmqmaxredeliverytimes" => {
                if let Ok(v) = value.parse() {
                    config.rmq_max_redelivery_times = v;
                }
            }
            "rmqmessageconsumetimeout" => {
                if let Ok(v) = value.parse() {
                    config.rmq_message_consume_timeout = v;
                }
            }
            "rmqmaxconsumethreadnums" => {
                if let Ok(v) = value.parse() {
                    config.rmq_max_consume_thread_nums = v;
                }
            }
            "rmqminconsumethreadnums" => {
                if let Ok(v) = value.parse() {
                    config.rmq_min_consume_thread_nums = v;
                }
            }
            "rmqmessagedestination" => set_str(&mut config.rmq_message_destination),
            "rmqpullmessagebatchnums" => {
                if let Ok(v) = value.parse() {
                    config.rmq_pull_message_batch_nums = v;
                }
            }
            "rmqpullmessagecachecapacity" => {
                if let Ok(v) = value.parse() {
                    config.rmq_pull_message_cache_capacity = v;
                }
            }
            _ => {}
        }
    }
}

// =================================================================================================
// Transport — the wire the OMS bridge sends over
// =================================================================================================

/// Request codes and flags of the RocketMQ remoting protocol used by this bridge. Every ext
/// field written around these codes must match `crate::broker`'s request parsers exactly
/// (that is the wire-compatibility contract).
pub mod remoting_codes {
    pub const SEND_MESSAGE: i32 = 10;
    pub const PULL_MESSAGE: i32 = 11;
    pub const QUERY_CONSUMER_OFFSET: i32 = 14;
    pub const UPDATE_CONSUMER_OFFSET: i32 = 15;
    /// Broker answer when a consumer group has no committed offset on a queue yet.
    pub const CONSUMER_OFFSET_NOT_EXIST: i32 = 22;
    /// Pull answers: found / no new message / offset moved / offset illegal. All of them are
    /// normal replies carrying `nextBeginOffset` in the ext fields.
    pub const PULL_FOUND: i32 = 0;
    pub const PULL_NO_NEW_MSG: i32 = 19;
    pub const PULL_OFFSET_MOVED: i32 = 20;
    pub const PULL_OFFSET_ILLEGAL: i32 = 21;
    /// `PullSysFlag` bit 0x4: the pull request carries an inline subscription expression.
    pub const PULL_SYS_FLAG_SUBSCRIPTION: i32 = 4;
}

/// Abstraction over the remoting wire so the bridge can be exercised without a live broker
/// (tests plug in an in-memory [`Transport`]).
pub trait Transport: Send + Sync + 'static {
    /// Two-way RPC: write `request` to `addr` and await the correlated response.
    fn invoke<'s>(
        &'s self,
        addr: &'s str,
        request: Command,
        timeout: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<Command>> + Send + 's>>;

    /// Fire-and-forget write; the response (if any) is never read.
    fn send_oneway<'s>(
        &'s self,
        addr: &'s str,
        request: Command,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 's>>;
}

/// Production [`Transport`] speaking the real remoting protocol over TCP.
pub struct RemotingTransport;

impl Transport for RemotingTransport {
    fn invoke<'s>(
        &'s self,
        addr: &'s str,
        request: Command,
        timeout: Duration,
    ) -> Pin<Box<dyn Future<Output = Result<Command>> + Send + 's>> {
        Box::pin(async move { remoting::invoke(addr, request, timeout).await })
    }

    fn send_oneway<'s>(
        &'s self,
        addr: &'s str,
        request: Command,
    ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 's>> {
        Box::pin(async move {
            let mut request = request;
            request.flag |= remoting::ONEWAY_FLAG;
            let mut stream = tokio::net::TcpStream::connect(addr).await?;
            stream.set_nodelay(true)?;
            remoting::write_command(&mut stream, &request).await
        })
    }
}

/// Map a non-success broker response onto the crate error type (Java reads the response code
/// plus the `remark` field the broker fills on failures).
fn broker_error(response: &Command) -> Error {
    let remark = response.remark.clone().unwrap_or_default();
    if response.code == remoting_codes::CONSUMER_OFFSET_NOT_EXIST {
        Error::NotFound(format!("broker code {}: {remark}", response.code))
    } else {
        Error::Invalid(format!(
            "broker responded with code {}: {remark}",
            response.code
        ))
    }
}

fn operation_timeout(config: &ClientConfig) -> Duration {
    Duration::from_millis(config.operation_timeout.max(0) as u64)
}

/// Java `MessageClientIDSetter.createUniqID` stand-in: a process-unique client message id
/// carried in the `UNIQ_KEY` property (the broker only uses it for timer-wheel recall).
fn next_uniq_key() -> String {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{:013X}{}{:08X}", now_millis(), std::process::id(), seq)
}

/// A pull answer, mirroring Java `PullResult` (found messages plus the three offsets).
#[derive(Clone, Debug, Default)]
pub struct PullResult {
    pub messages: Vec<RocketMqMessage>,
    pub next_begin_offset: i64,
    pub min_offset: i64,
    pub max_offset: i64,
}

/// Producer-side remoting client: builds SEND_MESSAGE (code 10) requests and turns broker
/// answers into OMS [`SendResult`]s.
#[derive(Clone)]
pub struct RemotingProducerClient {
    transport: Arc<dyn Transport>,
    addrs: Vec<String>,
    timeout: Duration,
    producer_group: String,
}

impl RemotingProducerClient {
    pub fn new(config: &ClientConfig, transport: Arc<dyn Transport>) -> Self {
        Self {
            transport,
            addrs: config.access_point_addrs(),
            timeout: operation_timeout(config),
            producer_group: config.rmq_producer_group.clone(),
        }
    }

    /// Try each access point in turn; the first reachable broker wins (the Java OMS bridge
    /// likewise sends straight to the configured access points).
    async fn invoke(&self, request: Command) -> Result<Command> {
        let mut last =
            Error::Invalid("no access points configured (set accessPoints)".into());
        for addr in &self.addrs {
            match self.transport.invoke(addr, request.clone(), self.timeout).await {
                Ok(response) => return Ok(response),
                Err(error) => last = error,
            }
        }
        Err(last)
    }

    /// Build a SEND_MESSAGE (code 10) request whose ext fields match `crate::broker`'s `send`
    /// parser: topic/producerGroup/queueId/sysFlag/flag/bornTimestamp/properties.
    fn build_send(message: &RocketMqMessage, producer_group: &str) -> Result<Command> {
        if message.body.is_empty() {
            return Err(Error::Invalid("message body must not be empty".into()));
        }
        let mut properties = message.properties.clone();
        properties
            .entry("UNIQ_KEY".to_string())
            .or_insert_with(next_uniq_key);
        // queueId -1 lets the broker pick a writable queue (Java `selectOneMessageQueue`).
        Ok(Command::request(remoting_codes::SEND_MESSAGE)
            .with_ext("topic", message.topic.clone())
            .with_ext("producerGroup", producer_group)
            .with_ext("queueId", -1)
            .with_ext("sysFlag", 0)
            .with_ext("flag", message.flag)
            .with_ext("bornTimestamp", now_millis())
            .with_ext("properties", encode_properties(&properties)?)
            .with_body(message.body.clone()))
    }

    pub async fn send(&self, message: &RocketMqMessage) -> Result<SendResult> {
        let request = Self::build_send(message, &self.producer_group)?;
        let response = self.invoke(request).await?;
        if response.code != 0 {
            return Err(broker_error(&response));
        }
        Ok(SendResult {
            message_id: response.required("msgId")?.to_string(),
            topic: message.topic.clone(),
            queue_id: response.field::<i32>("queueId")?,
            queue_offset: response.field::<i64>("queueOffset")?,
            region_id: response.ext_fields.get("MSG_REGION").cloned(),
            trace_id: None,
            properties: KeyValue::new(),
        })
    }

    pub async fn send_oneway(&self, message: &RocketMqMessage) -> Result<()> {
        let request = Self::build_send(message, &self.producer_group)?;
        let mut last =
            Error::Invalid("no access points configured (set accessPoints)".into());
        for addr in &self.addrs {
            match self.transport.send_oneway(addr, request.clone()).await {
                Ok(()) => return Ok(()),
                Err(error) => last = error,
            }
        }
        Err(last)
    }
}

/// Consumer-side remoting client: query/update consumer offsets (codes 14/15) and pull
/// messages (code 11).
#[derive(Clone)]
pub struct RemotingConsumerClient {
    transport: Arc<dyn Transport>,
    addrs: Vec<String>,
    timeout: Duration,
    consumer_group: String,
}

impl RemotingConsumerClient {
    pub fn new(config: &ClientConfig, transport: Arc<dyn Transport>) -> Self {
        Self {
            transport,
            addrs: config.access_point_addrs(),
            timeout: operation_timeout(config),
            // Java's OMS consumer group is the configured `consumerId`.
            consumer_group: config.consumer_id.clone().unwrap_or_default(),
        }
    }

    /// Address used as the logical `brokerName` of pulled [`MessageQueue`]s.
    pub fn primary_broker(&self) -> String {
        self.addrs.first().cloned().unwrap_or_default()
    }

    async fn invoke(&self, request: Command) -> Result<Command> {
        let mut last =
            Error::Invalid("no access points configured (set accessPoints)".into());
        for addr in &self.addrs {
            match self.transport.invoke(addr, request.clone(), self.timeout).await {
                Ok(response) => return Ok(response),
                Err(error) => last = error,
            }
        }
        Err(last)
    }

    /// QUERY_CONSUMER_OFFSET (code 14); `None` when the broker answers 22 — nothing has been
    /// committed for this queue yet and consumption starts from offset 0.
    pub async fn query_consumer_offset(
        &self,
        topic: &str,
        queue_id: i32,
    ) -> Result<Option<i64>> {
        let request = Command::request(remoting_codes::QUERY_CONSUMER_OFFSET)
            .with_ext("topic", topic)
            .with_ext("consumerGroup", self.consumer_group.clone())
            .with_ext("queueId", queue_id);
        let response = self.invoke(request).await?;
        if response.code == 0 {
            Ok(Some(response.field::<i64>("offset")?))
        } else if response.code == remoting_codes::CONSUMER_OFFSET_NOT_EXIST {
            Ok(None)
        } else {
            Err(broker_error(&response))
        }
    }

    /// UPDATE_CONSUMER_OFFSET (code 15): commit the consumption progress of one queue.
    pub async fn update_consumer_offset(
        &self,
        topic: &str,
        queue_id: i32,
        offset: i64,
    ) -> Result<()> {
        let request = Command::request(remoting_codes::UPDATE_CONSUMER_OFFSET)
            .with_ext("topic", topic)
            .with_ext("consumerGroup", self.consumer_group.clone())
            .with_ext("queueId", queue_id)
            .with_ext("commitOffset", offset.max(0));
        let response = self.invoke(request).await?;
        if response.code == 0 {
            Ok(())
        } else {
            Err(broker_error(&response))
        }
    }

    /// PULL_MESSAGE (code 11) with a TAG=* subscription. `sysFlag` bit 0x4 marks the inline
    /// expression (`PullSysFlag`), so the broker resolves the filter from the request instead
    /// of a registered heartbeat subscription.
    pub async fn pull(
        &self,
        topic: &str,
        queue_id: i32,
        offset: i64,
        max_nums: i32,
    ) -> Result<PullResult> {
        let request = Command::request(remoting_codes::PULL_MESSAGE)
            .with_ext("consumerGroup", self.consumer_group.clone())
            .with_ext("topic", topic)
            .with_ext("queueId", queue_id)
            .with_ext("queueOffset", offset.max(0) as u64)
            .with_ext("maxMsgNums", max_nums.clamp(1, 4096))
            .with_ext("sysFlag", remoting_codes::PULL_SYS_FLAG_SUBSCRIPTION)
            .with_ext("expressionType", "TAG")
            .with_ext("subscription", "*");
        let response = self.invoke(request).await?;
        match response.code {
            remoting_codes::PULL_FOUND
            | remoting_codes::PULL_NO_NEW_MSG
            | remoting_codes::PULL_OFFSET_MOVED
            | remoting_codes::PULL_OFFSET_ILLEGAL => {}
            _ => return Err(broker_error(&response)),
        }
        // The pull body is a sequence of stored records back to back; decode them one by one.
        let mut messages = Vec::new();
        let mut body = response.body.as_slice();
        while !body.is_empty() {
            let (message, used) = RocketMqMessage::decode(body)?;
            messages.push(message);
            body = &body[used..];
        }
        Ok(PullResult {
            messages,
            next_begin_offset: response
                .field::<i64>("nextBeginOffset")
                .unwrap_or(offset.max(0)),
            min_offset: response
                .ext_fields
                .get("minOffset")
                .and_then(|v| v.parse().ok())
                .unwrap_or(0),
            max_offset: response
                .ext_fields
                .get("maxOffset")
                .and_then(|v| v.parse().ok())
                .unwrap_or(0),
        })
    }
}

/// One pull round shared by the push and pull consumers: resolve the queue offset (broker
/// query on first use, cache afterwards), pull a batch sized to the remaining cache
/// capacity, enqueue what fits, and commit advanced offsets (code 15).
async fn pull_batch_into_cache(
    client: &RemotingConsumerClient,
    cache: &LocalMessageCache,
    topic: &str,
    queue_id: i32,
) -> Result<usize> {
    let mq = MessageQueue::new(topic.to_string(), client.primary_broker(), queue_id);
    let offset = match cache.peek_pull_offset(&mq) {
        Some(offset) => offset,
        None => {
            let committed = client
                .query_consumer_offset(topic, queue_id)
                .await?
                .unwrap_or(0);
            cache.update_pull_offset(&mq, committed);
            committed
        }
    };
    let batch = cache.next_pull_batch_nums();
    if batch <= 0 {
        // The cache is full: do not pull (and do not advance) until the app drains it.
        return Ok(0);
    }
    let result = client.pull(topic, queue_id, offset, batch).await?;
    let mut delivered = 0usize;
    for message in result.messages {
        if !cache.try_put_message(ConsumeRequest::new(message, mq.clone())) {
            // Cache filled mid-batch; keep the client-side offset at the last cached record
            // so the remainder is re-pulled instead of dropped.
            cache.update_pull_offset(&mq, offset + delivered as i64);
            return Ok(delivered);
        }
        delivered += 1;
    }
    if result.next_begin_offset != offset {
        cache.update_pull_offset(&mq, result.next_begin_offset);
        client
            .update_consumer_offset(topic, queue_id, result.next_begin_offset)
            .await?;
    }
    Ok(delivered)
}

/// Fixed interval between pull rounds that delivered nothing.
const PULL_IDLE_INTERVAL: Duration = Duration::from_millis(200);
/// Fixed interval between push-loop rounds that delivered nothing.
const PUSH_PULL_INTERVAL: Duration = Duration::from_millis(200);
/// Interval the paused push loop sleeps for.
const PUSH_IDLE_INTERVAL: Duration = Duration::from_millis(100);

// =================================================================================================
// Producer — Java: AbstractOMSProducer + ProducerImpl
// =================================================================================================

/// Java: `io.openmessaging.rocketmq.producer.AbstractOMSProducer` (implements `ServiceLifecycle`
/// + `MessageFactory`). Holds the shared `KeyValue` properties, the derived `ClientConfig`, and a
/// `started` flag. The Java class wraps a live `DefaultMQProducer`; the Rust port keeps the
/// lifecycle flag here and performs the actual sends through [`RemotingProducerClient`].
pub struct AbstractOmsProducer {
    pub properties: KeyValue,
    pub client_config: ClientConfig,
    started: bool,
}

impl AbstractOmsProducer {
    pub fn new(properties: KeyValue) -> Self {
        let client_config = BeanUtils::populate(&properties);
        Self {
            properties,
            client_config,
            started: false,
        }
    }

    pub fn is_started(&self) -> bool {
        self.started
    }

    pub fn startup(&mut self) {
        // Java: rocketmqProducer.start(); a live client would connect to the name server here.
        self.started = true;
    }

    pub fn shutdown(&mut self) {
        self.started = false;
    }

    /// Java: `checkMessageType` — only `BytesMessage` is supported.
    pub fn check_message_type(&self, _message: &BytesMessage) {
        // The Rust type system already restricts senders to `BytesMessage`.
    }

    /// Java: `createBytesMessage(queue, body)`.
    pub fn create_bytes_message(&self, queue: &str, body: Vec<u8>) -> BytesMessage {
        let mut message = BytesMessage::new();
        message.set_body(body);
        message.put_sys_headers(oms_keys::DESTINATION, queue);
        message
    }
}

/// Java: `io.openmessaging.rocketmq.producer.ProducerImpl` (implements `Producer`).
pub struct ProducerImpl {
    inner: AbstractOmsProducer,
    producer_client: RemotingProducerClient,
}

impl ProducerImpl {
    pub fn new(properties: KeyValue) -> Self {
        Self::with_transport(properties, Arc::new(RemotingTransport))
    }

    /// Build the producer over a custom [`Transport`] (tests use an in-memory one).
    pub fn with_transport(properties: KeyValue, transport: Arc<dyn Transport>) -> Self {
        let inner = AbstractOmsProducer::new(properties);
        let producer_client = RemotingProducerClient::new(&inner.client_config, transport);
        Self {
            inner,
            producer_client,
        }
    }

    pub fn attributes(&self) -> &KeyValue {
        &self.inner.properties
    }

    pub fn client_config(&self) -> &ClientConfig {
        &self.inner.client_config
    }

    /// Translate an OMS message into a RocketMQ message (real logic).
    pub fn to_rocketmq(&self, message: &BytesMessage) -> Result<RocketMqMessage> {
        self.inner.check_message_type(message);
        OmsUtil::msg_convert(message)
    }

    /// Java: `send(Message)` — synchronous send: translate, then SEND_MESSAGE (code 10) to
    /// the broker and map its answer (ext msgId/queueId/queueOffset) onto a [`SendResult`].
    pub async fn send(&self, message: &BytesMessage) -> Result<SendResult> {
        self.inner.check_message_type(message);
        let rmq = OmsUtil::msg_convert(message)?;
        self.producer_client.send(&rmq).await
    }

    /// Java: `sendAsync(Message)` — the send runs on a background task that completes the
    /// returned [`DefaultPromise`] (whose own [`Future`] impl resolves for the caller).
    pub async fn send_async(&self, message: &BytesMessage) -> DefaultPromise<SendResult> {
        self.inner.check_message_type(message);
        let promise: DefaultPromise<SendResult> = DefaultPromise::new();
        promise.start();
        match OmsUtil::msg_convert(message) {
            Ok(rmq) => {
                let client = self.producer_client.clone();
                // The caller keeps the original promise; the background task fills the clone.
                let task_promise = promise.clone();
                tokio::spawn(async move {
                    match client.send(&rmq).await {
                        Ok(result) => {
                            task_promise.set(result);
                        }
                        Err(error) => {
                            task_promise.set_failure(error.to_string());
                        }
                    }
                });
            }
            Err(e) => {
                promise.set_failure(e.to_string());
            }
        }
        promise
    }

    /// Java: `sendOneway(Message)` — write the request with the oneway flag; the response,
    /// if any, is never read.
    pub async fn send_oneway(&self, message: &BytesMessage) -> Result<()> {
        self.inner.check_message_type(message);
        let rmq = OmsUtil::msg_convert(message)?;
        self.producer_client.send_oneway(&rmq).await
    }
}

// =================================================================================================
// Consumer — LocalMessageCache + PushConsumerImpl + PullConsumerImpl
// =================================================================================================

/// Java: `io.openmessaging.rocketmq.consumer.LocalMessageCache` (implements `ServiceLifecycle`).
///
/// An in-memory, bounded, blocking cache of [`ConsumeRequest`]s feeding the consumers. Real
/// `try_put_message` / `take_message` / `ack` / `clear` logic; offset bookkeeping per
/// [`MessageQueue`] is real. The expiring-message reaper of the Java cache is not ported;
/// broker offset commits happen in `pull_batch_into_cache`.
pub struct LocalMessageCache {
    cache: Mutex<VecDeque<ConsumeRequest>>,
    cond: Condvar,
    capacity: usize,
    consumed: Mutex<HashMap<String, ConsumeRequest>>,
    pull_offset_table: Mutex<HashMap<MessageQueue, i64>>,
    batch_nums: i32,
}

impl LocalMessageCache {
    pub fn new(capacity: usize, batch_nums: i32) -> Self {
        Self {
            cache: Mutex::new(VecDeque::with_capacity(capacity)),
            cond: Condvar::new(),
            capacity,
            consumed: Mutex::new(HashMap::new()),
            pull_offset_table: Mutex::new(HashMap::new()),
            batch_nums,
        }
    }

    /// Java: blocking `put` — non-blocking variant that returns `false` when the cache is full.
    pub fn try_put_message(&self, request: ConsumeRequest) -> bool {
        let mut cache = self.cache.lock().unwrap();
        if cache.len() >= self.capacity {
            return false;
        }
        cache.push_back(request);
        self.cond.notify_one();
        true
    }

    /// Java: `submitConsumeRequest` — alias for the blocking-enqueue path the pull loop uses.
    pub fn submit_consume_request(&self, request: ConsumeRequest) {
        let mut cache = self.cache.lock().unwrap();
        cache.push_back(request);
        self.cond.notify_one();
    }

    /// Java: `poll(timeout)` — block until a request is available (or timeout), stamp its start
    /// time, and record it in the `consumed` map for later `ack`.
    pub fn take_message(&self, timeout: Duration) -> Option<ConsumeRequest> {
        let mut guard = self.cache.lock().unwrap();
        loop {
            if let Some(mut request) = guard.pop_front() {
                request.start_consume_time_millis = now_millis();
                self.consumed
                    .lock()
                    .unwrap()
                    .insert(request.message_id(), request.clone());
                return Some(request);
            }
            let (next, waited) = self.cond.wait_timeout(guard, timeout).unwrap();
            guard = next;
            if waited.timed_out() {
                return None;
            }
        }
    }

    /// Java: `ack(messageId)` — drop the in-flight request from the consumed map.
    pub fn ack(&self, message_id: &str) {
        self.consumed.lock().unwrap().remove(message_id);
    }

    /// Drop every pending and in-flight request.
    pub fn clear(&self) {
        self.cache.lock().unwrap().clear();
        self.consumed.lock().unwrap().clear();
    }

    /// Java: `nextPullBatchNums` — never ask for more than the cache can currently hold.
    pub fn next_pull_batch_nums(&self) -> i32 {
        let remaining = self.capacity.saturating_sub(self.cache.lock().unwrap().len());
        self.batch_nums.min(remaining as i32)
    }

    /// Java: `nextPullOffset(MessageQueue)` — cached offset, defaulting to 0.
    pub fn next_pull_offset(&self, queue: &MessageQueue) -> i64 {
        *self
            .pull_offset_table
            .lock()
            .unwrap()
            .entry(queue.clone())
            .or_insert(0)
    }

    /// Cached offset without inserting a default (`None` when this queue was never pulled).
    pub fn peek_pull_offset(&self, queue: &MessageQueue) -> Option<i64> {
        self.pull_offset_table.lock().unwrap().get(queue).copied()
    }

    /// Java: `updatePullOffset(MessageQueue, long)`.
    pub fn update_pull_offset(&self, queue: &MessageQueue, next_offset: i64) {
        self.pull_offset_table
            .lock()
            .unwrap()
            .insert(queue.clone(), next_offset);
    }
}

/// Consume status reported back through a [`MessageContext`] (Java: `ConsumeConcurrentlyStatus`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConsumeStatus {
    ConsumeSuccess,
    ReconsumeLater,
}

/// Java: `MessageListener.Context` — passed to `MessageListener.onReceived`; `ack()` marks the
/// message consumed.
pub struct MessageContext {
    inner: Mutex<CtxInner>,
}

struct CtxInner {
    properties: KeyValue,
    acked: bool,
    status: ConsumeStatus,
}

impl MessageContext {
    pub fn new() -> Self {
        let mut properties = KeyValue::new();
        properties.put(
            non_standard_keys::MESSAGE_CONSUME_STATUS,
            "RECONSUME_LATER",
        );
        Self {
            inner: Mutex::new(CtxInner {
                properties,
                acked: false,
                status: ConsumeStatus::ReconsumeLater,
            }),
        }
    }

    /// Java: `Context.attributes()` — the mutable context properties (e.g. consume status).
    pub fn attributes(&self) -> KeyValue {
        self.inner.lock().unwrap().properties.clone()
    }

    /// Java: `Context.ack()` — mark success.
    pub fn ack(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.acked = true;
        inner.status = ConsumeStatus::ConsumeSuccess;
        inner
            .properties
            .put(non_standard_keys::MESSAGE_CONSUME_STATUS, "CONSUME_SUCCESS");
    }

    pub fn is_acked(&self) -> bool {
        self.inner.lock().unwrap().acked
    }

    pub fn status(&self) -> ConsumeStatus {
        self.inner.lock().unwrap().status
    }
}

impl Default for MessageContext {
    fn default() -> Self {
        Self::new()
    }
}

/// Java: `io.openmessaging.consumer.MessageListener` — invoked when a message is delivered.
pub trait MessageListener: Send + Sync {
    fn on_received(&self, message: &BytesMessage, context: &MessageContext);
}

/// Java: `io.openmessaging.rocketmq.consumer.PushConsumerImpl` (implements `PushConsumer`).
pub struct PushConsumerImpl {
    properties: KeyValue,
    client_config: ClientConfig,
    started: bool,
    paused: bool,
    subscribe_table: HashMap<String, Arc<dyn MessageListener>>,
    local_message_cache: LocalMessageCache,
    consumer_client: RemotingConsumerClient,
}

impl PushConsumerImpl {
    pub fn new(properties: KeyValue) -> Result<Self> {
        Self::with_transport(properties, Arc::new(RemotingTransport))
    }

    /// Build the consumer over a custom [`Transport`] (tests use an in-memory one).
    pub fn with_transport(properties: KeyValue, transport: Arc<dyn Transport>) -> Result<Self> {
        let client_config = BeanUtils::populate(&properties);
        client_config.check_consumer_group()?;
        let local_message_cache = LocalMessageCache::new(
            client_config.rmq_pull_message_cache_capacity.max(1) as usize,
            client_config.rmq_pull_message_batch_nums,
        );
        let consumer_client = RemotingConsumerClient::new(&client_config, transport);
        Ok(Self {
            properties,
            client_config,
            started: false,
            paused: false,
            subscribe_table: HashMap::new(),
            local_message_cache,
            consumer_client,
        })
    }

    pub fn attributes(&self) -> &KeyValue {
        &self.properties
    }

    pub fn client_config(&self) -> &ClientConfig {
        &self.client_config
    }

    pub fn resume(&mut self) {
        self.paused = false;
    }

    pub fn suspend(&mut self) {
        self.paused = true;
    }

    pub fn is_suspended(&self) -> bool {
        self.paused
    }

    /// Java: `attachQueue(queueName, listener)` — register a listener for a topic/queue.
    pub fn attach_queue(&mut self, queue_name: &str, listener: Arc<dyn MessageListener>) -> &mut Self {
        self.subscribe_table.insert(queue_name.to_string(), listener);
        self
    }

    /// Java: `detachQueue(queueName)`.
    pub fn detach_queue(&mut self, queue_name: &str) -> &mut Self {
        self.subscribe_table.remove(queue_name);
        self
    }

    /// Real dispatch path the pull loop calls: find the listener by destination and invoke
    /// it with a fresh [`MessageContext`].
    pub fn deliver(&self, message: &BytesMessage) -> Result<()> {
        let topic = message
            .sys_headers
            .get(oms_keys::DESTINATION)
            .or_else(|| message.sys_headers.get(oms_keys::TOPIC))
            .ok_or_else(|| Error::Invalid("message has no DESTINATION/TOPIC".into()))?
            .to_string();
        let listener = self
            .subscribe_table
            .get(&topic)
            .ok_or_else(|| Error::NotFound(format!("topic {topic} is not attached to this consumer")))?;
        let context = MessageContext::new();
        listener.on_received(message, &context);
        if context.is_acked() {
            Ok(())
        } else {
            Err(Error::Invalid(format!(
                "message on topic {topic} was not acked by the listener"
            )))
        }
    }

    pub fn startup(&mut self) {
        self.started = true;
    }

    pub fn shutdown(&mut self) {
        self.started = false;
    }

    /// Java: `MessageListenerImpl.consumeMessage` loop. The RocketMQ push consumer is itself
    /// a pull loop internally; this port reproduces that: each round pulls one batch per
    /// attached topic into the local cache (`pull_batch_into_cache`) and dispatches every
    /// cached message to its listener. Runs until [`PushConsumerImpl::shutdown`].
    pub async fn run(&self) -> Result<()> {
        while self.started {
            if self.paused {
                tokio::time::sleep(PUSH_IDLE_INTERVAL).await;
                continue;
            }
            let mut delivered = 0usize;
            for topic in self.subscribe_table.keys() {
                match pull_batch_into_cache(
                    &self.consumer_client,
                    &self.local_message_cache,
                    topic,
                    0,
                )
                .await
                {
                    Ok(n) => delivered += n,
                    Err(error) => {
                        tracing::warn!(topic = %topic, %error, "OMS push pull failed");
                    }
                }
            }
            while let Some(request) = self.local_message_cache.take_message(Duration::ZERO) {
                let oms = OmsUtil::msg_convert_from_ext(&request.message);
                if let Err(error) = self.deliver(&oms) {
                    tracing::warn!(%error, "OMS push dispatch failed");
                }
                self.local_message_cache.ack(&request.message_id());
                delivered += 1;
            }
            if delivered == 0 {
                tokio::time::sleep(PUSH_PULL_INTERVAL).await;
            }
        }
        Ok(())
    }
}

/// Java: `io.openmessaging.rocketmq.consumer.PullConsumerImpl` (implements `PullConsumer`).
pub struct PullConsumerImpl {
    properties: KeyValue,
    client_config: ClientConfig,
    started: bool,
    attached_queues: HashSet<String>,
    local_message_cache: LocalMessageCache,
    consumer_client: RemotingConsumerClient,
}

impl PullConsumerImpl {
    pub fn new(properties: KeyValue) -> Result<Self> {
        Self::with_transport(properties, Arc::new(RemotingTransport))
    }

    /// Build the consumer over a custom [`Transport`] (tests use an in-memory one).
    pub fn with_transport(properties: KeyValue, transport: Arc<dyn Transport>) -> Result<Self> {
        let client_config = BeanUtils::populate(&properties);
        client_config.check_consumer_group()?;
        let local_message_cache = LocalMessageCache::new(
            client_config.rmq_pull_message_cache_capacity.max(1) as usize,
            client_config.rmq_pull_message_batch_nums,
        );
        let consumer_client = RemotingConsumerClient::new(&client_config, transport);
        Ok(Self {
            properties,
            client_config,
            started: false,
            attached_queues: HashSet::new(),
            local_message_cache,
            consumer_client,
        })
    }

    pub fn attributes(&self) -> &KeyValue {
        &self.properties
    }

    pub fn client_config(&self) -> &ClientConfig {
        &self.client_config
    }

    pub fn cache(&self) -> &LocalMessageCache {
        &self.local_message_cache
    }

    /// Java: `attachQueue(queueName)` — record the subscription the pull loop drives.
    pub fn attach_queue(&mut self, queue_name: &str) -> &mut Self {
        self.attached_queues.insert(queue_name.to_string());
        self
    }

    pub fn detach_queue(&mut self, queue_name: &str) -> &mut Self {
        self.attached_queues.remove(queue_name);
        self
    }

    /// Java: `receive()` — take the next cached message, converted to an OMS `BytesMessage`.
    pub fn receive(&self) -> Option<BytesMessage> {
        let timeout = Duration::from_millis(self.client_config.operation_timeout.max(0) as u64);
        self.receive_with_timeout(timeout)
    }

    pub fn receive_with_timeout(&self, timeout: Duration) -> Option<BytesMessage> {
        let request = self.local_message_cache.take_message(timeout)?;
        Some(OmsUtil::msg_convert_from_ext(&request.message))
    }

    /// Java: `ack(messageId)`.
    pub fn ack(&self, message_id: &str) {
        self.local_message_cache.ack(message_id);
    }

    pub fn startup(&mut self) {
        self.started = true;
        // The pull loop itself is `pull_loop`; the caller drives it (Java schedules it
        // internally). The Java cache's expire-message reaper is not ported.
    }

    pub fn shutdown(&mut self) {
        self.started = false;
    }

    /// One pull round for one attached topic (queue 0): query the committed offset on first
    /// use, PULL_MESSAGE (code 11) a batch into the local cache, and commit advanced offsets
    /// (code 15). Exposed for driving single rounds without the loop.
    pub async fn pull_once(&self, topic: &str) -> Result<usize> {
        pull_batch_into_cache(
            &self.consumer_client,
            &self.local_message_cache,
            topic,
            0,
        )
        .await
    }

    /// Java: `registerPullTaskCallback` / `doPullTask` — pull every attached queue in a loop
    /// until [`PullConsumerImpl::shutdown`].
    pub async fn pull_loop(&self) -> Result<()> {
        while self.started {
            let mut delivered = 0usize;
            for topic in &self.attached_queues {
                match self.pull_once(topic).await {
                    Ok(n) => delivered += n,
                    Err(error) => {
                        tracing::warn!(topic = %topic, %error, "OMS pull failed");
                    }
                }
            }
            if delivered == 0 {
                tokio::time::sleep(PULL_IDLE_INTERVAL).await;
            }
        }
        Ok(())
    }
}

// =================================================================================================
// MessagingAccessPointImpl — Java: io.openmessaging.rocketmq.MessagingAccessPointImpl
// =================================================================================================

/// Java: `io.openmessaging.rocketmq.MessagingAccessPointImpl` (implements `MessagingAccessPoint`).
/// Entry point that builds producers/consumers from `KeyValue` access-point properties.
pub struct MessagingAccessPointImpl {
    access_point_properties: KeyValue,
}

impl MessagingAccessPointImpl {
    pub fn new(access_point_properties: KeyValue) -> Self {
        Self {
            access_point_properties,
        }
    }

    pub fn attributes(&self) -> &KeyValue {
        &self.access_point_properties
    }

    pub fn impl_version(&self) -> &str {
        "0.3.0"
    }

    pub fn create_producer(&self) -> ProducerImpl {
        ProducerImpl::new(self.access_point_properties.clone())
    }

    pub fn create_producer_with(&self, properties: &KeyValue) -> ProducerImpl {
        let merged = KeyValue::merge(&[&self.access_point_properties, properties]);
        ProducerImpl::new(merged)
    }

    pub fn create_push_consumer(&self) -> Result<PushConsumerImpl> {
        PushConsumerImpl::new(self.access_point_properties.clone())
    }

    pub fn create_push_consumer_with(&self, properties: &KeyValue) -> Result<PushConsumerImpl> {
        let merged = KeyValue::merge(&[&self.access_point_properties, properties]);
        PushConsumerImpl::new(merged)
    }

    pub fn create_pull_consumer(&self) -> Result<PullConsumerImpl> {
        PullConsumerImpl::new(self.access_point_properties.clone())
    }

    pub fn create_pull_consumer_with(&self, properties: &KeyValue) -> Result<PullConsumerImpl> {
        let merged = KeyValue::merge(&[&self.access_point_properties, properties]);
        PullConsumerImpl::new(merged)
    }
}

// =================================================================================================
// Tests
// =================================================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn oms_message_roundtrip_to_rocketmq_properties() {
        // Build an OMS BytesMessage with both sys and user headers.
        let mut oms = BytesMessage::new();
        oms.set_body(b"hello-oms".to_vec());
        oms.put_sys_headers(oms_keys::DESTINATION, "TestTopic");
        oms.put_sys_headers(oms_keys::START_TIME, 1_700_000_000_000i64.to_string());
        oms.put_sys_headers(oms_keys::SHARDING_KEY, "shard-1");
        oms.put_user_headers(rmq_msg_keys::TAGS, "tagA");
        oms.put_user_headers(rmq_msg_keys::KEYS, "k1");

        // OMS -> RocketMQ.
        let rmq = OmsUtil::msg_convert(&oms).expect("convert to rocketmq");
        assert_eq!(rmq.topic, "TestTopic");
        assert_eq!(rmq.body, b"hello-oms");
        // System header wins; START_TIME mapped to the scheduled-delivery property.
        assert_eq!(rmq.properties.get(rmq_constants::START_DELIVER_TIME).unwrap(), "1700000000000");
        assert_eq!(rmq.properties.get(oms_keys::SHARDING_KEY).unwrap(), "shard-1");
        assert_eq!(rmq.properties.get(rmq_msg_keys::TAGS).unwrap(), "tagA");
        assert_eq!(rmq.properties.get(rmq_msg_keys::KEYS).unwrap(), "k1");

        // RocketMQ -> OMS (reverse), preserving headers.
        let back = OmsUtil::msg_convert_from_ext(&rmq);
        assert_eq!(back.body(), b"hello-oms");
        // OMS-builtin keys land in sys-headers; RocketMQ tags/keys land in user-headers.
        assert_eq!(back.sys_headers.get(oms_keys::DESTINATION).unwrap(), "TestTopic");
        assert_eq!(back.user_headers.get(rmq_msg_keys::TAGS).unwrap(), "tagA");
        assert_eq!(back.user_headers.get(rmq_msg_keys::KEYS).unwrap(), "k1");
        // The scheduled-delivery key is a RocketMQ property, not an OMS header -> user header.
        assert_eq!(
            back.user_headers.get(rmq_constants::START_DELIVER_TIME).unwrap(),
            "1700000000000"
        );

        // The RocketMQ message must still encode/decode round-trip cleanly.
        let encoded = rmq.encode().expect("encode");
        let (decoded, used) = RocketMqMessage::decode(&encoded).expect("decode");
        assert_eq!(used, encoded.len());
        assert_eq!(decoded.topic, rmq.topic);
        assert_eq!(decoded.body, rmq.body);
        assert_eq!(decoded.properties, rmq.properties);
    }

    #[test]
    fn default_promise_state_transitions() {
        let promise: DefaultPromise<i32> = DefaultPromise::new();
        assert!(promise.inner.state.lock().unwrap().future_state.is_init());

        promise.start();
        assert!(promise.is_running());
        assert!(!promise.is_done());

        // Setting a value completes successfully.
        assert!(promise.set(99));
        assert!(promise.is_success());
        assert!(promise.is_done());
        assert_eq!(promise.get().unwrap(), 99);

        // A second completion is rejected.
        assert!(!promise.set(1));
        assert!(!promise.set_failure("x"));

        // Failure path.
        let failed: DefaultPromise<i32> = DefaultPromise::new();
        failed.start();
        assert!(failed.set_failure("boom"));
        assert!(failed.is_failure());
        assert_eq!(failed.get_throwable().as_deref(), Some("boom"));
        assert!(failed.get().is_err());

        // Listener fires on completion.
        let listened: DefaultPromise<String> = DefaultPromise::new();
        listened.start();
        let fired = Arc::new(Mutex::new(false));
        let fired_clone = Arc::clone(&fired);
        listened.add_listener(move |_| *fired_clone.lock().unwrap() = true);
        listened.set("done".to_string());
        assert!(*fired.lock().unwrap());
    }

    #[test]
    fn default_promise_timeout_cancels() {
        let promise: DefaultPromise<i32> = DefaultPromise::new();
        promise.start();
        let result = promise.get_timeout(Duration::from_millis(20));
        assert!(result.is_err());
        assert!(promise.is_cancelled());
    }

    #[test]
    fn default_promise_future_resolves() {
        // Exercise the async Future impl by completing from another thread.
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .expect("runtime");
        rt.block_on(async {
            let promise: DefaultPromise<i32> = DefaultPromise::new();
            promise.start();
            let promise2 = promise.clone();
            std::thread::spawn(move || {
                std::thread::sleep(Duration::from_millis(10));
                promise2.set(7);
            });
            assert_eq!(promise.await.unwrap(), 7);
        });
    }

    #[test]
    fn local_message_cache_put_take_ack() {
        let cache = LocalMessageCache::new(2, 32);

        // Fill to capacity; the third put is rejected.
        let q = MessageQueue::new("T", "broker-a", 0);
        let m1 = RocketMqMessage {
            topic: "T".into(),
            queue_id: 0,
            ..RocketMqMessage::default()
        };
        let m2 = RocketMqMessage {
            topic: "T".into(),
            queue_id: 1,
            ..RocketMqMessage::default()
        };
        let m3 = RocketMqMessage {
            topic: "T".into(),
            queue_id: 2,
            ..RocketMqMessage::default()
        };
        assert!(cache.try_put_message(ConsumeRequest::new(m1.clone(), q.clone())));
        assert!(cache.try_put_message(ConsumeRequest::new(m2, q.clone())));
        assert!(!cache.try_put_message(ConsumeRequest::new(m3, q.clone())));

        // take_message returns requests and records them for ack.
        let first = cache
            .take_message(Duration::from_millis(50))
            .expect("take first");
        assert_eq!(first.message.queue_id, 0);
        assert!(first.start_consume_time_millis > 0);
        let id = first.message_id();
        assert!(cache.take_message(Duration::from_millis(50)).is_some());

        // Offset bookkeeping.
        assert_eq!(cache.next_pull_offset(&q), 0);
        cache.update_pull_offset(&q, 42);
        assert_eq!(cache.next_pull_offset(&q), 42);

        // ack removes the in-flight request.
        cache.ack(&id);
        assert!(cache.consumed.lock().unwrap().get(&id).is_none());

        // Empty cache -> take times out.
        assert!(cache.take_message(Duration::from_millis(20)).is_none());

        // clear wipes everything.
        cache.clear();
        assert!(cache.cache.lock().unwrap().is_empty());
        assert!(cache.consumed.lock().unwrap().is_empty());
    }

    #[test]
    fn client_config_bean_population() {
        let mut props = KeyValue::new();
        props.put("accessPoints", "127.0.0.1:9876");
        props.put("consumerId", "my-group");
        props.put("operationTimeout", "3000");
        props.put("rmq.pull.message.batch.nums", "64");
        props.put("rmq.pull.message.cache.capacity", "500");

        let config = BeanUtils::populate(&props);
        assert_eq!(config.access_points.as_deref(), Some("127.0.0.1:9876"));
        assert_eq!(config.consumer_id.as_deref(), Some("my-group"));
        assert_eq!(config.operation_timeout, 3000);
        assert_eq!(config.rmq_pull_message_batch_nums, 64);
        assert_eq!(config.rmq_pull_message_cache_capacity, 500);
        // Unknown key is ignored; defaults preserved.
        assert_eq!(config.rmq_producer_group, "__OMS_PRODUCER_DEFAULT_GROUP");
        assert_eq!(config.rmq_max_redelivery_times, 16);
    }

    #[test]
    fn access_point_builds_producers_and_consumers() {
        let mut ap_props = KeyValue::new();
        ap_props.put("consumerId", "group-1");
        let ap = MessagingAccessPointImpl::new(ap_props);
        assert_eq!(ap.impl_version(), "0.3.0");

        let producer = ap.create_producer();
        let msg = producer.inner.create_bytes_message("TopicA", b"x".to_vec());
        assert_eq!(msg.sys_headers.get(oms_keys::DESTINATION).unwrap(), "TopicA");

        let consumer = ap.create_pull_consumer().expect("pull consumer");
        assert_eq!(consumer.client_config().consumer_id.as_deref(), Some("group-1"));
    }

    // --- remoting bridge (send / pull over a mock transport) ---

    /// In-memory [`Transport`]: records every request and replays canned responses per
    /// request code.
    struct MockTransport {
        requests: Mutex<Vec<Command>>,
        responses: Mutex<HashMap<i32, Vec<Command>>>,
    }

    impl MockTransport {
        fn new(responses: HashMap<i32, Vec<Command>>) -> Self {
            Self {
                requests: Mutex::new(Vec::new()),
                responses: Mutex::new(responses),
            }
        }

        fn request(&self, index: usize) -> Command {
            self.requests.lock().unwrap()[index].clone()
        }
    }

    impl Transport for MockTransport {
        fn invoke<'s>(
            &'s self,
            _addr: &'s str,
            request: Command,
            _timeout: Duration,
        ) -> Pin<Box<dyn Future<Output = Result<Command>> + Send + 's>> {
            Box::pin(async move {
                self.requests.lock().unwrap().push(request.clone());
                let mut queues = self.responses.lock().unwrap();
                let queue = queues.get_mut(&request.code).ok_or_else(|| {
                    Error::Invalid(format!(
                        "no canned response for code {}",
                        request.code
                    ))
                })?;
                if queue.is_empty() {
                    return Err(Error::Invalid(format!(
                        "canned responses for code {} exhausted",
                        request.code
                    )));
                }
                Ok(queue.remove(0))
            })
        }

        fn send_oneway<'s>(
            &'s self,
            _addr: &'s str,
            request: Command,
        ) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 's>> {
            Box::pin(async move {
                self.requests.lock().unwrap().push(request);
                Ok(())
            })
        }
    }

    /// A response command whose code/ext fields stand in for a broker answer.
    fn response_stub(code: i32, exts: &[(&str, String)]) -> Command {
        let mut response = Command {
            code,
            ..Command::request(0)
        };
        response.flag = remoting::RESPONSE_FLAG;
        for (key, value) in exts {
            response = response.with_ext(*key, value.clone());
        }
        response
    }

    #[tokio::test]
    async fn producer_send_over_transport_maps_broker_response() {
        let mut props = KeyValue::new();
        props.put("accessPoints", "127.0.0.1:10911");
        props.put("rmq.producer.group", "oms-producer");
        let mut responses: HashMap<i32, Vec<Command>> = HashMap::new();
        // Ext fields mirror the success answer of `BrokerRuntime::send` (broker.rs `send`).
        responses.insert(
            remoting_codes::SEND_MESSAGE,
            vec![response_stub(
                0,
                &[
                    ("msgId", "0A0000000000000000000000C8".to_string()),
                    ("queueId", "2".to_string()),
                    ("queueOffset", "41".to_string()),
                    ("transactionId", String::new()),
                    ("MSG_REGION", "DefaultRegion".to_string()),
                    ("TRACE_ON", "false".to_string()),
                ],
            )],
        );
        let mock = Arc::new(MockTransport::new(responses));
        let producer = ProducerImpl::with_transport(props, mock.clone());

        let mut message = BytesMessage::new();
        message.set_body(b"hello-oms".to_vec());
        message.put_sys_headers(oms_keys::DESTINATION, "TopicA");
        message.put_user_headers(rmq_msg_keys::TAGS, "tagA");

        let result = producer.send(&message).await.expect("send");
        assert_eq!(result.message_id, "0A0000000000000000000000C8");
        assert_eq!(result.topic, "TopicA");
        assert_eq!(result.queue_id, 2);
        assert_eq!(result.queue_offset, 41);
        assert_eq!(result.region_id.as_deref(), Some("DefaultRegion"));

        // The request must match the broker's `send` ext field names exactly (wire compat).
        let request = mock.request(0);
        assert_eq!(request.code, remoting_codes::SEND_MESSAGE);
        assert_eq!(request.ext_fields.get("topic").map(String::as_str), Some("TopicA"));
        assert_eq!(
            request.ext_fields.get("producerGroup").map(String::as_str),
            Some("oms-producer")
        );
        assert_eq!(request.ext_fields.get("queueId").map(String::as_str), Some("-1"));
        assert_eq!(request.ext_fields.get("sysFlag").map(String::as_str), Some("0"));
        assert!(request.ext_fields.get("flag").unwrap().parse::<i32>().is_ok());
        assert!(
            request
                .ext_fields
                .get("bornTimestamp")
                .unwrap()
                .parse::<i64>()
                .is_ok()
        );
        let properties = request.ext_fields.get("properties").unwrap();
        assert!(properties.contains("TAGS\x01tagA\x02"));
        assert!(properties.contains("UNIQ_KEY\x01"));
        assert_eq!(request.body, b"hello-oms");
    }

    #[tokio::test]
    async fn pull_consumer_pulls_decodes_and_advances_offset() {
        let mut props = KeyValue::new();
        props.put("accessPoints", "127.0.0.1:10911");
        props.put("consumerId", "oms-group");

        // One stored message, encoded exactly as the broker serializes pull bodies.
        let mut stored = RocketMqMessage::default();
        stored.topic = "TopicA".to_string();
        stored.queue_id = 0;
        stored.queue_offset = 3;
        stored.body = b"m1".to_vec();
        stored
            .properties
            .insert(rmq_msg_keys::TAGS.to_string(), "tagA".to_string());
        stored.born_timestamp = now_millis();
        let body = stored.encode().expect("encode");

        let mut responses: HashMap<i32, Vec<Command>> = HashMap::new();
        // Nothing committed yet -> the broker answers 22 and consumption starts at 0.
        responses.insert(
            remoting_codes::QUERY_CONSUMER_OFFSET,
            vec![response_stub(remoting_codes::CONSUMER_OFFSET_NOT_EXIST, &[])],
        );
        // Ext fields mirror `pull_response` (broker.rs).
        responses.insert(
            remoting_codes::PULL_MESSAGE,
            vec![response_stub(
                remoting_codes::PULL_FOUND,
                &[
                    ("suggestWhichBrokerId", "0".to_string()),
                    ("nextBeginOffset", "4".to_string()),
                    ("minOffset", "0".to_string()),
                    ("maxOffset", "4".to_string()),
                    ("offsetDelta", "0".to_string()),
                ],
            )
            .with_body(body)],
        );
        responses.insert(
            remoting_codes::UPDATE_CONSUMER_OFFSET,
            vec![response_stub(0, &[])],
        );
        let mock = Arc::new(MockTransport::new(responses));

        let mut consumer =
            PullConsumerImpl::with_transport(props, mock.clone()).expect("consumer");
        consumer.attach_queue("TopicA");
        assert_eq!(consumer.pull_once("TopicA").await.expect("pull once"), 1);

        // The pulled record became an OMS message in the local cache.
        let oms = consumer.receive().expect("receive");
        assert_eq!(oms.body(), b"m1");
        assert_eq!(
            oms.user_headers.get(rmq_msg_keys::TAGS),
            Some("tagA")
        );
        assert_eq!(
            oms.sys_headers
                .get(oms_keys::DESTINATION),
            Some("TopicA")
        );

        // Wire order: offset query (code 14) -> pull (code 11) -> offset commit (code 15).
        let requests = mock.requests.lock().unwrap();
        assert_eq!(requests[0].code, remoting_codes::QUERY_CONSUMER_OFFSET);
        assert_eq!(requests[0].ext_fields.get("topic").map(String::as_str), Some("TopicA"));
        assert_eq!(
            requests[0].ext_fields.get("consumerGroup").map(String::as_str),
            Some("oms-group")
        );
        assert_eq!(requests[0].ext_fields.get("queueId").map(String::as_str), Some("0"));

        assert_eq!(requests[1].code, remoting_codes::PULL_MESSAGE);
        assert_eq!(requests[1].ext_fields.get("queueOffset").map(String::as_str), Some("0"));
        assert_eq!(requests[1].ext_fields.get("maxMsgNums").map(String::as_str), Some("32"));
        assert_eq!(requests[1].ext_fields.get("sysFlag").map(String::as_str), Some("4"));
        assert_eq!(
            requests[1].ext_fields.get("expressionType").map(String::as_str),
            Some("TAG")
        );
        assert_eq!(
            requests[1].ext_fields.get("subscription").map(String::as_str),
            Some("*")
        );

        assert_eq!(requests[2].code, remoting_codes::UPDATE_CONSUMER_OFFSET);
        assert_eq!(
            requests[2].ext_fields.get("commitOffset").map(String::as_str),
            Some("4")
        );
        drop(requests);

        // Client-side offset advanced to the broker's nextBeginOffset.
        let mq = MessageQueue::new("TopicA", "127.0.0.1:10911", 0);
        assert_eq!(consumer.cache().next_pull_offset(&mq), 4);
    }
}
