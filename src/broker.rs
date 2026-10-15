use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde::Deserialize;
use serde_json::json;
use tokio::sync::{Notify, watch};
use tracing::warn;

use crate::common::{
    Error, Message, Result, TopicConfig, decode_batch, decode_properties, now_millis,
};
use crate::filter::{self, Subscription};
use crate::remoting::{Command, Handler};
use crate::schedule::{
    self, BORN_TIMESTAMP, DELAY, DLQ_QUEUES, GroupConfig, PRODUCER_GROUP, RECALL_BODY, RECALL_TAG,
    RETRY_TOPIC, SCHEDULE_QUEUES, SCHEDULE_TOPIC, ScheduleError, TAGS, TIMER_DEL_UNIQKEY,
    TIMER_DELIVER_MS, TIMER_OUT_MS, TRACE_CONTEXT, TimerSettings, UNIQ_KEY, V3_4_9,
};
use crate::store::{FilterSnapshot, ReadResult, Store};

const MAX_MESSAGE_SIZE: usize = 4 * 1024 * 1024;
const MAX_PULL_BYTES: usize = 8 * 1024 * 1024;
/// `BrokerConfig.filterDataCleanTimeSpan`
const FILTER_DATA_KEEP_MS: i64 = 24 * 3600 * 1000;
/// `ScheduleMessageService` waits `FIRST_DELAY_TIME` before the first round, then
/// `DELAY_FOR_A_WHILE` between empty passes.
const SCHEDULE_POLL_MS: i64 = 100;
/// `MessageStoreConfig.flushDelayOffsetInterval`
const DELAY_OFFSET_PERSIST_MS: i64 = 10_000;
/// `TimerEnqueueGetService` waits `100L * precisionMs / 1000` when the timer queue is empty.
const TIMER_POLL_MS: i64 = 100;
/// `TimerMessageStore` sleeps `500L * precisionMs / 1000` between delivery retries.
const TIMER_RETRY_SLEEP_MS: u64 = 500;
/// `TimerMessageStore`: without `timerEnableRetryUntilSuccess` a delivery gives up after 3 tries.
const TIMER_PUT_RETRIES: usize = 3;
/// ConsumeQueue records one delivery round reads at a time. The reference iterates the queue
/// record by record and stops at the first record that is not due yet; reading a page is the
/// equivalent here because the cursor only advances over records that were delivered.
const SCHEDULE_BATCH: usize = 32;
/// `TopicValidator.GROUP_MAX_LENGTH`
const GROUP_MAX_LENGTH: usize = 120;

#[derive(Clone, Debug)]
pub struct BrokerConfig {
    pub broker_name: String,
    pub cluster_name: String,
    pub advertised_addr: SocketAddr,
    pub auto_create_topic: bool,
    pub default_queues: i32,
    pub enable_property_filter: bool,
    /// `BrokerConfig.autoCreateSubscriptionGroup`
    pub auto_create_group: bool,
    /// `BrokerConfig.rejectPullConsumerEnable`
    pub reject_pull_consumer: bool,
    /// `MessageStoreConfig.messageDelayLevel`
    pub message_delay_level: String,
    /// `MessageStoreConfig.timerWheelEnable`
    pub timer_wheel_enable: bool,
    /// `BrokerConfig.recallMessageEnable`
    pub recall_message_enable: bool,
}

impl Default for BrokerConfig {
    fn default() -> Self {
        Self {
            broker_name: "broker-a".into(),
            cluster_name: "DefaultCluster".into(),
            advertised_addr: SocketAddr::from(([127, 0, 0, 1], 10911)),
            auto_create_topic: true,
            default_queues: 8,
            enable_property_filter: false,
            auto_create_group: true,
            reject_pull_consumer: false,
            message_delay_level: schedule::DEFAULT_DELAY_LEVELS.into(),
            timer_wheel_enable: true,
            recall_message_enable: false,
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct HeartbeatSubscription {
    topic: String,
    sub_string: String,
    #[serde(default = "tag_expression")]
    expression_type: Option<String>,
    #[serde(default)]
    sub_version: i64,
}

fn tag_expression() -> Option<String> {
    Some("TAG".into())
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Consumer {
    group_name: String,
    /// `ConsumeType` name; `CONSUME_ACTIVELY` marks a pull consumer.
    #[serde(default)]
    consume_type: Option<String>,
    #[serde(default)]
    unit_mode: bool,
    #[serde(default)]
    subscription_data_set: Vec<HeartbeatSubscription>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Producer {
    group_name: String,
}

/// `CheckClientRequestBody`, of which only the subscription is ever inspected.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct CheckClientRequest {
    #[serde(default)]
    subscription_data: Option<HeartbeatSubscription>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Heartbeat {
    #[serde(rename = "clientID")]
    client_id: String,
    #[serde(default)]
    producer_data_set: Vec<Producer>,
    #[serde(default)]
    consumer_data_set: Vec<Consumer>,
    #[serde(default, rename = "isWithoutSub")]
    without_sub: bool,
    #[serde(default, rename = "heartbeatFingerprint")]
    fingerprint: i32,
}

struct Client {
    client_id: String,
    producers: BTreeSet<String>,
    consumers: Vec<Consumer>,
    last_seen: Instant,
}

/// `ConsumerFilterData`, minus the bloom bitmap the default configuration never calculates.
#[derive(Clone, Debug)]
struct FilterEntry {
    expression: String,
    expression_type: String,
    born_time: i64,
    dead_time: i64,
    client_version: i64,
    /// `None` mirrors a persisted expression that no longer compiles: the filter still exists
    /// but `ExpressionMessageFilter` treats a null compiled expression as a match.
    compiled: Option<Subscription>,
}

/// `ConsumerFilterManager`, keyed by (consumer group, topic) like its `subscriptionFilterData`.
#[derive(Default)]
struct FilterRegistry {
    entries: BTreeMap<(String, String), FilterEntry>,
    dirty: bool,
}

enum RegisterOutcome {
    Skip,
    Store(FilterEntry),
    /// A newer subscription whose expression no longer compiles evicts the previous filter.
    Evict,
}

impl FilterRegistry {
    fn get(&self, topic: &str, group: &str) -> Option<&FilterEntry> {
        self.entries.get(&(group.to_owned(), topic.to_owned()))
    }

    /// `ConsumerFilterManager.register(group, subscriptionList)` for one heartbeat.
    fn register_group(
        &mut self,
        known: &dyn Fn(&str) -> bool,
        group: &str,
        subs: &[HeartbeatSubscription],
    ) {
        for sub in subs {
            self.register_one(
                known(&sub.topic),
                group,
                &sub.topic,
                &sub.sub_string,
                sub.expression_type.as_deref().unwrap_or(""),
                sub.sub_version,
            );
        }
        let subscribed: BTreeSet<&str> = subs.iter().map(|sub| sub.topic.as_str()).collect();
        let now = now_millis();
        let mut dead = false;
        for ((entry_group, entry_topic), entry) in &mut self.entries {
            if entry_group == group
                && entry.dead_time == 0
                && !subscribed.contains(entry_topic.as_str())
            {
                entry.dead_time = now;
                dead = true;
            }
        }
        self.dirty |= dead;
    }

    /// `ConsumerFilterManager.register(topic, group, expression, type, clientVersion)`.
    fn register_one(
        &mut self,
        topic_known: bool,
        group: &str,
        topic: &str,
        expression: &str,
        expression_type: &str,
        client_version: i64,
    ) -> bool {
        if filter::is_tag_type(Some(expression_type)) || expression.is_empty() || !topic_known {
            return false;
        }
        let key = (group.to_owned(), topic.to_owned());
        let outcome = match self.entries.get(&key) {
            None => build_filter(expression, expression_type, client_version)
                .map_or(RegisterOutcome::Skip, RegisterOutcome::Store),
            Some(old) => {
                if client_version < old.client_version
                    || (client_version == old.client_version && old.dead_time == 0)
                {
                    RegisterOutcome::Skip
                } else if client_version > old.client_version
                    && (old.expression != expression || old.expression_type != expression_type)
                {
                    match build_filter(expression, expression_type, client_version) {
                        Some(entry) => RegisterOutcome::Store(entry),
                        None => RegisterOutcome::Evict,
                    }
                } else {
                    let mut current = old.clone();
                    current.client_version = client_version;
                    current.dead_time = 0;
                    RegisterOutcome::Store(current)
                }
            }
        };
        match outcome {
            RegisterOutcome::Skip => false,
            RegisterOutcome::Store(entry) => {
                self.entries.insert(key, entry);
                self.dirty = true;
                true
            }
            RegisterOutcome::Evict => {
                self.entries.remove(&key);
                self.dirty = true;
                false
            }
        }
    }

    /// `ConsumerFilterManager.unRegister`, invoked once a group loses every channel.
    fn mark_group_dead(&mut self, group: &str) {
        let now = now_millis();
        let mut dead = false;
        for ((entry_group, _), entry) in &mut self.entries {
            if entry_group == group && entry.dead_time == 0 {
                entry.dead_time = now;
                dead = true;
            }
        }
        self.dirty |= dead;
    }

    /// `ConsumerFilterManager.clean`, run from `encode` before every persist.
    fn clean(&mut self, keep_ms: i64) {
        let now = now_millis();
        let before = self.entries.len();
        self.entries
            .retain(|_, entry| entry.dead_time == 0 || now - entry.dead_time < keep_ms);
        self.dirty |= before != self.entries.len();
    }

    fn snapshots(&self) -> Vec<FilterSnapshot> {
        self.entries
            .iter()
            .map(|((group, topic), entry)| FilterSnapshot {
                topic: topic.clone(),
                consumer_group: group.clone(),
                expression: entry.expression.clone(),
                expression_type: entry.expression_type.clone(),
                born_time: entry.born_time,
                dead_time: entry.dead_time,
                client_version: entry.client_version,
            })
            .collect()
    }

    /// Restores `consumerFilter.json`; load time already marked every entry dead.
    fn hydrate(&mut self, store: &Store) -> Result<()> {
        for snapshot in store.consumer_filters()? {
            let compiled =
                Subscription::build(Some(&snapshot.expression_type), &snapshot.expression)
                    .ok()
                    .filter(|_| !filter::is_tag_type(Some(&snapshot.expression_type)));
            self.entries.insert(
                (snapshot.consumer_group, snapshot.topic),
                FilterEntry {
                    expression: snapshot.expression,
                    expression_type: snapshot.expression_type,
                    born_time: snapshot.born_time,
                    dead_time: snapshot.dead_time,
                    client_version: snapshot.client_version,
                    compiled,
                },
            );
        }
        self.dirty = false;
        Ok(())
    }
}

fn build_filter(
    expression: &str,
    expression_type: &str,
    client_version: i64,
) -> Option<FilterEntry> {
    let compiled = Subscription::build(Some(expression_type), expression).ok()?;
    Some(FilterEntry {
        expression: expression.to_owned(),
        expression_type: expression_type.to_owned(),
        born_time: now_millis(),
        dead_time: 0,
        client_version,
        compiled: Some(compiled),
    })
}

/// A pull subscription resolved the way `PullMessageProcessor` resolves it.
struct PullFilter {
    subscription: Subscription,
    expression_type: String,
}

impl PullFilter {
    fn is_tag(&self) -> bool {
        filter::is_tag_type(Some(&self.expression_type))
    }
}

/// `TimerMessageStore`'s wheel, indexed by the deadline its slot floors to.
///
/// The reference stores the wheel and its timer log separately; here the `rmq_sys_wheel_timer`
/// queue *is* the log, so replaying it from [`TimerCheckpoint::scan_offset`] rebuilds this index.
/// Because a deadline is kept as an absolute timestamp, an entry never has to be rolled forward
/// into a later turn of the wheel, which is why `TIMER_ROLL_TIMES` never appears on a delivery.
#[derive(Default)]
struct Wheel {
    checkpoint: schedule::TimerCheckpoint,
    slots: BTreeMap<i64, Vec<WheelEntry>>,
}

/// `TimerRequest`: one parked record, plus the scan time the reference reports as
/// `TIMER_ENQUEUE_MS` (`i64::MAX` for a record that went straight to delivery).
#[derive(Clone, Copy, Debug)]
struct WheelEntry {
    queue_offset: u64,
    enqueue_ms: i64,
}

pub struct Broker {
    store: Arc<Store>,
    config: BrokerConfig,
    clients: Mutex<BTreeMap<SocketAddr, Client>>,
    filters: Mutex<FilterRegistry>,
    group_heartbeats: Mutex<BTreeMap<String, i32>>,
    queue_counter: AtomicU64,
    messages: watch::Sender<u64>,
    pub topic_changes: Notify,
    started_at: i64,
    levels: schedule::DelayLevels,
    timer: TimerSettings,
    /// `ScheduleMessageService.offsetTable`, mirrored in memory and persisted every 10 seconds.
    delay_offsets: Mutex<schedule::DelayOffsets>,
    /// `TimerMessageStore`'s wheel and its queue cursor.
    wheel: Mutex<Wheel>,
    /// Stands in for the `RANDOM` draw `TimerMessageStore.isReject` makes in its middle band.
    reject_roll: AtomicU64,
}

struct Failure {
    code: i32,
    message: String,
    /// `PullMessageResponseHeader.forbiddenType`, set only on a NO_PERMISSION pull answer.
    forbidden_type: Option<i32>,
}

type BrokerResult<T> = std::result::Result<T, Failure>;

impl Failure {
    fn new(code: i32, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            forbidden_type: None,
        }
    }

    fn forbidden(mut self, forbidden_type: i32) -> Self {
        self.forbidden_type = Some(forbidden_type);
        self
    }

    /// `TimerMessageStore.doPut` answers `PUT_NO_RETRY` for `MESSAGE_ILLEGAL` and
    /// `PROPERTIES_SIZE_EXCEEDED`; those are the 13, 29 and 3 this server reports for a record no
    /// retry can fix, as opposed to the 1 an I/O or flow-control failure carries.
    fn retryable(&self) -> bool {
        !matches!(self.code, 3 | 13 | 29)
    }
}

impl From<Error> for Failure {
    fn from(error: Error) -> Self {
        let code = match error {
            Error::Invalid(_) | Error::Json(_) => 29,
            Error::Unsupported(_) => 3,
            Error::Io(_) => 1,
        };
        Self::new(code, error.to_string())
    }
}

pub fn validate_name(name: &str) -> Result<()> {
    if name.is_empty()
        || name.len() > 127
        || !name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"%_-|".contains(&b))
    {
        return Err(Error::Invalid(
            "name must be 1..127 ASCII letters, digits, %, _, - or |".into(),
        ));
    }
    Ok(())
}

/// `TopicValidator.checkTopic`: the broker builds retry and dead-letter topic names from a group,
/// so those may reach 255 bytes while every other topic is capped at 127.
pub fn validate_topic_name(name: &str) -> Result<()> {
    if schedule::is_retry_topic(name) || name.starts_with(schedule::DLQ_PREFIX) {
        if name.len() > 255
            || !name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"%_-|".contains(&b))
        {
            return Err(Error::Invalid(
                "retry or dead-letter topic name is too long or illegal".into(),
            ));
        }
        return Ok(());
    }
    validate_name(name)
}

/// `TopicValidator.validateGroup`: illegal characters are reported before the 120 character
/// limit groups carry, which is shorter than the 127 a plain topic name may reach.
pub fn validate_group(name: &str) -> Result<()> {
    if name.is_empty() {
        return Err(Error::Invalid("The specified group is blank.".into()));
    }
    if !name
        .bytes()
        .all(|b| b.is_ascii_alphanumeric() || b"%_-|".contains(&b))
    {
        return Err(Error::Invalid(format!(
            "The specified group: {name}, contains illegal characters, allowing only ^[%|a-zA-Z0-9_-]+$"
        )));
    }
    if name.len() > GROUP_MAX_LENGTH {
        return Err(Error::Invalid(format!(
            "The specified group: {name}, is longer than group max length: {GROUP_MAX_LENGTH}"
        )));
    }
    Ok(())
}

impl Broker {
    pub fn new(store: Arc<Store>, config: BrokerConfig) -> Result<Self> {
        validate_name(&config.broker_name)?;
        validate_name(&config.cluster_name)?;
        if !(1..=1024).contains(&config.default_queues)
            || config.advertised_addr.ip().is_unspecified()
            || config.advertised_addr.port() == 0
        {
            return Err(Error::Invalid(
                "invalid broker address or default queue count".into(),
            ));
        }
        // `ScheduleMessageService.load` fails the broker start on a malformed level table.
        let levels = schedule::DelayLevels::parse(&config.message_delay_level)?;
        // `MessageStoreConfig.timerWheelEnable` defaults to true, so accurate timer messages are
        // served unless the operator turns it off; a disabled wheel rejects them like the reference.
        let timer = TimerSettings {
            wheel_enable: config.timer_wheel_enable,
            ..Default::default()
        };
        if config.auto_create_topic && !store.topics()?.contains_key("TBW102") {
            let mut topic = TopicConfig::new("TBW102", config.default_queues);
            topic.perm = 7;
            store.put_topic(topic)?;
        }
        // `TopicConfigManager.init` registers the internal topics the schedule engine writes to.
        for (topic, queues) in schedule_system_topics(&levels, timer.wheel_enable) {
            if !store.topics()?.contains_key(topic) {
                let mut config = TopicConfig::new(topic, queues);
                config.perm = 7;
                store.put_topic(config)?;
            }
        }
        let mut delay_offsets = store.delay_offsets()?;
        correct_delay_offsets(&store, &levels, &mut delay_offsets);
        let mut checkpoint = store.timer_checkpoint()?;
        checkpoint.scan_offset = checkpoint
            .scan_offset
            .min(store.queue_max_offset(schedule::TIMER_TOPIC, 0));
        let (messages, _) = watch::channel(0);
        let mut filters = FilterRegistry::default();
        filters.hydrate(&store)?;
        Ok(Self {
            store,
            config,
            clients: Mutex::new(BTreeMap::new()),
            filters: Mutex::new(filters),
            group_heartbeats: Mutex::new(BTreeMap::new()),
            queue_counter: AtomicU64::new(0),
            messages,
            topic_changes: Notify::new(),
            started_at: now_millis(),
            levels,
            timer,
            delay_offsets: Mutex::new(delay_offsets),
            wheel: Mutex::new(Wheel {
                checkpoint,
                slots: BTreeMap::new(),
            }),
            reject_roll: AtomicU64::new(0),
        })
    }

    pub fn store(&self) -> &Arc<Store> {
        &self.store
    }
    pub fn config(&self) -> &BrokerConfig {
        &self.config
    }

    pub fn registration(&self) -> Result<Command> {
        let timestamp = now_millis();
        Ok(Command::request(103)
            .with_ext("brokerName", &self.config.broker_name)
            .with_ext("brokerAddr", self.config.advertised_addr)
            .with_ext("clusterName", &self.config.cluster_name)
            .with_ext("brokerId", 0)
            .with_ext("haServerAddr", "")
            .with_ext("compressed", false)
            .with_ext("bodyCrc32", 0)
            .with_body(serde_json::to_vec(&json!({
                "topicConfigSerializeWrapper": {
                    "dataVersion": {"timestamp": timestamp, "counter": timestamp, "stateVersion": 0},
                    "topicConfigTable": self.store.topics()?,
                    "topicQueueMappingInfoMap": {}
                }, "filterServerList": []
            }))?))
    }

    pub fn unregistration(&self) -> Command {
        Command::request(104)
            .with_ext("brokerName", &self.config.broker_name)
            .with_ext("brokerAddr", self.config.advertised_addr)
            .with_ext("clusterName", &self.config.cluster_name)
            .with_ext("brokerId", 0)
    }

    pub fn expire_clients(&self) {
        let before: BTreeSet<String> = self
            .clients
            .lock()
            .unwrap()
            .values()
            .flat_map(|client| client.consumers.iter().map(|c| c.group_name.clone()))
            .collect();
        self.clients
            .lock()
            .unwrap()
            .retain(|_, c| c.last_seen.elapsed() < Duration::from_secs(120));
        self.expire_groups(&before);
    }

    async fn storage<T, F>(&self, operation: F) -> Result<T>
    where
        T: Send + 'static,
        F: FnOnce(&Store) -> Result<T> + Send + 'static,
    {
        let store = self.store.clone();
        tokio::task::spawn_blocking(move || operation(&store))
            .await
            .map_err(|e| Error::Io(std::io::Error::other(e)))?
    }

    async fn send(&self, request: &Command, peer: SocketAddr) -> BrokerResult<Command> {
        let v2 = request.code != 10;
        let key = |long, short| if v2 { short } else { long };
        let topic = request.required(key("topic", "b"))?.to_owned();
        validate_topic_name(&topic)?;
        let group = request.required(key("producerGroup", "a"))?;
        validate_name(group)?;
        if topic == "TBW102" {
            return Err(Failure::new(
                16,
                "cannot send to the default topic template",
            ));
        }
        let sys_flag: i32 = request.field(key("sysFlag", "f"))?;
        if sys_flag & (0x0c | 0xc0) != 0 {
            return Err(Failure::new(
                3,
                "transactional and inner-batch messages are not implemented",
            ));
        }
        let properties = decode_properties(
            request
                .ext_fields
                .get(key("properties", "i"))
                .map_or("", String::as_str),
        )?;
        reject_unsupported_properties(&properties)?;
        let flag: i32 = request.field(key("flag", "h"))?;
        let born_timestamp: i64 = request.field(key("bornTimestamp", "g"))?;
        let reconsume_times = optional_field(request, key("reconsumeTimes", "j"), 0i32)?;
        if reconsume_times < 0 {
            return Err(Failure::new(13, "negative reconsumeTimes"));
        }
        if request.body.is_empty() || request.body.len() > MAX_MESSAGE_SIZE {
            return Err(Failure::new(13, "message body must be 1..4194304 bytes"));
        }
        let batch_flag = optional_field(request, key("batch", "m"), false)?;
        if request.code == 320 && !batch_flag {
            return Err(Failure::new(13, "SEND_BATCH_MESSAGE requires batch=true"));
        }
        if batch_flag && sys_flag & 1 != 0 {
            return Err(Failure::new(
                3,
                "compressed batch envelope is not supported",
            ));
        }
        let entries = if batch_flag {
            decode_batch(&request.body)?
        } else {
            vec![(flag, request.body.clone(), BTreeMap::new())]
        };
        if entries.is_empty() || entries.len() > 4096 {
            return Err(Failure::new(13, "batch must contain 1..4096 messages"));
        }
        let template = Message {
            topic: topic.clone(),
            queue_id: 0,
            sys_flag,
            flag,
            born_timestamp,
            born_host: peer,
            store_host: self.config.advertised_addr,
            reconsume_times,
            properties,
            ..Message::default()
        };
        let mut messages = Vec::with_capacity(entries.len());
        for (entry_flag, body, entry_properties) in entries {
            if body.is_empty() {
                return Err(Failure::new(13, "empty message in batch"));
            }
            let mut message = template.clone();
            message.flag = entry_flag;
            message.body = body;
            message.properties.extend(entry_properties);
            reject_unsupported_properties(&message.properties)?;
            message.encode()?;
            messages.push(message);
        }
        let requested_queue: i32 = request.field(key("queueId", "e"))?;
        let configs = self.store.topics()?;
        let config = match configs.get(&topic) {
            Some(config) => config.clone(),
            None => {
                if !self.config.auto_create_topic {
                    return Err(Failure::new(17, "topic does not exist"));
                }
                let default_topic = request.required(key("defaultTopic", "c"))?;
                let base = configs
                    .get(default_topic)
                    .ok_or_else(|| Failure::new(17, "default topic does not exist"))?;
                if base.perm & 1 == 0 {
                    return Err(Failure::new(16, "default topic is not inheritable"));
                }
                let queues: i32 = request.field(key("defaultTopicQueueNums", "d"))?;
                if queues <= 0 {
                    return Err(Failure::new(13, "invalid default queue count"));
                }
                let mut config = TopicConfig::new(&topic, queues.min(base.write_queue_nums));
                config.perm = base.perm & !1;
                config
            }
        };
        if config.perm & 2 == 0 {
            return Err(Failure::new(16, "topic is not writable"));
        }
        let queue_id = if requested_queue < 0 {
            (self.queue_counter.fetch_add(1, Ordering::Relaxed) % config.write_queue_nums as u64)
                as i32
        } else {
            requested_queue
        };
        if queue_id >= config.write_queue_nums {
            return Err(Failure::new(13, "queueId out of range"));
        }
        for message in &mut messages {
            message.queue_id = queue_id;
        }
        self.redirect_expired_retry(request, &topic, &mut messages)
            .await?;
        // `CommitLog.asyncPutMessages` refuses a batch that carries a delay level, and
        // `ScheduleMessageHook` parks the rest in the schedule queue instead of the real topic.
        if batch_flag {
            let level = schedule::delay_level(&template).map_err(Failure::from)?;
            if level > 0 {
                return Err(Failure::new(
                    13,
                    "a batch message cannot carry a delay level",
                ));
            }
        } else {
            for message in &mut messages {
                schedule::handle_schedule_message(
                    message,
                    &self.levels,
                    &self.timer,
                    now_millis(),
                    |deliver_ms| self.is_timer_reject(deliver_ms),
                )
                .map_err(|error| self.schedule_failure(&error))?;
            }
        }
        let creating = !configs.contains_key(&topic);
        let saved = self
            .storage(move |store| {
                if creating && !store.topics()?.contains_key(&topic) {
                    store.put_topic(config)?;
                }
                store.append_batch(messages)
            })
            .await?;
        if creating {
            self.topic_changes.notify_one();
        }
        self.messages
            .send_modify(|generation| *generation = generation.wrapping_add(1));
        let ids = saved
            .iter()
            .map(Message::message_id)
            .collect::<Vec<_>>()
            .join(",");
        let response = Command::response(request, 0)
            .with_ext("msgId", ids)
            .with_ext("queueId", queue_id)
            .with_ext("queueOffset", saved[0].queue_offset)
            .with_ext("transactionId", "")
            .with_ext("MSG_REGION", "DefaultRegion")
            .with_ext("TRACE_ON", false);
        // `attachRecallHandle` only names a message the wheel is going to release, so a producer
        // can cancel it before that happens.
        Ok(match self.recall_handle(request, &saved[0]) {
            Some(handle) => response.with_ext("recallHandle", handle),
            None => response,
        })
    }

    /// `SendMessageProcessor.attachRecallHandle`: skipped for a batch or a send-back, and for
    /// anything the wheel did not park. The handle's timestamp is one millisecond past the slot,
    /// which the transform floors back onto that same slot.
    fn recall_handle(&self, request: &Command, message: &Message) -> Option<String> {
        if request.code != 10 && request.code != 310 {
            return None;
        }
        let deliver_ms = message.properties.get(TIMER_OUT_MS)?.parse::<i64>().ok()?;
        let real_topic = message.properties.get(schedule::REAL_TOPIC)?;
        if real_topic.starts_with(schedule::RETRY_PREFIX) {
            return None;
        }
        // `MessageClientIDSetter.getUniqID` is null without the property, and the reference would
        // hand out a handle that recalls nothing.
        let uniq_key = message.properties.get(UNIQ_KEY)?;
        Some(schedule::build_recall_handle(
            real_topic,
            &self.config.broker_name,
            &(deliver_ms + 1).to_string(),
            uniq_key,
        ))
    }

    /// `TimerMessageStore.isReject` for the slot a new deadline would land in.
    fn is_timer_reject(&self, deliver_ms: i64) -> bool {
        let congest_num = self
            .wheel
            .lock()
            .unwrap()
            .slots
            .get(&deliver_ms)
            .map_or(0, |entries| entries.len() as i64);
        let roll = self.reject_roll.fetch_add(1, Ordering::Relaxed) % 1_000;
        schedule::is_reject(congest_num, self.timer.congest_num_each_slot, roll as i64)
    }

    /// `SendMessageProcessor.handlePutMessageResult`, whose remark quotes the broker's own settings.
    fn schedule_failure(&self, error: &ScheduleError) -> Failure {
        Failure::new(error.code(), error.remark(&self.timer))
    }

    /// `SendMessageProcessor.handleRetryAndDLQ`: a producer that publishes straight to a retry
    /// topic past the group's retry limit is redirected to that group's dead-letter topic.
    ///
    /// The reference redirects the same way while the group still holds an unexpired queue lock;
    /// that branch arrives with the ordered-message lock manager.
    async fn redirect_expired_retry(
        &self,
        request: &Command,
        topic: &str,
        messages: &mut [Message],
    ) -> BrokerResult<()> {
        if !schedule::is_retry_topic(topic) {
            return Ok(());
        }
        let group = schedule::parse_group(topic);
        let config = match self.group_config(group).await? {
            Some(config) => config,
            None => {
                return Err(Failure::new(
                    26,
                    format!("subscription group not exist, {group}"),
                ));
            }
        };
        let mut max_reconsume_times = config.retry_max_times;
        if request.version >= V3_4_9 {
            let key = if request.code == 10 {
                "maxReconsumeTimes"
            } else {
                "l"
            };
            if let Some(times) = optional_i32(request, key)? {
                max_reconsume_times = times;
            }
        }
        let reconsume_times = messages
            .first()
            .map_or(0, |message| message.reconsume_times);
        if reconsume_times <= max_reconsume_times {
            return Ok(());
        }
        let dlq = schedule::dlq_topic(group);
        if self
            .ensure_back_topic(&dlq, DLQ_QUEUES, 6, 0, false)
            .await?
            .is_none()
        {
            return Err(Failure::new(1, format!("topic[{dlq}] not exist")));
        }
        for message in messages {
            message.topic = dlq.clone();
            message.queue_id = 0;
            // `setDelayTimeLevel(0)` runs after the "-1" is put into the properties, so the
            // stored record reads as an ordinary message.
            message.properties.insert(DELAY.into(), "0".into());
        }
        Ok(())
    }

    fn subscription(
        &self,
        request: &Command,
        group: &str,
        topic: &str,
        sys_flag: i32,
    ) -> BrokerResult<PullFilter> {
        if sys_flag & 8 != 0 {
            return Err(Failure::new(3, "class filters are not supported"));
        }
        let client_version = optional_field(request, "subVersion", 0i64)?;
        if sys_flag & 4 != 0 {
            let expression_type = request
                .ext_fields
                .get("expressionType")
                .cloned()
                .unwrap_or_default();
            let subscription = request.required("subscription")?;
            // `FilterAPI.build` only rejects an empty non-tag expression; compiling happens in
            // `ConsumerFilterManager.build`, whose null result leaves the filter matching all.
            let compiled = Subscription::build(Some(&expression_type), subscription);
            if filter::is_tag_type(Some(&expression_type)) {
                return Ok(PullFilter {
                    subscription: compiled.map_err(|_| {
                        Failure::new(23, "parse the consumer's subscription failed")
                    })?,
                    expression_type,
                });
            }
            if subscription.is_empty() {
                return Err(Failure::new(23, "parse the consumer's subscription failed"));
            }
            return Ok(PullFilter {
                subscription: compiled.unwrap_or(Subscription::All),
                expression_type,
            });
        }

        let stored = {
            let clients = self.clients.lock().unwrap();
            clients
                .values()
                .flat_map(|c| &c.consumers)
                .filter(|c| c.group_name == group)
                .flat_map(|c| &c.subscription_data_set)
                .filter(|s| s.topic == topic)
                .max_by_key(|s| s.sub_version)
                .cloned()
                .ok_or_else(|| Failure::new(24, "the consumer's subscription does not exist"))?
        };
        if stored.sub_version < client_version {
            return Err(Failure::new(
                25,
                "the consumer's subscription is not latest",
            ));
        }
        let expression_type = stored.expression_type.unwrap_or_default();
        if filter::is_tag_type(Some(&expression_type)) {
            return Ok(PullFilter {
                subscription: Subscription::build(Some(&expression_type), &stored.sub_string)
                    .map_err(|_| Failure::new(23, "parse the consumer's subscription failed"))?,
                expression_type,
            });
        }
        let entry = self
            .filters
            .lock()
            .unwrap()
            .get(topic, group)
            .cloned()
            .ok_or_else(|| {
                Failure::new(
                    27,
                    "the broker's consumer filter data does not exist, the expression may be wrong",
                )
            })?;
        if entry.client_version < client_version {
            return Err(Failure::new(
                28,
                "the broker's consumer filter data is not latest",
            ));
        }
        Ok(PullFilter {
            subscription: entry.compiled.unwrap_or(Subscription::All),
            expression_type,
        })
    }

    async fn pull(&self, request: &Command) -> BrokerResult<Command> {
        let group = request.required("consumerGroup")?.to_owned();
        let topic = request.required("topic")?.to_owned();
        validate_topic_name(&topic)?;
        let queue_id: i32 = request.field("queueId")?;
        let offset: u64 = request.field("queueOffset")?;
        let max_nums: usize = request.field("maxMsgNums")?;
        if max_nums == 0 || max_nums > 4096 {
            return Err(Failure::new(29, "maxMsgNums must be 1..4096"));
        }
        // `PullMessageProcessor.processRequest` checks the group before the topic.
        let group_config = match self.group_config(&group).await? {
            Some(config) => config,
            None => {
                return Err(Failure::new(
                    26,
                    format!("subscription group [{group}] does not exist"),
                ));
            }
        };
        if !group_config.consume_enable {
            return Err(
                Failure::new(16, format!("subscription group no permission, {group}")).forbidden(2),
            );
        }
        let config = self
            .store
            .topics()?
            .get(&topic)
            .cloned()
            .ok_or_else(|| Failure::new(17, "topic does not exist"))?;
        if config.perm & 4 == 0 {
            return Err(Failure::new(
                16,
                format!("the topic[{topic}] pulling message is forbidden"),
            )
            .forbidden(3));
        }
        if queue_id < 0 || queue_id >= config.read_queue_nums {
            return Err(Failure::new(29, "queueId out of range"));
        }
        let sys_flag: i32 = request.field("sysFlag")?;
        if sys_flag & 16 != 0 || request.ext_fields.contains_key("liteTopic") {
            return Err(Failure::new(3, "Lite message pulling is not implemented"));
        }
        let filter = self.subscription(request, &group, &topic, sys_flag)?;
        if !filter.is_tag() && !self.config.enable_property_filter {
            return Err(Failure::new(
                1,
                format!(
                    "the broker does not support consumer to filter message by {}",
                    filter.expression_type
                ),
            ));
        }
        let max_bytes = optional_field(request, "maxMsgBytes", MAX_PULL_BYTES)?.min(MAX_PULL_BYTES);
        if max_bytes == 0 {
            return Err(Failure::new(29, "maxMsgBytes must be positive"));
        }
        if sys_flag & 1 != 0 {
            let committed: u64 = request.field("commitOffset")?;
            let topic = topic.clone();
            self.storage(move |store| store.commit_offset(&group, &topic, queue_id, committed))
                .await?;
        }
        let suspend = if sys_flag & 2 != 0 {
            Duration::from_millis(
                optional_field(request, "suspendTimeoutMillis", 0u64)?.min(30_000),
            )
        } else {
            Duration::ZERO
        };
        let deadline = tokio::time::Instant::now() + suspend;
        let mut updates = self.messages.subscribe();
        loop {
            let topic = topic.clone();
            let subscription = filter.subscription.clone();
            let read = self
                .storage(move |store| {
                    store.read(
                        &topic,
                        queue_id,
                        offset,
                        max_nums,
                        max_bytes,
                        Some(&subscription),
                    )
                })
                .await?;
            if !read.messages.is_empty() {
                return pull_response(request, 0, read);
            }
            if offset > read.max_offset || offset < read.min_offset {
                return pull_response(request, 21, read);
            }
            if read.next_begin_offset > offset {
                return pull_response(request, 20, read);
            }
            if suspend.is_zero()
                || tokio::time::timeout_at(deadline, updates.changed())
                    .await
                    .is_err()
            {
                return pull_response(request, 19, read);
            }
        }
    }

    async fn heartbeat(&self, request: &Command, peer: SocketAddr) -> BrokerResult<Command> {
        let heartbeat: Heartbeat = serde_json::from_slice(&request.body).map_err(Error::from)?;
        if heartbeat.client_id.is_empty() || heartbeat.client_id.len() > 512 {
            return Err(Failure::new(29, "invalid clientID"));
        }
        for producer in &heartbeat.producer_data_set {
            validate_name(&producer.group_name)?;
        }
        for consumer in &heartbeat.consumer_data_set {
            for subscription in &consumer.subscription_data_set {
                validate_name(&subscription.topic)?;
            }
        }
        let configs = self.store.topics()?;
        let mut consumers = Vec::with_capacity(heartbeat.consumer_data_set.len());
        let mut groups = Vec::with_capacity(heartbeat.consumer_data_set.len());
        for consumer in heartbeat.consumer_data_set {
            if self.config.reject_pull_consumer
                && consumer.consume_type.as_deref() == Some("CONSUME_ACTIVELY")
            {
                continue;
            }
            groups.push(consumer.group_name.clone());
            // `ClientManageProcessor.heartBeat` drops the whole consumer when its group has no
            // configuration, so the retry topic is never created and the group never registers.
            let Some(group) = self.group_config(&consumer.group_name).await? else {
                continue;
            };
            let order = consumer.subscription_data_set.iter().any(|subscription| {
                configs
                    .get(&subscription.topic)
                    .is_some_and(|config| config.order)
            });
            let sys_flag = if consumer.unit_mode { 2 } else { 0 };
            let retry = schedule::retry_topic(&consumer.group_name);
            // This store refuses a topic without queues, unlike a broker configured with
            // `retryQueueNums = 0`.
            self.ensure_back_topic(&retry, group.retry_queue_nums.max(1), 6, sys_flag, order)
                .await?;
            consumers.push(consumer);
        }
        let mut clients = self.clients.lock().unwrap();
        if heartbeat.without_sub {
            let existing = clients
                .get(&peer)
                .ok_or_else(|| Failure::new(24, "full heartbeat is required"))?;
            for consumer in &mut consumers {
                let old = existing
                    .consumers
                    .iter()
                    .find(|c| c.group_name == consumer.group_name)
                    .ok_or_else(|| Failure::new(24, "full heartbeat is required for new group"))?;
                consumer.subscription_data_set = old.subscription_data_set.clone();
            }
        }
        // `ConsumerManager.registerConsumerWithoutSub` never reaches the filter manager.
        let registered: Vec<Consumer> = if heartbeat.without_sub {
            Vec::new()
        } else {
            consumers.clone()
        };
        clients.insert(
            peer,
            Client {
                client_id: heartbeat.client_id,
                producers: heartbeat
                    .producer_data_set
                    .into_iter()
                    .map(|p| p.group_name)
                    .collect(),
                consumers,
                last_seen: Instant::now(),
            },
        );
        drop(clients);

        let mut sub_change = true;
        if heartbeat.fingerprint != 0 {
            // `heartBeatV2` reports a subscription change only for groups it has seen before.
            let mut previous = self.group_heartbeats.lock().unwrap();
            sub_change = false;
            for group in &groups {
                let seen = previous.insert(group.clone(), heartbeat.fingerprint);
                sub_change |= seen.is_some_and(|old| old != heartbeat.fingerprint);
            }
        }
        self.refresh_filters(&registered)?;

        Ok(Command::response(request, 0)
            .with_ext("IS_SUPPORT_HEART_BEAT_V2", "true")
            .with_ext("IS_SUB_CHANGE", sub_change.to_string()))
    }

    /// `DefaultConsumerIdsChangeListener` REGISTER: the whole subscription list of a group.
    fn refresh_filters(&self, consumers: &[Consumer]) -> Result<()> {
        if consumers.is_empty() {
            return Ok(());
        }
        let configs = self.store.topics()?;
        let mut filters = self.filters.lock().unwrap();
        for consumer in consumers {
            filters.register_group(
                &|topic| configs.contains_key(topic),
                &consumer.group_name,
                &consumer.subscription_data_set,
            );
        }
        Ok(())
    }

    /// Mirrors the scheduled `ConsumerFilterManager.persist`, including its `clean` step.
    pub async fn persist_filters(&self) -> Result<()> {
        let snapshots = {
            let mut filters = self.filters.lock().unwrap();
            filters.clean(FILTER_DATA_KEEP_MS);
            if !filters.dirty {
                return Ok(());
            }
            filters.snapshots()
        };
        let stored = self
            .storage(move |store| store.put_consumer_filters(&snapshots))
            .await;
        if stored.is_err() {
            self.filters.lock().unwrap().dirty = true;
        }
        stored
    }

    /// Marks filters of groups that lost their last channel dead, like the UNREGISTER event.
    fn expire_groups(&self, alive_before: &BTreeSet<String>) {
        let clients = self.clients.lock().unwrap();
        let alive: BTreeSet<String> = clients
            .values()
            .flat_map(|client| client.consumers.iter().map(|c| c.group_name.clone()))
            .collect();
        drop(clients);
        let mut filters = self.filters.lock().unwrap();
        for group in alive_before.difference(&alive) {
            filters.mark_group_dead(group);
        }
    }

    fn delay_offset(&self, level: i32) -> u64 {
        self.delay_offsets
            .lock()
            .unwrap()
            .offset_table
            .get(&level)
            .copied()
            .unwrap_or(0)
    }

    /// `ScheduleMessageService.updateOffset`, which only changes the in-memory cursor.
    fn set_delay_offset(&self, level: i32, offset: u64) {
        self.delay_offsets
            .lock()
            .unwrap()
            .offset_table
            .insert(level, offset);
    }

    /// `ScheduleMessageService.persist`, driven by the reference's 10 second task.
    pub async fn persist_delay_offsets(&self) -> Result<()> {
        let offsets = self.delay_offsets.lock().unwrap().clone();
        if offsets == self.store.delay_offsets()? {
            return Ok(());
        }
        self.storage(move |store| store.put_delay_offsets(&offsets))
            .await
    }

    /// `MessageStore.putMessage` for records this broker produced itself. The schedule hook runs
    /// again, exactly like the reference's put hooks, so a retried message with a delay level is
    /// parked in the schedule queue instead of landing on `%RETRY%` directly.
    async fn put_internal(&self, message: Message) -> BrokerResult<Message> {
        let mut message = message;
        schedule::handle_schedule_message(
            &mut message,
            &self.levels,
            &self.timer,
            now_millis(),
            |deliver_ms| self.is_timer_reject(deliver_ms),
        )
        .map_err(|error| self.schedule_failure(&error))?;
        let saved = self
            .storage(move |store| store.append_internal(vec![message]))
            .await?
            .remove(0);
        self.messages
            .send_modify(|generation| *generation = generation.wrapping_add(1));
        Ok(saved)
    }

    /// One level of `DeliverDelayedMessageTimerTask.executeOnTimeUp`; `wake` is the earliest
    /// deadline the caller should wait for, so a full pass costs one timer per broker.
    async fn deliver_level(&self, level: i32, wake: &mut i64) -> BrokerResult<usize> {
        let queue = schedule::delay_level_to_queue_id(level);
        let mut delivered = 0usize;
        loop {
            let offset = self.delay_offset(level);
            let read = self
                .storage(move |store| {
                    store.read(
                        SCHEDULE_TOPIC,
                        queue,
                        offset,
                        SCHEDULE_BATCH,
                        MAX_PULL_BYTES,
                        None,
                    )
                })
                .await?;
            if read.messages.is_empty() {
                *wake = (*wake).min(SCHEDULE_POLL_MS);
                return Ok(delivered);
            }
            let count = read.messages.len();
            for message in read.messages {
                let now = now_millis();
                let due = self
                    .levels
                    .deliver_timestamp(level, message.store_timestamp);
                let due = self.levels.correct_deliver_timestamp(now, level, due);
                if due > now {
                    // The cursor stays on this record; the next pass resumes exactly here.
                    *wake = (*wake).min(due - now);
                    return Ok(delivered);
                }
                match schedule::message_time_up(&message).map_err(Failure::from)? {
                    Some(restored) => {
                        self.put_internal(restored).await?;
                        delivered += 1;
                    }
                    // `transformDelayLevelMessage` always records a real topic, so a record
                    // without one is corrupt; skip it rather than stall the whole level.
                    None => warn!(
                        level,
                        queue_offset = message.queue_offset,
                        "schedule record has no real topic, dropping it"
                    ),
                }
                self.set_delay_offset(level, message.queue_offset + 1);
            }
            if count < SCHEDULE_BATCH {
                *wake = (*wake).min(SCHEDULE_POLL_MS);
                return Ok(delivered);
            }
        }
    }

    /// `TimerEnqueueGetService` and `TimerEnqueuePutService`: replay the timer queue from the
    /// checkpoint into the wheel. A record whose slot the write head already passed is delivered
    /// at once, exactly like the reference's `Long.MAX_VALUE` enqueue time.
    async fn scan_wheel(&self, wake: &mut i64) -> BrokerResult<usize> {
        let mut delivered = 0usize;
        loop {
            let offset = self.wheel.lock().unwrap().checkpoint.scan_offset;
            let read = self
                .storage(move |store| {
                    store.read(
                        schedule::TIMER_TOPIC,
                        0,
                        offset,
                        SCHEDULE_BATCH,
                        MAX_PULL_BYTES,
                        None,
                    )
                })
                .await?;
            let count = read.messages.len();
            if count == 0 {
                *wake = (*wake).min(TIMER_POLL_MS);
                return Ok(delivered);
            }
            let now = now_millis();
            let mut immediate = Vec::new();
            {
                let mut wheel = self.wheel.lock().unwrap();
                for message in read.messages {
                    wheel.checkpoint.scan_offset = message.queue_offset + 1;
                    let entry = WheelEntry {
                        queue_offset: message.queue_offset,
                        enqueue_ms: now,
                    };
                    let Some(deliver_ms) = message
                        .properties
                        .get(TIMER_OUT_MS)
                        .and_then(|value| value.parse::<i64>().ok())
                    else {
                        // `TimerEnqueueGetService` moves on when `TIMER_OUT_MS` will not parse;
                        // `timerSkipUnknownError` is left at its default of true.
                        warn!(
                            queue_offset = message.queue_offset,
                            "timer record has no deliver time, skipping it"
                        );
                        continue;
                    };
                    if self.timer.drain_ms(deliver_ms) <= now {
                        if message.properties.contains_key(TIMER_DEL_UNIQKEY) {
                            // `doPut` answers `PUT_NO_RETRY` for a delete record that is already
                            // due, so nothing is recalled and nothing is delivered.
                            continue;
                        }
                        if self.timer.is_too_old(deliver_ms, now) {
                            warn!(
                                deliver_ms,
                                "timer record is past the wheel span, delivering it late"
                            );
                        }
                        immediate.push((entry, message));
                    } else {
                        wheel.slots.entry(deliver_ms).or_default().push(entry);
                    }
                }
            }
            for (entry, message) in immediate {
                if self
                    .deliver_parked(message, i64::MAX, entry.enqueue_ms)
                    .await?
                {
                    delivered += 1;
                }
            }
            if count < SCHEDULE_BATCH {
                *wake = (*wake).min(TIMER_POLL_MS);
                return Ok(delivered);
            }
        }
    }

    /// `TimerDequeueGetService.dequeue`: drain every slot the read head has passed, oldest first.
    async fn deliver_due(&self, wake: &mut i64) -> BrokerResult<usize> {
        let now = now_millis();
        let mut delivered = 0usize;
        for (deliver_ms, entries) in self.take_due_slots(now) {
            // The slot's records are not contiguous in the queue, so each one is read back by its
            // own offset, which is what following the reference's linked list amounts to.
            let mut deletes = BTreeSet::new();
            let mut parked = Vec::with_capacity(entries.len());
            for entry in entries {
                let read = self
                    .storage(move |store| {
                        store.read(
                            schedule::TIMER_TOPIC,
                            0,
                            entry.queue_offset,
                            1,
                            MAX_PULL_BYTES,
                            None,
                        )
                    })
                    .await?;
                let Some(message) = read.messages.into_iter().next() else {
                    // `dequeue_get_msg_miss`: the record is gone, so the slot simply moves on.
                    warn!(
                        deliver_ms,
                        queue_offset = entry.queue_offset,
                        "timer record is missing from the queue"
                    );
                    continue;
                };
                // Deletes are collected before any normal record is offered, which is what the
                // reference's separate latch over `deleteMsgStack` guarantees.
                match message.properties.get(TIMER_DEL_UNIQKEY) {
                    Some(key) => {
                        deletes.insert(key.clone());
                    }
                    None => parked.push((entry, message)),
                }
            }
            for (entry, message) in parked {
                let cancelled = message
                    .properties
                    .get(UNIQ_KEY)
                    .is_some_and(|key| deletes.contains(key));
                if cancelled {
                    continue;
                }
                if self.deliver_parked(message, entry.enqueue_ms, now).await? {
                    delivered += 1;
                }
            }
        }
        self.set_wheel_wake(wake);
        Ok(delivered)
    }

    /// `TimerDequeuePutMessageService`: write one parked record back onto its real topic, then
    /// give up on it the way the reference does after three failed attempts.
    /// `enqueue_ms` is `i64::MAX` for a record that never reached the wheel; `now` is the time the
    /// record leaves it, which a record delivered during the scan does not have yet.
    async fn deliver_parked(
        &self,
        message: Message,
        enqueue_ms: i64,
        dequeue_ms: i64,
    ) -> BrokerResult<bool> {
        let Some(restored) = schedule::convert_timer_message(&message, enqueue_ms, dequeue_ms)
        else {
            warn!(
                queue_offset = message.queue_offset,
                "timer record has no real topic, dropping it"
            );
            return Ok(false);
        };
        let mut attempts = 0usize;
        loop {
            match self.put_internal(restored.clone()).await {
                Ok(_) => return Ok(true),
                Err(failure) => {
                    attempts += 1;
                    if attempts >= TIMER_PUT_RETRIES || !failure.retryable() {
                        warn!(
                            attempts,
                            code = failure.code,
                            error = failure.message,
                            "timer delivery failed, dropping the message"
                        );
                        return Ok(false);
                    }
                    tokio::time::sleep(Duration::from_millis(TIMER_RETRY_SLEEP_MS)).await;
                }
            }
        }
    }

    /// Take the slots whose deadline has come, leaving the lock before any I/O is attempted.
    fn take_due_slots(&self, now: i64) -> Vec<(i64, Vec<WheelEntry>)> {
        let mut wheel = self.wheel.lock().unwrap();
        let due: Vec<i64> = wheel
            .slots
            .keys()
            .filter(|deliver_ms| self.timer.drain_ms(**deliver_ms) <= now)
            .copied()
            .collect();
        due.into_iter()
            .map(|deliver_ms| {
                let entries = wheel.slots.remove(&deliver_ms).unwrap_or_default();
                (deliver_ms, entries)
            })
            .collect()
    }

    /// The next slot the wheel still holds, or a poll when it ran dry.
    fn set_wheel_wake(&self, wake: &mut i64) {
        let now = now_millis();
        let next = self
            .wheel
            .lock()
            .unwrap()
            .slots
            .keys()
            .next()
            .map_or(TIMER_POLL_MS, |deliver_ms| {
                (self.timer.drain_ms(*deliver_ms) - now).max(1)
            });
        *wake = (*wake).min(next);
    }

    /// `TimerFlushService`: the queue cursor alone is enough to rebuild the wheel, so it is the
    /// only part of `TimerCheckpoint` this broker persists.
    pub async fn persist_timer_checkpoint(&self) -> Result<()> {
        let checkpoint = self.wheel.lock().unwrap().checkpoint;
        if checkpoint == self.store.timer_checkpoint()? {
            return Ok(());
        }
        self.storage(move |store| store.put_timer_checkpoint(&checkpoint))
            .await
    }

    /// `ScheduleMessageService.start`: poll every configured level until the broker stops.
    pub async fn run_scheduler(&self, mut shutdown: watch::Receiver<bool>) -> Result<()> {
        let mut last_persist = now_millis();
        let mut last_flush = now_millis();
        loop {
            let mut wake = i64::MAX;
            // `DeliverDelayedMessageTimerTask.run` catches every throwable and retries, so one
            // failed round must not stop the levels after it.
            for level in self.levels.levels() {
                match self.deliver_level(level, &mut wake).await {
                    Ok(_) => {}
                    Err(failure) => {
                        warn!(
                            level,
                            code = failure.code,
                            error = failure.message,
                            "delayed delivery failed"
                        );
                        wake = wake.min(SCHEDULE_POLL_MS);
                    }
                }
            }
            // The wheel is only driven while enabled; a disabled one already rejected every timer
            // message at the store hook, so it has nothing to replay.
            if self.timer.wheel_enable {
                if let Err(failure) = self.scan_wheel(&mut wake).await {
                    warn!(
                        code = failure.code,
                        error = failure.message,
                        "timer enqueue pass failed"
                    );
                    wake = wake.min(TIMER_POLL_MS);
                }
                if let Err(failure) = self.deliver_due(&mut wake).await {
                    warn!(
                        code = failure.code,
                        error = failure.message,
                        "timer dequeue pass failed"
                    );
                    wake = wake.min(TIMER_POLL_MS);
                }
            }
            let now = now_millis();
            if now - last_persist >= DELAY_OFFSET_PERSIST_MS {
                self.persist_delay_offsets().await?;
                last_persist = now;
            }
            if now - last_flush >= self.timer.flush_interval_ms {
                self.persist_timer_checkpoint().await?;
                last_flush = now;
            }
            let wait = wake.max(1) as u64;
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(wait)) => {}
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow_and_update() {
                        break;
                    }
                }
            }
        }
        self.persist_delay_offsets().await?;
        self.persist_timer_checkpoint().await
    }

    /// `SubscriptionGroupManager.findSubscriptionGroupConfig`, including its auto-create branch.
    /// A group name the reference would reject yields `None` rather than a stored config.
    async fn group_config(&self, group: &str) -> BrokerResult<Option<GroupConfig>> {
        let group = group.to_owned();
        let auto_create = self.config.auto_create_group;
        self.storage(move |store| {
            if let Some(config) = store.group(&group)? {
                return Ok(Some(config));
            }
            if !auto_create || validate_group(&group).is_err() {
                return Ok(None);
            }
            let config = GroupConfig::new(group);
            store.put_group_if_absent(config.clone())?;
            Ok(Some(config))
        })
        .await
        .map_err(Failure::from)
    }

    /// `TopicConfigManager.createTopicInSendMessageBackMethod`: an existing topic is kept, and
    /// only its `order` flag is corrected. `autoCreateTopicEnable` does not gate this path.
    async fn ensure_back_topic(
        &self,
        topic: &str,
        queues: i32,
        perm: i32,
        sys_flag: i32,
        order: bool,
    ) -> BrokerResult<Option<TopicConfig>> {
        let topic = topic.to_owned();
        let created = TopicConfig {
            topic_name: topic.clone(),
            read_queue_nums: queues,
            write_queue_nums: queues,
            perm,
            topic_sys_flag: sys_flag,
            order,
            attributes: Default::default(),
        };
        let name = topic.clone();
        let outcome = self
            .storage(move |store| match store.topics()?.get(&name).cloned() {
                Some(existing) if existing.order == order => Ok(Some(existing)),
                Some(mut existing) => {
                    existing.order = order;
                    store.put_topic(existing.clone())?;
                    Ok(Some(existing))
                }
                None => {
                    store.put_topic(created.clone())?;
                    Ok(Some(created))
                }
            })
            .await;
        match outcome {
            Ok(config) => Ok(config),
            // The reference answers a locked or failed table with `topic[..] not exist`.
            Err(error) => {
                warn!(
                    topic,
                    error = error.to_string().as_str(),
                    "failed to create the retry or dead-letter topic"
                );
                Ok(None)
            }
        }
    }

    /// `AbstractSendMessageProcessor.consumerSendMsgBack`.
    async fn send_back(&self, request: &Command) -> BrokerResult<Command> {
        let group = request.required("group")?.to_owned();
        let offset: u64 = request.field("offset")?;
        let delay_level: i32 = request.field("delayLevel")?;
        let unit_mode: bool = optional_field(request, "unitMode", false)?;
        let origin_msg_id = request
            .ext_fields
            .get("originMsgId")
            .cloned()
            .unwrap_or_default();
        let version: i32 = request.version;

        let config = match self.group_config(&group).await? {
            Some(config) => config,
            None => {
                return Err(Failure::new(
                    26,
                    format!("subscription group not exist, {group}"),
                ));
            }
        };
        // A group with no retry queues silently drops the send-back, like the reference.
        if config.retry_queue_nums <= 0 {
            return Ok(Command::response(request, 0));
        }

        let mut max_reconsume_times = config.retry_max_times;
        if version >= V3_4_9
            && let Some(times) = optional_i32(request, "maxReconsumeTimes")?
        {
            max_reconsume_times = times;
        }

        let mut topic = schedule::retry_topic(&group);
        // `random.nextInt(retryQueueNums)`; the rotating counter is the same distribution here.
        let mut queue_id = (self.queue_counter.fetch_add(1, Ordering::Relaxed)
            % config.retry_queue_nums as u64) as i32;
        let sys_flag = if unit_mode { 2 } else { 0 };
        let config = match self
            .ensure_back_topic(&topic, config.retry_queue_nums, 6, sys_flag, false)
            .await?
        {
            Some(config) => config,
            None => {
                return Err(Failure::new(1, format!("topic[{topic}] not exist")));
            }
        };
        if config.perm & 2 == 0 {
            return Err(Failure::new(
                16,
                format!("the topic[{topic}] sending message is forbidden"),
            ));
        }

        let message = self.storage(move |store| store.message_at(offset)).await?;
        let Some(message) = message else {
            return Err(Failure::new(
                1,
                format!("look message by offset failed, {offset}"),
            ));
        };

        let mut properties = message.properties.clone();
        if !properties.contains_key(RETRY_TOPIC) {
            properties.insert(RETRY_TOPIC.into(), message.topic.clone());
        }
        let origin_id = if origin_msg_id.is_empty() {
            message.message_id()
        } else {
            origin_msg_id
        };
        properties.insert(schedule::ORIGIN_MESSAGE_ID.into(), origin_id);

        // `reconsumeTimes >= retryMaxTimes || delayLevel < 0` selects the dead-letter topic.
        let mut delay = delay_level;
        if message.reconsume_times >= max_reconsume_times || delay_level < 0 {
            topic = schedule::dlq_topic(&group);
            queue_id = 0;
            // `MessageExt.setDelayTimeLevel(0)` keeps the property, written as "0".
            delay = 0;
            if self
                .ensure_back_topic(&topic, DLQ_QUEUES, 6, 0, false)
                .await?
                .is_none()
            {
                return Err(Failure::new(1, format!("topic[{topic}] not exist")));
            }
        } else if delay == 0 {
            delay = 3 + message.reconsume_times;
        }
        properties.insert(DELAY.into(), delay.to_string());

        let record = Message {
            topic,
            queue_id,
            flag: message.flag,
            sys_flag: message.sys_flag,
            born_timestamp: message.born_timestamp,
            born_host: message.born_host,
            store_host: self.config.advertised_addr,
            reconsume_times: message.reconsume_times + 1,
            prepared_transaction_offset: 0,
            body: message.body,
            properties,
            ..Message::default()
        };
        self.put_internal(record).await?;
        Ok(Command::response(request, 0))
    }

    /// `RecallMessageProcessor.processRequest` (request code 370): cancel a parked timer message
    /// by writing a delete record into the slot the handle names.
    async fn recall(&self, request: &Command, peer: SocketAddr) -> BrokerResult<Command> {
        if !self.config.recall_message_enable {
            return Err(Failure::new(16, "recall failed, operation is forbidden"));
        }
        // The three remaining service gates of the reference hold by construction here: this
        // broker is always `ASYNC_MASTER`, `startAcceptSendRequestTimeStamp` is never set from its
        // default of 0, and `brokerPermission` is never narrowed, so no 11 or 14 can be reached.
        let topic = request.required("topic")?.to_owned();
        validate_topic_name(&topic)?;
        if !self.store.topics()?.contains_key(&topic) {
            return Err(Failure::new(
                17,
                format!("recall failed, the topic[{topic}] not exist"),
            ));
        }
        // Every rejection the decoder can make collapses to the same remark in the reference.
        let handle = schedule::parse_recall_handle(request.required("recallHandle")?)
            .map_err(|_| Failure::new(604, "recall handle is invalid"))?;
        if handle.topic != topic {
            return Err(Failure::new(604, "recall failed, topic not match"));
        }
        if handle.broker_name != self.config.broker_name {
            return Err(Failure::new(
                604,
                "recall failed, broker service not available",
            ));
        }
        // `NumberUtils.toLong(handle.getTimestampStr(), -1)`.
        let timestamp = handle.timestamp_str.parse::<i64>().unwrap_or(-1);
        let time_left = timestamp - now_millis();
        if time_left <= 0 || time_left >= self.timer.max_delay_ms {
            return Err(Failure::new(604, "recall failed, timestamp invalid"));
        }

        let now = now_millis();
        let delete_key = schedule::timer_delete_key(&handle.message_id).to_owned();
        let record = Message {
            topic: handle.topic.clone(),
            body: RECALL_BODY.to_vec(),
            born_timestamp: now,
            born_host: peer,
            store_host: self.config.advertised_addr,
            properties: BTreeMap::from([
                (TAGS.into(), RECALL_TAG.into()),
                (TIMER_DEL_UNIQKEY.into(), delete_key),
                (UNIQ_KEY.into(), handle.message_id.clone()),
                (TIMER_DELIVER_MS.into(), handle.timestamp_str.clone()),
                (BORN_TIMESTAMP.into(), now.to_string()),
                (TRACE_CONTEXT.into(), String::new()),
                (
                    PRODUCER_GROUP.into(),
                    request
                        .ext_fields
                        .get("producerGroup")
                        .cloned()
                        .unwrap_or_default(),
                ),
            ]),
            ..Message::default()
        };
        // `handlePutMessageResult`: anything outside the `PUT_OK` and flush families reports the
        // same system error, so the caller never learns why the record was refused.
        if let Err(failure) = self.put_internal(record).await {
            warn!(
                code = failure.code,
                error = failure.message,
                topic = handle.topic,
                "recall record was refused"
            );
            return Err(Failure::new(1, "recall failed, execute error"));
        }
        Ok(Command::response(request, 0)
            .with_ext("msgId", handle.message_id)
            .with_ext("MSG_REGION", "DefaultRegion"))
    }

    async fn process(&self, request: &Command, peer: SocketAddr) -> BrokerResult<Command> {
        match request.code {
            10 | 310 | 320 => self.send(request, peer).await,
            11 => self.pull(request).await,
            36 => self.send_back(request).await,
            370 => self.recall(request, peer).await,
            34 => self.heartbeat(request, peer).await,
            35 => {
                let client_id = request.required("clientID")?;
                let before: BTreeSet<String> = self
                    .clients
                    .lock()
                    .unwrap()
                    .values()
                    .flat_map(|client| client.consumers.iter().map(|c| c.group_name.clone()))
                    .collect();
                {
                    let mut clients = self.clients.lock().unwrap();
                    if let Some(client) = clients.get_mut(&peer).filter(|c| c.client_id == client_id)
                    {
                        if let Some(group) = request.ext_fields.get("producerGroup") {
                            client.producers.remove(group);
                        }
                        if let Some(group) = request.ext_fields.get("consumerGroup") {
                            client.consumers.retain(|c| &c.group_name != group);
                        }
                        if client.producers.is_empty() && client.consumers.is_empty() {
                            clients.remove(&peer);
                        }
                    }
                }
                self.expire_groups(&before);
                Ok(Command::response(request, 0))
            }
            38 => {
                let group = request.required("consumerGroup")?;
                let clients = self.clients.lock().unwrap();
                let ids: BTreeSet<_> = clients.values().filter(|c| c.consumers.iter().any(|c| c.group_name == group))
                    .map(|c| &c.client_id).collect();
                if ids.is_empty() { return Err(Failure::new(1, "the consumer's group info does not exist")); }
                json_response(request, json!({"consumerIdList":ids}))
            }
            14 | 15 => {
                let topic = request.required("topic")?.to_owned();
                let group = request.required("consumerGroup")?.to_owned();
                let queue_id: i32 = request.field("queueId")?;
                validate_name(&topic)?;
                validate_name(&group)?;
                self.check_queue(&topic, queue_id)?;
                if request.code == 15 {
                    let offset = request.field("commitOffset")?;
                    self.storage(move |store| store.commit_offset(&group, &topic, queue_id, offset)).await?;
                    Ok(Command::response(request, 0))
                } else {
                    match self.store.consumer_offset(&group, &topic, queue_id)? {
                        Some(offset) => Ok(Command::response(request, 0).with_ext("offset", offset)),
                        None => Err(Failure::new(22, "consumer offset not found")),
                    }
                }
            }
            17 => {
                let topic = request.required("topic")?;
                validate_name(topic)?;
                let mut config = TopicConfig::new(topic, request.field("writeQueueNums")?);
                config.read_queue_nums = request.field("readQueueNums")?;
                config.perm = request.field("perm")?;
                config.topic_sys_flag = optional_field(request, "topicSysFlag", 0i32)?;
                config.order = optional_field(request, "order", false)?;
                if request.ext_fields.get("topicFilterType").is_some_and(|v| v != "SINGLE_TAG")
                    || request.ext_fields.get("attributes").is_some_and(|v| !v.is_empty())
                {
                    return Err(Failure::new(3, "custom topic filter types and attributes are not implemented"));
                }
                if !(1..=1024).contains(&config.read_queue_nums) || !(1..=1024).contains(&config.write_queue_nums)
                    || !(0..=7).contains(&config.perm) || config.topic_sys_flag != 0
                {
                    return Err(Failure::new(29, "invalid queue counts, permissions or unsupported topicSysFlag"));
                }
                self.storage(move |store| store.put_topic(config)).await?;
                self.topic_changes.notify_one();
                Ok(Command::response(request, 0))
            }
            21 => json_response(request, json!({"topicConfigTable":self.store.topics()?, "dataVersion":{"counter":0,"timestamp":self.started_at}})),
            23 | 305 => json_response(request, json!({"topicList":self.store.topics()?.keys().collect::<Vec<_>>(),"brokerAddr":self.config.advertised_addr.to_string()})),
            351 => {
                let topic = request.required("topic")?;
                let configs = self.store.topics()?;
                let config = configs.get(topic).ok_or_else(|| Failure::new(17, "topic does not exist"))?;
                json_response(request, serde_json::to_value(config).map_err(Error::from)?)
            }
            29..=32 => {
                let topic = request.required("topic")?.to_owned();
                let queue_id: i32 = request.field("queueId")?;
                // `findOrCreateConsumeQueue` means the reference never answers an error here: a
                // topic or queue the broker has not written to reports an empty queue instead.
                let (key, value) = match request.code {
                    29 => {
                        let timestamp: i64 = request.field("timestamp")?;
                        ("offset", self.storage(move |store| store.search_offset(&topic, queue_id, timestamp)).await?.to_string())
                    }
                    30 => ("offset", self.store.queue_max_offset(&topic, queue_id).to_string()),
                    31 => ("offset", "0".into()),
                    _ => {
                        let read = self.storage(move |store| store.read(&topic, queue_id, 0, 1, MAX_PULL_BYTES, None)).await?;
                        ("timestamp", read.messages.first().map_or(-1, |m| m.store_timestamp).to_string())
                    }
                };
                Ok(Command::response(request, 0).with_ext(key, value))
            }
            33 => {
                let offset: u64 = request.field("offset")?;
                let message = self.storage(move |store| store.message_at(offset)).await?
                    .ok_or_else(|| Failure::new(22, "message not found"))?;
                Ok(Command::response(request, 0).with_body(message.encode()?))
            }
            43 => json_response(request, self.store.all_offsets()?),
            // `AdminBrokerProcessor.getAllDelayOffset` answers with `ScheduleMessageService.encode()`.
            45 => json_response(request, json!({
                "offsetTable": self.delay_offsets.lock().unwrap().offset_table,
                "dataVersion": {"counter": 0, "timestamp": self.started_at}
            })),
            200 => {
                if !request.body.is_empty() {
                    let config: GroupConfig =
                        serde_json::from_slice(&request.body).map_err(Error::from)?;
                    validate_group(&config.group_name)
                        .map_err(|error| Failure::new(3, error.to_string()))?;
                    self.storage(move |store| store.put_group(config)).await?;
                }
                Ok(Command::response(request, 0))
            }
            201 => {
                // Split metadata is not modelled, so this answers in one page exactly like a broker
                // with `enableSplitMetadata=false`: the Java client stops as soon as the page it
                // received reaches `totalGroupNum`, which a complete table always does.
                let groups = self.store.groups()?;
                let body = json!({
                    "subscriptionGroupTable": groups,
                    "forbiddenTable": {},
                    "dataVersion": {"counter": 0, "timestamp": self.started_at}
                });
                Ok(Command::response(request, 0)
                    .with_ext("totalGroupNum", groups.len() as i32)
                    .with_body(serde_json::to_vec(&body).map_err(Error::from)?))
            }
            207 => {
                let group = request.required("groupName")?.to_owned();
                let clean_offset = optional_field(request, "cleanOffset", false)?;
                self.storage(move |store| store.delete_group(&group, clean_offset))
                    .await?;
                Ok(Command::response(request, 0))
            }
            26 => Ok(Command::response(request, 0).with_body(format!(
                "brokerName={}\nbrokerClusterName={}\nbrokerId=0\nbrokerIP1={}\nlistenPort={}\nautoCreateTopicEnable={}\nflushDiskType=SYNC_FLUSH\nbrokerRole=ASYNC_MASTER\n",
                self.config.broker_name, self.config.cluster_name, self.config.advertised_addr.ip(),
                self.config.advertised_addr.port(), self.config.auto_create_topic).into_bytes())),
            28 => json_response(request, json!({"table":{
                "brokerVersionDesc":"RocketMQ-RS-0.1.0",
                "brokerName":self.config.broker_name,
                "bootTimestamp":self.started_at.to_string(),
                "commitLogMaxOffset":self.store.physical_len()?.to_string(),
                "topicCount":self.store.topics()?.len().to_string(),
                "clientCount":self.clients.lock().unwrap().len().to_string()
            }})),
            46 => {
                if request.body.is_empty() {
                    return Ok(Command::response(request, 0));
                }
                let body: CheckClientRequest =
                    serde_json::from_slice(&request.body).map_err(Error::from)?;
                let Some(subscription) = body.subscription_data else {
                    return Ok(Command::response(request, 0));
                };
                let expression_type = subscription.expression_type.unwrap_or_default();
                if filter::is_tag_type(Some(&expression_type)) {
                    return Ok(Command::response(request, 0));
                }
                if !self.config.enable_property_filter {
                    return Err(Failure::new(
                        1,
                        format!(
                            "the broker does not support consumer to filter message by {expression_type}"
                        ),
                    ));
                }
                if let Err(error) =
                    Subscription::build(Some(&expression_type), &subscription.sub_string)
                {
                    return Err(Failure::new(23, error.to_string()));
                }
                Ok(Command::response(request, 0))
            }
            _ => Err(Failure::new(3, format!("request code {} is not implemented", request.code))),
        }
    }

    fn check_queue(&self, topic: &str, queue_id: i32) -> BrokerResult<()> {
        let config = self
            .store
            .topics()?
            .get(topic)
            .cloned()
            .ok_or_else(|| Failure::new(17, "topic does not exist"))?;
        if queue_id < 0 || queue_id >= config.read_queue_nums.max(config.write_queue_nums) {
            return Err(Failure::new(29, "queueId out of range"));
        }
        Ok(())
    }
}

impl Handler for Broker {
    fn handle(
        &self,
        request: Command,
        peer: SocketAddr,
    ) -> Pin<Box<dyn Future<Output = Command> + Send + '_>> {
        Box::pin(async move {
            match self.process(&request, peer).await {
                Ok(response) => response,
                Err(failure) => {
                    let mut response = Command::response(&request, failure.code);
                    response.remark = Some(failure.message);
                    if let Some(kind) = failure.forbidden_type {
                        response = response.with_ext("forbiddenType", kind);
                    }
                    response
                }
            }
        })
    }

    fn disconnected(&self, peer: SocketAddr) {
        let before: BTreeSet<String> = self
            .clients
            .lock()
            .unwrap()
            .values()
            .flat_map(|client| client.consumers.iter().map(|c| c.group_name.clone()))
            .collect();
        self.clients.lock().unwrap().remove(&peer);
        self.expire_groups(&before);
    }
}

/// `TopicConfigManager.init`: the internal topics the schedule engine writes to.
fn schedule_system_topics(
    levels: &schedule::DelayLevels,
    timer_enabled: bool,
) -> Vec<(&'static str, i32)> {
    let mut topics = vec![(SCHEDULE_TOPIC, SCHEDULE_QUEUES.max(levels.max()).min(1024))];
    if timer_enabled {
        topics.push((schedule::TIMER_TOPIC, 1));
    }
    topics
}

/// `ScheduleMessageService.correctDelayOffset`: a cursor that left its queue is pulled back.
fn correct_delay_offsets(
    store: &Store,
    levels: &schedule::DelayLevels,
    offsets: &mut schedule::DelayOffsets,
) {
    for level in levels.levels() {
        let queue = schedule::delay_level_to_queue_id(level);
        let max = store.queue_max_offset(SCHEDULE_TOPIC, queue);
        if let Some(offset) = offsets.offset_table.get_mut(&level) {
            *offset = (*offset).min(max);
        }
    }
}

/// A nullable header field, where absent and malformed are different outcomes.
fn optional_i32(request: &Command, key: &str) -> Result<Option<i32>> {
    match request.ext_fields.get(key) {
        None => Ok(None),
        Some(value) => value
            .parse()
            .map(Some)
            .map_err(|_| Error::Invalid(format!("invalid {key}"))),
    }
}

fn optional_field<T: std::str::FromStr>(request: &Command, key: &str, default: T) -> Result<T> {
    match request.ext_fields.get(key) {
        None => Ok(default),
        Some(value) => value
            .parse()
            .map_err(|_| Error::Invalid(format!("invalid {key}"))),
    }
}

/// Properties whose broker-side behaviour this server does not implement yet.
///
/// Delay levels, accurate timers and the timer recall are all handled, by
/// `schedule::handle_schedule_message` and the wheel; only the transaction half message stays
/// outside the current feature set. A `TIMER_DEL_UNIQKEY` a producer sets by itself is left alone
/// here, because the reference only inspects it on the timer path and the wheel ignores a record
/// that carries no deliver time.
fn reject_unsupported_properties(properties: &BTreeMap<String, String>) -> BrokerResult<()> {
    if properties.contains_key("__LITE_TOPIC") {
        return Err(Failure::new(3, "Lite message delivery is not implemented"));
    }
    if properties
        .get("TRAN_MSG")
        .is_some_and(|value| value == "true")
    {
        return Err(Failure::new(
            3,
            "transactional half messages are not implemented",
        ));
    }
    Ok(())
}

fn json_response(request: &Command, value: serde_json::Value) -> BrokerResult<Command> {
    Ok(Command::response(request, 0).with_body(serde_json::to_vec(&value).map_err(Error::from)?))
}

fn pull_response(request: &Command, code: i32, read: ReadResult) -> BrokerResult<Command> {
    let mut body = Vec::new();
    for message in read.messages {
        body.extend(message.encode()?);
    }
    Ok(Command::response(request, code)
        .with_ext("suggestWhichBrokerId", 0)
        .with_ext("nextBeginOffset", read.next_begin_offset)
        .with_ext("minOffset", read.min_offset)
        .with_ext("maxOffset", read.max_offset)
        .with_ext("offsetDelta", 0)
        .with_body(body))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn known(_: &str) -> bool {
        true
    }

    #[test]
    fn filter_registration_follows_subscription_versions() {
        let mut registry = FilterRegistry::default();
        assert!(!registry.register_one(false, "g", "missing", "a = 1", "SQL92", 1));
        assert!(!registry.register_one(true, "g", "t", "a = 1", "TAG", 1));
        assert!(!registry.register_one(true, "g", "t", "", "SQL92", 1));
        assert!(registry.entries.is_empty());

        assert!(registry.register_one(true, "g", "t", "a = 1", "SQL92", 5));
        let born = registry.get("t", "g").unwrap().born_time;
        assert!(!registry.register_one(true, "g", "t", "a = 2", "SQL92", 4));
        assert_eq!(registry.get("t", "g").unwrap().expression, "a = 1");
        assert!(!registry.register_one(true, "g", "t", "a = 2", "SQL92", 5));
        assert_eq!(registry.get("t", "g").unwrap().client_version, 5);

        registry.mark_group_dead("g");
        assert!(registry.get("t", "g").unwrap().dead_time != 0);
        assert!(registry.register_one(true, "g", "t", "a = 1", "SQL92", 5));
        let alive = registry.get("t", "g").unwrap();
        assert_eq!(alive.dead_time, 0);
        assert_eq!(alive.born_time, born);

        assert!(registry.register_one(true, "g", "t", "a = 2", "SQL92", 6));
        assert_eq!(registry.get("t", "g").unwrap().expression, "a = 2");
        // A newer expression that cannot be parsed evicts the previous filter.
        assert!(!registry.register_one(true, "g", "t", "a =", "SQL92", 7));
        assert!(registry.get("t", "g").is_none());
    }

    #[test]
    fn filter_registration_marks_dropped_topics_dead_and_keeps_only_group_entries() {
        let mut registry = FilterRegistry::default();
        registry.register_group(
            &known,
            "g",
            &[
                subscription("keep", "a = 1", 1),
                subscription("drop", "b = 2", 1),
                tag_subscription("tagged", "tag-a", 1),
                subscription("invalid", "a =", 1),
            ],
        );
        assert_eq!(registry.get("keep", "g").unwrap().dead_time, 0);
        assert_eq!(registry.get("drop", "g").unwrap().dead_time, 0);
        // Tag subscriptions never reach the filter manager, and an uncompilable expression
        // leaves no entry behind.
        assert!(registry.get("tagged", "g").is_none());
        assert!(registry.get("invalid", "g").is_none());

        registry.register_group(
            &known,
            "g",
            &[
                subscription("keep", "a = 1", 2),
                tag_subscription("tagged", "tag-a", 2),
            ],
        );
        assert_ne!(registry.get("drop", "g").unwrap().dead_time, 0);
        assert_eq!(registry.get("keep", "g").unwrap().client_version, 2);
        // A second group is never affected by the first one's subscription list.
        assert!(registry.register_one(true, "other", "drop", "c = 3", "SQL92", 1));
        assert_eq!(registry.get("drop", "other").unwrap().dead_time, 0);
    }

    fn subscription(topic: &str, expression: &str, version: i64) -> HeartbeatSubscription {
        HeartbeatSubscription {
            topic: topic.into(),
            sub_string: expression.into(),
            expression_type: Some("SQL92".into()),
            sub_version: version,
        }
    }

    fn tag_subscription(topic: &str, expression: &str, version: i64) -> HeartbeatSubscription {
        HeartbeatSubscription {
            topic: topic.into(),
            sub_string: expression.into(),
            expression_type: Some("TAG".into()),
            sub_version: version,
        }
    }

    #[test]
    fn clean_only_removes_filters_that_stayed_dead_past_the_span() {
        fn stored(dead_time: i64) -> FilterEntry {
            FilterEntry {
                expression: "a = 1".into(),
                expression_type: "SQL92".into(),
                born_time: now_millis(),
                dead_time,
                client_version: 1,
                compiled: Subscription::build(Some("SQL92"), "a = 1").ok(),
            }
        }
        let now = now_millis();
        let mut registry = FilterRegistry::default();
        registry
            .entries
            .insert(("alive".into(), "t".into()), stored(0));
        registry
            .entries
            .insert(("fresh".into(), "t".into()), stored(now - 1_000));
        registry
            .entries
            .insert(("stale".into(), "t".into()), stored(now - 5_000));

        registry.clean(3_000);
        assert!(registry.get("t", "alive").is_some());
        assert!(registry.get("t", "fresh").is_some());
        assert!(registry.get("t", "stale").is_none());
        assert!(registry.dirty);
    }

    #[test]
    fn persisted_filters_restore_alive_with_their_compiled_expression() {
        let directory = TempDir::new().unwrap();
        {
            let store = Store::open(directory.path()).unwrap();
            let mut registry = FilterRegistry::default();
            registry.register_group(&known, "g", &[subscription("orders", "level > 5", 3)]);
            store
                .put_consumer_filters(&registry.snapshots())
                .expect("persist filters");
        }
        let store = Store::open(directory.path()).unwrap();
        let mut restored = FilterRegistry::default();
        restored.hydrate(&store).unwrap();
        let entry = restored.get("orders", "g").unwrap();
        assert_eq!(entry.expression, "level > 5");
        assert_eq!(entry.client_version, 3);
        assert_ne!(entry.dead_time, 0);
        assert!(entry.compiled.is_some());

        // `decode` keeps a filter whose expression no longer compiles, with a null expression.
        store
            .put_consumer_filters(&[FilterSnapshot {
                topic: "orders".into(),
                consumer_group: "g".into(),
                expression: "level >".into(),
                expression_type: "SQL92".into(),
                born_time: now_millis(),
                dead_time: 0,
                client_version: 9,
            }])
            .unwrap();
        drop(store);
        let store = Store::open(directory.path()).unwrap();
        let mut restored = FilterRegistry::default();
        restored.hydrate(&store).unwrap();
        let entry = restored.get("orders", "g").unwrap();
        assert!(entry.compiled.is_none());
        assert_eq!(entry.client_version, 9);
    }
}
