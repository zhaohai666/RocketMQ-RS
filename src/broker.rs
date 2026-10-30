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

use crate::auth;
use crate::common;
use crate::common::{
    Error, LMQ_QUEUE_ID, Message, PROPERTY_INNER_MULTI_DISPATCH, PROPERTY_LITE_TOPIC, Result,
    TopicConfig, decode_batch, decode_properties, get_parent_topic, is_lite_topic_queue,
    now_millis, to_lmq_name,
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

/// `TransactionalMessageUtil.buildHalfTopic`: prepared messages land here, invisible to
/// consumers, until the producer resolves them.
pub const TRANS_HALF_TOPIC: &str = "RMQ_SYS_TRANS_HALF_TOPIC";
/// `TransactionalMessageUtil.buildOpTopic`: one record per resolved half message, carrying the
/// half record's physical offset, so a restart can rebuild the pending set.
pub const TRANS_OP_TOPIC: &str = "RMQ_SYS_TRANS_OP_HALF_TOPIC";
/// `MessageConst.PROPERTY_TRANSACTION_PREPARED`
const TRAN_MSG: &str = "TRAN_MSG";
/// `MessageConst.PROPERTY_REAL_QUEUE_ID`; the topic itself is `schedule::REAL_TOPIC`.
const REAL_QID: &str = "REAL_QID";
/// `MessageConst.PROPERTY_POP_CK`, stamped on every message a pop hands out.
const POP_CK: &str = "RMQ_SYS_POP_CK";
/// `MessageSysFlag.TRANSACTION_PREPARED_TYPE`
const TRANS_PREPARED_FLAG: i32 = 0x4;
/// `MessageSysFlag.TRANSACTION_COMMIT_TYPE`; ROLLBACK (0xC) carries this bit too.
const TRANS_COMMIT_FLAG: i32 = 0x8;
/// `MessageSysFlag.INNER_BATCH_FLAG`: one record whose body splits into separate queue entries.
const INNER_BATCH_FLAG: i32 = 0x80;
/// `BrokerConfig.transactionTimeOut`, the age a half message must reach before a check round.
const TRANS_CHECK_TIMEOUT_MS: i64 = 6_000;
/// `BrokerConfig.transactionCheckInterval`
const TRANS_CHECK_INTERVAL_MS: i64 = 60_000;
/// Java `BrokerConfig.transactionCheckMax`: give up checking a half message after this
/// many rounds (no push channel here, so "give up" means stop re-reporting it).
const TRANS_CHECK_MAX_TIMES: u32 = 15;
/// `PopMessageProcessor`: one page of the real queue a pop round inspects.
const POP_SCAN_BATCH: usize = 64;
/// The per-queue page window stays proportional to what the caller may receive.
const POP_MAX_NUMS: usize = 32;

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
    /// `authConfig.authenticationEnabled` + `authorizationEnabled`: reject
    /// requests that fail plain-ACL signature verification.
    pub enable_acl: bool,
    /// Path of the legacy plain-ACL accounts file (Java `plain_acl.yml`;
    /// JSON or YAML). Loaded via [`Broker::load_acl_accounts`] at startup.
    pub acl_config_file: Option<String>,
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
            enable_acl: false,
            acl_config_file: None,
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

/// Mirrors Java's `ConsumerGroupInfo.subscriptionTable` + `channelInfoTable`: subscription
/// state is keyed by consumer group and decoupled from any single TCP channel. A proxy that
/// multiplexes many logical consumers over shared (and recycled) broker connections keeps
/// working because the subscription survives a channel close and self-heals via pull-time
/// compensation.
struct GroupInfo {
    /// topic -> subscription, mirroring `ConsumerGroupInfo.subscriptionTable`.
    subscriptions: BTreeMap<String, HeartbeatSubscription>,
    /// client ids (channel markers) currently associated with the group.
    clients: BTreeSet<String>,
    last_seen: Instant,
}

impl GroupInfo {
    fn new() -> Self {
        GroupInfo {
            subscriptions: BTreeMap::new(),
            clients: BTreeSet::new(),
            last_seen: Instant::now(),
        }
    }
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

/// ACL engine wired into the broker's request path (Java: the broker's
/// `AccessValidator` + `PlainAccessValidator`). Authentication always rejects
/// commands whose `AccessKey`/`Signature` ext fields do not verify against a
/// registered user; authorization additionally evaluates the caller's ACL.
struct BrokerAcl {
    authentication: Arc<auth::DefaultAuthenticationProvider>,
    authorization: Arc<auth::DefaultAuthorizationProvider>,
    authorization_enabled: bool,
    auth_metadata: Arc<auth::LocalAuthenticationMetadataProvider>,
    acl_metadata: Arc<auth::LocalAuthorizationMetadataProvider>,
    acl_config_file: Option<String>,
}

impl BrokerAcl {
    fn new(config: &BrokerConfig) -> Self {
        let auth_metadata = auth::LocalAuthenticationMetadataProvider::new();
        let acl_metadata = auth::LocalAuthorizationMetadataProvider::new();
        let auth_config = auth::AuthConfig {
            authentication_enabled: true,
            ..auth::AuthConfig::default()
        };
        let authorization_config = auth::AuthConfig {
            authorization_enabled: true,
            ..auth::AuthConfig::default()
        };
        Self {
            authentication: auth::DefaultAuthenticationProvider::new(
                auth_config,
                Arc::clone(&auth_metadata),
            ),
            authorization: auth::DefaultAuthorizationProvider::new(
                authorization_config,
                Arc::clone(&auth_metadata),
                Arc::clone(&acl_metadata),
            ),
            authorization_enabled: true,
            auth_metadata,
            acl_metadata,
            acl_config_file: config.acl_config_file.clone(),
        }
    }

    /// Startup bootstrap: load the plain-ACL accounts into the metadata
    /// providers. Returns the number of accounts loaded.
    async fn load_accounts(&self) -> Result<usize> {
        let Some(path) = self.acl_config_file.as_deref() else {
            return Ok(0);
        };
        auth::bootstrap_plain_acl(&self.auth_metadata, &self.acl_metadata, path).await
    }

    /// Gate one inbound command: signature authentication first, then (when
    /// the caller carries an access key) the authorization policy chain.
    async fn check(&self, command: &Command) -> Result<()> {
        self.authentication.authenticate_command(command).await?;
        if self.authorization_enabled {
            for ctx in self.authorization.build_contexts(command, "")? {
                self.authorization.authorize(&ctx).await?;
            }
        }
        Ok(())
    }
}

pub struct Broker {
    store: Arc<Store>,
    config: BrokerConfig,
    /// ACL engine: present only when `enable_acl` is set. Verifies every
    /// inbound remoting command's signature and (when enabled) evaluates the
    /// caller's authorization policy, like Java's `AccessValidator` hook.
    acl: Option<BrokerAcl>,
    /// `consumerTable`: group -> subscription/connection state (Java `ConsumerManager`).
    groups: Mutex<BTreeMap<String, GroupInfo>>,
    /// peer (TCP) -> client id, so `disconnected` can drop the channel marker.
    peer_channels: Mutex<BTreeMap<SocketAddr, String>>,
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
    /// `PopMessageProcessor`'s buffer: offsets handed out and still inside their invisible span,
    /// keyed by (group, topic, queue) and holding offset -> invisible-until epoch millis.
    pop_invisible: Mutex<BTreeMap<(String, String, i32), BTreeMap<u64, u64>>>,
    /// `ConsumerOffsetManager`'s pop side: the next offset each (group, topic, queue) scans from.
    pop_offsets: Mutex<BTreeMap<(String, String, i32), u64>>,
    /// `QueueLockManager`: one lock per (group, topic, queue). Java takes it around the
    /// whole read of a queue, so two pops of the same queue never hand out the same
    /// record; without it the read and the cursor advance race and a message is
    /// delivered twice.
    pop_locks: Mutex<BTreeMap<(String, String, i32), Arc<tokio::sync::Mutex<()>>>>,
    /// `LiteSubscriptionRegistry`: the lite topics each client subscribed to, so the
    /// lite pop path knows which light queues to read.
    lite_subscriptions: Mutex<BTreeMap<String, LiteSubscription>>,
    /// `PopConsumerLockService`: one lock per (group, light queue). Java takes it
    /// around the whole read of a light queue, so two pops of the same lite topic
    /// never hand out the same record.
    lite_locks: Mutex<BTreeMap<(String, String), Arc<tokio::sync::Mutex<()>>>>,
}

/// `LiteSubscriptionRegistry`'s entry for one client.
#[derive(Clone, Debug, Default)]
struct LiteSubscription {
    /// The consumer group the client belongs to.
    group: String,
    /// The bind (parent) topic the lite topics hang below.
    bind_topic: String,
    /// The lite topics the client subscribed to.
    lite_topics: BTreeSet<String>,
}

#[derive(Debug)]
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
            Error::NotFound(_) | Error::AlreadyExists(_) | Error::Timeout(_) | Error::Auth(_) => 29,
        };
        Self::new(code, error.to_string())
    }
}

pub fn validate_name(name: &str) -> Result<()> {
    // A light message queue is named `%LMQ%$parent$lite`; the `$` separator is
    // reserved for it and stays out of ordinary topic and group names.
    if is_lite_topic_queue(name) {
        return Ok(());
    }
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
        // `TransactionalMessageBridge`: the half and op topics exist from the first start.
        for topic in [TRANS_HALF_TOPIC, TRANS_OP_TOPIC] {
            if !store.topics()?.contains_key(topic) {
                let mut config = TopicConfig::new(topic, 1);
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
        let acl = if config.enable_acl {
            Some(BrokerAcl::new(&config))
        } else {
            None
        };
        Ok(Self {
            store,
            config,
            acl,
            groups: Mutex::new(BTreeMap::new()),
            peer_channels: Mutex::new(BTreeMap::new()),
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
            pop_invisible: Mutex::new(BTreeMap::new()),
            pop_offsets: Mutex::new(BTreeMap::new()),
            pop_locks: Mutex::new(BTreeMap::new()),
            lite_subscriptions: Mutex::new(BTreeMap::new()),
            lite_locks: Mutex::new(BTreeMap::new()),
        })
    }

    pub fn store(&self) -> &Arc<Store> {
        &self.store
    }
    pub fn config(&self) -> &BrokerConfig {
        &self.config
    }

    /// Startup bootstrap: when ACL is enabled and `aclConfigFile` is set, load
    /// the plain-ACL accounts into the broker's metadata providers (Java
    /// `AuthMigrator` at broker startup). Returns the account count (0 when
    /// ACL is disabled or no file is configured).
    pub async fn load_acl_accounts(&self) -> Result<usize> {
        match &self.acl {
            Some(acl) => {
                let accounts = acl.load_accounts().await?;
                if accounts == 0 {
                    tracing::warn!(
                        "broker ACL is enabled but no aclConfigFile is set; \
                         every request will be rejected until users are registered"
                    );
                }
                Ok(accounts)
            }
            None => Ok(0),
        }
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
        let before: BTreeSet<String> = self.groups.lock().unwrap().keys().cloned().collect();
        // Keep a group's subscription while it still has a live channel, or was touched
        // recently. A proxy re-pull re-creates it via compensate-on-pull anyway.
        self.groups.lock().unwrap().retain(|_, g| {
            !g.clients.is_empty() || g.last_seen.elapsed() < Duration::from_secs(120)
        });
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
        // COMMIT and ROLLBACK never reach this path from a producer (0xC carries the COMMIT bit),
        // and 0x40 is reserved; prepared and inner-batch records are accepted below.
        if sys_flag & (TRANS_COMMIT_FLAG | 0x40) != 0 {
            return Err(Failure::new(
                3,
                "commit, rollback and reserved sys flags are not accepted from producers",
            ));
        }
        let properties = decode_properties(
            request
                .ext_fields
                .get(key("properties", "i"))
                .map_or("", String::as_str),
        )?;
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
            apply_lite_dispatch(&mut message);
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
        // `SendMessageProcessor` marks a record whose producer asked for a transaction. The
        // half copy keeps the producer's own flags (the prepared bit is re-set here because a
        // `TRAN_MSG` property alone must still store as prepared).
        let transactional = template
            .properties
            .get(TRAN_MSG)
            .is_some_and(|value| value == "true")
            || template.sys_flag & TRANS_PREPARED_FLAG != 0;
        if transactional {
            if batch_flag {
                return Err(Failure::new(
                    13,
                    "a transactional message cannot be a batch",
                ));
            }
            if schedule::delay_level(&template).is_ok_and(|level| level > 0)
                || template.properties.contains_key(TIMER_OUT_MS)
            {
                return Err(Failure::new(
                    3,
                    "a transactional message cannot carry a delay level or a timer",
                ));
            }
        }
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
        } else if transactional {
            // `TransactionalMessageBridge.putHalfMessage`: the real destination moves into the
            // properties, the record lands on the half topic, and consumers never see it.
            for message in &mut messages {
                message.sys_flag |= TRANS_PREPARED_FLAG;
                message.properties.insert(
                    schedule::REAL_TOPIC.to_owned(),
                    std::mem::take(&mut message.topic),
                );
                message
                    .properties
                    .insert(REAL_QID.to_owned(), message.queue_id.to_string());
                message.topic = TRANS_HALF_TOPIC.to_owned();
                message.queue_id = 0;
                message
                    .properties
                    .insert(TRAN_MSG.to_owned(), "true".to_owned());
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
        // `CommitLog.splitInnerBatchMessage`: one record flagged `INNER_BATCH` carries several
        // encoded records in its body; each becomes its own queue entry with the parent's
        // destination and flags. A body that does not decode is refused at send time.
        if !batch_flag && template.sys_flag & INNER_BATCH_FLAG != 0 {
            let mut split = Vec::new();
            for parent in &messages {
                let mut rest = parent.body.as_slice();
                while !rest.is_empty() {
                    let (sub, consumed) = Message::decode(rest)
                        .map_err(|_| Failure::new(13, "inner batch body does not decode"))?;
                    rest = &rest[consumed..];
                    split.push(Message {
                        topic: parent.topic.clone(),
                        queue_id: parent.queue_id,
                        sys_flag: parent.sys_flag,
                        flag: sub.flag,
                        born_timestamp: parent.born_timestamp,
                        born_host: parent.born_host,
                        store_host: parent.store_host,
                        reconsume_times: parent.reconsume_times,
                        body: sub.body,
                        properties: sub.properties,
                        ..Message::default()
                    });
                }
            }
            if split.is_empty() || split.len() > 4096 {
                return Err(Failure::new(
                    13,
                    "an inner batch must carry 1..4096 messages",
                ));
            }
            messages = split;
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
            // `SendMessageProcessor` answers a prepared send with the half message's own id;
            // the producer parses the physical offset back out of it for `END_TRANSACTION`.
            .with_ext(
                "transactionId",
                transactional
                    .then(|| saved[0].message_id())
                    .unwrap_or_default(),
            )
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
            let mut groups = self.groups.lock().unwrap();
            let info = groups
                .entry(group.to_owned())
                .or_insert_with(GroupInfo::new);
            info.last_seen = Instant::now();
            if let Some(sub) = info.subscriptions.get(topic) {
                Some(sub.clone())
            } else {
                // Java `compensateSubscribeData`: a proxy-fronted consumer carries its
                // subscription in the pull request; register it on the fly and retry. This
                // is how the broker learns proxy-managed group subscriptions without a
                // separate heartbeat, and keeps the lookup independent of the TCP channel.
                match request.ext_fields.get("subscription") {
                    Some(sub_string) if !sub_string.is_empty() => {
                        let expression_type = request.ext_fields.get("expressionType").cloned();
                        let sub_version = optional_field(request, "subVersion", 0i64).unwrap_or(0);
                        let sub = HeartbeatSubscription {
                            topic: topic.to_owned(),
                            sub_string: sub_string.clone(),
                            expression_type,
                            sub_version,
                        };
                        info.subscriptions.insert(topic.to_owned(), sub.clone());
                        Some(sub)
                    }
                    _ => None,
                }
            }
        }
        .ok_or_else(|| Failure::new(24, "the consumer's subscription does not exist"))?;
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

    /// The topic configuration a request addresses. A light message queue is not a
    /// topic of its own: it borrows the permissions of the topic it hangs below and
    /// has exactly one queue.
    fn topic_config_for(&self, topic: &str) -> Option<TopicConfig> {
        let configs = self.store.topics().ok()?;
        if let Some(config) = configs.get(topic) {
            return Some(config.clone());
        }
        let parent = get_parent_topic(topic)?;
        let mut config = configs.get(parent)?.clone();
        config.read_queue_nums = 1;
        config.write_queue_nums = 1;
        Some(config)
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
            .topic_config_for(&topic)
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
        // Lite pulling rides `LITE_PULL_MESSAGE`; the ordinary pull path still refuses it.
        if request.code != 361
            && (sys_flag & 16 != 0 || request.ext_fields.contains_key("liteTopic"))
        {
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
        if heartbeat.without_sub {
            // `registerConsumerWithoutSub`: copy the prior subscription list from the group.
            let groups = self.groups.lock().unwrap();
            for consumer in &mut consumers {
                let Some(info) = groups.get(&consumer.group_name) else {
                    return Err(Failure::new(24, "full heartbeat is required for new group"));
                };
                let mut set = Vec::with_capacity(info.subscriptions.len());
                for sub in info.subscriptions.values() {
                    set.push(sub.clone());
                }
                consumer.subscription_data_set = set;
            }
        }
        // `ConsumerManager.registerConsumerWithoutSub` never reaches the filter manager.
        let registered: Vec<Consumer> = if heartbeat.without_sub {
            Vec::new()
        } else {
            consumers.clone()
        };
        // Register per group: keyed by group, independent of the TCP peer (Java parity).
        {
            let mut groups = self.groups.lock().unwrap();
            for consumer in &consumers {
                let info = groups
                    .entry(consumer.group_name.clone())
                    .or_insert_with(GroupInfo::new);
                info.last_seen = Instant::now();
                info.clients.insert(heartbeat.client_id.clone());
                if !heartbeat.without_sub {
                    for sub in &consumer.subscription_data_set {
                        info.subscriptions
                            .entry(sub.topic.clone())
                            .and_modify(|existing| {
                                if sub.sub_version >= existing.sub_version {
                                    *existing = sub.clone();
                                }
                            })
                            .or_insert_with(|| sub.clone());
                    }
                }
            }
        }
        self.peer_channels
            .lock()
            .unwrap()
            .insert(peer, heartbeat.client_id.clone());

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
        let alive: BTreeSet<String> = self.groups.lock().unwrap().keys().cloned().collect();
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

    /// `EndTransactionProcessor.processRequest`: the producer resolves a half message. The half
    /// record is looked up by the physical offset the producer parsed from its msg id; a commit
    /// re-puts the original record on its real topic, and every outcome appends an op record so
    /// the sweep never reports a resolved half message again.
    async fn end_transaction(&self, request: &Command) -> BrokerResult<Command> {
        let commit_offset: u64 = request.field("commitLogOffset")?;
        let commit_or_rollback: i32 = request.field("commitOrRollback")?;
        // The gRPC proxy has no producer group in its EndTransaction request, so the check is
        // only applied when the remoting header actually carries one.
        if let Some(group) = request.ext_fields.get("producerGroup") {
            validate_group(group).map_err(|error| Failure::new(29, error.to_string()))?;
        }
        let half = self
            .storage(move |store| store.message_at(commit_offset))
            .await?
            .ok_or_else(|| Failure::new(22, "the half message does not exist"))?;
        if half.topic != TRANS_HALF_TOPIC {
            return Err(Failure::new(
                3,
                "the message is not a transactional half message",
            ));
        }
        match commit_or_rollback {
            // COMMIT: `TransactionalMessageBridge.finishMessage` restores the real destination.
            0 => {
                let real_topic = half
                    .properties
                    .get(schedule::REAL_TOPIC)
                    .cloned()
                    .ok_or_else(|| Failure::new(3, "the half message has no real topic"))?;
                let real_queue = half
                    .properties
                    .get(REAL_QID)
                    .and_then(|value| value.parse::<i32>().ok())
                    .unwrap_or(half.queue_id);
                let mut restored = half.clone();
                restored.topic = real_topic;
                restored.queue_id = real_queue;
                restored.sys_flag &= !TRANS_PREPARED_FLAG;
                restored.prepared_transaction_offset = half.physical_offset;
                restored.properties.remove(schedule::REAL_TOPIC);
                restored.properties.remove(REAL_QID);
                restored.properties.remove(TRAN_MSG);
                self.put_internal(restored).await?;
                self.append_trans_op(half.physical_offset).await?;
            }
            // ROLLBACK and the producer's "no type" answer both discard the record, leaving
            // only the op marker behind, exactly like `eraseMessage`.
            1 | 2 => {
                self.append_trans_op(half.physical_offset).await?;
            }
            _ => {
                return Err(Failure::new(
                    29,
                    "commitOrRollback must be commit, rollback or not type",
                ));
            }
        }
        Ok(Command::response(request, 0))
    }

    /// `TransactionalMessageBridge.updateOpMessage`: one 8 byte big-endian half offset per record.
    async fn append_trans_op(&self, half_offset: u64) -> BrokerResult<()> {
        let record = Message {
            topic: TRANS_OP_TOPIC.to_owned(),
            queue_id: 0,
            store_host: self.config.advertised_addr,
            body: half_offset.to_be_bytes().to_vec(),
            ..Message::default()
        };
        self.storage(move |store| store.append_internal(vec![record]))
            .await?;
        Ok(())
    }

    /// `PopMessageProcessor.processRequest`: consume through an invisible buffer instead of a
    /// plain offset. Expired invisible entries are handed out first, then a fresh page of the
    /// real queue is scanned from this group's pop cursor.
    async fn pop(&self, request: &Command) -> BrokerResult<Command> {
        let group = request.required("consumerGroup")?.to_owned();
        let topic = request.required("topic")?.to_owned();
        let queue_id: i32 = request.field("queueId")?;
        let max_nums = optional_field(request, "maxMsgNums", 1usize)?.clamp(1, POP_MAX_NUMS);
        let invisible_time =
            optional_field(request, "invisibleTime", 15_000i64)?.clamp(0, 3_600_000);
        validate_name(&group)?;
        validate_topic_name(&topic)?;
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
            .topic_config_for(&topic)
            .ok_or_else(|| Failure::new(17, "topic does not exist"))?;
        if config.perm & 4 == 0 {
            return Err(Failure::new(
                16,
                format!("the topic[{topic}] pulling message is forbidden"),
            )
            .forbidden(3));
        }
        let queues: Vec<i32> = if queue_id < 0 {
            (0..config.read_queue_nums).collect()
        } else {
            if queue_id >= config.read_queue_nums {
                return Err(Failure::new(29, "queueId out of range"));
            }
            vec![queue_id]
        };
        // `PopMessageProcessor` evaluates the request's expression; only tags are modelled.
        let expression = request.ext_fields.get("exp").cloned().unwrap_or_default();
        let expression_type = request
            .ext_fields
            .get("expType")
            .cloned()
            .unwrap_or_else(|| "TAG".to_owned());
        let subscription = if expression.is_empty() || expression == "*" {
            None
        } else {
            if !filter::is_tag_type(Some(&expression_type)) {
                return Err(Failure::new(
                    1,
                    format!("the broker does not support pop filtering by {expression_type}"),
                ));
            }
            Some(
                Subscription::build(Some("TAG"), &expression)
                    .map_err(|_| Failure::new(23, "parse the consumer's subscription failed"))?,
            )
        };
        let now = now_millis();
        let invisible_until = now + invisible_time;
        // Java `PopMessageProcessor` holds the pop for `popTimeout` when the queue has
        // nothing to hand out, which is what turns the client's await duration into a
        // real long poll instead of an immediate empty answer.
        let pop_timeout = optional_field(request, "popTimeout", 0i64)?.clamp(0, 30_000);
        let deadline = tokio::time::Instant::now() + Duration::from_millis(pop_timeout as u64);
        let mut updates = self.messages.subscribe();
        let delivered = loop {
            let delivered = self
                .pop_queues(
                    &group,
                    &topic,
                    &queues,
                    max_nums,
                    now,
                    invisible_until,
                    subscription.as_ref(),
                )
                .await?;
            if !delivered.is_empty() || pop_timeout == 0 {
                break delivered;
            }
            if tokio::time::timeout_at(deadline, updates.changed())
                .await
                .is_err()
            {
                break delivered;
            }
        };
        if delivered.is_empty() {
            return Ok(Command::response(request, 0));
        }
        let mut body = Vec::new();
        let mut first_ck = String::new();
        for (index, message) in delivered.iter().enumerate() {
            let ck = pop_ck(
                &topic,
                &group,
                message.queue_id,
                message.queue_offset,
                now,
                invisible_until,
            );
            if index == 0 {
                first_ck = ck.clone();
            }
            let mut stamped = message.clone();
            stamped.properties.insert(POP_CK.to_owned(), ck);
            body.extend(stamped.encode()?);
        }
        let rest_num: u64 = delivered
            .iter()
            .map(|message| {
                self.store
                    .queue_max_offset(&topic, message.queue_id)
                    .saturating_sub(message.queue_offset + 1)
            })
            .sum();
        Ok(Command::response(request, 0)
            .with_ext("popCk", first_ck)
            .with_ext("popTime", now)
            .with_ext("invisibleTime", invisible_time)
            .with_ext("reviveQid", 0)
            .with_ext("restNum", rest_num as i64)
            .with_body(body))
    }

    /// The delivery core shared by the pop paths: hand out the records of `queues`
    /// that are still invisible or newly readable, and register what was handed out
    /// so a later pop does not offer the same record again.
    ///
    /// Java's `PopMessageProcessor` reads every queue of a `queueId < 0` request in
    /// turn, each of them under the queue's own `QueueLockManager` lock.
    async fn pop_queues(
        &self,
        group: &str,
        topic: &str,
        queues: &[i32],
        max_nums: usize,
        now: i64,
        invisible_until: i64,
        subscription: Option<&Subscription>,
    ) -> BrokerResult<Vec<Message>> {
        let mut delivered: Vec<Message> = Vec::new();
        for queue_id in queues {
            if delivered.len() >= max_nums {
                break;
            }
            // The read of a queue and the advance of its pop cursor belong to one
            // critical section: without the lock two pops of the same queue read
            // the same page and hand the same record out twice.
            let lock = self.pop_lock(group, topic, *queue_id);
            let _guard = lock.lock().await;
            let mut from_queue = self
                .pop_queue(
                    group,
                    topic,
                    *queue_id,
                    max_nums - delivered.len(),
                    now,
                    invisible_until,
                    subscription,
                )
                .await?;
            delivered.append(&mut from_queue);
        }
        Ok(delivered)
    }

    /// The lock that guards the read of one queue.
    fn pop_lock(&self, group: &str, topic: &str, queue_id: i32) -> Arc<tokio::sync::Mutex<()>> {
        let mut locks = self.pop_locks.lock().unwrap();
        locks
            .entry((group.to_owned(), topic.to_owned(), queue_id))
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }

    /// `PopMessageProcessor.popMsgFromQueue`: the records of a single queue that are
    /// either invisible but past their invisible span, or readable from the group's
    /// pop cursor.
    async fn pop_queue(
        &self,
        group: &str,
        topic: &str,
        queue_id: i32,
        max_nums: usize,
        now: i64,
        invisible_until: i64,
        subscription: Option<&Subscription>,
    ) -> BrokerResult<Vec<Message>> {
        // Phase one, under the locks: expired offsets and the scan start of this
        // queue. The guards are dropped before any I/O, exactly like the pull path
        // keeps its lock scope tight.
        let (expired, start, committed) = {
            let invisible = self.pop_invisible.lock().unwrap();
            let offsets = self.pop_offsets.lock().unwrap();
            let key = (group.to_owned(), topic.to_owned(), queue_id);
            let expired = invisible
                .get(&key)
                .map(|seen| {
                    seen.iter()
                        .filter(|(_, until)| **until <= now as u64)
                        .map(|(offset, _)| *offset)
                        .collect::<Vec<u64>>()
                })
                .unwrap_or_default();
            let committed = self
                .store
                .consumer_offset(group, topic, queue_id)
                .ok()
                .flatten()
                .unwrap_or(0);
            let start = offsets.get(&key).copied().unwrap_or(committed);
            (expired, start, committed)
        };
        // Phase two: the reads. Expired entries are offered before a fresh page of
        // the queue, in queue order, until the caller's count is reached.
        let mut delivered: Vec<Message> = Vec::new();
        for offset in &expired {
            if delivered.len() >= max_nums {
                break;
            }
            // A record the group already committed is not handed out again, even
            // when its invisible entry outlived the ack that cleared it.
            if *offset < committed {
                continue;
            }
            let topic = topic.to_owned();
            let offset = *offset;
            if let Some(message) = self
                .storage(move |store| store.message_by_queue_offset(&topic, queue_id, offset))
                .await?
            {
                if subscription.is_none_or(|s| s.matches(&message)) {
                    delivered.push(message);
                }
            }
        }
        if delivered.len() < max_nums {
            let topic = topic.to_owned();
            let read = self
                .storage(move |store| {
                    store.read(
                        &topic,
                        queue_id,
                        start,
                        POP_SCAN_BATCH,
                        MAX_PULL_BYTES,
                        None,
                    )
                })
                .await?;
            for message in read.messages {
                if delivered.len() >= max_nums {
                    break;
                }
                if !subscription.is_none_or(|s| s.matches(&message)) {
                    continue;
                }
                delivered.push(message);
            }
        }
        // Phase three: register what was handed out and advance the cursor. A racing
        // pop that already claimed an offset loses it here, which mirrors the
        // buffer's atomic claim.
        {
            let mut invisible = self.pop_invisible.lock().unwrap();
            let mut offsets = self.pop_offsets.lock().unwrap();
            let key = (group.to_owned(), topic.to_owned(), queue_id);
            let mut next = offsets.get(&key).copied().unwrap_or(committed);
            for message in &delivered {
                let seen = invisible.entry(key.clone()).or_default();
                seen.insert(message.queue_offset, invisible_until as u64);
                next = next.max(message.queue_offset + 1);
            }
            offsets.insert(key, next);
        }
        Ok(delivered)
    }

    /// `PopLiteMessageProcessor.processRequest`: a lite consumer reads the light
    /// queues of the lite topics it subscribed to, each of which keeps an offset
    /// of its own instead of sharing the bind topic's.
    async fn pop_lite(&self, request: &Command) -> BrokerResult<Command> {
        let group = request.required("consumerGroup")?.to_owned();
        let parent_topic = request.required("topic")?.to_owned();
        let client_id = request.required("clientId")?.to_owned();
        let max_nums = optional_field(request, "maxMsgNum", 1usize)?.clamp(1, POP_MAX_NUMS);
        let invisible_time =
            optional_field(request, "invisibleTime", 15_000i64)?.clamp(0, 3_600_000);
        validate_name(&group)?;
        validate_topic_name(&parent_topic)?;
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
            .topic_config_for(&parent_topic)
            .ok_or_else(|| Failure::new(17, "topic does not exist"))?;
        if config.perm & 4 == 0 {
            return Err(Failure::new(
                16,
                format!("the topic[{parent_topic}] pulling message is forbidden"),
            )
            .forbidden(3));
        }
        let light_queues = self.light_queues_of(&client_id, &group, &parent_topic);
        let now = now_millis();
        let invisible_until = now + invisible_time;
        // The lite pop waits for `popTimeout` as well, so a lite consumer's await
        // duration is a real long poll.
        let pop_timeout = optional_field(request, "popTimeout", 0i64)?.clamp(0, 30_000);
        let deadline = tokio::time::Instant::now() + Duration::from_millis(pop_timeout as u64);
        let mut updates = self.messages.subscribe();
        // Every message keeps the light queue it was read from, so its receipt
        // handle names the lite topic's own queue.
        let mut delivered: Vec<(String, Message)> = Vec::new();
        loop {
            delivered.clear();
            for lmq in &light_queues {
                if delivered.len() >= max_nums {
                    break;
                }
                // Java holds the light queue's lock around the whole read, so a
                // second pop of the same lite topic waits instead of handing out the
                // records the first one already took.
                let lock = self.lite_lock(&group, lmq);
                let _guard = lock.lock().await;
                let messages = self
                    .pop_queues(
                        &group,
                        lmq,
                        &[LMQ_QUEUE_ID],
                        max_nums - delivered.len(),
                        now,
                        invisible_until,
                        None,
                    )
                    .await?;
                for message in messages {
                    delivered.push((lmq.clone(), message));
                }
            }
            if !delivered.is_empty() || pop_timeout == 0 {
                break;
            }
            if tokio::time::timeout_at(deadline, updates.changed())
                .await
                .is_err()
            {
                break;
            }
        }
        if delivered.is_empty() {
            return Ok(Command::response(request, 0));
        }
        let mut body = Vec::new();
        let mut first_ck = String::new();
        let mut rest_num: u64 = 0;
        for (index, (lmq, message)) in delivered.iter().enumerate() {
            let ck = pop_ck(
                lmq,
                &group,
                message.queue_id,
                message.queue_offset,
                now,
                invisible_until,
            );
            if index == 0 {
                first_ck = ck.clone();
            }
            rest_num = rest_num.saturating_add(
                self.store
                    .queue_max_offset(lmq, message.queue_id)
                    .saturating_sub(message.queue_offset + 1),
            );
            let mut stamped = message.clone();
            stamped.properties.insert(POP_CK.to_owned(), ck);
            body.extend(stamped.encode()?);
        }
        Ok(Command::response(request, 0)
            .with_ext("popCk", first_ck)
            .with_ext("popTime", now)
            .with_ext("invisibleTime", invisible_time)
            .with_ext("reviveQid", 0)
            .with_ext("restNum", rest_num as i64)
            .with_body(body))
    }

    /// The light queues a client reads: the lite topics it subscribed to below
    /// `parent_topic`, named the way `LiteUtil.toLmqName` names them.
    fn light_queues_of(&self, client_id: &str, group: &str, parent_topic: &str) -> Vec<String> {
        let subscriptions = self.lite_subscriptions.lock().unwrap();
        let Some(subscription) = subscriptions.get(client_id) else {
            return Vec::new();
        };
        if subscription.group != group || subscription.bind_topic != parent_topic {
            return Vec::new();
        }
        subscription
            .lite_topics
            .iter()
            .filter_map(|lite| to_lmq_name(parent_topic, lite))
            .collect()
    }

    /// The lock that guards the read of one light queue.
    fn lite_lock(&self, group: &str, lmq: &str) -> Arc<tokio::sync::Mutex<()>> {
        let mut locks = self.lite_locks.lock().unwrap();
        locks
            .entry((group.to_owned(), lmq.to_owned()))
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }

    /// `LiteSubscriptionCtlProcessor.processRequest`: the proxy forwards the lite
    /// subscriptions of a client, so the broker knows which light queues to read.
    async fn lite_subscription_ctl(&self, request: &Command) -> BrokerResult<Command> {
        let client_id = request.required("clientId")?.to_owned();
        let group = request.required("consumerGroup")?.to_owned();
        let bind_topic = request.required("topic")?.to_owned();
        // The client id is a network identity (`ip@instance#unit`), not a topic
        // name, so only its shape is checked (Java `LiteSubscriptionCtlProcessor`).
        if client_id.is_empty() || client_id.len() > 512 {
            return Err(Failure::new(29, "invalid clientId"));
        }
        validate_name(&group)?;
        validate_topic_name(&bind_topic)?;
        let action: i32 = request.field("action")?;
        let lite_topics: Vec<String> = request
            .ext_fields
            .get("liteTopicSet")
            .map(|raw| {
                raw.split(',')
                    .filter(|topic| !topic.is_empty())
                    .map(str::to_owned)
                    .collect()
            })
            .unwrap_or_default();
        let mut subscriptions = self.lite_subscriptions.lock().unwrap();
        let subscription = subscriptions
            .entry(client_id)
            .or_insert_with(|| LiteSubscription {
                group: group.clone(),
                bind_topic: bind_topic.clone(),
                lite_topics: BTreeSet::new(),
            });
        subscription.group = group;
        subscription.bind_topic = bind_topic;
        match action {
            // `LiteSubscriptionAction.PARTIAL_ADD`
            0 => subscription.lite_topics.extend(lite_topics),
            // `LiteSubscriptionAction.PARTIAL_REMOVE`
            1 => subscription
                .lite_topics
                .retain(|topic| !lite_topics.contains(topic)),
            // `LiteSubscriptionAction.COMPLETE_ADD`
            2 => subscription.lite_topics = lite_topics.into_iter().collect(),
            // `LiteSubscriptionAction.COMPLETE_REMOVE`
            3 => subscription.lite_topics.clear(),
            _ => return Err(Failure::new(29, "unknown lite subscription action")),
        }
        Ok(Command::response(request, 0))
    }

    /// `AckMessageProcessor.processRequest`: an ack clears the invisible entry, whether it was
    /// addressed by its receipt handle or by its coordinates.
    async fn ack_pop(&self, request: &Command) -> BrokerResult<Command> {
        // parse_pop_ck yields (topic, group, queue, offset) — the map is keyed (group, topic).
        let (topic, group, queue_id, offset) = match request.ext_fields.get("extraInfo") {
            Some(ck) => parse_pop_ck(ck)
                .ok_or_else(|| Failure::new(3, "the receipt handle does not decode"))?,
            None => {
                let group = request.required("consumerGroup")?.to_owned();
                let topic = request.required("topic")?.to_owned();
                let queue_id: i32 = request.field("queueId")?;
                let offset: u64 = request.field("offset")?;
                (topic, group, queue_id, offset)
            }
        };
        self.pop_invisible
            .lock()
            .unwrap()
            .get_mut(&(group.clone(), topic.clone(), queue_id))
            .map(|entries| entries.remove(&offset));
        // `PopBufferMergeService`: an ack eventually becomes the group's committed offset so a
        // restart does not re-deliver records that were already consumed. Only advancing writes.
        let next = offset + 1;
        let committed = {
            let group = group.clone();
            let topic = topic.clone();
            self.storage(move |store| Ok(store.consumer_offset(&group, &topic, queue_id)?))
                .await
                .ok()
                .flatten()
                .unwrap_or(0)
        };
        if next > committed {
            self.storage(move |store| store.commit_offset(&group, &topic, queue_id, next))
                .await?;
        }
        Ok(Command::response(request, 0))
    }

    /// `ChangeInvisibleTimeProcessor.processRequest`: replace the invisible span of one
    /// outstanding message, addressed the same way an ack is.
    async fn change_invisible(&self, request: &Command) -> BrokerResult<Command> {
        let invisible_time = request.field::<i64>("invisibleTime")?.clamp(0, 3_600_000);
        // parse_pop_ck yields (topic, group, queue, offset) — the map is keyed (group, topic).
        let (topic, group, queue_id, offset) = match request.ext_fields.get("extraInfo") {
            Some(ck) => parse_pop_ck(ck)
                .ok_or_else(|| Failure::new(3, "the receipt handle does not decode"))?,
            None => {
                let group = request.required("consumerGroup")?.to_owned();
                let topic = request.required("topic")?.to_owned();
                let queue_id: i32 = request.field("queueId")?;
                let offset: u64 = request.field("offset")?;
                (topic, group, queue_id, offset)
            }
        };
        let until = now_millis() + invisible_time;
        self.pop_invisible
            .lock()
            .unwrap()
            .get_mut(&(group, topic, queue_id))
            .map(|entries| entries.insert(offset, until as u64));
        Ok(Command::response(request, 0))
    }

    /// `TransactionalMessageService.check`: half messages a producer never resolved are reported
    /// once `transactionTimeOut` passes. The reference asks its producer over the live channel;
    /// this server has no push channel, so the sweep only reports, and the half message stays
    /// pending until an `END_TRANSACTION` arrives. Op records decide what was already resolved.
    pub async fn run_transaction_check(&self, mut shutdown: watch::Receiver<bool>) -> Result<()> {
        let mut pending: BTreeMap<u64, (i64, u32)> = BTreeMap::new(); // offset -> (born_ts, check_times)
        loop {
            let mut wake = TRANS_CHECK_INTERVAL_MS;
            // `TransactionalMessageBridge.fetchOpInfo`: one page of op offsets decides which
            // half records were resolved.
            let op_len = self
                .storage(move |store| Ok(store.queue_max_offset(TRANS_OP_TOPIC, 0)))
                .await
                .unwrap_or(0);
            let resolved: BTreeSet<u64> = if op_len == 0 {
                BTreeSet::new()
            } else {
                match self
                    .storage(move |store| {
                        store.read(TRANS_OP_TOPIC, 0, 0, op_len as usize, MAX_PULL_BYTES, None)
                    })
                    .await
                {
                    Ok(read) => read
                        .messages
                        .iter()
                        .filter_map(|message| {
                            if message.body.len() == 8 {
                                Some(u64::from_be_bytes(message.body[..8].try_into().unwrap()))
                            } else {
                                None
                            }
                        })
                        .collect(),
                    Err(error) => {
                        warn!(error = error.to_string().as_str(), "op topic scan failed");
                        wake = wake.min(SCHEDULE_POLL_MS);
                        BTreeSet::new()
                    }
                }
            };
            pending.retain(|offset, _| !resolved.contains(offset));
            // `TransactionalMessageBridge.fetchHalfMessage`: new half records join the set.
            let cursor = pending.keys().next_back().copied().unwrap_or(0);
            let max = self
                .storage(move |store| Ok(store.queue_max_offset(TRANS_HALF_TOPIC, 0)))
                .await
                .unwrap_or(0);
            if max > cursor {
                if let Ok(read) = self
                    .storage(move |store| {
                        store.read(
                            TRANS_HALF_TOPIC,
                            0,
                            cursor,
                            ((max - cursor) as usize).min(POP_SCAN_BATCH),
                            MAX_PULL_BYTES,
                            None,
                        )
                    })
                    .await
                {
                    for message in read.messages {
                        pending
                            .entry(message.queue_offset)
                            .or_insert((message.store_timestamp, 0));
                    }
                }
            }
            let now = now_millis();
            let mut give_up = Vec::new();
            for (offset, (born, times)) in pending.iter_mut() {
                if now - *born >= TRANS_CHECK_TIMEOUT_MS {
                    *times += 1;
                    if *times > TRANS_CHECK_MAX_TIMES {
                        warn!(
                            half_offset = offset,
                            check_times = *times,
                            "transaction half message check exceeded max, giving up"
                        );
                        give_up.push(*offset);
                    } else {
                        warn!(
                            half_offset = offset,
                            age_ms = now - *born,
                            check_times = *times,
                            "transaction half message is still unresolved"
                        );
                    }
                }
            }
            for offset in give_up {
                pending.remove(&offset);
            }
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(wake.max(1) as u64)) => {}
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow_and_update() {
                        break;
                    }
                }
            }
        }
        Ok(())
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
    /// `QueryMessageProcessor.processRequest` (request code 12): a key-index lookup
    /// with an optional store-time window, or — when no key narrows the search — a
    /// time-range scan with an optional subscription filter. The body frames the hits
    /// as `u32 BE count`, then per message `u32 BE length + record`.
    async fn query_message(&self, request: &Command) -> BrokerResult<Command> {
        let topic = request.required("topic")?.to_owned();
        validate_name(&topic)?;
        let key = request.ext_fields.get("key").cloned().unwrap_or_default();
        let max_num = optional_field(request, "maxNum", 32usize)?.clamp(1, 64);
        let begin: i64 = optional_field(request, "beginTimestamp", 0i64)?;
        let end: i64 = optional_field(request, "endTimestamp", i64::MAX)?;
        let filter = match (
            request.ext_fields.get("expressionType"),
            request.ext_fields.get("expression"),
        ) {
            (Some(expression_type), Some(expression)) if !expression.is_empty() => Some(
                Subscription::build(Some(expression_type), expression)
                    .map_err(|error| Failure::new(23, error.to_string()))?,
            ),
            _ => None,
        };
        let messages = if key.is_empty() {
            self.storage(move |store| {
                store.scan_messages(&topic, begin, end, max_num, filter.as_ref())
            })
            .await?
        } else {
            self.storage(move |store| store.query_messages(&topic, &key, begin, end, max_num))
                .await?
        };
        let mut body = Vec::new();
        body.extend_from_slice(&(messages.len() as u32).to_be_bytes());
        for message in &messages {
            let bytes = message.encode()?;
            body.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
            body.extend_from_slice(&bytes);
        }
        Ok(Command::response(request, 0).with_body(body))
    }

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

        let message = if let Some(origin_topic) = request
            .ext_fields
            .get("originTopic")
            .map(String::as_str)
            .filter(|topic| {
                !topic.is_empty()
                    && self
                        .store
                        .topics()
                        .is_ok_and(|topics| topics.contains_key(*topic))
            }) {
            // `SendMsgBackRequestHeader.originTopic` + `queueId`: the offset names a position in
            // the origin topic's queue, which is what the gRPC proxy's DLQ forward carries.
            let queue_id = optional_field(request, "queueId", 0i32)?;
            let topic = origin_topic.to_owned();
            self.storage(move |store| store.message_by_queue_offset(&topic, queue_id, offset))
                .await?
        } else {
            self.storage(move |store| store.message_at(offset)).await?
        };
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
        // Java `AccessValidator`: every command is authenticated (and, with an
        // access key present, authorized) before its processor runs. A failure
        // surfaces as SYSTEM_ERROR with the auth error as the remark, matching
        // the Java broker's generic exception answer.
        if let Some(acl) = &self.acl {
            if let Err(error) = acl.check(request).await {
                return Err(Failure::new(1, error.to_string()));
            }
        }
        match request.code {
            10 | 310 | 320 => self.send(request, peer).await,
            11 | 361 => self.pull(request).await,
            12 => self.query_message(request).await,
            36 => self.send_back(request).await,
            370 => self.recall(request, peer).await,
            37 => self.end_transaction(request).await,
            200050 => self.pop(request).await,
            200051 => self.ack_pop(request).await,
            200053 => self.change_invisible(request).await,
            // `RequestCode.POP_LITE_MESSAGE`: read the light queues of a lite consumer.
            200070 => self.pop_lite(request).await,
            // `RequestCode.LITE_SUBSCRIPTION_CTL`: register the lite subscriptions of a client.
            200071 => self.lite_subscription_ctl(request).await,
            34 => self.heartbeat(request, peer).await,
            35 => {
                let client_id = request.required("clientID")?;
                let before: BTreeSet<String> = self.groups.lock().unwrap().keys().cloned().collect();
                {
                    let mut groups = self.groups.lock().unwrap();
                    for info in groups.values_mut() {
                        info.clients.remove(client_id);
                    }
                }
                let mut peer_channels = self.peer_channels.lock().unwrap();
                if peer_channels
                    .get(&peer)
                    .map(|id| id.as_str() == client_id)
                    .unwrap_or(false)
                {
                    peer_channels.remove(&peer);
                }
                self.expire_groups(&before);
                Ok(Command::response(request, 0))
            }
            38 => {
                let group = request.required("consumerGroup")?;
                let groups = self.groups.lock().unwrap();
                match groups.get(group) {
                    Some(info) => {
                        let ids: BTreeSet<String> =
                            info.clients.iter().cloned().collect();
                        json_response(request, json!({"consumerIdList": ids}))
                    }
                    None => Err(Failure::new(1, "the consumer's group info does not exist")),
                }
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
                if request.ext_fields.get("topicFilterType").is_some_and(|v| v != "SINGLE_TAG") {
                    return Err(Failure::new(3, "custom topic filter types are not implemented"));
                }
                // Java `TopicConfig.attributes`: a free-form map such as
                // `{"+message.type": "FIFO"}` (`AttributeParser.ATTR_ADD_PLUS_SIGN`
                // marks an attribute as user-settable). The proxy reads it to decide
                // which message types the topic accepts.
                if let Some(raw) = request.ext_fields.get("attributes").filter(|v| !v.is_empty()) {
                    let attributes: BTreeMap<String, String> = serde_json::from_str(raw).map_err(
                        |_| Failure::new(29, "attributes must be a JSON object of string values"),
                    )?;
                    for key in attributes.keys() {
                        if !key.starts_with('+') && !key.starts_with('-') {
                            return Err(Failure::new(
                                29,
                                "attribute keys must start with '+' or '-'",
                            ));
                        }
                    }
                    config.attributes = attributes;
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
            // `AdminBrokerProcessor.getSubscriptionGroup`: one group's config by name.
            // The Java proxy reads it through this code to learn the consumer group's
            // retry policy before it merges the client's `Settings`; a group the
            // broker does not know answers `ResponseCode.SUBSCRIPTION_GROUP_NOT_EXIST`.
            352 => {
                let group = request.required("group")?.to_owned();
                match self.group_config(&group).await? {
                    Some(config) => Ok(Command::response(request, 0)
                        .with_body(serde_json::to_vec(&config).map_err(Error::from)?)),
                    None => Ok(Command::response(request, 26)
                        .with_remark("No group in this broker")),
                }
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
                "clientCount":self.groups.lock().unwrap().len().to_string()
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
        // A light message queue carries one queue of its own; its offsets are
        // committed against the light queue name, not the topic it hangs below.
        if is_lite_topic_queue(topic) {
            if queue_id != 0 {
                return Err(Failure::new(29, "queueId out of range"));
            }
            return Ok(());
        }
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
        let before: BTreeSet<String> = self.groups.lock().unwrap().keys().cloned().collect();
        // Only drop the channel marker; keep the group's subscription so a proxy that
        // recycles its broker connection self-heals via compensate-on-pull (Java parity).
        if let Some(client_id) = self.peer_channels.lock().unwrap().remove(&peer) {
            let mut groups = self.groups.lock().unwrap();
            for info in groups.values_mut() {
                info.clients.remove(&client_id);
            }
        }
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

/// The receipt handle one pop hands out. Names may carry `|` but never `,`, so the comma is a
/// safe separator; the caller only ever decodes handles this broker produced.
fn pop_ck(
    topic: &str,
    group: &str,
    queue_id: i32,
    offset: u64,
    pop_time: i64,
    invisible_until: i64,
) -> String {
    common::pop_ck(topic, group, queue_id, offset, pop_time, invisible_until)
}

/// Decodes a receipt handle back to its coordinates; the timestamps are informational.
fn parse_pop_ck(ck: &str) -> Option<(String, String, i32, u64)> {
    common::parse_pop_ck(ck)
}

fn json_response(request: &Command, value: serde_json::Value) -> BrokerResult<Command> {
    Ok(Command::response(request, 0).with_body(serde_json::to_vec(&value).map_err(Error::from)?))
}

/// `SendMessageProcessor.sendMessage`: a lite message is dispatched to the light
/// queue of its bind topic, which gives the lite topic a queue and an offset of
/// its own instead of sharing the bind topic's.
fn apply_lite_dispatch(message: &mut Message) {
    let Some(lite_topic) = message.properties.get(PROPERTY_LITE_TOPIC).cloned() else {
        return;
    };
    if lite_topic.is_empty() {
        return;
    }
    let Some(lmq) = to_lmq_name(&message.topic, &lite_topic) else {
        return;
    };
    message
        .properties
        .insert(PROPERTY_INNER_MULTI_DISPATCH.to_owned(), lmq);
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

    async fn test_broker() -> (TempDir, Broker) {
        let dir = TempDir::new().unwrap();
        let store = Arc::new(Store::open(dir.path()).unwrap());
        let broker = Broker::new(store, BrokerConfig::default()).unwrap();
        (dir, broker)
    }

    fn peer() -> SocketAddr {
        SocketAddr::from(([127, 0, 0, 1], 5_555))
    }

    fn encode_props(properties: &[(&str, &str)]) -> String {
        properties
            .iter()
            .map(|(key, value)| format!("{key}\x01{value}\x02"))
            .collect()
    }

    fn send_request(topic: &str, body: &[u8], properties: &str, sys_flag: i32) -> Command {
        // Code 310 is the v2 send: the broker reads the short-name header fields, and the
        // auto-create branch needs the default-topic template.
        Command::request(310)
            .with_ext("b", topic)
            .with_ext("a", "g")
            .with_ext("c", "TBW102")
            .with_ext("d", 8)
            .with_ext("e", 0)
            .with_ext("f", sys_flag)
            .with_ext("h", 0)
            .with_ext("g", now_millis())
            .with_body(body.to_vec())
            .with_ext("i", properties)
    }

    /// A half message's msg id ends with its physical offset in hex, like the reference's.
    fn physical_offset_from_msg_id(msg_id: &str) -> u64 {
        u64::from_str_radix(&msg_id[msg_id.len() - 16..], 16).unwrap()
    }

    fn decode_records(body: &[u8]) -> Vec<Message> {
        let mut messages = Vec::new();
        let mut rest = body;
        while !rest.is_empty() {
            let (message, consumed) = Message::decode(rest).unwrap();
            rest = &rest[consumed..];
            messages.push(message);
        }
        messages
    }

    #[tokio::test]
    async fn transactional_messages_resolve_through_end_transaction() {
        let (_dir, broker) = test_broker().await;
        let send = send_request(
            "orders",
            b"payload",
            &encode_props(&[("TRAN_MSG", "true")]),
            0,
        );
        let response = broker.process(&send, peer()).await.unwrap();
        // The half record lands on the half topic, never on the real one.
        assert_eq!(broker.store.queue_max_offset(TRANS_HALF_TOPIC, 0), 1);
        assert_eq!(broker.store.queue_max_offset("orders", 0), 0);
        let half = broker
            .store
            .message_by_queue_offset(TRANS_HALF_TOPIC, 0, 0)
            .unwrap()
            .unwrap();
        assert_eq!(half.sys_flag & TRANS_PREPARED_FLAG, TRANS_PREPARED_FLAG);
        assert_eq!(half.properties.get(schedule::REAL_TOPIC).unwrap(), "orders");
        assert_eq!(half.properties.get(REAL_QID).unwrap(), "0");
        assert_eq!(response.ext_fields["msgId"], half.message_id());
        let parsed = physical_offset_from_msg_id(&response.ext_fields["msgId"]);
        assert_eq!(parsed, half.physical_offset);
        assert_eq!(response.ext_fields["transactionId"], half.message_id());
        // COMMIT restores the real destination with the prepared bit cleared.
        let commit = Command::request(37)
            .with_ext("commitLogOffset", half.physical_offset)
            .with_ext("commitOrRollback", 0)
            .with_ext("producerGroup", "g");
        broker.process(&commit, peer()).await.unwrap();
        assert_eq!(broker.store.queue_max_offset("orders", 0), 1);
        assert_eq!(broker.store.queue_max_offset(TRANS_OP_TOPIC, 0), 1);
        let delivered = broker
            .store
            .message_by_queue_offset("orders", 0, 0)
            .unwrap()
            .unwrap();
        assert_eq!(delivered.body, b"payload");
        assert_eq!(delivered.sys_flag & TRANS_PREPARED_FLAG, 0);
        assert!(!delivered.properties.contains_key(schedule::REAL_TOPIC));
        assert!(!delivered.properties.contains_key(TRAN_MSG));
        // ROLLBACK discards the record and appends the op marker only.
        let second_send = send_request(
            "orders",
            b"payload2",
            &encode_props(&[("TRAN_MSG", "true")]),
            0,
        );
        let second = broker.process(&second_send, peer()).await.unwrap();
        let second_half = broker
            .store
            .message_by_queue_offset(TRANS_HALF_TOPIC, 0, 1)
            .unwrap()
            .unwrap();
        let rollback = Command::request(37)
            .with_ext(
                "commitLogOffset",
                physical_offset_from_msg_id(&second.ext_fields["msgId"]),
            )
            .with_ext("commitOrRollback", 1)
            .with_ext("producerGroup", "g");
        broker.process(&rollback, peer()).await.unwrap();
        assert_eq!(broker.store.queue_max_offset("orders", 0), 1);
        assert_eq!(broker.store.queue_max_offset(TRANS_OP_TOPIC, 0), 2);
        let _ = second_half;
        // The gRPC proxy sends END_TRANSACTION without a producer group.
        let third_send = send_request(
            "orders",
            b"payload3",
            &encode_props(&[("TRAN_MSG", "true")]),
            0,
        );
        let third = broker.process(&third_send, peer()).await.unwrap();
        let commit = Command::request(37)
            .with_ext(
                "commitLogOffset",
                physical_offset_from_msg_id(&third.ext_fields["msgId"]),
            )
            .with_ext("commitOrRollback", 0);
        broker.process(&commit, peer()).await.unwrap();
        assert_eq!(broker.store.queue_max_offset("orders", 0), 2);
        // A bogus physical offset is refused outright.
        let wrong = Command::request(37)
            .with_ext("commitLogOffset", 999_999_999u64)
            .with_ext("commitOrRollback", 0)
            .with_ext("producerGroup", "g");
        assert!(broker.process(&wrong, peer()).await.is_err());
    }

    #[tokio::test]
    async fn send_back_resolves_queue_offsets_through_the_origin_topic() {
        let (_dir, broker) = test_broker().await;
        for index in 0..2 {
            let send = send_request("orders", format!("m{index}").as_bytes(), "", 0);
            broker.process(&send, peer()).await.unwrap();
        }
        let group_config = crate::schedule::GroupConfig::new("back-group");
        broker.store().put_group(group_config).unwrap();
        // delayLevel < 0 routes the record straight into the dead-letter topic; the offset is
        // the origin queue offset the proxy's receipt handle carries.
        let send_back = Command::request(36)
            .with_ext("group", "back-group")
            .with_ext("offset", 1u64)
            .with_ext("delayLevel", -1)
            .with_ext("originTopic", "orders")
            .with_ext("queueId", 0);
        broker.process(&send_back, peer()).await.unwrap();
        assert_eq!(broker.store.queue_max_offset("%DLQ%back-group", 0), 1);
        let dlq_record = broker
            .store
            .message_by_queue_offset("%DLQ%back-group", 0, 0)
            .unwrap()
            .unwrap();
        assert_eq!(dlq_record.body, b"m1");
        assert_eq!(dlq_record.properties.get(schedule::REAL_TOPIC), None);
        assert_eq!(
            dlq_record
                .properties
                .get(schedule::RETRY_TOPIC)
                .map(String::as_str),
            Some("orders")
        );
        // An origin topic the broker does not host falls back to the physical lookup.
        let legacy = Command::request(36)
            .with_ext("group", "back-group")
            .with_ext("offset", 0u64)
            .with_ext("delayLevel", -1)
            .with_ext("originTopic", "unused");
        broker.process(&legacy, peer()).await.unwrap();
        assert_eq!(broker.store.queue_max_offset("%DLQ%back-group", 0), 2);
    }

    #[tokio::test]
    async fn pop_acks_persist_the_consumer_offset() {
        let (_dir, broker) = test_broker().await;
        for index in 0..2 {
            let send = send_request("orders", format!("m{index}").as_bytes(), "", 0);
            broker.process(&send, peer()).await.unwrap();
        }
        let pop = Command::request(200050)
            .with_ext("consumerGroup", "cg")
            .with_ext("topic", "orders")
            .with_ext("queueId", 0)
            .with_ext("maxMsgNums", 1)
            .with_ext("invisibleTime", 100);
        let first = broker.process(&pop, peer()).await.unwrap();
        let delivered = decode_records(&first.body);
        assert_eq!(delivered.len(), 1);
        assert_eq!(delivered[0].body, b"m0");
        // Nothing is committed before the ack.
        assert_eq!(
            broker
                .store
                .consumer_offset("cg", "orders", 0)
                .unwrap()
                .unwrap_or(0),
            0
        );
        let ack = Command::request(200051)
            .with_ext("extraInfo", delivered[0].properties[POP_CK].as_str());
        broker.process(&ack, peer()).await.unwrap();
        assert_eq!(
            broker
                .store
                .consumer_offset("cg", "orders", 0)
                .unwrap()
                .unwrap_or(0),
            1
        );
        // A stale ack never moves the offset backwards.
        let stale = Command::request(200051)
            .with_ext("extraInfo", delivered[0].properties[POP_CK].as_str());
        broker.process(&stale, peer()).await.unwrap();
        assert_eq!(
            broker
                .store
                .consumer_offset("cg", "orders", 0)
                .unwrap()
                .unwrap_or(0),
            1
        );
    }

    #[tokio::test]
    async fn pop_delivers_stamps_receipts_and_revives_expired_entries() {
        let (_dir, broker) = test_broker().await;
        for index in 0..3 {
            let send = send_request("orders", format!("m{index}").as_bytes(), "", 0);
            broker.process(&send, peer()).await.unwrap();
        }
        let pop = || {
            Command::request(200050)
                .with_ext("consumerGroup", "cg")
                .with_ext("topic", "orders")
                .with_ext("queueId", 0)
                .with_ext("maxMsgNums", 2)
                .with_ext("invisibleTime", 100)
        };
        let first = broker.process(&pop(), peer()).await.unwrap();
        let first_batch = decode_records(&first.body);
        assert_eq!(first_batch.len(), 2);
        for message in &first_batch {
            assert!(message.body == b"m0" || message.body == b"m1");
        }
        assert!(first_batch[0].properties.contains_key(POP_CK));
        assert!(first.ext_fields.contains_key("popCk"));
        assert!(first.ext_fields.contains_key("restNum"));
        // The cursor advanced past both delivered offsets, so the third is next.
        let second = broker.process(&pop(), peer()).await.unwrap();
        let second_batch = decode_records(&second.body);
        assert_eq!(second_batch.len(), 1);
        assert_eq!(second_batch[0].body, b"m2");
        // Everything outstanding sits inside its invisible span.
        let third = broker.process(&pop(), peer()).await.unwrap();
        assert!(decode_records(&third.body).is_empty());
        // An ack clears one entry, a longer invisible span defers another.
        let ack = Command::request(200051)
            .with_ext("extraInfo", first_batch[0].properties[POP_CK].as_str());
        broker.process(&ack, peer()).await.unwrap();
        let change = Command::request(200053)
            .with_ext("extraInfo", second_batch[0].properties[POP_CK].as_str())
            .with_ext("invisibleTime", 3_600_000);
        broker.process(&change, peer()).await.unwrap();
        tokio::time::sleep(Duration::from_millis(150)).await;
        let fourth = broker.process(&pop(), peer()).await.unwrap();
        let revived = decode_records(&fourth.body);
        // The acked offset stays consumed, the deferred one stays invisible, only the
        // middle offset comes back.
        assert_eq!(revived.len(), 1);
        assert_eq!(revived[0].body, b"m1");
    }

    #[tokio::test]
    async fn inner_batch_records_split_into_queue_entries() {
        let (_dir, broker) = test_broker().await;
        let sub = |body: &[u8]| Message {
            topic: "orders".into(),
            queue_id: 0,
            body: body.to_vec(),
            born_host: peer(),
            store_host: peer(),
            ..Message::default()
        };
        let mut body = sub(b"first").encode().unwrap();
        body.extend(sub(b"second").encode().unwrap());
        let send = send_request("orders", &body, "", INNER_BATCH_FLAG);
        broker.process(&send, peer()).await.unwrap();
        assert_eq!(broker.store.queue_max_offset("orders", 0), 2);
        let first = broker
            .store
            .message_by_queue_offset("orders", 0, 0)
            .unwrap()
            .unwrap();
        assert_eq!(first.body, b"first");
        assert_eq!(first.sys_flag & INNER_BATCH_FLAG, INNER_BATCH_FLAG);
        let second = broker
            .store
            .message_by_queue_offset("orders", 0, 1)
            .unwrap()
            .unwrap();
        assert_eq!(second.body, b"second");
    }
}
