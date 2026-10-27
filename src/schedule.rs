//! Delay levels, accurate timer messages and the retry/dead-letter topics that consumers use.
//!
//! The reference keeps a delayed message in an internal queue and re-appends it when it is due, so
//! the observable contract is "accepted now, visible on the real topic later". This module owns the
//! pure transformations; [`crate::broker`] owns the threads that run them.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::common::{Error, Message, Result};

/// `TopicValidator.RMQ_SYS_SCHEDULE_TOPIC`
pub const SCHEDULE_TOPIC: &str = "SCHEDULE_TOPIC_XXXX";
/// `TimerMessageStore.TIMER_TOPIC`
pub const TIMER_TOPIC: &str = "rmq_sys_wheel_timer";
/// `MixAll.RETRY_GROUP_TOPIC_PREFIX`
pub const RETRY_PREFIX: &str = "%RETRY%";
/// `MixAll.DLQ_GROUP_TOPIC_PREFIX`
pub const DLQ_PREFIX: &str = "%DLQ%";
/// `AbstractSendMessageProcessor.DLQ_NUMS_PER_GROUP`
pub const DLQ_QUEUES: i32 = 1;
/// `TopicConfigManager.SCHEDULE_TOPIC_QUEUE_NUM`
pub const SCHEDULE_QUEUES: i32 = 18;
/// `MessageStoreConfig.messageDelayLevel`
pub const DEFAULT_DELAY_LEVELS: &str = "1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h";
/// `MessageStoreConfig.timerPrecisionMs`
pub const DEFAULT_TIMER_PRECISION_MS: i64 = 1_000;
/// `MessageStoreConfig.timerMaxDelaySec`
pub const DEFAULT_TIMER_MAX_DELAY_SEC: i64 = 3600 * 24 * 3;
/// `MessageStoreConfig.timerFlushIntervalMs`
pub const DEFAULT_TIMER_FLUSH_INTERVAL_MS: i64 = 1_000;
/// `MessageStoreConfig.timerCongestNumEachSlot`, whose default makes rejection unreachable.
pub const DEFAULT_TIMER_CONGEST_NUM_EACH_SLOT: i64 = i32::MAX as i64;
/// `TimerMessageStore.TIMER_WHEEL_TTL_DAY`
pub const TIMER_WHEEL_TTL_DAY: i64 = 7;
/// `TimerMessageStore.TIMER_BLANK_SLOTS`
pub const TIMER_BLANK_SLOTS: i64 = 60;
/// `MQVersion.Version.V3_4_9.ordinal()`; older clients may not override `retryMaxTimes`.
pub const V3_4_9: i32 = 121;

pub const DELAY: &str = "DELAY";
pub const REAL_TOPIC: &str = "REAL_TOPIC";
pub const REAL_QUEUE_ID: &str = "REAL_QID";
pub const RETRY_TOPIC: &str = "RETRY_TOPIC";
pub const ORIGIN_MESSAGE_ID: &str = "ORIGIN_MESSAGE_ID";
pub const UNIQ_KEY: &str = "UNIQ_KEY";
pub const TIMER_DELIVER_MS: &str = "TIMER_DELIVER_MS";
pub const TIMER_DELAY_SEC: &str = "TIMER_DELAY_SEC";
pub const TIMER_DELAY_MS: &str = "TIMER_DELAY_MS";
pub const TIMER_OUT_MS: &str = "TIMER_OUT_MS";
pub const TIMER_DEL_UNIQKEY: &str = "TIMER_DEL_UNIQKEY";
pub const TIMER_ENQUEUE_MS: &str = "TIMER_ENQUEUE_MS";
pub const TIMER_DEQUEUE_MS: &str = "TIMER_DEQUEUE_MS";
pub const TIMER_ROLL_TIMES: &str = "TIMER_ROLL_TIMES";
/// `MessageConst.PROPERTY_PRODUCER_GROUP`
pub const PRODUCER_GROUP: &str = "PGROUP";
/// `MessageConst.PROPERTY_BORN_TIMESTAMP`
pub const BORN_TIMESTAMP: &str = "BORN_TIMESTAMP";
/// `MessageConst.PROPERTY_TRACE_CONTEXT`
pub const TRACE_CONTEXT: &str = "TRACE_CONTEXT";
/// `MessageConst.PROPERTY_TAGS`
pub const TAGS: &str = "TAGS";
/// `RecallMessageProcessor.RECALL_MESSAGE_TAG`
pub const RECALL_TAG: &str = "_RECALL_TAG_";
/// The body `RecallMessageProcessor.buildMessage` writes for a delete record.
pub const RECALL_BODY: &[u8] = b"0";

/// `ScheduleMessageService.delayLevel2QueueId`
pub fn delay_level_to_queue_id(level: i32) -> i32 {
    level - 1
}

/// `ScheduleMessageService.queueId2DelayLevel`
pub fn queue_id_to_delay_level(queue_id: i32) -> i32 {
    queue_id + 1
}

/// `MixAll.getRetryTopic`
pub fn retry_topic(group: &str) -> String {
    format!("{RETRY_PREFIX}{group}")
}

/// `MixAll.getDLQTopic`
pub fn dlq_topic(group: &str) -> String {
    format!("{DLQ_PREFIX}{group}")
}

pub fn is_retry_topic(topic: &str) -> bool {
    topic.starts_with(RETRY_PREFIX)
}

/// `KeyBuilder.parseGroup`, including the pop retry V2 form `%RETRY%group+topic`.
pub fn parse_group(topic: &str) -> &str {
    let body = topic.trim_start_matches(RETRY_PREFIX);
    match body.split_once('+') {
        Some((group, _)) if !group.is_empty() => group,
        _ => body,
    }
}

/// `MessageExt.getDelayTimeLevel`, which propagates a malformed property as a parse error.
pub fn delay_level(message: &Message) -> Result<i32> {
    match message.properties.get(DELAY) {
        None => Ok(0),
        Some(value) => value
            .parse::<i32>()
            .map_err(|_| Error::Invalid(format!("invalid {DELAY} property: {value}"))),
    }
}

/// `ScheduleMessageService.parseDelayLevel`, which fails the broker start on a malformed table.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DelayLevels {
    table: BTreeMap<i32, i64>,
}

impl DelayLevels {
    pub fn parse(levels: &str) -> Result<Self> {
        const UNITS: [(&str, i64); 4] = [
            ("s", 1_000),
            ("m", 60 * 1_000),
            ("h", 3_600 * 1_000),
            ("d", 86_400 * 1_000),
        ];
        let mut table = BTreeMap::new();
        // Java splits on the single space, so a doubled space yields an empty level and fails.
        for (index, value) in levels.split(' ').enumerate() {
            let level = index as i32 + 1;
            let unit = value
                .chars()
                .last()
                .ok_or_else(|| Error::Invalid(format!("empty delay level at position {level}")))?;
            let unit = unit.to_string();
            let multiplier = UNITS
                .iter()
                .find(|(name, _)| *name == unit)
                .map(|(_, value)| *value)
                .ok_or_else(|| {
                    Error::Invalid(format!(
                        "unknown delay level unit {unit:?}, expecting s, m, h or d"
                    ))
                })?;
            let count: i64 = value[..value.len() - unit.len()]
                .parse()
                .map_err(|_| Error::Invalid(format!("invalid delay level {value:?}")))?;
            table.insert(
                level,
                multiplier
                    .checked_mul(count)
                    .ok_or_else(|| Error::Invalid(format!("delay level {value:?} overflows")))?,
            );
        }
        if table.is_empty() {
            return Err(Error::Invalid("empty delay level table".into()));
        }
        Ok(Self { table })
    }

    /// `ScheduleMessageService.getMaxDelayLevel`
    pub fn max(&self) -> i32 {
        *self.table.keys().next_back().unwrap_or(&0)
    }

    pub fn delay_ms(&self, level: i32) -> Option<i64> {
        self.table.get(&level).copied()
    }

    pub fn levels(&self) -> impl Iterator<Item = i32> + Clone {
        self.table.keys().copied()
    }

    /// `ScheduleMessageService.computeDeliverTimestamp`; an unknown level delivers one second later.
    pub fn deliver_timestamp(&self, level: i32, store_timestamp: i64) -> i64 {
        self.delay_ms(level)
            .map_or(store_timestamp.saturating_add(1_000), |delay| {
                store_timestamp.saturating_add(delay)
            })
    }

    /// `DeliverDelayedMessageTimerTask.correctDeliverTimestamp`, which repairs a skewed clock.
    pub fn correct_deliver_timestamp(&self, now: i64, level: i32, deliver: i64) -> i64 {
        match self.delay_ms(level) {
            Some(delay) if deliver > now.saturating_add(delay) => now,
            _ => deliver,
        }
    }
}

impl Default for DelayLevels {
    fn default() -> Self {
        Self::parse(DEFAULT_DELAY_LEVELS).expect("the built-in delay level table is well formed")
    }
}

/// `MessageStoreConfig` fields the schedule and timer hooks consult.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TimerSettings {
    pub wheel_enable: bool,
    pub precision_ms: i64,
    pub max_delay_ms: i64,
    /// `MessageStoreConfig.timerFlushIntervalMs`, the checkpoint cadence.
    pub flush_interval_ms: i64,
    /// `MessageStoreConfig.timerCongestNumEachSlot`, the per-slot congestion limit.
    pub congest_num_each_slot: i64,
}

impl Default for TimerSettings {
    fn default() -> Self {
        Self {
            wheel_enable: true,
            precision_ms: DEFAULT_TIMER_PRECISION_MS,
            max_delay_ms: DEFAULT_TIMER_MAX_DELAY_SEC.saturating_mul(1_000),
            flush_interval_ms: DEFAULT_TIMER_FLUSH_INTERVAL_MS,
            congest_num_each_slot: DEFAULT_TIMER_CONGEST_NUM_EACH_SLOT,
        }
    }
}

impl TimerSettings {
    /// The wheel tick that drains a slot: the reference floors the deadline *down* to the slot
    /// that precedes it, so the slot is emptied one precision interval after its stamp.
    pub fn drain_ms(&self, deliver_ms: i64) -> i64 {
        deliver_ms.saturating_add(self.precision_ms.max(1))
    }

    /// `TimerWheel.getSlot`: an entry older than the whole wheel plus its blank slots can no
    /// longer be placed, and the reference loses it at recovery.
    pub fn is_too_old(&self, deliver_ms: i64, now: i64) -> bool {
        let span = TIMER_WHEEL_TTL_DAY
            .saturating_mul(86_400_000)
            .saturating_sub(TIMER_BLANK_SLOTS.saturating_mul(self.precision_ms.max(1)));
        self.drain_ms(deliver_ms) < now.saturating_sub(span)
    }
}

/// Where a message has to be parked before it becomes visible, mirroring `HookUtils`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Parking {
    /// Stored on the topic the producer named.
    None,
    /// `SCHEDULE_TOPIC_XXXX` queue `level - 1`, delivered `storeTimestamp + level delay`.
    Delay { level: i32 },
    /// `rmq_sys_wheel_timer` queue 0, delivered at the floored timestamp.
    Timer { deliver_ms: i64 },
}

/// The three `PutMessageStatus` values the timer path can produce.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TimerReject {
    /// `WHEEL_TIMER_NOT_ENABLE`.
    NotEnabled,
    /// `WHEEL_TIMER_MSG_ILLEGAL`.
    MessageIllegal,
    /// `WHEEL_TIMER_FLOW_CONTROL`.
    FlowControl,
}

impl TimerReject {
    /// The `ResponseCode` `SendMessageProcessor` answers each status with.
    pub fn code(self) -> i32 {
        match self {
            Self::MessageIllegal => 13,
            Self::NotEnabled | Self::FlowControl => 1,
        }
    }

    /// The `SendMessageProcessor.handlePutMessageResult` remark, `String.format` included.
    pub fn remark(self, settings: &TimerSettings) -> String {
        let limit = settings.congest_num_each_slot;
        match self {
            Self::NotEnabled => format!(
                "accurate timer message is not enabled, timerWheelEnable is {}",
                settings.wheel_enable
            ),
            Self::MessageIllegal => format!(
                "timer message illegal, the delay time should not be bigger than the max delay {}ms; or if set del msg, the delay time should be bigger than the current time",
                settings.max_delay_ms
            ),
            Self::FlowControl => format!(
                "timer message is under flow control, max num limit is {} or the current value is greater than {} and less than {}, trigger random flow control",
                limit.saturating_mul(2),
                limit,
                limit.saturating_mul(2)
            ),
        }
    }
}

/// `TimerMessageStore.isReject`, whose middle band the reference decides with `RANDOM`.
/// `roll` is that draw, a number in `0..1000`, so the caller keeps the choice deterministic.
pub fn is_reject(congest_num: i64, limit: i64, roll: i64) -> bool {
    if congest_num <= limit {
        return false;
    }
    if congest_num >= limit.saturating_mul(2) {
        return true;
    }
    roll > 1_000 * (congest_num - limit) / (limit + 1)
}

/// Anything the schedule hook can reject a put with.
#[derive(Debug)]
pub enum ScheduleError {
    /// A `PutMessageStatus` from the accurate timer path.
    Timer(TimerReject),
    /// `MessageExt.getDelayTimeLevel` throwing on a malformed `DELAY` property.
    MalformedDelay(Error),
}

impl ScheduleError {
    pub fn code(&self) -> i32 {
        match self {
            Self::Timer(reject) => reject.code(),
            // The reference answers an uncaught exception with `ResponseCode.SYSTEM_ERROR`.
            Self::MalformedDelay(_) => 1,
        }
    }

    pub fn remark(&self, settings: &TimerSettings) -> String {
        match self {
            Self::Timer(reject) => reject.remark(settings),
            Self::MalformedDelay(error) => error.to_string(),
        }
    }
}

/// `HookUtils.checkIfTimerMessage`: a delay level wins and clears the timer properties.
pub fn is_timer_message(message: &mut Message) -> std::result::Result<bool, ScheduleError> {
    let level = delay_level(message).map_err(ScheduleError::MalformedDelay)?;
    if level > 0 {
        for key in [TIMER_DELIVER_MS, TIMER_DELAY_SEC, TIMER_DELAY_MS] {
            message.properties.remove(key);
        }
        return Ok(false);
    }
    if message.topic == TIMER_TOPIC || message.properties.contains_key(TIMER_OUT_MS) {
        return Ok(false);
    }
    Ok([TIMER_DELIVER_MS, TIMER_DELAY_MS, TIMER_DELAY_SEC]
        .iter()
        .any(|key| message.properties.contains_key(*key)))
}

/// `HookUtils.transformTimerMessage`. `now` is the broker clock and `reject` the wheel's own
/// congestion check (`TimerMessageStore.isReject`), which only runs for a future delivery.
pub fn transform_timer_message(
    message: &mut Message,
    settings: &TimerSettings,
    now: i64,
    reject: impl Fn(i64) -> bool,
) -> std::result::Result<Parking, ScheduleError> {
    if !settings.wheel_enable {
        return Err(ScheduleError::Timer(TimerReject::NotEnabled));
    }
    let deliver_ms = match (
        message.properties.get(TIMER_DELAY_SEC),
        message.properties.get(TIMER_DELAY_MS),
        message.properties.get(TIMER_DELIVER_MS),
    ) {
        (Some(seconds), _, _) => seconds
            .parse::<i64>()
            .ok()
            .and_then(|seconds| seconds.checked_mul(1_000))
            .and_then(|delay| delay.checked_add(now)),
        (_, Some(millis), _) => millis
            .parse::<i64>()
            .ok()
            .and_then(|delay| delay.checked_add(now)),
        (_, _, Some(target)) => target.parse::<i64>().ok(),
        _ => None,
    }
    .ok_or(ScheduleError::Timer(TimerReject::MessageIllegal))?;
    if deliver_ms > now {
        if deliver_ms - now > settings.max_delay_ms {
            return Err(ScheduleError::Timer(TimerReject::MessageIllegal));
        }
        let precision = settings.precision_ms.max(1);
        // The wheel stores the slot that *precedes* the deadline, so an exact multiple moves back.
        let deliver_ms = if deliver_ms % precision == 0 {
            deliver_ms - precision
        } else {
            deliver_ms / precision * precision
        };
        if reject(deliver_ms) {
            return Err(ScheduleError::Timer(TimerReject::FlowControl));
        }
        message
            .properties
            .insert(TIMER_OUT_MS.into(), deliver_ms.to_string());
        message
            .properties
            .insert(REAL_TOPIC.into(), message.topic.clone());
        message
            .properties
            .insert(REAL_QUEUE_ID.into(), message.queue_id.to_string());
        message.topic = TIMER_TOPIC.to_owned();
        message.queue_id = 0;
        return Ok(Parking::Timer { deliver_ms });
    }
    if message.properties.contains_key(TIMER_DEL_UNIQKEY) {
        return Err(ScheduleError::Timer(TimerReject::MessageIllegal));
    }
    // An already expired deadline stores the message on its own topic, timer properties included.
    Ok(Parking::None)
}

/// `HookUtils.handleScheduleMessage` plus `transformDelayLevelMessage`.
pub fn handle_schedule_message(
    message: &mut Message,
    levels: &DelayLevels,
    timer: &TimerSettings,
    now: i64,
    reject: impl Fn(i64) -> bool,
) -> std::result::Result<Parking, ScheduleError> {
    if message.topic != TIMER_TOPIC && is_timer_message(message)? {
        return transform_timer_message(message, timer, now, reject);
    }
    let level = delay_level(message).map_err(ScheduleError::MalformedDelay)?;
    if level > 0 {
        let level = level.min(levels.max());
        message
            .properties
            .insert(REAL_TOPIC.into(), message.topic.clone());
        message
            .properties
            .insert(REAL_QUEUE_ID.into(), message.queue_id.to_string());
        message.properties.insert(DELAY.into(), level.to_string());
        message.topic = SCHEDULE_TOPIC.to_owned();
        message.queue_id = delay_level_to_queue_id(level);
        return Ok(Parking::Delay { level });
    }
    Ok(Parking::None)
}

/// `ScheduleMessageService.messageTimeUp`: the record that is written when a delay level expires.
///
/// Only `DELAY`, `TIMER_DELIVER_MS` and `TIMER_DELAY_SEC` are removed; `TIMER_DELAY_MS` and
/// `TIMER_OUT_MS` stay visible on the delivered message exactly like the reference.
pub fn message_time_up(message: &Message) -> Result<Option<Message>> {
    let mut message = message.clone();
    let Some(topic) = message.properties.get(REAL_TOPIC).cloned() else {
        // Without a real topic the reference would deliver into `null`; keep the record parked
        // rather than inventing a destination.
        return Ok(None);
    };
    let queue_id = message
        .properties
        .get(REAL_QUEUE_ID)
        .and_then(|value| value.parse::<i32>().ok());
    message.topic = topic;
    message.queue_id = queue_id.unwrap_or(0);
    message.properties.remove(DELAY);
    message.properties.remove(TIMER_DELIVER_MS);
    message.properties.remove(TIMER_DELAY_SEC);
    Ok(Some(message))
}

/// `TimerMessageStore.convertMessage`: the record a timer slot hands back to the real topic.
///
/// Unlike `message_time_up`, the timer engine clears only the two routing properties, so
/// `TIMER_DELAY_*`, `TIMER_DELIVER_MS` and `TIMER_OUT_MS` all stay visible on the delivered
/// message, and it stamps the enqueue and dequeue times. `enqueue_ms` is `i64::MAX` for a record
/// that never reached the wheel because it was already due when it was scanned.
pub fn convert_timer_message(message: &Message, enqueue_ms: i64, now: i64) -> Option<Message> {
    let mut message = message.clone();
    let topic = message.properties.remove(REAL_TOPIC)?;
    let queue_id = message
        .properties
        .remove(REAL_QUEUE_ID)?
        .parse::<i32>()
        .ok()?;
    message
        .properties
        .insert(TIMER_ENQUEUE_MS.into(), enqueue_ms.to_string());
    message
        .properties
        .insert(TIMER_DEQUEUE_MS.into(), now.to_string());
    message.topic = topic;
    message.queue_id = queue_id;
    Some(message)
}

/// `TimerMessageStore.buildDeleteKey` with `appendTopicForTimerDeleteKey` left at its default.
pub fn timer_delete_key(uniq_key: &str) -> &str {
    uniq_key
}

/// `RecallMessageHandle.HandleV1`, the five space-separated tokens of a decoded handle.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecallHandle {
    pub topic: String,
    pub broker_name: String,
    /// Kept as the string the handle carried, exactly like `getTimestampStr`.
    pub timestamp_str: String,
    pub message_id: String,
}

const HANDLE_VERSION: &str = "v1";
/// `RecallMessageHandle.SEPARATOR`; the format has no escaping, so a topic or group that carries
/// a space is as unreadable here as it is in the reference.
const HANDLE_SEPARATOR: char = ' ';

/// `RecallMessageHandle.HandleV1.buildHandle`: base64url of the UTF-8 tokens, padding included.
pub fn build_recall_handle(
    topic: &str,
    broker_name: &str,
    timestamp_str: &str,
    message_id: &str,
) -> String {
    let separator = HANDLE_SEPARATOR.to_string();
    let raw = [
        HANDLE_VERSION,
        topic,
        broker_name,
        timestamp_str,
        message_id,
    ]
    .join(&separator);
    base64url_encode(raw.as_bytes())
}

/// `RecallMessageHandle.decodeHandle`. Every rejection collapses to the reference's one message.
pub fn parse_recall_handle(handle: &str) -> Result<RecallHandle> {
    const INVALID: &str = "recall handle is invalid";
    if handle.is_empty() {
        return Err(Error::Invalid(INVALID.into()));
    }
    let raw = base64url_decode(handle).map_err(|_| Error::Invalid(INVALID.into()))?;
    let raw = String::from_utf8(raw).map_err(|_| Error::Invalid(INVALID.into()))?;
    // `String.split(" ")` drops the trailing empty tokens, and both the version and the count are
    // checked on what is left.
    let mut items: Vec<&str> = raw.split(HANDLE_SEPARATOR).collect();
    while items.last().is_some_and(|item| item.is_empty()) {
        items.pop();
    }
    if items.len() < 5 || items[0] != HANDLE_VERSION {
        return Err(Error::Invalid(INVALID.into()));
    }
    Ok(RecallHandle {
        topic: items[1].to_owned(),
        broker_name: items[2].to_owned(),
        timestamp_str: items[3].to_owned(),
        message_id: items[4].to_owned(),
    })
}

const URL_ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

fn base64url_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        let value = (chunk[0] as u32) << 16
            | (chunk.get(1).copied().unwrap_or(0) as u32) << 8
            | chunk.get(2).copied().unwrap_or(0) as u32;
        out.push(URL_ALPHABET[(value >> 18) as usize & 63] as char);
        out.push(URL_ALPHABET[(value >> 12) as usize & 63] as char);
        out.push(if chunk.len() > 1 {
            URL_ALPHABET[(value >> 6) as usize & 63] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            URL_ALPHABET[value as usize & 63] as char
        } else {
            '='
        });
    }
    out
}

fn base64url_decode(text: &str) -> std::result::Result<Vec<u8>, ()> {
    // `java.util.Base64.getUrlDecoder` treats padding as the end of the data and does not require
    // it, so trailing '=' is trimmed and a leftover group of one character cannot be a byte.
    let trimmed = text.trim_end_matches('=');
    if trimmed.len() % 4 == 1 {
        return Err(());
    }
    let mut out = Vec::with_capacity(trimmed.len() * 3 / 4);
    let mut accumulator = 0u32;
    let mut bits = 0u32;
    for byte in trimmed.bytes() {
        let index = match byte {
            b'A'..=b'Z' => byte - b'A',
            b'a'..=b'z' => byte - b'a' + 26,
            b'0'..=b'9' => byte - b'0' + 52,
            b'-' => 62,
            b'_' => 63,
            _ => return Err(()),
        };
        accumulator = accumulator << 6 | index as u32;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((accumulator >> bits) as u8);
        }
    }
    Ok(out)
}

/// `GroupRetryPolicyType`: the only two strategies the reference recognises.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum GroupRetryPolicyType {
    EXPONENTIAL,
    #[default]
    CUSTOMIZED,
}

/// `ExponentialRetryPolicy`: the delay for attempt `n` is `initial * multiplier^n`, capped at `max`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct ExponentialRetryPolicy {
    pub initial: u64,
    pub max: u64,
    pub multiplier: u64,
}

impl Default for ExponentialRetryPolicy {
    fn default() -> Self {
        Self {
            initial: 5_000,
            max: 2 * 60 * 60 * 1_000,
            multiplier: 2,
        }
    }
}

/// `CustomizedRetryPolicy.next`: one millisecond delay per reconsume attempt.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct CustomizedRetryPolicy {
    pub next: Vec<u64>,
}

impl Default for CustomizedRetryPolicy {
    fn default() -> Self {
        Self {
            next: [
                1, 5, 10, 30, 60, 120, 180, 240, 300, 360, 420, 480, 540, 600, 1200, 1800, 3600,
                7200,
            ]
            .into_iter()
            .map(|seconds| seconds * 1_000)
            .collect(),
        }
    }
}

/// `GroupRetryPolicy`: the reference leaves both policy bodies null until a client fills one in,
/// so they serialise as absent and read back as `None`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct GroupRetryPolicy {
    #[serde(rename = "type")]
    pub policy_type: GroupRetryPolicyType,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exponential_retry_policy: Option<ExponentialRetryPolicy>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub customized_retry_policy: Option<CustomizedRetryPolicy>,
}

/// `SimpleSubscriptionData`: one topic's subscription inside a group config. The reference's
/// consumer lag calculator reads this table instead of the client registration cache.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct SimpleSubscriptionData {
    pub topic: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expression_type: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expression: Option<String>,
    pub version: i64,
}

/// `SubscriptionGroupConfig`, keeping the reference's defaults and JSON field names.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct GroupConfig {
    pub group_name: String,
    pub consume_enable: bool,
    pub consume_from_min_enable: bool,
    pub consume_broadcast_enable: bool,
    pub consume_message_orderly: bool,
    pub retry_queue_nums: i32,
    pub retry_max_times: i32,
    pub broker_id: i64,
    pub which_broker_when_consume_slowly: i64,
    pub notify_consumer_ids_changed_enable: bool,
    pub group_sys_flag: i32,
    pub consume_timeout_minute: i32,
    pub group_retry_policy: GroupRetryPolicy,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subscription_data_set: Option<Vec<SimpleSubscriptionData>>,
    pub attributes: BTreeMap<String, String>,
}

impl Default for GroupConfig {
    fn default() -> Self {
        Self {
            group_name: String::new(),
            consume_enable: true,
            consume_from_min_enable: true,
            consume_broadcast_enable: true,
            consume_message_orderly: false,
            retry_queue_nums: 1,
            retry_max_times: 16,
            broker_id: 0,
            which_broker_when_consume_slowly: 1,
            notify_consumer_ids_changed_enable: true,
            group_sys_flag: 0,
            consume_timeout_minute: 15,
            group_retry_policy: GroupRetryPolicy::default(),
            subscription_data_set: None,
            attributes: BTreeMap::new(),
        }
    }
}

impl GroupConfig {
    /// `SubscriptionGroupManager.findSubscriptionGroupConfig` auto-creates this shape.
    pub fn new(group_name: impl Into<String>) -> Self {
        Self {
            group_name: group_name.into(),
            ..Default::default()
        }
    }

    /// Java `SubscriptionGroupConfig.getLiteSubClientQuota`: the `lite.sub.client.quota`
    /// attribute, or the reference's default of 2000 when the group declares none.
    pub fn lite_sub_client_quota(&self) -> i32 {
        self.attributes
            .get("lite.sub.client.quota")
            .and_then(|value| value.parse::<i64>().ok())
            .unwrap_or(2000) as i32
    }

    /// Java `SubscriptionGroupConfig.getLiteBindTopic`: the `lite.bind.topic` attribute.
    pub fn lite_bind_topic(&self) -> Option<&str> {
        self.attributes
            .get("lite.bind.topic")
            .map(String::as_str)
            .filter(|value| !value.is_empty())
    }
}

/// `DelayOffsetSerializeWrapper`, the on-disk shape of `delayOffset.json`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DelayOffsets {
    pub offset_table: BTreeMap<i32, u64>,
}

/// `TimerCheckpoint`, reduced to what this broker has to remember between runs.
///
/// The reference also stores the wheel itself, because its timer log is a separate file; here the
/// queue `rmq_sys_wheel_timer` *is* the log, so replaying it from `scan_offset` rebuilds the wheel
/// exactly like `TimerMessageStore.recover` does. Deliveries that happened after the last flush are
/// replayed, which is the reference's at-least-once contract with a `timerFlushIntervalMs` window.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TimerCheckpoint {
    pub scan_offset: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(topic: &str, properties: &[(&str, &str)]) -> Message {
        Message {
            topic: topic.into(),
            queue_id: 3,
            properties: properties
                .iter()
                .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
                .collect(),
            body: b"payload".to_vec(),
            ..Default::default()
        }
    }

    #[test]
    fn delay_table_parses_the_default_and_rejects_malformed_entries() {
        let levels = DelayLevels::default();
        assert_eq!(levels.max(), 18);
        assert_eq!(levels.delay_ms(1), Some(1_000));
        assert_eq!(levels.delay_ms(3), Some(10_000));
        assert_eq!(levels.delay_ms(18), Some(2 * 3_600_000));
        assert_eq!(levels.deliver_timestamp(3, 1_000), 11_000);
        // An unlisted level falls back to one second, like the reference.
        assert_eq!(levels.deliver_timestamp(99, 1_000), 2_000);

        for bad in [
            "",
            "1x",
            "1s  5s",
            "s",
            "abc s",
            "1s 99999999999999999999999s",
        ] {
            assert!(DelayLevels::parse(bad).is_err(), "{bad:?} must not parse");
        }
        assert_eq!(
            DelayLevels::parse("1d").unwrap().delay_ms(1),
            Some(86_400_000)
        );
        assert_eq!(DelayLevels::parse("2m 3m").unwrap().max(), 2);
    }

    #[test]
    fn clock_skew_corrects_a_timestamp_beyond_one_level_window() {
        let levels = DelayLevels::default();
        // The level 1 window is one second, so a stamp further out is treated as skew.
        assert_eq!(levels.correct_deliver_timestamp(1_000, 1, 1_500), 1_500);
        assert_eq!(levels.correct_deliver_timestamp(1_000, 1, 2_001), 1_000);
        // An unknown level keeps the timestamp.
        assert_eq!(levels.correct_deliver_timestamp(1_000, 42, 9_000), 9_000);
    }

    #[test]
    fn retry_and_dlq_names_round_trip() {
        assert_eq!(retry_topic("g"), "%RETRY%g");
        assert_eq!(dlq_topic("g"), "%DLQ%g");
        assert!(is_retry_topic("%RETRY%g"));
        assert_eq!(parse_group("%RETRY%g"), "g");
        assert_eq!(parse_group("%RETRY%g+orders"), "g");
        assert_eq!(delay_level_to_queue_id(3), 2);
        assert_eq!(queue_id_to_delay_level(2), 3);
    }

    #[test]
    fn a_delay_level_parks_the_record_and_clamps_the_level() {
        let levels = DelayLevels::default();
        let mut message = record("orders", &[("DELAY", "3")]);
        let parking =
            handle_schedule_message(&mut message, &levels, &TimerSettings::default(), 0, |_| {
                false
            })
            .unwrap();
        assert_eq!(parking, Parking::Delay { level: 3 });
        assert_eq!(message.topic, SCHEDULE_TOPIC);
        assert_eq!(message.queue_id, 2);
        assert_eq!(message.properties[REAL_TOPIC], "orders");
        assert_eq!(message.properties[REAL_QUEUE_ID], "3");

        // Levels above the table clamp to the maximum instead of failing the send.
        let mut message = record("orders", &[("DELAY", "99")]);
        handle_schedule_message(&mut message, &levels, &TimerSettings::default(), 0, |_| {
            false
        })
        .unwrap();
        assert_eq!(message.queue_id, delay_level_to_queue_id(levels.max()));
        assert_eq!(message.properties[DELAY], "18");

        // A zero level is stored where the producer asked.
        let mut message = record("orders", &[("DELAY", "0")]);
        assert_eq!(
            handle_schedule_message(&mut message, &levels, &TimerSettings::default(), 0, |_| {
                false
            })
            .unwrap(),
            Parking::None
        );
        assert_eq!(message.topic, "orders");
        assert_eq!(message.queue_id, 3);

        let message = record("orders", &[("DELAY", "not-a-number")]);
        let error = delay_level(&message).unwrap_err().to_string();
        assert!(
            error.ends_with("invalid DELAY property: not-a-number"),
            "{error}"
        );
    }

    #[test]
    fn timer_properties_are_floored_to_the_wheel_precision() {
        let levels = DelayLevels::default();
        let mut message = record("orders", &[("TIMER_DELAY_SEC", "10")]);
        let parking = handle_schedule_message(
            &mut message,
            &levels,
            &TimerSettings::default(),
            100_000,
            |_| false,
        )
        .unwrap();
        // 110_000 is an exact multiple of the 1s precision, so it moves back one slot.
        assert_eq!(
            parking,
            Parking::Timer {
                deliver_ms: 109_000
            }
        );
        assert_eq!(message.topic, TIMER_TOPIC);
        assert_eq!(message.queue_id, 0);
        assert_eq!(message.properties[TIMER_OUT_MS], "109000");
        assert_eq!(message.properties[REAL_TOPIC], "orders");
        assert_eq!(message.properties[REAL_QUEUE_ID], "3");

        // A fractional deadline floors down.
        let mut message = record("orders", &[(TIMER_DELAY_MS, "10500")]);
        let parking = handle_schedule_message(
            &mut message,
            &levels,
            &TimerSettings::default(),
            100_000,
            |_| false,
        )
        .unwrap();
        assert_eq!(
            parking,
            Parking::Timer {
                deliver_ms: 110_000
            }
        );
    }

    #[test]
    fn a_delay_level_beats_the_timer_properties() {
        let levels = DelayLevels::default();
        let mut message = record(
            "orders",
            &[("DELAY", "2"), ("TIMER_DELIVER_MS", "999999999")],
        );
        let parking =
            handle_schedule_message(&mut message, &levels, &TimerSettings::default(), 0, |_| {
                false
            })
            .unwrap();
        assert_eq!(parking, Parking::Delay { level: 2 });
        assert!(message.properties.contains_key(DELAY));
        assert!(!message.properties.contains_key(TIMER_DELIVER_MS));
        assert_eq!(message.topic, SCHEDULE_TOPIC);
    }

    #[test]
    fn timer_messages_respect_the_enable_flag_bounds_and_flow_control() {
        let levels = DelayLevels::default();
        let disabled = TimerSettings {
            wheel_enable: false,
            ..Default::default()
        };
        let mut message = record("orders", &[("TIMER_DELAY_SEC", "10")]);
        let error =
            handle_schedule_message(&mut message, &levels, &disabled, 0, |_| false).unwrap_err();
        assert_eq!(error.code(), 1);
        assert_eq!(
            error.remark(&disabled),
            "accurate timer message is not enabled, timerWheelEnable is false"
        );

        // Unparseable or overflowing deadlines are illegal, as is exceeding the roll window.
        for properties in [
            [("TIMER_DELAY_SEC".to_owned(), "x".to_owned())].as_slice(),
            [("TIMER_DELAY_MS".to_owned(), "-".to_owned())].as_slice(),
            [("TIMER_DELIVER_MS".to_owned(), "0".to_owned())].as_slice(),
        ] {
            let mut message = Message {
                topic: "orders".into(),
                queue_id: 0,
                properties: properties.iter().cloned().collect(),
                ..Default::default()
            };
            let outcome = handle_schedule_message(
                &mut message,
                &levels,
                &TimerSettings::default(),
                1_000_000_000,
                |_| false,
            );
            if properties[0].1 == "0" {
                // Already expired: stored on its own topic with no parking.
                assert_eq!(outcome.unwrap(), Parking::None);
            } else {
                assert_eq!(outcome.unwrap_err().code(), 13);
            }
        }

        let mut message = record("orders", &[("TIMER_DELAY_SEC", "99999999999999999999")]);
        assert_eq!(
            handle_schedule_message(&mut message, &levels, &TimerSettings::default(), 0, |_| {
                false
            })
            .unwrap_err()
            .code(),
            13
        );

        let mut message = record("orders", &[("TIMER_DELAY_SEC", "10")]);
        assert_eq!(
            handle_schedule_message(&mut message, &levels, &TimerSettings::default(), 0, |_| {
                true
            })
            .unwrap_err()
            .code(),
            1
        );
    }

    #[test]
    fn an_expired_delete_or_a_rolled_record_is_left_alone() {
        let levels = DelayLevels::default();
        // `TIMER_DEL_UNIQKEY` on an expired deadline is the delete-then-expire race.
        let mut message = record(
            "orders",
            &[("TIMER_DELIVER_MS", "1"), ("TIMER_DEL_UNIQKEY", "uniq")],
        );
        assert_eq!(
            handle_schedule_message(
                &mut message,
                &levels,
                &TimerSettings::default(),
                1_000,
                |_| false
            )
            .unwrap_err()
            .code(),
            13
        );
        // A record already rolling on the timer topic is never transformed again.
        let mut message = record(TIMER_TOPIC, &[("TIMER_OUT_MS", "5000")]);
        assert_eq!(
            handle_schedule_message(&mut message, &levels, &TimerSettings::default(), 0, |_| {
                false
            })
            .unwrap(),
            Parking::None
        );
        assert_eq!(message.topic, TIMER_TOPIC);
    }

    #[test]
    fn message_time_up_restores_the_real_destination() {
        let stored = record(
            SCHEDULE_TOPIC,
            &[
                (DELAY, "3"),
                (REAL_TOPIC, "orders"),
                (REAL_QUEUE_ID, "5"),
                (TIMER_OUT_MS, "1234"),
                (TIMER_DELAY_MS, "10"),
            ],
        );
        let delivered = message_time_up(&stored).unwrap().unwrap();
        assert_eq!(delivered.topic, "orders");
        assert_eq!(delivered.queue_id, 5);
        assert!(!delivered.properties.contains_key(DELAY));
        assert!(!delivered.properties.contains_key(TIMER_DELIVER_MS));
        assert!(!delivered.properties.contains_key(TIMER_DELAY_SEC));
        // The reference leaves these two behind.
        assert_eq!(delivered.properties[TIMER_OUT_MS], "1234");
        assert_eq!(delivered.properties[TIMER_DELAY_MS], "10");

        let stored = record(SCHEDULE_TOPIC, &[(DELAY, "3")]);
        assert!(message_time_up(&stored).unwrap().is_none());
        let stored = record(
            SCHEDULE_TOPIC,
            &[(REAL_TOPIC, "orders"), (REAL_QUEUE_ID, "not-a-number")],
        );
        assert_eq!(message_time_up(&stored).unwrap().unwrap().queue_id, 0);
    }

    #[test]
    fn the_timer_engine_restores_the_destination_and_stamps_the_dequeue() {
        let settings = TimerSettings::default();
        let mut parked = record("orders", &[("TIMER_DELAY_MS", "5000")]);
        let Parking::Timer { deliver_ms } =
            transform_timer_message(&mut parked, &settings, 1_000, |_| false).unwrap()
        else {
            panic!("expected a timer park");
        };
        assert_eq!(deliver_ms, 5_000);
        assert_eq!(parked.topic, TIMER_TOPIC);
        assert_eq!(parked.queue_id, 0);

        parked.queue_offset = 7;
        let delivered = convert_timer_message(&parked, deliver_ms, 6_500).unwrap();
        assert_eq!(delivered.topic, "orders");
        assert_eq!(delivered.properties[TIMER_ENQUEUE_MS], "5000");
        assert_eq!(delivered.properties[TIMER_DEQUEUE_MS], "6500");
        // Only the two routing properties go; the stamps the producer set stay visible.
        assert!(!delivered.properties.contains_key(REAL_TOPIC));
        assert!(!delivered.properties.contains_key(REAL_QUEUE_ID));
        assert_eq!(delivered.properties[TIMER_OUT_MS], "5000");
        assert_eq!(delivered.properties[TIMER_DELAY_MS], "5000");

        // A record with no real topic has nowhere to go, so the wheel cannot convert it.
        let orphan = record(TIMER_TOPIC, &[(TIMER_OUT_MS, "5000")]);
        assert!(convert_timer_message(&orphan, 5_000, 6_000).is_none());
        // A malformed queue id is the reference's `NumberFormatException` on the same field.
        let broken = record("orders", &[(REAL_TOPIC, "orders"), (REAL_QUEUE_ID, "x")]);
        assert!(convert_timer_message(&broken, 5_000, 6_000).is_none());
    }

    #[test]
    fn slot_drain_and_the_wheel_span_bound_the_read_head() {
        let settings = TimerSettings::default();
        // `delayTime < currWriteTimeMs` for a slot-floor stamp is the same test as drain <= now.
        assert_eq!(settings.drain_ms(5_000), 6_000);
        assert_eq!(settings.drain_ms(5_500), 6_500);
        assert!(!settings.is_too_old(5_000, 6_000));
        let seven_days = TIMER_WHEEL_TTL_DAY * 86_400_000;
        let span = seven_days - TIMER_BLANK_SLOTS * settings.precision_ms;
        assert!(!settings.is_too_old(0, span + settings.precision_ms));
        assert!(settings.is_too_old(0, span + settings.precision_ms + 1));
    }

    #[test]
    fn congestion_rejection_matches_the_reference_bands() {
        // At or under the limit nothing is refused, and twice the limit always is.
        for roll in 0..1_000 {
            assert!(!is_reject(10, 10, roll));
            assert!(is_reject(20, 10, roll));
            assert!(is_reject(21, 10, roll));
        }
        // In between the reference draws `RANDOM.nextInt(1000)` against a fixed threshold.
        let threshold = 1_000 * (15 - 10) / (10 + 1);
        assert!(!is_reject(15, 10, threshold));
        assert!(is_reject(15, 10, threshold + 1));
        // The default limit is `Integer.MAX_VALUE`, so no realistic backlog can be refused.
        for congest in [0, 1_000, i32::MAX as i64] {
            assert!(!is_reject(congest, DEFAULT_TIMER_CONGEST_NUM_EACH_SLOT, 0));
        }
    }

    #[test]
    fn a_recall_handle_round_trips_with_the_bases_that_decode_it() {
        let handle = build_recall_handle(
            "order-topic",
            "broker-a",
            "1727000000001",
            "AC1F000100002A00",
        );
        // The reference uses padded base64url, so the length is a multiple of four.
        assert_eq!(handle.len() % 4, 0);
        assert!(!handle.contains(['+', '/', '=']) || handle.ends_with('='));
        let decoded = parse_recall_handle(&handle).unwrap();
        assert_eq!(
            decoded,
            RecallHandle {
                topic: "order-topic".into(),
                broker_name: "broker-a".into(),
                timestamp_str: "1727000000001".into(),
                message_id: "AC1F000100002A00".into(),
            }
        );

        for bad in [
            String::new(),
            "not base64 at all".into(),
            base64url_encode(b"v2 topic broker 1 id"),
            base64url_encode(b"v1 topic"),
            base64url_encode(b"v1 topic broker 1 "),
        ] {
            let error = parse_recall_handle(&bad).unwrap_err();
            assert!(
                matches!(&error, Error::Invalid(message) if message == "recall handle is invalid"),
                "{bad} failed with {error}"
            );
        }
        // Only the *trailing* separators are dropped, so an empty middle token still decodes.
        let decoded = parse_recall_handle(&base64url_encode(b"v1  broker 1 id")).unwrap();
        assert_eq!(decoded.topic, "");
    }

    #[test]
    fn the_timer_remarks_quote_the_brokers_own_settings() {
        let settings = TimerSettings {
            wheel_enable: false,
            congest_num_each_slot: 100,
            ..Default::default()
        };
        assert_eq!(
            TimerReject::NotEnabled.remark(&settings),
            "accurate timer message is not enabled, timerWheelEnable is false"
        );
        assert_eq!(
            TimerReject::MessageIllegal.remark(&settings),
            format!(
                "timer message illegal, the delay time should not be bigger than the max delay {}ms; or if set del msg, the delay time should be bigger than the current time",
                settings.max_delay_ms
            )
        );
        assert_eq!(
            TimerReject::FlowControl.remark(&settings),
            "timer message is under flow control, max num limit is 200 or the current value is greater than 100 and less than 200, trigger random flow control"
        );
        assert_eq!(
            ScheduleError::MalformedDelay(Error::Invalid("boom".into())).remark(&settings),
            "invalid data: boom"
        );
    }

    #[test]
    fn group_defaults_and_json_shape_match_the_reference() {
        let config = GroupConfig::new("group-a");
        assert!(config.consume_enable);
        assert!(config.consume_from_min_enable);
        assert!(config.consume_broadcast_enable);
        assert!(!config.consume_message_orderly);
        assert_eq!(config.retry_queue_nums, 1);
        assert_eq!(config.retry_max_times, 16);
        assert_eq!(config.broker_id, 0);
        assert_eq!(config.which_broker_when_consume_slowly, 1);
        assert!(config.notify_consumer_ids_changed_enable);
        assert_eq!(config.consume_timeout_minute, 15);
        assert!(config.attributes.is_empty());

        let json = serde_json::to_string(&config).unwrap();
        assert_eq!(serde_json::from_str::<GroupConfig>(&json).unwrap(), config);
        // A Java broker emits fields this broker does not model; they must not break decoding.
        let value: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(value["groupName"], "group-a");
        assert_eq!(value["retryMaxTimes"], 16);
        assert_eq!(
            serde_json::from_str::<GroupConfig>(
                r#"{"groupName":"g","groupRetryPolicy":{"retryPolicyList":[]},"unknown":1}"#
            )
            .unwrap()
            .retry_max_times,
            16
        );
    }
}
