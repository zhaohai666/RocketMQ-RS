use crate::common::{
    Error, LMQ_DISPATCH_SEPARATOR, LMQ_QUEUE_ID, Message, PROPERTY_INNER_MULTI_DISPATCH,
    PROPERTY_INNER_MULTI_QUEUE_OFFSET, PROPERTY_LITE_TOPIC, Result, TopicConfig, get_lite_topic,
    is_lite_topic_queue, is_lmq, now_millis, to_lmq_name,
};
use crate::filter::Subscription;
use crate::schedule::{DelayOffsets, GroupConfig, TimerCheckpoint};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions, TryLockError};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, MutexGuard};

const LOG_FILE: &str = "commitlog";
const LOCK_FILE: &str = ".store.lock";
const TOPICS_FILE: &str = "topics.json";
const OFFSETS_FILE: &str = "consumer_offsets.json";
const FILTERS_FILE: &str = "consumerFilter.json";
/// `SubscriptionGroupManager` persists its table as `config/subscriptionGroup.json`.
const GROUPS_FILE: &str = "subscriptionGroup.json";
/// `ScheduleMessageService` persists its delivery cursor as `delayOffset.json`.
const DELAY_OFFSETS_FILE: &str = "delayOffset.json";
/// `TimerCheckpoint` persists the timer queue cursor as `timercheck.json`.
const TIMER_CHECKPOINT_FILE: &str = "timercheck.json";
const MIN_RECORD_BYTES: usize = 91;
const MAX_RECORD_BYTES: usize = 16 * 1024 * 1024;
const MAX_BATCH_BYTES: usize = 64 * 1024 * 1024;
const MAX_BATCH_MESSAGES: usize = 1024;
const MAX_SCAN_RECORDS: usize = 4096;
const MAX_READ_BYTES: usize = 64 * 1024 * 1024;
const MAX_STATE_BYTES: u64 = 64 * 1024 * 1024;
const MAX_QUEUES: i32 = 1024;
const MAGIC_V1: i32 = -626843481;
const MAGIC_V2: i32 = -626843477;
static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

type QueueKey = (String, i32);

/// Whether `append` enforces the topic's write queue count.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum QueueCheck {
    TopicConfig,
    Any,
}

impl QueueCheck {
    fn is_enforced(self) -> bool {
        self == Self::TopicConfig
    }
}
type Queues = BTreeMap<QueueKey, Vec<Location>>;

#[derive(Clone, Copy)]
struct Location {
    physical: u64,
    length: u32,
    // A running maximum keeps timestamp searches valid when the clock moves backwards.
    timestamp_ceiling: i64,
}

/// One commitlog record's position in the message-key index: where the record sits in
/// its consume queue and when it was stored (Java `IndexFile`'s `(topic@key) -> idx`).
#[derive(Clone, Copy)]
struct KeyLocation {
    physical: u64,
    timestamp: i64,
}

/// `(topic, message key) -> commitlog positions`, appended in commitlog order.
type Keys = BTreeMap<(String, String), Vec<KeyLocation>>;

#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Offsets {
    offset_table: BTreeMap<String, BTreeMap<i32, u64>>,
}

struct Inner {
    directory: PathBuf,
    log: File,
    queues: Queues,
    key_index: Keys,
    physical: BTreeMap<u64, u32>,
    committed_len: u64,
    topics: BTreeMap<String, TopicConfig>,
    offsets: Offsets,
    filters: FilterState,
    groups: BTreeMap<String, GroupConfig>,
    delay_offsets: DelayOffsets,
    timer_checkpoint: TimerCheckpoint,
    fenced: bool,
    #[cfg(test)]
    fail_log_sync: bool,
    // Retaining the lock inode and handle prevents a second writer while idle.
    _lock: File,
}

pub struct Store {
    inner: Mutex<Inner>,
}

/// A persisted consumer filter, mirroring the JSON shape of `ConsumerFilterData`.
#[derive(Clone, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct FilterSnapshot {
    pub topic: String,
    pub consumer_group: String,
    pub expression: String,
    pub expression_type: String,
    pub born_time: i64,
    pub dead_time: i64,
    pub client_version: i64,
}

#[derive(Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct FiltersByTopic {
    group_filter_data: BTreeMap<String, FilterSnapshot>,
    topic: String,
}

#[derive(Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct FilterState {
    filter_data_by_topic: BTreeMap<String, FiltersByTopic>,
}

impl FilterState {
    fn group_by_topic(filters: &[FilterSnapshot]) -> Self {
        let mut state = Self::default();
        for filter in filters {
            let by_topic = state
                .filter_data_by_topic
                .entry(filter.topic.clone())
                .or_insert_with_key(|topic| FiltersByTopic {
                    topic: topic.clone(),
                    ..Default::default()
                });
            by_topic
                .group_filter_data
                .insert(filter.consumer_group.clone(), filter.clone());
        }
        state
    }

    fn flatten(&self) -> Vec<FilterSnapshot> {
        self.filter_data_by_topic
            .values()
            .flat_map(|by_topic| {
                by_topic.group_filter_data.values().map(move |filter| {
                    let mut filter = filter.clone();
                    filter.topic.clone_from(&by_topic.topic);
                    filter
                })
            })
            .collect()
    }
}

/// `ConsumerFilterManager.decode` treats every restored filter as dead until a heartbeat revives it.
fn mark_restored_filters_dead(state: &mut FilterState) {
    let stale = now_millis().saturating_sub(30_000);
    for by_topic in state.filter_data_by_topic.values_mut() {
        for filter in by_topic.group_filter_data.values_mut() {
            if filter.dead_time == 0 {
                filter.dead_time = stale.max(filter.born_time);
            }
        }
    }
}

#[derive(Debug)]
pub struct ReadResult {
    pub messages: Vec<Message>,
    pub next_begin_offset: u64,
    pub min_offset: u64,
    pub max_offset: u64,
}

impl Store {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        if path.as_os_str().is_empty() {
            return Err(invalid("empty store directory"));
        }
        create_directory_durably(path)?;
        let directory = fs::canonicalize(path)?;
        let lock_path = directory.join(LOCK_FILE);
        reject_nonregular(&lock_path)?;
        let lock = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)?;
        match lock.try_lock() {
            Ok(()) => {}
            Err(TryLockError::WouldBlock) => {
                return Err(invalid("store directory is already open by another writer"));
            }
            Err(TryLockError::Error(error)) => return Err(error.into()),
        }
        lock.sync_all()?;

        // Invalid state must fail closed before recovery can truncate a log tail.
        let mut topics: BTreeMap<String, TopicConfig> =
            load_json(&directory.join(TOPICS_FILE))?.unwrap_or_default();
        for (name, config) in &topics {
            validate_topic(config)?;
            if name != &config.topic_name {
                return Err(invalid("topic state key does not match topicName"));
            }
        }
        let offsets: Offsets = load_json(&directory.join(OFFSETS_FILE))?.unwrap_or_default();
        validate_offsets(&offsets)?;
        let mut filters: FilterState =
            load_json(&directory.join(FILTERS_FILE))?.unwrap_or_default();
        mark_restored_filters_dead(&mut filters);
        let groups: BTreeMap<String, GroupConfig> =
            load_json(&directory.join(GROUPS_FILE))?.unwrap_or_default();
        for (name, config) in &groups {
            validate_group(name, config)?;
        }
        let delay_offsets: DelayOffsets =
            load_json(&directory.join(DELAY_OFFSETS_FILE))?.unwrap_or_default();
        for level in delay_offsets.offset_table.keys() {
            if *level < 1 {
                return Err(invalid("delay offset level must be positive"));
            }
        }
        let timer_checkpoint: TimerCheckpoint =
            load_json(&directory.join(TIMER_CHECKPOINT_FILE))?.unwrap_or_default();

        let log_path = directory.join(LOG_FILE);
        reject_nonregular(&log_path)?;
        let mut log = OpenOptions::new()
            .read(true)
            .append(true)
            .create(true)
            .open(&log_path)?;
        let (queues, physical, key_index, committed_len) = recover(&mut log, &mut topics)?;
        log.sync_all()?;
        sync_directory(&directory)?;
        Ok(Self {
            inner: Mutex::new(Inner {
                directory,
                log,
                queues,
                key_index,
                physical,
                committed_len,
                topics,
                offsets,
                filters,
                groups,
                delay_offsets,
                timer_checkpoint,
                fenced: false,
                #[cfg(test)]
                fail_log_sync: false,
                _lock: lock,
            }),
        })
    }

    pub fn append(&self, message: Message) -> Result<Message> {
        let mut result = self.append_batch(vec![message])?;
        Ok(result.remove(0))
    }

    /// Runtime-atomic visibility is not crash atomicity: recovery may retain an unacknowledged batch prefix.
    pub fn append_batch(&self, messages: Vec<Message>) -> Result<Vec<Message>> {
        self.append_records(messages, QueueCheck::TopicConfig)
    }

    /// Records this broker produced itself (delay delivery, retry, dead letter) keep the queue id
    /// they were stored with, like `CommitLog`, which never consults the topic's write queue count.
    pub fn append_internal(&self, messages: Vec<Message>) -> Result<Vec<Message>> {
        self.append_records(messages, QueueCheck::Any)
    }

    fn append_records(
        &self,
        messages: Vec<Message>,
        queue_check: QueueCheck,
    ) -> Result<Vec<Message>> {
        let mut inner = self.lock()?;
        inner.ensure_writable()?;
        if messages.len() > MAX_BATCH_MESSAGES {
            return Err(invalid("batch exceeds 1024 messages"));
        }
        if messages.is_empty() {
            return Ok(Vec::new());
        }

        for message in &messages {
            validate_message(message)?;
            let write_queues = inner
                .topics
                .get(&message.topic)
                .map_or(8, |topic| topic.write_queue_nums);
            if queue_check.is_enforced() && message.queue_id >= write_queues {
                return Err(invalid("queue_id is outside the topic's write queues"));
            }
        }

        let mut encoded = Vec::new();
        let mut prepared = Vec::with_capacity(messages.len());
        let mut pending: BTreeMap<QueueKey, u64> = BTreeMap::new();
        let timestamp = now_millis();
        for mut message in messages {
            let key = (message.topic.clone(), message.queue_id);
            let next = pending
                .entry(key.clone())
                .or_insert_with(|| inner.queues.get(&key).map_or(0, |queue| queue.len() as u64));
            message.queue_offset = *next;
            *next = next
                .checked_add(1)
                .ok_or_else(|| invalid("queue offset overflow"))?;
            // `LmqDispatch.populateLmqOffsets`: a message dispatched to light queues
            // carries the offset each of those queues holds for it, so the light
            // queue keeps an offset sequence of its own.
            if message
                .properties
                .get(PROPERTY_LITE_TOPIC)
                .is_some_and(|lite| !lite.is_empty())
                && !message
                    .properties
                    .contains_key(PROPERTY_INNER_MULTI_DISPATCH)
            {
                // The broker names the light queue of a lite message; when the
                // property is missing it still follows from the lite topic.
                if let Some(lmq) = message
                    .properties
                    .get(PROPERTY_LITE_TOPIC)
                    .and_then(|lite| to_lmq_name(&message.topic, lite))
                {
                    message
                        .properties
                        .insert(PROPERTY_INNER_MULTI_DISPATCH.to_owned(), lmq);
                }
            }
            let targets = lmq_dispatch_targets(&message);
            if !targets.is_empty() {
                let mut offsets = Vec::with_capacity(targets.len());
                for (lmq, queue_id) in &targets {
                    let lmq_key = (lmq.clone(), *queue_id);
                    let next = pending.entry(lmq_key.clone()).or_insert_with(|| {
                        inner
                            .queues
                            .get(&lmq_key)
                            .map_or(0, |queue| queue.len() as u64)
                    });
                    offsets.push(*next);
                    *next = next
                        .checked_add(1)
                        .ok_or_else(|| invalid("queue offset overflow"))?;
                }
                message.properties.insert(
                    PROPERTY_INNER_MULTI_QUEUE_OFFSET.to_owned(),
                    offsets
                        .iter()
                        .map(|offset| offset.to_string())
                        .collect::<Vec<_>>()
                        .join(LMQ_DISPATCH_SEPARATOR),
                );
            }
            message.physical_offset = inner
                .committed_len
                .checked_add(encoded.len() as u64)
                .ok_or_else(|| invalid("physical offset overflow"))?;
            message.store_timestamp = timestamp;
            let bytes = message.encode()?;
            validate_record_length(bytes.len())?;
            let batch_len = encoded
                .len()
                .checked_add(bytes.len())
                .ok_or_else(|| invalid("batch length overflow"))?;
            if batch_len > MAX_BATCH_BYTES {
                return Err(invalid("batch exceeds 64 MiB"));
            }
            // Decode so returned fields and indexes match the persisted record.
            let (canonical, consumed) = Message::decode(&bytes)?;
            if consumed != bytes.len()
                || canonical.topic != message.topic
                || canonical.queue_id != message.queue_id
                || canonical.queue_offset != message.queue_offset
                || canonical.physical_offset != message.physical_offset
            {
                return Err(invalid("encoded message does not preserve record identity"));
            }
            prepared.push((canonical, bytes.len() as u32));
            encoded.extend_from_slice(&bytes);
        }
        let new_len = inner
            .committed_len
            .checked_add(encoded.len() as u64)
            .ok_or_else(|| invalid("physical offset overflow"))?;

        let write_result = (|| -> Result<()> {
            if inner.log.metadata()?.len() != inner.committed_len {
                return Err(invalid("commitlog length changed outside this store"));
            }
            inner.log.write_all(&encoded)?;
            inner.sync_log()?;
            Ok(())
        })();
        if let Err(error) = write_result {
            // Fence writes because failed writes or syncs leave durability uncertain until recovery.
            inner.fenced = true;
            return Err(error);
        }

        let mut result = Vec::with_capacity(prepared.len());
        for (message, length) in prepared {
            add_index(&mut inner.queues, &message, length);
            add_key_index(&mut inner.key_index, &message);
            inner.physical.insert(message.physical_offset, length);
            inner
                .topics
                .entry(message.topic.clone())
                .or_insert_with(|| TopicConfig::new(message.topic.clone(), 8));
            result.push(message);
        }
        inner.committed_len = new_len;
        Ok(result)
    }

    /// Allow the first match to exceed max_bytes so small limits cannot stall a reader.
    pub fn read(
        &self,
        topic: &str,
        queue_id: i32,
        offset: u64,
        max_nums: usize,
        max_bytes: usize,
        filter: Option<&Subscription>,
    ) -> Result<ReadResult> {
        validate_name(topic, "topic")?;
        validate_queue(queue_id)?;
        let mut inner = self.lock()?;
        let key = (topic.to_owned(), queue_id);
        let max_offset = inner.queues.get(&key).map_or(0, |queue| queue.len() as u64);
        let mut result = ReadResult {
            messages: Vec::new(),
            next_begin_offset: offset.min(max_offset),
            min_offset: 0,
            max_offset,
        };
        let mut scanned = 0;
        let mut scanned_bytes = 0usize;
        let mut returned_bytes = 0usize;
        let byte_limit = max_bytes.min(MAX_READ_BYTES);
        while result.next_begin_offset < max_offset
            && result.messages.len() < max_nums
            && scanned < MAX_SCAN_RECORDS
        {
            let queue_offset = result.next_begin_offset;
            let location = inner.queues[&key][queue_offset as usize];
            if scanned > 0 && scanned_bytes + location.length as usize > MAX_READ_BYTES {
                break;
            }
            let mut message = read_record(&mut inner.log, location.physical, location.length)?;
            if is_lite_topic_queue(topic) {
                read_light_queue_record(&mut message, topic, queue_offset)?;
            } else if message.topic != topic
                || message.queue_id != queue_id
                || message.queue_offset != queue_offset
            {
                return Err(invalid(
                    "commitlog record no longer matches its queue index",
                ));
            }
            scanned += 1;
            scanned_bytes += location.length as usize;
            let matches = filter.is_none_or(|filter| filter.matches(&message));
            if !matches {
                result.next_begin_offset += 1;
                continue;
            }
            let size = location.length as usize;
            if !result.messages.is_empty() && returned_bytes + size > byte_limit {
                // A matched message not returned must be retried, not skipped.
                break;
            }
            returned_bytes += size;
            result.messages.push(message);
            result.next_begin_offset += 1;
            if returned_bytes >= byte_limit {
                break;
            }
        }
        Ok(result)
    }

    pub fn queue_max_offset(&self, topic: &str, queue_id: i32) -> u64 {
        // This infallible API reads through poison, while poisoned writers remain blocked.
        let inner = self.inner.lock().unwrap_or_else(|error| error.into_inner());
        inner
            .queues
            .get(&(topic.to_owned(), queue_id))
            .map_or(0, |queue| queue.len() as u64)
    }

    pub fn message_at(&self, physical_offset: u64) -> Result<Option<Message>> {
        let mut inner = self.lock()?;
        let Some(&length) = inner.physical.get(&physical_offset) else {
            return Ok(None);
        };
        Ok(Some(read_record(&mut inner.log, physical_offset, length)?))
    }

    /// One record of a queue by its queue offset, the inverse of `read`'s page walk.
    pub fn message_by_queue_offset(
        &self,
        topic: &str,
        queue_id: i32,
        offset: u64,
    ) -> Result<Option<Message>> {
        let mut inner = self.lock()?;
        let Some(queue) = inner.queues.get(&(topic.to_owned(), queue_id)) else {
            return Ok(None);
        };
        let Some(&location) = queue.get(offset as usize) else {
            return Ok(None);
        };
        let mut message = read_record(&mut inner.log, location.physical, location.length)?;
        if is_lite_topic_queue(topic) {
            read_light_queue_record(&mut message, topic, offset)?;
        }
        Ok(Some(message))
    }

    /// `QueryMessageProcessor` key lookup: every record on `topic` carrying `key`
    /// (via the commitlog key index), within the inclusive store-time window.
    pub fn query_messages(
        &self,
        topic: &str,
        key: &str,
        begin: i64,
        end: i64,
        max_nums: usize,
    ) -> Result<Vec<Message>> {
        validate_name(topic, "topic")?;
        let mut inner = self.lock()?;
        // Cloned so record reads can take the log mutably while iterating.
        let Some(entries) = inner
            .key_index
            .get(&(topic.to_owned(), key.to_owned()))
            .cloned()
        else {
            return Ok(Vec::new());
        };
        let mut messages = Vec::new();
        for (scanned, entry) in entries.iter().enumerate() {
            if messages.len() >= max_nums || scanned >= MAX_SCAN_RECORDS {
                break;
            }
            if entry.timestamp < begin || entry.timestamp > end {
                continue;
            }
            let Some(&length) = inner.physical.get(&entry.physical) else {
                continue;
            };
            let message = read_record(&mut inner.log, entry.physical, length)?;
            if message.topic == topic {
                messages.push(message);
            }
        }
        Ok(messages)
    }

    /// Time-range scan across every stored queue of `topic` (`AdminBrokerProcessor
    /// queryMessage` fallback when no key narrows the search). `filter` applies the
    /// caller's subscription expression, enabling subscription/lite-topic searches.
    pub fn scan_messages(
        &self,
        topic: &str,
        begin: i64,
        end: i64,
        max_nums: usize,
        filter: Option<&Subscription>,
    ) -> Result<Vec<Message>> {
        validate_name(topic, "topic")?;
        if begin > end {
            return Ok(Vec::new());
        }
        let mut inner = self.lock()?;
        let queue_ids: Vec<i32> = inner
            .queues
            .keys()
            .filter(|(name, _)| name == topic)
            .map(|(_, queue_id)| *queue_id)
            .collect();
        let mut messages = Vec::new();
        let mut scanned = 0usize;
        for queue_id in queue_ids {
            if messages.len() >= max_nums || scanned >= MAX_SCAN_RECORDS {
                break;
            }
            let key = (topic.to_owned(), queue_id);
            // Skip the prefix that provably predates the window (running-max ceilings).
            let mut offset =
                inner.queues[&key].partition_point(|entry| entry.timestamp_ceiling < begin) as u64;
            let max_offset = inner.queues[&key].len() as u64;
            while offset < max_offset && messages.len() < max_nums && scanned < MAX_SCAN_RECORDS {
                let location = inner.queues[&key][offset as usize];
                let message = read_record(&mut inner.log, location.physical, location.length)?;
                scanned += 1;
                offset += 1;
                if message.store_timestamp < begin || message.store_timestamp > end {
                    if message.store_timestamp > end {
                        // Records per queue are appended in (wall-clock) store order; past the
                        // window only clock regressions could still match, which the scan cap
                        // bounds anyway.
                        break;
                    }
                    continue;
                }
                if filter.is_none_or(|filter| filter.matches(&message)) {
                    messages.push(message);
                }
            }
        }
        Ok(messages)
    }

    pub fn search_offset(&self, topic: &str, queue_id: i32, timestamp: i64) -> Result<u64> {
        validate_name(topic, "topic")?;
        validate_queue(queue_id)?;
        let inner = self.lock()?;
        Ok(inner
            .queues
            .get(&(topic.to_owned(), queue_id))
            .map_or(0, |queue| {
                queue.partition_point(|entry| entry.timestamp_ceiling < timestamp) as u64
            }))
    }

    pub fn topics(&self) -> Result<BTreeMap<String, TopicConfig>> {
        Ok(self.lock()?.topics.clone())
    }

    /// Retain read or write access to occupied queues so configuration changes cannot orphan data.
    pub fn put_topic(&self, config: TopicConfig) -> Result<()> {
        validate_topic(&config)?;
        let mut inner = self.lock()?;
        inner.ensure_writable()?;
        let covered = config.read_queue_nums.max(config.write_queue_nums);
        if inner.queues.iter().any(|((topic, queue), entries)| {
            topic == &config.topic_name && *queue >= covered && !entries.is_empty()
        }) {
            return Err(invalid(
                "topic queue reduction would orphan stored messages",
            ));
        }
        let mut topics = inner.topics.clone();
        topics.insert(config.topic_name.clone(), config);
        let bytes = encode_json(&topics)?;
        inner.persist(TOPICS_FILE, &bytes)?;
        inner.topics = topics;
        Ok(())
    }

    pub fn consumer_offset(&self, group: &str, topic: &str, queue_id: i32) -> Result<Option<u64>> {
        let key = offset_key(group, topic, queue_id)?;
        Ok(self
            .lock()?
            .offsets
            .offset_table
            .get(&key)
            .and_then(|queues| queues.get(&queue_id).copied()))
    }

    /// Offsets may move backwards or past the current queue end (admin reset).
    pub fn commit_offset(
        &self,
        group: &str,
        topic: &str,
        queue_id: i32,
        offset: u64,
    ) -> Result<()> {
        let key = offset_key(group, topic, queue_id)?;
        let mut inner = self.lock()?;
        inner.ensure_writable()?;
        let mut offsets = inner.offsets.clone();
        offsets
            .offset_table
            .entry(key)
            .or_default()
            .insert(queue_id, offset);
        let bytes = encode_json(&offsets)?;
        inner.persist(OFFSETS_FILE, &bytes)?;
        inner.offsets = offsets;
        Ok(())
    }

    pub fn all_offsets(&self) -> Result<serde_json::Value> {
        Ok(serde_json::to_value(&self.lock()?.offsets)?)
    }

    /// Snapshots restored from disk; every one of them is marked dead by recovery.
    pub fn consumer_filters(&self) -> Result<Vec<FilterSnapshot>> {
        Ok(self.lock()?.filters.flatten())
    }

    /// Replace the whole registry, mirroring `ConsumerFilterManager.encode`, which serializes
    /// the live map at persist time rather than merging per entry.
    pub fn put_consumer_filters(&self, filters: &[FilterSnapshot]) -> Result<()> {
        for filter in filters {
            validate_name(&filter.topic, "topic")?;
            validate_name(&filter.consumer_group, "consumer group")?;
        }
        let state = FilterState::group_by_topic(filters);
        let bytes = encode_json(&state)?;
        let mut inner = self.lock()?;
        inner.ensure_writable()?;
        inner.persist(FILTERS_FILE, &bytes)?;
        inner.filters = state;
        Ok(())
    }

    pub fn flush(&self) -> Result<()> {
        let mut inner = self.lock()?;
        inner.ensure_writable()?;
        if let Err(error) = inner.sync_log() {
            inner.fenced = true;
            return Err(error.into());
        }
        if let Err(error) = sync_directory(&inner.directory) {
            inner.fenced = true;
            return Err(error);
        }
        Ok(())
    }

    /// A fenced store must reopen because the durability of its unacknowledged tail is unknown.
    pub fn physical_len(&self) -> Result<u64> {
        let inner = self.lock()?;
        inner.ensure_writable()?;
        Ok(inner.committed_len)
    }

    /// `SubscriptionGroupManager.getSubscriptionGroupTable`.
    pub fn groups(&self) -> Result<BTreeMap<String, GroupConfig>> {
        Ok(self.lock()?.groups.clone())
    }

    pub fn group(&self, group: &str) -> Result<Option<GroupConfig>> {
        Ok(self.lock()?.groups.get(group).cloned())
    }

    /// Replace one group, mirroring `updateSubscriptionGroupConfig`, which persists immediately.
    pub fn put_group(&self, config: GroupConfig) -> Result<()> {
        validate_group(&config.group_name, &config)?;
        let mut inner = self.lock()?;
        inner.ensure_writable()?;
        let mut groups = inner.groups.clone();
        groups.insert(config.group_name.clone(), config);
        let bytes = encode_json(&groups)?;
        inner.persist(GROUPS_FILE, &bytes)?;
        inner.groups = groups;
        Ok(())
    }

    /// `insertIfAbsent`, which keeps the configuration a heartbeat auto-created earlier.
    pub fn put_group_if_absent(&self, config: GroupConfig) -> Result<Option<GroupConfig>> {
        validate_group(&config.group_name, &config)?;
        let mut inner = self.lock()?;
        inner.ensure_writable()?;
        if let Some(existing) = inner.groups.get(&config.group_name) {
            return Ok(Some(existing.clone()));
        }
        let mut groups = inner.groups.clone();
        groups.insert(config.group_name.clone(), config.clone());
        let bytes = encode_json(&groups)?;
        inner.persist(GROUPS_FILE, &bytes)?;
        inner.groups = groups;
        Ok(None)
    }

    /// `deleteSubscriptionGroupConfig`; `clean_offset` also drops the group's committed offsets.
    pub fn delete_group(&self, group: &str, clean_offset: bool) -> Result<bool> {
        let mut inner = self.lock()?;
        inner.ensure_writable()?;
        if !inner.groups.contains_key(group) {
            return Ok(false);
        }
        let mut groups = inner.groups.clone();
        groups.remove(group);
        let bytes = encode_json(&groups)?;
        inner.persist(GROUPS_FILE, &bytes)?;
        inner.groups = groups;
        if clean_offset {
            let mut offsets = inner.offsets.clone();
            let dropped: Vec<String> = offsets
                .offset_table
                .keys()
                .filter(|key| key.split_once('@').is_some_and(|(_, g)| g == group))
                .cloned()
                .collect();
            if !dropped.is_empty() {
                for key in dropped {
                    offsets.offset_table.remove(&key);
                }
                let bytes = encode_json(&offsets)?;
                inner.persist(OFFSETS_FILE, &bytes)?;
                inner.offsets = offsets;
            }
        }
        Ok(true)
    }

    /// `ScheduleMessageService.getOffsetTable`, as persisted by its 10 second task.
    pub fn delay_offsets(&self) -> Result<DelayOffsets> {
        Ok(self.lock()?.delay_offsets.clone())
    }

    pub fn put_delay_offsets(&self, offsets: &DelayOffsets) -> Result<()> {
        for level in offsets.offset_table.keys() {
            if *level < 1 {
                return Err(invalid("delay offset level must be positive"));
            }
        }
        let bytes = encode_json(offsets)?;
        let mut inner = self.lock()?;
        inner.ensure_writable()?;
        inner.persist(DELAY_OFFSETS_FILE, &bytes)?;
        inner.delay_offsets = offsets.clone();
        Ok(())
    }

    /// `TimerCheckpoint`, of which only the queue cursor cannot be rebuilt from the timer queue.
    pub fn timer_checkpoint(&self) -> Result<TimerCheckpoint> {
        Ok(self.lock()?.timer_checkpoint)
    }

    pub fn put_timer_checkpoint(&self, checkpoint: &TimerCheckpoint) -> Result<()> {
        let bytes = encode_json(checkpoint)?;
        let mut inner = self.lock()?;
        inner.ensure_writable()?;
        inner.persist(TIMER_CHECKPOINT_FILE, &bytes)?;
        inner.timer_checkpoint = *checkpoint;
        Ok(())
    }

    fn lock(&self) -> Result<MutexGuard<'_, Inner>> {
        self.inner
            .lock()
            .map_err(|_| invalid("store mutex was poisoned; reopen the store"))
    }
}

impl Inner {
    fn sync_log(&self) -> io::Result<()> {
        #[cfg(test)]
        if self.fail_log_sync {
            return Err(io::Error::other("injected commitlog sync failure"));
        }
        self.log.sync_data()
    }

    fn ensure_writable(&self) -> Result<()> {
        if self.fenced {
            Err(invalid(
                "store has an uncertain write/sync outcome; reopen before writing",
            ))
        } else {
            Ok(())
        }
    }

    fn persist(&mut self, name: &str, bytes: &[u8]) -> Result<()> {
        if let Err(error) = replace_json(&self.directory, name, bytes) {
            self.fenced = true;
            return Err(error);
        }
        Ok(())
    }
}

fn invalid(message: impl Into<String>) -> Error {
    Error::Invalid(message.into())
}

fn validate_name(name: &str, kind: &str) -> Result<()> {
    // A light message queue is named `%LMQ%$parent$lite`, so it carries the `$`
    // separator that a plain topic name may not use.
    if is_lite_topic_queue(name) {
        return Ok(());
    }
    if name.is_empty()
        || name.len() > 255
        || name == "."
        || name == ".."
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"_-%.|".contains(&byte))
    {
        return Err(invalid(format!(
            "invalid {kind}: use 1..255 ASCII letters/digits or _-%.| (no @ or path separators)"
        )));
    }
    Ok(())
}

fn validate_queue(queue_id: i32) -> Result<()> {
    if !(0..MAX_QUEUES).contains(&queue_id) {
        return Err(invalid("queue_id must be in 0..1024"));
    }
    Ok(())
}

fn validate_group(name: &str, config: &GroupConfig) -> Result<()> {
    validate_name(name, "consumer group")?;
    if name != config.group_name {
        return Err(invalid("group state key does not match groupName"));
    }
    if config.retry_queue_nums < 0 || config.retry_queue_nums > MAX_QUEUES {
        return Err(invalid("retryQueueNums must be in 0..=1024"));
    }
    if config.retry_max_times < 0 {
        return Err(invalid("retryMaxTimes must not be negative"));
    }
    if config.consume_timeout_minute <= 0 {
        return Err(invalid("consumeTimeoutMinute must be positive"));
    }
    Ok(())
}

fn validate_topic(config: &TopicConfig) -> Result<()> {
    validate_name(&config.topic_name, "topic")?;
    if !(1..=MAX_QUEUES).contains(&config.read_queue_nums)
        || !(1..=MAX_QUEUES).contains(&config.write_queue_nums)
    {
        return Err(invalid("topic read/write queue counts must be in 1..=1024"));
    }
    if config.perm < 0 || config.perm & !7 != 0 {
        return Err(invalid(
            "topic permission must contain only inherit/read/write bits",
        ));
    }
    Ok(())
}

fn validate_message(message: &Message) -> Result<()> {
    validate_name(&message.topic, "topic")?;
    validate_queue(message.queue_id)?;
    if message.body.len() > MAX_RECORD_BYTES {
        return Err(invalid("message body exceeds 16 MiB"));
    }
    let mut properties_size = 0usize;
    for (key, value) in &message.properties {
        if key.is_empty() || key.contains(['\u{1}', '\u{2}']) || value.contains(['\u{1}', '\u{2}'])
        {
            return Err(invalid(
                "message properties contain empty keys or reserved separators",
            ));
        }
        properties_size = properties_size
            .checked_add(key.len())
            .and_then(|size| size.checked_add(value.len()))
            .and_then(|size| size.checked_add(2))
            .ok_or_else(|| invalid("message properties length overflow"))?;
        if properties_size > i16::MAX as usize {
            return Err(invalid(
                "message properties exceed RocketMQ's signed short limit",
            ));
        }
    }
    Ok(())
}

fn validate_record_length(length: usize) -> Result<()> {
    if !(MIN_RECORD_BYTES..=MAX_RECORD_BYTES).contains(&length) {
        return Err(invalid(format!(
            "invalid commitlog record length: {length}"
        )));
    }
    Ok(())
}

fn offset_key(group: &str, topic: &str, queue_id: i32) -> Result<String> {
    validate_name(group, "consumer group")?;
    validate_name(topic, "topic")?;
    validate_queue(queue_id)?;
    Ok(format!("{topic}@{group}"))
}

fn validate_offsets(offsets: &Offsets) -> Result<()> {
    for (key, queues) in &offsets.offset_table {
        let (topic, group) = key
            .split_once('@')
            .ok_or_else(|| invalid("consumer offset key must be topic@group"))?;
        validate_name(topic, "topic")?;
        validate_name(group, "consumer group")?;
        for &queue_id in queues.keys() {
            validate_queue(queue_id)?;
        }
    }
    Ok(())
}

fn add_index(queues: &mut Queues, message: &Message, length: u32) {
    push_location(queues, &message.topic, message.queue_id, message, length);
    // `ConsumeQueue.multiDispatchLmqQueue`: a message dispatched to light queues
    // gets one more consume queue entry per light queue, pointing at the same
    // commitlog record.
    for (lmq, queue_id) in lmq_dispatch_targets(message) {
        push_location(queues, &lmq, queue_id, message, length);
    }
}

fn push_location(queues: &mut Queues, topic: &str, queue_id: i32, message: &Message, length: u32) {
    let queue = queues.entry((topic.to_owned(), queue_id)).or_default();
    let timestamp_ceiling = queue.last().map_or(message.store_timestamp, |previous| {
        previous.timestamp_ceiling.max(message.store_timestamp)
    });
    queue.push(Location {
        physical: message.physical_offset,
        length,
        timestamp_ceiling,
    });
}

/// `LmqDispatch.parseLmqQueueNames`: the light queues a message is dispatched to.
/// A lite message names its light queue through `PROPERTY_INNER_MULTI_DISPATCH`.
fn lmq_dispatch_targets(message: &Message) -> Vec<(String, i32)> {
    let Some(raw) = message.properties.get(PROPERTY_INNER_MULTI_DISPATCH) else {
        return Vec::new();
    };
    raw.split(LMQ_DISPATCH_SEPARATOR)
        .filter(|name| !name.is_empty())
        .filter(|name| is_lmq(name))
        .map(|name| (name.to_owned(), LMQ_QUEUE_ID))
        .collect()
}

/// A light queue holds the record of its parent topic, so the record keeps the
/// parent's topic while the light queue's own queue and offset are what the
/// consumer sees.
fn read_light_queue_record(message: &mut Message, topic: &str, queue_offset: u64) -> Result<()> {
    let Some(lite) = get_lite_topic(topic) else {
        return Err(invalid("light queue name is malformed"));
    };
    if message
        .properties
        .get(PROPERTY_LITE_TOPIC)
        .map(String::as_str)
        != Some(lite)
    {
        return Err(invalid(
            "commitlog record no longer matches its light queue index",
        ));
    }
    message.queue_id = LMQ_QUEUE_ID;
    message.queue_offset = queue_offset;
    Ok(())
}

/// `CommitLogDispatcherBuildIndex`: one entry per space-separated `KEYS` property value.
fn add_key_index(keys: &mut Keys, message: &Message) {
    let Some(raw) = message.properties.get("KEYS") else {
        return;
    };
    for key in raw.split(' ') {
        if key.is_empty() {
            continue;
        }
        keys.entry((message.topic.clone(), key.to_owned()))
            .or_default()
            .push(KeyLocation {
                physical: message.physical_offset,
                timestamp: message.store_timestamp,
            });
    }
}

fn recover(
    log: &mut File,
    topics: &mut BTreeMap<String, TopicConfig>,
) -> Result<(Queues, BTreeMap<u64, u32>, Keys, u64)> {
    let file_len = log.metadata()?.len();
    let mut position = 0u64;
    let mut queues = Queues::new();
    let mut physical = BTreeMap::new();
    let mut key_index = Keys::new();
    while position < file_len {
        let remaining = file_len - position;
        if remaining < 4 {
            break; // Torn length field at EOF only.
        }
        log.seek(SeekFrom::Start(position))?;
        let mut header = [0u8; 4];
        log.read_exact(&mut header)?;
        let length = u32::from_be_bytes(header);
        // A complete invalid length is corruption, even at the very end.
        validate_record_length(length as usize)?;
        if u64::from(length) > remaining {
            // Check the fixed prefix so corruption cannot masquerade as an incomplete tail.
            validate_partial_prefix(log, position, remaining)?;
            break;
        }
        let message = read_record(log, position, length)?;
        validate_message(&message)?;
        let key = (message.topic.clone(), message.queue_id);
        let expected_queue_offset = queues.get(&key).map_or(0, |queue| queue.len() as u64);
        if message.queue_offset != expected_queue_offset {
            return Err(invalid(format!(
                "non-contiguous queue offset at physical {position}"
            )));
        }
        let config = topics
            .entry(message.topic.clone())
            .or_insert_with(|| TopicConfig::new(message.topic.clone(), 8));
        if message.queue_id >= config.read_queue_nums.max(config.write_queue_nums) {
            return Err(invalid(format!(
                "record queue is outside its topic config at {position}"
            )));
        }
        add_index(&mut queues, &message, length);
        // The light queue holds the record at its own offset; a mismatch means the
        // dispatch metadata no longer describes the index.
        for (index, (lmq, queue_id)) in lmq_dispatch_targets(&message).iter().enumerate() {
            let assigned = queues
                .get(&(lmq.clone(), *queue_id))
                .map_or(0, |queue| queue.len() as u64)
                .saturating_sub(1);
            let stored = message
                .properties
                .get(PROPERTY_INNER_MULTI_QUEUE_OFFSET)
                .and_then(|value| value.split(LMQ_DISPATCH_SEPARATOR).nth(index))
                .and_then(|value| value.parse::<u64>().ok());
            if stored != Some(assigned) {
                return Err(invalid(format!(
                    "light queue offset mismatch at physical {position}"
                )));
            }
        }
        add_key_index(&mut key_index, &message);
        physical.insert(position, length);
        position += u64::from(length);
    }
    if position != file_len {
        // Only a plausible unfinished EOF record is safe to discard, not corrupt complete records.
        log.set_len(position)?;
        log.sync_data()?;
    }
    Ok((queues, physical, key_index, position))
}

fn validate_partial_prefix(log: &mut File, physical: u64, remaining: u64) -> Result<()> {
    let length = remaining.min(36) as usize;
    let mut prefix = [0u8; 36];
    log.seek(SeekFrom::Start(physical))?;
    log.read_exact(&mut prefix[..length])?;
    if length >= 8 {
        let magic = i32::from_be_bytes(prefix[4..8].try_into().unwrap());
        if magic != MAGIC_V1 && magic != MAGIC_V2 {
            return Err(invalid("invalid magic in incomplete commitlog tail"));
        }
    }
    if length >= 36 {
        let stored_physical = u64::from_be_bytes(prefix[28..36].try_into().unwrap());
        if stored_physical != physical {
            return Err(invalid(
                "incorrect physical offset in incomplete commitlog tail",
            ));
        }
    }
    Ok(())
}

fn read_record(log: &mut File, physical: u64, length: u32) -> Result<Message> {
    validate_record_length(length as usize)?;
    log.seek(SeekFrom::Start(physical))?;
    let mut bytes = vec![0; length as usize];
    log.read_exact(&mut bytes)?;
    let (message, consumed) = Message::decode(&bytes)?;
    if consumed != bytes.len() || message.physical_offset != physical {
        return Err(invalid(format!(
            "invalid record boundary/physical offset at {physical}"
        )));
    }
    Ok(message)
}

fn reject_nonregular(path: &Path) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if !metadata.file_type().is_file() => Err(invalid(format!(
            "store file is not a regular file: {}",
            path.display()
        ))),
        Ok(_) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

fn load_json<T: DeserializeOwned>(path: &Path) -> Result<Option<T>> {
    reject_nonregular(path)?;
    let file = match File::open(path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    if file.metadata()?.len() > MAX_STATE_BYTES {
        return Err(invalid("store JSON state exceeds 64 MiB"));
    }
    let mut bytes = Vec::new();
    file.take(MAX_STATE_BYTES + 1).read_to_end(&mut bytes)?;
    if bytes.len() as u64 > MAX_STATE_BYTES {
        return Err(invalid("store JSON state exceeds 64 MiB"));
    }
    Ok(Some(serde_json::from_slice(&bytes)?))
}

fn encode_json<T: Serialize>(value: &T) -> Result<Vec<u8>> {
    let bytes = serde_json::to_vec(value)?;
    if bytes.len() as u64 > MAX_STATE_BYTES {
        return Err(invalid("store JSON state exceeds 64 MiB"));
    }
    Ok(bytes)
}

fn replace_json(directory: &Path, name: &str, bytes: &[u8]) -> Result<()> {
    let destination = directory.join(name);
    reject_nonregular(&destination)?;
    let mut temporary = None;
    // create_new prevents stale temp files from being reused or deleted as ours.
    for _ in 0..128 {
        let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let path = directory.join(format!(".{name}.{}.{sequence}.tmp", std::process::id()));
        match OpenOptions::new().write(true).create_new(true).open(&path) {
            Ok(file) => {
                temporary = Some((path, file));
                break;
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error.into()),
        }
    }
    let (path, mut file) = temporary.ok_or_else(|| invalid("cannot reserve a state temp file"))?;
    let written = (|| -> Result<()> {
        file.write_all(bytes)?;
        file.sync_all()?;
        fs::rename(&path, &destination)?;
        sync_directory(directory)?;
        Ok(())
    })();
    if written.is_err() {
        // Only this invocation's create_new file; after rename this is a no-op.
        let _ = fs::remove_file(&path);
    }
    written
}

fn sync_directory(directory: &Path) -> Result<()> {
    File::open(directory)?.sync_all()?;
    Ok(())
}

fn create_directory_durably(path: &Path) -> Result<()> {
    // Sync parents as well so newly created directory entries survive a crash.
    let absolute = if path.is_absolute() {
        path.to_owned()
    } else {
        std::env::current_dir()?.join(path)
    };
    let mut missing = Vec::new();
    let mut cursor = absolute.as_path();
    while !cursor.try_exists()? {
        missing.push(cursor.to_owned());
        cursor = cursor
            .parent()
            .ok_or_else(|| invalid("invalid store directory"))?;
    }
    if !cursor.is_dir() {
        return Err(invalid("store path has a non-directory ancestor"));
    }
    for directory in missing.into_iter().rev() {
        match fs::create_dir(&directory) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists && directory.is_dir() => {}
            Err(error) => return Err(error.into()),
        }
        sync_directory(&directory)?;
        if let Some(parent) = directory.parent() {
            sync_directory(parent)?;
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn message(topic: &str, queue_id: i32, body: &[u8], tag: &str) -> Message {
        let mut message = Message {
            topic: topic.to_owned(),
            queue_id,
            body: body.to_vec(),
            born_timestamp: now_millis(),
            ..Message::default()
        };
        if !tag.is_empty() {
            message.properties.insert("TAGS".to_owned(), tag.to_owned());
        }
        message
    }

    fn tags(expression: &str) -> Subscription {
        Subscription::build(Some("TAG"), expression).unwrap()
    }

    fn sql(expression: &str) -> Subscription {
        Subscription::build(Some("SQL92"), expression).unwrap()
    }

    fn raw_record(queue_offset: u64, physical_offset: u64) -> Vec<u8> {
        Message {
            queue_offset,
            physical_offset,
            store_timestamp: 100,
            ..message("topic", 0, b"recovery-body", "A")
        }
        .encode()
        .unwrap()
    }

    fn write_tail(path: &Path, bytes: &[u8]) {
        let mut file = OpenOptions::new()
            .append(true)
            .open(path.join(LOG_FILE))
            .unwrap();
        file.write_all(bytes).unwrap();
        file.sync_all().unwrap();
    }

    #[test]
    fn reopen_preserves_messages_topics_and_consumer_offsets() {
        fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<Store>();
        let directory = TempDir::new().unwrap();
        let mut topic = TopicConfig::new("orders", 4);
        topic.attributes.insert("owner".into(), "test".into());
        let (saved, physical_len) = {
            let store = Store::open(directory.path().join("nested/store")).unwrap();
            store.put_topic(topic.clone()).unwrap();
            store.put_topic(TopicConfig::new("empty", 2)).unwrap();
            let saved = store
                .append(message("orders", 3, b"persist me", "A"))
                .unwrap();
            store.commit_offset("group", "orders", 3, 1).unwrap();
            store.flush().unwrap();
            (saved, store.physical_len().unwrap())
        };
        let store = Store::open(directory.path().join("nested/store")).unwrap();
        assert_eq!(store.topics().unwrap()["orders"], topic);
        assert!(store.topics().unwrap().contains_key("empty"));
        assert_eq!(
            store.consumer_offset("group", "orders", 3).unwrap(),
            Some(1)
        );
        assert_eq!(store.consumer_offset("other", "orders", 3).unwrap(), None);
        assert_eq!(
            store.all_offsets().unwrap(),
            serde_json::json!({
                "offsetTable": {"orders@group": {"3": 1}}
            })
        );
        assert_eq!(store.physical_len().unwrap(), physical_len);
        assert_eq!(
            store.message_at(saved.physical_offset).unwrap(),
            Some(saved.clone())
        );
        assert_eq!(store.message_at(1).unwrap(), None);
        assert_eq!(store.message_at(physical_len).unwrap(), None);
        assert_eq!(
            store.read("orders", 3, 0, 8, 1024, None).unwrap().messages,
            vec![saved]
        );
    }

    #[test]
    fn a_lite_message_gets_a_light_queue_with_an_offset_of_its_own() {
        let directory = TempDir::new().unwrap();
        let store = Store::open(directory.path()).unwrap();
        store.put_topic(TopicConfig::new("bind", 4)).unwrap();
        let mut lite = message("bind", 0, b"lite-body", "");
        lite.properties
            .insert(PROPERTY_LITE_TOPIC.to_owned(), "child".to_owned());
        let saved = store.append(lite).unwrap();
        // The broker names the light queue of a lite message.
        assert_eq!(
            saved
                .properties
                .get(PROPERTY_INNER_MULTI_DISPATCH)
                .map(String::as_str),
            Some("%LMQ%$bind$child")
        );
        assert_eq!(
            saved
                .properties
                .get(PROPERTY_INNER_MULTI_QUEUE_OFFSET)
                .map(String::as_str),
            Some("0")
        );
        // The bind topic keeps the message in its own queue.
        let parent = store.read("bind", 0, 0, 8, 1024, None).unwrap();
        assert_eq!(parent.messages.len(), 1);
        assert_eq!(parent.messages[0].body, b"lite-body");
        // The light queue holds the same record under an offset of its own.
        let light = store.read("%LMQ%$bind$child", 0, 0, 8, 1024, None).unwrap();
        assert_eq!(light.messages.len(), 1);
        assert_eq!(light.messages[0].body, b"lite-body");
        assert_eq!(light.messages[0].queue_id, 0);
        assert_eq!(light.messages[0].queue_offset, 0);
        assert_eq!(store.queue_max_offset("%LMQ%$bind$child", 0), 1);
        // A second lite topic of the same bind topic has its own light queue.
        let mut other = message("bind", 1, b"other-body", "");
        other
            .properties
            .insert(PROPERTY_LITE_TOPIC.to_owned(), "second".to_owned());
        store.append(other).unwrap();
        assert_eq!(store.queue_max_offset("%LMQ%$bind$second", 0), 1);
        assert_eq!(store.queue_max_offset("%LMQ%$bind$child", 0), 1);
        // The offsets of the light queue are committed against its own name.
        store
            .commit_offset("group", "%LMQ%$bind$child", 0, 1)
            .unwrap();
        assert_eq!(
            store
                .consumer_offset("group", "%LMQ%$bind$child", 0)
                .unwrap(),
            Some(1)
        );
        assert_eq!(store.consumer_offset("group", "bind", 0).unwrap(), None);
    }

    #[test]
    fn a_lite_message_survives_a_restart_with_its_light_queue() {
        let directory = TempDir::new().unwrap();
        {
            let store = Store::open(directory.path()).unwrap();
            store.put_topic(TopicConfig::new("bind", 4)).unwrap();
            let mut lite = message("bind", 0, b"lite-body", "");
            lite.properties
                .insert(PROPERTY_LITE_TOPIC.to_owned(), "child".to_owned());
            store.append(lite).unwrap();
            store.flush().unwrap();
        }
        let store = Store::open(directory.path()).unwrap();
        let light = store.read("%LMQ%$bind$child", 0, 0, 8, 1024, None).unwrap();
        assert_eq!(light.messages.len(), 1);
        assert_eq!(light.messages[0].body, b"lite-body");
        assert_eq!(light.messages[0].queue_offset, 0);
    }

    #[test]
    fn batches_and_interleaved_queues_keep_independent_contiguous_offsets() {
        let directory = TempDir::new().unwrap();
        let store = Store::open(directory.path()).unwrap();
        let batch = store
            .append_batch(vec![
                message("topic", 0, b"a", "A"),
                message("topic", 1, b"b", "B"),
                message("topic", 0, b"c", "C"),
                message("other", 0, b"d", "D"),
            ])
            .unwrap();
        assert_eq!(
            batch.iter().map(|m| m.queue_offset).collect::<Vec<_>>(),
            vec![0, 0, 1, 0]
        );
        let mut physical = 0;
        for entry in &batch {
            assert_eq!(entry.physical_offset, physical);
            physical += entry.encode().unwrap().len() as u64;
        }
        assert_eq!(store.physical_len().unwrap(), physical);
        assert!(store.append_batch(vec![]).unwrap().is_empty());
        drop(store);
        let store = Store::open(directory.path()).unwrap();
        assert_eq!(store.queue_max_offset("topic", 0), 2);
        assert_eq!(store.queue_max_offset("topic", 1), 1);
        assert_eq!(store.queue_max_offset("other", 0), 1);
        let next = store.append(message("topic", 0, b"next", "A")).unwrap();
        assert_eq!(next.queue_offset, 2);
        assert_eq!(next.physical_offset, physical);
        assert!(store.topics().unwrap().contains_key("other"));
    }

    #[test]
    fn tag_filters_advance_offsets_and_do_not_skip_a_byte_limited_match() {
        let directory = TempDir::new().unwrap();
        let store = Store::open(directory.path()).unwrap();
        let saved = store
            .append_batch(vec![
                message("topic", 0, b"zero", "X"),
                message("topic", 0, b"one", "A"),
                message("topic", 0, b"two", "X"),
                message("topic", 0, b"three", "B"),
                message("topic", 0, b"four", "X"),
            ])
            .unwrap();
        let padded = tags(" A || B ");
        let ab = tags("A || B");
        let none = tags("none");
        let all = tags("*");
        let result = store
            .read("topic", 0, 0, 8, usize::MAX, Some(&padded))
            .unwrap();
        assert_eq!(result.messages, vec![saved[1].clone(), saved[3].clone()]);
        assert_eq!(
            (
                result.next_begin_offset,
                result.min_offset,
                result.max_offset
            ),
            (5, 0, 5)
        );
        let result = store.read("topic", 0, 0, 8, 0, Some(&ab)).unwrap();
        assert_eq!(result.messages, vec![saved[1].clone()]);
        assert_eq!(result.next_begin_offset, 2);
        let first_size = saved[1].encode().unwrap().len();
        let result = store
            .read("topic", 0, 0, 8, first_size + 1, Some(&ab))
            .unwrap();
        assert_eq!(result.next_begin_offset, 3);
        assert_eq!(result.messages.len(), 1);
        let result = store.read("topic", 0, 3, 8, 1, Some(&ab)).unwrap();
        assert_eq!(result.messages, vec![saved[3].clone()]);
        assert_eq!(result.next_begin_offset, 4);
        let result = store
            .read("topic", 0, 0, 1, usize::MAX, Some(&none))
            .unwrap();
        assert!(result.messages.is_empty());
        assert_eq!(result.next_begin_offset, 5);
        assert_eq!(
            store
                .read("topic", 0, 99, 1, 1, None)
                .unwrap()
                .next_begin_offset,
            5
        );
        assert_eq!(
            store
                .read("topic", 0, 0, 0, 1, None)
                .unwrap()
                .next_begin_offset,
            0
        );
        assert_eq!(
            store
                .read("topic", 0, 0, 1, 1, Some(&all))
                .unwrap()
                .messages[0],
            saved[0]
        );
    }

    #[test]
    fn sql_filters_reject_missing_unparseable_and_non_matching_properties() {
        let directory = TempDir::new().unwrap();
        let store = Store::open(directory.path()).unwrap();
        let with = |body: &[u8], properties: &[(&str, &str)]| {
            let mut message = message("topic", 0, body, "X");
            for (key, value) in properties {
                message
                    .properties
                    .insert((*key).to_owned(), (*value).to_owned());
            }
            message
        };
        let saved = store
            .append_batch(vec![
                with(b"absent", &[]),
                with(b"ten", &[("level", "10")]),
                with(b"text", &[("level", "high")]),
                with(b"two", &[("level", "2")]),
                with(b"named", &[("level", "10"), ("channel", "web")]),
            ])
            .unwrap();
        let result = store
            .read("topic", 0, 0, 8, usize::MAX, Some(&sql("level > 5")))
            .unwrap();
        assert_eq!(
            result
                .messages
                .iter()
                .map(|message| message.queue_offset)
                .collect::<Vec<_>>(),
            vec![1, 4]
        );
        assert_eq!(result.next_begin_offset, 5);
        let both = store
            .read(
                "topic",
                0,
                0,
                8,
                usize::MAX,
                Some(&sql("level > 5 AND channel = 'web'")),
            )
            .unwrap();
        assert_eq!(both.messages, vec![saved[4].clone()]);
        // A property that cannot be converted to a number fails evaluation, so it never matches.
        let result = store
            .read("topic", 0, 0, 8, usize::MAX, Some(&sql("level >= 0")))
            .unwrap();
        assert_eq!(
            result
                .messages
                .iter()
                .map(|message| message.queue_offset)
                .collect::<Vec<_>>(),
            vec![1, 3, 4]
        );
        // The original parser rejects a string constant on the right of an ordered comparison.
        assert!(Subscription::build(Some("SQL92"), "level > 'x'").is_err());
    }

    #[test]
    fn consumer_filters_round_trip_and_are_marked_dead_on_reload() {
        let directory = TempDir::new().unwrap();
        let born_time = now_millis();
        let filter = FilterSnapshot {
            topic: "orders".into(),
            consumer_group: "sql-group".into(),
            expression: "level > 5".into(),
            expression_type: "SQL92".into(),
            born_time,
            dead_time: 0,
            client_version: 7,
        };
        {
            let store = Store::open(directory.path()).unwrap();
            store
                .put_consumer_filters(std::slice::from_ref(&filter))
                .unwrap();
            let raw: serde_json::Value =
                serde_json::from_slice(&fs::read(directory.path().join(FILTERS_FILE)).unwrap())
                    .unwrap();
            assert_eq!(raw["filterDataByTopic"]["orders"]["topic"], "orders");
            assert_eq!(
                raw["filterDataByTopic"]["orders"]["groupFilterData"]["sql-group"],
                serde_json::json!({
                    "topic": "orders",
                    "consumerGroup": "sql-group",
                    "expression": "level > 5",
                    "expressionType": "SQL92",
                    "bornTime": born_time,
                    "deadTime": 0,
                    "clientVersion": 7,
                })
            );
        }
        let store = Store::open(directory.path()).unwrap();
        let restored = store.consumer_filters().unwrap();
        assert_eq!(restored.len(), 1);
        // Recovery marks every persisted filter dead until a heartbeat revives it.
        assert!(restored[0].dead_time >= born_time);
        assert_eq!(restored[0].expression, filter.expression);
        assert_eq!(restored[0].client_version, 7);
        store.put_consumer_filters(&[]).unwrap();
        drop(store);
        assert!(
            Store::open(directory.path())
                .unwrap()
                .consumer_filters()
                .unwrap()
                .is_empty()
        );
        let store = Store::open(directory.path()).unwrap();
        assert!(
            store
                .put_consumer_filters(&[FilterSnapshot {
                    topic: "../escape".into(),
                    ..filter
                }])
                .is_err()
        );
    }

    #[test]
    fn filtering_has_a_bounded_scan_and_can_continue() {
        let directory = TempDir::new().unwrap();
        let store = Store::open(directory.path()).unwrap();
        for _ in 0..5 {
            store
                .append_batch((0..1024).map(|_| message("topic", 0, b"x", "A")).collect())
                .unwrap();
        }
        let b = tags("B");
        let result = store
            .read("topic", 0, 0, 100, usize::MAX, Some(&b))
            .unwrap();
        assert!(result.messages.is_empty());
        assert_eq!(result.next_begin_offset, MAX_SCAN_RECORDS as u64);
        let result = store
            .read(
                "topic",
                0,
                result.next_begin_offset,
                100,
                usize::MAX,
                Some(&b),
            )
            .unwrap();
        assert_eq!(result.next_begin_offset, 5120);
    }

    #[test]
    fn only_incomplete_eof_records_are_truncated() {
        let record = raw_record(1, raw_record(0, 0).len() as u64);
        for tail_len in [1, 2, 3, 4, 7, 8, 35, 36, 80, record.len() - 1] {
            let directory = TempDir::new().unwrap();
            let store = Store::open(directory.path()).unwrap();
            drop(store);
            let first = raw_record(0, 0);
            write_tail(directory.path(), &first);
            write_tail(directory.path(), &record[..tail_len]);
            let store = Store::open(directory.path()).unwrap();
            assert_eq!(store.physical_len().unwrap(), first.len() as u64);
            assert_eq!(store.queue_max_offset("topic", 0), 1);
            assert_eq!(
                fs::metadata(directory.path().join(LOG_FILE)).unwrap().len(),
                first.len() as u64
            );
            assert_eq!(
                store
                    .append(message("topic", 0, b"after", "A"))
                    .unwrap()
                    .queue_offset,
                1
            );
        }
    }

    #[test]
    fn complete_crc_corruption_fails_closed_without_truncation() {
        let directory = TempDir::new().unwrap();
        drop(Store::open(directory.path()).unwrap());
        let mut bytes = raw_record(0, 0);
        bytes[8] ^= 1; // Body CRC field, with an otherwise complete valid record.
        write_tail(directory.path(), &bytes);
        assert!(Store::open(directory.path()).is_err());
        assert_eq!(fs::read(directory.path().join(LOG_FILE)).unwrap(), bytes);
    }

    #[test]
    fn invalid_lengths_and_noncontiguous_offsets_fail_closed() {
        for length in [0u32, 4, 90, (MAX_RECORD_BYTES + 1) as u32, u32::MAX] {
            let directory = TempDir::new().unwrap();
            drop(Store::open(directory.path()).unwrap());
            let bytes = length.to_be_bytes();
            write_tail(directory.path(), &bytes);
            assert!(Store::open(directory.path()).is_err());
            assert_eq!(fs::read(directory.path().join(LOG_FILE)).unwrap(), bytes);
        }
        for (queue, physical) in [(1, 0), (0, 1)] {
            let directory = TempDir::new().unwrap();
            drop(Store::open(directory.path()).unwrap());
            let bytes = raw_record(queue, physical);
            write_tail(directory.path(), &bytes);
            assert!(Store::open(directory.path()).is_err());
            assert_eq!(fs::read(directory.path().join(LOG_FILE)).unwrap(), bytes);
        }
    }

    #[test]
    fn duplicate_open_is_rejected_and_drop_releases_the_lock() {
        let directory = TempDir::new().unwrap();
        let store = Store::open(directory.path()).unwrap();
        assert!(Store::open(directory.path()).is_err());
        assert!(Store::open(directory.path().join(".")).is_err());
        store
            .append(message("topic", 0, b"still usable", "A"))
            .unwrap();
        drop(store);
        assert!(Store::open(directory.path()).is_ok());
    }

    #[test]
    fn invalid_input_never_writes_a_partial_batch_or_state() {
        let directory = TempDir::new().unwrap();
        let store = Store::open(directory.path()).unwrap();
        store.put_topic(TopicConfig::new("topic", 2)).unwrap();
        let before = fs::read(directory.path().join(TOPICS_FILE)).unwrap();
        for bad in [
            message("../escape", 0, b"x", "A"),
            message("topic@group", 0, b"x", "A"),
            message("topic", -1, b"x", "A"),
            message("topic", 2, b"x", "A"),
            message("", 0, b"x", "A"),
            message("topic", 0, b"x", "bad\u{1}tag"),
        ] {
            assert!(
                store
                    .append_batch(vec![message("topic", 0, b"valid", "A"), bad])
                    .is_err()
            );
            assert_eq!(store.physical_len().unwrap(), 0);
            assert_eq!(store.queue_max_offset("topic", 0), 0);
        }
        let huge = message("topic", 0, &vec![0; MAX_RECORD_BYTES], "");
        assert!(store.append(huge).is_err());
        assert_eq!(
            fs::metadata(directory.path().join(LOG_FILE)).unwrap().len(),
            0
        );
        assert!(store.commit_offset("bad@group", "topic", 0, 1).is_err());
        assert!(store.commit_offset("group", "bad@topic", 0, 1).is_err());
        assert!(!directory.path().join(OFFSETS_FILE).exists());
        assert!(store.put_topic(TopicConfig::new("topic", 0)).is_err());
        assert_eq!(
            fs::read(directory.path().join(TOPICS_FILE)).unwrap(),
            before
        );
        assert_eq!(
            store
                .append(message("topic", 0, b"works", "A"))
                .unwrap()
                .queue_offset,
            0
        );
    }

    #[test]
    fn topic_queue_changes_do_not_orphan_data() {
        let directory = TempDir::new().unwrap();
        let store = Store::open(directory.path()).unwrap();
        store.put_topic(TopicConfig::new("topic", 2)).unwrap();
        store.append(message("topic", 1, b"existing", "A")).unwrap();
        assert!(store.put_topic(TopicConfig::new("topic", 1)).is_err());
        assert_eq!(store.topics().unwrap()["topic"].write_queue_nums, 2);
        let mut config = TopicConfig::new("topic", 2);
        config.write_queue_nums = 1;
        store.put_topic(config.clone()).unwrap();
        assert!(store.append(message("topic", 1, b"rejected", "A")).is_err());
        store.append(message("topic", 0, b"allowed", "A")).unwrap();
        drop(store);
        let store = Store::open(directory.path()).unwrap();
        assert_eq!(store.topics().unwrap()["topic"], config);
        assert_eq!(
            store
                .read("topic", 1, 0, 1, 1024, None)
                .unwrap()
                .messages
                .len(),
            1
        );
        store.put_topic(TopicConfig::new("topic", 3)).unwrap();
        store
            .append(message("topic", 2, b"new queue", "A"))
            .unwrap();
    }

    #[test]
    fn interrupted_batch_recovers_only_its_complete_prefix() {
        let directory = TempDir::new().unwrap();
        drop(Store::open(directory.path()).unwrap());
        let first = raw_record(0, 0);
        let second = raw_record(1, first.len() as u64);
        write_tail(directory.path(), &first);
        write_tail(directory.path(), &second[..second.len() / 2]);
        let store = Store::open(directory.path()).unwrap();
        assert_eq!(store.queue_max_offset("topic", 0), 1);
        assert_eq!(store.physical_len().unwrap(), first.len() as u64);
    }

    #[test]
    fn timestamp_search_is_a_lower_bound_even_when_clock_moves_backwards() {
        let directory = TempDir::new().unwrap();
        drop(Store::open(directory.path()).unwrap());
        let mut physical = 0;
        for (offset, timestamp) in [100, 300, 200, 400].into_iter().enumerate() {
            let bytes = Message {
                queue_offset: offset as u64,
                physical_offset: physical,
                store_timestamp: timestamp,
                ..message("topic", 0, b"x", "A")
            }
            .encode()
            .unwrap();
            write_tail(directory.path(), &bytes);
            physical += bytes.len() as u64;
        }
        let store = Store::open(directory.path()).unwrap();
        for (timestamp, expected) in [
            (0, 0),
            (100, 0),
            (101, 1),
            (250, 1),
            (300, 1),
            (301, 3),
            (401, 4),
        ] {
            assert_eq!(
                store.search_offset("topic", 0, timestamp).unwrap(),
                expected
            );
        }
        assert_eq!(store.search_offset("empty", 0, 100).unwrap(), 0);
    }

    #[test]
    fn failed_log_write_fences_mutations_without_publishing_indexes() {
        let directory = TempDir::new().unwrap();
        let store = Store::open(directory.path()).unwrap();
        // A read-only log handle forces a real write failure without releasing the directory lock.
        store.inner.lock().unwrap().log = File::open(directory.path().join(LOG_FILE)).unwrap();
        assert!(store.append(message("topic", 0, b"x", "A")).is_err());
        assert_eq!(store.queue_max_offset("topic", 0), 0);
        assert!(store.topics().unwrap().is_empty());
        assert!(store.append_batch(vec![]).is_err());
        assert!(store.put_topic(TopicConfig::new("other", 1)).is_err());
        assert!(store.commit_offset("group", "topic", 0, 1).is_err());
        assert!(store.flush().is_err());
        assert!(store.physical_len().is_err());
        drop(store);
        let store = Store::open(directory.path()).unwrap();
        assert!(store.append(message("topic", 0, b"reopened", "A")).is_ok());
    }

    #[test]
    fn sync_failure_keeps_the_whole_batch_invisible_and_fences_future_writes() {
        let directory = TempDir::new().unwrap();
        let store = Store::open(directory.path()).unwrap();
        store.inner.lock().unwrap().fail_log_sync = true;
        let batch = vec![
            message("topic", 0, b"first", "A"),
            message("topic", 0, b"second", "B"),
        ];
        assert!(store.append_batch(batch).is_err());
        assert!(fs::metadata(directory.path().join(LOG_FILE)).unwrap().len() > 0);
        assert_eq!(store.queue_max_offset("topic", 0), 0);
        assert_eq!(store.message_at(0).unwrap(), None);
        assert!(
            store
                .read("topic", 0, 0, 10, 1024, None)
                .unwrap()
                .messages
                .is_empty()
        );
        assert!(store.topics().unwrap().is_empty());
        store.inner.lock().unwrap().fail_log_sync = false;
        assert!(
            store
                .append(message("topic", 0, b"must not append", "A"))
                .is_err()
        );
        assert!(store.flush().is_err());
        drop(store);
        // Injected sync failure is not a power cut, so unacknowledged complete records can survive.
        let store = Store::open(directory.path()).unwrap();
        assert_eq!(store.queue_max_offset("topic", 0), 2);
        assert_eq!(
            store
                .append(message("topic", 0, b"next", "A"))
                .unwrap()
                .queue_offset,
            2
        );
    }

    #[test]
    fn failed_state_replacement_does_not_publish_memory_and_fences_writes() {
        let directory = TempDir::new().unwrap();
        let store = Store::open(directory.path()).unwrap();
        fs::create_dir(directory.path().join(TOPICS_FILE)).unwrap();
        assert!(store.put_topic(TopicConfig::new("topic", 2)).is_err());
        assert!(store.topics().unwrap().is_empty());
        assert!(
            store
                .append(message("topic", 0, b"must not append", "A"))
                .is_err()
        );
        assert_eq!(store.queue_max_offset("topic", 0), 0);
    }

    #[test]
    fn malformed_json_is_not_treated_as_empty_state() {
        let directory = TempDir::new().unwrap();
        drop(Store::open(directory.path()).unwrap());
        fs::write(directory.path().join(OFFSETS_FILE), b"{broken").unwrap();
        assert!(Store::open(directory.path()).is_err());
        fs::write(
            directory.path().join(OFFSETS_FILE),
            br#"{"offsetTable":{"topic@group@ambiguous":{"0":1}}}"#,
        )
        .unwrap();
        assert!(Store::open(directory.path()).is_err());
    }

    fn keyed(topic: &str, queue_id: i32, body: &[u8], keys: &str, tag: &str) -> Message {
        let mut message = message(topic, queue_id, body, tag);
        message
            .properties
            .insert("KEYS".to_owned(), keys.to_owned());
        message
    }

    #[test]
    fn key_index_answers_key_queries_and_survives_reopen() {
        let directory = TempDir::new().unwrap();
        let store = Store::open(directory.path()).unwrap();
        let first = store
            .append(keyed("orders", 0, b"one", "order-1 order-2", "A"))
            .unwrap();
        let second = store
            .append(keyed("orders", 1, b"two", "order-1", "B"))
            .unwrap();
        store.append(message("orders", 0, b"no-keys", "C")).unwrap();

        let hits = store
            .query_messages("orders", "order-1", 0, i64::MAX, 32)
            .unwrap();
        assert_eq!(hits, vec![first.clone(), second.clone()]);
        assert!(
            store
                .query_messages("orders", "order-missing", 0, i64::MAX, 32)
                .unwrap()
                .is_empty()
        );
        // Time windows are inclusive bounds on the store timestamp.
        let window = store
            .query_messages("orders", "order-1", first.store_timestamp, i64::MAX, 32)
            .unwrap();
        assert!(
            window
                .iter()
                .all(|message| message.store_timestamp >= first.store_timestamp)
        );

        drop(store);
        // Recovery rebuilds the key index from the commitlog.
        let store = Store::open(directory.path()).unwrap();
        assert_eq!(
            store
                .query_messages("orders", "order-1", 0, i64::MAX, 32)
                .unwrap(),
            vec![first, second]
        );
    }

    #[test]
    fn scan_messages_filters_by_window_and_subscription() {
        let directory = TempDir::new().unwrap();
        let store = Store::open(directory.path()).unwrap();
        let first = store.append(message("topic", 0, b"one", "A")).unwrap();
        let _second = store.append(message("topic", 1, b"two", "B")).unwrap();
        let _third = store.append(message("topic", 0, b"three", "C")).unwrap();

        // Unfiltered: every record inside the full window, across queues.
        assert_eq!(
            store
                .scan_messages("topic", 0, i64::MAX, 32, None)
                .unwrap()
                .len(),
            3
        );
        // Subscription filter narrows to tag A.
        let tagged = store
            .scan_messages("topic", 0, i64::MAX, 32, Some(&tags("A")))
            .unwrap();
        assert_eq!(tagged, vec![first]);
        // max_nums caps results.
        assert_eq!(
            store
                .scan_messages("topic", 0, i64::MAX, 1, None)
                .unwrap()
                .len(),
            1
        );
        // A begin past every store time and an inverted window are empty, not errors.
        assert!(
            store
                .scan_messages("topic", i64::MAX, i64::MAX, 32, None)
                .unwrap()
                .is_empty()
        );
        assert!(
            store
                .scan_messages("topic", i64::MAX, 0, 32, None)
                .unwrap()
                .is_empty()
        );
        // Empty topics scan to nothing.
        assert!(
            store
                .scan_messages("absent", 0, i64::MAX, 32, None)
                .unwrap()
                .is_empty()
        );
    }
}
