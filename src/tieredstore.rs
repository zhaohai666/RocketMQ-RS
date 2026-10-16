//! Tiered (cold) storage, ported from the Java `rocketmq-tieredstore` module
//! (`org.apache.rocketmq.tieredstore`) into a single self-contained file.
//!
//! Java class mapping and simplifications:
//! - `MessageStoreConfig` + `TieredStorageLevel` -> [`TieredStoreConfig`] /
//!   [`TieredStorageLevel`]. Config fields are reduced to the core set; size
//!   fields accept either integers or strings such as `"1g"` / `"512m"`.
//! - `TieredMessageStore` -> [`TieredMessageStore`]. The Java plugin `next`
//!   store (`MessageStore`) is replaced by the injected [`HotStoreReader`]
//!   trait so this module does not depend on `crate::store` types.
//! - `MessageStoreDispatcherImpl` + group-commit buffer -> [`MessageBufferManager`]
//!   (single in-process buffer; no executor pools, no read-ahead cache).
//! - `FlatFileStore` / `FlatMessageFile` / `FlatAppendFile` -> [`TieredFileStore`].
//!   Per-queue segment list only; the separate consume-queue/index files are
//!   replaced by an in-memory record-position index per segment.
//! - `PosixFileSegment` -> [`TieredFileSegment`] (POSIX backend only, no object
//!   stores). Directory layout: `<root>/<topic>/<queueId>/<baseOffset(20 digits)>`.
//! - `DefaultMetadataStore` (rocksdb) -> [`TieredMetadataStore`]: in-memory
//!   `BTreeMap` with optional JSON persistence file.
//! - `TieredStoreMetricsManager` (OpenTelemetry) -> [`TieredStoreMetrics`]
//!   snapshot struct.
//!
//! Message records are serialized with a manual binary codec because
//! `crate::common::Message` does not derive serde traits.
//!
//! Read path mirrors `TieredMessageStore#getMessageAsync`: when the storage
//! level says so, reads go to the cold store first and fall back to the hot
//! store on miss or error.

use std::collections::{BTreeMap, HashMap};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};
use tokio::sync::Mutex as TokioMutex;
use tracing::{debug, warn};

use crate::common::{Error, Message, Result, now_millis};

/// Size of one record header in a segment file: u32 payload length + u32 crc32.
const RECORD_HEADER_SIZE: usize = 8;
/// Segment file names are the base offset, zero padded to this width, matching
/// Java `MessageStoreUtil.offset2FileName`.
const FILE_NAME_DIGITS: usize = 20;

// ---------------------------------------------------------------------------
// Configuration (Java: MessageStoreConfig / TieredStorageLevel)
// ---------------------------------------------------------------------------

/// Java `MessageStoreConfig.TieredStorageLevel`.
///
/// Serialized with the Java enum names (`DISABLED`, `NOT_IN_DISK`, `NOT_IN_MEM`,
/// `FORCE`) via `SCREAMING_SNAKE_CASE` renaming.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TieredStorageLevel {
    /// Tiered storage disabled, all reads go to the hot store.
    #[default]
    Disabled = 0,
    /// Only read from the cold store when the offset is not on hot disk.
    NotInDisk = 1,
    /// Only read from the cold store when the offset is not in hot memory.
    NotInMem = 2,
    /// All reads go to the cold store first.
    Force = 3,
}

impl TieredStorageLevel {
    pub fn value(self) -> i32 {
        match self {
            Self::Disabled => 0,
            Self::NotInDisk => 1,
            Self::NotInMem => 2,
            Self::Force => 3,
        }
    }

    pub fn from_value(value: i32) -> Self {
        match value {
            1 => Self::NotInDisk,
            2 => Self::NotInMem,
            3 => Self::Force,
            _ => Self::Disabled,
        }
    }

    pub fn is_enable(self) -> bool {
        self.value() > 0
    }

    pub fn check(self, target: Self) -> bool {
        self.value() >= target.value()
    }
}

/// Parse a human readable byte size such as `"1g"`, `"512m"`, `"64kb"` or a
/// plain integer string.
pub fn parse_size(text: &str) -> Result<u64> {
    let trimmed = text.trim();
    let digits_end = trimmed
        .find(|c: char| !c.is_ascii_digit())
        .unwrap_or(trimmed.len());
    if digits_end == 0 {
        return Err(Error::Invalid(format!("invalid size string: {text:?}")));
    }
    let number: u64 = trimmed[..digits_end]
        .parse()
        .map_err(|_| Error::Invalid(format!("invalid size number: {text:?}")))?;
    let multiplier = match trimmed[digits_end..].trim().to_ascii_lowercase().as_str() {
        "" | "b" => 1u64,
        "k" | "kb" => 1024,
        "m" | "mb" => 1024 * 1024,
        "g" | "gb" => 1024 * 1024 * 1024,
        "t" | "tb" => 1024 * 1024 * 1024 * 1024,
        other => return Err(Error::Invalid(format!("unknown size unit: {other:?}"))),
    };
    number
        .checked_mul(multiplier)
        .ok_or_else(|| Error::Invalid(format!("size overflow: {text:?}")))
}

/// Serde helper for byte-size fields: accepts JSON integers and strings such
/// as `"1g"` / `"512m"`; serializes back as a plain integer.
mod size_format {
    use super::parse_size;
    use serde::de::Visitor;
    use serde::{Deserializer, Serializer};

    pub fn serialize<S: Serializer>(
        value: &u64,
        serializer: S,
    ) -> core::result::Result<S::Ok, S::Error> {
        serializer.serialize_u64(*value)
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> core::result::Result<u64, D::Error> {
        struct SizeVisitor;

        impl Visitor<'_> for SizeVisitor {
            type Value = u64;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("byte size as integer or string like \"1g\"")
            }

            fn visit_u64<E: serde::de::Error>(
                self,
                value: u64,
            ) -> core::result::Result<u64, E> {
                Ok(value)
            }

            fn visit_i64<E: serde::de::Error>(
                self,
                value: i64,
            ) -> core::result::Result<u64, E> {
                if value < 0 {
                    return Err(E::custom(format!("negative size: {value}")));
                }
                Ok(value as u64)
            }

            fn visit_str<E: serde::de::Error>(
                self,
                value: &str,
            ) -> core::result::Result<u64, E> {
                parse_size(value).map_err(E::custom)
            }
        }

        deserializer.deserialize_any(SizeVisitor)
    }
}

/// Tiered store configuration, ported from Java `MessageStoreConfig` (the
/// tiered storage subset). Serialized with camelCase keys and Java defaults.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct TieredStoreConfig {
    /// Java `tieredStorageLevel`.
    pub storage_level: TieredStorageLevel,
    /// Max size of one cold file segment. Java `tieredStoreCommitLogMaxSize`,
    /// default 1GB. Accepts `"1g"` / `"512m"` strings when deserializing.
    #[serde(with = "size_format")]
    pub file_segment_size: u64,
    /// Root path holding data-integrity check files. Java legacy
    /// `dataIntegrityRootPath`; empty means "alongside the store root".
    pub data_integrity_root_path: String,
    /// Reserved time in hours before cold files may be deleted.
    /// Java `tieredStoreFileReservedTime`, default 72h.
    pub file_reserved_time: u64,
    /// Interval in ms between expire-file sweeps. Java
    /// `tieredStoreDeleteFileInterval`, default 1h.
    pub delete_file_interval_ms: u64,
    /// Java `tieredStoreGroupCommit`, default true.
    pub group_commit_enable: bool,
    /// Java `tieredStoreGroupCommitTimeout`, default 30s.
    pub group_commit_timeout_ms: u64,
    /// Buffered message count that triggers an async commit. Java
    /// `tieredStoreGroupCommitCount`, default 4096.
    pub group_commit_count: usize,
    /// Buffered message size that triggers an async commit. Java
    /// `tieredStoreGroupCommitSize`, default 4MB.
    #[serde(with = "size_format")]
    pub group_commit_size: u64,
    /// Cached message count above which dispatch backpressure kicks in.
    /// Java `tieredStoreMaxGroupCommitCount`, default 10000.
    pub max_group_commit_count: usize,
    /// Max in-flight buffered messages before dispatch rejects appends.
    /// Java `tieredStoreMaxPendingLimit`, default 10000.
    pub max_pending_limit: usize,
    /// Capacity of the LRU cache of open file handles (see
    /// [`FileHandleCache`]). Java keeps one channel per segment; here the
    /// number of open handles is bounded.
    pub file_handle_cache_capacity: usize,
    /// Optional path of the JSON metadata persistence file. Empty means
    /// memory-only metadata (Java uses rocksdb).
    pub metadata_path: String,
}

impl Default for TieredStoreConfig {
    fn default() -> Self {
        Self {
            storage_level: TieredStorageLevel::NotInDisk,
            file_segment_size: 1024 * 1024 * 1024,
            data_integrity_root_path: String::new(),
            file_reserved_time: 72,
            delete_file_interval_ms: 60 * 60 * 1000,
            group_commit_enable: true,
            group_commit_timeout_ms: 30 * 1000,
            group_commit_count: 4 * 1024,
            group_commit_size: 4 * 1024 * 1024,
            max_group_commit_count: 10000,
            max_pending_limit: 10000,
            file_handle_cache_capacity: 64,
            metadata_path: String::new(),
        }
    }
}

/// Replace characters that are illegal in file names, so topics can be used
/// as path components safely.
fn sanitize_topic(topic: &str) -> String {
    topic
        .chars()
        .map(|c| {
            if matches!(c, '/' | '\\' | ':' | '*' | '?' | '"' | '<' | '>' | '|') {
                '_'
            } else {
                c
            }
        })
        .collect()
}

fn segment_file_name(base_offset: u64) -> String {
    format!("{:0width$}", base_offset, width = FILE_NAME_DIGITS)
}

// ---------------------------------------------------------------------------
// Message record codec
// ---------------------------------------------------------------------------

struct Cursor<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn take(&mut self, len: usize) -> Result<&'a [u8]> {
        if self.pos.saturating_add(len) > self.data.len() {
            return Err(Error::Invalid("truncated message record".to_string()));
        }
        let slice = &self.data[self.pos..self.pos + len];
        self.pos += len;
        Ok(slice)
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn u16(&mut self) -> Result<u16> {
        let bytes = self.take(2)?;
        Ok(u16::from_le_bytes(bytes.try_into().unwrap()))
    }

    fn u32(&mut self) -> Result<u32> {
        let bytes = self.take(4)?;
        Ok(u32::from_le_bytes(bytes.try_into().unwrap()))
    }

    fn u64(&mut self) -> Result<u64> {
        let bytes = self.take(8)?;
        Ok(u64::from_le_bytes(bytes.try_into().unwrap()))
    }

    fn i32(&mut self) -> Result<i32> {
        Ok(self.u32()? as i32)
    }

    fn i64(&mut self) -> Result<i64> {
        Ok(self.u64()? as i64)
    }
}

fn put_bytes(buffer: &mut Vec<u8>, bytes: &[u8]) {
    buffer.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    buffer.extend_from_slice(bytes);
}

fn put_str(buffer: &mut Vec<u8>, text: &str) {
    put_bytes(buffer, text.as_bytes());
}

fn put_addr(buffer: &mut Vec<u8>, addr: SocketAddr) {
    match addr.ip() {
        IpAddr::V4(v4) => {
            buffer.push(4);
            buffer.extend_from_slice(&v4.octets());
            buffer.extend_from_slice(&[0u8; 12]);
        }
        IpAddr::V6(v6) => {
            buffer.push(6);
            buffer.extend_from_slice(&v6.octets());
        }
    }
    buffer.extend_from_slice(&addr.port().to_le_bytes());
}

fn take_bytes(cursor: &mut Cursor<'_>) -> Result<Vec<u8>> {
    let len = cursor.u32()? as usize;
    Ok(cursor.take(len)?.to_vec())
}

fn take_str(cursor: &mut Cursor<'_>) -> Result<String> {
    let bytes = take_bytes(cursor)?;
    String::from_utf8(bytes).map_err(|_| Error::Invalid("invalid utf-8 in record".to_string()))
}

fn take_addr(cursor: &mut Cursor<'_>) -> Result<SocketAddr> {
    let tag = cursor.u8()?;
    let ip = match tag {
        4 => {
            let octets = cursor.take(16)?;
            IpAddr::V4(std::net::Ipv4Addr::new(
                octets[0], octets[1], octets[2], octets[3],
            ))
        }
        6 => {
            let octets = cursor.take(16)?;
            IpAddr::V6(std::net::Ipv6Addr::from(
                <[u8; 16]>::try_from(octets).unwrap(),
            ))
        }
        other => return Err(Error::Invalid(format!("invalid address tag: {other}"))),
    };
    let port = cursor.u16()?;
    Ok(SocketAddr::new(ip, port))
}

/// Binary encode a `Message` for cold storage.
fn encode_message(message: &Message) -> Vec<u8> {
    let mut buffer = Vec::with_capacity(message.body.len() + 256);
    put_str(&mut buffer, &message.topic);
    buffer.extend_from_slice(&message.queue_id.to_le_bytes());
    buffer.extend_from_slice(&message.queue_offset.to_le_bytes());
    buffer.extend_from_slice(&message.physical_offset.to_le_bytes());
    buffer.extend_from_slice(&message.flag.to_le_bytes());
    buffer.extend_from_slice(&message.sys_flag.to_le_bytes());
    buffer.extend_from_slice(&message.born_timestamp.to_le_bytes());
    put_addr(&mut buffer, message.born_host);
    buffer.extend_from_slice(&message.store_timestamp.to_le_bytes());
    put_addr(&mut buffer, message.store_host);
    buffer.extend_from_slice(&message.reconsume_times.to_le_bytes());
    buffer.extend_from_slice(&message.prepared_transaction_offset.to_le_bytes());
    put_bytes(&mut buffer, &message.body);
    buffer.extend_from_slice(&(message.properties.len() as u32).to_le_bytes());
    for (key, value) in &message.properties {
        put_str(&mut buffer, key);
        put_str(&mut buffer, value);
    }
    buffer
}

/// Decode a `Message` previously written by [`encode_message`].
fn decode_message(payload: &[u8]) -> Result<Message> {
    let mut cursor = Cursor {
        data: payload,
        pos: 0,
    };
    let mut message = Message::default();
    message.topic = take_str(&mut cursor)?;
    message.queue_id = cursor.i32()?;
    message.queue_offset = cursor.u64()?;
    message.physical_offset = cursor.u64()?;
    message.flag = cursor.i32()?;
    message.sys_flag = cursor.i32()?;
    message.born_timestamp = cursor.i64()?;
    message.born_host = take_addr(&mut cursor)?;
    message.store_timestamp = cursor.i64()?;
    message.store_host = take_addr(&mut cursor)?;
    message.reconsume_times = cursor.i32()?;
    message.prepared_transaction_offset = cursor.u64()?;
    message.body = take_bytes(&mut cursor)?;
    let property_count = cursor.u32()?;
    for _ in 0..property_count {
        let key = take_str(&mut cursor)?;
        let value = take_str(&mut cursor)?;
        message.properties.insert(key, value);
    }
    if cursor.pos != cursor.data.len() {
        return Err(Error::Invalid("trailing bytes in message record".to_string()));
    }
    Ok(message)
}

// ---------------------------------------------------------------------------
// Metadata store (Java: DefaultMetadataStore, rocksdb -> memory + JSON)
// ---------------------------------------------------------------------------

/// Java `TopicMetadata`.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct TopicMetadata {
    pub topic_id: i64,
    pub topic: String,
    pub reserve_time: i64,
    pub status: i32,
    pub update_timestamp: i64,
}

/// Java `QueueMetadata`.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct QueueMetadata {
    pub topic: String,
    pub queue_id: i32,
    pub min_offset: u64,
    pub max_offset: u64,
    pub update_timestamp: i64,
}

/// Java `FileSegmentMetadata` (simplified: `type`/`path`/`status` fields are
/// collapsed into `sealed`, because this port only has consume-queue-style
/// per-queue segments).
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct SegmentMetadata {
    pub base_offset: u64,
    pub size: u64,
    pub sealed: bool,
    pub min_timestamp: i64,
    pub max_timestamp: i64,
    pub create_timestamp: i64,
}

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
struct MetadataData {
    topics: BTreeMap<String, TopicMetadata>,
    /// Key: `"{topic}/{queueId}"`.
    queues: BTreeMap<String, QueueMetadata>,
    /// Key: `"{topic}/{queueId}"`, value sorted by base offset.
    segments: BTreeMap<String, Vec<SegmentMetadata>>,
}

fn queue_key(topic: &str, queue_id: i32) -> String {
    format!("{topic}/{queue_id}")
}

fn parse_queue_key(key: &str) -> Option<(String, i32)> {
    let (topic, queue_id) = key.rsplit_once('/')?;
    Some((topic.to_string(), queue_id.parse().ok()?))
}

/// In-memory metadata store with optional JSON persistence, replacing the
/// rocksdb-based Java `DefaultMetadataStore`.
#[derive(Debug)]
pub struct TieredMetadataStore {
    path: Option<PathBuf>,
    inner: Mutex<MetadataData>,
    next_topic_id: AtomicI64,
}

impl TieredMetadataStore {
    /// Open the metadata store. When `path` is `Some`, an existing JSON file
    /// is loaded; `persist` writes back to the same file. `None` keeps all
    /// state in memory only.
    pub fn open<P: AsRef<Path>>(path: Option<P>) -> Result<Self> {
        let path = path.map(|p| p.as_ref().to_path_buf());
        let mut data = MetadataData::default();
        let mut next_topic_id = 0i64;
        if let Some(path) = &path {
            if path.exists() {
                let text = fs::read_to_string(path)?;
                data = serde_json::from_str(&text).map_err(Error::Json)?;
                for topic in data.topics.values() {
                    next_topic_id = next_topic_id.max(topic.topic_id + 1);
                }
                debug!(
                    path = %path.display(),
                    topics = data.topics.len(),
                    queues = data.queues.len(),
                    segments = data.segments.len(),
                    "tiered metadata loaded"
                );
            }
        }
        Ok(Self {
            path,
            inner: Mutex::new(data),
            next_topic_id: AtomicI64::new(next_topic_id),
        })
    }

    pub fn add_topic(&self, topic: &str, reserve_time: i64) -> TopicMetadata {
        let mut inner = self.inner.lock().unwrap();
        if let Some(existing) = inner.topics.get(topic) {
            return existing.clone();
        }
        let metadata = TopicMetadata {
            topic_id: self.next_topic_id.fetch_add(1, Ordering::SeqCst),
            topic: topic.to_string(),
            reserve_time,
            status: 0,
            update_timestamp: now_millis(),
        };
        inner.topics.insert(topic.to_string(), metadata.clone());
        metadata
    }

    pub fn get_topic(&self, topic: &str) -> Option<TopicMetadata> {
        self.inner.lock().unwrap().topics.get(topic).cloned()
    }

    pub fn update_queue(&self, topic: &str, queue_id: i32, min_offset: u64, max_offset: u64) {
        let key = queue_key(topic, queue_id);
        let mut inner = self.inner.lock().unwrap();
        let entry = inner.queues.entry(key).or_insert_with(|| QueueMetadata {
            topic: topic.to_string(),
            queue_id,
            ..QueueMetadata::default()
        });
        entry.min_offset = min_offset;
        entry.max_offset = max_offset;
        entry.update_timestamp = now_millis();
    }

    pub fn get_queue(&self, topic: &str, queue_id: i32) -> Option<QueueMetadata> {
        self.inner
            .lock()
            .unwrap()
            .queues
            .get(&queue_key(topic, queue_id))
            .cloned()
    }

    pub fn update_segment(&self, topic: &str, queue_id: i32, metadata: SegmentMetadata) {
        let key = queue_key(topic, queue_id);
        let mut inner = self.inner.lock().unwrap();
        let segments = inner.segments.entry(key).or_default();
        match segments
            .iter_mut()
            .find(|item| item.base_offset == metadata.base_offset)
        {
            Some(existing) => {
                let create_timestamp = existing.create_timestamp;
                *existing = metadata;
                if existing.create_timestamp == 0 {
                    existing.create_timestamp = create_timestamp;
                }
            }
            None => segments.push(metadata),
        }
    }

    pub fn get_segment(
        &self,
        topic: &str,
        queue_id: i32,
        base_offset: u64,
    ) -> Option<SegmentMetadata> {
        self.inner
            .lock()
            .unwrap()
            .segments
            .get(&queue_key(topic, queue_id))?
            .iter()
            .find(|item| item.base_offset == base_offset)
            .cloned()
    }

    /// Snapshot of all per-queue segment lists, sorted by base offset.
    pub fn iterate_segments(&self) -> Vec<(String, i32, Vec<SegmentMetadata>)> {
        let inner = self.inner.lock().unwrap();
        inner
            .segments
            .iter()
            .filter_map(|(key, segments)| {
                let (topic, queue_id) = parse_queue_key(key)?;
                let mut segments = segments.clone();
                segments.sort_by_key(|item| item.base_offset);
                Some((topic, queue_id, segments))
            })
            .collect()
    }

    pub fn delete_topic(&self, topic: &str) -> Option<TopicMetadata> {
        let prefix = format!("{topic}/");
        let mut inner = self.inner.lock().unwrap();
        let removed = inner.topics.remove(topic);
        inner.queues.retain(|key, _| !key.starts_with(&prefix));
        inner.segments.retain(|key, _| !key.starts_with(&prefix));
        removed
    }

    /// Write the metadata snapshot to the JSON file when one is configured.
    pub fn persist(&self) -> Result<()> {
        let Some(path) = &self.path else {
            return Ok(());
        };
        let snapshot = {
            let inner = self.inner.lock().unwrap();
            serde_json::to_vec_pretty(&*inner).map_err(Error::Json)?
        };
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, snapshot)?;
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// LRU file handle cache
// ---------------------------------------------------------------------------

/// Simple LRU cache of open read handles, shared by all segments of a
/// [`TieredFileStore`]. Java `PosixFileSegment` keeps one read channel per
/// segment forever; here the number of open handles is bounded and the least
/// recently used entry is closed (dropped) on eviction.
///
/// Handles handed out are `try_clone` duplicates that share the underlying
/// file offset, so readers of the same path must be externally serialized
/// (segment reads hold the segment lock).
#[derive(Debug)]
pub struct FileHandleCache {
    capacity: usize,
    tick: u64,
    entries: HashMap<PathBuf, (File, u64)>,
}

impl FileHandleCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            tick: 0,
            entries: HashMap::new(),
        }
    }

    /// Return a readable handle for `path`, opening it on first use.
    pub fn get_or_open(&mut self, path: &Path) -> Result<File> {
        self.tick += 1;
        let tick = self.tick;
        if let Some((file, last_used)) = self.entries.get_mut(path) {
            *last_used = tick;
            return Ok(file.try_clone()?);
        }
        while self.entries.len() >= self.capacity {
            self.evict_one();
        }
        let file = OpenOptions::new().read(true).open(path)?;
        self.entries
            .insert(path.to_path_buf(), (file.try_clone()?, tick));
        Ok(file)
    }

    /// Drop the cached handle for `path` (used after deletes).
    pub fn invalidate(&mut self, path: &Path) {
        self.entries.remove(path);
    }

    /// Drop all cached handles.
    pub fn invalidate_all(&mut self) {
        self.entries.clear();
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn evict_one(&mut self) {
        let Some(victim) = self
            .entries
            .iter()
            .min_by_key(|(_, (_, last_used))| *last_used)
            .map(|(path, _)| path.clone())
        else {
            return;
        };
        self.entries.remove(&victim);
        debug!(path = %victim.display(), "tiered file handle evicted");
    }
}

// ---------------------------------------------------------------------------
// File segment (Java: PosixFileSegment + FlatAppendFile, per-queue slice)
// ---------------------------------------------------------------------------

/// Result of appending to a segment, mirroring Java `AppendResult`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AppendResult {
    /// Append accepted; payload carries the assigned queue offset.
    Success(u64),
    /// Segment is full; the caller must flush/seal it and roll a new one.
    FileFull,
    /// Segment is sealed and no longer accepts appends.
    Closed,
}

/// One append-only cold file covering the queue-offset range
/// `[base_offset, base_offset + record_count)` of a single topic queue.
///
/// Records are length + crc32 prefixed binary messages (see
/// [`encode_message`]). Appends go to an in-memory write buffer and become
/// readable after [`TieredFileSegment::flush`], mirroring the Java
/// append/commit position split.
#[derive(Debug)]
pub struct TieredFileSegment {
    root: PathBuf,
    pub topic: String,
    pub queue_id: i32,
    /// Queue offset of the first record in this segment.
    pub base_offset: u64,
    max_size: u64,
    inner: Mutex<SegmentInner>,
}

#[derive(Debug, Default)]
struct SegmentInner {
    /// Bytes not yet written to disk.
    append_buffer: Vec<u8>,
    /// Bytes actually present in the file (the commit position).
    committed_size: u64,
    /// Total records appended (committed + buffered).
    record_count: usize,
    /// File position of each committed record, indexed by
    /// `queue offset - base_offset`.
    record_positions: Vec<u64>,
    min_timestamp: i64,
    max_timestamp: i64,
    sealed: bool,
}

impl TieredFileSegment {
    pub fn new(root: PathBuf, topic: String, queue_id: i32, base_offset: u64, max_size: u64) -> Self {
        Self {
            root,
            topic,
            queue_id,
            base_offset,
            max_size,
            inner: Mutex::new(SegmentInner::default()),
        }
    }

    /// Full path of the backing file:
    /// `<root>/<topic>/<queueId>/<baseOffset(20 digits)>`.
    pub fn file_path(&self) -> PathBuf {
        self.root
            .join(sanitize_topic(&self.topic))
            .join(self.queue_id.to_string())
            .join(segment_file_name(self.base_offset))
    }

    pub fn max_size(&self) -> u64 {
        self.max_size
    }

    pub fn min_timestamp(&self) -> i64 {
        self.inner.lock().unwrap().min_timestamp
    }

    pub fn max_timestamp(&self) -> i64 {
        self.inner.lock().unwrap().max_timestamp
    }

    pub fn is_sealed(&self) -> bool {
        self.inner.lock().unwrap().sealed
    }

    pub fn seal(&self) {
        self.inner.lock().unwrap().sealed = true;
    }

    /// Total records appended, including records still in the write buffer.
    pub fn record_count(&self) -> usize {
        self.inner.lock().unwrap().record_count
    }

    /// Records that are committed to disk and therefore readable.
    pub fn committed_records(&self) -> usize {
        self.inner.lock().unwrap().record_positions.len()
    }

    pub fn committed_size(&self) -> u64 {
        self.inner.lock().unwrap().committed_size
    }

    /// Append one message to the write buffer. Returns the assigned queue
    /// offset, or `FileFull` when the record does not fit and the segment
    /// already holds data.
    pub fn append(&self, message: &Message) -> AppendResult {
        let mut inner = self.inner.lock().unwrap();
        if inner.sealed {
            return AppendResult::Closed;
        }
        let payload = encode_message(message);
        let record_len = payload.len() + RECORD_HEADER_SIZE;
        let has_data = inner.record_count > 0 || !inner.append_buffer.is_empty();
        if has_data
            && inner.committed_size as usize + inner.append_buffer.len() + record_len
                > self.max_size as usize
        {
            return AppendResult::FileFull;
        }
        let timestamp = message.store_timestamp;
        if inner.record_count == 0 || inner.min_timestamp > timestamp {
            inner.min_timestamp = timestamp;
        }
        if inner.record_count == 0 || inner.max_timestamp < timestamp {
            inner.max_timestamp = timestamp;
        }
        inner
            .append_buffer
            .extend_from_slice(&(payload.len() as u32).to_le_bytes());
        inner
            .append_buffer
            .extend_from_slice(&crc32fast::hash(&payload).to_le_bytes());
        inner.append_buffer.extend_from_slice(&payload);
        inner.record_count += 1;
        AppendResult::Success(self.base_offset + inner.record_count as u64 - 1)
    }

    /// Write buffered bytes to disk at the commit position and make the
    /// buffered records readable. Returns the new committed size.
    pub fn flush(&self) -> Result<u64> {
        let mut inner = self.inner.lock().unwrap();
        if inner.append_buffer.is_empty() {
            return Ok(inner.committed_size);
        }
        // Validate record boundaries in the buffer before touching the file.
        let buffer_len = inner.append_buffer.len();
        let mut positions = Vec::new();
        let mut pos = 0usize;
        while pos < buffer_len {
            if pos + RECORD_HEADER_SIZE > buffer_len {
                return Err(Error::Invalid(
                    "truncated record header in write buffer".to_string(),
                ));
            }
            let bytes = &inner.append_buffer[pos..pos + 4];
            let len = u32::from_le_bytes(bytes.try_into().unwrap()) as usize;
            if len == 0 || pos + RECORD_HEADER_SIZE + len > buffer_len {
                return Err(Error::Invalid(
                    "corrupted record length in write buffer".to_string(),
                ));
            }
            positions.push(inner.committed_size + pos as u64);
            pos += RECORD_HEADER_SIZE + len;
        }

        let path = self.file_path();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut file = OpenOptions::new().create(true).write(true).open(&path)?;
        file.seek(SeekFrom::Start(inner.committed_size))?;
        file.write_all(&inner.append_buffer)?;
        file.sync_all()?; // Java writeFileChannel.force(true)
        inner.committed_size += buffer_len as u64;
        inner.record_positions.extend(positions);
        inner.append_buffer.clear();
        debug!(path = %path.display(), size = inner.committed_size, "tiered segment flushed");
        Ok(inner.committed_size)
    }

    /// Read up to `max_count` committed records starting at queue `offset`.
    /// Only committed (flushed) data is visible, matching the Java commit
    /// position semantics.
    pub fn read(
        &self,
        cache: &mut FileHandleCache,
        offset: u64,
        max_count: usize,
    ) -> Result<Vec<Message>> {
        if offset < self.base_offset {
            return Err(Error::Invalid(format!(
                "offset {offset} is below segment base {}",
                self.base_offset
            )));
        }
        if max_count == 0 {
            return Ok(Vec::new());
        }
        let inner = self.inner.lock().unwrap();
        let index = (offset - self.base_offset) as usize;
        if index >= inner.record_positions.len() {
            return Err(Error::NotFound(format!(
                "offset {offset} not committed in segment {}",
                self.file_path().display()
            )));
        }
        let path = self.file_path();
        let mut messages = Vec::with_capacity(max_count.min(64));
        for position in inner.record_positions[index..]
            .iter()
            .take(max_count)
            .copied()
        {
            messages.push(read_record_at(&path, cache, position)?);
        }
        Ok(messages)
    }

    pub fn exists(&self) -> bool {
        self.file_path().exists()
    }

    /// Delete the backing file. In-memory state is kept so that callers can
    /// still inspect offsets until the segment is dropped.
    pub fn destroy(&self) -> Result<()> {
        let path = self.file_path();
        if path.exists() {
            fs::remove_file(&path)?;
            debug!(path = %path.display(), "tiered segment destroyed");
        }
        Ok(())
    }

    /// Restore state from persisted metadata by rescanning the file to
    /// rebuild the record position index.
    pub fn recover(&self, metadata: &SegmentMetadata) -> Result<()> {
        let (positions, committed_size) = self.scan_positions()?;
        if metadata.size != committed_size {
            warn!(
                path = %self.file_path().display(),
                metadata_size = metadata.size,
                file_size = committed_size,
                "tiered segment size mismatch after recovery, using file size"
            );
        }
        let mut inner = self.inner.lock().unwrap();
        inner.record_positions = positions;
        inner.record_count = inner.record_positions.len();
        inner.committed_size = committed_size;
        inner.sealed = metadata.sealed;
        inner.min_timestamp = metadata.min_timestamp;
        inner.max_timestamp = metadata.max_timestamp;
        Ok(())
    }

    /// Sequentially scan the file and collect the start position of every
    /// intact record. Stops at the first truncated/corrupt tail.
    fn scan_positions(&self) -> Result<(Vec<u64>, u64)> {
        let path = self.file_path();
        if !path.exists() {
            warn!(path = %path.display(), "tiered segment file missing during recovery");
            return Ok((Vec::new(), 0));
        }
        let mut file = File::open(&path)?;
        let file_size = file.metadata()?.len();
        let mut positions = Vec::new();
        let mut pos = 0u64;
        while pos + RECORD_HEADER_SIZE as u64 <= file_size {
            file.seek(SeekFrom::Start(pos))?;
            let mut header = [0u8; RECORD_HEADER_SIZE];
            file.read_exact(&mut header)?;
            let len = u32::from_le_bytes(header[0..4].try_into().unwrap()) as u64;
            if len == 0 || pos + RECORD_HEADER_SIZE as u64 + len > file_size {
                break;
            }
            positions.push(pos);
            pos += RECORD_HEADER_SIZE as u64 + len;
        }
        Ok((positions, pos))
    }
}

fn read_record_at(path: &Path, cache: &mut FileHandleCache, position: u64) -> Result<Message> {
    let mut file = cache.get_or_open(path)?;
    file.seek(SeekFrom::Start(position))?;
    let mut header = [0u8; RECORD_HEADER_SIZE];
    file.read_exact(&mut header)?;
    let len = u32::from_le_bytes(header[0..4].try_into().unwrap()) as usize;
    let crc = u32::from_le_bytes(header[4..8].try_into().unwrap());
    let mut payload = vec![0u8; len];
    file.read_exact(&mut payload)?;
    if crc32fast::hash(&payload) != crc {
        return Err(Error::Invalid(format!(
            "crc mismatch reading {}",
            path.display()
        )));
    }
    decode_message(&payload)
}

// ---------------------------------------------------------------------------
// File store (Java: FlatFileStore + FlatMessageFile, per-queue segments)
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
struct QueueFiles {
    /// Sorted by base offset; the last entry is the active write segment.
    segments: Vec<Arc<TieredFileSegment>>,
}

/// Cold file store managing per-queue segment lists plus the shared metadata
/// store and file handle cache. Port of Java `FlatFileStore` /
/// `FlatMessageFile` (consume-queue path only; the Java flat commit-log +
/// consume-queue pair is collapsed into one record stream per queue).
#[derive(Debug)]
pub struct TieredFileStore {
    config: TieredStoreConfig,
    root: PathBuf,
    metadata: Arc<TieredMetadataStore>,
    handles: Mutex<FileHandleCache>,
    queues: Mutex<BTreeMap<(String, i32), QueueFiles>>,
}

impl TieredFileStore {
    /// Open (or create) the store rooted at `root` and recover all segments
    /// registered in `metadata`.
    pub fn open(
        root: impl Into<PathBuf>,
        config: TieredStoreConfig,
        metadata: TieredMetadataStore,
    ) -> Result<Arc<Self>> {
        let root = root.into();
        fs::create_dir_all(&root)?;
        let store = Self {
            handles: Mutex::new(FileHandleCache::new(config.file_handle_cache_capacity)),
            config,
            root,
            metadata: Arc::new(metadata),
            queues: Mutex::new(BTreeMap::new()),
        };
        store.recover()?;
        Ok(Arc::new(store))
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn config(&self) -> &TieredStoreConfig {
        &self.config
    }

    pub fn metadata(&self) -> &Arc<TieredMetadataStore> {
        &self.metadata
    }

    fn recover(&self) -> Result<()> {
        for (topic, queue_id, list) in self.metadata.iterate_segments() {
            let mut segments = Vec::with_capacity(list.len());
            for item in list {
                let segment = Arc::new(TieredFileSegment::new(
                    self.root.clone(),
                    topic.clone(),
                    queue_id,
                    item.base_offset,
                    self.config.file_segment_size,
                ));
                segment.recover(&item)?;
                segments.push(segment);
            }
            self.queues
                .lock()
                .unwrap()
                .insert((topic, queue_id), QueueFiles { segments });
        }
        Ok(())
    }

    fn create_segment(&self, topic: &str, queue_id: i32, base_offset: u64) -> Arc<TieredFileSegment> {
        let segment = Arc::new(TieredFileSegment::new(
            self.root.clone(),
            topic.to_string(),
            queue_id,
            base_offset,
            self.config.file_segment_size,
        ));
        self.metadata.update_segment(
            topic,
            queue_id,
            SegmentMetadata {
                base_offset,
                create_timestamp: now_millis(),
                ..SegmentMetadata::default()
            },
        );
        segment
    }

    fn queue_segments(&self, topic: &str, queue_id: i32) -> Vec<Arc<TieredFileSegment>> {
        let key = (sanitize_topic(topic), queue_id);
        self.queues
            .lock()
            .unwrap()
            .get(&key)
            .map(|entry| entry.segments.clone())
            .unwrap_or_default()
    }

    /// Append one message to the cold store, rolling to a new segment when
    /// the active one is full. Returns the assigned queue offset.
    pub fn append_message(&self, message: &Message) -> Result<u64> {
        let topic = sanitize_topic(&message.topic);
        let queue_id = message.queue_id;
        // Java FlatMessageFile.recoverTopicMetadata / recoverQueueMetadata.
        self.metadata.add_topic(
            &topic,
            self.config.file_reserved_time as i64 * 60 * 60 * 1000,
        );
        // The guard is held for the whole append loop; nothing inside the
        // loop re-locks `self.queues`, so this cannot deadlock.
        let mut queue_table = self.queues.lock().unwrap();
        let entry = queue_table
            .entry((topic.clone(), queue_id))
            .or_default();
        loop {
            if entry.segments.is_empty() {
                let segment = self.create_segment(&topic, queue_id, message.queue_offset);
                entry.segments.push(segment);
            }
            let active = entry.segments.last().unwrap().clone();
            match active.append(message) {
                AppendResult::Success(offset) => return Ok(offset),
                AppendResult::FileFull => {
                    active.flush()?;
                    active.seal();
                    self.persist_segment(&active);
                    let segment = self.create_segment(&topic, queue_id, message.queue_offset);
                    entry.segments.push(segment);
                    debug!(
                        topic = %topic,
                        queue = queue_id,
                        base = message.queue_offset,
                        "tiered segment rolled"
                    );
                }
                AppendResult::Closed => {
                    let segment = self.create_segment(&topic, queue_id, message.queue_offset);
                    entry.segments.push(segment);
                }
            }
        }
    }

    /// Flush every segment and persist metadata. Returns the number of
    /// segments that were flushed.
    pub fn flush(&self) -> Result<usize> {
        let segments: Vec<Arc<TieredFileSegment>> = self
            .queues
            .lock()
            .unwrap()
            .values()
            .flat_map(|entry| entry.segments.iter().cloned())
            .collect();
        let mut flushed = 0usize;
        for segment in segments {
            if segment.flush()? > 0 {
                flushed += 1;
            }
            self.persist_segment(&segment);
        }
        for (topic, queue_id) in self.queue_keys() {
            let min_offset = self.min_offset(&topic, queue_id);
            let max_offset = self.max_offset(&topic, queue_id);
            if let (Some(min), Some(max)) = (min_offset, max_offset) {
                self.metadata.update_queue(&topic, queue_id, min, max);
            }
        }
        self.metadata.persist()?;
        Ok(flushed)
    }

    fn queue_keys(&self) -> Vec<(String, i32)> {
        self.queues.lock().unwrap().keys().cloned().collect()
    }

    fn persist_segment(&self, segment: &TieredFileSegment) {
        self.metadata.update_segment(
            &segment.topic,
            segment.queue_id,
            SegmentMetadata {
                base_offset: segment.base_offset,
                size: segment.committed_size(),
                sealed: segment.is_sealed(),
                min_timestamp: segment.min_timestamp(),
                max_timestamp: segment.max_timestamp(),
                create_timestamp: 0, // keep the value recorded at creation
            },
        );
    }

    /// Read up to `max_count` committed messages starting at queue `offset`,
    /// transparently crossing segment boundaries (Java
    /// `FlatAppendFile.readAsync`). Only committed data is visible.
    pub fn read(&self, topic: &str, queue_id: i32, offset: u64, max_count: usize) -> Result<Vec<Message>> {
        if max_count == 0 {
            return Ok(Vec::new());
        }
        let segments = self.queue_segments(topic, queue_id);
        let start = match segments.iter().rposition(|s| s.base_offset <= offset) {
            Some(index) => index,
            None => {
                return Err(Error::NotFound(format!(
                    "no cold segment for topic={topic} queue={queue_id} at offset {offset}"
                )));
            }
        };
        let sanitized = sanitize_topic(topic);
        let mut cache = self.handles.lock().unwrap();
        let mut result = Vec::with_capacity(max_count.min(64));
        for segment in &segments[start..] {
            if result.len() >= max_count {
                break;
            }
            let from = if segment.base_offset > offset {
                segment.base_offset
            } else {
                offset
            };
            match segment.read(&mut cache, from, max_count - result.len()) {
                Ok(mut messages) => result.append(&mut messages),
                Err(Error::NotFound(_)) => {}
                Err(error) => return Err(error),
            }
        }
        if result.is_empty() {
            let _ = sanitized; // sanitized name only used for lookups above
            return Err(Error::NotFound(format!(
                "offset {offset} not committed in cold store, topic={topic} queue={queue_id}"
            )));
        }
        Ok(result)
    }

    /// Smallest committed queue offset, or `None` when the queue has no
    /// committed cold data.
    pub fn min_offset(&self, topic: &str, queue_id: i32) -> Option<u64> {
        self.queue_segments(topic, queue_id)
            .iter()
            .find(|s| s.committed_records() > 0)
            .map(|s| s.base_offset)
    }

    /// Next queue offset after the last committed record (the consume-queue
    /// commit offset in Java terms), or `None` when empty.
    pub fn max_offset(&self, topic: &str, queue_id: i32) -> Option<u64> {
        self.queue_segments(topic, queue_id)
            .iter()
            .rev()
            .find(|s| s.committed_records() > 0)
            .map(|s| s.base_offset + s.committed_records() as u64)
    }

    /// Store timestamp of the message at `offset`, if it is committed.
    pub fn store_time(&self, topic: &str, queue_id: i32, offset: u64) -> Option<i64> {
        self.read(topic, queue_id, offset, 1)
            .ok()?
            .first()
            .map(|message| message.store_timestamp)
    }

    /// Delete all cold data of a topic (files plus metadata).
    pub fn delete_topic(&self, topic: &str) -> Result<()> {
        let sanitized = sanitize_topic(topic);
        let keys: Vec<(String, i32)> = self
            .queues
            .lock()
            .unwrap()
            .keys()
            .filter(|(key_topic, _)| key_topic == &sanitized)
            .cloned()
            .collect();
        for key in keys {
            let segments = self
                .queues
                .lock()
                .unwrap()
                .remove(&key)
                .map(|entry| entry.segments)
                .unwrap_or_default();
            for segment in segments {
                let path = segment.file_path();
                segment.destroy()?;
                self.handles.lock().unwrap().invalidate(&path);
            }
        }
        self.metadata.delete_topic(&sanitized);
        self.metadata.persist()
    }

    /// Metrics snapshot (Java `TieredStoreMetricsManager` gauge values).
    pub fn metrics(&self) -> TieredStoreMetrics {
        let queues = self.queues.lock().unwrap();
        let mut metrics = TieredStoreMetrics::default();
        let mut topics: BTreeMap<&str, ()> = BTreeMap::new();
        for queue in queues.values() {
            for segment in &queue.segments {
                metrics.total_size_bytes += segment.committed_size();
                metrics.message_count += segment.record_count() as u64;
                metrics.segment_count += 1;
                topics.insert(segment.topic.as_str(), ());
            }
        }
        metrics.topic_count = topics.len();
        metrics.queue_count = queues.len();
        metrics
    }
}

// ---------------------------------------------------------------------------
// Metrics (Java: TieredStoreMetricsManager, simplified snapshot)
// ---------------------------------------------------------------------------

/// Snapshot of the key tiered-store gauges. Java tracks these through
/// OpenTelemetry gauges (`rocketmq_storage_*`); here callers poll a snapshot.
#[derive(Clone, Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TieredStoreMetrics {
    /// Total bytes committed to the cold store.
    pub total_size_bytes: u64,
    /// Number of messages buffered in cold segments (committed + in-flight).
    pub message_count: u64,
    pub topic_count: usize,
    pub queue_count: usize,
    pub segment_count: usize,
    /// Messages currently waiting in the dispatch buffer.
    pub pending_dispatch_count: usize,
    /// Bytes currently waiting in the dispatch buffer.
    pub pending_dispatch_bytes: u64,
}

// ---------------------------------------------------------------------------
// Async dispatch buffer (Java: MessageStoreDispatcherImpl + group commit)
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
struct QueueBuffer {
    messages: Vec<Message>,
    bytes: usize,
}

/// Async buffer manager sitting between the hot store dispatch path and the
/// cold file store. Messages are buffered per topic queue and flushed into
/// cold file segments when the group-commit count/size thresholds are hit.
/// Port of Java `MessageStoreDispatcherImpl` (simplified: no executor pool,
/// no read-ahead cache, and backpressure rejects instead of suspending).
#[derive(Debug)]
pub struct MessageBufferManager {
    config: TieredStoreConfig,
    store: Arc<TieredFileStore>,
    buffers: TokioMutex<BTreeMap<(String, i32), QueueBuffer>>,
    pending_count: AtomicUsize,
    pending_size: AtomicU64,
}

impl MessageBufferManager {
    pub fn new(config: TieredStoreConfig, store: Arc<TieredFileStore>) -> Self {
        Self {
            config,
            store,
            buffers: TokioMutex::new(BTreeMap::new()),
            pending_count: AtomicUsize::new(0),
            pending_size: AtomicU64::new(0),
        }
    }

    pub fn pending_count(&self) -> usize {
        self.pending_count.load(Ordering::SeqCst)
    }

    pub fn pending_bytes(&self) -> u64 {
        self.pending_size.load(Ordering::SeqCst)
    }

    /// Buffer one dispatched message. Flushes the queue synchronously when a
    /// group-commit threshold is reached. Rejects with `Timeout` when the
    /// buffer exceeds the backpressure limits (Java suspends the append
    /// instead).
    pub async fn dispatch(&self, message: Message) -> Result<()> {
        let limit = self
            .config
            .max_group_commit_count
            .min(self.config.max_pending_limit);
        if self.pending_count() >= limit {
            return Err(Error::Timeout(format!(
                "tiered store dispatch buffer is full, pending={} limit={limit}",
                self.pending_count()
            )));
        }
        // Conservative estimate of the record size on disk.
        let bytes = message.body.len() + 128;
        let key = (message.topic.clone(), message.queue_id);
        {
            let mut buffers = self.buffers.lock().await;
            let entry = buffers.entry(key.clone()).or_default();
            entry.messages.push(message);
            entry.bytes += bytes;
        }
        self.pending_count.fetch_add(1, Ordering::SeqCst);
        self.pending_size.fetch_add(bytes as u64, Ordering::SeqCst);

        if !self.config.group_commit_enable {
            return Ok(());
        }
        let should_flush = {
            let buffers = self.buffers.lock().await;
            buffers
                .get(&key)
                .map(|entry| {
                    entry.messages.len() >= self.config.group_commit_count
                        || entry.bytes as u64 >= self.config.group_commit_size
                })
                .unwrap_or(false)
        };
        if should_flush {
            self.flush_queue(&key.0, key.1).await?;
        }
        Ok(())
    }

    /// Append all buffered messages of one queue to the cold store and flush
    /// the involved segments. Returns the number of messages flushed.
    pub async fn flush_queue(&self, topic: &str, queue_id: i32) -> Result<usize> {
        let buffered = self
            .buffers
            .lock()
            .await
            .remove(&(topic.to_string(), queue_id));
        let Some(buffer) = buffered else {
            return Ok(0);
        };
        self.pending_count
            .fetch_sub(buffer.messages.len(), Ordering::SeqCst);
        self.pending_size
            .fetch_sub(buffer.bytes as u64, Ordering::SeqCst);
        let mut flushed = 0usize;
        for message in &buffer.messages {
            self.store.append_message(message)?;
            flushed += 1;
        }
        self.store.flush()?;
        Ok(flushed)
    }

    /// Flush every buffered queue. Returns the number of messages flushed.
    pub async fn flush_all(&self) -> Result<usize> {
        let keys: Vec<(String, i32)> = self.buffers.lock().await.keys().cloned().collect();
        let mut flushed = 0usize;
        for (topic, queue_id) in keys {
            flushed += self.flush_queue(&topic, queue_id).await?;
        }
        Ok(flushed)
    }
}

// ---------------------------------------------------------------------------
// Message store facade (Java: TieredMessageStore)
// ---------------------------------------------------------------------------

/// Abstraction over the local ("hot") message store, playing the role of the
/// Java plugin `next` store. Integrators implement this on top of their own
/// store implementation; the tiered store never touches `crate::store`.
pub trait HotStoreReader: Send + Sync {
    /// Read up to `max_count` messages of a queue starting at `offset`.
    /// Implementations should return `Error::NotFound` when the requested
    /// offset is out of the hot range.
    fn read(&self, topic: &str, queue_id: i32, offset: u64, max_count: usize) -> Result<Vec<Message>>;

    /// Next offset after the last message of the queue (0 when empty).
    fn max_offset(&self, topic: &str, queue_id: i32) -> u64;

    /// Smallest offset still retained by the hot store (0 when empty).
    fn min_offset(&self, topic: &str, queue_id: i32) -> u64;

    /// Store timestamp of the message at `offset`, if it is still retained.
    fn store_time(&self, topic: &str, queue_id: i32, offset: u64) -> Option<i64>;
}

/// Facade combining the hot store (via [`HotStoreReader`]), the cold file
/// store and the dispatch buffer. Port of Java `TieredMessageStore`.
pub struct TieredMessageStore {
    config: TieredStoreConfig,
    hot: Arc<dyn HotStoreReader>,
    cold: Arc<TieredFileStore>,
    dispatcher: Arc<MessageBufferManager>,
}

impl TieredMessageStore {
    pub fn new(
        config: TieredStoreConfig,
        hot: Arc<dyn HotStoreReader>,
        cold: Arc<TieredFileStore>,
    ) -> Self {
        let dispatcher = Arc::new(MessageBufferManager::new(config.clone(), cold.clone()));
        Self {
            config,
            hot,
            cold,
            dispatcher,
        }
    }

    pub fn config(&self) -> &TieredStoreConfig {
        &self.config
    }

    pub fn cold_store(&self) -> &Arc<TieredFileStore> {
        &self.cold
    }

    pub fn dispatcher(&self) -> &Arc<MessageBufferManager> {
        &self.dispatcher
    }

    /// Decide whether a read for `(topic, queueId, offset)` should be served
    /// by the cold store, mirroring Java `TieredMessageStore#fetchFromCurrentStore`.
    ///
    /// Simplification: `NOT_IN_MEM` cannot be distinguished from `NOT_IN_DISK`
    /// because [`HotStoreReader`] cannot report page-cache residency, so both
    /// levels treat any offset outside the hot `[min, max)` range as "not in
    /// store".
    pub fn fetch_from_cold(&self, topic: &str, queue_id: i32, offset: u64, batch_size: usize) -> bool {
        let level = self.config.storage_level;
        if level.check(TieredStorageLevel::Force) {
            return true;
        }
        if !level.is_enable() {
            return false;
        }
        let Some(cold_max) = self.cold.max_offset(topic, queue_id) else {
            return false;
        };
        if offset >= cold_max {
            return false;
        }
        let _ = batch_size;
        let hot_min = self.hot.min_offset(topic, queue_id);
        let hot_max = self.hot.max_offset(topic, queue_id);
        // CommitLog empty or offset already evicted from the hot store.
        !(hot_min <= offset && offset < hot_max)
    }

    /// Java `TieredMessageStore#getMessage`: serve from the cold store when
    /// the level says so, falling back to the hot store on miss or error
    /// (Java falls back on `OFFSET_FOUND_NULL` / `NO_MATCHED_LOGIC_QUEUE` and
    /// on fetcher exceptions).
    pub fn get_message(&self, topic: &str, queue_id: i32, offset: u64, max_count: usize) -> Result<Vec<Message>> {
        if !self.fetch_from_cold(topic, queue_id, offset, max_count) {
            return self.hot.read(topic, queue_id, offset, max_count);
        }
        match self.cold.read(topic, queue_id, offset, max_count) {
            // Cold hit (including partial hits at the cold/hot boundary):
            // the result is returned without consulting the hot store.
            Ok(messages) if !messages.is_empty() => Ok(messages),
            // Cold miss or error: fall back to the hot store.
            _ => self.hot.read(topic, queue_id, offset, max_count),
        }
    }

    /// Java `getMinOffsetInQueue`: the smaller of hot and cold min offsets.
    pub fn min_offset_in_queue(&self, topic: &str, queue_id: i32) -> u64 {
        let hot_min = self.hot.min_offset(topic, queue_id);
        match self.cold.min_offset(topic, queue_id) {
            Some(cold_min) => hot_min.min(cold_min),
            None => hot_min,
        }
    }

    /// The larger of hot and cold next-offsets.
    pub fn max_offset_in_queue(&self, topic: &str, queue_id: i32) -> u64 {
        let hot_max = self.hot.max_offset(topic, queue_id);
        hot_max.max(self.cold.max_offset(topic, queue_id).unwrap_or(0))
    }

    /// Java `getMessageStoreTimeStampAsync`.
    pub fn message_store_timestamp(&self, topic: &str, queue_id: i32, offset: u64) -> Option<i64> {
        if self.fetch_from_cold(topic, queue_id, offset, 1) {
            if let Some(time) = self.cold.store_time(topic, queue_id, offset) {
                return Some(time);
            }
        }
        self.hot.store_time(topic, queue_id, offset)
    }

    /// Java `getEarliestMessageTimeAsync`: the earliest store time across hot
    /// and cold stores.
    pub fn earliest_message_time(&self, topic: &str, queue_id: i32) -> Option<i64> {
        let hot_time = self
            .hot
            .store_time(topic, queue_id, self.hot.min_offset(topic, queue_id));
        let cold_time = self
            .cold
            .min_offset(topic, queue_id)
            .and_then(|offset| self.cold.store_time(topic, queue_id, offset));
        match (hot_time, cold_time) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        }
    }

    /// Pull newly arrived messages from the hot store into the dispatch
    /// buffer so they are flushed into cold segments (Java registers the
    /// dispatcher on the next store; integrators can call this periodically
    /// or per-append instead). Returns the number of dispatched messages.
    pub async fn dispatch_from_hot(&self, topic: &str, queue_id: i32) -> Result<usize> {
        let mut dispatched = 0usize;
        loop {
            let cold_max = self.cold.max_offset(topic, queue_id).unwrap_or(0);
            let hot_max = self.hot.max_offset(topic, queue_id);
            if cold_max >= hot_max {
                break;
            }
            let batch = match self.hot.read(topic, queue_id, cold_max, 256) {
                Ok(batch) => batch,
                Err(Error::NotFound(_)) => break,
                Err(error) => return Err(error),
            };
            if batch.is_empty() {
                break;
            }
            for message in batch {
                self.dispatcher.dispatch(message).await?;
                dispatched += 1;
            }
        }
        Ok(dispatched)
    }

    /// Flush all buffered messages into cold segments.
    pub async fn flush_pending(&self) -> Result<usize> {
        self.dispatcher.flush_all().await
    }

    /// Flush cold segments to disk and persist metadata.
    pub fn flush_cold(&self) -> Result<usize> {
        self.cold.flush()
    }

    /// Metrics snapshot, including dispatch buffer occupancy.
    pub fn metrics(&self) -> TieredStoreMetrics {
        let mut metrics = self.cold.metrics();
        metrics.pending_dispatch_count = self.dispatcher.pending_count();
        metrics.pending_dispatch_bytes = self.dispatcher.pending_bytes();
        metrics
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn make_message(topic: &str, queue_id: i32, offset: u64, store_timestamp: i64) -> Message {
        Message {
            topic: topic.to_string(),
            queue_id,
            queue_offset: offset,
            physical_offset: offset * 100,
            flag: 0,
            sys_flag: 0,
            born_timestamp: store_timestamp - 10,
            born_host: "127.0.0.1:9876".parse().unwrap(),
            store_timestamp,
            store_host: "127.0.0.1:10911".parse().unwrap(),
            reconsume_times: 0,
            prepared_transaction_offset: 0,
            body: format!("body-{topic}-{queue_id}-{offset}").into_bytes(),
            properties: BTreeMap::from([
                ("KEYS".to_string(), format!("key-{offset}")),
                ("UNIQ_KEY".to_string(), format!("uniq-{offset}")),
            ]),
        }
    }

    fn memory_metadata() -> TieredMetadataStore {
        TieredMetadataStore::open(None::<&Path>).unwrap()
    }

    // -- config -------------------------------------------------------------

    #[test]
    fn test_config_defaults_and_size_parsing() {
        let config = TieredStoreConfig::default();
        assert_eq!(config.storage_level, TieredStorageLevel::NotInDisk);
        assert_eq!(config.file_segment_size, 1024 * 1024 * 1024);
        assert_eq!(config.file_reserved_time, 72);
        assert_eq!(config.delete_file_interval_ms, 60 * 60 * 1000);
        assert!(config.group_commit_enable);
        assert_eq!(config.group_commit_count, 4096);
        assert_eq!(config.group_commit_size, 4 * 1024 * 1024);
        assert_eq!(config.max_group_commit_count, 10000);
        assert_eq!(config.max_pending_limit, 10000);

        // Java level semantics.
        assert!(!TieredStorageLevel::Disabled.is_enable());
        assert!(TieredStorageLevel::Force.check(TieredStorageLevel::NotInDisk));
        assert!(!TieredStorageLevel::NotInDisk.check(TieredStorageLevel::Force));
        assert_eq!(TieredStorageLevel::from_value(3), TieredStorageLevel::Force);

        // String sizes.
        let config: TieredStoreConfig =
            serde_json::from_str(r#"{"storageLevel":"FORCE","fileSegmentSize":"512m"}"#).unwrap();
        assert_eq!(config.storage_level, TieredStorageLevel::Force);
        assert_eq!(config.file_segment_size, 512 * 1024 * 1024);

        // Numeric sizes; other fields fall back to defaults.
        let config: TieredStoreConfig =
            serde_json::from_str(r#"{"fileSegmentSize":4096}"#).unwrap();
        assert_eq!(config.file_segment_size, 4096);
        assert_eq!(config.storage_level, TieredStorageLevel::NotInDisk);

        // Roundtrip keeps all fields.
        let text = serde_json::to_string(&TieredStoreConfig::default()).unwrap();
        assert!(text.contains("\"fileSegmentSize\":1073741824"));
        let roundtrip: TieredStoreConfig = serde_json::from_str(&text).unwrap();
        assert_eq!(roundtrip, TieredStoreConfig::default());

        assert_eq!(parse_size("1g").unwrap(), 1024 * 1024 * 1024);
        assert_eq!(parse_size("512m").unwrap(), 512 * 1024 * 1024);
        assert_eq!(parse_size("64kb").unwrap(), 64 * 1024);
        assert_eq!(parse_size("1024b").unwrap(), 1024);
        assert_eq!(parse_size("4096").unwrap(), 4096);
        assert!(parse_size("abc").is_err());
        assert!(parse_size("12x").is_err());
        assert!(parse_size("").is_err());
    }

    // -- file handle cache ----------------------------------------------------

    #[test]
    fn test_file_handle_cache_lru() {
        let dir = tempfile::tempdir().unwrap();
        let path_a = dir.path().join("a.txt");
        let path_b = dir.path().join("b.txt");
        let path_c = dir.path().join("c.txt");
        fs::write(&path_a, b"alpha").unwrap();
        fs::write(&path_b, b"beta").unwrap();
        fs::write(&path_c, b"gamma").unwrap();

        let mut cache = FileHandleCache::new(2);
        assert!(cache.is_empty());

        let mut content = String::new();
        cache.get_or_open(&path_a).unwrap().read_to_string(&mut content).unwrap();
        cache.get_or_open(&path_b).unwrap().read_to_string(&mut content).unwrap();
        assert_eq!(cache.len(), 2);

        // Touch `a` so `b` becomes the LRU entry, then insert `c`.
        cache.get_or_open(&path_a).unwrap().read_to_string(&mut content).unwrap();
        cache.get_or_open(&path_c).unwrap().read_to_string(&mut content).unwrap();
        assert_eq!(cache.len(), 2);

        // Evicted entries can be reopened; contents stay readable.
        cache.invalidate(&path_a);
        cache.get_or_open(&path_b).unwrap().read_to_string(&mut content).unwrap();
        assert!(content.contains("alpha") && content.contains("beta") && content.contains("gamma"));
    }

    // -- segment --------------------------------------------------------------

    #[test]
    fn test_segment_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let segment = TieredFileSegment::new(
            dir.path().to_path_buf(),
            "topic-a".to_string(),
            3,
            100,
            1024 * 1024,
        );
        let messages: Vec<Message> = (0..5u64)
            .map(|i| make_message("topic-a", 3, 100 + i, 1000 + i as i64))
            .collect();

        for (index, message) in messages.iter().enumerate() {
            let result = segment.append(message);
            assert_eq!(result, AppendResult::Success(100 + index as u64));
        }
        assert_eq!(segment.record_count(), 5);
        assert_eq!(segment.committed_records(), 0);

        // Uncommitted data is not readable (Java commit-position semantics).
        let mut cache = FileHandleCache::new(4);
        assert!(segment.read(&mut cache, 100, 5).is_err());

        assert_eq!(segment.flush().unwrap(), segment.committed_size());
        assert_eq!(segment.committed_records(), 5);
        assert_eq!(segment.min_timestamp(), 1000);
        assert_eq!(segment.max_timestamp(), 1004);

        let loaded = segment.read(&mut cache, 100, 5).unwrap();
        assert_eq!(loaded.len(), 5);
        for (original, restored) in messages.iter().zip(&loaded) {
            assert_eq!(original, restored);
        }

        // Sealed segments reject appends.
        segment.seal();
        assert_eq!(
            segment.append(&make_message("topic-a", 3, 105, 0)),
            AppendResult::Closed
        );
        assert!(segment.exists());

        // Roundtrip through the metadata description.
        let metadata = SegmentMetadata {
            base_offset: 100,
            size: segment.committed_size(),
            sealed: true,
            min_timestamp: 1000,
            max_timestamp: 1004,
            create_timestamp: 0,
        };
        drop(cache);
        let recovered = TieredFileSegment::new(
            dir.path().to_path_buf(),
            "topic-a".to_string(),
            3,
            100,
            1024 * 1024,
        );
        recovered.recover(&metadata).unwrap();
        assert_eq!(recovered.committed_records(), 5);
        assert!(recovered.is_sealed());
        let mut cache = FileHandleCache::new(4);
        assert_eq!(recovered.read(&mut cache, 102, 2).unwrap()[0].queue_offset, 102);
    }

    // -- file store -----------------------------------------------------------

    #[test]
    fn test_file_store_rolling_and_recovery() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("tiered");
        let meta_path = dir.path().join("metadata.json");
        let config = TieredStoreConfig {
            file_segment_size: parse_size("4kb").unwrap(),
            metadata_path: meta_path.to_string_lossy().into_owned(),
            ..TieredStoreConfig::default()
        };
        let metadata = TieredMetadataStore::open(Some(meta_path.as_path())).unwrap();
        let store = TieredFileStore::open(root.clone(), config.clone(), metadata).unwrap();

        for offset in 0..32u64 {
            store
                .append_message(&make_message("rolling-topic", 0, offset, 10_000 + offset as i64 * 10))
                .unwrap();
        }
        store.flush().unwrap();

        let segment_count = store.metrics().segment_count;
        assert!(segment_count >= 2, "expected rolling across segments, got {segment_count}");
        assert_eq!(store.metrics().message_count, 32);
        assert_eq!(store.max_offset("rolling-topic", 0), Some(32));
        assert_eq!(store.min_offset("rolling-topic", 0), Some(0));

        let read = store.read("rolling-topic", 0, 10, 10).unwrap();
        assert_eq!(read.len(), 10);
        assert_eq!(read[0].queue_offset, 10);
        assert_eq!(read[9].queue_offset, 19);
        assert!(store.read("rolling-topic", 0, 100, 1).is_err());

        // Metadata file persisted with topic/queue/segment entries.
        let text = fs::read_to_string(&meta_path).unwrap();
        assert!(text.contains("rolling-topic"));

        // Reopen from disk and verify full recovery.
        drop(store);
        let metadata = TieredMetadataStore::open(Some(meta_path.as_path())).unwrap();
        assert!(metadata.get_topic("rolling-topic").is_some());
        let store = TieredFileStore::open(root, config, metadata).unwrap();
        assert_eq!(store.metrics().segment_count, segment_count);
        let read = store.read("rolling-topic", 0, 0, 32).unwrap();
        assert_eq!(read.len(), 32);
        for (index, message) in read.iter().enumerate() {
            assert_eq!(message.queue_offset, index as u64);
            assert_eq!(message.store_timestamp, 10_000 + index as i64 * 10);
        }

        store.delete_topic("rolling-topic").unwrap();
        assert!(store.min_offset("rolling-topic", 0).is_none());
        assert_eq!(store.metrics().segment_count, 0);
    }

    // -- fallback -------------------------------------------------------------

    struct MockHotStore {
        messages: Vec<Message>,
        read_calls: AtomicUsize,
    }

    impl MockHotStore {
        fn new(messages: Vec<Message>) -> Self {
            Self {
                messages,
                read_calls: AtomicUsize::new(0),
            }
        }

        fn read_calls(&self) -> usize {
            self.read_calls.load(Ordering::SeqCst)
        }
    }

    impl HotStoreReader for MockHotStore {
        fn read(&self, topic: &str, queue_id: i32, offset: u64, max_count: usize) -> Result<Vec<Message>> {
            self.read_calls.fetch_add(1, Ordering::SeqCst);
            let matched: Vec<Message> = self
                .messages
                .iter()
                .filter(|m| m.topic == topic && m.queue_id == queue_id && m.queue_offset >= offset)
                .take(max_count)
                .cloned()
                .collect();
            if matched.is_empty() {
                Err(Error::NotFound(format!("hot miss {topic}/{queue_id}@{offset}")))
            } else {
                Ok(matched)
            }
        }

        fn max_offset(&self, topic: &str, queue_id: i32) -> u64 {
            self.messages
                .iter()
                .filter(|m| m.topic == topic && m.queue_id == queue_id)
                .map(|m| m.queue_offset + 1)
                .max()
                .unwrap_or(0)
        }

        fn min_offset(&self, topic: &str, queue_id: i32) -> u64 {
            self.messages
                .iter()
                .filter(|m| m.topic == topic && m.queue_id == queue_id)
                .map(|m| m.queue_offset)
                .min()
                .unwrap_or(0)
        }

        fn store_time(&self, topic: &str, queue_id: i32, offset: u64) -> Option<i64> {
            self.messages
                .iter()
                .find(|m| m.topic == topic && m.queue_id == queue_id && m.queue_offset == offset)
                .map(|m| m.store_timestamp)
        }
    }

    #[tokio::test]
    async fn test_fallback_order() {
        let dir = tempfile::tempdir().unwrap();
        let config = TieredStoreConfig {
            storage_level: TieredStorageLevel::Force,
            ..TieredStoreConfig::default()
        };
        let cold = TieredFileStore::open(
            dir.path().join("cold"),
            config.clone(),
            memory_metadata(),
        )
        .unwrap();
        let hot = Arc::new(MockHotStore::new(
            (0..6u64).map(|i| make_message("fb-topic", 0, i, 100 + i as i64)).collect(),
        ));
        let store = TieredMessageStore::new(config, hot.clone(), cold.clone());

        // 1) Cold store empty (miss) -> hot store serves the read.
        let result = store.get_message("fb-topic", 0, 2, 2).unwrap();
        assert_eq!(hot.read_calls(), 1);
        assert_eq!(result.len(), 2);
        assert_eq!(result[0].queue_offset, 2);

        // 2) Push offsets 0..4 into the cold store.
        for offset in 0..4u64 {
            store
                .dispatcher()
                .dispatch(make_message("fb-topic", 0, offset, 100 + offset as i64))
                .await
                .unwrap();
        }
        assert_eq!(store.dispatcher().flush_all().await.unwrap(), 4);

        // 3) Cold hit -> served from cold, hot store untouched (Java returns
        //    FOUND without consulting `next`).
        let result = store.get_message("fb-topic", 0, 0, 10).unwrap();
        assert_eq!(hot.read_calls(), 1);
        assert_eq!(result.len(), 4);
        assert_eq!(result[0].queue_offset, 0);

        // 4) Cold read error (backing file removed) -> hot store fallback,
        //    mirroring the Java `exceptionally` path.
        let paths: Vec<PathBuf> = cold
            .queue_segments("fb-topic", 0)
            .iter()
            .map(|segment| segment.file_path())
            .collect();
        for path in &paths {
            fs::remove_file(path).unwrap();
        }
        cold.handles.lock().unwrap().invalidate_all();
        let result = store.get_message("fb-topic", 0, 0, 2).unwrap();
        assert_eq!(hot.read_calls(), 2);
        assert_eq!(result[0].queue_offset, 0);
    }

    #[tokio::test]
    async fn test_fetch_from_cold_levels() {
        let dir = tempfile::tempdir().unwrap();
        // Cold store holds offsets 0..4, hot store holds offsets 2..6.
        let config = TieredStoreConfig {
            storage_level: TieredStorageLevel::NotInDisk,
            ..TieredStoreConfig::default()
        };
        let cold = TieredFileStore::open(
            dir.path().join("cold"),
            config.clone(),
            memory_metadata(),
        )
        .unwrap();
        for offset in 0..4u64 {
            cold.append_message(&make_message("lv-topic", 0, offset, 200 + offset as i64))
                .unwrap();
        }
        cold.flush().unwrap();
        let hot = Arc::new(MockHotStore::new(
            (2..6u64).map(|i| make_message("lv-topic", 0, i, 200 + i as i64)).collect(),
        ));

        // NOT_IN_DISK: offset 0 is outside the hot range -> cold read.
        let store = TieredMessageStore::new(config.clone(), hot.clone(), cold.clone());
        assert!(store.fetch_from_cold("lv-topic", 0, 0, 2));
        let result = store.get_message("lv-topic", 0, 0, 2).unwrap();
        assert_eq!(hot.read_calls(), 0);
        assert_eq!(result[0].queue_offset, 0);

        // NOT_IN_DISK: offset 3 is inside the hot range -> hot store directly.
        let result = store.get_message("lv-topic", 0, 3, 2).unwrap();
        assert_eq!(hot.read_calls(), 1);
        assert_eq!(result[0].queue_offset, 3);

        // DISABLED: everything goes to the hot store, even cold hits.
        let disabled = TieredStoreConfig {
            storage_level: TieredStorageLevel::Disabled,
            ..TieredStoreConfig::default()
        };
        let store = TieredMessageStore::new(disabled, hot.clone(), cold.clone());
        assert!(!store.fetch_from_cold("lv-topic", 0, 0, 2));
        let result = store.get_message("lv-topic", 0, 0, 2).unwrap();
        assert_eq!(hot.read_calls(), 2);
        assert_eq!(result[0].queue_offset, 2);
    }

    // -- dispatch buffer --------------------------------------------------------

    #[tokio::test]
    async fn test_buffer_manager_flush_threshold() {
        let dir = tempfile::tempdir().unwrap();
        let config = TieredStoreConfig {
            group_commit_count: 4,
            ..TieredStoreConfig::default()
        };
        let cold = TieredFileStore::open(
            dir.path().join("cold"),
            config.clone(),
            memory_metadata(),
        )
        .unwrap();
        let manager = MessageBufferManager::new(config, cold.clone());

        for offset in 0..3u64 {
            manager
                .dispatch(make_message("buf-topic", 0, offset, 300 + offset as i64))
                .await
                .unwrap();
        }
        assert_eq!(manager.pending_count(), 3);
        // Nothing flushed yet -> cold read misses.
        assert!(cold.read("buf-topic", 0, 0, 3).is_err());

        // The 4th message crosses the count threshold -> automatic flush.
        manager
            .dispatch(make_message("buf-topic", 0, 3, 303))
            .await
            .unwrap();
        assert_eq!(manager.pending_count(), 0);
        let read = cold.read("buf-topic", 0, 0, 4).unwrap();
        assert_eq!(read.len(), 4);
        assert_eq!(read[3].queue_offset, 3);
        assert_eq!(cold.max_offset("buf-topic", 0), Some(4));

        // Backpressure: exceeding the pending limit rejects with Timeout.
        let config = TieredStoreConfig {
            group_commit_count: 100,
            max_pending_limit: 2,
            ..TieredStoreConfig::default()
        };
        let cold = TieredFileStore::open(
            dir.path().join("cold2"),
            config.clone(),
            memory_metadata(),
        )
        .unwrap();
        let manager = MessageBufferManager::new(config, cold);
        manager.dispatch(make_message("bp-topic", 0, 0, 0)).await.unwrap();
        manager.dispatch(make_message("bp-topic", 0, 1, 0)).await.unwrap();
        assert!(matches!(
            manager.dispatch(make_message("bp-topic", 0, 2, 0)).await,
            Err(Error::Timeout(_))
        ));
    }

    #[tokio::test]
    async fn test_dispatch_from_hot() {
        let dir = tempfile::tempdir().unwrap();
        let config = TieredStoreConfig {
            storage_level: TieredStorageLevel::Force,
            group_commit_count: 4,
            ..TieredStoreConfig::default()
        };
        let cold = TieredFileStore::open(
            dir.path().join("cold"),
            config.clone(),
            memory_metadata(),
        )
        .unwrap();
        let hot = Arc::new(MockHotStore::new(
            (0..8u64).map(|i| make_message("pull-topic", 0, i, 400 + i as i64)).collect(),
        ));
        let store = TieredMessageStore::new(config, hot.clone(), cold.clone());

        let moved = store.dispatch_from_hot("pull-topic", 0).await.unwrap();
        assert_eq!(moved, 8);
        let read = store
            .cold_store()
            .read("pull-topic", 0, 0, 8)
            .unwrap();
        assert_eq!(read.len(), 8);
        assert_eq!(read[7].queue_offset, 7);

        // Nothing new: a second pull is a no-op.
        let moved = store.dispatch_from_hot("pull-topic", 0).await.unwrap();
        assert_eq!(moved, 0);

        // Timestamp lookups are served from the cold store (Force level).
        assert_eq!(
            store.message_store_timestamp("pull-topic", 0, 5),
            Some(405)
        );
        assert_eq!(store.min_offset_in_queue("pull-topic", 0), 0);
        assert_eq!(store.max_offset_in_queue("pull-topic", 0), 8);
        assert_eq!(store.metrics().message_count, 8);
        assert_eq!(store.metrics().pending_dispatch_count, 0);
    }

    // -- metadata store ---------------------------------------------------------

    #[test]
    fn test_metadata_store_persistence() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("meta.json");

        {
            let store = TieredMetadataStore::open(Some(path.as_path())).unwrap();
            let topic = store.add_topic("meta-topic", 72);
            assert_eq!(topic.topic_id, 0);
            // add_topic is idempotent.
            assert_eq!(store.add_topic("meta-topic", 72).topic_id, 0);
            store.update_queue("meta-topic", 1, 10, 20);
            store.update_segment(
                "meta-topic",
                1,
                SegmentMetadata {
                    base_offset: 0,
                    size: 512,
                    sealed: false,
                    min_timestamp: 1,
                    max_timestamp: 2,
                    create_timestamp: now_millis(),
                },
            );
            store.persist().unwrap();
        }

        let store = TieredMetadataStore::open(Some(path.as_path())).unwrap();
        let topic = store.get_topic("meta-topic").unwrap();
        assert_eq!(topic.topic_id, 0);
        assert_eq!(topic.reserve_time, 72);
        let queue = store.get_queue("meta-topic", 1).unwrap();
        assert_eq!((queue.min_offset, queue.max_offset), (10, 20));
        let segments = store.iterate_segments();
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].0, "meta-topic");
        assert_eq!(segments[0].1, 1);
        assert_eq!(segments[0].2[0].size, 512);

        store.delete_topic("meta-topic");
        assert!(store.get_topic("meta-topic").is_none());
        assert!(store.iterate_segments().is_empty());
    }

    // -- message codec ------------------------------------------------------------

    #[test]
    fn test_message_codec_roundtrip() {
        let message = make_message("codec/topic", 7, 42, 1234567890);
        let encoded = encode_message(&message);
        let decoded = decode_message(&encoded).unwrap();
        assert_eq!(message, decoded);
        // Truncated payload is rejected.
        assert!(decode_message(&encoded[..encoded.len() - 1]).is_err());
    }
}
