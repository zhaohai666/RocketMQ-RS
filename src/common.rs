use std::{
    collections::BTreeMap,
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    time::{SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("JSON error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("invalid data: {0}")]
    Invalid(String),
    #[error("unsupported: {0}")]
    Unsupported(String),
    #[error("not found: {0}")]
    NotFound(String),
    #[error("already exists: {0}")]
    AlreadyExists(String),
    #[error("timeout: {0}")]
    Timeout(String),
    #[error("authentication/authorization failed: {0}")]
    Auth(String),
}

pub type Result<T> = std::result::Result<T, Error>;

/// Maximum remoting payload (excluding its four-byte length prefix), or message record.
pub const MAX_FRAME_SIZE: usize = 16 * 1024 * 1024;
pub const MESSAGE_MAGIC_CODE: i32 = -626_843_481;
pub const MESSAGE_MAGIC_CODE_V2: i32 = -626_843_477;
const BORN_HOST_V6: i32 = 1 << 4;
const STORE_HOST_V6: i32 = 1 << 5;
const MAX_SHORT_LENGTH: usize = i16::MAX as usize;

pub fn now_millis() -> i64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_millis().min(i64::MAX as u128) as i64,
        Err(error) => -(error.duration().as_millis().min(i64::MAX as u128) as i64),
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "camelCase", default)]
pub struct TopicConfig {
    pub topic_name: String,
    pub read_queue_nums: i32,
    pub write_queue_nums: i32,
    pub perm: i32,
    pub topic_sys_flag: i32,
    pub order: bool,
    pub attributes: BTreeMap<String, String>,
}

impl Default for TopicConfig {
    fn default() -> Self {
        Self {
            topic_name: String::new(),
            read_queue_nums: 8,
            write_queue_nums: 8,
            perm: 6,
            topic_sys_flag: 0,
            order: false,
            attributes: BTreeMap::new(),
        }
    }
}

impl TopicConfig {
    pub fn new(name: impl Into<String>, queues: i32) -> Self {
        Self {
            topic_name: name.into(),
            read_queue_nums: queues,
            write_queue_nums: queues,
            ..Self::default()
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct Message {
    pub topic: String,
    pub queue_id: i32,
    pub queue_offset: u64,
    pub physical_offset: u64,
    pub flag: i32,
    pub sys_flag: i32,
    pub born_timestamp: i64,
    pub born_host: SocketAddr,
    pub store_timestamp: i64,
    pub store_host: SocketAddr,
    pub reconsume_times: i32,
    pub prepared_transaction_offset: u64,
    /// Stored bytes, including compression when indicated by sys_flag.
    pub body: Vec<u8>,
    pub properties: BTreeMap<String, String>,
}

impl Default for Message {
    fn default() -> Self {
        Self {
            topic: String::new(),
            queue_id: 0,
            queue_offset: 0,
            physical_offset: 0,
            flag: 0,
            sys_flag: 0,
            born_timestamp: 0,
            born_host: SocketAddr::from(([0, 0, 0, 0], 0)),
            store_timestamp: 0,
            store_host: SocketAddr::from(([0, 0, 0, 0], 0)),
            reconsume_times: 0,
            prepared_transaction_offset: 0,
            body: Vec::new(),
            properties: BTreeMap::new(),
        }
    }
}

impl Message {
    pub fn encode(&self) -> Result<Vec<u8>> {
        if self.topic.len() > MAX_SHORT_LENGTH {
            return Err(invalid("topic exceeds signed short length"));
        }
        let properties = encode_properties(&self.properties)?;
        let v2 = self.topic.len() > i8::MAX as usize;
        let mut size = 91 + usize::from(v2);
        size += if self.born_host.is_ipv6() { 12 } else { 0 };
        size += if self.store_host.is_ipv6() { 12 } else { 0 };
        for len in [self.body.len(), self.topic.len(), properties.len()] {
            size = checked_size(size, len)?;
        }
        // The address family, not a caller's stale bits, determines the host layout.
        let mut sys_flag = self.sys_flag & !(BORN_HOST_V6 | STORE_HOST_V6);
        if self.born_host.is_ipv6() {
            sys_flag |= BORN_HOST_V6;
        }
        if self.store_host.is_ipv6() {
            sys_flag |= STORE_HOST_V6;
        }
        let mut out = Vec::with_capacity(size);
        out.extend_from_slice(&(size as i32).to_be_bytes());
        let magic = if v2 {
            MESSAGE_MAGIC_CODE_V2
        } else {
            MESSAGE_MAGIC_CODE
        };
        out.extend_from_slice(&magic.to_be_bytes());
        out.extend_from_slice(&body_crc(&self.body).to_be_bytes());
        out.extend_from_slice(&self.queue_id.to_be_bytes());
        out.extend_from_slice(&self.flag.to_be_bytes());
        out.extend_from_slice(&self.queue_offset.to_be_bytes());
        out.extend_from_slice(&self.physical_offset.to_be_bytes());
        out.extend_from_slice(&sys_flag.to_be_bytes());
        out.extend_from_slice(&self.born_timestamp.to_be_bytes());
        encode_host(&mut out, self.born_host);
        out.extend_from_slice(&self.store_timestamp.to_be_bytes());
        encode_host(&mut out, self.store_host);
        out.extend_from_slice(&self.reconsume_times.to_be_bytes());
        out.extend_from_slice(&self.prepared_transaction_offset.to_be_bytes());
        out.extend_from_slice(&(self.body.len() as i32).to_be_bytes());
        out.extend_from_slice(&self.body);
        if v2 {
            out.extend_from_slice(&(self.topic.len() as u16).to_be_bytes());
        } else {
            out.push(self.topic.len() as u8);
        }
        out.extend_from_slice(self.topic.as_bytes());
        out.extend_from_slice(&(properties.len() as u16).to_be_bytes());
        out.extend_from_slice(properties.as_bytes());
        Ok(out)
    }

    /// Decode one record; bytes after that record belong to the caller.
    pub fn decode(bytes: &[u8]) -> Result<(Self, usize)> {
        let size = Reader::new(bytes).length()?;
        if !(91..=MAX_FRAME_SIZE).contains(&size) {
            return Err(invalid("invalid message record size"));
        }
        let record = bytes
            .get(..size)
            .ok_or_else(|| invalid("truncated message record"))?;
        let mut input = Reader::new(record);
        input.i32()?;
        let v2 = match input.i32()? {
            MESSAGE_MAGIC_CODE => false,
            MESSAGE_MAGIC_CODE_V2 => true,
            _ => return Err(invalid("invalid message magic")),
        };
        let crc = input.u32()?;
        let queue_id = input.i32()?;
        let flag = input.i32()?;
        let queue_offset = input.u64()?;
        let physical_offset = input.u64()?;
        let sys_flag = input.i32()?;
        let born_timestamp = input.i64()?;
        let born_host = decode_host(&mut input, sys_flag & BORN_HOST_V6 != 0)?;
        let store_timestamp = input.i64()?;
        let store_host = decode_host(&mut input, sys_flag & STORE_HOST_V6 != 0)?;
        let reconsume_times = input.i32()?;
        let prepared_transaction_offset = input.u64()?;
        let body_len = input.length()?;
        let body = input.take(body_len)?;
        if body_crc(body) != crc {
            return Err(invalid("message body CRC mismatch"));
        }
        let topic_len = if v2 {
            input.short_length()?
        } else {
            let len = input.u8()? as usize;
            if len > i8::MAX as usize {
                return Err(invalid("V1 topic length overflows signed byte"));
            }
            len
        };
        let topic = utf8(input.take(topic_len)?)?.to_owned();
        let properties_len = input.short_length()?;
        let properties = decode_properties(utf8(input.take(properties_len)?)?)?;
        input.finish()?;
        Ok((
            Self {
                topic,
                queue_id,
                queue_offset,
                physical_offset,
                flag,
                sys_flag,
                born_timestamp,
                born_host,
                store_timestamp,
                store_host,
                reconsume_times,
                prepared_transaction_offset,
                body: body.to_vec(),
                properties,
            },
            size,
        ))
    }

    pub fn message_id(&self) -> String {
        let mut bytes = Vec::with_capacity(28);
        encode_host(&mut bytes, self.store_host);
        bytes.extend_from_slice(&self.physical_offset.to_be_bytes());
        const HEX: &[u8; 16] = b"0123456789ABCDEF";
        let mut id = String::with_capacity(bytes.len() * 2);
        for byte in bytes {
            id.push(HEX[(byte >> 4) as usize] as char);
            id.push(HEX[(byte & 15) as usize] as char);
        }
        id
    }
}

fn encode_host(out: &mut Vec<u8>, host: SocketAddr) {
    match host.ip() {
        IpAddr::V4(ip) => out.extend_from_slice(&ip.octets()),
        IpAddr::V6(ip) => out.extend_from_slice(&ip.octets()),
    }
    out.extend_from_slice(&u32::from(host.port()).to_be_bytes());
}

fn decode_host(input: &mut Reader<'_>, ipv6: bool) -> Result<SocketAddr> {
    let ip = if ipv6 {
        IpAddr::V6(Ipv6Addr::from(input.array::<16>()?))
    } else {
        IpAddr::V4(Ipv4Addr::from(input.array::<4>()?))
    };
    let port = u16::try_from(input.u32()?).map_err(|_| invalid("invalid host port"))?;
    Ok(SocketAddr::new(ip, port))
}

pub fn encode_properties(properties: &BTreeMap<String, String>) -> Result<String> {
    let mut size = 0usize;
    for (key, value) in properties {
        if key.is_empty() || key.contains(['\x01', '\x02']) || value.contains(['\x01', '\x02']) {
            return Err(invalid("invalid property key or delimiter in property"));
        }
        for len in [key.len(), value.len(), 2] {
            size = size
                .checked_add(len)
                .ok_or_else(|| invalid("properties size overflow"))?;
            if size > MAX_SHORT_LENGTH {
                return Err(invalid("properties exceed signed short length"));
            }
        }
    }
    let mut out = String::with_capacity(size);
    for (key, value) in properties {
        out.push_str(key);
        out.push('\x01');
        out.push_str(value);
        out.push('\x02');
    }
    Ok(out)
}

pub fn decode_properties(properties: &str) -> Result<BTreeMap<String, String>> {
    if properties.len() > MAX_SHORT_LENGTH {
        return Err(invalid("properties exceed signed short length"));
    }
    let mut out = BTreeMap::new();
    if properties.is_empty() {
        return Ok(out);
    }
    // Like Java, accept a final property without the trailing separator, and last key wins.
    let properties = properties.strip_suffix('\x02').unwrap_or(properties);
    for property in properties.split('\x02') {
        let (key, value) = property
            .split_once('\x01')
            .ok_or_else(|| invalid("missing property separator"))?;
        if key.is_empty() || value.contains('\x01') {
            return Err(invalid("invalid property entry"));
        }
        out.insert(key.to_owned(), value.to_owned());
    }
    Ok(out)
}

pub type BatchMessage = (i32, Vec<u8>, BTreeMap<String, String>);

/// Java producer batches have placeholder magic/CRC fields, unlike stored records.
pub fn decode_batch(body: &[u8]) -> Result<Vec<BatchMessage>> {
    if body.len() > MAX_FRAME_SIZE {
        return Err(invalid("batch exceeds maximum frame size"));
    }
    let mut input = Reader::new(body);
    let mut messages = Vec::new();
    while input.remaining() > 0 {
        let size = input.length()?;
        if !(22..=MAX_FRAME_SIZE).contains(&size) {
            return Err(invalid("invalid batch record size"));
        }
        let mut record = Reader::new(input.take(size - 4)?);
        record.i32()?; // Producer placeholder magic.
        record.i32()?; // Producer placeholder CRC.
        let flag = record.i32()?;
        let body_len = record.length()?;
        let bytes = record.take(body_len)?;
        let properties_len = record.short_length()?;
        let properties = decode_properties(utf8(record.take(properties_len)?)?)?;
        record.finish()?;
        messages.push((flag, bytes.to_vec(), properties));
    }
    Ok(messages)
}

fn body_crc(body: &[u8]) -> u32 {
    crc32fast::hash(body) & 0x7fff_ffff
}

pub(crate) fn invalid(message: impl Into<String>) -> Error {
    Error::Invalid(message.into())
}

pub(crate) fn checked_size(size: usize, extra: usize) -> Result<usize> {
    let size = size
        .checked_add(extra)
        .ok_or_else(|| invalid("length overflow"))?;
    if size > MAX_FRAME_SIZE {
        return Err(invalid("maximum frame size exceeded"));
    }
    Ok(size)
}

pub(crate) fn utf8(bytes: &[u8]) -> Result<&str> {
    std::str::from_utf8(bytes).map_err(|_| invalid("invalid UTF-8"))
}

/// Bounds-checked big-endian cursor shared by the two wire codecs.
pub(crate) struct Reader<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Reader<'a> {
    pub(crate) fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, position: 0 }
    }

    pub(crate) fn remaining(&self) -> usize {
        self.bytes.len() - self.position
    }

    pub(crate) fn take(&mut self, size: usize) -> Result<&'a [u8]> {
        let end = self
            .position
            .checked_add(size)
            .ok_or_else(|| invalid("length overflow"))?;
        let value = self
            .bytes
            .get(self.position..end)
            .ok_or_else(|| invalid("truncated wire data"))?;
        self.position = end;
        Ok(value)
    }

    pub(crate) fn array<const N: usize>(&mut self) -> Result<[u8; N]> {
        self.take(N)?
            .try_into()
            .map_err(|_| invalid("invalid fixed-width field"))
    }

    pub(crate) fn u8(&mut self) -> Result<u8> {
        Ok(self.array::<1>()?[0])
    }
    pub(crate) fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_be_bytes(self.array()?))
    }
    pub(crate) fn i16(&mut self) -> Result<i16> {
        Ok(i16::from_be_bytes(self.array()?))
    }
    pub(crate) fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_be_bytes(self.array()?))
    }
    pub(crate) fn i32(&mut self) -> Result<i32> {
        Ok(i32::from_be_bytes(self.array()?))
    }
    pub(crate) fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_be_bytes(self.array()?))
    }
    pub(crate) fn i64(&mut self) -> Result<i64> {
        Ok(i64::from_be_bytes(self.array()?))
    }

    pub(crate) fn length(&mut self) -> Result<usize> {
        usize::try_from(self.i32()?).map_err(|_| invalid("negative length"))
    }

    pub(crate) fn short_length(&mut self) -> Result<usize> {
        let length = self.u16()? as usize;
        if length > MAX_SHORT_LENGTH {
            return Err(invalid("length overflows signed short"));
        }
        Ok(length)
    }

    pub(crate) fn finish(&self) -> Result<()> {
        if self.remaining() == 0 {
            Ok(())
        } else {
            Err(invalid("trailing wire data"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Message {
        Message {
            topic: "Topic测试".into(),
            queue_id: 3,
            queue_offset: 0x0102_0304_0506_0708,
            physical_offset: 0x1122_3344_5566_7788,
            flag: -2,
            sys_flag: 8,
            born_timestamp: 1_700_000_000_123,
            born_host: "192.168.1.10:12345".parse().unwrap(),
            store_timestamp: 1_700_000_000_456,
            store_host: "127.0.0.1:10911".parse().unwrap(),
            reconsume_times: 2,
            prepared_transaction_offset: 42,
            body: b"123456789".to_vec(),
            properties: BTreeMap::from([
                ("KEYS".into(), "key1".into()),
                ("TAGS".into(), "tagA".into()),
            ]),
        }
    }

    #[test]
    fn topic_config_defaults_and_java_json_names() {
        let config: TopicConfig = serde_json::from_str(r#"{"topicName":"T"}"#).unwrap();
        assert_eq!(config, TopicConfig::new("T", 8));
        let json = serde_json::to_value(&config).unwrap();
        assert_eq!(json["readQueueNums"], 8);
        assert_eq!(json["writeQueueNums"], 8);
        assert_eq!(json["perm"], 6);
        assert_eq!(json["topicSysFlag"], 0);
        assert_eq!(json["attributes"], serde_json::json!({}));
        assert!(now_millis() > 1_600_000_000_000);
    }

    #[test]
    fn ipv4_wire_layout_crc_and_message_id() {
        let message = sample();
        let bytes = message.encode().unwrap();
        assert_eq!(
            bytes.len(),
            91 + message.body.len() + message.topic.len() + 20
        );
        assert_eq!(&bytes[4..8], &MESSAGE_MAGIC_CODE.to_be_bytes());
        // Independent standard CRC32 vector; Java masks off the sign bit.
        assert_eq!(&bytes[8..12], &0x4bf4_3926u32.to_be_bytes());
        assert_eq!(&bytes[12..16], &3i32.to_be_bytes());
        assert_eq!(&bytes[20..28], &message.queue_offset.to_be_bytes());
        assert_eq!(&bytes[48..56], &[192, 168, 1, 10, 0, 0, 0x30, 0x39]);
        assert_eq!(&bytes[84..88], &9i32.to_be_bytes());
        assert_eq!(&bytes[88..97], b"123456789");
        assert_eq!(message.message_id(), "7F00000100002A9F1122334455667788");
        assert_eq!(Message::decode(&bytes).unwrap(), (message, bytes.len()));
    }

    #[test]
    fn ipv6_host_flags_are_normalized_independently() {
        for born_v6 in [false, true] {
            for store_v6 in [false, true] {
                let mut message = sample();
                if born_v6 {
                    message.born_host = "[2001:db8::1]:1000".parse().unwrap();
                }
                if store_v6 {
                    message.store_host = "[::1]:10911".parse().unwrap();
                }
                message.sys_flag |= BORN_HOST_V6 | STORE_HOST_V6;
                let bytes = message.encode().unwrap();
                message.sys_flag &= !(BORN_HOST_V6 | STORE_HOST_V6);
                if born_v6 {
                    message.sys_flag |= BORN_HOST_V6;
                }
                if store_v6 {
                    message.sys_flag |= STORE_HOST_V6;
                }
                assert_eq!(Message::decode(&bytes).unwrap().0, message);
                assert_eq!(message.message_id().len(), if store_v6 { 56 } else { 32 });
                if store_v6 {
                    assert_eq!(
                        message.message_id(),
                        "0000000000000000000000000000000100002A9F1122334455667788"
                    );
                }
            }
        }
    }

    #[test]
    fn topic_version_boundaries_and_utf8_byte_lengths() {
        for length in [0, 127, 128, 32767] {
            let message = Message {
                topic: "t".repeat(length),
                ..Message::default()
            };
            let bytes = message.encode().unwrap();
            assert_eq!(bytes.len(), 91 + length + usize::from(length > 127));
            let magic = if length > 127 {
                MESSAGE_MAGIC_CODE_V2
            } else {
                MESSAGE_MAGIC_CODE
            };
            assert_eq!(&bytes[4..8], &magic.to_be_bytes());
            assert_eq!(Message::decode(&bytes).unwrap().0, message);
        }
        let message = Message {
            topic: "界".repeat(43),
            ..Message::default()
        };
        assert_eq!(
            &message.encode().unwrap()[4..8],
            &MESSAGE_MAGIC_CODE_V2.to_be_bytes()
        );
        let message = Message {
            topic: "x".repeat(32768),
            ..Message::default()
        };
        assert!(message.encode().is_err());
    }

    #[test]
    fn properties_roundtrip_limits_and_malformed_entries() {
        let properties =
            BTreeMap::from([("中文".into(), "内容".into()), ("empty".into(), "".into())]);
        let wire = encode_properties(&properties).unwrap();
        assert_eq!(decode_properties(&wire).unwrap(), properties);
        assert_eq!(decode_properties("a\x011\x02a\x012").unwrap()["a"], "2");
        for wire in [
            "a",
            "\x01v\x02",
            "a\x01b\x01c\x02",
            "\x02",
            "a\x01v\x02\x02",
        ] {
            assert!(decode_properties(wire).is_err(), "{wire:?}");
        }
        for key in ["", "a\x01", "a\x02"] {
            assert!(encode_properties(&BTreeMap::from([(key.into(), "v".into())])).is_err());
        }
        assert!(encode_properties(&BTreeMap::from([("k".into(), "v\x02".into())])).is_err());
        let mut large = BTreeMap::from([("k".into(), "v".repeat(32764))]);
        assert_eq!(encode_properties(&large).unwrap().len(), 32767);
        let message = Message {
            properties: large.clone(),
            ..Message::default()
        };
        assert_eq!(
            Message::decode(&message.encode().unwrap()).unwrap().0,
            message
        );
        large.get_mut("k").unwrap().push('v');
        assert!(encode_properties(&large).is_err());
        assert!(decode_properties(&"x".repeat(32768)).is_err());
    }

    #[test]
    fn decode_consumes_one_record_and_preserves_compressed_bytes() {
        let message = Message {
            sys_flag: 1,
            body: vec![0x78, 0x9c, 0, 255, 1],
            ..sample()
        };
        let bytes = message.encode().unwrap();
        let joined = [bytes.as_slice(), bytes.as_slice()].concat();
        let (first, used) = Message::decode(&joined).unwrap();
        assert_eq!(used, bytes.len());
        assert_eq!(first, message);
        assert_eq!(Message::decode(&joined[used..]).unwrap().0, message);
    }

    #[test]
    fn message_rejects_truncation_corruption_and_length_overflows() {
        let bytes = sample().encode().unwrap();
        for length in 0..bytes.len() {
            assert!(
                Message::decode(&bytes[..length]).is_err(),
                "length {length}"
            );
        }
        for (offset, value) in [
            (0, -1i32),
            (0, 90),
            (0, i32::MAX),
            (4, 0),
            (8, 0),
            (52, 65536),
            (84, -1),
            (84, i32::MAX),
        ] {
            let mut bad = bytes.clone();
            bad[offset..offset + 4].copy_from_slice(&value.to_be_bytes());
            assert!(
                Message::decode(&bad).is_err(),
                "offset {offset}, value {value}"
            );
        }
        let mut bad = bytes.clone();
        bad[88] ^= 1;
        assert!(Message::decode(&bad).is_err());
        let mut bad = bytes.clone();
        bad[97] = 128;
        assert!(Message::decode(&bad).is_err());
        let mut bad = bytes.clone();
        bad[98] = 255;
        assert!(Message::decode(&bad).is_err());
        let mut bad = Message::default().encode().unwrap();
        bad[89..91].copy_from_slice(&0xffffu16.to_be_bytes());
        assert!(Message::decode(&bad).is_err());
        let mut bad = Message {
            topic: "t".repeat(128),
            ..Message::default()
        }
        .encode()
        .unwrap();
        bad[88..90].copy_from_slice(&0x8000u16.to_be_bytes());
        assert!(Message::decode(&bad).is_err());
        let mut bad = bytes.clone();
        bad.push(0);
        let length = bad.len() as i32;
        bad[..4].copy_from_slice(&length.to_be_bytes());
        assert!(Message::decode(&bad).is_err());
    }

    fn batch_record(flag: i32, body: &[u8], properties: &str) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&((22 + body.len() + properties.len()) as i32).to_be_bytes());
        bytes.extend_from_slice(&[0; 8]);
        bytes.extend_from_slice(&flag.to_be_bytes());
        bytes.extend_from_slice(&(body.len() as i32).to_be_bytes());
        bytes.extend_from_slice(body);
        bytes.extend_from_slice(&(properties.len() as u16).to_be_bytes());
        bytes.extend_from_slice(properties.as_bytes());
        bytes
    }

    #[test]
    fn producer_batch_java_placeholders_and_multiple_records() {
        let one = batch_record(7, b"abc", "TAGS\x01a\x02");
        let two = batch_record(-1, b"", "");
        let decoded = decode_batch(&[one.clone(), two].concat()).unwrap();
        assert_eq!(
            decoded,
            vec![
                (
                    7,
                    b"abc".to_vec(),
                    BTreeMap::from([("TAGS".into(), "a".into())])
                ),
                (-1, vec![], BTreeMap::new())
            ]
        );
        assert!(decode_batch(&[]).unwrap().is_empty());
        for length in 1..one.len() {
            assert!(decode_batch(&one[..length]).is_err());
        }
        for (offset, value) in [(0, -1i32), (0, 21), (0, i32::MAX), (16, -1), (16, i32::MAX)] {
            let mut bad = one.clone();
            bad[offset..offset + 4].copy_from_slice(&value.to_be_bytes());
            assert!(decode_batch(&bad).is_err());
        }
        let mut bad = one.clone();
        bad[23..25].copy_from_slice(&0x8000u16.to_be_bytes());
        assert!(decode_batch(&bad).is_err());
        let mut bad = one;
        bad[25] = 255;
        assert!(decode_batch(&bad).is_err());
    }

    #[test]
    fn maximum_record_and_oversized_input() {
        let mut message = Message {
            body: vec![0; MAX_FRAME_SIZE - 91],
            ..Message::default()
        };
        let bytes = message.encode().unwrap();
        assert_eq!(bytes.len(), MAX_FRAME_SIZE);
        assert_eq!(Message::decode(&bytes).unwrap().1, MAX_FRAME_SIZE);
        message.body.push(0);
        assert!(message.encode().is_err());
        assert!(decode_batch(&vec![0; MAX_FRAME_SIZE + 1]).is_err());
    }

    #[test]
    fn arbitrary_and_mutated_inputs_never_panic() {
        let mut seed = 0x1234_5678u32;
        for size in 0..512 {
            let mut bytes = vec![0; size];
            for byte in &mut bytes {
                seed ^= seed << 13;
                seed ^= seed >> 17;
                seed ^= seed << 5;
                *byte = seed as u8;
            }
            let _ = Message::decode(&bytes);
            let _ = decode_batch(&bytes);
        }
        let original = sample().encode().unwrap();
        for index in 0..original.len() {
            for byte in [0, 127, 128, 255] {
                let mut bytes = original.clone();
                bytes[index] = byte;
                let _ = Message::decode(&bytes);
            }
        }
    }
}
