use std::{
    collections::BTreeMap,
    future::Future,
    io,
    net::SocketAddr,
    pin::Pin,
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicI32, Ordering},
    },
    time::Duration,
};

use serde::{Deserialize, Deserializer, Serialize};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::{Mutex, Semaphore, watch},
    task::{JoinError, JoinSet},
};

pub use crate::common::MAX_FRAME_SIZE;
use crate::common::{Error, Reader, Result, checked_size, invalid, utf8};

pub const RESPONSE_FLAG: i32 = 1;
pub const ONEWAY_FLAG: i32 = 2;
/// MQVersion.Version.V5_5_1.ordinal() in the Java server's protocol version table.
pub const PROTOCOL_VERSION: i32 = 515;
pub const MAX_CONNECTIONS: usize = 128;
pub const MAX_IN_FLIGHT_PER_CONNECTION: usize = 64;
pub const MAX_IN_FLIGHT_REQUESTS: usize = 1024;

static NEXT_OPAQUE: AtomicI32 = AtomicI32::new(0);
const LANGUAGES: [&str; 14] = [
    "JAVA", "CPP", "DOTNET", "PYTHON", "DELPHI", "ERLANG", "RUBY", "OTHER", "HTTP", "GO", "PHP",
    "OMS", "RUST", "NODE_JS",
];

#[derive(Default, Clone, Copy, Debug, PartialEq)]
pub enum SerializeType {
    #[default]
    Json,
    RocketMq,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Command {
    pub code: i32,
    pub language: String,
    pub version: i32,
    pub opaque: i32,
    pub flag: i32,
    #[serde(default)]
    pub remark: Option<String>,
    #[serde(default, deserialize_with = "nullable_fields")]
    pub ext_fields: BTreeMap<String, String>,
    #[serde(skip)]
    pub body: Vec<u8>,
    #[serde(skip)]
    pub serialize_type: SerializeType,
}

fn nullable_fields<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<BTreeMap<String, String>, D::Error> {
    Ok(Option::<BTreeMap<String, String>>::deserialize(deserializer)?.unwrap_or_default())
}

impl Command {
    pub fn request(code: i32) -> Self {
        Self {
            code,
            language: "RUST".into(),
            version: PROTOCOL_VERSION,
            // Atomic integer addition wraps just as Java AtomicInteger does.
            opaque: NEXT_OPAQUE.fetch_add(1, Ordering::Relaxed),
            flag: 0,
            remark: None,
            ext_fields: BTreeMap::new(),
            body: Vec::new(),
            serialize_type: SerializeType::Json,
        }
    }

    pub fn response(request: &Self, code: i32) -> Self {
        Self {
            code,
            language: "RUST".into(),
            version: PROTOCOL_VERSION,
            opaque: request.opaque,
            flag: RESPONSE_FLAG,
            remark: None,
            ext_fields: BTreeMap::new(),
            body: Vec::new(),
            serialize_type: request.serialize_type,
        }
    }

    pub fn with_ext(mut self, key: impl Into<String>, value: impl ToString) -> Self {
        self.ext_fields.insert(key.into(), value.to_string());
        self
    }

    pub fn with_body(mut self, body: Vec<u8>) -> Self {
        self.body = body;
        self
    }

    pub fn required(&self, key: &str) -> Result<&str> {
        self.ext_fields
            .get(key)
            .map(String::as_str)
            .ok_or_else(|| invalid(format!("missing field {key}")))
    }

    pub fn field<T: FromStr>(&self, key: &str) -> Result<T> {
        self.required(key)?
            .parse()
            .map_err(|_| invalid(format!("invalid field {key}")))
    }
}

fn language_code(language: &str) -> Result<u8> {
    LANGUAGES
        .iter()
        .position(|candidate| *candidate == language)
        .map(|index| index as u8)
        .ok_or_else(|| Error::Unsupported("unrecognized language code".into()))
}

/// A bounded sink prevents JSON escaping from allocating an oversized header.
struct HeaderWriter {
    bytes: Vec<u8>,
    limit: usize,
}

impl io::Write for HeaderWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if bytes.len() > self.limit.saturating_sub(self.bytes.len()) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "maximum header size exceeded",
            ));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub fn encode(command: &Command) -> Result<Vec<u8>> {
    let base_size = checked_size(4, command.body.len())?;
    let language = language_code(&command.language)?;
    let header = match command.serialize_type {
        SerializeType::Json => {
            let mut writer = HeaderWriter {
                bytes: Vec::new(),
                limit: MAX_FRAME_SIZE - base_size,
            };
            serde_json::to_writer(&mut writer, command)?;
            writer.bytes
        }
        SerializeType::RocketMq => {
            let code =
                i16::try_from(command.code).map_err(|_| invalid("code overflows signed short"))?;
            let version = i16::try_from(command.version)
                .map_err(|_| invalid("version overflows signed short"))?;
            let remark = command.remark.as_deref().unwrap_or("");
            let mut header_size = checked_size(21, remark.len())?;
            let mut ext_size = 0;
            for (key, value) in &command.ext_fields {
                // Although the wire slot is two bytes, Java reads it as a signed short.
                if key.len() > i16::MAX as usize {
                    return Err(invalid("extension key overflows signed short"));
                }
                for len in [6, key.len(), value.len()] {
                    ext_size = checked_size(ext_size, len)?;
                }
            }
            header_size = checked_size(header_size, ext_size)?;
            checked_size(base_size, header_size)?;
            let mut out = Vec::with_capacity(header_size);
            out.extend_from_slice(&code.to_be_bytes());
            out.push(language);
            out.extend_from_slice(&version.to_be_bytes());
            out.extend_from_slice(&command.opaque.to_be_bytes());
            out.extend_from_slice(&command.flag.to_be_bytes());
            out.extend_from_slice(&(remark.len() as i32).to_be_bytes());
            out.extend_from_slice(remark.as_bytes());
            out.extend_from_slice(&(ext_size as i32).to_be_bytes());
            for (key, value) in &command.ext_fields {
                out.extend_from_slice(&(key.len() as u16).to_be_bytes());
                out.extend_from_slice(key.as_bytes());
                out.extend_from_slice(&(value.len() as i32).to_be_bytes());
                out.extend_from_slice(value.as_bytes());
            }
            out
        }
    };
    let size = checked_size(base_size, header.len())?;
    let marker = ((match command.serialize_type {
        SerializeType::Json => 0u32,
        SerializeType::RocketMq => 1,
    }) << 24)
        | header.len() as u32;
    let mut out = Vec::with_capacity(size + 4);
    out.extend_from_slice(&(size as u32).to_be_bytes());
    out.extend_from_slice(&marker.to_be_bytes());
    out.extend_from_slice(&header);
    out.extend_from_slice(&command.body);
    Ok(out)
}

/// Decode exactly one complete frame, including its N4 prefix.
pub fn decode(bytes: &[u8]) -> Result<Command> {
    let mut input = Reader::new(bytes);
    let size = input.length()?;
    validate_frame_size(size)?;
    let payload = input.take(size)?;
    input.finish()?;
    decode_payload(payload)
}

fn validate_frame_size(size: usize) -> Result<()> {
    if !(4..=MAX_FRAME_SIZE).contains(&size) {
        Err(invalid("invalid frame size"))
    } else {
        Ok(())
    }
}

fn decode_payload(bytes: &[u8]) -> Result<Command> {
    let mut input = Reader::new(bytes);
    let marker = input.u32()?;
    let serialize_type = match marker >> 24 {
        0 => SerializeType::Json,
        1 => SerializeType::RocketMq,
        other => return Err(Error::Unsupported(format!("serialization type {other}"))),
    };
    let header_len = (marker & 0x00ff_ffff) as usize;
    let header = input.take(header_len)?;
    let mut command = match serialize_type {
        SerializeType::Json => {
            let command: Command = serde_json::from_slice(header)?;
            language_code(&command.language)?;
            command
        }
        SerializeType::RocketMq => {
            let mut header = Reader::new(header);
            let code = i32::from(header.i16()?);
            let language = LANGUAGES
                .get(header.u8()? as usize)
                .ok_or_else(|| invalid("unknown language code"))?
                .to_string();
            let version = i32::from(header.i16()?);
            let opaque = header.i32()?;
            let flag = header.i32()?;
            let remark_len = header.length()?;
            let remark = if remark_len == 0 {
                None
            } else {
                Some(utf8(header.take(remark_len)?)?.to_owned())
            };
            let ext_len = header.length()?;
            let mut fields = Reader::new(header.take(ext_len)?);
            header.finish()?;
            let mut ext_fields = BTreeMap::new();
            while fields.remaining() > 0 {
                let key_len = fields.short_length()?;
                let key = utf8(fields.take(key_len)?)?.to_owned();
                let value_len = fields.length()?;
                let value = utf8(fields.take(value_len)?)?.to_owned();
                ext_fields.insert(key, value);
            }
            Command {
                code,
                language,
                version,
                opaque,
                flag,
                remark,
                ext_fields,
                body: Vec::new(),
                serialize_type,
            }
        }
    };
    command.body = input.take(input.remaining())?.to_vec();
    command.serialize_type = serialize_type;
    Ok(command)
}

/// Partial reads are not cancellation-safe; retain this future until a frame completes.
pub async fn read_command<R: AsyncRead + Unpin>(reader: &mut R) -> Result<Option<Command>> {
    let mut prefix = [0u8; 4];
    if reader.read(&mut prefix[..1]).await? == 0 {
        return Ok(None);
    }
    reader.read_exact(&mut prefix[1..]).await?;
    let size = u32::from_be_bytes(prefix) as usize;
    validate_frame_size(size)?;
    let mut payload = vec![0; size];
    reader.read_exact(&mut payload).await?;
    Ok(Some(decode_payload(&payload)?))
}

pub async fn write_command<W: AsyncWrite + Unpin>(writer: &mut W, command: &Command) -> Result<()> {
    writer.write_all(&encode(command)?).await?;
    writer.flush().await?;
    Ok(())
}

pub trait Handler: Send + Sync + 'static {
    fn handle(
        &self,
        request: Command,
        peer: SocketAddr,
    ) -> Pin<Box<dyn Future<Output = Command> + Send + '_>>;
    fn disconnected(&self, _peer: SocketAddr) {}
}

async fn shutdown_signal(shutdown: &mut watch::Receiver<bool>) {
    loop {
        if *shutdown.borrow_and_update() {
            return;
        }
        // Closing the controlling watch sender is also a shutdown request.
        if shutdown.changed().await.is_err() {
            return;
        }
    }
}

struct DisconnectGuard {
    handler: Arc<dyn Handler>,
    peer: SocketAddr,
}

impl Drop for DisconnectGuard {
    fn drop(&mut self) {
        if std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            self.handler.disconnected(self.peer)
        }))
        .is_err()
        {
            tracing::error!(peer = %self.peer, "disconnect callback panicked");
        }
    }
}

fn request_finished(result: Option<std::result::Result<Result<()>, JoinError>>) -> Result<()> {
    match result {
        Some(Ok(result)) => result,
        Some(Err(error)) => Err(invalid(format!("request task failed: {error}"))),
        None => Ok(()),
    }
}

async fn serve_connection(
    stream: TcpStream,
    peer: SocketAddr,
    handler: Arc<dyn Handler>,
    mut shutdown: watch::Receiver<bool>,
    capacity: Arc<Semaphore>,
) -> Result<()> {
    let _disconnected = DisconnectGuard {
        handler: handler.clone(),
        peer,
    };
    stream.set_nodelay(true)?;
    let (mut reader, writer) = stream.into_split();
    let writer = Arc::new(Mutex::new(writer));
    let mut requests = JoinSet::new();
    let mut oneway_requests = JoinSet::new();
    let result = async {
        loop {
            // Backpressure is applied before decoding another large frame.
            while requests.len() + oneway_requests.len() >= MAX_IN_FLIGHT_PER_CONNECTION {
                tokio::select! {
                    biased;
                    _ = shutdown_signal(&mut shutdown) => return Ok(()),
                    result = requests.join_next(), if !requests.is_empty() => request_finished(result)?,
                    result = oneway_requests.join_next(), if !oneway_requests.is_empty() => request_finished(result)?,
                }
            }
            let request = {
                let read = read_command(&mut reader);
                tokio::pin!(read);
                loop {
                    tokio::select! {
                        biased;
                        _ = shutdown_signal(&mut shutdown) => return Ok(()),
                        result = requests.join_next(), if !requests.is_empty() => request_finished(result)?,
                        result = oneway_requests.join_next(), if !oneway_requests.is_empty() => request_finished(result)?,
                        result = &mut read => break result?,
                    }
                }
            };
            let Some(request) = request else { return Ok(()); };
            if request.flag & RESPONSE_FLAG != 0 {
                return Err(invalid("unsolicited response on server connection"));
            }
            let acquire = capacity.clone().acquire_owned();
            tokio::pin!(acquire);
            let permit = loop {
                tokio::select! {
                    biased;
                    _ = shutdown_signal(&mut shutdown) => return Ok(()),
                    result = requests.join_next(), if !requests.is_empty() => request_finished(result)?,
                    result = oneway_requests.join_next(), if !oneway_requests.is_empty() => request_finished(result)?,
                    permit = &mut acquire => break permit.map_err(|_| invalid("request capacity closed"))?,
                }
            };
            let handler = handler.clone();
            let writer = writer.clone();
            let oneway = request.flag & ONEWAY_FLAG != 0;
            let task = async move {
                let _permit = permit;
                let opaque = request.opaque;
                let serialize_type = request.serialize_type;
                let mut response = handler.handle(request, peer).await;
                if !oneway {
                    // Correlation is enforced by the transport, regardless of the handler's response.
                    response.opaque = opaque;
                    response.serialize_type = serialize_type;
                    response.flag = (response.flag | RESPONSE_FLAG) & !ONEWAY_FLAG;
                    let mut writer = writer.lock().await;
                    write_command(&mut *writer, &response).await?;
                }
                Ok(())
            };
            if oneway {
                oneway_requests.spawn(task);
            } else {
                requests.spawn(task);
            }
        }
    }.await;
    requests.abort_all();
    while let Some(completed) = requests.join_next().await {
        match completed {
            Ok(Err(error)) => {
                tracing::warn!(%peer, %error, "request failed while closing connection")
            }
            Err(error) if !error.is_cancelled() => {
                tracing::error!(%peer, %error, "request panicked while closing connection")
            }
            _ => {}
        }
    }
    // EOF must not discard accepted fire-and-forget writes; drain with a bounded grace period.
    let drained = tokio::time::timeout(Duration::from_secs(3), async {
        while let Some(completed) = oneway_requests.join_next().await {
            if let Err(error) = request_finished(Some(completed)) {
                tracing::warn!(%peer, %error, "oneway request failed while closing connection");
            }
        }
    })
    .await;
    if drained.is_err() {
        tracing::warn!(%peer, "oneway shutdown grace period expired");
        oneway_requests.abort_all();
        while oneway_requests.join_next().await.is_some() {}
    }
    result
}

/// Handlers must not block Tokio workers; oneway writes receive a bounded shutdown grace period.
pub async fn serve(
    listener: TcpListener,
    handler: Arc<dyn Handler>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let (stop, stopped) = watch::channel(false);
    let capacity = Arc::new(Semaphore::new(MAX_IN_FLIGHT_REQUESTS));
    let mut connections = JoinSet::new();
    let result = loop {
        tokio::select! {
            biased;
            _ = shutdown_signal(&mut shutdown) => break Ok(()),
            result = connections.join_next(), if !connections.is_empty() => {
                log_connection_result(result);
            }
            accepted = listener.accept(), if connections.len() < MAX_CONNECTIONS => {
                match accepted {
                    Ok((stream, peer)) => {
                        let handler = handler.clone();
                        let shutdown = stopped.clone();
                        let capacity = capacity.clone();
                        connections.spawn(async move {
                            let result = serve_connection(stream, peer, handler, shutdown, capacity).await;
                            if let Err(error) = &result {
                                tracing::warn!(%peer, %error, "remoting connection failed");
                            }
                            result
                        });
                    }
                    Err(error) => {
                        tracing::error!(%error, "remoting accept failed");
                        break Err(Error::Io(error));
                    }
                }
            }
        }
    };
    let _ = stop.send(true);
    while let Some(result) = connections.join_next().await {
        log_connection_result(Some(result));
    }
    result
}

fn log_connection_result(result: Option<std::result::Result<Result<()>, JoinError>>) {
    if let Some(Err(error)) = result {
        tracing::error!(%error, "remoting connection task failed");
    }
}

pub async fn invoke(addr: &str, request: Command, timeout: Duration) -> Result<Command> {
    if request.flag & (RESPONSE_FLAG | ONEWAY_FLAG) != 0 {
        return Err(invalid("invoke requires a two-way request"));
    }
    tokio::time::timeout(timeout, async {
        let mut stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        write_command(&mut stream, &request).await?;
        let response = read_command(&mut stream).await?.ok_or_else(|| {
            Error::Io(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "peer closed before RPC response",
            ))
        })?;
        if response.flag & RESPONSE_FLAG == 0 || response.flag & ONEWAY_FLAG != 0 {
            return Err(invalid("RPC response has invalid flags"));
        }
        if response.opaque != request.opaque {
            return Err(invalid("RPC response opaque mismatch"));
        }
        Ok(response)
    })
    .await
    .map_err(|_| Error::Io(io::Error::new(io::ErrorKind::TimedOut, "RPC timed out")))?
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;
    use tokio::sync::Notify;

    fn sample(serialize_type: SerializeType) -> Command {
        Command {
            code: 105,
            language: "JAVA".into(),
            version: 123,
            opaque: 0x0102_0304,
            flag: 1,
            remark: Some("ok".into()),
            ext_fields: BTreeMap::from([("k".into(), "v".into())]),
            body: vec![0xaa, 0xbb],
            serialize_type,
        }
    }

    fn json_frame(header: &[u8], body: &[u8]) -> Vec<u8> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&((4 + header.len() + body.len()) as u32).to_be_bytes());
        bytes.extend_from_slice(&(header.len() as u32).to_be_bytes());
        bytes.extend_from_slice(header);
        bytes.extend_from_slice(body);
        bytes
    }

    #[test]
    fn command_helpers_response_and_unique_opaque() {
        let request = Command::request(10)
            .with_ext("queueId", 3)
            .with_ext("bad", "oops")
            .with_body(vec![1]);
        assert_eq!(request.required("queueId").unwrap(), "3");
        assert_eq!(request.field::<i32>("queueId").unwrap(), 3);
        assert!(request.required("missing").is_err());
        assert!(request.field::<u32>("bad").is_err());
        let mut request = request;
        request.serialize_type = SerializeType::RocketMq;
        let response = Command::response(&request, 0);
        assert_eq!(response.opaque, request.opaque);
        assert_eq!(response.flag, RESPONSE_FLAG);
        assert_eq!(response.serialize_type, SerializeType::RocketMq);
        assert_eq!(request.version, PROTOCOL_VERSION);
        assert_eq!(response.version, PROTOCOL_VERSION);
        let handles: Vec<_> = (0..8)
            .map(|_| {
                std::thread::spawn(|| {
                    (0..1000)
                        .map(|_| Command::request(0).opaque)
                        .collect::<Vec<_>>()
                })
            })
            .collect();
        let ids: std::collections::BTreeSet<_> = handles
            .into_iter()
            .flat_map(|handle| handle.join().unwrap())
            .collect();
        assert_eq!(ids.len(), 8000);
    }

    #[test]
    fn rocketmq_header_matches_java_big_endian_fixture() {
        // A fixed Java-format fixture prevents matching encoder/decoder bugs from passing.
        let expected = vec![
            0, 0, 0, 37, 1, 0, 0, 31, 0, 105, 0, 0, 123, 1, 2, 3, 4, 0, 0, 0, 1, 0, 0, 0, 2, b'o',
            b'k', 0, 0, 0, 8, 0, 1, b'k', 0, 0, 0, 1, b'v', 0xaa, 0xbb,
        ];
        let command = sample(SerializeType::RocketMq);
        assert_eq!(encode(&command).unwrap(), expected);
        assert_eq!(decode(&expected).unwrap(), command);
    }

    #[test]
    fn java_json_header_names_null_fields_and_unknown_metadata() {
        let header = br#"{"code":105,"language":"JAVA","version":123,"opaque":16909060,"flag":1,"remark":"ok","extFields":{"k":"v"},"serializeTypeCurrentRPC":"JSON","suspended":false}"#;
        assert_eq!(
            decode(&json_frame(header, &[0xaa, 0xbb])).unwrap(),
            sample(SerializeType::Json)
        );
        let bytes = encode(&sample(SerializeType::Json)).unwrap();
        assert_eq!(bytes[4], 0);
        let size = u32::from_be_bytes(bytes[4..8].try_into().unwrap()) as usize;
        let value: serde_json::Value = serde_json::from_slice(&bytes[8..8 + size]).unwrap();
        assert_eq!(value["extFields"]["k"], "v");
        assert!(value.get("ext_fields").is_none());
        assert!(value.get("body").is_none());
        let header = br#"{"code":0,"language":"JAVA","version":0,"opaque":0,"flag":0,"extFields":null,"remark":null}"#;
        let command = decode(&json_frame(header, &[])).unwrap();
        assert!(command.ext_fields.is_empty());
        assert!(command.remark.is_none());
        let header = br#"{"code":0,"language":"JAVA","version":0,"opaque":0,"flag":0}"#;
        assert!(
            decode(&json_frame(header, &[]))
                .unwrap()
                .ext_fields
                .is_empty()
        );
    }

    #[test]
    fn language_codes_and_header_roundtrips() {
        for (index, language) in LANGUAGES.iter().enumerate() {
            for serialize_type in [SerializeType::Json, SerializeType::RocketMq] {
                let command = Command {
                    language: language.to_string(),
                    remark: Some("成功\n\u{0000}".into()),
                    ext_fields: BTreeMap::from([
                        ("主题".into(), "测试".into()),
                        ("empty".into(), "".into()),
                    ]),
                    ..sample(serialize_type)
                };
                let bytes = encode(&command).unwrap();
                if serialize_type == SerializeType::RocketMq {
                    assert_eq!(bytes[10], index as u8);
                }
                assert_eq!(decode(&bytes).unwrap(), command);
            }
        }
        let mut command = sample(SerializeType::RocketMq);
        command.remark = Some(String::new());
        let decoded = decode(&encode(&command).unwrap()).unwrap();
        assert!(decoded.remark.is_none()); // Java binary encoding conflates empty and null.
        command.language = "UNKNOWN".into();
        assert!(encode(&command).is_err());
    }

    #[test]
    fn signed_short_boundaries() {
        for value in [i16::MIN as i32, i16::MAX as i32] {
            let command = Command {
                code: value,
                version: value,
                ..sample(SerializeType::RocketMq)
            };
            assert_eq!(decode(&encode(&command).unwrap()).unwrap(), command);
        }
        for value in [i16::MIN as i32 - 1, i16::MAX as i32 + 1] {
            let mut command = sample(SerializeType::RocketMq);
            command.code = value;
            assert!(encode(&command).is_err());
            command.code = 0;
            command.version = value;
            assert!(encode(&command).is_err());
            command.serialize_type = SerializeType::Json;
            assert_eq!(decode(&encode(&command).unwrap()).unwrap(), command);
        }
        let mut command = sample(SerializeType::RocketMq);
        command.ext_fields = BTreeMap::from([("k".repeat(32767), "v".into())]);
        assert_eq!(decode(&encode(&command).unwrap()).unwrap(), command);
        command.ext_fields = BTreeMap::from([("k".repeat(32768), "v".into())]);
        assert!(encode(&command).is_err());
    }

    #[test]
    fn invalid_json_types_syntax_ranges_and_languages() {
        for header in [
            r#"{}"#,
            "null",
            "[]",
            "{",
            "{\"code\":NaN}",
            r#"{"code":1,"language":"JAVA","version":0,"opaque":0,"flag":0,"extFields":{"a":1}}"#,
            r#"{"code":2147483648,"language":"JAVA","version":0,"opaque":0,"flag":0}"#,
            r#"{"code":1,"language":"JAVA","version":0,"opaque":"0","flag":0}"#,
            r#"{"code":1,"language":"BAD","version":0,"opaque":0,"flag":0}"#,
            r#"{"code":1,"language":"JAVA","version":0,"opaque":0,"flag":0} garbage"#,
        ] {
            assert!(
                decode(&json_frame(header.as_bytes(), &[])).is_err(),
                "{header}"
            );
        }
        assert!(decode(&json_frame(&[0xff], &[])).is_err());
    }

    #[test]
    fn frame_truncations_trailing_bytes_invalid_markers_and_lengths() {
        for serialize_type in [SerializeType::Json, SerializeType::RocketMq] {
            let bytes = encode(&sample(serialize_type)).unwrap();
            for size in 0..bytes.len() {
                assert!(decode(&bytes[..size]).is_err(), "size {size}");
            }
            assert!(decode(&[bytes.clone(), vec![0]].concat()).is_err());
            for size in [0u32, 3, MAX_FRAME_SIZE as u32 + 1, 0x8000_0000, u32::MAX] {
                let mut bad = bytes.clone();
                bad[..4].copy_from_slice(&size.to_be_bytes());
                assert!(decode(&bad).is_err());
            }
            for kind in [2, 127, 255] {
                let mut bad = bytes.clone();
                bad[4] = kind;
                assert!(matches!(decode(&bad), Err(Error::Unsupported(_))));
            }
            for length in [0u32, 0x00ff_ffff] {
                let mut bad = bytes.clone();
                bad[5..8].copy_from_slice(&length.to_be_bytes()[1..]);
                assert!(decode(&bad).is_err());
            }
        }
    }

    #[test]
    fn binary_header_negative_oversized_lengths_and_invalid_utf8() {
        let bytes = encode(&sample(SerializeType::RocketMq)).unwrap();
        for offset in [21, 27, 34] {
            for length in [-1i32, i32::MAX] {
                let mut bad = bytes.clone();
                bad[offset..offset + 4].copy_from_slice(&length.to_be_bytes());
                assert!(decode(&bad).is_err(), "offset {offset}");
            }
        }
        for offset in [10, 25, 33, 38] {
            let mut bad = bytes.clone();
            bad[offset] = 255;
            assert!(decode(&bad).is_err(), "offset {offset}");
        }
        let mut bad = bytes.clone();
        bad[31..33].copy_from_slice(&0x8000u16.to_be_bytes());
        assert!(decode(&bad).is_err());
        let mut bad = bytes;
        bad[30] = 7; // extLen is one byte short of a complete entry.
        assert!(decode(&bad).is_err());
    }

    #[test]
    fn max_frame_and_json_escaping_budget() {
        let mut command = Command::request(0);
        command.serialize_type = SerializeType::RocketMq;
        command.body = vec![0; MAX_FRAME_SIZE - 4 - 21];
        let bytes = encode(&command).unwrap();
        assert_eq!(bytes.len(), MAX_FRAME_SIZE + 4);
        assert_eq!(decode(&bytes).unwrap(), command);
        command.body.push(0);
        assert!(encode(&command).is_err());
        command.serialize_type = SerializeType::Json;
        command.body.clear();
        command.remark = Some("\0".repeat(MAX_FRAME_SIZE / 6));
        assert!(encode(&command).is_err());
    }

    #[test]
    fn arbitrary_and_mutated_frames_never_panic() {
        let mut seed = 1u32;
        for size in 0..512 {
            let mut bytes = vec![0; size + 4];
            bytes[..4].copy_from_slice(&(size as u32).to_be_bytes());
            for byte in &mut bytes[4..] {
                seed = seed.wrapping_mul(1664525).wrapping_add(1013904223);
                *byte = (seed >> 24) as u8;
            }
            let _ = decode(&bytes);
        }
        for serialization in [SerializeType::Json, SerializeType::RocketMq] {
            let original = encode(&sample(serialization)).unwrap();
            for index in 0..original.len() {
                for byte in [0, 127, 128, 255] {
                    let mut bytes = original.clone();
                    bytes[index] = byte;
                    let _ = decode(&bytes);
                }
            }
        }
    }

    #[tokio::test]
    async fn fragmented_and_coalesced_reads_and_clean_eof() {
        let first = sample(SerializeType::Json);
        let second = sample(SerializeType::RocketMq);
        let bytes = [encode(&first).unwrap(), encode(&second).unwrap()].concat();
        let (mut writer, mut reader) = tokio::io::duplex(7);
        let sending = tokio::spawn(async move {
            for byte in bytes {
                writer.write_all(&[byte]).await.unwrap();
                tokio::task::yield_now().await;
            }
        });
        assert_eq!(read_command(&mut reader).await.unwrap(), Some(first));
        assert_eq!(read_command(&mut reader).await.unwrap(), Some(second));
        assert!(read_command(&mut reader).await.unwrap().is_none());
        sending.await.unwrap();
    }

    #[tokio::test]
    async fn partial_eof_is_an_error_and_large_prefix_is_rejected_without_body() {
        let bytes = encode(&sample(SerializeType::RocketMq)).unwrap();
        assert!(read_command(&mut &[][..]).await.unwrap().is_none());
        for length in 1..bytes.len() {
            assert!(
                read_command(&mut &bytes[..length]).await.is_err(),
                "length {length}"
            );
        }
        for size in [0u32, 3, MAX_FRAME_SIZE as u32 + 1, u32::MAX] {
            assert!(matches!(
                read_command(&mut &size.to_be_bytes()[..]).await,
                Err(Error::Invalid(_))
            ));
        }
        let (mut writer, mut reader) = tokio::io::duplex(1024);
        let command = sample(SerializeType::RocketMq);
        write_command(&mut writer, &command).await.unwrap();
        assert_eq!(read_command(&mut reader).await.unwrap(), Some(command));
    }

    #[derive(Default)]
    struct TestHandler {
        started: Notify,
        release: Notify,
        oneway: Notify,
        active: AtomicUsize,
        disconnected: AtomicUsize,
    }

    struct ActiveGuard<'a>(&'a AtomicUsize);
    impl Drop for ActiveGuard<'_> {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }

    impl Handler for TestHandler {
        fn handle(
            &self,
            request: Command,
            _peer: SocketAddr,
        ) -> Pin<Box<dyn Future<Output = Command> + Send + '_>> {
            Box::pin(async move {
                self.active.fetch_add(1, Ordering::SeqCst);
                let _guard = ActiveGuard(&self.active);
                match request.code {
                    1 => {
                        self.started.notify_one();
                        self.release.notified().await;
                    }
                    3 => self.oneway.notify_one(),
                    9 => panic!("intentional handler panic"),
                    _ => {}
                }
                // Intentionally wrong routing: transport must correct it.
                Command::request(request.code).with_body(request.body)
            })
        }
        fn disconnected(&self, _peer: SocketAddr) {
            self.disconnected.fetch_add(1, Ordering::SeqCst);
        }
    }

    async fn start_server(
        handler: Arc<TestHandler>,
    ) -> (
        SocketAddr,
        watch::Sender<bool>,
        tokio::task::JoinHandle<Result<()>>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (stop, shutdown) = watch::channel(false);
        let server = tokio::spawn(serve(listener, handler, shutdown));
        (addr, stop, server)
    }

    async fn within<T>(future: impl Future<Output = T>) -> T {
        tokio::time::timeout(Duration::from_secs(5), future)
            .await
            .expect("test timed out")
    }

    #[tokio::test]
    async fn multiplexes_long_poll_oneway_and_mixed_serializations() {
        let handler = Arc::new(TestHandler::default());
        let (addr, stop, server) = start_server(handler.clone()).await;
        let mut client = TcpStream::connect(addr).await.unwrap();
        let first = Command::request(1);
        write_command(&mut client, &first).await.unwrap();
        within(handler.started.notified()).await;
        let mut oneway = Command::request(3);
        oneway.flag = ONEWAY_FLAG;
        let mut fast = Command::request(2).with_body(vec![1, 2, 3]);
        fast.serialize_type = SerializeType::RocketMq;
        let bytes = [encode(&oneway).unwrap(), encode(&fast).unwrap()].concat();
        client.write_all(&bytes).await.unwrap();
        within(handler.oneway.notified()).await;
        let response = within(read_command(&mut client)).await.unwrap().unwrap();
        assert_eq!(response.opaque, fast.opaque);
        assert_eq!(response.flag, RESPONSE_FLAG);
        assert_eq!(response.serialize_type, SerializeType::RocketMq);
        assert_eq!(response.body, fast.body);
        assert_eq!(handler.active.load(Ordering::SeqCst), 1);
        handler.release.notify_one();
        let response = within(read_command(&mut client)).await.unwrap().unwrap();
        assert_eq!(response.opaque, first.opaque);
        assert_eq!(response.serialize_type, SerializeType::Json);
        stop.send(true).unwrap();
        within(server).await.unwrap().unwrap();
        assert_eq!(handler.active.load(Ordering::SeqCst), 0);
        assert_eq!(handler.disconnected.load(Ordering::SeqCst), 1);
        assert!(within(read_command(&mut client)).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn completed_request_does_not_cancel_a_partial_frame_read() {
        let handler = Arc::new(TestHandler::default());
        let (addr, stop, server) = start_server(handler.clone()).await;
        let mut client = TcpStream::connect(addr).await.unwrap();
        let first = Command::request(1);
        write_command(&mut client, &first).await.unwrap();
        within(handler.started.notified()).await;
        let second = Command::request(2);
        let bytes = encode(&second).unwrap();
        client.write_all(&bytes[..6]).await.unwrap();
        handler.release.notify_one();
        assert_eq!(
            within(read_command(&mut client))
                .await
                .unwrap()
                .unwrap()
                .opaque,
            first.opaque
        );
        client.write_all(&bytes[6..]).await.unwrap();
        assert_eq!(
            within(read_command(&mut client))
                .await
                .unwrap()
                .unwrap()
                .opaque,
            second.opaque
        );
        stop.send(true).unwrap();
        within(server).await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn shutdown_cancels_and_reaps_long_poll_and_idle_connections() {
        let handler = Arc::new(TestHandler::default());
        let (addr, stop, server) = start_server(handler.clone()).await;
        let mut client = TcpStream::connect(addr).await.unwrap();
        write_command(&mut client, &Command::request(1))
            .await
            .unwrap();
        within(handler.started.notified()).await;
        let mut idle = TcpStream::connect(addr).await.unwrap();
        write_command(&mut idle, &Command::request(2))
            .await
            .unwrap();
        within(read_command(&mut idle)).await.unwrap().unwrap();
        stop.send(false).unwrap(); // False updates must not stop a server.
        write_command(&mut idle, &Command::request(2))
            .await
            .unwrap();
        within(read_command(&mut idle)).await.unwrap().unwrap();
        assert_eq!(handler.active.load(Ordering::SeqCst), 1);
        stop.send(true).unwrap();
        within(server).await.unwrap().unwrap();
        assert_eq!(handler.active.load(Ordering::SeqCst), 0);
        assert_eq!(handler.disconnected.load(Ordering::SeqCst), 2);
        assert!(within(read_command(&mut client)).await.unwrap().is_none());
        assert!(within(read_command(&mut idle)).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn peer_disconnect_cancels_handler_and_panics_are_isolated() {
        let handler = Arc::new(TestHandler::default());
        let (addr, stop, server) = start_server(handler.clone()).await;
        let mut client = TcpStream::connect(addr).await.unwrap();
        write_command(&mut client, &Command::request(1))
            .await
            .unwrap();
        within(handler.started.notified()).await;
        drop(client);
        within(async {
            while handler.disconnected.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert_eq!(handler.active.load(Ordering::SeqCst), 0);
        let mut client = TcpStream::connect(addr).await.unwrap();
        write_command(&mut client, &Command::request(9))
            .await
            .unwrap();
        assert!(within(read_command(&mut client)).await.unwrap().is_none());
        let response = invoke(
            &addr.to_string(),
            Command::request(2),
            Duration::from_secs(2),
        )
        .await
        .unwrap();
        assert_eq!(response.code, 2);
        stop.send(true).unwrap();
        within(server).await.unwrap().unwrap();
        assert_eq!(handler.active.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn shutdown_already_true_or_sender_dropped() {
        for initially_stopped in [false, true] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let (sender, receiver) = watch::channel(initially_stopped);
            if !initially_stopped {
                drop(sender);
            }
            within(serve(listener, Arc::new(TestHandler::default()), receiver))
                .await
                .unwrap();
        }
    }

    #[tokio::test]
    async fn connection_in_flight_limit_applies_backpressure_and_stops() {
        let handler = Arc::new(TestHandler::default());
        let (addr, stop, server) = start_server(handler.clone()).await;
        let mut client = TcpStream::connect(addr).await.unwrap();
        for _ in 0..MAX_IN_FLIGHT_PER_CONNECTION {
            write_command(&mut client, &Command::request(1))
                .await
                .unwrap();
            within(handler.started.notified()).await;
        }
        assert_eq!(
            handler.active.load(Ordering::SeqCst),
            MAX_IN_FLIGHT_PER_CONNECTION
        );
        let next = Command::request(2);
        write_command(&mut client, &next).await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(30), read_command(&mut client))
                .await
                .is_err()
        );
        handler.release.notify_one();
        let released = within(read_command(&mut client)).await.unwrap().unwrap();
        assert_eq!(released.code, 1);
        let response = within(read_command(&mut client)).await.unwrap().unwrap();
        assert_eq!(response.opaque, next.opaque);
        stop.send(true).unwrap();
        within(server).await.unwrap().unwrap();
        assert_eq!(handler.active.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn server_wide_in_flight_limit_and_shutdown_while_waiting_for_capacity() {
        let handler = Arc::new(TestHandler::default());
        let (addr, stop, server) = start_server(handler.clone()).await;
        let mut clients = Vec::new();
        for _ in 0..MAX_IN_FLIGHT_REQUESTS / MAX_IN_FLIGHT_PER_CONNECTION {
            let mut client = TcpStream::connect(addr).await.unwrap();
            for _ in 0..MAX_IN_FLIGHT_PER_CONNECTION {
                write_command(&mut client, &Command::request(1))
                    .await
                    .unwrap();
                within(handler.started.notified()).await;
            }
            clients.push(client);
        }
        assert_eq!(
            handler.active.load(Ordering::SeqCst),
            MAX_IN_FLIGHT_REQUESTS
        );
        let mut extra = TcpStream::connect(addr).await.unwrap();
        write_command(&mut extra, &Command::request(2))
            .await
            .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(30), read_command(&mut extra))
                .await
                .is_err()
        );
        stop.send(true).unwrap();
        within(server).await.unwrap().unwrap();
        assert_eq!(handler.active.load(Ordering::SeqCst), 0);
        assert!(within(read_command(&mut extra)).await.unwrap().is_none());
    }

    #[tokio::test]
    async fn connection_limit_reopens_capacity_after_disconnect() {
        let handler = Arc::new(TestHandler::default());
        let (addr, stop, server) = start_server(handler.clone()).await;
        let mut clients = Vec::new();
        for _ in 0..MAX_CONNECTIONS {
            let mut client = TcpStream::connect(addr).await.unwrap();
            write_command(&mut client, &Command::request(2))
                .await
                .unwrap();
            within(read_command(&mut client)).await.unwrap().unwrap();
            clients.push(client);
        }
        let mut extra = TcpStream::connect(addr).await.unwrap();
        let request = Command::request(2);
        write_command(&mut extra, &request).await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(30), read_command(&mut extra))
                .await
                .is_err()
        );
        drop(clients.pop());
        let response = within(read_command(&mut extra)).await.unwrap().unwrap();
        assert_eq!(response.opaque, request.opaque);
        stop.send(true).unwrap();
        within(server).await.unwrap().unwrap();
        assert_eq!(
            handler.disconnected.load(Ordering::SeqCst),
            MAX_CONNECTIONS + 1
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_large_responses_never_interleave_frame_bytes() {
        let handler = Arc::new(TestHandler::default());
        let (addr, stop, server) = start_server(handler).await;
        let mut client = TcpStream::connect(addr).await.unwrap();
        let mut expected = BTreeMap::new();
        for value in 0u8..32 {
            let mut request = Command::request(2).with_body(vec![value; 64 * 1024]);
            if value % 2 == 0 {
                request.serialize_type = SerializeType::RocketMq;
            }
            expected.insert(
                request.opaque,
                (request.body.clone(), request.serialize_type),
            );
            write_command(&mut client, &request).await.unwrap();
        }
        while !expected.is_empty() {
            let response = within(read_command(&mut client)).await.unwrap().unwrap();
            let (body, serialization) = expected.remove(&response.opaque).unwrap();
            assert_eq!(response.body, body);
            assert_eq!(response.serialize_type, serialization);
        }
        stop.send(true).unwrap();
        within(server).await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn malformed_connection_is_closed_without_stopping_server() {
        let handler = Arc::new(TestHandler::default());
        let (addr, stop, server) = start_server(handler.clone()).await;
        let mut client = TcpStream::connect(addr).await.unwrap();
        client.write_all(&u32::MAX.to_be_bytes()).await.unwrap();
        assert!(within(read_command(&mut client)).await.unwrap().is_none());
        let response = invoke(
            &addr.to_string(),
            Command::request(2),
            Duration::from_secs(2),
        )
        .await
        .unwrap();
        assert_eq!(response.code, 2);
        stop.send(true).unwrap();
        within(server).await.unwrap().unwrap();
        assert_eq!(handler.disconnected.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn invoke_checks_response_opaque_flags_and_eof() {
        for scenario in 0..4 {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let server = tokio::spawn(async move {
                let (mut stream, _) = listener.accept().await.unwrap();
                let request = read_command(&mut stream).await.unwrap().unwrap();
                if scenario == 3 {
                    return;
                }
                let mut response = Command::response(&request, 0);
                match scenario {
                    0 => response.opaque = response.opaque.wrapping_add(1),
                    1 => response.flag = 0,
                    2 => response.flag |= ONEWAY_FLAG,
                    _ => {}
                }
                write_command(&mut stream, &response).await.unwrap();
            });
            let result = invoke(
                &addr.to_string(),
                Command::request(2),
                Duration::from_secs(2),
            )
            .await;
            assert!(result.is_err(), "scenario {scenario}");
            within(server).await.unwrap();
        }
    }

    #[tokio::test]
    async fn invoke_has_whole_rpc_deadline_and_rejects_oneway() {
        let handler = Arc::new(TestHandler::default());
        let (addr, stop, server) = start_server(handler).await;
        let result = invoke(
            &addr.to_string(),
            Command::request(1),
            Duration::from_millis(50),
        )
        .await;
        assert!(matches!(result, Err(Error::Io(error)) if error.kind() == io::ErrorKind::TimedOut));
        let mut oneway = Command::request(3);
        oneway.flag = ONEWAY_FLAG;
        assert!(matches!(
            invoke(&addr.to_string(), oneway, Duration::from_secs(1)).await,
            Err(Error::Invalid(_))
        ));
        stop.send(true).unwrap();
        within(server).await.unwrap().unwrap();
    }
}
