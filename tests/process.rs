use std::io::{BufRead, BufReader};
use std::net::{SocketAddr, TcpListener};
use std::path::Path;
use std::process::{Child, Command as ProcessCommand, Stdio};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::time::{Duration, Instant};

use rocketmq_rs::common::{Message, PROPERTY_POP_CK, now_millis};
use rocketmq_rs::remoting::{self, Command};
use serde_json::Value;
use tempfile::TempDir;

const TIMEOUT: Duration = Duration::from_secs(10);
const STANDALONE: &[&str] = &[
    "standalone",
    "--namesrv-listen",
    "127.0.0.1:0",
    "--broker-listen",
    "127.0.0.1:0",
];

struct Process(Child);

impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

// tracing colors use CSI sequences, including around field names.
fn strip_ansi(line: &str) -> String {
    let mut chars = line.chars();
    let mut clean = String::new();
    while let Some(ch) = chars.next() {
        if ch == '\x1b' && chars.next() == Some('[') {
            for ch in chars.by_ref() {
                if ('@'..='~').contains(&ch) {
                    break;
                }
            }
        } else {
            clean.push(ch);
        }
    }
    clean
}

fn spawn(dir: &Path, args: &[&str]) -> (Process, Receiver<String>) {
    let mut child = Process(
        ProcessCommand::new(env!("CARGO_BIN_EXE_rocketmq"))
            .args(args)
            .arg("--data-dir")
            .arg(dir)
            .env("RUST_LOG", "info")
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap(),
    );
    let stderr = child.0.stderr.take().unwrap();
    let (sender, receiver) = mpsc::channel();
    std::thread::spawn(move || {
        for line in BufReader::new(stderr).lines().map_while(Result::ok) {
            // Keep draining after the startup receiver is dropped.
            let _ = sender.send(strip_ansi(&line));
        }
    });
    (child, receiver)
}

async fn rpc(addr: &str, request: Command) -> Command {
    let response = remoting::invoke(addr, request, TIMEOUT).await.unwrap();
    assert_eq!(response.code, 0, "{:?}", response.remark);
    response
}

async fn start(dir: &Path) -> (Process, String) {
    let (child, lines) = spawn(dir, STANDALONE);
    let deadline = Instant::now() + TIMEOUT;
    let mut log = String::new();
    loop {
        let line = lines
            .recv_timeout(deadline.saturating_duration_since(Instant::now()))
            .unwrap_or_else(|error| panic!("broker startup: {error}\n{log}"));
        log.push_str(&line);
        log.push('\n');
        if line.contains("service=\"broker\"") {
            let field = line
                .split_whitespace()
                .find_map(|s| s.strip_prefix("listen="))
                .unwrap();
            let address: SocketAddr = field.parse().unwrap();
            assert_ne!(address.port(), 0);
            let address = address.to_string();
            // Bound precedes Store::open; require a successful RPC, not a sleep.
            rpc(&address, Command::request(28)).await;
            return (child, address);
        }
    }
}

fn expect_failure(dir: &Path, args: &[&str], expected: &str) {
    let (mut child, lines) = spawn(dir, args);
    let deadline = Instant::now() + TIMEOUT;
    let mut log = String::new();
    loop {
        match lines.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
            Ok(line) => {
                log.push_str(&line);
                log.push('\n');
            }
            Err(RecvTimeoutError::Disconnected) => break,
            Err(error) => panic!("child exit timeout: {error}\n{log}"),
        }
    }
    let status = loop {
        if let Some(status) = child.0.try_wait().unwrap() {
            break status;
        }
        assert!(Instant::now() < deadline, "child did not exit: {log}");
        std::thread::yield_now();
    };
    assert!(
        status.code().is_some_and(|code| code != 0),
        "{status}: {log}"
    );
    assert!(log.contains(expected), "{log}");
}

fn send(body: &[u8]) -> Command {
    Command::request(10)
        .with_ext("producerGroup", "test-producer")
        .with_ext("topic", "ProcessTopic")
        .with_ext("defaultTopic", "TBW102")
        .with_ext("defaultTopicQueueNums", 4)
        .with_ext("queueId", 0)
        .with_ext("sysFlag", 0)
        .with_ext("bornTimestamp", now_millis())
        .with_ext("flag", 0)
        .with_ext("properties", "TAGS\x01blue\x02KEYS\x01test-key\x02")
        .with_body(body.to_vec())
}

fn offset_request(code: i32) -> Command {
    Command::request(code)
        .with_ext("consumerGroup", "test-consumer")
        .with_ext("topic", "ProcessTopic")
        .with_ext("queueId", 0)
}

async fn pull(addr: &str, offset: u64) -> Message {
    let request = offset_request(11)
        .with_ext("queueOffset", offset)
        .with_ext("maxMsgNums", 1)
        .with_ext("sysFlag", 4)
        .with_ext("commitOffset", 0)
        .with_ext("suspendTimeoutMillis", 0)
        .with_ext("subVersion", 0)
        .with_ext("subscription", "*")
        .with_ext("expressionType", "TAG");
    let response = rpc(addr, request).await;
    let (message, consumed) = Message::decode(&response.body).unwrap();
    assert_eq!(consumed, response.body.len());
    message
}

#[tokio::test]
async fn acknowledged_state_survives_sigkill_and_restart() {
    // Directory declared first: process guards always drop before cleanup.
    let dir = TempDir::new().unwrap();
    let (mut broker, address) = start(dir.path()).await;
    let sent = rpc(&address, send(b"before-kill")).await;
    let msg_id = sent.ext_fields["msgId"].clone();
    let offset: u64 = sent.ext_fields["queueOffset"].parse().unwrap();
    assert_eq!(offset, 0);
    let topic_request = || Command::request(351).with_ext("topic", "ProcessTopic");
    let config: Value = serde_json::from_slice(&rpc(&address, topic_request()).await.body).unwrap();
    assert_eq!(config["readQueueNums"], 4);
    assert_eq!(config["writeQueueNums"], 4);
    rpc(&address, offset_request(15).with_ext("commitOffset", 1)).await;
    broker.0.kill().unwrap();
    let status = broker.0.wait().unwrap();
    assert!(!status.success());
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        assert_eq!(status.signal(), Some(9));
    }

    let (_restarted, address) = start(dir.path()).await;
    let recovered: Value =
        serde_json::from_slice(&rpc(&address, topic_request()).await.body).unwrap();
    assert_eq!(recovered, config);
    assert_eq!(
        rpc(&address, offset_request(14)).await.ext_fields["offset"],
        "1"
    );
    let stored = pull(&address, offset).await;
    assert_eq!(stored.body, b"before-kill");
    assert_eq!(stored.properties["TAGS"], "blue");
    assert_eq!(stored.queue_offset, offset);
    // Original storeHost stays in msgId even if restart uses another port.
    assert_eq!(stored.message_id(), msg_id);
    let second = rpc(&address, send(b"after-restart")).await;
    assert_eq!(second.ext_fields["queueOffset"], (offset + 1).to_string());
    assert_ne!(second.ext_fields["msgId"], msg_id);
    assert_eq!(pull(&address, offset + 1).await.body, b"after-restart");
}

fn pop_request(group: &str, invisible_time: i64) -> Command {
    Command::request(200050)
        .with_ext("consumerGroup", group)
        .with_ext("topic", "ProcessTopic")
        .with_ext("queueId", 0)
        .with_ext("maxMsgNums", 2)
        .with_ext("invisibleTime", invisible_time)
        .with_ext("popTimeout", 0)
}

fn records(body: &[u8]) -> Vec<Message> {
    let mut messages = Vec::new();
    let mut rest = body;
    while !rest.is_empty() {
        let (message, consumed) = Message::decode(rest).unwrap();
        rest = &rest[consumed..];
        messages.push(message);
    }
    messages
}

/// Java reconstructs the POP buffer from the revive topic after a crash. This
/// port restores `pop_state.json`: without it an acked message (or one still
/// inside its invisible span) is handed out again after the process is killed.
#[tokio::test]
async fn pop_ack_and_invisible_state_survive_sigkill() {
    let dir = TempDir::new().unwrap();
    let (mut broker, address) = start(dir.path()).await;
    let group = "pop-process-cg";
    let group_config = serde_json::to_vec(&serde_json::json!({"groupName": group})).unwrap();
    rpc(&address, Command::request(200).with_body(group_config)).await;
    for body in [b"p0".as_slice(), b"p1".as_slice(), b"p2".as_slice()] {
        rpc(&address, send(body)).await;
    }
    let first = rpc(&address, pop_request(group, 3_600_000)).await;
    let batch = records(&first.body);
    assert_eq!(batch.len(), 2);
    assert_eq!(batch[0].body, b"p0");
    assert_eq!(batch[1].body, b"p1");
    // Ack only p0. p1 must stay invisible across the restart.
    let ack = Command::request(200051)
        .with_ext("extraInfo", batch[0].properties[PROPERTY_POP_CK].as_str());
    rpc(&address, ack).await;

    broker.0.kill().unwrap();
    let status = broker.0.wait().unwrap();
    assert!(!status.success());
    #[cfg(unix)]
    {
        use std::os::unix::process::ExitStatusExt;
        assert_eq!(status.signal(), Some(9));
    }

    let (_restarted, address) = start(dir.path()).await;
    let second = rpc(&address, pop_request(group, 3_600_000)).await;
    let resumed = records(&second.body);
    // Only the never-delivered p2 may come back.
    assert_eq!(
        resumed.len(),
        1,
        "an acked or still-invisible message was re-delivered after a kill"
    );
    assert_eq!(resumed[0].body, b"p2");
}

#[test]
fn rejected_startup_does_not_touch_data_directory() {
    let occupied = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = occupied.local_addr().unwrap().to_string();
    let mut conflicting = STANDALONE.to_vec();
    *conflicting.last_mut().unwrap() = &address;
    for (args, expected) in [
        (&["broker", "--default-queues", "0"][..], "invalid value"),
        (&conflicting[..], "cannot bind broker"),
    ] {
        let dir = TempDir::new().unwrap();
        let sentinel = dir.path().join("sentinel");
        std::fs::write(&sentinel, b"must remain unchanged").unwrap();
        expect_failure(dir.path(), args, expected);
        assert_eq!(std::fs::read(&sentinel).unwrap(), b"must remain unchanged");
        assert_eq!(std::fs::read_dir(dir.path()).unwrap().count(), 1);
    }
}

#[tokio::test]
async fn duplicate_store_open_fails_without_disrupting_owner() {
    let dir = TempDir::new().unwrap();
    let (mut owner, address) = start(dir.path()).await;
    rpc(&address, send(b"lock-owner")).await;
    // Bypass the NameServer lock; the unused registration target is our own listener.
    let args = ["broker", "--listen", "127.0.0.1:0", "--namesrv", &address];
    expect_failure(
        &dir.path().join("broker"),
        &args,
        "already open by another writer",
    );
    assert!(owner.0.try_wait().unwrap().is_none());
    assert_eq!(pull(&address, 0).await.body, b"lock-owner");
    let sent = rpc(&address, send(b"still-writable")).await;
    assert_eq!(sent.ext_fields["queueOffset"], "1");
    assert_eq!(pull(&address, 1).await.body, b"still-writable");
}
