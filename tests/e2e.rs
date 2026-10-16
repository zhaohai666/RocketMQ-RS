use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use rocketmq_rs::broker::{Broker, BrokerConfig};
use rocketmq_rs::common::{Message, Result, TopicConfig, now_millis};
use rocketmq_rs::namesrv::Namesrv;
use rocketmq_rs::remoting::{self, Command, Handler, SerializeType};
use rocketmq_rs::store::Store;
use serde_json::{Value, json};
use tempfile::TempDir;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task::JoinHandle;

struct Running {
    addr: SocketAddr,
    shutdown: watch::Sender<bool>,
    task: JoinHandle<Result<()>>,
}

impl Running {
    async fn start(listener: TcpListener, handler: Arc<dyn Handler>) -> Self {
        let addr = listener.local_addr().unwrap();
        let (shutdown, receiver) = watch::channel(false);
        let task = tokio::spawn(remoting::serve(listener, handler, receiver));
        Self {
            addr,
            shutdown,
            task,
        }
    }

    async fn request(&self, request: Command) -> Command {
        remoting::invoke(&self.addr.to_string(), request, Duration::from_secs(5))
            .await
            .unwrap()
    }

    async fn stop(self) {
        self.shutdown.send(true).unwrap();
        tokio::time::timeout(Duration::from_secs(5), self.task)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
}

async fn start_broker(dir: &TempDir) -> (Arc<Broker>, Running) {
    start_broker_with(dir, false).await
}

async fn start_broker_with(dir: &TempDir, property_filter: bool) -> (Arc<Broker>, Running) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let store = Arc::new(Store::open(dir.path()).unwrap());
    let broker = Arc::new(
        Broker::new(
            store,
            BrokerConfig {
                advertised_addr: listener.local_addr().unwrap(),
                enable_property_filter: property_filter,
                ..BrokerConfig::default()
            },
        )
        .unwrap(),
    );
    let running = Running::start(listener, broker.clone()).await;
    (broker, running)
}

fn send(topic: &str, body: &[u8]) -> Command {
    Command::request(10)
        .with_ext("producerGroup", "test-producer")
        .with_ext("topic", topic)
        .with_ext("defaultTopic", "TBW102")
        .with_ext("defaultTopicQueueNums", 4)
        .with_ext("queueId", 0)
        .with_ext("sysFlag", 0)
        .with_ext("bornTimestamp", now_millis())
        .with_ext("flag", 0)
        .with_ext("properties", "TAGS\x01blue\x02KEYS\x01test-key\x02")
        .with_body(body.to_vec())
}

fn pull(topic: &str, offset: u64) -> Command {
    Command::request(11)
        .with_ext("consumerGroup", "test-consumer")
        .with_ext("topic", topic)
        .with_ext("queueId", 0)
        .with_ext("queueOffset", offset)
        .with_ext("maxMsgNums", 32)
        .with_ext("sysFlag", 4)
        .with_ext("commitOffset", 0)
        .with_ext("suspendTimeoutMillis", 0)
        .with_ext("subVersion", 0)
        .with_ext("subscription", "*")
        .with_ext("expressionType", "TAG")
}

fn messages(body: &[u8]) -> Vec<Message> {
    let mut result = Vec::new();
    let mut position = 0;
    while position < body.len() {
        let (message, consumed) = Message::decode(&body[position..]).unwrap();
        position += consumed;
        result.push(message);
    }
    result
}

fn send_with(topic: &str, body: &[u8], properties: &str) -> Command {
    send(topic, body).with_ext("properties", properties)
}

async fn send_heartbeat(stream: &mut TcpStream, group: &str, subs: Value) -> Command {
    let request = Command::request(34).with_body(
        serde_json::to_vec(&json!({
            "clientID": format!("client-{group}"),
            "producerDataSet": [],
            "consumerDataSet": [{"groupName": group, "subscriptionDataSet": subs}]
        }))
        .unwrap(),
    );
    remoting::write_command(stream, &request).await.unwrap();
    remoting::read_command(stream).await.unwrap().unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn registration_send_pull_offset_and_restart() {
    let ns_dir = TempDir::new().unwrap();
    let ns = Arc::new(Namesrv::open(ns_dir.path(), Duration::from_secs(120)).unwrap());
    let ns_server =
        Running::start(TcpListener::bind("127.0.0.1:0").await.unwrap(), ns.clone()).await;
    let dir = TempDir::new().unwrap();
    let (broker, server) = start_broker(&dir).await;
    assert_eq!(
        ns_server.request(broker.registration().unwrap()).await.code,
        0
    );
    let route = ns_server
        .request(Command::request(105).with_ext("topic", "TBW102"))
        .await;
    assert_eq!(route.code, 0, "{:?}", route.remark);
    let route: Value = serde_json::from_slice(&route.body).unwrap();
    assert_eq!(
        route["brokerDatas"][0]["brokerAddrs"]["0"],
        server.addr.to_string()
    );

    let mut send_request = send("TestTopic", b"hello-rust");
    send_request.serialize_type = SerializeType::RocketMq;
    let response = server.request(send_request).await;
    assert_eq!(response.code, 0, "{:?}", response.remark);
    assert_eq!(response.ext_fields["queueOffset"], "0");
    assert_eq!(response.serialize_type, SerializeType::RocketMq);
    assert_eq!(
        ns_server.request(broker.registration().unwrap()).await.code,
        0
    );
    assert_eq!(
        ns_server
            .request(Command::request(105).with_ext("topic", "TestTopic"))
            .await
            .code,
        0
    );
    let response = server.request(pull("TestTopic", 0)).await;
    assert_eq!(response.code, 0, "{:?}", response.remark);
    let decoded = messages(&response.body);
    assert_eq!(decoded.len(), 1);
    assert_eq!(decoded[0].body, b"hello-rust");
    assert_eq!(decoded[0].properties["TAGS"], "blue");
    assert_eq!(decoded[0].queue_offset, 0);
    assert_eq!(response.ext_fields["nextBeginOffset"], "1");
    let viewed = server
        .request(Command::request(33).with_ext("offset", decoded[0].physical_offset))
        .await;
    assert_eq!(messages(&viewed.body), decoded);

    let commit = Command::request(15)
        .with_ext("consumerGroup", "test-consumer")
        .with_ext("topic", "TestTopic")
        .with_ext("queueId", 0)
        .with_ext("commitOffset", 1);
    assert_eq!(server.request(commit).await.code, 0);
    assert_eq!(server.request(pull("TestTopic", 1)).await.code, 19);
    let moved = server.request(pull("TestTopic", 99)).await;
    assert_eq!(moved.code, 21);
    assert_eq!(moved.ext_fields["nextBeginOffset"], "1");
    let filtered = server
        .request(pull("TestTopic", 0).with_ext("subscription", "green"))
        .await;
    assert_eq!(filtered.code, 20);
    assert_eq!(filtered.ext_fields["nextBeginOffset"], "1");

    server.stop().await;
    drop(broker);
    let (broker, server) = start_broker(&dir).await;
    assert_eq!(
        ns_server.request(broker.registration().unwrap()).await.code,
        0
    );
    let query = Command::request(14)
        .with_ext("consumerGroup", "test-consumer")
        .with_ext("topic", "TestTopic")
        .with_ext("queueId", 0);
    let queried = server.request(query).await;
    assert_eq!(queried.code, 0, "{:?}", queried.remark);
    assert_eq!(queried.ext_fields["offset"], "1");
    assert_eq!(
        messages(&server.request(pull("TestTopic", 0)).await.body)[0].body,
        b"hello-rust"
    );
    assert_eq!(
        server
            .request(send("TestTopic", b"after-restart"))
            .await
            .ext_fields["queueOffset"],
        "1"
    );
    assert_eq!(ns_server.request(broker.unregistration()).await.code, 0);
    assert_eq!(
        ns_server
            .request(Command::request(105).with_ext("topic", "TestTopic"))
            .await
            .code,
        17
    );
    server.stop().await;
    ns_server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn long_poll_does_not_block_send_on_same_connection() {
    let dir = TempDir::new().unwrap();
    let (broker, server) = start_broker(&dir).await;
    broker
        .store()
        .put_topic(TopicConfig::new("LongPoll", 1))
        .unwrap();
    let mut stream = TcpStream::connect(server.addr).await.unwrap();
    let request = pull("LongPoll", 0)
        .with_ext("sysFlag", 6)
        .with_ext("suspendTimeoutMillis", 3000);
    let pull_opaque = request.opaque;
    remoting::write_command(&mut stream, &request)
        .await
        .unwrap();
    let request = send("LongPoll", b"wake-up");
    let send_opaque = request.opaque;
    remoting::write_command(&mut stream, &request)
        .await
        .unwrap();
    let results = tokio::time::timeout(Duration::from_secs(2), async {
        let first = remoting::read_command(&mut stream).await.unwrap().unwrap();
        let second = remoting::read_command(&mut stream).await.unwrap().unwrap();
        [first, second]
    })
    .await
    .unwrap();
    let sent = results.iter().find(|r| r.opaque == send_opaque).unwrap();
    let pulled = results.iter().find(|r| r.opaque == pull_opaque).unwrap();
    assert_eq!(sent.code, 0, "{:?}", sent.remark);
    assert_eq!(pulled.code, 0, "{:?}", pulled.remark);
    assert_eq!(messages(&pulled.body)[0].body, b"wake-up");
    drop(stream);
    server.stop().await;
}

#[tokio::test]
async fn accepted_oneway_survives_sender_half_close() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let dir = TempDir::new().unwrap();
    let (_broker, server) = start_broker(&dir).await;
    let mut stream = TcpStream::connect(server.addr).await.unwrap();
    let mut request = send("ClosedSender", b"accepted-before-eof");
    request.flag |= 2;
    remoting::write_command(&mut stream, &request)
        .await
        .unwrap();
    stream.shutdown().await.unwrap();
    let mut response = Vec::new();
    tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut response))
        .await
        .unwrap()
        .unwrap();
    assert!(response.is_empty());
    let response = server.request(pull("ClosedSender", 0)).await;
    assert_eq!(response.code, 0, "{:?}", response.remark);
    assert_eq!(messages(&response.body)[0].body, b"accepted-before-eof");
    server.stop().await;
}

fn batch_entry(body: &[u8], properties: &[u8]) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend(((22 + body.len() + properties.len()) as u32).to_be_bytes());
    bytes.extend(0u32.to_be_bytes());
    bytes.extend(0u32.to_be_bytes());
    bytes.extend(0u32.to_be_bytes());
    bytes.extend((body.len() as u32).to_be_bytes());
    bytes.extend(body);
    bytes.extend((properties.len() as u16).to_be_bytes());
    bytes.extend(properties);
    bytes
}

#[tokio::test]
async fn batch_v2_and_invalid_requests() {
    let dir = TempDir::new().unwrap();
    let (broker, server) = start_broker(&dir).await;
    let mut batch = batch_entry(b"first", b"TAGS\x01red\x02");
    batch.extend(batch_entry(b"second", b"TAGS\x01blue\x02"));
    let request = Command::request(320)
        .with_ext("a", "test-producer")
        .with_ext("b", "BatchTopic")
        .with_ext("c", "TBW102")
        .with_ext("d", 4)
        .with_ext("e", 0)
        .with_ext("f", 0)
        .with_ext("g", now_millis())
        .with_ext("h", 0)
        .with_ext("m", true)
        .with_body(batch.clone());
    let response = server.request(request).await;
    assert_eq!(response.code, 0, "{:?}", response.remark);
    assert_eq!(response.ext_fields["msgId"].split(',').count(), 2);
    let response = server
        .request(pull("BatchTopic", 0).with_ext("subscription", "blue"))
        .await;
    assert_eq!(response.code, 0);
    assert_eq!(response.ext_fields["nextBeginOffset"], "2");
    assert_eq!(messages(&response.body)[0].body, b"second");
    for request in [
        send("../escape", b"bad"),
        send("BatchTopic", b""),
        send("BatchTopic", b"bad").with_ext("queueId", 500),
        // COMMIT (0x8) is still refused from a producer; PREPARED (0x4) is now a legal half message.
        send("BatchTopic", b"bad").with_ext("sysFlag", 8),
        send_with("BatchTopic", &batch, "DELAY\x011\x02").with_ext("batch", true),
        pull("BatchTopic", 0).with_ext("maxMsgNums", -1),
        pull("BatchTopic", 0).with_ext("expressionType", "SQL92"),
        Command::request(200050),
        Command::request(999_999),
    ] {
        assert_ne!(server.request(request).await.code, 0);
    }
    assert_eq!(broker.store().queue_max_offset("BatchTopic", 0), 2);
    // A delay level parks the record in the schedule queue instead of the requested topic.
    let response = server
        .request(send_with("BatchTopic", b"delayed", "DELAY\x011\x02"))
        .await;
    assert_eq!(response.code, 0, "{:?}", response.remark);
    assert_eq!(response.ext_fields["queueId"], "0");
    assert_eq!(broker.store().queue_max_offset("BatchTopic", 0), 2);
    let parked = broker
        .store()
        .read("SCHEDULE_TOPIC_XXXX", 0, 0, 4, 8 * 1024 * 1024, None)
        .unwrap();
    assert_eq!(parked.messages.len(), 1);
    assert_eq!(parked.messages[0].properties["REAL_TOPIC"], "BatchTopic");
    assert_eq!(parked.messages[0].properties["REAL_QID"], "0");
    assert_eq!(parked.messages[0].queue_offset, 0);
    server.stop().await;
}

#[tokio::test]
async fn heartbeat_subscriptions_and_disconnect_cleanup() {
    let dir = TempDir::new().unwrap();
    let (broker, server) = start_broker(&dir).await;
    assert_eq!(
        server
            .request(send("Subscriptions", b"blue-message"))
            .await
            .code,
        0
    );
    let mut stream = TcpStream::connect(server.addr).await.unwrap();
    let heartbeat = Command::request(34).with_body(
        serde_json::to_vec(&json!({
            "clientID":"test-client", "producerDataSet":[],
            "consumerDataSet":[{"groupName":"test-consumer", "subscriptionDataSet":[{
                "topic":"Subscriptions", "subString":"blue", "expressionType":"TAG", "subVersion":1
            }]}]
        }))
        .unwrap(),
    );
    remoting::write_command(&mut stream, &heartbeat)
        .await
        .unwrap();
    assert_eq!(
        remoting::read_command(&mut stream)
            .await
            .unwrap()
            .unwrap()
            .code,
        0
    );
    let request = pull("Subscriptions", 0)
        .with_ext("sysFlag", 0)
        .with_ext("subVersion", 1);
    assert_eq!(server.request(request).await.code, 0);
    let members = server
        .request(Command::request(38).with_ext("consumerGroup", "test-consumer"))
        .await;
    let members: Value = serde_json::from_slice(&members.body).unwrap();
    assert_eq!(members["consumerIdList"], json!(["test-client"]));
    broker.disconnected(stream.local_addr().unwrap());
    assert_eq!(
        server
            .request(pull("Subscriptions", 0).with_ext("sysFlag", 0))
            .await
            .code,
        24
    );
    drop(stream);
    server.stop().await;
}

#[tokio::test]
async fn oneway_executes_without_response_and_malformed_frame_is_isolated() {
    use tokio::io::AsyncWriteExt;
    let dir = TempDir::new().unwrap();
    let (_broker, server) = start_broker(&dir).await;
    let mut stream = TcpStream::connect(server.addr).await.unwrap();
    let mut request = send("Oneway", b"fire-and-forget");
    request.flag |= 2;
    remoting::write_command(&mut stream, &request)
        .await
        .unwrap();
    assert!(
        tokio::time::timeout(
            Duration::from_millis(100),
            remoting::read_command(&mut stream)
        )
        .await
        .is_err()
    );
    let response = server.request(pull("Oneway", 0)).await;
    assert_eq!(response.code, 0, "{:?}", response.remark);
    stream.write_all(&u32::MAX.to_be_bytes()).await.unwrap();
    let disconnected =
        tokio::time::timeout(Duration::from_secs(2), remoting::read_command(&mut stream))
            .await
            .unwrap();
    assert!(disconnected.is_err() || disconnected.unwrap().is_none());
    assert_eq!(server.request(Command::request(28)).await.code, 0);
    server.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_sends_have_unique_contiguous_offsets() {
    let dir = TempDir::new().unwrap();
    let (broker, server) = start_broker(&dir).await;
    broker
        .store()
        .put_topic(TopicConfig::new("Concurrent", 1))
        .unwrap();
    let addr = server.addr.to_string();
    let mut tasks = tokio::task::JoinSet::new();
    for index in 0..32 {
        let addr = addr.clone();
        tasks.spawn(async move {
            let response = remoting::invoke(
                &addr,
                send("Concurrent", index.to_string().as_bytes()),
                Duration::from_secs(5),
            )
            .await
            .unwrap();
            assert_eq!(response.code, 0, "{:?}", response.remark);
            response.ext_fields["queueOffset"].parse::<u64>().unwrap()
        });
    }
    let mut offsets = Vec::new();
    while let Some(result) = tasks.join_next().await {
        offsets.push(result.unwrap());
    }
    offsets.sort_unstable();
    assert_eq!(offsets, (0..32).collect::<Vec<_>>());
    let response = server.request(pull("Concurrent", 0)).await;
    assert_eq!(messages(&response.body).len(), 32);
    server.stop().await;
}

#[tokio::test]
async fn sql92_inline_pull_needs_property_filter_and_matches_by_property() {
    let disabled = TempDir::new().unwrap();
    let (_broker, server) = start_broker(&disabled).await;
    let response = server
        .request(send_with(
            "SqlFilter",
            b"high",
            "TAGS\x01blue\x02level\x0110",
        ))
        .await;
    assert_eq!(response.code, 0, "{:?}", response.remark);
    assert_eq!(
        server
            .request(send_with("SqlFilter", b"low", "TAGS\x01blue\x02level\x011"))
            .await
            .code,
        0
    );

    let sql_pull = || {
        pull("SqlFilter", 0)
            .with_ext("expressionType", "SQL92")
            .with_ext("subscription", "level > 5")
    };
    // PullMessageProcessor answers SYSTEM_ERROR before it evaluates anything.
    let rejected = server.request(sql_pull()).await;
    assert_eq!(rejected.code, 1);
    assert_eq!(
        server
            .request(
                pull("SqlFilter", 0)
                    .with_ext("expressionType", "SQL92")
                    .with_ext("subscription", "")
            )
            .await
            .code,
        23
    );
    server.stop().await;

    let enabled = TempDir::new().unwrap();
    let (_broker, server) = start_broker_with(&enabled, true).await;
    assert_eq!(
        server
            .request(send_with(
                "SqlFilter",
                b"high",
                "TAGS\x01blue\x02level\x0110"
            ))
            .await
            .code,
        0
    );
    assert_eq!(
        server
            .request(send_with("SqlFilter", b"low", "TAGS\x01blue\x02level\x011"))
            .await
            .code,
        0
    );
    let response = server.request(sql_pull()).await;
    assert_eq!(response.code, 0, "{:?}", response.remark);
    let bodies = messages(&response.body)
        .into_iter()
        .map(|message| message.body)
        .collect::<Vec<_>>();
    assert_eq!(bodies, vec![b"high".to_vec()]);
    // An expression the original parser rejects leaves no filter data, so everything matches.
    let junk = server
        .request(
            pull("SqlFilter", 0)
                .with_ext("expressionType", "SQL92")
                .with_ext("subscription", "level >"),
        )
        .await;
    assert_eq!(junk.code, 0);
    assert_eq!(messages(&junk.body).len(), 2);
    server.stop().await;
}

#[tokio::test]
async fn registered_sql92_subscription_filters_and_reports_filter_data_state() {
    let dir = TempDir::new().unwrap();
    let (broker, server) = start_broker_with(&dir, true).await;
    for (body, level) in [("high", "10"), ("low", "1")] {
        assert_eq!(
            server
                .request(send_with(
                    "SqlFilter",
                    body.as_bytes(),
                    &format!("level\x01{level}")
                ))
                .await
                .code,
            0
        );
    }

    let mut stream = TcpStream::connect(server.addr).await.unwrap();
    let response = send_heartbeat(
        &mut stream,
        "test-consumer",
        json!([{"topic": "SqlFilter", "subString": "level > 5", "expressionType": "SQL92", "subVersion": 1}]),
    )
    .await;
    assert_eq!(response.code, 0);
    assert_eq!(response.ext_fields["IS_SUPPORT_HEART_BEAT_V2"], "true");

    let registered = || {
        pull("SqlFilter", 0)
            .with_ext("sysFlag", 0)
            .with_ext("subVersion", 1)
    };
    let response = server.request(registered()).await;
    assert_eq!(response.code, 0, "{:?}", response.remark);
    let bodies = messages(&response.body)
        .into_iter()
        .map(|message| message.body)
        .collect::<Vec<_>>();
    assert_eq!(bodies, vec![b"high".to_vec()]);
    assert_eq!(
        server
            .request(registered().with_ext("subVersion", 2))
            .await
            .code,
        25
    );

    // A group whose expression never compiles has no consumer filter data at all.
    let mut broken = TcpStream::connect(server.addr).await.unwrap();
    assert_eq!(
        send_heartbeat(
            &mut broken,
            "broken-group",
            json!([{"topic": "SqlFilter", "subString": "level >", "expressionType": "SQL92", "subVersion": 1}]),
        )
        .await
        .code,
        0
    );
    let response = server
        .request(
            pull("SqlFilter", 0)
                .with_ext("sysFlag", 0)
                .with_ext("consumerGroup", "broken-group")
                .with_ext("subVersion", 1),
        )
        .await;
    assert_eq!(response.code, 27);

    broker.persist_filters().await.unwrap();
    let persisted: Value =
        serde_json::from_slice(&std::fs::read(dir.path().join("consumerFilter.json")).unwrap())
            .unwrap();
    assert_eq!(
        persisted["filterDataByTopic"]["SqlFilter"]["groupFilterData"]["test-consumer"]["expression"],
        "level > 5"
    );
    drop(broken);
    server.stop().await;
}
