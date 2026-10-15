use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use rocketmq_rs::broker::{Broker, BrokerConfig};
use rocketmq_rs::common::{Message, Result, now_millis};
use rocketmq_rs::remoting::{self, Command, Handler};
use rocketmq_rs::schedule::{
    self as schedule, ExponentialRetryPolicy, GroupRetryPolicy, GroupRetryPolicyType,
    SimpleSubscriptionData,
};
use rocketmq_rs::store::Store;
use serde_json::json;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::JoinHandle;

struct Server {
    addr: SocketAddr,
    shutdown: watch::Sender<bool>,
    task: JoinHandle<Result<()>>,
}

impl Server {
    async fn start(listener: TcpListener, broker: Arc<Broker>) -> Self {
        let addr = listener.local_addr().unwrap();
        let (shutdown, receiver) = watch::channel(false);
        let task = tokio::spawn(remoting::serve(
            listener,
            broker.clone() as Arc<dyn Handler>,
            receiver,
        ));
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

async fn started(dir: &TempDir) -> (Arc<Broker>, Server) {
    started_with(dir, |_| {}).await
}

async fn started_with(
    dir: &TempDir,
    adjust: impl FnOnce(&mut BrokerConfig),
) -> (Arc<Broker>, Server) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = BrokerConfig {
        advertised_addr: listener.local_addr().unwrap(),
        ..BrokerConfig::default()
    };
    adjust(&mut config);
    let store = Arc::new(Store::open(dir.path()).unwrap());
    let broker = Arc::new(Broker::new(store, config).unwrap());
    let server = Server::start(listener, broker.clone()).await;
    (broker, server)
}

/// `ScheduleMessageService.start`, run exactly the way the binary runs it.
fn spawn_scheduler(broker: &Arc<Broker>) -> (watch::Sender<bool>, JoinHandle<Result<()>>) {
    let (stop, shutdown) = watch::channel(false);
    let broker = broker.clone();
    let task = tokio::spawn(async move { broker.run_scheduler(shutdown).await });
    (stop, task)
}

fn send(topic: &str, body: &[u8], properties: &str) -> Command {
    Command::request(10)
        .with_ext("producerGroup", "test-producer")
        .with_ext("topic", topic)
        .with_ext("defaultTopic", "TBW102")
        .with_ext("defaultTopicQueueNums", 4)
        .with_ext("queueId", 0)
        .with_ext("sysFlag", 0)
        .with_ext("bornTimestamp", now_millis())
        .with_ext("flag", 0)
        .with_ext("properties", properties)
        .with_body(body.to_vec())
}

fn send_back(group: &str, offset: u64, delay_level: i32, reconsume_times: i32) -> Command {
    Command::request(36)
        .with_ext("group", group)
        .with_ext("offset", offset)
        .with_ext("delayLevel", delay_level)
        .with_ext("maxReconsumeTimes", reconsume_times)
        .with_ext("originTopic", "unused")
        .with_ext("unitMode", false)
}

fn pull(group: &str, topic: &str, queue_id: i32) -> Command {
    Command::request(11)
        .with_ext("consumerGroup", group)
        .with_ext("topic", topic)
        .with_ext("queueId", queue_id)
        .with_ext("queueOffset", 0)
        .with_ext("maxMsgNums", 32)
        .with_ext("sysFlag", 4)
        .with_ext("commitOffset", 0)
        .with_ext("suspendTimeoutMillis", 0)
        .with_ext("subVersion", 0)
        .with_ext("subscription", "*")
        .with_ext("expressionType", "TAG")
}

fn records(broker: &Broker, topic: &str, queue_id: i32) -> Vec<Message> {
    broker
        .store()
        .read(topic, queue_id, 0, 64, 8 * 1024 * 1024, None)
        .unwrap()
        .messages
}

async fn wait_until(broker: &Arc<Broker>, topic: &str, queue_id: i32) -> Vec<Message> {
    tokio::time::timeout(Duration::from_secs(8), async {
        loop {
            let messages = records(broker, topic, queue_id);
            if !messages.is_empty() {
                return messages;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("nothing arrived on {topic} queue {queue_id}"))
}

#[tokio::test]
async fn delay_level_parks_then_delivers_and_keeps_the_cursor() {
    let dir = TempDir::new().unwrap();
    let (broker, server) = started(&dir).await;
    let response = server
        .request(send("Delayed", b"later", "DELAY\x011\x02"))
        .await;
    assert_eq!(response.code, 0, "{:?}", response.remark);
    // The answer carries the requested queue while the record waits in the schedule queue.
    assert_eq!(response.ext_fields["queueId"], "0");
    assert!(records(&broker, "Delayed", 0).is_empty());
    let parked = records(&broker, "SCHEDULE_TOPIC_XXXX", 0);
    assert_eq!(parked.len(), 1);
    assert_eq!(parked[0].properties["REAL_TOPIC"], "Delayed");
    assert_eq!(parked[0].properties["REAL_QID"], "0");

    let (stop, scheduler) = spawn_scheduler(&broker);
    let delivered = wait_until(&broker, "Delayed", 0).await;
    assert_eq!(delivered[0].body, b"later");
    assert_eq!(delivered[0].queue_id, 0);
    assert_eq!(delivered[0].queue_offset, 0);
    // `messageTimeUp` clears the delay property but keeps the real-topic backup.
    assert!(!delivered[0].properties.contains_key("DELAY"));
    assert_eq!(delivered[0].properties["REAL_TOPIC"], "Delayed");

    stop.send(true).unwrap();
    scheduler.await.unwrap().unwrap();
    assert_eq!(broker.store().delay_offsets().unwrap().offset_table[&1], 1);
    server.stop().await;
    drop(broker);

    // A restart resumes behind the delivered record instead of replaying it.
    let store = Arc::new(Store::open(dir.path()).unwrap());
    assert_eq!(store.delay_offsets().unwrap().offset_table[&1], 1);
    let broker = Arc::new(
        Broker::new(
            store,
            BrokerConfig {
                advertised_addr: "127.0.0.1:1".parse().unwrap(),
                ..BrokerConfig::default()
            },
        )
        .unwrap(),
    );
    let (stop, scheduler) = spawn_scheduler(&broker);
    tokio::time::sleep(Duration::from_millis(300)).await;
    stop.send(true).unwrap();
    scheduler.await.unwrap().unwrap();
    assert_eq!(records(&broker, "Delayed", 0).len(), 1);
    assert_eq!(broker.store().delay_offsets().unwrap().offset_table[&1], 1);
}

#[tokio::test]
async fn unknown_delay_level_clamps_to_the_longest_one() {
    let dir = TempDir::new().unwrap();
    let (broker, server) = started(&dir).await;
    let response = server
        .request(send("Wide", b"far", "DELAY\x0199\x02"))
        .await;
    assert_eq!(response.code, 0, "{:?}", response.remark);
    // `transformDelayLevelMessage` clamps the property itself to `maxDelayLevel`, which is 18.
    let parked = records(&broker, "SCHEDULE_TOPIC_XXXX", 17);
    assert_eq!(parked.len(), 1);
    assert_eq!(parked[0].properties["DELAY"], "18");
    server.stop().await;
}

#[tokio::test]
async fn malformed_delay_level_is_a_system_error() {
    let dir = TempDir::new().unwrap();
    let (broker, server) = started(&dir).await;
    let response = server
        .request(send("Broken", b"bad", "DELAY\x01not-a-number\x02"))
        .await;
    // The reference lets the `NumberFormatException` escape, which answers SYSTEM_ERROR.
    assert_eq!(response.code, 1, "{:?}", response.remark);
    assert!(records(&broker, "Broken", 0).is_empty());
    server.stop().await;
}

#[tokio::test]
async fn accurate_timer_messages_are_answered_as_disabled() {
    let dir = TempDir::new().unwrap();
    // `MessageStoreConfig.timerWheelEnable` defaults to true, so the operator has to turn it off.
    let (broker, server) = started_with(&dir, |config| config.timer_wheel_enable = false).await;
    let deliver = now_millis() + 60_000;
    let response = server
        .request(send(
            "Timers",
            b"soon",
            &format!("TIMER_DELIVER_MS\x01{deliver}\x02"),
        ))
        .await;
    assert_eq!(response.code, 1, "{:?}", response.remark);
    assert_eq!(
        response.remark.as_deref(),
        Some("accurate timer message is not enabled, timerWheelEnable is false")
    );
    assert!(records(&broker, "Timers", 0).is_empty());
    assert!(records(&broker, "rmq_sys_wheel_timer", 0).is_empty());
    server.stop().await;
}

#[tokio::test]
async fn a_timer_message_parks_in_the_wheel_and_delivers_at_its_slot() {
    let dir = TempDir::new().unwrap();
    let (broker, server) = started(&dir).await;
    let deliver = now_millis() + 2_000;
    let response = server
        .request(send(
            "Timers",
            b"precise",
            &format!("UNIQ_KEY\x01uniq-1\x02TIMER_DELIVER_MS\x01{deliver}\x02"),
        ))
        .await;
    assert_eq!(response.code, 0, "{:?}", response.remark);
    assert!(records(&broker, "Timers", 0).is_empty());
    // `attachRecallHandle` names the slot, one millisecond past the stamp the wheel floors back.
    let handle = response.ext_fields["recallHandle"].clone();
    assert!(!handle.is_empty());

    let parked = records(&broker, "rmq_sys_wheel_timer", 0);
    assert_eq!(parked.len(), 1);
    assert_eq!(parked[0].properties["REAL_TOPIC"], "Timers");
    assert_eq!(parked[0].properties["REAL_QID"], "0");
    let slot: i64 = parked[0].properties["TIMER_OUT_MS"].parse().unwrap();
    assert_eq!(slot % 1_000, 0);
    assert!(slot <= deliver);

    let (stop, scheduler) = spawn_scheduler(&broker);
    let delivered = wait_until(&broker, "Timers", 0).await;
    assert_eq!(delivered.len(), 1);
    assert_eq!(delivered[0].body, b"precise");
    assert_eq!(delivered[0].queue_id, 0);
    // `convertMessage` restores the destination but leaves every timer stamp behind.
    assert_eq!(delivered[0].properties["UNIQ_KEY"], "uniq-1");
    assert_eq!(
        delivered[0].properties["TIMER_DELIVER_MS"],
        deliver.to_string()
    );
    assert_eq!(delivered[0].properties["TIMER_OUT_MS"], slot.to_string());
    assert!(!delivered[0].properties.contains_key("REAL_TOPIC"));
    assert!(!delivered[0].properties.contains_key("REAL_QID"));
    let enqueue: i64 = delivered[0].properties["TIMER_ENQUEUE_MS"].parse().unwrap();
    let dequeue: i64 = delivered[0].properties["TIMER_DEQUEUE_MS"].parse().unwrap();
    assert!(dequeue >= slot, "{dequeue} drained before {slot}");
    assert!(enqueue <= dequeue);

    stop.send(true).unwrap();
    scheduler.await.unwrap().unwrap();
    // The scan cursor moved past the record, so a restart cannot park it a second time.
    assert_eq!(broker.store().timer_checkpoint().unwrap().scan_offset, 1);
    server.stop().await;
    drop(broker);

    let store = Arc::new(Store::open(dir.path()).unwrap());
    assert_eq!(store.timer_checkpoint().unwrap().scan_offset, 1);
    let broker = Arc::new(
        Broker::new(
            store,
            BrokerConfig {
                advertised_addr: "127.0.0.1:1".parse().unwrap(),
                ..BrokerConfig::default()
            },
        )
        .unwrap(),
    );
    let (stop, scheduler) = spawn_scheduler(&broker);
    tokio::time::sleep(Duration::from_millis(400)).await;
    stop.send(true).unwrap();
    scheduler.await.unwrap().unwrap();
    assert_eq!(records(&broker, "Timers", 0).len(), 1);
}

#[tokio::test]
async fn a_due_timer_record_delivers_without_waiting_for_the_wheel() {
    let dir = TempDir::new().unwrap();
    let (broker, server) = started(&dir).await;
    // A deadline that has already passed is stored on its own topic, exactly like `HookUtils`.
    let response = server
        .request(send(
            "Past",
            b"already",
            &format!("TIMER_DELIVER_MS\x01{}\x02", now_millis() - 5_000),
        ))
        .await;
    assert_eq!(response.code, 0, "{:?}", response.remark);
    let stored = records(&broker, "Past", 0);
    assert_eq!(stored.len(), 1);
    assert!(!stored[0].properties.contains_key("TIMER_OUT_MS"));
    // No handle, because nothing is parked for the wheel to release.
    assert!(!response.ext_fields.contains_key("recallHandle"));

    // A record the scan finds past its slot is delivered at once, with the reference's marker.
    // Only the transform writes the timer queue, so the record is planted the way a restart
    // would find it: parked, with its slot already behind the read head.
    broker
        .store()
        .append_internal(vec![Message {
            topic: "rmq_sys_wheel_timer".into(),
            body: b"stale".to_vec(),
            born_timestamp: now_millis(),
            born_host: "127.0.0.1:1".parse().unwrap(),
            store_host: broker.config().advertised_addr,
            properties: BTreeMap::from([
                ("REAL_TOPIC".to_owned(), "Late".to_owned()),
                ("REAL_QID".to_owned(), "0".to_owned()),
                ("TIMER_OUT_MS".to_owned(), "5000".to_owned()),
            ]),
            ..Message::default()
        }])
        .unwrap();
    assert_eq!(records(&broker, "rmq_sys_wheel_timer", 0).len(), 1);
    assert!(records(&broker, "Late", 0).is_empty());
    let (stop, scheduler) = spawn_scheduler(&broker);
    let delivered = wait_until(&broker, "Late", 0).await;
    assert_eq!(delivered[0].body, b"stale");
    assert_eq!(
        delivered[0].properties["TIMER_ENQUEUE_MS"],
        "9223372036854775807"
    );
    assert_eq!(broker.store().queue_max_offset("Late", 0), 1);
    stop.send(true).unwrap();
    scheduler.await.unwrap().unwrap();
    server.stop().await;
}

#[tokio::test]
async fn a_recall_only_cancels_the_message_the_handle_names() {
    let dir = TempDir::new().unwrap();
    let (broker, server) = started_with(&dir, |config| config.recall_message_enable = true).await;
    // Both records land in the same slot, so the delete has to name one of them.
    let deliver = now_millis() + 3_000;
    let mut handles = Vec::new();
    for tag in ["keep", "drop"] {
        let response = server
            .request(send(
                "Cancel",
                tag.as_bytes(),
                &format!("UNIQ_KEY\x01uniq-{tag}\x02TIMER_DELIVER_MS\x01{deliver}\x02"),
            ))
            .await;
        assert_eq!(response.code, 0, "{:?}", response.remark);
        handles.push(response.ext_fields["recallHandle"].clone());
    }

    let response = server
        .request(
            Command::request(370)
                .with_ext("producerGroup", "test-producer")
                .with_ext("topic", "Cancel")
                .with_ext("recallHandle", &handles[1]),
        )
        .await;
    assert_eq!(response.code, 0, "{:?}", response.remark);
    assert_eq!(response.ext_fields["msgId"], "uniq-drop");

    let parked = records(&broker, "rmq_sys_wheel_timer", 0);
    assert_eq!(
        parked.len(),
        3,
        "the delete record belongs in the timer queue"
    );
    let delete = &parked[2];
    assert_eq!(delete.topic, "rmq_sys_wheel_timer");
    assert_eq!(delete.body, b"0");
    assert_eq!(delete.properties["TAGS"], "_RECALL_TAG_");
    assert_eq!(delete.properties["TIMER_DEL_UNIQKEY"], "uniq-drop");
    assert_eq!(delete.properties["UNIQ_KEY"], "uniq-drop");
    assert_eq!(delete.properties["PGROUP"], "test-producer");
    // The delete record floors back into the very slot it cancels.
    assert_eq!(
        delete.properties["TIMER_OUT_MS"],
        parked[1].properties["TIMER_OUT_MS"]
    );

    let (stop, scheduler) = spawn_scheduler(&broker);
    let delivered = wait_until(&broker, "Cancel", 0).await;
    tokio::time::sleep(Duration::from_millis(1_500)).await;
    let delivered_after = records(&broker, "Cancel", 0);
    assert_eq!(
        delivered_after.len(),
        delivered.len(),
        "a cancelled message must not arrive late"
    );
    assert_eq!(delivered_after.len(), 1);
    assert_eq!(delivered_after[0].body, b"keep");
    stop.send(true).unwrap();
    scheduler.await.unwrap().unwrap();
    server.stop().await;
}

#[tokio::test]
async fn recall_requests_answer_the_reference_gates() {
    let build = |topic: &str, broker: &str, timestamp: &str| {
        schedule::build_recall_handle(topic, broker, timestamp, "uniq-1")
    };
    let recall = |handle: &str, topic: &str| {
        Command::request(370)
            .with_ext("producerGroup", "test-producer")
            .with_ext("topic", topic)
            .with_ext("recallHandle", handle)
    };

    let off = TempDir::new().unwrap();
    let (broker, server) = started(&off).await;
    // `BrokerConfig.recallMessageEnable` defaults to false, so nothing else is even inspected.
    let response = server
        .request(recall(&build("Recall", "broker-a", "1"), "Recall"))
        .await;
    assert_eq!(response.code, 16);
    assert_eq!(
        response.remark.as_deref(),
        Some("recall failed, operation is forbidden")
    );
    server.stop().await;
    drop(broker);

    let dir = TempDir::new().unwrap();
    let (_broker, server) = started_with(&dir, |config| config.recall_message_enable = true).await;
    let deliver = now_millis() + 60_000;
    let handle = build("Recall", "broker-a", &deliver.to_string());
    let response = server.request(recall(&handle, "Recall")).await;
    assert_eq!(response.code, 17, "{:?}", response.remark);
    assert_eq!(
        response.remark.as_deref(),
        Some("recall failed, the topic[Recall] not exist")
    );

    // The topic has to exist before the handle is read, so create the two the gates compare.
    for topic in ["Recall", "Other"] {
        let response = server
            .request(send(topic, b"x", "UNIQ_KEY\x01uniq-1\x02"))
            .await;
        assert_eq!(response.code, 0, "{:?}", response.remark);
    }
    for (remark, request) in [
        ("recall handle is invalid", recall("not-a-handle", "Recall")),
        ("recall failed, topic not match", recall(&handle, "Other")),
        (
            "recall failed, broker service not available",
            recall(&build("Recall", "broker-b", &deliver.to_string()), "Recall"),
        ),
    ] {
        let response = server.request(request).await;
        assert_eq!(response.code, 604, "{remark}");
        assert_eq!(response.remark.as_deref(), Some(remark));
    }
    // A timestamp at or before now, unparseable, or beyond the whole delay window all fail alike.
    for bad in ["0", "-1", "x"] {
        let response = server
            .request(recall(&build("Recall", "broker-a", bad), "Recall"))
            .await;
        assert_eq!(response.code, 604, "{bad}");
        assert_eq!(
            response.remark.as_deref(),
            Some("recall failed, timestamp invalid")
        );
    }
    let far = now_millis() + 300 * 86_400_000;
    let response = server
        .request(recall(
            &build("Recall", "broker-a", &far.to_string()),
            "Recall",
        ))
        .await;
    assert_eq!(response.code, 604);
    assert_eq!(
        response.remark.as_deref(),
        Some("recall failed, timestamp invalid")
    );
    server.stop().await;
}

#[tokio::test]
async fn a_timer_delay_past_the_window_is_illegal() {
    let dir = TempDir::new().unwrap();
    let (broker, server) = started(&dir).await;
    let response = server
        .request(send("Timers", b"long", "TIMER_DELAY_SEC\x01300000\x02"))
        .await;
    assert_eq!(response.code, 13, "{:?}", response.remark);
    assert_eq!(
        response.remark.as_deref(),
        Some(
            "timer message illegal, the delay time should not be bigger than the max delay 259200000ms; or if set del msg, the delay time should be bigger than the current time"
        )
    );
    assert!(records(&broker, "Timers", 0).is_empty());
    // A delete record that arrives on its own is refused the same way, before it is parked.
    let response = server
        .request(send(
            "Timers",
            b"del",
            "TIMER_DELIVER_MS\x011\x02TIMER_DEL_UNIQKEY\x01uniq-1\x02",
        ))
        .await;
    assert_eq!(response.code, 13, "{:?}", response.remark);
    assert_eq!(records(&broker, "Timers", 0).len(), 0);
    server.stop().await;
}

#[tokio::test]
async fn send_back_retries_through_the_schedule_queue() {
    let dir = TempDir::new().unwrap();
    let (broker, server) = started(&dir).await;
    let response = server
        .request(send("Back", b"body", "TAGS\x01blue\x02"))
        .await;
    assert_eq!(response.code, 0, "{:?}", response.remark);
    let original = records(&broker, "Back", 0).remove(0);

    let response = server
        .request(send_back("BackGroup", original.physical_offset, 0, 16))
        .await;
    assert_eq!(response.code, 0, "{:?}", response.remark);
    // `delayLevel == 0` selects `3 + reconsumeTimes`, so the put hook parks the record again.
    assert!(records(&broker, "%RETRY%BackGroup", 0).is_empty());
    let parked = records(&broker, "SCHEDULE_TOPIC_XXXX", 2);
    assert_eq!(parked.len(), 1);
    assert_eq!(parked[0].properties["REAL_TOPIC"], "%RETRY%BackGroup");
    assert_eq!(parked[0].properties["DELAY"], "3");
    assert_eq!(parked[0].properties["RETRY_TOPIC"], "Back");
    assert_eq!(
        parked[0].properties["ORIGIN_MESSAGE_ID"],
        original.message_id()
    );
    assert_eq!(parked[0].reconsume_times, 1);
    assert_eq!(parked[0].body, b"body");

    let group = broker.store().group("BackGroup").unwrap().unwrap();
    assert_eq!(group.retry_queue_nums, 1);
    assert_eq!(group.retry_max_times, 16);
    let retry = broker
        .store()
        .topics()
        .unwrap()
        .remove("%RETRY%BackGroup")
        .unwrap();
    assert_eq!(retry.read_queue_nums, 1);
    assert_eq!(retry.perm, 6);

    // A negative delay level dead-letters the record without parking it.
    let response = server
        .request(send_back("BackGroup", original.physical_offset, -1, 16))
        .await;
    assert_eq!(response.code, 0, "{:?}", response.remark);
    let dead = records(&broker, "%DLQ%BackGroup", 0);
    assert_eq!(dead.len(), 1);
    assert_eq!(dead[0].properties["DELAY"], "0");
    assert_eq!(dead[0].reconsume_times, 1);
    server.stop().await;
}

#[tokio::test]
async fn exhausted_reconsume_times_sends_back_to_the_dead_letter_topic() {
    let dir = TempDir::new().unwrap();
    let (broker, server) = started(&dir).await;
    let response = server
        .request(send("Exhausted", b"stale", "TAGS\x01blue\x02").with_ext("reconsumeTimes", 16))
        .await;
    assert_eq!(response.code, 0, "{:?}", response.remark);
    let original = records(&broker, "Exhausted", 0).remove(0);
    let response = server
        .request(send_back("ExhaustedGroup", original.physical_offset, 0, 16))
        .await;
    assert_eq!(response.code, 0, "{:?}", response.remark);
    assert!(records(&broker, "SCHEDULE_TOPIC_XXXX", 2).is_empty());
    let dead = records(&broker, "%DLQ%ExhaustedGroup", 0);
    assert_eq!(dead.len(), 1);
    assert_eq!(dead[0].reconsume_times, 17);
    assert_eq!(dead[0].properties["DELAY"], "0");
    assert_eq!(dead[0].properties["RETRY_TOPIC"], "Exhausted");
    server.stop().await;
}

#[tokio::test]
async fn send_back_without_a_group_config_is_refused() {
    let dir = TempDir::new().unwrap();
    let (broker, server) = started_with(&dir, |config| config.auto_create_group = false).await;
    // Neither the send back nor the pull invents a group when auto creation is off.
    let response = server.request(send_back("MissingGroup", 1, 0, 16)).await;
    assert_eq!(response.code, 26, "{:?}", response.remark);
    let response = server.request(pull("MissingGroup", "Anywhere", 0)).await;
    assert_eq!(response.code, 26, "{:?}", response.remark);

    let response = server
        .request(send("NoGroup", b"body", "TAGS\x01blue\x02"))
        .await;
    assert_eq!(response.code, 0, "{:?}", response.remark);
    let body = serde_json::to_vec(&json!({"groupName": "Known"})).unwrap();
    let response = server.request(Command::request(200).with_body(body)).await;
    assert_eq!(response.code, 0, "{:?}", response.remark);

    // With a group but no such record, the reference reports the failed offset lookup.
    let response = server.request(send_back("Known", 9_999, 0, 16)).await;
    assert_eq!(response.code, 1, "{:?}", response.remark);
    assert_eq!(
        response.remark.as_deref(),
        Some("look message by offset failed, 9999")
    );
    assert!(records(&broker, "%RETRY%Known", 0).is_empty());
    server.stop().await;
}

#[tokio::test]
async fn producer_retry_send_past_the_limit_goes_straight_to_the_dead_letter() {
    let dir = TempDir::new().unwrap();
    let (broker, server) = started(&dir).await;
    let response = server
        .request(
            send("%RETRY%ProducerGroup", b"stale", "TAGS\x01blue\x02")
                .with_ext("reconsumeTimes", 17),
        )
        .await;
    assert_eq!(response.code, 0, "{:?}", response.remark);
    assert!(records(&broker, "%RETRY%ProducerGroup", 0).is_empty());
    let dead = records(&broker, "%DLQ%ProducerGroup", 0);
    assert_eq!(dead.len(), 1);
    assert_eq!(dead[0].properties["DELAY"], "0");
    // The response still reports the queue the producer asked for.
    assert_eq!(response.ext_fields["queueId"], "0");

    let response = server
        .request(
            send("%RETRY%ProducerGroup", b"retry", "TAGS\x01blue\x02")
                .with_ext("reconsumeTimes", 3),
        )
        .await;
    assert_eq!(response.code, 0, "{:?}", response.remark);
    assert_eq!(records(&broker, "%RETRY%ProducerGroup", 0).len(), 1);
    assert_eq!(records(&broker, "%DLQ%ProducerGroup", 0).len(), 1);
    server.stop().await;
}

#[tokio::test]
async fn heartbeat_creates_the_retry_topic_and_skips_groups_without_config() {
    let dir = TempDir::new().unwrap();
    let (broker, server) = started(&dir).await;
    let heartbeat = |group: &str| {
        Command::request(34).with_body(
            serde_json::to_vec(&json!({
                "clientID": format!("client-{group}"),
                "producerDataSet": [],
                "consumerDataSet": [{
                    "groupName": group,
                    "consumeType": "CONSUME_PASSIVELY",
                    "unitMode": false,
                    "subscriptionDataSet": [{
                        "topic": "Heartbeat",
                        "subString": "*",
                        "expressionType": "TAG",
                        "subVersion": 1
                    }]
                }]
            }))
            .unwrap(),
        )
    };
    let response = server.request(heartbeat("Orderly")).await;
    assert_eq!(response.code, 0, "{:?}", response.remark);
    let retry = broker.store().topics().unwrap().remove("%RETRY%Orderly");
    assert!(retry.is_some());

    // A forbidden group is refused with the group forbidden type, before the topic is read.
    let body =
        serde_json::to_vec(&json!({"groupName": "Forbidden", "consumeEnable": false})).unwrap();
    let response = server.request(Command::request(200).with_body(body)).await;
    assert_eq!(response.code, 0, "{:?}", response.remark);
    let response = server.request(pull("Forbidden", "Heartbeat", 0)).await;
    assert_eq!(response.code, 16, "{:?}", response.remark);
    assert_eq!(response.ext_fields["forbiddenType"], "2");
    server.stop().await;
}

#[tokio::test]
async fn subscription_group_admin_round_trip() {
    let dir = TempDir::new().unwrap();
    let (broker, server) = started(&dir).await;
    let body = serde_json::to_vec(&json!({
        "groupName": "AdminGroup",
        "retryQueueNums": 2,
        "retryMaxTimes": 5
    }))
    .unwrap();
    let response = server.request(Command::request(200).with_body(body)).await;
    assert_eq!(response.code, 0, "{:?}", response.remark);
    let group = broker.store().group("AdminGroup").unwrap().unwrap();
    assert_eq!(group.retry_queue_nums, 2);
    assert_eq!(group.retry_max_times, 5);
    assert_eq!(group.consume_timeout_minute, 15);

    // A Java `SubscriptionGroupConfig.encode()` body carries the retry policy and subscription
    // set, and both must survive the store round trip rather than being dropped.
    let body = serde_json::to_vec(&json!({
        "groupName": "JavaGroup",
        "consumeEnable": true,
        "groupRetryPolicy": {
            "type": "EXPONENTIAL",
            "exponentialRetryPolicy": {"initial": 1000, "max": 60000, "multiplier": 3},
            "customizedRetryPolicy": null
        },
        "subscriptionDataSet": [
            {"topic": "JavaTopic", "expressionType": "SQL92", "expression": "a > 1", "version": 17}
        ],
        "attributes": {"consumer.name": "order"}
    }))
    .unwrap();
    assert_eq!(
        server
            .request(Command::request(200).with_body(body))
            .await
            .code,
        0
    );
    let group = broker.store().group("JavaGroup").unwrap().unwrap();
    assert_eq!(
        group.group_retry_policy,
        GroupRetryPolicy {
            policy_type: GroupRetryPolicyType::EXPONENTIAL,
            exponential_retry_policy: Some(ExponentialRetryPolicy {
                initial: 1_000,
                max: 60_000,
                multiplier: 3
            }),
            customized_retry_policy: None
        }
    );
    assert_eq!(
        group.subscription_data_set,
        Some(vec![SimpleSubscriptionData {
            topic: "JavaTopic".into(),
            expression_type: Some("SQL92".into()),
            expression: Some("a > 1".into()),
            version: 17
        }])
    );
    assert_eq!(group.attributes["consumer.name"], "order");

    let response = server.request(Command::request(201)).await;
    assert_eq!(response.code, 0, "{:?}", response.remark);
    let listed: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
    assert_eq!(
        listed["subscriptionGroupTable"]["AdminGroup"]["retryMaxTimes"],
        5
    );
    assert_eq!(
        listed["subscriptionGroupTable"]["JavaGroup"]["groupRetryPolicy"]["type"],
        "EXPONENTIAL"
    );
    assert_eq!(
        listed["subscriptionGroupTable"]["JavaGroup"]["subscriptionDataSet"][0]["expression"],
        "a > 1"
    );

    // An illegal group name is rejected before it reaches the table.
    let body = serde_json::to_vec(&json!({"groupName": "bad group"})).unwrap();
    assert_eq!(
        server
            .request(Command::request(200).with_body(body))
            .await
            .code,
        3
    );

    broker
        .store()
        .commit_offset("AdminGroup", "AdminTopic", 0, 7)
        .unwrap();
    assert_eq!(
        broker
            .store()
            .consumer_offset("AdminGroup", "AdminTopic", 0)
            .unwrap(),
        Some(7)
    );
    let response = server
        .request(
            Command::request(207)
                .with_ext("groupName", "AdminGroup")
                .with_ext("cleanOffset", true),
        )
        .await;
    assert_eq!(response.code, 0, "{:?}", response.remark);
    assert!(broker.store().group("AdminGroup").unwrap().is_none());
    assert!(
        broker
            .store()
            .consumer_offset("AdminGroup", "AdminTopic", 0)
            .unwrap()
            .is_none()
    );

    let response = server.request(Command::request(45)).await;
    assert_eq!(response.code, 0, "{:?}", response.remark);
    let offsets: serde_json::Value = serde_json::from_slice(&response.body).unwrap();
    assert!(offsets["offsetTable"].is_object());
    assert!(offsets["dataVersion"].is_object());
    server.stop().await;
}

#[tokio::test]
async fn group_without_retry_queues_drops_the_send_back() {
    let dir = TempDir::new().unwrap();
    let (broker, server) = started(&dir).await;
    let body = serde_json::to_vec(&json!({"groupName": "NoRetry", "retryQueueNums": 0})).unwrap();
    let response = server.request(Command::request(200).with_body(body)).await;
    assert_eq!(response.code, 0, "{:?}", response.remark);
    let response = server
        .request(send("Short", b"body", "TAGS\x01blue\x02"))
        .await;
    assert_eq!(response.code, 0, "{:?}", response.remark);
    let original = records(&broker, "Short", 0).remove(0);
    let response = server
        .request(send_back("NoRetry", original.physical_offset, 0, 16))
        .await;
    assert_eq!(response.code, 0, "{:?}", response.remark);
    assert!(
        !broker
            .store()
            .topics()
            .unwrap()
            .contains_key("%RETRY%NoRetry")
    );
    assert!(records(&broker, "SCHEDULE_TOPIC_XXXX", 0).is_empty());
    server.stop().await;
}
