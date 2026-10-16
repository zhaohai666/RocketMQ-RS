//! Rust port of `org.apache.rocketmq.proxy`: the gRPC proxy that fronts broker/namesrv
//! with the Apache RocketMQ v2 gRPC API (`rocketmq-apis`).
//!
//! The protobuf definitions under `proto/apache/rocketmq/v2/*.proto` are compiled by
//! `build.rs` into Rust types + tonic service traits. This module then provides:
//! - [`ProxyConfig`] — the port of Java `ProxyConfig`
//! - [`ProxyContext`] — shared runtime state (config + op counters)
//! - [`ProxyBackend`] — the forwarding layer; speaks our existing `remoting` protocol to
//!   namesrv (code 105 route query) and brokers (send/pull/offset commands)
//! - [`PooledRemotingClient`] — pooled remoting channels towards namesrv/broker
//!   (port of Java `ProxyChannelPools`)
//! - [`MessagingProcessor`] — the `MessagingService` server (17 RPCs)
//! - [`AdminProcessor`] — the `Admin` server (16 RPCs)
//! - [`serve_proxy`] — binds one port and hosts both gRPC services
//! - [`serve_remoting_front`] — the remoting-protocol front end (port of Java
//!   `RemotingProtocolServer`/`RemotingActivatingHook`): forwards route queries to the
//!   namesrv and message commands to broker masters
//!
//! Fidelity notes (Java proxy is ~40k LOC; this is a best-effort port):
//! - **Real forwarding** for query_route / send_message / receive_message (pop-lite:
//!   pull + receipt-handle offset commit) / ack_message / pull_message / update_offset /
//!   get_offset / query_offset / get_topic_route / runtime stats.
//! - **Average-hash load balancing** (Java `AverageHashLoadBalancingStats`):
//!   `query_assignment` hands each consumer group the contiguous queue slice
//!   `[i*N/M, (i+1)*N/M)` of the topic's readable queues, where `i` is the group's
//!   index and `M` the configured `consumer_group_count`; with
//!   `high_accuracy_allocation` a consistent-hash ring picks the owning group per
//!   queue instead.
//! - **Pooled forwarding**: commands ride persistent connections from
//!   [`PooledRemotingClient`] (idle 30s, <=32 per address, one in-flight request per
//!   connection at a time) instead of one short connection per invoke.
//! - **ACL hook**: when `enable_acl` is set, `MessagingProcessor` verifies the gRPC
//!   `Authorization` metadata (Credential/Signature) against the `crate::auth` user
//!   store using `auth::cal_signature` before each RPC body is processed.
//! - **Simplified**: the consumer-group index is derived from a hash of the group
//!   name modulo a *locally configured* group count — Java negotiates the group
//!   distribution across the proxy cluster at runtime, which a single process cannot
//!   observe; ACL signature content is the empty byte string (see [`AclGuard`]).
//! - **Stubs**: RPCs that need broker features this port does not implement (POP
//!   receipts, transactions, DLQ forwarding, lite topics, client introspection) answer
//!   with typed `NOT_IMPLEMENTED` statuses so the service surface stays complete.

#[allow(clippy::all)]
pub mod pb {
    include!(concat!(env!("OUT_DIR"), "/apache.rocketmq.v2.rs"));
}

use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::mpsc;
use tokio::sync::oneshot;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_stream::StreamExt;
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::{Request, Response, Status, Streaming};
use tracing::{info, warn};

use crate::auth;
use crate::common::{self, Error, Result};
use crate::remoting::{self, Command};

// ---------------------------------------------------------------------------
// ProxyConfig (Java `ProxyConfig`)
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct ProxyConfig {
    pub rocketmq_cluster_name: String,
    /// `host:port` entries of the namesrv cluster the proxy forwards route queries to.
    pub name_server_address: Vec<String>,
    pub name_server_domain: String,
    /// gRPC listen port; the single port serving both v2 services.
    pub listen_port: u16,
    /// Reserved remoting-port forwarding (the Java proxy also opens a remoting front end).
    pub remoting_listen_port: u16,
    pub proxy_cluster_name: String,
    pub proxy_name: String,
    pub local_serve_addr: String,
    pub enable_acl: bool,
    pub enable_trace: bool,
    pub high_accuracy_allocation: bool,
    pub load_balancing_strategy: String,
    /// Producer group stamped on messages forwarded with `SEND_MESSAGE`.
    pub default_producer_group: String,
    /// Per-command forwarding timeout towards namesrv/broker.
    pub forward_timeout_millis: u64,
    /// Number of consumer groups the average-hash balancer should assume for this
    /// proxy process (Java `AverageHashLoadBalancingStats` learns the group count
    /// through cluster-wide negotiation, which a single process cannot observe; the
    /// group index is `hash(group) % consumer_group_count`). `None` falls back to 1,
    /// i.e. this proxy owns every queue — the pre-balancing behavior.
    pub consumer_group_count: Option<u32>,
}

impl Default for ProxyConfig {
    fn default() -> Self {
        Self {
            rocketmq_cluster_name: "DefaultCluster".into(),
            name_server_address: Vec::new(),
            name_server_domain: String::new(),
            listen_port: 8081,
            remoting_listen_port: 8080,
            proxy_cluster_name: "DefaultProxyCluster".into(),
            proxy_name: "DefaultProxy".into(),
            local_serve_addr: String::new(),
            enable_acl: false,
            enable_trace: false,
            high_accuracy_allocation: false,
            load_balancing_strategy: "average_hash".into(),
            default_producer_group: "PID_DEFAULT".into(),
            forward_timeout_millis: 3_000,
            consumer_group_count: None,
        }
    }
}

impl ProxyConfig {
    /// Java `ProxyConfig.load`: read a JSON config file.
    pub fn load(path: &str) -> Result<Self> {
        let raw = std::fs::read_to_string(path)?;
        Ok(serde_json::from_str(&raw)?)
    }

    pub fn validate(&self) -> Result<()> {
        if self.listen_port == 0 {
            return Err(Error::Invalid("listenPort must be positive".into()));
        }
        for ns in &self.name_server_address {
            if ns.split(':').count() != 2 {
                return Err(Error::Invalid(format!("invalid nameserver address `{ns}`")));
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// ProxyContext (Java `ProxyContext` + `ProxyStatsManager`)
// ---------------------------------------------------------------------------

/// Shared proxy runtime state: config plus per-operation counters.
pub struct ProxyContext {
    pub config: ProxyConfig,
    /// Registry of connected 5.x clients (keyed by the `x-mq-client-id` metadata
    /// header), powering heartbeat/termination lifecycle, real consumer-group load
    /// balancing, server→client telemetry command dispatch and admin subscription
    /// introspection.
    pub(crate) clients: Arc<ClientRegistry>,
    started_at: Instant,
    send_ops: AtomicU64,
    receive_ops: AtomicU64,
    ack_ops: AtomicU64,
    errors: AtomicU64,
}

impl ProxyContext {
    pub fn new(config: ProxyConfig) -> Arc<Self> {
        Arc::new(Self {
            config,
            clients: Arc::new(ClientRegistry::default()),
            started_at: Instant::now(),
            send_ops: AtomicU64::new(0),
            receive_ops: AtomicU64::new(0),
            ack_ops: AtomicU64::new(0),
            errors: AtomicU64::new(0),
        })
    }

    fn record_op(&self, op: &AtomicU64) {
        op.fetch_add(1, Ordering::Relaxed);
    }

    fn record_error(&self) {
        self.errors.fetch_add(1, Ordering::Relaxed);
    }

    /// Counters served by `get_proxy_runtime_stats`.
    pub fn runtime_stats(&self) -> serde_json::Value {
        serde_json::json!({
            "proxyName": self.config.proxy_name,
            "clusterName": self.config.proxy_cluster_name,
            "uptimeSeconds": self.started_at.elapsed().as_secs(),
            "sendOps": self.send_ops.load(Ordering::Relaxed),
            "receiveOps": self.receive_ops.load(Ordering::Relaxed),
            "ackOps": self.ack_ops.load(Ordering::Relaxed),
            "errors": self.errors.load(Ordering::Relaxed),
        })
    }
}

// ---------------------------------------------------------------------------
// Client registry (ports Java `ClientManager` + telemetry session tracking)
// ---------------------------------------------------------------------------

/// Server→client telemetry command sender. `None` until the client opens its
/// `Telemetry` bidi stream; the server uses it to push `TelemetryCommand`s
/// (recover-orphaned-transaction, verify-message, reconnect, ...). The channel
/// carries `Result<TelemetryCommand, Status>` to match the `ReceiverStream`
/// the gRPC handler returns.
type TelemetrySender = mpsc::Sender<std::result::Result<pb::TelemetryCommand, Status>>;

/// Monotonic nonce source for server→client telemetry request/reply correlation
/// (thread-stack-trace / verify-message). Pushed commands carry a nonce the client
/// echoes back; the waiting Admin RPC matches it here.
static NONCE_COUNTER: AtomicU64 = AtomicU64::new(1);

fn next_nonce() -> String {
    format!("rmq-{}-{}", std::process::id(), NONCE_COUNTER.fetch_add(1, Ordering::SeqCst))
}

/// Outcome a waiting Admin RPC expects back from a client telemetry reply.
enum PendingOutcome {
    StackTrace(Option<String>),
    Verify(bool),
}

/// One in-flight server→client telemetry request awaiting its client reply.
struct PendingReply {
    tx: oneshot::Sender<PendingOutcome>,
}

/// Live state for one connected client (producer or consumer), keyed by the
/// `x-mq-client-id` metadata header that the 5.x SDKs attach to every RPC.
#[derive(Clone)]
struct ClientState {
    client_id: String,
    client_type: i32, // pb::ClientType
    group: String,
    language: String,
    version: String,
    hostname: String,
    /// Consumer subscriptions: topic -> filter expression.
    subscriptions: HashMap<String, pb::FilterExpression>,
    /// Producer topics.
    publish_topics: Vec<String>,
    /// Lite subscription sets keyed by bind topic (lite push consumers only).
    lite_topics: HashMap<String, Vec<String>>,
    message_model: i32, // pb::MessageModel
    last_heartbeat: Instant,
    telemetry: Option<TelemetrySender>,
}

impl Default for ClientState {
    fn default() -> Self {
        Self {
            client_id: String::new(),
            client_type: 0,
            group: String::new(),
            language: String::new(),
            version: String::new(),
            hostname: String::new(),
            subscriptions: HashMap::new(),
            publish_topics: Vec::new(),
            lite_topics: HashMap::new(),
            message_model: 0,
            last_heartbeat: Instant::now(),
            telemetry: None,
        }
    }
}

#[derive(Default)]
pub(crate) struct ClientRegistry {
    by_id: Mutex<HashMap<String, ClientState>>,
    /// group -> client ids currently connected (registered via heartbeat/telemetry).
    group_members: Mutex<HashMap<String, Vec<String>>>,
    /// nonce -> waiting Admin RPC awaiting a client telemetry reply (thread dump / verify).
    pending: Mutex<HashMap<String, PendingReply>>,
}

impl ClientRegistry {
    /// Insert or refresh a client. Preserves any already-open telemetry channel so
    /// re-registration (heartbeat vs telemetry settings) does not drop the stream.
    fn register(&self, mut state: ClientState) {
        let mut by_id = self.by_id.lock().unwrap();
        if let Some(prev) = by_id.get(&state.client_id) {
            state.telemetry = prev.telemetry.clone();
        }
        if !state.group.is_empty() {
            let mut groups = self.group_members.lock().unwrap();
            let entry = groups.entry(state.group.clone()).or_default();
            if !entry.contains(&state.client_id) {
                entry.push(state.client_id.clone());
            }
        }
        by_id.insert(state.client_id.clone(), state);
    }

    fn unregister(&self, client_id: &str) {
        let mut by_id = self.by_id.lock().unwrap();
        if let Some(state) = by_id.remove(client_id) {
            if !state.group.is_empty() {
                let mut groups = self.group_members.lock().unwrap();
                if let Some(members) = groups.get_mut(&state.group) {
                    members.retain(|id| id != client_id);
                    if members.is_empty() {
                        groups.remove(&state.group);
                    }
                }
            }
        }
    }

    fn set_telemetry(&self, client_id: &str, tx: TelemetrySender) {
        let mut by_id = self.by_id.lock().unwrap();
        match by_id.get_mut(client_id) {
            Some(state) => state.telemetry = Some(tx),
            None => {
                let mut state = ClientState::default();
                state.client_id = client_id.to_string();
                state.last_heartbeat = Instant::now();
                state.telemetry = Some(tx);
                by_id.insert(client_id.to_string(), state);
            }
        }
    }

    fn clear_telemetry(&self, client_id: &str) {
        let mut by_id = self.by_id.lock().unwrap();
        if let Some(state) = by_id.get_mut(client_id) {
            state.telemetry = None;
            // Drop seed entries that never heart-beated.
            if state.group.is_empty()
                && state.subscriptions.is_empty()
                && state.publish_topics.is_empty()
            {
                by_id.remove(client_id);
            }
        }
    }

    /// Push a telemetry command to one client; false if it has no open stream.
    fn push_command(&self, client_id: &str, command: pb::TelemetryCommand) -> bool {
        let by_id = self.by_id.lock().unwrap();
        match by_id.get(client_id).and_then(|s| s.telemetry.clone()) {
            Some(tx) => tx.try_send(Ok(command)).is_ok(),
            None => false,
        }
    }

    /// Push a telemetry command to every connected client of `group`.
    fn push_to_group(&self, group: &str, command: pb::TelemetryCommand) {
        let members = self.group_members.lock().unwrap();
        if let Some(ids) = members.get(group) {
            for id in ids {
                self.push_command(id, command.clone());
            }
        }
    }

    fn group_member_count(&self, group: &str) -> usize {
        self.group_members
            .lock()
            .unwrap()
            .get(group)
            .map(Vec::len)
            .unwrap_or(0)
    }

    /// Stable position of `client_id` within its group's (sorted) member list,
    /// used for deterministic, collision-free load balancing. `None` when the
    /// client is unknown (e.g. assignment before heartbeat).
    fn group_index_of(&self, group: &str, client_id: &str) -> Option<usize> {
        let members = self.group_members.lock().unwrap();
        members.get(group).and_then(|ids| {
            let mut sorted = ids.clone();
            sorted.sort();
            sorted.iter().position(|id| id == client_id)
        })
    }

    /// Consumer subscription relationships: (group, topic, expression, online, model).
    fn list_subscriptions(&self) -> Vec<(String, String, pb::FilterExpression, bool, i32)> {
        let by_id = self.by_id.lock().unwrap();
        let mut out = Vec::new();
        let mut seen = std::collections::HashSet::new();
        for state in by_id.values() {
            let online = state.telemetry.is_some();
            for (topic, expr) in &state.subscriptions {
                if seen.insert((state.group.clone(), topic.clone())) {
                    out.push((
                        state.group.clone(),
                        topic.clone(),
                        expr.clone(),
                        online,
                        state.message_model,
                    ));
                }
            }
        }
        out
    }

    /// Per-client subscription view for `DescribeSubscription`.
    fn describe_subscriptions(&self) -> Vec<(ClientState, Vec<(String, pb::FilterExpression)>)> {
        let by_id = self.by_id.lock().unwrap();
        by_id
            .values()
            .map(|state| {
                let subs = state
                    .subscriptions
                    .iter()
                    .map(|(t, e)| (t.clone(), e.clone()))
                    .collect();
                (state.clone(), subs)
            })
            .collect()
    }

    fn delete_subscription(&self, group: &str, topic: &str) {
        let mut by_id = self.by_id.lock().unwrap();
        for state in by_id.values_mut() {
            if state.group == group {
                state.subscriptions.remove(topic);
            }
        }
    }

    /// Union of every lite topic a single client subscribed to via
    /// `SyncLiteSubscription`. LITE push consumers receive from this set instead
    /// of a single `MessageQueue`.
    fn lite_topics_for(&self, client_id: &str) -> Vec<String> {
        let by_id = self.by_id.lock().unwrap();
        let mut out = Vec::new();
        if let Some(state) = by_id.get(client_id) {
            for set in state.lite_topics.values() {
                for topic in set {
                    if !out.contains(topic) {
                        out.push(topic.clone());
                    }
                }
            }
        }
        out
    }

    /// Union of lite topics across every client currently registered under `group`.
    /// Fallback used when `receive_message` carries a group but no resolvable client
    /// id, so lite consumers still get fanned out across their topics.
    fn lite_topics_for_group(&self, group: &str) -> Vec<String> {
        let by_id = self.by_id.lock().unwrap();
        let mut out = Vec::new();
        for state in by_id.values() {
            if state.group == group {
                for set in state.lite_topics.values() {
                    for topic in set {
                        if !out.contains(topic) {
                            out.push(topic.clone());
                        }
                    }
                }
            }
        }
        out
    }

    /// Apply a `SyncLiteSubscription` action to a client's lite topic sets and return
    /// the topics that were *removed* (so the proxy can broadcast
    /// `NotifyUnsubscribeLiteCommand` to the rest of the group). Action codes follow
    /// `LiteSubscriptionAction`: 0=PARTIAL_ADD, 1=PARTIAL_REMOVE, 2=COMPLETE_ADD, 3=COMPLETE_REMOVE.
    fn apply_lite_subscription(
        &self,
        client_id: &str,
        group: &str,
        bind_topic: &str,
        action: i32,
        topics: &[String],
    ) -> Vec<String> {
        let removed = {
            let mut by_id = self.by_id.lock().unwrap();
            match by_id.get_mut(client_id) {
                Some(state) => {
                    state.group = group.to_string();
                    match action {
                        // COMPLETE_REMOVE: drop the entire bind topic.
                        3 => state.lite_topics.remove(bind_topic).unwrap_or_default(),
                        // PARTIAL_REMOVE: drop the named topics from the bind topic's set.
                        1 => {
                            let mut removed = Vec::new();
                            if let Some(set) = state.lite_topics.get_mut(bind_topic) {
                                for t in topics {
                                    if let Some(pos) = set.iter().position(|x| x == t) {
                                        set.remove(pos);
                                        removed.push(t.clone());
                                    }
                                }
                                if set.is_empty() {
                                    state.lite_topics.remove(bind_topic);
                                }
                            }
                            removed
                        }
                        // COMPLETE_ADD: replace the bind topic's set wholesale.
                        2 => {
                            state.lite_topics.insert(bind_topic.to_string(), topics.to_vec());
                            Vec::new()
                        }
                        // PARTIAL_ADD (default): union with the existing set.
                        _ => {
                            let entry = state.lite_topics.entry(bind_topic.to_string()).or_default();
                            for t in topics {
                                if !entry.contains(t) {
                                    entry.push(t.clone());
                                }
                            }
                            Vec::new()
                        }
                    }
                }
                None => {
                    let mut state = ClientState::default();
                    state.client_id = client_id.to_string();
                    state.group = group.to_string();
                    state.last_heartbeat = Instant::now();
                    // Nothing to remove from a brand-new client; seed the set on add.
                    if action == 2 || action == 0 {
                        state.lite_topics.insert(bind_topic.to_string(), topics.to_vec());
                    }
                    by_id.insert(client_id.to_string(), state);
                    Vec::new()
                }
            }
        };
        // Keep group membership bookkeeping consistent so load balancing / counts see
        // this client even before a heartbeat arrives.
        if !group.is_empty() {
            let mut groups = self.group_members.lock().unwrap();
            let entry = groups.entry(group.to_string()).or_default();
            if !entry.contains(&client_id.to_string()) {
                entry.push(client_id.to_string());
            }
        }
        removed
    }

    /// Broadcast a lite-topic unsubscribe to every *other* client in `group`, so peer
    /// lite consumers stop pulling the removed topic (Java `LiteSubscriptionManager`).
    fn notify_unsubscribe_lite(&self, group: &str, except: &str, topic: &str) {
        let members = self.group_members.lock().unwrap();
        if let Some(ids) = members.get(group) {
            for id in ids {
                if id == except {
                    continue;
                }
                let command = pb::TelemetryCommand {
                    command: Some(pb::telemetry_command::Command::NotifyUnsubscribeLiteCommand(
                        pb::NotifyUnsubscribeLiteCommand {
                            lite_topic: topic.to_string(),
                            ..Default::default()
                        },
                    )),
                    ..Default::default()
                };
                self.push_command(id, command);
            }
        }
    }

    /// Snapshot of every connected client registered under `group`.
    fn clients_for_group(&self, group: &str) -> Vec<ClientState> {
        let by_id = self.by_id.lock().unwrap();
        by_id
            .values()
            .filter(|s| s.group == group)
            .cloned()
            .collect()
    }

    /// Lookup a single client by id (for `GetConsumerRunningInfo`).
    fn client_by_id(&self, client_id: &str) -> Option<ClientState> {
        self.by_id.lock().unwrap().get(client_id).cloned()
    }

    /// Whether the client currently has an open telemetry (server→client) stream.
    fn has_telemetry(&self, client_id: &str) -> bool {
        self.by_id
            .lock()
            .unwrap()
            .get(client_id)
            .map(|s| s.telemetry.is_some())
            .unwrap_or(false)
    }

    /// Register a server→client telemetry request awaiting a reply keyed by `nonce`.
    /// Returns the receiver the Admin RPC awaits on.
    fn register_pending(&self, nonce: &str) -> oneshot::Receiver<PendingOutcome> {
        let (tx, rx) = oneshot::channel();
        self.pending
            .lock()
            .unwrap()
            .insert(nonce.to_string(), PendingReply { tx });
        rx
    }

    /// Resolve a waiting request when its client reply arrives. Returns false if no
    /// matching request was registered (e.g. already timed out and cancelled).
    fn resolve_pending(&self, nonce: &str, outcome: PendingOutcome) -> bool {
        if let Some(reply) = self.pending.lock().unwrap().remove(nonce) {
            let _ = reply.tx.send(outcome);
            true
        } else {
            false
        }
    }

    /// Drop a pending request without resolving it (e.g. the client stream vanished
    /// before the command could be sent). Returns false if already gone.
    fn cancel_pending(&self, nonce: &str) -> bool {
        self.pending.lock().unwrap().remove(nonce).is_some()
    }
}

/// Extract the `x-mq-client-id` metadata the 5.x clients attach to every RPC.
fn client_id_of<T>(request: &Request<T>) -> Option<String> {
    request
        .metadata()
        .get("x-mq-client-id")
        .and_then(|v| v.to_str().ok())
        .map(ToOwned::to_owned)
}

/// Merge a client-reported `Settings` (sent on the telemetry stream) into the registry.
fn apply_settings(registry: &ClientRegistry, client_id: &str, settings: &pb::Settings) {
    let mut state = {
        let by_id = registry.by_id.lock().unwrap();
        by_id.get(client_id).cloned().unwrap_or_else(|| {
            let mut s = ClientState::default();
            s.client_id = client_id.to_string();
            s.last_heartbeat = Instant::now();
            s
        })
    };
    if let Some(ct) = settings.client_type {
        state.client_type = ct;
    }
    if let Some(ua) = &settings.user_agent {
        state.language = pb::Language::try_from(ua.language)
            .map(|l| l.as_str_name().to_string())
            .unwrap_or_default();
        state.version = ua.version.clone();
        state.hostname = ua.hostname.clone();
    }
    if let Some(pub_sub) = &settings.pub_sub {
        match pub_sub {
            pb::settings::PubSub::Publishing(p) => {
                state.publish_topics = p.topics.iter().map(|t| t.name.clone()).collect();
            }
            pb::settings::PubSub::Subscription(s) => {
                if let Some(g) = &s.group {
                    state.group = g.name.clone();
                }
                for entry in &s.subscriptions {
                    if let Some(t) = &entry.topic {
                        state.subscriptions.insert(
                            t.name.clone(),
                            entry.expression.clone().unwrap_or_default(),
                        );
                    }
                }
            }
        }
    }
    registry.register(state);
}

/// Periodic orphaned-transaction recovery (Java `TransactionalMessageCheckService`).
///
/// Half messages that have aged past the transaction timeout and carry no matching
/// commit/rollback op are re-issued to their producer group via
/// `RecoverOrphanedTransactionCommand` over the telemetry stream, prompting the
/// client to run its `checkLocalTransaction` callback. Best-effort: broker errors
/// are ignored and the scan simply retries on the next interval.
async fn run_transaction_recovery(context: Arc<ProxyContext>) {
    const HALF_TOPIC: &str = "RMQ_SYS_TRANS_HALF_TOPIC";
    const CHECK_INTERVAL_SECS: u64 = 30;
    const ORPHAN_AGE_MS: i64 = 60_000;
    let backend = ProxyBackend::from_config(&context.config);
    loop {
        tokio::time::sleep(Duration::from_secs(CHECK_INTERVAL_SECS)).await;
        let res = backend
            .invoke_broker(
                HALF_TOPIC,
                11,
                &[
                    ("topic", HALF_TOPIC.to_string()),
                    ("consumerGroup", "__transaction_check".to_string()),
                    ("queueId", "-1".to_string()),
                    ("queueOffset", "0".to_string()),
                    ("maxMsgNums", "64".to_string()),
                    ("sysFlag", "4".to_string()),
                    ("expressionType", "TAG".to_string()),
                    ("subExpression", "*".to_string()),
                ],
                Vec::new(),
            )
            .await;
        let Ok(res) = res else { continue };
        if res.code != 0 {
            continue;
        }
        let mut rest = res.body.as_slice();
        while !rest.is_empty() {
            match common::Message::decode(rest) {
                Ok((msg, consumed)) if consumed > 0 => {
                    rest = &rest[consumed..];
                    let born = msg
                        .properties
                        .get("BORN_TIMESTAMP")
                        .and_then(|v| v.parse::<i64>().ok())
                        .unwrap_or(0);
                    if now_millis() - born < ORPHAN_AGE_MS {
                        continue;
                    }
                    let group = msg
                        .properties
                        .get("PRODUCER_GROUP")
                        .cloned()
                        .unwrap_or_default();
                    if group.is_empty() {
                        continue;
                    }
                    let pb_msg = to_pb_message(&msg, None);
                    let transaction_id = msg
                        .properties
                        .get("UNIQ_KEY")
                        .cloned()
                        .unwrap_or_else(|| format!("{}@{}", msg.topic, msg.queue_offset));
                    let command = pb::TelemetryCommand {
                        status: None,
                        command: Some(
                            pb::telemetry_command::Command::RecoverOrphanedTransactionCommand(
                                pb::RecoverOrphanedTransactionCommand {
                                    message: Some(pb_msg),
                                    transaction_id,
                                },
                            ),
                        ),
                    };
                    context.clients.push_to_group(&group, command);
                }
                Ok(_) => break,
                Err(_) => break,
            }
        }
    }
}

// ---------------------------------------------------------------------------
// PooledRemotingClient (Java `ProxyChannelPools`)
// ---------------------------------------------------------------------------

/// Idle connections older than this are closed on the next checkout / sweep.
const POOL_IDLE_TIMEOUT: Duration = Duration::from_secs(30);
/// Maximum concurrent checkouts per destination address; when the gate is full,
/// additional invokes park until a lease is returned (its gate permit released).
const POOL_MAX_PER_ADDR: usize = 32;
/// Soft cap on parked idle connections per address; extras are closed on return.
const POOL_MAX_IDLE_PER_ADDR: usize = 8;

/// A pooled connection sitting in an address' idle list.
struct PooledConnection {
    read: OwnedReadHalf,
    write: OwnedWriteHalf,
    last_used: Instant,
}

/// Per-destination pool. The gate semaphore bounds concurrent checkouts (in-flight
/// exchanges), not parked connections — a permit is taken at checkout and released
/// when the lease is returned or dropped, which is also what wakes parked waiters.
struct AddressPool {
    idle: Mutex<Vec<PooledConnection>>,
    gate: Arc<Semaphore>,
}

/// One connection checked out of a pool. Healthy exchanges return it via
/// [`PooledLease::give_back`]; dropping the lease closes the socket and releases
/// the gate slot.
struct PooledLease {
    read: OwnedReadHalf,
    write: OwnedWriteHalf,
    gate: Option<OwnedSemaphorePermit>,
    pool: Arc<AddressPool>,
}

impl PooledLease {
    /// Hand a healthy connection back to the idle list. The gate permit is dropped
    /// right after, waking at most one parked checkout.
    fn give_back(mut self) {
        let mut idle = self.pool.idle.lock().unwrap();
        idle.push(PooledConnection {
            read: self.read,
            write: self.write,
            last_used: Instant::now(),
        });
        while idle.len() > POOL_MAX_IDLE_PER_ADDR {
            idle.remove(0);
        }
        drop(idle);
        drop(self.gate.take());
    }
}

/// Pooled remoting client: one connection pool per destination `host:port`
/// (port of Java `ProxyChannelPools`). Each connection carries at most one
/// in-flight request at a time — a lease is exclusive between `write_command`
/// and the matching `read_command`, so responses never interleave.
///
/// Unlike Java's Netty channel pools this does not multiplex concurrent requests
/// over one channel; concurrency comes from the pool growing up to
/// [`POOL_MAX_PER_ADDR`] concurrent checkouts per address.
#[derive(Clone, Default)]
pub struct PooledRemotingClient {
    pools: Arc<Mutex<HashMap<String, Arc<AddressPool>>>>,
}

impl PooledRemotingClient {
    pub fn new() -> Self {
        Self {
            pools: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn pool_for(&self, addr: &str) -> Arc<AddressPool> {
        let mut pools = self.pools.lock().unwrap();
        Arc::clone(pools.entry(addr.to_string()).or_insert_with(|| {
            Arc::new(AddressPool {
                idle: Mutex::new(Vec::new()),
                gate: Arc::new(Semaphore::new(POOL_MAX_PER_ADDR)),
            })
        }))
    }

    /// Drop idle connections that outlived [`POOL_IDLE_TIMEOUT`].
    fn sweep_idle(pool: &AddressPool) {
        pool.idle
            .lock()
            .unwrap()
            .retain(|conn| conn.last_used.elapsed() < POOL_IDLE_TIMEOUT);
    }

    async fn checkout(&self, addr: &str) -> Result<PooledLease> {
        let pool = self.pool_for(addr);
        // Bound concurrency first; parked here while the address has
        // POOL_MAX_PER_ADDR exchanges in flight.
        let gate = Arc::clone(&pool.gate)
            .acquire_owned()
            .await
            .map_err(|_| Error::Invalid("connection pool closed".into()))?;
        loop {
            Self::sweep_idle(&pool);
            // Scope the idle lock so it is released before the lease (which moves
            // `pool`) is constructed.
            let conn = {
                let mut idle = pool.idle.lock().unwrap();
                idle.pop()
            };
            if let Some(conn) = conn {
                if conn.last_used.elapsed() < POOL_IDLE_TIMEOUT {
                    return Ok(PooledLease {
                        read: conn.read,
                        write: conn.write,
                        gate: Some(gate),
                        pool,
                    });
                }
                // Expired in the idle list: dropping it closes the socket.
                continue;
            }
            let stream = tokio::net::TcpStream::connect(addr).await?;
            stream.set_nodelay(true)?;
            let (read, write) = stream.into_split();
            return Ok(PooledLease {
                read,
                write,
                gate: Some(gate),
                pool,
            });
        }
    }

    /// Execute one two-way request/response exchange on a pooled connection.
    pub async fn invoke(&self, addr: &str, request: Command, timeout: Duration) -> Result<Command> {
        if request.flag & (remoting::RESPONSE_FLAG | remoting::ONEWAY_FLAG) != 0 {
            return Err(Error::Invalid("invoke requires a two-way request".into()));
        }
        let mut lease = match tokio::time::timeout(timeout, self.checkout(addr)).await {
            Ok(Ok(lease)) => lease,
            Ok(Err(error)) => return Err(error),
            Err(_) => return Err(Error::Timeout(format!("pool checkout timed out for {addr}"))),
        };
        let opaque = request.opaque;
        let exchange = async {
            remoting::write_command(&mut lease.write, &request).await?;
            let response = remoting::read_command(&mut lease.read)
                .await?
                .ok_or_else(|| {
                    Error::Io(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "pooled peer closed the connection",
                    ))
                })?;
            if response.flag & remoting::RESPONSE_FLAG == 0
                || response.flag & remoting::ONEWAY_FLAG != 0
            {
                return Err(Error::Invalid("pooled RPC response has invalid flags".into()));
            }
            if response.opaque != opaque {
                return Err(Error::Invalid("pooled RPC response opaque mismatch".into()));
            }
            Ok(response)
        };
        // Bind before matching: the awaited future borrows the lease mutably, and
        // match-scrutinee temporaries would keep that borrow alive across the arms.
        let outcome = tokio::time::timeout(timeout, exchange).await;
        match outcome {
            Ok(Ok(response)) => {
                lease.give_back();
                Ok(response)
            }
            Ok(Err(error)) => {
                // Connection state is unknown after a failed exchange: dropping the
                // lease closes the socket instead of returning it to the pool.
                drop(lease);
                Err(error)
            }
            Err(_) => {
                drop(lease);
                Err(Error::Timeout(format!("pooled RPC timed out for {addr}")))
            }
        }
    }
}

// ---------------------------------------------------------------------------
// ProxyBackend: forwarding over the remoting protocol
// ---------------------------------------------------------------------------

/// One `brokerDatas` entry of the namesrv route payload.
#[derive(Clone, Debug)]
pub struct BrokerRoute {
    pub cluster: String,
    pub name: String,
    /// brokerId -> address; `0` is the master.
    pub addrs: BTreeMap<i64, String>,
}

/// One `queueDatas` entry of the namesrv route payload.
#[derive(Clone, Debug)]
pub struct QueueRoute {
    pub broker_name: String,
    pub read_queues: i32,
    pub write_queues: i32,
    /// RocketMQ permission bits (`4` read, `2` write).
    pub perm: i32,
}

/// Forwards commands to namesrv/broker over the remoting protocol.
///
/// Connections ride [`PooledRemotingClient`] (the port of Java `ProxyChannelPools`);
/// each destination keeps up to 32 persistent channels, idle ones are reaped after
/// 30s.
pub struct ProxyBackend {
    client: PooledRemotingClient,
    nameservers: Vec<String>,
    timeout: Duration,
    cursor: AtomicUsize,
}

impl Clone for ProxyBackend {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            nameservers: self.nameservers.clone(),
            timeout: self.timeout,
            cursor: AtomicUsize::new(self.cursor.load(Ordering::Relaxed)),
        }
    }
}

impl ProxyBackend {
    pub fn new(mut nameservers: Vec<String>, timeout: Duration) -> Self {
        nameservers.retain(|ns| !ns.trim().is_empty());
        Self {
            client: PooledRemotingClient::new(),
            nameservers,
            timeout,
            cursor: AtomicUsize::new(0),
        }
    }

    pub fn from_config(config: &ProxyConfig) -> Self {
        Self::new(
            config.name_server_address.clone(),
            Duration::from_millis(config.forward_timeout_millis),
        )
    }

    /// Round-robin over namesrv addresses until one answers.
    pub async fn invoke_namesrv(
        &self,
        code: i32,
        ext: &[(&str, String)],
        body: Vec<u8>,
    ) -> Result<Command> {
        let mut request = Command::request(code);
        for (key, value) in ext {
            request = request.with_ext(*key, value.clone());
        }
        request = request.with_body(body);
        self.invoke_namesrv_command(request).await
    }

    /// Round-robin a fully built command over the pooled namesrv channels.
    pub async fn invoke_namesrv_command(&self, request: Command) -> Result<Command> {
        if self.nameservers.is_empty() {
            return Err(Error::Invalid(
                "proxy has no nameserver address configured".into(),
            ));
        }
        let start = self.cursor.fetch_add(1, Ordering::Relaxed) % self.nameservers.len();
        let mut last: Option<Error> = None;
        for offset in 0..self.nameservers.len() {
            let addr = &self.nameservers[(start + offset) % self.nameservers.len()];
            match self.client.invoke(addr, request.clone(), self.timeout).await {
                Ok(response) => return Ok(response),
                Err(error) => {
                    warn!(nameserver = %addr, %error, "namesrv invoke failed, trying next");
                    last = Some(error);
                }
            }
        }
        Err(last.unwrap_or(Error::Timeout("namesrv invoke failed".into())))
    }

    /// Parse the code-105 route payload into broker/queue tables.
    pub fn parse_route(body: &[u8]) -> Result<(Vec<BrokerRoute>, Vec<QueueRoute>)> {
        let value: serde_json::Value = serde_json::from_slice(body)?;
        let mut brokers = Vec::new();
        for data in value
            .get("brokerDatas")
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
        {
            let mut addrs = BTreeMap::new();
            for (id, addr) in data
                .get("brokerAddrs")
                .and_then(serde_json::Value::as_object)
                .into_iter()
                .flatten()
            {
                if let (Ok(id), Some(addr)) = (id.parse::<i64>(), addr.as_str()) {
                    addrs.insert(id, addr.to_string());
                }
            }
            brokers.push(BrokerRoute {
                cluster: data
                    .get("cluster")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default()
                    .into(),
                name: data
                    .get("brokerName")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default()
                    .into(),
                addrs,
            });
        }
        let mut queues = Vec::new();
        for data in value
            .get("queueDatas")
            .and_then(serde_json::Value::as_array)
            .into_iter()
            .flatten()
        {
            queues.push(QueueRoute {
                broker_name: data
                    .get("brokerName")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default()
                    .into(),
                read_queues: data
                    .get("readQueueNums")
                    .and_then(serde_json::Value::as_i64)
                    .unwrap_or(0) as i32,
                write_queues: data
                    .get("writeQueueNums")
                    .and_then(serde_json::Value::as_i64)
                    .unwrap_or(0) as i32,
                perm: data
                    .get("perm")
                    .and_then(serde_json::Value::as_i64)
                    .unwrap_or(0) as i32,
            });
        }
        Ok((brokers, queues))
    }

    /// Fetch the full route for a topic from the namesrv.
    pub async fn topic_route(&self, topic: &str) -> Result<(Vec<BrokerRoute>, Vec<QueueRoute>)> {
        let response = self
            .invoke_namesrv(105, &[("topic", topic.to_string())], Vec::new())
            .await?;
        if response.code != 0 {
            return Err(Error::NotFound(format!(
                "topic route query failed (code {}): {}",
                response.code,
                response.remark.as_deref().unwrap_or("")
            )));
        }
        Self::parse_route(&response.body)
    }

    /// Resolve a topic to its master broker address.
    pub async fn master_addr(&self, topic: &str) -> Result<String> {
        let (brokers, queues) = self.topic_route(topic).await?;
        if queues.is_empty() {
            return Err(Error::NotFound(format!("topic route not found: {topic}")));
        }
        let names: std::collections::BTreeSet<&str> =
            queues.iter().map(|q| q.broker_name.as_str()).collect();
        for broker in &brokers {
            if names.contains(broker.name.as_str()) {
                if let Some(addr) = broker.addrs.get(&0) {
                    return Ok(addr.clone());
                }
            }
        }
        Err(Error::NotFound(format!("no master broker for topic {topic}")))
    }

    /// Resolve the topic's master, then invoke a broker command there.
    pub async fn invoke_broker(
        &self,
        topic: &str,
        code: i32,
        ext: &[(&str, String)],
        body: Vec<u8>,
    ) -> Result<Command> {
        let mut request = Command::request(code);
        for (key, value) in ext {
            request = request.with_ext(*key, value.clone());
        }
        request = request.with_body(body);
        self.invoke_broker_command(topic, request).await
    }

    /// Resolve the topic's master, then forward a fully built command over the
    /// pooled channel towards it.
    pub async fn invoke_broker_command(&self, topic: &str, request: Command) -> Result<Command> {
        let addr = self.master_addr(topic).await?;
        self.client.invoke(&addr, request, self.timeout).await
    }

    /// Resolve the topic's declared message types from the broker's
    /// `GET_TOPIC_CONFIG_BY_TOPIC` (remoting code 351), which returns the
    /// `TopicConfig` JSON (its `topicSysFlag` low bits encode the type). Falls
    /// back to `[Normal]` whenever the broker is unreachable or the topic is new.
    pub async fn topic_message_types(&self, topic: &str) -> Vec<i32> {
        match self
            .invoke_broker(topic, 351, &[("topic", topic.to_string())], Vec::new())
            .await
        {
            Ok(response) if response.code == 0 => {
                let value: serde_json::Value = match serde_json::from_slice(&response.body) {
                    Ok(v) => v,
                    Err(_) => return vec![pb::MessageType::Normal as i32],
                };
                let sys_flag = value
                    .get("topicSysFlag")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(0) as i32;
                message_types_from_sys_flag(sys_flag)
            }
            _ => vec![pb::MessageType::Normal as i32],
        }
    }
}

// ---------------------------------------------------------------------------
// Conversion helpers (Java `GrpcConverter` / `MessageConverter`)
// ---------------------------------------------------------------------------

fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// A msg id ends with its physical offset in hex (8 bytes = 16 hex chars), which is how the
/// broker's END_TRANSACTION and every transactional client resolve a half message.
fn physical_offset_from_msg_id(msg_id: &str) -> Option<u64> {
    if msg_id.len() < 16 {
        return None;
    }
    u64::from_str_radix(&msg_id[msg_id.len() - 16..], 16).ok()
}

fn pb_status(code: pb::Code, message: impl Into<String>) -> pb::Status {
    pb::Status {
        code: code as i32,
        message: message.into(),
    }
}

/// Map a remoting response code to the v2 `Code` space.
fn map_response_code(code: i32) -> pb::Code {
    match code {
        0 => pb::Code::Ok,
        17 => pb::Code::TopicNotFound,
        22 => pb::Code::OffsetNotFound,
        16 | 26 => pb::Code::ConsumerGroupNotFound,
        3 => pb::Code::NotImplemented,
        29 | 13 | 10 | 11 => pb::Code::BadRequest,
        _ => pb::Code::InternalError,
    }
}

/// Decode the declared message types of a topic from its `topicSysFlag` low bits,
/// matching Java `TopicSysFlag.getTopicMessageType` (mask `0x0F`):
/// `0`=NORMAL, `1`=FIFO, `2`=DELAY, `4`=TRANSACTION, `8`=MIXED (all four).
/// `QueryRoute`/`QueryAssignment` advertise these on every `MessageQueue` so 5.x
/// clients reject messages of an unsupported type up front.
fn message_types_from_sys_flag(sys_flag: i32) -> Vec<i32> {
    match sys_flag & 0x0F {
        1 => vec![pb::MessageType::Fifo as i32],
        2 => vec![pb::MessageType::Delay as i32],
        4 => vec![pb::MessageType::Transaction as i32],
        8 => vec![
            pb::MessageType::Normal as i32,
            pb::MessageType::Fifo as i32,
            pb::MessageType::Delay as i32,
            pb::MessageType::Transaction as i32,
        ],
        _ => vec![pb::MessageType::Normal as i32],
    }
}

/// RocketMQ perm bits -> v2 `Permission`.
fn perm_to_permission(perm: i32) -> pb::Permission {
    match (perm & 4, perm & 2) {
        (4, 2) => pb::Permission::ReadWrite,
        (4, _) => pb::Permission::Read,
        (_, 2) => pb::Permission::Write,
        _ => pb::Permission::None,
    }
}

/// `host:port` -> v2 `Endpoints` (IPv4 scheme).
fn endpoints_from_addr(addr: &str) -> Option<pb::Endpoints> {
    let (host, port) = addr.rsplit_once(':')?;
    let port: i32 = port.parse().ok()?;
    Some(pb::Endpoints {
        scheme: pb::AddressScheme::IPv4 as i32,
        addresses: vec![pb::Address {
            host: host.to_string(),
            port,
        }],
    })
}

const PROPERTIES: [&str; 5] = ["TAGS", "KEYS", "UNIQ_KEY", "SHARDING_KEY", "RECONSUME_TIMES"];

/// Build the remoting `properties` header string from a v2 message.
fn encode_send_properties(message: &pb::Message) -> Result<String> {
    let mut properties: BTreeMap<String, String> = BTreeMap::new();
    if let Some(system) = &message.system_properties {
        if let Some(tag) = &system.tag {
            properties.insert("TAGS".into(), tag.clone());
        }
        if !system.keys.is_empty() {
            properties.insert("KEYS".into(), system.keys.join(" "));
        }
        if !system.message_id.is_empty() {
            properties.insert("UNIQ_KEY".into(), system.message_id.clone());
        }
        if let Some(group) = &system.message_group {
            properties.insert("SHARDING_KEY".into(), group.clone());
        }
        if let Some(attempt) = system.delivery_attempt {
            properties.insert("RECONSUME_TIMES".into(), attempt.to_string());
        }
    }
    for (key, value) in &message.user_properties {
        properties.insert(key.clone(), value.clone());
    }
    common::encode_properties(&properties)
}

/// Convert a decoded remoting message into a v2 gRPC `Message`.
fn to_pb_message(message: &common::Message, receipt_handle: Option<String>) -> pb::Message {
    let system = pb::SystemProperties {
        tag: message.properties.get("TAGS").cloned(),
        keys: message
            .properties
            .get("KEYS")
            .map(|keys| keys.split(' ').map(str::to_string).collect())
            .unwrap_or_default(),
        message_id: message.message_id(),
        body_digest: None,
        body_encoding: pb::Encoding::Identity as i32,
        message_type: pb::MessageType::Normal as i32,
        born_timestamp: Some(prost_types::Timestamp {
            seconds: message.born_timestamp / 1000,
            nanos: ((message.born_timestamp % 1000) * 1_000_000) as i32,
        }),
        born_host: message.born_host.to_string(),
        store_timestamp: Some(prost_types::Timestamp {
            seconds: message.store_timestamp / 1000,
            nanos: ((message.store_timestamp % 1000) * 1_000_000) as i32,
        }),
        store_host: message.store_host.to_string(),
        delivery_timestamp: None,
        receipt_handle,
        queue_id: message.queue_id,
        queue_offset: Some(message.queue_offset as i64),
        invisible_duration: None,
        delivery_attempt: Some(message.reconsume_times + 1),
        message_group: message.properties.get("SHARDING_KEY").cloned(),
        trace_context: None,
        orphaned_transaction_recovery_duration: None,
        dead_letter_queue: None,
        lite_topic: None,
        priority: None,
    };
    let user_properties: std::collections::HashMap<String, String> = message
        .properties
        .iter()
        .filter(|(k, _)| !PROPERTIES.contains(&k.as_str()))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    pb::Message {
        topic: Some(pb::Resource {
            resource_namespace: String::new(),
            name: message.topic.clone(),
        }),
        user_properties,
        system_properties: Some(system),
        body: message.body.clone(),
    }
}

/// Receipt handle for the pop-lite flow: opaque to clients, parseable here.
fn encode_receipt_handle(topic: &str, group: &str, queue_id: i32, offset: i64) -> String {
    format!("rh:{topic}:{group}:{queue_id}:{offset}")
}

fn decode_receipt_handle(handle: &str) -> Option<(String, String, i32, i64)> {
    let rest = handle.strip_prefix("rh:")?;
    let mut parts = rest.split(':');
    let topic = parts.next()?.to_string();
    let group = parts.next()?.to_string();
    let queue_id = parts.next()?.parse().ok()?;
    let offset = parts.next()?.parse().ok()?;
    Some((topic, group, queue_id, offset))
}

// ---------------------------------------------------------------------------
// Load balancing (Java `AverageHashLoadBalancingStats`)
// ---------------------------------------------------------------------------

/// FNV-1a 64-bit: stable across runs and platforms, good enough for ring/slice
/// placement (no crypto requirements in the balancer).
fn fnv1a(bytes: &[u8]) -> u64 {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

/// Number of consumer groups the average-hash balancer assumes for this proxy.
fn consumer_group_count(config: &ProxyConfig) -> u64 {
    u64::from(config.consumer_group_count.unwrap_or(1)).max(1)
}

/// Index of `group` in the average-hash distribution: `hash(group) % group_count`.
///
/// Java (`AverageHashLoadBalancingStats`) learns how the proxy cluster negotiated
/// its consumer-group distribution through runtime metadata shared between proxy
/// peers; a single process cannot observe sibling proxies, so the group index is
/// derived from a stable hash of the group name modulo a locally configured
/// `ProxyConfig::consumer_group_count` (default 1 = "this proxy owns every queue").
/// Every proxy in a cluster must run the same group count for the per-group queue
/// slices to partition cleanly.
fn group_index(config: &ProxyConfig, group: &str) -> u64 {
    fnv1a(group.as_bytes()) % consumer_group_count(config)
}

/// Contiguous queue slice `[i*N/M, (i+1)*N/M)` handed to group index `i` when the
/// topic has `N` readable queues and `M` groups — the average-hash allocation:
/// floor boundaries so the slices are disjoint and cover `[0, N)` exactly.
fn average_hash_slice(group_idx: u64, total: u64, groups: u64) -> std::ops::Range<u64> {
    let start = group_idx * total / groups;
    let end = (group_idx + 1) * total / groups;
    start..end.max(start)
}

/// Virtual nodes each group occupies on the consistent-hash ring.
const RING_VIRTUAL_NODES: u64 = 64;

/// Consistent-hash ring over group indexes (simplified Java
/// `ConsistentHashLoadBalancer`): every group index occupies
/// [`RING_VIRTUAL_NODES`] ring positions hashed with FNV-1a; `key` resolves to the
/// first ring position at or after `hash(key)`, wrapping around.
fn ring_lookup(groups: u64, key: &[u8]) -> u64 {
    if groups <= 1 {
        return 0;
    }
    let mut ring: Vec<(u64, u64)> =
        Vec::with_capacity((groups * RING_VIRTUAL_NODES) as usize);
    for group in 0..groups {
        for v in 0..RING_VIRTUAL_NODES {
            let label = format!("vn:{group}:{v}");
            ring.push((fnv1a(label.as_bytes()), group));
        }
    }
    ring.sort_unstable();
    let point = fnv1a(key);
    let idx = ring.partition_point(|(pos, _)| *pos < point);
    ring[idx % ring.len()].1
}

/// Whether this group index owns queue `(broker, id)` of `topic` under
/// `high_accuracy_allocation`: the consistent-hash ring decides the owning group
/// per queue instead of a group-wide slice.
fn ring_owns_queue(config: &ProxyConfig, topic: &str, broker: &str, id: i32, mine: u64) -> bool {
    let groups = consumer_group_count(config);
    let key = format!("{topic}:{broker}:{id}");
    ring_lookup(groups, key.as_bytes()) == mine
}

// ---------------------------------------------------------------------------
// ACL hook (Java `AuthenticationInterceptor` on the gRPC path)
// ---------------------------------------------------------------------------

/// gRPC ACL guard for `MessagingProcessor`. Java authenticates gRPC clients by
/// parsing the `Authorization` metadata — `MQv2 HMAC-SHA1 Credential=<ak>, ...,
/// Signature=<base64>` — and verifying the signature over the canonical signed
/// content with the user's secret key via `auth::cal_signature`.
///
/// Adaptation note: the canonical gRPC signing content (all signed metadata values
/// plus the length-prefixed request body, per `DefaultAuthenticationContextBuilder`)
/// cannot be reconstructed from tonic's decoded requests, so the guard verifies the
/// signature over the **empty byte string**. Clients must sign `b""` with
/// HMAC-SHA1/Base64 (`auth::cal_signature`); user lookup, disable-status checks and
/// the signature algorithm stay fully inside `crate::auth`, which this module does
/// not modify.
#[derive(Clone)]
struct AclGuard {
    enabled: bool,
    metadata: Arc<auth::LocalAuthenticationMetadataProvider>,
}

impl AclGuard {
    fn from_config(config: &ProxyConfig) -> Self {
        Self {
            enabled: config.enable_acl,
            metadata: auth::LocalAuthenticationMetadataProvider::new(),
        }
    }

    /// Register a user so its signatures can be verified (mirrors Java's
    /// user-management path; the proxy exposes no Admin RPC for it yet).
    #[allow(dead_code)]
    async fn register_user(&self, username: &str, secret: &str) -> Result<()> {
        self.metadata
            .create_user(auth::User::new(username).with_secret(secret))
            .await
    }

    /// Extract `Credential`/`Signature` tokens from an `Authorization` header.
    /// Tokens are comma/space separated `Key=Value` pairs with case-insensitive
    /// keys (tolerates both `Credential=` and Java's `AccessKey=` spellings).
    fn parse_authorization(header: &str) -> Option<(String, String)> {
        let mut access_key = None;
        let mut signature = None;
        for token in header.split([',', ' ']) {
            let token = token.trim();
            if token.is_empty() {
                continue;
            }
            // Tokens without `=` (scheme prefixes like "MQv2 HMAC-SHA1") are skipped;
            // a signature value's own `==` padding survives split_once (first `=` only).
            let Some((key, value)) = token.split_once('=') else {
                continue;
            };
            match key.trim().to_ascii_lowercase().as_str() {
                "credential" | "accesskey" => access_key = Some(value.trim().to_string()),
                "signature" => signature = Some(value.trim().to_string()),
                _ => {}
            }
        }
        Some((access_key?, signature?))
    }

    /// Constant-time comparison of two signature strings.
    fn signature_matches(expected: &str, actual: &str) -> bool {
        expected.len() == actual.len()
            && expected
                .bytes()
                .zip(actual.bytes())
                .fold(0u8, |diff, (a, b)| diff | (a ^ b))
                == 0
    }

    /// Verify the caller's signature before an RPC body is processed. Failures map
    /// to gRPC `UNAUTHENTICATED`/`PERMISSION_DENIED`; a disabled guard always passes.
    ///
    /// Only usable for requests whose body type is `Sync` (protobuf messages): the
    /// `&Request` borrow is held across the verification await. Streaming bodies
    /// (`Streaming<TelemetryCommand>`) are not `Sync` — use
    /// [`AclGuard::authorization_metadata`] + [`AclGuard::check_header`] there.
    async fn check_access<T>(
        &self,
        request: &Request<T>,
        rpc_code: &str,
    ) -> std::result::Result<(), Status> {
        if !self.enabled {
            return Ok(());
        }
        let header = Self::authorization_metadata(request);
        self.check_header(header, rpc_code).await
    }

    /// Pull the `Authorization` metadata out of a request synchronously, as an
    /// owned string, so it never has to be borrowed across an await point.
    fn authorization_metadata<T>(request: &Request<T>) -> Option<String> {
        request
            .metadata()
            .get("authorization")
            .and_then(|value| value.to_str().ok())
            .map(ToOwned::to_owned)
    }

    /// Verify an owned Authorization header. Unlike [`AclGuard::check_access`] this
    /// is safe to await from handlers of `Streaming` requests, whose `&Request`
    /// handles are not `Sync` (the future only carries the owned `Option<String>`).
    async fn check_header(
        &self,
        header: Option<String>,
        rpc_code: &str,
    ) -> std::result::Result<(), Status> {
        if !self.enabled {
            return Ok(());
        }
        let header = header
            .as_deref()
            .ok_or_else(|| Status::unauthenticated("missing Authorization metadata"))?;
        let _ = rpc_code; // per-RPC authorization lands with the policy evaluator
        let (access_key, signature) =
            Self::parse_authorization(header).ok_or_else(|| {
                Status::unauthenticated(
                    "Authorization must carry Credential=<accessKey> and Signature=<signature>",
                )
            })?;
        let user = self
            .metadata
            .get_user(&access_key)
            .await
            .map_err(|error| Status::internal(error.to_string()))?
            .ok_or_else(|| Status::unauthenticated(format!("user {access_key} is not found")))?;
        if user.user_status == auth::UserStatus::Disable {
            return Err(Status::permission_denied(format!(
                "user {access_key} is disabled"
            )));
        }
        let secret = user.password.as_deref().unwrap_or("");
        let expected = auth::cal_signature(&[], secret);
        if !Self::signature_matches(&expected, &signature) {
            return Err(Status::unauthenticated("check signature failed"));
        }
        Ok(())
    }

    /// Verify a remoting command's signature (used by [`serve_remoting_front`]).
    /// Unlike the gRPC path, the full canonical signing content IS available here
    /// via `auth::build_authentication_context` (sorted ext values + body).
    async fn check_command(&self, command: &Command) -> Result<()> {
        if !self.enabled {
            return Ok(());
        }
        let ctx = auth::build_authentication_context(command);
        if ctx.username.is_empty() {
            return Err(Error::Auth(format!(
                "command {} is missing the {ACCESS_KEY} field",
                command.code,
                ACCESS_KEY = auth::ACCESS_KEY
            )));
        }
        let user = self
            .metadata
            .get_user(&ctx.username)
            .await?
            .ok_or_else(|| Error::Auth(format!("User:{} is not found", ctx.username)))?;
        if user.user_status == auth::UserStatus::Disable {
            return Err(Error::Auth(format!("User:{} is disabled", ctx.username)));
        }
        let secret = user.password.as_deref().unwrap_or("");
        let expected = auth::cal_signature(&ctx.content, secret);
        if !Self::signature_matches(&expected, &ctx.signature) {
            return Err(Error::Auth("check signature failed".into()));
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// MessagingService (Java `MessagingProcessor`)
// ---------------------------------------------------------------------------

type PbResult<T> = std::result::Result<Response<T>, Status>;

/// Implements every client-facing v2 RPC by converting gRPC requests into remoting
/// commands and forwarding them through [`ProxyBackend`].
pub struct MessagingProcessor {
    context: Arc<ProxyContext>,
    backend: ProxyBackend,
    acl: AclGuard,
}

impl MessagingProcessor {
    pub fn new(context: Arc<ProxyContext>) -> Self {
        let backend = ProxyBackend::from_config(&context.config);
        let acl = AclGuard::from_config(&context.config);
        Self {
            context,
            backend,
            acl,
        }
    }

    /// Cheap handle clone for spawned stream tasks.
    fn clone_for_stream(&self) -> Self {
        Self {
            context: Arc::clone(&self.context),
            backend: self.backend.clone(),
            acl: self.acl.clone(),
        }
    }

    fn error_status(&self, error: &Error) -> Status {
        self.context.record_error();
        match error {
            Error::NotFound(msg) => Status::not_found(msg.clone()),
            Error::Timeout(msg) => Status::deadline_exceeded(msg.clone()),
            Error::Unsupported(msg) => Status::unimplemented(msg.clone()),
            Error::Invalid(msg) => Status::invalid_argument(msg.clone()),
            _ => Status::internal(error.to_string()),
        }
    }

    async fn committed_offset(&self, topic: &str, group: &str, queue_id: i32) -> Result<i64> {
        let response = self
            .backend
            .invoke_broker(
                topic,
                14,
                &[
                    ("topic", topic.to_string()),
                    ("consumerGroup", group.to_string()),
                    ("queueId", queue_id.to_string()),
                ],
                Vec::new(),
            )
            .await?;
        if response.code != 0 {
            // 22 = QUERY_NOT_FOUND: the group never committed; start from the beginning.
            return Ok(0);
        }
        Ok(response
            .ext_fields
            .get("offset")
            .and_then(|v| v.parse().ok())
            .unwrap_or(0))
    }

    async fn pull_once(
        &self,
        topic: &str,
        group: &str,
        queue_id: i32,
        offset: i64,
        batch: i32,
    ) -> Result<(Vec<common::Message>, i64, i64)> {
        let response = self
            .backend
            .invoke_broker(
                topic,
                11,
                &[
                    ("topic", topic.to_string()),
                    ("consumerGroup", group.to_string()),
                    ("queueId", queue_id.to_string()),
                    ("queueOffset", offset.to_string()),
                    ("maxMsgNums", batch.max(1).to_string()),
                    ("sysFlag", "0".to_string()),
                    ("expressionType", "TAG".to_string()),
                    ("subExpression", "*".to_string()),
                ],
                Vec::new(),
            )
            .await?;
        if response.code != 0 {
            return Err(Error::NotFound(format!(
                "pull failed (code {}): {}",
                response.code,
                response.remark.as_deref().unwrap_or("")
            )));
        }
        // The pull body is a sequence of RocketMQ message records (broker's
        // `Message::encode`); decode them with the shared `common::Message` codec.
        let mut messages = Vec::new();
        let mut rest = response.body.as_slice();
        while !rest.is_empty() {
            match common::Message::decode(rest) {
                Ok((message, consumed)) if consumed > 0 => {
                    messages.push(message);
                    rest = &rest[consumed..];
                }
                Ok(_) => break,
                Err(error) => {
                    warn!(%error, "stopping pull body decode at undecodable record");
                    break;
                }
            }
        }
        let next = response
            .ext_fields
            .get("nextBeginOffset")
            .and_then(|v| v.parse().ok())
            .unwrap_or(offset + messages.len() as i64);
        let max = response
            .ext_fields
            .get("maxOffset")
            .and_then(|v| v.parse().ok())
            .unwrap_or(next);
        Ok((messages, next, max))
    }
}

#[tonic::async_trait]
impl pb::messaging_service_server::MessagingService for MessagingProcessor {
    async fn query_route(
        &self,
        request: Request<pb::QueryRouteRequest>,
    ) -> PbResult<pb::QueryRouteResponse> {
        self.acl.check_access(&request, "QueryRoute").await?;
        let request = request.into_inner();
        let topic = request
            .topic
            .as_ref()
            .map(|t| t.name.clone())
            .unwrap_or_default();
        if topic.is_empty() {
            return Err(Status::invalid_argument("topic is required"));
        }
        let (brokers, queues) = self
            .backend
            .topic_route(&topic)
            .await
            .map_err(|e| self.error_status(&e))?;
        // Advertise the topic's real message types (FIFO/Delay/Transaction/Mixed)
        // instead of the previous always-NORMAL placeholder.
        let accept_message_types = self.backend.topic_message_types(&topic).await;
        let mut message_queues = Vec::new();
        for queue in &queues {
            let master = brokers
                .iter()
                .find(|b| b.name == queue.broker_name)
                .and_then(|b| b.addrs.get(&0))
                .cloned();
            let endpoints = master.as_deref().and_then(endpoints_from_addr);
            for id in 0..queue.read_queues.max(0) {
                message_queues.push(pb::MessageQueue {
                    topic: request.topic.clone(),
                    id,
                    permission: perm_to_permission(queue.perm) as i32,
                    broker: Some(pb::Broker {
                        name: queue.broker_name.clone(),
                        id: 0,
                        endpoints: endpoints.clone(),
                    }),
                    accept_message_types: accept_message_types.clone(),
                });
            }
        }
        Ok(Response::new(pb::QueryRouteResponse {
            status: Some(pb_status(pb::Code::Ok, "")),
            message_queues,
        }))
    }

    async fn heartbeat(
        &self,
        request: Request<pb::HeartbeatRequest>,
    ) -> PbResult<pb::HeartbeatResponse> {
        self.acl.check_access(&request, "Heartbeat").await?;
        let client_id = client_id_of(&request);
        let request = request.into_inner();
        let group = request
            .group
            .as_ref()
            .map(|g| g.name.clone())
            .unwrap_or_default();
        if let Some(id) = client_id {
            // Register/update the client so load balancing, subscription introspection
            // and telemetry command dispatch can see it (Java `ClientManager`).
            let mut state = ClientState::default();
            state.client_id = id;
            state.client_type = request.client_type;
            state.group = group;
            state.last_heartbeat = Instant::now();
            self.context.clients.register(state);
        }
        Ok(Response::new(pb::HeartbeatResponse {
            status: Some(pb_status(pb::Code::Ok, "")),
        }))
    }

    async fn send_message(
        &self,
        request: Request<pb::SendMessageRequest>,
    ) -> PbResult<pb::SendMessageResponse> {
        self.acl.check_access(&request, "SendMessage").await?;
        let request = request.into_inner();
        if request.messages.is_empty() {
            return Err(Status::invalid_argument("no messages in request"));
        }
        let mut entries = Vec::with_capacity(request.messages.len());
        for message in &request.messages {
            self.context.record_op(&self.context.send_ops);
            let topic = message
                .topic
                .as_ref()
                .map(|t| t.name.clone())
                .unwrap_or_default();
            if topic.is_empty() {
                entries.push(pb::SendResultEntry {
                    status: Some(pb_status(pb::Code::IllegalTopic, "missing topic")),
                    ..Default::default()
                });
                continue;
            }
            if message.body.is_empty() {
                entries.push(pb::SendResultEntry {
                    status: Some(pb_status(pb::Code::MessageBodyEmpty, "empty body")),
                    ..Default::default()
                });
                continue;
            }
            let properties = match encode_send_properties(message) {
                Ok(properties) => properties,
                Err(error) => {
                    entries.push(pb::SendResultEntry {
                        status: Some(pb_status(pb::Code::BadRequest, error.to_string())),
                        ..Default::default()
                    });
                    continue;
                }
            };
            let born = message
                .system_properties
                .as_ref()
                .and_then(|s| s.born_timestamp.as_ref())
                .map(|ts| ts.seconds * 1000 + (ts.nanos / 1_000_000) as i64)
                .unwrap_or_else(now_millis);
            let response = self
                .backend
                .invoke_broker(
                    &topic,
                    10,
                    &[
                        ("topic", topic.clone()),
                        (
                            "producerGroup",
                            self.context.config.default_producer_group.clone(),
                        ),
                        // -1 lets the broker round-robin the write queues.
                        ("queueId", "-1".to_string()),
                        ("sysFlag", "0".to_string()),
                        ("flag", "0".to_string()),
                        ("bornTimestamp", born.to_string()),
                        ("properties", properties),
                    ],
                    message.body.clone(),
                )
                .await;
            match response {
                Ok(response) if response.code == 0 => {
                    entries.push(pb::SendResultEntry {
                        status: Some(pb_status(pb::Code::Ok, "")),
                        message_id: response
                            .ext_fields
                            .get("msgId")
                            .cloned()
                            .unwrap_or_default(),
                        transaction_id: response
                            .ext_fields
                            .get("transactionId")
                            .cloned()
                            .unwrap_or_default(),
                        offset: response
                            .ext_fields
                            .get("queueOffset")
                            .and_then(|v| v.parse().ok())
                            .unwrap_or(0),
                        recall_handle: String::new(),
                    });
                }
                Ok(response) => entries.push(pb::SendResultEntry {
                    status: Some(pb_status(
                        map_response_code(response.code),
                        response.remark.as_deref().unwrap_or("send failed"),
                    )),
                    ..Default::default()
                }),
                Err(error) => {
                    self.context.record_error();
                    entries.push(pb::SendResultEntry {
                        status: Some(pb_status(pb::Code::InternalError, error.to_string())),
                        ..Default::default()
                    });
                }
            }
        }
        Ok(Response::new(pb::SendMessageResponse {
            status: Some(pb_status(pb::Code::Ok, "")),
            entries,
        }))
    }

    async fn query_assignment(
        &self,
        request: Request<pb::QueryAssignmentRequest>,
    ) -> PbResult<pb::QueryAssignmentResponse> {
        self.acl.check_access(&request, "QueryAssignment").await?;
        // Identify the caller so its real position within the connected group can be
        // used for load balancing (falls back to the static hash when unknown).
        let client_id = client_id_of(&request).unwrap_or_default();
        // Average-hash load balancing (Java `AverageHashLoadBalancingStats`): instead
        // of fanning one consumer over every readable queue, each consumer group owns
        // a contiguous slice `[i*N/M, (i+1)*N/M)` of the topic's `N` readable queues,
        // where `i` is the group's index and `M` the assumed group count. With
        // `high_accuracy_allocation` the owning group is chosen per queue through a
        // consistent-hash ring instead of a whole-group slice.
        let request = request.into_inner();
        let topic = request
            .topic
            .as_ref()
            .map(|t| t.name.clone())
            .unwrap_or_default();
        let group = request
            .group
            .as_ref()
            .map(|g| g.name.clone())
            .unwrap_or_default();
        let (brokers, queues) = self
            .backend
            .topic_route(&topic)
            .await
            .map_err(|e| self.error_status(&e))?;
        // Flatten the readable queues in route order; the slice indexes into this
        // stable ordering.
        let mut readable: Vec<(&QueueRoute, i32)> = Vec::new();
        for queue in &queues {
            for id in 0..queue.read_queues.max(0) {
                readable.push((queue, id));
            }
        }

        let config = &self.context.config;
        // Prefer the real connected-group membership for load balancing; fall back to
        // the statically configured group count when no client has heart-beated yet.
        let live = self.context.clients.group_member_count(&group);
        let groups = if live > 0 {
            live as u64
        } else {
            consumer_group_count(config)
        };
        let mine = match self.context.clients.group_index_of(&group, &client_id) {
            Some(idx) => idx as u64,
            None => group_index(config, &group),
        };
        let average_hash = config
            .load_balancing_strategy
            .eq_ignore_ascii_case("average_hash");
        let owned: Vec<(&QueueRoute, i32)> = if !average_hash || groups <= 1 {
            // Unknown strategy (or a single group): every readable queue is assigned,
            // preserving the pre-balancing fan-out behavior.
            readable.iter().copied().collect()
        } else if config.high_accuracy_allocation {
            readable
                .iter()
                .copied()
                .filter(|(queue, id)| {
                    ring_owns_queue(config, &topic, &queue.broker_name, *id, mine)
                })
                .collect()
        } else {
            let slice = average_hash_slice(mine, readable.len() as u64, groups);
            readable
                .into_iter()
                .enumerate()
                .filter(|(index, _)| {
                    let index = *index as u64;
                    index >= slice.start && index < slice.end
                })
                .map(|(_, item)| item)
                .collect()
        };

        // Same type fidelity as QueryRoute: advertise the topic's real message types.
        let accept_message_types = self.backend.topic_message_types(&topic).await;
        let mut assignments = Vec::new();
        for (queue, id) in owned {
            let master = brokers
                .iter()
                .find(|b| b.name == queue.broker_name)
                .and_then(|b| b.addrs.get(&0))
                .cloned();
            let endpoints = master.as_deref().and_then(endpoints_from_addr);
            assignments.push(pb::Assignment {
                message_queue: Some(pb::MessageQueue {
                    topic: request.topic.clone(),
                    id,
                    permission: perm_to_permission(queue.perm) as i32,
                    broker: Some(pb::Broker {
                        name: queue.broker_name.clone(),
                        id: 0,
                        endpoints: endpoints.clone(),
                    }),
                    accept_message_types: accept_message_types.clone(),
                }),
            });
        }
        Ok(Response::new(pb::QueryAssignmentResponse {
            status: Some(pb_status(pb::Code::Ok, "")),
            assignments,
        }))
    }

    type ReceiveMessageStream =
        ReceiverStream<std::result::Result<pb::ReceiveMessageResponse, Status>>;

    async fn receive_message(
        &self,
        request: Request<pb::ReceiveMessageRequest>,
    ) -> PbResult<Self::ReceiveMessageStream> {
        self.acl.check_access(&request, "ReceiveMessage").await?;
        let client_id = client_id_of(&request).unwrap_or_default();
        let request = request.into_inner();
        self.context.record_op(&self.context.receive_ops);
        let group = request
            .group
            .as_ref()
            .map(|g| g.name.clone())
            .unwrap_or_default();
        let (topic, queue_id) = request
            .message_queue
            .as_ref()
            .map(|mq| {
                (
                    mq.topic.as_ref().map(|t| t.name.clone()).unwrap_or_default(),
                    mq.id,
                )
            })
            .unwrap_or_default();
        // LITE push consumers fan out across their registered lite topic set, which may
        // leave the explicit `messageQueue` empty, so only the group is strictly required.
        if group.is_empty() {
            return Err(Status::invalid_argument("group and messageQueue are required"));
        }
        let batch = request.batch_size.max(1);
        let (tx, rx) = mpsc::channel::<std::result::Result<pb::ReceiveMessageResponse, Status>>(16);
        let this = Arc::new(self.clone_for_stream());
        tokio::spawn(async move {
            // Lite routing: a LITE push consumer receives from the union of topics it
            // registered via `SyncLiteSubscription`, not a single `MessageQueue`. We fan
            // out across every lite topic's readable queues, reusing the normal pull path
            // (the broker's lite guard only trips on `sysFlag & 16`/`liteTopic`, which we
            // never send) and per-topic receipt handles so `ack_message` still commits.
            let mut lite_topics = if !client_id.is_empty() {
                this.context.clients.lite_topics_for(&client_id)
            } else {
                Vec::new()
            };
            if lite_topics.is_empty() {
                lite_topics = this.context.clients.lite_topics_for_group(&group);
            }
            if !lite_topics.is_empty() {
                for lt in &lite_topics {
                    let queues = match this.backend.topic_route(lt).await {
                        Ok((_, queues)) => queues,
                        Err(_) => continue,
                    };
                    for queue in &queues {
                        for qid in 0..queue.read_queues.max(0) {
                            let offset = match this.committed_offset(lt, &group, qid).await {
                                Ok(offset) => offset,
                                Err(_) => continue,
                            };
                            match this.pull_once(lt, &group, qid, offset, batch).await {
                                Ok((messages, _, _)) => {
                                    for message in &messages {
                                        let handle = encode_receipt_handle(
                                            lt,
                                            &group,
                                            qid,
                                            message.queue_offset as i64,
                                        );
                                        let pb_message = to_pb_message(message, Some(handle));
                                        if tx
                                            .send(Ok(pb::ReceiveMessageResponse {
                                                content: Some(
                                                    pb::receive_message_response::Content::Message(
                                                        pb_message,
                                                    ),
                                                ),
                                            }))
                                            .await
                                            .is_err()
                                        {
                                            return;
                                        }
                                    }
                                }
                                Err(_) => continue,
                            }
                        }
                    }
                }
                let _ = tx
                    .send(Ok(pb::ReceiveMessageResponse {
                        content: Some(pb::receive_message_response::Content::Status(
                            pb_status(pb::Code::Ok, ""),
                        )),
                    }))
                    .await;
                return;
            }

            // Pop-lite: pull from the committed offset, hand each message a receipt
            // handle; `ack_message` commits the offset it encodes.
            let offset = match this.committed_offset(&topic, &group, queue_id).await {
                Ok(offset) => offset,
                Err(error) => {
                    let _ = tx.send(Err(Status::internal(error.to_string()))).await;
                    return;
                }
            };
            match this.pull_once(&topic, &group, queue_id, offset, batch).await {
                Ok((messages, _next, _max)) => {
                    for message in &messages {
                        let handle =
                            encode_receipt_handle(&topic, &group, queue_id, message.queue_offset as i64);
                        let pb_message = to_pb_message(message, Some(handle));
                        if tx
                            .send(Ok(pb::ReceiveMessageResponse {
                                content: Some(pb::receive_message_response::Content::Message(
                                    pb_message,
                                )),
                            }))
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                    let _ = tx
                        .send(Ok(pb::ReceiveMessageResponse {
                            content: Some(pb::receive_message_response::Content::Status(
                                pb_status(pb::Code::Ok, ""),
                            )),
                        }))
                        .await;
                }
                Err(error) => {
                    let _ = tx.send(Err(Status::internal(error.to_string()))).await;
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn ack_message(
        &self,
        request: Request<pb::AckMessageRequest>,
    ) -> PbResult<pb::AckMessageResponse> {
        self.acl.check_access(&request, "AckMessage").await?;
        let request = request.into_inner();
        self.context.record_op(&self.context.ack_ops);
        let topic = request
            .topic
            .as_ref()
            .map(|t| t.name.clone())
            .unwrap_or_default();
        let mut entries = Vec::with_capacity(request.entries.len());
        for entry in &request.entries {
            let status = match decode_receipt_handle(&entry.receipt_handle) {
                Some((_, group, queue_id, offset)) => {
                    let result = self
                        .backend
                        .invoke_broker(
                            &topic,
                            15,
                            &[
                                ("topic", topic.clone()),
                                ("consumerGroup", group),
                                ("queueId", queue_id.to_string()),
                                ("commitOffset", (offset + 1).to_string()),
                            ],
                            Vec::new(),
                        )
                        .await;
                    match result {
                        Ok(response) if response.code == 0 => pb_status(pb::Code::Ok, ""),
                        Ok(response) => pb_status(
                            map_response_code(response.code),
                            response.remark.as_deref().unwrap_or("ack failed"),
                        ),
                        Err(error) => pb_status(pb::Code::InternalError, error.to_string()),
                    }
                }
                None => pb_status(pb::Code::InvalidReceiptHandle, "unparseable receipt handle"),
            };
            entries.push(pb::AckMessageResultEntry {
                message_id: entry.message_id.clone(),
                receipt_handle: entry.receipt_handle.clone(),
                status: Some(status),
            });
        }
        Ok(Response::new(pb::AckMessageResponse {
            status: Some(pb_status(pb::Code::Ok, "")),
            entries,
        }))
    }

    async fn forward_message_to_dead_letter_queue(
        &self,
        request: Request<pb::ForwardMessageToDeadLetterQueueRequest>,
    ) -> PbResult<pb::ForwardMessageToDeadLetterQueueResponse> {
        self.acl
            .check_access(&request, "ForwardMessageToDeadLetterQueue")
            .await?;
        let request = request.into_inner();
        let group = request
            .group
            .as_ref()
            .map(|g| g.name.clone())
            .unwrap_or_default();
        let topic = request
            .topic
            .as_ref()
            .map(|t| t.name.clone())
            .unwrap_or_default();
        if group.is_empty() || topic.is_empty() {
            return Err(Status::invalid_argument("group and topic are required"));
        }
        // The pop-lite handle carries the origin queue coordinates; `delayLevel = -1` makes the
        // broker's send-back route straight into the group's dead-letter topic.
        let Some((_, _, queue_id, offset)) = decode_receipt_handle(&request.receipt_handle) else {
            return Ok(Response::new(pb::ForwardMessageToDeadLetterQueueResponse {
                status: Some(pb_status(
                    pb::Code::InvalidReceiptHandle,
                    "unparseable receipt handle",
                )),
            }));
        };
        let result = self
            .backend
            .invoke_broker(
                &topic,
                36,
                &[
                    ("group", group),
                    ("offset", offset.max(0).to_string()),
                    ("delayLevel", "-1".to_string()),
                    ("originTopic", topic.clone()),
                    ("queueId", queue_id.to_string()),
                    ("originMsgId", request.message_id.clone()),
                ],
                Vec::new(),
            )
            .await;
        match result {
            Ok(response) if response.code == 0 => {
                Ok(Response::new(pb::ForwardMessageToDeadLetterQueueResponse {
                    status: Some(pb_status(pb::Code::Ok, "")),
                }))
            }
            Ok(response) => Ok(Response::new(pb::ForwardMessageToDeadLetterQueueResponse {
                status: Some(pb_status(
                    map_response_code(response.code),
                    response
                        .remark
                        .as_deref()
                        .unwrap_or("dead letter forwarding failed"),
                )),
            })),
            Err(error) => Err(Status::internal(error.to_string())),
        }
    }

    type PullMessageStream =
        ReceiverStream<std::result::Result<pb::PullMessageResponse, Status>>;

    async fn pull_message(
        &self,
        request: Request<pb::PullMessageRequest>,
    ) -> PbResult<Self::PullMessageStream> {
        self.acl.check_access(&request, "PullMessage").await?;
        let request = request.into_inner();
        let group = request
            .group
            .as_ref()
            .map(|g| g.name.clone())
            .unwrap_or_default();
        let (topic, queue_id) = request
            .message_queue
            .as_ref()
            .map(|mq| {
                (
                    mq.topic.as_ref().map(|t| t.name.clone()).unwrap_or_default(),
                    mq.id,
                )
            })
            .unwrap_or_default();
        if topic.is_empty() || group.is_empty() {
            return Err(Status::invalid_argument("group and messageQueue are required"));
        }
        let offset = request.offset;
        let batch = request.batch_size.max(1);
        let (tx, rx) = mpsc::channel::<std::result::Result<pb::PullMessageResponse, Status>>(16);
        let this = Arc::new(self.clone_for_stream());
        tokio::spawn(async move {
            match this.pull_once(&topic, &group, queue_id, offset, batch).await {
                Ok((messages, next, max)) => {
                    for message in &messages {
                        if tx
                            .send(Ok(pb::PullMessageResponse {
                                content: Some(pb::pull_message_response::Content::Message(
                                    to_pb_message(message, None),
                                )),
                            }))
                            .await
                            .is_err()
                        {
                            return;
                        }
                    }
                    let _ = tx
                        .send(Ok(pb::PullMessageResponse {
                            content: Some(pb::pull_message_response::Content::Status(pb_status(
                                pb::Code::Ok,
                                "",
                            ))),
                        }))
                        .await;
                    let _ = tx
                        .send(Ok(pb::PullMessageResponse {
                            content: Some(pb::pull_message_response::Content::NextOffset(next.max(max))),
                        }))
                        .await;
                }
                Err(error) => {
                    let _ = tx.send(Err(Status::internal(error.to_string()))).await;
                }
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn update_offset(
        &self,
        request: Request<pb::UpdateOffsetRequest>,
    ) -> PbResult<pb::UpdateOffsetResponse> {
        self.acl.check_access(&request, "UpdateOffset").await?;
        let request = request.into_inner();
        let group = request
            .group
            .as_ref()
            .map(|g| g.name.clone())
            .unwrap_or_default();
        let (topic, queue_id) = request
            .message_queue
            .as_ref()
            .map(|mq| {
                (
                    mq.topic.as_ref().map(|t| t.name.clone()).unwrap_or_default(),
                    mq.id,
                )
            })
            .unwrap_or_default();
        self.backend
            .invoke_broker(
                &topic,
                15,
                &[
                    ("topic", topic.clone()),
                    ("consumerGroup", group),
                    ("queueId", queue_id.to_string()),
                    ("commitOffset", request.offset.to_string()),
                ],
                Vec::new(),
            )
            .await
            .map_err(|e| self.error_status(&e))?;
        Ok(Response::new(pb::UpdateOffsetResponse {
            status: Some(pb_status(pb::Code::Ok, "")),
        }))
    }

    async fn get_offset(
        &self,
        request: Request<pb::GetOffsetRequest>,
    ) -> PbResult<pb::GetOffsetResponse> {
        self.acl.check_access(&request, "GetOffset").await?;
        let request = request.into_inner();
        let group = request
            .group
            .as_ref()
            .map(|g| g.name.clone())
            .unwrap_or_default();
        let (topic, queue_id) = request
            .message_queue
            .as_ref()
            .map(|mq| {
                (
                    mq.topic.as_ref().map(|t| t.name.clone()).unwrap_or_default(),
                    mq.id,
                )
            })
            .unwrap_or_default();
        let offset = self
            .committed_offset(&topic, &group, queue_id)
            .await
            .map_err(|e| self.error_status(&e))?;
        Ok(Response::new(pb::GetOffsetResponse {
            status: Some(pb_status(pb::Code::Ok, "")),
            offset,
        }))
    }

    async fn query_offset(
        &self,
        request: Request<pb::QueryOffsetRequest>,
    ) -> PbResult<pb::QueryOffsetResponse> {
        self.acl.check_access(&request, "QueryOffset").await?;
        let request = request.into_inner();
        let (topic, queue_id) = request
            .message_queue
            .as_ref()
            .map(|mq| {
                (
                    mq.topic.as_ref().map(|t| t.name.clone()).unwrap_or_default(),
                    mq.id,
                )
            })
            .unwrap_or_default();
        let timestamp = request
            .timestamp
            .as_ref()
            .map(|ts| ts.seconds * 1000)
            .unwrap_or_else(now_millis);
        let response = self
            .backend
            .invoke_broker(
                &topic,
                29,
                &[
                    ("topic", topic.clone()),
                    ("queueId", queue_id.to_string()),
                    ("timestamp", timestamp.to_string()),
                ],
                Vec::new(),
            )
            .await
            .map_err(|e| self.error_status(&e))?;
        if response.code != 0 {
            return Ok(Response::new(pb::QueryOffsetResponse {
                status: Some(pb_status(
                    map_response_code(response.code),
                    response.remark.as_deref().unwrap_or("query offset failed"),
                )),
                offset: -1,
            }));
        }
        let offset = response
            .ext_fields
            .get("offset")
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);
        Ok(Response::new(pb::QueryOffsetResponse {
            status: Some(pb_status(pb::Code::Ok, "")),
            offset,
        }))
    }

    async fn end_transaction(
        &self,
        request: Request<pb::EndTransactionRequest>,
    ) -> PbResult<pb::EndTransactionResponse> {
        self.acl.check_access(&request, "EndTransaction").await?;
        let request = request.into_inner();
        let topic = request
            .topic
            .as_ref()
            .map(|t| t.name.clone())
            .unwrap_or_default();
        if topic.is_empty() {
            return Err(Status::invalid_argument("topic is required"));
        }
        // The send response's transaction id is the half message's msg id, whose tail carries
        // the physical offset the broker's END_TRANSACTION resolves the record by.
        let transaction_id = if request.transaction_id.is_empty() {
            request.message_id.clone()
        } else {
            request.transaction_id.clone()
        };
        let Some(offset) = physical_offset_from_msg_id(&transaction_id) else {
            return Ok(Response::new(pb::EndTransactionResponse {
                status: Some(pb_status(
                    pb::Code::InvalidReceiptHandle,
                    "unparseable transaction id",
                )),
                ..Default::default()
            }));
        };
        let commit_or_rollback = match request.resolution {
            1 => 0, // COMMIT
            2 => 1, // ROLLBACK
            _ => 2, // unspecified discards the half record, like the reference's NOT_TYPE
        };
        let result = self
            .backend
            .invoke_broker(
                &topic,
                37,
                &[
                    ("commitLogOffset", offset.to_string()),
                    ("commitOrRollback", commit_or_rollback.to_string()),
                ],
                Vec::new(),
            )
            .await;
        match result {
            Ok(response) if response.code == 0 => Ok(Response::new(pb::EndTransactionResponse {
                status: Some(pb_status(pb::Code::Ok, "")),
                ..Default::default()
            })),
            Ok(response) => Ok(Response::new(pb::EndTransactionResponse {
                status: Some(pb_status(
                    pb::Code::InternalError,
                    response.remark.as_deref().unwrap_or("end transaction failed"),
                )),
                ..Default::default()
            })),
            Err(error) => Err(Status::internal(error.to_string())),
        }
    }

    type TelemetryStream = ReceiverStream<std::result::Result<pb::TelemetryCommand, Status>>;

    async fn telemetry(
        &self,
        request: Request<Streaming<pb::TelemetryCommand>>,
    ) -> PbResult<Self::TelemetryStream> {
        // Java opens the bidi stream, pushes its `Settings` (backoff, TLS, metrics)
        // and then processes server-initiated telemetry commands (recover orphaned
        // transaction, verify message, reconnect, ...). This port mirrors that:
        // register the session so server commands can be pushed, send the initial
        // `Settings`, and keep the registry in sync from the client's `Settings`.
        // `Streaming` bodies are not `Sync`, so the id is taken from the owned header.
        let authorization = AclGuard::authorization_metadata(&request);
        self.acl.check_header(authorization, "Telemetry").await?;
        let client_id = client_id_of(&request).unwrap_or_default();
        let mut inbound = request.into_inner();
        let (tx, rx) = mpsc::channel::<std::result::Result<pb::TelemetryCommand, Status>>(16);
        let cid = client_id.clone();
        if !cid.is_empty() {
            self.context.clients.set_telemetry(&cid, tx.clone());
        }
        // Server -> client: initial settings handshake.
        let _ = tx
            .send(Ok(pb::TelemetryCommand {
                command: Some(pb::telemetry_command::Command::Settings(pb::Settings {
                    request_timeout: Some(prost_types::Duration {
                        seconds: 3,
                        nanos: 0,
                    }),
                    ..Default::default()
                })),
                ..Default::default()
            }))
            .await;
        // Client -> server: drain reports; refresh the registry from client Settings.
        let recv_clients = Arc::clone(&self.context.clients);
        tokio::spawn(async move {
            while let Some(item) = inbound.next().await {
                match item {
                    Ok(cmd) => {
                        if let Some(pb::telemetry_command::Command::Settings(settings)) =
                            cmd.command.clone()
                        {
                            apply_settings(&recv_clients, &cid, &settings);
                        }
                        // Client-side reports the server actively requested: resolve the
                        // waiting Admin RPC (thread stack trace / verify message) by nonce.
                        match cmd.command {
                            Some(pb::telemetry_command::Command::ThreadStackTrace(ts)) => {
                                recv_clients.resolve_pending(
                                    &ts.nonce,
                                    PendingOutcome::StackTrace(ts.thread_stack_trace),
                                );
                            }
                            Some(pb::telemetry_command::Command::VerifyMessageResult(r)) => {
                                recv_clients
                                    .resolve_pending(&r.nonce, PendingOutcome::Verify(true));
                            }
                            _ => {}
                        }
                    }
                    Err(_) => break,
                }
            }
            // Stream closed: drop the telemetry sender and seed entry.
            recv_clients.clear_telemetry(&cid);
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn notify_client_termination(
        &self,
        request: Request<pb::NotifyClientTerminationRequest>,
    ) -> PbResult<pb::NotifyClientTerminationResponse> {
        self.acl
            .check_access(&request, "NotifyClientTermination")
            .await?;
        if let Some(id) = client_id_of(&request) {
            self.context.clients.unregister(&id);
        }
        Ok(Response::new(pb::NotifyClientTerminationResponse {
            status: Some(pb_status(pb::Code::Ok, "")),
        }))
    }

    async fn change_invisible_duration(
        &self,
        request: Request<pb::ChangeInvisibleDurationRequest>,
    ) -> PbResult<pb::ChangeInvisibleDurationResponse> {
        self.acl
            .check_access(&request, "ChangeInvisibleDuration")
            .await?;
        let request = request.into_inner();
        let group = request
            .group
            .as_ref()
            .map(|g| g.name.clone())
            .unwrap_or_default();
        let topic = request
            .topic
            .as_ref()
            .map(|t| t.name.clone())
            .unwrap_or_default();
        if group.is_empty() || topic.is_empty() {
            return Err(Status::invalid_argument("group and topic are required"));
        }
        let duration_ms = request
            .invisible_duration
            .as_ref()
            .map(|duration| duration.seconds.saturating_mul(1000) + (duration.nanos / 1_000_000) as i64)
            .unwrap_or(0);
        // A broker pop receipt (comma-separated coordinates) forwards to the broker's
        // CHANGE_MESSAGE_INVISIBLETIME; the pull-based pop-lite handle has no invisible state
        // to mutate on this proxy.
        if request.receipt_handle.starts_with("rh:") {
            return Ok(Response::new(pb::ChangeInvisibleDurationResponse {
                status: Some(pb_status(
                    pb::Code::InvalidReceiptHandle,
                    "pull-based receipt handles have no invisible duration",
                )),
                ..Default::default()
            }));
        }
        let result = self
            .backend
            .invoke_broker(
                &topic,
                200053,
                &[
                    ("consumerGroup", group),
                    ("topic", topic.clone()),
                    ("extraInfo", request.receipt_handle),
                    ("invisibleTime", duration_ms.to_string()),
                ],
                Vec::new(),
            )
            .await;
        match result {
            Ok(response) if response.code == 0 => {
                Ok(Response::new(pb::ChangeInvisibleDurationResponse {
                    status: Some(pb_status(pb::Code::Ok, "")),
                    ..Default::default()
                }))
            }
            Ok(response) => Ok(Response::new(pb::ChangeInvisibleDurationResponse {
                status: Some(pb_status(
                    map_response_code(response.code),
                    response
                        .remark
                        .as_deref()
                        .unwrap_or("invisible duration change failed"),
                )),
                ..Default::default()
            })),
            Err(error) => Err(Status::internal(error.to_string())),
        }
    }

    async fn recall_message(
        &self,
        request: Request<pb::RecallMessageRequest>,
    ) -> PbResult<pb::RecallMessageResponse> {
        self.acl.check_access(&request, "RecallMessage").await?;
        let request = request.into_inner();
        let topic = request
            .topic
            .as_ref()
            .map(|t| t.name.clone())
            .unwrap_or_default();
        if topic.is_empty() {
            return Err(Status::invalid_argument("topic is required"));
        }
        // `RecallMessageProcessor` (code 370): the handle names the timer slot a delete record
        // is written into; the broker owns the recall semantics.
        let result = self
            .backend
            .invoke_broker(
                &topic,
                370,
                &[("topic", topic.clone()), ("recallHandle", request.recall_handle)],
                Vec::new(),
            )
            .await;
        match result {
            Ok(response) if response.code == 0 => {
                Ok(Response::new(pb::RecallMessageResponse {
                    status: Some(pb_status(pb::Code::Ok, "")),
                    message_id: response.ext_fields.get("msgId").cloned().unwrap_or_default(),
                }))
            }
            Ok(response) => Ok(Response::new(pb::RecallMessageResponse {
                status: Some(pb_status(
                    map_response_code(response.code),
                    response.remark.as_deref().unwrap_or("recall failed"),
                )),
                message_id: String::new(),
            })),
            Err(error) => Err(Status::internal(error.to_string())),
        }
    }

    async fn sync_lite_subscription(
        &self,
        request: Request<pb::SyncLiteSubscriptionRequest>,
    ) -> PbResult<pb::SyncLiteSubscriptionResponse> {
        self.acl
            .check_access(&request, "SyncLiteSubscription")
            .await?;
        let client_id = client_id_of(&request).unwrap_or_default();
        let request = request.into_inner();
        let group = request
            .group
            .as_ref()
            .map(|g| g.name.clone())
            .unwrap_or_default();
        let topic = request
            .topic
            .as_ref()
            .map(|t| t.name.clone())
            .unwrap_or_default();
        if !topic.is_empty() {
            // Honor the action (PARTIAL_ADD / PARTIAL_REMOVE / COMPLETE_ADD /
            // COMPLETE_REMOVE) instead of always overwriting, and broadcast an
            // unsubscribe to the rest of the group when topics are removed.
            let removed = self.context.clients.apply_lite_subscription(
                &client_id,
                &group,
                &topic,
                request.action,
                &request.lite_topic_set,
            );
            for removed_topic in &removed {
                self.context
                    .clients
                    .notify_unsubscribe_lite(&group, &client_id, removed_topic);
            }
        }
        Ok(Response::new(pb::SyncLiteSubscriptionResponse {
            status: Some(pb_status(pb::Code::Ok, "")),
        }))
    }
}

// ---------------------------------------------------------------------------
// Admin (Java `AdminManager`)
// ---------------------------------------------------------------------------

/// Control-plane RPCs. Core ops forward to namesrv; the rest answer typed-unimplemented
/// (they need client-state introspection this port does not track).
pub struct AdminProcessor {
    context: Arc<ProxyContext>,
    backend: ProxyBackend,
}

impl AdminProcessor {
    pub fn new(context: Arc<ProxyContext>) -> Self {
        let backend = ProxyBackend::from_config(&context.config);
        Self { context, backend }
    }
}

#[tonic::async_trait]
impl pb::admin_server::Admin for AdminProcessor {
    async fn change_log_level(
        &self,
        _request: Request<pb::ChangeLogLevelRequest>,
    ) -> PbResult<pb::ChangeLogLevelResponse> {
        Ok(Response::new(pb::ChangeLogLevelResponse {
            remark: "log level is fixed at startup".into(),
        }))
    }

    async fn describe_topic_status(
        &self,
        request: Request<pb::DescribeTopicStatusRequest>,
    ) -> PbResult<pb::DescribeTopicStatusResponse> {
        let request = request.into_inner();
        let topic = request
            .topic
            .as_ref()
            .map(|t| t.name.clone())
            .unwrap_or_default();
        let (_, queues) = self
            .backend
            .topic_route(&topic)
            .await
            .map_err(|_| Status::not_found(format!("topic route not found: {topic}")))?;
        if queues.is_empty() {
            return Err(Status::not_found(format!("topic does not exist: {topic}")));
        }
        let types = self.backend.topic_message_types(&topic).await;
        let topic_message_type = types
            .into_iter()
            .next()
            .unwrap_or(pb::MessageType::Normal as i32);
        Ok(Response::new(pb::DescribeTopicStatusResponse {
            status: Some(pb_status(pb::Code::Ok, "")),
            topic_message_type,
            ..Default::default()
        }))
    }

    async fn list_subscription(
        &self,
        _request: Request<pb::ListSubscriptionRequest>,
    ) -> PbResult<pb::ListSubscriptionResponse> {
        let mut info = Vec::new();
        for (group, topic, expr, online, model) in self.context.clients.list_subscriptions() {
            info.push(pb::SubscriptionInfo {
                group: Some(pb::Resource {
                    resource_namespace: String::new(),
                    name: group,
                }),
                topic: Some(pb::Resource {
                    resource_namespace: String::new(),
                    name: topic,
                }),
                expression: Some(expr),
                online,
                last_update_timestamp: Some(prost_types::Timestamp {
                    seconds: now_millis() / 1000,
                    nanos: 0,
                }),
                message_model: model,
                subscription_consistency: true,
            });
        }
        Ok(Response::new(pb::ListSubscriptionResponse {
            status: Some(pb_status(pb::Code::Ok, "")),
            subscription_info: info,
        }))
    }

    async fn describe_subscription(
        &self,
        _request: Request<pb::DescribeSubscriptionRequest>,
    ) -> PbResult<pb::DescribeSubscriptionResponse> {
        let mut client_sub = Vec::new();
        for (state, subs) in self.context.clients.describe_subscriptions() {
            for (topic, expr) in subs {
                let sub_info = pb::SubscriptionInfo {
                    group: Some(pb::Resource {
                        resource_namespace: String::new(),
                        name: state.group.clone(),
                    }),
                    topic: Some(pb::Resource {
                        resource_namespace: String::new(),
                        name: topic,
                    }),
                    expression: Some(expr),
                    online: state.telemetry.is_some(),
                    last_update_timestamp: Some(prost_types::Timestamp {
                        seconds: now_millis() / 1000,
                        nanos: 0,
                    }),
                    message_model: state.message_model,
                    subscription_consistency: true,
                };
                client_sub.push(pb::describe_subscription_response::ClientSubscriptionInfo {
                    client_info: Some(pb::ClientInfo {
                        client_id: state.client_id.clone(),
                        version: state.version.clone(),
                        language: state.language.clone(),
                        hostname: state.hostname.clone(),
                        egress_ip: String::new(),
                        message_model: state.message_model,
                    }),
                    subscription_info: Some(sub_info),
                });
            }
        }
        Ok(Response::new(pb::DescribeSubscriptionResponse {
            status: Some(pb_status(pb::Code::Ok, "")),
            client_subscription_info: client_sub,
        }))
    }

    async fn delete_subscription(
        &self,
        request: Request<pb::DeleteSubscriptionRequest>,
    ) -> PbResult<pb::DeleteSubscriptionResponse> {
        let request = request.into_inner();
        let group = request
            .group
            .as_ref()
            .map(|g| g.name.clone())
            .unwrap_or_default();
        let topic = request
            .topic
            .as_ref()
            .map(|t| t.name.clone())
            .unwrap_or_default();
        if !group.is_empty() && !topic.is_empty() {
            self.context.clients.delete_subscription(&group, &topic);
        }
        Ok(Response::new(pb::DeleteSubscriptionResponse {
            status: Some(pb_status(pb::Code::Ok, "")),
        }))
    }

    async fn describe_group_accumulation(
        &self,
        request: Request<pb::DescribeGroupAccumulationRequest>,
    ) -> PbResult<pb::DescribeGroupAccumulationResponse> {
        let request = request.into_inner();
        let group = request
            .group
            .as_ref()
            .map(|g| g.name.clone())
            .unwrap_or_default();
        if group.is_empty() {
            return Err(Status::invalid_argument("group is required"));
        }
        // Group-wide aggregation needs a broker-side offset table scan (code 43) that has no
        // topic to route through, so the request's topic set is required here.
        let topic_names: Vec<String> = request
            .topics
            .iter()
            .filter(|topic| !topic.name.is_empty())
            .map(|topic| topic.name.clone())
            .collect();
        if topic_names.is_empty() {
            return Err(Status::invalid_argument(
                "topics are required: group-wide aggregation needs a routable topic",
            ));
        }
        let mut total = pb::describe_group_accumulation_response::GroupAccumulation::default();
        let mut per_topic = std::collections::HashMap::new();
        for topic in &topic_names {
            let (brokers, queues) = self
                .backend
                .topic_route(topic)
                .await
                .map_err(|error| Status::internal(error.to_string()))?;
            let mut accumulation =
                pb::describe_group_accumulation_response::GroupAccumulation::default();
            for queue in &queues {
                // The master hosts the write path this port serves; read its counters.
                let addr = brokers
                    .iter()
                    .find(|broker| broker.name == queue.broker_name)
                    .and_then(|broker| broker.addrs.get(&0))
                    .cloned()
                    .ok_or_else(|| {
                        Status::not_found(format!("no master for queue of {topic}"))
                    })?;
                // A route entry carries the queue count per broker, not per-queue ids;
                // enumerate the readable positions like every other route consumer.
                for id in 0..queue.read_queues.max(0) {
                    let max_offset = self
                        .backend
                        .client
                        .invoke(
                            &addr,
                            Command::request(30)
                                .with_ext("topic", topic.clone())
                                .with_ext("queueId", id.to_string()),
                            self.backend.timeout,
                        )
                        .await
                        .ok()
                        .and_then(|response| response.ext_fields.get("offset").cloned())
                        .and_then(|value| value.parse::<i64>().ok())
                        .unwrap_or(0);
                    let committed = self
                        .backend
                        .client
                        .invoke(
                            &addr,
                            Command::request(14)
                                .with_ext("consumerGroup", group.clone())
                                .with_ext("topic", topic.clone())
                                .with_ext("queueId", id.to_string()),
                            self.backend.timeout,
                        )
                        .await
                        .ok()
                        .filter(|response| response.code == 0)
                        .and_then(|response| response.ext_fields.get("offset").cloned())
                        .and_then(|value| value.parse::<i64>().ok())
                        .unwrap_or(0);
                    let lag = (max_offset - committed).max(0);
                    accumulation.ready_messages += lag;
                    accumulation.accumulation += lag;
                    total.ready_messages += lag;
                    total.accumulation += lag;
                }
            }
            per_topic.insert(topic.clone(), accumulation);
        }
        Ok(Response::new(pb::DescribeGroupAccumulationResponse {
            status: Some(pb_status(pb::Code::Ok, "")),
            accumulation: Some(total),
            topic_accumulation: per_topic,
            lite_topic_accumulation: Default::default(),
        }))
    }

    async fn list_consumer_connection(
        &self,
        request: Request<pb::ListConsumerConnectionRequest>,
    ) -> PbResult<pb::ListConsumerConnectionResponse> {
        let request = request.into_inner();
        let group = request
            .group
            .as_ref()
            .map(|g| g.name.clone())
            .unwrap_or_default();
        let clients = self.context.clients.clients_for_group(&group);
        let client_info = clients
            .into_iter()
            .map(|c| pb::ClientInfo {
                client_id: c.client_id,
                version: c.version,
                language: c.language,
                hostname: c.hostname,
                egress_ip: String::new(),
                message_model: c.message_model,
            })
            .collect();
        Ok(Response::new(pb::ListConsumerConnectionResponse {
            status: Some(pb_status(pb::Code::Ok, "")),
            client_info,
        }))
    }

    async fn reset_group_offset(
        &self,
        request: Request<pb::ResetGroupOffsetRequest>,
    ) -> PbResult<pb::ResetGroupOffsetResponse> {
        let request = request.into_inner();
        let group = request
            .group
            .as_ref()
            .map(|g| g.name.clone())
            .unwrap_or_default();
        let topic = request
            .topic
            .as_ref()
            .map(|t| t.name.clone())
            .unwrap_or_default();
        if group.is_empty() || topic.is_empty() {
            return Err(Status::invalid_argument("group and topic are required"));
        }
        let Some(timestamp) = request.reset_timestamp.as_ref().map(|ts| ts.seconds) else {
            return Err(Status::invalid_argument("reset_timestamp is required"));
        };
        let (brokers, queues) = self
            .backend
            .topic_route(&topic)
            .await
            .map_err(|error| Status::internal(error.to_string()))?;
        for queue in &queues {
            let addr = brokers
                .iter()
                .find(|broker| broker.name == queue.broker_name)
                .and_then(|broker| broker.addrs.get(&0))
                .cloned()
                .ok_or_else(|| Status::not_found(format!("no master for queue of {topic}")))?;
            for id in 0..queue.read_queues.max(0) {
                // `SEARCH_OFFSET_BY_TIMESTAMP` finds the first record at or after the timestamp;
                // committing it makes every earlier message re-consumable.
                let searched = self
                    .backend
                    .client
                    .invoke(
                        &addr,
                        Command::request(29)
                            .with_ext("topic", topic.clone())
                            .with_ext("queueId", id.to_string())
                            .with_ext("timestamp", timestamp.to_string()),
                        self.backend.timeout,
                    )
                    .await
                    .map_err(|error| Status::internal(error.to_string()))?;
                if searched.code != 0 {
                    return Ok(Response::new(pb::ResetGroupOffsetResponse {
                        status: Some(pb_status(
                            map_response_code(searched.code),
                            searched
                                .remark
                                .as_deref()
                                .unwrap_or("offset search failed"),
                        )),
                    }));
                }
                let offset = searched
                    .ext_fields
                    .get("offset")
                    .and_then(|value| value.parse::<i64>().ok())
                    .unwrap_or(0);
                let committed = self
                    .backend
                    .client
                    .invoke(
                        &addr,
                        Command::request(15)
                            .with_ext("consumerGroup", group.clone())
                            .with_ext("topic", topic.clone())
                            .with_ext("queueId", id.to_string())
                            .with_ext("commitOffset", offset.to_string()),
                        self.backend.timeout,
                    )
                    .await
                    .map_err(|error| Status::internal(error.to_string()))?;
                if committed.code != 0 {
                    return Ok(Response::new(pb::ResetGroupOffsetResponse {
                        status: Some(pb_status(
                            map_response_code(committed.code),
                            committed
                                .remark
                                .as_deref()
                                .unwrap_or("offset commit failed"),
                        )),
                    }));
                }
            }
        }
        Ok(Response::new(pb::ResetGroupOffsetResponse {
            status: Some(pb_status(pb::Code::Ok, "")),
        }))
    }

    async fn query_message(
        &self,
        request: Request<pb::ListMessageRequest>,
    ) -> PbResult<pb::ListMessageResponse> {
        let request = request.into_inner();
        let topic = request
            .topic
            .as_ref()
            .map(|t| t.name.clone())
            .unwrap_or_default();
        if topic.is_empty() {
            return Err(Status::invalid_argument("topic is required"));
        }
        // Only the by-id branch maps onto this broker's store today: a msg id carries the
        // physical offset the `VIEW_MESSAGE_BY_ID` admin code resolves it by. Key/time-range
        // scans need the broker index store.
        let Some(message_id) = request.search_key.as_ref().and_then(|key| match key {
            pb::list_message_request::SearchKey::MessageId(value) => Some(value.clone()),
            _ => None,
        }) else {
            return Ok(Response::new(pb::ListMessageResponse {
                status: Some(pb_status(
                    pb::Code::NotImplemented,
                    "only message-id queries are supported; key and time-range scans need the index store",
                )),
                ..Default::default()
            }));
        };
        let Some(offset) = physical_offset_from_msg_id(&message_id) else {
            return Ok(Response::new(pb::ListMessageResponse {
                status: Some(pb_status(
                    pb::Code::BadRequest,
                    "unparseable message id",
                )),
                ..Default::default()
            }));
        };
        let response = self
            .backend
            .invoke_broker(&topic, 33, &[("offset", offset.to_string())], Vec::new())
            .await
            .map_err(|error| Status::internal(error.to_string()))?;
        if response.code != 0 {
            return Ok(Response::new(pb::ListMessageResponse {
                status: Some(pb_status(
                    map_response_code(response.code),
                    response.remark.as_deref().unwrap_or("message view failed"),
                )),
                ..Default::default()
            }));
        }
        let (message, consumed) = common::Message::decode(&response.body)
            .map_err(|error| Status::internal(error.to_string()))?;
        if consumed != response.body.len() {
            return Err(Status::internal("trailing bytes after the viewed record"));
        }
        Ok(Response::new(pb::ListMessageResponse {
            status: Some(pb_status(pb::Code::Ok, "")),
            messages: vec![to_pb_message(&message, None)],
            scroll_id: String::new(),
        }))
    }

    async fn print_thread_stack_trace(
        &self,
        request: Request<pb::PrintThreadStackTraceRequest>,
    ) -> PbResult<pb::PrintThreadStackTraceResponse> {
        let request = request.into_inner();
        let group = request
            .group
            .as_ref()
            .map(|g| g.name.clone())
            .unwrap_or_default();
        // Resolve a target consumer: the explicit client id, else any online member of the group.
        let target = if !request.client_id.is_empty() {
            request.client_id.clone()
        } else {
            self.context
                .clients
                .clients_for_group(&group)
                .into_iter()
                .next()
                .map(|c| c.client_id)
                .unwrap_or_default()
        };
        // No connected client (or no open telemetry stream) -> nothing to dump.
        if target.is_empty() || !self.context.clients.has_telemetry(&target) {
            return Ok(Response::new(pb::PrintThreadStackTraceResponse {
                status: Some(pb_status(pb::Code::Ok, "")),
                thread_stack_trace: Some(String::new()),
            }));
        }
        let nonce = next_nonce();
        let mut rx = self.context.clients.register_pending(&nonce);
        let command = pb::TelemetryCommand {
            command: Some(pb::telemetry_command::Command::PrintThreadStackTraceCommand(
                pb::PrintThreadStackTraceCommand {
                    nonce: nonce.clone(),
                },
            )),
            ..Default::default()
        };
        if !self.context.clients.push_command(&target, command) {
            self.context.clients.cancel_pending(&nonce);
            return Ok(Response::new(pb::PrintThreadStackTraceResponse {
                status: Some(pb_status(pb::Code::Ok, "")),
                thread_stack_trace: Some(String::new()),
            }));
        }
        let trace = match tokio::time::timeout(Duration::from_secs(3), &mut rx).await {
            Ok(Ok(PendingOutcome::StackTrace(trace))) => trace.unwrap_or_default(),
            _ => String::new(),
        };
        Ok(Response::new(pb::PrintThreadStackTraceResponse {
            status: Some(pb_status(pb::Code::Ok, "")),
            thread_stack_trace: Some(trace),
        }))
    }

    async fn verify_message(
        &self,
        request: Request<pb::VerifyMessageRequest>,
    ) -> PbResult<pb::VerifyMessageResponse> {
        let request = request.into_inner();
        let group = request
            .group
            .as_ref()
            .map(|g| g.name.clone())
            .unwrap_or_default();
        let target = if !request.client_id.is_empty() {
            request.client_id.clone()
        } else {
            self.context
                .clients
                .clients_for_group(&group)
                .into_iter()
                .next()
                .map(|c| c.client_id)
                .unwrap_or_default()
        };
        if target.is_empty() || !self.context.clients.has_telemetry(&target) {
            return Ok(Response::new(pb::VerifyMessageResponse {
                status: Some(pb_status(
                    pb::Code::FailedToConsumeMessage,
                    "no connected consumer to verify consumption against",
                )),
            }));
        }
        let nonce = next_nonce();
        let mut rx = self.context.clients.register_pending(&nonce);
        let message = pb::Message {
            topic: request.topic.clone(),
            system_properties: Some(pb::SystemProperties {
                message_id: request.message_id.clone(),
                ..Default::default()
            }),
            ..Default::default()
        };
        let command = pb::TelemetryCommand {
            command: Some(pb::telemetry_command::Command::VerifyMessageCommand(
                pb::VerifyMessageCommand {
                    nonce: nonce.clone(),
                    message: Some(message),
                },
            )),
            ..Default::default()
        };
        if !self.context.clients.push_command(&target, command) {
            self.context.clients.cancel_pending(&nonce);
            return Ok(Response::new(pb::VerifyMessageResponse {
                status: Some(pb_status(
                    pb::Code::FailedToConsumeMessage,
                    "consumer stream unavailable",
                )),
            }));
        }
        let verified = match tokio::time::timeout(Duration::from_secs(3), &mut rx).await {
            Ok(Ok(PendingOutcome::Verify(ok))) => ok,
            _ => false,
        };
        let status = if verified {
            pb_status(pb::Code::Ok, "")
        } else {
            pb_status(
                pb::Code::FailedToConsumeMessage,
                "consumer did not confirm consumption",
            )
        };
        Ok(Response::new(pb::VerifyMessageResponse {
            status: Some(status),
        }))
    }

    async fn admin_send_message(
        &self,
        request: Request<pb::AdminSendMessageRequest>,
    ) -> PbResult<pb::AdminSendMessageResponse> {
        let request = request.into_inner();
        let topic = request
            .topic
            .as_ref()
            .map(|t| t.name.clone())
            .unwrap_or_default();
        if topic.is_empty() {
            return Err(Status::invalid_argument("topic is required"));
        }
        if request.body.is_empty() {
            return Err(Status::invalid_argument("body is required"));
        }
        // Reconstruct a v2 Message and forward to the broker exactly like send_message.
        let mut system = request.system_properties.unwrap_or_default();
        if system.tag.is_none() {
            system.tag = request.tag.clone();
        }
        if let Some(key) = &request.key {
            if system.keys.is_empty() && !key.is_empty() {
                system.keys = vec![key.clone()];
            }
        }
        let message = pb::Message {
            topic: request.topic.clone(),
            user_properties: request.user_properties,
            system_properties: Some(system),
            body: request.body.clone(),
        };
        let properties = match encode_send_properties(&message) {
            Ok(properties) => properties,
            Err(error) => {
                return Ok(Response::new(pb::AdminSendMessageResponse {
                    status: Some(pb_status(pb::Code::BadRequest, error.to_string())),
                    ..Default::default()
                }))
            }
        };
        let born = now_millis();
        let response = self
            .backend
            .invoke_broker(
                &topic,
                10,
                &[
                    ("topic", topic.clone()),
                    (
                        "producerGroup",
                        self.context.config.default_producer_group.clone(),
                    ),
                    ("queueId", "-1".to_string()),
                    ("sysFlag", "0".to_string()),
                    ("flag", "0".to_string()),
                    ("bornTimestamp", born.to_string()),
                    ("properties", properties),
                ],
                message.body.clone(),
            )
            .await;
        match response {
            Ok(response) if response.code == 0 => {
                Ok(Response::new(pb::AdminSendMessageResponse {
                    status: Some(pb_status(pb::Code::Ok, "")),
                    message_id: response
                        .ext_fields
                        .get("msgId")
                        .cloned()
                        .unwrap_or_default(),
                }))
            }
            Ok(response) => Ok(Response::new(pb::AdminSendMessageResponse {
                status: Some(pb_status(
                    map_response_code(response.code),
                    response.remark.as_deref().unwrap_or("send failed"),
                )),
                ..Default::default()
            })),
            Err(error) => Ok(Response::new(pb::AdminSendMessageResponse {
                status: Some(pb_status(pb::Code::InternalError, error.to_string())),
                ..Default::default()
            })),
        }
    }

    async fn get_consumer_running_info(
        &self,
        request: Request<pb::GetConsumerRunningInfoRequest>,
    ) -> PbResult<pb::GetConsumerRunningInfoResponse> {
        let request = request.into_inner();
        let group = request
            .group
            .as_ref()
            .map(|g| g.name.clone())
            .unwrap_or_default();
        // Prefer the explicit client id, else the first connected member of the group.
        let state = if !request.client_id.is_empty() {
            self.context.clients.client_by_id(&request.client_id)
        } else {
            self.context
                .clients
                .clients_for_group(&group)
                .into_iter()
                .next()
        };
        let Some(state) = state else {
            return Ok(Response::new(pb::GetConsumerRunningInfoResponse {
                status: Some(pb_status(pb::Code::Ok, "")),
                consumer_running_info: None,
            }));
        };
        let mut properties = std::collections::HashMap::new();
        properties.insert("clientId".to_string(), state.client_id.clone());
        properties.insert("version".to_string(), state.version.clone());
        properties.insert("language".to_string(), state.language.clone());
        properties.insert("hostname".to_string(), state.hostname.clone());
        properties.insert("group".to_string(), state.group.clone());
        properties.insert("messageModel".to_string(), state.message_model.to_string());
        // Per-queue process-queue snapshots live on the consumer, not the proxy, so the
        // queue table and consume-status table are empty here (same as list_subscription:
        // the proxy tracks subscriptions/online state, not client-side caches).
        let info = pb::ConsumerRunningInfo {
            properties,
            subscriptions: state.subscriptions,
            message_queue_table: Vec::new(),
            consume_status_table: std::collections::HashMap::new(),
        };
        Ok(Response::new(pb::GetConsumerRunningInfoResponse {
            status: Some(pb_status(pb::Code::Ok, "")),
            consumer_running_info: Some(info),
        }))
    }

    async fn get_topic_route(
        &self,
        request: Request<pb::GetTopicRouteRequest>,
    ) -> PbResult<pb::GetTopicRouteResponse> {
        let request = request.into_inner();
        let topic = request
            .topic
            .as_ref()
            .map(|t| t.name.clone())
            .unwrap_or_default();
        let response = self
            .backend
            .invoke_namesrv(105, &[("topic", topic.clone())], Vec::new())
            .await
            .map_err(|_| Status::not_found(format!("topic route not found: {topic}")))?;
        if response.code != 0 {
            return Ok(Response::new(pb::GetTopicRouteResponse {
                status: Some(pb_status(
                    map_response_code(response.code),
                    response.remark.as_deref().unwrap_or("route query failed"),
                )),
                ..Default::default()
            }));
        }
        Ok(Response::new(pb::GetTopicRouteResponse {
            status: Some(pb_status(pb::Code::Ok, "")),
            topic_route_data: String::from_utf8_lossy(&response.body).to_string(),
        }))
    }

    async fn query_time_span(
        &self,
        request: Request<pb::QueryTimeSpanRequest>,
    ) -> PbResult<pb::QueryTimeSpanResponse> {
        let request = request.into_inner();
        let group = request
            .group
            .as_ref()
            .map(|g| g.name.clone())
            .unwrap_or_default();
        // Topics to span: explicit list, else every topic the group subscribes to.
        let mut topics: Vec<String> = request
            .topics
            .iter()
            .map(|t| t.name.clone())
            .filter(|t| !t.is_empty())
            .collect();
        if topics.is_empty() {
            topics = self
                .context
                .clients
                .clients_for_group(&group)
                .into_iter()
                .flat_map(|c| c.subscriptions.into_keys())
                .collect();
        }
        let mut spans = Vec::new();
        for topic in topics {
            let (brokers, queues) = match self.backend.topic_route(&topic).await {
                Ok(route) => route,
                Err(_) => continue,
            };
            // Resolve once per topic (avoids a broker round-trip per queue).
            let accept_types = self.backend.topic_message_types(&topic).await;
            for queue in &queues {
                let master = brokers
                    .iter()
                    .find(|b| b.name == queue.broker_name)
                    .and_then(|b| b.addrs.get(&0))
                    .cloned();
                let Some(addr) = master else {
                    continue;
                };
                for id in 0..queue.read_queues.max(0) {
                    // Earliest (code 31) / latest (code 32) store timestamps per queue.
                    let min_ts = self
                        .backend
                        .client
                        .invoke(
                            &addr,
                            Command::request(31)
                                .with_ext("topic", topic.clone())
                                .with_ext("queueId", id.to_string()),
                            self.backend.timeout,
                        )
                        .await
                        .ok()
                        .filter(|r| r.code == 0)
                        .and_then(|r| {
                            r.ext_fields
                                .get("timestamp")
                                .and_then(|v| v.parse::<i64>().ok())
                        })
                        .unwrap_or(0);
                    let max_ts = self
                        .backend
                        .client
                        .invoke(
                            &addr,
                            Command::request(32)
                                .with_ext("topic", topic.clone())
                                .with_ext("queueId", id.to_string()),
                            self.backend.timeout,
                        )
                        .await
                        .ok()
                        .filter(|r| r.code == 0)
                        .and_then(|r| {
                            r.ext_fields
                                .get("timestamp")
                                .and_then(|v| v.parse::<i64>().ok())
                        })
                        .unwrap_or(0);
                    // Committed offset (code 14) and max offset (code 30) bound the consume watermark.
                    let committed = self
                        .backend
                        .client
                        .invoke(
                            &addr,
                            Command::request(14)
                                .with_ext("consumerGroup", group.clone())
                                .with_ext("topic", topic.clone())
                                .with_ext("queueId", id.to_string()),
                            self.backend.timeout,
                        )
                        .await
                        .ok()
                        .filter(|r| r.code == 0)
                        .and_then(|r| {
                            r.ext_fields
                                .get("offset")
                                .and_then(|v| v.parse::<i64>().ok())
                        })
                        .unwrap_or(0);
                    let max_off = self
                        .backend
                        .client
                        .invoke(
                            &addr,
                            Command::request(30)
                                .with_ext("topic", topic.clone())
                                .with_ext("queueId", id.to_string()),
                            self.backend.timeout,
                        )
                        .await
                        .ok()
                        .filter(|r| r.code == 0)
                        .and_then(|r| {
                            r.ext_fields
                                .get("offset")
                                .and_then(|v| v.parse::<i64>().ok())
                        })
                        .unwrap_or(0);
                    // The broker does not expose a queue-offset->timestamp mapping here, so the
                    // consume timestamp is approximated: caught-up (committed at the tail) means
                    // the latest store time; otherwise it is bounded below by the earliest.
                    let consume_ts = if max_off > 0 && committed >= max_off - 1 {
                        max_ts
                    } else if min_ts > 0 {
                        min_ts
                    } else {
                        0
                    };
                    let delay_time_ms = if max_ts > consume_ts {
                        max_ts - consume_ts
                    } else {
                        0
                    };
                    spans.push(pb::query_time_span_response::QueueTimeSpan {
                        message_queue: Some(pb::MessageQueue {
                            topic: Some(pb::Resource {
                                resource_namespace: String::new(),
                                name: topic.clone(),
                            }),
                            id,
                            permission: perm_to_permission(queue.perm) as i32,
                            broker: Some(pb::Broker {
                                name: queue.broker_name.clone(),
                                id: 0,
                                endpoints: endpoints_from_addr(&addr),
                            }),
                            accept_message_types: accept_types.clone(),
                        }),
                        min_timestamp: min_ts,
                        max_timestamp: max_ts,
                        consume_timestamp: consume_ts,
                        delay_time_ms,
                    });
                }
            }
        }
        Ok(Response::new(pb::QueryTimeSpanResponse {
            status: Some(pb_status(pb::Code::Ok, "")),
            queue_time_span_list: spans,
        }))
    }

    async fn get_proxy_runtime_stats(
        &self,
        _request: Request<pb::GetProxyRuntimeStatsRequest>,
    ) -> PbResult<pb::GetProxyRuntimeStatsResponse> {
        let stats = self.context.runtime_stats();
        Ok(Response::new(pb::GetProxyRuntimeStatsResponse {
            status: Some(pb_status(pb::Code::Ok, "")),
            proxy_name: stats
                .get("proxyName")
                .and_then(serde_json::Value::as_str)
                .unwrap_or_default()
                .into(),
            version: env!("CARGO_PKG_VERSION").into(),
            ..Default::default()
        }))
    }
}

// ---------------------------------------------------------------------------
// Server bootstrap (Java `GrpcServer` + `ProxyStartup`)
// ---------------------------------------------------------------------------

/// Bind the proxy's gRPC port and host both `MessagingService` and `Admin`.
pub async fn serve_proxy(
    listener: tokio::net::TcpListener,
    context: Arc<ProxyContext>,
    shutdown: tokio::sync::watch::Receiver<bool>,
) -> Result<()> {
    context.config.validate()?;
    // Background orphaned-transaction recovery: re-issues RecoverOrphanedTransactionCommand
    // to producer clients over their telemetry streams (Java `TransactionalMessageCheckService`).
    tokio::spawn(run_transaction_recovery(Arc::clone(&context)));
    let messaging = pb::messaging_service_server::MessagingServiceServer::new(
        MessagingProcessor::new(Arc::clone(&context)),
    );
    let admin = pb::admin_server::AdminServer::new(AdminProcessor::new(Arc::clone(&context)));
    info!(addr = %listener.local_addr()?, "grpc proxy listening");
    tonic::transport::Server::builder()
        .add_service(messaging)
        .add_service(admin)
        .serve_with_incoming_shutdown(
            TcpListenerStream::new(listener),
            async move {
                let mut shutdown = shutdown;
                let _ = shutdown.changed().await;
            },
        )
        .await
        .map_err(|error| Error::Io(std::io::Error::other(error)))?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Remoting front end (Java `RemotingProtocolServer`)
// ---------------------------------------------------------------------------

/// Route/metadata queries that ride the pooled namesrv channels
/// (GET_ROUTEINFO_BY_TOPIC=105, GET_ALL_TOPICLIST_FROM_NAMESERVER=106,
/// GET_TOPIC_LIST_FROM_NS=219; "等" covers future additions to this list).
const NAMESRV_FORWARD_CODES: [i32; 3] = [105, 106, 219];
/// Message/offset commands forwarded to the broker master
/// (SEND_MESSAGE=10, PULL_MESSAGE=11, QUERY_CONSUMER_OFFSET=14,
/// UPDATE_CONSUMER_OFFSET=15, QUERY_OFFSET=29 and the v5 pop family
/// POP_MESSAGE=200050, ACK_MESSAGE=200051, CHANGE_MESSAGE_INVISIBLE_TIME=200053).
const BROKER_FORWARD_CODES: [i32; 8] = [10, 11, 14, 15, 29, 200050, 200051, 200053];

/// Remoting response codes used when the front end itself answers.
const RESPONSE_SYSTEM_ERROR: i32 = 1;
const RESPONSE_REQUEST_CODE_NOT_SUPPORTED: i32 = 3;

/// Forwards inbound remoting requests to the namesrv or broker master and passes
/// the responses straight back (the proxy never answers these codes locally).
struct RemotingFrontProcessor {
    context: Arc<ProxyContext>,
    backend: ProxyBackend,
    acl: AclGuard,
}

impl RemotingFrontProcessor {
    fn new(context: Arc<ProxyContext>) -> Self {
        let backend = ProxyBackend::from_config(&context.config);
        let acl = AclGuard::from_config(&context.config);
        Self {
            context,
            backend,
            acl,
        }
    }

    async fn process(&self, request: Command) -> Command {
        let opaque = request.opaque;
        let serialize_type = request.serialize_type;
        let oneway = request.flag & remoting::ONEWAY_FLAG != 0;
        // Oneway client requests are forwarded as two-way; the response is dropped
        // (the transport suppresses replies for oneway frames anyway).
        let mut request = request;
        if oneway {
            request.flag &= !remoting::ONEWAY_FLAG;
        }
        let mut response = self.route_and_forward(request).await;
        response.opaque = opaque;
        response.serialize_type = serialize_type;
        response.flag = (response.flag | remoting::RESPONSE_FLAG) & !remoting::ONEWAY_FLAG;
        response
    }

    async fn route_and_forward(&self, request: Command) -> Command {
        let echo = request.clone();
        if self.context.config.enable_acl {
            if let Err(error) = self.acl.check_command(&request).await {
                let mut response = Command::response(&echo, RESPONSE_SYSTEM_ERROR);
                response.remark = Some(error.to_string());
                return response;
            }
        }
        if NAMESRV_FORWARD_CODES.contains(&request.code) {
            return match self.backend.invoke_namesrv_command(request).await {
                Ok(response) => response,
                Err(error) => {
                    let mut response = Command::response(&echo, RESPONSE_SYSTEM_ERROR);
                    response.remark = Some(error.to_string());
                    response
                }
            };
        }
        if BROKER_FORWARD_CODES.contains(&request.code) {
            let topic = request.ext_fields.get("topic").cloned().unwrap_or_default();
            if topic.is_empty() {
                let mut response = Command::response(&echo, RESPONSE_SYSTEM_ERROR);
                response.remark = Some("broker command is missing the topic field".into());
                return response;
            }
            return match self.backend.invoke_broker_command(&topic, request).await {
                Ok(response) => response,
                Err(error) => {
                    let mut response = Command::response(&echo, RESPONSE_SYSTEM_ERROR);
                    response.remark = Some(error.to_string());
                    response
                }
            };
        }
        let mut response = Command::response(&echo, RESPONSE_REQUEST_CODE_NOT_SUPPORTED);
        response.remark = Some(format!(
            "proxy remoting front end does not support request code {}",
            echo.code
        ));
        response
    }
}

impl remoting::Handler for RemotingFrontProcessor {
    fn handle(
        &self,
        request: Command,
        _peer: std::net::SocketAddr,
    ) -> Pin<Box<dyn Future<Output = Command> + Send + '_>> {
        Box::pin(async move { self.process(request).await })
    }
}

/// Bind the proxy's remoting-protocol front end (`ProxyConfig::remoting_listen_port`
/// semantics): accept remoting connections, forward route queries (105/106/219) to
/// the namesrv and message commands (10/11/14/15/29/200050/200051/200053) to the
/// topic's broker master, and pass broker/namesrv responses back unmodified.
pub async fn serve_remoting_front(
    listener: tokio::net::TcpListener,
    context: Arc<ProxyContext>,
    shutdown: tokio::sync::watch::Receiver<bool>,
) -> Result<()> {
    context.config.validate()?;
    let processor = Arc::new(RemotingFrontProcessor::new(Arc::clone(&context)));
    info!(addr = %listener.local_addr()?, "remoting front end listening");
    remoting::serve(listener, processor, shutdown).await
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_defaults_and_json_roundtrip() {
        let config = ProxyConfig::default();
        assert_eq!(config.listen_port, 8081);
        let json = serde_json::to_string(&config).unwrap();
        let parsed: ProxyConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.proxy_name, config.proxy_name);
        assert!(parsed.validate().is_ok());
    }

    #[test]
    fn config_rejects_bad_nameserver() {
        let config = ProxyConfig {
            name_server_address: vec!["localhost".into()],
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn route_payload_parses_brokers_and_queues() {
        let payload = serde_json::json!({
            "queueDatas": [
                {"brokerName": "broker-a", "readQueueNums": 8, "writeQueueNums": 8, "perm": 6, "topicSysFlag": 0},
                {"brokerName": "broker-b", "readQueueNums": 4, "writeQueueNums": 4, "perm": 4, "topicSysFlag": 0}
            ],
            "brokerDatas": [
                {"cluster": "DefaultCluster", "brokerName": "broker-a",
                 "brokerAddrs": {"0": "127.0.0.1:10911", "1": "127.0.0.1:10951"}},
                {"cluster": "DefaultCluster", "brokerName": "broker-b",
                 "brokerAddrs": {"0": "127.0.0.2:10911"}}
            ]
        });
        let body = serde_json::to_vec(&payload).unwrap();
        let (brokers, queues) = ProxyBackend::parse_route(&body).unwrap();
        assert_eq!(brokers.len(), 2);
        assert_eq!(queues.len(), 2);
        assert_eq!(brokers[0].addrs.get(&0).unwrap(), "127.0.0.1:10911");
        assert_eq!(queues[0].read_queues, 8);
        assert_eq!(perm_to_permission(queues[0].perm), pb::Permission::ReadWrite);
        assert_eq!(perm_to_permission(queues[1].perm), pb::Permission::Read);
    }

    #[test]
    fn receipt_handle_roundtrip() {
        let handle = encode_receipt_handle("topic-x", "group-y", 3, 42);
        let (topic, group, queue, offset) = decode_receipt_handle(&handle).unwrap();
        assert_eq!(topic, "topic-x");
        assert_eq!(group, "group-y");
        assert_eq!(queue, 3);
        assert_eq!(offset, 42);
        assert!(decode_receipt_handle("garbage").is_none());
    }

    #[test]
    fn endpoints_conversion() {
        let endpoints = endpoints_from_addr("127.0.0.1:10911").unwrap();
        assert_eq!(endpoints.scheme, pb::AddressScheme::IPv4 as i32);
        assert_eq!(endpoints.addresses[0].host, "127.0.0.1");
        assert_eq!(endpoints.addresses[0].port, 10911);
        assert!(endpoints_from_addr("no-port").is_none());
    }

    #[test]
    fn response_code_mapping() {
        assert_eq!(map_response_code(0), pb::Code::Ok);
        assert_eq!(map_response_code(17), pb::Code::TopicNotFound);
        assert_eq!(map_response_code(3), pb::Code::NotImplemented);
        assert_eq!(map_response_code(999), pb::Code::InternalError);
    }

    #[test]
    fn physical_offset_parses_the_last_sixteen_hex_chars() {
        // A canonical msg id carries its commitlog offset in the tail 16 hex chars.
        let id = "C0A8006500000000000000000000012A";
        assert_eq!(physical_offset_from_msg_id(id), Some(0x12A));
        // Short ids and non-hex tails are rejected, driving the BadRequest status.
        assert_eq!(physical_offset_from_msg_id("1234567890abcde"), None);
        assert_eq!(physical_offset_from_msg_id("nothexnothexnothex!!"), None);
    }

    #[test]
    fn send_properties_encode_reserved_and_user_keys() {
        let message = pb::Message {
            topic: Some(pb::Resource {
                resource_namespace: String::new(),
                name: "t".into(),
            }),
            user_properties: [("k1".to_string(), "v1".to_string())].into_iter().collect(),
            system_properties: Some(pb::SystemProperties {
                tag: Some("tag-a".into()),
                keys: vec!["k".into()],
                message_id: "mid-1".into(),
                message_group: Some("g".into()),
                ..Default::default()
            }),
            body: vec![1, 2, 3],
        };
        let properties = encode_send_properties(&message).unwrap();
        assert!(properties.contains("TAGS\x01tag-a\x02"));
        assert!(properties.contains("KEYS\x01k\x02"));
        assert!(properties.contains("UNIQ_KEY\x01mid-1\x02"));
        assert!(properties.contains("SHARDING_KEY\x01g\x02"));
        assert!(properties.contains("k1\x01v1\x02"));
    }

    #[test]
    fn average_hash_slice_partitions_all_queues() {
        // The slices of all groups are disjoint and cover [0, N) exactly.
        for total in 0..40u64 {
            for groups in 1..=7u64 {
                let mut covered = 0u64;
                for group in 0..groups {
                    let slice = average_hash_slice(group, total, groups);
                    assert_eq!(slice.start, covered);
                    assert!(slice.end >= slice.start);
                    covered = slice.end;
                }
                assert_eq!(covered, total);
            }
        }
    }

    #[test]
    fn group_index_is_stable_and_bounded() {
        let mut config = ProxyConfig::default();
        config.consumer_group_count = Some(4);
        let a = group_index(&config, "GID-order");
        assert_eq!(a, group_index(&config, "GID-order"));
        assert!(a < 4);
        // Default (unset) group count is 1: every group maps to index 0.
        assert_eq!(group_index(&ProxyConfig::default(), "anything"), 0);
        assert_eq!(consumer_group_count(&ProxyConfig::default()), 1);
    }

    #[test]
    fn ring_lookup_is_deterministic() {
        let config = ProxyConfig {
            consumer_group_count: Some(3),
            ..Default::default()
        };
        let first = ring_owns_queue(&config, "topic-a", "broker-1", 2, 0);
        assert_eq!(first, ring_owns_queue(&config, "topic-a", "broker-1", 2, 0));
        // With a single group the ring trivially owns every queue.
        let single = ProxyConfig::default();
        assert!(ring_owns_queue(&single, "topic-a", "broker-1", 2, 0));
    }

    #[test]
    fn authorization_header_parses_credential_and_signature() {
        let parsed = AclGuard::parse_authorization(
            "MQv2 HMAC-SHA1 Credential=rocketmq, SignedHeaders=, Signature=abc+123==",
        )
        .unwrap();
        assert_eq!(parsed.0, "rocketmq");
        assert_eq!(parsed.1, "abc+123==");
        // Java spelling `AccessKey=` is tolerated too.
        let parsed = AclGuard::parse_authorization("AccessKey=ak,Signature=sig")
            .unwrap();
        assert_eq!(parsed.0, "ak");
        assert_eq!(parsed.1, "sig");
        assert!(AclGuard::parse_authorization("Credential=only-key").is_none());
    }

    #[tokio::test]
    async fn acl_signature_verification_roundtrip() {
        let mut config = ProxyConfig::default();
        config.enable_acl = true;
        let guard = AclGuard::from_config(&config);
        guard.register_user("alice", "secret-1").await.unwrap();
        // Signature over the empty content (the guard's signing contract).
        let signature = auth::cal_signature(&[], "secret-1");
        let header = format!("Credential=alice, Signature={signature}");
        assert!(AclGuard::parse_authorization(&header).is_some());
        let (access_key, parsed_signature) =
            AclGuard::parse_authorization(&header).unwrap();
        let user = guard.metadata.get_user(&access_key).await.unwrap().unwrap();
        let expected = auth::cal_signature(&[], user.password.as_deref().unwrap_or(""));
        assert!(AclGuard::signature_matches(&expected, &parsed_signature));
        // Wrong secret must not match.
        let wrong = auth::cal_signature(&[], "other-secret");
        assert!(!AclGuard::signature_matches(&expected, &wrong));
        // Disabled guard passes everything (verify via check_access plumbing).
        let off = AclGuard::from_config(&ProxyConfig::default());
        assert!(!off.enabled);
    }
}
