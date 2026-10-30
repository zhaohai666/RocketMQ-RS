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
//! - **Real forwarding** for query_route / send_message / receive_message (broker POP,
//!   receipt handle = the broker's pop check point) / ack_message / pull_message /
//!   update_offset / get_offset / query_offset / get_topic_route / runtime stats.
//! - **Java's assignment model**: `query_assignment` follows `RouteActivity.queryAssignment`
//!   — a group that consumes in order (FIFO) owns every readable queue of the topic,
//!   every other group gets a single assignment whose queue id is `-1`. The `-1` tells
//!   the client that the server picks the queue, which is what the pop path does: the
//!   broker picks the queue (`PopMessageProcessor` reads all queues of a `queueId < 0`
//!   request) and keeps the message invisible until it is acked.
//! - **Pooled forwarding**: commands ride persistent connections from
//!   [`PooledRemotingClient`] (idle 30s, <=32 per address, one in-flight request per
//!   connection at a time) instead of one short connection per invoke.
//! - **ACL hook**: when `enable_acl` is set, `MessagingProcessor` verifies the gRPC
//!   `Authorization` metadata (Credential/Signature) against the `crate::auth` user
//!   store using `auth::cal_signature` before each RPC body is processed.
//! - **Simplified**: the ACL signature content is the empty byte string (see
//!   [`AclGuard`]); admin diagnostics that cannot reach a live client (thread dumps,
//!   verify-consumption) return typed empty/failed statuses instead of erroring the
//!   surface.
//! - **Stubs**: no service RPC answers `NOT_IMPLEMENTED` any more; admin diagnostics
//!   that cannot reach a live client (thread dumps, verify-consumption) return typed
//!   empty/failed statuses instead of erroring the surface.

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
    /// Producer group stamped on messages forwarded with `SEND_MESSAGE`.
    pub default_producer_group: String,
    /// Per-command forwarding timeout towards namesrv/broker.
    pub forward_timeout_millis: u64,
    /// Java `ProxyConfig.maxMessageSize`: the body size a producer may send.
    pub max_message_size: i32,
    /// Java `ProxyConfig.enableTopicMessageTypeCheck`: whether the broker validates a
    /// message's type against the topic's declared `accept_message_types`.
    pub enable_topic_message_type_check: bool,
    /// Java `ProxyConfig.maxLiteTopicSize`: the maximum length of one lite topic.
    pub max_lite_topic_size: i32,
    /// Java `ProxyConfig.grpcClientProducerMaxAttempts`: producer-side retry attempts
    /// the merged `Settings` advertises.
    pub grpc_client_producer_max_attempts: i32,
    /// Java `ProxyConfig.grpcClientProducerBackoffInitialMillis`.
    pub grpc_client_producer_backoff_initial_millis: i64,
    /// Java `ProxyConfig.grpcClientProducerBackoffMaxMillis`.
    pub grpc_client_producer_backoff_max_millis: i64,
    /// Java `ProxyConfig.grpcClientProducerBackoffMultiplier`.
    pub grpc_client_producer_backoff_multiplier: f32,
    /// Java `ProxyConfig.grpcClientConsumerMinLongPollingTimeoutMillis`.
    pub grpc_client_consumer_min_long_polling_timeout_millis: i64,
    /// Java `ProxyConfig.grpcClientConsumerMaxLongPollingTimeoutMillis`.
    pub grpc_client_consumer_max_long_polling_timeout_millis: i64,
    /// Java `ProxyConfig.grpcClientConsumerLongPollingBatchSize`.
    pub grpc_client_consumer_long_polling_batch_size: i32,
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
            default_producer_group: "PID_DEFAULT".into(),
            forward_timeout_millis: 3_000,
            max_message_size: 4 * 1024 * 1024,
            enable_topic_message_type_check: true,
            max_lite_topic_size: 64,
            grpc_client_producer_max_attempts: 3,
            grpc_client_producer_backoff_initial_millis: 10,
            grpc_client_producer_backoff_max_millis: 1_000,
            grpc_client_producer_backoff_multiplier: 2.0,
            grpc_client_consumer_min_long_polling_timeout_millis: 5_000,
            grpc_client_consumer_max_long_polling_timeout_millis: 20_000,
            grpc_client_consumer_long_polling_batch_size: 32,
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
    /// The gRPC listen address advertised to clients in `QueryRoute` endpoints.
    /// Clients talk gRPC only to the proxy, never to a broker remoting port, so
    /// every route response must point here rather than at `broker.addrs`.
    pub(crate) grpc_listen_addr: String,
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

/// Java `ProxyConfig.maxInvisibleTimeMills` (default 12h): the largest invisible
/// duration a receive/change-invisible request may carry before `ILLEGAL_INVISIBLE_TIME`.
const MAX_INVISIBLE_TIME_MS: i64 = 12 * 60 * 60 * 1000;

impl ProxyContext {
    pub fn new(config: ProxyConfig, grpc_listen_addr: std::net::SocketAddr) -> Arc<Self> {
        Arc::new(Self {
            config,
            grpc_listen_addr: grpc_listen_addr.to_string(),
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
    format!(
        "rmq-{}-{}",
        std::process::id(),
        NONCE_COUNTER.fetch_add(1, Ordering::SeqCst)
    )
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

// Java `MessageReceiptHandleManager`: per-channel (per-client) receipt-handle cache with a
// TTL, so a reconnecting client can still resolve handles the proxy handed out earlier.
// Java `GrpcChannelManager.scanNotActiveChannel` expires inactive channels; `reap_expired`
// mirrors both behaviours for the simplified `ClientRegistry`.
/// Largest age a cached receipt handle may reach before the reaper drops it (12h, matching
/// the maximum invisible span a pop may carry).
const HANDLE_CACHE_TTL_MS: i64 = 12 * 60 * 60 * 1000;
/// Drop a client idle longer than this (Java `GrpcChannelManager` channel expiry).
const CHANNEL_EXPIRED_SECS: u64 = 120;
/// How often `run_registry_reaper` sweeps expired handles and inactive clients.
const REGISTRY_REAPER_INTERVAL_SECS: u64 = 5;

/// One cached receipt handle (the broker's pop ck) keyed by the client that received it.
struct HandleCacheEntry {
    handle: String,
    expire_at: i64,
    topic: String,
    group: String,
    queue_id: i32,
    offset: u64,
}

#[derive(Default)]
pub(crate) struct ClientRegistry {
    by_id: Mutex<HashMap<String, ClientState>>,
    /// group -> client ids currently connected (registered via heartbeat/telemetry).
    group_members: Mutex<HashMap<String, Vec<String>>>,
    /// topic -> client ids of the producers that publish to it. In the 5.x gRPC
    /// model the producer group *is* the topic name (Java `SendMessageActivity`:
    /// "here use topicName as producerGroup for transactional checker"), so this
    /// index is what lets the transaction check reach the right producer.
    producer_topics: Mutex<HashMap<String, Vec<String>>>,
    /// nonce -> waiting Admin RPC awaiting a client telemetry reply (thread dump / verify).
    pending: Mutex<HashMap<String, PendingReply>>,
    /// Java `MessageReceiptHandleManager`: per-client receipt-handle cache (pop ck -> coords).
    handles: Mutex<HashMap<String, Vec<HandleCacheEntry>>>,
}

impl ClientRegistry {
    /// Insert or refresh a client. Preserves any already-open telemetry channel so
    /// re-registration (heartbeat vs telemetry settings) does not drop the stream.
    fn register(&self, mut state: ClientState) {
        let mut by_id = self.by_id.lock().unwrap();
        let previous_topics = by_id
            .get(&state.client_id)
            .map(|prev| prev.publish_topics.clone())
            .unwrap_or_default();
        if let Some(prev) = by_id.get(&state.client_id) {
            state.telemetry = prev.telemetry.clone();
            // A heartbeat re-registers with a fresh `ClientState` that carries no consumer
            // context; without this the client's lite subscriptions (registered via
            // `SyncLiteSubscription`) would be wiped on every heartbeat and the lite
            // consumer would fall back to pulling the bind topic's own queue.
            state.lite_topics = prev.lite_topics.clone();
            // The same applies to the producer topics a heartbeat does not carry.
            if state.publish_topics.is_empty() {
                state.publish_topics = previous_topics.clone();
            }
        }
        if !state.group.is_empty() {
            let mut groups = self.group_members.lock().unwrap();
            let entry = groups.entry(state.group.clone()).or_default();
            if !entry.contains(&state.client_id) {
                entry.push(state.client_id.clone());
            }
        }
        if !state.publish_topics.is_empty() {
            let mut topics = self.producer_topics.lock().unwrap();
            for topic in &state.publish_topics {
                let entry = topics.entry(topic.clone()).or_default();
                if !entry.contains(&state.client_id) {
                    entry.push(state.client_id.clone());
                }
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
            if !state.publish_topics.is_empty() {
                let mut topics = self.producer_topics.lock().unwrap();
                for topic in &state.publish_topics {
                    if let Some(members) = topics.get_mut(topic) {
                        members.retain(|id| id != client_id);
                        if members.is_empty() {
                            topics.remove(topic);
                        }
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

    /// Refresh a client's last-activity timestamp so the idle reaper keeps it alive
    /// (Java `GrpcClientChannel` updates `lastActiveTime` on every interaction).
    fn touch(&self, client_id: &str) {
        if let Some(state) = self.by_id.lock().unwrap().get_mut(client_id) {
            state.last_heartbeat = Instant::now();
        }
    }

    /// Cache a receipt handle the proxy just handed to `client_id`
    /// (Java `MessageReceiptHandleManager.put`).
    fn cache_handle(&self, client_id: &str, handle: &str, meta: (String, String, i32, u64)) {
        if handle.is_empty() || client_id.is_empty() {
            return;
        }
        let now = now_millis();
        let mut handles = self.handles.lock().unwrap();
        let entry = handles.entry(client_id.to_string()).or_default();
        entry.retain(|e| e.expire_at > now);
        entry.push(HandleCacheEntry {
            handle: handle.to_string(),
            expire_at: now + HANDLE_CACHE_TTL_MS,
            topic: meta.0,
            group: meta.1,
            queue_id: meta.2,
            offset: meta.3,
        });
        // Bound memory: keep only the most recent handles per client.
        if entry.len() > 1024 {
            entry.drain(0..entry.len() - 1024);
        }
    }

    /// Look up a cached receipt handle for `client_id`
    /// (Java `MessageReceiptHandleManager.get`), dropping expired entries.
    fn lookup_handle(&self, client_id: &str, handle: &str) -> Option<(String, String, i32, u64)> {
        let now = now_millis();
        let mut handles = self.handles.lock().unwrap();
        let entry = handles.get_mut(client_id)?;
        entry.retain(|e| e.expire_at > now);
        entry
            .iter()
            .find(|e| e.handle == handle)
            .map(|e| (e.topic.clone(), e.group.clone(), e.queue_id, e.offset))
    }

    /// Periodic cleanup (Java `MessageReceiptHandleManager` TTL + `GrpcChannelManager
    /// .scanNotActiveChannel`): drop expired receipt handles and evict clients idle beyond
    /// `CHANNEL_EXPIRED_SECS`.
    fn reap_expired(&self) {
        let now = now_millis();
        {
            let mut handles = self.handles.lock().unwrap();
            handles.retain(|_, v| {
                v.retain(|e| e.expire_at > now);
                !v.is_empty()
            });
        }
        let expired: Vec<String> = {
            let by_id = self.by_id.lock().unwrap();
            by_id
                .iter()
                .filter(|(_, state)| {
                    state.last_heartbeat.elapsed().as_secs() > CHANNEL_EXPIRED_SECS
                })
                .map(|(id, _)| id.clone())
                .collect()
        };
        for id in expired {
            // `unregister` clears the client from `by_id`, `group_members` and `producer_topics`.
            self.unregister(&id);
        }
    }

    /// Push a telemetry command to every producer that publishes to `topic`.
    /// Java `GrpcClientChannelManager` registers a producer under each topic it
    /// publishes to (the topic name *is* the producer group in the 5.x gRPC
    /// model), so this is the path the transaction check takes.
    fn push_to_producer_topic(&self, topic: &str, command: pb::TelemetryCommand) -> usize {
        let producers = self.producer_topics.lock().unwrap();
        let Some(ids) = producers.get(topic) else {
            return 0;
        };
        let mut pushed = 0;
        for id in ids {
            if self.push_command(id, command.clone()) {
                pushed += 1;
            }
        }
        pushed
    }

    /// Consumer's filter expression for `(client_id, topic)`, if the client registered one.
    /// Used to attach the subscription to the forwarded broker pull so the broker can
    /// compensate proxy-managed group state (Java `compensateSubscribeData`).
    fn subscription_for(&self, client_id: &str, topic: &str) -> Option<pb::FilterExpression> {
        self.by_id
            .lock()
            .unwrap()
            .get(client_id)
            .and_then(|state| state.subscriptions.get(topic).cloned())
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

    /// The bind (parent) topic a client's lite subscriptions hang below. A lite
    /// consumer is assigned the bind topic's queues, and its messages are stored
    /// there, so the proxy needs the name to pull from.
    fn lite_bind_topic(&self, client_id: &str) -> Option<String> {
        let by_id = self.by_id.lock().unwrap();
        by_id
            .get(client_id)
            .and_then(|state| state.lite_topics.keys().next().cloned())
    }

    /// The bind (parent) topic of any client registered under `group`.
    fn lite_bind_topic_of_group(&self, group: &str) -> Option<String> {
        let by_id = self.by_id.lock().unwrap();
        by_id
            .values()
            .filter(|state| state.group == group)
            .find_map(|state| state.lite_topics.keys().next().cloned())
    }

    /// Whether the client (or any member of its group) registered as a lite consumer
    /// (`ClientType::LitePushConsumer` / `LiteSimpleConsumer`). Used to stop the
    /// receive fall-back to the bind topic's own queue once the lite set is empty.
    fn is_lite_consumer(&self, client_id: &str, group: &str) -> bool {
        const LITE_TYPES: [i32; 2] = [5, 6]; // pb::ClientType::LitePushConsumer / LiteSimpleConsumer
        let by_id = self.by_id.lock().unwrap();
        if let Some(state) = by_id.get(client_id) {
            if LITE_TYPES.contains(&state.client_type) {
                return true;
            }
        }
        by_id
            .values()
            .any(|state| state.group == group && LITE_TYPES.contains(&state.client_type))
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
                            state
                                .lite_topics
                                .insert(bind_topic.to_string(), topics.to_vec());
                            Vec::new()
                        }
                        // PARTIAL_ADD (default): union with the existing set.
                        _ => {
                            let entry =
                                state.lite_topics.entry(bind_topic.to_string()).or_default();
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
                        state
                            .lite_topics
                            .insert(bind_topic.to_string(), topics.to_vec());
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
                    command: Some(
                        pb::telemetry_command::Command::NotifyUnsubscribeLiteCommand(
                            pb::NotifyUnsubscribeLiteCommand {
                                lite_topic: topic.to_string(),
                                ..Default::default()
                            },
                        ),
                    ),
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
                        state
                            .subscriptions
                            .insert(t.name.clone(), entry.expression.clone().unwrap_or_default());
                    }
                }
            }
        }
    }
    registry.register(state);
}

// ---------------------------------------------------------------------------
// GrpcClientSettingsManager (Java `proxy/.../grpc/v2/common/GrpcClientSettingsManager.java`)
// ---------------------------------------------------------------------------

/// A protobuf duration built from a millisecond count.
fn duration_millis(millis: i64) -> prost_types::Duration {
    prost_types::Duration {
        seconds: millis.div_euclid(1_000),
        nanos: (millis.rem_euclid(1_000) * 1_000_000) as i32,
    }
}

/// Java `GrpcClientSettingsManager.mergeProducerData`: the server tells the producer how
/// many attempts it may make and with which back-off, plus the body-size limit and
/// whether the broker validates message types.
fn merge_producer_settings(settings: &pb::Settings, config: &ProxyConfig) -> pb::Settings {
    let mut merged = settings.clone();
    let mut backoff = merged.backoff_policy.clone().unwrap_or_default();
    backoff.max_attempts = config.grpc_client_producer_max_attempts;
    backoff.strategy = Some(pb::retry_policy::Strategy::ExponentialBackoff(
        pb::ExponentialBackoff {
            initial: Some(duration_millis(
                config.grpc_client_producer_backoff_initial_millis,
            )),
            max: Some(duration_millis(
                config.grpc_client_producer_backoff_max_millis,
            )),
            multiplier: config.grpc_client_producer_backoff_multiplier,
        },
    ));
    merged.backoff_policy = Some(backoff);
    if let Some(pb::settings::PubSub::Publishing(publishing)) = merged.pub_sub.as_mut() {
        publishing.validate_message_type = config.enable_topic_message_type_check;
        publishing.max_body_size = config.max_message_size;
    }
    merged
}

/// Java `GrpcClientSettingsManager.mergeSubscriptionData`: the server tells the consumer
/// whether its group is FIFO, how many messages a long poll may return, how long a long
/// poll may wait, the lite-subscription quota, and — crucially — the group's retry
/// policy. A 5.x client refuses to start scanning without that back-off policy, so this
/// merge is what makes the push-consumer path work at all.
fn merge_subscription_settings(
    settings: &pb::Settings,
    group: &crate::schedule::GroupConfig,
    config: &ProxyConfig,
) -> pb::Settings {
    let mut merged = settings.clone();
    if let Some(pb::settings::PubSub::Subscription(subscription)) = merged.pub_sub.as_mut() {
        subscription.fifo = Some(group.consume_message_orderly);
        subscription.receive_batch_size = Some(config.grpc_client_consumer_long_polling_batch_size);
        subscription.long_polling_timeout = Some(duration_millis(
            config.grpc_client_consumer_max_long_polling_timeout_millis,
        ));
        subscription.lite_subscription_quota = Some(group.lite_sub_client_quota());
        subscription.max_lite_topic_size = Some(config.max_lite_topic_size);
    }
    let mut backoff = merged.backoff_policy.clone().unwrap_or_default();
    // Java: `setMaxAttempts(groupConfig.getRetryMaxTimes() + 1)` — the extra attempt
    // is the first delivery, the configured maximum counts the retries.
    backoff.max_attempts = group.retry_max_times + 1;
    backoff.strategy = Some(match group.group_retry_policy.policy_type {
        crate::schedule::GroupRetryPolicyType::EXPONENTIAL => {
            let policy = group
                .group_retry_policy
                .exponential_retry_policy
                .clone()
                .unwrap_or_default();
            pb::retry_policy::Strategy::ExponentialBackoff(pb::ExponentialBackoff {
                initial: Some(duration_millis(policy.initial as i64)),
                max: Some(duration_millis(policy.max as i64)),
                multiplier: policy.multiplier as f32,
            })
        }
        crate::schedule::GroupRetryPolicyType::CUSTOMIZED => {
            let policy = group
                .group_retry_policy
                .customized_retry_policy
                .clone()
                .unwrap_or_default();
            pb::retry_policy::Strategy::CustomizedBackoff(pb::CustomizedBackoff {
                next: policy
                    .next
                    .iter()
                    .map(|millis| duration_millis(*millis as i64))
                    .collect(),
            })
        }
    });
    merged.backoff_policy = Some(backoff);
    merged
}

/// Java `GrpcClientSettingsManager.getClientSettings`: merge the client-reported
/// `Settings` with the server's own data and return what the client should see.
async fn merge_client_settings(
    backend: &ProxyBackend,
    config: &ProxyConfig,
    settings: &pb::Settings,
) -> pb::Settings {
    match &settings.pub_sub {
        Some(pb::settings::PubSub::Publishing(_)) => merge_producer_settings(settings, config),
        Some(pb::settings::PubSub::Subscription(subscription)) => {
            let group = subscription
                .group
                .as_ref()
                .map(|g| g.name.clone())
                .unwrap_or_default();
            match backend
                .subscription_group_config(&config.rocketmq_cluster_name, &group)
                .await
            {
                Some(group_config) => merge_subscription_settings(settings, &group_config, config),
                // Java auto-creates the subscription group (`SubscriptionGroupManager`
                // with `autoCreateSubscriptionGroup`) before the settings merge, so an
                // unknown group still yields the auto-created defaults. Returning the
                // settings untouched would leave `lite_subscription_quota` unset — the
                // client reads it as 0 and refuses every lite subscription (42902).
                None => merge_subscription_settings(
                    settings,
                    &crate::schedule::GroupConfig::new(group),
                    config,
                ),
            }
        }
        None => settings.clone(),
    }
}

/// The bind (parent) topic of a lite consumer: the topic its lite subscriptions
/// were registered under. Falls back to the group's bind topic when the caller
/// carries no resolvable client id.
fn bind_topic_of(registry: &ClientRegistry, client_id: &str, group: &str) -> String {
    if !client_id.is_empty() {
        if let Some(bind_topic) = registry.lite_bind_topic(client_id) {
            return bind_topic;
        }
    }
    registry.lite_bind_topic_of_group(group).unwrap_or_default()
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
    const OP_TOPIC: &str = "RMQ_SYS_TRANS_OP_HALF_TOPIC";
    /// The reference scans every `transactionCheckInterval` (60s); half of that
    /// keeps the first check well inside a client's patience without hammering
    /// the store.
    const CHECK_INTERVAL_SECS: u64 = 30;
    /// Java `BrokerConfig.transactionTimeOut` (6s): the age a half message must
    /// reach before a check round may touch it.
    const ORPHAN_AGE_MS: i64 = 6_000;
    /// The synthetic consumer the check reads the half and op queues with. The
    /// broker auto-creates it (`autoCreateGroup`), like Java's
    /// `SubscriptionGroupManager` creates the check group on the first read.
    const CHECK_GROUP: &str = "__transaction_check";
    /// A pull round never walks more pages than this; the op topic grows by one
    /// 8-byte record per resolved transaction, so the bound is generous.
    const MAX_PULL_ROUNDS: usize = 1_000;
    let backend = ProxyBackend::from_config(&context.config);
    loop {
        tokio::time::sleep(Duration::from_secs(CHECK_INTERVAL_SECS)).await;
        // Replay the op topic: one 8-byte big-endian half offset per resolved
        // half message. A half message named by an op record is committed or
        // rolled back and must never be checked again — re-checking a committed
        // half would make the producer answer COMMIT a second time and duplicate
        // the delivery (Java `TransactionalMessageService.check` skips these too).
        let mut resolved = std::collections::HashSet::new();
        let mut cursor = 0u64;
        for _ in 0..MAX_PULL_ROUNDS {
            let response = match pull_check_queue(&backend, OP_TOPIC, CHECK_GROUP, cursor).await {
                Ok(response) => response,
                Err(error) => {
                    warn!(%error, "transaction recovery: op topic replay failed");
                    break;
                }
            };
            if response.code != 0 {
                break; // 19/20/21: nothing (or nothing more) at the cursor.
            }
            let mut rest = response.body.as_slice();
            while !rest.is_empty() {
                match common::Message::decode(rest) {
                    Ok((record, consumed)) if consumed > 0 => {
                        rest = &rest[consumed..];
                        if record.body.len() == 8 {
                            resolved
                                .insert(u64::from_be_bytes(record.body[..8].try_into().unwrap()));
                        }
                    }
                    _ => break,
                }
            }
            match response
                .ext_fields
                .get("nextBeginOffset")
                .and_then(|value| value.parse::<u64>().ok())
            {
                Some(next) if next > cursor => cursor = next,
                _ => break,
            }
        }
        // Scan the half queue: every record past the immunity age that no op
        // record resolves is an orphan and is offered to its producer.
        let mut cursor = 0u64;
        let mut scanned = 0usize;
        let mut pushed = 0usize;
        for _ in 0..MAX_PULL_ROUNDS {
            let response = match pull_check_queue(&backend, HALF_TOPIC, CHECK_GROUP, cursor).await {
                Ok(response) => response,
                Err(error) => {
                    warn!(%error, "transaction recovery: half topic scan failed");
                    break;
                }
            };
            if response.code != 0 {
                break; // 19/20/21: nothing (or nothing more) at the cursor.
            }
            let mut rest = response.body.as_slice();
            while !rest.is_empty() {
                let msg = match common::Message::decode(rest) {
                    Ok((msg, consumed)) if consumed > 0 => {
                        rest = &rest[consumed..];
                        msg
                    }
                    _ => break,
                };
                scanned += 1;
                if resolved.contains(&msg.physical_offset) {
                    continue;
                }
                if now_millis() - msg.born_timestamp < ORPHAN_AGE_MS {
                    continue;
                }
                // The half message carries its real topic (the one the producer
                // actually publishes to) as `REAL_TOPIC`; the 5.x gRPC producer
                // registers under that topic, so that is the key the recovery
                // command must be pushed by. `PRODUCER_GROUP` is the topic name too
                // (the proxy stamps it on the send), but `REAL_TOPIC` is the
                // authoritative destination for the listener lookup.
                let real_topic = msg
                    .properties
                    .get(crate::schedule::REAL_TOPIC)
                    .cloned()
                    .or_else(|| msg.properties.get("PRODUCER_GROUP").cloned())
                    .unwrap_or_default();
                if real_topic.is_empty() {
                    continue;
                }
                // Java's `TransactionalMessageCheckService` restores the real topic
                // before handing the message to the recovery command, so the client
                // can route it to the right transaction listener.
                let mut recovered = msg.clone();
                recovered.topic = real_topic.clone();
                let pb_msg = to_pb_message(&recovered, None);
                let transaction_id = msg
                    .properties
                    .get("UNIQ_KEY")
                    .cloned()
                    .unwrap_or_else(|| format!("{}@{}", real_topic, msg.queue_offset));
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
                let delivered = context.clients.push_to_producer_topic(&real_topic, command);
                if delivered == 0 {
                    warn!(%real_topic, offset = msg.physical_offset, "transaction recovery: no live producer for topic");
                } else {
                    info!(
                        %real_topic, offset = msg.physical_offset, producers = delivered,
                        "transaction recovery: orphaned half message offered for check"
                    );
                    pushed += delivered;
                }
            }
            match response
                .ext_fields
                .get("nextBeginOffset")
                .and_then(|value| value.parse::<u64>().ok())
            {
                Some(next) if next > cursor => cursor = next,
                _ => break,
            }
        }
        if scanned > 0 || !resolved.is_empty() {
            info!(
                scanned,
                pushed,
                resolved = resolved.len(),
                "transaction recovery round finished"
            );
        }
    }
}

/// One non-suspending `PULL_MESSAGE` read of a transaction system queue. The
/// half and op records always sit on queue 0 (the bridge store queues them
/// there), and an empty read answers `PULL_NOT_FOUND` (19) immediately instead
/// of long polling. `sysFlag=4` marks the subscription as carried in the
/// request, which the pull header requires.
async fn pull_check_queue(
    backend: &ProxyBackend,
    topic: &str,
    group: &str,
    offset: u64,
) -> Result<Command> {
    backend
        .invoke_broker(
            topic,
            11,
            &[
                ("topic", topic.to_string()),
                ("consumerGroup", group.to_string()),
                ("queueId", "0".to_string()),
                ("queueOffset", offset.to_string()),
                ("maxMsgNums", "64".to_string()),
                ("sysFlag", "4".to_string()),
                ("expressionType", "TAG".to_string()),
                ("subExpression", "*".to_string()),
                ("subscription", "*".to_string()),
            ],
            Vec::new(),
        )
        .await
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
            Err(_) => {
                return Err(Error::Timeout(format!(
                    "pool checkout timed out for {addr}"
                )));
            }
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
                return Err(Error::Invalid(
                    "pooled RPC response has invalid flags".into(),
                ));
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
    /// Cached consumer-group configs, keyed by group name. Java keeps the same
    /// data in a Guava cache (`ClusterMetadataService.subscriptionGroupConfigCache`)
    /// so a client that re-reports its `Settings` does not re-query the broker.
    group_configs: Arc<Mutex<HashMap<String, (crate::schedule::GroupConfig, Instant)>>>,
}

impl Clone for ProxyBackend {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            nameservers: self.nameservers.clone(),
            timeout: self.timeout,
            cursor: AtomicUsize::new(self.cursor.load(Ordering::Relaxed)),
            group_configs: Arc::clone(&self.group_configs),
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
            group_configs: Arc::new(Mutex::new(HashMap::new())),
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
            match self
                .client
                .invoke(addr, request.clone(), self.timeout)
                .await
            {
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
        Err(Error::NotFound(format!(
            "no master broker for topic {topic}"
        )))
    }

    /// Java `ClusterMetadataService.findOneBroker`: pick one broker of the cluster.
    /// The namesrv's `GET_BROKER_CLUSTER_INFO` (code 106) returns the whole
    /// `brokerAddrTable`, from which any broker of the cluster can be selected.
    pub async fn one_broker_addr(&self, cluster: &str) -> Result<String> {
        let response = self.invoke_namesrv(106, &[], Vec::new()).await?;
        if response.code != 0 {
            return Err(Error::NotFound(format!(
                "broker cluster info query failed (code {}): {}",
                response.code,
                response.remark.as_deref().unwrap_or("")
            )));
        }
        let value: serde_json::Value = serde_json::from_slice(&response.body)?;
        let table = value
            .get("brokerAddrTable")
            .and_then(serde_json::Value::as_object)
            .ok_or_else(|| Error::NotFound("broker cluster info is empty".into()))?;
        let mut fallback: Option<String> = None;
        for broker in table.values() {
            if !cluster.is_empty()
                && broker
                    .get("cluster")
                    .and_then(serde_json::Value::as_str)
                    .is_some_and(|name| name != cluster)
            {
                continue;
            }
            let Some(addrs) = broker
                .get("brokerAddrs")
                .and_then(serde_json::Value::as_object)
            else {
                continue;
            };
            // Prefer the master (broker id 0), like Java's `BrokerData.selectBrokerAddr`.
            if let Some(addr) = addrs.get("0").and_then(serde_json::Value::as_str) {
                return Ok(addr.to_string());
            }
            if fallback.is_none() {
                fallback = addrs
                    .values()
                    .find_map(|value| value.as_str())
                    .map(str::to_owned);
            }
        }
        fallback.ok_or_else(|| Error::NotFound("no broker registered in the cluster".into()))
    }

    /// Java `MQClientAPIImpl.getSubscriptionGroupConfig`: fetch one consumer group's
    /// config from a broker (remoting code 352). The broker auto-creates an unknown
    /// group, mirroring `SubscriptionGroupManager.findSubscriptionGroupConfig`.
    ///
    /// The answer is cached for `GROUP_CONFIG_CACHE_TTL`, like Java's
    /// `subscriptionGroupConfigCache` (`expireAfterAccess`).
    pub async fn subscription_group_config(
        &self,
        cluster: &str,
        group: &str,
    ) -> Option<crate::schedule::GroupConfig> {
        const GROUP_CONFIG_CACHE_TTL: Duration = Duration::from_secs(60);
        if group.is_empty() {
            return None;
        }
        {
            let cache = self.group_configs.lock().unwrap();
            if let Some((config, fetched_at)) = cache.get(group) {
                if fetched_at.elapsed() < GROUP_CONFIG_CACHE_TTL {
                    return Some(config.clone());
                }
            }
        }
        let addr = self.one_broker_addr(cluster).await.ok()?;
        let request = Command::request(352).with_ext("group", group.to_string());
        let response = self
            .client
            .invoke(&addr, request, self.timeout)
            .await
            .ok()?;
        if response.code != 0 {
            warn!(
                group,
                code = response.code,
                "subscription group config query failed"
            );
            return None;
        }
        let config: crate::schedule::GroupConfig = serde_json::from_slice(&response.body).ok()?;
        self.group_configs
            .lock()
            .unwrap()
            .insert(group.to_string(), (config.clone(), Instant::now()));
        Some(config)
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

    /// `invoke_broker` with an explicit transport timeout. A long-polled pull holds the
    /// broker for the whole suspend window, so its timeout must exceed it (the default
    /// `self.timeout` would abort the request mid-suspend).
    pub async fn invoke_broker_with_timeout(
        &self,
        topic: &str,
        code: i32,
        ext: &[(&str, String)],
        body: Vec<u8>,
        timeout: Duration,
    ) -> Result<Command> {
        let mut request = Command::request(code);
        for (key, value) in ext {
            request = request.with_ext(*key, value.clone());
        }
        request = request.with_body(body);
        let addr = self.master_addr(topic).await?;
        self.client.invoke(&addr, request, timeout).await
    }

    /// Invoke a broker command on a broker that was picked beforehand, instead of
    /// the topic's master. Java's `SendMessageQueueSelector` picks the queue
    /// first and sends the message to the broker that owns it.
    pub async fn invoke_broker_at(
        &self,
        addr: &str,
        code: i32,
        ext: &[(&str, String)],
        body: Vec<u8>,
    ) -> Result<Command> {
        let mut request = Command::request(code);
        for (key, value) in ext {
            request = request.with_ext(*key, value.clone());
        }
        request = request.with_body(body);
        self.client.invoke(&addr, request, self.timeout).await
    }

    /// Java `MessageQueueSelector.buildWrite`: the topic's write queues, sorted
    /// by broker name and queue id.
    ///
    /// Every writable broker contributes `writeQueueNums` queues; the list is
    /// what a message's sharding key is hashed into, so the same group always
    /// lands on the same queue of the same broker.
    pub async fn write_queues(&self, topic: &str) -> Result<Vec<WriteQueue>> {
        const PERM_WRITE: i32 = 0x1 << 1;
        let (brokers, queues) = self.topic_route(topic).await?;
        let mut result: Vec<WriteQueue> = Vec::new();
        for queue in queues {
            if queue.perm & PERM_WRITE == 0 {
                continue;
            }
            let Some(broker) = brokers
                .iter()
                .find(|broker| broker.name == queue.broker_name)
            else {
                continue;
            };
            let Some(addr) = broker.addrs.get(&0) else {
                continue;
            };
            for queue_id in 0..queue.write_queues {
                result.push(WriteQueue {
                    broker_name: queue.broker_name.clone(),
                    queue_id,
                    addr: addr.clone(),
                });
            }
        }
        result.sort_by(|left, right| {
            left.broker_name
                .cmp(&right.broker_name)
                .then(left.queue_id.cmp(&right.queue_id))
        });
        Ok(result)
    }

    /// Resolve the topic's master, then forward a fully built command over the
    /// pooled channel towards it.
    pub async fn invoke_broker_command(&self, topic: &str, request: Command) -> Result<Command> {
        let addr = self.master_addr(topic).await?;
        self.client.invoke(&addr, request, self.timeout).await
    }

    /// Resolve the topic's declared message types from the broker's
    /// `GET_TOPIC_CONFIG_BY_TOPIC` (remoting code 351), which returns the
    /// `TopicConfig` JSON. Java declares a topic's message type through the
    /// `+message.type` attribute (`TopicAttributes.TOPIC_MESSAGE_TYPE_ATTRIBUTE`,
    /// stored with `AttributeParser.ATTR_ADD_PLUS_SIGN`); the legacy `topicSysFlag`
    /// low bits are honoured when set.
    ///
    /// A topic without an explicit declaration is a **NORMAL** topic, like Java's
    /// `TopicConfig.getTopicMessageType()`: a 5.x client refuses to publish a
    /// FIFO / delay / transaction / priority / lite message to it, because the
    /// client checks the message type against `accept_message_types` before it
    /// sends (`ProducerImpl.send0`).
    pub async fn topic_message_types(&self, topic: &str) -> Vec<i32> {
        match self
            .invoke_broker(topic, 351, &[("topic", topic.to_string())], Vec::new())
            .await
        {
            Ok(response) if response.code == 0 => {
                let value: serde_json::Value = match serde_json::from_slice(&response.body) {
                    Ok(v) => v,
                    Err(_) => return all_message_types(),
                };
                if let Some(declared) = value
                    .get("attributes")
                    .and_then(|attributes| attributes.get(MESSAGE_TYPE_ATTRIBUTE))
                    .and_then(|v| v.as_str())
                {
                    return message_types_from_attribute(declared);
                }
                let sys_flag = value
                    .get("topicSysFlag")
                    .and_then(|v| v.as_i64())
                    .unwrap_or(0) as i32;
                if sys_flag != 0 {
                    return message_types_from_sys_flag(sys_flag);
                }
                // Java `TopicConfig.getTopicMessageType()`: no `+message.type`
                // attribute means the topic is a NORMAL topic.
                vec![pb::MessageType::Normal as i32]
            }
            // The topic does not exist (yet): the broker will create it on the
            // first send, so nothing can be validated against it.
            _ => all_message_types(),
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

/// Java `TopicAttributes.TOPIC_MESSAGE_TYPE_ATTRIBUTE`, stored under the
/// `AttributeParser.ATTR_ADD_PLUS_SIGN` ("+") prefixed key.
pub const MESSAGE_TYPE_ATTRIBUTE: &str = "+message.type";

/// Every message type the broker can store, i.e. Java's `TopicMessageType.MIXED`.
fn all_message_types() -> Vec<i32> {
    vec![
        pb::MessageType::Normal as i32,
        pb::MessageType::Fifo as i32,
        pb::MessageType::Delay as i32,
        pb::MessageType::Transaction as i32,
        pb::MessageType::Lite as i32,
        pb::MessageType::Priority as i32,
    ]
}

/// Java `TopicMessageType` -> the v2 `MessageType` values a topic accepts.
fn message_types_from_attribute(declared: &str) -> Vec<i32> {
    match declared.to_ascii_uppercase().as_str() {
        "NORMAL" => vec![pb::MessageType::Normal as i32],
        "FIFO" => vec![pb::MessageType::Fifo as i32],
        "DELAY" => vec![pb::MessageType::Delay as i32],
        "TRANSACTION" => vec![pb::MessageType::Transaction as i32],
        "LITE" => vec![pb::MessageType::Lite as i32],
        "PRIORITY" => vec![pb::MessageType::Priority as i32],
        // `MIXED`, and anything unknown, accepts the four classic types, like
        // Java's `RouteActivity.parseTopicMessageType`: a mixed topic takes
        // normal, FIFO, delay and transactional messages, but not lite or
        // priority ones.
        _ => mixed_message_types(),
    }
}

/// Java `TopicMessageType.MIXED`: the message types a mixed topic accepts.
fn mixed_message_types() -> Vec<i32> {
    vec![
        pb::MessageType::Normal as i32,
        pb::MessageType::Fifo as i32,
        pb::MessageType::Delay as i32,
        pb::MessageType::Transaction as i32,
    ]
}

/// The type of a message that is about to be published.
///
/// A 5.x client states the type in `system_properties.message_type`
/// (`PublishingMessageImpl`); when it does not, the type follows from the
/// message's own fields, like Java's `TopicMessageType.parseFromMessageProperty`.
fn message_type_of(message: &pb::Message) -> i32 {
    let Some(system) = message.system_properties.as_ref() else {
        return pb::MessageType::Normal as i32;
    };
    if system.message_type != 0 {
        return system.message_type;
    }
    if system
        .message_group
        .as_ref()
        .is_some_and(|group| !group.trim().is_empty())
    {
        return pb::MessageType::Fifo as i32;
    }
    if system
        .lite_topic
        .as_ref()
        .is_some_and(|topic| !topic.trim().is_empty())
    {
        return pb::MessageType::Lite as i32;
    }
    if system.delivery_timestamp.is_some() {
        return pb::MessageType::Delay as i32;
    }
    if system.priority.is_some() {
        return pb::MessageType::Priority as i32;
    }
    pb::MessageType::Normal as i32
}

/// The Java name of a v2 message type, used in the validation error message.
fn message_type_name(message_type: i32) -> &'static str {
    match message_type {
        value if value == pb::MessageType::Normal as i32 => "NORMAL",
        value if value == pb::MessageType::Fifo as i32 => "FIFO",
        value if value == pb::MessageType::Delay as i32 => "DELAY",
        value if value == pb::MessageType::Transaction as i32 => "TRANSACTION",
        value if value == pb::MessageType::Lite as i32 => "LITE",
        value if value == pb::MessageType::Priority as i32 => "PRIORITY",
        _ => "MESSAGE_TYPE_UNSPECIFIED",
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

const PROPERTIES: [&str; 6] = [
    "TAGS",
    "KEYS",
    "UNIQ_KEY",
    "SHARDING_KEY",
    "RECONSUME_TIMES",
    "PRIORITY",
];

/// Java `MessageConst.PROPERTY_LITE_TOPIC`: the lite topic a message belongs to.
/// A lite message is stored in its bind (parent) topic and carries the lite
/// topic name in this property, which is how a lite consumer tells the
/// messages of the lite topics it subscribed to apart.
const LITE_TOPIC_PROPERTY: &str = "__LITE_TOPIC";

/// Build the remoting `properties` header string from a v2 message.
/// Java `MessageSysFlag.TRANSACTION_PREPARED_TYPE`: a transactional message is stored as a
/// half message until the producer resolves it with `END_TRANSACTION`.
const TRANS_PREPARED_FLAG: i32 = 0x4;
/// Java `MessageConst.PROPERTY_TRANSACTION_PREPARED`.
const TRAN_MSG_PROPERTY: &str = "TRAN_MSG";

/// Whether the v2 message asks for a transactional (half) send.
fn is_transactional(message: &pb::Message) -> bool {
    message
        .system_properties
        .as_ref()
        .map(|system| system.message_type == pb::MessageType::Transaction as i32)
        .unwrap_or(false)
}

/// Java `String.hashCode()`: `s[0]*31^(n-1) + s[1]*31^(n-2) + … + s[n-1]`, over
/// the UTF-16 code units.
fn java_string_hash(value: &str) -> i32 {
    let mut hash: i32 = 0;
    for unit in value.encode_utf16() {
        hash = hash.wrapping_mul(31).wrapping_add(i32::from(unit));
    }
    hash
}

/// Guava `Hashing.consistentHash(long, int)`: the bucket a hash code falls into.
///
/// Guava spreads the code with a linear congruential generator and then walks
/// up the bucket numbers until one of them is out of range, so the mapping only
/// depends on the hash code and the bucket count. The proxy uses it to pin every
/// message of a message group onto the same queue, which is what gives FIFO
/// messages their order.
fn consistent_hash(hash: i64, buckets: usize) -> usize {
    if buckets == 0 {
        return 0;
    }
    let mut state = hash as u64;
    let mut result: usize = 0;
    loop {
        state = state
            .wrapping_mul(2_862_933_555_777_941_757)
            .wrapping_add(1);
        // `((int) (state >>> 33) + 1) / 2.147483648E9`
        let unit = ((state >> 33) as u32 as f64 + 1.0) / 2_147_483_648.0;
        // The float-to-int cast saturates, like Java's `d2i`.
        let candidate = ((result as f64 + 1.0) / unit) as i32;
        if candidate >= 0 && (candidate as usize) < buckets {
            result = candidate as usize;
        } else {
            break;
        }
    }
    result
}

/// The write queue a sharded message goes to.
#[derive(Clone, Debug)]
pub struct WriteQueue {
    broker_name: String,
    queue_id: i32,
    addr: String,
}

fn encode_send_properties(message: &pb::Message) -> Result<String> {
    let mut properties: BTreeMap<String, String> = BTreeMap::new();
    if is_transactional(message) {
        // `MessageConst.PROPERTY_TRANSACTION_PREPARED`: the broker keeps the message in
        // `RMQ_SYS_TRANS_HALF_TOPIC` until the producer commits or rolls it back.
        properties.insert(TRAN_MSG_PROPERTY.into(), "true".into());
    }
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
        // `MessageConst.PROPERTY_PRIORITY`: the broker stores the priority as a plain
        // property, and `to_pb_message` reads it back so `MessageView.getPriority` works.
        if let Some(priority) = system.priority {
            properties.insert("PRIORITY".into(), priority.to_string());
        }
        if let Some(lite_topic) = &system.lite_topic {
            if !lite_topic.is_empty() {
                properties.insert(LITE_TOPIC_PROPERTY.into(), lite_topic.clone());
            }
        }
    }
    for (key, value) in &message.user_properties {
        properties.insert(key.clone(), value.clone());
    }
    // A v2 message carries its delivery deadline in `system_properties`; the broker's
    // timer wheel only recognises the `TIMER_*` properties, so the deadline has to be
    // translated here or the message would be delivered immediately.
    if let Some(delivery) = message
        .system_properties
        .as_ref()
        .and_then(|system| system.delivery_timestamp.as_ref())
    {
        let deliver_ms = delivery.seconds * 1000 + i64::from(delivery.nanos) / 1_000_000;
        properties.insert(
            crate::schedule::TIMER_DELIVER_MS.into(),
            deliver_ms.to_string(),
        );
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
        lite_topic: message.properties.get(LITE_TOPIC_PROPERTY).cloned(),
        priority: message
            .properties
            .get("PRIORITY")
            .and_then(|value| value.parse().ok()),
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

/// Decode a QUERY_MESSAGE body: `u32 BE count`, then per message `u32 BE length + record`.
fn decode_query_frame(body: &[u8]) -> Result<Vec<common::Message>> {
    let mut cursor = 0usize;
    let take_u32 = |cursor: &mut usize| -> Result<u32> {
        if body.len() < *cursor + 4 {
            return Err(Error::Invalid("truncated query frame".into()));
        }
        let value = u32::from_be_bytes(body[*cursor..*cursor + 4].try_into().unwrap());
        *cursor += 4;
        Ok(value)
    };
    let count = take_u32(&mut cursor)?;
    let mut messages = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let length = take_u32(&mut cursor)? as usize;
        if body.len() < cursor + length {
            return Err(Error::Invalid("truncated query record".into()));
        }
        let (message, consumed) = common::Message::decode(&body[cursor..cursor + length])?;
        if consumed != length {
            return Err(Error::Invalid(
                "trailing bytes inside a query record".into(),
            ));
        }
        cursor += length;
        messages.push(message);
    }
    if cursor != body.len() {
        return Err(Error::Invalid(
            "trailing bytes after the query frame".into(),
        ));
    }
    Ok(messages)
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
        let (access_key, signature) = Self::parse_authorization(header).ok_or_else(|| {
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
            info!(
                %topic, %group, queue_id, code = response.code,
                "committed_offset: no commit found, starting from 0"
            );
            return Ok(0);
        }
        let offset = response
            .ext_fields
            .get("offset")
            .and_then(|v| v.parse().ok())
            .unwrap_or(0);
        info!(%topic, %group, queue_id, offset, "committed_offset");
        Ok(offset)
    }

    /// `POP_MESSAGE`: pop a message of the topic for the group.
    ///
    /// Java `ReceiveMessageActivity` hands the request to the broker's pop, which picks
    /// the queue, keeps the message invisible and answers with a receipt handle (the
    /// pop ck). A queue id below zero means "any queue of the topic", which is what the
    /// client asks for when the assignment only names the topic.
    async fn pop_once(
        &self,
        topic: &str,
        group: &str,
        queue_id: i32,
        batch: i32,
        invisible_time: i64,
        long_polling_ms: i64,
        filter: Option<pb::FilterExpression>,
        client_id: &str,
    ) -> Result<Vec<common::Message>> {
        // The broker refuses a pop of a group it does not know, and it creates the group
        // when its config is read (Java `SubscriptionGroupManager`).
        self.backend
            .subscription_group_config("", group)
            .await
            .ok_or_else(|| {
                Error::NotFound(format!("subscription group [{group}] does not exist"))
            })?;
        let mut fields = vec![
            ("topic", topic.to_string()),
            ("consumerGroup", group.to_string()),
            ("queueId", queue_id.to_string()),
            ("maxMsgNums", batch.max(1).to_string()),
            ("invisibleTime", invisible_time.to_string()),
            ("popTimeout", long_polling_ms.clamp(0, 30_000).to_string()),
        ];
        if let Some(filter) = filter.as_ref() {
            if !filter.expression.is_empty() && filter.expression != "*" {
                fields.push(("exp", filter.expression.clone()));
                fields.push((
                    "expType",
                    if filter.r#type == 2 {
                        "SQL92".to_string()
                    } else {
                        "TAG".to_string()
                    },
                ));
            }
        }
        // The broker holds the pop for the whole long-poll window; the transport needs
        // the suspend plus a margin instead of the default backend timeout.
        let timeout = Duration::from_millis(
            u64::try_from(long_polling_ms.clamp(0, 30_000)).unwrap_or(30_000) + 3_000,
        );
        let response = self
            .backend
            .invoke_broker_with_timeout(topic, 200050, &fields, Vec::new(), timeout)
            .await?;
        if response.code != 0 {
            return Err(Error::NotFound(format!(
                "pop failed (code {}): {}",
                response.code,
                response.remark.as_deref().unwrap_or("")
            )));
        }
        let mut messages = Vec::new();
        let mut rest = response.body.as_slice();
        while !rest.is_empty() {
            match common::Message::decode(rest) {
                Ok((message, consumed)) if consumed > 0 => {
                    let handle = message
                        .properties
                        .get(common::PROPERTY_POP_CK)
                        .cloned()
                        .unwrap_or_default();
                    if let Some(meta) = common::parse_pop_ck(&handle) {
                        self.context.clients.cache_handle(client_id, &handle, meta);
                    }
                    messages.push(message);
                    rest = &rest[consumed..];
                }
                Ok(_) => break,
                Err(error) => {
                    warn!(%error, "stopping pop body decode at undecodable record");
                    break;
                }
            }
        }
        Ok(messages)
    }

    /// `POP_LITE_MESSAGE`: read the light queues of the lite topics a client
    /// subscribed to. The broker holds the lite subscriptions of every client, so
    /// it knows which light queues belong to this consumer.
    async fn pop_lite_once(
        &self,
        bind_topic: &str,
        group: &str,
        client_id: &str,
        batch: i32,
        invisible_time: i64,
        long_polling_ms: i64,
    ) -> Result<Vec<common::Message>> {
        let timeout = Duration::from_millis(
            u64::try_from(long_polling_ms.clamp(0, 30_000)).unwrap_or(30_000) + 3_000,
        );
        let response = self
            .backend
            .invoke_broker_with_timeout(
                bind_topic,
                200070,
                &[
                    ("clientId", client_id.to_string()),
                    ("consumerGroup", group.to_string()),
                    ("topic", bind_topic.to_string()),
                    ("maxMsgNum", batch.max(1).to_string()),
                    ("invisibleTime", invisible_time.to_string()),
                    ("popTimeout", long_polling_ms.clamp(0, 30_000).to_string()),
                ],
                Vec::new(),
                timeout,
            )
            .await?;
        if response.code != 0 {
            // A lite consumer that has nothing registered yet simply gets nothing.
            return Ok(Vec::new());
        }
        let mut messages = Vec::new();
        let mut rest = response.body.as_slice();
        while !rest.is_empty() {
            match common::Message::decode(rest) {
                Ok((message, consumed)) if consumed > 0 => {
                    let handle = message
                        .properties
                        .get(common::PROPERTY_POP_CK)
                        .cloned()
                        .unwrap_or_default();
                    if let Some(meta) = common::parse_pop_ck(&handle) {
                        self.context.clients.cache_handle(client_id, &handle, meta);
                    }
                    messages.push(message);
                    rest = &rest[consumed..];
                }
                Ok(_) => break,
                Err(error) => {
                    warn!(%error, "stopping lite pop body decode at undecodable record");
                    break;
                }
            }
        }
        Ok(messages)
    }

    async fn pull_once(
        &self,
        topic: &str,
        group: &str,
        queue_id: i32,
        offset: i64,
        batch: i32,
        filter: Option<pb::FilterExpression>,
        suspend_ms: i64,
    ) -> Result<(Vec<common::Message>, i64, i64)> {
        // The broker learns a proxy-managed consumer group's subscription through the pull
        // request itself (Java `compensateSubscribeData`); forward the real filter expression
        // so the broker can register it and stop returning `code 24`.
        let sub_expr = match &filter {
            Some(f) if !f.expression.is_empty() => f.expression.clone(),
            _ => "*".to_string(),
        };
        let sub_type = match filter.as_ref().map(|f| f.r#type) {
            Some(2) => "SQL92".to_string(),
            _ => "TAG".to_string(),
        };
        // `PullMessageRequestHeader.sysFlag` `SUSPEND_FLAG` (0x2): the broker holds an
        // empty read for `suspendTimeoutMillis` (Java `PullMessageProcessor` long polling),
        // which is what turns the client's `awaitDuration` into a real long poll instead
        // of an immediate empty answer. The broker caps the suspend at 30s.
        let suspend_ms = suspend_ms.clamp(0, 30_000);
        let sys_flag = if suspend_ms > 0 { 2 } else { 0 };
        let mut fields = vec![
            ("topic", topic.to_string()),
            ("consumerGroup", group.to_string()),
            ("queueId", queue_id.to_string()),
            ("queueOffset", offset.to_string()),
            ("maxMsgNums", batch.max(1).to_string()),
            ("sysFlag", sys_flag.to_string()),
            ("expressionType", sub_type),
            ("subExpression", "*".to_string()),
            ("subscription", sub_expr),
            ("subVersion", "0".to_string()),
        ];
        if suspend_ms > 0 {
            fields.push(("suspendTimeoutMillis", suspend_ms.to_string()));
        }
        let response = if suspend_ms > 0 {
            // The broker holds this request for the whole suspend window; allow the
            // transport the suspend plus a margin instead of the default 3s timeout.
            self.backend
                .invoke_broker_with_timeout(
                    topic,
                    11,
                    &fields,
                    Vec::new(),
                    Duration::from_millis(u64::try_from(suspend_ms).unwrap_or(30_000) + 3_000),
                )
                .await?
        } else {
            self.backend
                .invoke_broker(topic, 11, &fields, Vec::new())
                .await?
        };
        let next_begin = response
            .ext_fields
            .get("nextBeginOffset")
            .and_then(|v| v.parse().ok())
            .unwrap_or(offset);
        let max = response
            .ext_fields
            .get("maxOffset")
            .and_then(|v| v.parse().ok())
            .unwrap_or(offset);
        let min = response
            .ext_fields
            .get("minOffset")
            .and_then(|v| v.parse().ok())
            .unwrap_or(offset);
        // The broker reports "nothing to read yet" as a normal outcome of the long
        // poll: 19 = no message at the offset, 20 = the offset moved forward,
        // 21 = the offset is out of range. Java's proxy turns these into an empty
        // pull result plus the offset to retry from, not into a failure.
        info!(
            %topic, %group, queue_id, offset, suspend_ms, code = response.code,
            messages = response.body.len(),
            next_begin, max, min,
            "pull_once: broker response"
        );
        match response.code {
            0 => {}
            19 => return Ok((Vec::new(), offset, max)),
            20 => return Ok((Vec::new(), next_begin, max)),
            21 => return Ok((Vec::new(), min, max)),
            code => {
                return Err(Error::NotFound(format!(
                    "pull failed (code {}): {}",
                    code,
                    response.remark.as_deref().unwrap_or("")
                )));
            }
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
        let next = next_begin.max(offset + messages.len() as i64);
        Ok((messages, next, max.max(next)))
    }

    /// Java `SendMessageQueueSelector.select`: the write queue a message goes to.
    ///
    /// A message that carries a message group (the sharding key) is pinned to
    /// one queue of the cluster, so all the messages of a group keep their
    /// order; a lite message falls back to its lite topic as the key. A message
    /// without either has no queue of its own and is left to the broker's round
    /// robin.
    async fn select_write_queue(&self, topic: &str, message: &pb::Message) -> Option<WriteQueue> {
        let system = message.system_properties.as_ref()?;
        let sharding_key = system
            .message_group
            .as_deref()
            .filter(|key| !key.trim().is_empty())
            .or_else(|| {
                system
                    .lite_topic
                    .as_deref()
                    .filter(|key| !key.trim().is_empty())
            })?;
        let queues = self.backend.write_queues(topic).await.ok()?;
        if queues.is_empty() {
            return None;
        }
        let bucket = consistent_hash(i64::from(java_string_hash(sharding_key)), queues.len());
        queues.get(bucket).cloned()
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
        let (_brokers, queues) = self
            .backend
            .topic_route(&topic)
            .await
            .map_err(|e| self.error_status(&e))?;
        // Advertise the topic's real message types (FIFO/Delay/Transaction/Mixed)
        // instead of the previous always-NORMAL placeholder.
        let accept_message_types = self.backend.topic_message_types(&topic).await;
        let mut message_queues = Vec::new();
        // Clients reach the cluster exclusively through this proxy's gRPC endpoint,
        // so every queue advertises the proxy address rather than the broker's
        // remoting port (which would reset a gRPC connection attempt).
        let endpoints = endpoints_from_addr(&self.context.grpc_listen_addr);
        for queue in &queues {
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
        // Java `DefaultTopicMessageTypeValidator`: a message's type must match
        // the type its topic declares. The topic types are looked up once per
        // topic per request.
        let mut accepted_types: std::collections::HashMap<String, Vec<i32>> =
            std::collections::HashMap::new();
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
            let actual_type = message_type_of(message);
            let accepted = match accepted_types.get(&topic) {
                Some(types) => types.clone(),
                None => {
                    let types = self.backend.topic_message_types(&topic).await;
                    accepted_types.insert(topic.clone(), types.clone());
                    types
                }
            };
            if !accepted.contains(&actual_type) {
                entries.push(pb::SendResultEntry {
                    status: Some(pb_status(
                        pb::Code::MessagePropertyConflictWithType,
                        format!(
                            "TopicMessageType validate failed, the expected type is {}, but actual type is {}",
                            accepted
                                .iter()
                                .map(|value| message_type_name(*value))
                                .collect::<Vec<_>>()
                                .join(","),
                            message_type_name(actual_type)
                        ),
                    )),
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
            // Java `SendMessageQueueSelector.select`: a message that carries a
            // message group (the sharding key) always goes to the same write
            // queue, which is what keeps FIFO messages in order. A message
            // without one is spread over the queues by the broker's round
            // robin.
            let target = self.select_write_queue(&topic, message).await;
            let (addr, queue_id) = match target {
                Some(queue) => (queue.addr, queue.queue_id),
                None => match self.backend.master_addr(&topic).await {
                    Ok(addr) => (addr, -1),
                    Err(error) => {
                        self.context.record_error();
                        entries.push(pb::SendResultEntry {
                            status: Some(pb_status(pb::Code::InternalError, error.to_string())),
                            ..Default::default()
                        });
                        continue;
                    }
                },
            };
            let response = self
                .backend
                .invoke_broker_at(
                    &addr,
                    10,
                    &[
                        ("topic", topic.clone()),
                        // Java `SendMessageActivity.buildMessage`: "here use topicName as
                        // producerGroup for transactional checker". The 5.x gRPC model has
                        // no separate producer group, so the topic name is the group the
                        // transaction check is routed by.
                        ("producerGroup", topic.clone()),
                        ("queueId", queue_id.to_string()),
                        // A transactional message must carry the prepared flag, so the
                        // broker stores it as a half message (`MessageSysFlag`
                        // `TRANSACTION_PREPARED_TYPE`).
                        (
                            "sysFlag",
                            if is_transactional(message) {
                                TRANS_PREPARED_FLAG.to_string()
                            } else {
                                "0".to_string()
                            },
                        ),
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
                        recall_handle: response
                            .ext_fields
                            .get("recallHandle")
                            .cloned()
                            .unwrap_or_default(),
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
        let (_brokers, queues) = self
            .backend
            .topic_route(&topic)
            .await
            .map_err(|e| self.error_status(&e))?;
        // Java `RouteActivity.queryAssignment`: a group that consumes in order (FIFO)
        // owns every readable queue of the topic, while every other group gets a
        // single assignment per broker whose queue id is -1. The -1 tells the client
        // that the server picks the queue, which is what the pop path does: the broker
        // chooses the queue and keeps the message invisible until it is acked. Slicing
        // the queues per client would make two clients of one group read the same queue.
        let group_config = self.backend.subscription_group_config("", &group).await;
        let fifo = group_config
            .as_ref()
            .is_some_and(|config| config.consume_message_orderly);
        // A lite group reads light queues, never the bind topic's own queues, so it
        // gets the "server picks" assignment even when it consumes in order.
        let lite = group_config
            .as_ref()
            .is_some_and(|config| config.lite_bind_topic().is_some_and(|t| !t.is_empty()));
        // Java iterates the route's queue data, which holds one entry per broker.
        // `PermName.PERM_READ` is `1 << 2`.
        const PERM_READ: i32 = 0x1 << 2;
        let owned: Vec<(&QueueRoute, i32)> = queues
            .iter()
            .filter(|queue| queue.perm & PERM_READ != 0 && queue.read_queues > 0)
            .flat_map(|queue| {
                if fifo && !lite {
                    (0..queue.read_queues)
                        .map(move |id| (queue, id))
                        .collect::<Vec<_>>()
                } else {
                    vec![(queue, -1)]
                }
            })
            .collect();

        // Same type fidelity as QueryRoute: advertise the topic's real message types.
        let accept_message_types = self.backend.topic_message_types(&topic).await;
        let mut assignments = Vec::new();
        // As in `query_route`, the proxy's gRPC endpoint is what clients connect to.
        let endpoints = endpoints_from_addr(&self.context.grpc_listen_addr);
        for (queue, id) in owned {
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
        if assignments.is_empty() {
            // Java answers `FORBIDDEN, "no readable queue"` when the topic has none.
            return Ok(Response::new(pb::QueryAssignmentResponse {
                status: Some(pb_status(pb::Code::Forbidden, "no readable queue")),
                assignments,
            }));
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
        self.context.clients.touch(&client_id);
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
                    mq.topic
                        .as_ref()
                        .map(|t| t.name.clone())
                        .unwrap_or_default(),
                    mq.id,
                )
            })
            .unwrap_or_default();
        // LITE push consumers fan out across their registered lite topic set, which may
        // leave the explicit `messageQueue` empty, so only the group is strictly required.
        if group.is_empty() {
            return Err(Status::invalid_argument(
                "group and messageQueue are required",
            ));
        }
        let batch = request.batch_size.max(1);
        let invisible_time = request
            .invisible_duration
            .as_ref()
            .map(|duration| duration.seconds * 1_000 + duration.nanos as i64 / 1_000_000)
            .unwrap_or(15_000);
        // The client's `awaitDuration` rides the request as `long_polling_timeout`; the
        // pull must suspend for it (Java `PullMessageProcessor` long polling) or every
        // receive on an empty queue returns immediately instead of waiting for a send.
        let mut long_polling_ms = request
            .long_polling_timeout
            .as_ref()
            .map(|duration| duration.seconds * 1_000 + duration.nanos as i64 / 1_000_000)
            .unwrap_or(0);
        // Java `ReceiveMessageActivity`: validate the invisible duration (min 0, max 12h)
        // and clamp the long-polling window to the configured [min, max] bounds instead of
        // a flat 0..30s clamp, matching `GrpcValidator.validateInvisibleTime` and the polling
        // time clamp in `ReceiveMessageActivity`.
        if invisible_time < 0 || invisible_time > MAX_INVISIBLE_TIME_MS {
            let remark = if invisible_time < 0 {
                "the invisibleTime is too small. min is 0".to_string()
            } else {
                format!("the invisibleTime is too large. max is {MAX_INVISIBLE_TIME_MS}")
            };
            return Err(Status::invalid_argument(remark));
        }
        let min_poll = self
            .context
            .config
            .grpc_client_consumer_min_long_polling_timeout_millis
            .max(0);
        let max_poll = self
            .context
            .config
            .grpc_client_consumer_max_long_polling_timeout_millis
            .max(0);
        if long_polling_ms > 0 {
            long_polling_ms = long_polling_ms.clamp(min_poll, min_poll.max(max_poll));
        }
        let (tx, rx) = mpsc::channel::<std::result::Result<pb::ReceiveMessageResponse, Status>>(16);
        let this = Arc::new(self.clone_for_stream());
        tokio::spawn(async move {
            // Lite routing: a LITE push consumer reads the light queues of the lite
            // topics it registered via `SyncLiteSubscription`. Every lite topic has a
            // queue and an offset of its own, so a lite consumer neither competes
            // with the normal consumers of the bind topic nor shares their offset.
            let mut lite_topics = if !client_id.is_empty() {
                this.context.clients.lite_topics_for(&client_id)
            } else {
                Vec::new()
            };
            if lite_topics.is_empty() {
                lite_topics = this.context.clients.lite_topics_for_group(&group);
            }
            // A lite consumer whose lite set is currently empty must get nothing: falling
            // back to a plain pull of the bind topic would hand it the dual-dispatched
            // lite messages anyway (Java `LitePushConsumerImpl` pops only light queues).
            if lite_topics.is_empty() && this.context.clients.is_lite_consumer(&client_id, &group) {
                let _ = tx
                    .send(Ok(pb::ReceiveMessageResponse {
                        content: Some(pb::receive_message_response::Content::Status(pb_status(
                            pb::Code::Ok,
                            "",
                        ))),
                    }))
                    .await;
                return;
            }
            if !lite_topics.is_empty() {
                // The bind topic is the topic the client was assigned; the lite topics
                // it subscribed to hang below it.
                let bind_topic = if topic.is_empty() {
                    bind_topic_of(&this.context.clients, &client_id, &group)
                } else {
                    topic.clone()
                };
                match this
                    .pop_lite_once(
                        &bind_topic,
                        &group,
                        &client_id,
                        batch,
                        invisible_time,
                        long_polling_ms,
                    )
                    .await
                {
                    Ok(messages) => {
                        for message in &messages {
                            // The receipt handle is the pop ck the broker stamped on the
                            // message; it names the light queue, so the ack commits the
                            // offset of the lite topic's own queue.
                            let handle = message
                                .properties
                                .get(common::PROPERTY_POP_CK)
                                .cloned()
                                .unwrap_or_default();
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
                return;
            }

            // Java `ReceiveMessageActivity` pops the message: the broker picks the queue,
            // keeps the message invisible for the invisible duration and hands out a
            // receipt handle (the pop ck). The ack and the invisible-duration change
            // are addressed by that handle, so an unacked message is not handed out
            // again before its invisible window ends.
            let filter = this.context.clients.subscription_for(&client_id, &topic);
            match this
                .pop_once(
                    &topic,
                    &group,
                    queue_id,
                    batch,
                    invisible_time,
                    long_polling_ms,
                    filter,
                    &client_id,
                )
                .await
            {
                Ok(messages) => {
                    for message in &messages {
                        let handle = message
                            .properties
                            .get(common::PROPERTY_POP_CK)
                            .cloned()
                            .unwrap_or_default();
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
        let client_id = client_id_of(&request).unwrap_or_default();
        self.context.clients.touch(&client_id);
        let request = request.into_inner();
        self.context.record_op(&self.context.ack_ops);
        let topic = request
            .topic
            .as_ref()
            .map(|t| t.name.clone())
            .unwrap_or_default();
        let mut entries = Vec::with_capacity(request.entries.len());
        for entry in &request.entries {
            // The receipt handle is the pop ck the broker handed out; the ack tells the
            // broker that the message is done, which clears its invisible entry and
            // advances the group's committed offset.
            let handle = entry.receipt_handle.clone();
            let result = self
                .backend
                .invoke_broker(&topic, 200051, &[("extraInfo", handle)], Vec::new())
                .await;
            let status = match result {
                Ok(response) if response.code == 0 => pb_status(pb::Code::Ok, ""),
                Ok(response) => pb_status(
                    map_response_code(response.code),
                    response.remark.as_deref().unwrap_or("ack failed"),
                ),
                Err(error) => pb_status(pb::Code::InternalError, error.to_string()),
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
        // The pop ck carries the origin queue coordinates; `delayLevel = -1` makes the
        // broker's send-back route straight into the group's dead-letter topic.
        let Some((_, _, queue_id, offset)) = common::parse_pop_ck(&request.receipt_handle) else {
            return Ok(Response::new(pb::ForwardMessageToDeadLetterQueueResponse {
                status: Some(pb_status(
                    pb::Code::InvalidReceiptHandle,
                    "unparseable receipt handle",
                )),
            }));
        };
        let offset = u64::from(offset);
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

    type PullMessageStream = ReceiverStream<std::result::Result<pb::PullMessageResponse, Status>>;

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
                    mq.topic
                        .as_ref()
                        .map(|t| t.name.clone())
                        .unwrap_or_default(),
                    mq.id,
                )
            })
            .unwrap_or_default();
        if topic.is_empty() || group.is_empty() {
            return Err(Status::invalid_argument(
                "group and messageQueue are required",
            ));
        }
        let offset = request.offset;
        let batch = request.batch_size.max(1);
        let (tx, rx) = mpsc::channel::<std::result::Result<pb::PullMessageResponse, Status>>(16);
        let this = Arc::new(self.clone_for_stream());
        tokio::spawn(async move {
            match this
                .pull_once(&topic, &group, queue_id, offset, batch, None, 0)
                .await
            {
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
                            content: Some(pb::pull_message_response::Content::NextOffset(
                                next.max(max),
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
                    mq.topic
                        .as_ref()
                        .map(|t| t.name.clone())
                        .unwrap_or_default(),
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
                    mq.topic
                        .as_ref()
                        .map(|t| t.name.clone())
                        .unwrap_or_default(),
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
                    mq.topic
                        .as_ref()
                        .map(|t| t.name.clone())
                        .unwrap_or_default(),
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
                    response
                        .remark
                        .as_deref()
                        .unwrap_or("end transaction failed"),
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
        let backend = self.backend.clone();
        let proxy_config = self.context.config.clone();
        tokio::spawn(async move {
            while let Some(item) = inbound.next().await {
                match item {
                    Ok(cmd) => {
                        if let Some(pb::telemetry_command::Command::Settings(settings)) =
                            cmd.command.clone()
                        {
                            apply_settings(&recv_clients, &cid, &settings);
                            // Java `ClientActivity.processAndWriteClientSettings`: the
                            // client reports its `Settings`, the server merges its own
                            // data into them and writes the result back on the same
                            // telemetry stream. The merged `Settings` is what carries
                            // the consumer group's retry policy — without it a 5.x
                            // push consumer never starts scanning for assignments.
                            let merged =
                                merge_client_settings(&backend, &proxy_config, &settings).await;
                            let _ = tx
                                .send(Ok(pb::TelemetryCommand {
                                    command: Some(pb::telemetry_command::Command::Settings(merged)),
                                    ..Default::default()
                                }))
                                .await;
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
        let client_id = client_id_of(&request).unwrap_or_default();
        self.context.clients.touch(&client_id);
        let _ = self
            .context
            .clients
            .lookup_handle(&client_id, &request.get_ref().receipt_handle);
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
            .map(|duration| {
                duration.seconds.saturating_mul(1000) + (duration.nanos / 1_000_000) as i64
            })
            .unwrap_or(0);
        // Java `GrpcValidator.validateInvisibleTime` (the single-arg overload, min 0)
        // plus `ProxyConfig.maxInvisibleTimeMills` (default 12h): a negative or
        // oversized duration is refused with ILLEGAL_INVISIBLE_TIME before the broker
        // is contacted.
        if duration_ms < 0 || duration_ms > MAX_INVISIBLE_TIME_MS {
            let remark = if duration_ms < 0 {
                "the invisibleTime is too small. min is 0".to_string()
            } else {
                format!("the invisibleTime is too large. max is {MAX_INVISIBLE_TIME_MS}")
            };
            return Ok(Response::new(pb::ChangeInvisibleDurationResponse {
                status: Some(pb_status(pb::Code::IllegalInvisibleTime, remark)),
                ..Default::default()
            }));
        }
        // The receipt handle is the pop ck the broker handed out; the invisible span of
        // that outstanding message is changed on the broker.
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
                &[
                    ("topic", topic.clone()),
                    ("recallHandle", request.recall_handle),
                ],
                Vec::new(),
            )
            .await;
        match result {
            Ok(response) if response.code == 0 => Ok(Response::new(pb::RecallMessageResponse {
                status: Some(pb_status(pb::Code::Ok, "")),
                message_id: response
                    .ext_fields
                    .get("msgId")
                    .cloned()
                    .unwrap_or_default(),
            })),
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
            // The broker keeps the lite subscriptions of every client, so its lite
            // pop path knows which light queues to read (Java
            // `LiteSubscriptionService.syncLiteSubscription`).
            let _ = self
                .backend
                .invoke_broker(
                    &topic,
                    200071,
                    &[
                        ("clientId", client_id.clone()),
                        ("consumerGroup", group.clone()),
                        ("topic", topic.clone()),
                        ("action", request.action.to_string()),
                        ("liteTopicSet", request.lite_topic_set.join(",")),
                    ],
                    Vec::new(),
                )
                .await;
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
                    .ok_or_else(|| Status::not_found(format!("no master for queue of {topic}")))?;
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
                            searched.remark.as_deref().unwrap_or("offset search failed"),
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
        let Some(search_key) = request.search_key.clone() else {
            return Err(Status::invalid_argument("search_key is required"));
        };
        let begin = request
            .begin_timestamp
            .as_ref()
            .map(|ts| ts.seconds * 1000 + (ts.nanos / 1_000_000) as i64)
            .unwrap_or(0);
        let end = request
            .end_timestamp
            .as_ref()
            .map(|ts| ts.seconds * 1000 + (ts.nanos / 1_000_000) as i64)
            .unwrap_or_else(now_millis);
        let max_nums = if request.max_message_nums <= 0 {
            32
        } else {
            request.max_message_nums.min(64) as usize
        };
        // By id: the msg id carries the physical offset `VIEW_MESSAGE_BY_ID` resolves.
        // Everything else rides the broker's QUERY_MESSAGE (code 12): key -> commitlog
        // key index; subscription / lite topic -> filtered time-range scan.
        let (scan_topic, exts): (String, Vec<(&str, String)>) = match &search_key {
            pb::list_message_request::SearchKey::MessageId(_) => (topic.clone(), Vec::new()),
            pb::list_message_request::SearchKey::MessageKey(key) => (
                topic.clone(),
                vec![
                    ("key", key.clone()),
                    ("maxNum", max_nums.to_string()),
                    ("beginTimestamp", begin.to_string()),
                    ("endTimestamp", end.to_string()),
                ],
            ),
            pb::list_message_request::SearchKey::Subscription(expression) => (
                topic.clone(),
                vec![
                    ("maxNum", max_nums.to_string()),
                    ("beginTimestamp", begin.to_string()),
                    ("endTimestamp", end.to_string()),
                    ("expressionType", "TAG".to_string()),
                    ("expression", expression.clone()),
                ],
            ),
            // A lite topic is a real topic in this store, so scanning it directly
            // answers "every message belonging to the lite topic".
            pb::list_message_request::SearchKey::LiteTopic(lite) => (
                lite.clone(),
                vec![
                    ("maxNum", max_nums.to_string()),
                    ("beginTimestamp", begin.to_string()),
                    ("endTimestamp", end.to_string()),
                ],
            ),
        };
        let code = if matches!(
            search_key,
            pb::list_message_request::SearchKey::MessageId(_)
        ) {
            33
        } else {
            12
        };
        let response = self
            .backend
            .invoke_broker(&scan_topic, code, &exts, Vec::new())
            .await
            .map_err(|error| Status::internal(error.to_string()))?;
        if response.code != 0 {
            return Ok(Response::new(pb::ListMessageResponse {
                status: Some(pb_status(
                    map_response_code(response.code),
                    response.remark.as_deref().unwrap_or("message query failed"),
                )),
                ..Default::default()
            }));
        }
        let messages = if code == 33 {
            let (message, consumed) = common::Message::decode(&response.body)
                .map_err(|error| Status::internal(error.to_string()))?;
            if consumed != response.body.len() {
                return Err(Status::internal("trailing bytes after the viewed record"));
            }
            vec![message]
        } else {
            decode_query_frame(&response.body)
                .map_err(|error| Status::internal(error.to_string()))?
        };
        Ok(Response::new(pb::ListMessageResponse {
            status: Some(pb_status(pb::Code::Ok, "")),
            messages: messages
                .iter()
                .map(|message| to_pb_message(message, None))
                .collect(),
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
            command: Some(
                pb::telemetry_command::Command::PrintThreadStackTraceCommand(
                    pb::PrintThreadStackTraceCommand {
                        nonce: nonce.clone(),
                    },
                ),
            ),
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
                }));
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
            Ok(response) if response.code == 0 => Ok(Response::new(pb::AdminSendMessageResponse {
                status: Some(pb_status(pb::Code::Ok, "")),
                message_id: response
                    .ext_fields
                    .get("msgId")
                    .cloned()
                    .unwrap_or_default(),
            })),
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
/// Background sweep (Java `GrpcChannelManager` cleanup + `MessageReceiptHandleManager` TTL):
/// periodically drops expired receipt handles and clients idle beyond `CHANNEL_EXPIRED_SECS`.
async fn run_registry_reaper(context: Arc<ProxyContext>) {
    let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(
        REGISTRY_REAPER_INTERVAL_SECS,
    ));
    loop {
        interval.tick().await;
        context.clients.reap_expired();
    }
}

pub async fn serve_proxy(
    listener: tokio::net::TcpListener,
    context: Arc<ProxyContext>,
    shutdown: tokio::sync::watch::Receiver<bool>,
) -> Result<()> {
    context.config.validate()?;
    // Background orphaned-transaction recovery: re-issues RecoverOrphanedTransactionCommand
    // to producer clients over their telemetry streams (Java `TransactionalMessageCheckService`).
    tokio::spawn(run_transaction_recovery(Arc::clone(&context)));
    // Background per-channel handle-cache TTL + inactive-client reaper (Java `MessageReceiptHandleManager`
    // + `GrpcChannelManager.scanNotActiveChannel`).
    tokio::spawn(run_registry_reaper(Arc::clone(&context)));
    let messaging = pb::messaging_service_server::MessagingServiceServer::new(
        MessagingProcessor::new(Arc::clone(&context)),
    );
    let admin = pb::admin_server::AdminServer::new(AdminProcessor::new(Arc::clone(&context)));
    info!(addr = %listener.local_addr()?, "grpc proxy listening");
    tonic::transport::Server::builder()
        .add_service(messaging)
        .add_service(admin)
        .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
            let mut shutdown = shutdown;
            let _ = shutdown.changed().await;
        })
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

    /// The queue a message group is pinned to must match Java's proxy, which
    /// hashes the group's name with Guava's `Hashing.consistentHash`. The
    /// expected values come from Guava 33.2.0 itself.
    #[test]
    fn message_group_picks_the_same_queue_as_java() {
        // (group, buckets, expected bucket)
        let cases = [
            ("message_group", 4, 0),
            ("message_group", 5, 4),
            ("message_group", 8, 5),
            ("g1", 7, 6),
            ("A", 8, 0),
            ("group-a", 3, 2),
            ("group-a", 8, 7),
            ("中文组", 8, 7),
        ];
        for (group, buckets, expected) in cases {
            let hash = i64::from(java_string_hash(group));
            assert_eq!(
                consistent_hash(hash, buckets),
                expected,
                "group {group} over {buckets} buckets"
            );
        }
    }

    /// Java's `String.hashCode()`: the value the proxy hashes.
    #[test]
    fn java_string_hash_matches_java() {
        assert_eq!(java_string_hash("message_group"), -1_225_525_177);
        assert_eq!(java_string_hash("g1"), 3242);
        assert_eq!(java_string_hash("A"), 65);
        assert_eq!(java_string_hash("group-a"), 293_427_347);
        assert_eq!(java_string_hash("中文组"), 20_070_666);
    }

    /// A consumer's `Settings` must carry the group's retry policy, or a 5.x
    /// push consumer never starts scanning for assignments.
    #[test]
    fn subscription_settings_carry_the_group_retry_policy() {
        let group = crate::schedule::GroupConfig::new("test");
        let config = ProxyConfig::default();
        let settings = pb::Settings {
            pub_sub: Some(pb::settings::PubSub::Subscription(pb::Subscription {
                group: Some(pb::Resource {
                    name: "test".into(),
                    ..Default::default()
                }),
                ..Default::default()
            })),
            ..Default::default()
        };
        let merged = merge_subscription_settings(&settings, &group, &config);
        let backoff = merged.backoff_policy.expect("backoff policy");
        // Java: `retryMaxTimes + 1` (16 + 1).
        assert_eq!(backoff.max_attempts, 17);
        match backoff.strategy.expect("strategy") {
            pb::retry_policy::Strategy::CustomizedBackoff(strategy) => {
                // The reference's ladder: 1s 5s 10s 30s 1m ... 2h.
                assert_eq!(strategy.next.len(), 18);
                assert_eq!(strategy.next[0], duration_millis(1_000));
                assert_eq!(strategy.next[17], duration_millis(7_200_000));
            }
            other => panic!("unexpected strategy {other:?}"),
        }
        match merged.pub_sub.expect("pub_sub") {
            pb::settings::PubSub::Subscription(subscription) => {
                assert_eq!(subscription.receive_batch_size, Some(32));
                assert_eq!(
                    subscription.long_polling_timeout,
                    Some(duration_millis(20_000))
                );
                assert_eq!(subscription.fifo, Some(false));
                assert_eq!(subscription.lite_subscription_quota, Some(2000));
                assert_eq!(subscription.max_lite_topic_size, Some(64));
            }
            other => panic!("unexpected pub_sub {other:?}"),
        }
    }

    /// An exponential group policy must reach the client as an exponential back-off.
    #[test]
    fn exponential_group_policy_becomes_an_exponential_backoff() {
        let mut group = crate::schedule::GroupConfig::new("fifo-group");
        group.group_retry_policy.policy_type = crate::schedule::GroupRetryPolicyType::EXPONENTIAL;
        group.group_retry_policy.exponential_retry_policy =
            Some(crate::schedule::ExponentialRetryPolicy {
                initial: 5_000,
                max: 7_200_000,
                multiplier: 2,
            });
        let settings = pb::Settings {
            pub_sub: Some(pb::settings::PubSub::Subscription(pb::Subscription {
                group: Some(pb::Resource {
                    name: "fifo-group".into(),
                    ..Default::default()
                }),
                ..Default::default()
            })),
            ..Default::default()
        };
        let merged = merge_subscription_settings(&settings, &group, &ProxyConfig::default());
        match merged
            .backoff_policy
            .expect("backoff")
            .strategy
            .expect("strategy")
        {
            pb::retry_policy::Strategy::ExponentialBackoff(strategy) => {
                assert_eq!(strategy.initial, Some(duration_millis(5_000)));
                assert_eq!(strategy.max, Some(duration_millis(7_200_000)));
                assert_eq!(strategy.multiplier, 2.0);
            }
            other => panic!("unexpected strategy {other:?}"),
        }
    }

    /// The producer side of the merge mirrors `mergeProducerData`.
    #[test]
    fn producer_settings_carry_the_producer_backoff() {
        let settings = pb::Settings {
            pub_sub: Some(pb::settings::PubSub::Publishing(pb::Publishing::default())),
            ..Default::default()
        };
        let merged = merge_producer_settings(&settings, &ProxyConfig::default());
        let backoff = merged.backoff_policy.expect("backoff policy");
        assert_eq!(backoff.max_attempts, 3);
        match backoff.strategy.expect("strategy") {
            pb::retry_policy::Strategy::ExponentialBackoff(strategy) => {
                assert_eq!(strategy.initial, Some(duration_millis(10)));
                assert_eq!(strategy.max, Some(duration_millis(1_000)));
                assert_eq!(strategy.multiplier, 2.0);
            }
            other => panic!("unexpected strategy {other:?}"),
        }
        match merged.pub_sub.expect("pub_sub") {
            pb::settings::PubSub::Publishing(publishing) => {
                assert!(publishing.validate_message_type);
                assert_eq!(publishing.max_body_size, 4 * 1024 * 1024);
            }
            other => panic!("unexpected pub_sub {other:?}"),
        }
    }

    /// A lite message carries its lite topic in `__LITE_TOPIC`, which is how a
    /// lite consumer tells the messages of the lite topics it subscribed to apart.
    #[test]
    fn a_lite_message_carries_its_lite_topic() {
        let message = pb::Message {
            topic: Some(pb::Resource {
                name: "parent_topic".into(),
                ..Default::default()
            }),
            system_properties: Some(pb::SystemProperties {
                lite_topic: Some("lite-topic-1".into()),
                message_type: pb::MessageType::Lite as i32,
                ..Default::default()
            }),
            ..Default::default()
        };
        let properties = encode_send_properties(&message).unwrap();
        assert!(properties.contains(LITE_TOPIC_PROPERTY));
        assert!(properties.contains("lite-topic-1"));
    }

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
        assert_eq!(
            perm_to_permission(queues[0].perm),
            pb::Permission::ReadWrite
        );
        assert_eq!(perm_to_permission(queues[1].perm), pb::Permission::Read);
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
    fn authorization_header_parses_credential_and_signature() {
        let parsed = AclGuard::parse_authorization(
            "MQv2 HMAC-SHA1 Credential=rocketmq, SignedHeaders=, Signature=abc+123==",
        )
        .unwrap();
        assert_eq!(parsed.0, "rocketmq");
        assert_eq!(parsed.1, "abc+123==");
        // Java spelling `AccessKey=` is tolerated too.
        let parsed = AclGuard::parse_authorization("AccessKey=ak,Signature=sig").unwrap();
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
        let (access_key, parsed_signature) = AclGuard::parse_authorization(&header).unwrap();
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
