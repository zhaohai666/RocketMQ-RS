//! Simplified Raft consensus for the controller — the piece Java solves with DLedger/JRaft.
//!
//! Wire format: `RAFT_VOTE=330` / `RAFT_APPEND=331` / `RAFT_HEARTBEAT=332` commands carrying JSON
//! bodies ([`VoteRequest`]/[`AppendRequest`]). A heartbeat is an empty append; the separate code
//! exists so the traffic can be told apart. Responses are [`VoteResponse`]/[`AppendResponse`].
//!
//! How this differs from full Raft / DLedger (deliberate simplifications):
//! - **In-memory state only**: term/vote/log are not persisted. A restarted node loses its log and
//!   re-joins by re-electing; its state machine is rebuilt from newly committed events. Java
//!   DLedger persists the log, which is what makes restart-safe commit indices possible.
//! - **No snapshots / log compaction**: broker-set metadata is tiny, so a full log is acceptable
//!   (Java has snapshot + log truncation).
//! - **Leader-side validation only**: requests are validated against the leader's
//!   `ReplicasInfoManager`, and committed events are applied blindly on every node. Java re-checks
//!   preconditions when replaying the log.
//! - **Quorum for even-sized clusters is relaxed** to `n/2` (see [`quorum`]) so a 2-node group
//!   keeps committing when one node is down; strict Raft tolerates zero failures at n=2.
//! - Replication is one sequential round per tick (no pipelining, no ReadIndex leases); conflict
//!   resolution backtracks `nextIndex` one entry per round, which is fine for tiny logs.

use std::collections::{BTreeSet, HashMap};
use std::future::Future;
use std::hash::{BuildHasher, Hasher, RandomState};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::controller::request_code;
use crate::controller::response_code;
use crate::controller::{
    AlterSyncStateSetEvent, ApplyBrokerIdEvent, CleanBrokerDataEvent, ElectMasterEvent,
    EventMessage, UpdateBrokerAddressEvent,
};
use crate::remoting::Command;

/// Interval between leader heartbeat/replication rounds.
const HEARTBEAT_INTERVAL: Duration = Duration::from_millis(100);
/// Timeout applied to each outgoing Raft RPC.
const RPC_TIMEOUT: Duration = Duration::from_millis(100);
/// Driver tick granularity.
const TICK: Duration = Duration::from_millis(10);
/// Randomized election timeout lower bound (Java DLedger uses similar ranges).
const ELECTION_TIMEOUT_MIN_MILLIS: u64 = 150;
/// Election timeout span: timeout is uniform in `[150, 300)` ms.
const ELECTION_TIMEOUT_SPAN_MILLIS: u64 = 150;

/// Voting quorum for a cluster of `size` members (self included). Odd sizes are the strict
/// majority `(size+1)/2`. Even sizes are relaxed to `size/2` so a 2-node group keeps committing
/// with one node down (strict Raft tolerates zero failures at n=2 anyway) — a documented
/// availability-over-safety simplification for even sizes.
fn quorum(size: usize) -> u64 {
    (((size + 1) / 2).max(1)) as u64
}

/// `std`-only source of pseudo-randomness (no `rand` dependency): `RandomState` is randomly seeded
/// per instance, mixed with the wall-clock subsecond nanos.
fn random_election_timeout() -> Duration {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.subsec_nanos() as u64)
        .unwrap_or(0);
    let jitter = RandomState::new().build_hasher().finish();
    let millis = ELECTION_TIMEOUT_MIN_MILLIS + (nanos ^ jitter) % ELECTION_TIMEOUT_SPAN_MILLIS;
    Duration::from_millis(millis)
}

// ---------------------------------------------------------------------------
// Log payload: the JSON-serializable form of a controller event
// ---------------------------------------------------------------------------

/// JSON-serializable form of a controller [`EventMessage`]; the `type` tag doubles as the
/// `event_type` field of the log entry.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum SerdeEvent {
    ElectMaster {
        new_master_elected: bool,
        broker_name: String,
        new_master_broker_id: Option<i64>,
    },
    AlterSyncStateSet {
        broker_name: String,
        new_sync_state_set: BTreeSet<i64>,
    },
    ApplyBrokerId {
        cluster_name: String,
        broker_name: String,
        broker_address: String,
        register_check_code: String,
        new_broker_id: i64,
    },
    UpdateBrokerAddress {
        cluster_name: String,
        broker_name: String,
        broker_address: String,
        broker_id: i64,
    },
    CleanBrokerData {
        broker_name: String,
        broker_id_set_to_clean: Option<BTreeSet<i64>>,
    },
}

impl SerdeEvent {
    /// Convert a controller event to its serializable log form. Returns `None` for event types
    /// outside the replicated set (none today).
    pub fn from_event(event: &dyn EventMessage) -> Option<Self> {
        if let Some(e) = event.as_any().downcast_ref::<ElectMasterEvent>() {
            return Some(SerdeEvent::ElectMaster {
                new_master_elected: e.new_master_elected,
                broker_name: e.broker_name.clone(),
                new_master_broker_id: e.new_master_broker_id,
            });
        }
        if let Some(e) = event.as_any().downcast_ref::<AlterSyncStateSetEvent>() {
            return Some(SerdeEvent::AlterSyncStateSet {
                broker_name: e.broker_name.clone(),
                new_sync_state_set: e.new_sync_state_set.clone(),
            });
        }
        if let Some(e) = event.as_any().downcast_ref::<ApplyBrokerIdEvent>() {
            return Some(SerdeEvent::ApplyBrokerId {
                cluster_name: e.cluster_name.clone(),
                broker_name: e.broker_name.clone(),
                broker_address: e.broker_address.clone(),
                register_check_code: e.register_check_code.clone(),
                new_broker_id: e.new_broker_id,
            });
        }
        if let Some(e) = event.as_any().downcast_ref::<UpdateBrokerAddressEvent>() {
            return Some(SerdeEvent::UpdateBrokerAddress {
                cluster_name: e.cluster_name.clone(),
                broker_name: e.broker_name.clone(),
                broker_address: e.broker_address.clone(),
                broker_id: e.broker_id,
            });
        }
        if let Some(e) = event.as_any().downcast_ref::<CleanBrokerDataEvent>() {
            return Some(SerdeEvent::CleanBrokerData {
                broker_name: e.broker_name.clone(),
                broker_id_set_to_clean: e.broker_id_set_to_clean.clone(),
            });
        }
        None
    }

    /// Rebuild the trait-object event for `ReplicasInfoManager::apply_event`.
    pub fn into_event(self) -> Box<dyn EventMessage> {
        match self {
            SerdeEvent::ElectMaster {
                new_master_elected,
                broker_name,
                new_master_broker_id,
            } => Box::new(ElectMasterEvent {
                new_master_elected,
                broker_name,
                new_master_broker_id,
            }),
            SerdeEvent::AlterSyncStateSet {
                broker_name,
                new_sync_state_set,
            } => Box::new(AlterSyncStateSetEvent {
                broker_name,
                new_sync_state_set,
            }),
            SerdeEvent::ApplyBrokerId {
                cluster_name,
                broker_name,
                broker_address,
                register_check_code,
                new_broker_id,
            } => Box::new(ApplyBrokerIdEvent {
                cluster_name,
                broker_name,
                broker_address,
                register_check_code,
                new_broker_id,
            }),
            SerdeEvent::UpdateBrokerAddress {
                cluster_name,
                broker_name,
                broker_address,
                broker_id,
            } => Box::new(UpdateBrokerAddressEvent {
                cluster_name,
                broker_name,
                broker_address,
                broker_id,
            }),
            SerdeEvent::CleanBrokerData {
                broker_name,
                broker_id_set_to_clean,
            } => Box::new(CleanBrokerDataEvent {
                broker_name,
                broker_id_set_to_clean,
            }),
        }
    }
}

/// One replicated log entry (1-based `index`).
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LogEntry {
    pub term: u64,
    pub index: u64,
    /// The replicated event; its `type` tag is the event type.
    pub event: SerdeEvent,
}

// ---------------------------------------------------------------------------
// Raft RPC messages (JSON command bodies)
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VoteRequest {
    pub term: u64,
    pub candidate_id: String,
    pub last_log_index: u64,
    pub last_log_term: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct VoteResponse {
    pub term: u64,
    pub vote_granted: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AppendRequest {
    pub term: u64,
    pub leader_id: String,
    pub prev_log_index: u64,
    pub prev_log_term: u64,
    pub leader_commit: u64,
    #[serde(default)]
    pub entries: Vec<LogEntry>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AppendResponse {
    pub term: u64,
    pub success: bool,
    /// Highest log index known to be replicated on the responder (when `success`).
    pub match_index: u64,
}

// ---------------------------------------------------------------------------
// Transport abstraction
// ---------------------------------------------------------------------------

/// Outgoing Raft RPC abstraction. The production implementation goes through
/// [`crate::remoting::invoke`]; tests wire nodes together with an in-memory transport.
pub trait RaftTransport: Send + Sync + 'static {
    /// Send `request` to `peer`, returning `None` on any transport/timeout error.
    fn send(
        &self,
        peer: &str,
        request: Command,
        timeout: Duration,
    ) -> Pin<Box<dyn Future<Output = Option<Command>> + Send>>;
}

/// Production transport over the standard remoting stack.
pub struct RemotingTransport;

impl RaftTransport for RemotingTransport {
    fn send(
        &self,
        peer: &str,
        request: Command,
        timeout: Duration,
    ) -> Pin<Box<dyn Future<Output = Option<Command>> + Send>> {
        // Own the peer id: the returned future must be 'static.
        let peer = peer.to_owned();
        Box::pin(async move { crate::remoting::invoke(&peer, request, timeout).await.ok() })
    }
}

/// Applies a committed event to the controller state machine (wired to `ReplicasInfoManager`).
pub type Applier = Arc<dyn Fn(SerdeEvent) + Send + Sync>;

// ---------------------------------------------------------------------------
// RaftNode
// ---------------------------------------------------------------------------

/// Node role (exported for [`RaftStatus`]).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RaftRole {
    Follower,
    Candidate,
    Leader,
}

/// Observability snapshot of a node (used by operators and tests).
#[derive(Clone, Debug)]
pub struct RaftStatus {
    pub role: RaftRole,
    pub term: u64,
    pub leader_id: Option<String>,
    pub commit_index: u64,
    pub last_applied: u64,
    pub log_len: u64,
}

struct RaftCore {
    role: RaftRole,
    term: u64,
    voted_for: Option<String>,
    leader_id: Option<String>,
    log: Vec<LogEntry>,
    commit_index: u64,
    last_applied: u64,
    /// Leader-volatile: next index to send per remote peer (1-based).
    next_index: HashMap<String, u64>,
    /// Leader-volatile: highest index known replicated per remote peer.
    match_index: HashMap<String, u64>,
    /// Candidate-volatile: votes granted this term.
    votes: BTreeSet<String>,
    /// Last time this node observed a valid leader (or granted a vote / started an election).
    last_activity: Instant,
    /// Last time this leader sent a replication/heartbeat round.
    last_replication: Instant,
    election_timeout: Duration,
}

impl RaftCore {
    fn last_log_index(&self) -> u64 {
        self.log.len() as u64
    }

    fn term_at(&self, index: u64) -> u64 {
        if index == 0 {
            0
        } else {
            self.log[(index - 1) as usize].term
        }
    }

    fn last_log_term(&self) -> u64 {
        self.term_at(self.last_log_index())
    }

    fn become_follower(&mut self, term: u64) {
        if term > self.term {
            self.term = term;
            self.voted_for = None;
        }
        self.role = RaftRole::Follower;
        self.leader_id = None;
        self.election_timeout = random_election_timeout();
        self.last_activity = Instant::now();
    }
}

enum Action {
    Elect,
    Replicate,
}

/// A simplified Raft node for the controller group. See the module docs for the list of
/// simplifications versus full Raft / DLedger.
pub struct RaftNode {
    id: String,
    /// Cluster member ids/addresses as configured (may or may not include this node's own id).
    peers: Vec<String>,
    /// `peers` minus this node — the RPC targets.
    remotes: Vec<String>,
    /// Voting membership size (handles both "peers includes self" and "peers is others" configs).
    cluster_size: usize,
    transport: Arc<dyn RaftTransport>,
    applier: Applier,
    core: Mutex<RaftCore>,
}

impl RaftNode {
    pub fn new(
        node_id: impl Into<String>,
        peers: Vec<String>,
        transport: Arc<dyn RaftTransport>,
        applier: Applier,
    ) -> Self {
        let id = node_id.into();
        let remotes: Vec<String> = peers.iter().filter(|p| **p != id).cloned().collect();
        let cluster_size = if peers.contains(&id) {
            peers.len()
        } else {
            peers.len() + 1
        };
        if !peers.is_empty() && !peers.contains(&id) {
            warn!(
                node = %id,
                peers = ?peers,
                "raft: node_id not present in peers; treating peers as the OTHER members"
            );
        }
        let core = RaftCore {
            role: RaftRole::Follower,
            term: 0,
            voted_for: None,
            leader_id: None,
            log: Vec::new(),
            commit_index: 0,
            last_applied: 0,
            next_index: HashMap::new(),
            match_index: HashMap::new(),
            votes: BTreeSet::new(),
            last_activity: Instant::now(),
            last_replication: Instant::now(),
            election_timeout: random_election_timeout(),
        };
        Self {
            id,
            peers,
            remotes,
            cluster_size,
            transport,
            applier,
            core: Mutex::new(core),
        }
    }

    /// Inbound Raft RPC entry point (routed from the controller request processor).
    pub fn receive(&self, request: &Command) -> Command {
        match request.code {
            request_code::RAFT_VOTE => self.handle_vote(request),
            request_code::RAFT_APPEND | request_code::RAFT_HEARTBEAT => self.handle_append(request),
            _ => Command::response(request, response_code::REQUEST_CODE_NOT_SUPPORTED),
        }
    }

    /// Replicate `events` through the log; returns `true` once they are committed (and therefore
    /// applied via the applier, both here and on the followers). Fails fast when this node is not
    /// the leader or loses leadership mid-round.
    pub async fn propose(&self, events: Vec<SerdeEvent>) -> bool {
        if events.is_empty() {
            return true;
        }
        let count = events.len() as u64;
        let (term, last_index) = {
            let mut core = self.core.lock().unwrap();
            if core.role != RaftRole::Leader {
                return false;
            }
            let first = core.last_log_index() + 1;
            let current_term = core.term;
            for (i, event) in events.into_iter().enumerate() {
                core.log.push(LogEntry {
                    term: current_term,
                    index: first + i as u64,
                    event,
                });
            }
            (current_term, first + count - 1)
        };
        // A couple of rounds is plenty for the tiny controller logs; the background replication
        // loop keeps converging stragglers for later proposals.
        for _ in 0..3 {
            self.replicate_round().await;
            self.advance_commit();
            // The guard must end before any await: the whole future has to stay `Send`.
            let (committed, still_leader) = {
                let core = self.core.lock().unwrap();
                (
                    core.commit_index >= last_index,
                    core.role == RaftRole::Leader && core.term == term,
                )
            };
            if committed {
                return true;
            }
            if !still_leader {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        false
    }

    /// Driver loop: ticks the timers and runs elections / leader replication. Spawned by
    /// `ControllerManager::run` in multi-node mode.
    pub async fn run(self: Arc<Self>, mut shutdown: tokio::sync::watch::Receiver<bool>) {
        info!(node = %self.id, peers = ?self.peers, "raft node started");
        loop {
            if *shutdown.borrow_and_update() {
                break;
            }
            tokio::select! {
                _ = shutdown.changed() => break,
                _ = tokio::time::sleep(TICK) => {}
            }
            let action = {
                let mut core = self.core.lock().unwrap();
                match core.role {
                    RaftRole::Leader => {
                        if core.last_replication.elapsed() >= HEARTBEAT_INTERVAL {
                            core.last_replication = Instant::now();
                            Some(Action::Replicate)
                        } else {
                            None
                        }
                    }
                    _ if core.last_activity.elapsed() >= core.election_timeout => {
                        Some(Action::Elect)
                    }
                    _ => None,
                }
            };
            match action {
                Some(Action::Elect) => self.hold_election().await,
                Some(Action::Replicate) => self.replicate_round().await,
                None => {}
            }
        }
        info!(node = %self.id, "raft node stopped");
    }

    pub fn status(&self) -> RaftStatus {
        let core = self.core.lock().unwrap();
        RaftStatus {
            role: core.role,
            term: core.term,
            leader_id: core.leader_id.clone(),
            commit_index: core.commit_index,
            last_applied: core.last_applied,
            log_len: core.log.len() as u64,
        }
    }

    pub fn is_leader(&self) -> bool {
        self.core.lock().unwrap().role == RaftRole::Leader
    }

    // -- election -----------------------------------------------------------

    async fn hold_election(&self) {
        let term;
        let request;
        {
            let mut core = self.core.lock().unwrap();
            core.role = RaftRole::Candidate;
            core.term += 1;
            term = core.term;
            core.voted_for = Some(self.id.clone());
            core.votes.clear();
            core.votes.insert(self.id.clone());
            core.leader_id = None;
            core.last_activity = Instant::now();
            core.election_timeout = random_election_timeout();
            let vote_req = VoteRequest {
                term,
                candidate_id: self.id.clone(),
                last_log_index: core.last_log_index(),
                last_log_term: core.last_log_term(),
            };
            request = Command::request(request_code::RAFT_VOTE)
                .with_body(serde_json::to_vec(&vote_req).unwrap_or_default());
        }
        info!(node = %self.id, term, "raft: starting election");
        for peer in &self.remotes {
            let Some(resp) = self
                .transport
                .send(peer, request.clone(), RPC_TIMEOUT)
                .await
            else {
                continue;
            };
            if resp.code != response_code::SUCCESS {
                continue;
            }
            let Ok(vote_resp) = serde_json::from_slice::<VoteResponse>(&resp.body) else {
                continue;
            };
            let mut core = self.core.lock().unwrap();
            if vote_resp.term > core.term {
                core.become_follower(vote_resp.term);
                return;
            }
            if core.role == RaftRole::Candidate && core.term == term && vote_resp.vote_granted {
                core.votes.insert(peer.clone());
            }
        }
        let mut core = self.core.lock().unwrap();
        if core.role == RaftRole::Candidate
            && core.term == term
            && core.votes.len() as u64 >= quorum(self.cluster_size)
        {
            core.role = RaftRole::Leader;
            core.leader_id = Some(self.id.clone());
            let next = core.last_log_index() + 1;
            core.next_index = self.remotes.iter().map(|p| (p.clone(), next)).collect();
            core.match_index = self.remotes.iter().map(|p| (p.clone(), 0)).collect();
            core.last_replication = Instant::now();
            info!(node = %self.id, term, "raft: became leader");
        }
    }

    // -- replication ---------------------------------------------------------

    /// One replication/heartbeat round: sends an append (or an empty heartbeat) to every remote
    /// peer and processes the responses (commit advance, leadership step-down, nextIndex backtrack).
    async fn replicate_round(&self) {
        let batch: Vec<(String, Command)> = {
            let core = self.core.lock().unwrap();
            if core.role != RaftRole::Leader {
                return;
            }
            self.remotes
                .iter()
                .map(|peer| {
                    let next = *core.next_index.get(peer).unwrap_or(&1);
                    let prev = next - 1;
                    let (code, entries) = if next <= core.last_log_index() {
                        (
                            request_code::RAFT_APPEND,
                            core.log[(next - 1) as usize..].to_vec(),
                        )
                    } else {
                        (request_code::RAFT_HEARTBEAT, Vec::new())
                    };
                    let append = AppendRequest {
                        term: core.term,
                        leader_id: self.id.clone(),
                        prev_log_index: prev,
                        prev_log_term: core.term_at(prev),
                        leader_commit: core.commit_index,
                        entries,
                    };
                    let command = Command::request(code)
                        .with_body(serde_json::to_vec(&append).unwrap_or_default());
                    (peer.clone(), command)
                })
                .collect()
        };
        for (peer, request) in batch {
            let Some(resp) = self.transport.send(&peer, request, RPC_TIMEOUT).await else {
                continue;
            };
            if resp.code != response_code::SUCCESS {
                continue;
            }
            let Ok(append_resp) = serde_json::from_slice::<AppendResponse>(&resp.body) else {
                continue;
            };
            self.on_append_response(&peer, append_resp);
        }
    }

    fn on_append_response(&self, peer: &str, resp: AppendResponse) {
        let mut core = self.core.lock().unwrap();
        if resp.term > core.term {
            core.become_follower(resp.term);
            return;
        }
        if core.role != RaftRole::Leader || resp.term != core.term {
            return;
        }
        if resp.success {
            let matched = resp
                .match_index
                .max(*core.match_index.get(peer).unwrap_or(&0));
            core.match_index.insert(peer.to_string(), matched);
            core.next_index.insert(peer.to_string(), matched + 1);
        } else {
            // Log inconsistency: backtrack one entry per round (fine for tiny logs).
            let next = core.next_index.entry(peer.to_string()).or_insert(1);
            *next = (*next - 1).max(1);
        }
        drop(core);
        // Commit advancement is response-independent: a leader whose quorum shrank to itself
        // (2-node group with the peer down) must still commit from its own match index.
        self.advance_commit();
    }

    /// Advance `commit_index` to the highest N of the current term replicated on a quorum, then
    /// apply everything newly committed.
    fn advance_commit(&self) {
        let mut core = self.core.lock().unwrap();
        if core.role != RaftRole::Leader {
            return;
        }
        let mut n = core.last_log_index();
        while n > core.commit_index {
            if core.term_at(n) == core.term
                && self.count_acks(&core, n) >= quorum(self.cluster_size)
            {
                break;
            }
            n -= 1;
        }
        if n > core.commit_index {
            core.commit_index = n;
            Self::apply_committed(&mut core, &self.applier);
        }
    }

    fn count_acks(&self, core: &RaftCore, index: u64) -> u64 {
        // Self always replicates its own log.
        1 + core.match_index.values().filter(|m| **m >= index).count() as u64
    }

    fn apply_committed(core: &mut RaftCore, applier: &Applier) {
        while core.last_applied < core.commit_index {
            let event = core.log[core.last_applied as usize].event.clone();
            (applier)(event);
            core.last_applied += 1;
        }
    }

    // -- inbound RPC handlers -------------------------------------------------

    fn handle_vote(&self, request: &Command) -> Command {
        let Ok(req) = serde_json::from_slice::<VoteRequest>(&request.body) else {
            return Command::response(request, response_code::REQUEST_CODE_NOT_SUPPORTED);
        };
        let mut core = self.core.lock().unwrap();
        if req.term > core.term {
            core.become_follower(req.term);
        }
        let up_to_date = req.last_log_term > core.last_log_term()
            || (req.last_log_term == core.last_log_term()
                && req.last_log_index >= core.last_log_index());
        let grant = req.term == core.term
            && up_to_date
            && core
                .voted_for
                .as_deref()
                .map(|v| v == req.candidate_id.as_str())
                .unwrap_or(true);
        if grant {
            core.voted_for = Some(req.candidate_id.clone());
            core.last_activity = Instant::now();
        }
        let resp = VoteResponse {
            term: core.term,
            vote_granted: grant,
        };
        Command::response(request, response_code::SUCCESS)
            .with_body(serde_json::to_vec(&resp).unwrap_or_default())
    }

    /// Handles both `RAFT_APPEND` and `RAFT_HEARTBEAT` (an append with no entries).
    fn handle_append(&self, request: &Command) -> Command {
        let Ok(req) = serde_json::from_slice::<AppendRequest>(&request.body) else {
            return Command::response(request, response_code::REQUEST_CODE_NOT_SUPPORTED);
        };
        let mut core = self.core.lock().unwrap();
        if req.term < core.term {
            let resp = AppendResponse {
                term: core.term,
                success: false,
                match_index: 0,
            };
            return Command::response(request, response_code::SUCCESS)
                .with_body(serde_json::to_vec(&resp).unwrap_or_default());
        }
        core.last_activity = Instant::now();
        if req.term > core.term {
            core.term = req.term;
            core.voted_for = None;
        }
        core.role = RaftRole::Follower;
        core.leader_id = Some(req.leader_id.clone());
        let consistent = req.prev_log_index == 0
            || (core.log.len() as u64 >= req.prev_log_index
                && core.term_at(req.prev_log_index) == req.prev_log_term);
        if !consistent {
            let resp = AppendResponse {
                term: core.term,
                success: false,
                match_index: 0,
            };
            return Command::response(request, response_code::SUCCESS)
                .with_body(serde_json::to_vec(&resp).unwrap_or_default());
        }
        // Truncate conflicts, then append the new entries (idempotent on re-delivery).
        let mut index = req.prev_log_index;
        for entry in req.entries {
            index += 1;
            let pos = index as usize;
            if core.log.len() >= pos {
                if core.log[pos - 1].term != entry.term {
                    core.log.truncate(pos - 1);
                    core.log.push(entry);
                }
            } else {
                core.log.push(entry);
            }
        }
        if req.leader_commit > core.commit_index {
            core.commit_index = req.leader_commit.min(core.last_log_index());
        }
        Self::apply_committed(&mut core, &self.applier);
        let resp = AppendResponse {
            term: core.term,
            success: true,
            match_index: index,
        };
        Command::response(request, response_code::SUCCESS)
            .with_body(serde_json::to_vec(&resp).unwrap_or_default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// In-memory transport wiring nodes directly together (no sockets).
    struct MemTransport {
        nodes: Mutex<HashMap<String, Arc<RaftNode>>>,
    }

    impl MemTransport {
        fn new() -> Self {
            Self {
                nodes: Mutex::new(HashMap::new()),
            }
        }

        fn register(&self, id: &str, node: Arc<RaftNode>) {
            self.nodes.lock().unwrap().insert(id.to_string(), node);
        }
    }

    impl RaftTransport for MemTransport {
        fn send(
            &self,
            peer: &str,
            request: Command,
            _timeout: Duration,
        ) -> Pin<Box<dyn Future<Output = Option<Command>> + Send>> {
            // Drop the lock before awaiting; receive() runs the peer's state machine inline.
            let node = self.nodes.lock().unwrap().get(peer).cloned();
            Box::pin(async move { node.map(|n| n.receive(&request)) })
        }
    }

    type Recorder = Arc<Mutex<Vec<SerdeEvent>>>;

    fn recorder_applier(recorder: &Recorder) -> Applier {
        let recorder = recorder.clone();
        Arc::new(move |event| recorder.lock().unwrap().push(event))
    }

    fn spawn_node(
        transport: &Arc<MemTransport>,
        id: &str,
        peers: Vec<String>,
        shutdown: &tokio::sync::watch::Receiver<bool>,
    ) -> (Arc<RaftNode>, Recorder) {
        let recorder: Recorder = Arc::default();
        let node = Arc::new(RaftNode::new(
            id,
            peers,
            transport.clone(),
            recorder_applier(&recorder),
        ));
        transport.register(id, node.clone());
        tokio::spawn(node.clone().run(shutdown.clone()));
        (node, recorder)
    }

    async fn wait_for_leader(nodes: &[Arc<RaftNode>], what: &str) -> Arc<RaftNode> {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if let Some(leader) = nodes.iter().find(|n| n.is_leader()) {
                return leader.clone();
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for leader ({what})"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    async fn wait_applied(nodes: &[Arc<RaftNode>], at_least: u64, what: &str) {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if nodes.iter().all(|n| n.status().last_applied >= at_least) {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for apply ({what})"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    fn apply_broker_id_event(broker_id: i64) -> SerdeEvent {
        SerdeEvent::ApplyBrokerId {
            cluster_name: "c".into(),
            broker_name: "b".into(),
            broker_address: format!("127.0.0.1:{broker_id}"),
            register_check_code: "code".into(),
            new_broker_id: broker_id,
        }
    }

    #[tokio::test]
    async fn three_node_cluster_commits_with_majority() {
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let transport = Arc::new(MemTransport::new());
        let peers = vec!["n1".to_string(), "n2".to_string(), "n3".to_string()];
        let mut nodes = Vec::new();
        let mut recorders = Vec::new();
        for id in ["n1", "n2", "n3"] {
            let (node, recorder) = spawn_node(&transport, id, peers.clone(), &shutdown_rx);
            nodes.push(node);
            recorders.push(recorder);
        }
        let leader = wait_for_leader(&nodes, "initial election").await;
        assert!(
            leader.propose(vec![apply_broker_id_event(1)]).await,
            "proposal should commit with a 3-node majority"
        );
        wait_applied(&nodes, 1, "replication to followers").await;
        for recorder in &recorders {
            let applied = recorder.lock().unwrap();
            assert_eq!(applied.len(), 1);
            assert_eq!(applied[0], apply_broker_id_event(1));
        }
        shutdown_tx.send(true).unwrap();
    }

    #[tokio::test]
    async fn two_node_cluster_keeps_committing_when_one_node_is_down() {
        // Quorum for a 2-node group is relaxed to 1 (see `quorum`); n2 never comes up.
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let transport = Arc::new(MemTransport::new());
        let (node, recorder) = spawn_node(
            &transport,
            "n1",
            vec!["n1".to_string(), "n2".to_string()],
            &shutdown_rx,
        );
        let leader = wait_for_leader(std::slice::from_ref(&node), "election with peer down").await;
        assert!(leader.propose(vec![apply_broker_id_event(1)]).await);
        assert_eq!(node.status().last_applied, 1);
        assert_eq!(recorder.lock().unwrap().len(), 1);
        shutdown_tx.send(true).unwrap();
    }

    #[tokio::test]
    async fn follower_appends_entries_and_applies_leader_commit() {
        // Direct RPC check: an append from a higher term is accepted, entries land in the log and
        // the carried leader_commit is applied immediately.
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let transport = Arc::new(MemTransport::new());
        let (node, recorder) = spawn_node(&transport, "n1", vec!["n1".to_string()], &shutdown_rx);
        let request = Command::request(request_code::RAFT_APPEND).with_body(
            serde_json::to_vec(&AppendRequest {
                term: 5,
                leader_id: "n9".into(),
                prev_log_index: 0,
                prev_log_term: 0,
                leader_commit: 1,
                entries: vec![LogEntry {
                    term: 5,
                    index: 1,
                    event: apply_broker_id_event(1),
                }],
            })
            .unwrap(),
        );
        let resp = node.receive(&request);
        let parsed: AppendResponse = serde_json::from_slice(&resp.body).unwrap();
        assert!(parsed.success);
        assert_eq!(parsed.match_index, 1);
        let status = node.status();
        assert_eq!(status.term, 5);
        assert_eq!(status.last_applied, 1);
        assert_eq!(*recorder.lock().unwrap(), vec![apply_broker_id_event(1)]);
        shutdown_tx.send(true).unwrap();
    }
}
