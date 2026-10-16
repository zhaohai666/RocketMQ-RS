//! Rust port of `org.apache.rocketmq.controller`: the HA controller that manages broker replica sets,
//! elects masters, and tracks the in-sync replica set.
//!
//! Java splits this across `DLedgerController`/`JRaftController` (consensus) and
//! `ReplicasInfoManager` (the in-memory state machine). This port keeps the faithful **state machine
//! and election logic**. Single-node mode (`ControllerConfig::peers` empty) applies events directly
//! to memory, exactly like the original port. With a non-empty `peers` list the events returned by
//! the manager are first committed through the simplified Raft group in [`crate::raft`], and only
//! then applied to the state machine on every group member.

use std::any::Any;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::common::Result;
use crate::raft::{RaftNode, RemotingTransport, SerdeEvent};
use crate::remoting::{Command, Handler};

/// Standard RocketMQ controller request codes (mirrors `RequestCode`).
pub mod request_code {
    pub const CONTROLLER_ELECT_MASTER: i32 = 309;
    pub const CONTROLLER_ALTER_SYNC_STATE_SET: i32 = 310;
    pub const CONTROLLER_GET_METADATA_INFO: i32 = 311;
    pub const BROKER_HEARTBEAT: i32 = 312;
    pub const CONTROLLER_GET_REPLICA_INFO: i32 = 313;
    pub const CONTROLLER_GET_SYNC_STATE_DATA: i32 = 314;
    pub const CLEAN_BROKER_DATA: i32 = 315;
    pub const CONTROLLER_GET_NEXT_BROKER_ID: i32 = 316;
    pub const GET_CONTROLLER_CONFIG: i32 = 317;
    pub const UPDATE_CONTROLLER_CONFIG: i32 = 318;
    pub const CONTROLLER_APPLY_BROKER_ID: i32 = 319;
    pub const CONTROLLER_REGISTER_BROKER: i32 = 320;
    // Raft consensus codes for the multi-controller group (`crate::raft`). Not part of the Java
    // protocol; this range was unused across the crate.
    pub const RAFT_VOTE: i32 = 330;
    pub const RAFT_APPEND: i32 = 331;
    pub const RAFT_HEARTBEAT: i32 = 332;
}

/// Controller-specific response codes (mirrors `ResponseCode` controller range).
pub mod response_code {
    pub const SUCCESS: i32 = 0;
    pub const CONTROLLER_BROKER_NEED_TO_BE_REGISTERED: i32 = 20001;
    pub const CONTROLLER_BROKER_METADATA_NOT_EXIST: i32 = 20002;
    pub const CONTROLLER_ALTER_SYNC_STATE_SET_FAILED: i32 = 20003;
    pub const CONTROLLER_INVALID_MASTER: i32 = 20004;
    pub const CONTROLLER_FENCED_MASTER_EPOCH: i32 = 20005;
    pub const CONTROLLER_FENCED_SYNC_STATE_SET_EPOCH: i32 = 20006;
    pub const CONTROLLER_INVALID_REPLICAS: i32 = 20007;
    pub const CONTROLLER_BROKER_NOT_ALIVE: i32 = 20008;
    pub const CONTROLLER_MASTER_STILL_EXIST: i32 = 20009;
    pub const CONTROLLER_MASTER_NOT_AVAILABLE: i32 = 20010;
    pub const CONTROLLER_ELECT_MASTER_FAILED: i32 = 20011;
    pub const CONTROLLER_BROKER_ID_INVALID: i32 = 20012;
    pub const CONTROLLER_INVALID_CLEAN_BROKER_METADATA: i32 = 20013;
    pub const CONTROLLER_INVALID_REQUEST: i32 = 20014;
    pub const REQUEST_CODE_NOT_SUPPORTED: i32 = 101;
}

/// First broker id a controller assigns within a broker set (Java `MixAll.FIRST_BROKER_CONTROLLER_ID`).
pub const FIRST_BROKER_CONTROLLER_ID: i64 = 1;

/// Controller configuration.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ControllerConfig {
    /// Allow electing an out-of-sync replica as master after a full shutdown.
    #[serde(default)]
    pub enable_elect_unclean_master: bool,
    /// Whether to notify brokers when their role changes after an election.
    #[serde(default = "default_true")]
    pub notify_broker_role_changed: bool,
    /// How long a broker is considered alive without a heartbeat.
    #[serde(default = "default_timeout")]
    pub heartbeat_timeout_millis: u64,
    /// Controller identity epoch (used to fence stale controllers).
    #[serde(default)]
    pub controller_id_epoch: i64,
    /// When false (this port), events are applied in-memory without a Raft log.
    #[serde(default)]
    pub enable_raft: bool,
    /// This node's identity within the controller Raft group. In multi-node mode (non-empty
    /// `peers`) it should be the node's own address and appear in `peers`; otherwise `peers` is
    /// treated as the list of *other* members. Ignored in single-node mode.
    #[serde(default = "default_node_id")]
    pub node_id: String,
    /// All members of the controller Raft group. Empty (the default) means single-node mode, where
    /// events are applied directly and [`crate::raft`] is never started.
    #[serde(default)]
    pub peers: Vec<String>,
    /// Comma list of config keys that cannot be updated at runtime.
    #[serde(default)]
    pub config_black_list: String,
}

fn default_true() -> bool {
    true
}
fn default_timeout() -> u64 {
    10_000
}
fn default_node_id() -> String {
    "n1".to_string()
}

impl Default for ControllerConfig {
    fn default() -> Self {
        Self {
            enable_elect_unclean_master: false,
            notify_broker_role_changed: true,
            heartbeat_timeout_millis: 10_000,
            controller_id_epoch: 0,
            enable_raft: false,
            node_id: default_node_id(),
            peers: Vec::new(),
            config_black_list: String::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// Event log
// ---------------------------------------------------------------------------

/// Kinds of controller events (Java `EventType`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EventType {
    AlterSyncStateSet,
    ApplyBrokerId,
    ElectMaster,
    Read,
    CleanBrokerData,
    UpdateBrokerAddress,
}

/// An event applied to the controller state machine.
pub trait EventMessage: Send + Sync + Any {
    fn event_type(&self) -> EventType;
    /// Downcast helper; implemented per concrete type so `&Self` coerces to `&dyn Any`.
    fn as_any(&self) -> &dyn Any;
}

#[derive(Clone, Debug)]
pub struct ElectMasterEvent {
    pub new_master_elected: bool,
    pub broker_name: String,
    pub new_master_broker_id: Option<i64>,
}

impl EventMessage for ElectMasterEvent {
    fn event_type(&self) -> EventType {
        EventType::ElectMaster
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Clone, Debug)]
pub struct AlterSyncStateSetEvent {
    pub broker_name: String,
    pub new_sync_state_set: BTreeSet<i64>,
}

impl EventMessage for AlterSyncStateSetEvent {
    fn event_type(&self) -> EventType {
        EventType::AlterSyncStateSet
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Clone, Debug)]
pub struct ApplyBrokerIdEvent {
    pub cluster_name: String,
    pub broker_name: String,
    pub broker_address: String,
    pub register_check_code: String,
    pub new_broker_id: i64,
}

impl EventMessage for ApplyBrokerIdEvent {
    fn event_type(&self) -> EventType {
        EventType::ApplyBrokerId
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Clone, Debug)]
pub struct UpdateBrokerAddressEvent {
    pub cluster_name: String,
    pub broker_name: String,
    pub broker_address: String,
    pub broker_id: i64,
}

impl EventMessage for UpdateBrokerAddressEvent {
    fn event_type(&self) -> EventType {
        EventType::UpdateBrokerAddress
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

#[derive(Clone, Debug)]
pub struct CleanBrokerDataEvent {
    pub broker_name: String,
    pub broker_id_set_to_clean: Option<BTreeSet<i64>>,
}

impl EventMessage for CleanBrokerDataEvent {
    fn event_type(&self) -> EventType {
        EventType::CleanBrokerData
    }

    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Result of a controller operation: a response, optional body, and the events to apply.
pub struct ControllerResult<T> {
    pub response: T,
    pub body: Option<Vec<u8>>,
    pub code: i32,
    pub remark: Option<String>,
    pub events: Vec<Box<dyn EventMessage>>,
}

impl<T> ControllerResult<T> {
    fn ok(response: T) -> Self {
        Self {
            response,
            body: None,
            code: response_code::SUCCESS,
            remark: None,
            events: Vec::new(),
        }
    }
    fn failed(response: T, code: i32, remark: impl Into<String>) -> Self {
        Self {
            response,
            body: None,
            code,
            remark: Some(remark.into()),
            events: Vec::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// Replica metadata
// ---------------------------------------------------------------------------

/// Per-broker-set replica directory (Java `BrokerReplicaInfo`): maps brokerId -> address + check code.
#[derive(Clone, Debug, Default)]
pub struct BrokerReplicaInfo {
    pub cluster_name: String,
    pub broker_name: String,
    pub next_assign_broker_id: i64,
    pub broker_id_info: BTreeMap<i64, (String, String)>,
}

impl BrokerReplicaInfo {
    pub fn new(cluster_name: impl Into<String>, broker_name: impl Into<String>) -> Self {
        Self {
            cluster_name: cluster_name.into(),
            broker_name: broker_name.into(),
            next_assign_broker_id: FIRST_BROKER_CONTROLLER_ID,
            broker_id_info: BTreeMap::new(),
        }
    }

    pub fn add_broker(&mut self, broker_id: i64, address: impl Into<String>, check_code: impl Into<String>) {
        self.broker_id_info
            .insert(broker_id, (address.into(), check_code.into()));
        if broker_id >= self.next_assign_broker_id {
            self.next_assign_broker_id = broker_id + 1;
        }
    }

    pub fn is_broker_exist(&self, broker_id: i64) -> bool {
        self.broker_id_info.contains_key(&broker_id)
    }

    pub fn all_broker(&self) -> BTreeSet<i64> {
        self.broker_id_info.keys().copied().collect()
    }

    pub fn broker_address(&self, broker_id: i64) -> Option<String> {
        self.broker_id_info.get(&broker_id).map(|(a, _)| a.clone())
    }

    pub fn broker_register_check_code(&self, broker_id: i64) -> Option<String> {
        self.broker_id_info.get(&broker_id).map(|(_, c)| c.clone())
    }

    pub fn update_broker_address(&mut self, broker_id: i64, address: impl Into<String>) {
        if let Some(entry) = self.broker_id_info.get_mut(&broker_id) {
            entry.0 = address.into();
        }
    }

    pub fn remove_broker_id(&mut self, broker_id: i64) {
        self.broker_id_info.remove(&broker_id);
    }

    pub fn is_empty(&self) -> bool {
        self.broker_id_info.is_empty()
    }
}

/// Per-broker-set sync state (Java `SyncStateInfo`): current master + in-sync replica set + epochs.
#[derive(Clone, Debug)]
pub struct SyncStateInfo {
    pub cluster_name: String,
    pub broker_name: String,
    pub master_epoch: i32,
    pub sync_state_set_epoch: i32,
    pub sync_state_set: BTreeSet<i64>,
    pub master_broker_id: Option<i64>,
}

impl SyncStateInfo {
    pub fn new(cluster_name: impl Into<String>, broker_name: impl Into<String>) -> Self {
        Self {
            cluster_name: cluster_name.into(),
            broker_name: broker_name.into(),
            master_epoch: 0,
            sync_state_set_epoch: 0,
            sync_state_set: BTreeSet::new(),
            master_broker_id: None,
        }
    }

    pub fn update_master_info(&mut self, master_broker_id: Option<i64>) {
        self.master_broker_id = master_broker_id;
        self.master_epoch += 1;
    }

    pub fn update_sync_state_set_info(&mut self, new_set: BTreeSet<i64>) {
        self.sync_state_set = new_set;
        self.sync_state_set_epoch += 1;
    }

    pub fn is_first_time_for_elect(&self) -> bool {
        self.master_epoch == 0
    }

    pub fn is_master_exist(&self) -> bool {
        self.master_broker_id.is_some()
    }

    pub fn remove_from_sync_state(&mut self, broker_id: i64) {
        self.sync_state_set.remove(&broker_id);
    }
}

// ---------------------------------------------------------------------------
// Election policy
// ---------------------------------------------------------------------------

/// Decides whether a broker replica is currently alive (Java `BrokerValidPredicate`).
pub trait BrokerAlive: Send + Sync {
    fn check(&self, cluster: &str, broker_name: &str, broker_id: i64) -> bool;
}

/// Provides liveness metadata used to rank election candidates (Java `BrokerLiveInfoGetter`).
pub trait BrokerLiveInfoGetter: Send + Sync {
    fn get(&self, cluster: &str, broker_name: &str, broker_id: i64) -> Option<BrokerLiveInfo>;
}

/// Liveness metadata for ranking election candidates.
#[derive(Clone, Copy, Debug)]
pub struct BrokerLiveInfo {
    pub epoch: i32,
    pub max_offset: i64,
    pub confirm_offset: i64,
    pub election_priority: i32,
    pub last_seen: Instant,
}

impl Default for BrokerLiveInfo {
    fn default() -> Self {
        Self {
            epoch: 0,
            max_offset: 0,
            confirm_offset: 0,
            election_priority: 0,
            last_seen: Instant::now(),
        }
    }
}

/// Java `ElectPolicy`: chooses a new master from candidate replica sets.
pub trait ElectPolicy: Send + Sync {
    fn elect(
        &self,
        cluster: &str,
        broker_name: &str,
        sync_state_brokers: &BTreeSet<i64>,
        all_replica_brokers: Option<&BTreeSet<i64>>,
        old_master: Option<i64>,
        prefer_broker_id: Option<i64>,
    ) -> Option<i64>;
}

/// Java `DefaultElectPolicy`: filters by liveness, keeps a valid old master, honors a preferred id,
/// then ranks candidates by (epoch, maxOffset, electionPriority).
pub struct DefaultElectPolicy {
    valid: Arc<dyn BrokerAlive>,
    getter: Arc<dyn BrokerLiveInfoGetter>,
}

impl DefaultElectPolicy {
    pub fn new(valid: Arc<dyn BrokerAlive>, getter: Arc<dyn BrokerLiveInfoGetter>) -> Self {
        Self { valid, getter }
    }

    fn try_elect(
        &self,
        cluster: &str,
        broker_name: &str,
        brokers: &BTreeSet<i64>,
        old_master: Option<i64>,
        prefer_broker_id: Option<i64>,
    ) -> Option<i64> {
        let alive: BTreeSet<i64> = brokers
            .iter()
            .copied()
            .filter(|id| self.valid.check(cluster, broker_name, *id))
            .collect();
        if alive.is_empty() {
            return None;
        }
        if let Some(old) = old_master {
            if alive.contains(&old) && (prefer_broker_id.is_none() || prefer_broker_id == Some(old)) {
                return Some(old);
            }
        }
        if let Some(prefer) = prefer_broker_id {
            return if alive.contains(&prefer) { Some(prefer) } else { None };
        }
        // Rank by (epoch desc, maxOffset desc, electionPriority asc).
        alive
            .iter()
            .map(|id| {
                let info = self.getter.get(cluster, broker_name, *id).unwrap_or_default();
                (*id, info)
            })
            .max_by(|a, b| {
                a.1.epoch
                    .cmp(&b.1.epoch)
                    .then(a.1.max_offset.cmp(&b.1.max_offset))
                    .then(b.1.election_priority.cmp(&a.1.election_priority))
            })
            .map(|(id, _)| id)
    }
}

impl ElectPolicy for DefaultElectPolicy {
    fn elect(
        &self,
        cluster: &str,
        broker_name: &str,
        sync_state_brokers: &BTreeSet<i64>,
        all_replica_brokers: Option<&BTreeSet<i64>>,
        old_master: Option<i64>,
        prefer_broker_id: Option<i64>,
    ) -> Option<i64> {
        if let Some(new) = self.try_elect(cluster, broker_name, sync_state_brokers, old_master, prefer_broker_id) {
            return Some(new);
        }
        if let Some(all) = all_replica_brokers {
            return self.try_elect(cluster, broker_name, all, old_master, prefer_broker_id);
        }
        None
    }
}

// ---------------------------------------------------------------------------
// State machine
// ---------------------------------------------------------------------------

/// The controller's in-memory state machine (Java `ReplicasInfoManager`). Read-only methods compute
/// a [`ControllerResult`]; the caller applies the returned events via [`ReplicasInfoManager::apply_event`].
pub struct ReplicasInfoManager {
    pub config: ControllerConfig,
    replica_info_table: HashMap<String, BrokerReplicaInfo>,
    sync_state_set_info_table: HashMap<String, SyncStateInfo>,
}

/// Response header for `electMaster`.
#[derive(Clone, Debug, Default)]
pub struct ElectMasterResponseHeader {
    pub master_broker_id: Option<i64>,
    pub master_address: Option<String>,
    pub master_epoch: i32,
    pub sync_state_set_epoch: i32,
    pub sync_state_set: BTreeSet<i64>,
}

/// Response header for `getReplicaInfo`.
#[derive(Clone, Debug, Default)]
pub struct GetReplicaInfoResponseHeader {
    pub master_broker_id: Option<i64>,
    pub master_address: Option<String>,
    pub master_epoch: i32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct SyncStateSetBody {
    #[serde(default)]
    sync_state_set: Vec<i64>,
    #[serde(default)]
    sync_state_set_epoch: i32,
}

impl ReplicasInfoManager {
    pub fn new(config: ControllerConfig) -> Self {
        Self {
            config,
            replica_info_table: HashMap::new(),
            sync_state_set_info_table: HashMap::new(),
        }
    }

    fn contains_broker(&self, broker_name: &str) -> bool {
        self.replica_info_table.contains_key(broker_name)
            && self.sync_state_set_info_table.contains_key(broker_name)
    }

    fn serialize_sync_state(&self, broker_name: &str) -> Option<Vec<u8>> {
        let info = self.sync_state_set_info_table.get(broker_name)?;
        let body = SyncStateSetBody {
            sync_state_set: info.sync_state_set.iter().copied().collect(),
            sync_state_set_epoch: info.sync_state_set_epoch,
        };
        serde_json::to_vec(&body).ok()
    }

    /// Java `applyBrokerId` path: returns an event that, when applied, registers a broker id.
    pub fn apply_broker_id(
        &self,
        cluster_name: &str,
        broker_name: &str,
        broker_address: &str,
        broker_id: i64,
        register_check_code: &str,
    ) -> ControllerResult<()> {
        let result = ControllerResult::ok(());
        let existing = self.replica_info_table.get(broker_name);
        if let Some(info) = existing {
            if !info.is_broker_exist(broker_id)
                || info.broker_register_check_code(broker_id).as_deref() == Some(register_check_code)
            {
                return ControllerResult {
                    events: vec![Box::new(ApplyBrokerIdEvent {
                        cluster_name: cluster_name.to_string(),
                        broker_name: broker_name.to_string(),
                        broker_address: broker_address.to_string(),
                        register_check_code: register_check_code.to_string(),
                        new_broker_id: broker_id,
                    })],
                    ..result
                };
            }
            return ControllerResult::failed(
                (),
                response_code::CONTROLLER_BROKER_ID_INVALID,
                format!("Fail to apply brokerId: {broker_id} in broker-set: {broker_name}"),
            );
        }
        // First broker in this broker set: any id is accepted only if it is the first id.
        if broker_id == FIRST_BROKER_CONTROLLER_ID {
            return ControllerResult {
                events: vec![Box::new(ApplyBrokerIdEvent {
                    cluster_name: cluster_name.to_string(),
                    broker_name: broker_name.to_string(),
                    broker_address: broker_address.to_string(),
                    register_check_code: register_check_code.to_string(),
                    new_broker_id: broker_id,
                })],
                ..result
            };
        }
        ControllerResult::failed(
            (),
            response_code::CONTROLLER_BROKER_ID_INVALID,
            format!("Broker-set: {broker_name} hasn't been registered, but broker tries to apply brokerId: {broker_id}"),
        )
    }

    /// Java `getNextBrokerId`.
    pub fn get_next_broker_id(&self, _cluster_name: &str, broker_name: &str) -> (i64, i32) {
        let next = self
            .replica_info_table
            .get(broker_name)
            .map(|info| info.next_assign_broker_id)
            .unwrap_or(FIRST_BROKER_CONTROLLER_ID);
        (next, response_code::SUCCESS)
    }

    /// Java `registerBroker`: returns the current master (if alive) and the sync state set; emits an
    /// `UpdateBrokerAddress` event when the broker's address changed.
    pub fn register_broker(
        &self,
        cluster_name: &str,
        broker_name: &str,
        broker_address: &str,
        broker_id: i64,
        alive: &dyn BrokerAlive,
    ) -> ControllerResult<()> {
        if !self.contains_broker(broker_name) {
            return ControllerResult::failed(
                (),
                response_code::CONTROLLER_BROKER_NEED_TO_BE_REGISTERED,
                format!("Broker-set: {broker_name} hasn't been registered in controller"),
            );
        }
        let info = &self.replica_info_table[broker_name];
        let sync = &self.sync_state_set_info_table[broker_name];
        if !info.is_broker_exist(broker_id) {
            return ControllerResult::failed(
                (),
                response_code::CONTROLLER_BROKER_NEED_TO_BE_REGISTERED,
                format!("BrokerId: {broker_id} hasn't been registered in broker-set: {broker_name}"),
            );
        }
        let mut result = ControllerResult::ok(());
        result.body = self.serialize_sync_state(broker_name);
        if let Some(master) = sync.master_broker_id {
            if sync.is_master_exist() && alive.check(cluster_name, broker_name, master) {
                result.code = response_code::SUCCESS;
            }
        }
        if info.broker_address(broker_id).as_deref() != Some(broker_address) {
            result.events.push(Box::new(UpdateBrokerAddressEvent {
                cluster_name: cluster_name.to_string(),
                broker_name: broker_name.to_string(),
                broker_address: broker_address.to_string(),
                broker_id,
            }));
        }
        result
    }

    /// Java `electMaster`.
    pub fn elect_master(
        &self,
        broker_name: &str,
        broker_id: Option<i64>,
        designate_elect: bool,
        elect_policy: &dyn ElectPolicy,
    ) -> ControllerResult<ElectMasterResponseHeader> {
        let empty = ElectMasterResponseHeader::default();
        if !self.contains_broker(broker_name) {
            let result = ControllerResult::failed(
                empty,
                response_code::CONTROLLER_BROKER_NEED_TO_BE_REGISTERED,
                "Broker hasn't been registered",
            );
            return result;
        }
        let sync = &self.sync_state_set_info_table[broker_name];
        let info = &self.replica_info_table[broker_name];
        let sync_state_set: BTreeSet<i64> = sync.sync_state_set.clone();
        let old_master = sync.master_broker_id;
        let all_replicas = if self.config.enable_elect_unclean_master {
            Some(info.all_broker())
        } else {
            None
        };

        let mut new_master = None;
        if sync.is_first_time_for_elect() {
            new_master = broker_id;
        }
        if new_master.is_none() || new_master == Some(-1) {
            let assigned = if designate_elect { broker_id } else { None };
            new_master = elect_policy.elect(
                &info.cluster_name,
                broker_name,
                &sync_state_set,
                all_replicas.as_ref(),
                old_master,
                assigned,
            );
        }

        if new_master.is_some() && new_master == old_master {
            let header = ElectMasterResponseHeader {
                master_broker_id: old_master,
                master_address: info.broker_address(old_master.unwrap_or(-1)),
                master_epoch: sync.master_epoch,
                sync_state_set_epoch: sync.sync_state_set_epoch,
                sync_state_set,
            };
            let mut result = ControllerResult::ok(header);
            result.body = self.serialize_sync_state(broker_name);
            result.code = response_code::CONTROLLER_MASTER_STILL_EXIST;
            result.remark = Some(format!(
                "The old master {old_master:?} is still alive, not need to elect new master for broker {broker_name}"
            ));
            return result;
        }

        if let Some(master) = new_master {
            let header = ElectMasterResponseHeader {
                master_broker_id: Some(master),
                master_address: info.broker_address(master),
                master_epoch: sync.master_epoch + 1,
                sync_state_set_epoch: sync.sync_state_set_epoch + 1,
                sync_state_set: BTreeSet::from([master]),
            };
            let mut result = ControllerResult::ok(header);
            result.body = self.serialize_sync_state(broker_name);
            result.events.push(Box::new(ElectMasterEvent {
                new_master_elected: true,
                broker_name: broker_name.to_string(),
                new_master_broker_id: Some(master),
            }));
            info!(broker = broker_name, master, "Elect new master");
            return result;
        }

        // Election failed.
        let mut result = if broker_id.is_none() || broker_id == Some(-1) {
            ControllerResult::failed(
                empty.clone(),
                response_code::CONTROLLER_MASTER_NOT_AVAILABLE,
                "Old master has down and failed to elect a new broker master",
            )
        } else {
            ControllerResult::failed(
                empty,
                response_code::CONTROLLER_ELECT_MASTER_FAILED,
                "Failed to elect a new master",
            )
        };
        if broker_id.is_none() || broker_id == Some(-1) {
            result.events.push(Box::new(ElectMasterEvent {
                new_master_elected: false,
                broker_name: broker_name.to_string(),
                new_master_broker_id: None,
            }));
        }
        warn!(broker = broker_name, "Failed to elect a new master");
        result
    }

    /// Java `alterSyncStateSet`.
    pub fn alter_sync_state_set(
        &self,
        broker_name: &str,
        new_sync_state_set: BTreeSet<i64>,
        master_broker_id: Option<i64>,
        master_epoch: i32,
        sync_state_set_epoch: i32,
        alive: &dyn BrokerAlive,
    ) -> ControllerResult<()> {
        if !self.contains_broker(broker_name) {
            return ControllerResult::failed(
                (),
                response_code::CONTROLLER_ALTER_SYNC_STATE_SET_FAILED,
                "Broker metadata is not existed",
            );
        }
        let info = &self.replica_info_table[broker_name];
        let sync = &self.sync_state_set_info_table[broker_name];
        let old = &sync.sync_state_set;
        if old.len() == new_sync_state_set.len() && old.is_superset(&new_sync_state_set) {
            return ControllerResult::failed(
                (),
                response_code::CONTROLLER_ALTER_SYNC_STATE_SET_FAILED,
                "The newSyncStateSet is equal with oldSyncStateSet, no needed to update syncStateSet",
            );
        }
        if sync.master_broker_id != master_broker_id {
            return ControllerResult::failed(
                (),
                response_code::CONTROLLER_INVALID_MASTER,
                format!("Rejecting alter syncStateSet because the current leader is {sync_master:?}, not {master_broker_id:?}", sync_master = sync.master_broker_id),
            );
        }
        if master_epoch != sync.master_epoch {
            return ControllerResult::failed(
                (),
                response_code::CONTROLLER_FENCED_MASTER_EPOCH,
                format!("Rejecting alter syncStateSet because the current master epoch is {}, not {master_epoch}", sync.master_epoch),
            );
        }
        if sync_state_set_epoch != sync.sync_state_set_epoch {
            return ControllerResult::failed(
                (),
                response_code::CONTROLLER_FENCED_SYNC_STATE_SET_EPOCH,
                format!("Rejecting alter syncStateSet because the current syncStateSet epoch is {}, not {sync_state_set_epoch}", sync.sync_state_set_epoch),
            );
        }
        for replica in &new_sync_state_set {
            if !info.is_broker_exist(*replica) {
                return ControllerResult::failed(
                    (),
                    response_code::CONTROLLER_INVALID_REPLICAS,
                    format!("Rejecting alter syncStateSet because the replicas {replica} don't exist"),
                );
            }
            if !alive.check(&info.cluster_name, broker_name, *replica) {
                return ControllerResult::failed(
                    (),
                    response_code::CONTROLLER_BROKER_NOT_ALIVE,
                    format!("Rejecting alter syncStateSet because the replicas {replica} don't alive"),
                );
            }
        }
        if !new_sync_state_set.contains(&sync.master_broker_id.unwrap_or(-1)) {
            return ControllerResult::failed(
                (),
                response_code::CONTROLLER_ALTER_SYNC_STATE_SET_FAILED,
                format!("Rejecting alter syncStateSet because the newSyncStateSet doesn't contain origin leader {:?}", sync.master_broker_id),
            );
        }
        let mut result = ControllerResult::ok(());
        let epoch = sync.sync_state_set_epoch + 1;
        let body = SyncStateSetBody {
            sync_state_set: new_sync_state_set.iter().copied().collect(),
            sync_state_set_epoch: epoch,
        };
        result.body = serde_json::to_vec(&body).ok();
        result.events.push(Box::new(AlterSyncStateSetEvent {
            broker_name: broker_name.to_string(),
            new_sync_state_set,
        }));
        result
    }

    /// Java `getReplicaInfo`.
    pub fn get_replica_info(&self, broker_name: &str) -> ControllerResult<GetReplicaInfoResponseHeader> {
        if self.contains_broker(broker_name) {
            let sync = &self.sync_state_set_info_table[broker_name];
            let info = &self.replica_info_table[broker_name];
            let master = sync.master_broker_id;
            let header = GetReplicaInfoResponseHeader {
                master_broker_id: master,
                master_address: info.broker_address(master.unwrap_or(-1)),
                master_epoch: sync.master_epoch,
            };
            let mut result = ControllerResult::ok(header);
            result.body = self.serialize_sync_state(broker_name);
            return result;
        }
        ControllerResult::failed(
            GetReplicaInfoResponseHeader::default(),
            response_code::CONTROLLER_BROKER_METADATA_NOT_EXIST,
            "Broker metadata is not existed",
        )
    }

    /// Java `getSyncStateData`: builds a JSON summary of every requested broker set's replicas.
    pub fn get_sync_state_data(
        &self,
        broker_names: &[String],
        alive: &dyn BrokerAlive,
    ) -> ControllerResult<serde_json::Value> {
        let mut replicas_info = serde_json::Map::new();
        for broker_name in broker_names {
            if !self.contains_broker(broker_name) {
                continue;
            }
            let sync = &self.sync_state_set_info_table[broker_name];
            let info = &self.replica_info_table[broker_name];
            let master = sync.master_broker_id;
            let mut in_sync = Vec::new();
            let mut not_in_sync = Vec::new();
            for (id, (address, _)) in &info.broker_id_info {
                let is_alive = alive.check(&info.cluster_name, broker_name, *id);
                let entry = serde_json::json!({
                    "brokerName": broker_name,
                    "brokerId": id,
                    "brokerAddress": address,
                    "alive": is_alive,
                });
                if sync.sync_state_set.contains(id) {
                    in_sync.push(entry);
                } else {
                    not_in_sync.push(entry);
                }
            }
            replicas_info.insert(
                broker_name.clone(),
                serde_json::json!({
                    "masterBrokerId": master,
                    "masterAddress": info.broker_address(master.unwrap_or(-1)),
                    "masterEpoch": sync.master_epoch,
                    "syncStateSetEpoch": sync.sync_state_set_epoch,
                    "inSyncReplicas": in_sync,
                    "notInSyncReplicas": not_in_sync,
                }),
            );
        }
        let value = serde_json::Value::Object(replicas_info);
        let body = serde_json::to_vec(&value).unwrap_or_default();
        let mut result = ControllerResult::ok(value);
        result.body = Some(body);
        result
    }

    /// Java `cleanBrokerData`.
    pub fn clean_broker_data(
        &self,
        cluster_name: &str,
        broker_name: &str,
        clean_living_broker: bool,
        broker_controller_ids_to_clean: Option<BTreeSet<i64>>,
        alive: &dyn BrokerAlive,
    ) -> ControllerResult<()> {
        if !clean_living_broker {
            if let Some(sync) = self.sync_state_set_info_table.get(broker_name) {
                if let Some(set) = &broker_controller_ids_to_clean {
                    if set.is_empty() && sync.master_broker_id.is_some() {
                        return ControllerResult::failed(
                            (),
                            response_code::CONTROLLER_INVALID_CLEAN_BROKER_METADATA,
                            format!("Broker {broker_name} is still alive, clean up failure"),
                        );
                    }
                    for id in set {
                        if alive.check(cluster_name, broker_name, *id) {
                            return ControllerResult::failed(
                                (),
                                response_code::CONTROLLER_INVALID_CLEAN_BROKER_METADATA,
                                format!("Broker [{broker_name}, {id}] is still alive, clean up failure"),
                            );
                        }
                    }
                }
            }
        }
        if !self.contains_broker(broker_name) {
            return ControllerResult::failed(
                (),
                response_code::CONTROLLER_INVALID_CLEAN_BROKER_METADATA,
                format!("Broker {broker_name} is not existed, clean broker data failure."),
            );
        }
        let mut result = ControllerResult::ok(());
        result.events.push(Box::new(CleanBrokerDataEvent {
            broker_name: broker_name.to_string(),
            broker_id_set_to_clean: broker_controller_ids_to_clean,
        }));
        result
    }

    /// Java `scanNeedReelectBrokerSets`: broker sets whose master is dead but have a live replica.
    pub fn scan_need_reelect_broker_sets(&self, alive: &dyn BrokerAlive) -> Vec<String> {
        let mut need = Vec::new();
        for (broker_name, sync) in &self.sync_state_set_info_table {
            if let Some(master) = sync.master_broker_id {
                let cluster = &sync.cluster_name;
                if !alive.check(cluster, broker_name, master) {
                    if let Some(info) = self.replica_info_table.get(broker_name) {
                        if info
                            .all_broker()
                            .iter()
                            .any(|id| alive.check(cluster, broker_name, *id))
                        {
                            need.push(broker_name.clone());
                        }
                    }
                }
            }
        }
        need
    }

    /// Apply a single event to the in-memory state (Java `applyEvent`).
    pub fn apply_event(&mut self, event: Box<dyn EventMessage>) {
        match event.event_type() {
            EventType::AlterSyncStateSet => {
                if let Some(ev) = event.as_any().downcast_ref::<AlterSyncStateSetEvent>() {
                    if let Some(sync) = self.sync_state_set_info_table.get_mut(&ev.broker_name) {
                        sync.update_sync_state_set_info(ev.new_sync_state_set.clone());
                    }
                }
            }
            EventType::ApplyBrokerId => {
                if let Some(ev) = event.as_any().downcast_ref::<ApplyBrokerIdEvent>() {
                    if let Some(info) = self.replica_info_table.get_mut(&ev.broker_name) {
                        info.add_broker(ev.new_broker_id, ev.broker_address.clone(), ev.register_check_code.clone());
                    } else {
                        let mut info = BrokerReplicaInfo::new(&ev.cluster_name, &ev.broker_name);
                        info.add_broker(ev.new_broker_id, ev.broker_address.clone(), ev.register_check_code.clone());
                        self.replica_info_table.insert(ev.broker_name.clone(), info);
                        self.sync_state_set_info_table
                            .insert(ev.broker_name.clone(), SyncStateInfo::new(&ev.cluster_name, &ev.broker_name));
                    }
                }
            }
            EventType::UpdateBrokerAddress => {
                if let Some(ev) = event.as_any().downcast_ref::<UpdateBrokerAddressEvent>() {
                    if let Some(info) = self.replica_info_table.get_mut(&ev.broker_name) {
                        info.update_broker_address(ev.broker_id, ev.broker_address.clone());
                    }
                }
            }
            EventType::ElectMaster => {
                if let Some(ev) = event.as_any().downcast_ref::<ElectMasterEvent>() {
                    if let Some(sync) = self.sync_state_set_info_table.get_mut(&ev.broker_name) {
                        if ev.new_master_elected {
                            sync.update_master_info(ev.new_master_broker_id);
                            if let Some(master) = ev.new_master_broker_id {
                                sync.update_sync_state_set_info(BTreeSet::from([master]));
                            }
                        } else {
                            sync.update_master_info(None);
                        }
                    }
                }
            }
            EventType::CleanBrokerData => {
                if let Some(ev) = event.as_any().downcast_ref::<CleanBrokerDataEvent>() {
                    self.handle_clean_broker_data(&ev.broker_name, &ev.broker_id_set_to_clean);
                }
            }
            EventType::Read => {}
        }
    }

    fn handle_clean_broker_data(&mut self, broker_name: &str, set: &Option<BTreeSet<i64>>) {
        match set {
            Some(set) if !set.is_empty() => {
                if let Some(info) = self.replica_info_table.get_mut(broker_name) {
                    for id in set {
                        info.remove_broker_id(*id);
                    }
                    if info.is_empty() {
                        self.replica_info_table.remove(broker_name);
                    }
                }
                if let Some(sync) = self.sync_state_set_info_table.get_mut(broker_name) {
                    for id in set {
                        sync.remove_from_sync_state(*id);
                    }
                    if sync.sync_state_set.is_empty() {
                        self.sync_state_set_info_table.remove(broker_name);
                    }
                }
            }
            _ => {
                self.replica_info_table.remove(broker_name);
                self.sync_state_set_info_table.remove(broker_name);
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Heartbeat manager
// ---------------------------------------------------------------------------

/// Tracks broker heartbeats and decides liveness (Java `BrokerHeartbeatManager`).
pub trait BrokerHeartbeatManager: BrokerAlive + Send + Sync {
    fn on_broker_heartbeat(
        &self,
        cluster: &str,
        broker_name: &str,
        broker_addr: &str,
        broker_id: i64,
        timeout_millis: u64,
        epoch: i32,
        max_offset: i64,
        confirm_offset: i64,
        election_priority: i32,
    );
}

/// Default in-memory heartbeat manager.
#[derive(Default)]
pub struct DefaultBrokerHeartbeatManager {
    heartbeats: Mutex<HashMap<(String, String, i64), BrokerLiveInfo>>,
    timeout: Duration,
}

impl DefaultBrokerHeartbeatManager {
    pub fn new(timeout: Duration) -> Self {
        Self {
            heartbeats: Mutex::new(HashMap::new()),
            timeout,
        }
    }
}

impl BrokerAlive for DefaultBrokerHeartbeatManager {
    fn check(&self, _cluster: &str, broker_name: &str, broker_id: i64) -> bool {
        let map = self.heartbeats.lock().unwrap();
        map.iter()
            .any(|((_, name, id), info)| {
                name == broker_name
                    && *id == broker_id
                    && info.last_seen.elapsed() < self.timeout
            })
    }
}

impl BrokerLiveInfoGetter for DefaultBrokerHeartbeatManager {
    fn get(&self, _cluster: &str, broker_name: &str, broker_id: i64) -> Option<BrokerLiveInfo> {
        let map = self.heartbeats.lock().unwrap();
        map.iter()
            .find(|((_, name, id), _)| name == broker_name && *id == broker_id)
            .map(|(_, info)| *info)
    }
}

impl BrokerHeartbeatManager for DefaultBrokerHeartbeatManager {
    fn on_broker_heartbeat(
        &self,
        _cluster: &str,
        broker_name: &str,
        _broker_addr: &str,
        broker_id: i64,
        _timeout_millis: u64,
        epoch: i32,
        max_offset: i64,
        confirm_offset: i64,
        election_priority: i32,
    ) {
        let mut map = self.heartbeats.lock().unwrap();
        map.insert(
            (_cluster.to_string(), broker_name.to_string(), broker_id),
            BrokerLiveInfo {
                epoch,
                max_offset,
                confirm_offset,
                election_priority,
                last_seen: Instant::now(),
            },
        );
    }
}

// ---------------------------------------------------------------------------
// Controller (single-node) + manager + server
// ---------------------------------------------------------------------------

/// Java `Controller`: applies the events produced by the manager either directly (single-node) or
/// through the Raft group in [`crate::raft`] (multi-node).
pub struct Controller {
    manager: Arc<Mutex<ReplicasInfoManager>>,
    heartbeat: Arc<DefaultBrokerHeartbeatManager>,
    elect_policy: Arc<dyn ElectPolicy>,
    config: ControllerConfig,
    /// Raft group handle; `None` in single-node mode (`peers` empty).
    raft: Option<Arc<RaftNode>>,
}

impl Controller {
    pub fn new(config: ControllerConfig) -> Arc<Self> {
        let heartbeat = Arc::new(DefaultBrokerHeartbeatManager::new(Duration::from_millis(
            config.heartbeat_timeout_millis,
        )));
        let elect_policy: Arc<dyn ElectPolicy> = Arc::new(DefaultElectPolicy::new(
            heartbeat.clone() as Arc<dyn BrokerAlive>,
            heartbeat.clone() as Arc<dyn BrokerLiveInfoGetter>,
        ));
        let manager = Arc::new(Mutex::new(ReplicasInfoManager::new(config.clone())));
        // Multi-node mode: commit events through the replicated log. The applier runs on every
        // commit (leader and followers) and feeds committed events back into the shared state
        // machine, so the leader never applies an event before it is committed.
        let raft = if config.peers.is_empty() {
            None
        } else {
            let applier_manager = manager.clone();
            let applier: Arc<dyn Fn(SerdeEvent) + Send + Sync> =
                Arc::new(move |event: SerdeEvent| {
                    applier_manager.lock().unwrap().apply_event(event.into_event());
                });
            Some(Arc::new(RaftNode::new(
                config.node_id.clone(),
                config.peers.clone(),
                Arc::new(RemotingTransport),
                applier,
            )))
        };
        Arc::new(Self {
            manager,
            heartbeat,
            elect_policy,
            config,
            raft,
        })
    }

    pub fn heartbeat(&self) -> &Arc<DefaultBrokerHeartbeatManager> {
        &self.heartbeat
    }

    /// Apply the events produced by a manager read. Single-node mode applies them straight into
    /// the state machine; Raft mode commits them through the replicated log first — the applier
    /// callback applies them on commit, so the caller must not apply them again.
    async fn apply<T>(&self, result: ControllerResult<T>) -> (i32, Option<String>, Option<Vec<u8>>) {
        let ControllerResult {
            code,
            remark,
            body,
            events,
            ..
        } = result;
        if let Some(raft) = &self.raft {
            if !events.is_empty() {
                let payload: Vec<SerdeEvent> = events
                    .iter()
                    .filter_map(|event| SerdeEvent::from_event(event.as_ref()))
                    .collect();
                if payload.len() != events.len() {
                    return (
                        response_code::CONTROLLER_INVALID_REQUEST,
                        Some("event type not supported by raft replication".to_string()),
                        None,
                    );
                }
                if !raft.propose(payload).await {
                    return (
                        response_code::CONTROLLER_INVALID_REQUEST,
                        Some("failed to commit events through the raft group".to_string()),
                        None,
                    );
                }
            }
            return (code, remark, body);
        }
        let mut manager = self.manager.lock().unwrap();
        for event in events {
            manager.apply_event(event);
        }
        (code, remark, body)
    }

    pub async fn apply_broker_id(
        &self,
        cluster: &str,
        broker_name: &str,
        broker_address: &str,
        broker_id: i64,
        check_code: &str,
    ) -> (i32, Option<String>) {
        let result = self.manager.lock().unwrap().apply_broker_id(
            cluster, broker_name, broker_address, broker_id, check_code,
        );
        let (code, remark, _) = self.apply(result).await;
        (code, remark)
    }

    pub fn get_next_broker_id(&self, cluster: &str, broker_name: &str) -> (i64, i32) {
        self.manager.lock().unwrap().get_next_broker_id(cluster, broker_name)
    }

    pub async fn register_broker(
        &self,
        cluster: &str,
        broker_name: &str,
        broker_address: &str,
        broker_id: i64,
    ) -> (i32, Option<String>, Option<Vec<u8>>) {
        let result = self.manager.lock().unwrap().register_broker(
            cluster, broker_name, broker_address, broker_id, &*self.heartbeat,
        );
        self.apply(result).await
    }

    pub async fn elect_master(
        &self,
        broker_name: &str,
        broker_id: Option<i64>,
        designate_elect: bool,
    ) -> (i32, Option<String>, Option<Vec<u8>>) {
        let result = self.manager.lock().unwrap().elect_master(
            broker_name,
            broker_id,
            designate_elect,
            &*self.elect_policy,
        );
        self.apply(result).await
    }

    pub async fn alter_sync_state_set(
        &self,
        broker_name: &str,
        new_set: BTreeSet<i64>,
        master_broker_id: Option<i64>,
        master_epoch: i32,
        sync_state_set_epoch: i32,
    ) -> (i32, Option<String>) {
        let result = self.manager.lock().unwrap().alter_sync_state_set(
            broker_name,
            new_set,
            master_broker_id,
            master_epoch,
            sync_state_set_epoch,
            &*self.heartbeat,
        );
        let (code, remark, _) = self.apply(result).await;
        (code, remark)
    }

    pub async fn get_replica_info(&self, broker_name: &str) -> (i32, Option<String>, Option<Vec<u8>>) {
        let result = self.manager.lock().unwrap().get_replica_info(broker_name);
        self.apply(result).await
    }

    pub async fn get_sync_state_data(&self, broker_names: &[String]) -> (i32, Option<String>, Option<Vec<u8>>) {
        let result = self
            .manager
            .lock()
            .unwrap()
            .get_sync_state_data(broker_names, &*self.heartbeat);
        self.apply(result).await
    }

    pub async fn clean_broker_data(
        &self,
        cluster: &str,
        broker_name: &str,
        clean_living_broker: bool,
        ids: Option<BTreeSet<i64>>,
    ) -> (i32, Option<String>) {
        let result = self.manager.lock().unwrap().clean_broker_data(
            cluster,
            broker_name,
            clean_living_broker,
            ids,
            &*self.heartbeat,
        );
        let (code, remark, _) = self.apply(result).await;
        (code, remark)
    }

    /// Entry point for inbound Raft RPCs (vote/append/heartbeat), routed from the request processor.
    pub fn raft_receive(&self, request: &Command) -> Command {
        match &self.raft {
            Some(raft) => raft.receive(request),
            None => Command::response(request, response_code::REQUEST_CODE_NOT_SUPPORTED),
        }
    }

    pub fn scan_need_reelect(&self) -> Vec<String> {
        self.manager
            .lock()
            .unwrap()
            .scan_need_reelect_broker_sets(&*self.heartbeat)
    }

    pub fn metadata(&self) -> serde_json::Value {
        serde_json::json!({
            "enableElectUncleanMaster": self.config.enable_elect_unclean_master,
            "notifyBrokerRoleChanged": self.config.notify_broker_role_changed,
            "enableRaft": self.config.enable_raft,
        })
    }
}

/// Java `ControllerRequestProcessor`: dispatches controller requests to the [`Controller`].
pub struct ControllerRequestProcessor {
    controller: Arc<Controller>,
}

impl ControllerRequestProcessor {
    pub fn new(controller: Arc<Controller>) -> Self {
        Self { controller }
    }
}

impl Handler for ControllerRequestProcessor {
    fn handle(&self, request: Command, _peer: std::net::SocketAddr) -> std::pin::Pin<Box<dyn std::future::Future<Output = Command> + Send + '_>> {
        let controller = self.controller.clone();
        Box::pin(async move { process(controller, request).await })
    }
}

async fn process(controller: Arc<Controller>, request: Command) -> Command {
    let code = request.code;
    let response = match code {
        request_code::BROKER_HEARTBEAT => {
            let broker_id: Option<i64> = request.ext_fields.get("brokerId").and_then(|v| v.parse().ok());
            match broker_id {
                Some(id) => {
                    controller.heartbeat().on_broker_heartbeat(
                        request.ext_fields.get("clusterName").map(String::as_str).unwrap_or(""),
                        request.ext_fields.get("brokerName").map(String::as_str).unwrap_or(""),
                        request.ext_fields.get("brokerAddr").map(String::as_str).unwrap_or(""),
                        id,
                        request
                            .ext_fields
                            .get("heartbeatTimeoutMills")
                            .and_then(|v| v.parse().ok())
                            .unwrap_or(controller.heartbeat().timeout.as_millis() as u64),
                        request.ext_fields.get("epoch").and_then(|v| v.parse().ok()).unwrap_or(0),
                        request.ext_fields.get("maxOffset").and_then(|v| v.parse().ok()).unwrap_or(0),
                        request.ext_fields.get("confirmOffset").and_then(|v| v.parse().ok()).unwrap_or(0),
                        request.ext_fields.get("electionPriority").and_then(|v| v.parse().ok()).unwrap_or(0),
                    );
                    Command::response(&request, response_code::SUCCESS)
                        .with_body(b"Heart beat success".to_vec())
                }
                None => Command::response(&request, response_code::CONTROLLER_INVALID_REQUEST)
                    .with_body(b"Heart beat with empty brokerId".to_vec()),
            }
        }
        request_code::RAFT_VOTE | request_code::RAFT_APPEND | request_code::RAFT_HEARTBEAT => {
            controller.raft_receive(&request)
        }
        request_code::CONTROLLER_GET_METADATA_INFO => {
            let json = serde_json::to_vec(&controller.metadata()).unwrap_or_default();
            Command::response(&request, response_code::SUCCESS).with_body(json)
        }
        request_code::CONTROLLER_GET_REPLICA_INFO => {
            let broker_name = request.ext_fields.get("brokerName").cloned().unwrap_or_default();
            let (code, _, body) = controller.get_replica_info(&broker_name).await;
            Command::response(&request, code).with_body(body.unwrap_or_default())
        }
        request_code::CONTROLLER_GET_SYNC_STATE_DATA => {
            let names: Vec<String> = request
                .body
                .is_empty()
                .then(Vec::new)
                .unwrap_or_else(|| serde_json::from_slice(&request.body).unwrap_or_default());
            let (code, _, body) = controller.get_sync_state_data(&names).await;
            Command::response(&request, code).with_body(body.unwrap_or_default())
        }
        request_code::CONTROLLER_ELECT_MASTER => {
            let broker_name = request.ext_fields.get("brokerName").cloned().unwrap_or_default();
            let broker_id: Option<i64> = request.ext_fields.get("brokerId").and_then(|v| v.parse().ok());
            let designate = request
                .ext_fields
                .get("designateElect")
                .and_then(|v| v.parse::<bool>().ok())
                .unwrap_or(false);
            let (code, _, body) = controller.elect_master(&broker_name, broker_id, designate).await;
            Command::response(&request, code).with_body(body.unwrap_or_default())
        }
        request_code::CONTROLLER_ALTER_SYNC_STATE_SET => {
            let broker_name = request.ext_fields.get("brokerName").cloned().unwrap_or_default();
            let master_broker_id: Option<i64> = request.ext_fields.get("masterBrokerId").and_then(|v| v.parse().ok());
            let master_epoch: i32 = request.ext_fields.get("masterEpoch").and_then(|v| v.parse().ok()).unwrap_or(0);
            let sync_epoch: i32 = request
                .ext_fields
                .get("syncStateSetEpoch")
                .and_then(|v| v.parse().ok())
                .unwrap_or(0);
            let new_set: BTreeSet<i64> = request
                .body
                .is_empty()
                .then(BTreeSet::new)
                .unwrap_or_else(|| {
                    serde_json::from_slice::<SyncStateSetBody>(&request.body)
                        .map(|b| b.sync_state_set.into_iter().collect())
                        .unwrap_or_default()
                });
            let (code, remark) = controller
                .alter_sync_state_set(
                    &broker_name, new_set, master_broker_id, master_epoch, sync_epoch,
                )
                .await;
            let mut resp = Command::response(&request, code);
            if let Some(remark) = remark {
                resp.remark = Some(remark);
            }
            resp
        }
        request_code::CONTROLLER_GET_NEXT_BROKER_ID => {
            let cluster = request.ext_fields.get("clusterName").cloned().unwrap_or_default();
            let broker_name = request.ext_fields.get("brokerName").cloned().unwrap_or_default();
            let (next, code) = controller.get_next_broker_id(&cluster, &broker_name);
            let mut resp = Command::response(&request, code)
                .with_ext("nextBrokerId", next);
            resp.body = serde_json::to_vec(&serde_json::json!({"nextBrokerId": next})).unwrap_or_default();
            resp
        }
        request_code::CONTROLLER_APPLY_BROKER_ID => {
            let cluster = request.ext_fields.get("clusterName").cloned().unwrap_or_default();
            let broker_name = request.ext_fields.get("brokerName").cloned().unwrap_or_default();
            let broker_address = request.ext_fields.get("brokerAddress").cloned().unwrap_or_default();
            let broker_id: i64 = request.ext_fields.get("appliedBrokerId").and_then(|v| v.parse().ok()).unwrap_or(0);
            let check_code = request.ext_fields.get("registerCheckCode").cloned().unwrap_or_default();
            let (code, remark) = controller
                .apply_broker_id(&cluster, &broker_name, &broker_address, broker_id, &check_code)
                .await;
            let mut resp = Command::response(&request, code);
            if let Some(remark) = remark {
                resp.remark = Some(remark);
            }
            resp
        }
        request_code::CONTROLLER_REGISTER_BROKER => {
            let cluster = request.ext_fields.get("clusterName").cloned().unwrap_or_default();
            let broker_name = request.ext_fields.get("brokerName").cloned().unwrap_or_default();
            let broker_address = request.ext_fields.get("brokerAddress").cloned().unwrap_or_default();
            let broker_id: i64 = request.ext_fields.get("brokerId").and_then(|v| v.parse().ok()).unwrap_or(0);
            let (code, remark, body) = controller
                .register_broker(&cluster, &broker_name, &broker_address, broker_id)
                .await;
            let mut resp = Command::response(&request, code).with_body(body.unwrap_or_default());
            if let Some(remark) = remark {
                resp.remark = Some(remark);
            }
            resp
        }
        request_code::CLEAN_BROKER_DATA => {
            let cluster = request.ext_fields.get("clusterName").cloned().unwrap_or_default();
            let broker_name = request.ext_fields.get("brokerName").cloned().unwrap_or_default();
            let clean_living: bool = request
                .ext_fields
                .get("cleanLivingBroker")
                .and_then(|v| v.parse().ok())
                .unwrap_or(false);
            let ids: Option<BTreeSet<i64>> = request
                .ext_fields
                .get("brokerControllerIdsToClean")
                .filter(|s| !s.is_empty())
                .map(|s| s.split(';').filter_map(|p| p.trim().parse().ok()).collect());
            let (code, remark) = controller
                .clean_broker_data(&cluster, &broker_name, clean_living, ids)
                .await;
            let mut resp = Command::response(&request, code);
            if let Some(remark) = remark {
                resp.remark = Some(remark);
            }
            resp
        }
        request_code::GET_CONTROLLER_CONFIG | request_code::UPDATE_CONTROLLER_CONFIG => {
            Command::response(&request, response_code::SUCCESS)
        }
        _ => Command::response(&request, response_code::REQUEST_CODE_NOT_SUPPORTED),
    };
    response
}

/// Java `ControllerManager`: owns the controller, serves it, and periodically re-elects dead masters.
pub struct ControllerManager {
    controller: Arc<Controller>,
    config: ControllerConfig,
}

impl ControllerManager {
    pub fn new(config: ControllerConfig) -> Arc<Self> {
        let controller = Controller::new(config.clone());
        Arc::new(Self { controller, config })
    }

    pub fn controller(&self) -> &Arc<Controller> {
        &self.controller
    }

    pub fn config(&self) -> &ControllerConfig {
        &self.config
    }

    /// Bind the controller listener and serve until `shutdown`.
    pub async fn run(self: Arc<Self>, listener: tokio::net::TcpListener, shutdown: tokio::sync::watch::Receiver<bool>) -> Result<()> {
        // Drive raft elections/heartbeats (multi-node mode only; single-node has no raft node).
        if let Some(raft) = &self.controller.raft {
            tokio::spawn(raft.clone().run(shutdown.clone()));
        }
        let processor = ControllerRequestProcessor::new(self.controller.clone());
        let server = crate::remoting::serve(listener, Arc::new(processor), shutdown.clone());
        let reelection = self.clone().reelection_loop(shutdown.clone());
        tokio::select! {
            result = server => result,
            result = reelection => result,
        }
    }

    async fn reelection_loop(self: Arc<Self>, mut shutdown: tokio::sync::watch::Receiver<bool>) -> Result<()> {
        let mut interval = tokio::time::interval(Duration::from_secs(5));
        loop {
            tokio::select! {
                _ = shutdown.changed() => return Ok(()),
                _ = interval.tick() => {
                    for broker_name in self.controller.scan_need_reelect() {
                        info!(broker = broker_name, "re-electing master for broker set with dead master");
                        self.controller.elect_master(&broker_name, None, false).await;
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manager() -> ReplicasInfoManager {
        ReplicasInfoManager::new(ControllerConfig::default())
    }

    // A heartbeat manager that reports every broker alive.
    struct AllAlive;
    impl BrokerAlive for AllAlive {
        fn check(&self, _: &str, _: &str, _: i64) -> bool {
            true
        }
    }

    #[test]
    fn apply_broker_id_then_register_then_elect() {
        let mgr = manager();
        let mut mgr = mgr;
        let r = mgr.apply_broker_id("c", "b", "127.0.0.1:10911", 1, "code");
        assert_eq!(r.code, response_code::SUCCESS);
        for ev in r.events {
            mgr.apply_event(ev);
        }
        // Second broker.
        let r = mgr.apply_broker_id("c", "b", "127.0.0.1:10912", 2, "code2");
        for ev in r.events {
            mgr.apply_event(ev);
        }
        // First election picks brokerId 1 (the applied id).
        let alive = AllAlive;
        let policy = DefaultElectPolicy::new(Arc::new(AllAlive), Arc::new(NopGetter));
        let r = mgr.elect_master("b", Some(1), false, &policy);
        assert_eq!(r.code, response_code::SUCCESS);
        assert_eq!(r.response.master_broker_id, Some(1));

        // Register broker 1 returns the elected master.
        let r = mgr.register_broker("c", "b", "127.0.0.1:10911", 1, &alive);
        assert_eq!(r.code, response_code::SUCCESS);
    }

    #[test]
    fn elect_keeps_alive_old_master() {
        let mgr = manager();
        let mut mgr = mgr;
        for (id, addr, code) in [(1, "127.0.0.1:1", "a"), (2, "127.0.0.1:2", "b")] {
            let r = mgr.apply_broker_id("c", "b", addr, id, code);
            for ev in r.events {
                mgr.apply_event(ev);
            }
        }
        let policy = DefaultElectPolicy::new(Arc::new(AllAlive), Arc::new(NopGetter));
        let r = mgr.elect_master("b", Some(1), false, &policy);
        assert_eq!(r.response.master_broker_id, Some(1));
        for ev in r.events {
            mgr.apply_event(ev);
        }
        // Re-elect with old master still alive -> unchanged.
        let r = mgr.elect_master("b", None, false, &policy);
        assert_eq!(r.code, response_code::CONTROLLER_MASTER_STILL_EXIST);
        assert_eq!(r.response.master_broker_id, Some(1));
    }

    #[test]
    fn alter_sync_state_set_validates_fencing_and_membership() {
        let mgr = manager();
        let mut mgr = mgr;
        let r = mgr.apply_broker_id("c", "b", "127.0.0.1:1", 1, "a");
        for ev in r.events {
            mgr.apply_event(ev);
        }
        let r = mgr.apply_broker_id("c", "b", "127.0.0.1:2", 2, "b");
        for ev in r.events {
            mgr.apply_event(ev);
        }
        let policy = DefaultElectPolicy::new(Arc::new(AllAlive), Arc::new(NopGetter));
        let r = mgr.elect_master("b", Some(1), false, &policy);
        for ev in r.events {
            mgr.apply_event(ev);
        }
        let alive = AllAlive;
        let sync = &mgr.sync_state_set_info_table["b"];
        // Wrong master epoch is fenced (a changed set passes the no-op check first).
        let r = mgr.alter_sync_state_set(
            "b",
            BTreeSet::from([1, 2]),
            Some(1),
            sync.master_epoch + 1,
            sync.sync_state_set_epoch,
            &alive,
        );
        assert_eq!(r.code, response_code::CONTROLLER_FENCED_MASTER_EPOCH);
        // Correct epoch + members exist and alive -> success.
        let r = mgr.alter_sync_state_set(
            "b",
            BTreeSet::from([1, 2]),
            Some(1),
            sync.master_epoch,
            sync.sync_state_set_epoch,
            &alive,
        );
        assert_eq!(r.code, response_code::SUCCESS);
    }

    #[test]
    fn scan_reelect_flags_dead_master() {
        // A manager whose master is dead but has a live replica should be re-electable.
        let mgr = manager();
        let mut mgr = mgr;
        let r = mgr.apply_broker_id("c", "b", "127.0.0.1:1", 1, "a");
        for ev in r.events {
            mgr.apply_event(ev);
        }
        let r = mgr.apply_broker_id("c", "b", "127.0.0.1:2", 2, "b");
        for ev in r.events {
            mgr.apply_event(ev);
        }
        let policy = DefaultElectPolicy::new(Arc::new(AllAlive), Arc::new(NopGetter));
        let r = mgr.elect_master("b", Some(1), false, &policy);
        for ev in r.events {
            mgr.apply_event(ev);
        }
        // Master (1) is dead; replica (2) is still alive.
        struct OnlyReplicaAlive;
        impl BrokerAlive for OnlyReplicaAlive {
            fn check(&self, _: &str, _: &str, broker_id: i64) -> bool {
                broker_id != 1
            }
        }
        let checker = OnlyReplicaAlive;
        assert!(mgr
            .scan_need_reelect_broker_sets(&checker)
            .contains(&"b".to_string()));
        assert!(!mgr
            .scan_need_reelect_broker_sets(&AllAlive)
            .contains(&"b".to_string()));
    }

    struct NopGetter;
    impl BrokerLiveInfoGetter for NopGetter {
        fn get(&self, _: &str, _: &str, _: i64) -> Option<BrokerLiveInfo> {
            None
        }
    }

    // Regression: with an empty `peers` the controller behaves exactly like the original
    // single-node port — no raft node is created and events apply directly to memory.
    #[tokio::test]
    async fn single_node_controller_bypasses_raft() {
        let controller = Controller::new(ControllerConfig::default());
        assert!(controller.raft.is_none());
        let (code, _) = controller
            .apply_broker_id("c", "b", "127.0.0.1:1", 1, "code")
            .await;
        assert_eq!(code, response_code::SUCCESS);
        let (next, code) = controller.get_next_broker_id("c", "b");
        assert_eq!(next, 2);
        assert_eq!(code, response_code::SUCCESS);
    }
}
