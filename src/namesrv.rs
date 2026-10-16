use crate::common::{Error, Result, TopicConfig};
use crate::remoting::{Command, Handler};
use flate2::{Decompress, FlushDecompress, Status};
use serde::{
    Deserialize, Deserializer, Serialize,
    de::{MapAccess, Visitor},
};
use serde_json::{Value, json};
use std::{
    collections::{BTreeMap, BTreeSet},
    fmt,
    fs::{self, File, OpenOptions},
    future::Future,
    io::{Read, Write},
    marker::PhantomData,
    net::{Ipv6Addr, SocketAddr},
    path::{Path, PathBuf},
    pin::Pin,
    sync::{Mutex, MutexGuard},
    time::{Duration, Instant},
};

const SUCCESS: i32 = 0;
const SYSTEM_ERROR: i32 = 1;
const NOT_SUPPORTED: i32 = 3;
const TOPIC_NOT_EXIST: i32 = 17;
const QUERY_NOT_FOUND: i32 = 22;
const MAX_BODY: usize = 16 * 1024 * 1024;
const MAX_HEADERS: usize = 128;
const MAX_HEADER_BYTES: usize = 128 * 1024;
const MAX_NAME: usize = 255;
const MAX_VALUE: usize = 64 * 1024;
const MAX_TOPICS: usize = 16_384;
const MAX_FILTERS: usize = 1024;
const ORDER_NAMESPACE: &str = "ORDER_TOPIC_CONFIG";
// PermName.PERM_WRITE / PERM_READ (Java common.constant.PermName).
const PERM_WRITE: i32 = 2;
const PERM_READ: i32 = 4;
// TopicValidator.SYNC_BROKER_MEMBER_GROUP_PREFIX: acting-master pickup skips these topics.
const SYNC_BROKER_MEMBER_GROUP_PREFIX: &str = "rmq_sys_SYNC_BROKER_MEMBER_";

type KvTable = BTreeMap<String, BTreeMap<String, String>>;

pub struct Namesrv {
    state: Mutex<State>,
    directory: PathBuf,
    broker_timeout: Duration,
    _directory_lock: File,
}

#[derive(Default)]
struct State {
    brokers: BTreeMap<String, BrokerGroup>,
    kv: KvTable,
    // Static topic mappings: topic -> broker name -> raw TopicQueueMappingInfo JSON
    // (Java RouteInfoManager.topicQueueMappingInfoTable). Accumulates like Java and is
    // never cleaned on unregistration/expiry; memory-only like the broker state above.
    topic_mappings: BTreeMap<String, BTreeMap<String, Value>>,
    // Fence KV until reopen because failed fsync/rename may leave memory and disk divergent.
    kv_failed: bool,
}

struct BrokerGroup {
    cluster: String,
    instances: BTreeMap<i64, BrokerInstance>,
    topics: BTreeMap<String, TopicConfig>,
    enable_acting_master: bool,
}

struct BrokerInstance {
    addr: String,
    ha_addr: String,
    filters: Vec<String>,
    last_seen: Instant,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct KvDisk {
    format_version: u32,
    #[serde(deserialize_with = "unique_namespaces")]
    table: KvTable,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DataVersion {
    timestamp: i64,
    counter: i64,
    #[serde(default)]
    state_version: i64,
}

impl DataVersion {
    fn validate(&self) -> Result<()> {
        if self.timestamp < 0 || self.counter < 0 || self.state_version < 0 {
            return invalid("negative data version");
        }
        Ok(())
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RegisterBody {
    topic_config_serialize_wrapper: TopicWrapper,
    #[serde(default)]
    filter_server_list: Vec<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct TopicWrapper {
    data_version: DataVersion,
    #[serde(deserialize_with = "unique_map")]
    topic_config_table: BTreeMap<String, WireTopic>,
    #[serde(default)]
    topic_queue_mapping_info_map: Option<BTreeMap<String, Value>>,
    /// Wire format field: accepted (and validated by serde) but never read back.
    #[serde(default)]
    #[allow(dead_code)]
    topic_queue_mapping_detail_map: Option<BTreeMap<String, Value>>,
    #[serde(default)]
    mapping_data_version: Option<DataVersion>,
}

// Do not use TopicConfig's permissive defaults for required wire fields.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct WireTopic {
    topic_name: String,
    read_queue_nums: i32,
    write_queue_nums: i32,
    perm: i32,
    #[serde(default)]
    topic_sys_flag: i32,
    #[serde(default)]
    order: bool,
    #[serde(default, deserialize_with = "unique_map")]
    attributes: BTreeMap<String, String>,
    #[serde(default)]
    topic_filter_type: Option<String>,
}

impl WireTopic {
    fn into_config(self) -> Result<TopicConfig> {
        validate_name(&self.topic_name)?;
        if !(0..=65_536).contains(&self.read_queue_nums)
            || !(0..=65_536).contains(&self.write_queue_nums)
            || !(0..=15).contains(&self.perm)
            || self.topic_sys_flag < 0
        {
            return invalid("invalid topic queue count, permission or flag");
        }
        if let Some(filter) = &self.topic_filter_type
            && filter != "SINGLE_TAG"
            && filter != "MULTI_TAG"
        {
            return invalid("invalid topic filter type");
        }
        if self.attributes.len() > 1024 {
            return invalid("too many topic attributes");
        }
        for (key, value) in &self.attributes {
            validate_name(key)?;
            validate_value(value)?;
        }
        Ok(TopicConfig {
            topic_name: self.topic_name,
            read_queue_nums: self.read_queue_nums,
            write_queue_nums: self.write_queue_nums,
            perm: self.perm,
            topic_sys_flag: self.topic_sys_flag,
            order: self.order,
            attributes: self.attributes,
        })
    }
}

struct Registration {
    cluster: String,
    name: String,
    id: i64,
    addr: String,
    ha_addr: String,
    topics: BTreeMap<String, TopicConfig>,
    filters: Vec<String>,
    enable_acting_master: bool,
    // topic -> broker name -> raw TopicQueueMappingInfo as registered on the wire.
    mappings: BTreeMap<String, BTreeMap<String, Value>>,
}

impl Namesrv {
    pub fn open(path: impl AsRef<Path>, broker_timeout: Duration) -> Result<Self> {
        if broker_timeout.is_zero() || broker_timeout.as_millis() > i64::MAX as u128 {
            return invalid("invalid broker timeout");
        }
        fs::create_dir_all(path.as_ref())?;
        let directory = fs::canonicalize(path.as_ref())?;
        let lock_path = directory.join("namesrv.lock");
        regular_or_missing(&lock_path)?;
        let lock = private_options()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(lock_path)?;
        lock.try_lock().map_err(|_| {
            Error::Invalid("NameServer state directory is locked or cannot be locked".into())
        })?;
        let kv_path = directory.join("kv.json");
        regular_or_missing(&kv_path)?;
        let kv = match File::open(&kv_path) {
            Ok(file) => {
                let mut bytes = Vec::new();
                file.take((MAX_BODY + 1) as u64).read_to_end(&mut bytes)?;
                if bytes.len() > MAX_BODY {
                    return invalid("KV state exceeds size limit");
                }
                let disk: KvDisk = serde_json::from_slice(&bytes)
                    .map_err(|_| Error::Invalid("invalid persisted KV data".into()))?;
                if disk.format_version != 1 {
                    return invalid("unsupported KV state version");
                }
                validate_kv(&disk.table)?;
                disk.table
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => BTreeMap::new(),
            Err(error) => return Err(error.into()),
        };
        // A pre-rename crash leaves a temporary file, never a committed value.
        let temp = directory.join("kv.json.tmp");
        if regular_or_missing(&temp)? {
            fs::remove_file(temp)?;
            File::open(&directory)?.sync_all()?;
        }
        Ok(Self {
            state: Mutex::new(State {
                kv,
                ..State::default()
            }),
            directory,
            broker_timeout,
            _directory_lock: lock,
        })
    }

    fn lock(&self) -> Result<MutexGuard<'_, State>> {
        self.state
            .lock()
            .map_err(|_| Error::Invalid("NameServer state lock poisoned".into()))
    }

    pub fn broker_count(&self) -> usize {
        self.state
            .lock()
            .map(|s| s.brokers.values().map(|b| b.instances.len()).sum())
            .unwrap_or(0)
    }

    pub fn expire_brokers(&self) {
        self.expire_at(Instant::now());
    }

    fn expire_at(&self, now: Instant) {
        if let Ok(mut state) = self.state.lock() {
            for group in state.brokers.values_mut() {
                let had_master = group.instances.contains_key(&0);
                group.instances.retain(|_, instance| {
                    now.saturating_duration_since(instance.last_seen) < self.broker_timeout
                });
                // Java routes expiry through the unregister path, so an acting-master
                // group whose master expired also loses its write permission here.
                if had_master
                    && group.enable_acting_master
                    && !group.instances.is_empty()
                    && !group.instances.contains_key(&0)
                {
                    for config in group.topics.values_mut() {
                        config.perm &= !PERM_WRITE;
                    }
                }
            }
            // Retain topics during master outages while a slave still advertises the group.
            state.brokers.retain(|_, group| !group.instances.is_empty());
        }
    }

    fn process(&self, request: &Command) -> Command {
        match self.dispatch(request) {
            Ok(response) => response,
            Err(Error::Unsupported(_)) => {
                failure(request, NOT_SUPPORTED, "request or feature not supported")
            }
            // Never echo malformed input, KV values, paths, or IO/JSON errors.
            Err(_) => failure(
                request,
                SYSTEM_ERROR,
                "invalid request or unavailable NameServer state",
            ),
        }
    }

    fn dispatch(&self, request: &Command) -> Result<Command> {
        validate_envelope(request)?;
        match request.code {
            103 => self.register(request),
            104 => self.unregister(request),
            904 => self.heartbeat(request),
            105 => self.route(request),
            106 => self.cluster_info(request),
            206 | 224 => self.topic_list(request),
            216 => self.delete_topic(request),
            100..=102 | 219 => self.kv_request(request),
            // RequestCode.WIPE_WRITE_PERM_OF_BROKER / ADD_WRITE_PERM_OF_BROKER.
            205 | 327 => self.write_perm(request),
            // RequestCode.GET_BROKER_MEMBER_GROUP.
            901 => self.broker_member_group(request),
            319 => {
                no_body(request)?;
                Ok(Command::response(request, SUCCESS).with_body(format!(
                    "brokerChannelExpiredTime={}\nenableAllTopicList=true\nenableTopicList=true\norderMessageEnable=true\ndeleteTopicWithBrokerRegistration=true\nsupportActingMaster=true\nsupportStaticTopic=true\n",
                    self.broker_timeout.as_millis()
                ).into_bytes()))
            }
            // Includes 318: remote properties must never control disk paths.
            _ => Err(Error::Unsupported("NameServer request code".into())),
        }
    }

    fn register(&self, request: &Command) -> Result<Command> {
        let registration = decode_registration(request)?;
        let mut state = self.lock()?;
        if let Some(group) = state.brokers.get(&registration.name)
            && group.cluster != registration.cluster
        {
            return invalid("broker name already belongs to another cluster");
        }
        if state.brokers.iter().any(|(name, group)| {
            name != &registration.name
                && group.cluster == registration.cluster
                && group
                    .instances
                    .values()
                    .any(|instance| instance.addr == registration.addr)
        }) {
            return invalid("broker address already belongs to another broker");
        }
        // Scoped block so the mutable borrow of the broker group ends before the
        // static-topic mapping table is updated (both live behind one MutexGuard).
        let master = {
            let group = state
                .brokers
                .entry(registration.name)
                .or_insert_with(|| BrokerGroup {
                    cluster: registration.cluster,
                    instances: BTreeMap::new(),
                    topics: BTreeMap::new(),
                    enable_acting_master: false,
                });
            // Java sets enableActingMaster on every registration (isOldVersionBroker -> false).
            group.enable_acting_master = registration.enable_acting_master;
            // Address migration / slave promotion: one address has exactly one ID.
            group
                .instances
                .retain(|id, instance| *id == registration.id || instance.addr != registration.addr);
            group.instances.insert(
                registration.id,
                BrokerInstance {
                    addr: registration.addr,
                    ha_addr: registration.ha_addr,
                    filters: registration.filters,
                    last_seen: Instant::now(),
                },
            );
            if registration.id == 0 {
                group.topics = registration.topics;
            }
            group
                .instances
                .get(&0)
                .map(|master| (master.addr.clone(), master.ha_addr.clone()))
        };
        // Java updates the mapping table on data-version change or first registration;
        // this port tracks no per-broker DataVersion, so it merges on every master
        // registration. Java accumulates per (topic, bname) and never removes entries.
        if registration.id == 0 {
            for (topic, inner) in registration.mappings {
                let slot = state.topic_mappings.entry(topic).or_default();
                for (bname, info) in inner {
                    slot.insert(bname, info);
                }
            }
        }
        let mut response = Command::response(request, SUCCESS);
        if let Some((addr, ha)) = master {
            response = response
                .with_ext("masterAddr", addr)
                .with_ext("haServerAddr", ha);
        }
        if !state.kv_failed
            && let Some(table) = state.kv.get(ORDER_NAMESPACE)
        {
            response.body = serde_json::to_vec(&json!({"table": table}))?;
        }
        Ok(response)
    }

    fn unregister(&self, request: &Command) -> Result<Command> {
        no_body(request)?;
        let cluster = name_field(request, "clusterName")?;
        let name = name_field(request, "brokerName")?;
        let addr = addr_field(request, "brokerAddr")?;
        let id = number_field(request, "brokerId")?;
        let mut state = self.lock()?;
        let Some(group) = state.brokers.get_mut(name) else {
            return Ok(failure(
                request,
                QUERY_NOT_FOUND,
                "broker instance not found",
            ));
        };
        if group.cluster != cluster || group.instances.get(&id).is_none_or(|b| b.addr != addr) {
            return Ok(failure(
                request,
                QUERY_NOT_FOUND,
                "broker instance not found",
            ));
        }
        group.instances.remove(&id);
        // Java cleanTopicByUnRegisterRequests: with acting master enabled, losing the
        // master wipes the write permission of the remaining group until re-registration.
        if group.enable_acting_master
            && !group.instances.is_empty()
            && *group.instances.keys().next().expect("checked non-empty") > 0
        {
            for config in group.topics.values_mut() {
                config.perm &= !PERM_WRITE;
            }
        }
        if group.instances.is_empty() {
            state.brokers.remove(name);
        }
        Ok(Command::response(request, SUCCESS))
    }

    fn heartbeat(&self, request: &Command) -> Result<Command> {
        no_body(request)?;
        let cluster = name_field(request, "clusterName")?;
        let addr = addr_field(request, "brokerAddr")?;
        let name = name_field(request, "brokerName")?;
        let id = optional_number(request, "brokerId")?;
        // Validate controller headers for compatibility without enabling election or remote timeouts.
        for field in [
            "epoch",
            "maxOffset",
            "confirmOffset",
            "heartbeatTimeoutMills",
            "electionPriority",
        ] {
            optional_number(request, field)?;
        }
        let mut state = self.lock()?;
        let found = state
            .brokers
            .get_mut(name)
            .filter(|b| b.cluster == cluster)
            .and_then(|b| {
                b.instances
                    .iter_mut()
                    .find(|(broker_id, instance)| {
                        instance.addr == addr && id.is_none_or(|id| id == **broker_id)
                    })
                    .map(|(_, instance)| instance)
            });
        let Some(instance) = found else {
            return Ok(failure(
                request,
                QUERY_NOT_FOUND,
                "broker instance not found; register first",
            ));
        };
        instance.last_seen = Instant::now();
        Ok(Command::response(request, SUCCESS))
    }

    fn route(&self, request: &Command) -> Result<Command> {
        no_body(request)?;
        let topic = name_field(request, "topic")?;
        optional_bool(request, "acceptStandardJsonOnly")?;
        let state = self.lock()?;
        let mut queues = Vec::new();
        let mut brokers = Vec::new();
        let mut filters = BTreeMap::new();
        for (name, group) in &state.brokers {
            if let Some(config) = group.topics.get(topic) {
                queues.push(json!({
                    "brokerName": name, "readQueueNums": config.read_queue_nums,
                    "writeQueueNums": config.write_queue_nums, "perm": config.perm,
                    "topicSysFlag": config.topic_sys_flag
                }));
                brokers.push(broker_data(name, group));
                for instance in group.instances.values() {
                    if !instance.filters.is_empty() {
                        filters.insert(&instance.addr, &instance.filters);
                    }
                }
            }
        }
        if queues.is_empty() {
            return Ok(failure(request, TOPIC_NOT_EXIST, "topic route not found"));
        }
        if state.kv_failed {
            return invalid("ordered topic KV state unavailable");
        }
        let mut route =
            json!({"queueDatas": queues, "brokerDatas": brokers, "filterServerTable": filters});
        // Java pickupTopicRouteData sets topicQueueMappingByBroker (may be absent) before
        // the acting-master handling; fastjson omits null fields, so only set non-empty.
        if let Some(mapping) = state.topic_mappings.get(topic)
            && !mapping.is_empty()
        {
            route["topicQueueMappingByBroker"] = json!(mapping);
        }
        apply_acting_master(&mut route["brokerDatas"], &state, topic);
        if let Some(value) = state
            .kv
            .get(ORDER_NAMESPACE)
            .and_then(|table| table.get(topic))
        {
            route["orderTopicConf"] = json!(value);
        }
        json_response(request, &route)
    }

    fn cluster_info(&self, request: &Command) -> Result<Command> {
        no_body(request)?;
        let state = self.lock()?;
        let mut brokers = BTreeMap::new();
        let mut clusters: BTreeMap<&str, BTreeSet<&str>> = BTreeMap::new();
        for (name, group) in &state.brokers {
            brokers.insert(name, broker_data(name, group));
            clusters.entry(&group.cluster).or_default().insert(name);
        }
        json_response(
            request,
            &json!({"brokerAddrTable": brokers, "clusterAddrTable": clusters}),
        )
    }

    fn topic_list(&self, request: &Command) -> Result<Command> {
        no_body(request)?;
        let cluster = if request.code == 224 {
            Some(name_field(request, "cluster")?)
        } else {
            None
        };
        let state = self.lock()?;
        let topics: BTreeSet<&str> = state
            .brokers
            .values()
            .filter(|group| cluster.is_none_or(|cluster| group.cluster == cluster))
            .flat_map(|group| group.topics.keys().map(String::as_str))
            .collect();
        json_response(request, &json!({"topicList": topics}))
    }

    fn delete_topic(&self, request: &Command) -> Result<Command> {
        no_body(request)?;
        let topic = name_field(request, "topic")?;
        // Keep clusterName for Java compatibility alongside the multi-cluster selector.
        let single = request.ext_fields.get("clusterName");
        let multiple = request.ext_fields.get("clusterNames");
        if single.is_some() && multiple.is_some() {
            return invalid("ambiguous cluster selector");
        }
        let selector = multiple.or(single).map(String::as_str).unwrap_or("");
        let mut clusters = BTreeSet::new();
        if !selector.is_empty() {
            for cluster in selector.split(';') {
                validate_name(cluster)?;
                clusters.insert(cluster);
            }
        }
        let mut state = self.lock()?;
        for group in state.brokers.values_mut() {
            if clusters.is_empty() || clusters.contains(group.cluster.as_str()) {
                group.topics.remove(topic);
            }
        }
        Ok(Command::response(request, SUCCESS))
    }

    fn kv_request(&self, request: &Command) -> Result<Command> {
        no_body(request)?;
        let namespace = name_field(request, "namespace")?;
        let key = if request.code == 219 {
            None
        } else {
            Some(name_field(request, "key")?)
        };
        let mut state = self.lock()?;
        if state.kv_failed {
            return invalid("KV persistence unavailable; reopen required");
        }
        match request.code {
            101 => match state
                .kv
                .get(namespace)
                .and_then(|table| table.get(key.unwrap()))
            {
                Some(value) => Ok(Command::response(request, SUCCESS).with_ext("value", value)),
                None => Ok(failure(request, QUERY_NOT_FOUND, "KV item not found")),
            },
            219 => match state.kv.get(namespace) {
                Some(table) => json_response(request, &json!({"table": table})),
                None => Ok(failure(request, QUERY_NOT_FOUND, "KV namespace not found")),
            },
            100 | 102 => {
                let mut next = state.kv.clone();
                let key = key.unwrap();
                if request.code == 100 {
                    // Avoid Command::required because an empty KV value is valid.
                    let value = request
                        .ext_fields
                        .get("value")
                        .ok_or_else(|| Error::Invalid("missing KV value".into()))?;
                    validate_value(value)?;
                    next.entry(namespace.to_owned())
                        .or_default()
                        .insert(key.to_owned(), value.to_owned());
                } else if let Some(table) = next.get_mut(namespace) {
                    table.remove(key);
                    if table.is_empty() {
                        next.remove(namespace);
                    }
                }
                if next == state.kv {
                    return Ok(Command::response(request, SUCCESS));
                }
                let bytes = serde_json::to_vec(&KvDisk {
                    format_version: 1,
                    table: next.clone(),
                })?;
                if bytes.len() > MAX_BODY {
                    return invalid("KV state exceeds size limit");
                }
                if let Err(error) = self.persist_kv(&bytes) {
                    state.kv_failed = true;
                    return Err(error);
                }
                state.kv = next;
                Ok(Command::response(request, SUCCESS))
            }
            _ => unreachable!(),
        }
    }

    fn persist_kv(&self, bytes: &[u8]) -> Result<()> {
        let temp = self.directory.join("kv.json.tmp");
        let target = self.directory.join("kv.json");
        regular_or_missing(&target)?;
        let mut file = private_options().write(true).create_new(true).open(&temp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        drop(file);
        fs::rename(&temp, &target)?;
        File::open(&self.directory)?.sync_all()?;
        Ok(())
    }

    // RequestCode 205 WIPE_WRITE_PERM_OF_BROKER / 327 ADD_WRITE_PERM_OF_BROKER
    // (Java RouteInfoManager.operateWritePermOfBroker): wipe clears the write bit of
    // every queue data of the broker, add resets it to READ|WRITE, and the response
    // carries the affected topic count in wipeTopicCount / addTopicCount. Unknown
    // brokers still succeed with a count of zero, as in Java. Unlike Java, this port
    // keeps topic snapshots only for masters, so slaves always report zero.
    fn write_perm(&self, request: &Command) -> Result<Command> {
        no_body(request)?;
        let name = name_field(request, "brokerName")?;
        let mut state = self.lock()?;
        let mut count = 0usize;
        if let Some(group) = state.brokers.get_mut(name) {
            for config in group.topics.values_mut() {
                if request.code == 205 {
                    config.perm &= !PERM_WRITE;
                } else {
                    config.perm = PERM_READ | PERM_WRITE;
                }
                count += 1;
            }
        }
        let mut response = Command::response(request, SUCCESS);
        if request.code == 205 {
            response = response.with_ext("wipeTopicCount", count);
        } else {
            response = response.with_ext("addTopicCount", count);
        }
        Ok(response)
    }

    // RequestCode 901 GET_BROKER_MEMBER_GROUP (Java RouteInfoManager.getBrokerMemberGroup):
    // mirrors the requested cluster/broker names verbatim and returns the group's
    // addresses, succeeding with an empty group when the broker is unknown.
    fn broker_member_group(&self, request: &Command) -> Result<Command> {
        no_body(request)?;
        let cluster = name_field(request, "clusterName")?;
        let name = name_field(request, "brokerName")?;
        let state = self.lock()?;
        let addrs: BTreeMap<String, &str> = state
            .brokers
            .get(name)
            .map(|group| {
                group
                    .instances
                    .iter()
                    .map(|(id, instance)| (id.to_string(), instance.addr.as_str()))
                    .collect()
            })
            .unwrap_or_default();
        json_response(
            request,
            &json!({"brokerMemberGroup": {"cluster": cluster, "brokerName": name, "brokerAddrs": addrs}}),
        )
    }
}

impl Handler for Namesrv {
    fn handle(
        &self,
        request: Command,
        _peer: SocketAddr,
    ) -> Pin<Box<dyn Future<Output = Command> + Send + '_>> {
        Box::pin(async move { self.process(&request) })
    }

    // Intentionally use the default disconnected(): broker RPCs may be short.
}

fn decode_registration(request: &Command) -> Result<Registration> {
    let cluster = name_field(request, "clusterName")?.to_owned();
    let name = name_field(request, "brokerName")?.to_owned();
    let addr = addr_field(request, "brokerAddr")?.to_owned();
    // An explicitly empty HA address means this broker has no HA listener.
    let ha_addr = request
        .ext_fields
        .get("haServerAddr")
        .ok_or_else(|| Error::Invalid("missing HA address header".into()))?
        .clone();
    if !ha_addr.is_empty() {
        validate_addr(&ha_addr)?;
    }
    let id = number_field(request, "brokerId")?;
    let compressed = optional_bool(request, "compressed")?.unwrap_or(false);
    let crc = optional_number(request, "bodyCrc32")?.unwrap_or(0);
    if crc > i32::MAX as i64
        || (crc != 0 && (crc32fast::hash(&request.body) & 0x7fff_ffff) as i64 != crc)
    {
        return invalid("registration CRC mismatch");
    }
    // Java RegisterBrokerRequestHeader.enableActingMaster is nullable: an absent header
    // marks an old-version broker and enables nothing (RouteInfoManager.isOldVersionBroker).
    let enable_acting_master = optional_bool(request, "enableActingMaster")?.unwrap_or(false);
    // Java stores isInBrokerContainer on the broker-side identity only; the NameServer
    // registration path carries no extra container fields, so accept and ignore the flag.
    optional_bool(request, "isInBrokerContainer")?;
    // Keep the operator's timeout authoritative so remote brokers cannot defeat expiry.
    if optional_number(request, "heartbeatTimeoutMillis")? == Some(0) {
        return invalid("invalid heartbeat timeout");
    }
    let (topics, filters, mappings) = if compressed {
        decode_compressed(&request.body)?
    } else {
        let body: RegisterBody = serde_json::from_slice(&request.body)?;
        let wrapper = body.topic_config_serialize_wrapper;
        wrapper.data_version.validate()?;
        if let Some(version) = &wrapper.mapping_data_version {
            version.validate()?;
        }
        // Java RouteInfoManager only consumes topicQueueMappingInfoMap; the detail map is
        // carried on the wire for broker-side consumption and is accepted but ignored here.
        let mappings = decode_mapping_info_map(wrapper.topic_queue_mapping_info_map)?;
        if wrapper.topic_config_table.len() > MAX_TOPICS {
            return invalid("too many topics");
        }
        let mut topics = BTreeMap::new();
        for (key, wire) in wrapper.topic_config_table {
            if key != wire.topic_name {
                return invalid("topic table key mismatch");
            }
            topics.insert(key, wire.into_config()?);
        }
        (topics, body.filter_server_list, mappings)
    };
    if filters.len() > MAX_FILTERS {
        return invalid("too many filter servers");
    }
    for filter in &filters {
        validate_addr(filter)?;
    }
    Ok(Registration {
        cluster,
        name,
        id,
        addr,
        ha_addr,
        topics,
        filters,
        enable_acting_master,
        mappings,
    })
}

// Validates raw TopicQueueMappingInfo JSON from the wire. Java keys the table by the
// map key (topic) and the inner info's bname (RouteInfoManager.registerBroker), so both
// must be present and well-formed; the rest of the info is echoed verbatim.
fn decode_mapping_info_map(
    map: Option<BTreeMap<String, Value>>,
) -> Result<BTreeMap<String, BTreeMap<String, Value>>> {
    let mut mappings = BTreeMap::new();
    let Some(map) = map else {
        return Ok(mappings);
    };
    if map.len() > MAX_TOPICS {
        return invalid("too many static topic mappings");
    }
    for (topic, info) in map {
        validate_name(&topic)?;
        let bname = info
            .get("bname")
            .and_then(Value::as_str)
            .ok_or_else(|| Error::Invalid("mapping info missing broker name".into()))?;
        validate_name(bname)?;
        mappings
            .entry(topic)
            .or_insert_with(BTreeMap::new)
            .insert(bname.to_owned(), info);
    }
    Ok(mappings)
}

fn decode_compressed(
    body: &[u8],
) -> Result<(
    BTreeMap<String, TopicConfig>,
    Vec<String>,
    BTreeMap<String, BTreeMap<String, Value>>,
)> {
    let inflated = inflate_bounded(body)?;
    let mut cursor = BinaryCursor(&inflated);
    let version: DataVersion = serde_json::from_slice(cursor.bytes()?)?;
    version.validate()?;
    let count = cursor.number()?;
    if count > MAX_TOPICS {
        return invalid("too many topics");
    }
    let mut topics = BTreeMap::new();
    for _ in 0..count {
        let text = std::str::from_utf8(cursor.bytes()?)
            .map_err(|_| Error::Invalid("invalid topic encoding".into()))?;
        let mut fields = text.splitn(6, ' ');
        let mut next = || {
            fields
                .next()
                .ok_or_else(|| Error::Invalid("incomplete topic encoding".into()))
        };
        let name = next()?.to_owned();
        let read_queue_nums = parse_i32(next()?)?;
        let write_queue_nums = parse_i32(next()?)?;
        let perm = parse_i32(next()?)?;
        let topic_filter_type = Some(next()?.to_owned());
        let attributes = match fields.next() {
            Some("") | None => BTreeMap::new(),
            Some(json) => {
                #[derive(Deserialize)]
                #[serde(transparent)]
                struct Attributes(
                    #[serde(deserialize_with = "unique_map")] BTreeMap<String, String>,
                );
                serde_json::from_str::<Attributes>(json)?.0
            }
        };
        let config = WireTopic {
            topic_name: name.clone(),
            read_queue_nums,
            write_queue_nums,
            perm,
            topic_sys_flag: 0,
            order: false,
            attributes,
            topic_filter_type,
        }
        .into_config()?;
        if topics.insert(name, config).is_some() {
            return invalid("duplicate topic");
        }
    }
    let filters = serde_json::from_slice(cursor.bytes()?)?;
    // Java RegisterBrokerBody (V5_0_0+): mapping count, then length-prefixed JSON infos
    // keyed by info.getTopic() on decode and by bname in RouteInfoManager.
    let mut mappings = BTreeMap::new();
    if !cursor.0.is_empty() {
        let count = cursor.number()?;
        if count > MAX_TOPICS {
            return invalid("too many static topic mappings");
        }
        for _ in 0..count {
            let text = std::str::from_utf8(cursor.bytes()?)
                .map_err(|_| Error::Invalid("invalid mapping encoding".into()))?;
            let info: Value = serde_json::from_str(text)?;
            let topic = info
                .get("topic")
                .and_then(Value::as_str)
                .ok_or_else(|| Error::Invalid("mapping info missing topic".into()))?;
            let bname = info
                .get("bname")
                .and_then(Value::as_str)
                .ok_or_else(|| Error::Invalid("mapping info missing broker name".into()))?;
            validate_name(topic)?;
            validate_name(bname)?;
            mappings
                .entry(topic.to_owned())
                .or_insert_with(BTreeMap::new)
                .insert(bname.to_owned(), info);
        }
        if !cursor.0.is_empty() {
            return invalid("trailing registration data");
        }
    }
    Ok((topics, filters, mappings))
}

fn inflate_bounded(body: &[u8]) -> Result<Vec<u8>> {
    let mut inflater = Decompress::new(true);
    let mut output = Vec::new();
    let mut buffer = [0u8; 8192];
    loop {
        let before_in = inflater.total_in();
        let before_out = inflater.total_out();
        let status = inflater
            .decompress(
                &body[before_in as usize..],
                &mut buffer,
                // Finish requires all output to fit, so stream bodies larger than this buffer.
                FlushDecompress::None,
            )
            .map_err(|_| Error::Invalid("invalid zlib stream".into()))?;
        let written = (inflater.total_out() - before_out) as usize;
        if output.len() + written > MAX_BODY {
            return invalid("inflated registration exceeds size limit");
        }
        output.extend_from_slice(&buffer[..written]);
        if status == Status::StreamEnd {
            if inflater.total_in() as usize != body.len() {
                return invalid("trailing compressed data");
            }
            return Ok(output);
        }
        if inflater.total_in() == before_in && written == 0 {
            return invalid("truncated zlib stream");
        }
    }
}

struct BinaryCursor<'a>(&'a [u8]);
impl<'a> BinaryCursor<'a> {
    fn number(&mut self) -> Result<usize> {
        let bytes: [u8; 4] = self
            .0
            .get(..4)
            .ok_or_else(|| Error::Invalid("truncated registration".into()))?
            .try_into()
            .unwrap();
        self.0 = &self.0[4..];
        let value = i32::from_be_bytes(bytes);
        if value < 0 {
            return invalid("negative registration length");
        }
        Ok(value as usize)
    }
    fn bytes(&mut self) -> Result<&'a [u8]> {
        let length = self.number()?;
        let bytes = self
            .0
            .get(..length)
            .ok_or_else(|| Error::Invalid("truncated registration".into()))?;
        self.0 = &self.0[length..];
        Ok(bytes)
    }
}

fn broker_data(name: &str, group: &BrokerGroup) -> Value {
    let addresses: BTreeMap<String, &str> = group
        .instances
        .iter()
        .map(|(id, b)| (id.to_string(), b.addr.as_str()))
        .collect();
    json!({"cluster": group.cluster, "brokerName": name, "brokerAddrs": addresses, "enableActingMaster": group.enable_acting_master})
}

// Java RouteInfoManager.pickupTopicRouteData acting-master block (this port always runs
// it and reports supportActingMaster=true in code 319): when a group advertised
// enableActingMaster and its master is gone, the surviving slave with the smallest
// brokerId is re-keyed as id 0 in the emitted route, but only when the group's queue
// data lost its write permission (wiped on master loss). Like Java, this mutates only
// the response clone, never stored state.
fn apply_acting_master(broker_datas: &mut Value, state: &State, topic: &str) {
    if topic.starts_with(SYNC_BROKER_MEMBER_GROUP_PREFIX) {
        return;
    }
    let Some(datas) = broker_datas.as_array_mut() else {
        return;
    };
    let needs_master = datas.iter().any(|data| {
        data["brokerAddrs"]
            .as_object()
            .is_some_and(|addrs| !addrs.is_empty() && !addrs.contains_key("0"))
    });
    if !needs_master {
        return;
    }
    for data in datas {
        let Some(name) = data["brokerName"].as_str() else {
            continue;
        };
        let Some(group) = state.brokers.get(name) else {
            continue;
        };
        if !group.enable_acting_master
            || group.instances.is_empty()
            || group.instances.contains_key(&0)
        {
            continue;
        }
        // Java promotes the acting master only when the group's queue data is not
        // writeable (PermName.isWriteable(queueData.getPerm()) is false).
        if !group
            .topics
            .get(topic)
            .is_some_and(|config| config.perm & PERM_WRITE == 0)
        {
            continue;
        }
        let min_id = match group.instances.keys().next() {
            Some(id) => *id,
            None => continue,
        };
        let addr = match group.instances.get(&min_id) {
            Some(instance) => instance.addr.clone(),
            None => continue,
        };
        if let Some(addrs) = data["brokerAddrs"].as_object_mut() {
            addrs.remove(&min_id.to_string());
            addrs.insert("0".to_owned(), Value::String(addr));
        }
    }
}

fn json_response(request: &Command, value: &impl Serialize) -> Result<Command> {
    Ok(Command::response(request, SUCCESS).with_body(serde_json::to_vec(value)?))
}

fn failure(request: &Command, code: i32, remark: &str) -> Command {
    let mut response = Command::response(request, code);
    response.remark = Some(remark.to_owned());
    response
}

fn invalid<T>(message: &str) -> Result<T> {
    Err(Error::Invalid(message.to_owned()))
}

fn validate_envelope(request: &Command) -> Result<()> {
    if request.body.len() > MAX_BODY || request.ext_fields.len() > MAX_HEADERS {
        return invalid("request exceeds size limit");
    }
    let mut total = 0usize;
    for (key, value) in &request.ext_fields {
        if key.is_empty()
            || key.len() > MAX_NAME
            || key.chars().any(char::is_control)
            || value.len() > MAX_VALUE
            || value.contains('\0')
        {
            return invalid("invalid extension field");
        }
        total = total.saturating_add(key.len()).saturating_add(value.len());
    }
    if total > MAX_HEADER_BYTES {
        return invalid("headers exceed size limit");
    }
    Ok(())
}

fn no_body(request: &Command) -> Result<()> {
    if !request.body.is_empty() {
        return invalid("unexpected request body");
    }
    Ok(())
}

fn validate_name(value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > MAX_NAME
        || value.chars().any(|c| c.is_control() || c.is_whitespace())
    {
        return invalid("invalid name");
    }
    Ok(())
}

fn validate_value(value: &str) -> Result<()> {
    if value.len() > MAX_VALUE || value.contains('\0') {
        return invalid("invalid value");
    }
    Ok(())
}

fn name_field<'a>(request: &'a Command, key: &str) -> Result<&'a str> {
    let value = request.required(key)?;
    validate_name(value)?;
    Ok(value)
}

fn addr_field<'a>(request: &'a Command, key: &str) -> Result<&'a str> {
    let value = request.required(key)?;
    validate_addr(value)?;
    Ok(value)
}

fn validate_addr(value: &str) -> Result<()> {
    if value.len() > 512 {
        return invalid("address too long");
    }
    let Some((host, port)) = value.rsplit_once(':') else {
        return invalid("address must contain a port");
    };
    let port = parse_number(port)?;
    if !(1..=65535).contains(&port) || host.is_empty() {
        return invalid("invalid address");
    }
    if host.starts_with('[') {
        if !host.ends_with(']') || host[1..host.len() - 1].parse::<Ipv6Addr>().is_err() {
            return invalid("invalid IPv6 address");
        }
    } else if host.contains(':') {
        if host.parse::<Ipv6Addr>().is_err() {
            return invalid("invalid IPv6 address");
        }
    } else if !host
        .bytes()
        .all(|c| c.is_ascii_alphanumeric() || matches!(c, b'.' | b'-' | b'_'))
    {
        return invalid("invalid host");
    }
    Ok(())
}

fn parse_number(value: &str) -> Result<i64> {
    if value.is_empty() || value.len() > 19 || !value.bytes().all(|c| c.is_ascii_digit()) {
        return invalid("invalid nonnegative decimal integer");
    }
    value
        .parse()
        .map_err(|_| Error::Invalid("integer overflow".into()))
}

fn parse_i32(value: &str) -> Result<i32> {
    i32::try_from(parse_number(value)?).map_err(|_| Error::Invalid("integer overflow".into()))
}

fn number_field(request: &Command, key: &str) -> Result<i64> {
    parse_number(request.required(key)?)
}

fn optional_number(request: &Command, key: &str) -> Result<Option<i64>> {
    request
        .ext_fields
        .get(key)
        .map(|value| parse_number(value))
        .transpose()
}

fn optional_bool(request: &Command, key: &str) -> Result<Option<bool>> {
    request
        .ext_fields
        .get(key)
        .map(|value| match value.as_str() {
            "true" => Ok(true),
            "false" => Ok(false),
            _ => invalid("invalid boolean"),
        })
        .transpose()
}

fn private_options() -> OpenOptions {
    let mut options = OpenOptions::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    options
}

fn regular_or_missing(path: &Path) -> Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_file() => Ok(true),
        Ok(_) => invalid("state entry is not a regular file"),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error.into()),
    }
}

fn validate_kv(table: &KvTable) -> Result<()> {
    for (namespace, entries) in table {
        validate_name(namespace)?;
        for (key, value) in entries {
            validate_name(key)?;
            validate_value(value)?;
        }
    }
    Ok(())
}

// Reject duplicates because serde's default visitor silently overwrites committed KV entries.
fn unique_map<'de, D, T>(deserializer: D) -> std::result::Result<BTreeMap<String, T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    struct Unique<T>(PhantomData<T>);
    impl<'de, T: Deserialize<'de>> Visitor<'de> for Unique<T> {
        type Value = BTreeMap<String, T>;
        fn expecting(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("an object with unique keys")
        }
        fn visit_map<M: MapAccess<'de>>(
            self,
            mut map: M,
        ) -> std::result::Result<Self::Value, M::Error> {
            let mut result = BTreeMap::new();
            while let Some((key, value)) = map.next_entry::<String, T>()? {
                if result.insert(key, value).is_some() {
                    return Err(serde::de::Error::custom("duplicate object key"));
                }
            }
            Ok(result)
        }
    }
    deserializer.deserialize_map(Unique(PhantomData))
}

fn unique_namespaces<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<KvTable, D::Error> {
    #[derive(Deserialize)]
    #[serde(transparent)]
    struct Namespace(#[serde(deserialize_with = "unique_map")] BTreeMap<String, String>);
    let table: BTreeMap<String, Namespace> = unique_map(deserializer)?;
    Ok(table
        .into_iter()
        .map(|(key, value)| (key, value.0))
        .collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::{Compression, write::ZlibEncoder};
    use tempfile::TempDir;

    fn server() -> (TempDir, Namesrv) {
        let directory = tempfile::tempdir().unwrap();
        let server = Namesrv::open(directory.path(), Duration::from_secs(30)).unwrap();
        (directory, server)
    }

    fn register(cluster: &str, name: &str, id: i64, addr: &str, topics: &[(&str, i32)]) -> Command {
        let table: BTreeMap<_, _> = topics
            .iter()
            .map(|(name, queues)| ((*name).to_owned(), TopicConfig::new(*name, *queues)))
            .collect();
        Command::request(103)
            .with_ext("clusterName", cluster)
            .with_ext("brokerName", name)
            .with_ext("brokerId", id)
            .with_ext("brokerAddr", addr)
            .with_ext("haServerAddr", "ha.example:10912")
            .with_ext("compressed", false)
            .with_ext("bodyCrc32", 0)
            .with_body(
                serde_json::to_vec(&json!({
                    "topicConfigSerializeWrapper": {
                        "dataVersion": {"timestamp": 1, "counter": 1}, "topicConfigTable": table
                    }, "filterServerList": []
                }))
                .unwrap(),
            )
    }

    fn instance_request(code: i32, cluster: &str, name: &str, id: i64, addr: &str) -> Command {
        Command::request(code)
            .with_ext("clusterName", cluster)
            .with_ext("brokerName", name)
            .with_ext("brokerId", id)
            .with_ext("brokerAddr", addr)
    }

    fn request_route(server: &Namesrv, topic: &str) -> Command {
        server.process(&Command::request(105).with_ext("topic", topic))
    }

    fn body(response: Command) -> Value {
        assert_eq!(response.code, SUCCESS, "{:?}", response.remark);
        serde_json::from_slice(&response.body).unwrap()
    }

    fn kv(code: i32, namespace: &str, key: &str) -> Command {
        Command::request(code)
            .with_ext("namespace", namespace)
            .with_ext("key", key)
    }

    #[test]
    fn master_snapshots_preserve_other_brokers_and_slaves_cannot_change_topics() {
        let (_dir, ns) = server();
        assert_eq!(
            ns.process(&register(
                "c",
                "a",
                0,
                "a:10911",
                &[("shared", 4), ("old", 2)]
            ))
            .code,
            0
        );
        assert_eq!(
            ns.process(&register(
                "c",
                "b",
                0,
                "b:10911",
                &[("shared", 8), ("old", 3)]
            ))
            .code,
            0
        );
        let response = ns.process(&register(
            "c",
            "a",
            1,
            "slave:10911",
            &[("shared", 100), ("slave-only", 2)],
        ));
        assert_eq!(response.code, 0);
        assert_eq!(response.ext_fields.get("masterAddr").unwrap(), "a:10911");
        assert_eq!(
            response.ext_fields.get("haServerAddr").unwrap(),
            "ha.example:10912"
        );
        let route = body(request_route(&ns, "shared"));
        assert_eq!(route["queueDatas"][0]["readQueueNums"], 4);
        assert_eq!(route["queueDatas"][1]["writeQueueNums"], 8);
        assert_eq!(route["brokerDatas"][0]["brokerAddrs"]["1"], "slave:10911");
        assert_eq!(route["brokerDatas"][0]["enableActingMaster"], false);
        assert_eq!(route["filterServerTable"], json!({}));
        assert_eq!(request_route(&ns, "slave-only").code, TOPIC_NOT_EXIST);
        assert_eq!(
            ns.process(&register("c", "a", 0, "a:10911", &[("shared", 6)]))
                .code,
            0
        );
        let old = body(request_route(&ns, "old"));
        assert_eq!(old["queueDatas"].as_array().unwrap().len(), 1);
        assert_eq!(old["queueDatas"][0]["brokerName"], "b");
        assert_eq!(
            body(request_route(&ns, "shared"))["queueDatas"][0]["readQueueNums"],
            6
        );
        assert_eq!(ns.broker_count(), 3);
    }

    #[test]
    fn address_replacement_and_promotion_are_exact_and_cluster_conflicts_are_rejected() {
        let (_dir, ns) = server();
        assert_eq!(
            ns.process(&register("c", "a", 0, "old:10911", &[("t", 4)]))
                .code,
            0
        );
        assert_eq!(
            ns.process(&register("c", "a", 0, "new:10911", &[("t", 8)]))
                .code,
            0
        );
        let stale = instance_request(104, "c", "a", 0, "old:10911");
        assert_eq!(ns.process(&stale).code, QUERY_NOT_FOUND);
        assert_eq!(ns.broker_count(), 1);
        assert_eq!(
            ns.process(&instance_request(104, "wrong", "a", 0, "new:10911"))
                .code,
            QUERY_NOT_FOUND
        );
        assert_eq!(
            ns.process(&register("elsewhere", "a", 0, "other:10911", &[("t", 1)]))
                .code,
            SYSTEM_ERROR
        );
        assert_eq!(
            ns.process(&register("c", "other", 0, "new:10911", &[("t", 1)]))
                .code,
            SYSTEM_ERROR
        );
        assert_eq!(
            ns.process(&register("c", "a", 1, "slave:10911", &[])).code,
            0
        );
        assert_eq!(
            ns.process(&register("c", "a", 0, "slave:10911", &[("t", 10)]))
                .code,
            0
        );
        assert_eq!(ns.broker_count(), 1);
        assert_eq!(
            ns.process(&instance_request(104, "c", "a", 1, "slave:10911"))
                .code,
            QUERY_NOT_FOUND
        );
        let route = body(request_route(&ns, "t"));
        assert_eq!(
            route["brokerDatas"][0]["brokerAddrs"],
            json!({"0":"slave:10911"})
        );
        assert_eq!(
            ns.process(&instance_request(104, "c", "a", 0, "slave:10911"))
                .code,
            0
        );
        assert_eq!(request_route(&ns, "t").code, TOPIC_NOT_EXIST);
        assert_eq!(
            body(ns.process(&Command::request(106)))["clusterAddrTable"],
            json!({})
        );
        assert_eq!(
            ns.process(&register("elsewhere", "a", 0, "new:10911", &[("t", 1)]))
                .code,
            0
        );
    }

    #[test]
    fn expiration_is_per_address_and_heartbeat_not_disconnect_controls_lifetime() {
        let (_dir, ns) = server();
        assert_eq!(
            ns.process(&register("c", "a", 0, "master:10911", &[("t", 4)]))
                .code,
            0
        );
        assert_eq!(
            ns.process(&register("c", "a", 1, "slave:10911", &[])).code,
            0
        );
        assert_eq!(
            ns.process(&register("other", "b", 0, "b:10911", &[("other", 4)]))
                .code,
            0
        );
        let past = Instant::now() - Duration::from_secs(31);
        {
            let mut state = ns.lock().unwrap();
            for group in state.brokers.values_mut() {
                for instance in group.instances.values_mut() {
                    instance.last_seen = past;
                }
            }
        }
        assert_eq!(
            ns.process(&instance_request(904, "c", "a", 1, "slave:10911"))
                .code,
            0
        );
        assert_eq!(
            ns.process(&instance_request(904, "c", "a", 0, "slave:10911"))
                .code,
            QUERY_NOT_FOUND
        );
        Handler::disconnected(&ns, "127.0.0.1:12000".parse().unwrap());
        assert_eq!(ns.broker_count(), 3);
        ns.expire_brokers();
        assert_eq!(ns.broker_count(), 1);
        let route = body(request_route(&ns, "t"));
        assert_eq!(
            route["brokerDatas"][0]["brokerAddrs"],
            json!({"1":"slave:10911"})
        );
        assert_eq!(request_route(&ns, "other").code, TOPIC_NOT_EXIST);
        assert_eq!(
            body(ns.process(&Command::request(106)))["clusterAddrTable"],
            json!({"c":["a"]})
        );
        assert_eq!(
            ns.process(&instance_request(904, "c", "a", 0, "master:10911"))
                .code,
            QUERY_NOT_FOUND
        );
        ns.expire_at(Instant::now() + Duration::from_secs(31));
        assert_eq!(ns.broker_count(), 0);
        assert_eq!(request_route(&ns, "t").code, TOPIC_NOT_EXIST);
    }

    #[test]
    fn reregistration_refreshes_lifetime_without_preserving_old_address_deadline() {
        let (_dir, ns) = server();
        ns.process(&register("c", "a", 0, "old:10911", &[("t", 1)]));
        ns.lock()
            .unwrap()
            .brokers
            .get_mut("a")
            .unwrap()
            .instances
            .get_mut(&0)
            .unwrap()
            .last_seen = Instant::now() - Duration::from_secs(31);
        assert_eq!(
            ns.process(&register("c", "a", 0, "new:10911", &[("t", 2)]))
                .code,
            0
        );
        ns.expire_brokers();
        assert_eq!(ns.broker_count(), 1);
        assert_eq!(
            body(request_route(&ns, "t"))["brokerDatas"][0]["brokerAddrs"]["0"],
            "new:10911"
        );
    }

    #[test]
    fn cluster_topic_lists_and_scoped_deletion_match_wire_shapes() {
        let (_dir, ns) = server();
        ns.process(&register(
            "c1",
            "a",
            0,
            "a:10911",
            &[("shared", 4), ("one", 4)],
        ));
        ns.process(&register(
            "c2",
            "b",
            0,
            "b:10911",
            &[("shared", 4), ("two", 4)],
        ));
        ns.process(&register("c3", "c", 0, "c:10911", &[("shared", 4)]));
        let clusters = body(ns.process(&Command::request(106)));
        assert_eq!(
            clusters["clusterAddrTable"],
            json!({"c1":["a"], "c2":["b"], "c3":["c"]})
        );
        assert_eq!(
            clusters["brokerAddrTable"]["a"]["brokerAddrs"]["0"],
            "a:10911"
        );
        assert_eq!(
            body(ns.process(&Command::request(206)))["topicList"],
            json!(["one", "shared", "two"])
        );
        assert_eq!(
            body(ns.process(&Command::request(224).with_ext("cluster", "c1")))["topicList"],
            json!(["one", "shared"])
        );
        assert_eq!(
            body(ns.process(&Command::request(224).with_ext("cluster", "missing")))["topicList"],
            json!([])
        );
        assert_eq!(
            ns.process(
                &Command::request(216)
                    .with_ext("topic", "shared")
                    .with_ext("clusterNames", "c1;c2")
            )
            .code,
            0
        );
        let route = body(request_route(&ns, "shared"));
        assert_eq!(route["brokerDatas"].as_array().unwrap().len(), 1);
        assert_eq!(route["brokerDatas"][0]["cluster"], "c3");
        assert_eq!(
            ns.process(
                &Command::request(216)
                    .with_ext("topic", "shared")
                    .with_ext("clusterName", "c3")
            )
            .code,
            0
        );
        assert_eq!(request_route(&ns, "shared").code, TOPIC_NOT_EXIST);
        assert_eq!(
            ns.process(&Command::request(216).with_ext("topic", "one"))
                .code,
            0
        );
        assert_eq!(request_route(&ns, "one").code, TOPIC_NOT_EXIST);
        assert_eq!(ns.broker_count(), 3);
    }

    #[test]
    fn kv_is_durable_namespace_scoped_and_uses_query_not_found() {
        let (dir, ns) = server();
        assert_eq!(ns.process(&kv(101, "n", "k")).code, QUERY_NOT_FOUND);
        assert_eq!(ns.process(&kv(219, "n", "unused")).code, QUERY_NOT_FOUND);
        assert_eq!(
            ns.process(&kv(100, "n", "k").with_ext("value", "v1")).code,
            0
        );
        assert_eq!(
            ns.process(&kv(100, "other", "k").with_ext("value", "v2"))
                .code,
            0
        );
        assert_eq!(
            body(ns.process(&kv(219, "n", "unused"))),
            json!({"table":{"k":"v1"}})
        );
        assert!(!dir.path().join("kv.json.tmp").exists());
        drop(ns);
        let ns = Namesrv::open(dir.path(), Duration::from_secs(30)).unwrap();
        assert_eq!(
            ns.process(&kv(101, "n", "k"))
                .ext_fields
                .get("value")
                .unwrap(),
            "v1"
        );
        assert_eq!(ns.process(&kv(102, "n", "k")).code, 0);
        assert_eq!(ns.process(&kv(102, "n", "k")).code, 0);
        drop(ns);
        let ns = Namesrv::open(dir.path(), Duration::from_secs(30)).unwrap();
        assert_eq!(ns.process(&kv(101, "n", "k")).code, QUERY_NOT_FOUND);
        assert_eq!(ns.process(&kv(219, "n", "unused")).code, QUERY_NOT_FOUND);
        assert_eq!(
            ns.process(&kv(101, "other", "k"))
                .ext_fields
                .get("value")
                .unwrap(),
            "v2"
        );
    }

    #[test]
    fn ordered_kv_and_filter_servers_are_returned_in_routes() {
        let (_dir, ns) = server();
        assert_eq!(
            ns.process(&kv(100, ORDER_NAMESPACE, "t").with_ext("value", "a:4"))
                .code,
            0
        );
        let mut request = register("c", "a", 0, "a:10911", &[("t", 4)]);
        let mut registration: Value = serde_json::from_slice(&request.body).unwrap();
        registration["filterServerList"] = json!(["filter:12000"]);
        registration["topicConfigSerializeWrapper"]["topicConfigTable"]["t"]["topicSysFlag"] =
            json!(3);
        request.body = serde_json::to_vec(&registration).unwrap();
        assert_eq!(body(ns.process(&request)), json!({"table":{"t":"a:4"}}));
        let route = body(request_route(&ns, "t"));
        assert_eq!(route["orderTopicConf"], "a:4");
        assert_eq!(route["queueDatas"][0]["topicSysFlag"], 3);
        assert_eq!(
            route["filterServerTable"],
            json!({"a:10911":["filter:12000"]})
        );
        assert_eq!(
            ns.process(&register("c", "a", 0, "a:10911", &[("t", 4)]))
                .code,
            0
        );
        assert_eq!(
            body(request_route(&ns, "t"))["filterServerTable"],
            json!({})
        );
    }

    #[test]
    fn empty_ha_address_is_explicitly_supported_without_inventing_replication() {
        let (_dir, ns) = server();
        let request = register("c", "a", 0, "a:10911", &[("t", 1)]).with_ext("haServerAddr", "");
        let response = ns.process(&request);
        assert_eq!(response.code, 0);
        assert_eq!(response.ext_fields.get("haServerAddr").unwrap(), "");
    }

    #[test]
    fn state_directory_lock_survives_idle_and_is_released_on_drop() {
        let (dir, ns) = server();
        assert!(Namesrv::open(dir.path(), Duration::from_secs(1)).is_err());
        drop(ns);
        assert!(Namesrv::open(dir.path(), Duration::from_secs(1)).is_ok());
        assert!(Namesrv::open(dir.path(), Duration::ZERO).is_err());
    }

    #[test]
    fn corrupt_kv_fails_closed_including_duplicate_keys_and_unknown_versions() {
        for bytes in [
            "",
            "{",
            "null",
            "{}",
            r#"{"formatVersion":2,"table":{}}"#,
            r#"{"formatVersion":1,"table":{"n":{"k":"a","k":"b"}}}"#,
            r#"{"formatVersion":1,"table":{"n":{},"n":{}}}"#,
            r#"{"formatVersion":1,"table":{"n":{"k":42}}}"#,
        ] {
            let dir = tempfile::tempdir().unwrap();
            fs::write(dir.path().join("kv.json"), bytes).unwrap();
            assert!(Namesrv::open(dir.path(), Duration::from_secs(30)).is_err());
            assert_eq!(
                fs::read(dir.path().join("kv.json")).unwrap(),
                bytes.as_bytes()
            );
        }
    }

    #[test]
    fn leftover_temporary_state_is_not_committed_and_io_failure_never_acknowledges() {
        let (dir, ns) = server();
        assert_eq!(
            ns.process(&kv(100, "n", "k").with_ext("value", "old")).code,
            0
        );
        fs::write(dir.path().join("kv.json.tmp"), b"uncommitted").unwrap();
        assert_eq!(
            ns.process(&kv(100, "n", "k").with_ext("value", "new")).code,
            SYSTEM_ERROR
        );
        assert_eq!(ns.process(&kv(101, "n", "k")).code, SYSTEM_ERROR);
        drop(ns);
        let ns = Namesrv::open(dir.path(), Duration::from_secs(30)).unwrap();
        assert!(!dir.path().join("kv.json.tmp").exists());
        assert_eq!(
            ns.process(&kv(101, "n", "k"))
                .ext_fields
                .get("value")
                .unwrap(),
            "old"
        );
        assert_eq!(
            ns.process(&kv(100, "n", "k").with_ext("value", "new")).code,
            0
        );
    }

    #[cfg(unix)]
    #[test]
    fn state_files_refuse_symlinks() {
        use std::os::unix::fs::symlink;
        let dir = tempfile::tempdir().unwrap();
        let external = tempfile::NamedTempFile::new().unwrap();
        symlink(external.path(), dir.path().join("kv.json")).unwrap();
        assert!(Namesrv::open(dir.path(), Duration::from_secs(30)).is_err());
        assert_eq!(fs::read(external.path()).unwrap(), b"");
    }

    fn compressed_bytes(data: &[u8]) -> Vec<u8> {
        let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(data).unwrap();
        encoder.finish().unwrap()
    }

    fn framed(output: &mut Vec<u8>, bytes: &[u8]) {
        output.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
        output.extend_from_slice(bytes);
    }

    fn java_registration(with_mapping_count: bool) -> Vec<u8> {
        let mut output = Vec::new();
        framed(
            &mut output,
            br#"{"timestamp":1,"counter":2,"stateVersion":0}"#,
        );
        output.extend_from_slice(&1i32.to_be_bytes());
        framed(
            &mut output,
            br#"compressed-topic 4 8 6 SINGLE_TAG {"message.type":"NORMAL"}"#,
        );
        framed(&mut output, br#"["filter:12000"]"#);
        if with_mapping_count {
            output.extend_from_slice(&0i32.to_be_bytes());
        }
        output
    }

    #[test]
    fn java_zlib_registration_supports_old_and_new_empty_mapping_trailers() {
        for new_format in [false, true] {
            let (_dir, ns) = server();
            let mut request = register("c", "a", 0, "a:10911", &[]).with_ext("compressed", true);
            request.body = compressed_bytes(&java_registration(new_format));
            let crc = crc32fast::hash(&request.body) & 0x7fff_ffff;
            request = request.with_ext("bodyCrc32", crc);
            assert_eq!(ns.process(&request).code, 0);
            let route = body(request_route(&ns, "compressed-topic"));
            assert_eq!(route["queueDatas"][0]["readQueueNums"], 4);
            assert_eq!(route["queueDatas"][0]["writeQueueNums"], 8);
            assert_eq!(
                route["filterServerTable"],
                json!({"a:10911":["filter:12000"]})
            );
        }
    }

    #[test]
    fn compression_rejects_truncation_trailing_bytes_json_and_expansion_bombs() {
        let (_dir, ns) = server();
        let valid = compressed_bytes(&java_registration(true));
        for length in 0..valid.len() {
            let request = register("c", "a", 0, "a:10911", &[])
                .with_ext("compressed", true)
                .with_body(valid[..length].to_vec());
            assert_eq!(ns.process(&request).code, SYSTEM_ERROR, "length {length}");
        }
        let mut trailing = valid;
        trailing.push(0);
        let request = register("c", "a", 0, "a:10911", &[])
            .with_ext("compressed", true)
            .with_body(trailing);
        assert_eq!(ns.process(&request).code, SYSTEM_ERROR);
        let request = register("c", "a", 0, "a:10911", &[]).with_ext("compressed", true);
        assert_eq!(ns.process(&request).code, SYSTEM_ERROR);
        assert!(inflate_bounded(&compressed_bytes(&vec![0; MAX_BODY + 1])).is_err());
        assert_eq!(
            inflate_bounded(&compressed_bytes(&vec![7; 32_000])).unwrap(),
            vec![7; 32_000]
        );
        assert_eq!(ns.broker_count(), 0);
    }

    #[test]
    fn compressed_lengths_are_rejected_and_static_mappings_do_not_mutate_routing() {
        let (_dir, ns) = server();
        for plain in [
            vec![],
            (-1i32).to_be_bytes().to_vec(),
            i32::MAX.to_be_bytes().to_vec(),
        ] {
            let request = register("c", "a", 0, "a:10911", &[])
                .with_ext("compressed", true)
                .with_body(compressed_bytes(&plain));
            assert_eq!(ns.process(&request).code, SYSTEM_ERROR);
        }
        // A valid compressed mapping trailer registers the broker and its mapping.
        let mut plain = java_registration(false);
        plain.extend_from_slice(&1i32.to_be_bytes());
        framed(&mut plain, br#"{"topic":"t","bname":"a","totalQueues":8,"epoch":1}"#);
        let request = register("c", "a", 0, "a:10911", &[])
            .with_ext("compressed", true)
            .with_body(compressed_bytes(&plain));
        assert_eq!(ns.process(&request).code, 0);
        assert_eq!(ns.broker_count(), 1);
        // Malformed mapping infos never register anything.
        for info in [br#"{"topic":"t"}"#.as_slice(), br#"{"bname":"a"}"#.as_slice()] {
            let mut plain = java_registration(false);
            plain.extend_from_slice(&1i32.to_be_bytes());
            framed(&mut plain, info);
            let request = register("c", "a", 0, "a:10911", &[])
                .with_ext("compressed", true)
                .with_body(compressed_bytes(&plain));
            assert_eq!(ns.process(&request).code, SYSTEM_ERROR);
        }
        assert_eq!(ns.broker_count(), 1);
    }

    #[test]
    fn crc32_uses_java_positive_mask_and_rejects_mismatch_before_updates() {
        let (_dir, ns) = server();
        let mut request = register("c", "a", 0, "a:10911", &[("t", 1)]);
        // Whitespace is valid JSON; find a fixture with CRC's sign bit set.
        while crc32fast::hash(&request.body) & 0x8000_0000 == 0 {
            request.body.push(b' ');
        }
        let crc = crc32fast::hash(&request.body) & 0x7fff_ffff;
        request = request.with_ext("bodyCrc32", crc);
        assert_eq!(ns.process(&request).code, 0);
        let request =
            register("c", "a", 0, "replacement:10911", &[("t", 99)]).with_ext("bodyCrc32", 1);
        assert_eq!(ns.process(&request).code, SYSTEM_ERROR);
        assert_eq!(
            body(request_route(&ns, "t"))["queueDatas"][0]["readQueueNums"],
            1
        );
    }

    #[test]
    fn malformed_registration_is_strict_and_transactional() {
        let (_dir, ns) = server();
        assert_eq!(
            ns.process(&register("c", "a", 0, "a:10911", &[("t", 4)]))
                .code,
            0
        );
        for (field, value) in [
            ("brokerId", "-1"),
            ("brokerId", "+1"),
            ("brokerId", "9223372036854775808"),
            ("compressed", "TRUE"),
            ("bodyCrc32", "-10"),
            ("bodyCrc32", "2147483648"),
            ("brokerAddr", "host:0"),
            ("brokerAddr", "host:65536"),
            ("brokerAddr", "host:not-port"),
            ("brokerName", ""),
            ("heartbeatTimeoutMillis", "invalid"),
        ] {
            let request = register("c", "a", 0, "new:10911", &[("t", 99)]).with_ext(field, value);
            assert_eq!(ns.process(&request).code, SYSTEM_ERROR, "field {field}");
        }
        for bytes in [b"".as_slice(), b"{}", b"null", b"{", b"[]"] {
            let request = register("c", "a", 0, "new:10911", &[]).with_body(bytes.to_vec());
            assert_eq!(ns.process(&request).code, SYSTEM_ERROR);
        }
        for (field, value) in [
            ("readQueueNums", json!(-1)),
            ("writeQueueNums", json!(1.5)),
            ("perm", json!(16)),
            ("topicName", json!("different")),
        ] {
            let mut request = register("c", "a", 0, "new:10911", &[("t", 99)]);
            let mut value_json: Value = serde_json::from_slice(&request.body).unwrap();
            value_json["topicConfigSerializeWrapper"]["topicConfigTable"]["t"][field] = value;
            request.body = serde_json::to_vec(&value_json).unwrap();
            assert_eq!(ns.process(&request).code, SYSTEM_ERROR);
        }
        assert_eq!(
            body(request_route(&ns, "t"))["queueDatas"][0]["readQueueNums"],
            4
        );
        assert_eq!(ns.broker_count(), 1);
    }

    #[test]
    fn unknown_and_unimplemented_features_return_three_not_success() {
        let (_dir, ns) = server();
        for code in [318, 322, 217, 900, 999_999] {
            assert_eq!(ns.process(&Command::request(code)).code, NOT_SUPPORTED);
        }
        assert_eq!(
            ns.process(&Command::request(318).with_body(b"kvConfigPath=/tmp/forbidden".to_vec()))
                .code,
            NOT_SUPPORTED
        );
        assert_eq!(ns.broker_count(), 0);
        let config = ns.process(&Command::request(319));
        assert_eq!(config.code, 0);
        assert!(
            String::from_utf8(config.body)
                .unwrap()
                .contains("brokerChannelExpiredTime=30000\n")
        );
    }

    #[test]
    fn input_limits_and_unexpected_bodies_are_rejected_without_exposing_values() {
        let (_dir, ns) = server();
        let request = kv(100, "n", "k").with_ext("value", "x".repeat(MAX_VALUE + 1));
        assert_eq!(ns.process(&request).code, SYSTEM_ERROR);
        let response =
            ns.process(&register("c", "a", 0, "a:10911", &[]).with_body(vec![b'x'; MAX_BODY + 1]));
        assert_eq!(response.code, SYSTEM_ERROR);
        assert!(!response.remark.unwrap().contains("xxxx"));
        assert_eq!(
            ns.process(
                &Command::request(105)
                    .with_ext("topic", "t")
                    .with_body(b"secret".to_vec())
            )
            .code,
            SYSTEM_ERROR
        );
        assert_eq!(ns.broker_count(), 0);
    }

    #[tokio::test]
    async fn handler_preserves_request_correlation_and_ignores_rpc_disconnect() {
        let (_dir, ns) = server();
        let peer = "127.0.0.1:22000".parse().unwrap();
        let mut request = register("c", "a", 0, "a:10911", &[("t", 1)]);
        request.opaque = 12345;
        let response = ns.handle(request, peer).await;
        assert_eq!(response.code, 0);
        assert_eq!(response.opaque, 12345);
        ns.disconnected(peer);
        assert_eq!(ns.broker_count(), 1);
        assert_eq!(
            ns.handle(Command::request(105).with_ext("topic", "t"), peer)
                .await
                .code,
            0
        );
    }

    #[test]
    fn static_topic_mappings_are_registered_and_returned_in_routes() {
        let (_dir, ns) = server();
        let mut request = register("c", "a", 0, "a:10911", &[("t", 4)]);
        let mut value: Value = serde_json::from_slice(&request.body).unwrap();
        value["topicConfigSerializeWrapper"]["topicQueueMappingInfoMap"] = json!({
            "t": {"topic": "t", "bname": "a", "totalQueues": 8, "epoch": 1,
                  "dirty": false, "currIdMap": {"0": 0}}
        });
        // The detail map is accepted but never stored, like the Java NameServer.
        value["topicConfigSerializeWrapper"]["topicQueueMappingDetailMap"] =
            json!({"t": {"topic": "t"}});
        request.body = serde_json::to_vec(&value).unwrap();
        assert_eq!(ns.process(&request).code, 0);
        // Java TopicRouteData.topicQueueMappingByBroker is keyed by bname and echoes
        // the registered info verbatim.
        let route = body(request_route(&ns, "t"));
        assert_eq!(route["topicQueueMappingByBroker"]["a"]["totalQueues"], 8);
        assert_eq!(route["topicQueueMappingByBroker"]["a"]["currIdMap"]["0"], 0);
        assert_eq!(route["topicQueueMappingByBroker"]["a"]["epoch"], 1);
        // Topics without mappings keep the field absent; fastjson omits nulls in Java.
        assert_eq!(
            ns.process(&register("c", "b", 0, "b:10911", &[("other", 1)]))
                .code,
            0
        );
        let route = body(request_route(&ns, "other"));
        assert!(route.get("topicQueueMappingByBroker").is_none());
        // Malformed mapping infos are rejected before any state mutation.
        let mut request = register("c", "a", 0, "a:10911", &[("t", 4)]);
        let mut value: Value = serde_json::from_slice(&request.body).unwrap();
        value["topicConfigSerializeWrapper"]["topicQueueMappingInfoMap"] =
            json!({"t": {"topic": "t"}});
        request.body = serde_json::to_vec(&value).unwrap();
        assert_eq!(ns.process(&request).code, SYSTEM_ERROR);
        assert_eq!(ns.broker_count(), 2);
    }

    #[test]
    fn acting_master_promotes_smallest_slave_and_wipes_write_perm_on_master_loss() {
        let (_dir, ns) = server();
        assert_eq!(
            ns.process(
                &register("c", "a", 0, "master:10911", &[("t", 6)])
                    .with_ext("enableActingMaster", true)
            )
            .code,
            0
        );
        assert_eq!(
            ns.process(
                &register("c", "a", 3, "slave3:10911", &[]).with_ext("enableActingMaster", true)
            )
            .code,
            0
        );
        assert_eq!(
            ns.process(
                &register("c", "a", 1, "slave1:10911", &[]).with_ext("enableActingMaster", true)
            )
            .code,
            0
        );
        // Master alive: addresses keep their registered ids and the flag echoes true.
        let route = body(request_route(&ns, "t"));
        assert_eq!(route["brokerDatas"][0]["enableActingMaster"], true);
        assert_eq!(route["brokerDatas"][0]["brokerAddrs"]["0"], "master:10911");
        assert_eq!(route["brokerDatas"][0]["brokerAddrs"]["1"], "slave1:10911");
        // Master lost by unregistration: write perm wiped, smallest slave re-keyed as 0.
        assert_eq!(
            ns.process(&instance_request(104, "c", "a", 0, "master:10911"))
                .code,
            0
        );
        let route = body(request_route(&ns, "t"));
        assert_eq!(route["queueDatas"][0]["perm"], 6 & !PERM_WRITE);
        assert_eq!(route["brokerDatas"][0]["brokerAddrs"]["0"], "slave1:10911");
        assert!(route["brokerDatas"][0]["brokerAddrs"].get("1").is_none());
        assert!(route["brokerDatas"][0]["brokerAddrs"].get("3").is_some());
        // Groups without the acting-master flag are never promoted or wiped.
        assert_eq!(
            ns.process(&register("c", "b", 0, "bmaster:10911", &[("t", 6)]))
                .code,
            0
        );
        assert_eq!(
            ns.process(&register("c", "b", 2, "bslave:10911", &[])).code,
            0
        );
        assert_eq!(
            ns.process(&instance_request(104, "c", "b", 0, "bmaster:10911"))
                .code,
            0
        );
        let route = body(request_route(&ns, "t"));
        let group_b = route["brokerDatas"]
            .as_array()
            .unwrap()
            .iter()
            .find(|data| data["brokerName"] == "b")
            .unwrap();
        assert_eq!(group_b["enableActingMaster"], false);
        assert_eq!(group_b["brokerAddrs"]["2"], "bslave:10911");
        assert!(group_b["brokerAddrs"].get("0").is_none());
        let perm = route["queueDatas"]
            .as_array()
            .unwrap()
            .iter()
            .find(|data| data["brokerName"] == "b")
            .unwrap()["perm"]
            .as_i64()
            .unwrap();
        assert_eq!(perm & PERM_WRITE as i64, PERM_WRITE as i64);
    }

    #[test]
    fn broker_container_registrations_are_accepted_like_plain_ones() {
        let (_dir, ns) = server();
        let request =
            register("c", "a", 0, "a:10911", &[("t", 4)]).with_ext("isInBrokerContainer", true);
        assert_eq!(ns.process(&request).code, 0);
        let route = body(request_route(&ns, "t"));
        assert_eq!(route["queueDatas"][0]["readQueueNums"], 4);
        assert_eq!(route["brokerDatas"][0]["brokerAddrs"]["0"], "a:10911");
        // An invalid flag value is still rejected before registration.
        let request =
            register("c", "a", 0, "a:10911", &[("t", 4)]).with_ext("isInBrokerContainer", "TRUE");
        assert_eq!(ns.process(&request).code, SYSTEM_ERROR);
    }

    #[test]
    fn write_perm_requests_wipe_and_restore_and_member_group_reports_addresses() {
        let (_dir, ns) = server();
        ns.process(&register("c", "a", 0, "a:10911", &[("t1", 6), ("t2", 7)]));
        ns.process(&register("c", "b", 0, "b:10911", &[("t1", 6)]));
        // Unknown brokers succeed with a zero count, as in Java.
        let wipe = ns.process(&Command::request(205).with_ext("brokerName", "missing"));
        assert_eq!(wipe.code, 0);
        assert_eq!(wipe.ext_fields.get("wipeTopicCount").unwrap(), "0");
        let wipe = ns.process(&Command::request(205).with_ext("brokerName", "a"));
        assert_eq!(wipe.code, 0);
        assert_eq!(wipe.ext_fields.get("wipeTopicCount").unwrap(), "2");
        // Permissions are per broker: a is wiped, b is untouched.
        let route = body(request_route(&ns, "t1"));
        let perm_of = |name: &str| {
            route["queueDatas"]
                .as_array()
                .unwrap()
                .iter()
                .find(|data| data["brokerName"] == name)
                .unwrap()["perm"]
                .as_i64()
                .unwrap()
        };
        assert_eq!(perm_of("a"), (6 & !PERM_WRITE) as i64);
        assert_eq!(perm_of("b"), 6);
        // Add resets to READ|WRITE unconditionally, dropping any other bits.
        let add = ns.process(&Command::request(327).with_ext("brokerName", "a"));
        assert_eq!(add.code, 0);
        assert_eq!(add.ext_fields.get("addTopicCount").unwrap(), "2");
        let route = body(request_route(&ns, "t2"));
        let perm = route["queueDatas"]
            .as_array()
            .unwrap()
            .iter()
            .find(|data| data["brokerName"] == "a")
            .unwrap()["perm"]
            .as_i64()
            .unwrap();
        assert_eq!(perm, (PERM_READ | PERM_WRITE) as i64);
        // GET_BROKER_MEMBER_GROUP mirrors the request names and current addresses.
        let member = ns.process(
            &Command::request(901)
                .with_ext("clusterName", "c")
                .with_ext("brokerName", "a"),
        );
        assert_eq!(member.code, 0);
        let value: Value = serde_json::from_slice(&member.body).unwrap();
        assert_eq!(value["brokerMemberGroup"]["cluster"], "c");
        assert_eq!(value["brokerMemberGroup"]["brokerName"], "a");
        assert_eq!(value["brokerMemberGroup"]["brokerAddrs"]["0"], "a:10911");
        // Unknown brokers return an empty group with success, as in Java.
        let member = ns.process(
            &Command::request(901)
                .with_ext("clusterName", "c")
                .with_ext("brokerName", "missing"),
        );
        assert_eq!(member.code, 0);
        let value: Value = serde_json::from_slice(&member.body).unwrap();
        assert_eq!(
            value["brokerMemberGroup"]["brokerAddrs"],
            json!({})
        );
    }
}
