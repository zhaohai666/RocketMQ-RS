//! Rust port of `org.apache.rocketmq.container`: run multiple broker controllers inside a single
//! process. Java's `BrokerContainer` hosts several `InnerBrokerController`s (master or slave) that
//! each bind their own listener and register with the NameServer; this module mirrors that by
//! spinning up one `broker::Broker` per spec and supervising them together.
//!
//! Note: the Rust broker is a single-node store with no master/slave replication, so an
//! `InnerBrokerController` with `broker_id > 0` runs as an independent broker rather than a true
//! replica. The container does not perform HA log shipping.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::{self, MissedTickBehavior};
use tracing::{info, warn};

use crate::broker::{Broker, BrokerConfig};
use crate::common::{Error, Result};
use crate::remoting::{Handler, invoke, serve};
use crate::store::Store;

const REGISTER_INTERVAL: Duration = Duration::from_secs(10);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(3);

fn default_cluster() -> String {
    "DefaultCluster".into()
}
fn default_broker_name() -> String {
    "broker-a".into()
}
fn default_queues() -> i32 {
    8
}
fn default_true() -> bool {
    true
}
fn default_false() -> bool {
    false
}
fn default_delay() -> String {
    crate::schedule::DEFAULT_DELAY_LEVELS.into()
}

/// One broker hosted inside the container. Mirrors a single `InnerBrokerController` spec.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct InnerBrokerSpec {
    /// Store directory for this inner broker.
    pub data_dir: PathBuf,
    /// TCP listener address (use port 0 for an ephemeral port).
    pub listen: SocketAddr,
    /// Concrete address clients should use; required when `listen` is wildcard.
    #[serde(default)]
    pub advertise: Option<SocketAddr>,
    /// 0 for a master, >0 for a slave (slaves are independent brokers in this port).
    #[serde(default)]
    pub broker_id: i64,
    #[serde(default = "default_broker_name")]
    pub broker_name: String,
    #[serde(default = "default_cluster")]
    pub cluster_name: String,
    /// NameServer addresses this inner broker registers with.
    #[serde(default)]
    pub nameservers: Vec<String>,
    #[serde(default = "default_queues")]
    pub default_queues: i32,
    #[serde(default = "default_true")]
    pub auto_create_topic: bool,
    #[serde(default = "default_false")]
    pub enable_property_filter: bool,
    #[serde(default = "default_true")]
    pub auto_create_group: bool,
    #[serde(default = "default_false")]
    pub reject_pull_consumer: bool,
    #[serde(default = "default_delay")]
    pub message_delay_level: String,
    #[serde(default = "default_true")]
    pub timer_wheel_enable: bool,
    #[serde(default = "default_false")]
    pub recall_message_enable: bool,
}

impl InnerBrokerSpec {
    fn to_broker_config(&self, advertised: SocketAddr) -> BrokerConfig {
        BrokerConfig {
            broker_name: self.broker_name.clone(),
            cluster_name: self.cluster_name.clone(),
            advertised_addr: advertised,
            auto_create_topic: self.auto_create_topic,
            default_queues: self.default_queues,
            enable_property_filter: self.enable_property_filter,
            auto_create_group: self.auto_create_group,
            reject_pull_consumer: self.reject_pull_consumer,
            message_delay_level: self.message_delay_level.clone(),
            timer_wheel_enable: self.timer_wheel_enable,
            recall_message_enable: self.recall_message_enable,
        }
    }
}

/// Container configuration loaded from JSON: the list of inner brokers.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BrokerContainerConfig {
    #[serde(default)]
    pub brokers: Vec<InnerBrokerSpec>,
}

impl BrokerContainerConfig {
    /// Load a container configuration from a JSON file (mirrors `BrokerContainerStartup`).
    pub fn load(path: impl AsRef<std::path::Path>) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .map_err(|error| Error::Io(std::io::Error::new(error.kind(), error.to_string())))?;
        serde_json::from_str(&text).map_err(Error::from)
    }
}

/// Interface for managing the inner brokers hosted by a container (Java: `IBrokerContainer`).
pub trait IBrokerContainer: Send + Sync {
    /// Number of inner brokers currently hosted.
    fn inner_broker_count(&self) -> usize;
    /// Names of the hosted inner brokers.
    fn inner_broker_names(&self) -> Vec<String>;
}

/// A running inner broker: its `Broker` handle plus the bound listener address.
pub struct InnerBrokerController {
    spec: InnerBrokerSpec,
    broker: Arc<Broker>,
    listen: SocketAddr,
}

impl InnerBrokerController {
    pub fn broker(&self) -> &Arc<Broker> {
        &self.broker
    }
    pub fn listen(&self) -> SocketAddr {
        self.listen
    }
    pub fn broker_name(&self) -> &str {
        &self.spec.broker_name
    }
    pub fn broker_id(&self) -> i64 {
        self.spec.broker_id
    }
}

/// Hosts and supervises several [`InnerBrokerController`]s within one process.
pub struct BrokerContainer {
    controllers: Vec<InnerBrokerController>,
    handles: Vec<JoinHandle<Result<()>>>,
}

impl BrokerContainer {
    /// Build controllers from raw specs, opening each store and constructing each broker.
    pub fn from_specs(specs: Vec<InnerBrokerSpec>) -> Result<Self> {
        let mut controllers = Vec::with_capacity(specs.len());
        for spec in specs {
            let listen = spec.listen;
            let advertised = spec.advertise.unwrap_or(listen);
            let broker = Arc::new(Broker::new(
                Arc::new(Store::open(&spec.data_dir)?),
                spec.to_broker_config(advertised),
            )?);
            controllers.push(InnerBrokerController {
                spec,
                broker,
                listen,
            });
        }
        Ok(Self {
            controllers,
            handles: Vec::new(),
        })
    }

    /// Build from a loaded [`BrokerContainerConfig`].
    pub fn from_config(config: BrokerContainerConfig) -> Result<Self> {
        Self::from_specs(config.brokers)
    }

    /// Bind every inner broker's listener and start serving; returns immediately. Call
    /// [`BrokerContainer::wait`] to block until shutdown.
    pub async fn start(&mut self, mut shutdown: watch::Receiver<bool>) -> Result<()> {
        for controller in &self.controllers {
            let listener = TcpListener::bind(controller.listen)
                .await
                .map_err(|error| {
                    Error::Io(std::io::Error::new(
                        error.kind(),
                        format!(
                            "cannot bind inner broker {} to {}: {error}",
                            controller.broker_name(),
                            controller.listen
                        ),
                    ))
                })?;
            let actual = listener.local_addr()?;
            info!(
                broker = controller.broker_name(),
                broker_id = controller.broker_id(),
                listen = %actual,
                "inner broker listener bound"
            );
            let handle = spawn_inner(
                controller.broker.clone(),
                listener,
                controller.spec.nameservers.clone(),
                shutdown.clone(),
            );
            self.handles.push(handle);
        }
        // Trigger shutdown when the parent container shuts down.
        tokio::spawn(async move {
            shutdown_requested(&mut shutdown).await;
        });
        Ok(())
    }

    /// Block until all inner brokers stop; returns the first error encountered.
    pub async fn wait(self) -> Result<()> {
        let mut result = Ok(());
        for handle in self.handles {
            match handle.await {
                Ok(Ok(())) => {}
                Ok(Err(error)) => {
                    warn!(%error, "inner broker exited with error");
                    if result.is_ok() {
                        result = Err(error);
                    }
                }
                Err(error) if error.is_cancelled() => {}
                Err(error) => {
                    if result.is_ok() {
                        result = Err(Error::Io(std::io::Error::other(error)));
                    }
                }
            }
        }
        result
    }

    pub fn controllers(&self) -> &[InnerBrokerController] {
        &self.controllers
    }
}

impl IBrokerContainer for BrokerContainer {
    fn inner_broker_count(&self) -> usize {
        self.controllers.len()
    }
    fn inner_broker_names(&self) -> Vec<String> {
        self.controllers
            .iter()
            .map(|c| c.broker_name().to_string())
            .collect()
    }
}

/// Spawn the four per-broker tasks (serve, scheduler, registration, expiry) and return a handle
/// that resolves when the broker is fully stopped.
fn spawn_inner(
    broker: Arc<Broker>,
    listener: TcpListener,
    nameservers: Vec<String>,
    shutdown: watch::Receiver<bool>,
) -> JoinHandle<Result<()>> {
    tokio::spawn(async move { run_inner(broker, listener, nameservers, shutdown).await })
}

async fn run_inner(
    broker: Arc<Broker>,
    listener: TcpListener,
    nameservers: Vec<String>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let (stop_reg, reg_shutdown) = watch::channel(false);
    let mut tasks: JoinSet<(&'static str, Result<()>)> = JoinSet::new();
    tasks.spawn({
        let s = shutdown.clone();
        let h = broker.clone();
        async move { ("serve", serve(listener, h as Arc<dyn Handler>, s).await) }
    });
    tasks.spawn({
        let s = shutdown.clone();
        let b = broker.clone();
        async move { ("scheduler", b.run_scheduler(s).await) }
    });
    tasks.spawn({
        let s = shutdown.clone();
        let b = broker.clone();
        async move { ("transaction check", b.run_transaction_check(s).await) }
    });
    tasks.spawn({
        let b = broker.clone();
        let ns = nameservers.clone();
        async move { ("registration", registration_loop(b, ns, reg_shutdown).await) }
    });
    tasks.spawn({
        let s = shutdown.clone();
        let b = broker.clone();
        async move { ("expiry", expiry_maintenance(b, s).await) }
    });

    let mut result = Ok(());
    loop {
        tokio::select! {
            biased;
            _ = shutdown_requested(&mut shutdown) => break,
            joined = tasks.join_next() => match joined {
                Some(Ok((name, Ok(())))) => {
                    info!(name, "inner broker task stopped");
                }
                Some(Ok((name, Err(error)))) => {
                    warn!(name, %error, "inner broker task failed");
                    if result.is_ok() {
                        result = Err(error);
                    }
                    break;
                }
                Some(Err(error)) => {
                    if result.is_ok() {
                        result = Err(Error::Io(std::io::Error::other(error)));
                    }
                    break;
                }
                None => break,
            },
        }
    }
    stop_reg.send_replace(true);
    // Allow registration to finish its unregister pass.
    drain(&mut tasks, &mut result, Duration::from_secs(5)).await;
    let store = broker.store().clone();
    let flushed = tokio::task::spawn_blocking(move || store.flush()).await;
    match flushed {
        Ok(Ok(())) => info!("inner broker store flushed"),
        Ok(Err(error)) => record_failure(&mut result, error),
        Err(error) => record_failure(&mut result, Error::Io(std::io::Error::other(error))),
    }
    result
}

async fn drain(
    tasks: &mut JoinSet<(&'static str, Result<()>)>,
    result: &mut Result<()>,
    timeout: Duration,
) {
    let drained = time::timeout(timeout, async {
        while let Some(joined) = tasks.join_next().await {
            match joined {
                Ok((_, Ok(()))) => {}
                Ok((_, Err(error))) => record_failure(result, error),
                Err(error) if error.is_cancelled() => {}
                Err(error) => record_failure(result, Error::Io(std::io::Error::other(error))),
            }
        }
    })
    .await;
    if drained.is_err() {
        tasks.abort_all();
        record_failure(
            result,
            Error::Io(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                "inner broker tasks did not stop within the timeout",
            )),
        );
    }
}

async fn expiry_maintenance(
    broker: Arc<Broker>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let mut interval = time::interval(Duration::from_secs(1));
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            biased;
            _ = shutdown_requested(&mut shutdown) => break,
            _ = interval.tick() => {
                broker.expire_clients();
            }
        }
    }
    Ok(())
}

async fn registration_loop(
    broker: Arc<Broker>,
    nameservers: Vec<String>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let mut interval =
        time::interval_at(time::Instant::now() + REGISTER_INTERVAL, REGISTER_INTERVAL);
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        let registered = tokio::select! {
            biased;
            _ = shutdown_requested(&mut shutdown) => return Ok(()),
            result = update_nameservers(&broker, &nameservers, false) => result?,
        };
        if registered {
            info!(
                nameservers = nameservers.len(),
                "inner broker registered with every configured NameServer"
            );
        }
        tokio::select! {
            biased;
            _ = shutdown_requested(&mut shutdown) => return Ok(()),
            _ = broker.topic_changes.notified() => {}
            _ = interval.tick() => {}
        }
    }
}

async fn update_nameservers(
    broker: &Broker,
    nameservers: &[String],
    unregister: bool,
) -> Result<bool> {
    let operation = if unregister { "unregister" } else { "register" };
    let mut all_succeeded = !nameservers.is_empty();
    let mut tasks: JoinSet<(String, Result<()>)> = JoinSet::new();
    for nameserver in nameservers {
        let request = if unregister {
            Ok(broker.unregistration())
        } else {
            broker.registration()
        };
        match request {
            Ok(request) => {
                let nameserver = nameserver.clone();
                tasks.spawn(async move {
                    let result = invoke_checked(&nameserver, request).await;
                    (nameserver, result)
                });
            }
            Err(error) => {
                all_succeeded = false;
                warn!(%nameserver, operation, %error, "cannot build NameServer request");
            }
        }
    }
    let mut task_result = Ok(());
    while let Some(joined) = tasks.join_next().await {
        match joined {
            Ok((nameserver, Err(error))) => {
                all_succeeded = false;
                warn!(%nameserver, operation, %error, "NameServer request failed");
            }
            Ok((_, Ok(()))) => {}
            Err(error) => record_failure(&mut task_result, Error::Io(std::io::Error::other(error))),
        }
    }
    task_result?;
    Ok(all_succeeded)
}

async fn invoke_checked(address: &str, request: crate::remoting::Command) -> Result<()> {
    let response = time::timeout(REQUEST_TIMEOUT, invoke(address, request, REQUEST_TIMEOUT))
        .await
        .map_err(|_| {
            Error::Io(std::io::Error::new(
                std::io::ErrorKind::TimedOut,
                format!("request to {address} timed out after 3 seconds"),
            ))
        })??;
    if response.code != 0 {
        return Err(Error::Invalid(format!(
            "request to {address} rejected with response code {}",
            response.code
        )));
    }
    Ok(())
}

async fn shutdown_requested(shutdown: &mut watch::Receiver<bool>) {
    loop {
        if *shutdown.borrow_and_update() {
            return;
        }
        if shutdown.changed().await.is_err() {
            return;
        }
    }
}

fn record_failure(result: &mut Result<()>, error: Error) {
    if result.is_ok() {
        *result = Err(error);
    } else {
        warn!(%error, "Additional error during inner broker shutdown");
    }
}

/// Java: `BrokerBootHook` — a startup hook run before/after an inner broker starts.
pub trait BrokerBootHook: Send + Sync {
    fn before_start(&self, _controller: &InnerBrokerController) {}
    fn after_start(&self, _controller: &InnerBrokerController) {}
}

/// Java: `ContainerClientHouseKeepingService` — manages the container's own client connections.
/// In this single-process port there is no separate container-facing client channel, so this is a
/// lightweight periodic keep-alive placeholder that mirrors the lifecycle without a dedicated port.
pub struct ContainerClientHouseKeepingService {
    interval: Duration,
}

impl ContainerClientHouseKeepingService {
    pub fn new() -> Self {
        Self {
            interval: Duration::from_secs(10),
        }
    }

    pub async fn run(self, mut shutdown: watch::Receiver<bool>) -> Result<()> {
        let mut ticker = time::interval(self.interval);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                biased;
                _ = shutdown_requested(&mut shutdown) => break,
                _ = ticker.tick() => {}
            }
        }
        Ok(())
    }
}

impl Default for ContainerClientHouseKeepingService {
    fn default() -> Self {
        Self::new()
    }
}

/// Java: `InnerSalveBrokerController` — a slave inner broker. With no master/slave replication in
/// this port, a slave is just an inner broker with `broker_id > 0`; this type aliases the same
/// controller so callers can express intent.
pub type InnerSlaveBrokerController = InnerBrokerController;
