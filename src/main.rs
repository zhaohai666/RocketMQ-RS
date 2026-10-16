use std::io::{self, Write};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;
use std::time::Duration;

use clap::{ArgAction, Args, Parser, Subcommand};
use rocketmq_rs::broker::{Broker, BrokerConfig};
use rocketmq_rs::common::{Error, Result};
use rocketmq_rs::container::{BrokerContainer, BrokerContainerConfig};
use rocketmq_rs::controller::{ControllerConfig, ControllerManager};
use rocketmq_rs::namesrv::Namesrv;
use rocketmq_rs::proxy::{ProxyConfig, ProxyContext};
use rocketmq_rs::remoting::{Command, Handler, invoke, serve};
use rocketmq_rs::store::Store;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::{JoinError, JoinSet};
use tokio::time::{self, MissedTickBehavior};
use tracing::{info, warn};
use tracing_subscriber::EnvFilter;

const REQUEST_TIMEOUT: Duration = Duration::from_secs(3);
const REGISTER_INTERVAL: Duration = Duration::from_secs(10);
const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(35);
const SECURITY_NOTICE: &str = "TLS and authentication are not implemented. Do not expose these listeners directly to the public Internet.";

#[derive(Debug, Parser)]
#[command(name = "rocketmq", version, about = "Rust RocketMQ development server and administration CLI", after_help = SECURITY_NOTICE)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Run a NameServer.
    Namesrv(NamesrvArgs),
    /// Run a single broker (no HA or replication).
    Broker(BrokerArgs),
    /// Run a NameServer and broker in one process for development/testing, not Java broker-container dynamic management.
    Standalone(StandaloneArgs),
    /// Run the HA controller (master election, sync-state management).
    Controller(ControllerArgs),
    /// Run the v2 gRPC proxy in front of namesrv/brokers.
    Proxy(ProxyArgs),
    /// Host several statically-configured brokers in one process.
    Container(ContainerArgs),
    /// Inspect clusters/routes/runtime or create a topic.
    Admin(AdminArgs),
}

#[derive(Debug, Args)]
struct NamesrvArgs {
    #[arg(long, default_value = "127.0.0.1:9876")]
    listen: SocketAddr,
    #[arg(long, default_value = "data/namesrv")]
    data_dir: PathBuf,
    #[arg(long, default_value_t = 120, value_parser = clap::value_parser!(u64).range(1..))]
    broker_timeout_secs: u64,
}

#[derive(Debug, Args)]
struct BrokerArgs {
    #[arg(long, default_value = "127.0.0.1:10911")]
    listen: SocketAddr,
    /// Concrete IP:port clients can reach; required for a wildcard listener.
    #[arg(long, value_parser = parse_advertise)]
    advertise: Option<SocketAddr>,
    /// NameServer host:port addresses; repeat this flag or separate with ';'.
    #[arg(long, default_value = "127.0.0.1:9876", value_delimiter = ';', action = ArgAction::Append, value_parser = parse_endpoint)]
    namesrv: Vec<String>,
    #[arg(long, default_value = "data/broker")]
    data_dir: PathBuf,
    #[arg(long, default_value = "broker-a", value_parser = parse_name)]
    broker_name: String,
    #[arg(long, default_value = "DefaultCluster", value_parser = parse_name)]
    cluster_name: String,
    #[arg(long, default_value_t = 8, value_parser = clap::value_parser!(i32).range(1..=1024))]
    default_queues: i32,
    #[arg(long, default_value_t = true, action = ArgAction::Set)]
    auto_create_topic: bool,
    /// BrokerConfig.enablePropertyFilter
    #[arg(long, default_value_t = false, action = ArgAction::Set)]
    enable_property_filter: bool,
    /// BrokerConfig.autoCreateSubscriptionGroup
    #[arg(long, default_value_t = true, action = ArgAction::Set)]
    auto_create_group: bool,
    /// BrokerConfig.rejectPullConsumerEnable
    #[arg(long, default_value_t = false, action = ArgAction::Set)]
    reject_pull_consumer: bool,
    /// MessageStoreConfig.messageDelayLevel, e.g. "1s 5s 10s 30s"
    #[arg(long, default_value = rocketmq_rs::schedule::DEFAULT_DELAY_LEVELS)]
    message_delay_level: String,
    /// MessageStoreConfig.timerWheelEnable
    #[arg(long, default_value_t = true, action = ArgAction::Set)]
    timer_wheel_enable: bool,
    /// BrokerConfig.recallMessageEnable
    #[arg(long, default_value_t = false, action = ArgAction::Set)]
    recall_message_enable: bool,
    /// Optional additional broker listener; no automatic VIP port is opened.
    #[arg(long)]
    vip_listen: Option<SocketAddr>,
}

#[derive(Debug, Args)]
struct StandaloneArgs {
    /// Loopback or concrete IP; port 0 selects an available port.
    #[arg(long, default_value = "127.0.0.1:9876", value_parser = parse_concrete_listener)]
    namesrv_listen: SocketAddr,
    /// Loopback or concrete IP; port 0 selects an available port.
    #[arg(long, default_value = "127.0.0.1:10911", value_parser = parse_concrete_listener)]
    broker_listen: SocketAddr,
    #[arg(long, default_value = "data")]
    data_dir: PathBuf,
    #[arg(long, default_value = "broker-a", value_parser = parse_name)]
    broker_name: String,
    #[arg(long, default_value_t = false, action = ArgAction::Set)]
    enable_property_filter: bool,
    /// MessageStoreConfig.timerWheelEnable
    #[arg(long, default_value_t = true, action = ArgAction::Set)]
    timer_wheel_enable: bool,
    /// BrokerConfig.recallMessageEnable
    #[arg(long, default_value_t = false, action = ArgAction::Set)]
    recall_message_enable: bool,
}

#[derive(Debug, Args)]
struct ControllerArgs {
    #[arg(long, default_value = "127.0.0.1:9878")]
    listen: SocketAddr,
}

#[derive(Debug, Args)]
struct ProxyArgs {
    #[arg(long, default_value = "127.0.0.1:8081")]
    listen: SocketAddr,
    /// Optional JSON ProxyConfig file; CLI flags override its values.
    #[arg(long)]
    config: Option<PathBuf>,
    /// NameServer host:port addresses; repeat this flag or separate with ';'.
    #[arg(long, value_delimiter = ';', action = ArgAction::Append, value_parser = parse_endpoint)]
    namesrv: Vec<String>,
}

#[derive(Debug, Args)]
struct ContainerArgs {
    /// JSON BrokerContainerConfig file describing the hosted inner brokers.
    #[arg(long)]
    config: PathBuf,
}

#[derive(Debug, Args)]
struct AdminArgs {
    #[command(subcommand)]
    command: AdminCommand,
}

#[derive(Debug, Subcommand)]
enum AdminCommand {
    ClusterList {
        #[arg(long, default_value = "127.0.0.1:9876", value_parser = parse_endpoint)]
        namesrv: String,
    },
    TopicRoute {
        #[arg(long, default_value = "127.0.0.1:9876", value_parser = parse_endpoint)]
        namesrv: String,
        #[arg(long, value_parser = parse_name)]
        topic: String,
    },
    TopicCreate {
        #[arg(long, default_value = "127.0.0.1:10911", value_parser = parse_endpoint)]
        broker: String,
        #[arg(long, value_parser = parse_name)]
        topic: String,
        #[arg(long, default_value_t = 4, value_parser = clap::value_parser!(i32).range(1..=1024))]
        queues: i32,
    },
    Runtime {
        #[arg(long, default_value = "127.0.0.1:10911", value_parser = parse_endpoint)]
        broker: String,
    },
}

fn parse_name(value: &str) -> std::result::Result<String, String> {
    if value.is_empty()
        || value.len() > 127
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"%_-|".contains(&byte))
    {
        return Err("expected 1..127 ASCII letters, digits, %, _, - or |".into());
    }
    Ok(value.to_owned())
}

fn parse_concrete_listener(value: &str) -> std::result::Result<SocketAddr, String> {
    let address = value
        .parse::<SocketAddr>()
        .map_err(|_| "expected an IP:port address (bracket IPv6 addresses)".to_owned())?;
    if address.ip().is_unspecified() {
        return Err(
            "a concrete IP is required; use a loopback address for local development".into(),
        );
    }
    Ok(address)
}

fn parse_advertise(value: &str) -> std::result::Result<SocketAddr, String> {
    let address = parse_concrete_listener(value)?;
    if address.port() == 0 {
        return Err("the advertised port must be nonzero".into());
    }
    Ok(address)
}

fn parse_endpoint(value: &str) -> std::result::Result<String, String> {
    if let Ok(address) = value.parse::<SocketAddr>() {
        if address.ip().is_unspecified() || address.port() == 0 {
            return Err("the destination must have a concrete IP and a nonzero port".into());
        }
        return Ok(address.to_string());
    }
    let (host, port) = value
        .rsplit_once(':')
        .ok_or_else(|| "expected host:port (bracket IPv6 addresses)".to_owned())?;
    if host.is_empty()
        || !host
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b".-_".contains(&byte))
        || !matches!(port.parse::<u16>(), Ok(1..=65535))
    {
        return Err("expected host:port with a nonzero port (bracket IPv6 addresses)".into());
    }
    Ok(value.to_owned())
}

fn validate_broker_listeners(args: &BrokerArgs) -> Result<()> {
    if args.advertise.is_none()
        && (args.listen.ip().is_unspecified()
            || args
                .vip_listen
                .is_some_and(|addr| addr.ip().is_unspecified()))
    {
        return Err(Error::Invalid(
            "wildcard broker listeners require an explicit --advertise concrete-IP:port".into(),
        ));
    }
    Ok(())
}

#[tokio::main]
async fn main() -> ExitCode {
    let cli = Cli::parse();
    match run(cli).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("rocketmq: {error}");
            ExitCode::FAILURE
        }
    }
}

async fn run(cli: Cli) -> Result<()> {
    let filter = match EnvFilter::try_from_default_env() {
        Ok(filter) => filter,
        Err(_) => EnvFilter::new("info"),
    };
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(io::stderr)
        .try_init()
        .map_err(|error| Error::Io(io::Error::other(error.to_string())))?;

    if let Commands::Admin(args) = cli.command {
        return run_admin(args.command).await;
    }

    let signals = ShutdownSignals::new()?;
    warn!("{SECURITY_NOTICE}");
    match cli.command {
        Commands::Namesrv(args) => supervise(prepare_namesrv(args).await?, signals).await,
        Commands::Broker(args) => supervise(prepare_broker(args).await?, signals).await,
        Commands::Standalone(args) => supervise(prepare_standalone(args).await?, signals).await,
        Commands::Controller(args) => run_controller(args, signals).await,
        Commands::Proxy(args) => run_proxy(args, signals).await,
        Commands::Container(args) => run_container(args, signals).await,
        Commands::Admin(_) => return Err(Error::Invalid("unexpected admin command".into())),
    }
}

/// Java `ControllerManager.startup`: serve the controller and run its re-election loop.
async fn run_controller(args: ControllerArgs, mut signals: ShutdownSignals) -> Result<()> {
    let (listener, actual) = bind_listener("controller", args.listen).await?;
    info!(listen = %actual, "controller starting");
    let manager = ControllerManager::new(ControllerConfig::default());
    let (stop, shutdown) = watch::channel(false);
    let mut task = tokio::spawn(manager.clone().run(listener, shutdown.clone()));
    tokio::select! {
        joined = &mut task => {
            joined.map_err(|error| Error::Io(io::Error::other(error)))?
        }
        _ = signals.wait() => {
            let _ = stop.send(true);
            task.await.map_err(|error| Error::Io(io::Error::other(error)))?
        }
    }
}

/// Java `ProxyStartup`: host the v2 gRPC proxy (MessagingService + Admin).
async fn run_proxy(args: ProxyArgs, mut signals: ShutdownSignals) -> Result<()> {
    let (listener, actual) = bind_listener("proxy-grpc", args.listen).await?;
    let mut config = match &args.config {
        Some(path) => ProxyConfig::load(path.to_str().unwrap_or_default())?,
        None => ProxyConfig::default(),
    };
    if !args.namesrv.is_empty() {
        config.name_server_address = args.namesrv.clone();
    }
    config.validate()?;
    if config.name_server_address.is_empty() {
        warn!("proxy has no nameserver address; route queries will fail until one is configured");
    }
    info!(listen = %actual, nameservers = ?config.name_server_address, "gRPC proxy starting");
    let context = ProxyContext::new(config);
    let (stop, shutdown) = watch::channel(false);
    let mut task = tokio::spawn(rocketmq_rs::proxy::serve_proxy(listener, context, shutdown.clone()));
    tokio::select! {
        joined = &mut task => {
            joined.map_err(|error| Error::Io(io::Error::other(error)))?
        }
        _ = signals.wait() => {
            let _ = stop.send(true);
            task.await.map_err(|error| Error::Io(io::Error::other(error)))?
        }
    }
}

/// Java `BrokerContainerStartup`: host statically configured inner brokers.
async fn run_container(args: ContainerArgs, mut signals: ShutdownSignals) -> Result<()> {
    let config = BrokerContainerConfig::load(&args.config)?;
    let mut container = BrokerContainer::from_config(config)?;
    let (stop, shutdown) = watch::channel(false);
    let mut task = tokio::spawn(async move {
        container.start(shutdown).await?;
        container.wait().await
    });
    tokio::select! {
        joined = &mut task => {
            joined.map_err(|error| Error::Io(io::Error::other(error)))?
        }
        _ = signals.wait() => {
            let _ = stop.send(true);
            task.await.map_err(|error| Error::Io(io::Error::other(error)))?
        }
    }
}

struct Server {
    name: &'static str,
    listener: TcpListener,
    handler: Arc<dyn Handler>,
}

struct Services {
    servers: Vec<Server>,
    namesrv: Option<Arc<Namesrv>>,
    broker: Option<Arc<Broker>>,
    nameservers: Vec<String>,
}

async fn bind_listener(name: &str, address: SocketAddr) -> Result<(TcpListener, SocketAddr)> {
    let listener = TcpListener::bind(address).await.map_err(|error| {
        Error::Io(io::Error::new(
            error.kind(),
            format!("cannot bind {name} to {address}: {error}"),
        ))
    })?;
    let actual = listener.local_addr()?;
    info!(service = name, listen = %actual, "TCP listener bound");
    Ok((listener, actual))
}

async fn prepare_namesrv(args: NamesrvArgs) -> Result<Services> {
    let (listener, _) = bind_listener("namesrv", args.listen).await?;
    let namesrv = Arc::new(Namesrv::open(
        args.data_dir,
        Duration::from_secs(args.broker_timeout_secs),
    )?);
    Ok(Services {
        servers: vec![Server {
            name: "namesrv",
            listener,
            handler: namesrv.clone(),
        }],
        namesrv: Some(namesrv),
        broker: None,
        nameservers: Vec::new(),
    })
}

async fn prepare_broker(args: BrokerArgs) -> Result<Services> {
    validate_broker_listeners(&args)?;
    let (listener, actual) = bind_listener("broker", args.listen).await?;
    let vip = match args.vip_listen {
        Some(address) => Some(bind_listener("broker-vip", address).await?.0),
        None => None,
    };
    let advertised_addr = match args.advertise {
        Some(address) => address,
        None => actual,
    };
    let broker = Arc::new(Broker::new(
        Arc::new(Store::open(args.data_dir)?),
        BrokerConfig {
            broker_name: args.broker_name,
            cluster_name: args.cluster_name,
            advertised_addr,
            auto_create_topic: args.auto_create_topic,
            default_queues: args.default_queues,
            enable_property_filter: args.enable_property_filter,
            auto_create_group: args.auto_create_group,
            reject_pull_consumer: args.reject_pull_consumer,
            message_delay_level: args.message_delay_level,
            timer_wheel_enable: args.timer_wheel_enable,
            recall_message_enable: args.recall_message_enable,
        },
    )?);
    info!(advertise = %advertised_addr, "Broker bound; NameServer registration is pending");
    let mut servers = vec![Server {
        name: "broker",
        listener,
        handler: broker.clone(),
    }];
    if let Some(listener) = vip {
        servers.push(Server {
            name: "broker-vip",
            listener,
            handler: broker.clone(),
        });
    }
    let mut nameservers = args.namesrv;
    nameservers.sort();
    nameservers.dedup();
    Ok(Services {
        servers,
        namesrv: None,
        broker: Some(broker),
        nameservers,
    })
}

async fn prepare_standalone(args: StandaloneArgs) -> Result<Services> {
    let (namesrv_listener, namesrv_address) = bind_listener("namesrv", args.namesrv_listen).await?;
    let (broker_listener, broker_address) = bind_listener("broker", args.broker_listen).await?;
    let namesrv = Arc::new(Namesrv::open(
        args.data_dir.join("namesrv"),
        Duration::from_secs(120),
    )?);
    let broker = Arc::new(Broker::new(
        Arc::new(Store::open(args.data_dir.join("broker"))?),
        BrokerConfig {
            broker_name: args.broker_name,
            cluster_name: "DefaultCluster".into(),
            advertised_addr: broker_address,
            auto_create_topic: true,
            default_queues: 8,
            enable_property_filter: args.enable_property_filter,
            timer_wheel_enable: args.timer_wheel_enable,
            recall_message_enable: args.recall_message_enable,
            ..BrokerConfig::default()
        },
    )?);
    info!(
        namesrv = %namesrv_address,
        advertise = %broker_address,
        "Standalone development server bound; NameServer registration is pending"
    );
    Ok(Services {
        servers: vec![
            Server {
                name: "namesrv",
                listener: namesrv_listener,
                handler: namesrv.clone(),
            },
            Server {
                name: "broker",
                listener: broker_listener,
                handler: broker.clone(),
            },
        ],
        namesrv: Some(namesrv),
        broker: Some(broker),
        nameservers: vec![namesrv_address.to_string()],
    })
}

struct ShutdownSignals {
    #[cfg(unix)]
    interrupt: tokio::signal::unix::Signal,
    #[cfg(unix)]
    terminate: tokio::signal::unix::Signal,
}

impl ShutdownSignals {
    fn new() -> Result<Self> {
        Ok(Self {
            #[cfg(unix)]
            interrupt: tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?,
            #[cfg(unix)]
            terminate: tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?,
        })
    }

    async fn wait(&mut self) -> Result<()> {
        #[cfg(unix)]
        {
            let received = tokio::select! {
                signal = self.interrupt.recv() => signal,
                signal = self.terminate.recv() => signal,
            };
            if received.is_none() {
                return Err(Error::Io(io::Error::other("signal stream closed")));
            }
        }
        #[cfg(not(unix))]
        tokio::signal::ctrl_c().await?;
        info!("Shutdown signal received");
        Ok(())
    }
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

type ServiceExit = (&'static str, Result<()>);

fn service_result(
    joined: std::result::Result<ServiceExit, JoinError>,
    unexpected: bool,
) -> Result<()> {
    match joined {
        Ok((name, Ok(()))) if unexpected => Err(Error::Io(io::Error::other(format!(
            "{name} stopped unexpectedly"
        )))),
        Ok((_, Ok(()))) => Ok(()),
        Ok((name, Err(error))) => Err(Error::Io(io::Error::other(format!("{name}: {error}")))),
        Err(error) => Err(Error::Io(io::Error::other(error))),
    }
}

fn record_failure(result: &mut Result<()>, error: Error) {
    if result.is_ok() {
        *result = Err(error);
    } else {
        warn!(%error, "Additional error during shutdown");
    }
}

async fn supervise(services: Services, mut signals: ShutdownSignals) -> Result<()> {
    let (stop, shutdown) = watch::channel(false);
    let (stop_registration, registration_shutdown) = watch::channel(false);
    let mut tasks = JoinSet::new();
    let mut registration_tasks = JoinSet::new();
    for server in services.servers {
        let shutdown = shutdown.clone();
        tasks.spawn(async move {
            (
                server.name,
                serve(server.listener, server.handler, shutdown).await,
            )
        });
    }
    let namesrv = services.namesrv;
    let broker = services.broker.clone();
    let maintenance_shutdown = shutdown.clone();
    tasks.spawn(async move {
        (
            "expiry maintenance",
            maintain_expiry(namesrv, broker, maintenance_shutdown).await,
        )
    });
    if let Some(broker) = services.broker.clone() {
        let scheduler_shutdown = shutdown.clone();
        tasks.spawn(async move {
            (
                "delay delivery",
                broker.run_scheduler(scheduler_shutdown).await,
            )
        });
    }
    if let Some(broker) = services.broker.clone() {
        let trans_shutdown = shutdown.clone();
        tasks.spawn(async move {
            (
                "transaction check",
                broker.run_transaction_check(trans_shutdown).await,
            )
        });
    }
    if let Some(broker) = services.broker.clone() {
        let nameservers = services.nameservers.clone();
        registration_tasks.spawn(async move {
            (
                "broker registration",
                registration_loop(broker, nameservers, registration_shutdown).await,
            )
        });
    }

    let mut result = tokio::select! {
        signal = signals.wait() => signal,
        joined = tasks.join_next() => match joined {
            Some(joined) => service_result(joined, true),
            None => Err(Error::Io(io::Error::other("all services stopped unexpectedly"))),
        },
        joined = registration_tasks.join_next(), if !registration_tasks.is_empty() => match joined {
            Some(joined) => service_result(joined, true),
            None => Err(Error::Io(io::Error::other("registration task disappeared"))),
        },
    };
    // Unregistration needs live listeners and must not race a new registration.
    stop_registration.send_replace(true);
    drain_tasks(&mut registration_tasks, &mut result).await;
    if let Some(broker) = &services.broker
        && let Err(error) = update_nameservers(broker, &services.nameservers, true).await
    {
        record_failure(&mut result, error);
    }
    stop.send_replace(true);
    drain_tasks(&mut tasks, &mut result).await;

    if let Some(broker) = services.broker {
        let store = broker.store().clone();
        let flushed = tokio::task::spawn_blocking(move || store.flush()).await;
        match flushed {
            Ok(Ok(())) => info!("Broker store flushed"),
            Ok(Err(error)) => record_failure(&mut result, error),
            Err(error) => record_failure(&mut result, Error::Io(io::Error::other(error))),
        }
    }
    result
}

async fn drain_tasks(tasks: &mut JoinSet<ServiceExit>, result: &mut Result<()>) {
    let drained = time::timeout(SHUTDOWN_TIMEOUT, async {
        while let Some(joined) = tasks.join_next().await {
            if let Err(error) = service_result(joined, false) {
                record_failure(result, error);
            }
        }
    })
    .await;
    if drained.is_err() {
        record_failure(
            result,
            Error::Io(io::Error::new(
                io::ErrorKind::TimedOut,
                "services did not stop within 35 seconds",
            )),
        );
        tasks.abort_all();
        while let Some(joined) = tasks.join_next().await {
            if matches!(&joined, Err(error) if error.is_cancelled()) {
                continue;
            }
            if let Err(error) = service_result(joined, false) {
                record_failure(result, error);
            }
        }
    }
}

async fn maintain_expiry(
    namesrv: Option<Arc<Namesrv>>,
    broker: Option<Arc<Broker>>,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let mut interval = time::interval(Duration::from_secs(1));
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut ticks = 0u8;
    loop {
        tokio::select! {
            biased;
            _ = shutdown_requested(&mut shutdown) => break,
            _ = interval.tick() => {
                if let Some(namesrv) = &namesrv {
                    namesrv.expire_brokers();
                }
                if let Some(broker) = &broker {
                    broker.expire_clients();
                }
                ticks = ticks.wrapping_add(1);
                if ticks == 10 {
                    ticks = 0;
                    if let Some(broker) = &broker
                        && let Err(error) = broker.persist_filters().await
                    {
                        warn!(%error, "consumer filter persist failed");
                    }
                }
            }
        }
    }
    // `BrokerController.persistConfig` schedules one final filter persist at shutdown.
    if let Some(broker) = &broker {
        broker.persist_filters().await?;
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
    let mut was_registered = false;
    loop {
        let registered = tokio::select! {
            biased;
            _ = shutdown_requested(&mut shutdown) => return Ok(()),
            result = update_nameservers(&broker, &nameservers, false) => result?,
        };
        if registered && !was_registered {
            info!(
                nameservers = nameservers.len(),
                "Broker registered with every configured NameServer"
            );
        }
        was_registered = registered;
        tokio::select! {
            biased;
            _ = shutdown_requested(&mut shutdown) => return Ok(()),
            _ = broker.topic_changes.notified() => {},
            _ = interval.tick() => {},
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
    let mut tasks = JoinSet::new();
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
                warn!(%nameserver, operation, %error, "Cannot build NameServer request; registration will be retried");
            }
        }
    }
    // Contact all NameServers concurrently; one unavailable peer cannot delay the rest.
    let mut task_result = Ok(());
    while let Some(joined) = tasks.join_next().await {
        match joined {
            Ok((nameserver, Err(error))) => {
                all_succeeded = false;
                warn!(%nameserver, operation, %error, "NameServer request failed; registration will be retried while running");
            }
            Ok((_, Ok(_))) => {}
            Err(error) => record_failure(&mut task_result, Error::Io(io::Error::other(error))),
        }
    }
    task_result?;
    Ok(all_succeeded)
}

async fn invoke_checked(address: &str, request: Command) -> Result<Command> {
    // The transport timeout may omit DNS or other phases, so bound the entire request.
    let response = time::timeout(REQUEST_TIMEOUT, invoke(address, request, REQUEST_TIMEOUT))
        .await
        .map_err(|_| {
            Error::Io(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("request to {address} timed out after 3 seconds"),
            ))
        })??;
    if response.code != 0 {
        // Do not dump response headers, bodies or potentially sensitive remarks.
        return Err(Error::Invalid(format!(
            "request to {address} rejected with response code {}",
            response.code
        )));
    }
    Ok(response)
}

fn admin_request(command: AdminCommand) -> (String, Command) {
    match command {
        AdminCommand::ClusterList { namesrv } => (namesrv, Command::request(106)),
        AdminCommand::TopicRoute { namesrv, topic } => {
            (namesrv, Command::request(105).with_ext("topic", topic))
        }
        AdminCommand::TopicCreate {
            broker,
            topic,
            queues,
        } => (
            broker,
            Command::request(17)
                .with_ext("topic", topic)
                .with_ext("readQueueNums", queues)
                .with_ext("writeQueueNums", queues)
                .with_ext("perm", 6)
                .with_ext("topicFilterType", "SINGLE_TAG")
                .with_ext("topicSysFlag", 0)
                .with_ext("order", false),
        ),
        AdminCommand::Runtime { broker } => (broker, Command::request(28)),
    }
}

fn admin_output(response: &Command) -> Result<String> {
    if response.body.is_empty() {
        return Ok(serde_json::to_string_pretty(&serde_json::json!({
            "code": response.code
        }))?);
    }
    match serde_json::from_slice::<serde_json::Value>(&response.body) {
        Ok(value) => Ok(serde_json::to_string_pretty(&value)?),
        Err(_) => std::str::from_utf8(&response.body)
            .map(str::to_owned)
            .map_err(|_| Error::Invalid("admin response is neither JSON nor UTF-8 text".into())),
    }
}

async fn run_admin(command: AdminCommand) -> Result<()> {
    let (address, request) = admin_request(command);
    let response = invoke_checked(&address, request).await?;
    let output = admin_output(&response)?;
    let mut stdout = io::stdout().lock();
    stdout.write_all(output.as_bytes())?;
    if !output.ends_with('\n') {
        stdout.write_all(b"\n")?;
    }
    stdout.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn clap_definition_is_consistent() {
        use clap::CommandFactory;
        Cli::command().debug_assert();
    }

    #[test]
    fn server_defaults_are_loopback() {
        let cli = Cli::try_parse_from(["rocketmq", "namesrv"]).expect("namesrv defaults");
        let Commands::Namesrv(args) = cli.command else {
            panic!("expected namesrv");
        };
        assert_eq!(args.listen.to_string(), "127.0.0.1:9876");
        assert_eq!(args.data_dir, PathBuf::from("data/namesrv"));
        assert_eq!(args.broker_timeout_secs, 120);

        let cli = Cli::try_parse_from(["rocketmq", "broker"]).expect("broker defaults");
        let Commands::Broker(args) = cli.command else {
            panic!("expected broker");
        };
        assert_eq!(args.listen.to_string(), "127.0.0.1:10911");
        assert_eq!(args.namesrv, ["127.0.0.1:9876"]);
        assert_eq!(args.data_dir, PathBuf::from("data/broker"));
        assert_eq!(args.broker_name, "broker-a");
        assert_eq!(args.cluster_name, "DefaultCluster");
        assert_eq!(args.default_queues, 8);
        assert!(args.auto_create_topic);
        assert!(args.vip_listen.is_none());
        assert!(validate_broker_listeners(&args).is_ok());
    }

    #[test]
    fn nameservers_accept_semicolons_and_repeated_flags() {
        let cli = Cli::try_parse_from([
            "rocketmq",
            "broker",
            "--namesrv",
            "127.0.0.1:9876;ns-b:9876",
            "--namesrv",
            "[::1]:19876",
        ])
        .expect("multiple nameservers");
        let Commands::Broker(args) = cli.command else {
            panic!("expected broker");
        };
        assert_eq!(args.namesrv, ["127.0.0.1:9876", "ns-b:9876", "[::1]:19876"]);
    }

    #[test]
    fn auto_create_topic_requires_an_explicit_boolean_value() {
        for enabled in ["true", "false"] {
            let cli = Cli::try_parse_from(["rocketmq", "broker", "--auto-create-topic", enabled])
                .expect("explicit boolean");
            let Commands::Broker(args) = cli.command else {
                panic!("expected broker");
            };
            assert_eq!(args.auto_create_topic, enabled == "true");
        }
        assert!(Cli::try_parse_from(["rocketmq", "broker", "--auto-create-topic"]).is_err());
        assert!(Cli::try_parse_from(["rocketmq", "broker", "--auto-create-topic", "yes"]).is_err());
    }

    #[test]
    fn wildcard_broker_requires_concrete_advertise() {
        let cli = Cli::try_parse_from(["rocketmq", "broker", "--listen", "0.0.0.0:10911"])
            .expect("socket syntax");
        let Commands::Broker(args) = cli.command else {
            panic!("expected broker");
        };
        assert!(validate_broker_listeners(&args).is_err());
        let cli = Cli::try_parse_from([
            "rocketmq",
            "broker",
            "--listen",
            "[::]:0",
            "--advertise",
            "[::1]:10911",
            "--vip-listen",
            "[::]:10909",
        ])
        .expect("explicit advertisement");
        let Commands::Broker(args) = cli.command else {
            panic!("expected broker");
        };
        assert!(validate_broker_listeners(&args).is_ok());
        for address in ["0.0.0.0:10911", "[::]:10911", "127.0.0.1:0", "host:10911"] {
            assert!(Cli::try_parse_from(["rocketmq", "broker", "--advertise", address]).is_err());
        }
    }

    #[test]
    fn standalone_allows_auto_ports_but_not_wildcards() {
        let cli = Cli::try_parse_from([
            "rocketmq",
            "standalone",
            "--namesrv-listen",
            "127.0.0.1:0",
            "--broker-listen",
            "[::1]:0",
            "--data-dir",
            "test-data",
            "--broker-name",
            "test-a",
        ])
        .expect("ephemeral standalone ports");
        let Commands::Standalone(args) = cli.command else {
            panic!("expected standalone");
        };
        assert_eq!(args.namesrv_listen.port(), 0);
        assert_eq!(args.broker_listen.port(), 0);
        assert_eq!(args.data_dir, PathBuf::from("test-data"));
        assert_eq!(args.broker_name, "test-a");
        for flag in ["--namesrv-listen", "--broker-listen"] {
            assert!(Cli::try_parse_from(["rocketmq", "standalone", flag, "0.0.0.0:0"]).is_err());
        }
    }

    #[test]
    fn invalid_values_and_unimplemented_features_are_rejected() {
        for args in [
            vec!["rocketmq", "broker", "--default-queues", "0"],
            vec!["rocketmq", "broker", "--default-queues", "1025"],
            vec!["rocketmq", "broker", "--broker-name", "../bad"],
            vec!["rocketmq", "broker", "--namesrv", ""],
            vec!["rocketmq", "broker", "--namesrv", "0.0.0.0:9876"],
            vec!["rocketmq", "broker", "--namesrv", "localhost:0"],
            vec!["rocketmq", "namesrv", "--broker-timeout-secs", "0"],
            vec!["rocketmq", "broker", "--ha-listen", "127.0.0.1:10912"],
            vec!["rocketmq", "broker", "--acl-enable"],
            vec!["rocketmq", "admin", "broker-config"],
            vec![
                "rocketmq",
                "admin",
                "topic-create",
                "--topic",
                "X",
                "--queues",
                "0",
            ],
            vec!["rocketmq", "admin", "topic-route"],
        ] {
            assert!(Cli::try_parse_from(args.clone()).is_err(), "{args:?}");
        }
    }

    #[test]
    fn admin_requests_match_protocol() {
        for (args, expected_code, expected_address) in [
            (
                vec![
                    "rocketmq",
                    "admin",
                    "cluster-list",
                    "--namesrv",
                    "localhost:9876",
                ],
                106,
                "localhost:9876",
            ),
            (
                vec![
                    "rocketmq",
                    "admin",
                    "topic-route",
                    "--namesrv",
                    "localhost:9876",
                    "--topic",
                    "X",
                ],
                105,
                "localhost:9876",
            ),
            (
                vec![
                    "rocketmq",
                    "admin",
                    "topic-create",
                    "--broker",
                    "127.0.0.1:10911",
                    "--topic",
                    "X",
                    "--queues",
                    "4",
                ],
                17,
                "127.0.0.1:10911",
            ),
            (
                vec!["rocketmq", "admin", "runtime", "--broker", "[::1]:10911"],
                28,
                "[::1]:10911",
            ),
        ] {
            let cli = Cli::try_parse_from(args).expect("admin command");
            let Commands::Admin(args) = cli.command else {
                panic!("expected admin");
            };
            let (address, request) = admin_request(args.command);
            assert_eq!(address, expected_address);
            assert_eq!(request.code, expected_code);
            if expected_code == 105 || expected_code == 17 {
                assert_eq!(
                    request.ext_fields.get("topic").map(String::as_str),
                    Some("X")
                );
            }
            if expected_code == 17 {
                for (key, value) in [
                    ("readQueueNums", "4"),
                    ("writeQueueNums", "4"),
                    ("perm", "6"),
                    ("topicFilterType", "SINGLE_TAG"),
                    ("topicSysFlag", "0"),
                    ("order", "false"),
                ] {
                    assert_eq!(request.ext_fields.get(key).map(String::as_str), Some(value));
                }
            }
        }
    }

    #[test]
    fn admin_output_handles_json_text_and_empty_without_dumping_headers() {
        let response = Command::request(0).with_ext("secret", "must-not-print");
        let output = admin_output(&response).expect("empty success response");
        assert!(!output.contains("secret"));
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&output).expect("JSON"),
            serde_json::json!({"code": 0})
        );
        let response = Command::request(0).with_body(br#"{"table":{"topicCount":"2"}}"#.to_vec());
        assert!(
            admin_output(&response)
                .expect("JSON response")
                .contains('\n')
        );
        let response = Command::request(0).with_body(b"brokerName=broker-a\n".to_vec());
        assert_eq!(
            admin_output(&response).expect("UTF-8 response"),
            "brokerName=broker-a\n"
        );
        let response = Command::request(0).with_body(vec![0xff]);
        assert!(admin_output(&response).is_err());
    }
}
