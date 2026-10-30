//! 真实复现：proxy 开启 ACL 后，未携带任何凭据的请求会被全部拒绝（"全拒"）。
//!
//! 对照 Java RocketMQ 的 PlainAccessValidator，本端口的 ACL 在 `enable_acl=true`
//! 时由 `proxy.rs` 的 `AclGuard` 在每条 gRPC RPC 入口执行 `check_access`：
//! 没有 `Authorization` 头 -> `UNAUTHENTICATED("missing Authorization metadata")`；
//! 即便带了凭据，因 `LocalAuthenticationMetadataProvider` 在启动时没有加载任何用户
//! （`register_user` 是 dead_code，无 plain_acl 文件加载），查不到用户 -> `not found`。
//!
//! 所以开启 ACL 但没配置用户 → 所有匿名请求一律被拒。本测试启动 namesrv + broker +
//! proxy（gRPC），分别用 `enableAcl=true` / `false` 跑同一发 QueryRoute，断言前者被拒、
//! 后者放行。

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use rocketmq_rs::broker::{Broker, BrokerConfig};
use rocketmq_rs::common::TopicConfig;
use rocketmq_rs::namesrv::Namesrv;
use rocketmq_rs::proxy::pb::messaging_service_client::MessagingServiceClient;
use rocketmq_rs::proxy::pb::{QueryRouteRequest, Resource};
use rocketmq_rs::proxy::{ProxyConfig, ProxyContext, serve_proxy};
use rocketmq_rs::remoting;
use rocketmq_rs::store::Store;
use tokio::net::TcpListener;
use tempfile::TempDir;
use tokio::sync::watch;

async fn start_namesrv(dir: &TempDir) -> (Arc<Namesrv>, SocketAddr, watch::Sender<bool>) {
    let ns = Arc::new(Namesrv::open(dir.path(), Duration::from_secs(120)).unwrap());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (shutdown, rx) = watch::channel(false);
    tokio::spawn(remoting::serve(listener, ns.clone(), rx));
    (ns, addr, shutdown)
}

async fn start_broker(
    dir: &TempDir,
    ns_addr: SocketAddr,
) -> (Arc<Broker>, SocketAddr, watch::Sender<bool>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let store = Arc::new(Store::open(dir.path()).unwrap());
    let broker = Arc::new(
        Broker::new(
            store,
            BrokerConfig {
                advertised_addr: addr,
                auto_create_topic: true,
                ..BrokerConfig::default()
            },
        )
        .unwrap(),
    );
    let (shutdown, rx) = watch::channel(false);
    tokio::spawn(remoting::serve(listener, broker.clone(), rx));

    // 让 broker 在 namesrv 上注册，并预建一个主题，保证 ACL=off 时路由可正常解析。
    let _ = broker
        .store()
        .put_topic(TopicConfig::new("TestTopic", 1));
    let reg = broker.registration().unwrap();
    let resp = remoting::invoke(&ns_addr.to_string(), reg, Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(resp.code, 0, "broker registration failed: {:?}", resp.remark);
    (broker, addr, shutdown)
}

async fn start_proxy(ns_addr: SocketAddr, enable_acl: bool) -> (SocketAddr, watch::Sender<bool>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let mut config = ProxyConfig::default();
    config.name_server_address = vec![ns_addr.to_string()];
    config.enable_acl = enable_acl;
    config.listen_port = addr.port();
    let context = ProxyContext::new(config, addr);
    let (shutdown, rx) = watch::channel(false);
    tokio::spawn(serve_proxy(listener, context, rx));
    // 等 gRPC server 绑定就绪。
    tokio::time::sleep(Duration::from_millis(300)).await;
    (addr, shutdown)
}

async fn connect_with_retry(addr: SocketAddr) -> MessagingServiceClient<tonic::transport::Channel> {
    for attempt in 0..20 {
        match MessagingServiceClient::connect(format!("http://{addr}")).await {
            Ok(client) => return client,
            Err(_) if attempt < 19 => tokio::time::sleep(Duration::from_millis(100)).await,
            Err(e) => panic!("proxy connect failed: {e}"),
        }
    }
    unreachable!()
}

fn query_route_request() -> QueryRouteRequest {
    QueryRouteRequest {
        topic: Some(Resource {
            name: "TestTopic".into(),
            ..Default::default()
        }),
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn acl_enabled_proxy_rejects_all_anonymous_requests() {
    let ns_dir = TempDir::new().unwrap();
    let broker_dir = TempDir::new().unwrap();
    let (_ns, ns_addr, ns_shutdown) = start_namesrv(&ns_dir).await;
    let (_broker, _broker_addr, broker_shutdown) = start_broker(&broker_dir, ns_addr).await;

    // ---- ACL = ON：匿名请求必须被拒 ----
    let (acl_addr, acl_shutdown) = start_proxy(ns_addr, true).await;
    let mut client = connect_with_retry(acl_addr).await;
    let result = client.query_route(query_route_request()).await;
    let err = result
        .err()
        .expect("ACL-enabled proxy MUST reject anonymous requests");
    let msg = err.message().to_lowercase();
    println!("ACL=ON QueryRoute -> gRPC error: {msg}");
    assert!(
        msg.contains("authorization")
            || msg.contains("not found")
            || msg.contains("unauthenticated")
            || msg.contains("credential")
            || msg.contains("signature"),
        "ACL=ON 拒绝原因应指向鉴权，实际: {msg}"
    );
    drop(acl_shutdown);

    // ---- ACL = OFF：同一请求应被正常处理（路由可解析，非 ACL 拒绝）----
    let (open_addr, open_shutdown) = start_proxy(ns_addr, false).await;
    let mut client = connect_with_retry(open_addr).await;
    let result = client.query_route(query_route_request()).await;
    match &result {
        Ok(resp) => {
            let mq_count = resp.get_ref().message_queues.len();
            println!("ACL=OFF QueryRoute -> OK, message_queues={mq_count}");
        }
        Err(e) => {
            let m = e.message().to_lowercase();
            println!("ACL=OFF QueryRoute -> non-ACL error (allowed): {m}");
            assert!(
                !m.contains("authorization")
                    && !m.contains("not found")
                    && !m.contains("unauthenticated")
                    && !m.contains("credential")
                    && !m.contains("signature"),
                "ACL=OFF 不应出现鉴权类拒绝，实际: {m}"
            );
        }
    }
    drop(open_shutdown);

    drop(ns_shutdown);
    drop(broker_shutdown);
}
