//! TLS 端到端：真实自签证书 + 真实 TLS 握手。
//!
//! 覆盖两条启动链路（此前 `serve_tls` / `invoke_tls` / tonic TLS 从未被任何
//! 启动路径或测试使用）：
//! 1. broker remoting over TLS：`serve_tls` 起真实 broker，`invoke_tls` 走
//!    完整握手发心跳，断言业务响应（code=0）。
//! 2. proxy gRPC over TLS：`serve_proxy_tls`（tonic `ServerTlsConfig`）起真实
//!    gRPC proxy，tonic 客户端带 CA 证书（`ClientTlsConfig`）调 QueryRoute。

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Unique file stem per call — tests run in parallel in one process and
/// same-nanosecond timestamps would collide, overwriting each other's PEMs.
static FILE_SEQ: AtomicU64 = AtomicU64::new(0);

fn unique_stem(tag: &str) -> String {
    format!(
        "rmq-tls-{tag}-{}-{}",
        std::process::id(),
        FILE_SEQ.fetch_add(1, Ordering::Relaxed)
    )
}

use rocketmq_rs::broker::{Broker, BrokerConfig};
use rocketmq_rs::common::TopicConfig;
use rocketmq_rs::namesrv::Namesrv;
use rocketmq_rs::proxy::pb::messaging_service_client::MessagingServiceClient;
use rocketmq_rs::proxy::pb::{QueryRouteRequest, Resource};
use rocketmq_rs::proxy::{ProxyConfig, ProxyContext, serve_proxy_tls};
use rocketmq_rs::remoting::{self, TlsConfig};
use rocketmq_rs::store::Store;
use tokio::net::TcpListener;
use tokio::sync::watch;

/// Generate a self-signed certificate valid for localhost/127.0.0.1, write it
/// to temp files, and return (cert_pem, key_pem, cert_path, key_path).
fn generate_cert() -> (String, String, String, String) {
    let cert =
        rcgen::generate_simple_self_signed(vec!["localhost".to_string(), "127.0.0.1".into()])
            .expect("rcgen self-signed cert");
    let cert_pem = cert.cert.pem();
    let key_pem = cert.key_pair.serialize_pem();
    let stem = unique_stem("cert");
    let cert_path = std::env::temp_dir().join(format!("{stem}.crt"));
    let key_path = std::env::temp_dir().join(format!("{stem}.key"));
    std::fs::write(&cert_path, &cert_pem).unwrap();
    std::fs::write(&key_path, &key_pem).unwrap();
    (
        cert_pem,
        key_pem,
        cert_path.to_string_lossy().into_owned(),
        key_path.to_string_lossy().into_owned(),
    )
}

fn tls_config(cert_path: &str, key_path: &str) -> TlsConfig {
    TlsConfig {
        mode: remoting::TlsMode::Enforced,
        server_cert_path: Some(cert_path.to_string()),
        server_key_path: Some(key_path.to_string()),
        ..TlsConfig::default()
    }
}

/// Client-side config: trust the self-signed server certificate strictly —
/// exercise the real webpki verifier, not the insecure test-mode escape hatch.
fn client_tls_config(cert_pem: &str) -> TlsConfig {
    let path = std::env::temp_dir().join(format!("{}.pem", unique_stem("trust")));
    std::fs::write(&path, cert_pem).unwrap();
    TlsConfig {
        mode: remoting::TlsMode::Enforced,
        auth_server: true,
        test_mode_enabled: false,
        client_trust_cert_path: Some(path.to_string_lossy().into_owned()),
        ..TlsConfig::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn broker_remoting_works_over_tls() {
    let (cert_pem, _key_pem, cert_path, key_path) = generate_cert();
    let dir = tempfile::tempdir().unwrap();
    let store = Arc::new(Store::open(dir.path()).unwrap());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
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
    let server = tokio::spawn(remoting::serve_tls(
        listener,
        broker.clone(),
        rx,
        tls_config(&cert_path, &key_path),
    ));
    tokio::time::sleep(Duration::from_millis(200)).await;
    if server.is_finished() {
        let outcome = server.await;
        panic!("serve_tls task exited early: {outcome:?}");
    }

    // Full TLS handshake + a real broker RPC (heartbeat) via invoke_tls.
    let client_tls = client_tls_config(&cert_pem);
    let request = remoting::Command::request(34)
        .with_ext("clientID", "tls-probe")
        .with_body(br#"{"clientID":"tls-probe"}"#.to_vec());
    let response = remoting::invoke_tls(
        &addr.to_string(),
        request,
        Duration::from_secs(5),
        client_tls,
    )
    .await
    .expect("invoke_tls over the broker must succeed");
    assert_eq!(response.code, 0, "heartbeat over TLS must succeed");
    println!("broker heartbeat over TLS -> code=0 OK");

    drop(shutdown);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn proxy_grpc_works_over_tls() {
    let (cert_pem, _key_pem, cert_path, key_path) = generate_cert();
    let ns_dir = tempfile::tempdir().unwrap();
    let broker_dir = tempfile::tempdir().unwrap();

    // Minimal namesrv + broker + topic so QueryRoute resolves.
    let ns = Arc::new(Namesrv::open(ns_dir.path(), Duration::from_secs(120)).unwrap());
    let ns_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let ns_addr = ns_listener.local_addr().unwrap();
    let (ns_shutdown, ns_rx) = watch::channel(false);
    tokio::spawn(remoting::serve(ns_listener, ns.clone(), ns_rx));

    let store = Arc::new(Store::open(broker_dir.path()).unwrap());
    let broker_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let broker_addr = broker_listener.local_addr().unwrap();
    let broker = Arc::new(
        Broker::new(
            store,
            BrokerConfig {
                advertised_addr: broker_addr,
                auto_create_topic: true,
                ..BrokerConfig::default()
            },
        )
        .unwrap(),
    );
    broker
        .store()
        .put_topic(TopicConfig::new("TlsTopic", 1))
        .ok();
    let (broker_shutdown, broker_rx) = watch::channel(false);
    tokio::spawn(remoting::serve(broker_listener, broker.clone(), broker_rx));
    let reg = broker.registration().unwrap();
    let resp = remoting::invoke(&ns_addr.to_string(), reg, Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(resp.code, 0, "registration failed");

    // gRPC proxy over TLS.
    let proxy_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_addr = proxy_listener.local_addr().unwrap();
    let config = ProxyConfig {
        name_server_address: vec![ns_addr.to_string()],
        listen_port: proxy_addr.port(),
        ..ProxyConfig::default()
    };
    let context = ProxyContext::new(config, proxy_addr);
    let (proxy_shutdown, proxy_rx) = watch::channel(false);
    tokio::spawn(serve_proxy_tls(
        proxy_listener,
        context,
        proxy_rx,
        Some(tls_config(&cert_path, &key_path)),
    ));
    tokio::time::sleep(Duration::from_millis(300)).await;

    // tonic client with the self-signed cert as its only trust anchor.
    let _ = rustls::crypto::ring::default_provider().install_default();
    let ca = tonic::transport::Certificate::from_pem(cert_pem);
    let channel = tonic::transport::Endpoint::from_shared(format!("https://{proxy_addr}"))
        .unwrap()
        .tls_config(
            tonic::transport::ClientTlsConfig::new()
                .ca_certificate(ca)
                .domain_name("localhost"),
        )
        .unwrap()
        .connect()
        .await
        .expect("TLS channel to the gRPC proxy must connect");
    let mut client = MessagingServiceClient::new(channel);
    let response = client
        .query_route(QueryRouteRequest {
            topic: Some(Resource {
                name: "TlsTopic".into(),
                ..Default::default()
            }),
            ..Default::default()
        })
        .await
        .expect("QueryRoute over TLS must succeed");
    println!(
        "gRPC QueryRoute over TLS -> OK, message_queues={}",
        response.get_ref().message_queues.len()
    );

    drop(proxy_shutdown);
    drop(broker_shutdown);
    drop(ns_shutdown);
}
