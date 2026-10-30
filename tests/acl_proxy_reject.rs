//! ACL 全链路验证：开启 ACL 后“无凭证全拒、带正确凭证放行、错误凭证拒绝”。
//!
//! 对照 Java RocketMQ 的 PlainAccessValidator / AclSigner：
//! - gRPC 路径（Java `DefaultAuthenticationContextBuilder.build`）：签名内容是
//!   `x-mq-date-time` 头的原始字节，客户端签名经 hex 编码放在 `Authorization:
//!   MQv2-HMAC-SHA1 Credential=<ak>, SignedHeaders=..., Signature=<hex>` 中；
//!   服务端 hex->Base64 后与 `HMAC-SHA1(datetime, secretKey)` 恒时比对。
//! - remoting 路径（Java `AclUtils.combineRequestContent`）：签名内容是除
//!   `Signature` 外全部 ext 字段值按 key 排序拼接 + 请求 body。
//! - 用户来源（Java `plain_acl.yml` + `AuthMigrator`）：启动时从配置文件
//!   加载账户（accessKey/secretKey/admin/topicPerms/...）写入认证/授权元数据。
//!
//! 本测试覆盖三条真实链路：
//! 1. proxy 开 ACL 且未配置用户 -> 匿名请求全拒；关 ACL -> 放行。
//! 2. proxy 开 ACL 且加载 plain_acl 账户文件 -> 正确签名的 QueryRoute 放行，
//!    错误 secretKey 的签名被拒，缺 datetime 被拒。
//! 3. broker 开 ACL -> 无 AccessKey 的 remoting 命令被拒（SYSTEM_ERROR=1），
//!    带正确 AccessKey/Signature 的命令放行（code=0）。

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use rocketmq_rs::auth::{self, cal_signature, sign_v2_hex};
use rocketmq_rs::broker::{Broker, BrokerConfig};
use rocketmq_rs::common::TopicConfig;
use rocketmq_rs::namesrv::Namesrv;
use rocketmq_rs::proxy::pb::messaging_service_client::MessagingServiceClient;
use rocketmq_rs::proxy::pb::{QueryRouteRequest, Resource};
use rocketmq_rs::proxy::{ProxyConfig, ProxyContext, serve_proxy};
use rocketmq_rs::remoting;
use rocketmq_rs::store::Store;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::sync::watch;

const ACCESS_KEY: &str = "ak-admin";
const SECRET_KEY: &str = "sk-top-secret";
const DATETIME: &str = "20261030T120000Z";

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
    enable_acl: bool,
    acl_config_file: Option<String>,
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
                enable_acl,
                acl_config_file,
                ..BrokerConfig::default()
            },
        )
        .unwrap(),
    );
    // 启动期 bootstrap：与 main.rs 的 prepare_broker 一致。
    let accounts = broker.load_acl_accounts().await.unwrap();
    println!("broker ACL bootstrap: accounts={accounts}");
    let (shutdown, rx) = watch::channel(false);
    tokio::spawn(remoting::serve(listener, broker.clone(), rx));

    // 让 broker 在 namesrv 上注册，并预建一个主题，保证 ACL=off 时路由可正常解析。
    let _ = broker.store().put_topic(TopicConfig::new("TestTopic", 1));
    let reg = broker.registration().unwrap();
    let resp = remoting::invoke(&ns_addr.to_string(), reg, Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(
        resp.code, 0,
        "broker registration failed: {:?}",
        resp.remark
    );
    (broker, addr, shutdown)
}

async fn start_proxy(
    ns_addr: SocketAddr,
    enable_acl: bool,
    acl_config_file: Option<String>,
) -> (SocketAddr, watch::Sender<bool>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let mut config = ProxyConfig::default();
    config.name_server_address = vec![ns_addr.to_string()];
    config.enable_acl = enable_acl;
    config.acl_config_file = acl_config_file;
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

/// Java 客户端形态的 Authorization 头 + datetime（hex 签名）。
fn grpc_authorization(secret_key: &str) -> String {
    let signature = sign_v2_hex(DATETIME.as_bytes(), secret_key);
    format!(
        "MQv2-HMAC-SHA1 Credential={ACCESS_KEY}/{DATETIME}, \
         SignedHeaders=x-mq-date-time, Signature={signature}"
    )
}

fn write_plain_acl_file(dir: &TempDir) -> String {
    let path = dir.path().join("plain_acl.json");
    std::fs::write(
        &path,
        serde_json::json!({
            "accounts": [
                {
                    "accessKey": ACCESS_KEY,
                    "secretKey": SECRET_KEY,
                    "admin": true,
                    "defaultTopicPerm": "SUB",
                    "defaultGroupPerm": "SUB"
                }
            ]
        })
        .to_string(),
    )
    .unwrap();
    path.to_string_lossy().into_owned()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn acl_enabled_proxy_rejects_all_anonymous_requests() {
    let ns_dir = TempDir::new().unwrap();
    let broker_dir = TempDir::new().unwrap();
    let (_ns, ns_addr, ns_shutdown) = start_namesrv(&ns_dir).await;
    let (_broker, _broker_addr, broker_shutdown) =
        start_broker(&broker_dir, ns_addr, false, None).await;

    // ---- ACL = ON 且未配置用户：匿名请求必须被拒 ----
    let (acl_addr, acl_shutdown) = start_proxy(ns_addr, true, None).await;
    let mut client = connect_with_retry(acl_addr).await;
    let result = client.query_route(query_route_request()).await;
    let err = result
        .err()
        .expect("ACL-enabled proxy MUST reject anonymous requests");
    let msg = err.message().to_lowercase();
    println!("ACL=ON(无用户) QueryRoute -> gRPC error: {msg}");
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
    let (open_addr, open_shutdown) = start_proxy(ns_addr, false, None).await;
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

/// Java 5.x gRPC 客户端行为对照：plain_acl 账户加载后，正确签名放行，
/// 错误签名/缺失 datetime 拒绝。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn acl_with_plain_acl_file_allows_signed_grpc_requests() {
    let ns_dir = TempDir::new().unwrap();
    let broker_dir = TempDir::new().unwrap();
    let acl_dir = TempDir::new().unwrap();
    let (_ns, ns_addr, ns_shutdown) = start_namesrv(&ns_dir).await;
    let (_broker, _broker_addr, broker_shutdown) =
        start_broker(&broker_dir, ns_addr, false, None).await;
    let acl_file = write_plain_acl_file(&acl_dir);

    let (addr, shutdown) = start_proxy(ns_addr, true, Some(acl_file)).await;
    let mut client = connect_with_retry(addr).await;

    // ---- 正确签名：必须放行并返回真实路由 ----
    let mut request = tonic::Request::new(query_route_request());
    request.metadata_mut().insert(
        "authorization",
        grpc_authorization(SECRET_KEY).parse().unwrap(),
    );
    request
        .metadata_mut()
        .insert("x-mq-date-time", DATETIME.parse().unwrap());
    let resp = client
        .query_route(request)
        .await
        .expect("signed request MUST pass authentication");
    println!(
        "ACL=ON(有用户) 正确签名 QueryRoute -> OK, message_queues={}",
        resp.get_ref().message_queues.len()
    );

    // ---- 错误 secretKey：必须被拒 ----
    let mut request = tonic::Request::new(query_route_request());
    request.metadata_mut().insert(
        "authorization",
        grpc_authorization("wrong-secret").parse().unwrap(),
    );
    request
        .metadata_mut()
        .insert("x-mq-date-time", DATETIME.parse().unwrap());
    let err = client
        .query_route(request)
        .await
        .err()
        .expect("wrong secret MUST be rejected");
    println!(
        "ACL=ON 错误签名 QueryRoute -> gRPC error: {}",
        err.message()
    );
    assert!(err.message().contains("signature"));

    // ---- 缺 datetime（Java: "datetime is null."）：必须被拒 ----
    let mut request = tonic::Request::new(query_route_request());
    request.metadata_mut().insert(
        "authorization",
        grpc_authorization(SECRET_KEY).parse().unwrap(),
    );
    let err = client
        .query_route(request)
        .await
        .err()
        .expect("missing datetime MUST be rejected");
    println!(
        "ACL=ON 缺 datetime QueryRoute -> gRPC error: {}",
        err.message()
    );
    assert!(err.message().contains("datetime"));

    drop(shutdown);
    drop(ns_shutdown);
    drop(broker_shutdown);
}

/// broker 侧 ACL（Java AccessValidator）：无 AccessKey 拒绝（SYSTEM_ERROR=1），
/// 带正确 AccessKey/Signature 放行。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn broker_acl_rejects_unsigned_and_accepts_signed_commands() {
    let acl_dir = TempDir::new().unwrap();
    let acl_file = write_plain_acl_file(&acl_dir);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let store_dir = TempDir::new().unwrap();
    let store = Arc::new(Store::open(store_dir.path()).unwrap());
    let broker = Arc::new(
        Broker::new(
            store,
            BrokerConfig {
                advertised_addr: addr,
                auto_create_topic: true,
                enable_acl: true,
                acl_config_file: Some(acl_file),
                ..BrokerConfig::default()
            },
        )
        .unwrap(),
    );
    broker.load_acl_accounts().await.unwrap();
    let (shutdown, rx) = watch::channel(false);
    tokio::spawn(remoting::serve(listener, broker.clone(), rx));
    tokio::time::sleep(Duration::from_millis(100)).await;

    // ---- 无 AccessKey：必须被拒（code=1，remark 指向鉴权）----
    let request = remoting::Command::request(34).with_ext("clientID", "test-client");
    let resp = remoting::invoke(&addr.to_string(), request, Duration::from_secs(5))
        .await
        .unwrap();
    println!(
        "broker ACL 无凭据 heartbeat -> code={} remark={:?}",
        resp.code, resp.remark
    );
    assert_eq!(
        resp.code, 1,
        "unsigned command MUST be rejected with SYSTEM_ERROR"
    );
    let remark = resp.remark.clone().unwrap_or_default().to_lowercase();
    assert!(
        remark.contains("username") || remark.contains("signature") || remark.contains("accesskey"),
        "remark 应指向鉴权失败，实际: {remark}"
    );

    // ---- 带正确签名：必须放行（code=0）----
    // heartbeat 的业务 body 是 JSON；签名内容 = ext 值按 key 排序拼接 + body
    // （Java `AclUtils.combineRequestContent`）。
    let body = br#"{"clientID":"test-client"}"#.to_vec();
    let mut fields = BTreeMap::new();
    fields.insert("clientID".to_string(), "test-client".to_string());
    fields.insert(auth::ACCESS_KEY.to_string(), ACCESS_KEY.to_string());
    let mut content = String::new();
    for value in fields.values() {
        content.push_str(value);
    }
    let mut bytes = content.into_bytes();
    bytes.extend_from_slice(&body);
    let signature = cal_signature(&bytes, SECRET_KEY);
    fields.insert(auth::SIGNATURE.to_string(), signature);
    let mut request = remoting::Command::request(34);
    for (key, value) in &fields {
        request = request.with_ext(key.as_str(), value.as_str());
    }
    request = request.with_body(body);
    let resp = remoting::invoke(&addr.to_string(), request, Duration::from_secs(5))
        .await
        .unwrap();
    println!(
        "broker ACL 正确签名 heartbeat -> code={} remark={:?}",
        resp.code, resp.remark
    );
    assert_eq!(
        resp.code, 0,
        "signed command MUST pass, remark={:?}",
        resp.remark
    );

    // ---- 错误 secretKey：必须被拒 ----
    let body = br#"{"clientID":"test-client"}"#.to_vec();
    let mut fields = BTreeMap::new();
    fields.insert("clientID".to_string(), "test-client".to_string());
    fields.insert(auth::ACCESS_KEY.to_string(), ACCESS_KEY.to_string());
    let mut content = String::new();
    for value in fields.values() {
        content.push_str(value);
    }
    let mut bytes = content.into_bytes();
    bytes.extend_from_slice(&body);
    let signature = cal_signature(&bytes, "wrong-secret");
    fields.insert(auth::SIGNATURE.to_string(), signature);
    let mut request = remoting::Command::request(34);
    for (key, value) in &fields {
        request = request.with_ext(key.as_str(), value.as_str());
    }
    request = request.with_body(body);
    let resp = remoting::invoke(&addr.to_string(), request, Duration::from_secs(5))
        .await
        .unwrap();
    println!(
        "broker ACL 错误签名 heartbeat -> code={} remark={:?}",
        resp.code, resp.remark
    );
    assert_eq!(resp.code, 1, "wrong signature MUST be rejected");

    drop(shutdown);
}
