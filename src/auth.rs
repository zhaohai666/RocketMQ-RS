//! Faithful Rust reimplementation of the Apache RocketMQ server-side `auth` module.
//!
//! This is a port of the Java package `org.apache.rocketmq.auth` (the authentication +
//! ACL authorization engine). It covers, in priority order:
//!
//!   * `AuthConfig` (config with serde + Java defaults)
//!   * Authorization model (`Resource`, `Acl`, `Policy`, `PolicyEntry`, `Environment`,
//!     `PolicyType`, `Decision`; the Java `RequestContext` is modeled by
//!     `AuthorizationContext`)
//!   * `AuthorizationEvaluator` + `DefaultAuthorizationProvider` + chain handlers
//!     (`UserAuthorizationHandler`, `AclAuthorizationHandler`) + in-memory metadata
//!     provider/manager implementing the policy-matching algorithm
//!   * `DefaultAuthorizationContextBuilder` (RemotingCommand -> request contexts)
//!   * Authentication (`AuthenticationEvaluator`, `DefaultAuthenticationProvider`,
//!     `DefaultAuthenticationHandler`, signature verification via HMAC-SHA1 + Base64)
//!   * `PlainAccessConfig` parsing + `AuthMigrator`
//!   * `IPAddressUtils` parity: IPv4 + IPv6 (incl. `::`-abbreviated forms and
//!     CIDR prefixes up to /128) range matching + validators
//!   * `AuditLogger` (tracing-structured who/when/action/resource/decision/srcIp)
//!   * gRPC v2 header mapping: `context_from_grpc_headers`, `sign_v2`,
//!     `canonical_v2_content`, `hex_to_base64` (Java `MQv2-HMAC-SHA1` scheme)
//!   * Pluggable `AuthorizationStrategyProvider` + `TtlCache` (subject -> ACL,
//!     config `cache_ttl_seconds`) replacing the Java caffeine caches
//!
//! References to Java equivalents are noted with `// Java: Class.method` comments.
//! The file is intentionally self-contained; it only depends on
//! `crate::common::{Error, Result, now_millis}` and the already-declared crates
//! (serde, serde_json, thiserror, tokio, tracing, sha1, hmac, std).

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::common::{Error, Result, now_millis};
use crate::remoting::Command;

// ---------------------------------------------------------------------------
// Wire / protocol constants used by the request context builders.
// Java: SessionCredentials / CommonConstants / AclConstants / MixAll.
// ---------------------------------------------------------------------------

pub const ACCESS_KEY: &str = "AccessKey";
pub const SIGNATURE: &str = "Signature";
pub const SECRET_KEY: &str = "SecretKey";

pub const COLON: &str = ":";
pub const ASTERISK: &str = "*";
pub const EQUAL: &str = "=";

pub const RETRY_GROUP_TOPIC_PREFIX: &str = "%RETRY%";
pub const DLQ_GROUP_TOPIC_PREFIX: &str = "%DLQ%";

// Java: AclConstants permission strings.
pub const PERM_PUB: &str = "PUB";
pub const PERM_SUB: &str = "SUB";
pub const PERM_PUB_SUB: &str = "PUB|SUB";
pub const PERM_SUB_PUB: &str = "SUB|PUB";
pub const PERM_DENY: &str = "DENY";

// A representative subset of RocketMQ request codes used by the RemotingCommand
// authorization context builder (`DefaultAuthorizationContextBuilder`).
// Java: org.apache.rocketmq.remoting.protocol.RequestCode.
pub mod request_code {
    pub const SEND_MESSAGE: i32 = 10;
    pub const PULL_MESSAGE: i32 = 11;
    pub const QUERY_MESSAGE: i32 = 12;
    pub const VIEW_MESSAGE_BY_ID: i32 = 14;
    pub const HEART_BEAT: i32 = 21;
    pub const UNREGISTER_CLIENT: i32 = 35;
    pub const CONSUMER_SEND_MSG_BACK: i32 = 36;
    pub const END_TRANSACTION: i32 = 37;
    pub const GET_CONSUMER_LIST_BY_GROUP: i32 = 38;
    pub const UPDATE_CONSUMER_OFFSET: i32 = 44;
    pub const QUERY_CONSUMER_OFFSET: i32 = 43;
    pub const GET_ROUTEINFO_BY_TOPIC: i32 = 105;
    pub const UPDATE_BROKER_CONFIG: i32 = 25;
    pub const GET_BROKER_CONFIG: i32 = 26;
    pub const SEND_MESSAGE_V2: i32 = 310;
    pub const SEND_BATCH_MESSAGE: i32 = 320;
    pub const SEND_REPLY_MESSAGE: i32 = 323;
    pub const SEND_REPLY_MESSAGE_V2: i32 = 324;
    pub const RECALL_MESSAGE: i32 = 341;
    pub const LITE_PULL_MESSAGE: i32 = 371;
    pub const LITE_SUBSCRIPTION_CTL: i32 = 369;
}

// ---------------------------------------------------------------------------
// Small standard Base64 encoder/decoder (no extra crate dependency).
// Java: org.apache.commons.codec.binary.Base64 (default alphabet, padding).
// ---------------------------------------------------------------------------

const BASE64_ALPHABET: &[u8; 64] =
    b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

/// Standard Base64 encoding (RFC 4648) with `=` padding.
pub fn base64_encode(input: &[u8]) -> String {
    let mut out = String::with_capacity((input.len() + 2) / 3 * 4);
    for chunk in input.chunks(3) {
        let b0 = chunk[0];
        let b1 = *chunk.get(1).unwrap_or(&0);
        let b2 = *chunk.get(2).unwrap_or(&0);
        let n = ((b0 as u32) << 16) | ((b1 as u32) << 8) | (b2 as u32);
        out.push(BASE64_ALPHABET[((n >> 18) & 63) as usize] as char);
        out.push(BASE64_ALPHABET[((n >> 12) & 63) as usize] as char);
        out.push(if chunk.len() > 1 {
            BASE64_ALPHABET[((n >> 6) & 63) as usize] as char
        } else {
            '='
        });
        out.push(if chunk.len() > 2 {
            BASE64_ALPHABET[(n & 63) as usize] as char
        } else {
            '='
        });
    }
    out
}

/// Standard Base64 decoding (RFC 4648). Rejects invalid input.
pub fn base64_decode(input: &str) -> Result<Vec<u8>> {
    let mut lookup = [255u8; 256];
    for (i, &c) in BASE64_ALPHABET.iter().enumerate() {
        lookup[c as usize] = i as u8;
    }
    let bytes = input.as_bytes();
    let mut out = Vec::with_capacity(bytes.len() / 4 * 3);
    let mut i = 0;
    while i < bytes.len() {
        let mut buf = [0u32; 4];
        let mut count = 0;
        while count < 4 && i < bytes.len() {
            let c = bytes[i];
            if c == b'=' || c == b' ' || c == b'\n' || c == b'\r' || c == b'\t' {
                if c == b'=' {
                    buf[count] = 0;
                }
                i += 1;
                // padding starts; stop accumulating
                if c == b'=' {
                    break;
                }
                continue;
            }
            let v = lookup[c as usize];
            if v == 255 {
                return Err(Error::Invalid(format!("invalid base64 char: {c}")));
            }
            buf[count] = u32::from(v);
            count += 1;
            i += 1;
        }
        if count == 0 {
            break;
        }
        let n = (buf[0] << 18) | (buf[1] << 12) | (buf[2] << 6) | buf[3];
        if count >= 2 {
            out.push((n >> 16) as u8);
        }
        if count >= 3 {
            out.push((n >> 8) as u8);
        }
        if count >= 4 {
            out.push(n as u8);
        }
        if count < 4 {
            break;
        }
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// HMAC-SHA1 signature. Java: org.apache.rocketmq.acl.common.AclSigner.
// ---------------------------------------------------------------------------

use hmac::{Hmac, Mac};
use sha1::Sha1;

type HmacSha1 = Hmac<Sha1>;

/// Compute the RocketMQ request signature: HMAC-SHA1 over `data` with `key`, Base64 encoded.
pub fn cal_signature(data: &[u8], key: &str) -> String {
    let mut mac =
        HmacSha1::new_from_slice(key.as_bytes()).expect("HMAC accepts keys of any length");
    mac.update(data);
    let result = mac.finalize();
    base64_encode(&result.into_bytes())
}

/// Constant-time string comparison (mirrors Java `MessageDigest.isEqual`).
fn ct_eq(a: &str, b: &str) -> bool {
    let (ab, bb) = (a.as_bytes(), b.as_bytes());
    if ab.len() != bb.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in ab.iter().zip(bb.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

// ---------------------------------------------------------------------------
// gRPC / v2 protocol authentication helpers.
// Java: DefaultAuthenticationContextBuilder (gRPC path) + GrpcConstants.
// The v2 wire scheme is `MQv2-HMAC-SHA1`; the `Authorization` metadata header
// carries `MQv2-HMAC-SHA1 Credential=<user>[/<datetime>], SignedHeaders=...,
// Signature=<hex(HMAC-SHA1)>` and `x-mq-date-time` carries the signed datetime.
// ---------------------------------------------------------------------------

/// Wire scheme identifier of the v2 gRPC authentication signature.
pub const MQV2_HMAC_SHA1: &str = "MQv2-HMAC-SHA1";

/// Lowercased gRPC metadata header names. Java: GrpcConstants.
pub mod grpc_header {
    pub const AUTHORIZATION: &str = "authorization";
    pub const DATE_TIME: &str = "x-mq-date-time";
    pub const CLIENT_ID: &str = "x-mq-client-id";
    pub const CHANNEL_ID: &str = "x-mq-channel-id";
    pub const RPC_NAME: &str = "x-mq-rpc-name";
    /// Java: GrpcConstants.REMOTE_ADDRESS (`rpc-remote-address`).
    pub const REMOTE_ADDRESS: &str = "rpc-remote-address";
    /// Alternative transport-injected remote address key.
    pub const X_REMOTE_ADDRESS: &str = "x-mq-remote-address";
}

/// Hex decode (Java: `org.apache.commons.codec.binary.Hex.decodeHex`).
pub fn hex_decode(input: &str) -> Result<Vec<u8>> {
    let hex = input.trim();
    if hex.len() % 2 != 0 {
        return Err(Error::Invalid(format!(
            "odd hex string length: {}",
            hex.len()
        )));
    }
    let mut out = Vec::with_capacity(hex.len() / 2);
    let bytes = hex.as_bytes();
    for pair in bytes.chunks(2) {
        let hi = (pair[0] as char)
            .to_digit(16)
            .ok_or_else(|| Error::Invalid(format!("invalid hex char: {}", pair[0] as char)))?;
        let lo = (pair[1] as char)
            .to_digit(16)
            .ok_or_else(|| Error::Invalid(format!("invalid hex char: {}", pair[1] as char)))?;
        out.push(((hi << 4) | lo) as u8);
    }
    Ok(out)
}

/// Hex encode, lowercase (Java: `Hex.encodeHexString`).
pub fn hex_encode(input: &[u8]) -> String {
    let mut out = String::with_capacity(input.len() * 2);
    for b in input {
        out.push(char::from_digit((b >> 4) as u32, 16).unwrap_or('0'));
        out.push(char::from_digit((b & 0xf) as u32, 16).unwrap_or('0'));
    }
    out
}

/// Java: DefaultAuthenticationContextBuilder.hexToBase64 — the v2 wire header
/// carries the signature hex-encoded while verification compares Base64.
pub fn hex_to_base64(input: &str) -> Result<String> {
    Ok(base64_encode(&hex_decode(input)?))
}

/// Compute the v2 (`MQv2-HMAC-SHA1`) signature: HMAC-SHA1 over the canonical
/// request content with the secret key, Base64 encoded. The canonical content
/// is produced by [`canonical_v2_content`].
pub fn sign_v2(data: &[u8], key: &str) -> String {
    cal_signature(data, key)
}

/// Hex-encoded form of the v2 signature (what goes into the `Signature=`
/// item of the `Authorization` header).
pub fn sign_v2_hex(data: &[u8], key: &str) -> String {
    let mut mac =
        HmacSha1::new_from_slice(key.as_bytes()).expect("HMAC accepts keys of any length");
    mac.update(data);
    hex_encode(&mac.finalize().into_bytes())
}

/// Build the v2 canonical signing content: headers sorted by name, each
/// rendered as `name:value\n` and concatenated.
pub fn canonical_v2_content(headers: &HashMap<String, String>) -> Vec<u8> {
    let mut names: Vec<&String> = headers.keys().collect();
    names.sort();
    let mut content = String::new();
    for name in names {
        content.push_str(name);
        content.push(':');
        content.push_str(headers[name].as_str());
        content.push('\n');
    }
    content.into_bytes()
}

// ---------------------------------------------------------------------------
// Enums.
// ---------------------------------------------------------------------------

/// Java: authentication.enums.SubjectType.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum SubjectType {
    User,
}

impl SubjectType {
    pub fn name(&self) -> &'static str {
        match self {
            SubjectType::User => "User",
        }
    }
    pub fn from_name(name: &str) -> Option<SubjectType> {
        match name.eq_ignore_ascii_case("User") {
            true => Some(SubjectType::User),
            false => None,
        }
    }
}

/// Java: authentication.enums.UserType.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum UserType {
    #[default]
    Normal,
    Super,
}

impl UserType {
    pub fn name(&self) -> &'static str {
        match self {
            UserType::Normal => "Normal",
            UserType::Super => "Super",
        }
    }
    pub fn from_name(name: &str) -> Option<UserType> {
        match name.eq_ignore_ascii_case("Super") {
            true => Some(UserType::Super),
            false => match name.eq_ignore_ascii_case("Normal") {
                true => Some(UserType::Normal),
                false => None,
            },
        }
    }
}

/// Java: authentication.enums.UserStatus.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum UserStatus {
    #[default]
    Enable,
    Disable,
}

impl UserStatus {
    pub fn name(&self) -> &'static str {
        match self {
            UserStatus::Enable => "enable",
            UserStatus::Disable => "disable",
        }
    }
    pub fn from_name(name: &str) -> Option<UserStatus> {
        match name.eq_ignore_ascii_case("disable") {
            true => Some(UserStatus::Disable),
            false => match name.eq_ignore_ascii_case("enable") {
                true => Some(UserStatus::Enable),
                false => None,
            },
        }
    }
}

/// Java: common.resource.ResourceType.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ResourceType {
    Any,
    Cluster,
    Topic,
    Group,
}

impl ResourceType {
    pub fn name(&self) -> &'static str {
        match self {
            ResourceType::Any => "Any",
            ResourceType::Cluster => "Cluster",
            ResourceType::Topic => "Topic",
            ResourceType::Group => "Group",
        }
    }
    pub fn from_name(name: &str) -> Option<ResourceType> {
        match name.eq_ignore_ascii_case("Any") {
            true => Some(ResourceType::Any),
            false => match name.eq_ignore_ascii_case("Cluster") {
                true => Some(ResourceType::Cluster),
                false => match name.eq_ignore_ascii_case("Topic") {
                    true => Some(ResourceType::Topic),
                    false => match name.eq_ignore_ascii_case("Group") {
                        true => Some(ResourceType::Group),
                        false => None,
                    },
                },
            },
        }
    }
}

/// Java: common.resource.ResourcePattern.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
pub enum ResourcePattern {
    Any,
    Literal,
    Prefixed,
}

impl ResourcePattern {
    pub fn name(&self) -> &'static str {
        match self {
            ResourcePattern::Any => "ANY",
            ResourcePattern::Literal => "LITERAL",
            ResourcePattern::Prefixed => "PREFIXED",
        }
    }
}

/// Java: common.action.Action.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Action {
    Unknown,
    All,
    Any,
    Pub,
    Sub,
    Create,
    Update,
    Delete,
    Get,
    List,
}

impl Action {
    pub fn name(&self) -> &'static str {
        match self {
            Action::Unknown => "Unknown",
            Action::All => "All",
            Action::Any => "Any",
            Action::Pub => "Pub",
            Action::Sub => "Sub",
            Action::Create => "Create",
            Action::Update => "Update",
            Action::Delete => "Delete",
            Action::Get => "Get",
            Action::List => "List",
        }
    }
    pub fn from_name(name: &str) -> Option<Action> {
        let n = name.trim();
        if n.eq_ignore_ascii_case("All") {
            Some(Action::All)
        } else if n.eq_ignore_ascii_case("Any") {
            Some(Action::Any)
        } else if n.eq_ignore_ascii_case("Pub") {
            Some(Action::Pub)
        } else if n.eq_ignore_ascii_case("Sub") {
            Some(Action::Sub)
        } else if n.eq_ignore_ascii_case("Create") {
            Some(Action::Create)
        } else if n.eq_ignore_ascii_case("Update") {
            Some(Action::Update)
        } else if n.eq_ignore_ascii_case("Delete") {
            Some(Action::Delete)
        } else if n.eq_ignore_ascii_case("Get") {
            Some(Action::Get)
        } else if n.eq_ignore_ascii_case("List") {
            Some(Action::List)
        } else {
            None
        }
    }
}

/// Java: authorization.enums.Decision.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Decision {
    Allow,
    Deny,
}

impl Decision {
    pub fn name(&self) -> &'static str {
        match self {
            Decision::Allow => "Allow",
            Decision::Deny => "Deny",
        }
    }
    pub fn from_name(name: &str) -> Option<Decision> {
        match name.eq_ignore_ascii_case("Deny") {
            true => Some(Decision::Deny),
            false => match name.eq_ignore_ascii_case("Allow") {
                true => Some(Decision::Allow),
                false => None,
            },
        }
    }
}

/// Java: authorization.enums.PolicyType.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum PolicyType {
    Custom,
    Default,
}

impl PolicyType {
    pub fn name(&self) -> &'static str {
        match self {
            PolicyType::Custom => "Custom",
            PolicyType::Default => "Default",
        }
    }
    pub fn from_name(name: &str) -> Option<PolicyType> {
        match name.eq_ignore_ascii_case("Default") {
            true => Some(PolicyType::Default),
            false => match name.eq_ignore_ascii_case("Custom") {
                true => Some(PolicyType::Custom),
                false => None,
            },
        }
    }
}

// ---------------------------------------------------------------------------
// AuthConfig. Java: config.AuthConfig.
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct AuthConfig {
    pub config_name: String,
    pub cluster_name: String,
    pub auth_config_path: String,

    pub authentication_enabled: bool,
    pub authentication_provider: String,
    pub authentication_metadata_provider: String,
    pub authentication_strategy: String,
    pub authentication_whitelist: Vec<String>,
    pub init_authentication_user: String,
    pub inner_client_authentication_credentials: String,

    pub authorization_enabled: bool,
    pub authorization_provider: String,
    pub authorization_metadata_provider: String,
    pub authorization_strategy: String,
    pub authorization_whitelist: Vec<String>,

    pub migrate_auth_from_v1_enabled: bool,

    /// Master switch for the authorization audit log. Java: the auth audit
    /// logger (`ROCKETMQ_AUTH_AUDIT_LOGGER_NAME`) is always wired up; here it
    /// is config-gated and defaults to `true`.
    pub audit_enabled: bool,
    /// TTL (seconds) of the in-process subject -> policy resolution cache.
    /// Mirrors the caffeine caches used by the Java metadata managers.
    pub cache_ttl_seconds: u64,

    pub user_cache_max_num: usize,
    pub user_cache_expired_second: u64,
    pub user_cache_refresh_second: u64,
    pub acl_cache_max_num: usize,
    pub acl_cache_expired_second: u64,
    pub acl_cache_refresh_second: u64,
    pub stateful_authentication_cache_max_num: usize,
    pub stateful_authentication_cache_expired_second: u64,
    pub stateful_authorization_cache_max_num: usize,
    pub stateful_authorization_cache_expired_second: u64,
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self {
            config_name: String::new(),
            cluster_name: String::new(),
            auth_config_path: String::new(),
            authentication_enabled: false,
            authentication_provider: String::new(),
            authentication_metadata_provider: String::new(),
            authentication_strategy: String::new(),
            authentication_whitelist: Vec::new(),
            init_authentication_user: String::new(),
            inner_client_authentication_credentials: String::new(),
            authorization_enabled: false,
            authorization_provider: String::new(),
            authorization_metadata_provider: String::new(),
            authorization_strategy: String::new(),
            authorization_whitelist: Vec::new(),
            migrate_auth_from_v1_enabled: false,
            audit_enabled: true,
            cache_ttl_seconds: 30,
            user_cache_max_num: 1000,
            user_cache_expired_second: 600,
            user_cache_refresh_second: 60,
            acl_cache_max_num: 1000,
            acl_cache_expired_second: 600,
            acl_cache_refresh_second: 60,
            stateful_authentication_cache_max_num: 10000,
            stateful_authentication_cache_expired_second: 60,
            stateful_authorization_cache_max_num: 10000,
            stateful_authorization_cache_expired_second: 60,
        }
    }
}

impl AuthConfig {
    /// Java: AuthConfig.isAuthenticationRequired(rpcCode).
    pub fn is_authentication_required(&self, rpc_code: &str) -> bool {
        self.authentication_enabled && !self.authentication_whitelist.iter().any(|c| c == rpc_code)
    }
}

// ---------------------------------------------------------------------------
// Authentication model: Subject + User.
// Java: authentication.model.{Subject,User}.
// ---------------------------------------------------------------------------

/// Java: authentication.model.User.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct User {
    pub username: String,
    #[serde(default)]
    pub password: Option<String>,
    #[serde(default)]
    pub user_type: UserType,
    #[serde(default)]
    pub user_status: UserStatus,
}

impl Default for User {
    fn default() -> Self {
        Self {
            username: String::new(),
            password: None,
            user_type: UserType::Normal,
            user_status: UserStatus::Enable,
        }
    }
}

impl User {
    pub fn new(username: impl Into<String>) -> Self {
        Self {
            username: username.into(),
            password: None,
            user_type: UserType::Normal,
            user_status: UserStatus::Enable,
        }
    }

    pub fn with_secret(mut self, secret: impl Into<String>) -> Self {
        self.password = Some(secret.into());
        self
    }

    pub fn with_type(mut self, user_type: UserType) -> Self {
        self.user_type = user_type;
        self
    }

    pub fn with_status(mut self, user_status: UserStatus) -> Self {
        self.user_status = user_status;
        self
    }
}

/// Java: authentication.model.Subject (interface; only the USER kind is supported here).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "subjectType", rename_all = "PascalCase")]
pub enum Subject {
    User(User),
}

impl Subject {
    /// Java: Subject.of(subjectKey) -> "User:username".
    pub fn of(subject_key: &str) -> Result<Subject> {
        let (typ, name) = subject_key
            .split_once(COLON)
            .ok_or_else(|| Error::Invalid(format!("invalid subject key: {subject_key}")))?;
        match SubjectType::from_name(typ) {
            Some(SubjectType::User) => Ok(Subject::User(User::new(name.to_string()))),
            None => Err(Error::Invalid(format!("unknown subject type: {typ}"))),
        }
    }

    /// Java: Subject.getSubjectKey().
    pub fn subject_key(&self) -> String {
        match self {
            Subject::User(u) => format!("{}{}{}", SubjectType::User.name(), COLON, u.username),
        }
    }

    pub fn subject_type(&self) -> SubjectType {
        SubjectType::User
    }

    pub fn is_subject(&self, subject_type: SubjectType) -> bool {
        self.subject_type() == subject_type
    }

    /// Convenience accessor for the embedded username (USER subjects only).
    pub fn username(&self) -> String {
        match self {
            Subject::User(u) => u.username.clone(),
        }
    }
}

// ---------------------------------------------------------------------------
// Authorization model.
// Java: authorization.model.{Resource,Environment,PolicyEntry,Policy,Acl}.
// ---------------------------------------------------------------------------

/// Java: authorization.model.Resource.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct Resource {
    pub resource_type: ResourceType,
    pub resource_name: Option<String>,
    pub resource_pattern: ResourcePattern,
}

impl Default for Resource {
    fn default() -> Self {
        Self {
            resource_type: ResourceType::Any,
            resource_name: None,
            resource_pattern: ResourcePattern::Any,
        }
    }
}

impl Resource {
    pub fn of(
        resource_type: ResourceType,
        resource_name: Option<String>,
        resource_pattern: ResourcePattern,
    ) -> Self {
        Self {
            resource_type,
            resource_name,
            resource_pattern,
        }
    }

    pub fn of_cluster(name: &str) -> Self {
        Self::of(
            ResourceType::Cluster,
            Some(name.to_string()),
            ResourcePattern::Literal,
        )
    }

    pub fn of_topic(name: &str) -> Self {
        Self::of(
            ResourceType::Topic,
            Some(name.to_string()),
            ResourcePattern::Literal,
        )
    }

    /// Java: Resource.ofGroup() strips the retry/DLQ prefix before storing the group name.
    pub fn of_group(name: &str) -> Self {
        let stripped = if is_retry_topic(name) || is_dlq_topic(name) {
            with_out_retry_and_dlq(name)
        } else {
            name.to_string()
        };
        Self::of(
            ResourceType::Group,
            Some(stripped),
            ResourcePattern::Literal,
        )
    }

    pub fn of_any(any_type: ResourceType) -> Self {
        Self::of(any_type, None, ResourcePattern::Any)
    }

    /// Java: Resource.of(resourceKey) parsing "Type:name", "Type:*", "Type:prefix*".
    pub fn from_key(key: &str) -> Option<Resource> {
        if key == ASTERISK {
            return Some(Resource::of(ResourceType::Any, None, ResourcePattern::Any));
        }
        let (typ, name) = key.split_once(COLON)?;
        let resource_type = ResourceType::from_name(typ)?;
        if name == ASTERISK {
            return Some(Resource::of(resource_type, None, ResourcePattern::Any));
        }
        if let Some(prefix) = name.strip_suffix(ASTERISK) {
            return Some(Resource::of(
                resource_type,
                Some(prefix.to_string()),
                ResourcePattern::Prefixed,
            ));
        }
        Some(Resource::of(
            resource_type,
            Some(name.to_string()),
            ResourcePattern::Literal,
        ))
    }

    /// Java: Resource.getResourceKey().
    pub fn resource_key(&self) -> String {
        match self.resource_type {
            ResourceType::Any => ASTERISK.to_string(),
            _ => match self.resource_pattern {
                ResourcePattern::Any => {
                    format!("{}{}{}", self.resource_type.name(), COLON, ASTERISK)
                }
                ResourcePattern::Literal => format!(
                    "{}{}{}",
                    self.resource_type.name(),
                    COLON,
                    self.resource_name.as_deref().unwrap_or("")
                ),
                ResourcePattern::Prefixed => format!(
                    "{}{}{}{}",
                    self.resource_type.name(),
                    COLON,
                    self.resource_name.as_deref().unwrap_or(""),
                    ASTERISK
                ),
            },
        }
    }

    /// Java: Resource.isMatch(resource).
    pub fn is_match(&self, other: &Resource) -> bool {
        if self.resource_type == ResourceType::Any {
            return true;
        }
        if self.resource_type != other.resource_type {
            return false;
        }
        let other_name = other.resource_name.as_deref().unwrap_or("");
        match self.resource_pattern {
            ResourcePattern::Any => true,
            ResourcePattern::Literal => self.resource_name.as_deref().unwrap_or("") == other_name,
            ResourcePattern::Prefixed => {
                other_name.starts_with(self.resource_name.as_deref().unwrap_or(""))
            }
        }
    }
}

/// Java: authorization.model.Environment (source IP restriction).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct Environment {
    pub source_ips: Vec<String>,
}

impl Default for Environment {
    fn default() -> Self {
        Self {
            source_ips: Vec::new(),
        }
    }
}

impl Environment {
    pub fn of(source_ip: &str) -> Option<Environment> {
        if source_ip.is_empty() {
            return None;
        }
        Some(Environment {
            source_ips: vec![source_ip.to_string()],
        })
    }

    /// Java: Environment.isMatch(environment) using IP range matching.
    pub fn is_match(&self, other: &Environment) -> bool {
        if self.source_ips.is_empty() {
            return true;
        }
        let target = match other.source_ips.first() {
            Some(t) => t,
            None => return false,
        };
        self.source_ips
            .iter()
            .any(|src| is_ip_in_range(target, src))
    }
}

/// Java: authorization.model.PolicyEntry.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct PolicyEntry {
    pub resource: Resource,
    pub actions: Vec<Action>,
    #[serde(default)]
    pub environment: Option<Environment>,
    pub decision: Decision,
}

impl Default for PolicyEntry {
    fn default() -> Self {
        Self {
            resource: Resource::default(),
            actions: Vec::new(),
            environment: None,
            decision: Decision::Deny,
        }
    }
}

impl PolicyEntry {
    pub fn of(
        resource: Resource,
        actions: Vec<Action>,
        environment: Option<Environment>,
        decision: Decision,
    ) -> Self {
        Self {
            resource,
            actions,
            environment,
            decision,
        }
    }

    pub fn is_match_resource(&self, resource: &Resource) -> bool {
        self.resource.is_match(resource)
    }

    /// Java: PolicyEntry.isMatchAction(List<Action>).
    pub fn is_match_action(&self, actions: &[Action]) -> bool {
        if self.actions.is_empty() {
            return false;
        }
        if actions.contains(&Action::Any) {
            return true;
        }
        actions
            .iter()
            .any(|a| self.actions.contains(a) || self.actions.contains(&Action::All))
    }

    pub fn is_match_environment(&self, environment: Option<&Environment>) -> bool {
        match &self.environment {
            None => true,
            Some(env) => match environment {
                None => env.source_ips.is_empty(),
                Some(other) => env.is_match(other),
            },
        }
    }
}

/// Java: authorization.model.Policy.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct Policy {
    pub policy_type: PolicyType,
    pub entries: Vec<PolicyEntry>,
}

impl Default for Policy {
    fn default() -> Self {
        Self {
            policy_type: PolicyType::Custom,
            entries: Vec::new(),
        }
    }
}

impl Policy {
    pub fn of(policy_type: PolicyType, entries: Vec<PolicyEntry>) -> Self {
        Self {
            policy_type,
            entries,
        }
    }

    pub fn of_resources(
        policy_type: PolicyType,
        resources: Vec<Resource>,
        actions: Vec<Action>,
        environment: Option<Environment>,
        decision: Decision,
    ) -> Self {
        let entries = resources
            .into_iter()
            .map(|r| PolicyEntry::of(r, actions.clone(), environment.clone(), decision))
            .collect();
        Self::of(policy_type, entries)
    }
}

/// Java: authorization.model.Acl.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct Acl {
    pub subject: Subject,
    pub policies: Vec<Policy>,
}

impl Default for Acl {
    fn default() -> Self {
        Self {
            subject: Subject::User(User::default()),
            policies: Vec::new(),
        }
    }
}

impl Acl {
    pub fn of(subject: Subject, policies: Vec<Policy>) -> Self {
        Self { subject, policies }
    }

    pub fn of_resources(
        subject: Subject,
        resources: Vec<Resource>,
        actions: Vec<Action>,
        environment: Option<Environment>,
        decision: Decision,
    ) -> Self {
        let policy = Policy::of_resources(
            PolicyType::Custom,
            resources,
            actions,
            environment,
            decision,
        );
        Self::of(subject, vec![policy])
    }

    pub fn get_policy(&self, policy_type: PolicyType) -> Option<&Policy> {
        self.policies.iter().find(|p| p.policy_type == policy_type)
    }

    /// Insert/update entries for the given policy type. Java: Acl.updatePolicy.
    pub fn update_policy(&mut self, policy_type: PolicyType, new_entries: Vec<PolicyEntry>) {
        if let Some(policy) = self
            .policies
            .iter_mut()
            .find(|p| p.policy_type == policy_type)
        {
            for entry in new_entries {
                if let Some(existing) = policy
                    .entries
                    .iter_mut()
                    .find(|e| e.resource == entry.resource)
                {
                    *existing = entry;
                } else {
                    policy.entries.push(entry);
                }
            }
        } else {
            self.policies.push(Policy::of(policy_type, new_entries));
        }
    }
}

// ---------------------------------------------------------------------------
// Authorization context + request context.
// Java: authorization.context.{AuthorizationContext,DefaultAuthorizationContext}
// and authorization.model.RequestContext.
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub struct AuthorizationContext {
    pub subject: Subject,
    pub resource: Resource,
    pub actions: Vec<Action>,
    pub source_ip: String,
    pub rpc_code: String,
    pub channel_id: Option<String>,
}

impl AuthorizationContext {
    pub fn of(subject: Subject, resource: Resource, actions: Vec<Action>, source_ip: &str) -> Self {
        Self {
            subject,
            resource,
            actions,
            source_ip: source_ip.to_string(),
            rpc_code: String::new(),
            channel_id: None,
        }
    }

    pub fn with_rpc_code(mut self, rpc_code: impl Into<String>) -> Self {
        self.rpc_code = rpc_code.into();
        self
    }

    pub fn with_channel_id(mut self, channel_id: Option<String>) -> Self {
        self.channel_id = channel_id;
        self
    }

    pub fn subject_key(&self) -> String {
        self.subject.subject_key()
    }

    pub fn resource_key(&self) -> String {
        self.resource.resource_key()
    }
}

// ---------------------------------------------------------------------------
// In-memory metadata providers (async, behind a tokio RwLock).
// Java: authorization.provider.LocalAuthorizationMetadataProvider /
//       authentication.provider.LocalAuthenticationMetadataProvider.
// ---------------------------------------------------------------------------

#[derive(Default)]
pub struct LocalAuthorizationMetadataProvider {
    acls: RwLock<HashMap<String, Acl>>,
}

impl LocalAuthorizationMetadataProvider {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub async fn create_acl(&self, acl: Acl) -> Result<()> {
        let key = acl.subject.subject_key();
        self.acls.write().await.insert(key, acl);
        Ok(())
    }

    pub async fn update_acl(&self, acl: Acl) -> Result<()> {
        self.create_acl(acl).await
    }

    pub async fn delete_acl(&self, subject: &Subject) -> Result<()> {
        self.acls.write().await.remove(&subject.subject_key());
        Ok(())
    }

    pub async fn get_acl(&self, subject: &Subject) -> Result<Option<Acl>> {
        Ok(self.acls.read().await.get(&subject.subject_key()).cloned())
    }

    /// Java: listAcl(subjectFilter, resourceFilter).
    pub async fn list_acl(
        &self,
        subject_filter: Option<&str>,
        resource_filter: Option<&str>,
    ) -> Result<Vec<Acl>> {
        let guard = self.acls.read().await;
        let mut result = Vec::new();
        for (key, acl) in guard.iter() {
            if let Some(f) = subject_filter {
                if !key.contains(f) {
                    continue;
                }
            }
            if let Some(f) = resource_filter {
                let matches = acl
                    .policies
                    .iter()
                    .flat_map(|p| p.entries.iter())
                    .any(|e| e.resource.resource_key().contains(f));
                if !matches {
                    continue;
                }
            }
            result.push(acl.clone());
        }
        Ok(result)
    }

    /// Test/utility: load a set of ACLs at once.
    pub async fn load(&self, acls: Vec<Acl>) -> Result<()> {
        let mut guard = self.acls.write().await;
        for acl in acls {
            guard.insert(acl.subject.subject_key(), acl);
        }
        Ok(())
    }
}

#[derive(Default)]
pub struct LocalAuthenticationMetadataProvider {
    users: RwLock<HashMap<String, User>>,
}

impl LocalAuthenticationMetadataProvider {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub async fn create_user(&self, user: User) -> Result<()> {
        self.users.write().await.insert(user.username.clone(), user);
        Ok(())
    }

    pub async fn update_user(&self, user: User) -> Result<()> {
        self.create_user(user).await
    }

    pub async fn delete_user(&self, username: &str) -> Result<()> {
        self.users.write().await.remove(username);
        Ok(())
    }

    pub async fn get_user(&self, username: &str) -> Result<Option<User>> {
        Ok(self.users.read().await.get(username).cloned())
    }

    pub async fn list_user(&self, filter: Option<&str>) -> Result<Vec<User>> {
        let guard = self.users.read().await;
        Ok(guard
            .iter()
            .filter(|(k, _)| match filter {
                Some(f) => k.contains(f),
                None => true,
            })
            .map(|(_, u)| u.clone())
            .collect())
    }

    pub async fn load(&self, users: Vec<User>) -> Result<()> {
        let mut guard = self.users.write().await;
        for u in users {
            guard.insert(u.username.clone(), u);
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Authorization chain handlers (chain of responsibility).
// Java: authorization.chain.{UserAuthorizationHandler,AclAuthorizationHandler}.
// ---------------------------------------------------------------------------

/// Java: authorization.chain.AclAuthorizationHandler — the core allow/deny decision.
pub struct AclAuthorizationHandler {
    metadata: Arc<LocalAuthorizationMetadataProvider>,
    /// Optional subject -> ACL TTL cache (Java uses caffeine for the same
    /// purpose). `None` disables caching. Shared via `Arc` so the cache
    /// survives across handler constructions.
    cache: Option<Arc<TtlCache<Acl>>>,
}

impl AclAuthorizationHandler {
    pub fn new(metadata: Arc<LocalAuthorizationMetadataProvider>) -> Self {
        Self {
            metadata,
            cache: None,
        }
    }

    /// Same handler with a private subject -> ACL resolution cache of the
    /// given TTL.
    pub fn new_cached(metadata: Arc<LocalAuthorizationMetadataProvider>, ttl: Duration) -> Self {
        Self::new_with_cache(metadata, Arc::new(TtlCache::new(ttl)))
    }

    /// Same handler sharing an externally-owned cache (used by the provider
    /// so entries survive across chain rebuilds).
    pub fn new_with_cache(
        metadata: Arc<LocalAuthorizationMetadataProvider>,
        cache: Arc<TtlCache<Acl>>,
    ) -> Self {
        Self {
            metadata,
            cache: Some(cache),
        }
    }

    /// Java: AclAuthorizationHandler.handle — always decides: `Ok(Some(()))`
    /// when allowed, `Err` when denied / no policy matched. Never abstains.
    pub async fn handle(&self, ctx: &AuthorizationContext) -> Result<Option<()>> {
        let acl = match &self.cache {
            Some(cache) => {
                let key = ctx.subject_key();
                match cache.get(&key).await {
                    Some(acl) => acl,
                    None => match self.metadata.get_acl(&ctx.subject).await? {
                        Some(acl) => {
                            cache.insert(&key, acl.clone()).await;
                            acl
                        }
                        // Do not cache negative lookups: a later createAcl
                        // should take effect immediately.
                        None => {
                            return Err(Error::Auth(format!(
                                "no matched policies for {}",
                                ctx.subject_key()
                            )));
                        }
                    },
                }
            }
            None => match self.metadata.get_acl(&ctx.subject).await? {
                Some(acl) => acl,
                None => {
                    return Err(Error::Auth(format!(
                        "no matched policies for {}",
                        ctx.subject_key()
                    )));
                }
            },
        };
        self.handle_with_acl(ctx, &acl).await.map(|_| Some(()))
    }

    /// Evaluate an already-resolved ACL (used by the cached path).
    async fn handle_with_acl(&self, ctx: &AuthorizationContext, acl: &Acl) -> Result<()> {
        let entry = self
            .match_policy_entries(ctx, acl)
            .ok_or_else(|| Error::Auth(format!("no matched policies for {}", ctx.subject_key())))?;

        if entry.decision == Decision::Deny {
            return Err(Error::Auth(format!(
                "{} has no permission to access {} from {}",
                ctx.subject_key(),
                ctx.resource_key(),
                ctx.source_ip
            )));
        }
        Ok(())
    }

    fn match_policy_entries<'a>(
        &self,
        ctx: &AuthorizationContext,
        acl: &'a Acl,
    ) -> Option<&'a PolicyEntry> {
        let mut entries: Vec<&'a PolicyEntry> = Vec::new();
        if let Some(policy) = acl.get_policy(PolicyType::Custom) {
            entries.extend(self.filter_entries(ctx, &policy.entries));
        }
        if entries.is_empty() {
            if let Some(policy) = acl.get_policy(PolicyType::Default) {
                entries.extend(self.filter_entries(ctx, &policy.entries));
            }
        }
        if entries.is_empty() {
            return None;
        }
        entries.sort_by(|a, b| compare_policy_entries(*a, *b));
        Some(entries[0])
    }

    fn filter_entries<'a>(
        &self,
        ctx: &AuthorizationContext,
        entries: &'a [PolicyEntry],
    ) -> Vec<&'a PolicyEntry> {
        let env = Environment::of(&ctx.source_ip);
        entries
            .iter()
            .filter(|e| {
                e.is_match_resource(&ctx.resource)
                    && e.is_match_action(&ctx.actions)
                    && e.is_match_environment(env.as_ref())
            })
            .collect()
    }
}

/// Java: authorization.chain.UserAuthorizationHandler — super users short-circuit allow.
pub struct UserAuthorizationHandler {
    metadata: Arc<LocalAuthenticationMetadataProvider>,
}

impl UserAuthorizationHandler {
    pub fn new(metadata: Arc<LocalAuthenticationMetadataProvider>) -> Self {
        Self { metadata }
    }

    /// Java: UserAuthorizationHandler.handle — runs as the first chain link.
    /// Returns `Ok(Some(()))` when this handler decided (super user allow),
    /// `Err` for disabled / missing users, `Ok(None)` to fall through to the
    /// ACL handler.
    pub async fn handle(&self, ctx: &AuthorizationContext) -> Result<Option<()>> {
        if !ctx.subject.is_subject(SubjectType::User) {
            return Ok(None);
        }
        let username = ctx.subject.username();
        // Java `UserAuthorizationHandler` abstains when the user is unknown (an ACL may exist
        // without an authentication record); only a disabled or super user is decided here.
        let Some(user) = self.metadata.get_user(&username).await? else {
            return Ok(None);
        };
        if user.user_status == UserStatus::Disable {
            return Err(Error::Auth(format!("User:{} is disabled", username)));
        }
        if user.user_type == UserType::Super {
            return Ok(Some(()));
        }
        Ok(None)
    }
}

/// Java: AclAuthorizationHandler.comparePolicyEntries — most specific resource first,
/// then DENY priority. Returns `Ordering` where `Less` means higher priority.
fn compare_policy_entries(a: &PolicyEntry, b: &PolicyEntry) -> Ordering {
    use ResourcePattern::{Any, Literal, Prefixed};
    let r1 = &a.resource;
    let r2 = &b.resource;

    let mut compare = Ordering::Equal;
    if r1.resource_type != r2.resource_type {
        if r1.resource_type == ResourceType::Any {
            compare = Ordering::Greater;
        } else if r2.resource_type == ResourceType::Any {
            compare = Ordering::Less;
        }
    } else if r1.resource_pattern == r2.resource_pattern {
        if r1.resource_pattern == Prefixed {
            let l1 = r1.resource_name.as_deref().unwrap_or("").len();
            let l2 = r2.resource_name.as_deref().unwrap_or("").len();
            compare = l2.cmp(&l1); // longer (more specific) prefix first
        }
    } else {
        let rank = |p: ResourcePattern| match p {
            Literal => 0,
            Prefixed => 1,
            Any => 2,
        };
        compare = rank(r1.resource_pattern).cmp(&rank(r2.resource_pattern));
    }

    if compare != Ordering::Equal {
        return compare;
    }

    // Tie-break by decision: DENY has higher priority (comes first).
    if a.decision != b.decision {
        return if a.decision == Decision::Deny {
            Ordering::Less
        } else {
            Ordering::Greater
        };
    }
    Ordering::Equal
}

// ---------------------------------------------------------------------------
// Default authorization provider + evaluator.
// Java: provider.DefaultAuthorizationProvider + AuthorizationEvaluator.
// ---------------------------------------------------------------------------

/// One link of the authorization handler chain. Java: a
/// `HandlerChain<DefaultAuthorizationContext, ...>` of typed handlers; here
/// modeled as an enum so pluggable strategy providers can hand back a list.
pub enum AuthorizationHandler {
    User(UserAuthorizationHandler),
    Acl(AclAuthorizationHandler),
}

impl AuthorizationHandler {
    /// Execute this link. `Ok(Some(()))` = decided (allow), `Ok(None)` =
    /// abstain (fall through to the next link), `Err` = decided (deny/error).
    pub async fn handle(&self, ctx: &AuthorizationContext) -> Result<Option<()>> {
        match self {
            AuthorizationHandler::User(h) => h.handle(ctx).await,
            AuthorizationHandler::Acl(h) => h.handle(ctx).await,
        }
    }
}

/// Java: authorization.strategy.AuthorizationStrategy — pluggable point that
/// decides which handler chain judges a request. The default implementation
/// preserves the built-in `User -> Acl` chain; upper layers may inject a
/// Stateful strategy provider without touching this module again.
pub trait AuthorizationStrategyProvider: Send + Sync {
    /// Returns the ordered handler list used to judge a request.
    fn handlers(&self) -> Vec<AuthorizationHandler>;
}

/// Default chain: super-user short-circuit followed by ACL evaluation.
/// Java: DefaultAuthorizationProvider.newHandlerChain().
pub struct DefaultAuthorizationStrategyProvider {
    pub auth_metadata: Arc<LocalAuthenticationMetadataProvider>,
    pub acl_metadata: Arc<LocalAuthorizationMetadataProvider>,
    /// Shared subject -> ACL cache; `None` disables caching.
    pub cache: Option<Arc<TtlCache<Acl>>>,
}

impl AuthorizationStrategyProvider for DefaultAuthorizationStrategyProvider {
    fn handlers(&self) -> Vec<AuthorizationHandler> {
        let acl = match &self.cache {
            Some(cache) => {
                AclAuthorizationHandler::new_with_cache(self.acl_metadata.clone(), cache.clone())
            }
            None => AclAuthorizationHandler::new(self.acl_metadata.clone()),
        };
        vec![
            AuthorizationHandler::User(UserAuthorizationHandler::new(self.auth_metadata.clone())),
            AuthorizationHandler::Acl(acl),
        ]
    }
}

pub struct DefaultAuthorizationProvider {
    auth_config: AuthConfig,
    auth_metadata: Arc<LocalAuthenticationMetadataProvider>,
    acl_metadata: Arc<LocalAuthorizationMetadataProvider>,
    /// Pluggable strategy; `None` falls back to the default chain above.
    strategy_provider: Option<Arc<dyn AuthorizationStrategyProvider>>,
    /// Shared subject -> ACL TTL cache (config: `cache_ttl_seconds`).
    policy_cache: Option<Arc<TtlCache<Acl>>>,
    /// Flag indicating this provider is the active authorization service.
    pub is_auth_service: bool,
}

impl DefaultAuthorizationProvider {
    fn policy_cache_for(auth_config: &AuthConfig) -> Option<Arc<TtlCache<Acl>>> {
        if auth_config.cache_ttl_seconds > 0 {
            Some(Arc::new(TtlCache::new(Duration::from_secs(
                auth_config.cache_ttl_seconds,
            ))))
        } else {
            None
        }
    }

    pub fn new(
        auth_config: AuthConfig,
        auth_metadata: Arc<LocalAuthenticationMetadataProvider>,
        acl_metadata: Arc<LocalAuthorizationMetadataProvider>,
    ) -> Arc<Self> {
        let policy_cache = Self::policy_cache_for(&auth_config);
        Arc::new(Self {
            auth_config,
            auth_metadata,
            acl_metadata,
            strategy_provider: None,
            policy_cache,
            is_auth_service: true,
        })
    }

    /// Same as [`Self::new`] but with a pluggable authorization strategy
    /// (Java: a custom `AuthorizationStrategy` implementation).
    pub fn new_with_strategy_provider(
        auth_config: AuthConfig,
        auth_metadata: Arc<LocalAuthenticationMetadataProvider>,
        acl_metadata: Arc<LocalAuthorizationMetadataProvider>,
        strategy_provider: Arc<dyn AuthorizationStrategyProvider>,
    ) -> Arc<Self> {
        let policy_cache = Self::policy_cache_for(&auth_config);
        Arc::new(Self {
            auth_config,
            auth_metadata,
            acl_metadata,
            strategy_provider: Some(strategy_provider),
            policy_cache,
            is_auth_service: true,
        })
    }

    /// Java: AbstractAuthorizationStrategy.doEvaluate gate + chain execution +
    /// DefaultAuthorizationProvider.doAuditLog.
    pub async fn authorize(&self, ctx: &AuthorizationContext) -> Result<()> {
        if !self.auth_config.authorization_enabled {
            return Ok(());
        }
        let result = self.run_chain(ctx).await;
        // Java: authorize(...) completes with doAuditLog(context, ex).
        if self.auth_config.audit_enabled {
            AuditLogger::log_authorization(ctx, &result);
        }
        result
    }

    async fn run_chain(&self, ctx: &AuthorizationContext) -> Result<()> {
        let handlers = match &self.strategy_provider {
            Some(strategy) => strategy.handlers(),
            None => DefaultAuthorizationStrategyProvider {
                auth_metadata: self.auth_metadata.clone(),
                acl_metadata: self.acl_metadata.clone(),
                cache: self.policy_cache.clone(),
            }
            .handlers(),
        };
        for handler in &handlers {
            match handler.handle(ctx).await {
                Ok(Some(())) => return Ok(()),
                Ok(None) => continue,
                Err(e) => return Err(e),
            }
        }
        // No handler decided: deny (same as an empty ACL match).
        Err(Error::Auth(format!(
            "no matched policies for {}",
            ctx.subject_key()
        )))
    }

    /// Java: DefaultAuthorizationContextBuilder.build(ChannelHandlerContext, RemotingCommand).
    pub fn build_contexts(
        &self,
        command: &Command,
        source_ip: &str,
    ) -> Result<Vec<AuthorizationContext>> {
        let fields = &command.ext_fields;
        let subject = match fields.get(ACCESS_KEY) {
            Some(ak) => Subject::User(User::new(ak.clone())),
            None => Subject::User(User::default()),
        };
        let code = command.code;
        let mut result: Vec<AuthorizationContext> = Vec::new();
        let add = |subject: &Subject,
                   resource: Resource,
                   actions: Vec<Action>,
                   out: &mut Vec<AuthorizationContext>| {
            out.push(
                AuthorizationContext::of(subject.clone(), resource, actions, source_ip)
                    .with_rpc_code(code.to_string()),
            );
        };

        match code {
            request_code::GET_ROUTEINFO_BY_TOPIC => {
                let topic = require_resource(fields.get("topic"), "topic")?;
                if is_retry_topic(&topic) {
                    add(
                        &subject,
                        Resource::of_group(&topic),
                        vec![Action::Sub, Action::Get],
                        &mut result,
                    );
                } else {
                    add(
                        &subject,
                        Resource::of_topic(&topic),
                        vec![Action::Pub, Action::Sub, Action::Get],
                        &mut result,
                    );
                }
            }
            request_code::SEND_MESSAGE | request_code::SEND_REPLY_MESSAGE => {
                let topic = require_resource(fields.get("topic"), "topic")?;
                if is_retry_topic(&topic) {
                    add(
                        &subject,
                        Resource::of_group(&topic),
                        vec![Action::Sub],
                        &mut result,
                    );
                } else {
                    add(
                        &subject,
                        Resource::of_topic(&topic),
                        vec![Action::Pub],
                        &mut result,
                    );
                }
            }
            request_code::SEND_MESSAGE_V2
            | request_code::SEND_BATCH_MESSAGE
            | request_code::SEND_REPLY_MESSAGE_V2 => {
                let topic = require_resource(fields.get("b"), "topic")?;
                if is_retry_topic(&topic) {
                    add(
                        &subject,
                        Resource::of_group(&topic),
                        vec![Action::Sub],
                        &mut result,
                    );
                } else {
                    add(
                        &subject,
                        Resource::of_topic(&topic),
                        vec![Action::Pub],
                        &mut result,
                    );
                }
            }
            request_code::END_TRANSACTION => {
                if let Some(topic) = fields.get("topic") {
                    if !topic.is_empty() {
                        add(
                            &subject,
                            Resource::of_topic(topic),
                            vec![Action::Pub],
                            &mut result,
                        );
                    }
                }
            }
            request_code::VIEW_MESSAGE_BY_ID => {
                let topic = require_resource(fields.get("topic"), "topic")?;
                let resource = if is_retry_topic(&topic) {
                    Resource::of_group(&topic)
                } else {
                    Resource::of_topic(&topic)
                };
                add(&subject, resource, vec![Action::Get], &mut result);
            }
            request_code::CONSUMER_SEND_MSG_BACK => {
                let group = require_resource(fields.get("group"), "consumer group")?;
                add(
                    &subject,
                    Resource::of_group(&group),
                    vec![Action::Sub],
                    &mut result,
                );
            }
            request_code::PULL_MESSAGE | request_code::LITE_PULL_MESSAGE => {
                let topic = require_resource(fields.get("topic"), "topic")?;
                let group = require_resource(fields.get("consumerGroup"), "consumer group")?;
                if !is_retry_topic(&topic) {
                    add(
                        &subject,
                        Resource::of_topic(&topic),
                        vec![Action::Sub],
                        &mut result,
                    );
                }
                add(
                    &subject,
                    Resource::of_group(&group),
                    vec![Action::Sub],
                    &mut result,
                );
            }
            request_code::QUERY_MESSAGE => {
                let topic = require_resource(fields.get("topic"), "topic")?;
                add(
                    &subject,
                    Resource::of_topic(&topic),
                    vec![Action::Sub, Action::Get],
                    &mut result,
                );
            }
            request_code::HEART_BEAT => {
                // Body carries heartbeat data; simplified: emit an empty list unless
                // an explicit consumer group/topic is present in ext fields.
                if let Some(group) = fields.get("consumerGroup") {
                    if !group.is_empty() {
                        add(
                            &subject,
                            Resource::of_group(group),
                            vec![Action::Sub],
                            &mut result,
                        );
                    }
                }
            }
            request_code::GET_CONSUMER_LIST_BY_GROUP => {
                let group = require_resource(fields.get("consumerGroup"), "consumer group")?;
                add(
                    &subject,
                    Resource::of_group(&group),
                    vec![Action::Sub, Action::Get],
                    &mut result,
                );
            }
            request_code::QUERY_CONSUMER_OFFSET => {
                let topic = require_resource(fields.get("topic"), "topic")?;
                let group = require_resource(fields.get("consumerGroup"), "consumer group")?;
                if !is_retry_topic(&topic) {
                    add(
                        &subject,
                        Resource::of_topic(&topic),
                        vec![Action::Sub, Action::Get],
                        &mut result,
                    );
                }
                add(
                    &subject,
                    Resource::of_group(&group),
                    vec![Action::Sub, Action::Get],
                    &mut result,
                );
            }
            request_code::UPDATE_CONSUMER_OFFSET => {
                let topic = require_resource(fields.get("topic"), "topic")?;
                let group = require_resource(fields.get("consumerGroup"), "consumer group")?;
                if !is_retry_topic(&topic) {
                    add(
                        &subject,
                        Resource::of_topic(&topic),
                        vec![Action::Sub, Action::Update],
                        &mut result,
                    );
                }
                add(
                    &subject,
                    Resource::of_group(&group),
                    vec![Action::Sub, Action::Update],
                    &mut result,
                );
            }
            request_code::LITE_SUBSCRIPTION_CTL => {
                // Simplified: rely on ext fields consumerGroup/topic when present.
                if let Some(group) = fields.get("consumerGroup") {
                    if !group.is_empty() {
                        add(
                            &subject,
                            Resource::of_group(group),
                            vec![Action::Sub],
                            &mut result,
                        );
                    }
                }
                if let Some(topic) = fields.get("topic") {
                    if !topic.is_empty() && !is_retry_topic(topic) {
                        add(
                            &subject,
                            Resource::of_topic(topic),
                            vec![Action::Sub],
                            &mut result,
                        );
                    }
                }
            }
            request_code::UPDATE_BROKER_CONFIG => {
                add(
                    &subject,
                    Resource::of_cluster(&self.auth_config.cluster_name),
                    vec![Action::Update],
                    &mut result,
                );
            }
            request_code::GET_BROKER_CONFIG => {
                add(
                    &subject,
                    Resource::of_cluster(&self.auth_config.cluster_name),
                    vec![Action::Get],
                    &mut result,
                );
            }
            // Unknown / unhandled codes behave like Java's annotation fallback: no
            // contexts are produced, so no authorization check is performed.
            _ => {}
        }

        for ctx in result.iter_mut() {
            ctx.channel_id = None;
        }
        Ok(result)
    }
}

fn require_resource(value: Option<&String>, name: &str) -> Result<String> {
    match value {
        Some(v) if !v.is_empty() => Ok(v.clone()),
        _ => Err(Error::Invalid(format!("{name} is null"))),
    }
}

/// Java: AuthorizationEvaluator.
pub struct AuthorizationEvaluator {
    provider: Arc<DefaultAuthorizationProvider>,
}

impl AuthorizationEvaluator {
    pub fn new(provider: Arc<DefaultAuthorizationProvider>) -> Self {
        Self { provider }
    }

    /// Java: AuthorizationEvaluator.evaluate(List<AuthorizationContext>).
    pub async fn evaluate(&self, contexts: &[AuthorizationContext]) -> Result<()> {
        if contexts.is_empty() {
            return Err(Error::Auth("authorization context is empty".into()));
        }
        for ctx in contexts {
            self.provider.authorize(ctx).await?;
        }
        Ok(())
    }

    pub async fn evaluate_command(&self, command: &Command, source_ip: &str) -> Result<()> {
        let contexts = self.provider.build_contexts(command, source_ip)?;
        if contexts.is_empty() {
            return Ok(());
        }
        self.evaluate(&contexts).await
    }
}

// ---------------------------------------------------------------------------
// Authorization metadata manager. Java: manager.AuthorizationMetadataManagerImpl.
// ---------------------------------------------------------------------------

pub struct AuthorizationMetadataManager {
    acl_metadata: Arc<LocalAuthorizationMetadataProvider>,
    auth_metadata: Arc<LocalAuthenticationMetadataProvider>,
}

impl AuthorizationMetadataManager {
    pub fn new(
        acl_metadata: Arc<LocalAuthorizationMetadataProvider>,
        auth_metadata: Arc<LocalAuthenticationMetadataProvider>,
    ) -> Self {
        Self {
            acl_metadata,
            auth_metadata,
        }
    }

    pub async fn create_acl(&self, acl: Acl) -> Result<()> {
        validate_acl(&acl)?;
        let subject = acl.subject.clone();
        let username = subject.username();
        if subject.is_subject(SubjectType::User) {
            let user = self
                .auth_metadata
                .get_user(&username)
                .await?
                .ok_or_else(|| {
                    Error::Auth(format!("subject {} does not exist", subject.subject_key()))
                })?;
            if user.user_status == UserStatus::Disable {
                return Err(Error::Auth(format!("User:{} is disabled", username)));
            }
        }
        self.acl_metadata.create_acl(acl).await
    }

    pub async fn get_acl(&self, subject: &Subject) -> Result<Option<Acl>> {
        self.acl_metadata.get_acl(subject).await
    }

    pub async fn delete_acl(&self, subject: &Subject) -> Result<()> {
        self.acl_metadata.delete_acl(subject).await
    }

    pub async fn list_acl(
        &self,
        subject_filter: Option<&str>,
        resource_filter: Option<&str>,
    ) -> Result<Vec<Acl>> {
        self.acl_metadata
            .list_acl(subject_filter, resource_filter)
            .await
    }
}

fn validate_acl(acl: &Acl) -> Result<()> {
    if acl.policies.is_empty() {
        return Err(Error::Invalid("the policies is empty".into()));
    }
    for policy in &acl.policies {
        if policy.entries.is_empty() {
            return Err(Error::Invalid("the policy entries is empty".into()));
        }
        for entry in &policy.entries {
            if entry.resource.resource_type == ResourceType::Any
                && entry.resource.resource_pattern == ResourcePattern::Any
                && entry.resource.resource_name.is_some()
            {
                return Err(Error::Invalid("invalid resource".into()));
            }
            if entry.actions.is_empty() {
                return Err(Error::Invalid("the actions is empty".into()));
            }
            if entry.actions.contains(&Action::Any) {
                return Err(Error::Invalid("the actions can not be Any".into()));
            }
            if entry.decision == Decision::Deny && entry.actions.contains(&Action::All) {
                // DENY + ALL is acceptable (matches Java semantics); keep decision.
            }
            if entry.decision != Decision::Allow && entry.decision != Decision::Deny {
                return Err(Error::Invalid("the decision is null or illegal".into()));
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// Authentication context + providers + evaluator.
// Java: authentication.context.{AuthenticationContext,DefaultAuthenticationContext}.
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, Default)]
pub struct AuthenticationContext {
    pub username: String,
    pub content: Vec<u8>,
    pub signature: String,
    pub rpc_code: String,
    pub channel_id: Option<String>,
}

/// Java: authentication.builder.DefaultAuthenticationContextBuilder.
/// Builds the canonical signing content from a RemotingCommand: all ext fields
/// (excluding the signature) sorted by key, concatenated, then the request body
/// appended — matching `AclUtils.combineRequestContent`.
pub fn build_authentication_context(command: &Command) -> AuthenticationContext {
    let username = command
        .ext_fields
        .get(ACCESS_KEY)
        .cloned()
        .unwrap_or_default();
    let signature = command
        .ext_fields
        .get(SIGNATURE)
        .cloned()
        .unwrap_or_default();
    let content = combine_request_content(&command.ext_fields, &command.body);
    AuthenticationContext {
        username,
        content,
        signature,
        rpc_code: command.code.to_string(),
        channel_id: None,
    }
}

fn combine_request_content(fields: &BTreeMap<String, String>, body: &[u8]) -> Vec<u8> {
    let mut content = String::new();
    // BTreeMap keeps the keys sorted; exclude the signature field.
    for (key, value) in fields {
        if key == SIGNATURE {
            continue;
        }
        content.push_str(value);
    }
    let mut bytes = content.into_bytes();
    bytes.extend_from_slice(body);
    bytes
}

/// Stamp a remoting command with `AccessKey`/`Signature` so an ACL-enabled
/// broker accepts it (Java: the proxy's `innerClientAuthenticationCredentials`
/// are applied through an `AclClientRPCHook` before every broker invocation).
/// The signature covers all ext values (sorted, `Signature` excluded)
/// concatenated with the body — `AclUtils.combineRequestContent`.
pub fn sign_remoting_command(request: &mut Command, access_key: &str, secret: &str) {
    request
        .ext_fields
        .insert(ACCESS_KEY.to_string(), access_key.to_string());
    request.ext_fields.remove(SIGNATURE);
    let content = combine_request_content(&request.ext_fields, &request.body);
    let signature = cal_signature(&content, secret);
    request.ext_fields.insert(SIGNATURE.to_string(), signature);
}

/// Java: authentication.chain.DefaultAuthenticationHandler — verifies the signature.
pub struct DefaultAuthenticationHandler {
    metadata: Arc<LocalAuthenticationMetadataProvider>,
}

impl DefaultAuthenticationHandler {
    pub fn new(metadata: Arc<LocalAuthenticationMetadataProvider>) -> Self {
        Self { metadata }
    }

    pub async fn handle(&self, ctx: &AuthenticationContext) -> Result<()> {
        if ctx.username.is_empty() {
            return Err(Error::Auth("username cannot be null".into()));
        }
        let user = self
            .metadata
            .get_user(&ctx.username)
            .await?
            .ok_or_else(|| Error::Auth(format!("User:{} is not found", ctx.username)))?;
        if user.user_status == UserStatus::Disable {
            return Err(Error::Auth(format!("User:{} is disabled", ctx.username)));
        }
        let secret = user.password.as_deref().unwrap_or("");
        let expected = cal_signature(&ctx.content, secret);
        if !ct_eq(&expected, &ctx.signature) {
            return Err(Error::Auth("check signature failed".into()));
        }
        Ok(())
    }
}

/// Java: authentication.provider.DefaultAuthenticationProvider.
pub struct DefaultAuthenticationProvider {
    auth_config: AuthConfig,
    metadata: Arc<LocalAuthenticationMetadataProvider>,
    pub is_auth_service: bool,
}

impl DefaultAuthenticationProvider {
    pub fn new(
        auth_config: AuthConfig,
        metadata: Arc<LocalAuthenticationMetadataProvider>,
    ) -> Arc<Self> {
        Arc::new(Self {
            auth_config,
            metadata,
            is_auth_service: true,
        })
    }

    /// Java: AbstractAuthenticationStrategy.doEvaluate gate + handler execution.
    pub async fn authenticate(&self, ctx: &AuthenticationContext) -> Result<()> {
        if !self.auth_config.authentication_enabled {
            return Ok(());
        }
        if !self.auth_config.is_authentication_required(&ctx.rpc_code) {
            return Ok(());
        }
        let handler = DefaultAuthenticationHandler::new(self.metadata.clone());
        handler.handle(ctx).await
    }

    pub async fn authenticate_command(&self, command: &Command) -> Result<()> {
        let ctx = build_authentication_context(command);
        self.authenticate(&ctx).await
    }
}

/// Java: AuthenticationEvaluator.
pub struct AuthenticationEvaluator {
    provider: Arc<DefaultAuthenticationProvider>,
}

impl AuthenticationEvaluator {
    pub fn new(provider: Arc<DefaultAuthenticationProvider>) -> Self {
        Self { provider }
    }

    pub async fn evaluate(&self, ctx: &AuthenticationContext) -> Result<()> {
        if ctx.username.is_empty() {
            return Ok(());
        }
        self.provider.authenticate(ctx).await
    }
}

/// Java: authentication.manager.AuthenticationMetadataManager.
pub struct AuthenticationMetadataManager {
    metadata: Arc<LocalAuthenticationMetadataProvider>,
}

impl AuthenticationMetadataManager {
    pub fn new(metadata: Arc<LocalAuthenticationMetadataProvider>) -> Self {
        Self { metadata }
    }

    pub async fn create_user(&self, user: User) -> Result<()> {
        self.metadata.create_user(user).await
    }

    pub async fn get_user(&self, username: &str) -> Result<Option<User>> {
        self.metadata.get_user(username).await
    }
}

// ---------------------------------------------------------------------------
// Lightweight TTL cache. Java uses caffeine (user/acl caches + the stateful
// strategy caches); here a simple `RwLock<HashMap<key, (value, expire_at)>>`
// with lazy expiration is sufficient.
// ---------------------------------------------------------------------------

pub struct TtlCache<V: Clone> {
    map: RwLock<HashMap<String, (V, i64)>>,
    ttl: Duration,
}

impl<V: Clone> TtlCache<V> {
    pub fn new(ttl: Duration) -> Self {
        Self {
            map: RwLock::new(HashMap::new()),
            ttl,
        }
    }

    /// Look up `key`; expired entries are treated as absent and evicted lazily.
    pub async fn get(&self, key: &str) -> Option<V> {
        let now = now_millis();
        let mut guard = self.map.write().await;
        match guard.get(key) {
            Some((value, expire_at)) if *expire_at > now => Some(value.clone()),
            Some(_) => {
                guard.remove(key);
                None
            }
            None => None,
        }
    }

    /// Insert or refresh `key` with the configured TTL.
    pub async fn insert(&self, key: &str, value: V) {
        let expire_at = now_millis() + self.ttl.as_millis() as i64;
        self.map
            .write()
            .await
            .insert(key.to_string(), (value, expire_at));
    }

    /// Remove `key` (e.g. when the backing ACL is deleted/updated).
    pub async fn remove(&self, key: &str) {
        self.map.write().await.remove(key);
    }

    /// Drop every entry (expired or not).
    pub async fn clear(&self) {
        self.map.write().await.clear();
    }

    /// Number of live (non-expired) entries; expired entries are evicted.
    pub async fn len(&self) -> usize {
        let now = now_millis();
        let mut guard = self.map.write().await;
        guard.retain(|_, (_, expire_at)| *expire_at > now);
        guard.len()
    }

    pub async fn is_empty(&self) -> bool {
        self.len().await == 0
    }
}

// ---------------------------------------------------------------------------
// Audit log. Java: the ROCKETMQ_AUTH_AUDIT_LOGGER_NAME slf4j logger used by
// DefaultAuthorizationProvider.doAuditLog — here via `tracing` structured
// fields: who / when / action / resource / decision / srcIp.
// ---------------------------------------------------------------------------

pub struct AuditLogger;

impl AuditLogger {
    /// Record one authorization decision. `result` is the provider's verdict:
    /// `Ok` maps to ALLOW, `Err` to DENY.
    pub fn log_authorization(ctx: &AuthorizationContext, result: &Result<()>) {
        let decision = if result.is_ok() {
            Decision::Allow
        } else {
            Decision::Deny
        };
        let actions = ctx
            .actions
            .iter()
            .map(|a| a.name())
            .collect::<Vec<_>>()
            .join(",");
        // Java format: "[AUTHORIZATION] Subject = {} is {} Action = {} from
        // sourceIp = {} on resource = {} for request = {}."
        tracing::info!(
            audit = true,
            subject = %ctx.subject_key(),
            action = %actions,
            resource = %ctx.resource_key(),
            decision = %decision.name(),
            src_ip = %ctx.source_ip,
            rpc_code = %ctx.rpc_code,
            timestamp = now_millis(),
            "[AUTHORIZATION] Subject = {} is {} Action = {} from sourceIp = {} on resource = {} for request = {}.",
            ctx.subject_key(),
            decision.name(),
            actions,
            ctx.source_ip,
            ctx.resource_key(),
            ctx.rpc_code,
        );
    }
}

// ---------------------------------------------------------------------------
// gRPC / v2 protocol header -> contexts.
// Java: AuthenticationMetadataManager gRPC path / GrpcClientSettings header
// conventions + DefaultAuthenticationContextBuilder.build(metadata, message).
// ---------------------------------------------------------------------------

/// Strip the port (and IPv6 brackets) from a `ip[:port]` / `[v6]:port` string.
fn strip_port(address: &str) -> String {
    let address = address.trim();
    if let Some(rest) = address.strip_prefix('[') {
        if let Some(end) = rest.find(']') {
            return rest[..end].to_string();
        }
        return rest.to_string();
    }
    // A bare IPv6 literal contains multiple ':': only strip when there is
    // exactly one ':' and the tail parses as a port.
    match address.rfind(':') {
        Some(pos) if address[..pos].find(':').is_none() => {
            let host = &address[..pos];
            let port = &address[pos + 1..];
            if port.is_empty() || !port.bytes().all(|b| b.is_ascii_digit()) {
                address.to_string()
            } else {
                host.to_string()
            }
        }
        _ => address.to_string(),
    }
}

/// Build an `AuthorizationContext` from the v2 gRPC metadata headers.
///
/// Recognized headers (Java: `GrpcConstants`):
///   * `authorization` — `MQv2-HMAC-SHA1 Credential=<user>[/<date>], ...,
///     Signature=<hex>`; the credential's first `/`-segment is the username
///     and the hex signature is converted to Base64 (Java hexToBase64).
///   * `x-mq-date-time` — signed datetime (required whenever `authorization`
///     is present, mirroring the Java builder).
///   * `rpc-remote-address` / `x-mq-remote-address` — source IP (port stripped).
///   * `x-mq-rpc-name` — rpc code; `x-mq-channel-id` — channel id.
///
/// The resource/actions cannot be derived from headers alone: the caller
/// enriches the returned context from the protobuf request body (Java:
/// DefaultAuthorizationContextBuilder.build(metadata, message)).
pub fn context_from_grpc_headers(
    headers: &HashMap<String, String>,
) -> Result<AuthorizationContext> {
    let get = |name: &str| -> Option<&String> {
        headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v)
    };

    let mut subject = Subject::User(User::default());
    if let Some(authorization) = get(grpc_header::AUTHORIZATION) {
        let authorization = authorization.trim();
        if !authorization.is_empty() {
            let (scheme, rest) = authorization
                .split_once(' ')
                .ok_or_else(|| Error::Auth("authentication header is incorrect.".into()))?;
            if !scheme.eq_ignore_ascii_case(MQV2_HMAC_SHA1) {
                return Err(Error::Auth(format!(
                    "unsupported authentication scheme: {scheme}"
                )));
            }
            let mut username: Option<String> = None;
            for item in rest.split(',') {
                let item = item.trim();
                let Some((key, value)) = item.split_once('=') else {
                    return Err(Error::Auth(
                        "authentication keyValues length is incorrect.".into(),
                    ));
                };
                match key.trim() {
                    "Credential" => {
                        let credential = value.trim();
                        let user = credential.split('/').next().unwrap_or(credential);
                        if user.is_empty() {
                            return Err(Error::Auth(
                                "authentication credential length is incorrect.".into(),
                            ));
                        }
                        username = Some(user.to_string());
                    }
                    "Signature" => {
                        // Validate the hex signature now (converted to the
                        // Base64 form used for verification).
                        hex_to_base64(value.trim())?;
                    }
                    _ => {}
                }
            }
            if get(grpc_header::DATE_TIME)
                .map(|v| v.trim().is_empty())
                .unwrap_or(true)
            {
                return Err(Error::Auth("datetime is null.".into()));
            }
            if let Some(username) = username {
                subject = Subject::User(User::new(username));
            }
        }
    }

    let source_ip = get(grpc_header::REMOTE_ADDRESS)
        .or_else(|| get(grpc_header::X_REMOTE_ADDRESS))
        .map(|addr| strip_port(addr.as_str()))
        .unwrap_or_default();
    let rpc_code = get(grpc_header::RPC_NAME).cloned().unwrap_or_default();
    let channel_id = get(grpc_header::CHANNEL_ID).cloned();

    Ok(
        AuthorizationContext::of(subject, Resource::default(), Vec::new(), &source_ip)
            .with_rpc_code(rpc_code)
            .with_channel_id(channel_id),
    )
}

// ---------------------------------------------------------------------------
// Migration: legacy PlainAccessConfig + AuthMigrator.
// Java: migration.v1.PlainAccessConfig + migration.AuthMigrator.
// ---------------------------------------------------------------------------

/// Java: migration.v1.PlainAccessConfig.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", default)]
pub struct PlainAccessConfig {
    pub access_key: String,
    pub secret_key: String,
    pub white_remote_address: String,
    pub admin: bool,
    pub default_topic_perm: String,
    pub default_group_perm: String,
    pub topic_perms: Vec<String>,
    pub group_perms: Vec<String>,
}

/// Java: migration.AuthMigrator — converts legacy accounts into the new
/// authentication metadata (users) + authorization metadata (ACLs).
pub struct AuthMigrator {
    auth_manager: AuthenticationMetadataManager,
    acl_manager: AuthorizationMetadataManager,
}

impl AuthMigrator {
    pub fn new(
        auth_metadata: Arc<LocalAuthenticationMetadataProvider>,
        acl_metadata: Arc<LocalAuthorizationMetadataProvider>,
    ) -> Self {
        Self {
            auth_manager: AuthenticationMetadataManager::new(auth_metadata.clone()),
            acl_manager: AuthorizationMetadataManager::new(acl_metadata, auth_metadata),
        }
    }

    pub async fn migrate(&self, accounts: &[PlainAccessConfig]) -> Result<()> {
        for account in accounts {
            let username = account.access_key.clone();
            if self.auth_manager.get_user(&username).await?.is_some() {
                continue;
            }
            self.create_user_and_acl(account).await?;
        }
        Ok(())
    }

    async fn create_user_and_acl(&self, account: &PlainAccessConfig) -> Result<()> {
        let user = User::new(account.access_key.clone())
            .with_secret(account.secret_key.clone())
            .with_type(if account.admin {
                UserType::Super
            } else {
                UserType::Normal
            })
            .with_status(UserStatus::Enable);
        self.auth_manager.create_user(user).await?;
        self.create_acl(account).await
    }

    async fn create_acl(&self, config: &PlainAccessConfig) -> Result<()> {
        let subject = Subject::User(User::new(config.access_key.clone()));
        let mut policies: Vec<Policy> = Vec::new();

        let mut custom = None;
        for topic_perm in &config.topic_perms {
            if let Some((name, perm)) = parse_perm_pair(topic_perm) {
                let entry = PolicyEntry::of(
                    Resource::of_topic(&name),
                    parse_actions(&perm),
                    environment_for(&config.white_remote_address),
                    parse_decision(&perm),
                );
                custom
                    .get_or_insert_with(|| Policy::of(PolicyType::Custom, Vec::new()))
                    .entries
                    .push(entry);
            }
        }
        for group_perm in &config.group_perms {
            if let Some((name, perm)) = parse_perm_pair(group_perm) {
                let entry = PolicyEntry::of(
                    Resource::of_group(&name),
                    parse_actions(&perm),
                    environment_for(&config.white_remote_address),
                    parse_decision(&perm),
                );
                custom
                    .get_or_insert_with(|| Policy::of(PolicyType::Custom, Vec::new()))
                    .entries
                    .push(entry);
            }
        }
        if let Some(custom) = custom {
            policies.push(custom);
        }

        let mut default = None;
        if !config.default_topic_perm.trim().is_empty() {
            let perm = config.default_topic_perm.trim().to_string();
            let entry = PolicyEntry::of(
                Resource::of_any(ResourceType::Topic),
                parse_actions(&perm),
                environment_for(&config.white_remote_address),
                parse_decision(&perm),
            );
            default
                .get_or_insert_with(|| Policy::of(PolicyType::Default, Vec::new()))
                .entries
                .push(entry);
        }
        if !config.default_group_perm.trim().is_empty() {
            let perm = config.default_group_perm.trim().to_string();
            let entry = PolicyEntry::of(
                Resource::of_any(ResourceType::Group),
                parse_actions(&perm),
                environment_for(&config.white_remote_address),
                parse_decision(&perm),
            );
            default
                .get_or_insert_with(|| Policy::of(PolicyType::Default, Vec::new()))
                .entries
                .push(entry);
        }
        if let Some(default) = default {
            policies.push(default);
        }

        if policies.is_empty() {
            return Ok(());
        }

        let acl = Acl::of(subject, policies);
        self.acl_manager.create_acl(acl).await
    }
}

/// Load a legacy plain-ACL accounts file (Java `plain_acl.yml`). Both the
/// top-level single-account form and the `accounts:` list form are accepted,
/// and the file may be YAML (`.yml`/`.yaml`) or JSON (any other extension).
/// Field names follow Java's `PlainAccessConfig` (camelCase).
pub fn load_plain_acl(path: &str) -> Result<Vec<PlainAccessConfig>> {
    let raw = std::fs::read_to_string(path)?;
    let lower = path.to_ascii_lowercase();
    let value: serde_json::Value = if lower.ends_with(".yml") || lower.ends_with(".yaml") {
        let yaml: serde_yaml::Value = serde_yaml::from_str(&raw)
            .map_err(|error| Error::Invalid(format!("invalid plain ACL YAML: {error}")))?;
        serde_json::to_value(yaml)
            .map_err(|error| Error::Invalid(format!("invalid plain ACL YAML: {error}")))?
    } else {
        serde_json::from_str(&raw)
            .map_err(|error| Error::Invalid(format!("invalid plain ACL JSON: {error}")))?
    };

    if let Some(accounts) = value.get("accounts") {
        let accounts: Vec<PlainAccessConfig> = serde_json::from_value(accounts.clone())
            .map_err(|error| Error::Invalid(format!("invalid plain ACL accounts: {error}")))?;
        if accounts.is_empty() {
            return Err(Error::Invalid("plain ACL file has no accounts".into()));
        }
        return Ok(accounts);
    }
    // Top-level single-account form (Java: AclUtils.getYamlDataObject fallback).
    let account: PlainAccessConfig = serde_json::from_value(value)
        .map_err(|error| Error::Invalid(format!("invalid plain ACL account: {error}")))?;
    if account.access_key.is_empty() {
        return Err(Error::Invalid("plain ACL file has no accessKey".into()));
    }
    Ok(vec![account])
}

/// Startup bootstrap: load a plain-ACL accounts file and migrate every account
/// into the authentication/authorization metadata providers (Java runs the
/// `AuthMigrator` against the loaded accounts when the server starts with ACL
/// enabled). Returns the number of accounts in the file.
pub async fn bootstrap_plain_acl(
    auth_metadata: &Arc<LocalAuthenticationMetadataProvider>,
    acl_metadata: &Arc<LocalAuthorizationMetadataProvider>,
    path: &str,
) -> Result<usize> {
    let accounts = load_plain_acl(path)?;
    AuthMigrator::new(auth_metadata.clone(), acl_metadata.clone())
        .migrate(&accounts)
        .await?;
    Ok(accounts.len())
}

fn parse_perm_pair(raw: &str) -> Option<(String, String)> {
    let mut parts = raw.splitn(2, EQUAL);
    let name = parts.next()?.trim().to_string();
    let perm = parts.next()?.trim().to_string();
    if name.is_empty() {
        return None;
    }
    Some((name, perm))
}

fn parse_decision(perm: &str) -> Decision {
    if perm.trim().eq_ignore_ascii_case(PERM_DENY) {
        Decision::Deny
    } else {
        Decision::Allow
    }
}

fn parse_actions(perm: &str) -> Vec<Action> {
    let trimmed = perm.trim();
    match trimmed.to_uppercase().as_str() {
        PERM_PUB => vec![Action::Pub],
        PERM_SUB => vec![Action::Sub],
        PERM_PUB_SUB | PERM_SUB_PUB => vec![Action::Pub, Action::Sub],
        PERM_DENY => vec![Action::All],
        _ => vec![Action::All],
    }
}

fn environment_for(white_remote_address: &str) -> Option<Environment> {
    if white_remote_address.is_empty() {
        None
    } else {
        Some(Environment {
            source_ips: vec![white_remote_address.to_string()],
        })
    }
}

// ---------------------------------------------------------------------------
// Helpers: retry/DLQ topic detection + IP range matching.
// Java: NamespaceUtil.isRetryTopic / isDLQTopic / withOutRetryAndDLQ, and
//       common.utils.IPAddressUtils.isIPInRange.
// ---------------------------------------------------------------------------

pub fn is_retry_topic(topic: &str) -> bool {
    topic.starts_with(RETRY_GROUP_TOPIC_PREFIX)
}

pub fn is_dlq_topic(topic: &str) -> bool {
    topic.starts_with(DLQ_GROUP_TOPIC_PREFIX)
}

pub fn with_out_retry_and_dlq(original: &str) -> String {
    if is_retry_topic(original) {
        original[RETRY_GROUP_TOPIC_PREFIX.len()..].to_string()
    } else if is_dlq_topic(original) {
        original[DLQ_GROUP_TOPIC_PREFIX.len()..].to_string()
    } else {
        original.to_string()
    }
}

/// Simplified `IPAddressUtils.isIPInRange`: supports exact match, `*` (any),
/// trailing-`*` prefix match, and CIDR (`a.b.c.d/n`).
pub fn is_ip_in_range(target: &str, source: &str) -> bool {
    let source = source.trim();
    if source.is_empty() || source == ASTERISK {
        return true;
    }
    if let Some((net, prefix)) = source.split_once('/') {
        if let Ok(prefix_len) = prefix.parse::<u8>() {
            return cidr_contains(net, prefix_len, target);
        }
        return false;
    }
    if source.contains('*') {
        let prefix = source.trim_end_matches('*').trim_end_matches('.');
        if prefix.is_empty() {
            return true;
        }
        return target == source || target.starts_with(prefix);
    }
    if let (Some(a), Some(b)) = (parse_ipv4(target), parse_ipv4(source)) {
        return a == b;
    }
    if let (Some(a), Some(b)) = (parse_ipv6(target), parse_ipv6(source)) {
        return a == b;
    }
    target == source || target == source.trim_end_matches('*')
}

fn parse_ipv4(ip: &str) -> Option<u32> {
    let mut parts = ip.trim().split('.');
    let mut addr: u32 = 0;
    for _ in 0..4 {
        let octet: u32 = parts.next()?.parse().ok()?;
        if octet > 255 {
            return None;
        }
        addr = (addr << 8) | octet;
    }
    if parts.next().is_some() {
        return None;
    }
    Some(addr)
}

/// `cidr_contains` dispatches on the address family: IPv4 first (legacy
/// path), then the full IPv6 implementation.
fn cidr_contains(net: &str, prefix_len: u8, target: &str) -> bool {
    if let (Some(a), Some(b)) = (parse_ipv4(net), parse_ipv4(target)) {
        if prefix_len == 0 {
            return true;
        }
        if prefix_len > 32 {
            return false;
        }
        let mask: u32 = if prefix_len == 32 {
            u32::MAX
        } else {
            (u32::MAX << (32 - prefix_len)) & u32::MAX
        };
        return (a & mask) == (b & mask);
    }
    cidr_contains_ipv6(net, prefix_len, target)
}

/// Java: IPAddressUtils.isIPInRange IPv6 branch — prefix match over the full
/// 128-bit address space (supports `::`-abbreviated and full-form literals).
fn cidr_contains_ipv6(net: &str, prefix_len: u8, target: &str) -> bool {
    let (net_addr, target_addr) = match (parse_ipv6(net), parse_ipv6(target)) {
        (Some(a), Some(b)) => (a, b),
        _ => return false,
    };
    if prefix_len == 0 {
        return true;
    }
    if prefix_len > 128 {
        return false;
    }
    if prefix_len == 128 {
        return net_addr == target_addr;
    }
    let mask = u128::MAX << (128 - prefix_len as u32);
    (net_addr & mask) == (target_addr & mask)
}

/// Parse an IPv6 literal into its 128-bit value. Supports full form, the `::`
/// zero-compression abbreviation (at most once), an embedded IPv4 tail
/// (`::ffff:192.168.0.1`), and a `%zone` suffix (stripped, ignored).
fn parse_ipv6(ip: &str) -> Option<u128> {
    let ip = ip.trim();
    // Strip the zone index: fe80::1%eth0.
    let ip = match ip.split_once('%') {
        Some((addr, _zone)) => addr,
        None => ip,
    };
    // Split off an embedded IPv4 tail and turn it into two trailing hextets.
    let mut tail_groups: Vec<u16> = Vec::new();
    let body = match ip.rfind(':') {
        Some(pos) if ip[pos + 1..].contains('.') => {
            let v4 = parse_ipv4(&ip[pos + 1..])?;
            tail_groups.push((v4 >> 16) as u16);
            tail_groups.push(v4 as u16);
            &ip[..pos]
        }
        _ => ip,
    };

    // Handle `::` compression: exactly one occurrence allowed.
    let (head, tail) = match body.find("::") {
        Some(pos) => {
            let rest = &body[pos + 2..];
            if rest.contains("::") {
                return None;
            }
            (&body[..pos], rest)
        }
        None => (body, ""),
    };

    let head_groups = parse_ipv6_groups(head)?;
    let mid_groups = parse_ipv6_groups(tail)?;
    let fixed = head_groups.len() + mid_groups.len() + tail_groups.len();

    let mut groups: Vec<u16> = Vec::with_capacity(8);
    groups.extend(head_groups);
    if body.contains("::") {
        if fixed >= 8 {
            return None;
        }
        for _ in 0..8 - fixed {
            groups.push(0);
        }
    } else if fixed != 8 {
        return None;
    }
    groups.extend(mid_groups);
    groups.extend(tail_groups);

    let mut addr: u128 = 0;
    for g in &groups {
        addr = (addr << 16) | u128::from(*g);
    }
    Some(addr)
}

/// Parse a `:`-separated list of hex hextets (possibly empty, possibly with a
/// single trailing ':' from a `::` split) into 16-bit groups.
fn parse_ipv6_groups(s: &str) -> Option<Vec<u16>> {
    let mut groups = Vec::new();
    if s.is_empty() {
        return Some(groups);
    }
    for part in s.split(':') {
        if part.is_empty() {
            // Only legal at the edges of a `::` split (leading/trailing colon).
            return None;
        }
        if part.len() > 4 {
            return None;
        }
        groups.push(u16::from_str_radix(part, 16).ok()?);
    }
    Some(groups)
}

// -- Validators. Java: IPAddressUtils.{isValidIp,isValidCidr,...} ------------

pub fn is_valid_ipv4(ip: &str) -> bool {
    parse_ipv4(ip).is_some()
}

pub fn is_valid_ipv6(ip: &str) -> bool {
    parse_ipv6(ip).is_some()
}

pub fn is_valid_ip(ip: &str) -> bool {
    is_valid_ipv4(ip) || is_valid_ipv6(ip)
}

pub fn is_valid_ipv4_cidr(cidr: &str) -> bool {
    match cidr.split_once('/') {
        Some((net, prefix)) => match prefix.parse::<u8>() {
            Ok(p) => (0..=32).contains(&p) && parse_ipv4(net).is_some(),
            Err(_) => false,
        },
        None => false,
    }
}

pub fn is_valid_ipv6_cidr(cidr: &str) -> bool {
    match cidr.split_once('/') {
        Some((net, prefix)) => match prefix.parse::<u8>() {
            Ok(p) => (0..=128).contains(&p) && parse_ipv6(net).is_some(),
            Err(_) => false,
        },
        None => false,
    }
}

pub fn is_valid_cidr(cidr: &str) -> bool {
    is_valid_ipv4_cidr(cidr) || is_valid_ipv6_cidr(cidr)
}

pub fn is_valid_ip_or_cidr(ip_or_cidr: &str) -> bool {
    is_valid_ip(ip_or_cidr) || is_valid_cidr(ip_or_cidr)
}

/// Re-export of `now_millis` so the module's time dependency is exercised and
/// available to callers building audits/timestamps.
pub fn current_time() -> i64 {
    now_millis()
}

// ---------------------------------------------------------------------------
// Unit tests.
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base64_roundtrip_known_vectors() {
        assert_eq!(base64_encode(b""), "");
        assert_eq!(base64_encode(b"hello"), "aGVsbG8=");
        assert_eq!(
            base64_encode(b"any carnal pleasure."),
            "YW55IGNhcm5hbCBwbGVhc3VyZS4="
        );
        // decode back
        assert_eq!(base64_decode("aGVsbG8=").unwrap(), b"hello");
        assert_eq!(
            base64_decode("YW55IGNhcm5hbCBwbGVhc3VyZS4=").unwrap(),
            b"any carnal pleasure."
        );
        assert!(base64_decode("!!!!").is_err());
    }

    #[test]
    fn resource_match_semantics() {
        // ANY type matches everything.
        let any = Resource::of_any(ResourceType::Any);
        assert!(any.is_match(&Resource::of_topic("X")));
        // type mismatch fails.
        assert!(!Resource::of_topic("X").is_match(&Resource::of_group("X")));
        // literal exact match.
        assert!(Resource::of_topic("A").is_match(&Resource::of_topic("A")));
        assert!(!Resource::of_topic("A").is_match(&Resource::of_topic("B")));
        // prefixed.
        let prefixed = Resource::from_key("Topic:Pre*").unwrap();
        assert!(prefixed.is_match(&Resource::of_topic("PrefixX")));
        assert!(!prefixed.is_match(&Resource::of_topic("Other")));
        // any pattern of the same type matches any name.
        let any_topic = Resource::from_key("Topic:*").unwrap();
        assert!(any_topic.is_match(&Resource::of_topic("Whatever")));
        // resource_key round trip.
        assert_eq!(
            Resource::from_key("Topic:foo").unwrap().resource_key(),
            "Topic:foo"
        );
        assert_eq!(
            Resource::from_key("Topic:*").unwrap().resource_key(),
            "Topic:*"
        );
        assert_eq!(
            Resource::from_key("Group:pre*").unwrap().resource_key(),
            "Group:pre*"
        );
        assert_eq!(Resource::from_key("*").unwrap().resource_key(), "*");
    }

    #[test]
    fn policy_priority_prefers_more_specific_resource_and_deny() {
        // DENY on a literal resource should outrank ALLOW on a prefixed one.
        let deny_literal = PolicyEntry::of(
            Resource::of_topic("A"),
            vec![Action::Pub],
            None,
            Decision::Deny,
        );
        let allow_prefixed = PolicyEntry::of(
            Resource::of(
                ResourceType::Topic,
                Some("A".into()),
                ResourcePattern::Prefixed,
            ),
            vec![Action::Pub],
            None,
            Decision::Allow,
        );
        // For an exact request "A", the literal (more specific) is chosen; deny wins.
        assert_eq!(
            compare_policy_entries(&deny_literal, &allow_prefixed),
            Ordering::Less
        );

        // Prefix length: longer prefix is more specific (comes first).
        let long = PolicyEntry::of(
            Resource::of(
                ResourceType::Topic,
                Some("Abc".into()),
                ResourcePattern::Prefixed,
            ),
            vec![Action::Pub],
            None,
            Decision::Allow,
        );
        let short = PolicyEntry::of(
            Resource::of(
                ResourceType::Topic,
                Some("A".into()),
                ResourcePattern::Prefixed,
            ),
            vec![Action::Pub],
            None,
            Decision::Allow,
        );
        assert_eq!(compare_policy_entries(&long, &short), Ordering::Less);

        // Equal resources: DENY first.
        let a_deny = PolicyEntry::of(
            Resource::of_topic("Z"),
            vec![Action::Pub],
            None,
            Decision::Deny,
        );
        let a_allow = PolicyEntry::of(
            Resource::of_topic("Z"),
            vec![Action::Pub],
            None,
            Decision::Allow,
        );
        assert_eq!(compare_policy_entries(&a_deny, &a_allow), Ordering::Less);
    }

    #[tokio::test]
    async fn authorization_allow_deny_super_user() {
        let acl_meta = LocalAuthorizationMetadataProvider::new();
        let auth_meta = LocalAuthenticationMetadataProvider::new();

        // Admin/super user: rocketmq.
        auth_meta
            .create_user(
                User::new("rocketmq")
                    .with_secret("secret")
                    .with_type(UserType::Super),
            )
            .await
            .unwrap();
        // Normal user: alice, with an explicit allow on TopicA PUB.
        auth_meta
            .create_user(User::new("alice").with_secret("alice-secret"))
            .await
            .unwrap();
        let acl = Acl::of_resources(
            Subject::User(User::new("alice")),
            vec![Resource::of_topic("TopicA")],
            vec![Action::Pub],
            None,
            Decision::Allow,
        );
        acl_meta.create_acl(acl).await.unwrap();

        let provider = DefaultAuthorizationProvider::new(
            AuthConfig {
                authorization_enabled: true,
                ..AuthConfig::default()
            },
            auth_meta,
            acl_meta,
        );

        // Super user is allowed even without an explicit ACL.
        let super_ctx = AuthorizationContext::of(
            Subject::User(User::new("rocketmq")),
            Resource::of_topic("NoSuchTopic"),
            vec![Action::Pub],
            "1.2.3.4",
        );
        assert!(provider.authorize(&super_ctx).await.is_ok());

        // Normal user allowed on TopicA/PUB.
        let allow_ctx = AuthorizationContext::of(
            Subject::User(User::new("alice")),
            Resource::of_topic("TopicA"),
            vec![Action::Pub],
            "1.2.3.4",
        );
        assert!(provider.authorize(&allow_ctx).await.is_ok());

        // Normal user denied on a topic with no matching policy.
        let deny_ctx = AuthorizationContext::of(
            Subject::User(User::new("alice")),
            Resource::of_topic("TopicB"),
            vec![Action::Pub],
            "1.2.3.4",
        );
        assert!(provider.authorize(&deny_ctx).await.is_err());

        // Normal user denied when requesting an action not granted.
        let wrong_action = AuthorizationContext::of(
            Subject::User(User::new("alice")),
            Resource::of_topic("TopicA"),
            vec![Action::Sub],
            "1.2.3.4",
        );
        assert!(provider.authorize(&wrong_action).await.is_err());
    }

    #[tokio::test]
    async fn authorization_respects_source_ip_environment() {
        let acl_meta = LocalAuthorizationMetadataProvider::new();
        let auth_meta = LocalAuthenticationMetadataProvider::new();
        auth_meta
            .create_user(User::new("bob").with_secret("s"))
            .await
            .unwrap();
        let acl = Acl::of_resources(
            Subject::User(User::new("bob")),
            vec![Resource::of_topic("T")],
            vec![Action::Pub],
            Environment::of("10.0.0.5"),
            Decision::Allow,
        );
        acl_meta.create_acl(acl).await.unwrap();
        let provider = DefaultAuthorizationProvider::new(
            AuthConfig {
                authorization_enabled: true,
                ..AuthConfig::default()
            },
            auth_meta,
            acl_meta,
        );

        let from_allowed = AuthorizationContext::of(
            Subject::User(User::new("bob")),
            Resource::of_topic("T"),
            vec![Action::Pub],
            "10.0.0.5",
        );
        assert!(provider.authorize(&from_allowed).await.is_ok());

        let from_denied = AuthorizationContext::of(
            Subject::User(User::new("bob")),
            Resource::of_topic("T"),
            vec![Action::Pub],
            "9.9.9.9",
        );
        assert!(provider.authorize(&from_denied).await.is_err());
    }

    #[tokio::test]
    async fn signature_verify_roundtrip() {
        // Build a canonical request content the same way the client would:
        // ext field values sorted by key, then the body, HMAC-SHA1 + Base64.
        let mut fields = BTreeMap::new();
        fields.insert("AccessKey".to_string(), "alice".to_string());
        fields.insert("topic".to_string(), "TopicA".to_string());
        fields.insert("Signature".to_string(), "placeholder".to_string());
        let body = b"body-data".to_vec();
        let content = combine_request_content(&fields, &body);
        let secret = "alice-secret";
        let signature = cal_signature(&content, secret);

        // Recompute independently and verify it is deterministic + matches.
        let again = cal_signature(&content, secret);
        assert_eq!(signature, again);
        assert!(!signature.is_empty());

        // A wrong secret must not validate.
        let wrong = cal_signature(&content, "other");
        assert_ne!(signature, wrong);

        // Feed through the authentication handler.
        let ctx = AuthenticationContext {
            username: "alice".into(),
            content,
            signature,
            rpc_code: request_code::SEND_MESSAGE.to_string(),
            channel_id: None,
        };
        let auth_meta = LocalAuthenticationMetadataProvider::new();
        auth_meta
            .create_user(User::new("alice").with_secret(secret))
            .await
            .unwrap();
        let provider = DefaultAuthenticationProvider::new(AuthConfig::default(), auth_meta);
        // authentication_enabled is false by default -> short-circuits to Ok.
        assert!(provider.authenticate(&ctx).await.is_ok());

        // With auth enabled it actually verifies the signature.
        let cfg = AuthConfig {
            authentication_enabled: true,
            ..AuthConfig::default()
        };
        let auth_meta2 = LocalAuthenticationMetadataProvider::new();
        auth_meta2
            .create_user(User::new("alice").with_secret(secret))
            .await
            .unwrap();
        let provider2 = DefaultAuthenticationProvider::new(cfg, auth_meta2);
        assert!(provider2.authenticate(&ctx).await.is_ok());

        let bad_ctx = AuthenticationContext {
            signature: "garbage".into(),
            ..ctx.clone()
        };
        assert!(provider2.authenticate(&bad_ctx).await.is_err());
    }

    #[test]
    fn plain_access_config_parse() {
        let json = r#"{
            "accessKey": "rocketmq",
            "secretKey": "12345678",
            "whiteRemoteAddress": "192.168.0.0/16",
            "admin": true,
            "defaultTopicPerm": "PUB|SUB",
            "defaultGroupPerm": "SUB",
            "topicPerms": ["TopicA=PUB", "TopicB=DENY"],
            "groupPerms": ["G1=SUB"]
        }"#;
        let cfg: PlainAccessConfig = serde_json::from_str(json).unwrap();
        assert_eq!(cfg.access_key, "rocketmq");
        assert_eq!(cfg.secret_key, "12345678");
        assert_eq!(cfg.white_remote_address, "192.168.0.0/16");
        assert!(cfg.admin);
        assert_eq!(cfg.default_topic_perm, "PUB|SUB");
        assert_eq!(cfg.topic_perms, vec!["TopicA=PUB", "TopicB=DENY"]);
        assert_eq!(cfg.group_perms, vec!["G1=SUB"]);
    }

    #[tokio::test]
    async fn migrator_builds_user_and_acl() {
        let acl_meta = LocalAuthorizationMetadataProvider::new();
        let auth_meta = LocalAuthenticationMetadataProvider::new();
        let migrator = AuthMigrator::new(auth_meta.clone(), acl_meta.clone());

        let account = PlainAccessConfig {
            access_key: "rocketmq".into(),
            secret_key: "secret".into(),
            white_remote_address: String::new(),
            admin: true,
            default_topic_perm: "PUB|SUB".into(),
            default_group_perm: String::new(),
            topic_perms: vec!["TopicA=PUB".into()],
            group_perms: vec!["G1=SUB".into()],
        };
        migrator.migrate(&[account]).await.unwrap();

        // User created as SUPER.
        let user = auth_meta.get_user("rocketmq").await.unwrap().unwrap();
        assert_eq!(user.user_type, UserType::Super);
        assert_eq!(user.password.as_deref(), Some("secret"));

        // ACL created with the expected policies.
        let acl = acl_meta
            .get_acl(&Subject::User(User::new("rocketmq")))
            .await
            .unwrap()
            .unwrap();
        assert!(acl.get_policy(PolicyType::Custom).is_some());
        assert!(acl.get_policy(PolicyType::Default).is_some());
        // TopicA=PUB -> ALLOW, TopicB-style DENY would be DENY; here TopicA is ALLOW.
        let custom = acl.get_policy(PolicyType::Custom).unwrap();
        assert!(
            custom.entries.iter().any(
                |e| e.resource == Resource::of_topic("TopicA") && e.decision == Decision::Allow
            )
        );
        let default = acl.get_policy(PolicyType::Default).unwrap();
        assert!(
            default
                .entries
                .iter()
                .any(|e| e.resource.resource_type == ResourceType::Topic
                    && e.actions.contains(&Action::Pub)
                    && e.actions.contains(&Action::Sub))
        );
    }

    #[test]
    fn subject_key_roundtrip() {
        let subject = Subject::User(User::new("rocketmq"));
        assert_eq!(subject.subject_key(), "User:rocketmq");
        let parsed = Subject::of("User:rocketmq").unwrap();
        assert_eq!(parsed, subject);
        assert!(Subject::of("Bogus:x").is_err());
    }

    #[test]
    fn ipv6_cidr_and_validators() {
        // Prefix matching within a /32.
        assert!(is_ip_in_range("2001:db8::1", "2001:db8::/32"));
        assert!(is_ip_in_range("2001:db8:abcd::9", "2001:db8::/32"));
        assert!(!is_ip_in_range("2001:db9::1", "2001:db8::/32"));
        // ::/0 matches everything (abbreviated zero network).
        assert!(is_ip_in_range("::1", "::/0"));
        assert!(is_ip_in_range("2001:db8::ffff", "::/0"));
        // /128 exact match.
        assert!(is_ip_in_range("2001:db8::1", "2001:db8::1/128"));
        assert!(!is_ip_in_range("2001:db8::2", "2001:db8::1/128"));
        // Abbreviated vs full form are the same address.
        assert!(is_ip_in_range(
            "2001:0db8:0000:0000:0000:0000:0000:0001",
            "2001:db8::1/128"
        ));
        // Embedded IPv4 tail.
        assert!(is_ip_in_range(
            "::ffff:192.168.1.9",
            "::ffff:192.168.0.0/112"
        ));
        assert!(!is_ip_in_range("::ffff:10.0.0.1", "::ffff:192.168.0.0/112"));
        // Zone index tolerated.
        assert!(is_ip_in_range("fe80::1%eth0", "fe80::/16"));
        // Exact (non-CIDR) IPv6 equality across spellings.
        assert!(is_ip_in_range("2001:db8::1", "2001:0db8::1"));
        // Malformed input never panics, just fails.
        assert!(!is_ip_in_range("garbage", "2001:db8::/32"));
        assert!(!is_ip_in_range("2001:db8::1", "bogus/32"));
        assert!(!is_ip_in_range("1:2:3:4:5:6:7:8:9", "2001:db8::/32"));

        // Validators (Java IPAddressUtils parity).
        assert!(is_valid_ipv6("2001:db8::1"));
        assert!(is_valid_ipv6("::"));
        assert!(!is_valid_ipv6("2001:db8:::1"));
        assert!(!is_valid_ipv6("1.2.3.4"));
        assert!(is_valid_ipv4("1.2.3.4"));
        assert!(!is_valid_ipv4("2001:db8::1"));
        assert!(is_valid_ipv6_cidr("2001:db8::/32"));
        assert!(is_valid_ipv6_cidr("::/0"));
        assert!(!is_valid_ipv6_cidr("2001:db8::/129"));
        assert!(!is_valid_ipv6_cidr("2001:db8::"));
        assert!(is_valid_ipv4_cidr("10.0.0.0/8"));
        assert!(!is_valid_ipv4_cidr("10.0.0.0/33"));
        assert!(is_valid_cidr("2001:db8::/32"));
        assert!(is_valid_ip_or_cidr("10.0.0.0/8"));
        assert!(is_valid_ip_or_cidr("::1"));
        assert!(!is_valid_ip_or_cidr("nope"));

        // Legacy IPv4 path unchanged.
        assert!(is_ip_in_range("192.168.1.5", "192.168.0.0/16"));
        assert!(!is_ip_in_range("10.1.1.1", "192.168.0.0/16"));
        assert!(is_ip_in_range("192.168.1.5", "192.168.*"));
    }

    #[test]
    fn v2_signing_and_grpc_header_context() {
        // Java DefaultAuthenticationContextBuilderTest#build1 vector: the hex
        // wire signature converts to the Base64 verification form.
        let base64 = hex_to_base64("D18A9CBCDDBA9041D6693268FEF15A989E64430B").unwrap();
        assert_eq!(base64, "0YqcvN26kEHWaTJo/vFamJ5kQws=");
        assert!(hex_to_base64("zz").is_err());
        assert_eq!(hex_encode(&hex_decode("D18A9C").unwrap()), "d18a9c");

        // canonical_v2_content sorts headers by name and joins `k:v\n`.
        let mut headers = HashMap::new();
        headers.insert("x-mq-client-id".to_string(), "cid".to_string());
        headers.insert("x-mq-date-time".to_string(), "20231227T194619Z".to_string());
        let content = canonical_v2_content(&headers);
        assert_eq!(
            String::from_utf8(content.clone()).unwrap(),
            "x-mq-client-id:cid\nx-mq-date-time:20231227T194619Z\n"
        );

        // sign_v2 = HMAC-SHA1 + Base64; deterministic and key-sensitive.
        let sig = sign_v2(&content, "secret");
        assert_eq!(sig, cal_signature(&content, "secret"));
        assert_ne!(sig, sign_v2(&content, "other"));
        // Hex form decodes back to the same digest as the Base64 form.
        let hex_sig = sign_v2_hex(&content, "secret");
        assert_eq!(hex_to_base64(&hex_sig).unwrap(), sig);

        // Full gRPC header context extraction (Java build1 shape).
        let mut grpc = HashMap::new();
        grpc.insert(
            "authorization".to_string(),
            "MQv2-HMAC-SHA1 Credential=abc, SignedHeaders=x-mq-date-time, \
             Signature=D18A9CBCDDBA9041D6693268FEF15A989E64430B"
                .to_string(),
        );
        grpc.insert("x-mq-date-time".to_string(), "20231227T194619Z".to_string());
        grpc.insert(
            "rpc-remote-address".to_string(),
            "10.0.0.7:9876".to_string(),
        );
        grpc.insert(
            "x-mq-rpc-name".to_string(),
            "apache.rocketmq.v2.SendMessage".to_string(),
        );
        grpc.insert("x-mq-client-id".to_string(), "client-1".to_string());
        let ctx = context_from_grpc_headers(&grpc).unwrap();
        assert_eq!(ctx.subject.username(), "abc");
        assert_eq!(ctx.source_ip, "10.0.0.7");
        assert_eq!(ctx.rpc_code, "apache.rocketmq.v2.SendMessage");

        // Credential with a datetime segment: username is the first part.
        grpc.insert(
            "authorization".to_string(),
            "MQv2-HMAC-SHA1 Credential=alice/20231227T194619Z, Signature=00".to_string(),
        );
        let ctx2 = context_from_grpc_headers(&grpc).unwrap();
        assert_eq!(ctx2.subject.username(), "alice");

        // No authorization header at all: anonymous context, no error.
        let anon = context_from_grpc_headers(&HashMap::new()).unwrap();
        assert_eq!(anon.subject.username(), "");

        // Missing datetime with authorization present: rejected (Java parity).
        let mut no_dt = grpc.clone();
        no_dt.remove("x-mq-date-time");
        assert!(context_from_grpc_headers(&no_dt).is_err());

        // Malformed authorization header: rejected.
        let mut bad = grpc.clone();
        bad.insert("authorization".to_string(), "MQv2-HMAC-SHA1".to_string());
        assert!(context_from_grpc_headers(&bad).is_err());
        bad.insert(
            "authorization".to_string(),
            "Basic abc, Signature=00".to_string(),
        );
        assert!(context_from_grpc_headers(&bad).is_err());
    }

    #[tokio::test]
    async fn ttl_cache_expires() {
        let cache: TtlCache<String> = TtlCache::new(Duration::from_millis(50));
        cache.insert("alice", "policy-a".to_string()).await;
        assert_eq!(cache.get("alice").await.as_deref(), Some("policy-a"));
        assert_eq!(cache.len().await, 1);

        // A different key is a miss.
        assert!(cache.get("bob").await.is_none());

        // After the TTL elapses the entry expires lazily.
        tokio::time::sleep(Duration::from_millis(80)).await;
        assert!(cache.get("alice").await.is_none());
        assert!(cache.is_empty().await);

        // remove/clear work.
        cache.insert("carol", "p".to_string()).await;
        cache.remove("carol").await;
        assert!(cache.get("carol").await.is_none());
        cache.insert("dave", "p".to_string()).await;
        cache.clear().await;
        assert!(cache.is_empty().await);
    }

    #[tokio::test]
    async fn pluggable_strategy_provider() {
        // A custom strategy provider with ONLY the ACL handler: no super-user
        // short-circuit, i.e. Super users are judged by their ACLs like
        // everyone else. Default behavior is covered by the tests above.
        struct AclOnly {
            acl_metadata: Arc<LocalAuthorizationMetadataProvider>,
        }
        impl AuthorizationStrategyProvider for AclOnly {
            fn handlers(&self) -> Vec<AuthorizationHandler> {
                vec![AuthorizationHandler::Acl(AclAuthorizationHandler::new(
                    self.acl_metadata.clone(),
                ))]
            }
        }

        let auth_meta = LocalAuthenticationMetadataProvider::new();
        let acl_meta = LocalAuthorizationMetadataProvider::new();
        auth_meta
            .create_user(
                User::new("rocketmq")
                    .with_secret("s")
                    .with_type(UserType::Super),
            )
            .await
            .unwrap();
        // Super user has NO acl -> with the AclOnly strategy it is denied.
        let provider = DefaultAuthorizationProvider::new_with_strategy_provider(
            AuthConfig {
                authorization_enabled: true,
                ..AuthConfig::default()
            },
            auth_meta.clone(),
            acl_meta.clone(),
            Arc::new(AclOnly {
                acl_metadata: acl_meta.clone(),
            }),
        );
        let super_ctx = AuthorizationContext::of(
            Subject::User(User::new("rocketmq")),
            Resource::of_topic("T"),
            vec![Action::Pub],
            "1.2.3.4",
        );
        assert!(provider.authorize(&super_ctx).await.is_err());

        // Granting an ACL lets the request through.
        acl_meta
            .create_acl(Acl::of_resources(
                Subject::User(User::new("rocketmq")),
                vec![Resource::of_topic("T")],
                vec![Action::Pub],
                None,
                Decision::Allow,
            ))
            .await
            .unwrap();
        assert!(provider.authorize(&super_ctx).await.is_ok());
    }

    #[tokio::test]
    async fn authorization_with_cache_and_audit_enabled() {
        // audit_enabled defaults to true and cache_ttl_seconds defaults to 30;
        // authorization semantics must be unchanged with both active.
        assert!(AuthConfig::default().audit_enabled);
        assert_eq!(AuthConfig::default().cache_ttl_seconds, 30);

        let acl_meta = LocalAuthorizationMetadataProvider::new();
        let auth_meta = LocalAuthenticationMetadataProvider::new();
        auth_meta
            .create_user(User::new("carol").with_secret("s"))
            .await
            .unwrap();
        acl_meta
            .create_acl(Acl::of_resources(
                Subject::User(User::new("carol")),
                vec![Resource::of_topic("T")],
                vec![Action::Pub],
                None,
                Decision::Allow,
            ))
            .await
            .unwrap();
        let provider = DefaultAuthorizationProvider::new(
            AuthConfig {
                authorization_enabled: true,
                audit_enabled: true,
                cache_ttl_seconds: 30,
                ..AuthConfig::default()
            },
            auth_meta,
            acl_meta.clone(),
        );
        let ctx = AuthorizationContext::of(
            Subject::User(User::new("carol")),
            Resource::of_topic("T"),
            vec![Action::Pub],
            "1.2.3.4",
        );
        // First call populates the cache, second call hits it.
        assert!(provider.authorize(&ctx).await.is_ok());
        assert!(provider.authorize(&ctx).await.is_ok());

        // Denials still logged + propagated with audit on.
        let deny_ctx = AuthorizationContext::of(
            Subject::User(User::new("carol")),
            Resource::of_topic("Other"),
            vec![Action::Pub],
            "1.2.3.4",
        );
        assert!(provider.authorize(&deny_ctx).await.is_err());

        // Updating the ACL is not blocked by the cache (TTL staleness only).
        acl_meta
            .create_acl(Acl::of_resources(
                Subject::User(User::new("dave")),
                vec![Resource::of_topic("T")],
                vec![Action::Pub],
                None,
                Decision::Allow,
            ))
            .await
            .unwrap();
        let dave_ctx = AuthorizationContext::of(
            Subject::User(User::new("dave")),
            Resource::of_topic("T"),
            vec![Action::Pub],
            "1.2.3.4",
        );
        assert!(provider.authorize(&dave_ctx).await.is_ok());
    }

    #[test]
    fn audit_logger_emits_without_panic() {
        let ctx = AuthorizationContext::of(
            Subject::User(User::new("alice")),
            Resource::of_topic("TopicA"),
            vec![Action::Pub],
            "1.2.3.4",
        );
        AuditLogger::log_authorization(&ctx, &Ok(()));
        AuditLogger::log_authorization(&ctx, &Err(Error::Auth("denied".into())));
    }
}
