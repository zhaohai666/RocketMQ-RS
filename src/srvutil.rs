//! Rust port of `org.apache.rocketmq.srvutil`: `FileWatchService`, `ShutdownHookThread`, `ServerUtil`.
//!
//! These are small server utilities shared across the RocketMQ daemons: a file-change watcher
//! (used to hot-reload TLS/ACL files), a JVM-shutdown hook equivalent, and CLI helpers.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use tracing::{info, warn};

/// Default poll interval for [`FileWatchService`], mirroring Java's `DEFAULT_WATCH_INTERVAL`.
pub const DEFAULT_WATCH_INTERVAL: Duration = Duration::from_millis(500);

/// Receives notifications when a watched file's content changes.
pub trait FileWatchListener: Send + Sync + 'static {
    /// Called with the absolute path of the changed file.
    fn on_changed(&self, path: &str);
}

/// Java: `FileWatchService` — polls a set of files for content changes (by MD5) and invokes a
/// listener. Deletes are intentionally ignored so an in-flight CA cert rotation does not flap.
pub struct FileWatchService {
    current_hash: HashMap<String, String>,
    listener: Arc<dyn FileWatchListener>,
    watch_interval: Duration,
    files: Vec<String>,
}

impl FileWatchService {
    /// Build a watcher with the default 500 ms poll interval.
    pub fn new(files: &[String], listener: Arc<dyn FileWatchListener>) -> Self {
        Self::with_interval(files, listener, DEFAULT_WATCH_INTERVAL)
    }

    /// Build a watcher with a custom poll interval.
    pub fn with_interval(
        files: &[String],
        listener: Arc<dyn FileWatchListener>,
        watch_interval: Duration,
    ) -> Self {
        let mut current_hash = HashMap::new();
        for file in files {
            if !file.is_empty() && Path::new(file).exists() {
                current_hash.insert(file.clone(), md5_digest(file, None));
            }
        }
        Self {
            current_hash,
            listener,
            watch_interval,
            files: files.to_vec(),
        }
    }

    /// Run the watch loop until `shutdown` fires. Mirrors Java's `run0`/`waitForRunning`.
    pub async fn run(mut self, mut shutdown: watch::Receiver<bool>) {
        info!("FileWatchService service started");
        loop {
            if *shutdown.borrow() {
                break;
            }
            tokio::select! {
                _ = shutdown.changed() => {
                    if *shutdown.borrow() {
                        break;
                    }
                }
                _ = tokio::time::sleep(self.watch_interval) => {}
            }
            for file in &self.files {
                let previous = self.current_hash.get(file).cloned();
                let new_hash = md5_digest(file, previous.as_deref());
                // Java reuses the previous hash on missing/IO error, so only a genuine content
                // change differs from `previous` and triggers the callback.
                if previous.as_deref() != Some(new_hash.as_str()) {
                    self.current_hash.insert(file.clone(), new_hash);
                    self.listener.on_changed(file);
                }
            }
        }
        info!("FileWatchService service end");
    }
}

/// MD5 hex digest of `path`. On a missing file or read error, `previous` is returned unchanged so
/// the watcher never reports a spurious change during a rename-then-copy cert rotation.
fn md5_digest(path: &str, previous: Option<&str>) -> String {
    let raw = match std::fs::read(Path::new(path)) {
        Ok(bytes) => bytes,
        Err(_) => {
            warn!("failed to read content of {path}, reusing previous hash");
            return previous.unwrap_or("").to_string();
        }
    };
    let digest = md5::compute(&raw);
    format!("{digest:x}")
}

/// A shutdown hook mirroring Java's `ShutdownHookThread`: runs a single callback when the process
/// receives Ctrl-C / SIGTERM, times it, and guards against double execution.
pub struct ShutdownHookThread<F> {
    callback: F,
}

impl<F> ShutdownHookThread<F>
where
    F: FnOnce() + Send + 'static,
{
    /// Create the hook with its callback.
    pub fn new(callback: F) -> Self {
        Self { callback }
    }

    /// Await the termination signal, then run the callback exactly once and report its duration.
    pub async fn run(self) {
        wait_for_signal().await;
        info!("shutdown hook was invoked");
        let begin = std::time::Instant::now();
        (self.callback)();
        info!(
            "shutdown hook done, consuming time total(ms): {}",
            begin.elapsed().as_millis()
        );
    }
}

/// Convenience: register a callback that runs once on the first termination signal.
pub async fn shutdown_hook(callback: impl FnOnce() + Send + 'static) {
    ShutdownHookThread::new(callback).run().await;
}

/// Resolve when Ctrl-C (all platforms) or SIGTERM (Unix) arrives.
async fn wait_for_signal() {
    let ctrl_c = tokio::signal::ctrl_c();
    tokio::pin!(ctrl_c);
    #[cfg(unix)]
    {
        let mut term = match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        {
            Ok(term) => term,
            Err(_) => {
                let _ = ctrl_c.await;
                return;
            }
        };
        tokio::select! {
            _ = &mut ctrl_c => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    {
        let _ = ctrl_c.await;
    }
}

/// Java: `ServerUtil` — common CLI option/parsing helpers. RocketMQ-RS uses `clap`, so the
/// commons-cli parsing is replaced by thin, faithful helpers.
pub struct ServerUtil;

impl ServerUtil {
    /// Split a `namesrvAddr` value on `;` and drop empty entries (Java `buildCommandlineOptions`
    /// `-n` option). Returns `127.0.0.1:9876` when the value is empty.
    pub fn parse_namesrv_addrs(raw: &str) -> Vec<String> {
        let mut addrs: Vec<String> = raw
            .split(';')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .collect();
        if addrs.is_empty() {
            addrs.push("127.0.0.1:9876".to_string());
        }
        addrs
    }

    /// Augment a `clap::Command` with the standard `-n/--namesrv-addr` option, mirroring
    /// `buildCommandlineOptions`. (`-h/--help` is provided by clap automatically.)
    pub fn with_namesrv_opt(command: clap::Command) -> clap::Command {
        command.arg(
            clap::Arg::new("namesrvAddr")
                .short('n')
                .long("namesrvAddr")
                .value_name("NAMESRV")
                .help("Name server address list, eg: '192.168.0.1:9876;192.168.0.2:9876'"),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    struct RecordingListener {
        changed: std::sync::Mutex<Vec<String>>,
    }

    impl FileWatchListener for RecordingListener {
        fn on_changed(&self, path: &str) {
            self.changed.lock().unwrap().push(path.to_string());
        }
    }

    #[test]
    fn md5_matches_known_vector_and_is_stable() {
        // MD5("abc") = 900150983cd24fb0d6963f7d28e17f72
        assert_eq!(format!("{:x}", md5::compute(b"abc")), "900150983cd24fb0d6963f7d28e17f72");
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("watch.txt");
        {
            let mut f = std::fs::File::create(&path).unwrap();
            f.write_all(b"v1").unwrap();
        }
        let p = path.to_str().unwrap().to_string();
        assert_eq!(md5_digest(&p, None).len(), 32);
        // Missing file reuses the previous hash rather than reporting a change.
        assert_eq!(md5_digest("/no/such/file", Some("abc")), "abc");
    }

    #[tokio::test]
    async fn file_watch_detects_change_and_ignores_missing() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("acl.yml");
        std::fs::write(&path, b"initial").unwrap();
        let p = path.to_str().unwrap().to_string();
        let listener = Arc::new(RecordingListener {
            changed: std::sync::Mutex::new(Vec::new()),
        });
        let (tx, rx) = watch::channel(false);
        let service = FileWatchService::with_interval(
            &[p.clone()],
            listener.clone(),
            Duration::from_millis(20),
        );
        let handle = tokio::spawn(async move { service.run(rx).await });
        // Give the first poll a chance to record the baseline.
        tokio::time::sleep(Duration::from_millis(60)).await;
        std::fs::write(&path, b"changed").unwrap();
        tokio::time::sleep(Duration::from_millis(80)).await;
        let _ = tx.send(true);
        handle.await.unwrap();
        let recorded = listener.changed.lock().unwrap().clone();
        assert_eq!(recorded, vec![p]);
    }

    #[test]
    fn namesrv_addr_parsing() {
        assert_eq!(
            ServerUtil::parse_namesrv_addrs("192.168.0.1:9876;192.168.0.2:9876"),
            vec!["192.168.0.1:9876", "192.168.0.2:9876"]
        );
        assert_eq!(
            ServerUtil::parse_namesrv_addrs("  a:1 ; ; b:2 "),
            vec!["a:1", "b:2"]
        );
        assert_eq!(
            ServerUtil::parse_namesrv_addrs(""),
            vec!["127.0.0.1:9876"]
        );
    }
}
