//! Client for the Java parity driver JAR.
//!
//! Launches the driver as a subprocess, speaks newline-delimited JSON
//! over stdin/stdout. Each parity case can extend the ops the driver JAR
//! handles on the Java side.

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};
use tokio::sync::Mutex;

pub struct JavaDriver {
    child: Child,
    stdin: Mutex<ChildStdin>,
    stdout: Mutex<BufReader<ChildStdout>>,
}

#[derive(Debug, Serialize)]
pub struct Request<'a> {
    pub id: String,
    pub op: &'a str,
    #[serde(flatten)]
    pub extra: Value,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct Response {
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub ok: bool,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(flatten)]
    pub body: Value,
}

impl JavaDriver {
    pub async fn start() -> Self {
        let jar = driver_jar_path();
        assert!(
            jar.exists(),
            "parity driver JAR not built at {} — run `mvn -q -e package` in tests/java-parity-driver/",
            jar.display()
        );
        // Ignite 2.17 uses reflection on java.nio.DirectByteBuffer.address.
        // Java 21 requires --add-opens to permit this.
        let mut cmd = Command::new("java");
        cmd.arg("--add-opens=java.base/java.nio=ALL-UNNAMED")
            .arg("--add-opens=java.base/sun.nio.ch=ALL-UNNAMED")
            .arg("--add-opens=java.base/java.lang=ALL-UNNAMED")
            .arg("--add-opens=java.base/java.util=ALL-UNNAMED")
            .arg("-jar")
            .arg(&jar);
        cmd.stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit());
        let mut child = cmd.spawn().expect("failed to spawn parity driver");
        let stdin = Mutex::new(child.stdin.take().unwrap());
        let stdout = Mutex::new(BufReader::new(child.stdout.take().unwrap()));
        Self {
            child,
            stdin,
            stdout,
        }
    }

    pub async fn call(&self, req: Request<'_>) -> Response {
        let line = serde_json::to_string(&req).expect("serialize request");
        {
            let mut w = self.stdin.lock().await;
            w.write_all(line.as_bytes()).await.expect("write request");
            w.write_all(b"\n").await.expect("write newline");
            w.flush().await.expect("flush stdin");
        }
        let mut r = self.stdout.lock().await;
        let mut buf = String::new();
        let n = r.read_line(&mut buf).await.expect("read response line");
        assert!(n > 0, "driver closed stdout without response");
        serde_json::from_str(&buf)
            .unwrap_or_else(|e| panic!("bad JSON from driver: {} ({})", buf.trim(), e))
    }

    pub async fn shutdown(mut self) {
        // Kill the child — simpler than waiting for a clean stdin close.
        // Tests don't need to measure driver shutdown cleanliness; they
        // just need the subprocess to exit so the test can return.
        let _ = self.child.kill().await;
        let _ = self.child.wait().await;
    }
}

fn driver_jar_path() -> PathBuf {
    // CARGO_MANIFEST_DIR = .../ignite-rs/ignite-rs
    let mft = Path::new(env!("CARGO_MANIFEST_DIR")).to_path_buf();
    mft.parent()
        .expect("ignite-rs crate has a parent")
        .join("tests/java-parity-driver/target/ignite-rs-parity-driver.jar")
}

/// `true` iff the parity driver JAR is built. Tests call this to skip
/// when `mvn package` hasn't run.
pub fn driver_jar_built() -> bool {
    driver_jar_path().exists()
}
