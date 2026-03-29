use bollard::container::{
    Config as DockerContainerConfig, CreateContainerOptions, ListContainersOptions, LogOutput,
    LogsOptions, NetworkingConfig, RemoveContainerOptions, StartContainerOptions,
    StopContainerOptions,
};
use bollard::image::CreateImageOptions;
use bollard::models::{EndpointIpamConfig, EndpointSettings, HostConfig, PortBinding};
use bollard::network::CreateNetworkOptions;
use bollard::Docker;
use futures_util::StreamExt;
use ignite_rs::cluster::ClusterState;
use ignite_rs::error::{IgniteError, IgniteResult};
use ignite_rs::query::SqlFieldsQuery;
#[cfg(feature = "ssl")]
use ignite_rs::{client_config_from_ca_and_client_pem, client_config_from_ca_pem};
use ignite_rs::{new_client, Client, ClientConfig};
use std::collections::HashMap;
use std::env;
use std::fs;
use std::future::Future;
use std::io;
use std::net::{Ipv4Addr, Shutdown, TcpListener, TcpStream};
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock, Weak};
use std::thread;
use std::time::{Duration, Instant};
use testcontainers::{GenericImage, ImageArgs, RunnableImage};
use tokio::runtime::{Builder as TokioRuntimeBuilder, Handle as TokioHandle};

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const DEFAULT_IGNITE_IMAGE: &str = "apacheignite/ignite";
const DEFAULT_IGNITE_TAG: &str = "2.17.0-arm64";
const DOCKER_API_TIMEOUT: Duration = Duration::from_secs(30);
const SINGLE_NODE_READY_TIMEOUT: Duration = Duration::from_secs(300);
const CLUSTER_READY_TIMEOUT: Duration = Duration::from_secs(480);
const CHURN_RESET_READY_TIMEOUT: Duration = Duration::from_secs(20);
const READY_PROBE_TIMEOUT: Duration = Duration::from_secs(15);
const IMAGE_PULL_TIMEOUT: Duration = Duration::from_secs(300);
const LOG_READY_TIMEOUT: Duration = Duration::from_secs(300);
const DEFAULT_START_RETRIES: usize = 120;
const DEFAULT_START_DELAY_MS: u64 = 500;
const BASE_CLUSTER_NODE_COUNT: usize = 3;
const DISCOVERY_CLUSTER_NODE_COUNT: usize = 4;
const CLUSTER_3_BASE_PORT: u16 = 12100;
const CLUSTER_3_CHURN_BASE_PORT: u16 = 12400;
const IGNITE_PORT: u16 = 10800;
const SINGLE_NODE_PROFILE: &str = "single-node";
const SINGLE_NODE_CHURN_PROFILE: &str = "single-node-churn";
const SINGLE_NODE_AUTH_PROFILE: &str = "single-node-auth";
const SINGLE_NODE_TLS_PROFILE: &str = "single-node-tls";
const SINGLE_NODE_MTLS_PROFILE: &str = "single-node-mtls";
const CLUSTER_3_PROFILE: &str = "cluster-3";
const CLUSTER_3_CHURN_PROFILE: &str = "cluster-3-churn";
const DEFAULT_AUTH_USERNAME: &str = "ignite";
const DEFAULT_AUTH_PASSWORD: &str = "ignite";
const FIXTURE_MANAGED_LABEL: &str = "io.github.ignite-rs.fixture.managed";
const FIXTURE_PROFILE_LABEL: &str = "io.github.ignite-rs.fixture.profile";
const FIXTURE_RESOURCE_LABEL: &str = "io.github.ignite-rs.fixture.resource";
#[cfg(feature = "ssl")]
const DEFAULT_TLS_SERVER_NAME: &str = "ignite.apache.org";
const CONTAINER_SSL_ASSETS_DIR: &str = "/opt/ignite/apache-ignite/config/codex-ssl";
const LOG_READY_MESSAGE: &str = "Topology snapshot";

// ---------------------------------------------------------------------------
// Statics
// ---------------------------------------------------------------------------

static SHARED_ENV: OnceLock<Arc<IgniteTestEnv>> = OnceLock::new();
static SHARED_SINGLE_NODE_CHURN_ENV: OnceLock<Arc<IgniteTestEnv>> = OnceLock::new();
static SHARED_AUTH_ENV: OnceLock<Arc<IgniteTestEnv>> = OnceLock::new();
static SHARED_CLUSTER_3_ENV: OnceLock<Arc<IgniteClusterEnv>> = OnceLock::new();
static SHARED_CLUSTER_3_CHURN_ENV: OnceLock<Arc<IgniteClusterEnv>> = OnceLock::new();
#[cfg(feature = "ssl")]
static SHARED_TLS_ENV: OnceLock<Arc<IgniteTestEnv>> = OnceLock::new();
#[cfg(feature = "ssl")]
static SHARED_MTLS_ENV: OnceLock<Arc<IgniteTestEnv>> = OnceLock::new();
static CONTEXT_REGISTRY: OnceLock<Mutex<HashMap<String, Weak<IgniteContext>>>> = OnceLock::new();
static DOCKER: OnceLock<Docker> = OnceLock::new();

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum IgniteProfile {
    DefaultSingleNode,
    SingleNodeChurn,
    ThreeNodeCluster,
    ThreeNodeClusterChurn,
    AuthSingleNode,
    CustomClientPort(u16),
    #[cfg(feature = "ssl")]
    TlsSingleNode,
    #[cfg(feature = "ssl")]
    MtlsSingleNode,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum FixtureScope {
    Process,
    CargoSession,
}

#[derive(Clone, Debug)]
pub struct IgniteScope {
    context: Arc<IgniteContext>,
}

#[derive(Clone, Debug)]
pub struct IgniteContext {
    profile: IgniteProfile,
    scope: FixtureScope,
    kind: IgniteContextKind,
}

// ---------------------------------------------------------------------------
// Internal types
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
enum IgniteContextKind {
    Single(Arc<IgniteTestEnv>),
    Cluster(Arc<IgniteClusterEnv>),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ProfileKind {
    Single,
    Cluster,
}

#[derive(Clone, Copy, Debug)]
struct ProfileDescriptor {
    profile: IgniteProfile,
    key: &'static str,
    kind: ProfileKind,
    default_scope: FixtureScope,
}

enum TestEnvHandle {
    Single(Arc<IgniteTestEnv>),
    Cluster(Arc<IgniteClusterEnv>),
}

// ---------------------------------------------------------------------------
// IgniteTestEnv
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct IgniteTestEnv {
    addr: String,
    username: Option<String>,
    password: Option<String>,
    tls_server_name: Option<String>,
    ca_pem: Option<String>,
    client_cert_pem: Option<String>,
    client_key_pem: Option<String>,
    managed: Option<ManagedContainer>,
}

impl IgniteTestEnv {
    fn external(addr: String) -> Self {
        Self {
            addr,
            username: None,
            password: None,
            tls_server_name: None,
            ca_pem: None,
            client_cert_pem: None,
            client_key_pem: None,
            managed: None,
        }
    }

    fn external_auth(addr: String, username: String, password: String) -> Self {
        Self {
            addr,
            username: Some(username),
            password: Some(password),
            tls_server_name: None,
            ca_pem: None,
            client_cert_pem: None,
            client_key_pem: None,
            managed: None,
        }
    }

    #[cfg(feature = "ssl")]
    fn external_tls(
        addr: String,
        server_name: String,
        ca_pem: String,
        client_cert_pem: Option<String>,
        client_key_pem: Option<String>,
    ) -> Self {
        Self {
            addr,
            username: None,
            password: None,
            tls_server_name: Some(server_name),
            ca_pem: Some(ca_pem),
            client_cert_pem,
            client_key_pem,
            managed: None,
        }
    }

    fn containerized(profile: &str) -> Self {
        let managed = block_on_fixture(create_single_node(profile));
        let addr = format!("{}:{}", docker_host_addr(), managed.port);
        let (username, password) = auth_defaults_for_profile(profile);
        let (tls_server_name, ca_pem, client_cert_pem, client_key_pem) =
            tls_defaults_for_profile(profile);

        Self {
            addr,
            username,
            password,
            tls_server_name,
            ca_pem,
            client_cert_pem,
            client_key_pem,
            managed: Some(managed),
        }
    }

    pub fn addr(&self) -> &str {
        &self.addr
    }

    pub fn is_managed(&self) -> bool {
        self.managed.is_some()
    }

    pub fn username(&self) -> Option<&str> {
        self.username.as_deref()
    }

    pub fn password(&self) -> Option<&str> {
        self.password.as_deref()
    }

    pub fn tls_server_name(&self) -> Option<&str> {
        self.tls_server_name.as_deref()
    }

    pub fn ca_pem(&self) -> Option<&str> {
        self.ca_pem.as_deref()
    }

    pub fn client_cert_pem(&self) -> Option<&str> {
        self.client_cert_pem.as_deref()
    }

    pub fn client_key_pem(&self) -> Option<&str> {
        self.client_key_pem.as_deref()
    }

    pub fn client_config(&self) -> IgniteResult<ClientConfig> {
        let mut conf = build_fixture_client_config(
            self.addr(),
            self.tls_server_name(),
            self.ca_pem(),
            self.client_cert_pem(),
            self.client_key_pem(),
        )?;
        conf.username = self.username.clone();
        conf.password = self.password.clone();
        // Single-node fixtures run in containers whose advertised endpoint
        // (internal container IP) is unreachable from the host.  Partition
        // awareness would try to connect to that internal address and break
        // the channel.  Tests that need PA override the config explicitly.
        conf.partition_awareness_enabled = false;
        Ok(conf)
    }

    pub fn stop(&self) {
        let m = self
            .managed
            .as_ref()
            .expect("control requires managed fixture");
        stop_container(&m.name);
    }

    pub fn start(&self) {
        let m = self
            .managed
            .as_ref()
            .expect("control requires managed fixture");
        start_container(&m.name);
        let addr = format!("{}:{}", docker_host_addr(), m.port);
        block_on_fixture(wait_for_tcp_ready(&addr, LOG_READY_TIMEOUT));
        ensure_profile_container_ready(&m.name, &m.profile);
    }

    pub fn restart(&self) {
        self.stop();
        self.start();
    }

    pub async fn wait_for_ready(&self) -> IgniteResult<()> {
        wait_for_client_ready(self.client_config()?).await
    }
}

// ---------------------------------------------------------------------------
// IgniteClusterEnv
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct IgniteClusterEnv {
    addrs: Vec<String>,
    managed: Option<ManagedCluster>,
}

impl IgniteClusterEnv {
    fn external(addrs: Vec<String>) -> Self {
        Self {
            addrs,
            managed: None,
        }
    }

    fn containerized(profile: &str) -> Self {
        let managed = block_on_fixture(create_cluster(profile));
        let addrs = managed.addresses();
        Self {
            addrs,
            managed: Some(managed),
        }
    }

    fn borrowed(addrs: Vec<String>, prefix: String, network_name: String, profile: &str) -> Self {
        let managed = ManagedCluster {
            profile: profile.to_string(),
            name_prefix: prefix,
            network_name,
            owned: false,
        };
        Self {
            addrs,
            managed: Some(managed),
        }
    }

    pub fn addr(&self) -> &str {
        self.addrs
            .first()
            .expect("expected at least one cluster address")
    }

    pub fn addresses(&self) -> &[String] {
        &self.addrs
    }

    pub fn node_addr(&self, index: usize) -> String {
        if let Some(m) = &self.managed {
            return m.node_address(index);
        }
        self.addrs
            .get(index)
            .cloned()
            .expect("cluster node index out of bounds for external fixture")
    }

    pub fn is_managed(&self) -> bool {
        self.managed.is_some()
    }

    pub fn stop_node(&self, index: usize) {
        self.managed
            .as_ref()
            .expect("control requires managed fixture")
            .stop_node(index);
    }

    pub fn start_node(&self, index: usize) {
        self.managed
            .as_ref()
            .expect("control requires managed fixture")
            .start_node(index);
    }

    pub fn restart_node(&self, index: usize) {
        self.managed
            .as_ref()
            .expect("control requires managed fixture")
            .restart_node(index);
    }

    pub fn stop_all(&self) {
        self.managed
            .as_ref()
            .expect("control requires managed fixture")
            .stop_all();
    }

    pub fn start_all(&self) {
        self.managed
            .as_ref()
            .expect("control requires managed fixture")
            .start_all();
    }

    pub fn restart_all(&self) {
        self.managed
            .as_ref()
            .expect("control requires managed fixture")
            .restart_all();
    }

    fn stop_extra_nodes(&self) {
        if let Some(m) = &self.managed {
            m.stop_extra_nodes();
        }
    }

    fn ensure_base_nodes_running(&self) {
        if let Some(m) = &self.managed {
            m.ensure_base_nodes_running();
        }
    }

    async fn restart_base_nodes_sequentially(&self) -> IgniteResult<()> {
        let m = self
            .managed
            .as_ref()
            .expect("control requires managed fixture");
        m.stop_all();
        m.start_node(0);
        wait_for_cluster_ready_with_timeout(&[m.node_address(0)], CLUSTER_READY_TIMEOUT).await?;
        for index in 1..base_cluster_node_count(&m.profile) {
            m.start_node(index);
        }
        Ok(())
    }

    pub async fn wait_for_ready(&self) -> IgniteResult<()> {
        let addrs = if let Some(m) = &self.managed {
            m.running_addresses()
        } else {
            self.addrs.clone()
        };
        if addrs.is_empty() {
            return Err(IgniteError::from("cluster has no running nodes"));
        }
        wait_for_cluster_ready(&addrs).await
    }

    async fn wait_for_ready_with_timeout(&self, timeout: Duration) -> IgniteResult<()> {
        let addrs = if let Some(m) = &self.managed {
            m.running_addresses()
        } else {
            self.addrs.clone()
        };
        if addrs.is_empty() {
            return Err(IgniteError::from("cluster has no running nodes"));
        }
        wait_for_cluster_ready_with_timeout(&addrs, timeout).await
    }
}

// ---------------------------------------------------------------------------
// DelayedHandshakeEnv
// ---------------------------------------------------------------------------

#[derive(Debug)]
pub struct DelayedHandshakeEnv {
    addr: String,
    _base_env: Option<Arc<IgniteTestEnv>>,
    join: Option<thread::JoinHandle<()>>,
}

impl DelayedHandshakeEnv {
    pub fn addr(&self) -> &str {
        &self.addr
    }
}

impl Drop for DelayedHandshakeEnv {
    fn drop(&mut self) {
        if self.join.is_none() {
            return;
        }
        let _ = TcpStream::connect(self.addr());
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

// ---------------------------------------------------------------------------
// ManagedContainer / ManagedCluster
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct ManagedContainer {
    name: String,
    profile: String,
    port: u16,
}

impl Drop for ManagedContainer {
    fn drop(&mut self) {
        fixture_debug(&format!("ManagedContainer::drop: removing {}", self.name));
        remove_container(&self.name);
    }
}

#[derive(Debug)]
struct ManagedCluster {
    profile: String,
    name_prefix: String,
    network_name: String,
    owned: bool,
}

impl ManagedCluster {
    fn container_name(&self, index: usize) -> String {
        format!("{}-node-{}", self.name_prefix, index)
    }

    fn addresses(&self) -> Vec<String> {
        (0..base_cluster_node_count(&self.profile))
            .map(|i| {
                format!(
                    "{}:{}",
                    docker_host_addr(),
                    cluster_node_client_port(&self.profile, i)
                )
            })
            .collect()
    }

    fn running_addresses(&self) -> Vec<String> {
        (0..managed_cluster_node_count(&self.profile))
            .filter(|i| container_is_running(&self.container_name(*i)))
            .map(|i| {
                format!(
                    "{}:{}",
                    docker_host_addr(),
                    cluster_node_client_port(&self.profile, i)
                )
            })
            .collect()
    }

    fn node_address(&self, index: usize) -> String {
        assert!(
            index < managed_cluster_node_count(&self.profile),
            "node index out of bounds"
        );
        format!(
            "{}:{}",
            docker_host_addr(),
            cluster_node_client_port(&self.profile, index)
        )
    }

    fn stop_node(&self, index: usize) {
        assert_cluster_node_index(&self.profile, index);
        stop_container(&self.container_name(index));
    }

    fn start_node(&self, index: usize) {
        assert_cluster_node_index(&self.profile, index);
        let name = self.container_name(index);
        if !container_is_running(&name) {
            start_container(&name);
        }
    }

    fn restart_node(&self, index: usize) {
        assert_cluster_node_index(&self.profile, index);
        let name = self.container_name(index);
        if container_is_running(&name) {
            stop_container(&name);
        }
        start_container(&name);
    }

    fn stop_all(&self) {
        for i in 0..base_cluster_node_count(&self.profile) {
            stop_container(&self.container_name(i));
        }
    }

    fn start_all(&self) {
        for i in 0..base_cluster_node_count(&self.profile) {
            self.start_node(i);
        }
    }

    fn restart_all(&self) {
        for i in 0..base_cluster_node_count(&self.profile) {
            self.restart_node(i);
        }
    }

    fn ensure_base_nodes_running(&self) {
        for i in 0..base_cluster_node_count(&self.profile) {
            self.start_node(i);
        }
    }

    fn stop_extra_nodes(&self) {
        for i in base_cluster_node_count(&self.profile)..managed_cluster_node_count(&self.profile) {
            let name = self.container_name(i);
            if container_is_running(&name) {
                stop_container(&name);
            }
        }
    }
}

impl Drop for ManagedCluster {
    fn drop(&mut self) {
        if !self.owned {
            fixture_debug(&format!(
                "ManagedCluster::drop: skipping borrowed cluster {}",
                self.name_prefix
            ));
            return;
        }
        fixture_debug(&format!(
            "ManagedCluster::drop: removing cluster {}",
            self.name_prefix
        ));
        for i in 0..managed_cluster_node_count(&self.profile) {
            remove_container(&self.container_name(i));
        }
        remove_network(&self.network_name);
        let config_root = cluster_generated_config_root(&self.profile, &self.name_prefix);
        let _ = fs::remove_dir_all(config_root);
    }
}

// ---------------------------------------------------------------------------
// IgniteContext
// ---------------------------------------------------------------------------

impl IgniteContext {
    pub fn profile(&self) -> IgniteProfile {
        self.profile
    }

    pub fn scope(&self) -> FixtureScope {
        self.scope
    }

    pub fn single_env(&self) -> Option<&Arc<IgniteTestEnv>> {
        match &self.kind {
            IgniteContextKind::Single(env) => Some(env),
            IgniteContextKind::Cluster(_) => None,
        }
    }

    pub fn cluster_env(&self) -> Option<&Arc<IgniteClusterEnv>> {
        match &self.kind {
            IgniteContextKind::Single(_) => None,
            IgniteContextKind::Cluster(env) => Some(env),
        }
    }

    pub fn client_config(&self) -> IgniteResult<ClientConfig> {
        match &self.kind {
            IgniteContextKind::Single(env) => env.client_config(),
            IgniteContextKind::Cluster(env) => {
                let mut conf = ClientConfig::from_addresses(env.addresses().iter().cloned());
                // Containerised clusters advertise internal IPs that are
                // unreachable from the host.  PA tests that need partition
                // awareness override the config explicitly.
                conf.partition_awareness_enabled = false;
                Ok(conf)
            }
        }
    }

    pub async fn wait_for_ready(&self) -> IgniteResult<()> {
        match &self.kind {
            IgniteContextKind::Single(env) => env.wait_for_ready().await,
            IgniteContextKind::Cluster(env) => env.wait_for_ready().await,
        }
    }

    pub async fn connect(&self) -> IgniteResult<TestClient> {
        let conf = self.client_config()?;
        match &self.kind {
            IgniteContextKind::Single(env) => connect_with_config_and_env(conf, env.clone()).await,
            IgniteContextKind::Cluster(env) => {
                connect_with_config_and_cluster_env(conf, env.clone()).await
            }
        }
    }

    async fn connect_for_bootstrap(&self) -> IgniteResult<TestClient> {
        match &self.kind {
            IgniteContextKind::Single(env) => {
                let conf = self.client_config()?;
                connect_with_config_and_env(conf, env.clone()).await
            }
            IgniteContextKind::Cluster(env) => {
                let mut conf = self.client_config()?;
                conf.partition_awareness_enabled = false;
                conf.handshake_timeout = Some(READY_PROBE_TIMEOUT);
                conf.request_timeout = Some(READY_PROBE_TIMEOUT);
                connect_with_config_and_cluster_env(conf, env.clone()).await
            }
        }
    }

    pub async fn ensure_cluster_active(&self) -> IgniteResult<()> {
        let client = self.connect_for_bootstrap().await?;
        match client.cluster().state().await {
            Ok(ClusterState::Active) => Ok(()),
            Ok(_) => client.cluster().set_state(ClusterState::Active).await,
            Err(err) => Err(err),
        }
    }

    pub async fn ensure_cache(&self, name: &str) -> IgniteResult<()> {
        let client = self.connect_for_bootstrap().await?;
        let _ = client.get_or_create_cache::<i32, i32>(name).await?;
        Ok(())
    }

    pub async fn ensure_table(&self, sql: &str) -> IgniteResult<()> {
        let client = self.connect_for_bootstrap().await?;
        let _ = client
            .sql_fields::<i64>(SqlFieldsQuery::new(sql))
            .await?
            .fetch_all()
            .await?;
        Ok(())
    }

    pub async fn reset_profile_state(&self) -> IgniteResult<()> {
        match &self.kind {
            IgniteContextKind::Single(env) => {
                if self.profile == IgniteProfile::SingleNodeChurn && env.is_managed() {
                    env.restart();
                }
                self.wait_for_ready().await
            }
            IgniteContextKind::Cluster(env) => {
                if self.profile == IgniteProfile::ThreeNodeClusterChurn && env.is_managed() {
                    env.stop_extra_nodes();
                    env.ensure_base_nodes_running();
                    match env
                        .wait_for_ready_with_timeout(CHURN_RESET_READY_TIMEOUT)
                        .await
                    {
                        Ok(()) => return Ok(()),
                        Err(_) => {
                            env.restart_base_nodes_sequentially().await?;
                            return self.wait_for_ready().await;
                        }
                    }
                }
                self.wait_for_ready().await
            }
        }
    }

    pub fn stop_node(&self, index: usize) {
        self.cluster_env()
            .expect("cluster-only operation")
            .stop_node(index);
    }

    pub fn start_node(&self, index: usize) {
        self.cluster_env()
            .expect("cluster-only operation")
            .start_node(index);
    }

    pub fn restart_node(&self, index: usize) {
        self.cluster_env()
            .expect("cluster-only operation")
            .restart_node(index);
    }

    pub fn restart_all(&self) {
        self.cluster_env()
            .expect("cluster-only operation")
            .restart_all();
    }
}

// ---------------------------------------------------------------------------
// IgniteScope
// ---------------------------------------------------------------------------

impl IgniteScope {
    pub fn profile(&self) -> IgniteProfile {
        self.context.profile()
    }

    pub fn scope(&self) -> FixtureScope {
        self.context.scope()
    }

    pub fn single_env(&self) -> Option<&Arc<IgniteTestEnv>> {
        self.context.single_env()
    }

    pub fn cluster_env(&self) -> Option<&Arc<IgniteClusterEnv>> {
        self.context.cluster_env()
    }

    pub fn context(&self) -> &Arc<IgniteContext> {
        &self.context
    }

    pub fn client_config(&self) -> IgniteResult<ClientConfig> {
        self.context.client_config()
    }

    pub async fn wait_for_ready(&self) -> IgniteResult<()> {
        self.context.wait_for_ready().await
    }

    pub async fn connect(&self) -> IgniteResult<TestClient> {
        self.context.connect().await
    }
}

// ---------------------------------------------------------------------------
// TestClient
// ---------------------------------------------------------------------------

pub struct TestClient {
    _env: TestEnvHandle,
    inner: Client,
}

impl Deref for TestClient {
    type Target = Client;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

// ---------------------------------------------------------------------------
// Docker operations
// ---------------------------------------------------------------------------

fn docker_client() -> &'static Docker {
    DOCKER.get_or_init(|| {
        if let Ok(host) = std::env::var("DOCKER_HOST") {
            if let Some(path) = host.strip_prefix("unix://") {
                return Docker::connect_with_unix(path, 120, bollard::API_DEFAULT_VERSION)
                    .expect("failed to connect to Docker/Podman via DOCKER_HOST unix socket");
            }
            // tcp:// or http:// — bollard reads DOCKER_HOST automatically.
            return Docker::connect_with_http_defaults()
                .expect("failed to connect to Docker/Podman via DOCKER_HOST");
        }
        // No DOCKER_HOST: try Colima socket (macOS), then local/http defaults.
        if let Some(home) = std::env::var_os("HOME") {
            let colima = std::path::PathBuf::from(&home).join(".colima/default/docker.sock");
            if colima.exists() {
                if let Ok(d) = Docker::connect_with_unix(
                    colima.to_str().unwrap(),
                    120,
                    bollard::API_DEFAULT_VERSION,
                ) {
                    return d;
                }
            }
        }
        Docker::connect_with_local_defaults()
            .or_else(|_| Docker::connect_with_http_defaults())
            .expect("failed to connect to Docker/Podman")
    })
}

fn docker_host_addr() -> String {
    env::var("TESTCONTAINERS_HOST_OVERRIDE")
        .ok()
        .filter(|v| !v.trim().is_empty())
        .unwrap_or_else(|| "127.0.0.1".to_string())
}

/// Runs an async future from a synchronous context. Used for Docker churn
/// operations (stop/start/restart) and container provisioning during
/// `OnceLock::get_or_init`.
fn block_on_fixture<F>(future: F) -> F::Output
where
    F: Future + Send,
    F::Output: Send,
{
    match TokioHandle::try_current() {
        Ok(handle) if handle.runtime_flavor() == tokio::runtime::RuntimeFlavor::MultiThread => {
            tokio::task::block_in_place(|| handle.block_on(future))
        }
        Ok(_) => thread::scope(|scope| {
            scope
                .spawn(|| {
                    TokioRuntimeBuilder::new_current_thread()
                        .enable_all()
                        .build()
                        .expect("failed to build temporary fixture runtime")
                        .block_on(future)
                })
                .join()
                .expect("fixture runtime thread panicked")
        }),
        Err(_) => TokioRuntimeBuilder::new_current_thread()
            .enable_all()
            .build()
            .expect("failed to build fixture runtime")
            .block_on(future),
    }
}

fn container_is_running(name: &str) -> bool {
    block_on_fixture(async move {
        match tokio::time::timeout(
            DOCKER_API_TIMEOUT,
            docker_client().inspect_container(name, None),
        )
        .await
        {
            Ok(Ok(info)) => info.state.and_then(|s| s.running).unwrap_or(false),
            _ => false,
        }
    })
}

fn start_container(name: &str) {
    fixture_debug(&format!("start_container: {name}"));
    block_on_fixture(async move {
        tokio::time::timeout(
            DOCKER_API_TIMEOUT,
            docker_client().start_container::<String>(name, None::<StartContainerOptions<String>>),
        )
        .await
        .unwrap_or_else(|_| panic!("timed out starting container {name}"))
        .unwrap_or_else(|err| {
            if !err.to_string().contains("is already started") {
                panic!("failed to start container {name}: {err}");
            }
        });
    });
}

fn stop_container(name: &str) {
    fixture_debug(&format!("stop_container: {name}"));
    block_on_fixture(async move {
        tokio::time::timeout(
            DOCKER_API_TIMEOUT,
            docker_client().stop_container(name, Some(StopContainerOptions { t: 1 })),
        )
        .await
        .unwrap_or_else(|_| panic!("timed out stopping container {name}"))
        .unwrap_or_else(|err| {
            let msg = err.to_string();
            if !msg.contains("is not running") && !msg.contains("No such container") {
                panic!("failed to stop container {name}: {err}");
            }
        });
    });
}

fn remove_container(name: &str) {
    fixture_debug(&format!("remove_container: {name}"));
    block_on_fixture(async move {
        let _ = tokio::time::timeout(
            DOCKER_API_TIMEOUT,
            docker_client().remove_container(
                name,
                Some(RemoveContainerOptions {
                    force: true,
                    v: true,
                    ..Default::default()
                }),
            ),
        )
        .await;
    });
}

fn remove_network(name: &str) {
    fixture_debug(&format!("remove_network: {name}"));
    block_on_fixture(async move {
        let _ =
            tokio::time::timeout(DOCKER_API_TIMEOUT, docker_client().remove_network(name)).await;
    });
}

async fn ensure_network(name: &str) {
    let result = tokio::time::timeout(
        DOCKER_API_TIMEOUT,
        docker_client().create_network(CreateNetworkOptions {
            name: name.to_string(),
            check_duplicate: true,
            labels: managed_resource_labels_from_name(name, "network"),
            ..Default::default()
        }),
    )
    .await;

    match result {
        Ok(Ok(_)) => {}
        Ok(Err(err)) => {
            if !err.to_string().contains("already exists") {
                panic!("failed to create network {name}: {err}");
            }
        }
        Err(_) => panic!("timed out creating network {name}"),
    }
}

async fn ensure_image_available() {
    let image = test_image_name();
    let tag = test_image_tag();
    let pull_start = Instant::now();

    let mut stream = docker_client().create_image(
        Some(CreateImageOptions {
            from_image: image.clone(),
            tag: tag.clone(),
            ..Default::default()
        }),
        None,
        None,
    );

    loop {
        let remaining = IMAGE_PULL_TIMEOUT
            .checked_sub(pull_start.elapsed())
            .unwrap_or(Duration::ZERO);
        if remaining.is_zero() {
            panic!("timed out pulling image {image}:{tag} after {IMAGE_PULL_TIMEOUT:?}");
        }
        let chunk_timeout = remaining.min(DOCKER_API_TIMEOUT);
        match tokio::time::timeout(chunk_timeout, stream.next()).await {
            Ok(Some(result)) => {
                result.unwrap_or_else(|err| panic!("failed to pull image: {err}"));
            }
            Ok(None) => break,
            Err(_) => {
                panic!("timed out pulling image {image}:{tag} (no progress for {chunk_timeout:?})")
            }
        }
    }
}

async fn create_docker_container(
    profile: &str,
    image: &RunnableImage<GenericImage>,
    network_ip: Option<(String, String)>,
) {
    let envs: Vec<String> = image.env_vars().map(|(k, v)| format!("{k}={v}")).collect();
    let binds: Vec<String> = image
        .volumes()
        .map(|(h, c)| format!("{h}:{c}:ro"))
        .collect();
    let explicit_ports = image.ports().clone();
    let mut exposed_ports: HashMap<String, HashMap<(), ()>> = image
        .expose_ports()
        .into_iter()
        .map(|p| (format!("{p}/tcp"), HashMap::new()))
        .collect();
    if let Some(ports) = &explicit_ports {
        for p in ports {
            exposed_ports.insert(format!("{}/tcp", p.internal), HashMap::new());
        }
    }
    let port_bindings = explicit_ports.as_ref().map(|ports| {
        ports
            .iter()
            .map(|p| {
                (
                    format!("{}/tcp", p.internal),
                    Some(vec![PortBinding {
                        host_ip: Some("127.0.0.1".to_string()),
                        host_port: Some(p.local.to_string()),
                    }]),
                )
            })
            .collect()
    });
    let args: Vec<String> = image.args().clone().into_iterator().collect();

    let container_name = image
        .container_name()
        .clone()
        .expect("fixture image missing container name");
    let network_name = image.network().clone();

    let networking_config = network_name.as_ref().map(|net| {
        let endpoint = match &network_ip {
            Some((ip, _)) => EndpointSettings {
                ipam_config: Some(EndpointIpamConfig {
                    ipv4_address: Some(ip.clone()),
                    ..Default::default()
                }),
                aliases: Some(vec![container_name.clone()]),
                ..Default::default()
            },
            None => EndpointSettings {
                aliases: Some(vec![container_name.clone()]),
                ..Default::default()
            },
        };
        NetworkingConfig {
            endpoints_config: HashMap::from([(net.clone(), endpoint)]),
        }
    });

    let config = DockerContainerConfig {
        image: Some(image.descriptor()),
        env: Some(envs),
        labels: Some(managed_resource_labels(profile, "container")),
        host_config: Some(HostConfig {
            binds: if binds.is_empty() { None } else { Some(binds) },
            network_mode: network_name,
            port_bindings,
            publish_all_ports: Some(explicit_ports.is_none()),
            ..Default::default()
        }),
        networking_config,
        entrypoint: image.entrypoint().map(|e| vec![e]),
        cmd: if args.is_empty() { None } else { Some(args) },
        exposed_ports: Some(exposed_ports),
        ..Default::default()
    };

    // Remove any existing container with same name first.
    let _ = docker_client()
        .remove_container(
            &container_name,
            Some(RemoveContainerOptions {
                force: true,
                v: true,
                ..Default::default()
            }),
        )
        .await;

    match tokio::time::timeout(
        DOCKER_API_TIMEOUT,
        docker_client().create_container(
            Some(CreateContainerOptions {
                name: container_name.clone(),
            }),
            config.clone(),
        ),
    )
    .await
    {
        Ok(Ok(_)) => {}
        Ok(Err(err)) => {
            if err.to_string().contains("No such image") || err.to_string().contains("not found") {
                ensure_image_available().await;
                tokio::time::timeout(
                    DOCKER_API_TIMEOUT,
                    docker_client().create_container(
                        Some(CreateContainerOptions {
                            name: container_name.clone(),
                        }),
                        config,
                    ),
                )
                .await
                .unwrap_or_else(|_| panic!("timed out creating container after image pull"))
                .unwrap_or_else(|err| panic!("failed to create container: {err}"));
            } else {
                panic!("failed to create container {container_name}: {err}");
            }
        }
        Err(_) => panic!("timed out creating container {container_name}"),
    }

    tokio::time::timeout(
        DOCKER_API_TIMEOUT,
        docker_client()
            .start_container::<String>(&container_name, None::<StartContainerOptions<String>>),
    )
    .await
    .unwrap_or_else(|_| panic!("timed out starting container {container_name}"))
    .unwrap_or_else(|err| {
        if !err.to_string().contains("is already started") {
            panic!("failed to start container {container_name}: {err}");
        }
    });
}

async fn wait_for_container_log(name: &str, message: &str, timeout: Duration) {
    wait_for_container_log_since(name, message, timeout, 0).await;
}

/// Poll-based log readiness check.  Podman's `follow: true` log stream on
/// macOS can silently stall, so we periodically fetch accumulated log output
/// (non-follow) and check for the readiness marker.
///
/// `since` is a Unix timestamp; pass 0 to read from the beginning.
async fn wait_for_container_log_since(name: &str, message: &str, timeout: Duration, since: i64) {
    let deadline = tokio::time::Instant::now() + timeout;
    let poll_interval = Duration::from_secs(2);

    loop {
        let mut logs = String::new();
        let mut stream = docker_client().logs::<String>(
            name,
            Some(LogsOptions {
                follow: false,
                stdout: true,
                stderr: true,
                since,
                ..Default::default()
            }),
        );
        while let Some(Ok(log)) = stream.next().await {
            match &log {
                LogOutput::StdOut { message: m } | LogOutput::StdErr { message: m } => {
                    logs.push_str(&String::from_utf8_lossy(m));
                }
                _ => {}
            }
        }
        if logs.contains(message) {
            return;
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("timed out waiting for '{message}' in {name} logs after {timeout:?}");
        }
        tokio::time::sleep(poll_interval).await;
    }
}

async fn get_mapped_port(name: &str) -> u16 {
    let port_spec = format!("{IGNITE_PORT}/tcp");
    let inspect = tokio::time::timeout(
        DOCKER_API_TIMEOUT,
        docker_client().inspect_container(name, None),
    )
    .await
    .unwrap_or_else(|_| panic!("timed out inspecting container {name}"))
    .unwrap_or_else(|err| panic!("failed to inspect container {name}: {err}"));

    inspect
        .network_settings
        .and_then(|ns| ns.ports)
        .and_then(|ports| ports.get(&port_spec).cloned())
        .and_then(|bindings| bindings?.into_iter().next())
        .and_then(|b| b.host_port?.parse().ok())
        .unwrap_or_else(|| panic!("no mapped port for {name}"))
}

async fn network_subnet(name: &str) -> Option<String> {
    let inspect = tokio::time::timeout(
        DOCKER_API_TIMEOUT,
        docker_client().inspect_network::<String>(name, None),
    )
    .await
    .ok()?
    .ok()?;
    inspect
        .ipam
        .and_then(|ipam| ipam.config)
        .and_then(|configs| configs.into_iter().find_map(|c| c.subnet))
}

fn cluster_static_ip_from_subnet(subnet: &str, index: usize) -> Option<String> {
    let (base, _prefix) = subnet.split_once('/')?;
    let mut octets = base.parse::<Ipv4Addr>().ok()?.octets();
    let host_octet = 2u8.checked_add(index as u8)?;
    octets[3] = host_octet;
    Some(Ipv4Addr::from(octets).to_string())
}

// ---------------------------------------------------------------------------
// Stale container cleanup
// ---------------------------------------------------------------------------

/// Remove fixture containers from other processes (different PIDs) to prevent
/// resource exhaustion when xtask runs many test binaries sequentially.
/// Returns `true` if at least one container was removed.
async fn cleanup_stale_fixture_containers(current_name: &str) -> bool {
    let filters: HashMap<String, Vec<String>> = HashMap::from([(
        "label".to_string(),
        vec![format!("{FIXTURE_MANAGED_LABEL}=true")],
    )]);
    let containers = match tokio::time::timeout(
        DOCKER_API_TIMEOUT,
        docker_client().list_containers(Some(ListContainersOptions {
            all: true,
            filters,
            ..Default::default()
        })),
    )
    .await
    {
        Ok(Ok(list)) => list,
        _ => return false, // best-effort; don't fail the test if listing fails
    };
    let mut removed_any = false;
    for c in containers {
        let names = c.names.unwrap_or_default();
        let name = match names.first() {
            Some(n) => n.strip_prefix('/').unwrap_or(n),
            None => continue,
        };
        if name == current_name {
            continue; // will be handled by create_docker_container's force-remove
        }
        // Only clean up containers with the same fixture-name prefix (same
        // profile + "ignite-rs-fixture-" format).  This avoids removing
        // xtask-provisioned containers for other profiles.
        if !name.starts_with("ignite-rs-fixture-") {
            continue;
        }
        fixture_debug(&format!(
            "cleanup_stale_fixture_containers: removing {name}"
        ));
        let _ = docker_client()
            .remove_container(
                name,
                Some(RemoveContainerOptions {
                    force: true,
                    v: true,
                    ..Default::default()
                }),
            )
            .await;
        removed_any = true;
    }
    removed_any
}

// ---------------------------------------------------------------------------
// Container / cluster creation
// ---------------------------------------------------------------------------

async fn create_single_node(profile: &str) -> ManagedContainer {
    let name = format!(
        "ignite-rs-fixture-{}-{}",
        sanitize_identifier(profile),
        std::process::id()
    );
    fixture_debug(&format!(
        "create_single_node: profile={profile} name={name}"
    ));

    // Remove stale fixture containers from prior test-binary processes to
    // prevent resource exhaustion (each Ignite JVM uses 512 MB heap).
    // The brief pause after cleanup gives the Podman VM time to reclaim
    // memory before the new JVM starts.
    if cleanup_stale_fixture_containers(&name).await {
        tokio::time::sleep(Duration::from_secs(3)).await;
    }

    let config_path = fixture_config_path(profile, 0);
    let container_config_path = single_node_container_config_path();
    let image = build_runnable_image(profile, &name, None, &config_path, container_config_path);

    create_docker_container(profile, &image, None).await;

    let port = get_mapped_port(&name).await;
    let addr = format!("{}:{}", docker_host_addr(), port);
    wait_for_tcp_ready(&addr, LOG_READY_TIMEOUT).await;
    ensure_profile_container_ready_async(&name, profile).await;

    fixture_debug(&format!(
        "create_single_node: ready — addr={addr} container={name}",
    ));
    ManagedContainer {
        name,
        profile: profile.to_string(),
        port,
    }
}

async fn create_cluster(profile: &str) -> ManagedCluster {
    let name_prefix = format!(
        "ignite-rs-fixture-{}-{}",
        sanitize_identifier(profile),
        std::process::id()
    );
    let network_name = format!("{name_prefix}-net");
    fixture_debug(&format!(
        "create_cluster: profile={profile} prefix={name_prefix}"
    ));

    ensure_network(&network_name).await;

    let subnet = network_subnet(&network_name).await;
    let node_count = managed_cluster_node_count(profile);

    // Create all nodes (base + extra for churn).
    for index in 0..node_count {
        let container_name = format!("{name_prefix}-node-{index}");
        let static_ip = subnet
            .as_deref()
            .and_then(|s| cluster_static_ip_from_subnet(s, index));

        let discovery_addrs: Vec<String> = (0..base_cluster_node_count(profile))
            .map(|peer| {
                let host = subnet
                    .as_deref()
                    .and_then(|s| cluster_static_ip_from_subnet(s, peer))
                    .unwrap_or_else(|| format!("{name_prefix}-node-{peer}"));
                format!("{host}:47500")
            })
            .collect();

        let client_port = cluster_node_client_port(profile, index);
        let config_path = generated_cluster_config_path(
            profile,
            &name_prefix,
            &network_name,
            index,
            &discovery_addrs,
            static_ip.as_deref(),
            client_port,
        );

        let container_config_path = cluster_node_container_config_path();
        let image = build_runnable_image(
            profile,
            &container_name,
            Some(&network_name),
            &config_path,
            container_config_path,
        );

        let network_ip = static_ip.map(|ip| (ip, network_name.clone()));
        create_docker_container(profile, &image, network_ip).await;
        wait_for_container_log(&container_name, LOG_READY_MESSAGE, LOG_READY_TIMEOUT).await;
    }

    // Stop extra nodes (only base nodes should be running initially).
    for index in base_cluster_node_count(profile)..node_count {
        let name = format!("{name_prefix}-node-{index}");
        let _ = docker_client()
            .stop_container(&name, Some(StopContainerOptions { t: 1 }))
            .await;
    }

    // Activate cluster on auth profiles.
    if profile == SINGLE_NODE_AUTH_PROFILE {
        // Auth profiles with cluster topology need activation.
        let addr = format!(
            "{}:{}",
            docker_host_addr(),
            cluster_node_client_port(profile, 0)
        );
        let conf = ready_probe_client_config(&addr);
        if let Ok(client) = new_client(conf).await {
            let _ = client.cluster().set_state(ClusterState::Active).await;
        }
    }

    ManagedCluster {
        profile: profile.to_string(),
        name_prefix,
        network_name,
        owned: true,
    }
}

async fn ensure_profile_container_ready_async(name: &str, profile: &str) {
    if profile == SINGLE_NODE_AUTH_PROFILE {
        // Auth containers need cluster activation after start.
        let mut last_err = None;
        for _ in 0..start_retries() {
            let conf =
                ready_probe_client_config(&format!("{}:{}", docker_host_addr(), IGNITE_PORT));
            match new_client(conf).await {
                Ok(client) => match client.cluster().set_state(ClusterState::Active).await {
                    Ok(_) => return,
                    Err(err) => last_err = Some(err),
                },
                Err(err) => last_err = Some(err),
            }
            tokio::time::sleep(start_delay()).await;
        }
        if let Some(err) = last_err {
            fixture_debug(&format!(
                "ensure_profile_container_ready_async: activation failed for {name}: {err}"
            ));
        }
    }
}

fn ensure_profile_container_ready(name: &str, profile: &str) {
    if profile == SINGLE_NODE_AUTH_PROFILE {
        block_on_fixture(ensure_profile_container_ready_async(name, profile));
    }
}

// ---------------------------------------------------------------------------
// Image building
// ---------------------------------------------------------------------------

fn build_runnable_image(
    profile: &str,
    container_name: &str,
    network_name: Option<&str>,
    config_path: &Path,
    container_config_path: &str,
) -> RunnableImage<GenericImage> {
    let mut image = GenericImage::new(test_image_name(), test_image_tag())
        .with_volume(
            config_path.display().to_string(),
            container_config_path.to_string(),
        )
        .with_env_var("CONFIG_URI", container_config_path)
        .with_env_var("JVM_OPTS", profile_jvm_opts(profile));

    if !matches!(profile, CLUSTER_3_PROFILE | CLUSTER_3_CHURN_PROFILE) {
        image = image.with_exposed_port(IGNITE_PORT);
    }

    for (host_path, container_path) in extra_container_bind_mounts(profile) {
        image = image.with_volume(host_path.display().to_string(), container_path.to_string());
    }
    for (key, value) in extra_container_env(profile) {
        image = image.with_env_var(key, value);
    }

    let mut runnable = RunnableImage::from(image).with_container_name(container_name.to_string());
    if let Some(network) = network_name {
        runnable = runnable.with_network(network.to_string());
    }

    if matches!(profile, CLUSTER_3_PROFILE | CLUSTER_3_CHURN_PROFILE) {
        if let Some(index) = container_name
            .rsplit_once("-node-")
            .and_then(|(_, s)| s.parse::<usize>().ok())
        {
            let port = cluster_node_client_port(profile, index);
            runnable = runnable.with_mapped_port((port, port));
        }
    }

    runnable
}

// ---------------------------------------------------------------------------
// Profile configuration helpers
// ---------------------------------------------------------------------------

fn profile_descriptor(profile: IgniteProfile) -> ProfileDescriptor {
    match profile {
        IgniteProfile::DefaultSingleNode => ProfileDescriptor {
            profile,
            key: SINGLE_NODE_PROFILE,
            kind: ProfileKind::Single,
            default_scope: FixtureScope::CargoSession,
        },
        IgniteProfile::SingleNodeChurn => ProfileDescriptor {
            profile,
            key: SINGLE_NODE_CHURN_PROFILE,
            kind: ProfileKind::Single,
            default_scope: FixtureScope::Process,
        },
        IgniteProfile::ThreeNodeCluster => ProfileDescriptor {
            profile,
            key: CLUSTER_3_PROFILE,
            kind: ProfileKind::Cluster,
            default_scope: FixtureScope::CargoSession,
        },
        IgniteProfile::ThreeNodeClusterChurn => ProfileDescriptor {
            profile,
            key: CLUSTER_3_CHURN_PROFILE,
            kind: ProfileKind::Cluster,
            default_scope: FixtureScope::Process,
        },
        IgniteProfile::AuthSingleNode => ProfileDescriptor {
            profile,
            key: SINGLE_NODE_AUTH_PROFILE,
            kind: ProfileKind::Single,
            default_scope: FixtureScope::CargoSession,
        },
        IgniteProfile::CustomClientPort(_) => ProfileDescriptor {
            profile,
            key: "custom-client-port",
            kind: ProfileKind::Single,
            default_scope: FixtureScope::Process,
        },
        #[cfg(feature = "ssl")]
        IgniteProfile::TlsSingleNode => ProfileDescriptor {
            profile,
            key: SINGLE_NODE_TLS_PROFILE,
            kind: ProfileKind::Single,
            default_scope: FixtureScope::CargoSession,
        },
        #[cfg(feature = "ssl")]
        IgniteProfile::MtlsSingleNode => ProfileDescriptor {
            profile,
            key: SINGLE_NODE_MTLS_PROFILE,
            kind: ProfileKind::Single,
            default_scope: FixtureScope::CargoSession,
        },
    }
}

fn default_fixture_scope(profile: IgniteProfile) -> FixtureScope {
    profile_descriptor(profile).default_scope
}

fn profile_jvm_opts(profile: &str) -> &'static str {
    match profile {
        CLUSTER_3_PROFILE | CLUSTER_3_CHURN_PROFILE => "-Xms256m -Xmx256m -DIGNITE_QUIET=false",
        _ => "-Xms512m -Xmx512m -DIGNITE_QUIET=false",
    }
}

fn auth_defaults_for_profile(profile: &str) -> (Option<String>, Option<String>) {
    match profile {
        SINGLE_NODE_AUTH_PROFILE => (
            Some(DEFAULT_AUTH_USERNAME.to_string()),
            Some(DEFAULT_AUTH_PASSWORD.to_string()),
        ),
        _ => (None, None),
    }
}

fn tls_defaults_for_profile(
    profile: &str,
) -> (
    Option<String>,
    Option<String>,
    Option<String>,
    Option<String>,
) {
    #[cfg(feature = "ssl")]
    match profile {
        SINGLE_NODE_TLS_PROFILE => (
            Some(DEFAULT_TLS_SERVER_NAME.to_string()),
            Some(
                ssl_fixture_assets_dir()
                    .join("ca.pem")
                    .display()
                    .to_string(),
            ),
            None,
            None,
        ),
        SINGLE_NODE_MTLS_PROFILE => {
            let client_pem = ssl_fixture_assets_dir()
                .join("client_full.pem")
                .display()
                .to_string();
            (
                Some(DEFAULT_TLS_SERVER_NAME.to_string()),
                Some(
                    ssl_fixture_assets_dir()
                        .join("ca.pem")
                        .display()
                        .to_string(),
                ),
                Some(client_pem.clone()),
                Some(client_pem),
            )
        }
        _ => (None, None, None, None),
    }

    #[cfg(not(feature = "ssl"))]
    {
        let _ = profile;
        (None, None, None, None)
    }
}

fn extra_container_bind_mounts(profile: &str) -> Vec<(PathBuf, &'static str)> {
    match profile {
        SINGLE_NODE_TLS_PROFILE | SINGLE_NODE_MTLS_PROFILE => {
            vec![(ssl_fixture_assets_dir(), CONTAINER_SSL_ASSETS_DIR)]
        }
        _ => Vec::new(),
    }
}

fn extra_container_env(profile: &str) -> Vec<(&'static str, &'static str)> {
    match profile {
        SINGLE_NODE_AUTH_PROFILE => vec![
            ("IGNITE_ENABLE_EXPERIMENTAL_COMMAND", "true"),
            ("OPTION_LIBS", "ignite-indexing"),
        ],
        _ => Vec::new(),
    }
}

fn cluster_node_client_port(profile: &str, index: usize) -> u16 {
    let base = match profile {
        CLUSTER_3_CHURN_PROFILE => CLUSTER_3_CHURN_BASE_PORT,
        _ => CLUSTER_3_BASE_PORT,
    };
    base + index as u16
}

fn base_cluster_node_count(_profile: &str) -> usize {
    BASE_CLUSTER_NODE_COUNT
}

fn managed_cluster_node_count(profile: &str) -> usize {
    match profile {
        CLUSTER_3_CHURN_PROFILE => DISCOVERY_CLUSTER_NODE_COUNT,
        _ => BASE_CLUSTER_NODE_COUNT,
    }
}

fn assert_cluster_node_index(profile: &str, index: usize) {
    assert!(
        index < managed_cluster_node_count(profile),
        "cluster node index {index} out of bounds for profile {profile}"
    );
}

// ---------------------------------------------------------------------------
// Fixture resolution
// ---------------------------------------------------------------------------

fn context_registry() -> &'static Mutex<HashMap<String, Weak<IgniteContext>>> {
    CONTEXT_REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

fn lock_context_registry() -> std::sync::MutexGuard<'static, HashMap<String, Weak<IgniteContext>>> {
    context_registry().lock().unwrap_or_else(|p| p.into_inner())
}

fn context_registry_key(profile: IgniteProfile, scope: FixtureScope) -> String {
    let d = profile_descriptor(profile);
    let scope_key = match scope {
        FixtureScope::Process => "process",
        FixtureScope::CargoSession => "cargo-session",
    };
    format!("{}:{}:{}", scope_key, d.key, profile_identity(profile))
}

fn profile_identity(profile: IgniteProfile) -> String {
    match profile {
        IgniteProfile::DefaultSingleNode => env::var("IGNITE_ADDR")
            .ok()
            .filter(|v| !v.trim().is_empty())
            .unwrap_or_else(|| format!("managed:{}", std::process::id())),
        IgniteProfile::SingleNodeChurn => format!("managed:{}", std::process::id()),
        IgniteProfile::ThreeNodeCluster => env::var("IGNITE_3NODE_ADDRS")
            .ok()
            .filter(|v| !v.trim().is_empty())
            .unwrap_or_else(|| format!("managed:{}", std::process::id())),
        IgniteProfile::ThreeNodeClusterChurn => env::var("IGNITE_3NODE_CHURN_ADDRS")
            .ok()
            .filter(|v| !v.trim().is_empty())
            .unwrap_or_else(|| format!("managed:{}", std::process::id())),
        IgniteProfile::AuthSingleNode => env::var("IGNITE_AUTH_ADDR")
            .ok()
            .filter(|v| !v.trim().is_empty())
            .unwrap_or_else(|| format!("managed:{}", std::process::id())),
        IgniteProfile::CustomClientPort(port) => format!("127.0.0.1:{port}"),
        #[cfg(feature = "ssl")]
        IgniteProfile::TlsSingleNode => env::var("IGNITE_TLS_ADDR")
            .ok()
            .filter(|v| !v.trim().is_empty())
            .unwrap_or_else(|| format!("managed:{}", std::process::id())),
        #[cfg(feature = "ssl")]
        IgniteProfile::MtlsSingleNode => env::var("IGNITE_TLS_ADDR")
            .ok()
            .filter(|v| !v.trim().is_empty())
            .map(|a| format!("{a}:mtls"))
            .unwrap_or_else(|| format!("managed-mtls:{}", std::process::id())),
    }
}

fn build_ignite_context(profile: IgniteProfile, scope: FixtureScope) -> IgniteContext {
    let d = profile_descriptor(profile);
    let kind = match d.kind {
        ProfileKind::Single => IgniteContextKind::Single(resolve_single_env(profile, scope)),
        ProfileKind::Cluster => IgniteContextKind::Cluster(resolve_cluster_env(profile, scope)),
    };
    IgniteContext {
        profile: d.profile,
        scope,
        kind,
    }
}

/// Returns `Some(value)` only if the env var is set and non-empty.
fn env_non_empty(key: &str) -> Option<String> {
    env::var(key).ok().filter(|v| !v.trim().is_empty())
}

fn resolve_single_env(profile: IgniteProfile, scope: FixtureScope) -> Arc<IgniteTestEnv> {
    match profile {
        IgniteProfile::DefaultSingleNode => {
            if let Some(addr) = env_non_empty("IGNITE_ADDR") {
                Arc::new(IgniteTestEnv::external(addr))
            } else {
                resolve_single_managed_env(&SHARED_ENV, SINGLE_NODE_PROFILE, scope)
            }
        }
        IgniteProfile::SingleNodeChurn => resolve_single_managed_env(
            &SHARED_SINGLE_NODE_CHURN_ENV,
            SINGLE_NODE_CHURN_PROFILE,
            scope,
        ),
        IgniteProfile::AuthSingleNode => {
            if let Some(addr) = env_non_empty("IGNITE_AUTH_ADDR") {
                let username = env_non_empty("IGNITE_AUTH_USERNAME")
                    .unwrap_or_else(|| DEFAULT_AUTH_USERNAME.to_string());
                let password = env_non_empty("IGNITE_AUTH_PASSWORD")
                    .unwrap_or_else(|| DEFAULT_AUTH_PASSWORD.to_string());
                Arc::new(IgniteTestEnv::external_auth(addr, username, password))
            } else {
                resolve_single_managed_env(&SHARED_AUTH_ENV, SINGLE_NODE_AUTH_PROFILE, scope)
            }
        }
        IgniteProfile::CustomClientPort(port) => {
            Arc::new(IgniteTestEnv::external(format!("127.0.0.1:{port}")))
        }
        #[cfg(feature = "ssl")]
        IgniteProfile::TlsSingleNode => {
            if let Some(addr) = env_non_empty("IGNITE_TLS_ADDR") {
                let server_name = env_non_empty("IGNITE_TLS_SERVER_NAME")
                    .unwrap_or_else(|| DEFAULT_TLS_SERVER_NAME.to_string());
                let ca_pem = env_non_empty("IGNITE_TLS_CA_PEM")
                    .expect("IGNITE_TLS_CA_PEM required when IGNITE_TLS_ADDR is set");
                Arc::new(IgniteTestEnv::external_tls(
                    addr,
                    server_name,
                    ca_pem,
                    None,
                    None,
                ))
            } else {
                resolve_single_managed_env(&SHARED_TLS_ENV, SINGLE_NODE_TLS_PROFILE, scope)
            }
        }
        #[cfg(feature = "ssl")]
        IgniteProfile::MtlsSingleNode => {
            if let Some(addr) = env_non_empty("IGNITE_TLS_ADDR") {
                let server_name = env_non_empty("IGNITE_TLS_SERVER_NAME")
                    .unwrap_or_else(|| DEFAULT_TLS_SERVER_NAME.to_string());
                let ca_pem = env_non_empty("IGNITE_TLS_CA_PEM")
                    .expect("IGNITE_TLS_CA_PEM required when IGNITE_TLS_ADDR is set");
                let client_cert_pem = env_non_empty("IGNITE_TLS_CLIENT_CERT_PEM")
                    .expect("IGNITE_TLS_CLIENT_CERT_PEM required");
                let client_key_pem = env_non_empty("IGNITE_TLS_CLIENT_KEY_PEM")
                    .expect("IGNITE_TLS_CLIENT_KEY_PEM required");
                Arc::new(IgniteTestEnv::external_tls(
                    addr,
                    server_name,
                    ca_pem,
                    Some(client_cert_pem),
                    Some(client_key_pem),
                ))
            } else {
                resolve_single_managed_env(&SHARED_MTLS_ENV, SINGLE_NODE_MTLS_PROFILE, scope)
            }
        }
        IgniteProfile::ThreeNodeCluster | IgniteProfile::ThreeNodeClusterChurn => {
            unreachable!("cluster profiles must resolve through resolve_cluster_env")
        }
    }
}

fn resolve_single_managed_env(
    shared: &OnceLock<Arc<IgniteTestEnv>>,
    profile: &str,
    scope: FixtureScope,
) -> Arc<IgniteTestEnv> {
    match scope {
        FixtureScope::Process => Arc::new(IgniteTestEnv::containerized(profile)),
        FixtureScope::CargoSession => shared
            .get_or_init(|| Arc::new(IgniteTestEnv::containerized(profile)))
            .clone(),
    }
}

fn resolve_cluster_env(profile: IgniteProfile, scope: FixtureScope) -> Arc<IgniteClusterEnv> {
    match profile {
        IgniteProfile::ThreeNodeCluster => {
            if let Some(addrs) = env_non_empty("IGNITE_3NODE_ADDRS") {
                return Arc::new(IgniteClusterEnv::external(parse_address_list(&addrs)));
            }
            resolve_cluster_managed_env(&SHARED_CLUSTER_3_ENV, CLUSTER_3_PROFILE, scope)
        }
        IgniteProfile::ThreeNodeClusterChurn => {
            if let (Some(addrs), Some(prefix)) = (
                env_non_empty("IGNITE_3NODE_CHURN_ADDRS"),
                env_non_empty("IGNITE_3NODE_CHURN_PREFIX"),
            ) {
                let addr_list = parse_address_list(&addrs);
                let network_name = format!("{prefix}-net");
                return SHARED_CLUSTER_3_CHURN_ENV
                    .get_or_init(|| {
                        Arc::new(IgniteClusterEnv::borrowed(
                            addr_list,
                            prefix,
                            network_name,
                            CLUSTER_3_CHURN_PROFILE,
                        ))
                    })
                    .clone();
            }
            resolve_cluster_managed_env(&SHARED_CLUSTER_3_CHURN_ENV, CLUSTER_3_CHURN_PROFILE, scope)
        }
        _ => unreachable!("single-node profiles must resolve through resolve_single_env"),
    }
}

fn resolve_cluster_managed_env(
    shared: &OnceLock<Arc<IgniteClusterEnv>>,
    profile: &str,
    scope: FixtureScope,
) -> Arc<IgniteClusterEnv> {
    match scope {
        FixtureScope::Process => Arc::new(IgniteClusterEnv::containerized(profile)),
        FixtureScope::CargoSession => shared
            .get_or_init(|| Arc::new(IgniteClusterEnv::containerized(profile)))
            .clone(),
    }
}

// ---------------------------------------------------------------------------
// Debug helpers (for fixture_env_pure_test)
// ---------------------------------------------------------------------------

#[cfg(test)]
pub(crate) fn debug_profile_descriptor(
    profile: IgniteProfile,
) -> (&'static str, &'static str, FixtureScope) {
    let d = profile_descriptor(profile);
    let kind = match d.kind {
        ProfileKind::Single => "single",
        ProfileKind::Cluster => "cluster",
    };
    (d.key, kind, d.default_scope)
}

#[cfg(test)]
pub(crate) fn debug_context_registry_key(profile: IgniteProfile, scope: FixtureScope) -> String {
    context_registry_key(profile, scope)
}

#[cfg(test)]
pub(crate) fn debug_context_registry_contains(profile: IgniteProfile, scope: FixtureScope) -> bool {
    let key = context_registry_key(profile, scope);
    lock_context_registry().contains_key(&key)
}

#[cfg(test)]
pub(crate) fn debug_prune_context_registry() {
    lock_context_registry().retain(|_, weak| weak.strong_count() > 0);
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

pub fn ignite_scope(profile: IgniteProfile) -> IgniteScope {
    IgniteScope {
        context: ignite_context(profile, default_fixture_scope(profile)),
    }
}

pub fn ignite_context(profile: IgniteProfile, scope: FixtureScope) -> Arc<IgniteContext> {
    let key = context_registry_key(profile, scope);
    {
        let mut registry = lock_context_registry();
        registry.retain(|_, weak| weak.strong_count() > 0);
        if let Some(existing) = registry.get(&key).and_then(Weak::upgrade) {
            return existing;
        }
    }

    let context = Arc::new(build_ignite_context(profile, scope));

    let mut registry = lock_context_registry();
    registry.retain(|_, weak| weak.strong_count() > 0);
    if let Some(existing) = registry.get(&key).and_then(Weak::upgrade) {
        return existing;
    }
    registry.insert(key, Arc::downgrade(&context));
    context
}

pub fn ignite_test_env() -> Arc<IgniteTestEnv> {
    ignite_context(
        IgniteProfile::DefaultSingleNode,
        default_fixture_scope(IgniteProfile::DefaultSingleNode),
    )
    .single_env()
    .expect("default single-node resolved as cluster")
    .clone()
}

pub fn ignite_single_node_churn_env() -> Arc<IgniteTestEnv> {
    ignite_context(
        IgniteProfile::SingleNodeChurn,
        default_fixture_scope(IgniteProfile::SingleNodeChurn),
    )
    .single_env()
    .expect("churn resolved as cluster")
    .clone()
}

pub fn ignite_auth_env() -> Arc<IgniteTestEnv> {
    ignite_context(
        IgniteProfile::AuthSingleNode,
        default_fixture_scope(IgniteProfile::AuthSingleNode),
    )
    .single_env()
    .expect("auth resolved as cluster")
    .clone()
}

#[cfg(feature = "ssl")]
pub fn ignite_tls_env() -> Arc<IgniteTestEnv> {
    ignite_context(
        IgniteProfile::TlsSingleNode,
        default_fixture_scope(IgniteProfile::TlsSingleNode),
    )
    .single_env()
    .expect("tls resolved as cluster")
    .clone()
}

#[cfg(feature = "ssl")]
pub fn ignite_mtls_env() -> Arc<IgniteTestEnv> {
    ignite_context(
        IgniteProfile::MtlsSingleNode,
        default_fixture_scope(IgniteProfile::MtlsSingleNode),
    )
    .single_env()
    .expect("mtls resolved as cluster")
    .clone()
}

pub fn ignite_cluster3_env() -> Arc<IgniteClusterEnv> {
    ignite_context(
        IgniteProfile::ThreeNodeCluster,
        default_fixture_scope(IgniteProfile::ThreeNodeCluster),
    )
    .cluster_env()
    .expect("cluster resolved as single-node")
    .clone()
}

pub fn ignite_cluster3_churn_env() -> Arc<IgniteClusterEnv> {
    ignite_context(
        IgniteProfile::ThreeNodeClusterChurn,
        default_fixture_scope(IgniteProfile::ThreeNodeClusterChurn),
    )
    .cluster_env()
    .expect("churn cluster resolved as single-node")
    .clone()
}

pub fn delayed_handshake_env(delay: Duration) -> io::Result<DelayedHandshakeEnv> {
    if let Ok(addr) = env::var("IGNITE_DELAYED_HANDSHAKE_ADDR") {
        return Ok(DelayedHandshakeEnv {
            addr,
            _base_env: None,
            join: None,
        });
    }

    let env = ignite_test_env();
    let target_addr = env.addr().to_string();
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let addr = listener.local_addr()?.to_string();
    let join = thread::spawn(move || {
        let Ok((mut client_stream, _)) = listener.accept() else {
            return;
        };
        let Ok(mut server_stream) = TcpStream::connect(&target_addr) else {
            return;
        };
        let Ok(mut client_reader) = client_stream.try_clone() else {
            return;
        };
        let Ok(mut server_writer) = server_stream.try_clone() else {
            return;
        };

        let upstream = thread::spawn(move || {
            let _ = io::copy(&mut client_reader, &mut server_writer);
            let _ = server_writer.shutdown(Shutdown::Write);
        });

        thread::sleep(delay);
        let _ = io::copy(&mut server_stream, &mut client_stream);
        let _ = client_stream.shutdown(Shutdown::Write);
        let _ = upstream.join();
    });

    Ok(DelayedHandshakeEnv {
        addr,
        _base_env: Some(env),
        join: Some(join),
    })
}

// ---------------------------------------------------------------------------
// Connection functions
// ---------------------------------------------------------------------------

pub async fn connect() -> IgniteResult<TestClient> {
    ignite_scope(IgniteProfile::DefaultSingleNode)
        .connect()
        .await
}

pub async fn connect_profile(profile: IgniteProfile) -> IgniteResult<TestClient> {
    ignite_scope(profile).connect().await
}

pub async fn connect_auth() -> IgniteResult<TestClient> {
    ignite_scope(IgniteProfile::AuthSingleNode).connect().await
}

pub async fn connect_with_config(conf: ClientConfig) -> IgniteResult<TestClient> {
    let env = ignite_test_env();
    connect_with_config_and_env(conf, env).await
}

#[cfg(feature = "ssl")]
pub async fn connect_tls() -> IgniteResult<TestClient> {
    ignite_scope(IgniteProfile::TlsSingleNode).connect().await
}

#[cfg(feature = "ssl")]
pub async fn connect_mtls() -> IgniteResult<TestClient> {
    ignite_scope(IgniteProfile::MtlsSingleNode).connect().await
}

pub async fn connect_cluster3() -> IgniteResult<TestClient> {
    ignite_scope(IgniteProfile::ThreeNodeCluster)
        .connect()
        .await
}

pub async fn connect_with_cluster3_config(conf: ClientConfig) -> IgniteResult<TestClient> {
    let env = ignite_cluster3_env();
    connect_with_config_and_cluster_env(conf, env).await
}

pub async fn connect_cluster3_churn() -> IgniteResult<TestClient> {
    ignite_scope(IgniteProfile::ThreeNodeClusterChurn)
        .connect()
        .await
}

pub async fn connect_with_cluster3_churn_config(conf: ClientConfig) -> IgniteResult<TestClient> {
    let env = ignite_cluster3_churn_env();
    connect_with_config_and_cluster_env(conf, env).await
}

async fn connect_with_config_and_env(
    conf: ClientConfig,
    env: Arc<IgniteTestEnv>,
) -> IgniteResult<TestClient> {
    let mut last_err = None;
    for _ in 0..start_retries() {
        match new_client(conf.clone()).await {
            Ok(client) => {
                return Ok(TestClient {
                    _env: TestEnvHandle::Single(env),
                    inner: client,
                });
            }
            Err(err) => {
                last_err = Some(err);
                tokio::time::sleep(start_delay()).await;
            }
        }
    }
    Err(last_err.expect("missing Ignite start failure"))
}

async fn connect_with_config_and_cluster_env(
    conf: ClientConfig,
    env: Arc<IgniteClusterEnv>,
) -> IgniteResult<TestClient> {
    let mut last_err = None;
    for _ in 0..start_retries() {
        match new_client(conf.clone()).await {
            Ok(client) => {
                return Ok(TestClient {
                    _env: TestEnvHandle::Cluster(env),
                    inner: client,
                });
            }
            Err(err) => {
                last_err = Some(err);
                tokio::time::sleep(start_delay()).await;
            }
        }
    }
    Err(last_err.expect("missing Ignite start failure"))
}

// ---------------------------------------------------------------------------
// Readiness
// ---------------------------------------------------------------------------

async fn wait_for_client_ready(conf: ClientConfig) -> IgniteResult<()> {
    let deadline = tokio::time::Instant::now() + SINGLE_NODE_READY_TIMEOUT;
    let start = Instant::now();
    let addr_display = conf.addresses.first().cloned().unwrap_or_default();
    let mut last_err = None;
    let probe_conf = ready_probe_from_config(&conf);

    for _ in 0..start_retries() {
        match new_client(probe_conf.clone()).await {
            Ok(client) => match client.get_cache_names().await {
                Ok(_) => {
                    drop(client);
                    return Ok(());
                }
                Err(err) => last_err = Some(err),
            },
            Err(err) => last_err = Some(err),
        }

        if tokio::time::Instant::now() >= deadline {
            break;
        }
        tokio::time::sleep(start_delay()).await;
    }

    let elapsed = start.elapsed();
    let last_err = last_err.expect("missing Ignite readiness failure");
    Err(io::Error::new(
        io::ErrorKind::TimedOut,
        format!(
            "Ignite node at {} not ready after {:.1}s (last error: {})",
            addr_display,
            elapsed.as_secs_f64(),
            last_err
        ),
    )
    .into())
}

async fn wait_for_cluster_ready(addrs: &[String]) -> IgniteResult<()> {
    wait_for_cluster_ready_with_timeout(addrs, CLUSTER_READY_TIMEOUT).await
}

async fn wait_for_cluster_ready_with_timeout(
    addrs: &[String],
    timeout: Duration,
) -> IgniteResult<()> {
    let deadline = tokio::time::Instant::now() + timeout;
    let mut last_err = None;

    loop {
        let mut all_ready = true;
        for addr in addrs {
            let conf = ready_probe_client_config(addr);
            match new_client(conf).await {
                Ok(client) => match client.get_cache_names().await {
                    Ok(_) => drop(client),
                    Err(err) => {
                        last_err = Some(err);
                        all_ready = false;
                        break;
                    }
                },
                Err(err) => {
                    last_err = Some(err);
                    all_ready = false;
                    break;
                }
            }
        }

        if all_ready {
            return Ok(());
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(last_err.expect("missing cluster readiness failure"));
        }
        tokio::time::sleep(start_delay()).await;
    }
}

fn ready_probe_client_config(addr: &str) -> ClientConfig {
    let mut conf = ClientConfig::new(addr);
    conf.handshake_timeout = Some(READY_PROBE_TIMEOUT);
    conf.request_timeout = Some(READY_PROBE_TIMEOUT);
    conf.partition_awareness_enabled = false;
    conf.retry_limit = 0;
    conf
}

fn ready_probe_from_config(conf: &ClientConfig) -> ClientConfig {
    let mut probe = conf.clone();
    probe.handshake_timeout = Some(conf.handshake_timeout.unwrap_or(READY_PROBE_TIMEOUT));
    probe.request_timeout = Some(conf.request_timeout.unwrap_or(READY_PROBE_TIMEOUT));
    probe.partition_awareness_enabled = false;
    probe.retry_limit = 0;
    probe
}

/// TCP-based readiness check: connect, handshake, and issue a lightweight
/// request.  This avoids the unreliable Podman log-streaming API entirely.
async fn wait_for_tcp_ready(addr: &str, timeout: Duration) {
    wait_for_tcp_ready_result(addr, timeout)
        .await
        .unwrap_or_else(|e| panic!("{e}"));
}

async fn wait_for_tcp_ready_result(addr: &str, timeout: Duration) -> IgniteResult<()> {
    let deadline = tokio::time::Instant::now() + timeout;
    let conf = ready_probe_client_config(addr);
    loop {
        if let Ok(client) = new_client(conf.clone()).await {
            if client.get_cache_names().await.is_ok() {
                drop(client);
                return Ok(());
            }
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!("timed out waiting for TCP readiness at {addr} after {timeout:?}"),
            )
            .into());
        }
        tokio::time::sleep(Duration::from_secs(2)).await;
    }
}

// ---------------------------------------------------------------------------
// Cluster config generation
// ---------------------------------------------------------------------------

fn generated_cluster_config_path(
    profile: &str,
    name_prefix: &str,
    _network_name: &str,
    index: usize,
    discovery_seed_addresses: &[String],
    static_ip: Option<&str>,
    client_port: u16,
) -> PathBuf {
    let config_root = cluster_generated_config_root(profile, name_prefix);
    fs::create_dir_all(&config_root).expect("failed to create cluster config dir");

    let path = config_root.join(format!("cluster-node-{index}.xml"));
    let template_path = fixture_assets_dir().join("cluster-3-node.xml");
    let mut xml = fs::read_to_string(&template_path)
        .unwrap_or_else(|err| panic!("failed to read cluster config template: {err}"));

    let discovery_xml = discovery_seed_addresses
        .iter()
        .map(|addr| format!("<value>{addr}</value>"))
        .collect::<Vec<_>>()
        .join("\n                                ");
    xml = xml.replace("__DISCOVERY_NODE_ADDRESSES__", &discovery_xml);

    let address_resolver = static_ip
        .map(|ip| {
            let host_addr = docker_host_addr();
            format!(
                r#"<property name="addressResolver">
            <bean class="org.apache.ignite.configuration.BasicAddressResolver">
                <constructor-arg>
                    <map>
                        <entry key="{ip}:{client_port}" value="{host_addr}:{client_port}"/>
                    </map>
                </constructor-arg>
            </bean>
        </property>"#
            )
        })
        .unwrap_or_default();
    xml = xml.replace("__ADDRESS_RESOLVER_PROPERTY__", &address_resolver);
    xml = xml.replace(
        "<property name=\"port\" value=\"10800\"/>",
        &format!("<property name=\"port\" value=\"{client_port}\"/>"),
    );

    fs::write(&path, xml).unwrap_or_else(|err| panic!("failed to write cluster config: {err}"));
    path
}

fn cluster_generated_config_root(profile: &str, name_prefix: &str) -> PathBuf {
    env::temp_dir()
        .join("ignite-rs-fixtures")
        .join("configs")
        .join(sanitize_identifier(profile))
        .join(sanitize_identifier(name_prefix))
}

// ---------------------------------------------------------------------------
// Utility helpers
// ---------------------------------------------------------------------------

fn test_image_name() -> String {
    env::var("IGNITE_TEST_IMAGE").unwrap_or_else(|_| DEFAULT_IGNITE_IMAGE.to_string())
}

fn test_image_tag() -> String {
    env::var("IGNITE_TEST_TAG").unwrap_or_else(|_| DEFAULT_IGNITE_TAG.to_string())
}

fn sanitize_identifier(input: &str) -> String {
    input
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '-' || c == '_' {
                c
            } else {
                '-'
            }
        })
        .collect()
}

fn fixture_config_path(profile: &str, _index: usize) -> PathBuf {
    let dir = fixture_assets_dir();
    match profile {
        SINGLE_NODE_AUTH_PROFILE => dir.join("single-node-auth.xml"),
        SINGLE_NODE_TLS_PROFILE | SINGLE_NODE_MTLS_PROFILE => dir.join("single-node-tls.xml"),
        _ => dir.join("single-node.xml"),
    }
}

fn fixture_assets_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("common")
        .join("fixtures")
        .join("ignite")
}

fn ssl_fixture_assets_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("..")
        .join("..")
        .join("ignite")
        .join("modules")
        .join("platforms")
        .join("cpp")
        .join("odbc-test")
        .join("config")
        .join("ssl")
}

fn single_node_container_config_path() -> &'static str {
    "/opt/ignite/apache-ignite/config/codex-single-node.xml"
}

fn cluster_node_container_config_path() -> &'static str {
    "/opt/ignite/apache-ignite/config/codex-cluster-node.xml"
}

fn start_retries() -> usize {
    env::var("IGNITE_TEST_START_RETRIES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_START_RETRIES)
}

fn start_delay() -> Duration {
    let millis = env::var("IGNITE_TEST_START_DELAY_MS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_START_DELAY_MS);
    Duration::from_millis(millis)
}

fn fixture_debug(message: &str) {
    if env::var("IGNITE_TEST_DEBUG").is_ok() {
        eprintln!("[fixture] {message}");
    }
}

fn managed_resource_labels(profile: &str, resource: &str) -> HashMap<String, String> {
    HashMap::from([
        (FIXTURE_MANAGED_LABEL.to_string(), "true".to_string()),
        (FIXTURE_PROFILE_LABEL.to_string(), profile.to_string()),
        (FIXTURE_RESOURCE_LABEL.to_string(), resource.to_string()),
    ])
}

fn managed_resource_labels_from_name(name: &str, resource: &str) -> HashMap<String, String> {
    let profile = name
        .strip_prefix("ignite-rs-fixture-")
        .and_then(|rest| rest.split_once('-').map(|(p, _)| p))
        .unwrap_or("unknown");
    managed_resource_labels(profile, resource)
}

fn parse_address_list(raw: &str) -> Vec<String> {
    raw.split(',')
        .map(str::trim)
        .filter(|a| !a.is_empty())
        .map(ToOwned::to_owned)
        .collect()
}

fn build_fixture_client_config(
    addr: &str,
    tls_server_name: Option<&str>,
    ca_pem: Option<&str>,
    client_cert_pem: Option<&str>,
    client_key_pem: Option<&str>,
) -> IgniteResult<ClientConfig> {
    #[cfg(feature = "ssl")]
    if let Some(ca_pem) = ca_pem {
        let server_name = tls_server_name.unwrap_or(DEFAULT_TLS_SERVER_NAME);
        return match (client_cert_pem, client_key_pem) {
            (Some(cert), Some(key)) => {
                client_config_from_ca_and_client_pem(addr, ca_pem, cert, key, server_name)
            }
            _ => client_config_from_ca_pem(addr, ca_pem, server_name),
        };
    }

    #[cfg(not(feature = "ssl"))]
    let _ = (tls_server_name, ca_pem, client_cert_pem, client_key_pem);

    let mut conf = ClientConfig::new(addr);
    // Ensure fixture clients always have timeouts so that a misbehaving
    // container cannot hang the test process indefinitely.
    conf.handshake_timeout = Some(Duration::from_secs(10));
    conf.request_timeout = Some(Duration::from_secs(30));
    Ok(conf)
}

pub async fn ensure_rainbow_table(client: &Client) -> IgniteResult<()> {
    let statements = rainbow_statements()?;
    for sql in statements {
        let res = match client.sql_fields::<i64>(SqlFieldsQuery::new(&sql)).await {
            Ok(cursor) => cursor.fetch_all().await,
            Err(err) => Err(err),
        };
        if let Err(err) = res {
            let msg = err.to_string();
            if !msg.contains("Table already exists")
                && !msg.contains("Duplicate key")
                && !msg.contains("already in cache")
            {
                return Err(err);
            }
        }
    }
    Ok(())
}

fn rainbow_statements() -> IgniteResult<Vec<String>> {
    let raw = fs::read_to_string(
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests")
            .join("resources")
            .join("rainbow.sql"),
    )
    .map_err(IgniteError::from)?;

    let uncommented = raw
        .lines()
        .map(|line| line.split_once("--").map(|(head, _)| head).unwrap_or(line))
        .collect::<Vec<_>>()
        .join("\n");

    Ok(uncommented
        .split(';')
        .map(str::trim)
        .filter(|stmt| !stmt.is_empty())
        .map(ToOwned::to_owned)
        .collect())
}
